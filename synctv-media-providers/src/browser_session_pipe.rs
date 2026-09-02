use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::Deserialize;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::Semaphore;
use url::Url;

use crate::web_session::{SessionCookie, WebPagePlaybackDiscovery};
use crate::{ProviderClientError, PROVIDER_DESKTOP_WEB_USER_AGENT};

const BROWSER_QUEUE_TIMEOUT: Duration = Duration::from_secs(8);
const BROWSER_RENDER_TIMEOUT: Duration = Duration::from_secs(27);
const BROWSER_ACTIVE_CAPTURE_TIMEOUT: Duration = Duration::from_secs(17);
const CDP_STARTUP_TIMEOUT: Duration = Duration::from_secs(12);
const CDP_COMMAND_TIMEOUT: Duration = Duration::from_secs(4);
const CDP_POLL_TIMEOUT: Duration = Duration::from_millis(1200);
const BOOTSTRAP_SETTLE_DELAY: Duration = Duration::from_millis(450);
const BOOTSTRAP_POLL_INTERVAL: Duration = Duration::from_millis(400);
const BROWSER_PROFILE_CLEANUP_DELAY: Duration = Duration::from_millis(300);
const BROWSER_RENDER_COMPLETION_RESERVE: Duration = Duration::from_millis(1200);
const MAX_CONCURRENT_BROWSER_RENDERS: usize = 1;
const MAX_CDP_MESSAGE_BYTES: usize = 2 * 1024 * 1024;
const LOW_MEMORY_SINGLE_PROCESS_THRESHOLD_KIB: u64 = 1_200_000;

static BROWSER_RENDER_SEMAPHORE: Semaphore = Semaphore::const_new(MAX_CONCURRENT_BROWSER_RENDERS);

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrowserPageDiagnostics {
    pub ready_state: String,
    pub html_length: usize,
    pub resource_count: usize,
    pub media_resource_count: usize,
    pub video_element_count: usize,
    pub source_element_count: usize,
    pub has_m3u8: bool,
    pub has_mpd: bool,
    pub has_mp4: bool,
    pub has_blob_video: bool,
    pub has_video_id: bool,
    pub has_tv_id: bool,
    pub has_drm_marker: bool,
    pub has_license_resource: bool,
}

#[derive(Debug, Clone)]
pub struct BrowserPageObservation {
    pub discovery: WebPagePlaybackDiscovery,
    pub diagnostics: BrowserPageDiagnostics,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BootstrapSnapshot {
    current_url: String,
    title: String,
    ready_state: String,
    candidate_urls: Vec<String>,
    xhr_count: usize,
    fetch_count: usize,
    scanned_response_count: usize,
    truncated_response_count: usize,
    inline_manifest_count: usize,
    has_license_resource: bool,
}

#[derive(Default)]
struct CookiePreparationStats {
    input_count: usize,
    effective_count: usize,
    expired_dropped: usize,
    duplicate_dropped: usize,
}

struct ChromiumProcess {
    child: Child,
    profile_dir: Option<PathBuf>,
    page_host: String,
}

impl ChromiumProcess {
    async fn shutdown(&mut self) {
        let started = Instant::now();
        let kill_requested = self.child.start_kill().is_ok();
        let wait_finished = response_first_timeout(Duration::from_millis(700), self.child.wait())
            .await
            .is_ok();
        let mut cleanup_deferred = false;
        if let Some(profile_dir) = self.profile_dir.take() {
            tokio::time::sleep(Duration::from_millis(60)).await;
            if let Err(error) = tokio::fs::remove_dir_all(&profile_dir).await {
                if error.kind() != std::io::ErrorKind::NotFound {
                    cleanup_deferred = true;
                    schedule_profile_cleanup(profile_dir, self.page_host.clone());
                }
            }
        }
        tracing::info!(
            target: "synctv_media_providers::browser_session",
            stage = "browser_shutdown",
            page_host = %self.page_host,
            kill_requested,
            wait_finished,
            cleanup_deferred,
            elapsed_ms = started.elapsed().as_millis(),
            "Minimal provider bootstrap browser shutdown"
        );
    }
}

impl Drop for ChromiumProcess {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
        if let Some(profile_dir) = self.profile_dir.take() {
            schedule_profile_cleanup(profile_dir, self.page_host.clone());
        }
    }
}

fn schedule_profile_cleanup(profile_dir: PathBuf, page_host: String) {
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        return;
    };
    let _cleanup_task = runtime.spawn(async move {
        for (attempt, delay) in [300_u64, 700, 1400].into_iter().enumerate() {
            tokio::time::sleep(Duration::from_millis(delay)).await;
            match tokio::fs::remove_dir_all(&profile_dir).await {
                Ok(()) => {
                    tracing::info!(
                        target: "synctv_media_providers::browser_session",
                        stage = "browser_deferred_cleanup_complete",
                        page_host = %page_host,
                        attempt = attempt + 1,
                        "Deferred Chromium profile cleanup completed"
                    );
                    return;
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
                Err(_) if attempt < 2 => {}
                Err(error) => tracing::warn!(
                    target: "synctv_media_providers::browser_session",
                    stage = "browser_deferred_cleanup_failed",
                    page_host = %page_host,
                    error = %error,
                    "Deferred Chromium profile cleanup failed"
                ),
            }
        }
    });
}

async fn response_first_timeout<T>(
    timeout: Duration,
    future: impl std::future::Future<Output = T>,
) -> Result<T, ()> {
    tokio::pin!(future);
    let deadline = tokio::time::sleep(timeout);
    tokio::pin!(deadline);
    tokio::select! {
        biased;
        output = &mut future => Ok(output),
        () = &mut deadline => Err(()),
    }
}

struct CdpPipe {
    input: ChildStdin,
    output: ChildStdout,
    buffered: Vec<u8>,
}

impl CdpPipe {
    async fn send(&mut self, payload: &Value) -> Result<(), ProviderClientError> {
        let mut bytes = serde_json::to_vec(payload)
            .map_err(|error| ProviderClientError::Parse(error.to_string()))?;
        bytes.push(0);
        self.input.write_all(&bytes).await.map_err(|error| {
            ProviderClientError::Network(format!("write Chromium CDP pipe: {error}"))
        })?;
        self.input.flush().await.map_err(|error| {
            ProviderClientError::Network(format!("flush Chromium CDP pipe: {error}"))
        })?;
        Ok(())
    }

    async fn receive(&mut self) -> Result<Value, ProviderClientError> {
        loop {
            if let Some(index) = self.buffered.iter().position(|byte| *byte == 0) {
                let frame = self.buffered.drain(..=index).collect::<Vec<_>>();
                let payload = &frame[..frame.len().saturating_sub(1)];
                if payload.is_empty() {
                    continue;
                }
                return serde_json::from_slice(payload)
                    .map_err(|error| ProviderClientError::Parse(error.to_string()));
            }
            if self.buffered.len() >= MAX_CDP_MESSAGE_BYTES {
                return Err(ProviderClientError::Network(format!(
                    "Chromium CDP pipe frame exceeded {MAX_CDP_MESSAGE_BYTES} bytes"
                )));
            }
            let mut chunk = [0_u8; 4096];
            let read = self.output.read(&mut chunk).await.map_err(|error| {
                ProviderClientError::Network(format!("read Chromium CDP pipe: {error}"))
            })?;
            if read == 0 {
                return Err(ProviderClientError::Network(
                    "Chromium CDP pipe closed".to_string(),
                ));
            }
            self.buffered.extend_from_slice(&chunk[..read]);
        }
    }
}

pub async fn render_web_page_playback(
    raw_url: &str,
    allowed_domains: &'static [&'static str],
    cookies: &[SessionCookie],
) -> Result<BrowserPageObservation, ProviderClientError> {
    let page_url = validate_provider_url(raw_url, allowed_domains)?;
    let page_host = page_url.host_str().unwrap_or("").to_string();
    let single_process = chromium_single_process_enabled();
    let request_started = Instant::now();

    tracing::info!(
        target: "synctv_media_providers::browser_session",
        stage = "bootstrap_render_requested",
        page_host = %page_host,
        render_timeout_ms = BROWSER_RENDER_TIMEOUT.as_millis(),
        active_capture_timeout_ms = BROWSER_ACTIVE_CAPTURE_TIMEOUT.as_millis(),
        max_concurrent_renders = MAX_CONCURRENT_BROWSER_RENDERS,
        single_process,
        strategy = "xhr_bootstrap_hook",
        "Starting minimal official-page bootstrap capture"
    );

    let permit = response_first_timeout(BROWSER_QUEUE_TIMEOUT, BROWSER_RENDER_SEMAPHORE.acquire())
        .await
        .map_err(|()| {
            ProviderClientError::Network("browser bootstrap queue timed out".to_string())
        })?
        .map_err(|error| {
            ProviderClientError::Network(format!("browser bootstrap semaphore closed: {error}"))
        })?;

    let render_started = Instant::now();
    let deadline = render_started + BROWSER_RENDER_TIMEOUT;
    let result = match response_first_timeout(
        BROWSER_RENDER_TIMEOUT,
        render_bootstrap_inner(&page_url, allowed_domains, cookies, single_process, deadline),
    )
    .await
    {
        Ok(result) => result,
        Err(()) => Err(ProviderClientError::Network(format!(
            "minimal browser bootstrap timed out after {}s",
            BROWSER_RENDER_TIMEOUT.as_secs()
        ))),
    };
    drop(permit);

    tracing::info!(
        target: "synctv_media_providers::browser_session",
        stage = "bootstrap_render_finished",
        page_host = %page_host,
        success = result.is_ok(),
        elapsed_ms = render_started.elapsed().as_millis(),
        total_elapsed_ms = request_started.elapsed().as_millis(),
        cgroup_memory_current_bytes = cgroup_memory_current_bytes().unwrap_or_default(),
        cgroup_pids_current = cgroup_pids_current().unwrap_or_default(),
        strategy = "xhr_bootstrap_hook",
        "Minimal official-page bootstrap capture finished"
    );
    result
}

async fn render_bootstrap_inner(
    page_url: &Url,
    allowed_domains: &'static [&'static str],
    cookies: &[SessionCookie],
    single_process: bool,
    deadline: Instant,
) -> Result<BrowserPageObservation, ProviderClientError> {
    let page_host = page_url.host_str().unwrap_or("").to_string();
    let profile_dir =
        std::env::temp_dir().join(format!("synctv-chromium-{}", uuid::Uuid::new_v4().simple()));
    tokio::fs::create_dir_all(&profile_dir)
        .await
        .map_err(|error| ProviderClientError::Network(format!("create browser profile: {error}")))?;

    let (mut browser, mut pipe) = match start_chromium_pipe(&profile_dir, &page_host, single_process).await {
        Ok(value) => value,
        Err(error) => {
            schedule_profile_cleanup(profile_dir, page_host.clone());
            return Err(error);
        }
    };

    let result = async {
        let startup_started = Instant::now();
        let mut command_id = 0_u64;

        let stage_started = Instant::now();
        let targets = cdp_call(
            &mut pipe,
            &mut command_id,
            None,
            "Target.getTargets",
            json!({}),
            CDP_STARTUP_TIMEOUT,
        )
        .await?;
        let target_wait_ms = stage_started.elapsed().as_millis();
        let target_id = first_page_target_id(&targets).ok_or_else(|| {
            ProviderClientError::Parse("Chromium returned no page target".to_string())
        })?;

        let (cookie_params, cookie_stats) = prepare_chromium_cookies(cookies);
        let stage_started = Instant::now();
        if !cookie_params.is_empty() {
            cdp_call(
                &mut pipe,
                &mut command_id,
                None,
                "Storage.setCookies",
                json!({ "cookies": cookie_params }),
                CDP_COMMAND_TIMEOUT,
            )
            .await?;
        }
        let cookie_install_ms = stage_started.elapsed().as_millis();

        let stage_started = Instant::now();
        let attached = cdp_call(
            &mut pipe,
            &mut command_id,
            None,
            "Target.attachToTarget",
            json!({ "targetId": target_id, "flatten": true }),
            CDP_COMMAND_TIMEOUT,
        )
        .await?;
        let attach_ms = stage_started.elapsed().as_millis();
        let session_id = attached
            .get("sessionId")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ProviderClientError::Parse(
                    "Chromium Target.attachToTarget returned no sessionId".to_string(),
                )
            })?
            .to_string();

        let stage_started = Instant::now();
        cdp_call(
            &mut pipe,
            &mut command_id,
            Some(&session_id),
            "Page.addScriptToEvaluateOnNewDocument",
            json!({ "source": bootstrap_hook_script() }),
            CDP_COMMAND_TIMEOUT,
        )
        .await?;
        let hook_install_ms = stage_started.elapsed().as_millis();
        let startup_elapsed_ms = startup_started.elapsed().as_millis();

        tracing::info!(
            target: "synctv_media_providers::browser_session",
            stage = "bootstrap_hook_ready",
            page_host = %page_host,
            cookie_input_count = cookie_stats.input_count,
            cookie_count = cookie_stats.effective_count,
            cookie_expired_dropped = cookie_stats.expired_dropped,
            cookie_duplicate_dropped = cookie_stats.duplicate_dropped,
            target_wait_ms,
            cookie_install_ms,
            attach_ms,
            hook_install_ms,
            startup_elapsed_ms,
            body_scan_limit_bytes = 262_144,
            fetch_body_scan_limit_bytes = 65_536,
            chromium_events_enabled = false,
            full_dom_probe = false,
            codec_probe = false,
            forced_video_play = false,
            "Installed bounded XHR/fetch bootstrap hook before navigation"
        );

        let _navigation_id = cdp_send_command(
            &mut pipe,
            &mut command_id,
            Some(&session_id),
            "Page.navigate",
            json!({ "url": page_url.as_str() }),
        )
        .await?;
        let navigation_started = Instant::now();
        let active_deadline = std::cmp::min(
            deadline,
            navigation_started + BROWSER_ACTIVE_CAPTURE_TIMEOUT,
        );
        tracing::info!(
            target: "synctv_media_providers::browser_session",
            stage = "bootstrap_navigation_started",
            page_host = %page_host,
            startup_elapsed_ms,
            active_budget_ms = active_deadline
                .saturating_duration_since(navigation_started)
                .as_millis(),
            hard_remaining_ms = deadline.saturating_duration_since(navigation_started).as_millis(),
            "Official provider page navigation started with a separate active-capture budget"
        );
        tokio::time::sleep(BOOTSTRAP_SETTLE_DELAY).await;

        let snapshot = poll_bootstrap(
            &mut pipe,
            &mut command_id,
            &session_id,
            &page_host,
            active_deadline,
        )
        .await?;

        if !snapshot.candidate_urls.is_empty() {
            let _stop_id = cdp_send_command(
                &mut pipe,
                &mut command_id,
                Some(&session_id),
                "Page.stopLoading",
                json!({}),
            )
            .await;
        }

        let final_url = match validate_provider_url(&snapshot.current_url, allowed_domains) {
            Ok(url) => url,
            Err(_) if snapshot.current_url.is_empty() || snapshot.current_url == "about:blank" => {
                page_url.clone()
            }
            Err(error) => return Err(error),
        };
        let media_urls = normalize_media_urls(&final_url, snapshot.candidate_urls.clone());
        let lower_urls = media_urls
            .iter()
            .map(|value| value.to_ascii_lowercase())
            .collect::<Vec<_>>();
        let has_m3u8 = lower_urls.iter().any(|value| value.contains(".m3u8"));
        let has_mpd = lower_urls.iter().any(|value| value.contains(".mpd"));
        let has_mp4 = lower_urls.iter().any(|value| value.contains(".mp4"));
        let provider_request_count = snapshot.xhr_count.saturating_add(snapshot.fetch_count);
        let diagnostics = BrowserPageDiagnostics {
            ready_state: snapshot.ready_state.clone(),
            html_length: 0,
            resource_count: provider_request_count,
            media_resource_count: media_urls.len(),
            video_element_count: 0,
            source_element_count: 0,
            has_m3u8,
            has_mpd,
            has_mp4,
            has_blob_video: false,
            has_video_id: false,
            has_tv_id: false,
            has_drm_marker: snapshot.has_license_resource,
            has_license_resource: snapshot.has_license_resource,
        };

        tracing::info!(
            target: "synctv_media_providers::browser_session",
            stage = "bootstrap_observation_complete",
            page_host = %page_host,
            ready_state = %snapshot.ready_state,
            provider_request_count,
            xhr_count = snapshot.xhr_count,
            fetch_count = snapshot.fetch_count,
            scanned_response_count = snapshot.scanned_response_count,
            truncated_response_count = snapshot.truncated_response_count,
            inline_manifest_count = snapshot.inline_manifest_count,
            candidate_count = media_urls.len(),
            has_license_resource = snapshot.has_license_resource,
            active_remaining_ms = active_deadline
                .saturating_duration_since(Instant::now())
                .as_millis(),
            hard_remaining_ms = deadline.saturating_duration_since(Instant::now()).as_millis(),
            "Minimal iQiyi player-bootstrap observation completed"
        );

        Ok(BrowserPageObservation {
            discovery: WebPagePlaybackDiscovery {
                page_url: final_url.to_string(),
                title: (!snapshot.title.trim().is_empty())
                    .then(|| snapshot.title.trim().to_string()),
                media_urls,
                drm_detected: snapshot.has_license_resource,
            },
            diagnostics,
        })
    }
    .await;

    drop(pipe);
    browser.shutdown().await;
    result
}

async fn poll_bootstrap(
    pipe: &mut CdpPipe,
    command_id: &mut u64,
    session_id: &str,
    page_host: &str,
    deadline: Instant,
) -> Result<BootstrapSnapshot, ProviderClientError> {
    let started = Instant::now();
    let mut last_snapshot: Option<BootstrapSnapshot> = None;
    let mut stable_response_polls = 0_usize;
    let mut previous_response_count = 0_usize;
    let mut attempts = 0_usize;

    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining <= BROWSER_RENDER_COMPLETION_RESERVE {
            break;
        }
        attempts = attempts.saturating_add(1);
        let timeout = std::cmp::min(
            CDP_POLL_TIMEOUT,
            remaining.saturating_sub(BROWSER_RENDER_COMPLETION_RESERVE),
        );
        let evaluation = cdp_call(
            pipe,
            command_id,
            Some(session_id),
            "Runtime.evaluate",
            json!({
                "expression": bootstrap_snapshot_script(),
                "returnByValue": true,
                "awaitPromise": false,
            }),
            timeout,
        )
        .await;

        match evaluation {
            Ok(value) => {
                let serialized = value
                    .pointer("/result/value")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        ProviderClientError::Parse(
                            "bootstrap probe did not return a serialized value".to_string(),
                        )
                    })?;
                let snapshot: BootstrapSnapshot = serde_json::from_str(serialized)
                    .map_err(|error| ProviderClientError::Parse(error.to_string()))?;
                let provider_request_count = snapshot.xhr_count.saturating_add(snapshot.fetch_count);

                tracing::info!(
                    target: "synctv_media_providers::browser_session",
                    stage = "bootstrap_probe",
                    page_host = %page_host,
                    attempt = attempts,
                    elapsed_ms = started.elapsed().as_millis(),
                    ready_state = %snapshot.ready_state,
                    provider_request_count,
                    xhr_count = snapshot.xhr_count,
                    fetch_count = snapshot.fetch_count,
                    scanned_response_count = snapshot.scanned_response_count,
                    truncated_response_count = snapshot.truncated_response_count,
                    inline_manifest_count = snapshot.inline_manifest_count,
                    candidate_count = snapshot.candidate_urls.len(),
                    "Polling only the tiny bootstrap capture state"
                );

                if !snapshot.candidate_urls.is_empty() || snapshot.has_license_resource {
                    return Ok(snapshot);
                }

                if snapshot.scanned_response_count > previous_response_count {
                    stable_response_polls = 0;
                } else if snapshot.scanned_response_count > 0 {
                    stable_response_polls = stable_response_polls.saturating_add(1);
                }
                previous_response_count = snapshot.scanned_response_count;
                let page_settled = matches!(snapshot.ready_state.as_str(), "interactive" | "complete");
                if page_settled && stable_response_polls >= 2 {
                    return Ok(snapshot);
                }
                last_snapshot = Some(snapshot);
            }
            Err(error) => {
                tracing::info!(
                    target: "synctv_media_providers::browser_session",
                    stage = "bootstrap_probe_delayed",
                    page_host = %page_host,
                    attempt = attempts,
                    elapsed_ms = started.elapsed().as_millis(),
                    error = %error,
                    "Renderer was busy; keeping the same Chromium and retrying the tiny bootstrap snapshot"
                );
            }
        }

        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining <= BROWSER_RENDER_COMPLETION_RESERVE + BOOTSTRAP_POLL_INTERVAL {
            break;
        }
        tokio::time::sleep(BOOTSTRAP_POLL_INTERVAL).await;
    }

    last_snapshot.ok_or_else(|| {
        ProviderClientError::Network(
            "iQiyi bootstrap capture ended before the page returned a usable snapshot".to_string(),
        )
    })
}

async fn start_chromium_pipe(
    profile_dir: &Path,
    page_host: &str,
    single_process: bool,
) -> Result<(ChromiumProcess, CdpPipe), ProviderClientError> {
    let chromium_bin = chromium_binary();
    let stderr_file = std::fs::File::create(profile_dir.join("chromium-stderr.log")).map_err(|error| {
        ProviderClientError::Network(format!("create Chromium stderr log: {error}"))
    })?;
    let mut command = Command::new("/bin/sh");
    command
        .arg("-c")
        .arg("exec 3<&0 4>&1; exec \"$@\"")
        .arg("synctv-chromium-pipe")
        .arg(&chromium_bin)
        .arg("--headless=new")
        .arg("--no-sandbox")
        .arg("--no-zygote")
        .arg("--disable-dev-shm-usage")
        .arg("--disable-gpu")
        .arg("--disable-background-networking")
        .arg("--disable-component-update")
        .arg("--disable-default-apps")
        .arg("--disable-extensions")
        .arg("--disable-sync")
        .arg("--disable-notifications")
        .arg("--disable-domain-reliability")
        .arg("--disable-breakpad")
        .arg("--disable-crash-reporter")
        .arg("--disable-application-cache")
        .arg("--mute-audio")
        .arg("--blink-settings=imagesEnabled=false")
        .arg("--disk-cache-size=1")
        .arg("--media-cache-size=1")
        .arg("--renderer-process-limit=1");
    if single_process {
        command.arg("--single-process");
    }
    command
        .arg("--remote-debugging-pipe")
        .arg(format!("--user-agent={PROVIDER_DESKTOP_WEB_USER_AGENT}"))
        .arg(format!("--user-data-dir={}", profile_dir.display()))
        .arg("about:blank")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::from(stderr_file))
        .kill_on_drop(true);

    let mut child = command.spawn().map_err(|error| {
        ProviderClientError::Network(format!("start Chromium pipe ({chromium_bin}): {error}"))
    })?;
    let browser_pid = child.id().unwrap_or_default();
    let input = child.stdin.take().ok_or_else(|| {
        ProviderClientError::Network("Chromium CDP pipe stdin was not created".to_string())
    })?;
    let output = child.stdout.take().ok_or_else(|| {
        ProviderClientError::Network("Chromium CDP pipe stdout was not created".to_string())
    })?;

    tracing::info!(
        target: "synctv_media_providers::browser_session",
        stage = "bootstrap_chromium_spawned",
        page_host = %page_host,
        chromium_bin = %chromium_bin,
        browser_pid,
        single_process,
        cgroup_memory_current_bytes = cgroup_memory_current_bytes().unwrap_or_default(),
        cgroup_pids_current = cgroup_pids_current().unwrap_or_default(),
        "Started stripped-down Chromium only to let the official page create its bootstrap request"
    );

    Ok((
        ChromiumProcess {
            child,
            profile_dir: Some(profile_dir.to_path_buf()),
            page_host: page_host.to_string(),
        },
        CdpPipe {
            input,
            output,
            buffered: Vec::with_capacity(4096),
        },
    ))
}

async fn cdp_send_command(
    pipe: &mut CdpPipe,
    command_id: &mut u64,
    session_id: Option<&str>,
    method: &str,
    params: Value,
) -> Result<u64, ProviderClientError> {
    *command_id = command_id.saturating_add(1);
    let current_id = *command_id;
    let mut payload = json!({
        "id": current_id,
        "method": method,
        "params": params,
    });
    if let Some(session_id) = session_id {
        if let Some(object) = payload.as_object_mut() {
            object.insert("sessionId".to_string(), json!(session_id));
        }
    }
    pipe.send(&payload).await?;
    Ok(current_id)
}

async fn cdp_call(
    pipe: &mut CdpPipe,
    command_id: &mut u64,
    session_id: Option<&str>,
    method: &str,
    params: Value,
    timeout: Duration,
) -> Result<Value, ProviderClientError> {
    let current_id = cdp_send_command(pipe, command_id, session_id, method, params).await?;
    let result = response_first_timeout(timeout, async {
        loop {
            let response = pipe.receive().await?;
            if response.get("id").and_then(Value::as_u64) != Some(current_id) {
                continue;
            }
            if let Some(expected_session) = session_id {
                if response.get("sessionId").and_then(Value::as_str) != Some(expected_session) {
                    continue;
                }
            }
            if let Some(error) = response.get("error") {
                return Err(ProviderClientError::Network(format!(
                    "Chromium CDP command {method} failed: {error}"
                )));
            }
            return Ok(response.get("result").cloned().unwrap_or(Value::Null));
        }
    })
    .await;
    result.map_err(|()| {
        ProviderClientError::Network(format!(
            "Chromium CDP command {method} timed out after {}ms",
            timeout.as_millis()
        ))
    })?
}

fn first_page_target_id(targets: &Value) -> Option<String> {
    targets
        .get("targetInfos")?
        .as_array()?
        .iter()
        .find_map(|target| {
            (target.get("type").and_then(Value::as_str) == Some("page"))
                .then(|| target.get("targetId").and_then(Value::as_str))
                .flatten()
                .map(str::to_string)
        })
}

fn prepare_chromium_cookies(cookies: &[SessionCookie]) -> (Vec<Value>, CookiePreparationStats) {
    let now = unix_timestamp_now();
    let mut seen = HashSet::new();
    let mut prepared = Vec::with_capacity(cookies.len());
    let mut stats = CookiePreparationStats {
        input_count: cookies.len(),
        ..CookiePreparationStats::default()
    };

    for cookie in cookies.iter().rev() {
        if cookie.expires_at.is_some_and(|expires_at| expires_at <= now) {
            stats.expired_dropped += 1;
            continue;
        }
        let domain = cookie
            .domain
            .trim()
            .trim_start_matches('.')
            .to_ascii_lowercase();
        let path = if cookie.path.is_empty() {
            "/".to_string()
        } else {
            cookie.path.clone()
        };
        if !seen.insert((cookie.name.clone(), domain, path)) {
            stats.duplicate_dropped += 1;
            continue;
        }
        prepared.push(chromium_cookie_param(cookie));
    }
    prepared.reverse();
    stats.effective_count = prepared.len();
    (prepared, stats)
}

fn chromium_cookie_param(cookie: &SessionCookie) -> Value {
    let path = if cookie.path.is_empty() {
        "/"
    } else {
        cookie.path.as_str()
    };
    let mut value = json!({
        "name": cookie.name,
        "value": cookie.value,
        "domain": cookie.domain.trim(),
        "path": path,
        "secure": cookie.secure,
        "httpOnly": cookie.http_only,
    });
    if !cookie.session_only {
        if let Some(expires_at) = cookie.expires_at {
            if let Some(object) = value.as_object_mut() {
                object.insert("expires".to_string(), json!(expires_at));
            }
        }
    }
    value
}

fn normalize_media_urls(page_url: &Url, raw_urls: Vec<String>) -> Vec<String> {
    let mut seen = HashSet::new();
    raw_urls
        .into_iter()
        .filter_map(|raw| {
            let trimmed = raw.trim();
            if trimmed.is_empty() || trimmed.starts_with("blob:") || trimmed.starts_with("data:") {
                return None;
            }
            let url = page_url.join(trimmed).ok()?;
            if !matches!(url.scheme(), "http" | "https") {
                return None;
            }
            let normalized = url.to_string();
            seen.insert(normalized.clone()).then_some(normalized)
        })
        .collect()
}

fn validate_provider_url(
    raw_url: &str,
    allowed_domains: &[&str],
) -> Result<Url, ProviderClientError> {
    let url = Url::parse(raw_url)
        .map_err(|error| ProviderClientError::InvalidConfig(error.to_string()))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(ProviderClientError::InvalidConfig(
            "browser provider session only supports HTTP(S) URLs".to_string(),
        ));
    }
    let host = url.host_str().ok_or_else(|| {
        ProviderClientError::InvalidConfig("browser provider URL has no host".to_string())
    })?;
    if !allowed_domains
        .iter()
        .any(|allowed| domain_matches(host, allowed))
    {
        return Err(ProviderClientError::InvalidConfig(format!(
            "browser provider URL host is outside the session allowlist: {host}"
        )));
    }
    Ok(url)
}

fn domain_matches(host: &str, allowed_domain: &str) -> bool {
    let host = host.trim().trim_start_matches('.').to_ascii_lowercase();
    let allowed = allowed_domain
        .trim()
        .trim_start_matches('.')
        .to_ascii_lowercase();
    !host.is_empty()
        && !allowed.is_empty()
        && (host == allowed || host.ends_with(&format!(".{allowed}")))
}

fn chromium_binary() -> String {
    std::env::var("CHROMIUM_BIN").unwrap_or_else(|_| {
        if Path::new("/usr/bin/chromium").is_file() {
            "/usr/bin/chromium".to_string()
        } else {
            "chromium".to_string()
        }
    })
}

fn chromium_single_process_enabled() -> bool {
    if let Ok(raw) = std::env::var("SYNCTV_CHROMIUM_SINGLE_PROCESS") {
        let value = raw.trim().to_ascii_lowercase();
        if matches!(value.as_str(), "1" | "true" | "yes" | "on") {
            return true;
        }
        if matches!(value.as_str(), "0" | "false" | "no" | "off") {
            return false;
        }
    }
    host_mem_total_kib().is_some_and(|total| total <= LOW_MEMORY_SINGLE_PROCESS_THRESHOLD_KIB)
}

fn unix_timestamp_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_secs()).ok())
        .unwrap_or(i64::MAX)
}

fn read_trimmed_file(path: &str) -> Option<String> {
    let value = std::fs::read_to_string(path).ok()?;
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_string())
}

fn cgroup_memory_current_bytes() -> Option<u64> {
    read_trimmed_file("/sys/fs/cgroup/memory.current")?.parse().ok()
}

fn cgroup_pids_current() -> Option<u64> {
    read_trimmed_file("/sys/fs/cgroup/pids.current")?.parse().ok()
}

fn host_mem_total_kib() -> Option<u64> {
    std::fs::read_to_string("/proc/meminfo")
        .ok()?
        .lines()
        .find_map(|line| {
            let value = line.strip_prefix("MemTotal:")?.trim();
            value.split_whitespace().next()?.parse().ok()
        })
}

fn bootstrap_snapshot_script() -> &'static str {
    r#"(() => {
        const state = window.__synctvIqiyiBootstrap || {};
        return JSON.stringify({
            currentUrl: location.href || '',
            title: document.title || '',
            readyState: document.readyState || '',
            candidateUrls: Array.isArray(state.candidateUrls) ? state.candidateUrls : [],
            xhrCount: Number(state.xhrCount) || 0,
            fetchCount: Number(state.fetchCount) || 0,
            scannedResponseCount: Number(state.scannedResponseCount) || 0,
            truncatedResponseCount: Number(state.truncatedResponseCount) || 0,
            inlineManifestCount: Number(state.inlineManifestCount) || 0,
            hasLicenseResource: state.hasLicenseResource === true
        });
    })()"#
}

fn bootstrap_hook_script() -> &'static str {
    r#"(() => {
        'use strict';
        const MAX_XHR_CHARS = 262144;
        const MAX_FETCH_BYTES = 65536;
        const MAX_CANDIDATES = 24;
        const MAX_JSON_NODES = 1500;
        const MEDIA_KEY_RE = /(?:m3u8|mpd|play.?url|stream.?url|video.?url|media.?url|playback.?url)/i;
        const state = window.__synctvIqiyiBootstrap = {
            candidateUrls: [],
            xhrCount: 0,
            fetchCount: 0,
            scannedResponseCount: 0,
            truncatedResponseCount: 0,
            inlineManifestCount: 0,
            hasLicenseResource: false
        };
        const seen = new Set();
        const providerHost = (raw) => {
            try {
                const host = new URL(String(raw || ''), location.href).hostname.toLowerCase();
                return host === 'iqiyi.com' || host.endsWith('.iqiyi.com') || host === 'qiyi.com' || host.endsWith('.qiyi.com');
            } catch (_) {
                return false;
            }
        };
        const normalize = (text) => String(text || '')
            .replace(/\\u003a/gi, ':')
            .replace(/\\u002f/gi, '/')
            .replace(/\\u0026/gi, '&')
            .replace(/\\u003d/gi, '=')
            .replace(/\\x3a/gi, ':')
            .replace(/\\x2f/gi, '/')
            .replace(/\\x26/gi, '&')
            .replace(/\\x3d/gi, '=')
            .replace(/\\\//g, '/')
            .replace(/&amp;/gi, '&');
        const keepUrl = (raw, trustedMediaField = false) => {
            if (state.candidateUrls.length >= MAX_CANDIDATES) return;
            try {
                const value = new URL(String(raw || ''), location.href);
                if (value.protocol !== 'http:' && value.protocol !== 'https:') return;
                const lower = value.href.toLowerCase();
                if (!trustedMediaField && !/(?:\.m3u8|\.mpd|\.mp4|\.m4s|\.ts)(?:[?#]|$)/i.test(lower)) return;
                if (seen.has(value.href)) return;
                seen.add(value.href);
                state.candidateUrls.push(value.href);
            } catch (_) {}
        };
        const scanJson = (value, keyHint, depth, budget) => {
            if (budget.count <= 0 || depth > 8 || state.candidateUrls.length >= MAX_CANDIDATES) return;
            budget.count -= 1;
            if (typeof value === 'string') {
                const text = value.trim();
                if (MEDIA_KEY_RE.test(keyHint || '') && /^(?:https?:)?\/\//i.test(text)) {
                    keepUrl(text, true);
                }
                return;
            }
            if (!value || typeof value !== 'object') return;
            if (Array.isArray(value)) {
                for (const item of value) {
                    scanJson(item, keyHint, depth + 1, budget);
                    if (budget.count <= 0 || state.candidateUrls.length >= MAX_CANDIDATES) break;
                }
                return;
            }
            for (const [key, child] of Object.entries(value)) {
                scanJson(child, key, depth + 1, budget);
                if (budget.count <= 0 || state.candidateUrls.length >= MAX_CANDIDATES) break;
            }
        };
        const scan = (rawText) => {
            if (!rawText) return;
            let text = String(rawText);
            if (text.length > MAX_XHR_CHARS) {
                state.truncatedResponseCount += 1;
                text = text.slice(0, MAX_XHR_CHARS);
            }
            text = normalize(text);
            state.scannedResponseCount += 1;
            if (text.includes('#EXTM3U')) state.inlineManifestCount += 1;
            if (/(widevine|playready|com\.widevine\.alpha|license[_-]?url)/i.test(text)) {
                state.hasLicenseResource = true;
            }
            const absolute = /(?:https?:)?\/\/[^\s\"'<>\\]+?(?:\.m3u8|\.mpd|\.mp4|\.m4s|\.ts)(?:\?[^\s\"'<>\\]*)?/gi;
            for (const match of text.matchAll(absolute)) keepUrl(match[0], false);
            const trimmed = text.trimStart();
            if (trimmed.startsWith('{') || trimmed.startsWith('[')) {
                try {
                    scanJson(JSON.parse(text), '', 0, { count: MAX_JSON_NODES });
                } catch (_) {}
            }
        };
        const readFetchTextLimited = async (response) => {
            const body = response && response.body;
            if (!body || typeof body.getReader !== 'function') {
                const text = String(await response.text());
                if (text.length > MAX_FETCH_BYTES) state.truncatedResponseCount += 1;
                return text.slice(0, MAX_FETCH_BYTES);
            }
            const reader = body.getReader();
            const decoder = new TextDecoder();
            let text = '';
            let total = 0;
            try {
                while (total < MAX_FETCH_BYTES) {
                    const result = await reader.read();
                    if (result.done) break;
                    const value = result.value || new Uint8Array();
                    const remaining = MAX_FETCH_BYTES - total;
                    const chunk = value.byteLength > remaining ? value.subarray(0, remaining) : value;
                    total += chunk.byteLength;
                    text += decoder.decode(chunk, { stream: total < MAX_FETCH_BYTES });
                    if (value.byteLength > remaining) {
                        state.truncatedResponseCount += 1;
                        break;
                    }
                }
                text += decoder.decode();
            } catch (_) {
                return text;
            } finally {
                try {
                    await reader.cancel();
                } catch (_) {}
            }
            return text;
        };

        const Xhr = window.XMLHttpRequest;
        if (Xhr && Xhr.prototype) {
            const originalOpen = Xhr.prototype.open;
            Xhr.prototype.open = function(method, url, ...rest) {
                this.__synctvProviderUrl = providerHost(url) ? String(url) : '';
                return originalOpen.call(this, method, url, ...rest);
            };
            const originalSend = Xhr.prototype.send;
            Xhr.prototype.send = function(...args) {
                if (this.__synctvProviderUrl) {
                    state.xhrCount += 1;
                    this.addEventListener('loadend', () => {
                        try {
                            if (this.responseType === '' || this.responseType === 'text') {
                                scan(this.responseText || '');
                            } else if (this.responseType === 'json' && this.response != null) {
                                scan(JSON.stringify(this.response));
                            }
                        } catch (_) {}
                    }, { once: true });
                }
                return originalSend.apply(this, args);
            };
        }

        const originalFetch = window.fetch;
        if (typeof originalFetch === 'function') {
            window.fetch = function(input, init) {
                const rawUrl = typeof input === 'string' ? input : (input && input.url) || '';
                const provider = providerHost(rawUrl);
                if (provider) state.fetchCount += 1;
                return originalFetch.call(this, input, init).then((response) => {
                    if (!provider) return response;
                    try {
                        const contentLength = Number(response.headers.get('content-length') || 0);
                        const contentType = String(response.headers.get('content-type') || '').toLowerCase();
                        if (contentLength > MAX_FETCH_BYTES) {
                            state.truncatedResponseCount += 1;
                        } else if (/(json|text|javascript)/.test(contentType)) {
                            readFetchTextLimited(response.clone()).then(scan).catch(() => {});
                        }
                    } catch (_) {}
                    return response;
                });
            };
        }
    })();"#
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cookie(name: &str, value: &str, expires_at: Option<i64>) -> SessionCookie {
        SessionCookie {
            name: name.to_string(),
            value: value.to_string(),
            domain: ".iqiyi.com".to_string(),
            path: "/".to_string(),
            secure: true,
            http_only: true,
            session_only: expires_at.is_none(),
            expires_at,
        }
    }

    #[test]
    fn bootstrap_hook_is_bounded_and_cookie_blind() {
        let script = bootstrap_hook_script();
        assert!(script.contains("MAX_XHR_CHARS"));
        assert!(script.contains("MAX_FETCH_BYTES"));
        assert!(script.contains("MAX_JSON_NODES"));
        assert!(script.contains("getReader"));
        assert!(script.contains("MEDIA_KEY_RE"));
        assert!(!script.contains("document.cookie"));
        assert!(!script.contains("responseURL"));
    }

    #[test]
    fn bootstrap_snapshot_stays_constant_cost() {
        let script = bootstrap_snapshot_script();
        assert!(!script.contains("querySelectorAll"));
        assert!(!script.contains("getEntriesByType"));
        assert!(script.contains("__synctvIqiyiBootstrap"));
    }

    #[test]
    fn cookie_preparation_drops_expired_and_duplicate_values() {
        let now = unix_timestamp_now();
        let cookies = vec![
            cookie("P00001", "old", None),
            cookie("expired", "value", Some(now.saturating_sub(1))),
            cookie("P00001", "latest", None),
            cookie("QC005", "value", None),
        ];
        let (prepared, stats) = prepare_chromium_cookies(&cookies);
        assert_eq!(stats.input_count, 4);
        assert_eq!(stats.expired_dropped, 1);
        assert_eq!(stats.duplicate_dropped, 1);
        assert_eq!(prepared.len(), 2);
    }

    #[test]
    fn provider_url_validation_stays_scoped() {
        assert!(validate_provider_url("https://www.iqiyi.com/v_demo.html", &["iqiyi.com"]).is_ok());
        assert!(validate_provider_url("https://example.com/v_demo.html", &["iqiyi.com"]).is_err());
    }
}
