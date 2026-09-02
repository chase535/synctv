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

const BROWSER_QUEUE_TIMEOUT: Duration = Duration::from_secs(10);
const BROWSER_RENDER_TIMEOUT: Duration = Duration::from_secs(30);
const CDP_STARTUP_TIMEOUT: Duration = Duration::from_secs(12);
const CDP_COMMAND_TIMEOUT: Duration = Duration::from_secs(6);
const CDP_PROBE_TIMEOUT: Duration = Duration::from_secs(8);
const CDP_SLOW_COMMAND_THRESHOLD: Duration = Duration::from_millis(500);
const BROWSER_POST_NAVIGATION_SETTLE_DELAY: Duration = Duration::from_millis(1200);
const BROWSER_PROBE_INTERVAL: Duration = Duration::from_millis(900);
const BROWSER_RENDER_COMPLETION_RESERVE: Duration = Duration::from_millis(1500);
const BLOB_VIDEO_GRACE_DELAY: Duration = Duration::from_millis(2200);
const BROWSER_PROFILE_CLEANUP_DELAY: Duration = Duration::from_millis(400);
const MAX_CONCURRENT_BROWSER_RENDERS: usize = 1;
const MAX_BROWSER_PROBE_ATTEMPTS: usize = 3;
const MAX_CDP_MESSAGE_BYTES: usize = 4 * 1024 * 1024;
const MAX_LOGGED_MEDIA_HOSTS: usize = 5;
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
struct BrowserProbePayload {
    current_url: String,
    title: String,
    ready_state: String,
    resource_count: usize,
    xhr_fetch_count: usize,
    segment_like_count: usize,
    video_element_count: usize,
    source_element_count: usize,
    video_with_current_src_count: usize,
    video_with_src_attr_count: usize,
    video_paused_count: usize,
    video_error_count: usize,
    video_ready_state_max: usize,
    video_network_state_max: usize,
    media_urls: Vec<String>,
    resource_host_kinds: Vec<String>,
    has_blob_video: bool,
    has_license_resource: bool,
    play_attempted: bool,
    play_pending: bool,
    play_fulfilled: bool,
    play_rejected: bool,
    play_error_name: String,
    visibility_state: String,
    document_has_focus: bool,
    viewport_width: usize,
    viewport_height: usize,
    mse_h264_supported: bool,
    mse_aac_supported: bool,
    video_h264_can_play: bool,
    navigator_webdriver: bool,
    eme_available: bool,
}

struct BrowserProbeOutcome {
    payload: BrowserProbePayload,
    reason: &'static str,
    attempts: usize,
    elapsed: Duration,
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
        let kill_started = Instant::now();
        let kill_requested = self.child.start_kill().is_ok();
        let wait_finished = response_first_timeout(Duration::from_secs(1), self.child.wait())
            .await
            .is_ok();
        let kill_wait_ms = kill_started.elapsed().as_millis();

        let mut cleanup_success = true;
        let mut cleanup_deferred = false;
        if let Some(profile_dir) = self.profile_dir.take() {
            tokio::time::sleep(Duration::from_millis(100)).await;
            match tokio::fs::remove_dir_all(&profile_dir).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    cleanup_success = false;
                    cleanup_deferred = true;
                    tracing::info!(
                        target: "synctv_media_providers::browser_session",
                        stage = "browser_profile_cleanup_deferred",
                        page_host = %self.page_host,
                        error_kind = ?error.kind(),
                        error = %error,
                        "Chromium profile cleanup will be retried asynchronously"
                    );
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
            kill_wait_ms,
            cleanup_success,
            cleanup_deferred,
            elapsed_ms = started.elapsed().as_millis(),
            "Authenticated browser page render diagnostics"
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
        tracing::warn!(
            target: "synctv_media_providers::browser_session",
            stage = "browser_abort_cleanup_not_scheduled",
            page_host = %page_host,
            "No Tokio runtime was available for deferred Chromium profile cleanup"
        );
        return;
    };

    let _cleanup_task = runtime.spawn(async move {
        let delays = [
            BROWSER_PROFILE_CLEANUP_DELAY,
            Duration::from_millis(800),
            Duration::from_millis(1600),
        ];
        for (index, delay) in delays.into_iter().enumerate() {
            tokio::time::sleep(delay).await;
            match tokio::fs::remove_dir_all(&profile_dir).await {
                Ok(()) => {
                    tracing::info!(
                        target: "synctv_media_providers::browser_session",
                        stage = "browser_deferred_cleanup_complete",
                        page_host = %page_host,
                        attempt = index + 1,
                        "Deferred Chromium profile cleanup completed"
                    );
                    return;
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
                Err(_) if index + 1 < delays.len() => {}
                Err(error) => {
                    tracing::warn!(
                        target: "synctv_media_providers::browser_session",
                        stage = "browser_deferred_cleanup_failed",
                        page_host = %page_host,
                        attempts = delays.len(),
                        error_kind = ?error.kind(),
                        error = %error,
                        "Deferred Chromium profile cleanup exhausted retries"
                    );
                }
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
            let mut chunk = [0_u8; 8192];
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
    let page_host = page_host(raw_url);
    let request_started = Instant::now();
    let single_process = chromium_single_process_enabled();

    tracing::info!(
        target: "synctv_media_providers::browser_session",
        stage = "render_requested",
        page_host = %page_host,
        cookie_count = cookies.len(),
        max_concurrent_renders = MAX_CONCURRENT_BROWSER_RENDERS,
        queue_timeout_ms = BROWSER_QUEUE_TIMEOUT.as_millis(),
        render_timeout_ms = BROWSER_RENDER_TIMEOUT.as_millis(),
        single_process,
        transport = "pipe",
        "Authenticated browser page render diagnostics"
    );
    log_container_resources("render_requested", &page_host);

    let queue_started = Instant::now();
    let permit = response_first_timeout(BROWSER_QUEUE_TIMEOUT, BROWSER_RENDER_SEMAPHORE.acquire())
        .await
        .map_err(|_| {
            ProviderClientError::Network(format!(
                "browser discovery slot timed out after {}s",
                BROWSER_QUEUE_TIMEOUT.as_secs()
            ))
        })?
        .map_err(|error| {
            ProviderClientError::Network(format!("browser discovery semaphore closed: {error}"))
        })?;
    tracing::info!(
        target: "synctv_media_providers::browser_session",
        stage = "browser_slot_acquired",
        page_host = %page_host,
        wait_ms = queue_started.elapsed().as_millis(),
        "Authenticated browser page render diagnostics"
    );

    let render_started = Instant::now();
    let render_deadline = render_started + BROWSER_RENDER_TIMEOUT;
    let result = match response_first_timeout(
        BROWSER_RENDER_TIMEOUT,
        render_web_page_playback_inner(
            raw_url,
            allowed_domains,
            cookies,
            single_process,
            render_deadline,
        ),
    )
    .await
    {
        Ok(result) => result,
        Err(()) => {
            let elapsed = render_started.elapsed();
            let overshoot = elapsed.saturating_sub(BROWSER_RENDER_TIMEOUT);
            tracing::warn!(
                target: "synctv_media_providers::browser_session",
                stage = "render_timeout",
                page_host = %page_host,
                render_elapsed_ms = elapsed.as_millis(),
                timeout_ms = BROWSER_RENDER_TIMEOUT.as_millis(),
                timeout_overshoot_ms = overshoot.as_millis(),
                total_elapsed_ms = request_started.elapsed().as_millis(),
                single_process,
                transport = "pipe",
                "Authenticated browser page render diagnostics"
            );
            Err(ProviderClientError::Network(format!(
                "browser page rendering timed out after {}s",
                BROWSER_RENDER_TIMEOUT.as_secs()
            )))
        }
    };

    drop(permit);
    log_container_resources("render_finished", &page_host);
    tracing::info!(
        target: "synctv_media_providers::browser_session",
        stage = "render_finished",
        page_host = %page_host,
        success = result.is_ok(),
        render_elapsed_ms = render_started.elapsed().as_millis(),
        total_elapsed_ms = request_started.elapsed().as_millis(),
        single_process,
        transport = "pipe",
        "Authenticated browser page render diagnostics"
    );
    result
}

async fn render_web_page_playback_inner(
    raw_url: &str,
    allowed_domains: &'static [&'static str],
    cookies: &[SessionCookie],
    single_process: bool,
    render_deadline: Instant,
) -> Result<BrowserPageObservation, ProviderClientError> {
    let page_url = validate_provider_url(raw_url, allowed_domains)?;
    let page_host = page_url.host_str().unwrap_or("").to_string();
    let profile_dir =
        std::env::temp_dir().join(format!("synctv-chromium-{}", uuid::Uuid::new_v4().simple()));
    tokio::fs::create_dir_all(&profile_dir)
        .await
        .map_err(|error| ProviderClientError::Network(format!("create browser profile: {error}")))?;

    tracing::info!(
        target: "synctv_media_providers::browser_session",
        stage = "profile_ready",
        page_host = %page_host,
        profile_parent = %profile_dir.parent().unwrap_or(Path::new("/tmp")).display(),
        single_process,
        transport = "pipe",
        "Authenticated browser page render diagnostics"
    );

    let (mut browser, mut pipe) = match start_chromium_pipe(&profile_dir, &page_host, single_process).await {
        Ok(value) => value,
        Err(error) => {
            schedule_profile_cleanup(profile_dir, page_host.clone());
            return Err(error);
        }
    };

    let result = async {
        let mut command_id = 0_u64;
        let startup_started = Instant::now();
        let targets = cdp_call_with_timeout(
            &mut pipe,
            &mut command_id,
            None,
            "Target.getTargets",
            json!({}),
            CDP_STARTUP_TIMEOUT,
        )
        .await?;
        tracing::info!(
            target: "synctv_media_providers::browser_session",
            stage = "chromium_ready",
            page_host = %page_host,
            startup_elapsed_ms = startup_started.elapsed().as_millis(),
            single_process,
            transport = "pipe",
            "Chromium CDP pipe accepted the first browser command"
        );
        log_container_resources("chromium_ready", &page_host);

        let target_id = match first_page_target_id(&targets) {
            Some(target_id) => target_id,
            None => {
                let created = cdp_call(
                    &mut pipe,
                    &mut command_id,
                    None,
                    "Target.createTarget",
                    json!({ "url": "about:blank" }),
                )
                .await?;
                created
                    .get("targetId")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .ok_or_else(|| {
                        ProviderClientError::Parse(
                            "Chromium Target.createTarget returned no targetId".to_string(),
                        )
                    })?
            }
        };

        if !cookies.is_empty() {
            let cookie_started = Instant::now();
            let (cookie_params, cookie_stats) = prepare_chromium_cookies(cookies);
            if !cookie_params.is_empty() {
                cdp_call(
                    &mut pipe,
                    &mut command_id,
                    None,
                    "Storage.setCookies",
                    json!({ "cookies": cookie_params }),
                )
                .await?;
            }
            tracing::info!(
                target: "synctv_media_providers::browser_session",
                stage = "cookies_installed",
                page_host = %page_host,
                method = "Storage.setCookies",
                cookie_input_count = cookie_stats.input_count,
                cookie_count = cookie_stats.effective_count,
                cookie_expired_dropped = cookie_stats.expired_dropped,
                cookie_duplicate_dropped = cookie_stats.duplicate_dropped,
                elapsed_ms = cookie_started.elapsed().as_millis(),
                "Authenticated browser page render diagnostics"
            );
        }

        let attach_started = Instant::now();
        let attached = cdp_call(
            &mut pipe,
            &mut command_id,
            None,
            "Target.attachToTarget",
            json!({ "targetId": target_id, "flatten": true }),
        )
        .await?;
        let session_id = attached
            .get("sessionId")
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| {
                ProviderClientError::Parse(
                    "Chromium Target.attachToTarget returned no sessionId".to_string(),
                )
            })?;
        tracing::info!(
            target: "synctv_media_providers::browser_session",
            stage = "page_session_attached",
            page_host = %page_host,
            elapsed_ms = attach_started.elapsed().as_millis(),
            transport = "pipe",
            "Authenticated browser page render diagnostics"
        );

        // The headless page reports visibilityState=visible but hasFocus()=false.
        // Provider player bootstraps can treat that as an inactive tab even when
        // Chromium background throttling is disabled. Dispatch both focus hints
        // without waiting for acknowledgements; they share the same CDP pipe and
        // therefore arrive before Page.navigate, while their late responses are
        // harmlessly skipped by the first Runtime.evaluate command id.
        let focus_emulation_command_id = cdp_send_command(
            &mut pipe,
            &mut command_id,
            Some(&session_id),
            "Emulation.setFocusEmulationEnabled",
            json!({ "enabled": true }),
        )
        .await?;
        let bring_to_front_command_id = cdp_send_command(
            &mut pipe,
            &mut command_id,
            Some(&session_id),
            "Page.bringToFront",
            json!({}),
        )
        .await?;

        tracing::info!(
            target: "synctv_media_providers::browser_session",
            stage = "cdp_configured",
            page_host = %page_host,
            event_domains_enabled = false,
            images_disabled = true,
            browser_level_cookie_install = true,
            interactive_probe = true,
            navigation_ack_waited = false,
            response_first_deadlines = true,
            state_aware_probe = true,
            final_observation_skipped = true,
            focus_emulation = true,
            bring_to_front = true,
            focus_emulation_command_id,
            bring_to_front_command_id,
            max_probe_attempts = MAX_BROWSER_PROBE_ATTEMPTS,
            render_completion_reserve_ms = BROWSER_RENDER_COMPLETION_RESERVE.as_millis(),
            transport = "pipe",
            flattened_session = true,
            "Authenticated browser page render diagnostics"
        );

        let navigate_started = Instant::now();
        let navigation_command_id = cdp_send_command(
            &mut pipe,
            &mut command_id,
            Some(&session_id),
            "Page.navigate",
            json!({ "url": page_url.as_str() }),
        )
        .await?;
        tracing::info!(
            target: "synctv_media_providers::browser_session",
            stage = "navigation_started",
            page_host = %page_host,
            navigation_command_id,
            ack_waited = false,
            settle_delay_ms = BROWSER_POST_NAVIGATION_SETTLE_DELAY.as_millis(),
            elapsed_ms = navigate_started.elapsed().as_millis(),
            render_elapsed_ms = startup_started.elapsed().as_millis(),
            "Authenticated browser page render diagnostics"
        );

        tokio::time::sleep(BROWSER_POST_NAVIGATION_SETTLE_DELAY).await;
        let probe = wait_for_browser_signal(
            &mut pipe,
            &mut command_id,
            &session_id,
            &page_host,
            render_deadline,
        )
        .await?;

        let final_url = match validate_provider_url(&probe.payload.current_url, allowed_domains) {
            Ok(url) => url,
            Err(_) if probe.payload.current_url.is_empty() || probe.payload.current_url == "about:blank" => {
                page_url.clone()
            }
            Err(error) => return Err(error),
        };
        let media_urls = normalize_media_urls(&final_url, probe.payload.media_urls.clone());
        let has_m3u8 = media_urls.iter().any(|url| url.to_ascii_lowercase().contains(".m3u8"));
        let has_mpd = media_urls.iter().any(|url| url.to_ascii_lowercase().contains(".mpd"));
        let has_mp4 = media_urls.iter().any(|url| url.to_ascii_lowercase().contains(".mp4"));
        let diagnostics = BrowserPageDiagnostics {
            ready_state: probe.payload.ready_state.clone(),
            html_length: 0,
            resource_count: probe.payload.resource_count,
            media_resource_count: probe.payload.media_urls.len(),
            video_element_count: probe.payload.video_element_count,
            source_element_count: probe.payload.source_element_count,
            has_m3u8,
            has_mpd,
            has_mp4,
            has_blob_video: probe.payload.has_blob_video,
            has_video_id: false,
            has_tv_id: false,
            has_drm_marker: false,
            has_license_resource: probe.payload.has_license_resource,
        };
        let drm_detected = probe.payload.has_license_resource;

        tracing::info!(
            target: "synctv_media_providers::browser_session",
            stage = "observation_complete",
            page_host = %page_host,
            observation_source = "probe_snapshot",
            final_observation_skipped = true,
            probe_reason = probe.reason,
            probe_attempts = probe.attempts,
            probe_elapsed_ms = probe.elapsed.as_millis(),
            ready_state = %diagnostics.ready_state,
            resource_count = diagnostics.resource_count,
            media_resource_count = diagnostics.media_resource_count,
            media_count = media_urls.len(),
            media_hosts = %summarize_media_hosts(&media_urls),
            media_kinds = %summarize_media_kinds(&media_urls),
            has_blob_video = diagnostics.has_blob_video,
            drm_detected,
            play_attempted = probe.payload.play_attempted,
            play_pending = probe.payload.play_pending,
            play_fulfilled = probe.payload.play_fulfilled,
            play_rejected = probe.payload.play_rejected,
            play_error_name = %probe.payload.play_error_name,
            document_has_focus = probe.payload.document_has_focus,
            render_budget_remaining_ms = render_deadline
                .saturating_duration_since(Instant::now())
                .as_millis(),
            single_process,
            transport = "pipe",
            "Authenticated browser page render diagnostics"
        );

        Ok(BrowserPageObservation {
            discovery: WebPagePlaybackDiscovery {
                page_url: final_url.to_string(),
                title: (!probe.payload.title.trim().is_empty())
                    .then(|| probe.payload.title.trim().to_string()),
                media_urls,
                drm_detected,
            },
            diagnostics,
        })
    }
    .await;

    drop(pipe);
    browser.shutdown().await;
    result
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
        .arg("--disable-client-side-phishing-detection")
        .arg("--disable-breakpad")
        .arg("--disable-crash-reporter")
        .arg("--disable-application-cache")
        .arg("--metrics-recording-only")
        .arg("--mute-audio")
        .arg("--hide-scrollbars")
        .arg("--blink-settings=imagesEnabled=false")
        .arg("--disk-cache-size=1")
        .arg("--media-cache-size=1")
        .arg("--no-first-run")
        .arg("--no-default-browser-check")
        .arg("--no-service-autorun")
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

    let spawn_started = Instant::now();
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
        stage = "chromium_spawned",
        page_host = %page_host,
        chromium_bin = %chromium_bin,
        browser_pid,
        spawn_elapsed_ms = spawn_started.elapsed().as_millis(),
        single_process,
        transport = "pipe",
        "Authenticated browser page render diagnostics"
    );
    log_container_resources("chromium_spawned", page_host);

    Ok((
        ChromiumProcess {
            child,
            profile_dir: Some(profile_dir.to_path_buf()),
            page_host: page_host.to_string(),
        },
        CdpPipe {
            input,
            output,
            buffered: Vec::with_capacity(8192),
        },
    ))
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
    let total_kib = host_meminfo().total_kib;
    total_kib != 0 && total_kib <= LOW_MEMORY_SINGLE_PROCESS_THRESHOLD_KIB
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
) -> Result<Value, ProviderClientError> {
    cdp_call_with_timeout(
        pipe,
        command_id,
        session_id,
        method,
        params,
        CDP_COMMAND_TIMEOUT,
    )
    .await
}

async fn cdp_call_with_timeout(
    pipe: &mut CdpPipe,
    command_id: &mut u64,
    session_id: Option<&str>,
    method: &str,
    params: Value,
    timeout: Duration,
) -> Result<Value, ProviderClientError> {
    let current_id = cdp_send_command(pipe, command_id, session_id, method, params).await?;
    let started = Instant::now();
    let mut skipped_response_count = 0_usize;
    let result = response_first_timeout(timeout, async {
        loop {
            let response = pipe.receive().await?;
            if response.get("id").and_then(Value::as_u64) != Some(current_id) {
                skipped_response_count = skipped_response_count.saturating_add(1);
                continue;
            }
            if let Some(expected_session) = session_id {
                if response.get("sessionId").and_then(Value::as_str) != Some(expected_session) {
                    skipped_response_count = skipped_response_count.saturating_add(1);
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

    match result {
        Ok(inner) => {
            let elapsed = started.elapsed();
            let deadline_overshoot = elapsed.saturating_sub(timeout);
            if elapsed > timeout {
                tracing::info!(
                    target: "synctv_media_providers::browser_session",
                    stage = "cdp_command_late_success",
                    method,
                    success = inner.is_ok(),
                    elapsed_ms = elapsed.as_millis(),
                    timeout_ms = timeout.as_millis(),
                    deadline_overshoot_ms = deadline_overshoot.as_millis(),
                    skipped_response_count,
                    transport = "pipe",
                    "Chromium CDP command completed after its nominal deadline because the response was ready when the executor resumed"
                );
            } else if elapsed >= CDP_SLOW_COMMAND_THRESHOLD {
                tracing::info!(
                    target: "synctv_media_providers::browser_session",
                    stage = "cdp_command_slow",
                    method,
                    success = inner.is_ok(),
                    elapsed_ms = elapsed.as_millis(),
                    timeout_ms = timeout.as_millis(),
                    skipped_response_count,
                    transport = "pipe",
                    "Chromium CDP command diagnostics"
                );
            }
            inner
        }
        Err(()) => {
            let elapsed = started.elapsed();
            tracing::warn!(
                target: "synctv_media_providers::browser_session",
                stage = "cdp_command_timeout",
                method,
                elapsed_ms = elapsed.as_millis(),
                timeout_ms = timeout.as_millis(),
                timeout_overshoot_ms = elapsed.saturating_sub(timeout).as_millis(),
                skipped_response_count,
                transport = "pipe",
                "Chromium CDP command diagnostics"
            );
            Err(ProviderClientError::Network(format!(
                "Chromium CDP command {method} timed out after {}ms",
                timeout.as_millis()
            )))
        }
    }
}

async fn wait_for_browser_signal(
    pipe: &mut CdpPipe,
    command_id: &mut u64,
    session_id: &str,
    expected_page_host: &str,
    render_deadline: Instant,
) -> Result<BrowserProbeOutcome, ProviderClientError> {
    let started = Instant::now();
    let mut attempts = 0_usize;
    let mut last_successful: Option<BrowserProbePayload> = None;

    loop {
        let remaining = render_deadline.saturating_duration_since(Instant::now());
        if remaining <= BROWSER_RENDER_COMPLETION_RESERVE {
            if let Some(payload) = last_successful.take() {
                tracing::info!(
                    target: "synctv_media_providers::browser_session",
                    stage = "page_probe_budget_exhausted",
                    page_host = %expected_page_host,
                    successful_attempts = attempts,
                    elapsed_ms = started.elapsed().as_millis(),
                    render_budget_remaining_ms = remaining.as_millis(),
                    reserve_ms = BROWSER_RENDER_COMPLETION_RESERVE.as_millis(),
                    ready_state = %payload.ready_state,
                    resource_count = payload.resource_count,
                    video_element_count = payload.video_element_count,
                    play_pending = payload.play_pending,
                    "Returning the last successful browser probe before the outer render deadline"
                );
                return Ok(BrowserProbeOutcome {
                    payload,
                    reason: "render_budget_exhausted",
                    attempts,
                    elapsed: started.elapsed(),
                });
            }
            return Err(ProviderClientError::Network(
                "browser discovery render budget exhausted before the first page probe completed"
                    .to_string(),
            ));
        }

        attempts = attempts.saturating_add(1);
        let probe_timeout = std::cmp::min(
            CDP_PROBE_TIMEOUT,
            remaining.saturating_sub(BROWSER_RENDER_COMPLETION_RESERVE),
        );
        let evaluation = cdp_call_with_timeout(
            pipe,
            command_id,
            Some(session_id),
            "Runtime.evaluate",
            json!({
                "expression": browser_probe_script(),
                "returnByValue": true,
                "awaitPromise": false,
                "userGesture": true,
            }),
            probe_timeout,
        )
        .await;

        let evaluation = match evaluation {
            Ok(value) => value,
            Err(error) => {
                if let Some(payload) = last_successful.take() {
                    tracing::warn!(
                        target: "synctv_media_providers::browser_session",
                        stage = "page_probe_retry_failed",
                        page_host = %expected_page_host,
                        attempt = attempts,
                        elapsed_ms = started.elapsed().as_millis(),
                        probe_timeout_ms = probe_timeout.as_millis(),
                        render_budget_remaining_ms = render_deadline
                            .saturating_duration_since(Instant::now())
                            .as_millis(),
                        previous_ready_state = %payload.ready_state,
                        previous_resource_count = payload.resource_count,
                        previous_video_element_count = payload.video_element_count,
                        previous_play_pending = payload.play_pending,
                        error = %error,
                        "Returning the last successful browser probe snapshot instead of failing the whole render"
                    );
                    return Ok(BrowserProbeOutcome {
                        payload,
                        reason: "probe_retry_failed",
                        attempts: attempts.saturating_sub(1),
                        elapsed: started.elapsed(),
                    });
                }
                return Err(error);
            }
        };

        let serialized = evaluation
            .pointer("/result/value")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ProviderClientError::Parse(
                    "browser probe did not return a serialized value".to_string(),
                )
            })?;
        let payload: BrowserProbePayload = serde_json::from_str(serialized)
            .map_err(|error| ProviderClientError::Parse(error.to_string()))?;
        let current_host = page_host(&payload.current_url);

        tracing::info!(
            target: "synctv_media_providers::browser_session",
            stage = "page_probe",
            page_host = %expected_page_host,
            current_host = %current_host,
            attempt = attempts,
            max_attempts = MAX_BROWSER_PROBE_ATTEMPTS,
            probe_timeout_ms = probe_timeout.as_millis(),
            render_budget_remaining_ms = render_deadline
                .saturating_duration_since(Instant::now())
                .as_millis(),
            elapsed_ms = started.elapsed().as_millis(),
            ready_state = %payload.ready_state,
            resource_count = payload.resource_count,
            xhr_fetch_count = payload.xhr_fetch_count,
            segment_like_count = payload.segment_like_count,
            video_element_count = payload.video_element_count,
            source_element_count = payload.source_element_count,
            video_with_current_src_count = payload.video_with_current_src_count,
            video_with_src_attr_count = payload.video_with_src_attr_count,
            video_paused_count = payload.video_paused_count,
            video_error_count = payload.video_error_count,
            video_ready_state_max = payload.video_ready_state_max,
            video_network_state_max = payload.video_network_state_max,
            media_count = payload.media_urls.len(),
            media_hosts = %summarize_media_hosts(&payload.media_urls),
            media_kinds = %summarize_media_kinds(&payload.media_urls),
            resource_host_kinds = %payload.resource_host_kinds.join(","),
            has_blob_video = payload.has_blob_video,
            has_license_resource = payload.has_license_resource,
            play_attempted = payload.play_attempted,
            play_pending = payload.play_pending,
            play_fulfilled = payload.play_fulfilled,
            play_rejected = payload.play_rejected,
            play_error_name = %payload.play_error_name,
            visibility_state = %payload.visibility_state,
            document_has_focus = payload.document_has_focus,
            viewport_width = payload.viewport_width,
            viewport_height = payload.viewport_height,
            mse_h264_supported = payload.mse_h264_supported,
            mse_aac_supported = payload.mse_aac_supported,
            video_h264_can_play = payload.video_h264_can_play,
            navigator_webdriver = payload.navigator_webdriver,
            eme_available = payload.eme_available,
            cgroup_memory_current_bytes = cgroup_memory_current_bytes().unwrap_or_default(),
            self_rss_kib = process_rss_kib().unwrap_or_default(),
            transport = "pipe",
            "Authenticated browser page render diagnostics"
        );

        let elapsed = started.elapsed();
        let navigation_committed = !current_host.is_empty();
        let reason = if !payload.media_urls.is_empty() {
            Some("media_url")
        } else if payload.has_license_resource {
            Some("license_resource")
        } else if payload.has_blob_video && elapsed >= BLOB_VIDEO_GRACE_DELAY {
            Some("blob_video")
        } else if payload.ready_state == "complete" {
            Some("document_complete")
        } else if attempts >= MAX_BROWSER_PROBE_ATTEMPTS && navigation_committed {
            Some("probe_attempt_limit")
        } else if attempts >= MAX_BROWSER_PROBE_ATTEMPTS {
            Some("navigation_not_committed")
        } else {
            None
        };

        if let Some(reason) = reason {
            return Ok(BrowserProbeOutcome {
                payload,
                reason,
                attempts,
                elapsed,
            });
        }

        last_successful = Some(payload);
        let remaining = render_deadline.saturating_duration_since(Instant::now());
        let minimum_next_probe_budget =
            BROWSER_RENDER_COMPLETION_RESERVE + Duration::from_millis(250);
        if remaining > minimum_next_probe_budget {
            let sleep_budget = remaining.saturating_sub(minimum_next_probe_budget);
            tokio::time::sleep(std::cmp::min(BROWSER_PROBE_INTERVAL, sleep_budget)).await;
        }
    }
}

fn prepare_chromium_cookies(cookies: &[SessionCookie]) -> (Vec<Value>, CookiePreparationStats) {
    let now = unix_timestamp_now();
    let mut seen = HashSet::new();
    let mut prepared_reversed = Vec::with_capacity(cookies.len());
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
        let key = (cookie.name.clone(), domain, path);
        if !seen.insert(key) {
            stats.duplicate_dropped += 1;
            continue;
        }
        prepared_reversed.push(chromium_cookie_param(cookie));
    }
    prepared_reversed.reverse();
    stats.effective_count = prepared_reversed.len();
    (prepared_reversed, stats)
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

fn unix_timestamp_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_secs()).ok())
        .unwrap_or(i64::MAX)
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

fn summarize_media_hosts(media_urls: &[String]) -> String {
    let mut seen = HashSet::new();
    let mut hosts = Vec::new();
    for raw_url in media_urls {
        let Some(host) = Url::parse(raw_url)
            .ok()
            .and_then(|url| url.host_str().map(str::to_string))
        else {
            continue;
        };
        if seen.insert(host.clone()) {
            hosts.push(host);
        }
        if hosts.len() >= MAX_LOGGED_MEDIA_HOSTS {
            break;
        }
    }
    if hosts.is_empty() {
        "none".to_string()
    } else {
        hosts.join(",")
    }
}

fn summarize_media_kinds(media_urls: &[String]) -> String {
    let mut m3u8 = 0_usize;
    let mut mpd = 0_usize;
    let mut mp4 = 0_usize;
    let mut other = 0_usize;
    for raw_url in media_urls {
        let lower = raw_url.to_ascii_lowercase();
        if lower.contains(".m3u8") {
            m3u8 += 1;
        } else if lower.contains(".mpd") {
            mpd += 1;
        } else if lower.contains(".mp4") {
            mp4 += 1;
        } else {
            other += 1;
        }
    }
    format!("m3u8={m3u8},mpd={mpd},mp4={mp4},other={other}")
}

fn page_host(raw_url: &str) -> String {
    Url::parse(raw_url)
        .ok()
        .and_then(|url| url.host_str().map(str::to_string))
        .unwrap_or_default()
}

fn read_trimmed_file(path: &str) -> Option<String> {
    let text = std::fs::read_to_string(path).ok()?;
    let trimmed = text.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

fn cgroup_memory_current_bytes() -> Option<u64> {
    read_trimmed_file("/sys/fs/cgroup/memory.current")
        .and_then(|value| value.parse::<u64>().ok())
        .or_else(|| {
            read_trimmed_file("/sys/fs/cgroup/memory/memory.usage_in_bytes")
                .and_then(|value| value.parse::<u64>().ok())
        })
}

fn cgroup_memory_peak_bytes() -> Option<u64> {
    read_trimmed_file("/sys/fs/cgroup/memory.peak").and_then(|value| value.parse::<u64>().ok())
}

fn cgroup_memory_max() -> String {
    read_trimmed_file("/sys/fs/cgroup/memory.max")
        .or_else(|| read_trimmed_file("/sys/fs/cgroup/memory/memory.limit_in_bytes"))
        .unwrap_or_else(|| "unknown".to_string())
}

fn cgroup_swap_current_bytes() -> Option<u64> {
    read_trimmed_file("/sys/fs/cgroup/memory.swap.current")
        .and_then(|value| value.parse::<u64>().ok())
}

fn cgroup_memory_events() -> String {
    read_trimmed_file("/sys/fs/cgroup/memory.events")
        .map(|events| {
            events
                .lines()
                .map(|line| line.split_whitespace().collect::<Vec<_>>().join("="))
                .collect::<Vec<_>>()
                .join(",")
        })
        .unwrap_or_else(|| "unavailable".to_string())
}

fn cgroup_cpu_stat() -> String {
    read_trimmed_file("/sys/fs/cgroup/cpu.stat")
        .map(|stats| {
            stats
                .lines()
                .map(|line| line.split_whitespace().collect::<Vec<_>>().join("="))
                .collect::<Vec<_>>()
                .join(",")
        })
        .unwrap_or_else(|| "unavailable".to_string())
}

fn cgroup_pids_current() -> Option<u64> {
    read_trimmed_file("/sys/fs/cgroup/pids.current")
        .and_then(|value| value.parse::<u64>().ok())
}

fn process_rss_kib() -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| {
            let value = line.strip_prefix("VmRSS:")?.trim();
            value
                .split_whitespace()
                .next()
                .and_then(|value| value.parse::<u64>().ok())
        })
}

#[derive(Default)]
struct HostMemInfo {
    total_kib: u64,
    available_kib: u64,
    swap_total_kib: u64,
    swap_free_kib: u64,
}

fn host_meminfo() -> HostMemInfo {
    let mut info = HostMemInfo::default();
    let Ok(text) = std::fs::read_to_string("/proc/meminfo") else {
        return info;
    };
    for line in text.lines() {
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        let value = value
            .split_whitespace()
            .next()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or_default();
        match key {
            "MemTotal" => info.total_kib = value,
            "MemAvailable" => info.available_kib = value,
            "SwapTotal" => info.swap_total_kib = value,
            "SwapFree" => info.swap_free_kib = value,
            _ => {}
        }
    }
    info
}

fn log_container_resources(stage: &'static str, page_host: &str) {
    let host = host_meminfo();
    tracing::info!(
        target: "synctv_media_providers::browser_session",
        stage,
        page_host = %page_host,
        cgroup_memory_current_bytes = cgroup_memory_current_bytes().unwrap_or_default(),
        cgroup_memory_peak_bytes = cgroup_memory_peak_bytes().unwrap_or_default(),
        cgroup_memory_max = %cgroup_memory_max(),
        cgroup_swap_current_bytes = cgroup_swap_current_bytes().unwrap_or_default(),
        cgroup_memory_events = %cgroup_memory_events(),
        cgroup_cpu_stat = %cgroup_cpu_stat(),
        cgroup_pids_current = cgroup_pids_current().unwrap_or_default(),
        self_rss_kib = process_rss_kib().unwrap_or_default(),
        host_mem_total_kib = host.total_kib,
        host_mem_available_kib = host.available_kib,
        host_swap_total_kib = host.swap_total_kib,
        host_swap_free_kib = host.swap_free_kib,
        "Container resource diagnostics"
    );
}

fn browser_probe_script() -> &'static str {
    r#"(() => {
        const entries = performance.getEntriesByType('resource');
        const mediaPattern = /\.(?:m3u8|mpd|mp4)(?:[?#]|$)/i;
        const segmentPattern = /(?:\.m4s|\.ts)(?:[?#]|$)/i;
        const mediaInitiators = new Set(['video', 'audio', 'media']);
        const resources = entries.map((entry) => ({
            name: entry.name || '',
            initiator: entry.initiatorType || 'other'
        }));
        const mediaUrls = resources
            .filter((entry) => mediaPattern.test(entry.name) || mediaInitiators.has(entry.initiator))
            .map((entry) => entry.name);
        const videos = Array.from(document.querySelectorAll('video'));
        const sources = Array.from(document.querySelectorAll('source'));
        for (const element of [...videos, ...sources]) {
            const value = element.currentSrc || element.src || element.getAttribute('src') || '';
            if (value && !value.startsWith('blob:') && !value.startsWith('data:')) mediaUrls.push(value);
        }

        const state = window.__synctvPlaybackProbeState || (window.__synctvPlaybackProbeState = {
            playAttempted: false,
            playPending: false,
            playFulfilled: false,
            playRejected: false,
            playErrorName: ''
        });
        if (videos.length > 0 && !state.playAttempted) {
            const candidate = videos.find((video) => video.currentSrc || video.src || video.querySelector('source')) || videos[0];
            state.playAttempted = true;
            state.playPending = true;
            candidate.muted = true;
            candidate.autoplay = true;
            try {
                const result = candidate.play();
                if (result && typeof result.then === 'function') {
                    result.then(() => {
                        state.playPending = false;
                        state.playFulfilled = true;
                    }).catch((error) => {
                        state.playPending = false;
                        state.playRejected = true;
                        state.playErrorName = error && error.name ? String(error.name) : 'Error';
                    });
                } else {
                    state.playPending = false;
                    state.playFulfilled = true;
                }
            } catch (error) {
                state.playPending = false;
                state.playRejected = true;
                state.playErrorName = error && error.name ? String(error.name) : 'Error';
            }
        }

        const hostCounts = new Map();
        for (const entry of resources) {
            try {
                const host = new URL(entry.name, location.href).hostname;
                if (!host) continue;
                const key = `${host}|${entry.initiator}`;
                hostCounts.set(key, (hostCounts.get(key) || 0) + 1);
            } catch (_) {}
        }
        const resourceHostKinds = Array.from(hostCounts.entries())
            .sort((left, right) => right[1] - left[1])
            .slice(0, 8)
            .map(([key, count]) => `${key}=${count}`);

        const h264 = 'video/mp4; codecs="avc1.42E01E"';
        const aac = 'audio/mp4; codecs="mp4a.40.2"';
        const codecVideo = document.createElement('video');
        const hasMediaSource = typeof MediaSource !== 'undefined';
        return JSON.stringify({
            currentUrl: location.href || '',
            title: document.title || '',
            readyState: document.readyState || '',
            resourceCount: resources.length,
            xhrFetchCount: resources.filter((entry) => entry.initiator === 'fetch' || entry.initiator === 'xmlhttprequest').length,
            segmentLikeCount: resources.filter((entry) => segmentPattern.test(entry.name)).length,
            videoElementCount: videos.length,
            sourceElementCount: sources.length,
            videoWithCurrentSrcCount: videos.filter((video) => !!video.currentSrc).length,
            videoWithSrcAttrCount: videos.filter((video) => !!(video.getAttribute('src') || video.src)).length,
            videoPausedCount: videos.filter((video) => video.paused).length,
            videoErrorCount: videos.filter((video) => !!video.error).length,
            videoReadyStateMax: videos.reduce((value, video) => Math.max(value, Number(video.readyState) || 0), 0),
            videoNetworkStateMax: videos.reduce((value, video) => Math.max(value, Number(video.networkState) || 0), 0),
            mediaUrls: Array.from(new Set(mediaUrls)),
            resourceHostKinds,
            hasBlobVideo: videos.some((video) => (video.currentSrc || video.src || '').startsWith('blob:')),
            hasLicenseResource: resources.some((entry) => /(widevine|playready|drm|license)/i.test(entry.name)),
            playAttempted: !!state.playAttempted,
            playPending: !!state.playPending,
            playFulfilled: !!state.playFulfilled,
            playRejected: !!state.playRejected,
            playErrorName: state.playErrorName || '',
            visibilityState: document.visibilityState || '',
            documentHasFocus: typeof document.hasFocus === 'function' ? document.hasFocus() : false,
            viewportWidth: Math.max(0, Number(window.innerWidth) || 0),
            viewportHeight: Math.max(0, Number(window.innerHeight) || 0),
            mseH264Supported: hasMediaSource && MediaSource.isTypeSupported(h264),
            mseAacSupported: hasMediaSource && MediaSource.isTypeSupported(aac),
            videoH264CanPlay: !!codecVideo.canPlayType(h264),
            navigatorWebdriver: navigator.webdriver === true,
            emeAvailable: typeof navigator.requestMediaKeySystemAccess === 'function'
        });
    })()"#
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
    fn provider_url_validation_rejects_cross_domain_navigation() {
        assert!(validate_provider_url("https://www.iqiyi.com/v_demo.html", &["iqiyi.com"]).is_ok());
        assert!(validate_provider_url("https://example.com/v_demo.html", &["iqiyi.com"]).is_err());
    }

    #[test]
    fn browser_media_normalization_drops_blob_and_duplicates() {
        let page = Url::parse("https://www.iqiyi.com/v_demo.html").expect("page");
        let urls = normalize_media_urls(
            &page,
            vec![
                "//cdn.example/movie.m3u8".to_string(),
                "https://cdn.example/movie.m3u8".to_string(),
                "blob:https://www.iqiyi.com/id".to_string(),
            ],
        );
        assert_eq!(urls, vec!["https://cdn.example/movie.m3u8"]);
    }

    #[test]
    fn cookie_preparation_drops_expired_and_keeps_latest_duplicate() {
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
        assert_eq!(stats.effective_count, 2);
        assert_eq!(prepared.len(), 2);
        assert!(prepared.iter().any(|value| {
            value.get("name").and_then(Value::as_str) == Some("P00001")
                && value.get("value").and_then(Value::as_str) == Some("latest")
        }));
    }

    #[test]
    fn probe_script_never_logs_resource_paths_or_cookie_values() {
        let script = browser_probe_script();
        assert!(script.contains("hostname"));
        assert!(!script.contains("document.cookie"));
        assert!(!script.contains("location.search"));
    }

    #[test]
    fn adaptive_probe_budget_keeps_shutdown_reserve() {
        assert!(MAX_BROWSER_PROBE_ATTEMPTS >= 3);
        assert!(BROWSER_RENDER_COMPLETION_RESERVE >= Duration::from_millis(1000));
        assert!(CDP_PROBE_TIMEOUT < BROWSER_RENDER_TIMEOUT);
    }
}
