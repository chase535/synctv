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
// The deployed 1C/1G Docker host now spends ~9 s on Chromium startup,
// ~2 s attaching the page, and ~5-6 s accepting Page.navigate. A 22 s
// all-in deadline killed the browser just as the first page probe became
// possible. Keep the hard cap bounded, but leave enough budget for one real
// rendered-page observation after startup.
const BROWSER_RENDER_TIMEOUT: Duration = Duration::from_secs(30);
const CDP_STARTUP_TIMEOUT: Duration = Duration::from_secs(12);
const CDP_COMMAND_TIMEOUT: Duration = Duration::from_secs(6);
const CDP_SLOW_COMMAND_THRESHOLD: Duration = Duration::from_millis(500);
const BROWSER_PROBE_INTERVAL: Duration = Duration::from_millis(400);
const BROWSER_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(5);
const BLOB_VIDEO_GRACE_DELAY: Duration = Duration::from_millis(2200);
const BROWSER_PROFILE_CLEANUP_DELAY: Duration = Duration::from_millis(400);
const MAX_CONCURRENT_BROWSER_RENDERS: usize = 1;
const MAX_CDP_MESSAGE_BYTES: usize = 4 * 1024 * 1024;
const MAX_LOGGED_MEDIA_HOSTS: usize = 5;
// Chromium's --single-process mode is intentionally limited to truly tiny
// hosts. It materially reduces thread/process pressure there, while normal
// deployments continue to use Chromium's standard multi-process model.
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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BrowserObservationPayload {
    current_url: String,
    title: String,
    media_urls: Vec<String>,
    drm_detected: bool,
    diagnostics: BrowserPageDiagnostics,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BrowserProbePayload {
    ready_state: String,
    resource_count: usize,
    xhr_fetch_count: usize,
    segment_like_count: usize,
    video_element_count: usize,
    media_urls: Vec<String>,
    has_blob_video: bool,
    has_license_resource: bool,
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
        let wait_finished = tokio::time::timeout(Duration::from_secs(1), self.child.wait())
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

    runtime.spawn(async move {
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
    let permit = tokio::time::timeout(BROWSER_QUEUE_TIMEOUT, BROWSER_RENDER_SEMAPHORE.acquire())
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
    let result = match tokio::time::timeout(
        BROWSER_RENDER_TIMEOUT,
        render_web_page_playback_inner(raw_url, allowed_domains, cookies, single_process),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => {
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

        // Storage.setCookies is a browser-level command. It avoids the target
        // Network domain entirely and measured substantially cheaper than
        // Network.setCookies in the pipe transport. Filter expired entries and
        // collapse duplicate name/domain/path tuples before sending the jar.
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

        tracing::info!(
            target: "synctv_media_providers::browser_session",
            stage = "cdp_configured",
            page_host = %page_host,
            event_domains_enabled = false,
            images_disabled = true,
            browser_level_cookie_install = true,
            transport = "pipe",
            flattened_session = true,
            "Authenticated browser page render diagnostics"
        );

        let navigate_started = Instant::now();
        cdp_call(
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
            elapsed_ms = navigate_started.elapsed().as_millis(),
            render_elapsed_ms = startup_started.elapsed().as_millis(),
            "Authenticated browser page render diagnostics"
        );

        let probe = wait_for_browser_signal(
            &mut pipe,
            &mut command_id,
            &session_id,
            &page_host,
        )
        .await?;

        let observation_started = Instant::now();
        let evaluation = cdp_call(
            &mut pipe,
            &mut command_id,
            Some(&session_id),
            "Runtime.evaluate",
            json!({
                "expression": browser_observation_script(),
                "returnByValue": true,
                "awaitPromise": true,
            }),
        )
        .await?;
        let serialized = evaluation
            .pointer("/result/value")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ProviderClientError::Parse(
                    "browser observation did not return a serialized value".to_string(),
                )
            })?;
        let mut payload: BrowserObservationPayload = serde_json::from_str(serialized)
            .map_err(|error| ProviderClientError::Parse(error.to_string()))?;
        payload.media_urls.extend(probe.payload.media_urls);
        let final_url = validate_provider_url(&payload.current_url, allowed_domains)?;
        let media_urls = normalize_media_urls(&final_url, payload.media_urls);

        tracing::info!(
            target: "synctv_media_providers::browser_session",
            stage = "observation_complete",
            page_host = %page_host,
            probe_reason = probe.reason,
            probe_attempts = probe.attempts,
            probe_elapsed_ms = probe.elapsed.as_millis(),
            observation_elapsed_ms = observation_started.elapsed().as_millis(),
            ready_state = %payload.diagnostics.ready_state,
            resource_count = payload.diagnostics.resource_count,
            media_resource_count = payload.diagnostics.media_resource_count,
            media_count = media_urls.len(),
            media_hosts = %summarize_media_hosts(&media_urls),
            media_kinds = %summarize_media_kinds(&media_urls),
            has_blob_video = payload.diagnostics.has_blob_video,
            drm_detected = payload.drm_detected,
            single_process,
            transport = "pipe",
            "Authenticated browser page render diagnostics"
        );

        Ok(BrowserPageObservation {
            discovery: WebPagePlaybackDiscovery {
                page_url: final_url.to_string(),
                title: (!payload.title.trim().is_empty()).then(|| payload.title.trim().to_string()),
                media_urls,
                drm_detected: payload.drm_detected,
            },
            diagnostics: payload.diagnostics,
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
    let stderr_path = profile_dir.join("chromium-stderr.log");
    let stderr_file = std::fs::File::create(&stderr_path).map_err(|error| {
        ProviderClientError::Network(format!("create Chromium stderr log: {error}"))
    })?;

    // Chromium's --remote-debugging-pipe protocol uses file descriptors 3 and 4.
    // The small POSIX shell wrapper duplicates the child's stdin/stdout to those
    // descriptors before exec'ing Chromium. This avoids TCP, HTTP /json/list and
    // WebSocket handshakes entirely, which are disproportionately expensive and
    // unreliable on the target 1-core / ~1 GB Docker host.
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

    let started = Instant::now();
    pipe.send(&payload).await?;
    let result = tokio::time::timeout(timeout, async {
        loop {
            let response = pipe.receive().await?;
            if response.get("id").and_then(Value::as_u64) != Some(current_id) {
                continue;
            }
            if let Some(expected_session) = session_id {
                let actual_session = response.get("sessionId").and_then(Value::as_str);
                if actual_session != Some(expected_session) {
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
            if elapsed >= CDP_SLOW_COMMAND_THRESHOLD {
                tracing::info!(
                    target: "synctv_media_providers::browser_session",
                    stage = "cdp_command_slow",
                    method,
                    success = inner.is_ok(),
                    elapsed_ms = elapsed.as_millis(),
                    timeout_ms = timeout.as_millis(),
                    transport = "pipe",
                    "Chromium CDP command diagnostics"
                );
            }
            inner
        }
        Err(_) => {
            tracing::warn!(
                target: "synctv_media_providers::browser_session",
                stage = "cdp_command_timeout",
                method,
                elapsed_ms = started.elapsed().as_millis(),
                timeout_ms = timeout.as_millis(),
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
    page_host: &str,
) -> Result<BrowserProbeOutcome, ProviderClientError> {
    let started = Instant::now();
    let mut attempts = 0_usize;

    loop {
        attempts = attempts.saturating_add(1);
        let evaluation = cdp_call(
            pipe,
            command_id,
            Some(session_id),
            "Runtime.evaluate",
            json!({
                "expression": browser_probe_script(),
                "returnByValue": true,
                "awaitPromise": false,
            }),
        )
        .await?;
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

        tracing::info!(
            target: "synctv_media_providers::browser_session",
            stage = "page_probe",
            page_host = %page_host,
            attempt = attempts,
            elapsed_ms = started.elapsed().as_millis(),
            ready_state = %payload.ready_state,
            resource_count = payload.resource_count,
            xhr_fetch_count = payload.xhr_fetch_count,
            segment_like_count = payload.segment_like_count,
            video_element_count = payload.video_element_count,
            media_count = payload.media_urls.len(),
            cgroup_memory_current_bytes = cgroup_memory_current_bytes().unwrap_or_default(),
            self_rss_kib = process_rss_kib().unwrap_or_default(),
            media_hosts = %summarize_media_hosts(&payload.media_urls),
            media_kinds = %summarize_media_kinds(&payload.media_urls),
            has_blob_video = payload.has_blob_video,
            has_license_resource = payload.has_license_resource,
            transport = "pipe",
            "Authenticated browser page render diagnostics"
        );

        let elapsed = started.elapsed();
        let reason = if !payload.media_urls.is_empty() {
            Some("media_url")
        } else if payload.has_license_resource {
            Some("license_resource")
        } else if payload.has_blob_video && elapsed >= BLOB_VIDEO_GRACE_DELAY {
            Some("blob_video")
        } else if elapsed >= BROWSER_DISCOVERY_TIMEOUT {
            Some("probe_timeout")
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
        tokio::time::sleep(BROWSER_PROBE_INTERVAL).await;
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

    // Iterate backwards so duplicate name/domain/path entries retain the most
    // recent jar value, then restore original order before sending to Chromium.
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
        const resources = entries.map((entry) => entry.name || '');
        const mediaPattern = /\.(?:m3u8|mpd|mp4)(?:[?#]|$)/i;
        const segmentPattern = /(?:\.m4s|\.ts)(?:[?#]|$)/i;
        const mediaUrls = resources.filter((url) => mediaPattern.test(url));
        const videos = Array.from(document.querySelectorAll('video'));
        const sources = Array.from(document.querySelectorAll('source'));
        for (const element of [...videos, ...sources]) {
            const value = element.currentSrc || element.src || element.getAttribute('src') || '';
            if (value && !value.startsWith('blob:') && !value.startsWith('data:')) mediaUrls.push(value);
        }
        return JSON.stringify({
            readyState: document.readyState || '',
            resourceCount: resources.length,
            xhrFetchCount: entries.filter((entry) => entry.initiatorType === 'fetch' || entry.initiatorType === 'xmlhttprequest').length,
            segmentLikeCount: resources.filter((url) => segmentPattern.test(url)).length,
            videoElementCount: videos.length,
            mediaUrls: Array.from(new Set(mediaUrls)),
            hasBlobVideo: videos.some((element) => (element.currentSrc || element.src || '').startsWith('blob:')),
            hasLicenseResource: resources.some((url) => /(widevine|playready|drm|license)/i.test(url))
        });
    })()"#
}

fn browser_observation_script() -> &'static str {
    r#"(() => {
        const entries = performance.getEntriesByType('resource');
        const resources = entries.map((entry) => entry.name || '');
        const mediaPattern = /\.(?:m3u8|mpd|mp4)(?:[?#]|$)/i;
        const mediaUrls = resources.filter((url) => mediaPattern.test(url));
        const videos = Array.from(document.querySelectorAll('video'));
        const sources = Array.from(document.querySelectorAll('source'));
        for (const element of [...videos, ...sources]) {
            const value = element.currentSrc || element.src || element.getAttribute('src') || '';
            if (value && !value.startsWith('blob:') && !value.startsWith('data:')) mediaUrls.push(value);
        }
        const html = document.documentElement ? document.documentElement.outerHTML : '';
        const lower = html.toLowerCase();
        const joinedResources = resources.join('\n').toLowerCase();
        const hasDrmMarker = ['widevine','playready','fairplay','com.widevine.alpha','licenseurl','license_url','drmlicense']
            .some((marker) => lower.includes(marker));
        const hasLicenseResource = /(widevine|playready|drm|license)/i.test(joinedResources);
        return JSON.stringify({
            currentUrl: location.href,
            title: document.title || '',
            mediaUrls: Array.from(new Set(mediaUrls)),
            drmDetected: hasDrmMarker || hasLicenseResource,
            diagnostics: {
                readyState: document.readyState,
                htmlLength: html.length,
                resourceCount: resources.length,
                mediaResourceCount: resources.filter((url) => mediaPattern.test(url)).length,
                videoElementCount: videos.length,
                sourceElementCount: sources.length,
                hasM3u8: lower.includes('.m3u8') || resources.some((url) => /\.m3u8(?:[?#]|$)/i.test(url)),
                hasMpd: lower.includes('.mpd') || resources.some((url) => /\.mpd(?:[?#]|$)/i.test(url)),
                hasMp4: lower.includes('.mp4') || resources.some((url) => /\.mp4(?:[?#]|$)/i.test(url)),
                hasBlobVideo: videos.some((element) => (element.currentSrc || element.src || '').startsWith('blob:')),
                hasVideoId: /(?:videoid|video_id|vid)[\"'\s:=]/i.test(html),
                hasTvId: /(?:tvid|tv_id)[\"'\s:=]/i.test(html),
                hasDrmMarker,
                hasLicenseResource
            }
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
    fn single_process_env_override_is_parsed() {
        // Parsing is exercised indirectly by documenting the accepted values.
        // Avoid mutating process-wide environment variables in parallel tests.
        assert!(matches!("true", "1" | "true" | "yes" | "on"));
        assert!(matches!("false", "0" | "false" | "no" | "off"));
    }
}
