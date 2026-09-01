use std::collections::HashSet;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio::process::{Child, Command};
use tokio::sync::Semaphore;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use url::Url;

use crate::web_session::{SessionCookie, WebPagePlaybackDiscovery};
use crate::{ProviderClientError, PROVIDER_DESKTOP_WEB_USER_AGENT};

const BROWSER_QUEUE_TIMEOUT: Duration = Duration::from_secs(10);
const BROWSER_RENDER_TIMEOUT: Duration = Duration::from_secs(22);
const BROWSER_START_TIMEOUT: Duration = Duration::from_secs(12);
const BROWSER_START_POLL_INTERVAL: Duration = Duration::from_millis(200);
const DEVTOOLS_TCP_ATTEMPT_TIMEOUT: Duration = Duration::from_millis(250);
const DEVTOOLS_TARGET_TIMEOUT: Duration = Duration::from_secs(4);
const DEVTOOLS_HTTP_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(1);
const DEVTOOLS_HTTP_RETRY_INTERVAL: Duration = Duration::from_millis(150);
const PAGE_CDP_CONNECT_TIMEOUT: Duration = Duration::from_secs(4);
const CDP_COMMAND_TIMEOUT: Duration = Duration::from_secs(4);
const CDP_SLOW_COMMAND_THRESHOLD: Duration = Duration::from_millis(500);
const BROWSER_PROBE_INTERVAL: Duration = Duration::from_millis(450);
const BROWSER_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(6);
const BLOB_VIDEO_GRACE_DELAY: Duration = Duration::from_millis(2500);
const BROWSER_PROFILE_CLEANUP_DELAY: Duration = Duration::from_millis(400);
const MAX_CONCURRENT_BROWSER_RENDERS: usize = 1;
const MAX_BROWSER_STDERR_TAIL_BYTES: u64 = 16 * 1024;
const MAX_LOGGED_MEDIA_HOSTS: usize = 5;

static BROWSER_RENDER_SEMAPHORE: Semaphore = Semaphore::const_new(MAX_CONCURRENT_BROWSER_RENDERS);

type CdpSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

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

#[derive(Debug, Clone)]
struct DevToolsEndpoint {
    browser_ws_url: String,
    host: String,
    port: u16,
    endpoint_seen_elapsed: Duration,
    tcp_ready_elapsed: Duration,
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
            // Give Chromium's utility children a brief chance to release files.
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
                Err(error) if index + 1 < delays.len() => {
                    tracing::debug!(
                        target: "synctv_media_providers::browser_session",
                        stage = "browser_deferred_cleanup_retry",
                        page_host = %page_host,
                        attempt = index + 1,
                        error_kind = ?error.kind(),
                        error = %error,
                        "Deferred Chromium profile cleanup will retry"
                    );
                }
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

pub async fn render_web_page_playback(
    raw_url: &str,
    allowed_domains: &'static [&'static str],
    cookies: &[SessionCookie],
) -> Result<BrowserPageObservation, ProviderClientError> {
    let page_host = page_host(raw_url);
    let request_started = Instant::now();

    tracing::info!(
        target: "synctv_media_providers::browser_session",
        stage = "render_requested",
        page_host = %page_host,
        cookie_count = cookies.len(),
        max_concurrent_renders = MAX_CONCURRENT_BROWSER_RENDERS,
        queue_timeout_ms = BROWSER_QUEUE_TIMEOUT.as_millis(),
        render_timeout_ms = BROWSER_RENDER_TIMEOUT.as_millis(),
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
        render_web_page_playback_inner(raw_url, allowed_domains, cookies),
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
        "Authenticated browser page render diagnostics"
    );

    result
}

async fn render_web_page_playback_inner(
    raw_url: &str,
    allowed_domains: &'static [&'static str],
    cookies: &[SessionCookie],
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
        "Authenticated browser page render diagnostics"
    );

    let (mut browser, devtools) = match start_chromium(&profile_dir, &page_host).await {
        Ok(browser) => browser,
        Err(error) => {
            schedule_profile_cleanup(profile_dir, page_host.clone());
            return Err(error);
        }
    };

    let result = async {
        let target_started = Instant::now();
        let target_ws_url = find_page_target(&devtools, &page_host).await?;
        tracing::info!(
            target: "synctv_media_providers::browser_session",
            stage = "page_target_ready",
            page_host = %page_host,
            elapsed_ms = target_started.elapsed().as_millis(),
            "Authenticated browser page render diagnostics"
        );

        let connect_started = Instant::now();
        let (mut socket, _) = tokio::time::timeout(
            PAGE_CDP_CONNECT_TIMEOUT,
            connect_async(target_ws_url.as_str()),
        )
        .await
        .map_err(|_| {
            ProviderClientError::Network(format!(
                "connect Chromium page CDP timed out after {}s",
                PAGE_CDP_CONNECT_TIMEOUT.as_secs()
            ))
        })?
        .map_err(|error| {
            ProviderClientError::Network(format!("connect Chromium page CDP: {error}"))
        })?;

        tracing::info!(
            target: "synctv_media_providers::browser_session",
            stage = "page_cdp_connected",
            page_host = %page_host,
            elapsed_ms = connect_started.elapsed().as_millis(),
            "Authenticated browser page render diagnostics"
        );

        // Deliberately do not enable event domains. This fallback only sends
        // commands and evaluates page state. Avoiding Network/Page event streams
        // materially reduces CPU and websocket-buffer pressure on a 1-core VPS.
        let mut command_id = 0_u64;
        tracing::info!(
            target: "synctv_media_providers::browser_session",
            stage = "cdp_configured",
            page_host = %page_host,
            event_domains_enabled = false,
            images_disabled = true,
            devtools_tcp_verified = true,
            "Authenticated browser page render diagnostics"
        );

        if !cookies.is_empty() {
            let cookie_started = Instant::now();
            let cookie_params = cookies.iter().map(chromium_cookie_param).collect::<Vec<_>>();
            cdp_call(
                &mut socket,
                &mut command_id,
                "Network.setCookies",
                json!({ "cookies": cookie_params }),
            )
            .await?;
            tracing::info!(
                target: "synctv_media_providers::browser_session",
                stage = "cookies_installed",
                page_host = %page_host,
                cookie_count = cookies.len(),
                elapsed_ms = cookie_started.elapsed().as_millis(),
                "Authenticated browser page render diagnostics"
            );
        }

        let navigate_started = Instant::now();
        cdp_call(
            &mut socket,
            &mut command_id,
            "Page.navigate",
            json!({ "url": page_url.as_str() }),
        )
        .await?;
        tracing::info!(
            target: "synctv_media_providers::browser_session",
            stage = "navigation_started",
            page_host = %page_host,
            elapsed_ms = navigate_started.elapsed().as_millis(),
            "Authenticated browser page render diagnostics"
        );

        let probe = wait_for_browser_signal(&mut socket, &mut command_id, &page_host).await?;

        let observation_started = Instant::now();
        let evaluation = cdp_call(
            &mut socket,
            &mut command_id,
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

    browser.shutdown().await;
    result
}

async fn start_chromium(
    profile_dir: &Path,
    page_host: &str,
) -> Result<(ChromiumProcess, DevToolsEndpoint), ProviderClientError> {
    let chromium_bin = chromium_binary();
    let stderr_path = profile_dir.join("chromium-stderr.log");
    let stderr_file = std::fs::File::create(&stderr_path).map_err(|error| {
        ProviderClientError::Network(format!("create Chromium stderr log: {error}"))
    })?;

    let mut command = Command::new(&chromium_bin);
    command
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
        .arg("--renderer-process-limit=1")
        .arg("--remote-allow-origins=*")
        .arg("--remote-debugging-address=127.0.0.1")
        .arg("--remote-debugging-port=0")
        .arg(format!("--user-agent={PROVIDER_DESKTOP_WEB_USER_AGENT}"))
        .arg(format!("--user-data-dir={}", profile_dir.display()))
        .arg("about:blank")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::from(stderr_file))
        .kill_on_drop(true);

    let spawn_started = Instant::now();
    let mut child = command.spawn().map_err(|error| {
        ProviderClientError::Network(format!("start Chromium ({chromium_bin}): {error}"))
    })?;
    let browser_pid = child.id().unwrap_or_default();

    tracing::info!(
        target: "synctv_media_providers::browser_session",
        stage = "chromium_spawned",
        page_host = %page_host,
        chromium_bin = %chromium_bin,
        browser_pid,
        spawn_elapsed_ms = spawn_started.elapsed().as_millis(),
        "Authenticated browser page render diagnostics"
    );
    log_container_resources("chromium_spawned", page_host);

    let startup_started = Instant::now();
    let mut endpoint_seen_at: Option<Instant> = None;
    let mut endpoint_source = "none";
    let mut last_port = 0_u16;
    let mut last_path = String::new();
    let mut poll_count = 0_u32;

    let startup_result = tokio::time::timeout(BROWSER_START_TIMEOUT, async {
        loop {
            poll_count = poll_count.saturating_add(1);
            match child.try_wait() {
                Ok(Some(status)) => {
                    return Err(ProviderClientError::Network(format!(
                        "Chromium exited before DevTools became reachable: status={status}; stderr_tail={}",
                        browser_stderr_tail(&stderr_path)
                    )));
                }
                Ok(None) => {}
                Err(error) => {
                    return Err(ProviderClientError::Network(format!(
                        "check Chromium startup status: {error}"
                    )));
                }
            }

            let mut candidate = browser_devtools_active_port(profile_dir)
                .map(|(port, path)| (port, path, "DevToolsActivePort"));
            if candidate.is_none() && poll_count % 5 == 0 {
                candidate = browser_devtools_ws_from_stderr(&stderr_path).map(|url| {
                    let parsed = Url::parse(&url).ok();
                    let port = parsed.as_ref().and_then(Url::port).unwrap_or_default();
                    let path = parsed
                        .as_ref()
                        .map(|url| url.path().to_string())
                        .unwrap_or_default();
                    (port, path, "stderr")
                });
            }

            if let Some((port, path, source)) = candidate {
                if port != 0 && !path.is_empty() {
                    last_port = port;
                    last_path.clone_from(&path);
                    if endpoint_seen_at.is_none() {
                        endpoint_seen_at = Some(Instant::now());
                        endpoint_source = source;
                        tracing::info!(
                            target: "synctv_media_providers::browser_session",
                            stage = "devtools_endpoint_seen",
                            page_host = %page_host,
                            browser_pid,
                            debugging_port = port,
                            source,
                            elapsed_ms = startup_started.elapsed().as_millis(),
                            "Chromium published a DevTools endpoint; waiting for loopback TCP readiness"
                        );
                    }

                    if let Some(host) = probe_devtools_tcp(port).await {
                        let browser_ws_url = format_devtools_ws_url(host, port, &path);
                        let endpoint_seen_elapsed = endpoint_seen_at
                            .map(|seen| seen.duration_since(startup_started))
                            .unwrap_or_default();
                        return Ok(DevToolsEndpoint {
                            browser_ws_url,
                            host: host.to_string(),
                            port,
                            endpoint_seen_elapsed,
                            tcp_ready_elapsed: startup_started.elapsed(),
                        });
                    }
                }
            }

            tokio::time::sleep(BROWSER_START_POLL_INTERVAL).await;
        }
    })
    .await;

    let devtools = match startup_result {
        Ok(Ok(endpoint)) => endpoint,
        Ok(Err(error)) => {
            log_container_resources("chromium_start_failed", page_host);
            let _ = child.start_kill();
            let _ = tokio::time::timeout(Duration::from_secs(1), child.wait()).await;
            return Err(error);
        }
        Err(_) => {
            let stderr_tail = browser_stderr_tail(&stderr_path);
            log_container_resources("chromium_start_timeout", page_host);
            tracing::warn!(
                target: "synctv_media_providers::browser_session",
                stage = "chromium_start_timeout",
                page_host = %page_host,
                chromium_bin = %chromium_bin,
                browser_pid,
                elapsed_ms = startup_started.elapsed().as_millis(),
                endpoint_seen = endpoint_seen_at.is_some(),
                endpoint_source,
                last_debugging_port = last_port,
                last_endpoint_path_len = last_path.len(),
                tcp_ready = false,
                stderr_tail = %stderr_tail,
                "Authenticated browser page render diagnostics"
            );
            let _ = child.start_kill();
            let _ = tokio::time::timeout(Duration::from_secs(1), child.wait()).await;
            return Err(ProviderClientError::Network(format!(
                "Chromium DevTools TCP endpoint was not reachable within {}s; endpoint_seen={}; port={last_port}; stderr_tail={stderr_tail}",
                BROWSER_START_TIMEOUT.as_secs(),
                endpoint_seen_at.is_some()
            )));
        }
    };

    let readiness_lag = devtools
        .tcp_ready_elapsed
        .saturating_sub(devtools.endpoint_seen_elapsed);
    tracing::info!(
        target: "synctv_media_providers::browser_session",
        stage = "chromium_ready",
        page_host = %page_host,
        chromium_bin = %chromium_bin,
        browser_pid,
        debugging_port = devtools.port,
        devtools_host = %devtools.host,
        endpoint_seen_ms = devtools.endpoint_seen_elapsed.as_millis(),
        tcp_ready_ms = devtools.tcp_ready_elapsed.as_millis(),
        readiness_lag_ms = readiness_lag.as_millis(),
        stderr_bytes = std::fs::metadata(&stderr_path)
            .map(|metadata| metadata.len())
            .unwrap_or_default(),
        "Authenticated browser page render diagnostics"
    );
    log_container_resources("chromium_ready", page_host);

    Ok((
        ChromiumProcess {
            child,
            profile_dir: Some(profile_dir.to_path_buf()),
            page_host: page_host.to_string(),
        },
        devtools,
    ))
}

async fn probe_devtools_tcp(port: u16) -> Option<&'static str> {
    let ipv4 = tokio::time::timeout(
        DEVTOOLS_TCP_ATTEMPT_TIMEOUT,
        TcpStream::connect(("127.0.0.1", port)),
    )
    .await;
    if matches!(ipv4, Ok(Ok(_))) {
        return Some("127.0.0.1");
    }

    let ipv6 = tokio::time::timeout(
        DEVTOOLS_TCP_ATTEMPT_TIMEOUT,
        TcpStream::connect(("::1", port)),
    )
    .await;
    if matches!(ipv6, Ok(Ok(_))) {
        return Some("::1");
    }
    None
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

fn browser_devtools_active_port(profile_dir: &Path) -> Option<(u16, String)> {
    let text = std::fs::read_to_string(profile_dir.join("DevToolsActivePort")).ok()?;
    parse_devtools_active_port(&text)
}

fn parse_devtools_active_port(text: &str) -> Option<(u16, String)> {
    let mut lines = text.lines();
    let port = lines.next()?.trim().parse::<u16>().ok()?;
    let endpoint = lines.next()?.trim();
    if endpoint.starts_with("ws://") || endpoint.starts_with("wss://") {
        let parsed = Url::parse(endpoint).ok()?;
        return Some((parsed.port()?, parsed.path().to_string()));
    }
    if !endpoint.starts_with("/devtools/browser/") {
        return None;
    }
    Some((port, endpoint.to_string()))
}

fn browser_devtools_ws_from_stderr(path: &Path) -> Option<String> {
    let stderr_tail = browser_stderr_tail(path);
    extract_devtools_ws_url(&stderr_tail)
}

fn extract_devtools_ws_url(text: &str) -> Option<String> {
    let (_, remainder) = text.rsplit_once("DevTools listening on ")?;
    let candidate = remainder.split_whitespace().next()?.trim();
    (candidate.starts_with("ws://") || candidate.starts_with("wss://"))
        .then(|| candidate.to_string())
}

fn format_devtools_ws_url(host: &str, port: u16, path: &str) -> String {
    if host.contains(':') {
        format!("ws://[{host}]:{port}{path}")
    } else {
        format!("ws://{host}:{port}{path}")
    }
}

fn browser_stderr_tail(path: &Path) -> String {
    let Ok(mut file) = std::fs::File::open(path) else {
        return "unavailable".to_string();
    };
    let file_len = file.metadata().map(|metadata| metadata.len()).unwrap_or_default();
    let start = file_len.saturating_sub(MAX_BROWSER_STDERR_TAIL_BYTES);
    if file.seek(SeekFrom::Start(start)).is_err() {
        return "unavailable".to_string();
    }

    let mut buffer = Vec::new();
    if file
        .take(MAX_BROWSER_STDERR_TAIL_BYTES)
        .read_to_end(&mut buffer)
        .is_err()
    {
        return "unavailable".to_string();
    }

    let compact = String::from_utf8_lossy(&buffer)
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    if compact.is_empty() {
        "empty".to_string()
    } else {
        compact
    }
}

async fn find_page_target(
    devtools: &DevToolsEndpoint,
    page_host: &str,
) -> Result<String, ProviderClientError> {
    let endpoint = if devtools.host.contains(':') {
        format!("http://[{}]:{}/json/list", devtools.host, devtools.port)
    } else {
        format!("http://{}:{}/json/list", devtools.host, devtools.port)
    };
    let client = reqwest::Client::builder()
        .no_proxy()
        .connect_timeout(DEVTOOLS_HTTP_ATTEMPT_TIMEOUT)
        .timeout(DEVTOOLS_HTTP_ATTEMPT_TIMEOUT)
        .pool_max_idle_per_host(0)
        .build()
        .map_err(|error| {
            ProviderClientError::Network(format!("build Chromium DevTools HTTP client: {error}"))
        })?;

    let started = Instant::now();
    let mut attempts = 0_usize;
    let mut last_error = "no DevTools page target returned".to_string();

    loop {
        attempts = attempts.saturating_add(1);
        let attempt_started = Instant::now();
        match client.get(&endpoint).send().await {
            Ok(response) => match response.error_for_status() {
                Ok(response) => match response.json::<Value>().await {
                    Ok(targets) => {
                        let target_count = targets.as_array().map(Vec::len).unwrap_or_default();
                        if let Some(target_ws_url) = page_target_ws_from_json(&targets) {
                            tracing::info!(
                                target: "synctv_media_providers::browser_session",
                                stage = "devtools_target_list",
                                page_host = %page_host,
                                success = true,
                                attempts,
                                target_count,
                                attempt_elapsed_ms = attempt_started.elapsed().as_millis(),
                                elapsed_ms = started.elapsed().as_millis(),
                                "Authenticated browser page render diagnostics"
                            );
                            return Ok(target_ws_url);
                        }
                        last_error = format!("DevTools returned {target_count} target(s) but no page target");
                    }
                    Err(error) => {
                        last_error = format!("decode Chromium DevTools target list: {error}");
                    }
                },
                Err(error) => {
                    last_error = format!("Chromium DevTools target list HTTP status: {error}");
                }
            },
            Err(error) => {
                last_error = format!(
                    "query Chromium DevTools target list: {error}; is_connect={}; is_timeout={}",
                    error.is_connect(),
                    error.is_timeout()
                );
            }
        }

        if started.elapsed() >= DEVTOOLS_TARGET_TIMEOUT {
            tracing::warn!(
                target: "synctv_media_providers::browser_session",
                stage = "devtools_target_list",
                page_host = %page_host,
                success = false,
                attempts,
                elapsed_ms = started.elapsed().as_millis(),
                tcp_preverified = true,
                error = %last_error,
                browser_ws_path_len = devtools.browser_ws_url.len(),
                "Authenticated browser page render diagnostics"
            );
            return Err(ProviderClientError::Network(format!(
                "discover Chromium page target timed out after {}s (attempts={attempts}): {last_error}",
                DEVTOOLS_TARGET_TIMEOUT.as_secs()
            )));
        }

        tokio::time::sleep(DEVTOOLS_HTTP_RETRY_INTERVAL).await;
    }
}

fn page_target_ws_from_json(targets: &Value) -> Option<String> {
    targets.as_array()?.iter().find_map(|target| {
        (target.get("type").and_then(Value::as_str) == Some("page"))
            .then(|| target.get("webSocketDebuggerUrl").and_then(Value::as_str))
            .flatten()
            .map(str::to_string)
    })
}

async fn cdp_call(
    socket: &mut CdpSocket,
    command_id: &mut u64,
    method: &str,
    params: Value,
) -> Result<Value, ProviderClientError> {
    *command_id = command_id.saturating_add(1);
    let current_id = *command_id;
    let payload = json!({
        "id": current_id,
        "method": method,
        "params": params,
    });

    let started = Instant::now();
    socket
        .send(Message::text(payload.to_string()))
        .await
        .map_err(|error| {
            ProviderClientError::Network(format!("send Chromium CDP command {method}: {error}"))
        })?;

    let result = tokio::time::timeout(CDP_COMMAND_TIMEOUT, async {
        loop {
            let message = socket
                .next()
                .await
                .ok_or_else(|| {
                    ProviderClientError::Network("Chromium CDP connection closed".to_string())
                })?
                .map_err(|error| {
                    ProviderClientError::Network(format!("read Chromium CDP response: {error}"))
                })?;
            if !message.is_text() {
                continue;
            }
            let response: Value = serde_json::from_str(message.to_text().map_err(|error| {
                ProviderClientError::Parse(format!("decode Chromium CDP text: {error}"))
            })?)
            .map_err(|error| ProviderClientError::Parse(error.to_string()))?;
            if response.get("id").and_then(Value::as_u64) != Some(current_id) {
                continue;
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
                "Chromium CDP command diagnostics"
            );
            Err(ProviderClientError::Network(format!(
                "Chromium CDP command {method} timed out after {}s",
                CDP_COMMAND_TIMEOUT.as_secs()
            )))
        }
    }
}

async fn wait_for_browser_signal(
    socket: &mut CdpSocket,
    command_id: &mut u64,
    page_host: &str,
) -> Result<BrowserProbeOutcome, ProviderClientError> {
    let started = Instant::now();
    let mut attempts = 0_usize;

    loop {
        attempts = attempts.saturating_add(1);
        let evaluation = cdp_call(
            socket,
            command_id,
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

fn cgroup_swap_max() -> String {
    read_trimmed_file("/sys/fs/cgroup/memory.swap.max")
        .unwrap_or_else(|| "unavailable".to_string())
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

fn pressure_summary(kind: &str) -> String {
    let path = format!("/proc/pressure/{kind}");
    read_trimmed_file(&path)
        .map(|value| value.split_whitespace().take(8).collect::<Vec<_>>().join(" "))
        .unwrap_or_else(|| "unavailable".to_string())
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
        cgroup_swap_max = %cgroup_swap_max(),
        cgroup_memory_events = %cgroup_memory_events(),
        cgroup_cpu_stat = %cgroup_cpu_stat(),
        cgroup_pids_current = cgroup_pids_current().unwrap_or_default(),
        self_rss_kib = process_rss_kib().unwrap_or_default(),
        host_mem_total_kib = host.total_kib,
        host_mem_available_kib = host.available_kib,
        host_swap_total_kib = host.swap_total_kib,
        host_swap_free_kib = host.swap_free_kib,
        psi_memory = %pressure_summary("memory"),
        psi_cpu = %pressure_summary("cpu"),
        psi_io = %pressure_summary("io"),
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

    #[test]
    fn parses_chromium_devtools_active_port_file() {
        assert_eq!(
            parse_devtools_active_port("44385\n/devtools/browser/abc\n"),
            Some((44385, "/devtools/browser/abc".to_string()))
        );
    }

    #[test]
    fn extracts_browser_devtools_websocket_from_stderr_tail() {
        let text = "noise\nDevTools listening on ws://127.0.0.1:44385/devtools/browser/abc\n";
        assert_eq!(
            extract_devtools_ws_url(text).as_deref(),
            Some("ws://127.0.0.1:44385/devtools/browser/abc")
        );
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
}
