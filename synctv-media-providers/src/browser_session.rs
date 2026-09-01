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

const BROWSER_QUEUE_TIMEOUT: Duration = Duration::from_secs(12);
const BROWSER_RENDER_TIMEOUT: Duration = Duration::from_secs(22);
const BROWSER_START_TIMEOUT: Duration = Duration::from_secs(12);
const BROWSER_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const BROWSER_START_POLL_INTERVAL: Duration = Duration::from_millis(100);
const CDP_COMMAND_TIMEOUT: Duration = Duration::from_secs(4);
const BROWSER_PROBE_INTERVAL: Duration = Duration::from_millis(400);
const BROWSER_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(7);
const BLOB_VIDEO_GRACE_DELAY: Duration = Duration::from_secs(3);
const MAX_CONCURRENT_BROWSER_RENDERS: usize = 1;
const MAX_BROWSER_STDERR_TAIL_BYTES: u64 = 16 * 1024;
const MAX_LOGGED_MEDIA_HOSTS: usize = 5;
const CDP_MAX_TOTAL_BUFFER_SIZE: u64 = 1_000_000;
const CDP_MAX_RESOURCE_BUFFER_SIZE: u64 = 256_000;

const BLOCKED_RESOURCE_URL_PATTERNS: &[&str] = &[
    "*.png", "*.jpg", "*.jpeg", "*.gif", "*.webp", "*.svg", "*.ico", "*.woff", "*.woff2",
    "*.ttf", "*.otf",
];

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

struct ChromiumProcess {
    child: Child,
    profile_dir: PathBuf,
}

impl ChromiumProcess {
    async fn shutdown(&mut self) {
        let _ = self.child.kill().await;
        let _ = tokio::fs::remove_dir_all(&self.profile_dir).await;
    }
}

impl Drop for ChromiumProcess {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
        let _ = std::fs::remove_dir_all(&self.profile_dir);
    }
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
            tracing::warn!(
                target: "synctv_media_providers::browser_session",
                stage = "browser_slot_timeout",
                page_host = %page_host,
                wait_ms = queue_started.elapsed().as_millis(),
                "Authenticated browser page render diagnostics"
            );
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
    let result = tokio::time::timeout(
        BROWSER_RENDER_TIMEOUT,
        render_web_page_playback_inner(raw_url, allowed_domains, cookies),
    )
    .await
    .map_err(|_| {
        tracing::warn!(
            target: "synctv_media_providers::browser_session",
            stage = "render_timeout",
            page_host = %page_host,
            render_elapsed_ms = render_started.elapsed().as_millis(),
            total_elapsed_ms = request_started.elapsed().as_millis(),
            "Authenticated browser page render diagnostics"
        );
        ProviderClientError::Network(format!(
            "browser page rendering timed out after {}s",
            BROWSER_RENDER_TIMEOUT.as_secs()
        ))
    })?;

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
        .map_err(|error| {
            ProviderClientError::Network(format!("create browser profile: {error}"))
        })?;

    tracing::info!(
        target: "synctv_media_providers::browser_session",
        stage = "profile_ready",
        page_host = %page_host,
        profile_parent = %profile_dir.parent().unwrap_or(Path::new("/tmp")).display(),
        "Authenticated browser page render diagnostics"
    );

    let (mut browser, browser_ws_url) = match start_chromium(&profile_dir, &page_host).await {
        Ok(browser) => browser,
        Err(error) => {
            let _ = tokio::fs::remove_dir_all(&profile_dir).await;
            return Err(error);
        }
    };

    let result = async {
        let target_started = Instant::now();
        let target_ws_url = find_page_target(&browser_ws_url).await?;
        tracing::info!(
            target: "synctv_media_providers::browser_session",
            stage = "page_target_ready",
            page_host = %page_host,
            elapsed_ms = target_started.elapsed().as_millis(),
            "Authenticated browser page render diagnostics"
        );

        let connect_started = Instant::now();
        let (mut socket, _) = tokio::time::timeout(
            BROWSER_CONNECT_TIMEOUT,
            connect_async(target_ws_url.as_str()),
        )
        .await
        .map_err(|_| {
            ProviderClientError::Network(format!(
                "connect browser page CDP timed out after {}s",
                BROWSER_CONNECT_TIMEOUT.as_secs()
            ))
        })?
        .map_err(|error| {
            ProviderClientError::Network(format!("connect browser page CDP: {error}"))
        })?;
        tracing::info!(
            target: "synctv_media_providers::browser_session",
            stage = "page_cdp_connected",
            page_host = %page_host,
            elapsed_ms = connect_started.elapsed().as_millis(),
            "Authenticated browser page render diagnostics"
        );

        let mut command_id = 0_u64;
        cdp_call(
            &mut socket,
            &mut command_id,
            "Network.enable",
            json!({
                "maxTotalBufferSize": CDP_MAX_TOTAL_BUFFER_SIZE,
                "maxResourceBufferSize": CDP_MAX_RESOURCE_BUFFER_SIZE,
            }),
        )
        .await?;
        cdp_call(&mut socket, &mut command_id, "Page.enable", json!({})).await?;
        cdp_call(&mut socket, &mut command_id, "Runtime.enable", json!({})).await?;
        cdp_call_best_effort(
            &mut socket,
            &mut command_id,
            "Network.setCacheDisabled",
            json!({ "cacheDisabled": true }),
            &page_host,
        )
        .await;
        cdp_call_best_effort(
            &mut socket,
            &mut command_id,
            "Network.setBlockedURLs",
            json!({ "urls": BLOCKED_RESOURCE_URL_PATTERNS }),
            &page_host,
        )
        .await;

        tracing::info!(
            target: "synctv_media_providers::browser_session",
            stage = "cdp_configured",
            page_host = %page_host,
            blocked_pattern_count = BLOCKED_RESOURCE_URL_PATTERNS.len(),
            devtools_total_buffer_bytes = CDP_MAX_TOTAL_BUFFER_SIZE,
            devtools_resource_buffer_bytes = CDP_MAX_RESOURCE_BUFFER_SIZE,
            "Authenticated browser page render diagnostics"
        );

        if !cookies.is_empty() {
            let cookie_params = cookies
                .iter()
                .map(chromium_cookie_param)
                .collect::<Vec<_>>();
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
        let media_hosts = summarize_media_hosts(&media_urls);
        let media_kinds = summarize_media_kinds(&media_urls);

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
            media_hosts = %media_hosts,
            media_kinds = %media_kinds,
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
) -> Result<(ChromiumProcess, String), ProviderClientError> {
    let chromium_bin = chromium_binary();
    let stderr_path = profile_dir.join("chromium-stderr.log");
    let stderr_file = std::fs::File::create(&stderr_path).map_err(|error| {
        ProviderClientError::Network(format!("create Chromium stderr log: {error}"))
    })?;

    let mut command = Command::new(&chromium_bin);
    command
        .arg("--headless=new")
        .arg("--no-sandbox")
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
        .arg("--metrics-recording-only")
        .arg("--mute-audio")
        .arg("--hide-scrollbars")
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
    let startup = tokio::time::timeout(BROWSER_START_TIMEOUT, async {
        loop {
            match child.try_wait() {
                Ok(Some(status)) => {
                    return Err(ProviderClientError::Network(format!(
                        "Chromium exited before DevTools became ready: status={status}; stderr_tail={}",
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

            if let Some((browser_ws_url, debugging_port)) =
                browser_devtools_active_port(profile_dir)
            {
                return Ok((browser_ws_url, debugging_port));
            }
            if let Some((browser_ws_url, debugging_port)) =
                browser_devtools_ws_from_stderr(&stderr_path)
            {
                return Ok((browser_ws_url, debugging_port));
            }

            tokio::time::sleep(BROWSER_START_POLL_INTERVAL).await;
        }
    })
    .await;

    let (browser_ws_url, debugging_port) = match startup {
        Ok(Ok(ready)) => ready,
        Ok(Err(error)) => {
            log_container_resources("chromium_start_failed", page_host);
            let _ = child.kill().await;
            return Err(error);
        }
        Err(_) => {
            let stderr_tail = browser_stderr_tail(&stderr_path);
            let stderr_bytes = std::fs::metadata(&stderr_path)
                .map(|metadata| metadata.len())
                .unwrap_or_default();
            log_container_resources("chromium_start_timeout", page_host);
            tracing::warn!(
                target: "synctv_media_providers::browser_session",
                stage = "chromium_start_timeout",
                page_host = %page_host,
                chromium_bin = %chromium_bin,
                browser_pid,
                elapsed_ms = startup_started.elapsed().as_millis(),
                stderr_bytes,
                stderr_tail = %stderr_tail,
                "Authenticated browser page render diagnostics"
            );
            let _ = child.kill().await;
            return Err(ProviderClientError::Network(format!(
                "Chromium DevTools startup timed out after {}s; binary={chromium_bin}; stderr_tail={stderr_tail}",
                BROWSER_START_TIMEOUT.as_secs()
            )));
        }
    };

    tracing::info!(
        target: "synctv_media_providers::browser_session",
        stage = "chromium_ready",
        page_host = %page_host,
        chromium_bin = %chromium_bin,
        browser_pid,
        debugging_port,
        elapsed_ms = startup_started.elapsed().as_millis(),
        stderr_bytes = std::fs::metadata(&stderr_path)
            .map(|metadata| metadata.len())
            .unwrap_or_default(),
        "Authenticated browser page render diagnostics"
    );
    log_container_resources("chromium_ready", page_host);

    Ok((
        ChromiumProcess {
            child,
            profile_dir: profile_dir.to_path_buf(),
        },
        browser_ws_url,
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

fn browser_devtools_active_port(profile_dir: &Path) -> Option<(String, u16)> {
    let text = std::fs::read_to_string(profile_dir.join("DevToolsActivePort")).ok()?;
    parse_devtools_active_port(&text)
}

fn parse_devtools_active_port(text: &str) -> Option<(String, u16)> {
    let mut lines = text.lines();
    let port = lines.next()?.trim().parse::<u16>().ok()?;
    let endpoint = lines.next()?.trim();
    if endpoint.starts_with("ws://") || endpoint.starts_with("wss://") {
        return Some((endpoint.to_string(), port));
    }
    if !endpoint.starts_with("/devtools/browser/") {
        return None;
    }
    Some((format!("ws://127.0.0.1:{port}{endpoint}"), port))
}

fn browser_devtools_ws_from_stderr(path: &Path) -> Option<(String, u16)> {
    let stderr_tail = browser_stderr_tail(path);
    let browser_ws_url = extract_devtools_ws_url(&stderr_tail)?;
    let port = Url::parse(&browser_ws_url).ok()?.port()?;
    Some((browser_ws_url, port))
}

fn extract_devtools_ws_url(text: &str) -> Option<String> {
    let (_, remainder) = text.rsplit_once("DevTools listening on ")?;
    let candidate = remainder.split_whitespace().next()?.trim();
    (candidate.starts_with("ws://") || candidate.starts_with("wss://"))
        .then(|| candidate.to_string())
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

async fn find_page_target(browser_ws_url: &str) -> Result<String, ProviderClientError> {
    let (mut browser_socket, _) =
        tokio::time::timeout(BROWSER_CONNECT_TIMEOUT, connect_async(browser_ws_url))
            .await
            .map_err(|_| {
                ProviderClientError::Network(format!(
                    "connect Chromium browser CDP timed out after {}s",
                    BROWSER_CONNECT_TIMEOUT.as_secs()
                ))
            })?
            .map_err(|error| {
                ProviderClientError::Network(format!("connect Chromium browser CDP: {error}"))
            })?;
    let mut command_id = 0_u64;
    let targets = cdp_call(
        &mut browser_socket,
        &mut command_id,
        "Target.getTargets",
        json!({}),
    )
    .await?;

    let target_id = targets
        .get("targetInfos")
        .and_then(Value::as_array)
        .and_then(|targets| {
            targets
                .iter()
                .find(|target| target.get("type").and_then(Value::as_str) == Some("page"))
        })
        .and_then(|target| target.get("targetId"))
        .and_then(Value::as_str)
        .map(str::to_string);

    let target_id = match target_id {
        Some(target_id) => target_id,
        None => cdp_call(
            &mut browser_socket,
            &mut command_id,
            "Target.createTarget",
            json!({ "url": "about:blank" }),
        )
        .await?
        .get("targetId")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| {
            ProviderClientError::Parse(
                "Chromium Target.createTarget did not return targetId".to_string(),
            )
        })?,
    };

    page_target_ws_url(browser_ws_url, &target_id)
}

fn page_target_ws_url(
    browser_ws_url: &str,
    target_id: &str,
) -> Result<String, ProviderClientError> {
    let mut url = Url::parse(browser_ws_url).map_err(|error| {
        ProviderClientError::Parse(format!("invalid Chromium browser websocket URL: {error}"))
    })?;
    url.set_path(&format!("/devtools/page/{target_id}"));
    url.set_query(None);
    url.set_fragment(None);
    Ok(url.to_string())
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
            tracing::debug!(
                target: "synctv_media_providers::browser_session",
                stage = "cdp_command",
                method,
                success = inner.is_ok(),
                elapsed_ms = started.elapsed().as_millis(),
                "Chromium CDP command diagnostics"
            );
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

async fn cdp_call_best_effort(
    socket: &mut CdpSocket,
    command_id: &mut u64,
    method: &str,
    params: Value,
    page_host: &str,
) {
    if let Err(error) = cdp_call(socket, command_id, method, params).await {
        tracing::warn!(
            target: "synctv_media_providers::browser_session",
            stage = "cdp_optional_command_failed",
            page_host = %page_host,
            method,
            error = %error,
            "Chromium CDP optional optimization was not applied"
        );
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
    let domain = cookie.domain.trim();
    let path = if cookie.path.is_empty() {
        "/"
    } else {
        cookie.path.as_str()
    };
    let mut value = json!({
        "name": cookie.name,
        "value": cookie.value,
        "domain": domain,
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
            m3u8 = m3u8.saturating_add(1);
        } else if lower.contains(".mpd") {
            mpd = mpd.saturating_add(1);
        } else if lower.contains(".mp4") {
            mp4 = mp4.saturating_add(1);
        } else {
            other = other.saturating_add(1);
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

fn cgroup_memory_max() -> String {
    read_trimmed_file("/sys/fs/cgroup/memory.max")
        .or_else(|| read_trimmed_file("/sys/fs/cgroup/memory/memory.limit_in_bytes"))
        .unwrap_or_else(|| "unknown".to_string())
}

fn cgroup_memory_peak_bytes() -> Option<u64> {
    read_trimmed_file("/sys/fs/cgroup/memory.peak")
        .and_then(|value| value.parse::<u64>().ok())
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

fn cgroup_cpu_max() -> String {
    read_trimmed_file("/sys/fs/cgroup/cpu.max").unwrap_or_else(|| "unavailable".to_string())
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

fn cgroup_pids_max() -> String {
    read_trimmed_file("/sys/fs/cgroup/pids.max").unwrap_or_else(|| "unavailable".to_string())
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

fn log_container_resources(stage: &'static str, page_host: &str) {
    tracing::info!(
        target: "synctv_media_providers::browser_session",
        stage = stage,
        page_host = %page_host,
        cgroup_memory_current_bytes = cgroup_memory_current_bytes().unwrap_or_default(),
        cgroup_memory_peak_bytes = cgroup_memory_peak_bytes().unwrap_or_default(),
        cgroup_memory_max = %cgroup_memory_max(),
        cgroup_swap_current_bytes = cgroup_swap_current_bytes().unwrap_or_default(),
        cgroup_swap_max = %cgroup_swap_max(),
        cgroup_memory_events = %cgroup_memory_events(),
        cgroup_cpu_max = %cgroup_cpu_max(),
        cgroup_cpu_stat = %cgroup_cpu_stat(),
        cgroup_pids_current = cgroup_pids_current().unwrap_or_default(),
        cgroup_pids_max = %cgroup_pids_max(),
        self_rss_kib = process_rss_kib().unwrap_or_default(),
        "Container resource diagnostics"
    );
}

fn browser_probe_script() -> &'static str {
    r#"(() => {
        const resourceEntries = performance.getEntriesByType('resource');
        const resources = resourceEntries.map((entry) => entry.name || '');
        const mediaPattern = /\.(?:m3u8|mpd|mp4)(?:[?#]|$)/i;
        const segmentPattern = /(?:\.m4s|\.ts)(?:[?#]|$)/i;
        const mediaUrls = resources.filter((url) => mediaPattern.test(url));
        const videoElements = Array.from(document.querySelectorAll('video'));
        const sourceElements = Array.from(document.querySelectorAll('source'));
        for (const element of [...videoElements, ...sourceElements]) {
            const value = element.currentSrc || element.src || element.getAttribute('src') || '';
            if (value && !value.startsWith('blob:') && !value.startsWith('data:')) {
                mediaUrls.push(value);
            }
        }
        return JSON.stringify({
            readyState: document.readyState || '',
            resourceCount: resources.length,
            xhrFetchCount: resourceEntries.filter((entry) => entry.initiatorType === 'fetch' || entry.initiatorType === 'xmlhttprequest').length,
            segmentLikeCount: resources.filter((url) => segmentPattern.test(url)).length,
            videoElementCount: videoElements.length,
            mediaUrls: Array.from(new Set(mediaUrls)),
            hasBlobVideo: videoElements.some((element) => (element.currentSrc || element.src || '').startsWith('blob:')),
            hasLicenseResource: resources.some((url) => /(widevine|playready|drm|license)/i.test(url))
        });
    })()"#
}

fn browser_observation_script() -> &'static str {
    r#"(() => {
        const resources = performance.getEntriesByType('resource').map((entry) => entry.name || '');
        const mediaPattern = /\.(?:m3u8|mpd|mp4)(?:[?#]|$)/i;
        const mediaUrls = resources.filter((url) => mediaPattern.test(url));
        const videoElements = Array.from(document.querySelectorAll('video'));
        const sourceElements = Array.from(document.querySelectorAll('source'));
        for (const element of [...videoElements, ...sourceElements]) {
            const value = element.currentSrc || element.src || element.getAttribute('src') || '';
            if (value && !value.startsWith('blob:') && !value.startsWith('data:')) {
                mediaUrls.push(value);
            }
        }
        const html = document.documentElement ? document.documentElement.outerHTML : '';
        const lower = html.toLowerCase();
        const joinedResources = resources.join('\n').toLowerCase();
        const hasDrmMarker = [
            'widevine',
            'playready',
            'fairplay',
            'com.widevine.alpha',
            'licenseurl',
            'license_url',
            'drmlicense'
        ].some((marker) => lower.includes(marker));
        const hasLicenseResource = /(widevine|playready|drm|license)/i.test(joinedResources);
        const uniqueMedia = Array.from(new Set(mediaUrls));
        return JSON.stringify({
            currentUrl: location.href,
            title: document.title || '',
            mediaUrls: uniqueMedia,
            drmDetected: hasDrmMarker || hasLicenseResource,
            diagnostics: {
                readyState: document.readyState,
                htmlLength: html.length,
                resourceCount: resources.length,
                mediaResourceCount: resources.filter((url) => mediaPattern.test(url)).length,
                videoElementCount: videoElements.length,
                sourceElementCount: sourceElements.length,
                hasM3u8: lower.includes('.m3u8') || resources.some((url) => /\.m3u8(?:[?#]|$)/i.test(url)),
                hasMpd: lower.includes('.mpd') || resources.some((url) => /\.mpd(?:[?#]|$)/i.test(url)),
                hasMp4: lower.includes('.mp4') || resources.some((url) => /\.mp4(?:[?#]|$)/i.test(url)),
                hasBlobVideo: videoElements.some((element) => (element.currentSrc || element.src || '').startsWith('blob:')),
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
    fn parses_chromium_devtools_active_port_file() {
        assert_eq!(
            parse_devtools_active_port("44385\n/devtools/browser/abc\n"),
            Some((
                "ws://127.0.0.1:44385/devtools/browser/abc".to_string(),
                44385
            ))
        );
    }

    #[test]
    fn rejects_incomplete_chromium_devtools_active_port_file() {
        assert!(parse_devtools_active_port("44385\n").is_none());
        assert!(parse_devtools_active_port("not-a-port\n/devtools/browser/abc\n").is_none());
    }

    #[test]
    fn extracts_browser_devtools_websocket_from_stderr_tail() {
        let text =
            "dbus noise\nDevTools listening on ws://127.0.0.1:44385/devtools/browser/abc\nmore noise";
        assert_eq!(
            extract_devtools_ws_url(text).as_deref(),
            Some("ws://127.0.0.1:44385/devtools/browser/abc")
        );
    }

    #[test]
    fn builds_page_target_websocket_from_browser_endpoint() {
        assert_eq!(
            page_target_ws_url("ws://127.0.0.1:44385/devtools/browser/abc", "page-id")
                .expect("page target URL"),
            "ws://127.0.0.1:44385/devtools/page/page-id"
        );
    }

    #[test]
    fn media_diagnostics_do_not_log_query_strings() {
        let urls = vec![
            "https://cdn-a.example/movie.m3u8?token=secret".to_string(),
            "https://cdn-b.example/movie.mp4?authorization=secret".to_string(),
        ];
        assert_eq!(summarize_media_hosts(&urls), "cdn-a.example,cdn-b.example");
        assert_eq!(
            summarize_media_kinds(&urls),
            "m3u8=1,mpd=0,mp4=1,other=0"
        );
    }
}
