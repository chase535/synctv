use std::collections::HashSet;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

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

const BROWSER_START_TIMEOUT: Duration = Duration::from_secs(20);
const BROWSER_RENDER_TIMEOUT: Duration = Duration::from_secs(40);
const BROWSER_START_POLL_INTERVAL: Duration = Duration::from_millis(100);
const CDP_COMMAND_TIMEOUT: Duration = Duration::from_secs(10);
const PAGE_READY_POLL_INTERVAL: Duration = Duration::from_millis(250);
const PAGE_READY_POLL_ATTEMPTS: usize = 40;
const PAGE_SETTLE_DELAY: Duration = Duration::from_secs(3);
const MAX_CONCURRENT_BROWSER_RENDERS: usize = 2;
const MAX_BROWSER_STDERR_PREVIEW_BYTES: u64 = 4096;
const MAX_BROWSER_STDERR_SCAN_BYTES: u64 = 16384;

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
    let _permit = BROWSER_RENDER_SEMAPHORE.acquire().await.map_err(|error| {
        ProviderClientError::Network(format!("browser discovery semaphore closed: {error}"))
    })?;

    tokio::time::timeout(
        BROWSER_RENDER_TIMEOUT,
        render_web_page_playback_inner(raw_url, allowed_domains, cookies),
    )
    .await
    .map_err(|_| ProviderClientError::Network("browser page rendering timed out".to_string()))?
}

async fn render_web_page_playback_inner(
    raw_url: &str,
    allowed_domains: &'static [&'static str],
    cookies: &[SessionCookie],
) -> Result<BrowserPageObservation, ProviderClientError> {
    let page_url = validate_provider_url(raw_url, allowed_domains)?;
    let profile_dir =
        std::env::temp_dir().join(format!("synctv-chromium-{}", uuid::Uuid::new_v4().simple()));
    tokio::fs::create_dir_all(&profile_dir)
        .await
        .map_err(|error| {
            ProviderClientError::Network(format!("create browser profile: {error}"))
        })?;

    let (mut browser, browser_ws_url) = start_chromium(&profile_dir).await?;
    let result = async {
        let target_ws_url = find_page_target(&browser_ws_url).await?;
        let (mut socket, _) = connect_async(target_ws_url.as_str())
            .await
            .map_err(|error| {
                ProviderClientError::Network(format!("connect browser page CDP: {error}"))
            })?;
        let mut command_id = 0_u64;

        cdp_call(&mut socket, &mut command_id, "Network.enable", json!({})).await?;
        cdp_call(&mut socket, &mut command_id, "Page.enable", json!({})).await?;
        cdp_call(&mut socket, &mut command_id, "Runtime.enable", json!({})).await?;

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
        }

        cdp_call(
            &mut socket,
            &mut command_id,
            "Page.navigate",
            json!({ "url": page_url.as_str() }),
        )
        .await?;
        wait_for_page_ready(&mut socket, &mut command_id).await?;
        tokio::time::sleep(PAGE_SETTLE_DELAY).await;

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
        let payload: BrowserObservationPayload = serde_json::from_str(serialized)
            .map_err(|error| ProviderClientError::Parse(error.to_string()))?;
        let final_url = validate_provider_url(&payload.current_url, allowed_domains)?;
        let media_urls = normalize_media_urls(&final_url, payload.media_urls);

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
) -> Result<(ChromiumProcess, String), ProviderClientError> {
    let chromium_bin = chromium_binary();
    let debugging_port = reserve_local_port()?;
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
        .arg("--metrics-recording-only")
        .arg("--no-first-run")
        .arg("--no-default-browser-check")
        .arg("--remote-allow-origins=*")
        .arg("--remote-debugging-address=127.0.0.1")
        .arg(format!("--remote-debugging-port={debugging_port}"))
        .arg(format!("--user-agent={PROVIDER_DESKTOP_WEB_USER_AGENT}"))
        .arg(format!("--user-data-dir={}", profile_dir.display()))
        .arg("about:blank")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::from(stderr_file))
        .kill_on_drop(true);

    let mut child = command.spawn().map_err(|error| {
        ProviderClientError::Network(format!("start Chromium ({chromium_bin}): {error}"))
    })?;

    let startup = tokio::time::timeout(BROWSER_START_TIMEOUT, async {
        loop {
            match child.try_wait() {
                Ok(Some(status)) => {
                    return Err(ProviderClientError::Network(format!(
                        "Chromium exited before DevTools became ready: status={status}; stderr={}",
                        browser_stderr_preview(&stderr_path)
                    )));
                }
                Ok(None) => {}
                Err(error) => {
                    return Err(ProviderClientError::Network(format!(
                        "check Chromium startup status: {error}"
                    )));
                }
            }

            if let Some(browser_ws_url) = browser_devtools_ws_url(&stderr_path) {
                return Ok(browser_ws_url);
            }

            tokio::time::sleep(BROWSER_START_POLL_INTERVAL).await;
        }
    })
    .await;

    let browser_ws_url = match startup {
        Ok(result) => result?,
        Err(_) => {
            return Err(ProviderClientError::Network(format!(
                "Chromium DevTools startup timed out after {}s; binary={chromium_bin}; stderr={}",
                BROWSER_START_TIMEOUT.as_secs(),
                browser_stderr_preview(&stderr_path)
            )));
        }
    };

    tracing::debug!(
        target: "synctv_media_providers::browser_session",
        chromium_bin = %chromium_bin,
        debugging_port,
        "Chromium browser CDP websocket ready"
    );

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

fn reserve_local_port() -> Result<u16, ProviderClientError> {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0))
        .map_err(|error| ProviderClientError::Network(format!("reserve Chromium port: {error}")))?;
    let port = listener
        .local_addr()
        .map_err(|error| ProviderClientError::Network(format!("read Chromium port: {error}")))?
        .port();
    drop(listener);
    Ok(port)
}

fn read_limited_file(path: &Path, max_bytes: u64) -> Option<String> {
    let file = std::fs::File::open(path).ok()?;
    let mut buffer = Vec::new();
    let mut limited = file.take(max_bytes);
    limited.read_to_end(&mut buffer).ok()?;
    Some(String::from_utf8_lossy(&buffer).into_owned())
}

fn browser_devtools_ws_url(path: &Path) -> Option<String> {
    let text = read_limited_file(path, MAX_BROWSER_STDERR_SCAN_BYTES)?;
    extract_devtools_ws_url(&text)
}

fn extract_devtools_ws_url(text: &str) -> Option<String> {
    let (_, remainder) = text.rsplit_once("DevTools listening on ")?;
    let candidate = remainder.split_whitespace().next()?.trim();
    (candidate.starts_with("ws://") || candidate.starts_with("wss://"))
        .then(|| candidate.to_string())
}

fn browser_stderr_preview(path: &Path) -> String {
    let Some(preview) = read_limited_file(path, MAX_BROWSER_STDERR_PREVIEW_BYTES) else {
        return "unavailable".to_string();
    };
    let compact = preview.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.is_empty() {
        "empty".to_string()
    } else {
        compact
    }
}

async fn find_page_target(browser_ws_url: &str) -> Result<String, ProviderClientError> {
    let (mut browser_socket, _) = connect_async(browser_ws_url)
        .await
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
            targets.iter().find_map(|target| {
                (target.get("type").and_then(Value::as_str) == Some("page"))
                    .then(|| target.get("targetId").and_then(Value::as_str))
                    .flatten()
                    .map(str::to_string)
            })
        });

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

fn page_target_ws_url(browser_ws_url: &str, target_id: &str) -> Result<String, ProviderClientError> {
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
    socket
        .send(Message::text(payload.to_string()))
        .await
        .map_err(|error| {
            ProviderClientError::Network(format!("send Chromium CDP command: {error}"))
        })?;

    tokio::time::timeout(CDP_COMMAND_TIMEOUT, async {
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
    .await
    .map_err(|_| ProviderClientError::Network(format!("Chromium CDP command {method} timed out")))?
}

async fn wait_for_page_ready(
    socket: &mut CdpSocket,
    command_id: &mut u64,
) -> Result<(), ProviderClientError> {
    for _ in 0..PAGE_READY_POLL_ATTEMPTS {
        let result = cdp_call(
            socket,
            command_id,
            "Runtime.evaluate",
            json!({
                "expression": "document.readyState",
                "returnByValue": true,
            }),
        )
        .await?;
        if result.pointer("/result/value").and_then(Value::as_str) == Some("complete") {
            return Ok(());
        }
        tokio::time::sleep(PAGE_READY_POLL_INTERVAL).await;
    }
    Ok(())
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
    fn extracts_browser_devtools_websocket_from_stderr() {
        let text = "noise before\nDevTools listening on ws://127.0.0.1:44385/devtools/browser/abc\nnoise after";
        assert_eq!(
            extract_devtools_ws_url(text).as_deref(),
            Some("ws://127.0.0.1:44385/devtools/browser/abc")
        );
    }

    #[test]
    fn builds_page_target_websocket_from_browser_endpoint() {
        assert_eq!(
            page_target_ws_url(
                "ws://127.0.0.1:44385/devtools/browser/abc",
                "page-id"
            )
            .expect("page target URL"),
            "ws://127.0.0.1:44385/devtools/page/page-id"
        );
    }
}
