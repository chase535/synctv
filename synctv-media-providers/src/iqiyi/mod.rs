use std::collections::HashSet;

use regex::Regex;
use url::Url;

use crate::browser_session::{
    render_web_page_playback, BrowserPageDiagnostics, BrowserPageObservation,
};
use crate::web_session::{
    discover_web_page_playback, ScopedWebSessionClient, SessionCookie, WebPagePlaybackDiscovery,
};
use crate::ProviderClientError;

pub const IQIYI_SESSION_DOMAINS: &[&str] = &["iqiyi.com", "qiyi.com"];

#[derive(Clone)]
pub struct IqiyiClient {
    session: ScopedWebSessionClient,
}

impl IqiyiClient {
    pub fn new(
        client: reqwest::Client,
        cookies: Vec<SessionCookie>,
    ) -> Result<Self, ProviderClientError> {
        Ok(Self {
            session: ScopedWebSessionClient::new(client, IQIYI_SESSION_DOMAINS, cookies)?,
        })
    }

    #[must_use]
    pub fn cookies(&self) -> &[SessionCookie] {
        self.session.cookies()
    }

    pub fn validate_url(&self, url: &str) -> Result<url::Url, ProviderClientError> {
        self.session.validate_url(url)
    }

    /// Fetch an iQiyi web resource using the authenticated server-side session.
    ///
    /// The shared provider HTTP client identifies as a current desktop browser.
    /// The mobile embedded WebView is only used to establish the login session
    /// and its mobile User-Agent is never copied into these server requests.
    ///
    /// This primitive deliberately does not decrypt DRM media or synthesize
    /// playback licenses. Provider-specific playback code may only consume
    /// upstream resources that the authenticated account is legitimately
    /// authorized to request.
    pub async fn fetch_page(&self, url: &str) -> Result<String, ProviderClientError> {
        self.session.get_text(url).await
    }

    /// Discover direct HTTP(S) media explicitly exposed by the authenticated page.
    ///
    /// Static HTML/bootstrap discovery remains the fast path. If it yields no
    /// media and no DRM marker, a local headless Chromium instance loads the
    /// official page with the authenticated cookie jar, executes the provider's
    /// normal page JavaScript, and observes only media URLs exposed through the
    /// rendered DOM or the browser Performance API. The renderer does not call
    /// private signing APIs, derive device credentials, inspect response bodies,
    /// or construct DRM licenses.
    pub async fn discover_playback(
        &self,
        url: &str,
    ) -> Result<WebPagePlaybackDiscovery, ProviderClientError> {
        let page_url = self.validate_url(url)?;
        let html = self.fetch_page(url).await?;
        let mut discovery = discover_web_page_playback(&page_url, &html)?;
        discover_serialized_web_media(&html, &mut discovery)?;
        prioritize_full_hd_or_better(&mut discovery.media_urls);
        log_static_diagnostics(&page_url, &html, &discovery);

        if discovery.media_urls.is_empty() && !discovery.drm_detected {
            match render_web_page_playback(
                page_url.as_str(),
                IQIYI_SESSION_DOMAINS,
                self.session.cookies(),
            )
            .await
            {
                Ok(rendered) => {
                    log_browser_diagnostics(&page_url, &rendered.diagnostics, &rendered.discovery);
                    merge_browser_discovery(&mut discovery, rendered);
                    prioritize_full_hd_or_better(&mut discovery.media_urls);
                }
                Err(error) => {
                    tracing::warn!(
                        target: "synctv_media_providers::iqiyi",
                        page_host = page_url.host_str().unwrap_or(""),
                        error = %error,
                        "iQiyi browser discovery fallback failed"
                    );
                }
            }
        }

        Ok(discovery)
    }
}

fn discover_serialized_web_media(
    html: &str,
    discovery: &mut WebPagePlaybackDiscovery,
) -> Result<(), ProviderClientError> {
    // Work on a scan-only copy. JSON embedded in HTML commonly escapes URL
    // punctuation, so normalize those textual escapes before looking for URLs.
    // This never executes page JavaScript.
    let decoded = html_escape::decode_html_entities(html);
    let normalized = decoded
        .replace("\\u003A", ":")
        .replace("\\u003a", ":")
        .replace("\\u002F", "/")
        .replace("\\u002f", "/")
        .replace("\\u002E", ".")
        .replace("\\u002e", ".")
        .replace("\\u003F", "?")
        .replace("\\u003f", "?")
        .replace("\\u0026", "&")
        .replace("\\u003D", "=")
        .replace("\\u003d", "=")
        .replace("\\x3A", ":")
        .replace("\\x3a", ":")
        .replace("\\x2F", "/")
        .replace("\\x2f", "/")
        .replace("\\x2E", ".")
        .replace("\\x2e", ".")
        .replace("\\x3F", "?")
        .replace("\\x3f", "?")
        .replace("\\x26", "&")
        .replace("\\x3D", "=")
        .replace("\\x3d", "=")
        .replace("\\/", "/");
    let media_regex =
        Regex::new(r#"(?i)(?:https?:)?//[^\s\"'<>\\]+?\.(?:m3u8|mpd|mp4)(?:\?[^\s\"'<>\\]*)?"#)
            .map_err(|error| ProviderClientError::Parse(error.to_string()))?;
    let page_url = Url::parse(&discovery.page_url)
        .map_err(|error| ProviderClientError::Parse(error.to_string()))?;

    let mut seen = discovery.media_urls.iter().cloned().collect::<HashSet<_>>();
    for matched in media_regex.find_iter(&normalized) {
        let candidate = matched.as_str().trim_end_matches([',', ';', '}', ']', ')']);
        let parsed = if candidate.starts_with("//") {
            page_url.join(candidate)
        } else {
            Url::parse(candidate)
        };
        let Ok(url) = parsed else {
            continue;
        };
        if !matches!(url.scheme(), "http" | "https") {
            continue;
        }
        let value = url.to_string();
        if seen.insert(value.clone()) {
            discovery.media_urls.push(value);
        }
    }
    Ok(())
}

fn merge_browser_discovery(
    discovery: &mut WebPagePlaybackDiscovery,
    rendered: BrowserPageObservation,
) {
    let BrowserPageObservation {
        discovery: browser_discovery,
        diagnostics: _,
    } = rendered;
    discovery.drm_detected |= browser_discovery.drm_detected;
    discovery.page_url = browser_discovery.page_url;
    if discovery.title.is_none() {
        discovery.title = browser_discovery.title;
    }
    let mut seen = discovery.media_urls.iter().cloned().collect::<HashSet<_>>();
    for url in browser_discovery.media_urls {
        if seen.insert(url.clone()) {
            discovery.media_urls.push(url);
        }
    }
}

fn log_static_diagnostics(page_url: &Url, html: &str, discovery: &WebPagePlaybackDiscovery) {
    let lower = html.to_ascii_lowercase();
    tracing::info!(
        target: "synctv_media_providers::iqiyi",
        stage = "static_html",
        page_host = page_url.host_str().unwrap_or(""),
        html_bytes = html.len(),
        media_count = discovery.media_urls.len(),
        drm_detected = discovery.drm_detected,
        has_m3u8 = lower.contains(".m3u8"),
        has_mpd = lower.contains(".mpd"),
        has_mp4 = lower.contains(".mp4"),
        has_video_tag = lower.contains("<video"),
        has_source_tag = lower.contains("<source"),
        has_blob_url = lower.contains("blob:"),
        has_video_id = lower.contains("videoid") || lower.contains("video_id"),
        has_tv_id = lower.contains("tvid") || lower.contains("tv_id"),
        has_unicode_url_escape = lower.contains(r"\u003a") || lower.contains(r"\u002f"),
        has_hex_url_escape = lower.contains(r"\x3a") || lower.contains(r"\x2f"),
        "iQiyi playback discovery diagnostics"
    );
}

fn log_browser_diagnostics(
    page_url: &Url,
    diagnostics: &BrowserPageDiagnostics,
    discovery: &WebPagePlaybackDiscovery,
) {
    tracing::info!(
        target: "synctv_media_providers::iqiyi",
        stage = "rendered_page",
        page_host = page_url.host_str().unwrap_or(""),
        ready_state = %diagnostics.ready_state,
        html_length = diagnostics.html_length,
        resource_count = diagnostics.resource_count,
        media_resource_count = diagnostics.media_resource_count,
        video_element_count = diagnostics.video_element_count,
        source_element_count = diagnostics.source_element_count,
        media_count = discovery.media_urls.len(),
        drm_detected = discovery.drm_detected,
        has_m3u8 = diagnostics.has_m3u8,
        has_mpd = diagnostics.has_mpd,
        has_mp4 = diagnostics.has_mp4,
        has_blob_video = diagnostics.has_blob_video,
        has_video_id = diagnostics.has_video_id,
        has_tv_id = diagnostics.has_tv_id,
        has_drm_marker = diagnostics.has_drm_marker,
        has_license_resource = diagnostics.has_license_resource,
        "iQiyi playback discovery diagnostics"
    );
}

fn explicit_video_height(raw_url: &str) -> Option<u16> {
    fn from_text(value: &str) -> Option<u16> {
        let lower = value.to_ascii_lowercase();
        [
            (("3840x2160", "2160p", "_2160", "-2160"), 2160),
            (("2560x1440", "1440p", "_1440", "-1440"), 1440),
            (("1920x1080", "1080p", "_1080", "-1080"), 1080),
            (("1280x720", "720p", "_720", "-720"), 720),
            (("854x480", "480p", "_480", "-480"), 480),
        ]
        .into_iter()
        .find_map(|((size, label, underscore, dash), height)| {
            (lower.contains(size)
                || lower.contains(label)
                || lower.contains(underscore)
                || lower.contains(dash))
            .then_some(height)
        })
    }

    if let Ok(url) = Url::parse(raw_url) {
        for (key, value) in url.query_pairs() {
            if matches!(
                key.to_ascii_lowercase().as_str(),
                "height" | "resolution" | "res" | "quality" | "definition" | "size"
            ) {
                if let Ok(height) = value.parse::<u16>() {
                    if matches!(height, 480 | 720 | 1080 | 1440 | 2160) {
                        return Some(height);
                    }
                }
                if let Some(height) = from_text(&value) {
                    return Some(height);
                }
            }
        }
    }
    from_text(raw_url)
}

fn prioritize_full_hd_or_better(media_urls: &mut Vec<String>) {
    let best = media_urls
        .iter()
        .enumerate()
        .filter_map(|(index, url)| explicit_video_height(url).map(|height| (index, height)))
        .filter(|(_, height)| *height >= 1080)
        .max_by_key(|(_, height)| *height);
    let Some((index, _)) = best else {
        return;
    };
    if index != 0 {
        let preferred = media_urls.remove(index);
        media_urls.insert(0, preferred);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PROVIDER_DESKTOP_WEB_USER_AGENT;

    #[test]
    fn desktop_web_identity_tracks_current_stable_chrome() {
        assert!(PROVIDER_DESKTOP_WEB_USER_AGENT.contains("Windows NT 10.0"));
        assert!(PROVIDER_DESKTOP_WEB_USER_AGENT.contains("Chrome/152."));
    }

    #[test]
    fn discovers_serialized_full_hd_web_media_and_prefers_it() {
        let mut discovery = WebPagePlaybackDiscovery {
            page_url: "https://www.iqiyi.com/v_demo.html".to_string(),
            title: None,
            media_urls: vec!["https://cdn.example/movie_720p.mp4".to_string()],
            drm_detected: false,
        };
        let html = r#"
            <script>
              window.__BOOTSTRAP__ = {
                "primary":"https:\/\/cdn.example\/movie_1080p.m3u8?token=abc\u0026expires=9999999999",
                "backup":"https:\/\/cdn.example\/movie_720p.m3u8"
              };
            </script>
        "#;

        discover_serialized_web_media(html, &mut discovery).expect("discover media");
        prioritize_full_hd_or_better(&mut discovery.media_urls);

        assert_eq!(
            discovery.media_urls.first().map(String::as_str),
            Some("https://cdn.example/movie_1080p.m3u8?token=abc&expires=9999999999")
        );
        assert!(discovery
            .media_urls
            .iter()
            .any(|url| url == "https://cdn.example/movie_720p.m3u8"));
    }

    #[test]
    fn discovers_protocol_relative_serialized_media() {
        let mut discovery = WebPagePlaybackDiscovery {
            page_url: "https://www.iqiyi.com/v_demo.html".to_string(),
            title: None,
            media_urls: Vec::new(),
            drm_detected: false,
        };
        let html = r#"
            <script>
              window.__BOOTSTRAP__ = {
                "media":"\/\/cdn.example\/movie_1080p.m3u8?token=abc"
              };
            </script>
        "#;

        discover_serialized_web_media(html, &mut discovery).expect("discover media");

        assert_eq!(
            discovery.media_urls,
            vec!["https://cdn.example/movie_1080p.m3u8?token=abc"]
        );
    }

    #[test]
    fn discovers_unicode_and_hex_serialized_media_urls() {
        let mut discovery = WebPagePlaybackDiscovery {
            page_url: "https://www.iqiyi.com/v_demo.html".to_string(),
            title: None,
            media_urls: Vec::new(),
            drm_detected: false,
        };
        let html = r#"
            <script>
              const primary = "https\u003A\u002F\u002Fcdn.example\u002Fmovie_1080p.m3u8?token=abc\u0026expires=999";
              const backup = "https\x3A\x2F\x2Fcdn.example\x2Fmovie_720p.mp4?token=def\x26expires=999";
            </script>
        "#;

        discover_serialized_web_media(html, &mut discovery).expect("discover media");

        assert!(discovery
            .media_urls
            .iter()
            .any(|url| url == "https://cdn.example/movie_1080p.m3u8?token=abc&expires=999"));
        assert!(discovery
            .media_urls
            .iter()
            .any(|url| url == "https://cdn.example/movie_720p.mp4?token=def&expires=999"));
    }

    #[test]
    fn does_not_invent_full_hd_when_page_only_exposes_720p() {
        let mut urls = vec![
            "https://cdn.example/movie_720p.m3u8".to_string(),
            "https://cdn.example/movie_480p.mp4".to_string(),
        ];
        prioritize_full_hd_or_better(&mut urls);
        assert_eq!(urls[0], "https://cdn.example/movie_720p.m3u8");
        assert!(!urls
            .iter()
            .any(|url| explicit_video_height(url) == Some(1080)));
    }
}
