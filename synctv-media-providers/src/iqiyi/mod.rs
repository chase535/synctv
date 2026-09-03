use std::collections::HashSet;

use percent_encoding::percent_decode_str;
use regex::Regex;
use url::Url;

use crate::web_session::{
    discover_web_page_playback, ScopedWebSessionClient, SessionCookie, WebPagePlaybackDiscovery,
};
use crate::ProviderClientError;

mod tmts;

pub const IQIYI_SESSION_DOMAINS: &[&str] = &["iqiyi.com", "qiyi.com", "iq.com"];

const ABSOLUTE_MEDIA_PATTERN: &str =
    r#"(?i)(?:https?:)?//[^\s\"'<>\\]+?\.(?:m3u8|mpd|mp4)(?:\?[^\s\"'<>\\]*)?"#;
const ROOT_RELATIVE_MEDIA_PATTERN: &str =
    r#"(?i)[\"'](/[^/\s\"'<>\\][^\s\"'<>\\]*?\.(?:m3u8|mpd|mp4)(?:\?[^\s\"'<>\\]*)?)"#;

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
    pub async fn fetch_page(&self, url: &str) -> Result<String, ProviderClientError> {
        self.session.get_text(url).await
    }

    /// Discover playable media using only small HTTP requests.
    ///
    /// The authenticated page and serialized HTML remain the cheapest path. If
    /// they expose no media, SyncTV resolves the page identifiers and queries
    /// iQiyi's TMTS metadata endpoint. The request uses the existing scoped cookie
    /// jar and a small MD5 timestamp signature; no Chromium, JavaScript runtime,
    /// CDP connection, media decoder, or DRM-license request is started.
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
            match tmts::discover_tmts_media(&self.session, &page_url, &html).await {
                Ok(tmts_discovery) => {
                    merge_tmts_discovery(&mut discovery, tmts_discovery);
                    prioritize_full_hd_or_better(&mut discovery.media_urls);
                }
                Err(error) => tracing::warn!(
                    target: "synctv_media_providers::iqiyi",
                    stage = "tmts_http_failed",
                    page_host = page_url.host_str().unwrap_or(""),
                    error = %error,
                    "Pure HTTP iQiyi TMTS discovery failed"
                ),
            }
        }

        Ok(discovery)
    }
}

fn discover_serialized_web_media(
    html: &str,
    discovery: &mut WebPagePlaybackDiscovery,
) -> Result<(), ProviderClientError> {
    let normalized = normalize_serialized_scan_text(html);
    let absolute_media_regex = Regex::new(ABSOLUTE_MEDIA_PATTERN)
        .map_err(|error| ProviderClientError::Parse(error.to_string()))?;
    let root_relative_media_regex = Regex::new(ROOT_RELATIVE_MEDIA_PATTERN)
        .map_err(|error| ProviderClientError::Parse(error.to_string()))?;
    let page_url = Url::parse(&discovery.page_url)
        .map_err(|error| ProviderClientError::Parse(error.to_string()))?;

    let mut seen = discovery.media_urls.iter().cloned().collect::<HashSet<_>>();
    for matched in absolute_media_regex.find_iter(&normalized) {
        push_media_candidate(
            matched.as_str(),
            &page_url,
            &mut seen,
            &mut discovery.media_urls,
        );
    }
    for captures in root_relative_media_regex.captures_iter(&normalized) {
        if let Some(candidate) = captures.get(1) {
            push_media_candidate(
                candidate.as_str(),
                &page_url,
                &mut seen,
                &mut discovery.media_urls,
            );
        }
    }
    Ok(())
}

fn normalize_serialized_scan_text(html: &str) -> String {
    let mut normalized = html_escape::decode_html_entities(html).into_owned();
    for _ in 0..3 {
        let previous = normalized.clone();
        normalized = normalized
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
        normalized = percent_decode_str(&normalized)
            .decode_utf8_lossy()
            .into_owned();
        if normalized == previous {
            break;
        }
    }
    normalized
}

fn push_media_candidate(
    raw_candidate: &str,
    page_url: &Url,
    seen: &mut HashSet<String>,
    media_urls: &mut Vec<String>,
) {
    let candidate = raw_candidate
        .trim()
        .trim_end_matches([',', ';', '}', ']', ')']);
    let Ok(url) = page_url.join(candidate) else {
        return;
    };
    if !matches!(url.scheme(), "http" | "https") {
        return;
    }
    let value = url.to_string();
    if seen.insert(value.clone()) {
        media_urls.push(value);
    }
}

fn merge_tmts_discovery(
    discovery: &mut WebPagePlaybackDiscovery,
    tmts_discovery: tmts::TmtsDiscovery,
) {
    discovery.drm_detected |= tmts_discovery.drm_detected;
    let mut seen = discovery.media_urls.iter().cloned().collect::<HashSet<_>>();
    for url in tmts_discovery.media_urls {
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
        has_tv_id = lower.contains("tvid") || lower.contains("tv_id"),
        strategy = "static_then_http_tmts",
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
    }

    #[test]
    fn discovers_unicode_hex_and_percent_encoded_urls() {
        let mut discovery = WebPagePlaybackDiscovery {
            page_url: "https://www.iqiyi.com/v_demo.html".to_string(),
            title: None,
            media_urls: Vec::new(),
            drm_detected: false,
        };
        let html = r#"
            <script>
              const first = "https\u003A\u002F\u002Fcdn.example\u002Fmovie_1080p.m3u8?x=1\u0026y=2";
              const second = "https%3A%2F%2Fcdn.example%2Fmovie_720p.mp4%3Fx%3D1";
            </script>
        "#;
        discover_serialized_web_media(html, &mut discovery).expect("discover media");
        assert_eq!(discovery.media_urls.len(), 2);
    }

    #[test]
    fn does_not_invent_full_hd_when_page_only_exposes_720p() {
        let mut urls = vec![
            "https://cdn.example/movie_720p.m3u8".to_string(),
            "https://cdn.example/movie_480p.mp4".to_string(),
        ];
        prioritize_full_hd_or_better(&mut urls);
        assert_eq!(urls[0], "https://cdn.example/movie_720p.m3u8");
    }
}
