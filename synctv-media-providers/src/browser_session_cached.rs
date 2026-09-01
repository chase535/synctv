use std::sync::LazyLock;
use std::time::{Duration, Instant};

use moka::future::Cache;
use sha2::{Digest, Sha256};

#[path = "browser_session.rs"]
mod implementation;

pub use implementation::{BrowserPageDiagnostics, BrowserPageObservation};

use crate::web_session::SessionCookie;
use crate::ProviderClientError;

const BROWSER_OBSERVATION_CACHE_CAPACITY: u64 = 128;
const BROWSER_OBSERVATION_CACHE_TTL: Duration = Duration::from_secs(10);

static BROWSER_OBSERVATION_CACHE: LazyLock<Cache<String, BrowserPageObservation>> =
    LazyLock::new(|| {
        Cache::builder()
            .max_capacity(BROWSER_OBSERVATION_CACHE_CAPACITY)
            .time_to_live(BROWSER_OBSERVATION_CACHE_TTL)
            .build()
    });

/// Render a provider page while coalescing duplicate authenticated browser work.
///
/// Playback startup may validate the same source through preflight, duration
/// probing, and data-plane generation within a very small time window. Starting
/// a Chromium process for each caller creates avoidable CPU/memory pressure and
/// can make the outer HTTP request exceed its deadline. The cache key includes
/// the full page URL, domain allowlist, and a SHA-256 digest of the complete
/// cookie state, so observations are never reused across different authenticated
/// sessions. Neither cookie values nor the resulting digest are logged.
///
/// Moka's `try_get_with` coalesces concurrent misses for the same key. Failed
/// renders are not inserted, so a transient Chromium/CDP failure does not poison
/// subsequent attempts.
pub async fn render_web_page_playback(
    raw_url: &str,
    allowed_domains: &'static [&'static str],
    cookies: &[SessionCookie],
) -> Result<BrowserPageObservation, ProviderClientError> {
    let cache_key = browser_observation_cache_key(raw_url, allowed_domains, cookies);
    let page_host = page_host(raw_url);

    if let Some(observation) = BROWSER_OBSERVATION_CACHE.get(&cache_key).await {
        tracing::info!(
            target: "synctv_media_providers::browser_session",
            stage = "cache_hit",
            page_host = %page_host,
            "Authenticated browser page render diagnostics"
        );
        return Ok(observation);
    }

    let raw_url = raw_url.to_string();
    let cookies = cookies.to_vec();
    BROWSER_OBSERVATION_CACHE
        .try_get_with(cache_key, async move {
            let started_at = Instant::now();
            tracing::info!(
                target: "synctv_media_providers::browser_session",
                stage = "cache_miss_leader",
                page_host = %page_host,
                "Authenticated browser page render diagnostics"
            );
            let result =
                implementation::render_web_page_playback(&raw_url, allowed_domains, &cookies).await;
            tracing::info!(
                target: "synctv_media_providers::browser_session",
                stage = "render_complete",
                page_host = %page_host,
                success = result.is_ok(),
                elapsed_ms = started_at.elapsed().as_millis(),
                "Authenticated browser page render diagnostics"
            );
            result.map_err(|error| error.to_string())
        })
        .await
        .map_err(|error| ProviderClientError::Network(error.as_ref().clone()))
}

fn page_host(raw_url: &str) -> String {
    url::Url::parse(raw_url)
        .ok()
        .and_then(|url| url.host_str().map(str::to_string))
        .unwrap_or_default()
}

fn browser_observation_cache_key(
    raw_url: &str,
    allowed_domains: &[&str],
    cookies: &[SessionCookie],
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(raw_url.as_bytes());
    hasher.update([0]);

    let mut domains = allowed_domains.to_vec();
    domains.sort_unstable();
    for domain in domains {
        hasher.update(domain.as_bytes());
        hasher.update([0]);
    }

    let mut ordered_cookies = cookies.iter().collect::<Vec<_>>();
    ordered_cookies.sort_by(|left, right| {
        left.domain
            .cmp(&right.domain)
            .then_with(|| left.path.cmp(&right.path))
            .then_with(|| left.name.cmp(&right.name))
            .then_with(|| left.value.cmp(&right.value))
    });

    for cookie in ordered_cookies {
        hasher.update(cookie.domain.as_bytes());
        hasher.update([0]);
        hasher.update(cookie.path.as_bytes());
        hasher.update([0]);
        hasher.update(cookie.name.as_bytes());
        hasher.update([0]);
        hasher.update(cookie.value.as_bytes());
        hasher.update([0]);
        hasher.update([
            u8::from(cookie.secure),
            u8::from(cookie.http_only),
            u8::from(cookie.session_only),
        ]);
        hasher.update(cookie.expires_at.unwrap_or_default().to_le_bytes());
    }

    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session_cookie(name: &str, value: &str) -> SessionCookie {
        SessionCookie {
            name: name.to_string(),
            value: value.to_string(),
            domain: ".iqiyi.com".to_string(),
            path: "/".to_string(),
            secure: true,
            http_only: true,
            session_only: false,
            expires_at: Some(2_000_000_000),
        }
    }

    #[test]
    fn browser_cache_key_is_cookie_scoped_and_order_independent() {
        let first = session_cookie("P00001", "alpha");
        let second = session_cookie("QC005", "beta");
        let forward = browser_observation_cache_key(
            "https://www.iqiyi.com/v_demo.html",
            &["iqiyi.com", "qiyi.com"],
            &[first.clone(), second.clone()],
        );
        let reverse = browser_observation_cache_key(
            "https://www.iqiyi.com/v_demo.html",
            &["qiyi.com", "iqiyi.com"],
            &[second.clone(), first.clone()],
        );
        assert_eq!(forward, reverse);

        let changed = session_cookie("P00001", "different");
        let changed_key = browser_observation_cache_key(
            "https://www.iqiyi.com/v_demo.html",
            &["iqiyi.com", "qiyi.com"],
            &[changed, second],
        );
        assert_ne!(forward, changed_key);
    }

    #[test]
    fn browser_diagnostics_host_omits_path_and_query() {
        assert_eq!(
            page_host("https://www.iqiyi.com/v_demo.html?token=secret"),
            "www.iqiyi.com"
        );
    }
}
