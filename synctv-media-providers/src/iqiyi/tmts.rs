use std::collections::HashSet;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use md5::{Digest, Md5};
use regex::Regex;
use serde_json::Value;
use url::Url;

use crate::web_session::ScopedWebSessionClient;
use crate::ProviderClientError;

const RESOLVER_TIMEOUT: Duration = Duration::from_secs(8);
const TMTS_KEY: &str = "d5fb4bd9d50c4be6948c97edd7254b0e";
const TMTS_SRC: &str = "76f90cbd92f94a2e925d83e8ccd22cb7";
const MAX_JSON_SCAN_NODES: usize = 2_048;
const MAX_STREAMS: usize = 16;

#[derive(Debug, Default)]
pub(super) struct TmtsDiscovery {
    pub media_urls: Vec<String>,
    pub drm_detected: bool,
}

#[derive(Debug, Default)]
struct PlaybackIds {
    tvid: Option<String>,
    vid: Option<String>,
    tvid_source: &'static str,
    vid_source: &'static str,
}

#[derive(Debug)]
struct TmtsStream {
    quality_rank: u8,
    quality_id: String,
    url: String,
}

pub(super) async fn discover_tmts_media(
    session: &ScopedWebSessionClient,
    page_url: &Url,
    html: &str,
) -> Result<TmtsDiscovery, ProviderClientError> {
    match tokio::time::timeout(
        RESOLVER_TIMEOUT,
        discover_tmts_media_inner(session, page_url, html),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => Err(ProviderClientError::Network(format!(
            "iQiyi TMTS resolver timed out after {}s",
            RESOLVER_TIMEOUT.as_secs()
        ))),
    }
}

async fn discover_tmts_media_inner(
    session: &ScopedWebSessionClient,
    page_url: &Url,
    html: &str,
) -> Result<TmtsDiscovery, ProviderClientError> {
    let started = Instant::now();
    let normalized = super::normalize_serialized_scan_text(html);
    let mut ids = extract_page_ids(&normalized)?;

    if ids.tvid.is_none() {
        if let Some(slug) = page_video_slug(page_url) {
            if let Ok(Some(tvid)) = decode_tvid(session, slug).await {
                ids.tvid = Some(tvid);
                ids.tvid_source = "decode_api";
            }
        }
    }

    let Some(tvid) = ids.tvid.clone() else {
        tracing::info!(
            target: "synctv_media_providers::iqiyi",
            stage = "tmts_http_skipped",
            page_host = page_url.host_str().unwrap_or(""),
            reason = "missing_tvid",
            elapsed_ms = started.elapsed().as_millis(),
            "Skipping iQiyi TMTS discovery because no tvid could be resolved"
        );
        return Ok(TmtsDiscovery::default());
    };

    if ids.vid.is_none() {
        if let Ok(Some(vid)) = fetch_baseinfo_vid(session, &tvid).await {
            ids.vid = Some(vid);
            ids.vid_source = "baseinfo_api";
        }
    }

    let Some(vid) = ids.vid.as_deref() else {
        tracing::info!(
            target: "synctv_media_providers::iqiyi",
            stage = "tmts_http_skipped",
            page_host = page_url.host_str().unwrap_or(""),
            reason = "missing_vid",
            tvid_source = ids.tvid_source,
            elapsed_ms = started.elapsed().as_millis(),
            "Skipping iQiyi TMTS discovery because no vid could be resolved"
        );
        return Ok(TmtsDiscovery::default());
    };

    tracing::info!(
        target: "synctv_media_providers::iqiyi",
        stage = "tmts_http_request",
        page_host = page_url.host_str().unwrap_or(""),
        tvid_source = ids.tvid_source,
        vid_source = ids.vid_source,
        cookie_count = session.cookies().len(),
        strategy = "official_http_tmts",
        "Querying iQiyi TMTS without launching a browser"
    );

    let https_url = build_tmts_url("https", &tvid, vid)?;
    let (response, transport) = match session.get_text(https_url.as_str()).await {
        Ok(response) => (response, "https"),
        Err(https_error) => {
            let http_url = build_tmts_url("http", &tvid, vid)?;
            match session.get_text(http_url.as_str()).await {
                Ok(response) => (response, "http_fallback"),
                Err(_) => return Err(https_error),
            }
        }
    };

    let root = parse_tmts_response(&response)?;
    let response_code = root
        .get("code")
        .and_then(value_as_string)
        .unwrap_or_default();
    let drm_detected = response_has_drm_marker(&response);
    let mut streams = extract_streams(&root);
    streams.sort_by_key(|stream| std::cmp::Reverse(stream.quality_rank));

    let highest_vd = streams
        .first()
        .map(|stream| stream.quality_id.clone())
        .unwrap_or_default();
    let mut seen = HashSet::new();
    let media_urls = streams
        .into_iter()
        .filter_map(|stream| seen.insert(stream.url.clone()).then_some(stream.url))
        .take(MAX_STREAMS)
        .collect::<Vec<_>>();

    tracing::info!(
        target: "synctv_media_providers::iqiyi",
        stage = "tmts_http_complete",
        page_host = page_url.host_str().unwrap_or(""),
        elapsed_ms = started.elapsed().as_millis(),
        response_bytes = response.len(),
        response_code = %response_code,
        transport,
        stream_count = media_urls.len(),
        highest_vd = %highest_vd,
        drm_detected,
        strategy = "official_http_tmts",
        "Pure HTTP iQiyi TMTS discovery completed"
    );

    if response_code != "A00000" {
        return Ok(TmtsDiscovery {
            media_urls: Vec::new(),
            drm_detected,
        });
    }

    Ok(TmtsDiscovery {
        media_urls,
        drm_detected,
    })
}

fn extract_page_ids(html: &str) -> Result<PlaybackIds, ProviderClientError> {
    let tvid_patterns = [
        r#"(?i)data-(?:player|shareplattrigger)-tvid\s*=\s*[\"'](\d+)[\"']"#,
        r#"(?i)[\"'](?:tvid|tv_id|tvId)[\"']\s*[:=]\s*[\"']?(\d{5,})"#,
        r#"(?i)\b(?:tvid|tv_id|tvId)\s*[:=]\s*[\"']?(\d{5,})"#,
    ];
    let vid_patterns = [
        r#"(?i)data-(?:player|shareplattrigger)-videoid\s*=\s*[\"']([a-f0-9]+)[\"']"#,
        r#"(?i)[\"'](?:vid|video_id|videoId)[\"']\s*[:=]\s*[\"']([a-f0-9]{16,64})[\"']"#,
    ];

    Ok(PlaybackIds {
        tvid: first_capture(html, &tvid_patterns)?,
        vid: first_capture(html, &vid_patterns)?.filter(|value| looks_like_vid(value)),
        tvid_source: "html",
        vid_source: "html",
    })
}

fn first_capture(text: &str, patterns: &[&str]) -> Result<Option<String>, ProviderClientError> {
    for pattern in patterns {
        let regex = Regex::new(pattern)
            .map_err(|error| ProviderClientError::Parse(error.to_string()))?;
        if let Some(value) = regex
            .captures(text)
            .and_then(|captures| captures.get(1))
            .map(|value| value.as_str().to_string())
        {
            return Ok(Some(value));
        }
    }
    Ok(None)
}

fn page_video_slug(page_url: &Url) -> Option<&str> {
    let filename = page_url.path_segments()?.next_back()?;
    filename.strip_prefix("v_")?.strip_suffix(".html")
}

async fn decode_tvid(
    session: &ScopedWebSessionClient,
    slug: &str,
) -> Result<Option<String>, ProviderClientError> {
    if slug.is_empty() || !slug.chars().all(|ch| ch.is_ascii_alphanumeric()) {
        return Ok(None);
    }
    let url = format!(
        "https://pcw-api.iq.com/api/decode/{slug}?platformId=3&modeCode=intl&langCode=sg"
    );
    let response = session.get_text(&url).await?;
    let root: Value = serde_json::from_str(&response)
        .map_err(|error| ProviderClientError::Parse(error.to_string()))?;
    Ok(root.get("data").and_then(value_as_string).filter(|value| {
        !value.is_empty() && value.chars().all(|ch| ch.is_ascii_digit())
    }))
}

async fn fetch_baseinfo_vid(
    session: &ScopedWebSessionClient,
    tvid: &str,
) -> Result<Option<String>, ProviderClientError> {
    let url = format!("https://pcw-api.iqiyi.com/video/video/baseinfo/{tvid}");
    let response = session.get_text(&url).await?;
    let root: Value = serde_json::from_str(&response)
        .map_err(|error| ProviderClientError::Parse(error.to_string()))?;
    let mut remaining = MAX_JSON_SCAN_NODES;
    Ok(find_vid_in_json(&root, 0, &mut remaining))
}

fn find_vid_in_json(value: &Value, depth: usize, remaining: &mut usize) -> Option<String> {
    if *remaining == 0 || depth > 8 {
        return None;
    }
    *remaining = (*remaining).saturating_sub(1);
    match value {
        Value::Object(object) => {
            for (key, child) in object {
                if matches!(key.to_ascii_lowercase().as_str(), "vid" | "videoid" | "video_id") {
                    if let Some(candidate) =
                        value_as_string(child).filter(|value| looks_like_vid(value))
                    {
                        return Some(candidate);
                    }
                }
                if let Some(candidate) = find_vid_in_json(child, depth + 1, remaining) {
                    return Some(candidate);
                }
            }
            None
        }
        Value::Array(values) => values
            .iter()
            .find_map(|child| find_vid_in_json(child, depth + 1, remaining)),
        _ => None,
    }
}

fn looks_like_vid(value: &str) -> bool {
    (16..=64).contains(&value.len()) && value.chars().all(|ch| ch.is_ascii_hexdigit())
}

fn build_tmts_url(scheme: &str, tvid: &str, vid: &str) -> Result<Url, ProviderClientError> {
    let timestamp = unix_millis();
    let timestamp_text = timestamp.to_string();
    let sc = md5_hex(&format!("{timestamp}{TMTS_KEY}{tvid}"));
    let mut url = Url::parse(&format!(
        "{scheme}://cache.m.iqiyi.com/jp/tmts/{tvid}/{vid}/"
    ))
    .map_err(|error| ProviderClientError::Parse(error.to_string()))?;
    url.query_pairs_mut()
        .append_pair("tvid", tvid)
        .append_pair("vid", vid)
        .append_pair("src", TMTS_SRC)
        .append_pair("sc", &sc)
        .append_pair("t", &timestamp_text);
    Ok(url)
}

fn parse_tmts_response(raw: &str) -> Result<Value, ProviderClientError> {
    let trimmed = raw.trim();
    let json = trimmed
        .strip_prefix("var tvInfoJs=")
        .unwrap_or(trimmed)
        .trim()
        .trim_end_matches(';')
        .trim();
    if let Ok(value) = serde_json::from_str::<Value>(json) {
        return Ok(value);
    }
    let start = json.find('{').ok_or_else(|| {
        ProviderClientError::Parse("iQiyi TMTS response contained no JSON object".to_string())
    })?;
    let end = json.rfind('}').ok_or_else(|| {
        ProviderClientError::Parse("iQiyi TMTS response contained incomplete JSON".to_string())
    })?;
    serde_json::from_str(&json[start..=end])
        .map_err(|error| ProviderClientError::Parse(error.to_string()))
}

fn extract_streams(root: &Value) -> Vec<TmtsStream> {
    root.pointer("/data/vidl")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|stream| {
            let raw_url = stream
                .get("m3utx")
                .or_else(|| stream.get("m3u"))
                .and_then(Value::as_str)?;
            let url = Url::parse(raw_url).ok()?;
            if !matches!(url.scheme(), "http" | "https") {
                return None;
            }
            let quality_id = stream
                .get("vd")
                .and_then(value_as_string)
                .unwrap_or_default();
            Some(TmtsStream {
                quality_rank: quality_rank(&quality_id),
                quality_id,
                url: url.to_string(),
            })
        })
        .collect()
}

fn quality_rank(vd: &str) -> u8 {
    match vd {
        "96" => 1,
        "1" => 2,
        "2" => 3,
        "21" => 4,
        "4" | "17" => 5,
        "5" => 6,
        "18" => 7,
        _ => 0,
    }
}

fn response_has_drm_marker(response: &str) -> bool {
    let lower = response.to_ascii_lowercase();
    [
        "widevine",
        "playready",
        "com.widevine.alpha",
        "license_url",
        "licenseurl",
        "drmlicense",
    ]
    .iter()
    .any(|marker| lower.contains(marker))
}

fn value_as_string(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn md5_hex(value: &str) -> String {
    hex::encode(Md5::digest(value.as_bytes()))
}

fn unix_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_ids_from_standard_player_attributes() {
        let html = r#"<div data-player-tvid="123456789" data-player-videoid="abcdef1234567890abcdef1234567890"></div>"#;
        let ids = extract_page_ids(html).expect("ids");
        assert_eq!(ids.tvid.as_deref(), Some("123456789"));
        assert_eq!(
            ids.vid.as_deref(),
            Some("abcdef1234567890abcdef1234567890")
        );
    }

    #[test]
    fn extracts_generic_embedded_tvid() {
        let html = r#"<script>window.__DATA__={"tvId":2176777301438100};</script>"#;
        let ids = extract_page_ids(html).expect("ids");
        assert_eq!(ids.tvid.as_deref(), Some("2176777301438100"));
        assert!(ids.vid.is_none());
    }

    #[test]
    fn extracts_page_slug_only_from_video_pages() {
        let video = Url::parse("https://www.iqiyi.com/v_lmzehv13aw.html").expect("url");
        let other = Url::parse("https://www.iqiyi.com/a_123.html").expect("url");
        assert_eq!(page_video_slug(&video), Some("lmzehv13aw"));
        assert_eq!(page_video_slug(&other), None);
    }

    #[test]
    fn parses_tmts_streams_in_known_quality_order() {
        let root = serde_json::json!({
            "code": "A00000",
            "data": {
                "vidl": [
                    {"vd": 4, "m3utx": "https://cdn.example/720.m3u8"},
                    {"vd": 18, "m3utx": "https://cdn.example/1080.m3u8"},
                    {"vd": 2, "m3utx": "https://cdn.example/480.m3u8"}
                ]
            }
        });
        let mut streams = extract_streams(&root);
        streams.sort_by_key(|stream| std::cmp::Reverse(stream.quality_rank));
        assert_eq!(streams[0].quality_id, "18");
        assert_eq!(streams[1].quality_id, "4");
    }

    #[test]
    fn tmts_signature_matches_known_vector() {
        assert_eq!(
            md5_hex("1700000000000d5fb4bd9d50c4be6948c97edd7254b0e123456789"),
            "40a701d8394f2ecf4cf38a8b9860ea86"
        );
    }
}
