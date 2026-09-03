use std::collections::HashSet;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use md5::{Digest, Md5};
use percent_encoding::percent_decode_str;
use regex::Regex;
use serde_json::{json, Value};
use url::{form_urlencoded, Url};

use crate::web_session::{ScopedWebSessionClient, SessionCookie};
use crate::ProviderClientError;

const DASH_ENDPOINT: &str = "https://cache.video.iqiyi.com/dash";
const VF_SUFFIX: &str = "ulc2h7tka0mdrf2lkb1n6m6mulc2htbn";
const DEFAULT_MANIFEST_BASE: &str = "https://cache-m.iq.com/dc/dt/";
const MAX_JSON_SCAN_NODES: usize = 4_000;
const MAX_MEDIA_CANDIDATES: usize = 24;

#[derive(Debug, Default)]
pub(super) struct DashDiscovery {
    pub media_urls: Vec<String>,
    pub drm_detected: bool,
}

#[derive(Debug, Default)]
struct PlaybackIds {
    tvid: Option<String>,
    vid: Option<String>,
}

pub(super) async fn discover_dash_media(
    session: &ScopedWebSessionClient,
    page_url: &Url,
    html: &str,
) -> Result<DashDiscovery, ProviderClientError> {
    let normalized = super::normalize_serialized_scan_text(html);
    let ids = extract_playback_ids(&normalized)?;
    let Some(tvid) = ids.tvid.as_deref() else {
        tracing::info!(
            target: "synctv_media_providers::iqiyi",
            stage = "dash_http_skipped",
            page_host = page_url.host_str().unwrap_or(""),
            reason = "missing_tvid",
            "Skipping pure HTTP iQiyi dash discovery because the page exposed no tvid"
        );
        return Ok(DashDiscovery::default());
    };

    let cookies = session.cookies();
    let request_url = build_dash_url(tvid, ids.vid.as_deref().unwrap_or(""), cookies)?;
    let started = Instant::now();
    tracing::info!(
        target: "synctv_media_providers::iqiyi",
        stage = "dash_http_request",
        page_host = page_url.host_str().unwrap_or(""),
        tvid_present = true,
        vid_present = ids.vid.is_some(),
        passport_cookie_present = cookie_value(cookies, "P00001").is_some(),
        device_cookie_present = cookie_value(cookies, "QC005").is_some(),
        dfp_cookie_present = cookie_value(cookies, "__dfp").is_some(),
        strategy = "signed_http_dash",
        "Querying iQiyi dash metadata without launching a browser"
    );

    let response = session.get_text(request_url.as_str()).await?;
    let parsed = parse_json_or_jsonp(&response)?;
    let mut discovery = extract_dash_discovery(&parsed);
    discovery.drm_detected |= response_has_drm_marker(&response);

    let videos = parsed
        .pointer("/data/program/video")
        .and_then(Value::as_array);
    let program_video_count = videos.map(Vec::len).unwrap_or_default();
    let highest_bid = videos
        .into_iter()
        .flatten()
        .filter_map(|video| value_as_u64(video.get("bid")))
        .max()
        .unwrap_or_default();
    let inline_manifest_count = videos
        .into_iter()
        .flatten()
        .filter(|video| {
            video
                .get("m3u8")
                .and_then(Value::as_str)
                .is_some_and(|value| value.contains("#EXTM3U"))
        })
        .count();
    let response_code = parsed.get("code").and_then(value_as_i64).unwrap_or_default();

    tracing::info!(
        target: "synctv_media_providers::iqiyi",
        stage = "dash_http_complete",
        page_host = page_url.host_str().unwrap_or(""),
        elapsed_ms = started.elapsed().as_millis(),
        response_bytes = response.len(),
        response_code,
        program_video_count,
        highest_bid,
        inline_manifest_count,
        candidate_count = discovery.media_urls.len(),
        drm_detected = discovery.drm_detected,
        strategy = "signed_http_dash",
        "Pure HTTP iQiyi dash discovery completed"
    );

    Ok(discovery)
}

fn extract_playback_ids(html: &str) -> Result<PlaybackIds, ProviderClientError> {
    let tvid_regex = Regex::new(
        r#"(?ix)(?:[\"']?tv_?id[\"']?|param\s*\[\s*[\"']tvid[\"']\s*\])\s*[:=]\s*[\"']?(\d{5,})"#,
    )
    .map_err(|error| ProviderClientError::Parse(error.to_string()))?;
    let vid_regex = Regex::new(
        r#"(?ix)(?:[\"']vid[\"']|param\s*\[\s*[\"']vid[\"']\s*\])\s*[:=]\s*[\"']([0-9a-z]{8,})"#,
    )
    .map_err(|error| ProviderClientError::Parse(error.to_string()))?;

    Ok(PlaybackIds {
        tvid: tvid_regex
            .captures(html)
            .and_then(|captures| captures.get(1))
            .map(|value| value.as_str().to_string()),
        vid: vid_regex
            .captures(html)
            .and_then(|captures| captures.get(1))
            .map(|value| value.as_str().to_string()),
    })
}

fn build_dash_url(
    tvid: &str,
    vid: &str,
    cookies: &[SessionCookie],
) -> Result<Url, ProviderClientError> {
    let tm = unix_millis();
    let tm_string = tm.to_string();
    let pck = cookie_value(cookies, "P00001").unwrap_or_default();
    let dfp = cookie_value(cookies, "__dfp")
        .and_then(|value| value.split('@').next())
        .unwrap_or_default();
    let k_uid = cookie_value(cookies, "QC005")
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| uuid::Uuid::new_v4().simple().to_string());
    let uid = extract_uid(cookies).unwrap_or_default();
    let auth_key = iqiyi_auth_key(tm, tvid);
    let bop = json!({"version": "10.0", "dfp": dfp, "b_ft1": 28}).to_string();
    let ut = if pck.is_empty() { "0" } else { "1" };

    let mut serializer = form_urlencoded::Serializer::new(String::new());
    for (key, value) in [
        ("tvid", tvid),
        ("bid", "800"),
        ("vid", vid),
        ("src", "01010031010000000000"),
        ("vt", "0"),
        ("rs", "1"),
        ("uid", uid.as_str()),
        ("ori", "pcw"),
        ("ps", "1"),
        ("k_uid", k_uid.as_str()),
        ("pt", "0"),
        ("d", "0"),
        ("s", ""),
        ("lid", "0"),
        ("cf", "0"),
        ("ct", "0"),
        ("authKey", auth_key.as_str()),
        ("k_tag", "1"),
        ("dfp", dfp),
        ("locale", "zh_cn"),
        ("pck", pck),
        ("k_err_retries", "0"),
        ("up", ""),
        ("qd_v", "a1"),
        ("tm", tm_string.as_str()),
    ] {
        serializer.append_pair(key, value);
    }
    serializer
        .append_pair("k_ft1", "706436220846084")
        .append_pair("k_ft4", "1162321298202628")
        .append_pair("k_ft5", "150994945")
        .append_pair("k_ft7", "4")
        .append_pair("fr_300", "120_120_120_120_120_120")
        .append_pair("fr_500", "120_120_120_120_120_120")
        .append_pair("fr_600", "120_120_120_120_120_120")
        .append_pair("fr_800", "120_120_120_120_120_120")
        .append_pair("fr_1020", "120_120_120_120_120_120")
        .append_pair("bop", &bop)
        .append_pair("sr", "1")
        .append_pair("ost", "0")
        .append_pair("ut", ut);

    let query = serializer.finish();
    let path_and_query = format!("/dash?{query}");
    let vf = iqiyi_vf(&path_and_query);
    Url::parse(&format!("{DASH_ENDPOINT}?{query}&vf={vf}"))
        .map_err(|error| ProviderClientError::Parse(error.to_string()))
}

fn iqiyi_auth_key(tm: u128, tvid: &str) -> String {
    let empty_md5 = md5_hex("");
    md5_hex(&format!("{empty_md5}{tm}{tvid}"))
}

fn iqiyi_vf(path_and_query: &str) -> String {
    md5_hex(&format!("{path_and_query}{VF_SUFFIX}"))
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

fn cookie_value<'a>(cookies: &'a [SessionCookie], name: &str) -> Option<&'a str> {
    cookies
        .iter()
        .rev()
        .find(|cookie| cookie.name.eq_ignore_ascii_case(name) && !cookie.value.is_empty())
        .map(|cookie| cookie.value.as_str())
}

fn extract_uid(cookies: &[SessionCookie]) -> Option<String> {
    ["P00002", "I00002"].into_iter().find_map(|name| {
        let raw = cookie_value(cookies, name)?;
        let decoded = percent_decode_str(raw).decode_utf8_lossy();
        let parsed = serde_json::from_str::<Value>(&decoded).ok()?;
        parsed
            .get("uid")
            .or_else(|| parsed.pointer("/data/uid"))
            .and_then(value_as_string)
    })
}

fn parse_json_or_jsonp(raw: &str) -> Result<Value, ProviderClientError> {
    let trimmed = raw.trim();
    if let Ok(value) = serde_json::from_str::<Value>(trimmed) {
        return Ok(value);
    }
    let start = trimmed.find('{').ok_or_else(|| {
        ProviderClientError::Parse("iQiyi dash response contained no JSON object".to_string())
    })?;
    let end = trimmed.rfind('}').ok_or_else(|| {
        ProviderClientError::Parse("iQiyi dash response contained incomplete JSON".to_string())
    })?;
    serde_json::from_str::<Value>(&trimmed[start..=end])
        .map_err(|error| ProviderClientError::Parse(error.to_string()))
}

fn extract_dash_discovery(root: &Value) -> DashDiscovery {
    let manifest_base = root
        .pointer("/data/dm3u8")
        .and_then(Value::as_str)
        .and_then(|value| Url::parse(value).ok())
        .or_else(|| Url::parse(DEFAULT_MANIFEST_BASE).ok());
    let mut media_urls = Vec::new();
    let mut seen = HashSet::new();

    let mut videos = root
        .pointer("/data/program/video")
        .and_then(Value::as_array)
        .map(|values| values.iter().collect::<Vec<_>>())
        .unwrap_or_default();
    videos.sort_by_key(|video| {
        std::cmp::Reverse(value_as_u64(video.get("bid")).unwrap_or_default())
    });

    for video in videos {
        for key in [
            "m3u8Url",
            "mpdUrl",
            "playUrl",
            "playbackUrl",
            "streamUrl",
            "videoUrl",
            "mediaUrl",
        ] {
            if let Some(raw) = video.get(key).and_then(Value::as_str) {
                push_resolved_url(
                    raw,
                    manifest_base.as_ref(),
                    true,
                    &mut seen,
                    &mut media_urls,
                );
            }
        }
        if let Some(manifest) = video.get("m3u8").and_then(Value::as_str) {
            extract_inline_master_urls(
                manifest,
                manifest_base.as_ref(),
                &mut seen,
                &mut media_urls,
            );
        }
    }

    let mut remaining = MAX_JSON_SCAN_NODES;
    scan_json_for_media(
        root,
        "",
        0,
        manifest_base.as_ref(),
        &mut remaining,
        &mut seen,
        &mut media_urls,
    );

    DashDiscovery {
        media_urls,
        drm_detected: false,
    }
}

fn scan_json_for_media(
    value: &Value,
    key_hint: &str,
    depth: usize,
    base: Option<&Url>,
    remaining: &mut usize,
    seen: &mut HashSet<String>,
    output: &mut Vec<String>,
) {
    if *remaining == 0 || depth > 8 || output.len() >= MAX_MEDIA_CANDIDATES {
        return;
    }
    *remaining = (*remaining).saturating_sub(1);
    match value {
        Value::String(raw) => {
            let trusted_key = matches_media_key(key_hint);
            push_resolved_url(raw, base, trusted_key, seen, output);
        }
        Value::Array(values) => {
            for child in values {
                scan_json_for_media(child, key_hint, depth + 1, base, remaining, seen, output);
                if *remaining == 0 || output.len() >= MAX_MEDIA_CANDIDATES {
                    break;
                }
            }
        }
        Value::Object(values) => {
            for (key, child) in values {
                scan_json_for_media(child, key, depth + 1, base, remaining, seen, output);
                if *remaining == 0 || output.len() >= MAX_MEDIA_CANDIDATES {
                    break;
                }
            }
        }
        _ => {}
    }
}

fn matches_media_key(key: &str) -> bool {
    matches!(
        key.to_ascii_lowercase().as_str(),
        "m3u8url"
            | "mpdurl"
            | "playurl"
            | "playbackurl"
            | "streamurl"
            | "videourl"
            | "mediaurl"
    )
}

fn push_resolved_url(
    raw: &str,
    base: Option<&Url>,
    trusted_key: bool,
    seen: &mut HashSet<String>,
    output: &mut Vec<String>,
) {
    let raw = raw.trim();
    if raw.is_empty() || raw.starts_with('#') || raw.contains('\n') || raw.len() > 8_192 {
        return;
    }
    let lower = raw.to_ascii_lowercase();
    let looks_media = lower.contains(".m3u8") || lower.contains(".mpd") || lower.contains(".mp4");
    if !trusted_key && !looks_media {
        return;
    }
    let parsed = Url::parse(raw)
        .ok()
        .or_else(|| base.and_then(|base| base.join(raw).ok()));
    let Some(url) = parsed else {
        return;
    };
    if !matches!(url.scheme(), "http" | "https") {
        return;
    }
    let normalized = url.to_string();
    if seen.insert(normalized.clone()) {
        output.push(normalized);
    }
}

fn extract_inline_master_urls(
    manifest: &str,
    base: Option<&Url>,
    seen: &mut HashSet<String>,
    output: &mut Vec<String>,
) {
    if !manifest.contains("#EXTM3U") {
        return;
    }
    let is_master = manifest.contains("#EXT-X-STREAM-INF");
    for line in manifest
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
    {
        let lower = line.to_ascii_lowercase();
        if is_master || lower.contains(".m3u8") || lower.contains(".mpd") {
            push_resolved_url(line, base, true, seen, output);
        }
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

fn value_as_u64(value: Option<&Value>) -> Option<u64> {
    match value? {
        Value::Number(number) => number.as_u64(),
        Value::String(value) => value.parse().ok(),
        _ => None,
    }
}

fn value_as_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Number(number) => number.as_i64(),
        Value::String(value) => value.parse().ok(),
        _ => None,
    }
}

fn value_as_string(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_common_domestic_playback_ids() {
        let html = r#"<script>window.Q = {"tvId": 123456789, "vid": "abcdef1234567890"};</script>"#;
        let ids = extract_playback_ids(html).expect("ids");
        assert_eq!(ids.tvid.as_deref(), Some("123456789"));
        assert_eq!(ids.vid.as_deref(), Some("abcdef1234567890"));
    }

    #[test]
    fn extracts_legacy_param_playback_ids() {
        let html = r#"param['tvid'] = "987654321"; param['vid'] = "a1b2c3d4e5f6";"#;
        let ids = extract_playback_ids(html).expect("ids");
        assert_eq!(ids.tvid.as_deref(), Some("987654321"));
        assert_eq!(ids.vid.as_deref(), Some("a1b2c3d4e5f6"));
    }

    #[test]
    fn vf_matches_known_cmd5x_compatible_vector() {
        assert_eq!(
            iqiyi_vf("/dash?tvid=123456&bid=800"),
            "3cdc09857ae92daa73fbd9fb584c501c"
        );
    }

    #[test]
    fn auth_key_matches_known_vector() {
        assert_eq!(
            iqiyi_auth_key(1_700_000_000_000, "123456789"),
            "3f9f0c4b4e2f995717d0546d9186498f"
        );
    }

    #[test]
    fn extracts_manifest_urls_in_quality_order() {
        let root = json!({
            "data": {
                "dm3u8": "https://cache-m.iq.com/dc/dt/",
                "program": {
                    "video": [
                        {"bid": 500, "m3u8Url": "720/master.m3u8"},
                        {"bid": 800, "m3u8Url": "2160/master.m3u8"}
                    ]
                }
            }
        });
        let discovery = extract_dash_discovery(&root);
        assert_eq!(
            discovery.media_urls.first().map(String::as_str),
            Some("https://cache-m.iq.com/dc/dt/2160/master.m3u8")
        );
    }

    #[test]
    fn inline_media_playlist_does_not_return_individual_segments() {
        let root = json!({
            "data": {
                "program": {
                    "video": [{
                        "bid": 600,
                        "m3u8": "#EXTM3U\n#EXTINF:5,\nhttps://data.video.iqiyi.com/videos/a.ts\n"
                    }]
                }
            }
        });
        let discovery = extract_dash_discovery(&root);
        assert!(discovery.media_urls.is_empty());
    }
}
