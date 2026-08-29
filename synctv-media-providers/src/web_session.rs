use std::time::{SystemTime, UNIX_EPOCH};

use reqwest::header::COOKIE;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::{check_response, text_with_limit, ProviderClientError};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SessionCookie {
    pub name: String,
    pub value: String,
    pub domain: String,
    pub path: String,
    pub secure: bool,
    pub http_only: bool,
    pub session_only: bool,
    pub expires_at: Option<i64>,
}

impl SessionCookie {
    fn normalized_domain(&self) -> String {
        normalize_domain(&self.domain)
    }

    fn is_expired(&self, now: i64) -> bool {
        self.expires_at.is_some_and(|expires_at| expires_at <= now)
    }

    fn applies_to(&self, url: &Url, now: i64) -> bool {
        if self.is_expired(now) || self.name.is_empty() {
            return false;
        }
        let Some(host) = url.host_str() else {
            return false;
        };
        let domain = self.normalized_domain();
        if domain.is_empty() || !domain_matches(host, &domain) {
            return false;
        }
        if self.secure && url.scheme() != "https" {
            return false;
        }
        let path = if self.path.is_empty() { "/" } else { &self.path };
        cookie_path_matches(url.path(), path)
    }
}

#[derive(Clone)]
pub struct ScopedWebSessionClient {
    client: reqwest::Client,
    allowed_domains: &'static [&'static str],
    cookies: Vec<SessionCookie>,
}

impl ScopedWebSessionClient {
    pub fn new(
        client: reqwest::Client,
        allowed_domains: &'static [&'static str],
        cookies: Vec<SessionCookie>,
    ) -> Result<Self, ProviderClientError> {
        if allowed_domains.is_empty() {
            return Err(ProviderClientError::InvalidConfig(
                "web session must define at least one allowed domain".to_string(),
            ));
        }

        for cookie in &cookies {
            validate_cookie(cookie, allowed_domains)?;
        }

        Ok(Self {
            client,
            allowed_domains,
            cookies,
        })
    }

    #[must_use]
    pub fn cookies(&self) -> &[SessionCookie] {
        &self.cookies
    }

    pub fn validate_url(&self, raw_url: &str) -> Result<Url, ProviderClientError> {
        let url = Url::parse(raw_url)
            .map_err(|error| ProviderClientError::InvalidConfig(error.to_string()))?;
        if !matches!(url.scheme(), "https" | "http") {
            return Err(ProviderClientError::InvalidConfig(
                "provider web session only supports HTTP(S) URLs".to_string(),
            ));
        }
        let host = url.host_str().ok_or_else(|| {
            ProviderClientError::InvalidConfig("provider URL has no host".to_string())
        })?;
        if !self
            .allowed_domains
            .iter()
            .any(|allowed| domain_matches(host, allowed))
        {
            return Err(ProviderClientError::InvalidConfig(format!(
                "provider URL host is outside the session allowlist: {host}"
            )));
        }
        Ok(url)
    }

    pub async fn get_text(&self, raw_url: &str) -> Result<String, ProviderClientError> {
        let url = self.validate_url(raw_url)?;
        let mut request = self.client.get(url.clone());
        let cookie_header = self.cookie_header(&url);
        if !cookie_header.is_empty() {
            request = request.header(COOKIE, cookie_header);
        }
        let response = check_response(request.send().await?).await?;
        text_with_limit(response).await
    }

    fn cookie_header(&self, url: &Url) -> String {
        let now = unix_timestamp_now();
        self.cookies
            .iter()
            .filter(|cookie| cookie.applies_to(url, now))
            .map(|cookie| format!("{}={}", cookie.name, cookie.value))
            .collect::<Vec<_>>()
            .join("; ")
    }
}

fn validate_cookie(
    cookie: &SessionCookie,
    allowed_domains: &[&str],
) -> Result<(), ProviderClientError> {
    if cookie.name.trim().is_empty()
        || cookie
            .name
            .chars()
            .any(|ch| ch.is_control() || matches!(ch, ';' | '='))
    {
        return Err(ProviderClientError::InvalidConfig(
            "provider session contains an invalid cookie name".to_string(),
        ));
    }
    if cookie
        .value
        .chars()
        .any(|ch| ch.is_control() || ch == ';')
    {
        return Err(ProviderClientError::InvalidConfig(
            "provider session contains an invalid cookie value".to_string(),
        ));
    }
    if !cookie.path.is_empty() && !cookie.path.starts_with('/') {
        return Err(ProviderClientError::InvalidConfig(
            "provider session contains an invalid cookie path".to_string(),
        ));
    }
    let domain = normalize_domain(&cookie.domain);
    if domain.is_empty()
        || !allowed_domains
            .iter()
            .any(|allowed| domain_matches(&domain, allowed))
    {
        return Err(ProviderClientError::InvalidConfig(format!(
            "provider session cookie domain is outside the allowlist: {}",
            cookie.domain
        )));
    }
    Ok(())
}

fn normalize_domain(value: &str) -> String {
    value.trim().trim_start_matches('.').to_ascii_lowercase()
}

fn domain_matches(host: &str, allowed_domain: &str) -> bool {
    let host = normalize_domain(host);
    let allowed = normalize_domain(allowed_domain);
    !host.is_empty()
        && !allowed.is_empty()
        && (host == allowed || host.ends_with(&format!(".{allowed}")))
}

fn cookie_path_matches(request_path: &str, cookie_path: &str) -> bool {
    if cookie_path == "/" || request_path == cookie_path {
        return true;
    }
    request_path.starts_with(cookie_path)
        && (cookie_path.ends_with('/')
            || request_path
                .as_bytes()
                .get(cookie_path.len())
                .is_some_and(|byte| *byte == b'/'))
}

fn unix_timestamp_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_secs()).ok())
        .unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cookie(domain: &str, path: &str, secure: bool) -> SessionCookie {
        SessionCookie {
            name: "session".to_string(),
            value: "secret".to_string(),
            domain: domain.to_string(),
            path: path.to_string(),
            secure,
            http_only: true,
            session_only: true,
            expires_at: None,
        }
    }

    #[test]
    fn rejects_cookie_domains_outside_provider_allowlist() {
        let client = reqwest::Client::new();
        let result = ScopedWebSessionClient::new(
            client,
            &["iqiyi.com"],
            vec![cookie("example.com", "/", true)],
        );
        assert!(result.is_err());
    }

    #[test]
    fn rejects_request_urls_outside_provider_allowlist() {
        let client = ScopedWebSessionClient::new(
            reqwest::Client::new(),
            &["iqiyi.com"],
            vec![cookie(".iqiyi.com", "/", true)],
        )
        .expect("session client");
        assert!(client.validate_url("https://example.com/video").is_err());
        assert!(client
            .validate_url("https://www.iqiyi.com/video")
            .is_ok());
    }

    #[test]
    fn cookie_header_respects_domain_path_and_secure_flags() {
        let client = ScopedWebSessionClient::new(
            reqwest::Client::new(),
            &["qq.com"],
            vec![
                cookie("qq.com", "/", true),
                SessionCookie {
                    name: "video".to_string(),
                    value: "value".to_string(),
                    domain: "v.qq.com".to_string(),
                    path: "/x".to_string(),
                    secure: false,
                    http_only: false,
                    session_only: true,
                    expires_at: None,
                },
            ],
        )
        .expect("session client");

        let https = Url::parse("https://v.qq.com/x/cover").expect("url");
        assert_eq!(client.cookie_header(&https), "session=secret; video=value");

        let http = Url::parse("http://v.qq.com/x/cover").expect("url");
        assert_eq!(client.cookie_header(&http), "video=value");

        let other_path = Url::parse("https://v.qq.com/xyz/cover").expect("url");
        assert_eq!(client.cookie_header(&other_path), "session=secret");
    }

    #[test]
    fn rejects_cookie_header_injection() {
        let result = ScopedWebSessionClient::new(
            reqwest::Client::new(),
            &["iqiyi.com"],
            vec![SessionCookie {
                name: "session".to_string(),
                value: "safe; injected=value".to_string(),
                domain: "iqiyi.com".to_string(),
                path: "/".to_string(),
                secure: true,
                http_only: true,
                session_only: true,
                expires_at: None,
            }],
        );
        assert!(result.is_err());
    }
}
