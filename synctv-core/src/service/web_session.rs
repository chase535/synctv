use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use synctv_media_providers::{
    web_session::SessionCookie, IqiyiClient, TencentVideoClient,
};

use crate::{
    models::{ProviderCredential, ProviderWebSessionCookie, UserId, UserProviderCredential},
    repository::UserProviderCredentialRepository,
    Error, Result,
};

pub const WEB_SESSION_SERVER_ID: &str = "web-session";
const MAX_WEB_SESSION_COOKIES: usize = 256;
const MAX_WEB_SESSION_COOKIE_BYTES: usize = 128 * 1024;
const MAX_WEB_SESSION_LABEL_BYTES: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WebSessionProvider {
    Iqiyi,
    TencentVideo,
}

impl WebSessionProvider {
    pub const ALL: [Self; 2] = [Self::Iqiyi, Self::TencentVideo];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Iqiyi => "iqiyi",
            Self::TencentVideo => "tencent_video",
        }
    }

    #[must_use]
    pub const fn default_label(self) -> &'static str {
        match self {
            Self::Iqiyi => "iQiyi",
            Self::TencentVideo => "Tencent Video",
        }
    }
}

#[derive(Debug, Clone)]
pub struct BindWebSessionRequest {
    pub user_id: UserId,
    pub provider: WebSessionProvider,
    pub label: String,
    pub cookies: Vec<ProviderWebSessionCookie>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WebSessionBinding {
    pub credential_id: i64,
    pub provider: WebSessionProvider,
    pub server_id: String,
    pub label: String,
    pub cookie_count: usize,
    pub expires_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct WebSessionCredentialService {
    credential_repo: Arc<UserProviderCredentialRepository>,
    http_client: reqwest::Client,
}

impl WebSessionCredentialService {
    #[must_use]
    pub fn new(
        credential_repo: Arc<UserProviderCredentialRepository>,
        http_client: reqwest::Client,
    ) -> Self {
        Self {
            credential_repo,
            http_client,
        }
    }

    fn normalize_label(provider: WebSessionProvider, label: &str) -> Result<String> {
        let trimmed = label.trim();
        let label = if trimmed.is_empty() {
            provider.default_label()
        } else {
            trimmed
        };
        if label.len() > MAX_WEB_SESSION_LABEL_BYTES {
            return Err(Error::InvalidInput(format!(
                "provider web-session label exceeds {MAX_WEB_SESSION_LABEL_BYTES} bytes"
            )));
        }
        Ok(label.to_string())
    }

    fn cookie_payload_bytes(cookies: &[ProviderWebSessionCookie]) -> usize {
        cookies.iter().fold(0usize, |total, cookie| {
            total
                .saturating_add(cookie.name.len())
                .saturating_add(cookie.value.len())
                .saturating_add(cookie.domain.len())
                .saturating_add(cookie.path.len())
        })
    }

    fn session_cookies(cookies: &[ProviderWebSessionCookie]) -> Vec<SessionCookie> {
        cookies
            .iter()
            .map(|cookie| SessionCookie {
                name: cookie.name.clone(),
                value: cookie.value.clone(),
                domain: cookie.domain.clone(),
                path: cookie.path.clone(),
                secure: cookie.secure,
                http_only: cookie.http_only,
                session_only: cookie.session_only,
                expires_at: cookie.expires_at,
            })
            .collect()
    }

    fn validate_cookies(
        &self,
        provider: WebSessionProvider,
        cookies: &[ProviderWebSessionCookie],
    ) -> Result<()> {
        if cookies.is_empty() {
            return Err(Error::InvalidInput(
                "provider web-session must contain at least one cookie".to_string(),
            ));
        }
        if cookies.len() > MAX_WEB_SESSION_COOKIES {
            return Err(Error::InvalidInput(format!(
                "provider web-session exceeds {MAX_WEB_SESSION_COOKIES} cookies"
            )));
        }
        if Self::cookie_payload_bytes(cookies) > MAX_WEB_SESSION_COOKIE_BYTES {
            return Err(Error::InvalidInput(format!(
                "provider web-session cookie payload exceeds {MAX_WEB_SESSION_COOKIE_BYTES} bytes"
            )));
        }

        let cookies = Self::session_cookies(cookies);
        let result = match provider {
            WebSessionProvider::Iqiyi => {
                IqiyiClient::new(self.http_client.clone(), cookies).map(|_| ())
            }
            WebSessionProvider::TencentVideo => {
                TencentVideoClient::new(self.http_client.clone(), cookies).map(|_| ())
            }
        };
        result.map_err(|error| {
            Error::InvalidInput(format!(
                "invalid {} web-session cookies: {error}",
                provider.as_str()
            ))
        })
    }

    fn binding_from_credential(
        provider: WebSessionProvider,
        credential: UserProviderCredential,
    ) -> Result<WebSessionBinding> {
        let ProviderCredential::WebSession { label, cookies } = credential.credential_data else {
            return Err(Error::InvalidInput(format!(
                "{} credential is not a web-session credential",
                provider.as_str()
            )));
        };
        if credential.provider != provider.as_str() || credential.server_id != WEB_SESSION_SERVER_ID {
            return Err(Error::InvalidInput(
                "provider web-session credential identity mismatch".to_string(),
            ));
        }

        Ok(WebSessionBinding {
            credential_id: credential.id,
            provider,
            server_id: credential.server_id,
            label,
            cookie_count: cookies.len(),
            expires_at: credential.expires_at,
            created_at: credential.created_at,
            updated_at: credential.updated_at,
        })
    }

    pub async fn bind(&self, request: BindWebSessionRequest) -> Result<WebSessionBinding> {
        self.validate_cookies(request.provider, &request.cookies)?;
        let label = Self::normalize_label(request.provider, &request.label)?;

        if let Some(existing) = self
            .credential_repo
            .get_by_provider_and_server(
                request.user_id,
                request.provider.as_str(),
                WEB_SESSION_SERVER_ID,
            )
            .await?
        {
            if !matches!(existing.credential_data, ProviderCredential::WebSession { .. }) {
                return Err(Error::Conflict(format!(
                    "{} credential slot already contains a different credential type",
                    request.provider.as_str()
                )));
            }
        }

        let now = crate::SystemClock.now();
        let credential = UserProviderCredential {
            id: 0,
            user_id: request.user_id,
            provider: request.provider.as_str().to_string(),
            server_id: WEB_SESSION_SERVER_ID.to_string(),
            provider_instance_name: None,
            credential_data: ProviderCredential::WebSession {
                label,
                cookies: request.cookies,
            },
            expires_at: None,
            created_at: now,
            updated_at: now,
        };
        let stored = self
            .credential_repo
            .upsert_by_user_provider_server(&credential)
            .await?;
        Self::binding_from_credential(request.provider, stored)
    }

    pub async fn list(&self, user_id: UserId) -> Result<Vec<WebSessionBinding>> {
        let mut bindings = Vec::with_capacity(WebSessionProvider::ALL.len());
        for provider in WebSessionProvider::ALL {
            let Some(credential) = self
                .credential_repo
                .get_by_provider_and_server(user_id, provider.as_str(), WEB_SESSION_SERVER_ID)
                .await?
            else {
                continue;
            };
            bindings.push(Self::binding_from_credential(provider, credential)?);
        }
        Ok(bindings)
    }

    pub async fn unbind(&self, user_id: UserId, provider: WebSessionProvider) -> Result<bool> {
        let Some(credential) = self
            .credential_repo
            .get_by_provider_and_server(user_id, provider.as_str(), WEB_SESSION_SERVER_ID)
            .await?
        else {
            return Ok(false);
        };

        if !matches!(credential.credential_data, ProviderCredential::WebSession { .. }) {
            return Err(Error::Conflict(format!(
                "{} credential slot contains a different credential type",
                provider.as_str()
            )));
        }
        self.credential_repo.delete(credential.id).await?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cookie(domain: &str) -> ProviderWebSessionCookie {
        ProviderWebSessionCookie {
            name: "session".to_string(),
            value: "secret".to_string(),
            domain: domain.to_string(),
            path: "/".to_string(),
            secure: true,
            http_only: true,
            session_only: true,
            expires_at: None,
        }
    }

    #[test]
    fn web_session_provider_names_match_source_provider_names() {
        assert_eq!(WebSessionProvider::Iqiyi.as_str(), "iqiyi");
        assert_eq!(WebSessionProvider::TencentVideo.as_str(), "tencent_video");
    }

    #[test]
    fn cookie_payload_accounting_includes_secret_values() {
        let cookies = vec![cookie("iqiyi.com")];
        assert_eq!(
            WebSessionCredentialService::cookie_payload_bytes(&cookies),
            "session".len() + "secret".len() + "iqiyi.com".len() + "/".len()
        );
    }

    #[test]
    fn label_is_trimmed_and_empty_label_uses_provider_default() {
        assert_eq!(
            WebSessionCredentialService::normalize_label(WebSessionProvider::Iqiyi, "  ")
                .expect("default label"),
            "iQiyi"
        );
        assert_eq!(
            WebSessionCredentialService::normalize_label(
                WebSessionProvider::TencentVideo,
                "  Family VIP  ",
            )
            .expect("trimmed label"),
            "Family VIP"
        );
    }

    #[test]
    fn provider_client_rejects_foreign_cookie_domains() {
        let client = reqwest::Client::new();
        let cookies = WebSessionCredentialService::session_cookies(&[cookie("example.com")]);
        assert!(IqiyiClient::new(client, cookies).is_err());
    }
}
