use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::{Digest, Sha256};
use synctv_media_providers::{web_session::SessionCookie, IqiyiClient, TencentVideoClient};

use crate::{
    models::{
        ProviderCredential, ProviderPlaybackSession, ProviderWebSessionCookie, RoomId, UserId,
        UserProviderCredential, WebSessionPlaybackSession,
    },
    provider::{
        credential_resolver::credential_revision, ProviderStore, ProviderStoreExt, StoreError,
    },
    repository::{
        NewProviderPlaybackSession, ProviderPlaybackSessionRepository,
        UserProviderCredentialRepository,
    },
    Error, Result,
};

pub const WEB_SESSION_SERVER_ID: &str = "web-session";
const MAX_WEB_SESSION_COOKIES: usize = 256;
const MAX_WEB_SESSION_COOKIE_BYTES: usize = 128 * 1024;
const MAX_WEB_SESSION_LABEL_BYTES: usize = 128;
const WEB_PLAYBACK_LOCK_TTL: Duration = Duration::from_secs(45);
const WEB_PLAYBACK_WAIT_STEP: Duration = Duration::from_millis(100);
const WEB_PLAYBACK_WAIT_STEPS: usize = 300;
const MAX_WEB_PLAYBACK_CACHE_TTL: Duration = Duration::from_hours(6);

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

/// Server-internal authenticated browser session.
///
/// This type intentionally does not implement `Serialize` or `Debug`: cookie
/// values must never cross an API boundary or be emitted by structured logs.
#[derive(Clone)]
pub(crate) struct WebSessionAccess {
    pub provider: WebSessionProvider,
    pub server_id: String,
    pub credential_owner_id: UserId,
    pub credential_revision: String,
    pub cookies: Vec<SessionCookie>,
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

fn validate_web_session_cookies(
    http_client: &reqwest::Client,
    provider: WebSessionProvider,
    cookies: &[ProviderWebSessionCookie],
) -> Result<Vec<SessionCookie>> {
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
    if cookie_payload_bytes(cookies) > MAX_WEB_SESSION_COOKIE_BYTES {
        return Err(Error::InvalidInput(format!(
            "provider web-session cookie payload exceeds {MAX_WEB_SESSION_COOKIE_BYTES} bytes"
        )));
    }

    let cookies = session_cookies(cookies);
    let result = match provider {
        WebSessionProvider::Iqiyi => {
            IqiyiClient::new(http_client.clone(), cookies.clone()).map(|_| ())
        }
        WebSessionProvider::TencentVideo => {
            TencentVideoClient::new(http_client.clone(), cookies.clone()).map(|_| ())
        }
    };
    result.map_err(|error| {
        Error::InvalidInput(format!(
            "invalid {} web-session cookies: {error}",
            provider.as_str()
        ))
    })?;
    Ok(cookies)
}

pub(crate) async fn load_web_session_access(
    credential_repo: &UserProviderCredentialRepository,
    http_client: &reqwest::Client,
    user_id: UserId,
    provider: WebSessionProvider,
) -> Result<WebSessionAccess> {
    let credential = credential_repo
        .get_by_provider_and_server(user_id, provider.as_str(), WEB_SESSION_SERVER_ID)
        .await?
        .ok_or_else(|| {
            Error::InvalidInput(format!(
                "{} web-session login is required",
                provider.as_str()
            ))
        })?;
    if credential.is_expired() {
        return Err(Error::InvalidInput(format!(
            "{} web-session credential has expired",
            provider.as_str()
        )));
    }
    if credential.provider != provider.as_str() || credential.server_id != WEB_SESSION_SERVER_ID {
        return Err(Error::InvalidInput(
            "provider web-session credential identity mismatch".to_string(),
        ));
    }

    let revision = credential_revision(credential.id, credential.updated_at);
    let ProviderCredential::WebSession { cookies, .. } = credential.credential_data else {
        return Err(Error::InvalidInput(format!(
            "{} credential is not a web-session credential",
            provider.as_str()
        )));
    };
    let cookies = validate_web_session_cookies(http_client, provider, &cookies)?;

    Ok(WebSessionAccess {
        provider,
        server_id: credential.server_id,
        credential_owner_id: user_id,
        credential_revision: revision,
        cookies,
    })
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
        if credential.provider != provider.as_str() || credential.server_id != WEB_SESSION_SERVER_ID
        {
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
        validate_web_session_cookies(&self.http_client, request.provider, &request.cookies)?;
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
            if !matches!(
                existing.credential_data,
                ProviderCredential::WebSession { .. }
            ) {
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

        if !matches!(
            credential.credential_data,
            ProviderCredential::WebSession { .. }
        ) {
            return Err(Error::Conflict(format!(
                "{} credential slot contains a different credential type",
                provider.as_str()
            )));
        }
        self.credential_repo.delete(credential.id).await?;
        Ok(true)
    }
}

/// Room identity used to partition one authenticated upstream playback parse.
#[derive(Debug, Clone)]
pub(crate) struct WebSessionPlaybackRequest {
    pub room_id: RoomId,
    pub playback_generation: i64,
    pub provider: WebSessionProvider,
    pub credential_owner_id: UserId,
    pub resource_key: String,
    pub resource_version: Option<String>,
    pub paused: bool,
}

/// Provider resolver output plus the maximum time its public playback result may be reused.
pub(crate) struct WebSessionResolvedPlayback<T> {
    pub value: T,
    pub cache_ttl: Duration,
}

/// Coordinates room-level web-session playback so concurrent members share one parse.
///
/// The cache value is required to contain only playback output that is safe to
/// return to room members. Credentials are passed only to the resolver closure
/// and are never stored by this coordinator.
#[derive(Clone)]
pub(crate) struct WebSessionPlaybackCoordinator {
    playback_sessions: Arc<ProviderPlaybackSessionRepository>,
    store: Arc<dyn ProviderStore>,
}

impl WebSessionPlaybackCoordinator {
    pub(crate) fn new(
        playback_sessions: Arc<ProviderPlaybackSessionRepository>,
        store: Arc<dyn ProviderStore>,
    ) -> Self {
        Self {
            playback_sessions,
            store,
        }
    }

    fn resource_fingerprint(request: &WebSessionPlaybackRequest) -> String {
        let mut hasher = Sha256::new();
        hasher.update(request.resource_key.as_bytes());
        hasher.update([0]);
        if let Some(version) = &request.resource_version {
            hasher.update(version.as_bytes());
        }
        hex::encode(hasher.finalize())
    }

    fn cache_key(request: &WebSessionPlaybackRequest, access: &WebSessionAccess) -> String {
        format!(
            "room-playback:{}:{}:{}:{}:{}:{}",
            request.provider.as_str(),
            request.room_id,
            request.playback_generation,
            request.credential_owner_id,
            access.credential_revision,
            Self::resource_fingerprint(request),
        )
    }

    fn lock_key(cache_key: &str) -> String {
        format!("lock:{cache_key}")
    }

    fn store_error(operation: &str, error: &StoreError) -> Error {
        Error::Internal(format!(
            "provider room playback {operation} failed: {error}"
        ))
    }

    async fn cached<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        self.store
            .get(key)
            .await
            .map_err(|error| Self::store_error("cache read", &error))
    }

    async fn wait_for_cached<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        for _ in 0..WEB_PLAYBACK_WAIT_STEPS {
            if let Some(value) = self.cached(key).await? {
                return Ok(Some(value));
            }
            tokio::time::sleep(WEB_PLAYBACK_WAIT_STEP).await;
        }
        Ok(None)
    }

    async fn ensure_session(
        &self,
        request: &WebSessionPlaybackRequest,
        access: &WebSessionAccess,
    ) -> Result<()> {
        let session = WebSessionPlaybackSession {
            server_id: access.server_id.clone(),
            credential_revision: access.credential_revision.clone(),
        };
        let session = match request.provider {
            WebSessionProvider::Iqiyi => ProviderPlaybackSession::Iqiyi(session),
            WebSessionProvider::TencentVideo => ProviderPlaybackSession::TencentVideo(session),
        };
        self.playback_sessions
            .upsert(NewProviderPlaybackSession {
                room_id: request.room_id,
                playback_generation: request.playback_generation,
                provider_instance_name: None,
                credential_owner_id: request.credential_owner_id,
                resource_key: request.resource_key.clone(),
                resource_version: request.resource_version.clone(),
                session,
                paused: request.paused,
            })
            .await?;
        Ok(())
    }

    pub(crate) async fn resolve<T, F, Fut>(
        &self,
        request: WebSessionPlaybackRequest,
        access: WebSessionAccess,
        resolver: F,
    ) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
        F: FnOnce(WebSessionAccess) -> Fut + Send,
        Fut: Future<Output = Result<WebSessionResolvedPlayback<T>>> + Send,
    {
        if request.playback_generation <= 0 {
            return Err(Error::InvalidInput(
                "web-session playback generation must be positive".to_string(),
            ));
        }
        if request.resource_key.trim().is_empty() {
            return Err(Error::InvalidInput(
                "web-session playback resource key is required".to_string(),
            ));
        }
        if access.provider != request.provider
            || access.credential_owner_id != request.credential_owner_id
        {
            return Err(Error::Internal(
                "provider web-session access identity mismatch".to_string(),
            ));
        }

        let cache_key = Self::cache_key(&request, &access);
        if let Some(value) = self.cached::<T>(&cache_key).await? {
            self.ensure_session(&request, &access).await?;
            return Ok(value);
        }

        let lock_key = Self::lock_key(&cache_key);
        let mut resolver = Some(resolver);
        for _ in 0..2 {
            match self.store.lock(&lock_key, WEB_PLAYBACK_LOCK_TTL).await {
                Ok(_guard) => {
                    if let Some(value) = self.cached::<T>(&cache_key).await? {
                        self.ensure_session(&request, &access).await?;
                        return Ok(value);
                    }

                    let resolve = resolver.take().ok_or_else(|| {
                        Error::Internal(
                            "provider room playback resolver was already consumed".to_string(),
                        )
                    })?;
                    let resolved = resolve(access.clone()).await?;
                    let cache_ttl = resolved
                        .cache_ttl
                        .max(Duration::from_secs(1))
                        .min(MAX_WEB_PLAYBACK_CACHE_TTL);

                    // Persist the room/session identity before publishing the cached
                    // result. Waiters can only observe a cache entry whose playback
                    // generation was still current when the resolver completed.
                    self.ensure_session(&request, &access).await?;
                    self.store
                        .set(&cache_key, &resolved.value, cache_ttl)
                        .await
                        .map_err(|error| Self::store_error("cache write", &error))?;
                    return Ok(resolved.value);
                }
                Err(StoreError::LockFailed(_)) => {
                    if let Some(value) = self.wait_for_cached::<T>(&cache_key).await? {
                        self.ensure_session(&request, &access).await?;
                        return Ok(value);
                    }
                }
                Err(error) => return Err(Self::store_error("lock acquisition", &error)),
            }
        }

        Err(Error::Conflict(
            "provider room playback resolution is already in progress".to_string(),
        ))
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
            cookie_payload_bytes(&cookies),
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
        assert!(validate_web_session_cookies(
            &client,
            WebSessionProvider::Iqiyi,
            &[cookie("example.com")],
        )
        .is_err());
    }

    #[test]
    fn playback_cache_key_does_not_embed_resource_url() {
        let request = WebSessionPlaybackRequest {
            room_id: RoomId::expect_positive(7),
            playback_generation: 11,
            provider: WebSessionProvider::Iqiyi,
            credential_owner_id: UserId::expect_positive(13),
            resource_key: "https://www.iqiyi.com/v_abcdef.html?secretish=query".to_string(),
            resource_version: Some("episode-1".to_string()),
            paused: false,
        };
        let access = WebSessionAccess {
            provider: WebSessionProvider::Iqiyi,
            server_id: WEB_SESSION_SERVER_ID.to_string(),
            credential_owner_id: UserId::expect_positive(13),
            credential_revision: "42:1700000000123456".to_string(),
            cookies: Vec::new(),
        };
        let key = WebSessionPlaybackCoordinator::cache_key(&request, &access);
        assert!(!key.contains("iqiyi.com"));
        assert!(!key.contains("secretish"));
        assert!(key.contains("42:1700000000123456"));
    }
}
