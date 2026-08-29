//! Authenticated web-session video providers.
//!
//! These adapters intentionally keep provider credentials on the server. They
//! use an authenticated browser session to discover playback resources exposed
//! by the official provider page, then publish only credential-free HTTP(S)
//! media URLs. DRM/device-license bypasses are deliberately out of scope.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use synctv_media_providers::{
    web_session::WebPagePlaybackDiscovery, IqiyiClient, ProviderClientError, TencentVideoClient,
};

use super::{
    MediaProvider, PlaybackInfo, PlaybackProxyAutoPolicy, PlaybackProxyAutoReason,
    PlaybackProxyPolicy, PlaybackResult, ProviderContext, ProviderCredentialDependency,
    ProviderCredentialPolicy, ProviderError, SourceConfig,
};
use crate::models::{
    detect_direct_url_format, IqiyiMediaSourceConfig, MediaSourceConfig, PlaybackDirectUrlMedia,
    PlaybackKind, PlaybackMedia, PlaybackMediaProvider, PlaybackProxyMode, SourceProvider,
    TencentVideoMediaSourceConfig,
};
use crate::repository::ProviderPlaybackSessionRepository;
use crate::service::web_session::{
    load_web_session_access, WebSessionPlaybackCoordinator, WebSessionPlaybackRequest,
    WebSessionProvider, WebSessionResolvedPlayback, WEB_SESSION_SERVER_ID,
};

const ROOM_PLAYBACK_CACHE_TTL: Duration = Duration::from_secs(120);
const SIGNED_URL_EXPIRY_MARGIN_SECONDS: i64 = 5;

#[derive(Clone)]
pub struct IqiyiProvider {
    http_client: reqwest::Client,
}

#[derive(Clone)]
pub struct TencentVideoProvider {
    http_client: reqwest::Client,
}

impl IqiyiProvider {
    pub const NAME: &'static str = "iqiyi";

    #[must_use]
    pub fn with_http_client(http_client: reqwest::Client) -> Self {
        Self { http_client }
    }
}

impl TencentVideoProvider {
    pub const NAME: &'static str = "tencent_video";

    #[must_use]
    pub fn with_http_client(http_client: reqwest::Client) -> Self {
        Self { http_client }
    }
}

#[derive(Debug, Clone, Copy)]
struct WebVideoSource<'a> {
    url: &'a str,
    shared: bool,
    proxy_mode: PlaybackProxyMode,
}

fn iqiyi_source(config: &MediaSourceConfig) -> Result<WebVideoSource<'_>, ProviderError> {
    let MediaSourceConfig::Iqiyi(IqiyiMediaSourceConfig {
        url,
        shared,
        proxy_mode,
    }) = config
    else {
        return Err(ProviderError::InvalidConfig(
            "iQiyi provider requires iQiyi media source_config".to_string(),
        ));
    };
    Ok(WebVideoSource {
        url,
        shared: *shared,
        proxy_mode: *proxy_mode,
    })
}

fn tencent_source(config: &MediaSourceConfig) -> Result<WebVideoSource<'_>, ProviderError> {
    let MediaSourceConfig::TencentVideo(TencentVideoMediaSourceConfig {
        url,
        shared,
        proxy_mode,
    }) = config
    else {
        return Err(ProviderError::InvalidConfig(
            "Tencent Video provider requires Tencent Video media source_config".to_string(),
        ));
    };
    Ok(WebVideoSource {
        url,
        shared: *shared,
        proxy_mode: *proxy_mode,
    })
}

fn source_from(
    source_config: SourceConfig<'_>,
    provider: WebSessionProvider,
) -> Result<WebVideoSource<'_>, ProviderError> {
    let SourceConfig::Media(config) = source_config else {
        return Err(ProviderError::InvalidConfig(format!(
            "{} dynamic playlists are not supported",
            provider.as_str()
        )));
    };
    match provider {
        WebSessionProvider::Iqiyi => iqiyi_source(config),
        WebSessionProvider::TencentVideo => tencent_source(config),
    }
}

fn source_provider(provider: WebSessionProvider) -> SourceProvider {
    match provider {
        WebSessionProvider::Iqiyi => SourceProvider::Iqiyi,
        WebSessionProvider::TencentVideo => SourceProvider::TencentVideo,
    }
}

fn credential_owner(
    ctx: &ProviderContext<'_>,
    source: WebVideoSource<'_>,
) -> Result<crate::models::UserId, ProviderError> {
    ctx.resolve_credential_user_id(ProviderCredentialPolicy::from_shared(source.shared), true)?
        .ok_or(ProviderError::CredentialRequired)
}

fn ensure_supported_proxy_mode(mode: PlaybackProxyMode) -> Result<(), ProviderError> {
    if matches!(
        mode,
        PlaybackProxyMode::Auto | PlaybackProxyMode::DirectPrefer | PlaybackProxyMode::DirectOnly
    ) {
        Ok(())
    } else {
        Err(ProviderError::UnsupportedFormat(
            "authenticated web-session providers do not expose server proxy playback yet"
                .to_string(),
        ))
    }
}

fn proxy_policy(mode: PlaybackProxyMode) -> PlaybackProxyPolicy {
    PlaybackProxyPolicy {
        current_mode: mode,
        supported_modes: vec![
            PlaybackProxyMode::Auto,
            PlaybackProxyMode::DirectPrefer,
            PlaybackProxyMode::DirectOnly,
        ],
        auto_policies: vec![PlaybackProxyAutoPolicy::new(
            "direct",
            PlaybackProxyMode::DirectOnly,
            PlaybackProxyAutoReason::SignedResource,
        )],
    }
}

fn provider_client_error(error: ProviderClientError) -> ProviderError {
    match error {
        ProviderClientError::Network(message) => ProviderError::NetworkError(message),
        ProviderClientError::Http { status, url, .. } => ProviderError::UpstreamHttp {
            status: status.as_u16(),
            url,
        },
        ProviderClientError::Api { code, message } => {
            ProviderError::ApiError(format!("provider API {code}: {message}"))
        }
        ProviderClientError::Parse(message) => ProviderError::ParseError(message),
        ProviderClientError::Auth(message) => ProviderError::Authentication(message),
        ProviderClientError::InvalidConfig(message)
        | ProviderClientError::InvalidHeader(message) => ProviderError::InvalidConfig(message),
        ProviderClientError::ResponseTooLarge { size } => {
            ProviderError::ApiError(format!("provider response is too large: {size} bytes"))
        }
    }
}

fn core_error(error: crate::Error) -> ProviderError {
    match error {
        crate::Error::Authentication(message) => ProviderError::Authentication(message),
        crate::Error::InvalidInput(message) => ProviderError::InvalidConfig(message),
        crate::Error::NotFound(message) => ProviderError::CredentialNotFound(message),
        crate::Error::ClientIncompatible {
            reason,
            required_capability,
        } => ProviderError::ClientIncompatible {
            reason,
            required_capability,
        },
        crate::Error::Timeout(message) | crate::Error::ServiceUnavailable(message) => {
            ProviderError::NetworkError(message)
        }
        other => ProviderError::Internal(other.to_string()),
    }
}

fn signed_url_expiry(url: &str) -> Option<i64> {
    crate::provider::url_expiration_timestamp(url)
}

fn discovery_cache_ttl(discovery: &WebPagePlaybackDiscovery) -> Duration {
    let now = crate::SystemClock.now().timestamp();
    let signed_ttl = discovery
        .media_urls
        .iter()
        .filter_map(|url| signed_url_expiry(url))
        .map(|expires_at| {
            expires_at
                .saturating_sub(now)
                .saturating_sub(SIGNED_URL_EXPIRY_MARGIN_SECONDS)
        })
        .filter(|ttl| *ttl > 0)
        .min()
        .and_then(|ttl| u64::try_from(ttl).ok())
        .map(Duration::from_secs);
    signed_ttl.map_or(ROOM_PLAYBACK_CACHE_TTL, |ttl| {
        ttl.min(ROOM_PLAYBACK_CACHE_TTL)
    })
}

fn playback_from_discovery(
    provider: WebSessionProvider,
    discovery: WebPagePlaybackDiscovery,
) -> Result<PlaybackResult, ProviderError> {
    if discovery.drm_detected {
        return Err(ProviderError::UnsupportedFormat(format!(
            "{} page requires a DRM/device-license playback path that SyncTV does not bypass",
            provider.as_str()
        )));
    }

    let now = crate::SystemClock.now().timestamp();
    let medias = discovery
        .media_urls
        .into_iter()
        .filter_map(|url| {
            let expires_at = signed_url_expiry(&url);
            if expires_at.is_some_and(|expires_at| expires_at <= now) {
                return None;
            }
            Some(PlaybackMedia {
                name: String::new(),
                format: detect_direct_url_format(&url).to_string(),
                expire_at: expires_at.and_then(|value| chrono::DateTime::from_timestamp(value, 0)),
                metadata: None,
                p2p_swarm_id: None,
                provider: PlaybackMediaProvider::DirectUrl(PlaybackDirectUrlMedia::Direct {
                    url,
                    headers: HashMap::new(),
                }),
            })
        })
        .enumerate()
        .map(|(index, mut media)| {
            media.name = if index == 0 {
                "Default".to_string()
            } else {
                format!("Source {}", index + 1)
            };
            media
        })
        .collect::<Vec<_>>();

    if medias.is_empty() {
        return Err(ProviderError::UnsupportedFormat(format!(
            "{} authenticated page did not expose a current credential-free HTTP(S) playback resource",
            provider.as_str()
        )));
    }

    Ok(PlaybackResult {
        playback_infos: HashMap::from([(
            "direct".to_string(),
            PlaybackInfo {
                thumbnail: None,
                medias,
                default_media_index: Some(0),
                subtitles: Vec::new(),
                default_subtitle_index: None,
                danmakus: Vec::new(),
                default_danmaku_index: None,
            },
        )]),
        default_mode: "direct".to_string(),
        provider: source_provider(provider),
        provider_instance_name: None,
        duration_seconds: None,
        playback_kind: Some(PlaybackKind::Regular),
        metadata: None,
    })
}

fn filter_for_client(
    mut result: PlaybackResult,
    profile: Option<&crate::provider::PlaybackClientProfile>,
) -> PlaybackResult {
    let original_default = result.default_mode.clone();
    result.playback_infos = std::mem::take(&mut result.playback_infos)
        .into_iter()
        .filter_map(|(mode_name, info)| {
            crate::provider::build_direct_playback_info_for_client(&mode_name, &info, profile)
                .map(|info| (mode_name, info))
        })
        .collect();
    if !result.playback_infos.contains_key(&original_default) {
        result.default_mode = result
            .playback_infos
            .keys()
            .min()
            .cloned()
            .unwrap_or_default();
    }
    result
}

fn validate_url(
    http_client: &reqwest::Client,
    provider: WebSessionProvider,
    source: WebVideoSource<'_>,
) -> Result<(), ProviderError> {
    ensure_supported_proxy_mode(source.proxy_mode)?;
    match provider {
        WebSessionProvider::Iqiyi => IqiyiClient::new(http_client.clone(), Vec::new())
            .and_then(|client| client.validate_url(source.url).map(|_| ())),
        WebSessionProvider::TencentVideo => {
            TencentVideoClient::new(http_client.clone(), Vec::new())
                .and_then(|client| client.validate_url(source.url).map(|_| ()))
        }
    }
    .map_err(provider_client_error)
}

fn credential_dependencies(
    ctx: &ProviderContext<'_>,
    provider: WebSessionProvider,
    source_config: SourceConfig<'_>,
) -> Result<Vec<ProviderCredentialDependency>, ProviderError> {
    let source = source_from(source_config, provider)?;
    let owner = credential_owner(ctx, source)?;
    Ok(vec![ProviderCredentialDependency::new(
        source_provider(provider),
        owner,
        WEB_SESSION_SERVER_ID,
    )])
}

async fn generate_playback(
    http_client: &reqwest::Client,
    provider: WebSessionProvider,
    ctx: &ProviderContext<'_>,
    source_config: &MediaSourceConfig,
) -> Result<PlaybackResult, ProviderError> {
    ctx.check_active()
        .map_err(|error| ProviderError::NetworkError(error.to_string()))?;
    let source = match provider {
        WebSessionProvider::Iqiyi => iqiyi_source(source_config)?,
        WebSessionProvider::TencentVideo => tencent_source(source_config)?,
    };
    ensure_supported_proxy_mode(source.proxy_mode)?;
    let owner = credential_owner(ctx, source)?;

    let credential_repo = ctx.credential_repo.ok_or_else(|| {
        ProviderError::Internal("provider credential repository is not configured".to_string())
    })?;
    let db = ctx.db.ok_or_else(|| {
        ProviderError::Internal("provider database context is not configured".to_string())
    })?;
    let store = ctx
        .store
        .clone()
        .ok_or_else(|| ProviderError::Internal("provider store is not configured".to_string()))?;
    let room_id = ctx
        .room_id
        .ok_or_else(|| ProviderError::MissingField("room_id".to_string()))?;
    let playback_generation = ctx
        .playback_generation
        .ok_or_else(|| ProviderError::MissingField("playback_generation".to_string()))?;

    let access = load_web_session_access(credential_repo, http_client, owner, provider)
        .await
        .map_err(core_error)?;
    let coordinator = WebSessionPlaybackCoordinator::new(
        Arc::new(ProviderPlaybackSessionRepository::new(db.clone())),
        store,
    );
    let request = WebSessionPlaybackRequest {
        room_id,
        playback_generation,
        provider,
        credential_owner_id: owner,
        resource_key: source.url.to_string(),
        resource_version: None,
        paused: !ctx.playback_is_playing.unwrap_or(true),
    };
    let source_url = source.url.to_string();
    let http_client = http_client.clone();

    let result = coordinator
        .resolve(request, access, move |access| async move {
            let discovery = match provider {
                WebSessionProvider::Iqiyi => IqiyiClient::new(http_client, access.cookies)
                    .map_err(provider_client_error)?
                    .discover_playback(&source_url)
                    .await
                    .map_err(provider_client_error)?,
                WebSessionProvider::TencentVideo => {
                    TencentVideoClient::new(http_client, access.cookies)
                        .map_err(provider_client_error)?
                        .discover_playback(&source_url)
                        .await
                        .map_err(provider_client_error)?
                }
            };
            let cache_ttl = discovery_cache_ttl(&discovery);
            let value = playback_from_discovery(provider, discovery).map_err(crate::Error::from)?;
            Ok(WebSessionResolvedPlayback { value, cache_ttl })
        })
        .await
        .map_err(core_error)?;

    let profile = ctx.playback_client_profile();
    let result = filter_for_client(result, profile);
    super::require_compatible_playback_route(result, source.proxy_mode, profile)
}

#[async_trait]
impl MediaProvider for IqiyiProvider {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    async fn generate_playback(
        &self,
        ctx: &ProviderContext<'_>,
        source_config: &MediaSourceConfig,
    ) -> Result<PlaybackResult, ProviderError> {
        generate_playback(
            &self.http_client,
            WebSessionProvider::Iqiyi,
            ctx,
            source_config,
        )
        .await
    }

    fn playback_proxy_policy(
        &self,
        source_config: SourceConfig<'_>,
    ) -> Result<Option<PlaybackProxyPolicy>, ProviderError> {
        let source = source_from(source_config, WebSessionProvider::Iqiyi)?;
        Ok(Some(proxy_policy(source.proxy_mode)))
    }

    fn set_playback_proxy_mode(
        &self,
        source_config: &mut MediaSourceConfig,
        mode: PlaybackProxyMode,
    ) -> Result<(), ProviderError> {
        ensure_supported_proxy_mode(mode)?;
        let MediaSourceConfig::Iqiyi(config) = source_config else {
            return Err(ProviderError::InvalidConfig(
                "iQiyi provider requires iQiyi media source_config".to_string(),
            ));
        };
        config.proxy_mode = mode;
        Ok(())
    }

    async fn validate_source_config(
        &self,
        _ctx: &ProviderContext<'_>,
        source_config: SourceConfig<'_>,
    ) -> Result<(), ProviderError> {
        validate_url(
            &self.http_client,
            WebSessionProvider::Iqiyi,
            source_from(source_config, WebSessionProvider::Iqiyi)?,
        )
    }

    fn credential_dependencies(
        &self,
        ctx: &ProviderContext<'_>,
        source_config: SourceConfig<'_>,
    ) -> Result<Vec<ProviderCredentialDependency>, ProviderError> {
        credential_dependencies(ctx, WebSessionProvider::Iqiyi, source_config)
    }
}

#[async_trait]
impl MediaProvider for TencentVideoProvider {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    async fn generate_playback(
        &self,
        ctx: &ProviderContext<'_>,
        source_config: &MediaSourceConfig,
    ) -> Result<PlaybackResult, ProviderError> {
        generate_playback(
            &self.http_client,
            WebSessionProvider::TencentVideo,
            ctx,
            source_config,
        )
        .await
    }

    fn playback_proxy_policy(
        &self,
        source_config: SourceConfig<'_>,
    ) -> Result<Option<PlaybackProxyPolicy>, ProviderError> {
        let source = source_from(source_config, WebSessionProvider::TencentVideo)?;
        Ok(Some(proxy_policy(source.proxy_mode)))
    }

    fn set_playback_proxy_mode(
        &self,
        source_config: &mut MediaSourceConfig,
        mode: PlaybackProxyMode,
    ) -> Result<(), ProviderError> {
        ensure_supported_proxy_mode(mode)?;
        let MediaSourceConfig::TencentVideo(config) = source_config else {
            return Err(ProviderError::InvalidConfig(
                "Tencent Video provider requires Tencent Video media source_config".to_string(),
            ));
        };
        config.proxy_mode = mode;
        Ok(())
    }

    async fn validate_source_config(
        &self,
        _ctx: &ProviderContext<'_>,
        source_config: SourceConfig<'_>,
    ) -> Result<(), ProviderError> {
        validate_url(
            &self.http_client,
            WebSessionProvider::TencentVideo,
            source_from(source_config, WebSessionProvider::TencentVideo)?,
        )
    }

    fn credential_dependencies(
        &self,
        ctx: &ProviderContext<'_>,
        source_config: SourceConfig<'_>,
    ) -> Result<Vec<ProviderCredentialDependency>, ProviderError> {
        credential_dependencies(ctx, WebSessionProvider::TencentVideo, source_config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{IqiyiMediaSourceConfig, TencentVideoMediaSourceConfig, UserId};
    use crate::provider::ProviderActor;

    #[test]
    fn shared_source_uses_resource_owner_credential() {
        let viewer = UserId::expect_positive(41);
        let owner = UserId::expect_positive(42);
        let ctx = ProviderContext::new("test", ProviderActor::User(viewer))
            .with_credential_owner_id(owner);
        let config = MediaSourceConfig::Iqiyi(IqiyiMediaSourceConfig {
            url: "https://www.iqiyi.com/v_demo.html".to_string(),
            shared: true,
            proxy_mode: PlaybackProxyMode::Auto,
        });

        let dependencies = credential_dependencies(
            &ctx,
            WebSessionProvider::Iqiyi,
            SourceConfig::media(&config),
        )
        .expect("credential dependency");
        assert_eq!(dependencies.len(), 1);
        assert_eq!(dependencies[0].user_id, owner);
        assert_eq!(dependencies[0].server_id, WEB_SESSION_SERVER_ID);
    }

    #[test]
    fn non_shared_source_uses_viewer_credential() {
        let viewer = UserId::expect_positive(43);
        let owner = UserId::expect_positive(44);
        let ctx = ProviderContext::new("test", ProviderActor::User(viewer))
            .with_credential_owner_id(owner);
        let config = MediaSourceConfig::TencentVideo(TencentVideoMediaSourceConfig {
            url: "https://v.qq.com/x/cover/demo.html".to_string(),
            shared: false,
            proxy_mode: PlaybackProxyMode::Auto,
        });

        let dependencies = credential_dependencies(
            &ctx,
            WebSessionProvider::TencentVideo,
            SourceConfig::media(&config),
        )
        .expect("credential dependency");
        assert_eq!(dependencies.len(), 1);
        assert_eq!(dependencies[0].user_id, viewer);
    }

    #[test]
    fn proxy_only_is_rejected_until_server_transport_exists() {
        assert!(ensure_supported_proxy_mode(PlaybackProxyMode::Only).is_err());
        assert!(ensure_supported_proxy_mode(PlaybackProxyMode::Prefer).is_err());
        assert!(ensure_supported_proxy_mode(PlaybackProxyMode::Auto).is_ok());
    }

    #[test]
    fn drm_discovery_is_not_exposed_as_direct_playback() {
        let discovery = WebPagePlaybackDiscovery {
            page_url: "https://www.iqiyi.com/v_demo.html".to_string(),
            title: Some("VIP".to_string()),
            media_urls: vec!["https://cdn.example/video.mp4".to_string()],
            drm_detected: true,
        };
        assert!(playback_from_discovery(WebSessionProvider::Iqiyi, discovery).is_err());
    }

    #[test]
    fn room_cache_ttl_respects_signed_url_expiry() {
        let now = crate::SystemClock.now().timestamp();
        let discovery = WebPagePlaybackDiscovery {
            page_url: "https://www.iqiyi.com/v_demo.html".to_string(),
            title: None,
            media_urls: vec![format!(
                "https://cdn.example/video.mp4?AWSAccessKeyId=key&Signature=sig&Expires={}",
                now + 30
            )],
            drm_detected: false,
        };
        let ttl = discovery_cache_ttl(&discovery);
        assert!(ttl <= Duration::from_secs(25));
        assert!(ttl >= Duration::from_secs(20));
    }
}
