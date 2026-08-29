from pathlib import Path

path = Path("synctv-api-http/src/providers/common.rs")
text = path.read_text()


def replace_once(old: str, new: str, name: str) -> None:
    global text
    if old not in text:
        raise SystemExit(f"expected {name} anchor not found")
    text = text.replace(old, new, 1)


replace_once(
    "use synctv_api_common::impls::{ApiError, EndpointRateLimitCategory};\n",
    """use synctv_api_common::impls::{
    validate_proto_request, ApiError, EndpointRateLimitCategory,
};
use synctv_core::models::provider_instance::ProviderWebSessionCookie;
use synctv_core::service::{
    BindWebSessionRequest as CoreBindWebSessionRequest,
    WebSessionBinding as CoreWebSessionBinding, WebSessionCredentialService, WebSessionProvider,
};
use synctv_proto::source_config::SourceProvider as ProtoSourceProvider;
""",
    "impl import",
)

replace_once(
    """    AddProviderInstanceRequest, AddProviderInstanceResponse, DeleteProviderInstanceRequest,
    DeleteProviderInstanceResponse, DisableProviderInstanceRequest,
    DisableProviderInstanceResponse, EnableProviderInstanceRequest, EnableProviderInstanceResponse,
    ListAvailableProviderInstancesRequest, ListProviderBackendsRequest,
    ListProviderInstancesRequest, ListProviderInstancesResponse, PlaybackProxyPolicy,
    PrepareDirectUrlRequest, PrepareLiveProxyRequest, PrepareRtmpRequest, PreparedMediaSource,
    ProviderBackendsResponse, ProviderInstanceQuery, ProviderInstancesResponse,
    ReconnectProviderInstanceRequest, ReconnectProviderInstanceResponse,
    ResolvePlaybackProxyPolicyRequest, UpdateProviderInstanceRequest,
    UpdateProviderInstanceResponse,
""",
    """    AddProviderInstanceRequest, AddProviderInstanceResponse, BindWebSessionRequest,
    BindWebSessionResponse, DeleteProviderInstanceRequest, DeleteProviderInstanceResponse,
    DisableProviderInstanceRequest, DisableProviderInstanceResponse, EnableProviderInstanceRequest,
    EnableProviderInstanceResponse, ListAvailableProviderInstancesRequest,
    ListProviderBackendsRequest, ListProviderInstancesRequest, ListProviderInstancesResponse,
    ListWebSessionsResponse, PlaybackProxyPolicy, PrepareDirectUrlRequest, PrepareLiveProxyRequest,
    PrepareRtmpRequest, PreparedMediaSource, ProviderBackendsResponse, ProviderInstanceQuery,
    ProviderInstancesResponse, ReconnectProviderInstanceRequest, ReconnectProviderInstanceResponse,
    ResolvePlaybackProxyPolicyRequest, UnbindWebSessionRequest, UnbindWebSessionResponse,
    UpdateProviderInstanceRequest, UpdateProviderInstanceResponse,
    WebSessionBinding as ProtoWebSessionBinding,
""",
    "proto import",
)

replace_once(
    '        .route("/backends/{providerType}", get(list_backends))\n}\n',
    """        .route("/backends/{providerType}", get(list_backends))
        .route(
            "/web-sessions",
            get(list_web_sessions).post(bind_web_session),
        )
        .route("/web-sessions/unbind", post(unbind_web_session))
}
""",
    "route",
)

helpers = r'''fn web_session_provider_from_proto(provider: i32) -> Result<WebSessionProvider, ApiError> {
    let provider = ProtoSourceProvider::try_from(provider)
        .map_err(|_| ApiError::InvalidInput("Unsupported source_provider".to_string()))?;
    match provider {
        ProtoSourceProvider::Iqiyi => Ok(WebSessionProvider::Iqiyi),
        ProtoSourceProvider::TencentVideo => Ok(WebSessionProvider::TencentVideo),
        _ => Err(ApiError::InvalidInput(
            "web-session provider must be iQiyi or Tencent Video".to_string(),
        )),
    }
}

const fn web_session_provider_to_proto(provider: WebSessionProvider) -> i32 {
    match provider {
        WebSessionProvider::Iqiyi => ProtoSourceProvider::Iqiyi as i32,
        WebSessionProvider::TencentVideo => ProtoSourceProvider::TencentVideo as i32,
    }
}

fn web_session_binding_to_proto(
    binding: CoreWebSessionBinding,
) -> Result<ProtoWebSessionBinding, ApiError> {
    let cookie_count = u32::try_from(binding.cookie_count).map_err(|_| {
        ApiError::Internal("provider web-session cookie count exceeds u32::MAX".to_string())
    })?;
    Ok(ProtoWebSessionBinding {
        credential_id: binding.credential_id,
        provider: web_session_provider_to_proto(binding.provider),
        server_id: binding.server_id,
        label: binding.label,
        cookie_count,
        expires_at: binding.expires_at.map(|value| value.timestamp()),
        created_at: binding.created_at.timestamp(),
        updated_at: binding.updated_at.timestamp(),
    })
}

fn web_session_service(state: &AppState) -> WebSessionCredentialService {
    WebSessionCredentialService::new(
        state
            .shared_api_runtime
            .playback_transport_services
            .credential_repo
            .clone(),
        reqwest::Client::new(),
    )
}

pub(crate) async fn bind_web_session(
    request_meta: RequestMetadata,
    State(state): State<AppState>,
    Json(req): Json<BindWebSessionRequest>,
) -> AppResult<Json<BindWebSessionResponse>> {
    let api = state.shared_api_runtime.provider_common_api.clone();
    let executor = api.clone();
    let service = web_session_service(&state);
    executor
        .execute_user_endpoint(
            &request_meta.0,
            EndpointRateLimitCategory::Write,
            move |validated| async move {
                validate_proto_request(&req)?;
                let provider = web_session_provider_from_proto(req.provider)?;
                let cookies = req
                    .cookies
                    .into_iter()
                    .map(|cookie| ProviderWebSessionCookie {
                        name: cookie.name,
                        value: cookie.value,
                        domain: cookie.domain,
                        path: cookie.path,
                        secure: cookie.secure,
                        http_only: cookie.http_only,
                        session_only: cookie.session_only,
                        expires_at: cookie.expires_at,
                    })
                    .collect();
                let binding = service
                    .bind(CoreBindWebSessionRequest {
                        user_id: validated.user_id(),
                        provider,
                        label: req.label,
                        cookies,
                    })
                    .await
                    .map_err(ApiError::from)?;
                Ok(BindWebSessionResponse {
                    binding: Some(web_session_binding_to_proto(binding)?),
                })
            },
        )
        .await
        .map(Json)
        .map_err(map_api_error)
}

pub(crate) async fn list_web_sessions(
    request_meta: RequestMetadata,
    State(state): State<AppState>,
) -> AppResult<Json<ListWebSessionsResponse>> {
    let api = state.shared_api_runtime.provider_common_api.clone();
    let executor = api.clone();
    let service = web_session_service(&state);
    executor
        .execute_user_endpoint(
            &request_meta.0,
            EndpointRateLimitCategory::Read,
            move |validated| async move {
                let bindings = service
                    .list(validated.user_id())
                    .await
                    .map_err(ApiError::from)?
                    .into_iter()
                    .map(web_session_binding_to_proto)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(ListWebSessionsResponse { bindings })
            },
        )
        .await
        .map(Json)
        .map_err(map_api_error)
}

pub(crate) async fn unbind_web_session(
    request_meta: RequestMetadata,
    State(state): State<AppState>,
    Json(req): Json<UnbindWebSessionRequest>,
) -> AppResult<Json<UnbindWebSessionResponse>> {
    let api = state.shared_api_runtime.provider_common_api.clone();
    let executor = api.clone();
    let service = web_session_service(&state);
    executor
        .execute_user_endpoint(
            &request_meta.0,
            EndpointRateLimitCategory::Write,
            move |validated| async move {
                validate_proto_request(&req)?;
                let provider = web_session_provider_from_proto(req.provider)?;
                let removed = service
                    .unbind(validated.user_id(), provider)
                    .await
                    .map_err(ApiError::from)?;
                Ok(UnbindWebSessionResponse { removed })
            },
        )
        .await
        .map(Json)
        .map_err(map_api_error)
}

'''
replace_once(
    "pub(crate) async fn prepare_direct_url(\n",
    helpers + "pub(crate) async fn prepare_direct_url(\n",
    "handler",
)

path.write_text(text)
