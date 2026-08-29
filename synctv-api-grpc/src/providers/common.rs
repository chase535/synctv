use std::sync::Arc;
use tonic::{Request, Response, Status};

use crate::grpc::map_api_error;
use synctv_api_common::api_runtime::SharedApiRuntime;
use synctv_api_common::impls::admin::RequestContext;
use synctv_api_common::impls::{validate_proto_request, ApiError, EndpointRateLimitCategory};
use synctv_core::models::provider_instance::ProviderWebSessionCookie;
use synctv_core::service::{
    BindWebSessionRequest as CoreBindWebSessionRequest, WebSessionBinding as CoreWebSessionBinding,
    WebSessionCredentialService, WebSessionProvider,
};
use synctv_proto::source_config::SourceProvider as ProtoSourceProvider;

use synctv_proto::providers::common::provider_common_service_server::ProviderCommonService;
use synctv_proto::providers::common::{
    AddProviderInstanceRequest, AddProviderInstanceResponse, BindWebSessionRequest,
    BindWebSessionResponse, DeleteProviderInstanceRequest, DeleteProviderInstanceResponse,
    DisableProviderInstanceRequest, DisableProviderInstanceResponse, EnableProviderInstanceRequest,
    EnableProviderInstanceResponse, ListAvailableProviderInstancesRequest,
    ListProviderBackendsRequest, ListProviderInstancesRequest, ListProviderInstancesResponse,
    ListWebSessionsRequest, ListWebSessionsResponse, PlaybackProxyPolicy, PrepareDirectUrlRequest,
    PrepareLiveProxyRequest, PrepareRtmpRequest, PreparedMediaSource, ProviderBackendsResponse,
    ProviderInstancesResponse, ReconnectProviderInstanceRequest, ReconnectProviderInstanceResponse,
    ResolvePlaybackProxyPolicyRequest, UnbindWebSessionRequest, UnbindWebSessionResponse,
    UpdateProviderInstanceRequest, UpdateProviderInstanceResponse,
    WebSessionBinding as ProtoWebSessionBinding,
};

#[derive(Clone)]
pub struct ProviderCommonGrpcService {
    api: Arc<synctv_api_common::providers::ProviderCommonApiImpl>,
    runtime_settings: Arc<synctv_api_common::ApiRuntimeSettings>,
    web_session_service: Arc<WebSessionCredentialService>,
}

impl ProviderCommonGrpcService {
    #[must_use]
    pub fn new(
        shared_api_runtime: &Arc<SharedApiRuntime>,
        runtime_settings: Arc<synctv_api_common::ApiRuntimeSettings>,
    ) -> Self {
        Self {
            api: shared_api_runtime.provider_common_api.clone(),
            runtime_settings,
            web_session_service: Arc::new(WebSessionCredentialService::new(
                shared_api_runtime
                    .playback_transport_services
                    .credential_repo
                    .clone(),
                reqwest::Client::new(),
            )),
        }
    }

    fn grpc_request_context<T: std::fmt::Debug>(
        request: &Request<T>,
        runtime_settings: &synctv_api_common::ApiRuntimeSettings,
    ) -> Result<RequestContext, Status> {
        let ip_address =
            crate::grpc::extract_client_ip(request, runtime_settings)?.map(|ip| ip.to_string());
        let user_agent = crate::grpc::request_user_agent(request)?;
        Ok(RequestContext {
            ip_address,
            user_agent,
        })
    }
}

fn web_session_provider_from_proto(provider: i32) -> Result<WebSessionProvider, ApiError> {
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

#[tonic::async_trait]
impl ProviderCommonService for ProviderCommonGrpcService {
    async fn prepare_direct_url(
        &self,
        request: Request<PrepareDirectUrlRequest>,
    ) -> Result<Response<PreparedMediaSource>, Status> {
        let metadata = super::provider_request_metadata(&request, &self.runtime_settings)?;
        let req = request.into_inner();
        let api = self.api.clone();
        let executor_api = api.clone();
        executor_api
            .execute_user_endpoint(
                &metadata,
                EndpointRateLimitCategory::Read,
                move |_| async move { api.prepare_direct_url(req) },
            )
            .await
            .map(Response::new)
            .map_err(map_api_error)
    }

    async fn prepare_live_proxy(
        &self,
        request: Request<PrepareLiveProxyRequest>,
    ) -> Result<Response<PreparedMediaSource>, Status> {
        let metadata = super::provider_request_metadata(&request, &self.runtime_settings)?;
        let req = request.into_inner();
        let api = self.api.clone();
        let executor_api = api.clone();
        executor_api
            .execute_user_endpoint(
                &metadata,
                EndpointRateLimitCategory::Read,
                move |_| async move { api.prepare_live_proxy(req).await },
            )
            .await
            .map(Response::new)
            .map_err(map_api_error)
    }

    async fn prepare_rtmp(
        &self,
        request: Request<PrepareRtmpRequest>,
    ) -> Result<Response<PreparedMediaSource>, Status> {
        let metadata = super::provider_request_metadata(&request, &self.runtime_settings)?;
        let req = request.into_inner();
        let api = self.api.clone();
        let executor_api = api.clone();
        executor_api
            .execute_user_endpoint(
                &metadata,
                EndpointRateLimitCategory::Read,
                move |_| async move { api.prepare_rtmp(req) },
            )
            .await
            .map(Response::new)
            .map_err(map_api_error)
    }

    async fn resolve_playback_proxy_policy(
        &self,
        request: Request<ResolvePlaybackProxyPolicyRequest>,
    ) -> Result<Response<PlaybackProxyPolicy>, Status> {
        let metadata = super::provider_request_metadata(&request, &self.runtime_settings)?;
        let req = request.into_inner();
        let api = self.api.clone();
        let executor_api = api.clone();
        executor_api
            .execute_user_endpoint(
                &metadata,
                EndpointRateLimitCategory::Read,
                move |_| async move { api.resolve_playback_proxy_policy(req).await },
            )
            .await
            .map(Response::new)
            .map_err(map_api_error)
    }

    async fn list_available_provider_instances(
        &self,
        request: Request<ListAvailableProviderInstancesRequest>,
    ) -> Result<Response<ProviderInstancesResponse>, Status> {
        let metadata = super::provider_request_metadata(&request, &self.runtime_settings)?;
        let req = request.into_inner();
        let api = self.api.clone();
        let executor_api = api.clone();

        executor_api
            .execute_user_endpoint(
                &metadata,
                EndpointRateLimitCategory::Read,
                move |_| async move { api.list_available_provider_instances(req).await },
            )
            .await
            .map(Response::new)
            .map_err(map_api_error)
    }

    async fn list_provider_backends(
        &self,
        request: Request<ListProviderBackendsRequest>,
    ) -> Result<Response<ProviderBackendsResponse>, Status> {
        let metadata = super::provider_request_metadata(&request, &self.runtime_settings)?;
        let req = request.into_inner();
        let api = self.api.clone();
        let executor_api = api.clone();

        executor_api
            .execute_user_endpoint(
                &metadata,
                EndpointRateLimitCategory::Read,
                move |_| async move { api.list_provider_backends(req).await },
            )
            .await
            .map(Response::new)
            .map_err(map_api_error)
    }

    async fn bind_web_session(
        &self,
        request: Request<BindWebSessionRequest>,
    ) -> Result<Response<BindWebSessionResponse>, Status> {
        let metadata = super::provider_request_metadata(&request, &self.runtime_settings)?;
        let req = request.into_inner();
        let api = self.api.clone();
        let executor_api = api.clone();
        let web_session_service = self.web_session_service.clone();

        executor_api
            .execute_user_endpoint(
                &metadata,
                EndpointRateLimitCategory::Write,
                move |validated| {
                    let web_session_service = web_session_service.clone();
                    async move {
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
                        let binding = web_session_service
                            .bind(CoreBindWebSessionRequest {
                                user_id: validated.user_id,
                                provider,
                                label: req.label,
                                cookies,
                            })
                            .await
                            .map_err(ApiError::from)?;
                        Ok(BindWebSessionResponse {
                            binding: Some(web_session_binding_to_proto(binding)?),
                        })
                    }
                },
            )
            .await
            .map(Response::new)
            .map_err(map_api_error)
    }

    async fn list_web_sessions(
        &self,
        request: Request<ListWebSessionsRequest>,
    ) -> Result<Response<ListWebSessionsResponse>, Status> {
        let metadata = super::provider_request_metadata(&request, &self.runtime_settings)?;
        let req = request.into_inner();
        let api = self.api.clone();
        let executor_api = api.clone();
        let web_session_service = self.web_session_service.clone();

        executor_api
            .execute_user_endpoint(
                &metadata,
                EndpointRateLimitCategory::Read,
                move |validated| {
                    let web_session_service = web_session_service.clone();
                    async move {
                        validate_proto_request(&req)?;
                        let bindings = web_session_service
                            .list(validated.user_id)
                            .await
                            .map_err(ApiError::from)?
                            .into_iter()
                            .map(web_session_binding_to_proto)
                            .collect::<Result<Vec<_>, _>>()?;
                        Ok(ListWebSessionsResponse { bindings })
                    }
                },
            )
            .await
            .map(Response::new)
            .map_err(map_api_error)
    }

    async fn unbind_web_session(
        &self,
        request: Request<UnbindWebSessionRequest>,
    ) -> Result<Response<UnbindWebSessionResponse>, Status> {
        let metadata = super::provider_request_metadata(&request, &self.runtime_settings)?;
        let req = request.into_inner();
        let api = self.api.clone();
        let executor_api = api.clone();
        let web_session_service = self.web_session_service.clone();

        executor_api
            .execute_user_endpoint(
                &metadata,
                EndpointRateLimitCategory::Write,
                move |validated| {
                    let web_session_service = web_session_service.clone();
                    async move {
                        validate_proto_request(&req)?;
                        let provider = web_session_provider_from_proto(req.provider)?;
                        let removed = web_session_service
                            .unbind(validated.user_id, provider)
                            .await
                            .map_err(ApiError::from)?;
                        Ok(UnbindWebSessionResponse { removed })
                    }
                },
            )
            .await
            .map(Response::new)
            .map_err(map_api_error)
    }

    async fn list_provider_instances(
        &self,
        request: Request<ListProviderInstancesRequest>,
    ) -> Result<Response<ListProviderInstancesResponse>, Status> {
        let metadata = super::provider_request_metadata(&request, &self.runtime_settings)?;
        let req = request.into_inner();
        let api = self.api.clone();
        let executor_api = api.clone();

        executor_api
            .execute_admin_endpoint(&metadata, move |_| async move {
                api.list_provider_instances(req).await
            })
            .await
            .map(Response::new)
            .map_err(map_api_error)
    }

    async fn add_provider_instance(
        &self,
        request: Request<AddProviderInstanceRequest>,
    ) -> Result<Response<AddProviderInstanceResponse>, Status> {
        let metadata = super::provider_request_metadata(&request, &self.runtime_settings)?;
        let ctx = Self::grpc_request_context(&request, &self.runtime_settings)?;
        let req = request.into_inner();
        let api = self.api.clone();
        let executor_api = api.clone();

        executor_api
            .execute_admin_endpoint_with_control(&metadata, move |request_control, validated| {
                let api = api.clone();
                let ctx = ctx.clone();
                async move {
                    api.add_provider_instance(req, &validated.user_id, &ctx, Some(&request_control))
                        .await
                }
            })
            .await
            .map(Response::new)
            .map_err(map_api_error)
    }

    async fn update_provider_instance(
        &self,
        request: Request<UpdateProviderInstanceRequest>,
    ) -> Result<Response<UpdateProviderInstanceResponse>, Status> {
        let metadata = super::provider_request_metadata(&request, &self.runtime_settings)?;
        let ctx = Self::grpc_request_context(&request, &self.runtime_settings)?;
        let req = request.into_inner();
        let api = self.api.clone();
        let executor_api = api.clone();

        executor_api
            .execute_admin_endpoint_with_control(&metadata, move |request_control, validated| {
                let api = api.clone();
                let ctx = ctx.clone();
                async move {
                    api.update_provider_instance(
                        req,
                        &validated.user_id,
                        &ctx,
                        Some(&request_control),
                    )
                    .await
                }
            })
            .await
            .map(Response::new)
            .map_err(map_api_error)
    }

    async fn delete_provider_instance(
        &self,
        request: Request<DeleteProviderInstanceRequest>,
    ) -> Result<Response<DeleteProviderInstanceResponse>, Status> {
        let metadata = super::provider_request_metadata(&request, &self.runtime_settings)?;
        let ctx = Self::grpc_request_context(&request, &self.runtime_settings)?;
        let req = request.into_inner();
        let api = self.api.clone();
        let executor_api = api.clone();

        executor_api
            .execute_admin_endpoint(&metadata, move |validated| {
                let api = api.clone();
                let ctx = ctx.clone();
                async move {
                    api.delete_provider_instance(req, &validated.user_id, &ctx)
                        .await
                }
            })
            .await
            .map(Response::new)
            .map_err(map_api_error)
    }

    async fn reconnect_provider_instance(
        &self,
        request: Request<ReconnectProviderInstanceRequest>,
    ) -> Result<Response<ReconnectProviderInstanceResponse>, Status> {
        let metadata = super::provider_request_metadata(&request, &self.runtime_settings)?;
        let ctx = Self::grpc_request_context(&request, &self.runtime_settings)?;
        let req = request.into_inner();
        let api = self.api.clone();
        let executor_api = api.clone();

        executor_api
            .execute_admin_endpoint_with_control(&metadata, move |request_control, validated| {
                let api = api.clone();
                let ctx = ctx.clone();
                async move {
                    api.reconnect_provider_instance(
                        req,
                        &validated.user_id,
                        &ctx,
                        Some(&request_control),
                    )
                    .await
                }
            })
            .await
            .map(Response::new)
            .map_err(map_api_error)
    }

    async fn enable_provider_instance(
        &self,
        request: Request<EnableProviderInstanceRequest>,
    ) -> Result<Response<EnableProviderInstanceResponse>, Status> {
        let metadata = super::provider_request_metadata(&request, &self.runtime_settings)?;
        let req = request.into_inner();
        let api = self.api.clone();
        let executor_api = api.clone();

        executor_api
            .execute_admin_endpoint_with_control(&metadata, move |request_control, _| {
                let api = api.clone();
                async move {
                    api.enable_provider_instance(req, Some(&request_control))
                        .await
                }
            })
            .await
            .map(Response::new)
            .map_err(map_api_error)
    }

    async fn disable_provider_instance(
        &self,
        request: Request<DisableProviderInstanceRequest>,
    ) -> Result<Response<DisableProviderInstanceResponse>, Status> {
        let metadata = super::provider_request_metadata(&request, &self.runtime_settings)?;
        let req = request.into_inner();
        let api = self.api.clone();
        let executor_api = api.clone();

        executor_api
            .execute_admin_endpoint_with_control(&metadata, move |request_control, _| {
                let api = api.clone();
                async move {
                    api.disable_provider_instance(req, Some(&request_control))
                        .await
                }
            })
            .await
            .map(Response::new)
            .map_err(map_api_error)
    }
}
