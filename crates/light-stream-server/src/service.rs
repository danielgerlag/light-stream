use std::sync::Arc;

use light_stream_core::{
    AdministrationIntent, AdministrationLifecycle, AdministrationOperation,
    AdministrationRequestId, Capability, CapabilityReport, CapabilitySupport, ClusterId, GroupId,
    HealthStatus, NodeDescriptor, NodeId, Permission, PrincipalId, ResourceScope, SecurityPolicy,
};
use light_stream_proto::{
    admit_replay_lease_from_wire, advance_retention_from_wire, bookmark_page_to_wire,
    bookmark_to_wire, bootstrap_from_wire, bootstrap_to_wire, capabilities_to_wire,
    checkpoint_cas_to_wire, checkpoint_to_wire, compare_and_set_checkpoint_from_wire,
    create_bookmark_from_wire, create_stream_bookmark_from_wire, create_stream_from_wire,
    delete_bookmark_from_wire, delete_stream_bookmark_from_wire, domain_error_to_wire,
    fetch_from_wire, fetch_protected_from_wire, fetch_to_wire, get_checkpoint_from_wire,
    get_replay_lease_from_wire, health_to_wire, list_bookmarks_from_wire,
    list_stream_bookmarks_from_wire, publish_batch_and_route_from_wire, publish_probe_from_wire,
    publish_receipt_to_wire, receipt_from_wire, release_replay_lease_from_wire,
    renew_replay_lease_from_wire, replay_lease_to_wire, resolve_bookmark_from_wire,
    resolve_stream_bookmark_from_wire, retention_result_to_wire, retention_status_from_wire,
    retention_status_to_wire, route_to_wire, stream_bookmark_page_to_wire, stream_bookmark_to_wire,
    stream_selector_from_wire, stream_to_wire, unsupported_publish_to_wire,
    v1::{self, light_stream_server::LightStream},
};
use tonic::{Request, Response, Status};

use crate::{
    BUILD_REVISION,
    lifecycle::LifecycleController,
    runtime::ClusterManager,
    security::{Permit, RuntimeSecurityConfig, action},
};

pub struct PublicApi {
    cluster: Arc<ClusterManager>,
    security: RuntimeSecurityConfig,
    public_address: String,
    peer_address: String,
    lifecycle: LifecycleController,
}

impl PublicApi {
    pub fn new(
        cluster: Arc<ClusterManager>,
        security: RuntimeSecurityConfig,
        public_address: String,
        peer_address: String,
        lifecycle: LifecycleController,
    ) -> Self {
        Self {
            cluster,
            security,
            public_address,
            peer_address,
            lifecycle,
        }
    }

    fn capabilities(&self) -> Vec<CapabilityReport> {
        [
            Capability::Health,
            Capability::Bootstrap,
            Capability::Publish,
            Capability::Fetch,
            Capability::Receipt,
            Capability::Diagnostics,
            Capability::Bookmarks,
            Capability::Retention,
            Capability::ProtectedReplay,
            Capability::ConsumerCheckpoints,
            Capability::Security,
        ]
        .into_iter()
        .map(|capability| CapabilityReport::new(capability, CapabilitySupport::Available))
        .collect()
    }

    async fn policy_for_request(&self) -> Result<Option<SecurityPolicy>, Status> {
        if self.security.mode() == light_stream_core::SecurityMode::LocalInsecure {
            return Ok(None);
        }
        if self.cluster.identity().await.is_some() {
            if let Ok(policy) = self.cluster.confirmed_security_policy().await {
                self.security
                    .renew_policy(policy)
                    .map_err(|error| Status::unavailable(error.to_string()))?;
            }
            return self
                .security
                .current_policy()
                .map_err(|error| Status::unavailable(error.to_string()));
        }
        Ok(self.security.bootstrap_policy().cloned())
    }

    async fn admit<A, T>(
        &self,
        request: &Request<T>,
        permission: Permission,
        resource: ResourceScope,
        claimed_principal: Option<&PrincipalId>,
    ) -> Result<Permit<A>, Status> {
        let policy = self.policy_for_request().await?;
        let permit = self.security.authorize(
            request.metadata(),
            policy.as_ref(),
            permission,
            &resource,
            claimed_principal,
        )?;
        let _ = permit.principal();
        Ok(permit)
    }

    async fn configured_cluster_scope(&self) -> ResourceScope {
        let cluster = match self.security.configured_cluster() {
            Some(cluster) => cluster,
            None => self
                .cluster
                .identity()
                .await
                .unwrap_or_else(|| ClusterId::from_uuid(uuid::Uuid::nil())),
        };
        ResourceScope::Cluster { cluster }
    }

    async fn reauthorize<A, B>(
        &self,
        permit: &Permit<A>,
        permission: Permission,
        resource: ResourceScope,
    ) -> Result<Permit<B>, Status> {
        let policy = self.policy_for_request().await?;
        self.security
            .reauthorize(permit, policy.as_ref(), permission, &resource)
    }
}

#[tonic::async_trait]
impl LightStream for PublicApi {
    async fn health(
        &self,
        request: Request<v1::HealthRequest>,
    ) -> Result<Response<v1::HealthResponse>, Status> {
        let resource = self.configured_cluster_scope().await;
        let _permit = self
            .admit::<action::ClusterObserve, _>(
                &request,
                Permission::ClusterObserve,
                resource,
                None,
            )
            .await?;
        let cluster_id = self.cluster.identity().await;
        let operational = self.lifecycle.snapshot();
        Ok(Response::new(health_to_wire(
            &HealthStatus::new(true, BUILD_REVISION, self.security.mode()).with_operational(
                operational.phase,
                operational.generation,
                operational.readiness,
            ),
            &self.public_address,
            &self.peer_address,
            cluster_id.is_some(),
            cluster_id,
        )))
    }

    async fn capabilities(
        &self,
        request: Request<v1::CapabilitiesRequest>,
    ) -> Result<Response<v1::CapabilitiesResponse>, Status> {
        let resource = self.configured_cluster_scope().await;
        let _permit = self
            .admit::<action::ClusterObserve, _>(
                &request,
                Permission::ClusterObserve,
                resource,
                None,
            )
            .await?;
        Ok(Response::new(capabilities_to_wire(
            BUILD_REVISION,
            &self.capabilities(),
        )))
    }

    async fn bootstrap(
        &self,
        request: Request<v1::BootstrapRequest>,
    ) -> Result<Response<v1::BootstrapResponse>, Status> {
        let cluster = request
            .get_ref()
            .cluster_id
            .parse::<ClusterId>()
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let _permit = self
            .admit::<action::ClusterBootstrap, _>(
                &request,
                Permission::ClusterBootstrap,
                ResourceScope::Cluster { cluster },
                None,
            )
            .await?;
        let spec = bootstrap_from_wire(request.into_inner())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let result = match self.security.bootstrap_policy() {
            Some(policy) => self.cluster.bootstrap_secured(spec, policy.clone()).await,
            None => self.cluster.bootstrap(spec).await,
        };
        let response = match result {
            Ok(result) => bootstrap_to_wire(&result),
            Err(error) => v1::BootstrapResponse {
                result: Some(v1::bootstrap_response::Result::Error(domain_error_to_wire(
                    &error,
                ))),
            },
        };
        Ok(Response::new(response))
    }

    async fn create_stream(
        &self,
        request: Request<v1::CreateStreamRequest>,
    ) -> Result<Response<v1::StreamResponse>, Status> {
        let _permit = self
            .admit::<action::StreamCreate, _>(
                &request,
                Permission::StreamCreate,
                all_streams_scope(&request.get_ref().cluster_id)?,
                None,
            )
            .await?;
        let (cluster, spec) = create_stream_from_wire(request.into_inner())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let result = match self.cluster.create_stream(cluster, spec).await {
            Ok(value) => v1::stream_response::Result::Stream(stream_to_wire(&value)),
            Err(error) => v1::stream_response::Result::Error(domain_error_to_wire(&error)),
        };
        Ok(Response::new(v1::StreamResponse {
            result: Some(result),
        }))
    }

    async fn describe_stream(
        &self,
        request: Request<v1::DescribeStreamRequest>,
    ) -> Result<Response<v1::StreamResponse>, Status> {
        let discover = if request.get_ref().stream_id.is_empty() {
            Some(
                self.admit::<action::StreamDiscover, _>(
                    &request,
                    Permission::StreamDiscover,
                    all_streams_scope(&request.get_ref().cluster_id)?,
                    None,
                )
                .await?,
            )
        } else {
            let _permit = self
                .admit::<action::StreamDescribe, _>(
                    &request,
                    Permission::StreamDescribe,
                    stream_scope(&request.get_ref().cluster_id, &request.get_ref().stream_id)?,
                    None,
                )
                .await?;
            None
        };
        let request = request.into_inner();
        let (cluster, stream_id, name) =
            stream_selector_from_wire(request.cluster_id, request.stream_id, request.stream_name)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let result = match self.cluster.describe_stream(cluster, stream_id, name).await {
            Ok(value) => {
                if let Some(discover) = discover {
                    let _permit = self
                        .reauthorize::<action::StreamDiscover, action::StreamDescribe>(
                            &discover,
                            Permission::StreamDescribe,
                            ResourceScope::Stream {
                                cluster,
                                stream: value.stream(),
                            },
                        )
                        .await?;
                }
                v1::stream_response::Result::Stream(stream_to_wire(&value))
            }
            Err(error) => v1::stream_response::Result::Error(domain_error_to_wire(&error)),
        };
        Ok(Response::new(v1::StreamResponse {
            result: Some(result),
        }))
    }

    async fn list_streams(
        &self,
        request: Request<v1::ListStreamsRequest>,
    ) -> Result<Response<v1::ListStreamsResponse>, Status> {
        let _permit = self
            .admit::<action::StreamDescribe, _>(
                &request,
                Permission::StreamDescribe,
                all_streams_scope(&request.get_ref().cluster_id)?,
                None,
            )
            .await?;
        let cluster = request.into_inner().cluster_id.parse().map_err(
            |error: light_stream_core::DomainError| Status::invalid_argument(error.to_string()),
        )?;
        let result = match self.cluster.list_streams(cluster).await {
            Ok(values) => v1::list_streams_response::Result::Success(v1::StreamList {
                streams: values.iter().map(stream_to_wire).collect(),
            }),
            Err(error) => v1::list_streams_response::Result::Error(domain_error_to_wire(&error)),
        };
        Ok(Response::new(v1::ListStreamsResponse {
            result: Some(result),
        }))
    }

    async fn delete_stream(
        &self,
        request: Request<v1::DeleteStreamRequest>,
    ) -> Result<Response<v1::StreamResponse>, Status> {
        let _permit = self
            .admit::<action::StreamDelete, _>(
                &request,
                Permission::StreamDelete,
                stream_scope(&request.get_ref().cluster_id, &request.get_ref().stream_id)?,
                None,
            )
            .await?;
        let request = request.into_inner();
        let cluster =
            request
                .cluster_id
                .parse()
                .map_err(|error: light_stream_core::DomainError| {
                    Status::invalid_argument(error.to_string())
                })?;
        let stream =
            request
                .stream_id
                .parse()
                .map_err(|error: light_stream_core::DomainError| {
                    Status::invalid_argument(error.to_string())
                })?;
        let result = match self.cluster.delete_stream(cluster, stream).await {
            Ok(value) => v1::stream_response::Result::Stream(stream_to_wire(&value)),
            Err(error) => v1::stream_response::Result::Error(domain_error_to_wire(&error)),
        };
        Ok(Response::new(v1::StreamResponse {
            result: Some(result),
        }))
    }

    async fn resolve_route(
        &self,
        request: Request<v1::RouteRequest>,
    ) -> Result<Response<v1::RouteResponse>, Status> {
        let discover = if request.get_ref().stream_id.is_empty() {
            Some(
                self.admit::<action::StreamDiscover, _>(
                    &request,
                    Permission::StreamDiscover,
                    all_streams_scope(&request.get_ref().cluster_id)?,
                    None,
                )
                .await?,
            )
        } else {
            let _permit = self
                .admit::<action::RouteResolve, _>(
                    &request,
                    Permission::RouteResolve,
                    stream_scope(&request.get_ref().cluster_id, &request.get_ref().stream_id)?,
                    None,
                )
                .await?;
            None
        };
        let request = request.into_inner();
        let partition = light_stream_core::PartitionId::new(request.partition_id);
        let (cluster, stream_id, name) =
            stream_selector_from_wire(request.cluster_id, request.stream_id, request.stream_name)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let result = match self
            .cluster
            .route(cluster, stream_id, name, partition)
            .await
        {
            Ok(route) => {
                if let Some(discover) = discover {
                    let _permit = self
                        .reauthorize::<action::StreamDiscover, action::RouteResolve>(
                            &discover,
                            Permission::RouteResolve,
                            ResourceScope::Stream {
                                cluster,
                                stream: route.stream(),
                            },
                        )
                        .await?;
                }
                v1::route_response::Result::Route(route_to_wire(
                    &route,
                    self.cluster.route_leader(route.group()).await.as_ref(),
                ))
            }
            Err(error) => v1::route_response::Result::Error(domain_error_to_wire(&error)),
        };
        Ok(Response::new(v1::RouteResponse {
            result: Some(result),
        }))
    }

    async fn publish(
        &self,
        request: Request<v1::PublishRequest>,
    ) -> Result<Response<v1::PublishResponse>, Status> {
        let claimed = producer_principal(request.get_ref().request_id.as_ref())?;
        let _permit = self
            .admit::<action::Publish, _>(
                &request,
                Permission::Publish,
                stream_scope(&request.get_ref().cluster_id, &request.get_ref().stream_id)?,
                claimed.as_ref(),
            )
            .await?;
        publish_probe_from_wire(request.into_inner())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        Ok(Response::new(unsupported_publish_to_wire()))
    }

    async fn commit_publish(
        &self,
        request: Request<v1::PublishRequest>,
    ) -> Result<Response<v1::PublishResponse>, Status> {
        let claimed = producer_principal(request.get_ref().request_id.as_ref())?;
        let _permit = self
            .admit::<action::Publish, _>(
                &request,
                Permission::Publish,
                stream_scope(&request.get_ref().cluster_id, &request.get_ref().stream_id)?,
                claimed.as_ref(),
            )
            .await?;
        let (batch, group, revision) = publish_batch_and_route_from_wire(request.into_inner())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let response = match self.cluster.publish(batch, group, revision).await {
            Ok(receipt) => v1::PublishResponse {
                result: Some(v1::publish_response::Result::Success(
                    publish_receipt_to_wire(&receipt),
                )),
            },
            Err(error) => v1::PublishResponse {
                result: Some(v1::publish_response::Result::Error(domain_error_to_wire(
                    &error,
                ))),
            },
        };
        Ok(Response::new(response))
    }

    async fn fetch(
        &self,
        request: Request<v1::FetchRequest>,
    ) -> Result<Response<v1::FetchResponse>, Status> {
        let _permit = self
            .admit::<action::Fetch, _>(
                &request,
                Permission::Fetch,
                stream_scope(&request.get_ref().cluster_id, &request.get_ref().stream_id)?,
                None,
            )
            .await?;
        let (cluster, partition, offset, limit, group, revision) =
            fetch_from_wire(request.into_inner())
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let response = match self
            .cluster
            .fetch(cluster, partition, offset, limit, group, revision)
            .await
        {
            Ok(page) => fetch_to_wire(&page),
            Err(error) => v1::FetchResponse {
                result: Some(v1::fetch_response::Result::Error(domain_error_to_wire(
                    &error,
                ))),
            },
        };
        Ok(Response::new(response))
    }

    async fn get_receipt(
        &self,
        request: Request<v1::ReceiptRequest>,
    ) -> Result<Response<v1::ReceiptResponse>, Status> {
        let claimed = producer_principal(request.get_ref().request_id.as_ref())?;
        let _permit = self
            .admit::<action::ReceiptRead, _>(
                &request,
                Permission::ReceiptRead,
                stream_scope(&request.get_ref().cluster_id, &request.get_ref().stream_id)?,
                claimed.as_ref(),
            )
            .await?;
        let (cluster, partition, request_id, group, revision) =
            receipt_from_wire(request.into_inner())
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let response = match self
            .cluster
            .receipt(cluster, partition, &request_id, group, revision)
            .await
        {
            Ok(receipt) => v1::ReceiptResponse {
                result: Some(v1::receipt_response::Result::Success(
                    publish_receipt_to_wire(&receipt),
                )),
            },
            Err(error) => v1::ReceiptResponse {
                result: Some(v1::receipt_response::Result::Error(domain_error_to_wire(
                    &error,
                ))),
            },
        };
        Ok(Response::new(response))
    }

    async fn create_bookmark(
        &self,
        request: Request<v1::CreateBookmarkRequest>,
    ) -> Result<Response<v1::BookmarkResponse>, Status> {
        let _permit = self
            .admit::<action::BookmarkManage, _>(
                &request,
                Permission::BookmarkManage,
                stream_scope(&request.get_ref().cluster_id, &request.get_ref().stream_id)?,
                None,
            )
            .await?;
        let (cluster, spec, group, revision) = create_bookmark_from_wire(request.into_inner())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let result = match self
            .cluster
            .create_bookmark(cluster, spec, group, revision)
            .await
        {
            Ok(bookmark) => v1::bookmark_response::Result::Bookmark(bookmark_to_wire(&bookmark)),
            Err(error) => v1::bookmark_response::Result::Error(domain_error_to_wire(&error)),
        };
        Ok(Response::new(v1::BookmarkResponse {
            result: Some(result),
        }))
    }

    async fn resolve_bookmark(
        &self,
        request: Request<v1::ResolveBookmarkRequest>,
    ) -> Result<Response<v1::BookmarkResponse>, Status> {
        let _permit = self
            .admit::<action::BookmarkRead, _>(
                &request,
                Permission::BookmarkRead,
                stream_scope(&request.get_ref().cluster_id, &request.get_ref().stream_id)?,
                None,
            )
            .await?;
        let (cluster, partition, name, group, revision) =
            resolve_bookmark_from_wire(request.into_inner())
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let result = match self
            .cluster
            .resolve_bookmark(cluster, partition, &name, group, revision)
            .await
        {
            Ok(bookmark) => v1::bookmark_response::Result::Bookmark(bookmark_to_wire(&bookmark)),
            Err(error) => v1::bookmark_response::Result::Error(domain_error_to_wire(&error)),
        };
        Ok(Response::new(v1::BookmarkResponse {
            result: Some(result),
        }))
    }

    async fn delete_bookmark(
        &self,
        request: Request<v1::DeleteBookmarkRequest>,
    ) -> Result<Response<v1::BookmarkResponse>, Status> {
        let _permit = self
            .admit::<action::BookmarkManage, _>(
                &request,
                Permission::BookmarkManage,
                stream_scope(&request.get_ref().cluster_id, &request.get_ref().stream_id)?,
                None,
            )
            .await?;
        let (cluster, partition, id, group, revision) =
            delete_bookmark_from_wire(request.into_inner())
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let result = match self
            .cluster
            .delete_bookmark(cluster, partition, id, group, revision)
            .await
        {
            Ok(bookmark) => v1::bookmark_response::Result::Bookmark(bookmark_to_wire(&bookmark)),
            Err(error) => v1::bookmark_response::Result::Error(domain_error_to_wire(&error)),
        };
        Ok(Response::new(v1::BookmarkResponse {
            result: Some(result),
        }))
    }

    async fn list_bookmarks(
        &self,
        request: Request<v1::ListBookmarksRequest>,
    ) -> Result<Response<v1::ListBookmarksResponse>, Status> {
        let _permit = self
            .admit::<action::BookmarkRead, _>(
                &request,
                Permission::BookmarkRead,
                stream_scope(&request.get_ref().cluster_id, &request.get_ref().stream_id)?,
                None,
            )
            .await?;
        let (cluster, page_request, group, revision) =
            list_bookmarks_from_wire(request.into_inner())
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
        match self
            .cluster
            .list_bookmarks(cluster, &page_request, group, revision)
            .await
        {
            Ok(page) => Ok(Response::new(bookmark_page_to_wire(&page))),
            Err(error) => Ok(Response::new(v1::ListBookmarksResponse {
                result: Some(v1::list_bookmarks_response::Result::Error(
                    domain_error_to_wire(&error),
                )),
            })),
        }
    }

    async fn create_stream_bookmark(
        &self,
        request: Request<v1::CreateStreamBookmarkRequest>,
    ) -> Result<Response<v1::StreamBookmarkResponse>, Status> {
        let _permit = self
            .admit::<action::BookmarkManage, _>(
                &request,
                Permission::BookmarkManage,
                stream_scope(&request.get_ref().cluster_id, &request.get_ref().stream_id)?,
                None,
            )
            .await?;
        let (cluster, id, name, vector) = create_stream_bookmark_from_wire(request.into_inner())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let result = match self
            .cluster
            .create_stream_bookmark(cluster, id, name, vector)
            .await
        {
            Ok(bookmark) => {
                v1::stream_bookmark_response::Result::Bookmark(stream_bookmark_to_wire(&bookmark))
            }
            Err(error) => v1::stream_bookmark_response::Result::Error(domain_error_to_wire(&error)),
        };
        Ok(Response::new(v1::StreamBookmarkResponse {
            result: Some(result),
        }))
    }

    async fn resolve_stream_bookmark(
        &self,
        request: Request<v1::ResolveStreamBookmarkRequest>,
    ) -> Result<Response<v1::StreamBookmarkResponse>, Status> {
        let _permit = self
            .admit::<action::BookmarkRead, _>(
                &request,
                Permission::BookmarkRead,
                stream_scope(&request.get_ref().cluster_id, &request.get_ref().stream_id)?,
                None,
            )
            .await?;
        let (cluster, stream, name) = resolve_stream_bookmark_from_wire(request.into_inner())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let result = match self
            .cluster
            .resolve_stream_bookmark(cluster, stream, &name)
            .await
        {
            Ok(bookmark) => {
                v1::stream_bookmark_response::Result::Bookmark(stream_bookmark_to_wire(&bookmark))
            }
            Err(error) => v1::stream_bookmark_response::Result::Error(domain_error_to_wire(&error)),
        };
        Ok(Response::new(v1::StreamBookmarkResponse {
            result: Some(result),
        }))
    }

    async fn delete_stream_bookmark(
        &self,
        request: Request<v1::DeleteStreamBookmarkRequest>,
    ) -> Result<Response<v1::StreamBookmarkResponse>, Status> {
        let _permit = self
            .admit::<action::BookmarkManage, _>(
                &request,
                Permission::BookmarkManage,
                stream_scope(&request.get_ref().cluster_id, &request.get_ref().stream_id)?,
                None,
            )
            .await?;
        let (cluster, stream, id) = delete_stream_bookmark_from_wire(request.into_inner())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let result = match self
            .cluster
            .delete_stream_bookmark(cluster, stream, id)
            .await
        {
            Ok(bookmark) => {
                v1::stream_bookmark_response::Result::Bookmark(stream_bookmark_to_wire(&bookmark))
            }
            Err(error) => v1::stream_bookmark_response::Result::Error(domain_error_to_wire(&error)),
        };
        Ok(Response::new(v1::StreamBookmarkResponse {
            result: Some(result),
        }))
    }

    async fn list_stream_bookmarks(
        &self,
        request: Request<v1::ListStreamBookmarksRequest>,
    ) -> Result<Response<v1::ListStreamBookmarksResponse>, Status> {
        let _permit = self
            .admit::<action::BookmarkRead, _>(
                &request,
                Permission::BookmarkRead,
                stream_scope(&request.get_ref().cluster_id, &request.get_ref().stream_id)?,
                None,
            )
            .await?;
        let request = list_stream_bookmarks_from_wire(request.into_inner())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        match self.cluster.list_stream_bookmarks(&request).await {
            Ok(page) => Ok(Response::new(stream_bookmark_page_to_wire(&page))),
            Err(error) => Ok(Response::new(v1::ListStreamBookmarksResponse {
                result: Some(v1::list_stream_bookmarks_response::Result::Error(
                    domain_error_to_wire(&error),
                )),
            })),
        }
    }

    async fn advance_retention(
        &self,
        request: Request<v1::AdvanceRetentionRequest>,
    ) -> Result<Response<v1::AdvanceRetentionResponse>, Status> {
        let claimed = mutation_principal(request.get_ref().request_id.as_ref())?;
        let _permit = self
            .admit::<action::RetentionManage, _>(
                &request,
                Permission::RetentionManage,
                stream_scope(&request.get_ref().cluster_id, &request.get_ref().stream_id)?,
                claimed.as_ref(),
            )
            .await?;
        let (cluster, request, group, revision) = advance_retention_from_wire(request.into_inner())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let result = match self
            .cluster
            .advance_retention(cluster, request, group, revision)
            .await
        {
            Ok(result) => {
                v1::advance_retention_response::Result::Success(retention_result_to_wire(&result))
            }
            Err(error) => {
                v1::advance_retention_response::Result::Error(domain_error_to_wire(&error))
            }
        };
        Ok(Response::new(v1::AdvanceRetentionResponse {
            result: Some(result),
        }))
    }

    async fn get_retention_status(
        &self,
        request: Request<v1::RetentionStatusRequest>,
    ) -> Result<Response<v1::RetentionStatusResponse>, Status> {
        let _permit = self
            .admit::<action::RetentionRead, _>(
                &request,
                Permission::RetentionRead,
                stream_scope(&request.get_ref().cluster_id, &request.get_ref().stream_id)?,
                None,
            )
            .await?;
        let (cluster, partition, group, revision) =
            retention_status_from_wire(request.into_inner())
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let result = match self
            .cluster
            .retention_status(cluster, partition, group, revision)
            .await
        {
            Ok(status) => {
                v1::retention_status_response::Result::Status(retention_status_to_wire(&status))
            }
            Err(error) => {
                v1::retention_status_response::Result::Error(domain_error_to_wire(&error))
            }
        };
        Ok(Response::new(v1::RetentionStatusResponse {
            result: Some(result),
        }))
    }

    async fn admit_replay_lease(
        &self,
        request: Request<v1::AdmitReplayLeaseRequest>,
    ) -> Result<Response<v1::ReplayLeaseResponse>, Status> {
        let range = request
            .get_ref()
            .range
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("replay range is required"))?;
        let claimed = mutation_principal(request.get_ref().request_id.as_ref())?;
        let _permit = self
            .admit::<action::ReplayManage, _>(
                &request,
                Permission::ReplayManage,
                stream_scope(&request.get_ref().cluster_id, &range.stream_id)?,
                claimed.as_ref(),
            )
            .await?;
        let (cluster, request, group, revision) =
            admit_replay_lease_from_wire(request.into_inner())
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let result = match self
            .cluster
            .admit_replay_lease(cluster, request, group, revision)
            .await
        {
            Ok(lease) => v1::replay_lease_response::Result::Lease(replay_lease_to_wire(&lease)),
            Err(error) => v1::replay_lease_response::Result::Error(domain_error_to_wire(&error)),
        };
        Ok(Response::new(v1::ReplayLeaseResponse {
            result: Some(result),
        }))
    }

    async fn renew_replay_lease(
        &self,
        request: Request<v1::RenewReplayLeaseRequest>,
    ) -> Result<Response<v1::ReplayLeaseResponse>, Status> {
        let claimed = mutation_principal(request.get_ref().request_id.as_ref())?;
        let _permit = self
            .admit::<action::ReplayManage, _>(
                &request,
                Permission::ReplayManage,
                stream_scope(&request.get_ref().cluster_id, &request.get_ref().stream_id)?,
                claimed.as_ref(),
            )
            .await?;
        let (cluster, request, group, revision) =
            renew_replay_lease_from_wire(request.into_inner())
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let result = match self
            .cluster
            .renew_replay_lease(cluster, request, group, revision)
            .await
        {
            Ok(lease) => v1::replay_lease_response::Result::Lease(replay_lease_to_wire(&lease)),
            Err(error) => v1::replay_lease_response::Result::Error(domain_error_to_wire(&error)),
        };
        Ok(Response::new(v1::ReplayLeaseResponse {
            result: Some(result),
        }))
    }

    async fn release_replay_lease(
        &self,
        request: Request<v1::ReleaseReplayLeaseRequest>,
    ) -> Result<Response<v1::ReplayLeaseResponse>, Status> {
        let claimed = mutation_principal(request.get_ref().request_id.as_ref())?;
        let _permit = self
            .admit::<action::ReplayManage, _>(
                &request,
                Permission::ReplayManage,
                stream_scope(&request.get_ref().cluster_id, &request.get_ref().stream_id)?,
                claimed.as_ref(),
            )
            .await?;
        let (cluster, request, group, revision) =
            release_replay_lease_from_wire(request.into_inner())
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let result = match self
            .cluster
            .release_replay_lease(cluster, request, group, revision)
            .await
        {
            Ok(lease) => v1::replay_lease_response::Result::Lease(replay_lease_to_wire(&lease)),
            Err(error) => v1::replay_lease_response::Result::Error(domain_error_to_wire(&error)),
        };
        Ok(Response::new(v1::ReplayLeaseResponse {
            result: Some(result),
        }))
    }

    async fn get_replay_lease(
        &self,
        request: Request<v1::GetReplayLeaseRequest>,
    ) -> Result<Response<v1::ReplayLeaseResponse>, Status> {
        let permit = self
            .admit::<action::ReplayRead, _>(
                &request,
                Permission::ReplayRead,
                stream_scope(&request.get_ref().cluster_id, &request.get_ref().stream_id)?,
                None,
            )
            .await?;
        let (cluster, partition, lease, group, revision) =
            get_replay_lease_from_wire(request.into_inner())
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let result = match self
            .cluster
            .replay_lease(cluster, partition, lease, group, revision)
            .await
        {
            Ok(lease)
                if permit.principal().is_some_and(|principal| {
                    principal != lease.request().request().principal()
                }) =>
            {
                return Err(Status::permission_denied(
                    "replay lease belongs to another principal",
                ));
            }
            Ok(lease) => v1::replay_lease_response::Result::Lease(replay_lease_to_wire(&lease)),
            Err(error) => v1::replay_lease_response::Result::Error(domain_error_to_wire(&error)),
        };
        Ok(Response::new(v1::ReplayLeaseResponse {
            result: Some(result),
        }))
    }

    async fn fetch_protected(
        &self,
        request: Request<v1::FetchProtectedRequest>,
    ) -> Result<Response<v1::FetchResponse>, Status> {
        let permit = self
            .admit::<action::ReplayRead, _>(
                &request,
                Permission::ReplayRead,
                stream_scope(&request.get_ref().cluster_id, &request.get_ref().stream_id)?,
                None,
            )
            .await?;
        let (request, group, revision) = fetch_protected_from_wire(request.into_inner())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let lease = self
            .cluster
            .replay_lease(
                request.cluster(),
                request.partition(),
                request.lease(),
                group,
                revision,
            )
            .await
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        if permit
            .principal()
            .is_some_and(|principal| principal != lease.request().request().principal())
        {
            return Err(Status::permission_denied(
                "replay lease belongs to another principal",
            ));
        }
        let response = match self.cluster.fetch_protected(request, group, revision).await {
            Ok(page) => fetch_to_wire(&page),
            Err(error) => v1::FetchResponse {
                result: Some(v1::fetch_response::Result::Error(domain_error_to_wire(
                    &error,
                ))),
            },
        };
        Ok(Response::new(response))
    }

    async fn get_checkpoint(
        &self,
        request: Request<v1::GetCheckpointRequest>,
    ) -> Result<Response<v1::GetCheckpointResponse>, Status> {
        let key = request
            .get_ref()
            .key
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("checkpoint key is required"))?;
        let _permit = self
            .admit::<action::CheckpointRead, _>(
                &request,
                Permission::CheckpointRead,
                stream_scope(&key.cluster_id, &key.stream_id)?,
                None,
            )
            .await?;
        let (key, group, revision) = get_checkpoint_from_wire(request.into_inner())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let result = match self.cluster.checkpoint(key, group, revision).await {
            Ok(checkpoint) => {
                v1::get_checkpoint_response::Result::Checkpoint(checkpoint_to_wire(&checkpoint))
            }
            Err(error) => v1::get_checkpoint_response::Result::Error(domain_error_to_wire(&error)),
        };
        Ok(Response::new(v1::GetCheckpointResponse {
            result: Some(result),
        }))
    }

    async fn compare_and_set_checkpoint(
        &self,
        request: Request<v1::CompareAndSetCheckpointRequest>,
    ) -> Result<Response<v1::CompareAndSetCheckpointResponse>, Status> {
        let key = request
            .get_ref()
            .key
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("checkpoint key is required"))?;
        let claimed = mutation_principal(request.get_ref().request_id.as_ref())?;
        let _permit = self
            .admit::<action::CheckpointManage, _>(
                &request,
                Permission::CheckpointManage,
                stream_scope(&key.cluster_id, &key.stream_id)?,
                claimed.as_ref(),
            )
            .await?;
        let (mutation, group, revision) =
            compare_and_set_checkpoint_from_wire(request.into_inner())
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let result = match self
            .cluster
            .compare_and_set_checkpoint(mutation, group, revision)
            .await
        {
            Ok(result) => checkpoint_cas_to_wire(&result),
            Err(error) => {
                v1::compare_and_set_checkpoint_response::Result::Error(domain_error_to_wire(&error))
            }
        };
        Ok(Response::new(v1::CompareAndSetCheckpointResponse {
            result: Some(result),
        }))
    }

    async fn get_security_policy(
        &self,
        request: Request<v1::GetSecurityPolicyRequest>,
    ) -> Result<Response<v1::SecurityPolicyResponse>, Status> {
        let _permit = self
            .admit::<action::SecurityObserve, _>(
                &request,
                Permission::SecurityObserve,
                cluster_scope(&request.get_ref().cluster_id)?,
                None,
            )
            .await?;
        let result = match self.cluster.confirmed_security_policy().await {
            Ok(policy) => {
                self.security
                    .renew_policy(policy.clone())
                    .map_err(|error| Status::unavailable(error.to_string()))?;
                v1::security_policy_response::Result::Policy(security_policy_summary(&policy))
            }
            Err(error) => v1::security_policy_response::Result::Error(domain_error_to_wire(&error)),
        };
        Ok(Response::new(v1::SecurityPolicyResponse {
            result: Some(result),
        }))
    }

    async fn apply_security_mutation(
        &self,
        request: Request<v1::ApplySecurityMutationRequest>,
    ) -> Result<Response<v1::SecurityPolicyResponse>, Status> {
        let mutation: light_stream_core::SecurityMutation =
            serde_json::from_str(&request.get_ref().mutation_json)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let claimed = mutation.request().principal().clone();
        let _permit = self
            .admit::<action::SecurityAdmin, _>(
                &request,
                Permission::SecurityAdmin,
                cluster_scope(&request.get_ref().cluster_id)?,
                Some(&claimed),
            )
            .await?;
        let cluster: ClusterId = request.get_ref().cluster_id.parse().map_err(
            |error: light_stream_core::DomainError| Status::invalid_argument(error.to_string()),
        )?;
        if cluster != self.security.configured_cluster().unwrap_or(cluster) {
            return Err(Status::permission_denied(
                "security mutation targets another cluster",
            ));
        }
        let result = match self.cluster.apply_security_mutation(mutation).await {
            Ok(policy) => {
                self.security
                    .renew_policy(policy.clone())
                    .map_err(|error| Status::unavailable(error.to_string()))?;
                v1::security_policy_response::Result::Policy(security_policy_summary(&policy))
            }
            Err(error) => v1::security_policy_response::Result::Error(domain_error_to_wire(&error)),
        };
        Ok(Response::new(v1::SecurityPolicyResponse {
            result: Some(result),
        }))
    }

    async fn activate_secured_transport(
        &self,
        request: Request<v1::ActivateSecuredTransportRequest>,
    ) -> Result<Response<v1::SecurityPolicyResponse>, Status> {
        let claimed = mutation_principal(request.get_ref().request_id.as_ref())?;
        let _permit = self
            .admit::<action::SecurityAdmin, _>(
                &request,
                Permission::SecurityAdmin,
                cluster_scope(&request.get_ref().cluster_id)?,
                claimed.as_ref(),
            )
            .await?;
        let request = request.into_inner();
        let cluster: ClusterId =
            request
                .cluster_id
                .parse()
                .map_err(|error: light_stream_core::DomainError| {
                    Status::invalid_argument(error.to_string())
                })?;
        let policy: SecurityPolicy = serde_json::from_str(&request.policy_json)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let nodes = request
            .nodes
            .into_iter()
            .map(|node| {
                Ok(NodeDescriptor::new(
                    NodeId::new(node.node_id)?,
                    node.public_uri,
                    node.peer_uri,
                ))
            })
            .collect::<Result<Vec<_>, light_stream_core::DomainError>>()
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let mutation_request = light_stream_proto::mutation_request_id_from_wire(
            request
                .request_id
                .ok_or_else(|| Status::invalid_argument("request_id is required"))?,
        )
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let result = match self
            .cluster
            .activate_secured_transport(
                cluster,
                mutation_request,
                request.expected_topology_revision,
                nodes,
                policy,
            )
            .await
        {
            Ok(policy) => {
                v1::security_policy_response::Result::Policy(security_policy_summary(&policy))
            }
            Err(error) => v1::security_policy_response::Result::Error(domain_error_to_wire(&error)),
        };
        Ok(Response::new(v1::SecurityPolicyResponse {
            result: Some(result),
        }))
    }

    async fn diagnostics(
        &self,
        request: Request<v1::DiagnosticsRequest>,
    ) -> Result<Response<v1::DiagnosticsResponse>, Status> {
        let resource = self.configured_cluster_scope().await;
        let _permit = self
            .admit::<action::ClusterObserve, _>(
                &request,
                Permission::ClusterObserve,
                resource,
                None,
            )
            .await?;
        let diagnostic = self.cluster.diagnostics().await;
        Ok(Response::new(v1::DiagnosticsResponse {
            node_id: diagnostic.node_id,
            lifecycle: diagnostic.lifecycle,
            peers: diagnostic
                .peers
                .into_iter()
                .map(|peer| v1::NodeDescriptor {
                    node_id: peer.node_id().get(),
                    public_uri: peer.public_uri().to_owned(),
                    peer_uri: peer.peer_uri().to_owned(),
                })
                .collect(),
            groups: diagnostic
                .groups
                .into_iter()
                .map(|group| v1::GroupDiagnostics {
                    group: match group.group {
                        light_stream_core::ConsensusGroup::Control => {
                            v1::ConsensusGroup::Control as i32
                        }
                        light_stream_core::ConsensusGroup::Data => v1::ConsensusGroup::Data as i32,
                    },
                    group_id: group.group_id,
                    local_role: group.local_role,
                    has_current_leader: group.current_leader.is_some(),
                    current_leader_id: group.current_leader.unwrap_or_default(),
                    effective_uniform: group.effective_uniform,
                    effective_voters: group.effective_voters,
                    effective_learners: group.effective_learners,
                    committed_uniform: group.committed_uniform,
                    committed_voters: group.committed_voters,
                    committed_learners: group.committed_learners,
                    has_last_log: group.last_log_index.is_some(),
                    last_log_index: group.last_log_index.unwrap_or_default(),
                    has_local_committed: group.local_committed_index.is_some(),
                    local_committed_index: group.local_committed_index.unwrap_or_default(),
                    has_cluster_committed: group.cluster_committed_index.is_some(),
                    cluster_committed_index: group.cluster_committed_index.unwrap_or_default(),
                    has_last_applied: group.last_applied_index.is_some(),
                    last_applied_index: group.last_applied_index.unwrap_or_default(),
                    replication: group
                        .replication
                        .into_iter()
                        .map(|value| v1::ReplicationProgress {
                            target_node_id: value.target_node_id,
                            has_matched_log: value.matched_log_index.is_some(),
                            matched_log_index: value.matched_log_index.unwrap_or_default(),
                        })
                        .collect(),
                    has_snapshot: group.snapshot_index.is_some(),
                    snapshot_index: group.snapshot_index.unwrap_or_default(),
                    has_purged: group.purged_index.is_some(),
                    purged_index: group.purged_index.unwrap_or_default(),
                    has_slot: group.slot.is_some(),
                    slot: group.slot.unwrap_or_default().into(),
                    cache_budget_bytes: group.cache_budget_bytes.unwrap_or_default() as u64,
                    write_buffer_budget_bytes: group.write_buffer_budget_bytes.unwrap_or_default()
                        as u64,
                })
                .collect(),
            unsupported_claims: diagnostic.unsupported_claims,
            data_group_slots: u32::from(diagnostic.data_group_slots),
            data_group_count: diagnostic.data_group_count as u32,
            rocksdb_cache_budget_bytes: diagnostic.rocksdb_cache_budget_bytes as u64,
            rocksdb_write_buffer_budget_bytes: diagnostic.rocksdb_write_buffer_budget_bytes as u64,
            per_group_cache_bytes: diagnostic.per_group_cache_bytes as u64,
            per_group_write_buffer_bytes: diagnostic.per_group_write_buffer_bytes as u64,
        }))
    }

    async fn snapshot_group(
        &self,
        request: Request<v1::SnapshotGroupRequest>,
    ) -> Result<Response<v1::SnapshotGroupResponse>, Status> {
        let _permit = self
            .admit::<action::SnapshotManage, _>(
                &request,
                Permission::SnapshotManage,
                cluster_scope(&request.get_ref().cluster_id)?,
                None,
            )
            .await?;
        let request = request.into_inner();
        let result = match request.cluster_id.parse::<light_stream_core::ClusterId>() {
            Ok(cluster) => {
                self.cluster
                    .snapshot_group(cluster, request.group_id, request.purge)
                    .await
            }
            Err(error) => Err(error),
        };
        Ok(Response::new(match result {
            Ok(snapshot) => v1::SnapshotGroupResponse {
                result: Some(v1::snapshot_group_response::Result::Success(
                    v1::SnapshotGroupResult {
                        group_id: snapshot.group_id,
                        snapshot_index: snapshot.snapshot_index,
                        purged: snapshot.purged_index.is_some(),
                        purged_index: snapshot.purged_index.unwrap_or_default(),
                    },
                )),
            },
            Err(error) => v1::SnapshotGroupResponse {
                result: Some(v1::snapshot_group_response::Result::Error(
                    domain_error_to_wire(&error),
                )),
            },
        }))
    }

    async fn replace_voter(
        &self,
        request: Request<v1::ReplaceVoterRequest>,
    ) -> Result<Response<v1::AdministrationResponse>, Status> {
        let _permit = self
            .admit::<action::ClusterAdmin, _>(
                &request,
                Permission::ClusterAdmin,
                cluster_scope(&request.get_ref().cluster_id)?,
                None,
            )
            .await?;
        let request = request.into_inner();
        let result = (|| {
            let add = request.add_node.ok_or_else(|| {
                light_stream_core::DomainError::InvalidIdentity {
                    kind: "replacement node".to_owned(),
                    reason: "node descriptor is required".to_owned(),
                }
            })?;
            Ok((
                request.cluster_id.parse::<ClusterId>()?,
                AdministrationIntent::ReplaceVoter {
                    request: request.request_id.parse::<AdministrationRequestId>()?,
                    expected_topology_revision: request.expected_topology_revision,
                    remove: NodeId::new(request.remove_node_id)?,
                    add: NodeDescriptor::new(
                        NodeId::new(add.node_id)?,
                        add.public_uri,
                        add.peer_uri,
                    ),
                },
            ))
        })();
        let result = match result {
            Ok((cluster, intent)) => self.cluster.begin_administration(cluster, intent).await,
            Err(error) => Err(error),
        };
        Ok(Response::new(administration_response(result)))
    }

    async fn transfer_leadership(
        &self,
        request: Request<v1::TransferLeadershipRequest>,
    ) -> Result<Response<v1::AdministrationResponse>, Status> {
        let _permit = self
            .admit::<action::ClusterAdmin, _>(
                &request,
                Permission::ClusterAdmin,
                cluster_scope(&request.get_ref().cluster_id)?,
                None,
            )
            .await?;
        let request = request.into_inner();
        let result = (|| {
            Ok((
                request.cluster_id.parse::<ClusterId>()?,
                AdministrationIntent::TransferLeader {
                    request: request.request_id.parse::<AdministrationRequestId>()?,
                    group: GroupId::new(request.group_id)?,
                    target: NodeId::new(request.target_node_id)?,
                },
            ))
        })();
        let result = match result {
            Ok((cluster, intent)) => self.cluster.begin_administration(cluster, intent).await,
            Err(error) => Err(error),
        };
        Ok(Response::new(administration_response(result)))
    }

    async fn get_administration(
        &self,
        request: Request<v1::AdministrationStatusRequest>,
    ) -> Result<Response<v1::AdministrationResponse>, Status> {
        let _permit = self
            .admit::<action::ClusterAdmin, _>(
                &request,
                Permission::ClusterAdmin,
                cluster_scope(&request.get_ref().cluster_id)?,
                None,
            )
            .await?;
        let request = request.into_inner();
        let result = match (
            request.cluster_id.parse::<ClusterId>(),
            request.request_id.parse::<AdministrationRequestId>(),
        ) {
            (Ok(cluster), Ok(request)) => {
                self.cluster
                    .administration_operation(cluster, request)
                    .await
            }
            (Err(error), _) | (_, Err(error)) => Err(error),
        };
        Ok(Response::new(administration_response(result)))
    }

    async fn abort_administration(
        &self,
        request: Request<v1::AbortAdministrationRequest>,
    ) -> Result<Response<v1::AdministrationResponse>, Status> {
        let _permit = self
            .admit::<action::ClusterAdmin, _>(
                &request,
                Permission::ClusterAdmin,
                cluster_scope(&request.get_ref().cluster_id)?,
                None,
            )
            .await?;
        let request = request.into_inner();
        let result = match (
            request.cluster_id.parse::<ClusterId>(),
            request.request_id.parse::<AdministrationRequestId>(),
        ) {
            (Ok(cluster), Ok(request)) => self.cluster.abort_administration(cluster, request).await,
            (Err(error), _) | (_, Err(error)) => Err(error),
        };
        Ok(Response::new(administration_response(result)))
    }
}

fn administration_response(
    result: Result<AdministrationOperation, light_stream_core::DomainError>,
) -> v1::AdministrationResponse {
    match result {
        Ok(operation) => {
            let (request_id, kind, remove_node_id, add_node, group_id, target_node_id) =
                match operation.intent() {
                    AdministrationIntent::ReplaceVoter {
                        request,
                        remove,
                        add,
                        ..
                    } => (
                        request.to_string(),
                        "replace_voter".to_owned(),
                        remove.get(),
                        Some(v1::NodeDescriptor {
                            node_id: add.node_id().get(),
                            public_uri: add.public_uri().to_owned(),
                            peer_uri: add.peer_uri().to_owned(),
                        }),
                        0,
                        0,
                    ),
                    AdministrationIntent::TransferLeader {
                        request,
                        group,
                        target,
                    } => (
                        request.to_string(),
                        "transfer_leader".to_owned(),
                        0,
                        None,
                        group.get(),
                        target.get(),
                    ),
                };
            let (lifecycle, topology_revision) = match operation.lifecycle() {
                AdministrationLifecycle::Pending => ("pending".to_owned(), 0),
                AdministrationLifecycle::Complete {
                    completed_topology_revision,
                } => ("complete".to_owned(), *completed_topology_revision),
                AdministrationLifecycle::Aborted {
                    completed_topology_revision,
                } => ("aborted".to_owned(), *completed_topology_revision),
            };
            v1::AdministrationResponse {
                result: Some(v1::administration_response::Result::Operation(
                    v1::AdministrationOperation {
                        request_id,
                        kind,
                        lifecycle,
                        topology_revision,
                        remove_node_id,
                        add_node,
                        group_id,
                        target_node_id,
                    },
                )),
            }
        }
        Err(error) => v1::AdministrationResponse {
            result: Some(v1::administration_response::Result::Error(
                domain_error_to_wire(&error),
            )),
        },
    }
}

fn cluster_scope(value: &str) -> Result<ResourceScope, Status> {
    Ok(ResourceScope::Cluster {
        cluster: value
            .parse()
            .map_err(|error: light_stream_core::DomainError| {
                Status::invalid_argument(error.to_string())
            })?,
    })
}

fn all_streams_scope(value: &str) -> Result<ResourceScope, Status> {
    Ok(ResourceScope::AllStreams {
        cluster: value
            .parse()
            .map_err(|error: light_stream_core::DomainError| {
                Status::invalid_argument(error.to_string())
            })?,
    })
}

fn stream_scope(cluster: &str, stream: &str) -> Result<ResourceScope, Status> {
    Ok(ResourceScope::Stream {
        cluster: cluster
            .parse()
            .map_err(|error: light_stream_core::DomainError| {
                Status::invalid_argument(error.to_string())
            })?,
        stream: stream
            .parse()
            .map_err(|error: light_stream_core::DomainError| {
                Status::invalid_argument(error.to_string())
            })?,
    })
}

fn producer_principal(
    value: Option<&v1::ProducerRequestId>,
) -> Result<Option<PrincipalId>, Status> {
    value
        .map(|value| {
            PrincipalId::parse(&value.principal_id)
                .map_err(|error| Status::invalid_argument(error.to_string()))
        })
        .transpose()
}

fn mutation_principal(
    value: Option<&v1::MutationRequestId>,
) -> Result<Option<PrincipalId>, Status> {
    value
        .map(|value| {
            PrincipalId::parse(&value.principal_id)
                .map_err(|error| Status::invalid_argument(error.to_string()))
        })
        .transpose()
}

fn security_policy_summary(policy: &SecurityPolicy) -> v1::SecurityPolicySummary {
    v1::SecurityPolicySummary {
        cluster_id: policy.cluster().to_string(),
        policy_revision: policy.revision().get(),
        revocation_revision: policy.revocation_revision().get(),
    }
}
