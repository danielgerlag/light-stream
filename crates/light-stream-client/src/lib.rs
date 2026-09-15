use std::{
    future::Future,
    time::{Duration, Instant},
};

use light_stream_core::{
    AmbiguousRequest, BookmarkId, BookmarkName, BookmarkPage, BookmarkPageRequest,
    BookmarkPublicationSequence, BootstrapResult, BootstrapSpec, Capability, CapabilityReport,
    CapabilitySupport, ClusterId, CommittedBookmark, CommittedRecord, CommittedRecordRange,
    CommittedStreamBookmark, ConsensusGroup, CreateStreamSpec, DomainError, FetchPage,
    HealthStatus, LeaderHint, LeaseRelease, LeaseRenewal, MAX_PUBLIC_MESSAGE_BYTES, NodeDescriptor,
    PartitionId, PartitionKey, PartitionRoute, ProducerRequestId, ProducerSessionId, PublishBatch,
    PublishProbe, PublishReceipt, RecordOffset, ReplayLease, ReplayLeaseId, ReplayLeaseRequest,
    RequestOutcome, RequestSequence, RetentionRequest, RetentionResult, RetentionStatus,
    SecurityMode, StreamBookmarkPage, StreamBookmarkPageRequest, StreamCursorVector,
    StreamDescriptor, StreamId, StreamName,
};
use light_stream_proto::{
    bookmark_from_wire, domain_error_from_wire, mutation_request_id_to_wire,
    replay_lease_from_wire, retention_result_from_wire, retention_status_from_response,
    route_from_wire, security_mode_from_wire, stream_bookmark_from_wire, stream_from_wire,
    v1::{
        self, administration_response, advance_retention_response, bookmark_response,
        bootstrap_response, fetch_response, light_stream_client::LightStreamClient,
        list_bookmarks_response, list_stream_bookmarks_response, list_streams_response,
        publish_response, receipt_response, replay_lease_response, retention_status_response,
        route_response, snapshot_group_response, stream_bookmark_response, stream_response,
    },
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tonic::{
    Code, Request, Response, Status,
    transport::{Channel, Endpoint},
};

const DEFAULT_DEADLINE: Duration = Duration::from_secs(5);

#[derive(Clone, Copy)]
struct Deadline {
    expires_at: Instant,
}

impl Deadline {
    fn after(duration: Duration) -> Self {
        Self {
            expires_at: Instant::now() + duration,
        }
    }

    fn remaining(self) -> Option<Duration> {
        self.expires_at
            .checked_duration_since(Instant::now())
            .filter(|remaining| !remaining.is_zero())
    }

    fn expired(self) -> bool {
        self.remaining().is_none()
    }
}

enum AttemptError {
    Deadline,
    Client(ClientError),
}

#[derive(Clone)]
enum BookmarkCall {
    Create(v1::CreateBookmarkRequest),
    Resolve(v1::ResolveBookmarkRequest),
    Delete(v1::DeleteBookmarkRequest),
}

#[derive(Clone)]
enum StreamBookmarkCall {
    Create(v1::CreateStreamBookmarkRequest),
    Resolve(v1::ResolveStreamBookmarkRequest),
    Delete(v1::DeleteStreamBookmarkRequest),
}

#[derive(Clone)]
enum ReplayMutationCall {
    Admit(v1::AdmitReplayLeaseRequest),
    Renew(v1::RenewReplayLeaseRequest),
    Release(v1::ReleaseReplayLeaseRequest),
}

#[derive(Clone)]
enum AdministrationCall {
    Replace(v1::ReplaceVoterRequest),
    Transfer(v1::TransferLeadershipRequest),
    Status(v1::AdministrationStatusRequest),
    Abort(v1::AbortAdministrationRequest),
}

impl AdministrationCall {
    const fn is_mutation(&self) -> bool {
        !matches!(self, Self::Status(_))
    }
}

impl ReplayMutationCall {
    fn set_route(&mut self, group: light_stream_core::GroupId, revision: u64) {
        match self {
            Self::Admit(request) => {
                request.route_group_id = group.get();
                request.route_revision = revision;
            }
            Self::Renew(request) => {
                request.route_group_id = group.get();
                request.route_revision = revision;
            }
            Self::Release(request) => {
                request.route_group_id = group.get();
                request.route_revision = revision;
            }
        }
    }
}

impl From<ClientError> for AttemptError {
    fn from(error: ClientError) -> Self {
        Self::Client(error)
    }
}

impl From<DomainError> for AttemptError {
    fn from(error: DomainError) -> Self {
        Self::Client(error.into())
    }
}

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("invalid endpoint {endpoint:?}: {reason}")]
    InvalidEndpoint { endpoint: String, reason: String },
    #[error("connection failed: {0}")]
    Connection(String),
    #[error("request failed: {0}")]
    Request(String),
    #[error("invalid server response: {0}")]
    Protocol(String),
    #[error(transparent)]
    Domain(#[from] DomainError),
}

impl ClientError {
    pub const fn exit_code(&self) -> i32 {
        match self {
            Self::Domain(DomainError::UnsupportedOperation { .. }) => 3,
            Self::Domain(
                DomainError::InvalidIdentity { .. }
                | DomainError::InvalidName { .. }
                | DomainError::InvalidRange { .. }
                | DomainError::InvalidPayload { .. },
            ) => 2,
            Self::Domain(
                DomainError::BootstrapConflict { .. }
                | DomainError::IdentityMismatch { .. }
                | DomainError::ReceiptConflict
                | DomainError::ReceiptExpired
                | DomainError::ReceiptNotFound
                | DomainError::StreamNotFound
                | DomainError::StreamNotActive
                | DomainError::StreamNameConflict
                | DomainError::BookmarkNotFound
                | DomainError::BookmarkNameConflict
                | DomainError::CursorExpired { .. }
                | DomainError::ReplayLeaseNotFound { .. }
                | DomainError::ReplayLeaseInactive { .. }
                | DomainError::ReplayLeaseConflict
                | DomainError::ReplayLeaseRangeViolation
                | DomainError::ReplayLeaseLifetimeExhausted
                | DomainError::MutationConflict
                | DomainError::MutationReceiptExpired
                | DomainError::ResourceLimit { .. }
                | DomainError::StaleRoute,
            ) => 4,
            Self::Domain(
                DomainError::NotBootstrapped
                | DomainError::Storage { .. }
                | DomainError::NotLeader { .. }
                | DomainError::QuorumUnavailable { .. }
                | DomainError::ClusterForming
                | DomainError::LeaseClockUnavailable,
            ) => 5,
            Self::InvalidEndpoint { .. } => 2,
            _ => 1,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ServerHealth {
    pub status: HealthStatus,
    pub public_address: String,
    pub peer_address: String,
    pub bootstrapped: bool,
    pub cluster_id: Option<ClusterId>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReplicationDiagnostics {
    pub target_node_id: u64,
    pub matched_log_index: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GroupDiagnostics {
    pub group: String,
    pub group_id: u64,
    pub local_role: String,
    pub current_leader: Option<u64>,
    pub effective_uniform: bool,
    pub effective_voters: Vec<u64>,
    pub effective_learners: Vec<u64>,
    pub committed_uniform: bool,
    pub committed_voters: Vec<u64>,
    pub committed_learners: Vec<u64>,
    pub last_log_index: Option<u64>,
    pub local_committed_index: Option<u64>,
    pub cluster_committed_index: Option<u64>,
    pub last_applied_index: Option<u64>,
    pub replication: Vec<ReplicationDiagnostics>,
    pub snapshot_index: Option<u64>,
    pub purged_index: Option<u64>,
    pub slot: Option<u16>,
    pub cache_budget_bytes: u64,
    pub write_buffer_budget_bytes: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct NodeDiagnostics {
    pub node_id: u64,
    pub lifecycle: String,
    pub peers: Vec<NodeDescriptor>,
    pub groups: Vec<GroupDiagnostics>,
    pub data_group_slots: u32,
    pub data_group_count: u32,
    pub rocksdb_cache_budget_bytes: u64,
    pub rocksdb_write_buffer_budget_bytes: u64,
    pub per_group_cache_bytes: u64,
    pub per_group_write_buffer_bytes: u64,
    pub unsupported_claims: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ResolvedRoute {
    pub route: PartitionRoute,
    pub leader: Option<LeaderHint>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SnapshotGroupResult {
    pub group_id: u64,
    pub snapshot_index: u64,
    pub purged_index: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AdministrationStatus {
    pub request_id: String,
    pub kind: String,
    pub lifecycle: String,
    pub topology_revision: u64,
    pub remove_node_id: Option<u64>,
    pub add_node: Option<NodeDescriptor>,
    pub group_id: Option<u64>,
    pub target_node_id: Option<u64>,
}

#[derive(Clone)]
pub struct Client {
    endpoint: String,
    seeds: Vec<String>,
    deadline: Duration,
    retry: bool,
}

impl Client {
    pub async fn connect(endpoint: impl Into<String>) -> Result<Self, ClientError> {
        Self::connect_with_options(endpoint, Vec::new(), DEFAULT_DEADLINE, true).await
    }

    pub async fn connect_with_options(
        endpoint: impl Into<String>,
        seeds: Vec<String>,
        deadline: Duration,
        retry: bool,
    ) -> Result<Self, ClientError> {
        let endpoint = endpoint.into();
        if deadline.is_zero() {
            return Err(ClientError::InvalidEndpoint {
                endpoint,
                reason: "deadline must be greater than zero".to_owned(),
            });
        }
        let mut all_seeds = vec![endpoint.clone()];
        for seed in seeds {
            validate_endpoint(&seed)?;
            if !all_seeds.contains(&seed) {
                all_seeds.push(seed);
            }
        }
        validate_endpoint(&endpoint)?;
        Ok(Self {
            endpoint,
            seeds: all_seeds,
            deadline,
            retry,
        })
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    async fn connect_initial(
        &self,
        deadline: Deadline,
    ) -> Result<LightStreamClient<Channel>, AttemptError> {
        let mut index = 0usize;
        loop {
            let endpoint = &self.seeds[index % self.seeds.len()];
            match connect_client(endpoint, deadline).await {
                Ok(client) => return Ok(client),
                Err(AttemptError::Client(ClientError::Connection(_))) if self.retry => {}
                Err(error) => return Err(error),
            }
            if deadline.expired() {
                return Err(AttemptError::Deadline);
            }
            index = index.wrapping_add(1);
        }
    }

    pub async fn health(&self) -> Result<ServerHealth, ClientError> {
        let deadline = Deadline::after(self.deadline);
        let mut client = self
            .connect_initial(deadline)
            .await
            .map_err(|error| request_attempt_error("health", error))?;
        let response = execute_rpc(deadline, v1::HealthRequest {}, |request| {
            client.health(request)
        })
        .await
        .map_err(|error| request_attempt_error("health", error))?;
        let security_mode = security_mode_from_wire(response.security_mode)?;
        Ok(ServerHealth {
            status: HealthStatus::new(response.ready, response.revision, security_mode),
            public_address: response.public_address,
            peer_address: response.peer_address,
            bootstrapped: response.bootstrapped,
            cluster_id: if response.cluster_id.is_empty() {
                None
            } else {
                Some(response.cluster_id.parse()?)
            },
        })
    }

    pub async fn capabilities(&self) -> Result<Vec<CapabilityReport>, ClientError> {
        let deadline = Deadline::after(self.deadline);
        let mut client = self
            .connect_initial(deadline)
            .await
            .map_err(|error| request_attempt_error("capabilities", error))?;
        let response = execute_rpc(deadline, v1::CapabilitiesRequest {}, |request| {
            client.capabilities(request)
        })
        .await
        .map_err(|error| request_attempt_error("capabilities", error))?;
        response
            .capabilities
            .into_iter()
            .map(|value| {
                let capability = Capability::parse(&value.name).ok_or_else(|| {
                    ClientError::Protocol(format!("unknown capability {:?}", value.name))
                })?;
                let support = match v1::CapabilityState::try_from(value.state).ok() {
                    Some(v1::CapabilityState::Available) => CapabilitySupport::Available,
                    Some(v1::CapabilityState::Unsupported) => CapabilitySupport::Unsupported {
                        available_phase: value.available_phase,
                    },
                    _ => {
                        return Err(ClientError::Protocol(format!(
                            "unknown capability state {}",
                            value.state
                        )));
                    }
                };
                Ok(CapabilityReport::new(capability, support))
            })
            .collect()
    }

    pub async fn publish_probe(&self, probe: PublishProbe) -> Result<PublishReceipt, ClientError> {
        let deadline = Deadline::after(self.deadline);
        let mut client = self
            .connect_initial(deadline)
            .await
            .map_err(|error| request_attempt_error("publish probe", error))?;
        let request = v1::PublishRequest {
            cluster_id: probe.cluster().to_string(),
            stream_id: probe.partition().stream().to_string(),
            partition_id: probe.partition().partition().get(),
            request_id: Some(v1::ProducerRequestId {
                principal_id: probe.request().principal().to_string(),
                producer_session_id: probe.request().session().to_string(),
                sequence: probe.request().sequence().get(),
            }),
            records: probe.records().to_vec(),
            route_group_id: 0,
            route_revision: 0,
            bookmark_name: String::new(),
        };
        let response = execute_rpc(deadline, request, |request| client.publish(request))
            .await
            .map_err(|error| request_attempt_error("publish probe", error))?;
        match response.result {
            Some(publish_response::Result::Unsupported(value)) => {
                Err(DomainError::UnsupportedOperation {
                    operation: value.operation,
                    available_phase: value.available_phase,
                }
                .into())
            }
            Some(publish_response::Result::Success(value)) => publish_success_from_wire(value),
            Some(publish_response::Result::Error(value)) => Err(decode_domain_error(value)?.into()),
            None => Err(ClientError::Protocol(
                "publish response omitted its typed result".to_owned(),
            )),
        }
    }

    pub async fn bootstrap(&self, spec: BootstrapSpec) -> Result<BootstrapResult, ClientError> {
        let deadline = Deadline::after(self.deadline);
        let mut client = self
            .connect_initial(deadline)
            .await
            .map_err(|error| request_attempt_error("bootstrap", error))?;
        let response = execute_rpc(
            deadline,
            v1::BootstrapRequest {
                cluster_id: spec.cluster().to_string(),
                stream_id: spec.stream().to_string(),
                stream_name: spec.stream_name().to_string(),
                seed_node_id: 0,
                members: Vec::new(),
            },
            |request| client.bootstrap(request),
        )
        .await
        .map_err(|error| request_attempt_error("bootstrap", error))?;
        match response.result {
            Some(bootstrap_response::Result::Success(value)) => Ok(BootstrapResult::new(
                value.cluster_id.parse()?,
                value.stream_id.parse()?,
                StreamName::parse(value.stream_name)?,
                light_stream_core::GroupId::new(value.control_group_id)?,
                light_stream_core::GroupId::new(value.data_group_id)?,
            )),
            Some(bootstrap_response::Result::Error(value)) => {
                Err(decode_domain_error(value)?.into())
            }
            None => Err(ClientError::Protocol(
                "bootstrap response omitted its typed result".to_owned(),
            )),
        }
    }

    pub async fn bootstrap_three_voter(
        &self,
        spec: BootstrapSpec,
        seed_node_id: u64,
        members: &[NodeDescriptor],
    ) -> Result<BootstrapResult, ClientError> {
        let deadline = Deadline::after(self.deadline);
        let mut client = self
            .connect_initial(deadline)
            .await
            .map_err(|error| request_attempt_error("bootstrap", error))?;
        let response = execute_rpc(
            deadline,
            v1::BootstrapRequest {
                cluster_id: spec.cluster().to_string(),
                stream_id: spec.stream().to_string(),
                stream_name: spec.stream_name().to_string(),
                seed_node_id,
                members: members
                    .iter()
                    .map(|member| v1::NodeDescriptor {
                        node_id: member.node_id().get(),
                        public_uri: member.public_uri().to_owned(),
                        peer_uri: member.peer_uri().to_owned(),
                    })
                    .collect(),
            },
            |request| client.bootstrap(request),
        )
        .await
        .map_err(|error| request_attempt_error("bootstrap", error))?;
        match response.result {
            Some(bootstrap_response::Result::Success(value)) => Ok(BootstrapResult::new(
                value.cluster_id.parse()?,
                value.stream_id.parse()?,
                StreamName::parse(value.stream_name)?,
                light_stream_core::GroupId::new(value.control_group_id)?,
                light_stream_core::GroupId::new(value.data_group_id)?,
            )),
            Some(bootstrap_response::Result::Error(value)) => {
                Err(decode_domain_error(value)?.into())
            }
            None => Err(ClientError::Protocol(
                "bootstrap response omitted its typed result".to_owned(),
            )),
        }
    }

    pub async fn create_stream(
        &self,
        cluster: ClusterId,
        spec: CreateStreamSpec,
    ) -> Result<StreamDescriptor, ClientError> {
        let request = v1::CreateStreamRequest {
            cluster_id: cluster.to_string(),
            request_id: spec.request_id().to_string(),
            stream_name: spec.name().to_string(),
            partition_count: spec.partition_count(),
        };
        let deadline = Deadline::after(self.deadline);
        let mut endpoints = self.seeds.clone();
        let mut index = 0usize;
        loop {
            let endpoint = endpoints[index % endpoints.len()].clone();
            let response = match connect_client(&endpoint, deadline).await {
                Ok(mut client) => {
                    execute_rpc(deadline, request.clone(), |value| {
                        client.create_stream(value)
                    })
                    .await
                }
                Err(error) => Err(error),
            };
            match response {
                Ok(value) => match value.result {
                    Some(stream_response::Result::Stream(stream)) => {
                        return Ok(stream_from_wire(stream)?);
                    }
                    Some(stream_response::Result::Error(error)) => {
                        let error = decode_domain_error(error)?;
                        if let DomainError::NotLeader { leader, .. } = &error
                            && self.retry
                        {
                            if let Some(leader) = leader {
                                index = add_hint(&mut endpoints, leader.public_uri())?;
                            }
                        } else {
                            return Err(error.into());
                        }
                    }
                    None => {
                        return Err(ClientError::Protocol(
                            "create stream response omitted its typed result".to_owned(),
                        ));
                    }
                },
                Err(AttemptError::Client(ClientError::Connection(_) | ClientError::Request(_)))
                    if self.retry =>
                {
                    index = index.wrapping_add(1);
                }
                Err(error) => return Err(request_attempt_error("create stream", error)),
            }
            retry_sleep(deadline)
                .await
                .map_err(|_| non_write_deadline_error())?;
        }
    }

    pub async fn describe_stream(
        &self,
        cluster: ClusterId,
        stream_id: Option<StreamId>,
        name: Option<&StreamName>,
    ) -> Result<StreamDescriptor, ClientError> {
        let deadline = Deadline::after(self.deadline);
        let mut client = self
            .connect_initial(deadline)
            .await
            .map_err(|error| request_attempt_error("describe stream", error))?;
        let response = execute_rpc(
            deadline,
            v1::DescribeStreamRequest {
                cluster_id: cluster.to_string(),
                stream_id: stream_id.map_or_else(String::new, |value| value.to_string()),
                stream_name: name.map_or_else(String::new, ToString::to_string),
            },
            |value| client.describe_stream(value),
        )
        .await
        .map_err(|error| request_attempt_error("describe stream", error))?;
        match response.result {
            Some(stream_response::Result::Stream(value)) => Ok(stream_from_wire(value)?),
            Some(stream_response::Result::Error(error)) => Err(decode_domain_error(error)?.into()),
            None => Err(ClientError::Protocol(
                "describe stream response omitted its typed result".to_owned(),
            )),
        }
    }

    pub async fn list_streams(
        &self,
        cluster: ClusterId,
    ) -> Result<Vec<StreamDescriptor>, ClientError> {
        let deadline = Deadline::after(self.deadline);
        let mut client = self
            .connect_initial(deadline)
            .await
            .map_err(|error| request_attempt_error("list streams", error))?;
        let response = execute_rpc(
            deadline,
            v1::ListStreamsRequest {
                cluster_id: cluster.to_string(),
            },
            |value| client.list_streams(value),
        )
        .await
        .map_err(|error| request_attempt_error("list streams", error))?;
        match response.result {
            Some(list_streams_response::Result::Success(value)) => value
                .streams
                .into_iter()
                .map(|value| stream_from_wire(value).map_err(ClientError::Domain))
                .collect(),
            Some(list_streams_response::Result::Error(error)) => {
                Err(decode_domain_error(error)?.into())
            }
            None => Err(ClientError::Protocol(
                "list streams response omitted its typed result".to_owned(),
            )),
        }
    }

    pub async fn delete_stream(
        &self,
        cluster: ClusterId,
        stream_id: StreamId,
    ) -> Result<StreamDescriptor, ClientError> {
        let deadline = Deadline::after(self.deadline);
        let mut client = self
            .connect_initial(deadline)
            .await
            .map_err(|error| request_attempt_error("delete stream", error))?;
        let response = execute_rpc(
            deadline,
            v1::DeleteStreamRequest {
                cluster_id: cluster.to_string(),
                stream_id: stream_id.to_string(),
            },
            |value| client.delete_stream(value),
        )
        .await
        .map_err(|error| request_attempt_error("delete stream", error))?;
        match response.result {
            Some(stream_response::Result::Stream(value)) => Ok(stream_from_wire(value)?),
            Some(stream_response::Result::Error(error)) => Err(decode_domain_error(error)?.into()),
            None => Err(ClientError::Protocol(
                "delete stream response omitted its typed result".to_owned(),
            )),
        }
    }

    pub async fn resolve_route(
        &self,
        cluster: ClusterId,
        stream_id: Option<StreamId>,
        name: Option<&StreamName>,
        partition: PartitionId,
    ) -> Result<ResolvedRoute, ClientError> {
        let deadline = Deadline::after(self.deadline);
        let mut endpoints = self.seeds.clone();
        let mut index = 0usize;
        let request = v1::RouteRequest {
            cluster_id: cluster.to_string(),
            stream_id: stream_id.map_or_else(String::new, |value| value.to_string()),
            stream_name: name.map_or_else(String::new, ToString::to_string),
            partition_id: partition.get(),
        };
        loop {
            let endpoint = endpoints[index % endpoints.len()].clone();
            let result = match connect_client(&endpoint, deadline).await {
                Ok(mut client) => {
                    execute_rpc(deadline, request.clone(), |value| {
                        client.resolve_route(value)
                    })
                    .await
                }
                Err(error) => Err(error),
            };
            match result {
                Ok(value) => match value.result {
                    Some(route_response::Result::Route(value)) => {
                        let leader = value
                            .leader
                            .as_ref()
                            .map(|item| {
                                Ok::<LeaderHint, DomainError>(LeaderHint::new(
                                    light_stream_core::NodeId::new(item.node_id)?,
                                    item.public_uri.clone(),
                                ))
                            })
                            .transpose()?;
                        return Ok(ResolvedRoute {
                            route: route_from_wire(value)?,
                            leader,
                        });
                    }
                    Some(route_response::Result::Error(error)) => {
                        let error = decode_domain_error(error)?;
                        if let DomainError::NotLeader { leader, .. } = &error
                            && self.retry
                        {
                            if let Some(leader) = leader {
                                index = add_hint(&mut endpoints, leader.public_uri())?;
                            }
                        } else {
                            return Err(error.into());
                        }
                    }
                    None => {
                        return Err(ClientError::Protocol(
                            "route response omitted its typed result".to_owned(),
                        ));
                    }
                },
                Err(AttemptError::Client(ClientError::Connection(_) | ClientError::Request(_)))
                    if self.retry =>
                {
                    index = index.wrapping_add(1);
                }
                Err(error) => return Err(request_attempt_error("resolve route", error)),
            }
            retry_sleep(deadline)
                .await
                .map_err(|_| non_write_deadline_error())?;
        }
    }

    pub async fn publish(&self, batch: PublishBatch) -> Result<PublishReceipt, ClientError> {
        let route = self
            .resolve_route(
                batch.cluster(),
                Some(batch.partition().stream()),
                None,
                batch.partition().partition(),
            )
            .await?;
        self.publish_with_resolved_route(batch, route).await
    }

    pub async fn publish_with_route_hint(
        &self,
        batch: PublishBatch,
        group: light_stream_core::GroupId,
        route_revision: u64,
    ) -> Result<PublishReceipt, ClientError> {
        self.publish_with_resolved_route(
            batch.clone(),
            ResolvedRoute {
                route: PartitionRoute::new(
                    batch.cluster(),
                    batch.partition().stream(),
                    StreamName::parse("cached-route")?,
                    batch.partition().partition(),
                    group,
                    route_revision,
                ),
                leader: None,
            },
        )
        .await
    }

    async fn publish_with_resolved_route(
        &self,
        batch: PublishBatch,
        route: ResolvedRoute,
    ) -> Result<PublishReceipt, ClientError> {
        let request_id = batch.request().clone();
        let mut request = publish_request_to_wire(&batch, &route.route);
        let deadline = Deadline::after(self.deadline);
        let mut endpoints = self.seeds.clone();
        if let Some(leader) = route.leader {
            add_hint(&mut endpoints, leader.public_uri())?;
        }
        let mut index = 0usize;
        let mut request_may_have_reached = false;
        loop {
            if deadline.expired() {
                return Err(publish_deadline_error(request_id, request_may_have_reached));
            }
            let endpoint = endpoints[index % endpoints.len()].clone();
            let mut next_index = index.wrapping_add(1);
            let result = self
                .publish_once(
                    &endpoint,
                    request.clone(),
                    deadline,
                    &mut request_may_have_reached,
                )
                .await;
            match result {
                Ok(receipt) => return Ok(receipt),
                Err(AttemptError::Deadline) => {
                    return Err(publish_deadline_error(request_id, request_may_have_reached));
                }
                Err(AttemptError::Client(ClientError::Domain(DomainError::NotLeader {
                    leader,
                    ..
                }))) => {
                    if !self.retry {
                        return Err(ClientError::Domain(DomainError::NotLeader {
                            group: ConsensusGroup::Data,
                            leader,
                        }));
                    }
                    if let Some(hint) = leader {
                        next_index = add_hint(&mut endpoints, hint.public_uri())?;
                    }
                }
                Err(AttemptError::Client(ClientError::Domain(
                    DomainError::QuorumUnavailable { .. },
                )))
                | Err(AttemptError::Client(ClientError::Connection(_)))
                | Err(AttemptError::Client(ClientError::Request(_)))
                    if self.retry => {}
                Err(AttemptError::Client(ClientError::Domain(DomainError::StaleRoute)))
                    if self.retry =>
                {
                    let route = self
                        .resolve_route(
                            batch.cluster(),
                            Some(batch.partition().stream()),
                            None,
                            batch.partition().partition(),
                        )
                        .await?;
                    request.route_group_id = route.route.group().get();
                    request.route_revision = route.route.route_revision();
                    if let Some(leader) = route.leader {
                        next_index = add_hint(&mut endpoints, leader.public_uri())?;
                    }
                }
                Err(AttemptError::Client(error)) => return Err(error),
            }
            index = next_index;
            retry_sleep(deadline).await.map_err(|_| {
                publish_deadline_error(request_id.clone(), request_may_have_reached)
            })?;
        }
    }

    pub async fn fetch(
        &self,
        cluster: ClusterId,
        partition: PartitionKey,
        offset: RecordOffset,
        limit: u32,
    ) -> Result<FetchPage, ClientError> {
        let route = self
            .resolve_route(
                cluster,
                Some(partition.stream()),
                None,
                partition.partition(),
            )
            .await?;
        let mut request = v1::FetchRequest {
            cluster_id: cluster.to_string(),
            stream_id: partition.stream().to_string(),
            partition_id: partition.partition().get(),
            offset: offset.get(),
            limit,
            route_group_id: route.route.group().get(),
            route_revision: route.route.route_revision(),
        };
        let deadline = Deadline::after(self.deadline);
        let mut endpoints = self.seeds.clone();
        if let Some(leader) = route.leader {
            add_hint(&mut endpoints, leader.public_uri())?;
        }
        let mut index = 0usize;
        loop {
            let endpoint = endpoints[index % endpoints.len()].clone();
            let mut next_index = index.wrapping_add(1);
            match self.fetch_once(&endpoint, request.clone(), deadline).await {
                Ok(value) => return Ok(value),
                Err(AttemptError::Deadline) => return Err(non_write_deadline_error()),
                Err(AttemptError::Client(ClientError::Domain(DomainError::NotLeader {
                    leader,
                    ..
                }))) => {
                    if !self.retry {
                        return Err(ClientError::Domain(DomainError::NotLeader {
                            group: ConsensusGroup::Data,
                            leader,
                        }));
                    }
                    if let Some(hint) = leader {
                        next_index = add_hint(&mut endpoints, hint.public_uri())?;
                    }
                }
                Err(AttemptError::Client(ClientError::Domain(
                    DomainError::QuorumUnavailable { .. },
                )))
                | Err(AttemptError::Client(ClientError::Connection(_)))
                | Err(AttemptError::Client(ClientError::Request(_)))
                    if self.retry => {}
                Err(AttemptError::Client(ClientError::Domain(DomainError::StaleRoute)))
                    if self.retry =>
                {
                    let route = self
                        .resolve_route(
                            cluster,
                            Some(partition.stream()),
                            None,
                            partition.partition(),
                        )
                        .await?;
                    request.route_group_id = route.route.group().get();
                    request.route_revision = route.route.route_revision();
                    if let Some(leader) = route.leader {
                        next_index = add_hint(&mut endpoints, leader.public_uri())?;
                    }
                }
                Err(AttemptError::Client(error)) => return Err(error),
            }
            index = next_index;
            retry_sleep(deadline)
                .await
                .map_err(|_| non_write_deadline_error())?;
        }
    }

    pub async fn fetch_with_route_hint(
        &self,
        cluster: ClusterId,
        partition: PartitionKey,
        offset: RecordOffset,
        limit: u32,
        group: light_stream_core::GroupId,
        route_revision: u64,
    ) -> Result<FetchPage, ClientError> {
        let deadline = Deadline::after(self.deadline);
        let request = v1::FetchRequest {
            cluster_id: cluster.to_string(),
            stream_id: partition.stream().to_string(),
            partition_id: partition.partition().get(),
            offset: offset.get(),
            limit,
            route_group_id: group.get(),
            route_revision,
        };
        self.fetch_once(&self.endpoint, request, deadline)
            .await
            .map_err(|error| request_attempt_error("fetch", error))
    }

    pub async fn receipt(
        &self,
        cluster: ClusterId,
        partition: PartitionKey,
        request: ProducerRequestId,
    ) -> Result<PublishReceipt, ClientError> {
        let route = self
            .resolve_route(
                cluster,
                Some(partition.stream()),
                None,
                partition.partition(),
            )
            .await?;
        let mut request = v1::ReceiptRequest {
            cluster_id: cluster.to_string(),
            stream_id: partition.stream().to_string(),
            partition_id: partition.partition().get(),
            request_id: Some(request_id_to_wire(&request)),
            route_group_id: route.route.group().get(),
            route_revision: route.route.route_revision(),
        };
        let deadline = Deadline::after(self.deadline);
        let mut endpoints = self.seeds.clone();
        if let Some(leader) = route.leader {
            add_hint(&mut endpoints, leader.public_uri())?;
        }
        let mut index = 0usize;
        loop {
            let endpoint = endpoints[index % endpoints.len()].clone();
            let mut next_index = index.wrapping_add(1);
            match self
                .receipt_once(&endpoint, request.clone(), deadline)
                .await
            {
                Ok(value) => return Ok(value),
                Err(AttemptError::Deadline) => return Err(non_write_deadline_error()),
                Err(AttemptError::Client(ClientError::Domain(DomainError::NotLeader {
                    leader,
                    ..
                }))) => {
                    if !self.retry {
                        return Err(ClientError::Domain(DomainError::NotLeader {
                            group: ConsensusGroup::Data,
                            leader,
                        }));
                    }
                    if let Some(hint) = leader {
                        next_index = add_hint(&mut endpoints, hint.public_uri())?;
                    }
                }
                Err(AttemptError::Client(ClientError::Domain(
                    DomainError::QuorumUnavailable { .. },
                )))
                | Err(AttemptError::Client(ClientError::Connection(_)))
                | Err(AttemptError::Client(ClientError::Request(_)))
                    if self.retry => {}
                Err(AttemptError::Client(ClientError::Domain(DomainError::StaleRoute)))
                    if self.retry =>
                {
                    let route = self
                        .resolve_route(
                            cluster,
                            Some(partition.stream()),
                            None,
                            partition.partition(),
                        )
                        .await?;
                    request.route_group_id = route.route.group().get();
                    request.route_revision = route.route.route_revision();
                    if let Some(leader) = route.leader {
                        next_index = add_hint(&mut endpoints, leader.public_uri())?;
                    }
                }
                Err(AttemptError::Client(error)) => return Err(error),
            }
            index = next_index;
            retry_sleep(deadline)
                .await
                .map_err(|_| non_write_deadline_error())?;
        }
    }

    pub async fn receipt_with_route_hint(
        &self,
        cluster: ClusterId,
        partition: PartitionKey,
        request: ProducerRequestId,
        group: light_stream_core::GroupId,
        route_revision: u64,
    ) -> Result<PublishReceipt, ClientError> {
        let deadline = Deadline::after(self.deadline);
        let request = v1::ReceiptRequest {
            cluster_id: cluster.to_string(),
            stream_id: partition.stream().to_string(),
            partition_id: partition.partition().get(),
            request_id: Some(request_id_to_wire(&request)),
            route_group_id: group.get(),
            route_revision,
        };
        self.receipt_once(&self.endpoint, request, deadline)
            .await
            .map_err(|error| request_attempt_error("receipt", error))
    }

    pub async fn create_bookmark(
        &self,
        cluster: ClusterId,
        partition: PartitionKey,
        id: BookmarkId,
        name: BookmarkName,
        offset: RecordOffset,
    ) -> Result<CommittedBookmark, ClientError> {
        let route = self
            .resolve_route(
                cluster,
                Some(partition.stream()),
                None,
                partition.partition(),
            )
            .await?;
        let deadline = Deadline::after(self.deadline);
        let mut endpoints = self.seeds.clone();
        if let Some(leader) = route.leader {
            add_hint(&mut endpoints, leader.public_uri())?;
        }
        let request = v1::CreateBookmarkRequest {
            cluster_id: cluster.to_string(),
            stream_id: partition.stream().to_string(),
            partition_id: partition.partition().get(),
            bookmark_id: id.to_string(),
            name: name.to_string(),
            next_offset: offset.get(),
            route_group_id: route.route.group().get(),
            route_revision: route.route.route_revision(),
        };
        self.execute_bookmark_call(
            deadline,
            endpoints,
            "create bookmark",
            BookmarkCall::Create(request),
        )
        .await
    }

    pub async fn resolve_bookmark(
        &self,
        cluster: ClusterId,
        partition: PartitionKey,
        name: BookmarkName,
    ) -> Result<CommittedBookmark, ClientError> {
        let route = self
            .resolve_route(
                cluster,
                Some(partition.stream()),
                None,
                partition.partition(),
            )
            .await?;
        let deadline = Deadline::after(self.deadline);
        let mut endpoints = self.seeds.clone();
        if let Some(leader) = route.leader {
            add_hint(&mut endpoints, leader.public_uri())?;
        }
        let request = v1::ResolveBookmarkRequest {
            cluster_id: cluster.to_string(),
            stream_id: partition.stream().to_string(),
            partition_id: partition.partition().get(),
            name: name.to_string(),
            route_group_id: route.route.group().get(),
            route_revision: route.route.route_revision(),
        };
        self.execute_bookmark_call(
            deadline,
            endpoints,
            "resolve bookmark",
            BookmarkCall::Resolve(request),
        )
        .await
    }

    pub async fn delete_bookmark(
        &self,
        cluster: ClusterId,
        partition: PartitionKey,
        id: BookmarkId,
    ) -> Result<CommittedBookmark, ClientError> {
        let route = self
            .resolve_route(
                cluster,
                Some(partition.stream()),
                None,
                partition.partition(),
            )
            .await?;
        let deadline = Deadline::after(self.deadline);
        let mut endpoints = self.seeds.clone();
        if let Some(leader) = route.leader {
            add_hint(&mut endpoints, leader.public_uri())?;
        }
        let request = v1::DeleteBookmarkRequest {
            cluster_id: cluster.to_string(),
            stream_id: partition.stream().to_string(),
            partition_id: partition.partition().get(),
            bookmark_id: id.to_string(),
            route_group_id: route.route.group().get(),
            route_revision: route.route.route_revision(),
        };
        self.execute_bookmark_call(
            deadline,
            endpoints,
            "delete bookmark",
            BookmarkCall::Delete(request),
        )
        .await
    }

    pub async fn list_bookmarks(
        &self,
        cluster: ClusterId,
        request: BookmarkPageRequest,
    ) -> Result<BookmarkPage, ClientError> {
        let route = self
            .resolve_route(
                cluster,
                Some(request.partition().stream()),
                None,
                request.partition().partition(),
            )
            .await?;
        let deadline = Deadline::after(self.deadline);
        let mut endpoints = self.seeds.clone();
        if let Some(leader) = route.leader {
            add_hint(&mut endpoints, leader.public_uri())?;
        }
        let wire_request = v1::ListBookmarksRequest {
            cluster_id: cluster.to_string(),
            stream_id: request.partition().stream().to_string(),
            partition_id: request.partition().partition().get(),
            limit: request.limit(),
            publication_ceiling: request
                .publication_ceiling()
                .map_or(0, BookmarkPublicationSequence::get),
            before_publication: request.before().map_or(0, BookmarkPublicationSequence::get),
            route_group_id: route.route.group().get(),
            route_revision: route.route.route_revision(),
        };
        let mut index = 0usize;
        loop {
            let endpoint = endpoints[index % endpoints.len()].clone();
            let mut client = connect_client(&endpoint, deadline)
                .await
                .map_err(|error| request_attempt_error("list bookmarks", error))?;
            let response = execute_rpc(deadline, wire_request.clone(), |request| {
                client.list_bookmarks(request)
            })
            .await
            .map_err(|error| request_attempt_error("list bookmarks", error))?;
            match response.result {
                Some(list_bookmarks_response::Result::Page(page)) => {
                    return Ok(BookmarkPage::new(
                        page.bookmarks
                            .into_iter()
                            .map(bookmark_from_wire)
                            .collect::<Result<Vec<_>, _>>()?,
                        BookmarkPublicationSequence::new(page.publication_ceiling),
                        (page.next_before != 0)
                            .then(|| BookmarkPublicationSequence::new(page.next_before)),
                    ));
                }
                Some(list_bookmarks_response::Result::Error(value)) => {
                    let error = decode_domain_error(value)?;
                    if let DomainError::NotLeader {
                        leader: Some(hint), ..
                    } = &error
                        && self.retry
                    {
                        index = add_hint(&mut endpoints, hint.public_uri())?;
                        retry_sleep(deadline)
                            .await
                            .map_err(|_| non_write_deadline_error())?;
                        continue;
                    }
                    return Err(error.into());
                }
                None => {
                    return Err(ClientError::Protocol(
                        "bookmark page response omitted its typed result".to_owned(),
                    ));
                }
            }
        }
    }

    pub async fn advance_retention(
        &self,
        cluster: ClusterId,
        request: RetentionRequest,
    ) -> Result<RetentionResult, ClientError> {
        let route = self
            .resolve_route(
                cluster,
                Some(request.partition().stream()),
                None,
                request.partition().partition(),
            )
            .await?;
        let mutation = request.request().clone();
        let mut wire_request = v1::AdvanceRetentionRequest {
            cluster_id: cluster.to_string(),
            stream_id: request.partition().stream().to_string(),
            partition_id: request.partition().partition().get(),
            request_id: Some(mutation_request_id_to_wire(request.request())),
            target_floor: request.target_floor().get(),
            route_group_id: route.route.group().get(),
            route_revision: route.route.route_revision(),
        };
        let deadline = Deadline::after(self.deadline);
        let mut endpoints = self.seeds.clone();
        if let Some(leader) = route.leader {
            add_hint(&mut endpoints, leader.public_uri())?;
        }
        let mut index = 0usize;
        let mut request_may_have_reached = false;
        loop {
            if deadline.expired() {
                return Err(mutation_deadline_error(mutation, request_may_have_reached));
            }
            let endpoint = endpoints[index % endpoints.len()].clone();
            let mut next_index = index.wrapping_add(1);
            match self
                .advance_retention_once(
                    &endpoint,
                    wire_request.clone(),
                    deadline,
                    &mut request_may_have_reached,
                )
                .await
            {
                Ok(result) => return Ok(result),
                Err(AttemptError::Deadline) => {
                    return Err(mutation_deadline_error(mutation, request_may_have_reached));
                }
                Err(AttemptError::Client(ClientError::Domain(DomainError::NotLeader {
                    leader,
                    ..
                }))) if self.retry => {
                    if let Some(hint) = leader {
                        next_index = add_hint(&mut endpoints, hint.public_uri())?;
                    }
                }
                Err(AttemptError::Client(ClientError::Domain(
                    DomainError::QuorumUnavailable { .. },
                )))
                | Err(AttemptError::Client(ClientError::Connection(_)))
                | Err(AttemptError::Client(ClientError::Request(_)))
                    if self.retry => {}
                Err(AttemptError::Client(ClientError::Domain(DomainError::StaleRoute)))
                    if self.retry =>
                {
                    let route = self
                        .resolve_route(
                            cluster,
                            Some(request.partition().stream()),
                            None,
                            request.partition().partition(),
                        )
                        .await?;
                    wire_request.route_group_id = route.route.group().get();
                    wire_request.route_revision = route.route.route_revision();
                    if let Some(leader) = route.leader {
                        next_index = add_hint(&mut endpoints, leader.public_uri())?;
                    }
                }
                Err(AttemptError::Client(error)) => return Err(error),
            }
            index = next_index;
            retry_sleep(deadline)
                .await
                .map_err(|_| mutation_deadline_error(mutation.clone(), request_may_have_reached))?;
        }
    }

    pub async fn retention_status(
        &self,
        cluster: ClusterId,
        partition: PartitionKey,
    ) -> Result<RetentionStatus, ClientError> {
        let route = self
            .resolve_route(
                cluster,
                Some(partition.stream()),
                None,
                partition.partition(),
            )
            .await?;
        let deadline = Deadline::after(self.deadline);
        let mut endpoints = self.seeds.clone();
        if let Some(leader) = route.leader {
            add_hint(&mut endpoints, leader.public_uri())?;
        }
        let mut request = v1::RetentionStatusRequest {
            cluster_id: cluster.to_string(),
            stream_id: partition.stream().to_string(),
            partition_id: partition.partition().get(),
            route_group_id: route.route.group().get(),
            route_revision: route.route.route_revision(),
        };
        let mut index = 0usize;
        loop {
            let endpoint = endpoints[index % endpoints.len()].clone();
            let mut next_index = index.wrapping_add(1);
            let mut client = match connect_client(&endpoint, deadline).await {
                Ok(client) => client,
                Err(AttemptError::Deadline) => return Err(non_write_deadline_error()),
                Err(_) if self.retry => {
                    index = next_index;
                    retry_sleep(deadline)
                        .await
                        .map_err(|_| non_write_deadline_error())?;
                    continue;
                }
                Err(error) => return Err(request_attempt_error("retention status", error)),
            };
            let response = match execute_rpc(deadline, request.clone(), |request| {
                client.get_retention_status(request)
            })
            .await
            {
                Ok(response) => response,
                Err(AttemptError::Deadline) => return Err(non_write_deadline_error()),
                Err(_) if self.retry => {
                    index = next_index;
                    retry_sleep(deadline)
                        .await
                        .map_err(|_| non_write_deadline_error())?;
                    continue;
                }
                Err(error) => return Err(request_attempt_error("retention status", error)),
            };
            match response.result {
                Some(retention_status_response::Result::Status(status)) => {
                    return retention_status_from_response(status).map_err(ClientError::Domain);
                }
                Some(retention_status_response::Result::Error(value)) => {
                    let error = decode_domain_error(value)?;
                    match error {
                        DomainError::NotLeader {
                            leader: Some(hint), ..
                        } if self.retry => {
                            next_index = add_hint(&mut endpoints, hint.public_uri())?;
                        }
                        DomainError::StaleRoute if self.retry => {
                            let route = self
                                .resolve_route(
                                    cluster,
                                    Some(partition.stream()),
                                    None,
                                    partition.partition(),
                                )
                                .await?;
                            request.route_group_id = route.route.group().get();
                            request.route_revision = route.route.route_revision();
                            if let Some(leader) = route.leader {
                                next_index = add_hint(&mut endpoints, leader.public_uri())?;
                            }
                        }
                        DomainError::QuorumUnavailable { .. } if self.retry => {}
                        other => return Err(other.into()),
                    }
                    index = next_index;
                    retry_sleep(deadline)
                        .await
                        .map_err(|_| non_write_deadline_error())?;
                }
                None => {
                    return Err(ClientError::Protocol(
                        "retention status response omitted its typed result".to_owned(),
                    ));
                }
            }
        }
    }

    pub async fn admit_replay_lease(
        &self,
        request: ReplayLeaseRequest,
    ) -> Result<ReplayLease, ClientError> {
        let cluster = request.cluster();
        let partition = request.range().partition();
        let route = self
            .resolve_route(
                cluster,
                Some(partition.stream()),
                None,
                partition.partition(),
            )
            .await?;
        let route_group_id = route.route.group().get();
        let route_revision = route.route.route_revision();
        self.execute_replay_mutation(
            cluster,
            partition,
            request.request().clone(),
            route,
            ReplayMutationCall::Admit(v1::AdmitReplayLeaseRequest {
                cluster_id: cluster.to_string(),
                range: Some(v1::ReplayRange {
                    stream_id: partition.stream().to_string(),
                    partition_id: partition.partition().get(),
                    start_offset: request.range().start().get(),
                    end_offset: request.range().end().get(),
                }),
                request_id: Some(mutation_request_id_to_wire(request.request())),
                duration_ms: request.duration().as_millis(),
                max_bytes: request.max_bytes().get(),
                route_group_id,
                route_revision,
            }),
        )
        .await
    }

    pub async fn renew_replay_lease(
        &self,
        cluster: ClusterId,
        request: LeaseRenewal,
    ) -> Result<ReplayLease, ClientError> {
        let partition = request.partition();
        let route = self
            .resolve_route(
                cluster,
                Some(partition.stream()),
                None,
                partition.partition(),
            )
            .await?;
        self.renew_replay_lease_with_resolved_route(cluster, partition, request, route)
            .await
    }

    pub async fn renew_replay_lease_with_route_hint(
        &self,
        cluster: ClusterId,
        request: LeaseRenewal,
        group: light_stream_core::GroupId,
        route_revision: u64,
    ) -> Result<ReplayLease, ClientError> {
        let partition = request.partition();
        self.renew_replay_lease_with_resolved_route(
            cluster,
            partition,
            request,
            ResolvedRoute {
                route: PartitionRoute::new(
                    cluster,
                    partition.stream(),
                    StreamName::parse("cached-route")?,
                    partition.partition(),
                    group,
                    route_revision,
                ),
                leader: None,
            },
        )
        .await
    }

    async fn renew_replay_lease_with_resolved_route(
        &self,
        cluster: ClusterId,
        partition: PartitionKey,
        request: LeaseRenewal,
        route: ResolvedRoute,
    ) -> Result<ReplayLease, ClientError> {
        let route_group_id = route.route.group().get();
        let route_revision = route.route.route_revision();
        self.execute_replay_mutation(
            cluster,
            partition,
            request.request().clone(),
            route,
            ReplayMutationCall::Renew(v1::RenewReplayLeaseRequest {
                cluster_id: cluster.to_string(),
                stream_id: partition.stream().to_string(),
                partition_id: partition.partition().get(),
                lease_id: request.lease().to_string(),
                request_id: Some(mutation_request_id_to_wire(request.request())),
                duration_ms: request.duration().as_millis(),
                route_group_id,
                route_revision,
            }),
        )
        .await
    }

    pub async fn release_replay_lease(
        &self,
        cluster: ClusterId,
        request: LeaseRelease,
    ) -> Result<ReplayLease, ClientError> {
        let partition = request.partition();
        let route = self
            .resolve_route(
                cluster,
                Some(partition.stream()),
                None,
                partition.partition(),
            )
            .await?;
        self.release_replay_lease_with_resolved_route(cluster, partition, request, route)
            .await
    }

    pub async fn release_replay_lease_with_route_hint(
        &self,
        cluster: ClusterId,
        request: LeaseRelease,
        group: light_stream_core::GroupId,
        route_revision: u64,
    ) -> Result<ReplayLease, ClientError> {
        let partition = request.partition();
        self.release_replay_lease_with_resolved_route(
            cluster,
            partition,
            request,
            ResolvedRoute {
                route: PartitionRoute::new(
                    cluster,
                    partition.stream(),
                    StreamName::parse("cached-route")?,
                    partition.partition(),
                    group,
                    route_revision,
                ),
                leader: None,
            },
        )
        .await
    }

    async fn release_replay_lease_with_resolved_route(
        &self,
        cluster: ClusterId,
        partition: PartitionKey,
        request: LeaseRelease,
        route: ResolvedRoute,
    ) -> Result<ReplayLease, ClientError> {
        let route_group_id = route.route.group().get();
        let route_revision = route.route.route_revision();
        self.execute_replay_mutation(
            cluster,
            partition,
            request.request().clone(),
            route,
            ReplayMutationCall::Release(v1::ReleaseReplayLeaseRequest {
                cluster_id: cluster.to_string(),
                stream_id: partition.stream().to_string(),
                partition_id: partition.partition().get(),
                lease_id: request.lease().to_string(),
                request_id: Some(mutation_request_id_to_wire(request.request())),
                route_group_id,
                route_revision,
            }),
        )
        .await
    }

    async fn execute_replay_mutation(
        &self,
        cluster: ClusterId,
        partition: PartitionKey,
        mutation: light_stream_core::MutationRequestId,
        route: ResolvedRoute,
        mut call: ReplayMutationCall,
    ) -> Result<ReplayLease, ClientError> {
        let deadline = Deadline::after(self.deadline);
        let mut endpoints = self.seeds.clone();
        if let Some(leader) = route.leader {
            add_hint(&mut endpoints, leader.public_uri())?;
        }
        let mut index = 0usize;
        let mut request_may_have_reached = false;
        loop {
            if deadline.expired() {
                return Err(mutation_deadline_error(mutation, request_may_have_reached));
            }
            let endpoint = endpoints[index % endpoints.len()].clone();
            let mut next_index = index.wrapping_add(1);
            match self
                .replay_mutation_once(
                    &endpoint,
                    call.clone(),
                    deadline,
                    &mut request_may_have_reached,
                )
                .await
            {
                Ok(lease) => return Ok(lease),
                Err(AttemptError::Deadline) => {
                    return Err(mutation_deadline_error(mutation, request_may_have_reached));
                }
                Err(AttemptError::Client(ClientError::Domain(DomainError::NotLeader {
                    leader,
                    ..
                }))) if self.retry => {
                    if let Some(hint) = leader {
                        next_index = add_hint(&mut endpoints, hint.public_uri())?;
                    }
                }
                Err(AttemptError::Client(ClientError::Domain(
                    DomainError::QuorumUnavailable { .. },
                )))
                | Err(AttemptError::Client(ClientError::Connection(_)))
                | Err(AttemptError::Client(ClientError::Request(_)))
                    if self.retry => {}
                Err(AttemptError::Client(ClientError::Domain(DomainError::StaleRoute)))
                    if self.retry =>
                {
                    let route = self
                        .resolve_route(
                            cluster,
                            Some(partition.stream()),
                            None,
                            partition.partition(),
                        )
                        .await?;
                    call.set_route(route.route.group(), route.route.route_revision());
                    if let Some(leader) = route.leader {
                        next_index = add_hint(&mut endpoints, leader.public_uri())?;
                    }
                }
                Err(AttemptError::Client(error)) => return Err(error),
            }
            index = next_index;
            retry_sleep(deadline)
                .await
                .map_err(|_| mutation_deadline_error(mutation.clone(), request_may_have_reached))?;
        }
    }

    pub async fn replay_lease(
        &self,
        cluster: ClusterId,
        partition: PartitionKey,
        lease: ReplayLeaseId,
    ) -> Result<ReplayLease, ClientError> {
        let route = self
            .resolve_route(
                cluster,
                Some(partition.stream()),
                None,
                partition.partition(),
            )
            .await?;
        let deadline = Deadline::after(self.deadline);
        let mut endpoints = self.seeds.clone();
        if let Some(leader) = route.leader {
            add_hint(&mut endpoints, leader.public_uri())?;
        }
        let mut request = v1::GetReplayLeaseRequest {
            cluster_id: cluster.to_string(),
            stream_id: partition.stream().to_string(),
            partition_id: partition.partition().get(),
            lease_id: lease.to_string(),
            route_group_id: route.route.group().get(),
            route_revision: route.route.route_revision(),
        };
        let mut index = 0usize;
        loop {
            let endpoint = endpoints[index % endpoints.len()].clone();
            let mut next_index = index.wrapping_add(1);
            let mut client = match connect_client(&endpoint, deadline).await {
                Ok(client) => client,
                Err(AttemptError::Deadline) => return Err(non_write_deadline_error()),
                Err(_) if self.retry => {
                    index = next_index;
                    retry_sleep(deadline)
                        .await
                        .map_err(|_| non_write_deadline_error())?;
                    continue;
                }
                Err(error) => return Err(request_attempt_error("replay lease", error)),
            };
            let response = match execute_rpc(deadline, request.clone(), |request| {
                client.get_replay_lease(request)
            })
            .await
            {
                Ok(response) => response,
                Err(AttemptError::Deadline) => return Err(non_write_deadline_error()),
                Err(_) if self.retry => {
                    index = next_index;
                    retry_sleep(deadline)
                        .await
                        .map_err(|_| non_write_deadline_error())?;
                    continue;
                }
                Err(error) => return Err(request_attempt_error("replay lease", error)),
            };
            match response.result {
                Some(replay_lease_response::Result::Lease(lease)) => {
                    return replay_lease_from_wire(lease).map_err(ClientError::Domain);
                }
                Some(replay_lease_response::Result::Error(value)) => {
                    let error = decode_domain_error(value)?;
                    match error {
                        DomainError::NotLeader {
                            leader: Some(hint), ..
                        } if self.retry => {
                            next_index = add_hint(&mut endpoints, hint.public_uri())?;
                        }
                        DomainError::StaleRoute if self.retry => {
                            let route = self
                                .resolve_route(
                                    cluster,
                                    Some(partition.stream()),
                                    None,
                                    partition.partition(),
                                )
                                .await?;
                            request.route_group_id = route.route.group().get();
                            request.route_revision = route.route.route_revision();
                            if let Some(leader) = route.leader {
                                next_index = add_hint(&mut endpoints, leader.public_uri())?;
                            }
                        }
                        DomainError::QuorumUnavailable { .. } if self.retry => {}
                        other => return Err(other.into()),
                    }
                    index = next_index;
                    retry_sleep(deadline)
                        .await
                        .map_err(|_| non_write_deadline_error())?;
                }
                None => {
                    return Err(ClientError::Protocol(
                        "replay lease response omitted its typed result".to_owned(),
                    ));
                }
            }
        }
    }

    pub async fn fetch_protected(
        &self,
        lease: &ReplayLease,
        offset: RecordOffset,
        limit: u32,
    ) -> Result<FetchPage, ClientError> {
        let cluster = lease.request().cluster();
        let partition = lease.range().partition();
        let route = self
            .resolve_route(
                cluster,
                Some(partition.stream()),
                None,
                partition.partition(),
            )
            .await?;
        let deadline = Deadline::after(self.deadline);
        let mut endpoints = self.seeds.clone();
        if let Some(leader) = route.leader {
            add_hint(&mut endpoints, leader.public_uri())?;
        }
        let mut request = v1::FetchProtectedRequest {
            cluster_id: cluster.to_string(),
            stream_id: partition.stream().to_string(),
            partition_id: partition.partition().get(),
            lease_id: lease.id().to_string(),
            offset: offset.get(),
            limit,
            route_group_id: route.route.group().get(),
            route_revision: route.route.route_revision(),
        };
        let mut index = 0usize;
        loop {
            let endpoint = endpoints[index % endpoints.len()].clone();
            let mut next_index = index.wrapping_add(1);
            let mut client = match connect_client(&endpoint, deadline).await {
                Ok(client) => client,
                Err(AttemptError::Deadline) => return Err(non_write_deadline_error()),
                Err(_) if self.retry => {
                    index = next_index;
                    retry_sleep(deadline)
                        .await
                        .map_err(|_| non_write_deadline_error())?;
                    continue;
                }
                Err(error) => return Err(request_attempt_error("protected fetch", error)),
            };
            let response = match execute_rpc(deadline, request.clone(), |request| {
                client.fetch_protected(request)
            })
            .await
            {
                Ok(response) => response,
                Err(AttemptError::Deadline) => return Err(non_write_deadline_error()),
                Err(_) if self.retry => {
                    index = next_index;
                    retry_sleep(deadline)
                        .await
                        .map_err(|_| non_write_deadline_error())?;
                    continue;
                }
                Err(error) => return Err(request_attempt_error("protected fetch", error)),
            };
            match response.result {
                Some(fetch_response::Result::Success(value)) => {
                    return fetch_success_from_wire(value);
                }
                Some(fetch_response::Result::Error(value)) => {
                    let error = decode_domain_error(value)?;
                    match error {
                        DomainError::NotLeader {
                            leader: Some(hint), ..
                        } if self.retry => {
                            next_index = add_hint(&mut endpoints, hint.public_uri())?;
                        }
                        DomainError::StaleRoute if self.retry => {
                            let route = self
                                .resolve_route(
                                    cluster,
                                    Some(partition.stream()),
                                    None,
                                    partition.partition(),
                                )
                                .await?;
                            request.route_group_id = route.route.group().get();
                            request.route_revision = route.route.route_revision();
                            if let Some(leader) = route.leader {
                                next_index = add_hint(&mut endpoints, leader.public_uri())?;
                            }
                        }
                        DomainError::QuorumUnavailable { .. } if self.retry => {}
                        other => return Err(other.into()),
                    }
                    index = next_index;
                    retry_sleep(deadline)
                        .await
                        .map_err(|_| non_write_deadline_error())?;
                }
                None => {
                    return Err(ClientError::Protocol(
                        "protected fetch response omitted its typed result".to_owned(),
                    ));
                }
            }
        }
    }

    pub async fn create_stream_bookmark(
        &self,
        cluster: ClusterId,
        id: BookmarkId,
        name: BookmarkName,
        vector: StreamCursorVector,
    ) -> Result<CommittedStreamBookmark, ClientError> {
        if vector.cluster() != cluster {
            return Err(DomainError::IdentityMismatch {
                reason: "stream bookmark vector belongs to another cluster".to_owned(),
            }
            .into());
        }
        let request = v1::CreateStreamBookmarkRequest {
            cluster_id: cluster.to_string(),
            stream_id: vector.stream().to_string(),
            bookmark_id: id.to_string(),
            name: name.to_string(),
            positions: vector
                .positions()
                .iter()
                .map(|cursor| v1::StreamBookmarkPosition {
                    partition_id: cursor.partition().partition().get(),
                    next_offset: cursor.next_offset().get(),
                })
                .collect(),
        };
        self.execute_stream_bookmark_call(
            Deadline::after(self.deadline),
            self.seeds.clone(),
            "create stream bookmark",
            StreamBookmarkCall::Create(request),
        )
        .await
    }

    pub async fn resolve_stream_bookmark(
        &self,
        cluster: ClusterId,
        stream: StreamId,
        name: BookmarkName,
    ) -> Result<CommittedStreamBookmark, ClientError> {
        self.execute_stream_bookmark_call(
            Deadline::after(self.deadline),
            self.seeds.clone(),
            "resolve stream bookmark",
            StreamBookmarkCall::Resolve(v1::ResolveStreamBookmarkRequest {
                cluster_id: cluster.to_string(),
                stream_id: stream.to_string(),
                name: name.to_string(),
            }),
        )
        .await
    }

    pub async fn delete_stream_bookmark(
        &self,
        cluster: ClusterId,
        stream: StreamId,
        id: BookmarkId,
    ) -> Result<CommittedStreamBookmark, ClientError> {
        self.execute_stream_bookmark_call(
            Deadline::after(self.deadline),
            self.seeds.clone(),
            "delete stream bookmark",
            StreamBookmarkCall::Delete(v1::DeleteStreamBookmarkRequest {
                cluster_id: cluster.to_string(),
                stream_id: stream.to_string(),
                bookmark_id: id.to_string(),
            }),
        )
        .await
    }

    pub async fn list_stream_bookmarks(
        &self,
        request: StreamBookmarkPageRequest,
    ) -> Result<StreamBookmarkPage, ClientError> {
        let deadline = Deadline::after(self.deadline);
        let mut endpoints = self.seeds.clone();
        let wire_request = v1::ListStreamBookmarksRequest {
            cluster_id: request.cluster().to_string(),
            stream_id: request.stream().to_string(),
            limit: request.limit(),
            publication_ceiling: request
                .publication_ceiling()
                .map_or(0, BookmarkPublicationSequence::get),
            before_publication: request.before().map_or(0, BookmarkPublicationSequence::get),
        };
        let mut index = 0usize;
        loop {
            let endpoint = endpoints[index % endpoints.len()].clone();
            let mut client = connect_client(&endpoint, deadline)
                .await
                .map_err(|error| request_attempt_error("list stream bookmarks", error))?;
            let response = execute_rpc(deadline, wire_request.clone(), |request| {
                client.list_stream_bookmarks(request)
            })
            .await
            .map_err(|error| request_attempt_error("list stream bookmarks", error))?;
            match response.result {
                Some(list_stream_bookmarks_response::Result::Page(page)) => {
                    return Ok(StreamBookmarkPage::new(
                        page.bookmarks
                            .into_iter()
                            .map(stream_bookmark_from_wire)
                            .collect::<Result<Vec<_>, _>>()?,
                        BookmarkPublicationSequence::new(page.publication_ceiling),
                        (page.next_before != 0)
                            .then(|| BookmarkPublicationSequence::new(page.next_before)),
                    ));
                }
                Some(list_stream_bookmarks_response::Result::Error(value)) => {
                    let error = decode_domain_error(value)?;
                    if let DomainError::NotLeader {
                        leader: Some(hint), ..
                    } = &error
                        && self.retry
                    {
                        index = add_hint(&mut endpoints, hint.public_uri())?;
                        retry_sleep(deadline)
                            .await
                            .map_err(|_| non_write_deadline_error())?;
                        continue;
                    }
                    return Err(error.into());
                }
                None => {
                    return Err(ClientError::Protocol(
                        "stream bookmark page response omitted its typed result".to_owned(),
                    ));
                }
            }
        }
    }

    async fn execute_stream_bookmark_call(
        &self,
        deadline: Deadline,
        mut endpoints: Vec<String>,
        operation: &'static str,
        call: StreamBookmarkCall,
    ) -> Result<CommittedStreamBookmark, ClientError> {
        let mut index = 0usize;
        loop {
            let endpoint = endpoints[index % endpoints.len()].clone();
            let mut client = connect_client(&endpoint, deadline)
                .await
                .map_err(|error| request_attempt_error(operation, error))?;
            let response = match call.clone() {
                StreamBookmarkCall::Create(request) => {
                    execute_rpc(deadline, request, |request| {
                        client.create_stream_bookmark(request)
                    })
                    .await
                }
                StreamBookmarkCall::Resolve(request) => {
                    execute_rpc(deadline, request, |request| {
                        client.resolve_stream_bookmark(request)
                    })
                    .await
                }
                StreamBookmarkCall::Delete(request) => {
                    execute_rpc(deadline, request, |request| {
                        client.delete_stream_bookmark(request)
                    })
                    .await
                }
            }
            .map_err(|error| request_attempt_error(operation, error))?;
            match response.result {
                Some(stream_bookmark_response::Result::Bookmark(bookmark)) => {
                    return stream_bookmark_from_wire(bookmark).map_err(ClientError::Domain);
                }
                Some(stream_bookmark_response::Result::Error(value)) => {
                    let error = decode_domain_error(value)?;
                    if let DomainError::NotLeader {
                        leader: Some(hint), ..
                    } = &error
                        && self.retry
                    {
                        index = add_hint(&mut endpoints, hint.public_uri())?;
                        retry_sleep(deadline)
                            .await
                            .map_err(|_| non_write_deadline_error())?;
                        continue;
                    }
                    return Err(error.into());
                }
                None => {
                    return Err(ClientError::Protocol(
                        "stream bookmark response omitted its typed result".to_owned(),
                    ));
                }
            }
        }
    }

    async fn execute_bookmark_call(
        &self,
        deadline: Deadline,
        mut endpoints: Vec<String>,
        operation: &'static str,
        call: BookmarkCall,
    ) -> Result<CommittedBookmark, ClientError> {
        let mut index = 0usize;
        loop {
            let endpoint = endpoints[index % endpoints.len()].clone();
            let mut client = connect_client(&endpoint, deadline)
                .await
                .map_err(|error| request_attempt_error(operation, error))?;
            let response = match call.clone() {
                BookmarkCall::Create(request) => {
                    execute_rpc(deadline, request, |request| client.create_bookmark(request)).await
                }
                BookmarkCall::Resolve(request) => {
                    execute_rpc(deadline, request, |request| {
                        client.resolve_bookmark(request)
                    })
                    .await
                }
                BookmarkCall::Delete(request) => {
                    execute_rpc(deadline, request, |request| client.delete_bookmark(request)).await
                }
            }
            .map_err(|error| request_attempt_error(operation, error))?;
            match response.result {
                Some(bookmark_response::Result::Bookmark(bookmark)) => {
                    return bookmark_from_wire(bookmark).map_err(ClientError::Domain);
                }
                Some(bookmark_response::Result::Error(value)) => {
                    let error = decode_domain_error(value)?;
                    if let DomainError::NotLeader {
                        leader: Some(hint), ..
                    } = &error
                        && self.retry
                    {
                        index = add_hint(&mut endpoints, hint.public_uri())?;
                        retry_sleep(deadline)
                            .await
                            .map_err(|_| non_write_deadline_error())?;
                        continue;
                    }
                    return Err(error.into());
                }
                None => {
                    return Err(ClientError::Protocol(
                        "bookmark response omitted its typed result".to_owned(),
                    ));
                }
            }
        }
    }

    pub async fn diagnostics(&self) -> Result<NodeDiagnostics, ClientError> {
        let deadline = Deadline::after(self.deadline);
        let mut client = self
            .connect_initial(deadline)
            .await
            .map_err(|error| request_attempt_error("diagnostics", error))?;
        let response = execute_rpc(deadline, v1::DiagnosticsRequest {}, |request| {
            client.diagnostics(request)
        })
        .await
        .map_err(|error| request_attempt_error("diagnostics", error))?;
        NodeDiagnostics::from_wire(response)
    }

    pub async fn snapshot_group(
        &self,
        cluster: ClusterId,
        group_id: u64,
        purge: bool,
    ) -> Result<SnapshotGroupResult, ClientError> {
        let deadline = Deadline::after(self.deadline);
        let mut client = self
            .connect_initial(deadline)
            .await
            .map_err(|error| request_attempt_error("snapshot_group", error))?;
        let response = execute_rpc(
            deadline,
            v1::SnapshotGroupRequest {
                cluster_id: cluster.to_string(),
                group_id,
                purge,
            },
            |request| client.snapshot_group(request),
        )
        .await
        .map_err(|error| request_attempt_error("snapshot_group", error))?;
        match response.result {
            Some(snapshot_group_response::Result::Success(result)) => Ok(SnapshotGroupResult {
                group_id: result.group_id,
                snapshot_index: result.snapshot_index,
                purged_index: result.purged.then_some(result.purged_index),
            }),
            Some(snapshot_group_response::Result::Error(error)) => {
                Err(domain_error_from_wire(error)?.into())
            }
            None => Err(ClientError::Protocol(
                "snapshot group response omitted its typed result".to_owned(),
            )),
        }
    }

    pub async fn replace_voter(
        &self,
        cluster: ClusterId,
        request_id: &str,
        expected_topology_revision: u64,
        remove_node_id: u64,
        add_node: &NodeDescriptor,
    ) -> Result<AdministrationStatus, ClientError> {
        self.administration_call(AdministrationCall::Replace(v1::ReplaceVoterRequest {
            cluster_id: cluster.to_string(),
            request_id: request_id.to_owned(),
            expected_topology_revision,
            remove_node_id,
            add_node: Some(v1::NodeDescriptor {
                node_id: add_node.node_id().get(),
                public_uri: add_node.public_uri().to_owned(),
                peer_uri: add_node.peer_uri().to_owned(),
            }),
        }))
        .await
    }

    pub async fn transfer_leadership(
        &self,
        cluster: ClusterId,
        request_id: &str,
        group_id: u64,
        target_node_id: u64,
    ) -> Result<AdministrationStatus, ClientError> {
        self.administration_call(AdministrationCall::Transfer(
            v1::TransferLeadershipRequest {
                cluster_id: cluster.to_string(),
                request_id: request_id.to_owned(),
                group_id,
                target_node_id,
            },
        ))
        .await
    }

    pub async fn administration_status(
        &self,
        cluster: ClusterId,
        request_id: &str,
    ) -> Result<AdministrationStatus, ClientError> {
        self.administration_call(AdministrationCall::Status(
            v1::AdministrationStatusRequest {
                cluster_id: cluster.to_string(),
                request_id: request_id.to_owned(),
            },
        ))
        .await
    }

    pub async fn abort_administration(
        &self,
        cluster: ClusterId,
        request_id: &str,
    ) -> Result<AdministrationStatus, ClientError> {
        self.administration_call(AdministrationCall::Abort(v1::AbortAdministrationRequest {
            cluster_id: cluster.to_string(),
            request_id: request_id.to_owned(),
        }))
        .await
    }

    async fn administration_call(
        &self,
        call: AdministrationCall,
    ) -> Result<AdministrationStatus, ClientError> {
        let deadline = Deadline::after(self.deadline);
        let mutation = call.is_mutation();
        let mut endpoints = self.seeds.clone();
        let mut index = 0usize;
        loop {
            let endpoint = endpoints[index % endpoints.len()].clone();
            let mut next_index = index.wrapping_add(1);
            match self
                .administration_once(&endpoint, call.clone(), deadline)
                .await
            {
                Ok(status) => return Ok(status),
                Err(AttemptError::Deadline) => {
                    return Err(ClientError::Domain(DomainError::QuorumUnavailable {
                        group: ConsensusGroup::Control,
                        outcome: if mutation {
                            RequestOutcome::AmbiguousCommit
                        } else {
                            RequestOutcome::NotApplicable
                        },
                        request: None,
                    }));
                }
                Err(AttemptError::Client(ClientError::Domain(DomainError::NotLeader {
                    leader,
                    ..
                }))) if self.retry => {
                    if let Some(hint) = leader {
                        next_index = add_hint(&mut endpoints, hint.public_uri())?;
                    }
                }
                Err(AttemptError::Client(ClientError::Connection(_) | ClientError::Request(_)))
                    if self.retry => {}
                Err(AttemptError::Client(error)) => return Err(error),
            }
            retry_sleep(deadline)
                .await
                .map_err(|_| non_write_deadline_error())?;
            index = next_index;
        }
    }

    async fn administration_once(
        &self,
        endpoint: &str,
        call: AdministrationCall,
        deadline: Deadline,
    ) -> Result<AdministrationStatus, AttemptError> {
        let mut client = connect_client(endpoint, deadline).await?;
        let response = match call {
            AdministrationCall::Replace(request) => {
                let (request, timeout) = timed_request(deadline, request)?;
                await_rpc(deadline, timeout, client.replace_voter(request)).await?
            }
            AdministrationCall::Transfer(request) => {
                let (request, timeout) = timed_request(deadline, request)?;
                await_rpc(deadline, timeout, client.transfer_leadership(request)).await?
            }
            AdministrationCall::Status(request) => {
                let (request, timeout) = timed_request(deadline, request)?;
                await_rpc(deadline, timeout, client.get_administration(request)).await?
            }
            AdministrationCall::Abort(request) => {
                let (request, timeout) = timed_request(deadline, request)?;
                await_rpc(deadline, timeout, client.abort_administration(request)).await?
            }
        };
        administration_status_from_wire(response).map_err(AttemptError::Client)
    }

    async fn publish_once(
        &self,
        endpoint: &str,
        request: v1::PublishRequest,
        deadline: Deadline,
        request_may_have_reached: &mut bool,
    ) -> Result<PublishReceipt, AttemptError> {
        let mut client = connect_client(endpoint, deadline).await?;
        let (request, timeout) = timed_request(deadline, request)?;
        *request_may_have_reached = true;
        let response = await_rpc(deadline, timeout, client.commit_publish(request)).await?;
        match response.result {
            Some(publish_response::Result::Success(value)) => {
                publish_success_from_wire(value).map_err(AttemptError::Client)
            }
            Some(publish_response::Result::Error(value)) => {
                Err(AttemptError::Client(decode_domain_error(value)?.into()))
            }
            Some(publish_response::Result::Unsupported(value)) => Err(AttemptError::Client(
                DomainError::UnsupportedOperation {
                    operation: value.operation,
                    available_phase: value.available_phase,
                }
                .into(),
            )),
            None => Err(AttemptError::Client(ClientError::Protocol(
                "publish response omitted its typed result".to_owned(),
            ))),
        }
    }

    async fn advance_retention_once(
        &self,
        endpoint: &str,
        request: v1::AdvanceRetentionRequest,
        deadline: Deadline,
        request_may_have_reached: &mut bool,
    ) -> Result<RetentionResult, AttemptError> {
        let mut client = connect_client(endpoint, deadline).await?;
        let (request, timeout) = timed_request(deadline, request)?;
        *request_may_have_reached = true;
        let response = await_rpc(deadline, timeout, client.advance_retention(request)).await?;
        match response.result {
            Some(advance_retention_response::Result::Success(result)) => {
                retention_result_from_wire(result)
                    .map_err(ClientError::Domain)
                    .map_err(AttemptError::Client)
            }
            Some(advance_retention_response::Result::Error(value)) => {
                Err(AttemptError::Client(decode_domain_error(value)?.into()))
            }
            None => Err(AttemptError::Client(ClientError::Protocol(
                "retention response omitted its typed result".to_owned(),
            ))),
        }
    }

    async fn replay_mutation_once(
        &self,
        endpoint: &str,
        call: ReplayMutationCall,
        deadline: Deadline,
        request_may_have_reached: &mut bool,
    ) -> Result<ReplayLease, AttemptError> {
        let mut client = connect_client(endpoint, deadline).await?;
        *request_may_have_reached = true;
        let response = match call {
            ReplayMutationCall::Admit(request) => {
                let (request, timeout) = timed_request(deadline, request)?;
                await_rpc(deadline, timeout, client.admit_replay_lease(request)).await?
            }
            ReplayMutationCall::Renew(request) => {
                let (request, timeout) = timed_request(deadline, request)?;
                await_rpc(deadline, timeout, client.renew_replay_lease(request)).await?
            }
            ReplayMutationCall::Release(request) => {
                let (request, timeout) = timed_request(deadline, request)?;
                await_rpc(deadline, timeout, client.release_replay_lease(request)).await?
            }
        };
        match response.result {
            Some(replay_lease_response::Result::Lease(lease)) => replay_lease_from_wire(lease)
                .map_err(ClientError::Domain)
                .map_err(AttemptError::Client),
            Some(replay_lease_response::Result::Error(value)) => {
                Err(AttemptError::Client(decode_domain_error(value)?.into()))
            }
            None => Err(AttemptError::Client(ClientError::Protocol(
                "replay lease response omitted its typed result".to_owned(),
            ))),
        }
    }

    async fn fetch_once(
        &self,
        endpoint: &str,
        request: v1::FetchRequest,
        deadline: Deadline,
    ) -> Result<FetchPage, AttemptError> {
        let mut client = connect_client(endpoint, deadline).await?;
        let response = execute_rpc(deadline, request, |request| client.fetch(request)).await?;
        match response.result {
            Some(fetch_response::Result::Success(value)) => {
                let partition = PartitionKey::new(
                    value.stream_id.parse()?,
                    PartitionId::new(value.partition_id),
                );
                Ok(FetchPage::new(
                    partition,
                    value
                        .records
                        .into_iter()
                        .map(|record| {
                            CommittedRecord::new(RecordOffset::new(record.offset), record.payload)
                        })
                        .collect(),
                    RecordOffset::new(value.next_offset),
                ))
            }
            Some(fetch_response::Result::Error(value)) => {
                Err(AttemptError::Client(decode_domain_error(value)?.into()))
            }
            None => Err(AttemptError::Client(ClientError::Protocol(
                "fetch response omitted its typed result".to_owned(),
            ))),
        }
    }

    async fn receipt_once(
        &self,
        endpoint: &str,
        request: v1::ReceiptRequest,
        deadline: Deadline,
    ) -> Result<PublishReceipt, AttemptError> {
        let mut client = connect_client(endpoint, deadline).await?;
        let response =
            execute_rpc(deadline, request, |request| client.get_receipt(request)).await?;
        match response.result {
            Some(receipt_response::Result::Success(value)) => {
                publish_success_from_wire(value).map_err(AttemptError::Client)
            }
            Some(receipt_response::Result::Error(value)) => {
                Err(AttemptError::Client(decode_domain_error(value)?.into()))
            }
            None => Err(AttemptError::Client(ClientError::Protocol(
                "receipt response omitted its typed result".to_owned(),
            ))),
        }
    }
}

fn publish_request_to_wire(batch: &PublishBatch, route: &PartitionRoute) -> v1::PublishRequest {
    v1::PublishRequest {
        cluster_id: batch.cluster().to_string(),
        stream_id: batch.partition().stream().to_string(),
        partition_id: batch.partition().partition().get(),
        request_id: Some(request_id_to_wire(batch.request())),
        records: batch.records().to_vec(),
        route_group_id: route.group().get(),
        route_revision: route.route_revision(),
        bookmark_name: batch
            .bookmark()
            .map_or_else(String::new, ToString::to_string),
    }
}

fn request_id_to_wire(request: &ProducerRequestId) -> v1::ProducerRequestId {
    v1::ProducerRequestId {
        principal_id: request.principal().to_string(),
        producer_session_id: request.session().to_string(),
        sequence: request.sequence().get(),
    }
}

fn administration_status_from_wire(
    response: v1::AdministrationResponse,
) -> Result<AdministrationStatus, ClientError> {
    match response.result {
        Some(administration_response::Result::Operation(operation)) => Ok(AdministrationStatus {
            request_id: operation.request_id,
            kind: operation.kind,
            lifecycle: operation.lifecycle,
            topology_revision: operation.topology_revision,
            remove_node_id: (operation.remove_node_id != 0).then_some(operation.remove_node_id),
            add_node: match operation.add_node {
                Some(node) => Some(NodeDescriptor::new(
                    light_stream_core::NodeId::new(node.node_id).map_err(ClientError::from)?,
                    node.public_uri,
                    node.peer_uri,
                )),
                None => None,
            },
            group_id: (operation.group_id != 0).then_some(operation.group_id),
            target_node_id: (operation.target_node_id != 0).then_some(operation.target_node_id),
        }),
        Some(administration_response::Result::Error(error)) => {
            Err(domain_error_from_wire(error)?.into())
        }
        None => Err(ClientError::Protocol(
            "administration response omitted its typed result".to_owned(),
        )),
    }
}

fn publish_success_from_wire(value: v1::PublishSuccess) -> Result<PublishReceipt, ClientError> {
    let request = value
        .request_id
        .ok_or_else(|| ClientError::Protocol("publish success omitted request_id".to_owned()))?;
    let range = value
        .range
        .ok_or_else(|| ClientError::Protocol("publish success omitted range".to_owned()))?;
    let stream = range.stream_id.parse::<StreamId>()?;
    let producer_request = ProducerRequestId::new(
        light_stream_core::PrincipalId::parse(request.principal_id)?,
        request.producer_session_id.parse::<ProducerSessionId>()?,
        RequestSequence::new(request.sequence),
    );
    let committed_range = CommittedRecordRange::new(
        PartitionKey::new(stream, PartitionId::new(range.partition_id)),
        RecordOffset::new(range.first_offset),
        range.count,
    )?;
    let bookmark = value
        .bookmark
        .map(bookmark_from_wire)
        .transpose()
        .map_err(ClientError::Domain)?;
    Ok(PublishReceipt::new(
        producer_request,
        committed_range,
        bookmark,
    ))
}

fn decode_domain_error(value: v1::ErrorResult) -> Result<DomainError, ClientError> {
    domain_error_from_wire(value).map_err(ClientError::Domain)
}

async fn connect_client(
    endpoint: &str,
    deadline: Deadline,
) -> Result<LightStreamClient<Channel>, AttemptError> {
    let channel = connect_channel(endpoint, deadline).await?;
    Ok(LightStreamClient::new(channel)
        .max_decoding_message_size(MAX_PUBLIC_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_PUBLIC_MESSAGE_BYTES))
}

async fn connect_channel(endpoint: &str, deadline: Deadline) -> Result<Channel, AttemptError> {
    let timeout = deadline.remaining().ok_or(AttemptError::Deadline)?;
    let transport = Endpoint::from_shared(endpoint.to_owned())
        .map_err(|error| ClientError::InvalidEndpoint {
            endpoint: endpoint.to_owned(),
            reason: error.to_string(),
        })?
        .connect_timeout(timeout)
        .timeout(timeout);
    match tokio::time::timeout(timeout, transport.connect()).await {
        Ok(Ok(channel)) => Ok(channel),
        Ok(Err(_)) if deadline.expired() => Err(AttemptError::Deadline),
        Ok(Err(error)) => Err(ClientError::Connection(error.to_string()).into()),
        Err(_) => Err(AttemptError::Deadline),
    }
}

fn timed_request<T>(deadline: Deadline, value: T) -> Result<(Request<T>, Duration), AttemptError> {
    let timeout = deadline.remaining().ok_or(AttemptError::Deadline)?;
    let mut request = Request::new(value);
    request.set_timeout(timeout);
    Ok((request, timeout))
}

async fn await_rpc<T>(
    deadline: Deadline,
    timeout: Duration,
    rpc: impl Future<Output = Result<Response<T>, Status>>,
) -> Result<T, AttemptError> {
    match tokio::time::timeout(timeout, rpc).await {
        Ok(Ok(response)) => Ok(response.into_inner()),
        Ok(Err(status)) if status.code() == Code::DeadlineExceeded || deadline.expired() => {
            Err(AttemptError::Deadline)
        }
        Ok(Err(status)) => Err(ClientError::Request(status.to_string()).into()),
        Err(_) => Err(AttemptError::Deadline),
    }
}

async fn execute_rpc<RequestValue, ResponseValue, Call, CallFuture>(
    deadline: Deadline,
    value: RequestValue,
    call: Call,
) -> Result<ResponseValue, AttemptError>
where
    Call: FnOnce(Request<RequestValue>) -> CallFuture,
    CallFuture: Future<Output = Result<Response<ResponseValue>, Status>>,
{
    let (request, timeout) = timed_request(deadline, value)?;
    await_rpc(deadline, timeout, call(request)).await
}

async fn retry_sleep(deadline: Deadline) -> Result<(), AttemptError> {
    let remaining = deadline.remaining().ok_or(AttemptError::Deadline)?;
    tokio::time::sleep(Duration::from_millis(25).min(remaining)).await;
    if deadline.expired() {
        Err(AttemptError::Deadline)
    } else {
        Ok(())
    }
}

fn request_attempt_error(operation: &str, error: AttemptError) -> ClientError {
    match error {
        AttemptError::Deadline => ClientError::Request(format!("{operation} deadline expired")),
        AttemptError::Client(error) => error,
    }
}

fn publish_deadline_error(
    request: ProducerRequestId,
    request_may_have_reached: bool,
) -> ClientError {
    DomainError::QuorumUnavailable {
        group: ConsensusGroup::Data,
        outcome: if request_may_have_reached {
            RequestOutcome::AmbiguousCommit
        } else {
            RequestOutcome::DefiniteNoCommit
        },
        request: Some(AmbiguousRequest::Publish { request }),
    }
    .into()
}

fn mutation_deadline_error(
    request: light_stream_core::MutationRequestId,
    request_may_have_reached: bool,
) -> ClientError {
    DomainError::QuorumUnavailable {
        group: ConsensusGroup::Data,
        outcome: if request_may_have_reached {
            RequestOutcome::AmbiguousCommit
        } else {
            RequestOutcome::DefiniteNoCommit
        },
        request: Some(AmbiguousRequest::Mutation { request }),
    }
    .into()
}

fn fetch_success_from_wire(value: v1::FetchSuccess) -> Result<FetchPage, ClientError> {
    Ok(FetchPage::new(
        PartitionKey::new(
            value.stream_id.parse()?,
            PartitionId::new(value.partition_id),
        ),
        value
            .records
            .into_iter()
            .map(|record| CommittedRecord::new(RecordOffset::new(record.offset), record.payload))
            .collect(),
        RecordOffset::new(value.next_offset),
    ))
}

fn non_write_deadline_error() -> ClientError {
    DomainError::QuorumUnavailable {
        group: ConsensusGroup::Data,
        outcome: RequestOutcome::NotApplicable,
        request: None,
    }
    .into()
}

fn validate_endpoint(endpoint: &str) -> Result<(), ClientError> {
    Endpoint::from_shared(endpoint.to_owned())
        .map(|_| ())
        .map_err(|error| ClientError::InvalidEndpoint {
            endpoint: endpoint.to_owned(),
            reason: error.to_string(),
        })
}

fn add_hint(endpoints: &mut Vec<String>, endpoint: &str) -> Result<usize, ClientError> {
    validate_endpoint(endpoint)?;
    if let Some(position) = endpoints.iter().position(|value| value == endpoint) {
        let endpoint = endpoints.remove(position);
        endpoints.insert(0, endpoint);
    } else {
        endpoints.insert(0, endpoint.to_owned());
    }
    Ok(0)
}

impl NodeDiagnostics {
    fn from_wire(value: v1::DiagnosticsResponse) -> Result<Self, ClientError> {
        Ok(Self {
            node_id: value.node_id,
            lifecycle: value.lifecycle,
            peers: value
                .peers
                .into_iter()
                .map(|peer| {
                    Ok(NodeDescriptor::new(
                        light_stream_core::NodeId::new(peer.node_id)?,
                        peer.public_uri,
                        peer.peer_uri,
                    ))
                })
                .collect::<Result<Vec<_>, DomainError>>()?,
            groups: value
                .groups
                .into_iter()
                .map(|group| GroupDiagnostics {
                    group: match v1::ConsensusGroup::try_from(group.group).ok() {
                        Some(v1::ConsensusGroup::Control) => "control",
                        Some(v1::ConsensusGroup::Data) => "data",
                        _ => "unknown",
                    }
                    .to_owned(),
                    group_id: group.group_id,
                    local_role: group.local_role,
                    current_leader: group.has_current_leader.then_some(group.current_leader_id),
                    effective_uniform: group.effective_uniform,
                    effective_voters: group.effective_voters,
                    effective_learners: group.effective_learners,
                    committed_uniform: group.committed_uniform,
                    committed_voters: group.committed_voters,
                    committed_learners: group.committed_learners,
                    last_log_index: group.has_last_log.then_some(group.last_log_index),
                    local_committed_index: group
                        .has_local_committed
                        .then_some(group.local_committed_index),
                    cluster_committed_index: group
                        .has_cluster_committed
                        .then_some(group.cluster_committed_index),
                    last_applied_index: group.has_last_applied.then_some(group.last_applied_index),
                    replication: group
                        .replication
                        .into_iter()
                        .map(|progress| ReplicationDiagnostics {
                            target_node_id: progress.target_node_id,
                            matched_log_index: progress
                                .has_matched_log
                                .then_some(progress.matched_log_index),
                        })
                        .collect(),
                    snapshot_index: group.has_snapshot.then_some(group.snapshot_index),
                    purged_index: group.has_purged.then_some(group.purged_index),
                    slot: group.has_slot.then_some(group.slot as u16),
                    cache_budget_bytes: group.cache_budget_bytes,
                    write_buffer_budget_bytes: group.write_buffer_budget_bytes,
                })
                .collect(),
            data_group_slots: value.data_group_slots,
            data_group_count: value.data_group_count,
            rocksdb_cache_budget_bytes: value.rocksdb_cache_budget_bytes,
            rocksdb_write_buffer_budget_bytes: value.rocksdb_write_buffer_budget_bytes,
            per_group_cache_bytes: value.per_group_cache_bytes,
            per_group_write_buffer_bytes: value.per_group_write_buffer_bytes,
            unsupported_claims: value.unsupported_claims,
        })
    }
}

pub fn default_probe(payload: Vec<u8>) -> Result<PublishProbe, DomainError> {
    PublishProbe::new(
        "018f3f7e-5b3b-7c11-98f7-b65ac15f65be".parse()?,
        PartitionKey::new(
            "018f3f7e-5b3b-7c11-98f7-b65ac15f65bf".parse()?,
            PartitionId::new(0),
        ),
        ProducerRequestId::new(
            light_stream_core::PrincipalId::parse("ls01-probe")?,
            "018f3f7e-5b3b-7c11-98f7-b65ac15f65c0".parse()?,
            RequestSequence::new(1),
        ),
        vec![payload],
    )
}

pub fn security_mode_name(mode: SecurityMode) -> &'static str {
    match mode {
        SecurityMode::LocalInsecure => "local-insecure",
        SecurityMode::Secured => "secured",
    }
}

#[cfg(test)]
mod tests {
    use light_stream_core::PrincipalId;

    use super::*;

    fn request_id() -> ProducerRequestId {
        ProducerRequestId::new(
            PrincipalId::parse("deadline-test").unwrap(),
            "018f3f7e-5b3b-7c11-98f7-b65ac15f65c0".parse().unwrap(),
            RequestSequence::new(42),
        )
    }

    #[test]
    fn publish_deadline_preserves_request_and_commit_certainty() {
        let request = request_id();
        for (reached, expected) in [
            (false, RequestOutcome::DefiniteNoCommit),
            (true, RequestOutcome::AmbiguousCommit),
        ] {
            let ClientError::Domain(DomainError::QuorumUnavailable {
                group,
                outcome,
                request: actual,
            }) = publish_deadline_error(request.clone(), reached)
            else {
                panic!("publish deadline must be a typed quorum result");
            };
            assert_eq!(ConsensusGroup::Data, group);
            assert_eq!(expected, outcome);
            assert_eq!(
                Some(AmbiguousRequest::Publish {
                    request: request.clone()
                }),
                actual
            );
        }
    }

    #[test]
    fn non_write_deadline_is_not_ambiguous() {
        let ClientError::Domain(DomainError::QuorumUnavailable {
            group,
            outcome,
            request,
        }) = non_write_deadline_error()
        else {
            panic!("read deadline must be a typed quorum result");
        };
        assert_eq!(ConsensusGroup::Data, group);
        assert_eq!(RequestOutcome::NotApplicable, outcome);
        assert_eq!(None, request);
    }

    #[test]
    fn bookmark_domain_failures_use_conflict_exit_code() {
        assert_eq!(
            ClientError::Domain(DomainError::BookmarkNotFound).exit_code(),
            4
        );
        assert_eq!(
            ClientError::Domain(DomainError::BookmarkNameConflict).exit_code(),
            4
        );
    }

    #[test]
    fn deadline_expires_once_and_never_resets() {
        let deadline = Deadline {
            expires_at: Instant::now() - Duration::from_millis(1),
        };
        assert!(deadline.remaining().is_none());
        assert!(deadline.expired());
    }

    #[test]
    fn leader_hint_is_the_next_endpoint() {
        let mut endpoints = vec![
            "http://127.0.0.1:7101".to_owned(),
            "http://127.0.0.1:7102".to_owned(),
            "http://127.0.0.1:7103".to_owned(),
        ];
        let next_index =
            add_hint(&mut endpoints, "http://127.0.0.1:7103").expect("valid leader hint");

        assert_eq!(0, next_index);
        assert_eq!("http://127.0.0.1:7103", endpoints[next_index]);
    }
}
