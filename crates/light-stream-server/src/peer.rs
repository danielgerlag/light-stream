use std::{
    collections::BTreeMap,
    future::Future,
    marker::PhantomData,
    sync::Arc,
    time::{Duration, Instant},
};

use light_stream_core::{ClusterId, DomainError, NodeDescriptor};
use light_stream_storage::{CONTROL_GROUP_ID, ControlRaftConfig, DATA_GROUP_ID, DataRaftConfig};
use openraft::{
    BasicNode, OptionalSend, RaftTypeConfig,
    errors::{NetworkError, RPCError, ReplicationClosed, StreamingError, Timeout, Unreachable},
    network::{RPCOption, RPCTypes, RaftNetworkFactory, RaftNetworkV2},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
    },
    type_config::alias::{SnapshotOf, VoteOf},
};
use serde::{Serialize, de::DeserializeOwned};
use tonic::{Code, Request, Response, Status, transport::Endpoint};

use crate::{config::PeerRoutes, manifest::FormationSpec, runtime::ClusterManager};

pub(crate) mod wire {
    tonic::include_proto!("lightstream.peer.v1");
}

pub const PEER_PROTOCOL_VERSION: u32 = 1;
pub const PEER_CODEC_VERSION: u32 = 1;
pub const MAX_PEER_MESSAGE_BYTES: usize = 40 * 1024 * 1024;
const LIFECYCLE_GROUP_ID: u64 = 0;
const LIFECYCLE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Copy)]
struct RpcDeadline {
    timeout: Duration,
    expires_at: Instant,
}

impl RpcDeadline {
    fn after(timeout: Duration) -> Self {
        Self {
            timeout,
            expires_at: Instant::now() + timeout,
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

#[derive(Clone)]
pub struct TonicNetworkFactory<C> {
    cluster_id: ClusterId,
    group_id: u64,
    sender_node_id: u64,
    members: Arc<BTreeMap<u64, NodeDescriptor>>,
    peer_routes: PeerRoutes,
    marker: PhantomData<C>,
}

impl<C> TonicNetworkFactory<C> {
    pub fn new(
        cluster_id: ClusterId,
        group_id: u64,
        sender_node_id: u64,
        members: impl IntoIterator<Item = NodeDescriptor>,
        peer_routes: PeerRoutes,
    ) -> Self {
        Self {
            cluster_id,
            group_id,
            sender_node_id,
            members: Arc::new(
                members
                    .into_iter()
                    .map(|member| (member.node_id().get(), member))
                    .collect(),
            ),
            peer_routes,
            marker: PhantomData,
        }
    }

    fn build_network(&self, target: u64, node: &BasicNode) -> TonicRaftNetwork<C> {
        let configured = self.members.get(&target);
        let (endpoint, valid) = match configured {
            Some(member) if member.peer_uri() == node.addr => (
                Some(
                    self.peer_routes
                        .get(target)
                        .unwrap_or_else(|| member.peer_uri())
                        .to_owned(),
                ),
                true,
            ),
            _ => (None, false),
        };
        TonicRaftNetwork {
            cluster_id: self.cluster_id,
            group_id: self.group_id,
            sender_node_id: self.sender_node_id,
            target_node_id: target,
            endpoint,
            valid,
            marker: PhantomData,
        }
    }
}

impl<C> RaftNetworkFactory<C> for TonicNetworkFactory<C>
where
    C: RaftTypeConfig<NodeId = u64, Node = BasicNode>,
    AppendEntriesRequest<C>: Serialize,
    AppendEntriesResponse<C>: DeserializeOwned,
    VoteRequest<C>: Serialize,
    VoteResponse<C>: DeserializeOwned,
{
    type Network = TonicRaftNetwork<C>;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        self.build_network(target, node)
    }

    async fn new_heartbeat_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        self.build_network(target, node)
    }

    async fn new_snapshot_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        self.build_network(target, node)
    }
}

pub struct TonicRaftNetwork<C> {
    cluster_id: ClusterId,
    group_id: u64,
    sender_node_id: u64,
    target_node_id: u64,
    endpoint: Option<String>,
    valid: bool,
    marker: PhantomData<C>,
}

impl<C> TonicRaftNetwork<C>
where
    C: RaftTypeConfig<NodeId = u64>,
{
    fn unavailable(&self, reason: impl ToString) -> RPCError<C> {
        RPCError::Unreachable(Unreachable::from_string(reason))
    }

    fn timeout(&self, action: RPCTypes, timeout: Duration) -> RPCError<C> {
        RPCError::Timeout(Timeout {
            action,
            id: self.sender_node_id,
            target: self.target_node_id,
            timeout,
        })
    }

    async fn client(
        &self,
        action: RPCTypes,
        deadline: RpcDeadline,
    ) -> Result<wire::peer_service_client::PeerServiceClient<tonic::transport::Channel>, RPCError<C>>
    {
        if !self.valid {
            return Err(self
                .unavailable("Openraft node metadata conflicts with the durable peer directory"));
        }
        let endpoint = self
            .endpoint
            .as_ref()
            .ok_or_else(|| self.unavailable("target is absent from the durable peer directory"))?;
        let timeout = deadline
            .remaining()
            .ok_or_else(|| self.timeout(action, deadline.timeout))?;
        let transport = Endpoint::from_shared(endpoint.clone())
            .map_err(|error| self.unavailable(error))?
            .connect_timeout(timeout)
            .timeout(timeout);
        match tokio::time::timeout(timeout, transport.connect()).await {
            Ok(Ok(channel)) => Ok(wire::peer_service_client::PeerServiceClient::new(channel)
                .max_decoding_message_size(MAX_PEER_MESSAGE_BYTES)
                .max_encoding_message_size(MAX_PEER_MESSAGE_BYTES)),
            Ok(Err(_)) if deadline.expired() => Err(self.timeout(action, deadline.timeout)),
            Ok(Err(error)) => Err(self.unavailable(error)),
            Err(_) => Err(self.timeout(action, deadline.timeout)),
        }
    }

    fn request<T: Serialize>(
        &self,
        value: &T,
        timeout: Duration,
    ) -> Result<Request<wire::PeerRequest>, RPCError<C>> {
        let payload = serde_json::to_vec(value)
            .map_err(|error| RPCError::Network(NetworkError::new(&error)))?;
        if payload.len() > MAX_PEER_MESSAGE_BYTES {
            return Err(RPCError::Network(NetworkError::from_string(
                "encoded peer request exceeds the configured limit",
            )));
        }
        let mut request = Request::new(wire::PeerRequest {
            envelope: Some(wire::PeerEnvelope {
                protocol_version: PEER_PROTOCOL_VERSION,
                codec_version: PEER_CODEC_VERSION,
                cluster_id: self.cluster_id.to_string(),
                group_id: self.group_id,
                sender_node_id: self.sender_node_id,
                target_node_id: self.target_node_id,
                payload,
            }),
        });
        request.set_timeout(timeout);
        Ok(request)
    }

    fn response<T: DeserializeOwned>(
        &self,
        response: wire::PeerResponse,
    ) -> Result<T, RPCError<C>> {
        let envelope = response.envelope.ok_or_else(|| {
            RPCError::Network(NetworkError::from_string(
                "peer response omitted its envelope",
            ))
        })?;
        if envelope.protocol_version != PEER_PROTOCOL_VERSION
            || envelope.codec_version != PEER_CODEC_VERSION
            || envelope.cluster_id != self.cluster_id.to_string()
            || envelope.group_id != self.group_id
            || envelope.sender_node_id != self.target_node_id
            || envelope.target_node_id != self.sender_node_id
            || envelope.payload.len() > MAX_PEER_MESSAGE_BYTES
        {
            return Err(RPCError::Network(NetworkError::from_string(
                "peer response envelope does not match the request",
            )));
        }
        serde_json::from_slice(&envelope.payload)
            .map_err(|error| RPCError::Network(NetworkError::new(&error)))
    }

    fn status(&self, action: RPCTypes, deadline: RpcDeadline, status: Status) -> RPCError<C> {
        match status.code() {
            Code::DeadlineExceeded => self.timeout(action, deadline.timeout),
            Code::Unavailable => self.unavailable(status),
            _ => RPCError::Network(NetworkError::from_string(status)),
        }
    }

    async fn response_with_deadline<T>(
        &self,
        action: RPCTypes,
        deadline: RpcDeadline,
        timeout: Duration,
        rpc: impl Future<Output = Result<Response<T>, Status>>,
    ) -> Result<T, RPCError<C>> {
        match tokio::time::timeout(timeout, rpc).await {
            Ok(Ok(response)) => Ok(response.into_inner()),
            Ok(Err(_)) if deadline.expired() => Err(self.timeout(action, deadline.timeout)),
            Ok(Err(status)) => Err(self.status(action, deadline, status)),
            Err(_) => Err(self.timeout(action, deadline.timeout)),
        }
    }
}

impl<C> RaftNetworkV2<C> for TonicRaftNetwork<C>
where
    C: RaftTypeConfig<NodeId = u64, Node = BasicNode>,
    AppendEntriesRequest<C>: Serialize,
    AppendEntriesResponse<C>: DeserializeOwned,
    VoteRequest<C>: Serialize,
    VoteResponse<C>: DeserializeOwned,
{
    type SnapshotData = Vec<u8>;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<C>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<C>, RPCError<C>> {
        let deadline = RpcDeadline::after(option.soft_ttl());
        let mut client = self.client(RPCTypes::AppendEntries, deadline).await?;
        let timeout = deadline
            .remaining()
            .ok_or_else(|| self.timeout(RPCTypes::AppendEntries, deadline.timeout))?;
        let request = self.request(&rpc, timeout)?;
        let response = self
            .response_with_deadline(
                RPCTypes::AppendEntries,
                deadline,
                timeout,
                client.append_entries(request),
            )
            .await?;
        self.response(response)
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<C>,
        option: RPCOption,
    ) -> Result<VoteResponse<C>, RPCError<C>> {
        let deadline = RpcDeadline::after(option.soft_ttl());
        let mut client = self.client(RPCTypes::Vote, deadline).await?;
        let timeout = deadline
            .remaining()
            .ok_or_else(|| self.timeout(RPCTypes::Vote, deadline.timeout))?;
        let request = self.request(&rpc, timeout)?;
        let response = self
            .response_with_deadline(RPCTypes::Vote, deadline, timeout, client.vote(request))
            .await?;
        self.response(response)
    }

    async fn pre_vote(
        &mut self,
        rpc: VoteRequest<C>,
        option: RPCOption,
    ) -> Result<VoteResponse<C>, RPCError<C>> {
        let deadline = RpcDeadline::after(option.soft_ttl());
        let mut client = self.client(RPCTypes::Vote, deadline).await?;
        let timeout = deadline
            .remaining()
            .ok_or_else(|| self.timeout(RPCTypes::Vote, deadline.timeout))?;
        let request = self.request(&rpc, timeout)?;
        let response = self
            .response_with_deadline(RPCTypes::Vote, deadline, timeout, client.pre_vote(request))
            .await?;
        self.response(response)
    }

    async fn full_snapshot(
        &mut self,
        _vote: VoteOf<C>,
        _snapshot: SnapshotOf<C, Self::SnapshotData>,
        _cancel: impl std::future::Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<C>, StreamingError<C>> {
        Err(StreamingError::Network(NetworkError::from_string(
            "full_snapshot is unsupported until LS06",
        )))
    }
}

pub struct PeerApi {
    cluster: Arc<ClusterManager>,
}

impl PeerApi {
    pub fn new(cluster: Arc<ClusterManager>) -> Self {
        Self { cluster }
    }
}

#[tonic::async_trait]
impl wire::peer_service_server::PeerService for PeerApi {
    async fn append_entries(
        &self,
        request: Request<wire::PeerRequest>,
    ) -> Result<Response<wire::PeerResponse>, Status> {
        let envelope = require_envelope(request)?;
        let response = match envelope.group_id {
            CONTROL_GROUP_ID => {
                let rpc = decode::<AppendEntriesRequest<ControlRaftConfig>>(&envelope)?;
                let value = self
                    .cluster
                    .peer_control(&envelope)
                    .await?
                    .append_entries(rpc)
                    .await
                    .map_err(internal_status)?;
                encode_response(&envelope, &value)?
            }
            group_id if group_id >= DATA_GROUP_ID => {
                let rpc = decode::<AppendEntriesRequest<DataRaftConfig>>(&envelope)?;
                let value = self
                    .cluster
                    .peer_data(&envelope)
                    .await?
                    .append_entries(rpc)
                    .await
                    .map_err(internal_status)?;
                encode_response(&envelope, &value)?
            }
            _ => return Err(Status::invalid_argument("unknown Raft group")),
        };
        Ok(Response::new(response))
    }

    async fn vote(
        &self,
        request: Request<wire::PeerRequest>,
    ) -> Result<Response<wire::PeerResponse>, Status> {
        self.vote_request(request, false).await
    }

    async fn pre_vote(
        &self,
        request: Request<wire::PeerRequest>,
    ) -> Result<Response<wire::PeerResponse>, Status> {
        self.vote_request(request, true).await
    }

    async fn prepare_join(
        &self,
        request: Request<wire::PeerRequest>,
    ) -> Result<Response<wire::PeerResponse>, Status> {
        let envelope = require_envelope(request)?;
        if envelope.group_id != LIFECYCLE_GROUP_ID {
            return Err(Status::invalid_argument(
                "lifecycle requests must use group zero",
            ));
        }
        let formation = decode::<FormationSpec>(&envelope)?;
        self.cluster.prepare_join(&envelope, formation).await?;
        Ok(Response::new(encode_response(&envelope, &())?))
    }

    async fn activate(
        &self,
        request: Request<wire::PeerRequest>,
    ) -> Result<Response<wire::PeerResponse>, Status> {
        let envelope = require_envelope(request)?;
        if envelope.group_id != LIFECYCLE_GROUP_ID {
            return Err(Status::invalid_argument(
                "lifecycle requests must use group zero",
            ));
        }
        let formation = decode::<FormationSpec>(&envelope)?;
        self.cluster.activate(&envelope, &formation).await?;
        Ok(Response::new(encode_response(&envelope, &())?))
    }
}

impl PeerApi {
    async fn vote_request(
        &self,
        request: Request<wire::PeerRequest>,
        pre_vote: bool,
    ) -> Result<Response<wire::PeerResponse>, Status> {
        let envelope = require_envelope(request)?;
        let response = match envelope.group_id {
            CONTROL_GROUP_ID => {
                let rpc = decode::<VoteRequest<ControlRaftConfig>>(&envelope)?;
                let raft = self.cluster.peer_control(&envelope).await?;
                let value = if pre_vote {
                    raft.pre_vote(rpc).await
                } else {
                    raft.vote(rpc).await
                }
                .map_err(internal_status)?;
                encode_response(&envelope, &value)?
            }
            group_id if group_id >= DATA_GROUP_ID => {
                let rpc = decode::<VoteRequest<DataRaftConfig>>(&envelope)?;
                let raft = self.cluster.peer_data(&envelope).await?;
                let value = if pre_vote {
                    raft.pre_vote(rpc).await
                } else {
                    raft.vote(rpc).await
                }
                .map_err(internal_status)?;
                encode_response(&envelope, &value)?
            }
            _ => return Err(Status::invalid_argument("unknown Raft group")),
        };
        Ok(Response::new(response))
    }
}

pub async fn prepare_remote(
    formation: &FormationSpec,
    sender_node_id: u64,
    target: &NodeDescriptor,
) -> Result<(), DomainError> {
    lifecycle_call("prepare_join", formation, sender_node_id, target).await
}

pub async fn activate_remote(
    formation: &FormationSpec,
    sender_node_id: u64,
    target: &NodeDescriptor,
) -> Result<(), DomainError> {
    lifecycle_call("activate", formation, sender_node_id, target).await
}

async fn lifecycle_call(
    operation: &str,
    formation: &FormationSpec,
    sender_node_id: u64,
    target: &NodeDescriptor,
) -> Result<(), DomainError> {
    let endpoint = Endpoint::from_shared(target.peer_uri().to_owned())
        .map_err(storage_error)?
        .connect_timeout(LIFECYCLE_TIMEOUT)
        .timeout(LIFECYCLE_TIMEOUT);
    let channel = tokio::time::timeout(LIFECYCLE_TIMEOUT, endpoint.connect())
        .await
        .map_err(|_| DomainError::Storage {
            reason: format!(
                "{operation} timed out connecting to node {}",
                target.node_id()
            ),
        })?
        .map_err(storage_error)?;
    let mut client = wire::peer_service_client::PeerServiceClient::new(channel)
        .max_decoding_message_size(MAX_PEER_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_PEER_MESSAGE_BYTES);
    let payload = serde_json::to_vec(formation).map_err(storage_error)?;
    let mut request = Request::new(wire::PeerRequest {
        envelope: Some(wire::PeerEnvelope {
            protocol_version: PEER_PROTOCOL_VERSION,
            codec_version: PEER_CODEC_VERSION,
            cluster_id: formation.cluster_id.to_string(),
            group_id: LIFECYCLE_GROUP_ID,
            sender_node_id,
            target_node_id: target.node_id().get(),
            payload,
        }),
    });
    request.set_timeout(LIFECYCLE_TIMEOUT);
    let result = if operation == "prepare_join" {
        client.prepare_join(request).await
    } else {
        client.activate(request).await
    };
    result.map_err(storage_error)?;
    Ok(())
}

fn require_envelope(request: Request<wire::PeerRequest>) -> Result<wire::PeerEnvelope, Status> {
    let envelope = request
        .into_inner()
        .envelope
        .ok_or_else(|| Status::invalid_argument("peer envelope is required"))?;
    if envelope.protocol_version != PEER_PROTOCOL_VERSION {
        return Err(Status::failed_precondition(
            "unsupported peer protocol version",
        ));
    }
    if envelope.codec_version != PEER_CODEC_VERSION {
        return Err(Status::failed_precondition(
            "unsupported peer codec version",
        ));
    }
    if envelope.payload.len() > MAX_PEER_MESSAGE_BYTES {
        return Err(Status::resource_exhausted(
            "peer payload exceeds the configured limit",
        ));
    }
    Ok(envelope)
}

fn decode<T: DeserializeOwned>(envelope: &wire::PeerEnvelope) -> Result<T, Status> {
    serde_json::from_slice(&envelope.payload)
        .map_err(|error| Status::invalid_argument(error.to_string()))
}

fn encode_response<T: Serialize>(
    request: &wire::PeerEnvelope,
    value: &T,
) -> Result<wire::PeerResponse, Status> {
    let payload = serde_json::to_vec(value).map_err(|error| Status::internal(error.to_string()))?;
    if payload.len() > MAX_PEER_MESSAGE_BYTES {
        return Err(Status::resource_exhausted(
            "peer response exceeds the configured limit",
        ));
    }
    Ok(wire::PeerResponse {
        envelope: Some(wire::PeerEnvelope {
            protocol_version: PEER_PROTOCOL_VERSION,
            codec_version: PEER_CODEC_VERSION,
            cluster_id: request.cluster_id.clone(),
            group_id: request.group_id,
            sender_node_id: request.target_node_id,
            target_node_id: request.sender_node_id,
            payload,
        }),
    })
}

fn internal_status(error: impl std::fmt::Display) -> Status {
    Status::internal(error.to_string())
}

fn storage_error(error: impl std::fmt::Display) -> DomainError {
    DomainError::Storage {
        reason: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use light_stream_core::{
        BootstrapSpec, ClusterId, MAX_PUBLISH_BYTES, PartitionId, PartitionKey, PrincipalId,
        ProducerRequestId, ProducerSessionId, PublishBatch, RequestSequence, StreamId, StreamName,
    };
    use openraft::{Entry, EntryPayload, Vote};
    use uuid::Uuid;

    use super::*;

    #[test]
    fn rpc_deadline_uses_one_decreasing_soft_ttl_budget() {
        let timeout = Duration::from_secs(1);
        let deadline = RpcDeadline::after(timeout);
        let first = deadline.remaining().unwrap();
        assert!(first <= timeout);
        assert_eq!(timeout, deadline.timeout);

        let expired = RpcDeadline::after(Duration::ZERO);
        assert!(expired.remaining().is_none());
        assert!(expired.expired());
    }

    #[test]
    fn durable_peer_identity_is_checked_before_using_route_override() {
        let cluster = ClusterId::from_uuid(Uuid::new_v4());
        let durable_uri = "http://127.0.0.1:7202";
        let override_uri = "http://127.0.0.1:7302";
        let target = NodeDescriptor::new(
            light_stream_core::NodeId::new(2).unwrap(),
            "http://127.0.0.1:7102",
            durable_uri,
        );
        let routes = PeerRoutes::parse(vec![format!("2={override_uri}")], 1).unwrap();
        let factory =
            TonicNetworkFactory::<DataRaftConfig>::new(cluster, DATA_GROUP_ID, 1, [target], routes);

        let valid = factory.build_network(2, &BasicNode::new(durable_uri));
        assert!(valid.valid);
        assert_eq!(Some(override_uri), valid.endpoint.as_deref());

        let invalid = factory.build_network(2, &BasicNode::new("http://127.0.0.1:7999"));
        assert!(!invalid.valid);
        assert_eq!(None, invalid.endpoint.as_deref());
    }

    #[test]
    fn maximum_publish_batch_fits_the_peer_limit() {
        let cluster = ClusterId::from_uuid(Uuid::new_v4());
        let stream = StreamId::from_uuid(Uuid::new_v4());
        let batch = PublishBatch::new(
            cluster,
            PartitionKey::new(stream, PartitionId::new(0)),
            ProducerRequestId::new(
                PrincipalId::parse("boundary").unwrap(),
                ProducerSessionId::from_uuid(Uuid::new_v4()),
                RequestSequence::new(1),
            ),
            vec![vec![255; MAX_PUBLISH_BYTES]],
        )
        .unwrap_err();
        assert!(matches!(batch, DomainError::InvalidPayload { .. }));

        let records = vec![vec![255; 1024 * 1024]; 8];
        let batch = PublishBatch::new(
            cluster,
            PartitionKey::new(stream, PartitionId::new(0)),
            ProducerRequestId::new(
                PrincipalId::parse("boundary").unwrap(),
                ProducerSessionId::from_uuid(Uuid::new_v4()),
                RequestSequence::new(1),
            ),
            records,
        )
        .unwrap();
        let entry = Entry::<<DataRaftConfig as RaftTypeConfig>::LeaderId, _, u64, BasicNode> {
            log_id: openraft::LogId::new(
                openraft::impls::leader_id_adv::LeaderId {
                    term: 1,
                    node_id: 1,
                },
                1,
            ),
            payload: EntryPayload::Normal(light_stream_storage::GroupCommand::Publish { batch }),
        };
        let request = AppendEntriesRequest::<DataRaftConfig> {
            vote: Vote::new_committed(1, 1),
            prev_log_id: None,
            entries: vec![entry],
            leader_commit: None,
        };
        assert!(serde_json::to_vec(&request).unwrap().len() < MAX_PEER_MESSAGE_BYTES);
        let _ = BootstrapSpec::new(cluster, stream, StreamName::parse("bootstrap").unwrap());
    }
}
