use std::{
    cmp::Ordering,
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    future::Future,
    io::{Read, Seek, SeekFrom, Write},
    marker::PhantomData,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    time::{Duration, Instant, SystemTime},
};

use crc32fast::Hasher as Crc32;
use light_stream_core::{ClusterId, DomainError, GroupId, NodeDescriptor, NodeId};
use light_stream_storage::{
    CONTROL_GROUP_ID, ControlRaftConfig, DATA_GROUP_ID, DataRaftConfig, RocksStateMachine,
    SnapshotArtifact, SnapshotDigest,
};
use openraft::{
    BasicNode, OptionalSend, Raft, RaftTypeConfig, Snapshot,
    errors::{NetworkError, RPCError, ReplicationClosed, StreamingError, Timeout, Unreachable},
    network::{RPCOption, RPCTypes, RaftNetworkFactory, RaftNetworkV2},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
    },
    raft::{TransferLeaderRequest, TransferLeaderResponse},
    storage::RaftStateMachine,
    type_config::alias::{LogIdOf, SnapshotMetaOf, SnapshotOf, VoteOf},
    type_config::async_runtime::WatchReceiver,
    vote::RaftVote,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};
use tonic::{Code, Request, Response, Status, transport::Endpoint};

use crate::{
    config::PeerRoutes,
    manifest::FormationSpec,
    runtime::ClusterManager,
    security::{PeerOperation, RuntimeSecurityConfig},
};

pub(crate) mod wire {
    tonic::include_proto!("lightstream.peer.v1");
}

pub const PEER_PROTOCOL_VERSION: u32 = 1;
pub const PEER_CODEC_VERSION: u32 = 1;
pub const MAX_PEER_MESSAGE_BYTES: usize = 40 * 1024 * 1024;
const LIFECYCLE_GROUP_ID: u64 = 0;
const LIFECYCLE_TIMEOUT: Duration = Duration::from_secs(10);
const SNAPSHOT_CHUNK_BYTES: usize = 1024 * 1024;
const ABANDONED_SNAPSHOT_AGE: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct SnapshotIntent {
    cluster_id: String,
    group_id: u64,
    sender_node_id: u64,
    target_node_id: u64,
    transfer_id: String,
    vote_json: Vec<u8>,
    meta_json: Vec<u8>,
    byte_len: u64,
    sha256: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ReplacementPreparation {
    pub formation: FormationSpec,
    pub topology: light_stream_core::ClusterTopology,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ReplacementRetirement {
    pub formation: FormationSpec,
    pub transitional_topology: light_stream_core::ClusterTopology,
    pub final_topology: light_stream_core::ClusterTopology,
}

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
    topology: PeerTopology,
    peer_routes: PeerRoutes,
    security: RuntimeSecurityConfig,
    marker: PhantomData<C>,
}

#[derive(Clone, Debug)]
pub struct PeerTopology {
    members: Arc<RwLock<BTreeMap<u64, NodeDescriptor>>>,
}

impl PeerTopology {
    pub fn new(members: impl IntoIterator<Item = NodeDescriptor>) -> Self {
        Self {
            members: Arc::new(RwLock::new(
                members
                    .into_iter()
                    .map(|member| (member.node_id().get(), member))
                    .collect(),
            )),
        }
    }

    pub fn replace(
        &self,
        members: impl IntoIterator<Item = NodeDescriptor>,
    ) -> Result<(), DomainError> {
        *self.members.write().map_err(|_| DomainError::Storage {
            reason: "peer topology lock poisoned".to_owned(),
        })? = members
            .into_iter()
            .map(|member| (member.node_id().get(), member))
            .collect();
        Ok(())
    }
}

impl<C> TonicNetworkFactory<C> {
    #[cfg(test)]
    pub fn new(
        cluster_id: ClusterId,
        group_id: u64,
        sender_node_id: u64,
        members: impl IntoIterator<Item = NodeDescriptor>,
        peer_routes: PeerRoutes,
    ) -> Self {
        Self::with_topology(
            cluster_id,
            group_id,
            sender_node_id,
            PeerTopology::new(members),
            peer_routes,
            RuntimeSecurityConfig::LocalInsecure,
        )
    }

    pub fn with_topology(
        cluster_id: ClusterId,
        group_id: u64,
        sender_node_id: u64,
        topology: PeerTopology,
        peer_routes: PeerRoutes,
        security: RuntimeSecurityConfig,
    ) -> Self {
        Self {
            cluster_id,
            group_id,
            sender_node_id,
            topology,
            peer_routes,
            security,
            marker: PhantomData,
        }
    }

    fn build_network(&self, target: u64, node: &BasicNode) -> TonicRaftNetwork<C> {
        let members = self.topology.members.read().ok();
        let configured = members.as_ref().and_then(|members| members.get(&target));
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
            expected_peer_uri: configured.map(|member| member.peer_uri().to_owned()),
            valid,
            security: self.security.clone(),
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
    TransferLeaderRequest<C>: Serialize,
    TransferLeaderResponse<C>: DeserializeOwned,
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
    expected_peer_uri: Option<String>,
    valid: bool,
    security: RuntimeSecurityConfig,
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
        let transport = self
            .security
            .configure_peer_endpoint(
                transport,
                self.expected_peer_uri
                    .as_deref()
                    .ok_or_else(|| self.unavailable("target peer URI is unavailable"))?,
                light_stream_core::NodeId::new(self.target_node_id)
                    .map_err(|error| self.unavailable(error))?,
                match action {
                    RPCTypes::AppendEntries => PeerOperation::AppendEntries,
                    RPCTypes::Vote => PeerOperation::Vote,
                    RPCTypes::InstallSnapshot => PeerOperation::SnapshotBegin,
                    RPCTypes::TransferLeader => PeerOperation::TransferLeader,
                },
                if self.group_id == CONTROL_GROUP_ID {
                    crate::security::PeerRecoveryScope::ControlGroup
                } else {
                    crate::security::PeerRecoveryScope::None
                },
            )
            .map_err(|error| self.unavailable(error))?;
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
    TransferLeaderRequest<C>: Serialize,
    TransferLeaderResponse<C>: DeserializeOwned,
{
    type SnapshotData = SnapshotArtifact;

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
        vote: VoteOf<C>,
        snapshot: SnapshotOf<C, Self::SnapshotData>,
        cancel: impl std::future::Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<C>, StreamingError<C>> {
        let transfer = async {
            let vote_json = serde_json::to_vec(&vote)
                .map_err(|error| StreamingError::Network(NetworkError::new(&error)))?;
            let meta_json = serde_json::to_vec(&snapshot.meta)
                .map_err(|error| StreamingError::Network(NetworkError::new(&error)))?;
            let sha256 = snapshot.snapshot.digest().as_bytes().to_vec();
            let mut transfer_digest = Sha256::new();
            transfer_digest.update(self.cluster_id.as_uuid().as_bytes());
            transfer_digest.update(self.group_id.to_be_bytes());
            transfer_digest.update(self.sender_node_id.to_be_bytes());
            transfer_digest.update(self.target_node_id.to_be_bytes());
            transfer_digest.update(&vote_json);
            transfer_digest.update(&meta_json);
            transfer_digest.update(&sha256);
            let transfer_id = format!("{:x}", transfer_digest.finalize());
            let identity = wire::SnapshotIdentity {
                protocol_version: PEER_PROTOCOL_VERSION,
                cluster_id: self.cluster_id.to_string(),
                group_id: self.group_id,
                sender_node_id: self.sender_node_id,
                target_node_id: self.target_node_id,
                transfer_id,
            };
            let deadline = RpcDeadline::after(option.soft_ttl());
            let mut client = self
                .client(RPCTypes::InstallSnapshot, deadline)
                .await
                .map_err(StreamingError::from)?;
            let timeout = deadline
                .remaining()
                .ok_or_else(|| self.timeout(RPCTypes::InstallSnapshot, deadline.timeout))
                .map_err(StreamingError::from)?;
            let mut request = Request::new(wire::SnapshotBeginRequest {
                identity: Some(identity.clone()),
                vote_json,
                meta_json,
                byte_len: snapshot.snapshot.len(),
                sha256,
            });
            request.set_timeout(timeout);
            let begin = self
                .response_with_deadline(
                    RPCTypes::InstallSnapshot,
                    deadline,
                    timeout,
                    client.begin_snapshot(request),
                )
                .await
                .map_err(StreamingError::from)?;
            let mut offset = begin.durable_offset;
            if offset > snapshot.snapshot.len() {
                return Err(StreamingError::Network(NetworkError::from_string(
                    "receiver snapshot offset exceeds the artifact",
                )));
            }
            while offset < snapshot.snapshot.len() {
                let end = offset
                    .saturating_add(SNAPSHOT_CHUNK_BYTES as u64)
                    .min(snapshot.snapshot.len());
                let artifact = snapshot.snapshot.clone();
                let data = tokio::task::spawn_blocking(move || {
                    artifact.read_chunk(offset, SNAPSHOT_CHUNK_BYTES)
                })
                .await
                .map_err(|error| StreamingError::Network(NetworkError::new(&error)))?
                .map_err(|error| StreamingError::Network(NetworkError::new(&error)))?;
                let mut crc = Crc32::new();
                crc.update(&data);
                let timeout = deadline
                    .remaining()
                    .ok_or_else(|| self.timeout(RPCTypes::InstallSnapshot, deadline.timeout))
                    .map_err(StreamingError::from)?;
                let mut request = Request::new(wire::SnapshotChunkRequest {
                    identity: Some(identity.clone()),
                    offset,
                    data,
                    crc32: crc.finalize(),
                });
                request.set_timeout(timeout);
                let progress = self
                    .response_with_deadline(
                        RPCTypes::InstallSnapshot,
                        deadline,
                        timeout,
                        client.put_snapshot_chunk(request),
                    )
                    .await
                    .map_err(StreamingError::from)?;
                offset = progress.durable_offset;
                if offset != end {
                    return Err(StreamingError::Network(NetworkError::from_string(
                        "receiver returned an invalid snapshot offset",
                    )));
                }
            }
            let timeout = deadline
                .remaining()
                .ok_or_else(|| self.timeout(RPCTypes::InstallSnapshot, deadline.timeout))
                .map_err(StreamingError::from)?;
            let mut request = Request::new(wire::SnapshotFinishRequest {
                identity: Some(identity),
            });
            request.set_timeout(timeout);
            let finish = self
                .response_with_deadline(
                    RPCTypes::InstallSnapshot,
                    deadline,
                    timeout,
                    client.finish_snapshot(request),
                )
                .await
                .map_err(StreamingError::from)?;
            serde_json::from_slice(&finish.response_json)
                .map_err(|error| StreamingError::Network(NetworkError::new(&error)))
        };
        tokio::pin!(transfer);
        tokio::pin!(cancel);
        tokio::select! {
            result = &mut transfer => result,
            closed = &mut cancel => Err(StreamingError::Closed(closed)),
        }
    }

    async fn transfer_leader(
        &mut self,
        rpc: TransferLeaderRequest<C>,
        option: RPCOption,
    ) -> Result<TransferLeaderResponse<C>, RPCError<C>> {
        let deadline = RpcDeadline::after(option.soft_ttl());
        let mut client = self.client(RPCTypes::TransferLeader, deadline).await?;
        let timeout = deadline
            .remaining()
            .ok_or_else(|| self.timeout(RPCTypes::TransferLeader, deadline.timeout))?;
        let request = self.request(&rpc, timeout)?;
        let response = self
            .response_with_deadline(
                RPCTypes::TransferLeader,
                deadline,
                timeout,
                client.transfer_leader(request),
            )
            .await?;
        self.response(response)
    }
}

pub struct PeerApi {
    cluster: Arc<ClusterManager>,
    security: RuntimeSecurityConfig,
    snapshot_groups: AsyncMutex<BTreeMap<u64, Arc<AsyncMutex<()>>>>,
}

fn require_snapshot_identity(
    identity: Option<wire::SnapshotIdentity>,
) -> Result<wire::SnapshotIdentity, Status> {
    let identity = identity.ok_or_else(|| Status::invalid_argument("snapshot identity missing"))?;
    if identity.protocol_version != PEER_PROTOCOL_VERSION {
        return Err(Status::failed_precondition(
            "snapshot peer protocol version mismatch",
        ));
    }
    if identity.transfer_id.len() != 64
        || !identity
            .transfer_id
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(Status::invalid_argument("invalid snapshot transfer ID"));
    }
    Ok(identity)
}

fn snapshot_envelope(identity: &wire::SnapshotIdentity) -> wire::PeerEnvelope {
    wire::PeerEnvelope {
        protocol_version: identity.protocol_version,
        codec_version: PEER_CODEC_VERSION,
        cluster_id: identity.cluster_id.clone(),
        group_id: identity.group_id,
        sender_node_id: identity.sender_node_id,
        target_node_id: identity.target_node_id,
        payload: Vec::new(),
    }
}

async fn validate_snapshot_target(
    cluster: &ClusterManager,
    identity: &wire::SnapshotIdentity,
) -> Result<(), Status> {
    let envelope = snapshot_envelope(identity);
    match identity.group_id {
        CONTROL_GROUP_ID => {
            cluster.peer_control(&envelope).await?;
        }
        group_id if group_id >= DATA_GROUP_ID => {
            cluster.peer_data(&envelope).await?;
        }
        _ => return Err(Status::invalid_argument("unknown snapshot group")),
    }
    Ok(())
}

fn snapshot_paths(
    directory: &Path,
    transfer_id: &str,
) -> Result<(PathBuf, PathBuf, PathBuf), Status> {
    if transfer_id.len() != 64
        || !transfer_id
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(Status::invalid_argument("invalid snapshot transfer ID"));
    }
    Ok((
        directory.join(format!("{transfer_id}.intent")),
        directory.join(format!("{transfer_id}.part")),
        directory.join(format!("{transfer_id}.progress")),
    ))
}

fn read_snapshot_progress(path: &Path) -> Result<u64, Status> {
    serde_json::from_slice(&fs::read(path).map_err(internal_status)?).map_err(internal_status)
}

fn write_snapshot_progress(directory: &Path, path: &Path, offset: u64) -> Result<(), Status> {
    let temporary = path.with_extension("progress.tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)
        .map_err(internal_status)?;
    file.write_all(&serde_json::to_vec(&offset).map_err(internal_status)?)
        .map_err(internal_status)?;
    file.sync_all().map_err(internal_status)?;
    fs::rename(&temporary, path).map_err(internal_status)?;
    File::open(directory)
        .and_then(|directory| directory.sync_all())
        .map_err(internal_status)
}

fn reset_snapshot_stage(
    directory: &Path,
    part_path: &Path,
    progress_path: &Path,
) -> Result<(), Status> {
    let part = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(part_path)
        .map_err(internal_status)?;
    part.sync_all().map_err(internal_status)?;
    write_snapshot_progress(directory, progress_path, 0)
}

fn hash_snapshot_file(path: &Path) -> std::io::Result<(u64, SnapshotDigest)> {
    let mut file = File::open(path)?;
    let byte_len = file.metadata()?.len();
    let mut digest = Sha256::new();
    let mut buffer = vec![0; SNAPSHOT_CHUNK_BYTES];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok((byte_len, SnapshotDigest::from_slice(&digest.finalize())?))
}

fn recover_snapshot_stage(
    directory: &Path,
    part_path: &Path,
    progress_path: &Path,
    byte_len: u64,
) -> Result<u64, Status> {
    let part = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(part_path)
        .map_err(internal_status)?;
    if !progress_path.exists() {
        part.sync_all().map_err(internal_status)?;
        write_snapshot_progress(directory, progress_path, 0)?;
    }
    let mut offset = read_snapshot_progress(progress_path)?;
    let part_len = part.metadata().map_err(internal_status)?.len();
    if offset > byte_len || offset > part_len {
        reset_snapshot_stage(directory, part_path, progress_path)?;
        offset = 0;
    } else if part_len > offset {
        part.set_len(offset).map_err(internal_status)?;
        part.sync_all().map_err(internal_status)?;
    }
    Ok(offset)
}

fn remove_snapshot_stage(directory: &Path, transfer_id: &str) -> Result<(), Status> {
    let (intent, part, progress) = snapshot_paths(directory, transfer_id)?;
    for path in [intent, part, progress] {
        match fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(internal_status(error)),
        }
    }
    Ok(())
}

fn remove_abandoned_snapshot_stages(directory: &Path) -> Result<(), Status> {
    let now = SystemTime::now();
    for entry in fs::read_dir(directory).map_err(internal_status)? {
        let entry = entry.map_err(internal_status)?;
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("intent") {
            continue;
        }
        let old = entry
            .metadata()
            .and_then(|metadata| metadata.modified())
            .ok()
            .and_then(|modified| now.duration_since(modified).ok())
            .is_some_and(|age| age >= ABANDONED_SNAPSHOT_AGE);
        if old && let Some(transfer_id) = path.file_stem().and_then(|value| value.to_str()) {
            remove_snapshot_stage(directory, transfer_id)?;
        }
    }
    Ok(())
}

fn remove_superseded_snapshot_stages(
    directory: &Path,
    installed: &SnapshotIntent,
) -> Result<(), Status> {
    for entry in fs::read_dir(directory).map_err(internal_status)? {
        let entry = entry.map_err(internal_status)?;
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("intent")
            || path.file_stem().and_then(|value| value.to_str())
                == Some(installed.transfer_id.as_str())
        {
            continue;
        }
        let Ok(candidate) = fs::read(&path).map_err(internal_status).and_then(|bytes| {
            serde_json::from_slice::<SnapshotIntent>(&bytes).map_err(internal_status)
        }) else {
            continue;
        };
        if candidate.cluster_id == installed.cluster_id
            && candidate.group_id == installed.group_id
            && candidate.sender_node_id == installed.sender_node_id
            && candidate.target_node_id == installed.target_node_id
            && candidate.meta_json == installed.meta_json
            && candidate.byte_len == installed.byte_len
            && candidate.sha256 == installed.sha256
        {
            remove_snapshot_stage(directory, &candidate.transfer_id)?;
        }
    }
    Ok(())
}

fn validate_snapshot_intent(
    identity: &wire::SnapshotIdentity,
    intent: &SnapshotIntent,
) -> Result<(), Status> {
    if intent.cluster_id != identity.cluster_id
        || intent.group_id != identity.group_id
        || intent.sender_node_id != identity.sender_node_id
        || intent.target_node_id != identity.target_node_id
        || intent.transfer_id != identity.transfer_id
    {
        return Err(Status::permission_denied(
            "snapshot identity conflicts with the durable transfer intent",
        ));
    }
    Ok(())
}

fn validate_snapshot_history<C>(
    vote: &VoteOf<C>,
    meta: &SnapshotMetaOf<C>,
    committed: Option<&LogIdOf<C>>,
) -> Result<(), Status>
where
    C: RaftTypeConfig,
{
    if let Some(last_log_id) = &meta.last_log_id {
        match vote.leader_id().partial_cmp(&last_log_id.leader_id) {
            Some(Ordering::Less) | None => {
                return Err(Status::invalid_argument(
                    "snapshot vote is below or incomparable with its last log leader",
                ));
            }
            Some(Ordering::Equal | Ordering::Greater) => {}
        }
        if let Some(committed) = committed
            && last_log_id > committed
            && last_log_id.index <= committed.index
        {
            return Err(Status::invalid_argument(
                "snapshot contradicts the locally committed log index",
            ));
        }
    }
    Ok(())
}

async fn install_received_snapshot<C>(
    raft: &Raft<C, RocksStateMachine<C>>,
    vote_json: &[u8],
    meta_json: &[u8],
    artifact: SnapshotArtifact,
) -> Result<Vec<u8>, Status>
where
    C: RaftTypeConfig<NodeId = u64, Node = BasicNode>,
    VoteOf<C>: DeserializeOwned,
    SnapshotMetaOf<C>: DeserializeOwned,
    SnapshotResponse<C>: Serialize,
    RocksStateMachine<C>: RaftStateMachine<C, SnapshotData = SnapshotArtifact>,
{
    let vote: VoteOf<C> = serde_json::from_slice(vote_json).map_err(|error| {
        Status::invalid_argument(format!("invalid snapshot vote metadata: {error}"))
    })?;
    let meta: SnapshotMetaOf<C> = serde_json::from_slice(meta_json).map_err(|error| {
        Status::invalid_argument(format!("invalid snapshot artifact metadata: {error}"))
    })?;
    let metrics = raft.metrics().borrow_watched().clone();
    validate_snapshot_history::<C>(&vote, &meta, metrics.local_committed.as_ref())?;
    let response = raft
        .install_full_snapshot(
            vote,
            Snapshot {
                meta,
                snapshot: artifact,
            },
        )
        .await
        .map_err(|error| Status::failed_precondition(format!("snapshot rejected: {error}")))?;
    serde_json::to_vec(&response).map_err(internal_status)
}

impl PeerApi {
    pub fn new(cluster: Arc<ClusterManager>) -> Self {
        let security = cluster.runtime_security().clone();
        Self {
            cluster,
            security,
            snapshot_groups: AsyncMutex::new(BTreeMap::new()),
        }
    }

    async fn authenticated_envelope(
        &self,
        operation: PeerOperation,
        request: Request<wire::PeerRequest>,
    ) -> Result<wire::PeerEnvelope, Status> {
        let claimed = request
            .get_ref()
            .envelope
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("peer envelope is required"))?;
        let cluster: ClusterId = claimed
            .cluster_id
            .parse()
            .map_err(|_| Status::permission_denied("peer cluster identity is invalid"))?;
        let node = light_stream_core::NodeId::new(claimed.sender_node_id)
            .map_err(|_| Status::permission_denied("peer node identity is invalid"))?;
        let recovery_scope = self.cluster.peer_recovery_scope(claimed.group_id);
        self.security
            .authenticate_peer(&request, cluster, node, operation, recovery_scope)?;
        require_envelope(request)
    }

    async fn authenticate_snapshot<T>(
        &self,
        operation: PeerOperation,
        request: &Request<T>,
        identity: &wire::SnapshotIdentity,
    ) -> Result<(), Status> {
        let cluster: ClusterId = identity
            .cluster_id
            .parse()
            .map_err(|_| Status::permission_denied("peer cluster identity is invalid"))?;
        let node = light_stream_core::NodeId::new(identity.sender_node_id)
            .map_err(|_| Status::permission_denied("peer node identity is invalid"))?;
        let recovery_scope = self.cluster.peer_recovery_scope(identity.group_id);
        self.security
            .authenticate_peer(request, cluster, node, operation, recovery_scope)
    }

    async fn snapshot_transfer_lock(
        &self,
        identity: &wire::SnapshotIdentity,
    ) -> OwnedMutexGuard<()> {
        let lock = {
            let mut groups = self.snapshot_groups.lock().await;
            groups
                .entry(identity.group_id)
                .or_insert_with(|| Arc::new(AsyncMutex::new(())))
                .clone()
        };
        lock.lock_owned().await
    }
}

#[tonic::async_trait]
impl wire::peer_service_server::PeerService for PeerApi {
    async fn append_entries(
        &self,
        request: Request<wire::PeerRequest>,
    ) -> Result<Response<wire::PeerResponse>, Status> {
        let envelope = self
            .authenticated_envelope(PeerOperation::AppendEntries, request)
            .await?;
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
                if matches!(
                    &value,
                    AppendEntriesResponse::Success | AppendEntriesResponse::PartialSuccess(_)
                ) && let Ok(Some(policy)) = self.cluster.security_policy().await
                {
                    self.security
                        .renew_policy(policy)
                        .map_err(internal_status)?;
                }
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

    async fn probe_write_authority(
        &self,
        request: Request<wire::PeerRequest>,
    ) -> Result<Response<wire::PeerResponse>, Status> {
        let envelope = self
            .authenticated_envelope(PeerOperation::ProbeWriteAuthority, request)
            .await?;
        let ready = self.cluster.peer_write_authority(&envelope).await?;
        Ok(Response::new(encode_response(&envelope, &ready)?))
    }

    async fn vote(
        &self,
        request: Request<wire::PeerRequest>,
    ) -> Result<Response<wire::PeerResponse>, Status> {
        self.vote_request(request, false, PeerOperation::Vote).await
    }

    async fn pre_vote(
        &self,
        request: Request<wire::PeerRequest>,
    ) -> Result<Response<wire::PeerResponse>, Status> {
        self.vote_request(request, true, PeerOperation::PreVote)
            .await
    }

    async fn prepare_join(
        &self,
        request: Request<wire::PeerRequest>,
    ) -> Result<Response<wire::PeerResponse>, Status> {
        let envelope = self
            .authenticated_envelope(PeerOperation::PrepareJoin, request)
            .await?;
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
        let envelope = self
            .authenticated_envelope(PeerOperation::Activate, request)
            .await?;
        if envelope.group_id != LIFECYCLE_GROUP_ID {
            return Err(Status::invalid_argument(
                "lifecycle requests must use group zero",
            ));
        }
        let formation = decode::<FormationSpec>(&envelope)?;
        self.cluster.activate(&envelope, &formation).await?;
        Ok(Response::new(encode_response(&envelope, &())?))
    }

    async fn prepare_replacement(
        &self,
        request: Request<wire::PeerRequest>,
    ) -> Result<Response<wire::PeerResponse>, Status> {
        let envelope = self
            .authenticated_envelope(PeerOperation::PrepareReplacement, request)
            .await?;
        if envelope.group_id != LIFECYCLE_GROUP_ID {
            return Err(Status::invalid_argument(
                "lifecycle requests must use group zero",
            ));
        }
        let preparation = decode::<ReplacementPreparation>(&envelope)?;
        self.cluster
            .prepare_replacement(&envelope, preparation)
            .await?;
        Ok(Response::new(encode_response(&envelope, &())?))
    }

    async fn activate_replacement(
        &self,
        request: Request<wire::PeerRequest>,
    ) -> Result<Response<wire::PeerResponse>, Status> {
        let envelope = self
            .authenticated_envelope(PeerOperation::ActivateReplacement, request)
            .await?;
        if envelope.group_id != LIFECYCLE_GROUP_ID {
            return Err(Status::invalid_argument(
                "lifecycle requests must use group zero",
            ));
        }
        let preparation = decode::<ReplacementPreparation>(&envelope)?;
        self.cluster
            .activate_replacement(&envelope, &preparation)
            .await?;
        Ok(Response::new(encode_response(&envelope, &())?))
    }

    async fn retire_replacement(
        &self,
        request: Request<wire::PeerRequest>,
    ) -> Result<Response<wire::PeerResponse>, Status> {
        let envelope = self
            .authenticated_envelope(PeerOperation::RetireReplacement, request)
            .await?;
        if envelope.group_id != LIFECYCLE_GROUP_ID {
            return Err(Status::invalid_argument(
                "lifecycle requests must use group zero",
            ));
        }
        let retirement = decode::<ReplacementRetirement>(&envelope)?;
        self.cluster
            .retire_replacement(&envelope, retirement)
            .await?;
        Ok(Response::new(encode_response(&envelope, &())?))
    }

    async fn transfer_leader(
        &self,
        request: Request<wire::PeerRequest>,
    ) -> Result<Response<wire::PeerResponse>, Status> {
        let envelope = self
            .authenticated_envelope(PeerOperation::TransferLeader, request)
            .await?;
        let response = match envelope.group_id {
            CONTROL_GROUP_ID => {
                let rpc = decode::<TransferLeaderRequest<ControlRaftConfig>>(&envelope)?;
                let raft = self.cluster.peer_control(&envelope).await?;
                let value = raft
                    .handle_transfer_leader(rpc)
                    .await
                    .map_err(internal_status)?;
                encode_response(&envelope, &value)?
            }
            group_id if group_id >= DATA_GROUP_ID => {
                let rpc = decode::<TransferLeaderRequest<DataRaftConfig>>(&envelope)?;
                let raft = self.cluster.peer_data(&envelope).await?;
                let value = raft
                    .handle_transfer_leader(rpc)
                    .await
                    .map_err(internal_status)?;
                encode_response(&envelope, &value)?
            }
            _ => return Err(Status::invalid_argument("unknown Raft group")),
        };
        Ok(Response::new(response))
    }

    async fn begin_snapshot(
        &self,
        request: Request<wire::SnapshotBeginRequest>,
    ) -> Result<Response<wire::SnapshotProgressResponse>, Status> {
        let claimed = request
            .get_ref()
            .identity
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("snapshot identity missing"))?;
        self.authenticate_snapshot(PeerOperation::SnapshotBegin, &request, claimed)
            .await?;
        let request = request.into_inner();
        let identity = require_snapshot_identity(request.identity)?;
        validate_snapshot_target(&self.cluster, &identity).await?;
        let _transfer = self.snapshot_transfer_lock(&identity).await;
        if request.sha256.len() != 32 {
            return Err(Status::invalid_argument(
                "snapshot descriptor has an invalid digest",
            ));
        }
        let intent = SnapshotIntent {
            cluster_id: identity.cluster_id.clone(),
            group_id: identity.group_id,
            sender_node_id: identity.sender_node_id,
            target_node_id: identity.target_node_id,
            transfer_id: identity.transfer_id.clone(),
            vote_json: request.vote_json,
            meta_json: request.meta_json,
            byte_len: request.byte_len,
            sha256: request.sha256,
        };
        let directory = self.cluster.snapshot_incoming_directory(identity.group_id);
        fs::create_dir_all(&directory).map_err(internal_status)?;
        remove_abandoned_snapshot_stages(&directory)?;
        let (intent_path, part_path, progress_path) =
            snapshot_paths(&directory, &identity.transfer_id)?;
        if intent_path.exists() {
            let existing: SnapshotIntent =
                serde_json::from_slice(&fs::read(&intent_path).map_err(internal_status)?)
                    .map_err(internal_status)?;
            if existing != intent {
                return Err(Status::already_exists(
                    "snapshot transfer ID has another descriptor",
                ));
            }
        } else {
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&intent_path)
                .map_err(internal_status)?;
            file.write_all(&serde_json::to_vec(&intent).map_err(internal_status)?)
                .map_err(internal_status)?;
            file.sync_all().map_err(internal_status)?;
            File::open(&directory)
                .and_then(|directory| directory.sync_all())
                .map_err(internal_status)?;
        }
        let offset =
            recover_snapshot_stage(&directory, &part_path, &progress_path, intent.byte_len)?;
        eprintln!(
            "{}",
            serde_json::json!({
                "event": "snapshot_transfer_began",
                "group_id": identity.group_id,
                "sender_node_id": identity.sender_node_id,
                "target_node_id": identity.target_node_id,
                "transfer_id": identity.transfer_id,
                "durable_offset": offset,
            })
        );
        Ok(Response::new(wire::SnapshotProgressResponse {
            durable_offset: offset,
            response_json: Vec::new(),
        }))
    }

    async fn put_snapshot_chunk(
        &self,
        request: Request<wire::SnapshotChunkRequest>,
    ) -> Result<Response<wire::SnapshotProgressResponse>, Status> {
        let claimed = request
            .get_ref()
            .identity
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("snapshot identity missing"))?;
        self.authenticate_snapshot(PeerOperation::SnapshotChunk, &request, claimed)
            .await?;
        let request = request.into_inner();
        let identity = require_snapshot_identity(request.identity)?;
        validate_snapshot_target(&self.cluster, &identity).await?;
        let _transfer = self.snapshot_transfer_lock(&identity).await;
        if let Some(delay) = self.cluster.snapshot_verification_delay(identity.group_id) {
            tokio::time::sleep(delay).await;
        }
        if request.data.is_empty() || request.data.len() > SNAPSHOT_CHUNK_BYTES {
            return Err(Status::resource_exhausted(
                "snapshot chunk is empty or oversized",
            ));
        }
        let mut crc = Crc32::new();
        crc.update(&request.data);
        if crc.finalize() != request.crc32 {
            return Err(Status::data_loss("snapshot chunk checksum mismatch"));
        }
        let directory = self.cluster.snapshot_incoming_directory(identity.group_id);
        let (intent_path, part_path, progress_path) =
            snapshot_paths(&directory, &identity.transfer_id)?;
        let intent: SnapshotIntent =
            serde_json::from_slice(&fs::read(&intent_path).map_err(internal_status)?)
                .map_err(internal_status)?;
        validate_snapshot_intent(&identity, &intent)?;
        let mut part = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&part_path)
            .map_err(internal_status)?;
        let offset = read_snapshot_progress(&progress_path)?;
        if request.offset != offset || part.metadata().map_err(internal_status)?.len() != offset {
            return Err(Status::failed_precondition(
                "snapshot chunk offset does not match the durable offset",
            ));
        }
        let next = offset
            .checked_add(request.data.len() as u64)
            .ok_or_else(|| Status::resource_exhausted("snapshot offset overflow"))?;
        if next > intent.byte_len {
            return Err(Status::resource_exhausted(
                "snapshot chunk exceeds the declared artifact",
            ));
        }
        part.seek(SeekFrom::Start(offset))
            .map_err(internal_status)?;
        part.write_all(&request.data).map_err(internal_status)?;
        part.sync_data().map_err(internal_status)?;
        write_snapshot_progress(&directory, &progress_path, next)?;
        Ok(Response::new(wire::SnapshotProgressResponse {
            durable_offset: next,
            response_json: Vec::new(),
        }))
    }

    async fn finish_snapshot(
        &self,
        request: Request<wire::SnapshotFinishRequest>,
    ) -> Result<Response<wire::SnapshotProgressResponse>, Status> {
        let claimed = request
            .get_ref()
            .identity
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("snapshot identity missing"))?;
        self.authenticate_snapshot(PeerOperation::SnapshotFinish, &request, claimed)
            .await?;
        let identity = require_snapshot_identity(request.into_inner().identity)?;
        validate_snapshot_target(&self.cluster, &identity).await?;
        let _transfer = self.snapshot_transfer_lock(&identity).await;
        let directory = self.cluster.snapshot_incoming_directory(identity.group_id);
        let (intent_path, part_path, progress_path) =
            snapshot_paths(&directory, &identity.transfer_id)?;
        let intent: SnapshotIntent =
            serde_json::from_slice(&fs::read(&intent_path).map_err(internal_status)?)
                .map_err(internal_status)?;
        validate_snapshot_intent(&identity, &intent)?;
        let (byte_len, digest) = hash_snapshot_file(&part_path).map_err(internal_status)?;
        if read_snapshot_progress(&progress_path)? != intent.byte_len
            || byte_len != intent.byte_len
            || digest.as_bytes().as_slice() != intent.sha256
        {
            reset_snapshot_stage(&directory, &part_path, &progress_path)?;
            return Err(Status::data_loss(
                "snapshot length or digest does not match its descriptor",
            ));
        }
        let response_json = match identity.group_id {
            CONTROL_GROUP_ID => {
                let raft = self
                    .cluster
                    .peer_control(&snapshot_envelope(&identity))
                    .await?;
                install_received_snapshot::<ControlRaftConfig>(
                    &raft,
                    &intent.vote_json,
                    &intent.meta_json,
                    SnapshotArtifact::open_verified(part_path.clone(), byte_len, digest)
                        .map_err(internal_status)?,
                )
                .await?
            }
            group_id if group_id >= DATA_GROUP_ID => {
                let raft = self
                    .cluster
                    .peer_data(&snapshot_envelope(&identity))
                    .await?;
                install_received_snapshot::<DataRaftConfig>(
                    &raft,
                    &intent.vote_json,
                    &intent.meta_json,
                    SnapshotArtifact::open_verified(part_path.clone(), byte_len, digest)
                        .map_err(internal_status)?,
                )
                .await?
            }
            _ => return Err(Status::invalid_argument("unknown snapshot group")),
        };
        eprintln!(
            "{}",
            serde_json::json!({
                "event": "snapshot_install_completed",
                "group_id": identity.group_id,
                "sender_node_id": identity.sender_node_id,
                "target_node_id": identity.target_node_id,
                "transfer_id": identity.transfer_id,
                "byte_len": intent.byte_len,
                "chunk_count": intent.byte_len.div_ceil(SNAPSHOT_CHUNK_BYTES as u64),
            })
        );
        remove_superseded_snapshot_stages(&directory, &intent)?;
        remove_snapshot_stage(&directory, &intent.transfer_id)?;
        Ok(Response::new(wire::SnapshotProgressResponse {
            durable_offset: intent.byte_len,
            response_json,
        }))
    }
}

impl PeerApi {
    async fn vote_request(
        &self,
        request: Request<wire::PeerRequest>,
        pre_vote: bool,
        operation: PeerOperation,
    ) -> Result<Response<wire::PeerResponse>, Status> {
        let envelope = self.authenticated_envelope(operation, request).await?;
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
    security: &RuntimeSecurityConfig,
) -> Result<(), DomainError> {
    lifecycle_call("prepare_join", formation, sender_node_id, target, security).await
}

pub async fn activate_remote(
    formation: &FormationSpec,
    sender_node_id: u64,
    target: &NodeDescriptor,
    security: &RuntimeSecurityConfig,
) -> Result<(), DomainError> {
    lifecycle_call("activate", formation, sender_node_id, target, security).await
}

pub async fn prepare_replacement_remote(
    preparation: &ReplacementPreparation,
    sender_node_id: u64,
    target: &NodeDescriptor,
    security: &RuntimeSecurityConfig,
) -> Result<(), DomainError> {
    lifecycle_payload_call(
        "prepare_replacement",
        preparation.formation.cluster_id,
        serde_json::to_vec(preparation).map_err(storage_error)?,
        sender_node_id,
        target,
        security,
    )
    .await
}

pub async fn activate_replacement_remote(
    preparation: &ReplacementPreparation,
    sender_node_id: u64,
    target: &NodeDescriptor,
    security: &RuntimeSecurityConfig,
) -> Result<(), DomainError> {
    lifecycle_payload_call(
        "activate_replacement",
        preparation.formation.cluster_id,
        serde_json::to_vec(preparation).map_err(storage_error)?,
        sender_node_id,
        target,
        security,
    )
    .await
}

pub async fn retire_replacement_remote(
    retirement: &ReplacementRetirement,
    sender_node_id: u64,
    target: &NodeDescriptor,
    security: &RuntimeSecurityConfig,
) -> Result<(), DomainError> {
    lifecycle_payload_call(
        "retire_replacement",
        retirement.formation.cluster_id,
        serde_json::to_vec(retirement).map_err(storage_error)?,
        sender_node_id,
        target,
        security,
    )
    .await
}

pub async fn probe_write_authority_remote(
    cluster_id: ClusterId,
    group_id: GroupId,
    sender_node_id: NodeId,
    endpoint_uri: &str,
    target: &NodeDescriptor,
    security: &RuntimeSecurityConfig,
    timeout: Duration,
) -> Result<bool, Status> {
    let deadline = RpcDeadline::after(timeout);
    let remaining = deadline
        .remaining()
        .ok_or_else(|| Status::deadline_exceeded("write-authority probe timed out"))?;
    let endpoint = Endpoint::from_shared(endpoint_uri.to_owned())
        .map_err(|error| Status::unavailable(error.to_string()))?
        .connect_timeout(remaining)
        .timeout(remaining);
    let endpoint = security
        .configure_peer_endpoint(
            endpoint,
            target.peer_uri(),
            target.node_id(),
            PeerOperation::ProbeWriteAuthority,
            crate::security::PeerRecoveryScope::None,
        )
        .map_err(Status::unavailable)?;
    let channel = tokio::time::timeout_at(deadline.expires_at.into(), endpoint.connect())
        .await
        .map_err(|_| Status::deadline_exceeded("write-authority probe timed out"))?
        .map_err(|error| Status::unavailable(error.to_string()))?;
    let mut client = wire::peer_service_client::PeerServiceClient::new(channel)
        .max_decoding_message_size(MAX_PEER_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_PEER_MESSAGE_BYTES);
    let mut request = Request::new(wire::PeerRequest {
        envelope: Some(wire::PeerEnvelope {
            protocol_version: PEER_PROTOCOL_VERSION,
            codec_version: PEER_CODEC_VERSION,
            cluster_id: cluster_id.to_string(),
            group_id: group_id.get(),
            sender_node_id: sender_node_id.get(),
            target_node_id: target.node_id().get(),
            payload: Vec::new(),
        }),
    });
    request.set_timeout(
        deadline
            .remaining()
            .ok_or_else(|| Status::deadline_exceeded("write-authority probe timed out"))?,
    );
    let response = tokio::time::timeout_at(
        deadline.expires_at.into(),
        client.probe_write_authority(request),
    )
    .await
    .map_err(|_| Status::deadline_exceeded("write-authority probe timed out"))??
    .into_inner();
    let envelope = response
        .envelope
        .ok_or_else(|| Status::internal("write-authority response omitted its envelope"))?;
    if envelope.protocol_version != PEER_PROTOCOL_VERSION
        || envelope.codec_version != PEER_CODEC_VERSION
        || envelope.cluster_id != cluster_id.to_string()
        || envelope.group_id != group_id.get()
        || envelope.sender_node_id != target.node_id().get()
        || envelope.target_node_id != sender_node_id.get()
    {
        return Err(Status::permission_denied(
            "write-authority response identity is invalid",
        ));
    }
    decode(&envelope)
}

async fn lifecycle_call(
    operation: &str,
    formation: &FormationSpec,
    sender_node_id: u64,
    target: &NodeDescriptor,
    security: &RuntimeSecurityConfig,
) -> Result<(), DomainError> {
    lifecycle_payload_call(
        operation,
        formation.cluster_id,
        serde_json::to_vec(formation).map_err(storage_error)?,
        sender_node_id,
        target,
        security,
    )
    .await
}

async fn lifecycle_payload_call(
    operation: &str,
    cluster_id: ClusterId,
    payload: Vec<u8>,
    sender_node_id: u64,
    target: &NodeDescriptor,
    security: &RuntimeSecurityConfig,
) -> Result<(), DomainError> {
    let endpoint = Endpoint::from_shared(target.peer_uri().to_owned())
        .map_err(storage_error)?
        .connect_timeout(LIFECYCLE_TIMEOUT)
        .timeout(LIFECYCLE_TIMEOUT);
    let endpoint = security
        .configure_peer_endpoint(
            endpoint,
            target.peer_uri(),
            target.node_id(),
            lifecycle_peer_operation(operation),
            crate::security::PeerRecoveryScope::None,
        )
        .map_err(storage_error)?;
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
    let mut request = Request::new(wire::PeerRequest {
        envelope: Some(wire::PeerEnvelope {
            protocol_version: PEER_PROTOCOL_VERSION,
            codec_version: PEER_CODEC_VERSION,
            cluster_id: cluster_id.to_string(),
            group_id: LIFECYCLE_GROUP_ID,
            sender_node_id,
            target_node_id: target.node_id().get(),
            payload,
        }),
    });
    request.set_timeout(LIFECYCLE_TIMEOUT);
    let result = match operation {
        "prepare_join" => client.prepare_join(request).await,
        "prepare_replacement" => client.prepare_replacement(request).await,
        "activate_replacement" => client.activate_replacement(request).await,
        "retire_replacement" => client.retire_replacement(request).await,
        _ => client.activate(request).await,
    };
    result.map_err(storage_error)?;
    Ok(())
}

fn lifecycle_peer_operation(operation: &str) -> PeerOperation {
    match operation {
        "prepare_join" => PeerOperation::PrepareJoin,
        "prepare_replacement" => PeerOperation::PrepareReplacement,
        "activate_replacement" => PeerOperation::ActivateReplacement,
        "retire_replacement" => PeerOperation::RetireReplacement,
        _ => PeerOperation::Activate,
    }
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
    use openraft::{Entry, EntryPayload, SnapshotMeta, Vote};
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
        assert_eq!(Some(durable_uri), valid.expected_peer_uri.as_deref());
        assert_eq!(2, valid.target_node_id);

        let invalid = factory.build_network(2, &BasicNode::new("http://127.0.0.1:7999"));
        assert!(!invalid.valid);
        assert_eq!(None, invalid.endpoint.as_deref());
    }

    #[test]
    fn raft_network_factory_observes_replaced_authorized_topology() {
        let cluster = ClusterId::from_uuid(Uuid::new_v4());
        let node2 = NodeDescriptor::new(
            light_stream_core::NodeId::new(2).unwrap(),
            "http://127.0.0.1:7102",
            "http://127.0.0.1:7202",
        );
        let topology = PeerTopology::new([node2.clone()]);
        let factory = TonicNetworkFactory::<DataRaftConfig>::with_topology(
            cluster,
            DATA_GROUP_ID,
            1,
            topology.clone(),
            PeerRoutes::default(),
            RuntimeSecurityConfig::LocalInsecure,
        );
        assert!(
            factory
                .build_network(2, &BasicNode::new(node2.peer_uri()))
                .valid
        );
        let node4 = NodeDescriptor::new(
            light_stream_core::NodeId::new(4).unwrap(),
            "http://127.0.0.1:7104",
            "http://127.0.0.1:7204",
        );
        topology.replace([node4.clone()]).unwrap();
        assert!(
            !factory
                .build_network(2, &BasicNode::new(node2.peer_uri()))
                .valid
        );
        assert!(
            factory
                .build_network(4, &BasicNode::new(node4.peer_uri()))
                .valid
        );
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

    #[test]
    fn snapshot_transfer_ids_cannot_escape_the_incoming_directory() {
        let directory = Path::new("/tmp/incoming");
        assert!(snapshot_paths(directory, "../snapshot").is_err());
        let transfer_id = "a".repeat(64);
        let (intent, part, progress) = snapshot_paths(directory, &transfer_id).unwrap();
        assert_eq!(directory.join(format!("{transfer_id}.intent")), intent);
        assert_eq!(directory.join(format!("{transfer_id}.part")), part);
        assert_eq!(directory.join(format!("{transfer_id}.progress")), progress);
    }

    #[test]
    fn snapshot_vote_must_cover_the_last_log_leader() {
        let meta = SnapshotMeta {
            last_log_id: Some(openraft::LogId::new(
                openraft::impls::leader_id_adv::LeaderId {
                    term: 2,
                    node_id: 1,
                },
                9,
            )),
            last_membership: Default::default(),
        };
        assert!(
            validate_snapshot_history::<DataRaftConfig>(&Vote::new_committed(1, 1), &meta, None,)
                .is_err()
        );
        assert!(
            validate_snapshot_history::<DataRaftConfig>(&Vote::new_committed(2, 1), &meta, None,)
                .is_ok()
        );
        let committed = openraft::LogId::new(
            openraft::impls::leader_id_adv::LeaderId {
                term: 1,
                node_id: 1,
            },
            10,
        );
        assert!(
            validate_snapshot_history::<DataRaftConfig>(
                &Vote::new_committed(2, 1),
                &meta,
                Some(&committed),
            )
            .is_err()
        );
    }

    #[test]
    fn snapshot_resume_truncates_bytes_beyond_the_durable_progress() {
        let directory = tempfile::tempdir().unwrap();
        let transfer_id = "b".repeat(64);
        let (_, part_path, progress_path) = snapshot_paths(directory.path(), &transfer_id).unwrap();
        fs::write(&part_path, b"durable-torn-tail").unwrap();
        write_snapshot_progress(directory.path(), &progress_path, 7).unwrap();

        let offset =
            recover_snapshot_stage(directory.path(), &part_path, &progress_path, 1024).unwrap();

        assert_eq!(7, offset);
        assert_eq!(b"durable", fs::read(part_path).unwrap().as_slice());
    }
}
