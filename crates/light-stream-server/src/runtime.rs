use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use light_stream_core::{
    AmbiguousRequest, BookmarkId, BookmarkName, BookmarkPage, BookmarkPageRequest,
    BootstrapCommand, BootstrapResult, BootstrapSpec, BootstrapTopology, ClusterId,
    CommittedBookmark, CommittedStreamBookmark, ConsensusGroup, CreateBookmarkSpec,
    CreateStreamSpec, DomainError, FetchPage, GroupId, LeaderHint, LeaseRelease, LeaseRenewal,
    NodeDescriptor, PartitionId, PartitionKey, PartitionRoute, ProducerRequestId,
    ProtectedFetchRequest, PublishBatch, PublishReceipt, RecordOffset, ReplayLease, ReplayLeaseId,
    ReplayLeaseRequest, RequestOutcome, RetentionRequest, RetentionResult, RetentionStatus,
    StreamBookmarkPage, StreamBookmarkPageRequest, StreamCursorVector, StreamDescriptor, StreamId,
    StreamLifecycle, StreamName,
};
use light_stream_storage::{
    ApplyResult, CONTROL_GROUP_ID, ClockObservation, CommittedStateReader, ControlRaftConfig,
    DATA_GROUP_ID, DataRaftConfig, GroupCommand, GroupIdentity, GroupKind, GroupStorageBudget,
    NoRemoteNetworkFactory, RocksStateMachine, create_control_store, create_data_store,
    open_control_store, open_data_store,
};
use openraft::{
    BasicNode, Config, Raft, ReadPolicy, ServerState, SnapshotPolicy,
    errors::{ClientWriteError, LinearizableReadError, RaftError},
    type_config::async_runtime::WatchReceiver,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};

use crate::{
    config::PeerRoutes,
    manifest::{
        FormationSpec, GroupPoolConfig, NODE_MANIFEST_VERSION, NodeManifestV1, NodeManifestV2,
        PersistedNodeState,
    },
    peer::{self, TonicNetworkFactory, wire},
};

pub(crate) type ControlRaft = Raft<ControlRaftConfig, RocksStateMachine<ControlRaftConfig>>;
pub(crate) type DataRaft = Raft<DataRaftConfig, RocksStateMachine<DataRaftConfig>>;

const ROOT_MANIFEST: &str = "cluster.json";
const OPERATION_TIMEOUT: Duration = Duration::from_secs(3);
const FORMATION_TIMEOUT: Duration = Duration::from_secs(15);
const LEASE_CLOCK_SKEW: Duration = Duration::from_secs(2);

#[derive(Clone, Debug)]
enum ActiveManifest {
    V1(NodeManifestV1),
    V2(NodeManifestV2),
}

impl ActiveManifest {
    fn cluster_id(&self) -> ClusterId {
        match self {
            Self::V1(manifest) => manifest.cluster_id,
            Self::V2(manifest) => manifest.formation.cluster_id,
        }
    }

    fn group_pool(&self) -> &GroupPoolConfig {
        match self {
            Self::V1(manifest) => &manifest.group_pool,
            Self::V2(manifest) => &manifest.formation.group_pool,
        }
    }

    fn lifecycle(&self) -> &'static str {
        match self {
            Self::V1(_) => "active",
            Self::V2(manifest) => match manifest.state {
                PersistedNodeState::Joining => "joining",
                PersistedNodeState::Forming => "forming",
                PersistedNodeState::Active => "active",
            },
        }
    }

    fn is_application_active(&self) -> bool {
        matches!(self, Self::V1(_))
            || matches!(
                self,
                Self::V2(NodeManifestV2 {
                    state: PersistedNodeState::Active,
                    ..
                })
            )
    }
}

struct ActiveCluster {
    manifest: RwLock<ActiveManifest>,
    control: ControlRaft,
    data: BTreeMap<u64, DataGroup>,
    control_reader: CommittedStateReader,
    maintenance_shutdown: AtomicBool,
}

struct DataGroup {
    raft: DataRaft,
    reader: CommittedStateReader,
    slot: u16,
    budget: GroupStorageBudget,
}

impl ActiveCluster {
    async fn shutdown(&self) -> Result<(), DomainError> {
        self.maintenance_shutdown.store(true, Ordering::Release);
        self.control.shutdown().await.map_err(raft_fatal)?;
        for group in self.data.values() {
            group.raft.shutdown().await.map_err(raft_fatal)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ReplicationDiagnostic {
    pub target_node_id: u64,
    pub matched_log_index: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct GroupDiagnostic {
    pub group: ConsensusGroup,
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
    pub replication: Vec<ReplicationDiagnostic>,
    pub snapshot_index: Option<u64>,
    pub purged_index: Option<u64>,
    pub slot: Option<u16>,
    pub cache_budget_bytes: Option<usize>,
    pub write_buffer_budget_bytes: Option<usize>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct NodeDiagnostic {
    pub node_id: u64,
    pub lifecycle: String,
    pub peers: Vec<NodeDescriptor>,
    pub groups: Vec<GroupDiagnostic>,
    pub data_group_slots: u16,
    pub data_group_count: usize,
    pub rocksdb_cache_budget_bytes: usize,
    pub rocksdb_write_buffer_budget_bytes: usize,
    pub per_group_cache_bytes: usize,
    pub per_group_write_buffer_bytes: usize,
    pub unsupported_claims: Vec<String>,
}

pub struct ClusterManager {
    data_dir: PathBuf,
    local: NodeDescriptor,
    receipt_window: usize,
    peer_routes: PeerRoutes,
    group_pool: GroupPoolConfig,
    verification_delay: Option<(u64, Duration)>,
    active: RwLock<Option<Arc<ActiveCluster>>>,
    bootstrap_lock: Mutex<()>,
}

impl ClusterManager {
    pub async fn open(
        data_dir: PathBuf,
        local: NodeDescriptor,
        receipt_window: usize,
        peer_routes: PeerRoutes,
        group_pool: GroupPoolConfig,
        verification_delay: Option<(u64, Duration)>,
    ) -> Result<Self, DomainError> {
        let manifest = read_manifest(&data_dir)?;
        if manifest.is_none() && has_group_storage(&data_dir)? {
            return Err(DomainError::Storage {
                reason: "group storage exists without an authorizing node manifest".to_owned(),
            });
        }
        let manager = Self {
            data_dir,
            local,
            receipt_window,
            peer_routes,
            group_pool,
            verification_delay,
            active: RwLock::new(None),
            bootstrap_lock: Mutex::new(()),
        };
        if let Some(manifest) = manifest {
            let active = match manifest {
                ActiveManifest::V1(manifest) => {
                    if !manager.peer_routes.is_empty() {
                        return Err(DomainError::InvalidName {
                            kind: "peer route".to_owned(),
                            reason: "standalone manifests have no remote peer targets".to_owned(),
                        });
                    }
                    manager.open_v1(manifest).await?
                }
                ActiveManifest::V2(manifest) => {
                    manager
                        .peer_routes
                        .validate_topology(manager.local.node_id(), &manifest.formation.members)?;
                    manager.open_v2(manifest).await?
                }
            };
            let active = Arc::new(active);
            spawn_retention_maintenance(active.clone());
            *manager.active.write().await = Some(active);
        }
        Ok(manager)
    }

    pub async fn bootstrap(
        &self,
        command: BootstrapCommand,
    ) -> Result<BootstrapResult, DomainError> {
        let _guard = self.bootstrap_lock.lock().await;
        match command.topology() {
            BootstrapTopology::Standalone => {
                if !self.peer_routes.is_empty() {
                    return Err(DomainError::InvalidName {
                        kind: "peer route".to_owned(),
                        reason: "standalone bootstrap has no remote peer targets".to_owned(),
                    });
                }
                self.bootstrap_standalone(command.spec()).await
            }
            BootstrapTopology::ThreeVoter {
                seed_node_id,
                members,
            } => {
                let formation = FormationSpec::try_new(
                    command.spec().clone(),
                    seed_node_id.get(),
                    members.clone(),
                    self.group_pool.clone(),
                )?;
                self.peer_routes
                    .validate_topology(self.local.node_id(), &formation.members)?;
                self.bootstrap_three_voter(formation).await
            }
        }
    }

    async fn bootstrap_standalone(
        &self,
        spec: &BootstrapSpec,
    ) -> Result<BootstrapResult, DomainError> {
        if let Some(active) = self.active.read().await.as_ref() {
            let manifest = active.manifest.read().await;
            return match &*manifest {
                ActiveManifest::V1(existing) if existing.bootstrap == *spec => {
                    bootstrap_result(spec)
                }
                _ => Err(DomainError::BootstrapConflict {
                    reason: "cluster or bootstrap topology differs".to_owned(),
                }),
            };
        }
        let manifest = NodeManifestV1 {
            format_version: light_stream_storage::STORAGE_FORMAT_VERSION,
            cluster_id: spec.cluster(),
            bootstrap: spec.clone(),
            node_id: self.local.node_id().get(),
            control_group_id: CONTROL_GROUP_ID,
            data_group_id: DATA_GROUP_ID,
            group_pool: self.group_pool.clone(),
        };
        write_manifest(&self.data_dir, &manifest)?;
        let active = self.create_v1(manifest).await?;
        let active = Arc::new(active);
        spawn_retention_maintenance(active.clone());
        *self.active.write().await = Some(active);
        bootstrap_result(spec)
    }

    async fn bootstrap_three_voter(
        &self,
        formation: FormationSpec,
    ) -> Result<BootstrapResult, DomainError> {
        if formation.seed_node_id != self.local.node_id() {
            return Err(DomainError::BootstrapConflict {
                reason: "the contacted node is not the declared seed".to_owned(),
            });
        }
        let local = formation.local(self.local.node_id()).ok_or_else(|| {
            DomainError::BootstrapConflict {
                reason: "the contacted node is absent from the topology".to_owned(),
            }
        })?;
        if local != &self.local {
            return Err(DomainError::BootstrapConflict {
                reason: "the contacted node descriptor conflicts with process configuration"
                    .to_owned(),
            });
        }

        let active = if let Some(active) = self.active.read().await.as_ref().cloned() {
            {
                let manifest = active.manifest.read().await;
                match &*manifest {
                    ActiveManifest::V2(existing) if existing.formation == formation => {}
                    _ => {
                        return Err(DomainError::BootstrapConflict {
                            reason: "cluster or bootstrap topology differs".to_owned(),
                        });
                    }
                }
            }
            active
        } else {
            let manifest = NodeManifestV2::new(
                self.local.node_id(),
                formation.clone(),
                PersistedNodeState::Forming,
            );
            write_manifest(&self.data_dir, &manifest)?;
            let active = Arc::new(self.create_v2(manifest, true).await?);
            spawn_retention_maintenance(active.clone());
            *self.active.write().await = Some(active.clone());
            active
        };

        if active.manifest.read().await.is_application_active() {
            self.activate_members(&formation).await?;
            return bootstrap_result(&formation.bootstrap);
        }
        self.form_cluster(&active, &formation).await?;
        bootstrap_result(&formation.bootstrap)
    }

    async fn form_cluster(
        &self,
        active: &Arc<ActiveCluster>,
        formation: &FormationSpec,
    ) -> Result<(), DomainError> {
        if prove_active(active, formation).await.is_ok() {
            self.set_active(active).await?;
            return self.activate_members(formation).await;
        }
        for member in &formation.members {
            if member.node_id() != self.local.node_id() {
                peer::prepare_remote(formation, self.local.node_id().get(), member).await?;
            }
        }

        initialize_seed_if_pristine(
            &active.control,
            self.local.node_id().get(),
            self.local.peer_uri(),
        )
        .await?;
        for group in active.data.values() {
            initialize_seed_if_pristine(
                &group.raft,
                self.local.node_id().get(),
                self.local.peer_uri(),
            )
            .await?;
        }
        elect_seed(&active.control, self.local.node_id().get()).await?;
        for group in active.data.values() {
            elect_seed(&group.raft, self.local.node_id().get()).await?;
        }

        ensure_bootstrap_command(
            &active.control,
            &active.control_reader,
            GroupCommand::BootstrapControl {
                spec: formation.bootstrap.clone(),
                data_groups: formation.group_pool.data_group_ids()?,
                max_streams: formation.group_pool.max_streams,
                max_partitions_per_stream: formation.group_pool.max_partitions_per_stream,
            },
            &formation.bootstrap,
        )
        .await?;
        for group in active.data.values() {
            ensure_bootstrap_command(
                &group.raft,
                &group.reader,
                GroupCommand::BootstrapData {
                    spec: formation.bootstrap.clone(),
                },
                &formation.bootstrap,
            )
            .await?;
        }

        add_learners(&active.control, formation).await?;
        for group in active.data.values() {
            add_learners(&group.raft, formation).await?;
        }
        wait_exact_replication(&active.control, formation).await?;
        for group in active.data.values() {
            wait_exact_replication(&group.raft, formation).await?;
        }
        converge_membership(&active.control, formation).await?;
        for group in active.data.values() {
            converge_membership(&group.raft, formation).await?;
        }
        prove_active(active, formation).await?;
        self.set_active(active).await?;
        self.activate_members(formation).await
    }

    async fn activate_members(&self, formation: &FormationSpec) -> Result<(), DomainError> {
        let deadline = Instant::now() + FORMATION_TIMEOUT;
        for member in &formation.members {
            if member.node_id() == self.local.node_id() {
                continue;
            }
            loop {
                match peer::activate_remote(formation, self.local.node_id().get(), member).await {
                    Ok(()) => break,
                    Err(error) if Instant::now() < deadline => {
                        let _ = error;
                        tokio::time::sleep(Duration::from_millis(25)).await;
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn prepare_join(
        &self,
        envelope: &wire::PeerEnvelope,
        formation: FormationSpec,
    ) -> Result<(), tonic::Status> {
        validate_lifecycle_envelope(&self.local, envelope, &formation)?;
        self.peer_routes
            .validate_topology(self.local.node_id(), &formation.members)
            .map_err(internal_status)?;
        if formation.seed_node_id.get() != envelope.sender_node_id {
            return Err(tonic::Status::permission_denied(
                "prepare_join sender is not the declared seed",
            ));
        }
        let _guard = self.bootstrap_lock.lock().await;
        if let Some(active) = self.active.read().await.as_ref() {
            let manifest = active.manifest.read().await;
            return match &*manifest {
                ActiveManifest::V2(existing) if existing.formation == formation => Ok(()),
                _ => Err(tonic::Status::already_exists(
                    "node already belongs to another cluster or topology",
                )),
            };
        }
        if has_group_storage(&self.data_dir).map_err(internal_status)? {
            return Err(tonic::Status::failed_precondition(
                "group storage exists without a matching manifest",
            ));
        }
        let manifest =
            NodeManifestV2::new(self.local.node_id(), formation, PersistedNodeState::Joining);
        write_manifest(&self.data_dir, &manifest).map_err(internal_status)?;
        let active = Arc::new(
            self.create_v2(manifest, true)
                .await
                .map_err(internal_status)?,
        );
        spawn_retention_maintenance(active.clone());
        *self.active.write().await = Some(active);
        Ok(())
    }

    pub(crate) async fn activate(
        &self,
        envelope: &wire::PeerEnvelope,
        formation: &FormationSpec,
    ) -> Result<(), tonic::Status> {
        validate_lifecycle_envelope(&self.local, envelope, formation)?;
        let active = self
            .active
            .read()
            .await
            .clone()
            .ok_or_else(|| tonic::Status::failed_precondition("node is pristine"))?;
        {
            let manifest = active.manifest.read().await;
            match &*manifest {
                ActiveManifest::V2(existing) if &existing.formation == formation => {}
                _ => {
                    return Err(tonic::Status::failed_precondition(
                        "activation topology conflicts with the durable manifest",
                    ));
                }
            }
        }
        prove_active(&active, formation)
            .await
            .map_err(|error| tonic::Status::failed_precondition(error.to_string()))?;
        self.set_active(&active).await.map_err(internal_status)
    }

    async fn set_active(&self, active: &Arc<ActiveCluster>) -> Result<(), DomainError> {
        let mut manifest = active.manifest.write().await;
        let ActiveManifest::V2(current) = &mut *manifest else {
            return Ok(());
        };
        if current.state != PersistedNodeState::Active {
            current.state = PersistedNodeState::Active;
            write_manifest(&self.data_dir, current)?;
        }
        Ok(())
    }

    pub(crate) async fn peer_control(
        &self,
        envelope: &wire::PeerEnvelope,
    ) -> Result<ControlRaft, tonic::Status> {
        let active = self.peer_cluster(envelope, CONTROL_GROUP_ID).await?;
        Ok(active.control.clone())
    }

    pub(crate) async fn peer_data(
        &self,
        envelope: &wire::PeerEnvelope,
    ) -> Result<DataRaft, tonic::Status> {
        let active = self.peer_cluster(envelope, envelope.group_id).await?;
        active
            .data
            .get(&envelope.group_id)
            .map(|value| value.raft.clone())
            .ok_or_else(|| tonic::Status::invalid_argument("unknown data Raft group"))
    }

    async fn peer_cluster(
        &self,
        envelope: &wire::PeerEnvelope,
        expected_group: u64,
    ) -> Result<Arc<ActiveCluster>, tonic::Status> {
        let active = self
            .active
            .read()
            .await
            .clone()
            .ok_or_else(|| tonic::Status::failed_precondition("node is pristine"))?;
        let manifest_guard = active.manifest.read().await;
        let ActiveManifest::V2(manifest) = &*manifest_guard else {
            return Err(tonic::Status::failed_precondition(
                "standalone nodes do not accept remote Raft traffic",
            ));
        };
        if envelope.cluster_id != manifest.formation.cluster_id.to_string()
            || envelope.group_id != expected_group
            || envelope.target_node_id != self.local.node_id().get()
            || envelope.sender_node_id == self.local.node_id().get()
            || manifest.formation.member(envelope.sender_node_id).is_none()
        {
            return Err(tonic::Status::permission_denied(
                "peer envelope conflicts with the durable topology",
            ));
        }
        if expected_group != CONTROL_GROUP_ID && !active.data.contains_key(&expected_group) {
            return Err(tonic::Status::permission_denied(
                "peer envelope names an unauthorized data group",
            ));
        }
        drop(manifest_guard);
        Ok(active)
    }

    pub async fn create_stream(
        &self,
        cluster: ClusterId,
        spec: CreateStreamSpec,
    ) -> Result<StreamDescriptor, DomainError> {
        let active = self.application_cluster().await?;
        validate_cluster(&active, cluster).await?;
        let descriptor = control_write(
            &active,
            GroupCommand::CreateStreamIntent {
                spec,
                stream_id: StreamId::from_uuid(uuid::Uuid::new_v4()),
            },
        )
        .await?;
        let required = descriptor
            .placements()
            .iter()
            .map(|value| value.group())
            .collect::<BTreeSet<_>>();
        for group_id in required {
            let group = active
                .data
                .get(&group_id.get())
                .ok_or(DomainError::ClusterForming)?;
            let voters = match &*active.manifest.read().await {
                ActiveManifest::V1(_) => BTreeSet::from([self.local.node_id().get()]),
                ActiveManifest::V2(value) => value.formation.voter_ids(),
            };
            if !membership_is_exact(&group.raft.metrics().borrow_watched(), &voters) {
                return Err(DomainError::ClusterForming);
            }
            control_write(
                &active,
                GroupCommand::ReplicaReady {
                    stream_id: descriptor.stream(),
                    group_id,
                },
            )
            .await?;
        }
        control_write(
            &active,
            GroupCommand::ActivateStream {
                stream_id: descriptor.stream(),
            },
        )
        .await
    }

    pub async fn describe_stream(
        &self,
        cluster: ClusterId,
        stream_id: Option<StreamId>,
        name: Option<StreamName>,
    ) -> Result<StreamDescriptor, DomainError> {
        let active = self.application_cluster().await?;
        validate_cluster(&active, cluster).await?;
        linearize_control(&active).await?;
        match (stream_id, name) {
            (Some(id), None) => active
                .control_reader
                .stream_by_id(id)?
                .ok_or(DomainError::StreamNotFound),
            (None, Some(name)) => active
                .control_reader
                .stream_by_name(&name)?
                .filter(|value| value.lifecycle() == StreamLifecycle::Active)
                .ok_or(DomainError::StreamNotFound),
            _ => Err(DomainError::InvalidIdentity {
                kind: "stream selector".to_owned(),
                reason: "provide exactly one of stream ID or stream name".to_owned(),
            }),
        }
    }

    pub async fn list_streams(
        &self,
        cluster: ClusterId,
    ) -> Result<Vec<StreamDescriptor>, DomainError> {
        let active = self.application_cluster().await?;
        validate_cluster(&active, cluster).await?;
        linearize_control(&active).await?;
        active.control_reader.active_streams()
    }

    pub async fn delete_stream(
        &self,
        cluster: ClusterId,
        stream_id: StreamId,
    ) -> Result<StreamDescriptor, DomainError> {
        let active = self.application_cluster().await?;
        validate_cluster(&active, cluster).await?;
        control_write(&active, GroupCommand::BeginDeleteStream { stream_id }).await?;
        control_write(&active, GroupCommand::FinishDeleteStream { stream_id }).await
    }

    pub async fn route(
        &self,
        cluster: ClusterId,
        stream_id: Option<StreamId>,
        name: Option<StreamName>,
        partition: PartitionId,
    ) -> Result<PartitionRoute, DomainError> {
        let descriptor = self.describe_stream(cluster, stream_id, name).await?;
        let placement =
            descriptor
                .placement(partition)
                .ok_or_else(|| DomainError::InvalidRange {
                    reason: "partition is outside the stream partition set".to_owned(),
                })?;
        Ok(PartitionRoute::new(
            descriptor.cluster(),
            descriptor.stream(),
            descriptor.name().clone(),
            partition,
            placement.group(),
            descriptor.revision(),
        ))
    }

    pub async fn route_leader(&self, group_id: GroupId) -> Option<LeaderHint> {
        let active = self.active.read().await.as_ref().cloned()?;
        let leader_id = active
            .data
            .get(&group_id.get())?
            .raft
            .metrics()
            .borrow_watched()
            .current_leader?;
        let manifest = active.manifest.read().await;
        match &*manifest {
            ActiveManifest::V1(_) if leader_id == self.local.node_id().get() => Some(
                LeaderHint::new(self.local.node_id(), self.local.public_uri()),
            ),
            ActiveManifest::V2(value) => value
                .formation
                .member(leader_id)
                .map(|member| LeaderHint::new(member.node_id(), member.public_uri())),
            _ => None,
        }
    }

    pub async fn publish(
        &self,
        batch: PublishBatch,
        route_group_id: Option<GroupId>,
        route_revision: Option<u64>,
    ) -> Result<PublishReceipt, DomainError> {
        let request = batch.request().clone();
        let active = self.application_cluster().await?;
        let route = resolve_data_route(
            &active,
            batch.cluster(),
            batch.partition(),
            route_group_id,
            route_revision,
        )
        .await?;
        let group = active
            .data
            .get(&route.group().get())
            .ok_or(DomainError::StaleRoute)?;
        if let Some((group_id, delay)) = self.verification_delay
            && group_id == route.group().get()
        {
            tokio::time::sleep(delay).await;
        }
        let response = tokio::time::timeout(
            OPERATION_TIMEOUT,
            group.raft.client_write(GroupCommand::Publish { batch }),
        )
        .await
        .map_err(|_| DomainError::QuorumUnavailable {
            group: ConsensusGroup::Data,
            outcome: RequestOutcome::AmbiguousCommit,
            request: Some(AmbiguousRequest::Publish {
                request: request.clone(),
            }),
        })?
        .map_err(|error| map_write_error(error, &active, ConsensusGroup::Data))?;
        match response.data {
            ApplyResult::Published(receipt) => Ok(receipt),
            ApplyResult::Rejected(error) => Err(error),
            other => Err(DomainError::Storage {
                reason: format!("unexpected publish apply result {other}"),
            }),
        }
    }

    pub async fn fetch(
        &self,
        cluster: ClusterId,
        partition: PartitionKey,
        offset: RecordOffset,
        limit: u32,
        route_group_id: Option<GroupId>,
        route_revision: Option<u64>,
    ) -> Result<FetchPage, DomainError> {
        let active = self.application_cluster().await?;
        let route =
            resolve_data_route(&active, cluster, partition, route_group_id, route_revision).await?;
        let group = active
            .data
            .get(&route.group().get())
            .ok_or(DomainError::StaleRoute)?;
        linearize(&active, &group.raft).await?;
        group.reader.fetch(cluster, partition, offset, limit)
    }

    pub async fn receipt(
        &self,
        cluster: ClusterId,
        partition: PartitionKey,
        request: &ProducerRequestId,
        route_group_id: Option<GroupId>,
        route_revision: Option<u64>,
    ) -> Result<PublishReceipt, DomainError> {
        let active = self.application_cluster().await?;
        let route =
            resolve_data_route(&active, cluster, partition, route_group_id, route_revision).await?;
        let group = active
            .data
            .get(&route.group().get())
            .ok_or(DomainError::StaleRoute)?;
        linearize(&active, &group.raft).await?;
        group.reader.receipt(partition, request)
    }

    pub async fn create_bookmark(
        &self,
        cluster: ClusterId,
        spec: CreateBookmarkSpec,
        route_group_id: Option<GroupId>,
        route_revision: Option<u64>,
    ) -> Result<CommittedBookmark, DomainError> {
        let active = self.application_cluster().await?;
        let route = resolve_data_route(
            &active,
            cluster,
            spec.partition(),
            route_group_id,
            route_revision,
        )
        .await?;
        let group = active
            .data
            .get(&route.group().get())
            .ok_or(DomainError::StaleRoute)?;
        let response = tokio::time::timeout(
            OPERATION_TIMEOUT,
            group.raft.client_write(GroupCommand::CreateBookmark {
                id: spec.id(),
                partition: spec.partition(),
                name: spec.name().clone(),
                offset: spec.offset(),
            }),
        )
        .await
        .map_err(|_| DomainError::QuorumUnavailable {
            group: ConsensusGroup::Data,
            outcome: RequestOutcome::AmbiguousCommit,
            request: None,
        })?
        .map_err(|error| map_write_error(error, &active, ConsensusGroup::Data))?;
        match response.data {
            ApplyResult::Bookmark(bookmark) => Ok(bookmark),
            ApplyResult::Rejected(error) => Err(error),
            other => Err(DomainError::Storage {
                reason: format!("unexpected bookmark apply result {other}"),
            }),
        }
    }

    pub async fn delete_bookmark(
        &self,
        cluster: ClusterId,
        partition: PartitionKey,
        id: BookmarkId,
        route_group_id: Option<GroupId>,
        route_revision: Option<u64>,
    ) -> Result<CommittedBookmark, DomainError> {
        let active = self.application_cluster().await?;
        let route =
            resolve_data_route(&active, cluster, partition, route_group_id, route_revision).await?;
        let group = active
            .data
            .get(&route.group().get())
            .ok_or(DomainError::StaleRoute)?;
        let response = tokio::time::timeout(
            OPERATION_TIMEOUT,
            group
                .raft
                .client_write(GroupCommand::DeleteBookmark { partition, id }),
        )
        .await
        .map_err(|_| DomainError::QuorumUnavailable {
            group: ConsensusGroup::Data,
            outcome: RequestOutcome::AmbiguousCommit,
            request: None,
        })?
        .map_err(|error| map_write_error(error, &active, ConsensusGroup::Data))?;
        match response.data {
            ApplyResult::Bookmark(bookmark) => Ok(bookmark),
            ApplyResult::Rejected(error) => Err(error),
            other => Err(DomainError::Storage {
                reason: format!("unexpected bookmark delete result {other}"),
            }),
        }
    }

    pub async fn resolve_bookmark(
        &self,
        cluster: ClusterId,
        partition: PartitionKey,
        name: &BookmarkName,
        route_group_id: Option<GroupId>,
        route_revision: Option<u64>,
    ) -> Result<CommittedBookmark, DomainError> {
        let active = self.application_cluster().await?;
        let route =
            resolve_data_route(&active, cluster, partition, route_group_id, route_revision).await?;
        let group = active
            .data
            .get(&route.group().get())
            .ok_or(DomainError::StaleRoute)?;
        linearize(&active, &group.raft).await?;
        group.reader.resolve_bookmark(partition, name)
    }

    pub async fn list_bookmarks(
        &self,
        cluster: ClusterId,
        request: &BookmarkPageRequest,
        route_group_id: Option<GroupId>,
        route_revision: Option<u64>,
    ) -> Result<BookmarkPage, DomainError> {
        let active = self.application_cluster().await?;
        let route = resolve_data_route(
            &active,
            cluster,
            request.partition(),
            route_group_id,
            route_revision,
        )
        .await?;
        let group = active
            .data
            .get(&route.group().get())
            .ok_or(DomainError::StaleRoute)?;
        linearize(&active, &group.raft).await?;
        group.reader.list_bookmarks(request)
    }

    pub async fn advance_retention(
        &self,
        cluster: ClusterId,
        request: RetentionRequest,
        route_group_id: Option<GroupId>,
        route_revision: Option<u64>,
    ) -> Result<RetentionResult, DomainError> {
        let active = self.application_cluster().await?;
        let route = resolve_data_route(
            &active,
            cluster,
            request.partition(),
            route_group_id,
            route_revision,
        )
        .await?;
        let group = active
            .data
            .get(&route.group().get())
            .ok_or(DomainError::StaleRoute)?;
        maintain_group_retention(&active, group, request.partition()).await?;
        let mutation = request.request().clone();
        let response = tokio::time::timeout(
            OPERATION_TIMEOUT,
            group.raft.client_write(GroupCommand::AdvanceRetention {
                request,
                clock: lease_clock_observation()?,
            }),
        )
        .await
        .map_err(|_| DomainError::QuorumUnavailable {
            group: ConsensusGroup::Data,
            outcome: RequestOutcome::AmbiguousCommit,
            request: Some(AmbiguousRequest::Mutation { request: mutation }),
        })?
        .map_err(|error| map_write_error(error, &active, ConsensusGroup::Data))?;
        match response.data {
            ApplyResult::Retention(result) => {
                if let Err(error) =
                    maintain_group_retention(&active, group, result.partition()).await
                {
                    eprintln!(
                        "{}",
                        serde_json::json!({
                            "event": "retention_maintenance_failed",
                            "partition": result.partition(),
                            "detail": error.to_string(),
                        })
                    );
                }
                Ok(result)
            }
            ApplyResult::Rejected(error) => Err(error),
            other => Err(DomainError::Storage {
                reason: format!("unexpected retention apply result {other}"),
            }),
        }
    }

    pub async fn retention_status(
        &self,
        cluster: ClusterId,
        partition: PartitionKey,
        route_group_id: Option<GroupId>,
        route_revision: Option<u64>,
    ) -> Result<RetentionStatus, DomainError> {
        let active = self.application_cluster().await?;
        let route =
            resolve_data_route(&active, cluster, partition, route_group_id, route_revision).await?;
        let group = active
            .data
            .get(&route.group().get())
            .ok_or(DomainError::StaleRoute)?;
        maintain_group_retention(&active, group, partition).await
    }

    pub async fn admit_replay_lease(
        &self,
        cluster: ClusterId,
        request: ReplayLeaseRequest,
        route_group_id: Option<GroupId>,
        route_revision: Option<u64>,
    ) -> Result<ReplayLease, DomainError> {
        let partition = request.range().partition();
        let mutation = request.request().clone();
        let active = self.application_cluster().await?;
        let route =
            resolve_data_route(&active, cluster, partition, route_group_id, route_revision).await?;
        let group = active
            .data
            .get(&route.group().get())
            .ok_or(DomainError::StaleRoute)?;
        maintain_group_retention(&active, group, partition).await?;
        let response = tokio::time::timeout(
            OPERATION_TIMEOUT,
            group.raft.client_write(GroupCommand::AdmitReplayLease {
                request,
                clock: lease_clock_observation()?,
            }),
        )
        .await
        .map_err(|_| DomainError::QuorumUnavailable {
            group: ConsensusGroup::Data,
            outcome: RequestOutcome::AmbiguousCommit,
            request: Some(AmbiguousRequest::Mutation { request: mutation }),
        })?
        .map_err(|error| map_write_error(error, &active, ConsensusGroup::Data))?;
        match response.data {
            ApplyResult::ReplayLease(lease) => Ok(lease),
            ApplyResult::Rejected(error) => Err(error),
            other => Err(DomainError::Storage {
                reason: format!("unexpected replay lease apply result {other}"),
            }),
        }
    }

    pub async fn renew_replay_lease(
        &self,
        cluster: ClusterId,
        request: LeaseRenewal,
        route_group_id: Option<GroupId>,
        route_revision: Option<u64>,
    ) -> Result<ReplayLease, DomainError> {
        let partition = request.partition();
        let mutation = request.request().clone();
        let active = self.application_cluster().await?;
        let route =
            resolve_data_route(&active, cluster, partition, route_group_id, route_revision).await?;
        let group = active
            .data
            .get(&route.group().get())
            .ok_or(DomainError::StaleRoute)?;
        maintain_group_retention(&active, group, partition).await?;
        let response = tokio::time::timeout(
            OPERATION_TIMEOUT,
            group.raft.client_write(GroupCommand::RenewReplayLease {
                request,
                clock: lease_clock_observation()?,
            }),
        )
        .await
        .map_err(|_| DomainError::QuorumUnavailable {
            group: ConsensusGroup::Data,
            outcome: RequestOutcome::AmbiguousCommit,
            request: Some(AmbiguousRequest::Mutation { request: mutation }),
        })?
        .map_err(|error| map_write_error(error, &active, ConsensusGroup::Data))?;
        match response.data {
            ApplyResult::ReplayLease(lease) => Ok(lease),
            ApplyResult::Rejected(error) => Err(error),
            other => Err(DomainError::Storage {
                reason: format!("unexpected replay lease renewal result {other}"),
            }),
        }
    }

    pub async fn release_replay_lease(
        &self,
        cluster: ClusterId,
        request: LeaseRelease,
        route_group_id: Option<GroupId>,
        route_revision: Option<u64>,
    ) -> Result<ReplayLease, DomainError> {
        let partition = request.partition();
        let mutation = request.request().clone();
        let active = self.application_cluster().await?;
        let route =
            resolve_data_route(&active, cluster, partition, route_group_id, route_revision).await?;
        let group = active
            .data
            .get(&route.group().get())
            .ok_or(DomainError::StaleRoute)?;
        let response = tokio::time::timeout(
            OPERATION_TIMEOUT,
            group.raft.client_write(GroupCommand::ReleaseReplayLease {
                request,
                clock: lease_clock_observation()?,
            }),
        )
        .await
        .map_err(|_| DomainError::QuorumUnavailable {
            group: ConsensusGroup::Data,
            outcome: RequestOutcome::AmbiguousCommit,
            request: Some(AmbiguousRequest::Mutation { request: mutation }),
        })?
        .map_err(|error| map_write_error(error, &active, ConsensusGroup::Data))?;
        match response.data {
            ApplyResult::ReplayLease(lease) => {
                if let Err(error) = maintain_group_retention(&active, group, partition).await {
                    eprintln!(
                        "{}",
                        serde_json::json!({
                            "event": "retention_maintenance_failed",
                            "partition": partition,
                            "detail": error.to_string(),
                        })
                    );
                }
                Ok(lease)
            }
            ApplyResult::Rejected(error) => Err(error),
            other => Err(DomainError::Storage {
                reason: format!("unexpected replay lease release result {other}"),
            }),
        }
    }

    pub async fn replay_lease(
        &self,
        cluster: ClusterId,
        partition: PartitionKey,
        lease: ReplayLeaseId,
        route_group_id: Option<GroupId>,
        route_revision: Option<u64>,
    ) -> Result<ReplayLease, DomainError> {
        let active = self.application_cluster().await?;
        let route =
            resolve_data_route(&active, cluster, partition, route_group_id, route_revision).await?;
        let group = active
            .data
            .get(&route.group().get())
            .ok_or(DomainError::StaleRoute)?;
        maintain_group_retention(&active, group, partition).await?;
        group.reader.replay_lease(partition, lease)
    }

    pub async fn fetch_protected(
        &self,
        request: ProtectedFetchRequest,
        route_group_id: Option<GroupId>,
        route_revision: Option<u64>,
    ) -> Result<FetchPage, DomainError> {
        let active = self.application_cluster().await?;
        let route = resolve_data_route(
            &active,
            request.cluster(),
            request.partition(),
            route_group_id,
            route_revision,
        )
        .await?;
        let group = active
            .data
            .get(&route.group().get())
            .ok_or(DomainError::StaleRoute)?;
        maintain_group_retention(&active, group, request.partition()).await?;
        group.reader.fetch_protected(
            request.cluster(),
            request.partition(),
            request.lease(),
            request.offset(),
            request.limit(),
        )
    }

    pub async fn create_stream_bookmark(
        &self,
        cluster: ClusterId,
        id: BookmarkId,
        name: BookmarkName,
        vector: StreamCursorVector,
    ) -> Result<CommittedStreamBookmark, DomainError> {
        let active = self.application_cluster().await?;
        if vector.cluster() != cluster {
            return Err(DomainError::IdentityMismatch {
                reason: "stream bookmark vector belongs to another cluster".to_owned(),
            });
        }
        for cursor in vector.positions() {
            let route =
                resolve_data_route(&active, cluster, cursor.partition(), None, None).await?;
            let group = active
                .data
                .get(&route.group().get())
                .ok_or(DomainError::StaleRoute)?;
            linearize(&active, &group.raft).await?;
            let tail = group.reader.partition_tail(cursor.partition())?;
            if cursor.next_offset() > tail {
                return Err(DomainError::InvalidRange {
                    reason: format!(
                        "stream bookmark offset {} is past partition {} tail {}",
                        cursor.next_offset().get(),
                        cursor.partition().partition().get(),
                        tail.get()
                    ),
                });
            }
        }
        let response = tokio::time::timeout(
            OPERATION_TIMEOUT,
            active
                .control
                .client_write(GroupCommand::CreateStreamBookmark { id, name, vector }),
        )
        .await
        .map_err(|_| DomainError::QuorumUnavailable {
            group: ConsensusGroup::Control,
            outcome: RequestOutcome::AmbiguousCommit,
            request: None,
        })?
        .map_err(|error| map_write_error(error, &active, ConsensusGroup::Control))?;
        match response.data {
            ApplyResult::StreamBookmark(bookmark) => Ok(bookmark),
            ApplyResult::Rejected(error) => Err(error),
            other => Err(DomainError::Storage {
                reason: format!("unexpected stream bookmark apply result {other}"),
            }),
        }
    }

    pub async fn delete_stream_bookmark(
        &self,
        cluster: ClusterId,
        stream_id: StreamId,
        id: BookmarkId,
    ) -> Result<CommittedStreamBookmark, DomainError> {
        let active = self.application_cluster().await?;
        validate_cluster(&active, cluster).await?;
        let response = tokio::time::timeout(
            OPERATION_TIMEOUT,
            active
                .control
                .client_write(GroupCommand::DeleteStreamBookmark { stream_id, id }),
        )
        .await
        .map_err(|_| DomainError::QuorumUnavailable {
            group: ConsensusGroup::Control,
            outcome: RequestOutcome::AmbiguousCommit,
            request: None,
        })?
        .map_err(|error| map_write_error(error, &active, ConsensusGroup::Control))?;
        match response.data {
            ApplyResult::StreamBookmark(bookmark) => Ok(bookmark),
            ApplyResult::Rejected(error) => Err(error),
            other => Err(DomainError::Storage {
                reason: format!("unexpected stream bookmark delete result {other}"),
            }),
        }
    }

    pub async fn resolve_stream_bookmark(
        &self,
        cluster: ClusterId,
        stream_id: StreamId,
        name: &BookmarkName,
    ) -> Result<CommittedStreamBookmark, DomainError> {
        let active = self.application_cluster().await?;
        linearize_control(&active).await?;
        validate_cluster(&active, cluster).await?;
        active
            .control_reader
            .resolve_stream_bookmark(stream_id, name)
    }

    pub async fn list_stream_bookmarks(
        &self,
        request: &StreamBookmarkPageRequest,
    ) -> Result<StreamBookmarkPage, DomainError> {
        let active = self.application_cluster().await?;
        linearize_control(&active).await?;
        validate_cluster(&active, request.cluster()).await?;
        active.control_reader.list_stream_bookmarks(request)
    }

    pub async fn identity(&self) -> Option<ClusterId> {
        let active = self.active.read().await.as_ref().cloned()?;
        let manifest = active.manifest.read().await;
        manifest
            .is_application_active()
            .then(|| manifest.cluster_id())
    }

    pub async fn diagnostics(&self) -> NodeDiagnostic {
        let active = self.active.read().await.as_ref().cloned();
        let Some(active) = active else {
            return NodeDiagnostic {
                node_id: self.local.node_id().get(),
                lifecycle: "pristine".to_owned(),
                peers: vec![self.local.clone()],
                groups: Vec::new(),
                data_group_slots: self.group_pool.max_data_groups,
                data_group_count: 0,
                rocksdb_cache_budget_bytes: self.group_pool.rocksdb_cache_bytes,
                rocksdb_write_buffer_budget_bytes: self.group_pool.rocksdb_write_buffer_bytes,
                per_group_cache_bytes: self
                    .group_pool
                    .per_group_budget()
                    .map_or(0, |value| value.cache_bytes),
                per_group_write_buffer_bytes: self
                    .group_pool
                    .per_group_budget()
                    .map_or(0, |value| value.write_buffer_bytes),
                unsupported_claims: unsupported_claims(),
            };
        };
        let manifest = active.manifest.read().await.clone();
        let peers = match &manifest {
            ActiveManifest::V1(_) => vec![self.local.clone()],
            ActiveManifest::V2(value) => value.formation.members.clone(),
        };
        let pool = manifest.group_pool().clone();
        let budget = pool.per_group_budget().ok();
        let mut groups = vec![group_diagnostic(
            ConsensusGroup::Control,
            CONTROL_GROUP_ID,
            &active.control,
        )];
        groups.extend(active.data.iter().map(|(id, value)| {
            let mut diagnostic = group_diagnostic(ConsensusGroup::Data, *id, &value.raft);
            diagnostic.slot = Some(value.slot);
            diagnostic.cache_budget_bytes = Some(value.budget.cache_bytes);
            diagnostic.write_buffer_budget_bytes = Some(value.budget.write_buffer_bytes);
            diagnostic
        }));
        NodeDiagnostic {
            node_id: self.local.node_id().get(),
            lifecycle: manifest.lifecycle().to_owned(),
            peers,
            groups,
            data_group_slots: pool.max_data_groups,
            data_group_count: active.data.len(),
            rocksdb_cache_budget_bytes: pool.rocksdb_cache_bytes,
            rocksdb_write_buffer_budget_bytes: pool.rocksdb_write_buffer_bytes,
            per_group_cache_bytes: budget.map_or(0, |value| value.cache_bytes),
            per_group_write_buffer_bytes: budget.map_or(0, |value| value.write_buffer_bytes),
            unsupported_claims: unsupported_claims(),
        }
    }

    pub async fn shutdown(&self) -> Result<(), DomainError> {
        if let Some(active) = self.active.read().await.as_ref() {
            active.shutdown().await?;
        }
        Ok(())
    }

    async fn application_cluster(&self) -> Result<Arc<ActiveCluster>, DomainError> {
        let active = self
            .active
            .read()
            .await
            .clone()
            .ok_or(DomainError::NotBootstrapped)?;
        if !active.manifest.read().await.is_application_active() {
            return Err(DomainError::ClusterForming);
        }
        Ok(active)
    }

    async fn open_v1(&self, manifest: NodeManifestV1) -> Result<ActiveCluster, DomainError> {
        manifest.validate()?;
        if manifest.group_pool != self.group_pool {
            return Err(DomainError::IdentityMismatch {
                reason: "configured group pool conflicts with the durable manifest".to_owned(),
            });
        }
        validate_group_directories(&self.data_dir, &manifest.group_pool.data_group_ids()?)?;
        if manifest.node_id != self.local.node_id().get() {
            return Err(DomainError::IdentityMismatch {
                reason: format!(
                    "stored node {} does not match configured node {}",
                    manifest.node_id,
                    self.local.node_id()
                ),
            });
        }
        let control_identity = group_identity(
            manifest.cluster_id,
            manifest.control_group_id,
            GroupKind::Control,
        )?;
        let budget = manifest.group_pool.per_group_budget()?;
        let control_handles = open_control_store(
            &group_path(&self.data_dir, manifest.control_group_id),
            &control_identity,
            self.receipt_window,
            budget,
        )
        .map_err(storage_error)?;
        let control_reader = control_handles.reader.clone();
        let control = Raft::new(
            self.local.node_id().get(),
            raft_config(format!("{}-control", manifest.cluster_id), false)?,
            NoRemoteNetworkFactory,
            control_handles.log_store,
            control_handles.state_machine,
        )
        .await
        .map_err(raft_fatal)?;
        let mut data = BTreeMap::new();
        for (slot, group_id) in manifest
            .group_pool
            .data_group_ids()?
            .into_iter()
            .enumerate()
        {
            let identity = group_identity(manifest.cluster_id, group_id.get(), GroupKind::Data)?;
            let handles = open_data_store(
                &group_path(&self.data_dir, group_id.get()),
                &identity,
                self.receipt_window,
                budget,
            )
            .map_err(storage_error)?;
            let reader = handles.reader.clone();
            let raft = Raft::new(
                self.local.node_id().get(),
                raft_config(format!("{}-data-{}", manifest.cluster_id, group_id), false)?,
                NoRemoteNetworkFactory,
                handles.log_store,
                handles.state_machine,
            )
            .await
            .map_err(raft_fatal)?;
            data.insert(
                group_id.get(),
                DataGroup {
                    raft,
                    reader,
                    slot: slot as u16,
                    budget,
                },
            );
        }
        recover_standalone(&control, self.local.node_id().get()).await?;
        for group in data.values() {
            recover_standalone(&group.raft, self.local.node_id().get()).await?;
        }
        validate_bootstrap_readers(&control_reader, &data, &manifest.bootstrap)?;
        Ok(ActiveCluster {
            manifest: RwLock::new(ActiveManifest::V1(manifest)),
            control,
            data,
            control_reader,
            maintenance_shutdown: AtomicBool::new(false),
        })
    }

    async fn create_v1(&self, manifest: NodeManifestV1) -> Result<ActiveCluster, DomainError> {
        let control_identity = group_identity(
            manifest.cluster_id,
            manifest.control_group_id,
            GroupKind::Control,
        )?;
        let budget = manifest.group_pool.per_group_budget()?;
        let control_handles = create_control_store(
            &group_path(&self.data_dir, manifest.control_group_id),
            control_identity,
            self.receipt_window,
            budget,
        )
        .map_err(storage_error)?;
        let control_reader = control_handles.reader.clone();
        let control = Raft::new(
            self.local.node_id().get(),
            raft_config(format!("{}-control", manifest.cluster_id), false)?,
            NoRemoteNetworkFactory,
            control_handles.log_store,
            control_handles.state_machine,
        )
        .await
        .map_err(raft_fatal)?;
        let mut data = BTreeMap::new();
        for (slot, group_id) in manifest
            .group_pool
            .data_group_ids()?
            .into_iter()
            .enumerate()
        {
            let identity = group_identity(manifest.cluster_id, group_id.get(), GroupKind::Data)?;
            let handles = create_data_store(
                &group_path(&self.data_dir, group_id.get()),
                identity,
                self.receipt_window,
                budget,
            )
            .map_err(storage_error)?;
            let reader = handles.reader.clone();
            let raft = Raft::new(
                self.local.node_id().get(),
                raft_config(format!("{}-data-{}", manifest.cluster_id, group_id), false)?,
                NoRemoteNetworkFactory,
                handles.log_store,
                handles.state_machine,
            )
            .await
            .map_err(raft_fatal)?;
            data.insert(
                group_id.get(),
                DataGroup {
                    raft,
                    reader,
                    slot: slot as u16,
                    budget,
                },
            );
        }
        initialize_seed_if_pristine(&control, self.local.node_id().get(), self.local.peer_uri())
            .await?;
        for group in data.values() {
            initialize_seed_if_pristine(
                &group.raft,
                self.local.node_id().get(),
                self.local.peer_uri(),
            )
            .await?;
        }
        elect_seed(&control, self.local.node_id().get()).await?;
        for group in data.values() {
            elect_seed(&group.raft, self.local.node_id().get()).await?;
        }
        ensure_bootstrap_command(
            &control,
            &control_reader,
            GroupCommand::BootstrapControl {
                spec: manifest.bootstrap.clone(),
                data_groups: manifest.group_pool.data_group_ids()?,
                max_streams: manifest.group_pool.max_streams,
                max_partitions_per_stream: manifest.group_pool.max_partitions_per_stream,
            },
            &manifest.bootstrap,
        )
        .await?;
        for group in data.values() {
            ensure_bootstrap_command(
                &group.raft,
                &group.reader,
                GroupCommand::BootstrapData {
                    spec: manifest.bootstrap.clone(),
                },
                &manifest.bootstrap,
            )
            .await?;
        }
        Ok(ActiveCluster {
            manifest: RwLock::new(ActiveManifest::V1(manifest)),
            control,
            data,
            control_reader,
            maintenance_shutdown: AtomicBool::new(false),
        })
    }

    async fn open_v2(&self, manifest: NodeManifestV2) -> Result<ActiveCluster, DomainError> {
        manifest.validate_local(&self.local)?;
        let active = self
            .create_v2(
                manifest.clone(),
                manifest.state != PersistedNodeState::Active,
            )
            .await?;
        if manifest.state == PersistedNodeState::Active {
            wait_active_local_recovery(&active, &manifest.formation).await?;
        } else if manifest.state == PersistedNodeState::Forming
            && manifest.formation.seed_node_id == self.local.node_id()
        {
            active
                .control
                .trigger()
                .elect(false)
                .await
                .map_err(raft_fatal)?;
            for group in active.data.values() {
                group
                    .raft
                    .trigger()
                    .elect(false)
                    .await
                    .map_err(raft_fatal)?;
            }
        }
        Ok(active)
    }

    async fn create_v2(
        &self,
        manifest: NodeManifestV2,
        create: bool,
    ) -> Result<ActiveCluster, DomainError> {
        if manifest.formation.group_pool != self.group_pool {
            return Err(DomainError::IdentityMismatch {
                reason: "configured group pool conflicts with the durable manifest".to_owned(),
            });
        }
        validate_group_directories(
            &self.data_dir,
            &manifest.formation.group_pool.data_group_ids()?,
        )?;
        let control_identity = group_identity(
            manifest.formation.cluster_id,
            manifest.formation.control_group_id.get(),
            GroupKind::Control,
        )?;
        let budget = manifest.formation.group_pool.per_group_budget()?;
        let control_path = group_path(&self.data_dir, CONTROL_GROUP_ID);
        let control_handles = if create {
            create_control_store(&control_path, control_identity, self.receipt_window, budget)
        } else {
            open_control_store(
                &control_path,
                &control_identity,
                self.receipt_window,
                budget,
            )
        }
        .map_err(storage_error)?;
        let control_reader = control_handles.reader.clone();
        let members = manifest.formation.members.clone();
        let control = Raft::new(
            self.local.node_id().get(),
            raft_config(format!("{}-control", manifest.formation.cluster_id), true)?,
            TonicNetworkFactory::<ControlRaftConfig>::new(
                manifest.formation.cluster_id,
                CONTROL_GROUP_ID,
                self.local.node_id().get(),
                members.clone(),
                self.peer_routes.clone(),
            ),
            control_handles.log_store,
            control_handles.state_machine,
        )
        .await
        .map_err(raft_fatal)?;
        let mut data = BTreeMap::new();
        for (slot, group_id) in manifest
            .formation
            .group_pool
            .data_group_ids()?
            .into_iter()
            .enumerate()
        {
            let identity = group_identity(
                manifest.formation.cluster_id,
                group_id.get(),
                GroupKind::Data,
            )?;
            let path = group_path(&self.data_dir, group_id.get());
            let handles = if create {
                create_data_store(&path, identity, self.receipt_window, budget)
            } else {
                open_data_store(&path, &identity, self.receipt_window, budget)
            }
            .map_err(storage_error)?;
            let reader = handles.reader.clone();
            let raft = Raft::new(
                self.local.node_id().get(),
                raft_config(
                    format!("{}-data-{}", manifest.formation.cluster_id, group_id),
                    true,
                )?,
                TonicNetworkFactory::<DataRaftConfig>::new(
                    manifest.formation.cluster_id,
                    group_id.get(),
                    self.local.node_id().get(),
                    members.clone(),
                    self.peer_routes.clone(),
                ),
                handles.log_store,
                handles.state_machine,
            )
            .await
            .map_err(raft_fatal)?;
            data.insert(
                group_id.get(),
                DataGroup {
                    raft,
                    reader,
                    slot: slot as u16,
                    budget,
                },
            );
        }
        Ok(ActiveCluster {
            manifest: RwLock::new(ActiveManifest::V2(manifest)),
            control,
            data,
            control_reader,
            maintenance_shutdown: AtomicBool::new(false),
        })
    }
}

async fn wait_active_local_recovery(
    active: &ActiveCluster,
    formation: &FormationSpec,
) -> Result<(), DomainError> {
    let deadline = Instant::now() + FORMATION_TIMEOUT;
    loop {
        if prove_active(active, formation).await.is_ok() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(DomainError::Storage {
                reason: "active node did not recover its durable membership and bootstrap state"
                    .to_owned(),
            });
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn initialize_seed_if_pristine<C>(
    raft: &Raft<C, RocksStateMachine<C>>,
    node_id: u64,
    peer_uri: &str,
) -> Result<(), DomainError>
where
    C: openraft::RaftTypeConfig<D = GroupCommand, R = ApplyResult, NodeId = u64, Node = BasicNode>,
    RocksStateMachine<C>: openraft::storage::RaftStateMachine<C, SnapshotData = Vec<u8>>,
{
    let metrics = raft.metrics().borrow_watched().clone();
    if metrics.last_log_index.is_none()
        && metrics
            .membership_config
            .membership()
            .nodes()
            .next()
            .is_none()
    {
        raft.initialize(BTreeMap::from([(node_id, BasicNode::new(peer_uri))]))
            .await
            .map_err(raft_fatal)?;
    }
    Ok(())
}

async fn elect_seed<C>(
    raft: &Raft<C, RocksStateMachine<C>>,
    node_id: u64,
) -> Result<(), DomainError>
where
    C: openraft::RaftTypeConfig<D = GroupCommand, R = ApplyResult, NodeId = u64, Node = BasicNode>,
    RocksStateMachine<C>: openraft::storage::RaftStateMachine<C, SnapshotData = Vec<u8>>,
{
    raft.trigger().elect(false).await.map_err(raft_fatal)?;
    raft.wait(Some(FORMATION_TIMEOUT))
        .current_leader(node_id, "wait for the seed leader")
        .await
        .map_err(raft_fatal)?;
    raft.wait_for_recovery(Some(FORMATION_TIMEOUT))
        .await
        .map_err(raft_fatal)?;
    Ok(())
}

async fn recover_standalone<C>(
    raft: &Raft<C, RocksStateMachine<C>>,
    node_id: u64,
) -> Result<(), DomainError>
where
    C: openraft::RaftTypeConfig<D = GroupCommand, R = ApplyResult, NodeId = u64, Node = BasicNode>,
    RocksStateMachine<C>: openraft::storage::RaftStateMachine<C, SnapshotData = Vec<u8>>,
{
    raft.wait_for_recovery(Some(FORMATION_TIMEOUT))
        .await
        .map_err(raft_fatal)?;
    raft.trigger().elect(false).await.map_err(raft_fatal)?;
    raft.wait(Some(FORMATION_TIMEOUT))
        .current_leader(node_id, "wait for the recovered standalone leader")
        .await
        .map_err(raft_fatal)?;
    Ok(())
}

async fn ensure_bootstrap_command<C>(
    raft: &Raft<C, RocksStateMachine<C>>,
    reader: &CommittedStateReader,
    command: GroupCommand,
    expected: &BootstrapSpec,
) -> Result<(), DomainError>
where
    C: openraft::RaftTypeConfig<D = GroupCommand, R = ApplyResult, NodeId = u64, Node = BasicNode>,
    RocksStateMachine<C>: openraft::storage::RaftStateMachine<C, SnapshotData = Vec<u8>>,
{
    match reader.bootstrap_spec()? {
        Some(stored) if &stored == expected => return Ok(()),
        Some(_) => {
            return Err(DomainError::IdentityMismatch {
                reason: "stored bootstrap identity conflicts with the manifest".to_owned(),
            });
        }
        None => {}
    }
    let response = raft.client_write(command).await.map_err(raft_fatal)?;
    match response.data {
        ApplyResult::Bootstrapped(_) => Ok(()),
        ApplyResult::Rejected(error) => Err(error),
        other => Err(DomainError::Storage {
            reason: format!("unexpected bootstrap apply result {other}"),
        }),
    }
}

async fn add_learners<C>(
    raft: &Raft<C, RocksStateMachine<C>>,
    formation: &FormationSpec,
) -> Result<(), DomainError>
where
    C: openraft::RaftTypeConfig<D = GroupCommand, R = ApplyResult, NodeId = u64, Node = BasicNode>,
    RocksStateMachine<C>: openraft::storage::RaftStateMachine<C, SnapshotData = Vec<u8>>,
{
    for member in &formation.members {
        if member.node_id() == formation.seed_node_id {
            continue;
        }
        let metrics = raft.metrics().borrow_watched().clone();
        if metrics
            .membership_config
            .membership()
            .get_node(&member.node_id().get())
            .is_none()
        {
            raft.add_learner(
                member.node_id().get(),
                BasicNode::new(member.peer_uri()),
                true,
            )
            .await
            .map_err(raft_fatal)?;
        }
    }
    Ok(())
}

async fn wait_exact_replication<C>(
    raft: &Raft<C, RocksStateMachine<C>>,
    formation: &FormationSpec,
) -> Result<(), DomainError>
where
    C: openraft::RaftTypeConfig<D = GroupCommand, R = ApplyResult, NodeId = u64, Node = BasicNode>,
    RocksStateMachine<C>: openraft::storage::RaftStateMachine<C, SnapshotData = Vec<u8>>,
{
    let deadline = Instant::now() + FORMATION_TIMEOUT;
    loop {
        let metrics = raft.metrics().borrow_watched().clone();
        let exact = metrics.last_log_index.is_some_and(|leader_last| {
            formation.members.iter().all(|member| {
                member.node_id() == formation.seed_node_id
                    || metrics
                        .replication
                        .as_ref()
                        .and_then(|progress| progress.get(&member.node_id().get()))
                        .and_then(|matched| matched.as_ref())
                        .is_some_and(|matched| matched.index == leader_last)
            })
        });
        if exact {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(DomainError::Storage {
                reason: "learner replication did not exactly match the leader".to_owned(),
            });
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn converge_membership<C>(
    raft: &Raft<C, RocksStateMachine<C>>,
    formation: &FormationSpec,
) -> Result<(), DomainError>
where
    C: openraft::RaftTypeConfig<D = GroupCommand, R = ApplyResult, NodeId = u64, Node = BasicNode>,
    RocksStateMachine<C>: openraft::storage::RaftStateMachine<C, SnapshotData = Vec<u8>>,
{
    if !membership_is_exact(&raft.metrics().borrow_watched(), &formation.voter_ids()) {
        raft.change_membership(formation.voter_ids(), false)
            .await
            .map_err(raft_fatal)?;
    }
    let deadline = Instant::now() + FORMATION_TIMEOUT;
    loop {
        let metrics = raft.metrics().borrow_watched().clone();
        if membership_is_exact(&metrics, &formation.voter_ids()) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(DomainError::Storage {
                reason: "effective and committed membership did not converge".to_owned(),
            });
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn membership_is_exact<C>(metrics: &openraft::RaftMetrics<C>, voters: &BTreeSet<u64>) -> bool
where
    C: openraft::RaftTypeConfig<NodeId = u64>,
{
    let effective = metrics.membership_config.membership();
    let committed = metrics.committed_membership_config.membership();
    effective.get_joint_config().len() == 1
        && committed.get_joint_config().len() == 1
        && effective.voter_ids().collect::<BTreeSet<_>>() == *voters
        && committed.voter_ids().collect::<BTreeSet<_>>() == *voters
        && effective.learner_ids().next().is_none()
        && committed.learner_ids().next().is_none()
}

async fn prove_active(
    active: &ActiveCluster,
    formation: &FormationSpec,
) -> Result<(), DomainError> {
    validate_bootstrap_readers(&active.control_reader, &active.data, &formation.bootstrap)?;
    if !membership_is_exact(
        &active.control.metrics().borrow_watched(),
        &formation.voter_ids(),
    ) || active.data.values().any(|group| {
        !membership_is_exact(
            &group.raft.metrics().borrow_watched(),
            &formation.voter_ids(),
        )
    }) {
        return Err(DomainError::ClusterForming);
    }
    Ok(())
}

fn validate_bootstrap_readers(
    control: &CommittedStateReader,
    data: &BTreeMap<u64, DataGroup>,
    expected: &BootstrapSpec,
) -> Result<(), DomainError> {
    if control.bootstrap_spec()?.as_ref() != Some(expected) {
        return Err(DomainError::IdentityMismatch {
            reason: "all state machines must contain the manifest bootstrap identity".to_owned(),
        });
    }
    for group in data.values() {
        if group.reader.bootstrap_spec()?.as_ref() != Some(expected) {
            return Err(DomainError::IdentityMismatch {
                reason: "all state machines must contain the manifest bootstrap identity"
                    .to_owned(),
            });
        }
    }
    Ok(())
}

async fn resolve_data_route(
    active: &Arc<ActiveCluster>,
    cluster: ClusterId,
    partition: PartitionKey,
    supplied_group: Option<GroupId>,
    supplied_revision: Option<u64>,
) -> Result<PartitionRoute, DomainError> {
    linearize_control(active).await?;
    validate_cluster(active, cluster).await?;
    let route = active
        .control_reader
        .route(partition.stream(), partition.partition())?;
    if supplied_group.is_some_and(|value| value != route.group())
        || supplied_revision.is_some_and(|value| value != route.route_revision())
    {
        return Err(DomainError::StaleRoute);
    }
    Ok(route)
}

async fn validate_cluster(
    active: &Arc<ActiveCluster>,
    cluster: ClusterId,
) -> Result<(), DomainError> {
    if cluster != active.manifest.read().await.cluster_id() {
        return Err(DomainError::IdentityMismatch {
            reason: "request cluster does not match the durable manifest".to_owned(),
        });
    }
    Ok(())
}

async fn linearize_control(active: &Arc<ActiveCluster>) -> Result<(), DomainError> {
    tokio::time::timeout(
        OPERATION_TIMEOUT,
        active.control.ensure_linearizable(ReadPolicy::ReadIndex),
    )
    .await
    .map_err(|_| DomainError::QuorumUnavailable {
        group: ConsensusGroup::Control,
        outcome: RequestOutcome::NotApplicable,
        request: None,
    })?
    .map(|_| ())
    .map_err(|error| map_read_error(error, active, ConsensusGroup::Control))
}

async fn control_write(
    active: &Arc<ActiveCluster>,
    command: GroupCommand,
) -> Result<StreamDescriptor, DomainError> {
    let response = tokio::time::timeout(OPERATION_TIMEOUT, active.control.client_write(command))
        .await
        .map_err(|_| DomainError::QuorumUnavailable {
            group: ConsensusGroup::Control,
            outcome: RequestOutcome::AmbiguousCommit,
            request: None,
        })?
        .map_err(|error| map_write_error(error, active, ConsensusGroup::Control))?;
    match response.data {
        ApplyResult::Stream(value) => Ok(value),
        ApplyResult::Rejected(error) => Err(error),
        other => Err(DomainError::Storage {
            reason: format!("unexpected catalog apply result {other}"),
        }),
    }
}

async fn linearize(active: &Arc<ActiveCluster>, raft: &DataRaft) -> Result<(), DomainError> {
    let result = tokio::time::timeout(
        OPERATION_TIMEOUT,
        raft.ensure_linearizable(ReadPolicy::ReadIndex),
    )
    .await
    .map_err(|_| DomainError::QuorumUnavailable {
        group: ConsensusGroup::Data,
        outcome: RequestOutcome::NotApplicable,
        request: None,
    })?;
    result.map_err(|error| map_read_error(error, active, ConsensusGroup::Data))?;
    Ok(())
}

async fn maintain_group_retention(
    active: &Arc<ActiveCluster>,
    group: &DataGroup,
    partition: PartitionKey,
) -> Result<RetentionStatus, DomainError> {
    for _ in 0..64 {
        let observation = lease_clock_observation()?;
        if !group
            .reader
            .retention_maintenance_needed(partition, observation.lower_bound())?
        {
            linearize(active, &group.raft).await?;
            return group.reader.retention_status(partition);
        }
        let status = group.reader.retention_status(partition)?;
        let response = tokio::time::timeout(
            OPERATION_TIMEOUT,
            group.raft.client_write(GroupCommand::MaintainRetention {
                partition,
                expected_cursor: status.reclaim_cursor(),
                max_records: 1024,
                max_payload_bytes: 8 * 1024 * 1024,
                clock: observation,
            }),
        )
        .await
        .map_err(|_| DomainError::QuorumUnavailable {
            group: ConsensusGroup::Data,
            outcome: RequestOutcome::AmbiguousCommit,
            request: None,
        })?
        .map_err(|error| map_write_error(error, active, ConsensusGroup::Data))?;
        match response.data {
            ApplyResult::RetentionStatus(next) => {
                if next.reclaim_cursor() == status.reclaim_cursor()
                    && next.logical_floor() != next.reclaim_cursor()
                {
                    return Err(DomainError::Storage {
                        reason: "retention maintenance made no progress".to_owned(),
                    });
                }
            }
            ApplyResult::Rejected(error) => return Err(error),
            other => {
                return Err(DomainError::Storage {
                    reason: format!("unexpected retention maintenance result {other}"),
                });
            }
        }
    }
    Err(DomainError::ResourceLimit {
        resource: "retention_maintenance_steps".to_owned(),
        limit: 64,
    })
}

fn spawn_retention_maintenance(active: Arc<ActiveCluster>) {
    tokio::spawn(async move {
        while !active.maintenance_shutdown.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(250)).await;
            if active.maintenance_shutdown.load(Ordering::Acquire) {
                break;
            }
            for group in active.data.values() {
                if group.raft.metrics().borrow_watched().state != ServerState::Leader {
                    continue;
                }
                let partitions = match group.reader.retention_partitions() {
                    Ok(partitions) => partitions,
                    Err(error) => {
                        eprintln!(
                            "{}",
                            serde_json::json!({
                                "event": "retention_partition_scan_failed",
                                "detail": error.to_string(),
                            })
                        );
                        continue;
                    }
                };
                for partition in partitions {
                    let observation = match lease_clock_observation() {
                        Ok(observation) => observation,
                        Err(error) => {
                            eprintln!(
                                "{}",
                                serde_json::json!({
                                    "event": "retention_clock_unavailable",
                                    "detail": error.to_string(),
                                })
                            );
                            break;
                        }
                    };
                    match group
                        .reader
                        .retention_maintenance_needed(partition, observation.lower_bound())
                    {
                        Ok(true) => {
                            if let Err(error) =
                                maintain_group_retention(&active, group, partition).await
                            {
                                eprintln!(
                                    "{}",
                                    serde_json::json!({
                                        "event": "retention_background_maintenance_failed",
                                        "partition": partition,
                                        "detail": error.to_string(),
                                    })
                                );
                            }
                        }
                        Ok(false) => {}
                        Err(error) => {
                            eprintln!(
                                "{}",
                                serde_json::json!({
                                    "event": "retention_maintenance_probe_failed",
                                    "partition": partition,
                                    "detail": error.to_string(),
                                })
                            );
                        }
                    }
                }
            }
        }
    });
}

fn map_write_error<C>(
    error: RaftError<C, ClientWriteError<C>>,
    active: &Arc<ActiveCluster>,
    group: ConsensusGroup,
) -> DomainError
where
    C: openraft::RaftTypeConfig<NodeId = u64, Node = BasicNode>,
{
    match error {
        RaftError::APIError(ClientWriteError::ForwardToLeader(forward)) => DomainError::NotLeader {
            group,
            leader: leader_hint(active, forward.leader_id, forward.leader_node.as_ref()),
        },
        RaftError::APIError(ClientWriteError::ChangeMembershipError(error)) => {
            DomainError::Storage {
                reason: error.to_string(),
            }
        }
        RaftError::Fatal(error) => raft_fatal(error),
    }
}

fn map_read_error<C>(
    error: RaftError<C, LinearizableReadError<C>>,
    active: &Arc<ActiveCluster>,
    group: ConsensusGroup,
) -> DomainError
where
    C: openraft::RaftTypeConfig<NodeId = u64, Node = BasicNode>,
{
    match error {
        RaftError::APIError(LinearizableReadError::ForwardToLeader(forward)) => {
            DomainError::NotLeader {
                group,
                leader: leader_hint(active, forward.leader_id, forward.leader_node.as_ref()),
            }
        }
        RaftError::APIError(LinearizableReadError::QuorumNotEnough(error)) => {
            eprintln!(
                "{}",
                serde_json::json!({
                    "event": "linearizable_read_quorum_unavailable",
                    "group": group,
                    "detail": error.to_string(),
                })
            );
            DomainError::QuorumUnavailable {
                group,
                outcome: RequestOutcome::NotApplicable,
                request: None,
            }
        }
        RaftError::Fatal(error) => raft_fatal(error),
    }
}

fn leader_hint(
    active: &Arc<ActiveCluster>,
    leader_id: Option<u64>,
    node: Option<&BasicNode>,
) -> Option<LeaderHint> {
    let manifest = active.manifest.try_read().ok()?;
    let ActiveManifest::V2(manifest) = &*manifest else {
        return None;
    };
    let leader_id = leader_id?;
    let member = manifest.formation.member(leader_id)?;
    if node.is_some_and(|node| node.addr == member.peer_uri()) {
        Some(LeaderHint::new(member.node_id(), member.public_uri()))
    } else {
        None
    }
}

fn group_diagnostic<C>(
    group: ConsensusGroup,
    group_id: u64,
    raft: &Raft<C, RocksStateMachine<C>>,
) -> GroupDiagnostic
where
    C: openraft::RaftTypeConfig<D = GroupCommand, R = ApplyResult, NodeId = u64, Node = BasicNode>,
    RocksStateMachine<C>: openraft::storage::RaftStateMachine<C, SnapshotData = Vec<u8>>,
{
    let metrics = raft.metrics().borrow_watched().clone();
    let effective = metrics.membership_config.membership();
    let committed = metrics.committed_membership_config.membership();
    GroupDiagnostic {
        group,
        group_id,
        local_role: role_name(metrics.state).to_owned(),
        current_leader: metrics.current_leader,
        effective_uniform: effective.get_joint_config().len() == 1,
        effective_voters: effective.voter_ids().collect(),
        effective_learners: effective.learner_ids().collect(),
        committed_uniform: committed.get_joint_config().len() == 1,
        committed_voters: committed.voter_ids().collect(),
        committed_learners: committed.learner_ids().collect(),
        last_log_index: metrics.last_log_index,
        local_committed_index: metrics.local_committed.map(|value| value.index),
        cluster_committed_index: metrics.cluster_committed.map(|value| value.index),
        last_applied_index: metrics.last_applied.map(|value| value.index),
        replication: metrics
            .replication
            .unwrap_or_default()
            .into_iter()
            .map(|(target_node_id, matched)| ReplicationDiagnostic {
                target_node_id,
                matched_log_index: matched.map(|value| value.index),
            })
            .collect(),
        snapshot_index: metrics.snapshot.map(|value| value.index),
        purged_index: metrics.purged.map(|value| value.index),
        slot: None,
        cache_budget_bytes: None,
        write_buffer_budget_bytes: None,
    }
}

fn role_name(state: ServerState) -> &'static str {
    match state {
        ServerState::Learner => "learner",
        ServerState::Follower => "follower",
        ServerState::Candidate => "candidate",
        ServerState::Leader => "leader",
        ServerState::Shutdown => "shutdown",
    }
}

fn validate_lifecycle_envelope(
    local: &NodeDescriptor,
    envelope: &wire::PeerEnvelope,
    formation: &FormationSpec,
) -> Result<(), tonic::Status> {
    if envelope.cluster_id != formation.cluster_id.to_string()
        || envelope.target_node_id != local.node_id().get()
        || formation.local(local.node_id()) != Some(local)
        || formation.member(envelope.sender_node_id).is_none()
    {
        return Err(tonic::Status::permission_denied(
            "lifecycle envelope conflicts with the requested topology",
        ));
    }
    Ok(())
}

fn bootstrap_result(spec: &BootstrapSpec) -> Result<BootstrapResult, DomainError> {
    Ok(BootstrapResult::new(
        spec.cluster(),
        spec.stream(),
        spec.stream_name().clone(),
        light_stream_core::GroupId::new(CONTROL_GROUP_ID)?,
        light_stream_core::GroupId::new(DATA_GROUP_ID)?,
    ))
}

fn group_identity(
    cluster: ClusterId,
    group_id: u64,
    kind: GroupKind,
) -> Result<GroupIdentity, DomainError> {
    Ok(GroupIdentity::new(
        cluster,
        light_stream_core::GroupId::new(group_id)?,
        kind,
    ))
}

fn raft_config(cluster_name: String, replicated: bool) -> Result<Arc<Config>, DomainError> {
    let config = Config {
        cluster_name,
        election_timeout_min: 300,
        election_timeout_max: 600,
        heartbeat_interval: 75,
        enable_pre_vote: Some(true),
        snapshot_policy: if replicated {
            SnapshotPolicy::Never
        } else {
            SnapshotPolicy::LogsSinceLast(64)
        },
        max_in_snapshot_log_to_keep: if replicated { u64::MAX } else { 0 },
        ..Default::default()
    };
    config
        .validate()
        .map(Arc::new)
        .map_err(|error| DomainError::Storage {
            reason: error.to_string(),
        })
}

fn group_path(data_dir: &Path, group_id: u64) -> PathBuf {
    data_dir.join("groups").join(group_id.to_string())
}

fn manifest_path(data_dir: &Path) -> PathBuf {
    data_dir.join(ROOT_MANIFEST)
}

fn read_manifest(data_dir: &Path) -> Result<Option<ActiveManifest>, DomainError> {
    let path = manifest_path(data_dir);
    if !path.exists() {
        return Ok(None);
    }
    let bytes = fs::read(&path).map_err(storage_error)?;
    let header: ManifestHeader = serde_json::from_slice(&bytes).map_err(storage_error)?;
    match header.format_version {
        light_stream_storage::STORAGE_FORMAT_VERSION => {
            let manifest: NodeManifestV1 = serde_json::from_slice(&bytes).map_err(storage_error)?;
            manifest.validate()?;
            Ok(Some(ActiveManifest::V1(manifest)))
        }
        NODE_MANIFEST_VERSION => {
            let manifest: NodeManifestV2 = serde_json::from_slice(&bytes).map_err(storage_error)?;
            Ok(Some(ActiveManifest::V2(manifest)))
        }
        version => Err(DomainError::Storage {
            reason: format!("unsupported node manifest version {version}"),
        }),
    }
}

#[derive(Deserialize)]
struct ManifestHeader {
    format_version: u32,
}

fn write_manifest(data_dir: &Path, manifest: &impl Serialize) -> Result<(), DomainError> {
    fs::create_dir_all(data_dir).map_err(storage_error)?;
    let target = manifest_path(data_dir);
    let temporary = data_dir.join("cluster.json.tmp");
    let mut file = File::create(&temporary).map_err(storage_error)?;
    let bytes = serde_json::to_vec_pretty(manifest).map_err(storage_error)?;
    file.write_all(&bytes).map_err(storage_error)?;
    file.write_all(b"\n").map_err(storage_error)?;
    file.sync_all().map_err(storage_error)?;
    fs::rename(&temporary, &target).map_err(storage_error)?;
    File::open(data_dir)
        .and_then(|directory| directory.sync_all())
        .map_err(storage_error)?;
    Ok(())
}

fn has_group_storage(data_dir: &Path) -> Result<bool, DomainError> {
    let groups = data_dir.join("groups");
    if !groups.exists() {
        return Ok(false);
    }
    Ok(groups.read_dir().map_err(storage_error)?.next().is_some())
}

fn validate_group_directories(data_dir: &Path, data_groups: &[GroupId]) -> Result<(), DomainError> {
    let groups = data_dir.join("groups");
    if !groups.exists() {
        return Ok(());
    }
    let mut allowed = data_groups
        .iter()
        .map(|value| value.get().to_string())
        .collect::<BTreeSet<_>>();
    allowed.insert(CONTROL_GROUP_ID.to_string());
    for entry in groups.read_dir().map_err(storage_error)? {
        let name = entry
            .map_err(storage_error)?
            .file_name()
            .to_string_lossy()
            .into_owned();
        if !allowed.contains(&name) {
            return Err(DomainError::Storage {
                reason: format!("unauthorized or excess group directory {name}"),
            });
        }
    }
    Ok(())
}

fn unsupported_claims() -> Vec<String> {
    vec![
        "snapshot_after_purge:UNSUPPORTED_LS06".to_owned(),
        "raft_owned_payload_reclaim:DEFERRED_LS06".to_owned(),
        "secured_mode:UNSUPPORTED_LS08".to_owned(),
        "independent_hosts:BLOCKED".to_owned(),
    ]
}

fn raft_fatal(error: impl std::fmt::Display) -> DomainError {
    DomainError::Storage {
        reason: error.to_string(),
    }
}

fn storage_error(error: impl std::fmt::Display) -> DomainError {
    DomainError::Storage {
        reason: error.to_string(),
    }
}

fn lease_clock_observation() -> Result<ClockObservation, DomainError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| DomainError::LeaseClockUnavailable)?;
    let now = u64::try_from(now.as_millis()).map_err(|_| DomainError::LeaseClockUnavailable)?;
    let skew = u64::try_from(LEASE_CLOCK_SKEW.as_millis())
        .map_err(|_| DomainError::LeaseClockUnavailable)?;
    ClockObservation::new(now.saturating_sub(skew), now.saturating_add(skew))
}

fn internal_status(error: impl std::fmt::Display) -> tonic::Status {
    tonic::Status::internal(error.to_string())
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    fn local(node_id: u64) -> NodeDescriptor {
        NodeDescriptor::new(
            light_stream_core::NodeId::new(node_id).unwrap(),
            format!("http://127.0.0.1:{}", 7100 + node_id),
            format!("http://127.0.0.1:{}", 7200 + node_id),
        )
    }

    #[tokio::test]
    async fn bootstrap_is_explicit_and_idempotent() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/test-data/light-stream-server/bootstrap");
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        let manager = ClusterManager::open(
            path.clone(),
            local(1),
            8,
            PeerRoutes::default(),
            GroupPoolConfig::default(),
            None,
        )
        .await
        .unwrap();
        assert!(manager.identity().await.is_none());
        let spec = BootstrapSpec::new(
            ClusterId::from_uuid(Uuid::new_v4()),
            light_stream_core::StreamId::from_uuid(Uuid::new_v4()),
            light_stream_core::StreamName::parse("bootstrap").unwrap(),
        );
        let first = manager
            .bootstrap(BootstrapCommand::standalone(spec.clone()))
            .await
            .unwrap();
        let second = manager
            .bootstrap(BootstrapCommand::standalone(spec))
            .await
            .unwrap();
        assert_eq!(first, second);
        manager.shutdown().await.unwrap();
        drop(manager);
        let reopened = ClusterManager::open(
            path.clone(),
            local(1),
            8,
            PeerRoutes::default(),
            GroupPoolConfig::default(),
            None,
        )
        .await
        .unwrap();
        assert!(reopened.identity().await.is_some());
        reopened.shutdown().await.unwrap();
        let _ = fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn startup_refuses_group_storage_without_manifest() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/test-data/light-stream-server/orphan-groups");
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(path.join("groups/1")).unwrap();
        let error = ClusterManager::open(
            path.clone(),
            local(1),
            8,
            PeerRoutes::default(),
            GroupPoolConfig::default(),
            None,
        )
        .await
        .err()
        .unwrap();
        assert!(matches!(error, DomainError::Storage { .. }));
        let _ = fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn joining_startup_recreates_only_manifest_authorized_stores() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/test-data/light-stream-server/joining-recovery");
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        let spec = BootstrapSpec::new(
            ClusterId::from_uuid(Uuid::new_v4()),
            light_stream_core::StreamId::from_uuid(Uuid::new_v4()),
            light_stream_core::StreamName::parse("bootstrap").unwrap(),
        );
        let formation = FormationSpec::try_new(
            spec,
            1,
            vec![local(1), local(2), local(3)],
            GroupPoolConfig::default(),
        )
        .unwrap();
        write_manifest(
            &path,
            &NodeManifestV2::new(
                light_stream_core::NodeId::new(2).unwrap(),
                formation,
                PersistedNodeState::Joining,
            ),
        )
        .unwrap();
        let manager = ClusterManager::open(
            path.clone(),
            local(2),
            8,
            PeerRoutes::default(),
            GroupPoolConfig::default(),
            None,
        )
        .await
        .unwrap();
        assert!(path.join("groups/1/rocksdb").is_dir());
        assert!(path.join("groups/2/rocksdb").is_dir());
        assert!(manager.identity().await.is_none());
        manager.shutdown().await.unwrap();
        let _ = fs::remove_dir_all(path);
    }
}
