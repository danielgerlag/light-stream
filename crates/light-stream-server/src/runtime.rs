use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fs::{self, File},
    future::Future,
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Arc, RwLock as StdRwLock, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use light_stream_core::{
    ActiveExport, ActiveExportPhase, AdministrationIntent, AdministrationLifecycle,
    AdministrationOperation, AdministrationRequestId, AmbiguousRequest, BookmarkId, BookmarkName,
    BookmarkPage, BookmarkPageRequest, BootstrapCommand, BootstrapResult, BootstrapSpec,
    BootstrapTopology, CheckpointCasResult, CheckpointKey, CheckpointMutation, ClusterId,
    ClusterTopology, CommittedBookmark, CommittedCheckpoint, CommittedStreamBookmark,
    ConsensusGroup, CreateBookmarkSpec, CreateStreamSpec, DomainError, ExportAbortReason,
    ExportDeadline, ExportFenceObservation, ExportStatus, ExportStatusPhase, FetchPage, GroupId,
    LeaderHint, LeaseRelease, LeaseRenewal, NodeDescriptor, NodeId, NodePhase, OperationalProof,
    PartitionId, PartitionKey, PartitionRoute, ProducerRequestId, ProtectedFetchRequest,
    PublishBatch, PublishReceipt, ReadinessReason, RecordOffset, ReplayLease, ReplayLeaseId,
    ReplayLeaseRequest, RequestOutcome, RetentionRequest, RetentionResult, RetentionStatus,
    SecurityMutation, SecurityPolicy, StreamBookmarkPage, StreamBookmarkPageRequest,
    StreamCursorVector, StreamDescriptor, StreamId, StreamLifecycle, StreamName, WriteReadiness,
};
#[cfg(test)]
use light_stream_core::{ExportId, ExportIntent, MutationRequestId};
use light_stream_storage::{
    ApplyResult, CONTROL_GROUP_ID, ClockObservation, CommittedStateReader, ControlRaftConfig,
    DATA_GROUP_ID, DataRaftConfig, ExportApplyResult, ExportCommand, GroupCommand, GroupIdentity,
    GroupKind, GroupStorageBudget, NoRemoteNetworkFactory, RocksStateMachine, SnapshotArtifact,
    create_control_store, create_data_store, open_control_store, open_control_store_with_topology,
    open_data_store,
};
use openraft::{
    BasicNode, Config, Instant as OpenRaftInstant, Raft, RaftMetrics, ReadPolicy, ServerState,
    SnapshotPolicy,
    errors::{ClientWriteError, LinearizableReadError, RaftError},
    raft::ClientWriteResponse,
    type_config::async_runtime::WatchReceiver,
};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{Mutex, RwLock},
    task::JoinSet,
};

use crate::{
    config::PeerRoutes,
    export::{
        CoordinatorShutdownError, ExportCoordinator, MaterializationError, ProposalPoll,
        ProposalStart, ReadyArtifactError,
    },
    lifecycle::{DrainOutcome, LifecycleController, MutationPermit},
    manifest::{
        FormationSpec, GroupPoolConfig, LEGACY_NODE_MANIFEST_VERSION, LegacyNodeManifestV3,
        NODE_MANIFEST_VERSION, NodeManifestV1, NodeManifestV2, PREVIOUS_NODE_MANIFEST_VERSION,
        PersistedNodeState,
    },
    peer::{self, TonicNetworkFactory, wire},
    publish_scheduler::{
        PublishQueueSnapshot, PublishScheduler, PublishSchedulerConfig, PublishVerificationDelays,
    },
    security::{PeerRecoveryScope, RuntimeSecurityConfig},
    tasks::{StopToken, TaskGroup, TaskGroupError},
};

pub(crate) type ControlRaft = Raft<ControlRaftConfig, RocksStateMachine<ControlRaftConfig>>;
pub(crate) type DataRaft = Raft<DataRaftConfig, RocksStateMachine<DataRaftConfig>>;
type VerificationDelayConfig = (Option<(u64, Duration)>, Option<(u64, Duration)>);

#[derive(Debug, thiserror::Error)]
pub(crate) enum ShutdownPreparationError {
    #[error(transparent)]
    Tasks(#[from] TaskGroupError),
    #[error(transparent)]
    Export(CoordinatorShutdownError),
    #[error("bootstrap did not reach a shutdown-safe point before the drain deadline")]
    BootstrapDeadline,
}

impl ShutdownPreparationError {
    pub(crate) fn teardown_safe(&self) -> bool {
        match self {
            Self::Tasks(error) => error.all_joined(),
            Self::Export(error) => error.teardown_safe(),
            Self::BootstrapDeadline => false,
        }
    }
}

const ROOT_MANIFEST: &str = "cluster.json";
const OPERATION_TIMEOUT: Duration = Duration::from_secs(3);
const READ_INDEX_FAST_PATH_TIMEOUT: Duration = Duration::from_millis(100);
const FORMATION_TIMEOUT: Duration = Duration::from_secs(15);
const SNAPSHOT_OPERATION_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const SNAPSHOT_TRANSFER_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const LEASE_CLOCK_SKEW: Duration = Duration::from_secs(2);
const RAFT_ELECTION_TIMEOUT_MAX_MS: u64 = 600;
const OPERATIONAL_QUORUM_MAX_AGE: Duration = Duration::from_millis(RAFT_ELECTION_TIMEOUT_MAX_MS);
const READINESS_SAMPLE_TIMEOUT: Duration = OPERATIONAL_QUORUM_MAX_AGE;
const MAX_READINESS_PROBES: usize = 4;
const READINESS_PROBE_TIMEOUT: Duration = Duration::from_millis(100);
const READINESS_PROBE_POLICY: ReadinessProbePolicy = ReadinessProbePolicy {
    max_in_flight: MAX_READINESS_PROBES,
    per_probe_timeout: READINESS_PROBE_TIMEOUT,
};
const EXPORT_RECONCILE_INTERVAL: Duration = Duration::from_millis(100);

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
                PersistedNodeState::Retired => "retired",
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
    operational: RwLock<BTreeMap<u64, OperationalProof>>,
    peer_topology: Option<peer::PeerTopology>,
    topology_manifest_dirty: AtomicBool,
    maintenance: Mutex<Option<TaskGroup>>,
    maintenance_unjoined: AtomicBool,
    administration_delay: Option<Duration>,
    publish_leader_hints: Arc<StdRwLock<BTreeMap<u64, LeaderHint>>>,
    security: RuntimeSecurityConfig,
    lifecycle: LifecycleController,
    export: ExportCoordinator,
}

struct DataGroup {
    group_id: GroupId,
    raft: DataRaft,
    reader: CommittedStateReader,
    publisher: PublishScheduler,
    slot: u16,
    budget: GroupStorageBudget,
}

impl ActiveCluster {
    async fn start_maintenance(self: &Arc<Self>, data_dir: PathBuf) -> Result<(), DomainError> {
        let keep_ready =
            self.control_reader
                .active_export()?
                .and_then(|export| match export.phase() {
                    ActiveExportPhase::Available(_) | ActiveExportPhase::Releasing(_) => {
                        Some(export.spec().export())
                    }
                    _ => None,
                });
        self.export.cleanup_spool_files(keep_ready)?;
        let mut tasks = TaskGroup::new();
        let active = Arc::downgrade(self);
        tasks.spawn("topology-sync", move |stop| {
            topology_sync_loop(active, data_dir, stop)
        });

        let active = Arc::downgrade(self);
        tasks.spawn("retention-maintenance", move |stop| {
            retention_maintenance_loop(active, stop)
        });

        let active = Arc::downgrade(self);
        let control = self.control.clone();
        let reader = self.control_reader.clone();
        tasks.spawn("operational-probe-control", move |stop| {
            operational_probe_loop(
                active,
                GroupId::new(CONTROL_GROUP_ID).expect("control group ID is nonzero"),
                control,
                reader,
                stop,
            )
        });
        for group in self.data.values() {
            let active = Arc::downgrade(self);
            let group_id = group.group_id;
            let raft = group.raft.clone();
            let reader = group.reader.clone();
            tasks.spawn(
                format!("operational-probe-{}", group_id.get()),
                move |stop| operational_probe_loop(active, group_id, raft, reader, stop),
            );
        }

        if !matches!(
            &*self.manifest.read().await,
            ActiveManifest::V2(NodeManifestV2 {
                state: PersistedNodeState::Retired,
                ..
            })
        ) {
            let active = Arc::downgrade(self);
            tasks.spawn("administration-reconciler", move |stop| {
                administration_reconciler_loop(active, stop)
            });

            let active = Arc::downgrade(self);
            tasks.spawn("export-reconciler", move |stop| {
                export_reconciler_loop(active, stop)
            });
        }

        let mut maintenance = self.maintenance.lock().await;
        if maintenance.is_some() {
            return Err(DomainError::Storage {
                reason: "cluster maintenance is already running".to_owned(),
            });
        }
        *maintenance = Some(tasks);
        Ok(())
    }

    async fn stop_maintenance(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<(), ShutdownPreparationError> {
        self.export.cancel_materialization().await;
        let tasks = self.maintenance.lock().await.take();
        let task_result = match tasks {
            Some(tasks) => tasks.stop_and_join(deadline).await,
            None => Ok(()),
        };
        if task_result.as_ref().is_err_and(|error| !error.all_joined()) {
            self.maintenance_unjoined.store(true, Ordering::Release);
        }
        let export_result = self.export.stop_and_join(deadline).await;
        task_result.map_err(ShutdownPreparationError::Tasks)?;
        export_result.map_err(ShutdownPreparationError::Export)
    }

    async fn shutdown(&self) -> Result<(), DomainError> {
        let mut failures = Vec::new();
        if let Err(error) = self
            .stop_maintenance(tokio::time::Instant::now() + FORMATION_TIMEOUT)
            .await
        {
            failures.push(error.to_string());
        }
        if self.maintenance_unjoined.load(Ordering::Acquire) {
            return Err(DomainError::Storage {
                reason: "maintenance tasks remained active after abort".to_owned(),
            });
        }
        if let Err(error) = self.control.shutdown().await {
            failures.push(error.to_string());
        }
        for group in self.data.values() {
            group.publisher.shutdown().await;
            if let Err(error) = group.raft.shutdown().await {
                failures.push(error.to_string());
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(DomainError::Storage {
                reason: format!("cluster shutdown failures: {}", failures.join("; ")),
            })
        }
    }
}

#[derive(Clone, Copy)]
struct ReadinessProbePolicy {
    max_in_flight: usize,
    per_probe_timeout: Duration,
}

struct AuthorityProbe {
    order: usize,
    cluster: ClusterId,
    group: GroupId,
    sender: NodeId,
    endpoint: String,
    target: NodeDescriptor,
    security: RuntimeSecurityConfig,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AuthorityProbeOutcome {
    Ready,
    Stale,
    Unsupported,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AuthorityProbeResult {
    order: usize,
    group: GroupId,
    outcome: AuthorityProbeOutcome,
}

fn has_recent_quorum<C>(metrics: &RaftMetrics<C>) -> bool
where
    C: openraft::RaftTypeConfig<NodeId = u64, Term = u64>,
{
    if metrics.state != ServerState::Leader {
        return false;
    }
    let mut voters = metrics.committed_membership_config.membership().voter_ids();
    if voters.next() == Some(metrics.id) && voters.next().is_none() {
        return true;
    }
    metrics.last_quorum_acked.is_some_and(|acknowledged| {
        OpenRaftInstant::elapsed(&acknowledged.into_inner()) <= OPERATIONAL_QUORUM_MAX_AGE
    })
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
    pub publish_queue: Option<PublishQueueSnapshot>,
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

#[cfg(test)]
impl NodeDiagnostic {
    pub(crate) fn empty_for_test() -> Self {
        Self {
            node_id: 1,
            lifecycle: "active".to_owned(),
            peers: Vec::new(),
            groups: Vec::new(),
            data_group_slots: 1,
            data_group_count: 0,
            rocksdb_cache_budget_bytes: 0,
            rocksdb_write_buffer_budget_bytes: 0,
            per_group_cache_bytes: 0,
            per_group_write_buffer_bytes: 0,
            unsupported_claims: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct SnapshotGroupResult {
    pub group_id: u64,
    pub snapshot_index: u64,
    pub purged_index: Option<u64>,
}

pub struct ClusterManager {
    data_dir: PathBuf,
    local: NodeDescriptor,
    receipt_window: usize,
    peer_routes: PeerRoutes,
    group_pool: GroupPoolConfig,
    publish_scheduler: PublishSchedulerConfig,
    verification_delays: VerificationDelayConfig,
    export_limits: light_stream_export::ExportLimits,
    security: RuntimeSecurityConfig,
    lifecycle: LifecycleController,
    active: RwLock<Option<Arc<ActiveCluster>>>,
    bootstrap_lock: Arc<Mutex<()>>,
}

#[derive(Clone)]
pub(crate) struct ClusterManagerConfig {
    pub receipt_window: usize,
    pub peer_routes: PeerRoutes,
    pub group_pool: GroupPoolConfig,
    pub publish_scheduler: PublishSchedulerConfig,
    pub verification_delays: VerificationDelayConfig,
    pub export_limits: light_stream_export::ExportLimits,
    pub security: RuntimeSecurityConfig,
    pub lifecycle: LifecycleController,
}

impl ClusterManager {
    pub(crate) fn runtime_security(&self) -> &RuntimeSecurityConfig {
        &self.security
    }

    pub async fn open(
        data_dir: PathBuf,
        local: NodeDescriptor,
        config: ClusterManagerConfig,
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
            receipt_window: config.receipt_window,
            peer_routes: config.peer_routes,
            group_pool: config.group_pool,
            publish_scheduler: config.publish_scheduler,
            verification_delays: config.verification_delays,
            export_limits: config.export_limits,
            security: config.security,
            lifecycle: config.lifecycle,
            active: RwLock::new(None),
            bootstrap_lock: Arc::new(Mutex::new(())),
        };
        if let Some(manifest) = manifest {
            let active = match manifest {
                ActiveManifest::V1(manifest) => {
                    manager.security.validate_durable_profile(
                        &crate::manifest::DurableSecurityProfile::LocalInsecure,
                    )?;
                    if !manager.peer_routes.is_empty() {
                        return Err(DomainError::InvalidName {
                            kind: "peer route".to_owned(),
                            reason: "standalone manifests have no remote peer targets".to_owned(),
                        });
                    }
                    manager.open_v1(manifest).await?
                }
                ActiveManifest::V2(manifest) => {
                    let security_transition =
                        manager.security.can_transition_from(&manifest.security);
                    if !security_transition {
                        manager
                            .security
                            .validate_durable_profile(&manifest.security)?;
                    }
                    manager.open_v2(manifest, security_transition).await?
                }
            };
            let active = Arc::new(active);
            if let ActiveManifest::V2(manifest) = &*active.manifest.read().await {
                write_manifest(&manager.data_dir, manifest)?;
            }
            active.start_maintenance(manager.data_dir.clone()).await?;
            *manager.active.write().await = Some(active);
        }
        Ok(manager)
    }

    pub async fn bootstrap(
        self: &Arc<Self>,
        command: BootstrapCommand,
    ) -> Result<BootstrapResult, DomainError> {
        self.submit_bootstrap(command, None).await
    }

    pub async fn bootstrap_secured(
        self: &Arc<Self>,
        command: BootstrapCommand,
        policy: SecurityPolicy,
    ) -> Result<BootstrapResult, DomainError> {
        self.submit_bootstrap(command, Some(policy)).await
    }

    async fn submit_bootstrap(
        self: &Arc<Self>,
        command: BootstrapCommand,
        security: Option<SecurityPolicy>,
    ) -> Result<BootstrapResult, DomainError> {
        let guard = self.bootstrap_lock.clone().lock_owned().await;
        let permit = self.lifecycle.try_admit_mutation()?;
        let manager = self.clone();
        tokio::spawn(async move {
            let _guard = guard;
            let _permit = permit;
            manager.bootstrap_accepted(command, security).await
        })
        .await
        .map_err(|error| DomainError::Storage {
            reason: format!("accepted bootstrap task failed: {error}"),
        })?
    }

    async fn bootstrap_accepted(
        &self,
        command: BootstrapCommand,
        security: Option<SecurityPolicy>,
    ) -> Result<BootstrapResult, DomainError> {
        if let Some(policy) = &security {
            self.security.validate_local_peer_policy(policy)?;
        }
        match command.topology() {
            BootstrapTopology::Standalone => {
                if security.is_some() {
                    return Err(DomainError::UnsupportedOperation {
                        operation: "secured standalone bootstrap".to_owned(),
                        available_phase: "LS08 three-voter secured clusters".to_owned(),
                    });
                }
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
                self.security.validate_members(&formation.members)?;
                self.peer_routes
                    .validate_topology(self.local.node_id(), &formation.members)?;
                self.bootstrap_three_voter(formation, security).await
            }
        }
    }

    async fn bootstrap_standalone(
        &self,
        spec: &BootstrapSpec,
    ) -> Result<BootstrapResult, DomainError> {
        if let Some(active) = self.active.read().await.as_ref().cloned() {
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
        active.start_maintenance(self.data_dir.clone()).await?;
        *self.active.write().await = Some(active);
        bootstrap_result(spec)
    }

    async fn bootstrap_three_voter(
        &self,
        formation: FormationSpec,
        security: Option<SecurityPolicy>,
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
            let manifest = NodeManifestV2::new_with_security(
                self.local.node_id(),
                formation.clone(),
                PersistedNodeState::Forming,
                self.security.durable_profile(),
            )?;
            write_manifest(&self.data_dir, &manifest)?;
            let active = Arc::new(self.create_v2(manifest, true).await?);
            active.start_maintenance(self.data_dir.clone()).await?;
            *self.active.write().await = Some(active.clone());
            active
        };

        if active.manifest.read().await.is_application_active() {
            if security.as_ref().is_some_and(|expected| {
                active
                    .control_reader
                    .security_policy()
                    .ok()
                    .flatten()
                    .as_ref()
                    != Some(expected)
            }) {
                return Err(DomainError::BootstrapConflict {
                    reason: "security policy differs from the active cluster".to_owned(),
                });
            }
            self.activate_members(&formation).await?;
            return bootstrap_result(&formation.bootstrap);
        }
        self.form_cluster(&active, &formation, security).await?;
        bootstrap_result(&formation.bootstrap)
    }

    async fn form_cluster(
        &self,
        active: &Arc<ActiveCluster>,
        formation: &FormationSpec,
        security: Option<SecurityPolicy>,
    ) -> Result<(), DomainError> {
        let topology = topology_from_formation(formation)?;
        if prove_active(active, &formation.bootstrap, &topology)
            .await
            .is_ok()
        {
            self.set_active(active).await?;
            return self.activate_members(formation).await;
        }
        for member in &formation.members {
            if member.node_id() != self.local.node_id() {
                peer::prepare_remote(
                    formation,
                    self.local.node_id().get(),
                    member,
                    &self.security,
                )
                .await?;
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
                topology: Some(ClusterTopology::try_new(
                    1,
                    formation.members.clone(),
                    formation.members.iter().map(|member| member.node_id()),
                )?),
                security,
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
        prove_active(active, &formation.bootstrap, &topology).await?;
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
                match peer::activate_remote(
                    formation,
                    self.local.node_id().get(),
                    member,
                    &self.security,
                )
                .await
                {
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
        let _permit = self
            .lifecycle
            .try_admit_mutation()
            .map_err(internal_status)?;
        if let Some(active) = self.active.read().await.as_ref().cloned() {
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
        let manifest = NodeManifestV2::new_with_security(
            self.local.node_id(),
            formation,
            PersistedNodeState::Joining,
            self.security.durable_profile(),
        )
        .map_err(internal_status)?;
        write_manifest(&self.data_dir, &manifest).map_err(internal_status)?;
        let active = Arc::new(
            self.create_v2(manifest, true)
                .await
                .map_err(internal_status)?,
        );
        active
            .start_maintenance(self.data_dir.clone())
            .await
            .map_err(internal_status)?;
        *self.active.write().await = Some(active);
        Ok(())
    }

    pub(crate) async fn prepare_replacement(
        &self,
        envelope: &wire::PeerEnvelope,
        preparation: peer::ReplacementPreparation,
    ) -> Result<(), tonic::Status> {
        validate_replacement_envelope(&self.local, envelope, &preparation)?;
        let nodes = preparation
            .topology
            .authorized_nodes()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        self.peer_routes
            .validate_topology(self.local.node_id(), &nodes)
            .map_err(internal_status)?;
        let _guard = self.bootstrap_lock.lock().await;
        let _permit = self
            .lifecycle
            .try_admit_mutation()
            .map_err(internal_status)?;
        if let Some(active) = self.active.read().await.as_ref().cloned() {
            let mut manifest = active.manifest.write().await;
            return match &mut *manifest {
                ActiveManifest::V2(existing)
                    if existing.formation.cluster_id == preparation.formation.cluster_id =>
                {
                    if preparation.topology.revision() < existing.topology.revision() {
                        return Err(tonic::Status::failed_precondition(
                            "replacement topology is older than the durable topology",
                        ));
                    }
                    existing.topology = preparation.topology.clone();
                    if let Some(peers) = &active.peer_topology {
                        peers.replace(nodes).map_err(internal_status)?;
                    }
                    write_manifest(&self.data_dir, existing).map_err(internal_status)
                }
                _ => Err(tonic::Status::already_exists(
                    "node already belongs to another cluster",
                )),
            };
        }
        if has_group_storage(&self.data_dir).map_err(internal_status)? {
            return Err(tonic::Status::failed_precondition(
                "group storage exists without a matching manifest",
            ));
        }
        let manifest = NodeManifestV2::with_topology_and_security(
            self.local.node_id(),
            preparation.formation,
            preparation.topology,
            PersistedNodeState::Joining,
            self.security.durable_profile(),
        )
        .map_err(internal_status)?;
        write_manifest(&self.data_dir, &manifest).map_err(internal_status)?;
        let active = Arc::new(
            self.create_v2(manifest, true)
                .await
                .map_err(internal_status)?,
        );
        active
            .start_maintenance(self.data_dir.clone())
            .await
            .map_err(internal_status)?;
        *self.active.write().await = Some(active);
        Ok(())
    }

    pub(crate) async fn activate(
        &self,
        envelope: &wire::PeerEnvelope,
        formation: &FormationSpec,
    ) -> Result<(), tonic::Status> {
        validate_lifecycle_envelope(&self.local, envelope, formation)?;
        let _guard = self.bootstrap_lock.lock().await;
        let _permit = self
            .lifecycle
            .try_admit_mutation()
            .map_err(internal_status)?;
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
        let topology = {
            let manifest = active.manifest.read().await;
            let ActiveManifest::V2(manifest) = &*manifest else {
                return Err(tonic::Status::failed_precondition(
                    "replacement activation requires a replicated manifest",
                ));
            };
            manifest.topology.clone()
        };
        prove_active(&active, &formation.bootstrap, &topology)
            .await
            .map_err(|error| tonic::Status::failed_precondition(error.to_string()))?;
        self.set_active(&active).await.map_err(internal_status)
    }

    pub(crate) async fn activate_replacement(
        &self,
        envelope: &wire::PeerEnvelope,
        preparation: &peer::ReplacementPreparation,
    ) -> Result<(), tonic::Status> {
        validate_replacement_envelope(&self.local, envelope, preparation)?;
        let _guard = self.bootstrap_lock.lock().await;
        let _permit = self
            .lifecycle
            .try_admit_mutation()
            .map_err(internal_status)?;
        let active = self
            .active
            .read()
            .await
            .clone()
            .ok_or_else(|| tonic::Status::failed_precondition("node is pristine"))?;
        sync_active_topology(&active, &preparation.topology)
            .await
            .map_err(internal_status)?;
        prove_active(
            &active,
            &preparation.formation.bootstrap,
            &preparation.topology,
        )
        .await
        .map_err(|error| tonic::Status::failed_precondition(error.to_string()))?;
        self.set_active(&active).await.map_err(internal_status)
    }

    pub(crate) async fn retire_replacement(
        &self,
        envelope: &wire::PeerEnvelope,
        retirement: peer::ReplacementRetirement,
    ) -> Result<(), tonic::Status> {
        validate_replacement_envelope(
            &self.local,
            envelope,
            &peer::ReplacementPreparation {
                formation: retirement.formation.clone(),
                topology: retirement.transitional_topology,
            },
        )?;
        let _guard = self.bootstrap_lock.lock().await;
        let _permit = self
            .lifecycle
            .try_admit_mutation()
            .map_err(internal_status)?;
        let active = self
            .active
            .read()
            .await
            .clone()
            .ok_or_else(|| tonic::Status::failed_precondition("node is pristine"))?;
        let mut manifest = active.manifest.write().await;
        let ActiveManifest::V2(manifest) = &mut *manifest else {
            return Err(tonic::Status::failed_precondition(
                "standalone node cannot be retired by replacement",
            ));
        };
        manifest.topology = retirement.final_topology.clone();
        manifest.state = PersistedNodeState::Retired;
        if let Some(peers) = &active.peer_topology {
            peers
                .replace(
                    retirement
                        .final_topology
                        .authorized_nodes()
                        .values()
                        .cloned(),
                )
                .map_err(internal_status)?;
        }
        write_manifest(&self.data_dir, manifest).map_err(internal_status)
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

    pub(crate) async fn peer_write_authority(
        &self,
        envelope: &wire::PeerEnvelope,
    ) -> Result<bool, tonic::Status> {
        let active = self.peer_cluster(envelope, envelope.group_id).await?;
        if active.lifecycle.snapshot().phase != NodePhase::Running {
            return Ok(false);
        }
        if active
            .control_reader
            .active_export()
            .map_err(internal_status)?
            .is_some()
        {
            return Ok(false);
        }
        Self::active_group_write_authority(
            &active,
            self.local.node_id(),
            GroupId::new(envelope.group_id).map_err(internal_status)?,
        )
        .await
        .map_err(internal_status)
    }

    pub(crate) fn snapshot_incoming_directory(&self, group_id: u64) -> PathBuf {
        group_path(&self.data_dir, group_id).join("snapshots/incoming")
    }

    async fn active_group_write_authority(
        active: &ActiveCluster,
        local: NodeId,
        group: GroupId,
    ) -> Result<bool, DomainError> {
        if active.control_reader.active_export()?.is_some() {
            return Ok(false);
        }
        let proof = active.operational.read().await.get(&group.get()).cloned();
        if group.get() == CONTROL_GROUP_ID {
            let metrics = active.control.metrics().borrow_watched().clone();
            return Ok(has_recent_quorum(&metrics)
                && proof.is_some_and(|proof| {
                    metrics.state == ServerState::Leader
                        && proof.matches(
                            local,
                            metrics.current_term,
                            metrics.last_applied.as_ref().map_or(0, |value| value.index),
                        )
                }));
        }
        let data = active
            .data
            .get(&group.get())
            .ok_or_else(|| storage_error(format!("unknown data Raft group {group}")))?;
        let metrics = data.raft.metrics().borrow_watched().clone();
        Ok(has_recent_quorum(&metrics)
            && proof.is_some_and(|proof| {
                metrics.state == ServerState::Leader
                    && proof.matches(
                        local,
                        metrics.current_term,
                        metrics.last_applied.map_or(0, |value| value.index),
                    )
            }))
    }

    pub(crate) fn snapshot_verification_delay(&self, group_id: u64) -> Option<Duration> {
        self.verification_delays
            .0
            .filter(|(configured_group, _)| *configured_group == group_id)
            .map(|(_, delay)| delay)
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
            || NodeId::new(envelope.sender_node_id)
                .ok()
                .and_then(|node| manifest.topology.node(node))
                .is_none()
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
        let permit = self.lifecycle.try_admit_mutation()?;
        let active = self.application_cluster().await?;
        validate_cluster(&active, cluster).await?;
        let descriptor = control_write(
            &active,
            GroupCommand::CreateStreamIntent {
                spec,
                stream_id: StreamId::from_uuid(uuid::Uuid::new_v4()),
            },
            &permit,
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
                ActiveManifest::V2(value) => value
                    .topology
                    .desired_voters()
                    .iter()
                    .map(|node| node.get())
                    .collect(),
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
                &permit,
            )
            .await?;
        }
        control_write(
            &active,
            GroupCommand::ActivateStream {
                stream_id: descriptor.stream(),
            },
            &permit,
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
        let permit = self.lifecycle.try_admit_mutation()?;
        let active = self.application_cluster().await?;
        validate_cluster(&active, cluster).await?;
        control_write(
            &active,
            GroupCommand::BeginDeleteStream { stream_id },
            &permit,
        )
        .await?;
        control_write(
            &active,
            GroupCommand::FinishDeleteStream { stream_id },
            &permit,
        )
        .await
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
        let permit = self.lifecycle.try_admit_mutation()?;
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
        require_operational_leader(
            &active,
            group.group_id,
            &group.raft,
            &group.reader,
            RequestOutcome::DefiniteNoCommit,
            Some(AmbiguousRequest::Publish {
                request: request.clone(),
            }),
        )
        .await?;
        group.publisher.try_admit(batch, permit)?.wait().await
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
        linearize(&active, group).await?;
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
        linearize(&active, group).await?;
        group.reader.receipt(partition, request)
    }

    pub async fn checkpoint(
        &self,
        key: CheckpointKey,
        route_group_id: Option<GroupId>,
        route_revision: Option<u64>,
    ) -> Result<CommittedCheckpoint, DomainError> {
        let active = self.application_cluster().await?;
        let route = resolve_data_route(
            &active,
            key.cluster(),
            key.partition(),
            route_group_id,
            route_revision,
        )
        .await?;
        let group = active
            .data
            .get(&route.group().get())
            .ok_or(DomainError::StaleRoute)?;
        linearize(&active, group).await?;
        group.reader.checkpoint(&key)
    }

    pub async fn compare_and_set_checkpoint(
        &self,
        mutation: CheckpointMutation,
        route_group_id: Option<GroupId>,
        route_revision: Option<u64>,
    ) -> Result<CheckpointCasResult, DomainError> {
        let permit = self.lifecycle.try_admit_mutation()?;
        let request = mutation.request().clone();
        let active = self.application_cluster().await?;
        let route = resolve_data_route(
            &active,
            mutation.key().cluster(),
            mutation.key().partition(),
            route_group_id,
            route_revision,
        )
        .await?;
        let group = active
            .data
            .get(&route.group().get())
            .ok_or(DomainError::StaleRoute)?;
        require_operational_leader(
            &active,
            group.group_id,
            &group.raft,
            &group.reader,
            RequestOutcome::DefiniteNoCommit,
            Some(AmbiguousRequest::Mutation {
                request: request.clone(),
            }),
        )
        .await?;
        let response = submitted_data_write(
            &active,
            group,
            GroupCommand::CompareAndSetCheckpoint { mutation },
            &permit,
            Some(AmbiguousRequest::Mutation { request }),
        )
        .await?;
        match response.data {
            ApplyResult::Checkpoint(result) => Ok(result),
            ApplyResult::Rejected(error) => Err(error),
            other => Err(DomainError::Storage {
                reason: format!("unexpected checkpoint apply result {other}"),
            }),
        }
    }

    pub async fn create_bookmark(
        &self,
        cluster: ClusterId,
        spec: CreateBookmarkSpec,
        route_group_id: Option<GroupId>,
        route_revision: Option<u64>,
    ) -> Result<CommittedBookmark, DomainError> {
        let permit = self.lifecycle.try_admit_mutation()?;
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
        let response = submitted_data_write(
            &active,
            group,
            GroupCommand::CreateBookmark {
                id: spec.id(),
                partition: spec.partition(),
                name: spec.name().clone(),
                offset: spec.offset(),
            },
            &permit,
            None,
        )
        .await?;
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
        let permit = self.lifecycle.try_admit_mutation()?;
        let active = self.application_cluster().await?;
        let route =
            resolve_data_route(&active, cluster, partition, route_group_id, route_revision).await?;
        let group = active
            .data
            .get(&route.group().get())
            .ok_or(DomainError::StaleRoute)?;
        let response = submitted_data_write(
            &active,
            group,
            GroupCommand::DeleteBookmark { partition, id },
            &permit,
            None,
        )
        .await?;
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
        linearize(&active, group).await?;
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
        linearize(&active, group).await?;
        group.reader.list_bookmarks(request)
    }

    pub async fn advance_retention(
        &self,
        cluster: ClusterId,
        request: RetentionRequest,
        route_group_id: Option<GroupId>,
        route_revision: Option<u64>,
    ) -> Result<RetentionResult, DomainError> {
        let permit = self.lifecycle.try_admit_mutation()?;
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
        maintain_group_retention(&active, group, request.partition(), Some(&permit)).await?;
        let mutation = request.request().clone();
        let response = submitted_data_write(
            &active,
            group,
            GroupCommand::AdvanceRetention {
                request,
                clock: lease_clock_observation()?,
            },
            &permit,
            Some(AmbiguousRequest::Mutation { request: mutation }),
        )
        .await?;
        match response.data {
            ApplyResult::Retention(result) => {
                if let Err(error) =
                    maintain_group_retention(&active, group, result.partition(), Some(&permit))
                        .await
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
        let _permit = self.lifecycle.try_admit_mutation()?;
        let active = self.application_cluster().await?;
        let route =
            resolve_data_route(&active, cluster, partition, route_group_id, route_revision).await?;
        let group = active
            .data
            .get(&route.group().get())
            .ok_or(DomainError::StaleRoute)?;
        maintain_group_retention(&active, group, partition, Some(&_permit)).await
    }

    pub async fn admit_replay_lease(
        &self,
        cluster: ClusterId,
        request: ReplayLeaseRequest,
        route_group_id: Option<GroupId>,
        route_revision: Option<u64>,
    ) -> Result<ReplayLease, DomainError> {
        let permit = self.lifecycle.try_admit_mutation()?;
        let partition = request.range().partition();
        let mutation = request.request().clone();
        let active = self.application_cluster().await?;
        let route =
            resolve_data_route(&active, cluster, partition, route_group_id, route_revision).await?;
        let group = active
            .data
            .get(&route.group().get())
            .ok_or(DomainError::StaleRoute)?;
        maintain_group_retention(&active, group, partition, Some(&permit)).await?;
        let response = submitted_data_write(
            &active,
            group,
            GroupCommand::AdmitReplayLease {
                request,
                clock: lease_clock_observation()?,
            },
            &permit,
            Some(AmbiguousRequest::Mutation { request: mutation }),
        )
        .await?;
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
        let permit = self.lifecycle.try_admit_mutation()?;
        let partition = request.partition();
        let mutation = request.request().clone();
        let active = self.application_cluster().await?;
        let route =
            resolve_data_route(&active, cluster, partition, route_group_id, route_revision).await?;
        let group = active
            .data
            .get(&route.group().get())
            .ok_or(DomainError::StaleRoute)?;
        maintain_group_retention(&active, group, partition, Some(&permit)).await?;
        let response = submitted_data_write(
            &active,
            group,
            GroupCommand::RenewReplayLease {
                request,
                clock: lease_clock_observation()?,
            },
            &permit,
            Some(AmbiguousRequest::Mutation { request: mutation }),
        )
        .await?;
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
        let permit = self.lifecycle.try_admit_mutation()?;
        let partition = request.partition();
        let mutation = request.request().clone();
        let active = self.application_cluster().await?;
        let route =
            resolve_data_route(&active, cluster, partition, route_group_id, route_revision).await?;
        let group = active
            .data
            .get(&route.group().get())
            .ok_or(DomainError::StaleRoute)?;
        let response = submitted_data_write(
            &active,
            group,
            GroupCommand::ReleaseReplayLease {
                request,
                clock: lease_clock_observation()?,
            },
            &permit,
            Some(AmbiguousRequest::Mutation { request: mutation }),
        )
        .await?;
        match response.data {
            ApplyResult::ReplayLease(lease) => {
                if let Err(error) =
                    maintain_group_retention(&active, group, partition, Some(&permit)).await
                {
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
        let permit = self.lifecycle.try_admit_mutation()?;
        let active = self.application_cluster().await?;
        let route =
            resolve_data_route(&active, cluster, partition, route_group_id, route_revision).await?;
        let group = active
            .data
            .get(&route.group().get())
            .ok_or(DomainError::StaleRoute)?;
        maintain_group_retention(&active, group, partition, Some(&permit)).await?;
        group.reader.replay_lease(partition, lease)
    }

    pub async fn fetch_protected(
        &self,
        request: ProtectedFetchRequest,
        route_group_id: Option<GroupId>,
        route_revision: Option<u64>,
    ) -> Result<FetchPage, DomainError> {
        let _permit = self.lifecycle.try_admit_mutation()?;
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
        maintain_group_retention(&active, group, request.partition(), Some(&_permit)).await?;
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
        let permit = self.lifecycle.try_admit_mutation()?;
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
            linearize(&active, group).await?;
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
        let response = submitted_control_write(
            &active,
            GroupCommand::CreateStreamBookmark { id, name, vector },
            &permit,
            None,
        )
        .await?;
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
        let permit = self.lifecycle.try_admit_mutation()?;
        let active = self.application_cluster().await?;
        validate_cluster(&active, cluster).await?;
        let response = submitted_control_write(
            &active,
            GroupCommand::DeleteStreamBookmark { stream_id, id },
            &permit,
            None,
        )
        .await?;
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

    #[cfg(test)]
    pub(crate) async fn begin_export(
        &self,
        intent: ExportIntent,
        deadline: ExportDeadline,
    ) -> Result<ExportStatus, DomainError> {
        let active = self.application_cluster().await?;
        linearize_export_control(&active).await?;
        let request = intent.request().clone();
        export_status_from_apply(
            export_control_write(
                &active,
                GroupCommand::Export(ExportCommand::Begin { intent, deadline }),
                Some(AmbiguousRequest::Mutation { request }),
            )
            .await?,
        )
    }

    #[cfg(test)]
    pub(crate) async fn export_status(
        &self,
        request: &MutationRequestId,
    ) -> Result<Option<ExportStatus>, DomainError> {
        let active = self.application_cluster().await?;
        linearize_export_control(&active).await?;
        if let Some(export) = active.control_reader.active_export()?
            && export.spec().request() == request
        {
            return Ok(Some(ExportStatus::active(&export)));
        }
        Ok(active
            .control_reader
            .export_receipt(request)?
            .map(ExportStatus::Terminal))
    }

    #[cfg(test)]
    pub(crate) async fn open_export_artifact(
        &self,
        export: ExportId,
        expected: light_stream_core::ArtifactIdentity,
    ) -> Result<File, DomainError> {
        let active = self.application_cluster().await?;
        active
            .export
            .open_ready(export, expected)
            .map_err(ready_artifact_domain_error)
    }

    #[cfg(test)]
    pub(crate) async fn request_export_completion(
        &self,
        request: MutationRequestId,
        export: ExportId,
        artifact: light_stream_core::ArtifactIdentity,
    ) -> Result<ExportStatus, DomainError> {
        let active = self.application_cluster().await?;
        linearize_export_control(&active).await?;
        export_status_from_apply(
            export_control_write(
                &active,
                GroupCommand::Export(ExportCommand::RequestCompletion {
                    request: request.clone(),
                    export,
                    artifact,
                }),
                Some(AmbiguousRequest::Mutation { request }),
            )
            .await?,
        )
    }

    #[cfg(test)]
    pub(crate) async fn request_export_abort(
        &self,
        request: MutationRequestId,
        reason: ExportAbortReason,
        observed_clock: ExportDeadline,
    ) -> Result<ExportStatus, DomainError> {
        let active = self.application_cluster().await?;
        linearize_export_control(&active).await?;
        export_status_from_apply(
            export_control_write(
                &active,
                GroupCommand::Export(ExportCommand::RequestAbort {
                    request: request.clone(),
                    reason,
                    observed_clock,
                }),
                Some(AmbiguousRequest::Mutation { request }),
            )
            .await?,
        )
    }

    pub(crate) async fn export_state_snapshot(&self) -> Option<ExportStatusPhase> {
        let active = self.active.read().await.as_ref().cloned()?;
        active
            .control_reader
            .active_export()
            .ok()
            .flatten()
            .map(|export| ExportStatus::active(&export))
            .and_then(|status| match status {
                ExportStatus::Active(status) => Some(status.phase()),
                ExportStatus::Terminal(_) => None,
            })
    }

    #[cfg(test)]
    async fn reconcile_export_once(&self) -> Result<(), DomainError> {
        let active = self.application_cluster().await?;
        reconcile_export_tick(&active, None).await
    }

    pub(crate) async fn write_readiness(&self) -> WriteReadiness {
        tokio::time::timeout(READINESS_SAMPLE_TIMEOUT, self.calculate_write_readiness())
            .await
            .unwrap_or_else(|_| WriteReadiness::NotReady {
                reasons: vec![ReadinessReason::GroupAuthorityStale {
                    group: GroupId::new(CONTROL_GROUP_ID).expect("control group ID is nonzero"),
                }],
            })
    }

    async fn calculate_write_readiness(&self) -> WriteReadiness {
        let Some(active) = self.active.read().await.as_ref().cloned() else {
            return WriteReadiness::NotReady {
                reasons: vec![ReadinessReason::NotBootstrapped],
            };
        };
        let (cluster_id, peers) = {
            let manifest = active.manifest.read().await;
            if !manifest.is_application_active() {
                return WriteReadiness::NotReady {
                    reasons: vec![match manifest.lifecycle() {
                        "retired" => ReadinessReason::Retired,
                        _ => ReadinessReason::Forming,
                    }],
                };
            }
            let cluster_id = manifest.cluster_id();
            let peers = match &*manifest {
                ActiveManifest::V1(_) => {
                    BTreeMap::from([(self.local.node_id().get(), self.local.clone())])
                }
                ActiveManifest::V2(manifest) => manifest
                    .topology
                    .authorized_nodes()
                    .iter()
                    .map(|(node, descriptor)| (node.get(), descriptor.clone()))
                    .collect(),
            };
            (cluster_id, peers)
        };
        match active.control_reader.active_export() {
            Ok(Some(_)) => {
                return WriteReadiness::NotReady {
                    reasons: vec![ReadinessReason::ExportInProgress],
                };
            }
            Ok(None) => {}
            Err(_) => {
                return WriteReadiness::NotReady {
                    reasons: vec![ReadinessReason::StorageFailure],
                };
            }
        }
        if matches!(
            self.security.current_policy(),
            Err(DomainError::SecurityPolicyStale)
        ) {
            return WriteReadiness::NotReady {
                reasons: vec![ReadinessReason::SecurityPolicyStale],
            };
        }
        let operational = active.operational.read().await.clone();
        let control_group = GroupId::new(CONTROL_GROUP_ID).expect("control group ID is nonzero");
        let control_metrics = active.control.metrics().borrow_watched().clone();
        let mut groups = vec![(
            control_group,
            control_metrics.current_leader,
            control_metrics.current_term,
            control_metrics.last_applied.map_or(0, |value| value.index),
            has_recent_quorum(&control_metrics),
        )];
        groups.extend(active.data.values().map(|group| {
            let metrics = group.raft.metrics().borrow_watched().clone();
            (
                group.group_id,
                metrics.current_leader,
                metrics.current_term,
                metrics.last_applied.map_or(0, |value| value.index),
                has_recent_quorum(&metrics),
            )
        }));

        let mut reasons = vec![None; groups.len()];
        let mut probes = Vec::new();
        for (order, (group, leader, term, applied, recent_quorum)) in groups.into_iter().enumerate()
        {
            let Some(leader) = leader else {
                reasons[order] = Some(ReadinessReason::GroupLeaderUnknown { group });
                continue;
            };
            if leader == self.local.node_id().get() {
                if !recent_quorum
                    || !operational
                        .get(&group.get())
                        .is_some_and(|proof| proof.matches(self.local.node_id(), term, applied))
                {
                    reasons[order] = Some(ReadinessReason::GroupAuthorityStale { group });
                }
                continue;
            }
            let Some(target) = peers.get(&leader) else {
                reasons[order] = Some(ReadinessReason::GroupAuthorityStale { group });
                continue;
            };
            probes.push(AuthorityProbe {
                order,
                cluster: cluster_id,
                group,
                sender: self.local.node_id(),
                endpoint: readiness_probe_endpoint(&self.peer_routes, leader, target),
                target: target.clone(),
                security: self.security.clone(),
            });
        }

        for result in sample_remote_authority(probes, READINESS_PROBE_POLICY).await {
            reasons[result.order] = match result.outcome {
                AuthorityProbeOutcome::Ready => None,
                AuthorityProbeOutcome::Stale => Some(ReadinessReason::GroupAuthorityStale {
                    group: result.group,
                }),
                AuthorityProbeOutcome::Unsupported => Some(ReadinessReason::ProbeUnsupported {
                    group: result.group,
                }),
            };
        }
        let reasons = reasons.into_iter().flatten().collect::<Vec<_>>();
        if reasons.is_empty() {
            WriteReadiness::Ready
        } else {
            WriteReadiness::NotReady { reasons }
        }
    }

    pub(crate) fn peer_recovery_scope(&self, group_id: u64) -> PeerRecoveryScope {
        if group_id == CONTROL_GROUP_ID {
            PeerRecoveryScope::ControlGroup
        } else {
            PeerRecoveryScope::None
        }
    }

    pub async fn security_policy(&self) -> Result<Option<SecurityPolicy>, DomainError> {
        let active = self.active.read().await;
        match active.as_ref() {
            Some(active) => active.control_reader.security_policy(),
            None => Ok(None),
        }
    }

    pub async fn confirmed_security_policy(&self) -> Result<SecurityPolicy, DomainError> {
        let active = self.application_cluster().await?;
        linearize_control(&active).await?;
        active
            .control_reader
            .security_policy()?
            .ok_or(DomainError::SecurityPolicyStale)
    }

    pub async fn apply_security_mutation(
        &self,
        mutation: SecurityMutation,
    ) -> Result<SecurityPolicy, DomainError> {
        let permit = self.lifecycle.try_admit_mutation()?;
        let request = mutation.request().clone();
        let active = self.application_cluster().await?;
        let response = submitted_control_write(
            &active,
            GroupCommand::ApplySecurityMutation { mutation },
            &permit,
            Some(AmbiguousRequest::Mutation { request }),
        )
        .await?;
        match response.data {
            ApplyResult::SecurityPolicy(policy) => Ok(policy),
            ApplyResult::Rejected(error) => Err(error),
            other => Err(DomainError::Storage {
                reason: format!("unexpected security policy apply result {other}"),
            }),
        }
    }

    pub async fn activate_secured_transport(
        &self,
        cluster: ClusterId,
        request: light_stream_core::MutationRequestId,
        expected_topology_revision: u64,
        nodes: Vec<NodeDescriptor>,
        policy: SecurityPolicy,
    ) -> Result<SecurityPolicy, DomainError> {
        let permit = self.lifecycle.try_admit_mutation()?;
        let active = self.application_cluster().await?;
        linearize_control(&active).await?;
        validate_cluster(&active, cluster).await?;
        let current =
            active
                .control_reader
                .cluster_topology()?
                .ok_or_else(|| DomainError::Storage {
                    reason: "control topology is missing".to_owned(),
                })?;
        if current.revision() != expected_topology_revision {
            return Err(DomainError::SecurityPolicyConflict);
        }
        let topology = ClusterTopology::try_new(
            expected_topology_revision.saturating_add(1),
            nodes,
            current.desired_voters().iter().copied(),
        )?;
        let response = submitted_control_write(
            &active,
            GroupCommand::ActivateSecuredTransport {
                request,
                topology,
                policy,
            },
            &permit,
            None,
        )
        .await?;
        match response.data {
            ApplyResult::SecurityPolicy(policy) => Ok(policy),
            ApplyResult::Rejected(error) => Err(error),
            other => Err(DomainError::Storage {
                reason: format!("unexpected secured transport apply result {other}"),
            }),
        }
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
            ActiveManifest::V2(value) => value
                .topology
                .authorized_nodes()
                .values()
                .cloned()
                .collect(),
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
            diagnostic.publish_queue = Some(value.publisher.snapshot());
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

    pub async fn snapshot_group(
        &self,
        cluster: ClusterId,
        group_id: u64,
        purge: bool,
    ) -> Result<SnapshotGroupResult, DomainError> {
        let _permit = self.lifecycle.try_admit_mutation()?;
        let active = self.application_cluster().await?;
        validate_cluster(&active, cluster).await?;
        match group_id {
            CONTROL_GROUP_ID => {
                active
                    .control
                    .ensure_linearizable(ReadPolicy::ReadIndex)
                    .await
                    .map_err(|error| map_read_error(error, &active, ConsensusGroup::Control))?;
                snapshot_raft_group(&active.control, &active.control_reader, group_id, purge).await
            }
            group_id if group_id >= DATA_GROUP_ID => {
                let group =
                    active
                        .data
                        .get(&group_id)
                        .ok_or_else(|| DomainError::InvalidIdentity {
                            kind: "Raft group".to_owned(),
                            reason: format!("unknown data group {group_id}"),
                        })?;
                group
                    .raft
                    .ensure_linearizable(ReadPolicy::ReadIndex)
                    .await
                    .map_err(|error| map_read_error(error, &active, ConsensusGroup::Data))?;
                snapshot_raft_group(&group.raft, &group.reader, group_id, purge).await
            }
            _ => Err(DomainError::InvalidIdentity {
                kind: "Raft group".to_owned(),
                reason: format!("unknown group {group_id}"),
            }),
        }
    }

    pub async fn begin_administration(
        &self,
        cluster: ClusterId,
        intent: AdministrationIntent,
    ) -> Result<AdministrationOperation, DomainError> {
        let permit = self.lifecycle.try_admit_mutation()?;
        let active = self.application_cluster().await?;
        validate_cluster(&active, cluster).await?;
        control_administration_write(
            &active,
            GroupCommand::BeginAdministration { intent },
            Some(&permit),
        )
        .await
    }

    pub async fn administration_operation(
        &self,
        cluster: ClusterId,
        request: AdministrationRequestId,
    ) -> Result<AdministrationOperation, DomainError> {
        let active = self.application_cluster().await?;
        linearize_control(&active).await?;
        validate_cluster(&active, cluster).await?;
        active
            .control_reader
            .administration_operation(request)?
            .ok_or_else(|| DomainError::InvalidIdentity {
                kind: "administration request".to_owned(),
                reason: "request was not found".to_owned(),
            })
    }

    pub async fn abort_administration(
        &self,
        cluster: ClusterId,
        request: AdministrationRequestId,
    ) -> Result<AdministrationOperation, DomainError> {
        let permit = self.lifecycle.try_admit_mutation()?;
        let active = self.application_cluster().await?;
        validate_cluster(&active, cluster).await?;
        if let Some(operation) = active.control_reader.administration_operation(request)?
            && let AdministrationIntent::ReplaceVoter { remove, add, .. } = operation.intent()
        {
            let topology =
                active
                    .control_reader
                    .cluster_topology()?
                    .ok_or_else(|| DomainError::Storage {
                        reason: "replacement topology is missing".to_owned(),
                    })?;
            let mut original = topology
                .desired_voters()
                .iter()
                .map(|node| node.get())
                .collect::<BTreeSet<_>>();
            original.remove(&add.node_id().get());
            original.insert(remove.get());
            if !all_groups_have_membership(&active, &original) {
                return Err(DomainError::UnsupportedOperation {
                    operation: "abort after membership change".to_owned(),
                    available_phase: "manual recovery".to_owned(),
                });
            }
        }
        control_administration_write(
            &active,
            GroupCommand::AbortAdministration { request },
            Some(&permit),
        )
        .await
    }

    pub async fn shutdown(&self) -> Result<(), DomainError> {
        if let Some(active) = self.active.read().await.as_ref().cloned() {
            active.shutdown().await?;
        }
        Ok(())
    }

    pub(crate) async fn begin_shutdown(
        &self,
        maintenance_timeout: Duration,
        drain_grace: Duration,
    ) -> (Result<(), ShutdownPreparationError>, DrainOutcome) {
        let drain_deadline = tokio::time::Instant::now() + drain_grace;
        let drain = self.lifecycle.start_drain();
        let guard = tokio::time::timeout_at(drain_deadline, self.bootstrap_lock.lock()).await;
        let _guard = match guard {
            Ok(guard) => guard,
            Err(_) => {
                let outcome = self.lifecycle.finish_drain(drain, drain_deadline).await;
                return (Err(ShutdownPreparationError::BootstrapDeadline), outcome);
            }
        };
        let maintenance_deadline =
            (tokio::time::Instant::now() + maintenance_timeout).min(drain_deadline);
        let maintenance = if let Some(active) = self.active.read().await.as_ref().cloned() {
            active.stop_maintenance(maintenance_deadline).await
        } else {
            Ok(())
        };
        let outcome = self.lifecycle.finish_drain(drain, drain_deadline).await;
        (maintenance, outcome)
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
        if let ActiveManifest::V2(manifest) = &*active.manifest.read().await
            && manifest.topology.node(self.local.node_id()).is_none()
        {
            return Err(DomainError::ClusterForming);
        }
        let control_membership = active
            .control
            .metrics()
            .borrow_watched()
            .committed_membership_config
            .membership()
            .voter_ids()
            .collect::<BTreeSet<_>>();
        if !control_membership.contains(&self.local.node_id().get()) {
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
        let publish_leader_hints = Arc::new(StdRwLock::new(leader_hints([&self.local])));
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
            let publisher = PublishScheduler::spawn(
                raft.clone(),
                self.publish_scheduler,
                PublishVerificationDelays {
                    before_submit: self
                        .verification_delays
                        .0
                        .filter(|(delayed_group, _)| *delayed_group == group_id.get())
                        .map(|(_, delay)| delay),
                    after_commit: self
                        .verification_delays
                        .1
                        .filter(|(delayed_group, _)| *delayed_group == group_id.get())
                        .map(|(_, delay)| delay),
                },
                publish_leader_hints.clone(),
            );
            data.insert(
                group_id.get(),
                DataGroup {
                    group_id,
                    raft,
                    reader,
                    publisher,
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
            operational: RwLock::new(BTreeMap::new()),
            peer_topology: None,
            topology_manifest_dirty: AtomicBool::new(false),
            maintenance: Mutex::new(None),
            maintenance_unjoined: AtomicBool::new(false),
            administration_delay: self.verification_delays.0.map(|(_, delay)| delay),
            publish_leader_hints,
            security: self.security.clone(),
            lifecycle: self.lifecycle.clone(),
            export: ExportCoordinator::open(&self.data_dir, self.export_limits)?,
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
        let publish_leader_hints = Arc::new(StdRwLock::new(leader_hints([&self.local])));
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
            let publisher = PublishScheduler::spawn(
                raft.clone(),
                self.publish_scheduler,
                PublishVerificationDelays {
                    before_submit: self
                        .verification_delays
                        .0
                        .filter(|(delayed_group, _)| *delayed_group == group_id.get())
                        .map(|(_, delay)| delay),
                    after_commit: self
                        .verification_delays
                        .1
                        .filter(|(delayed_group, _)| *delayed_group == group_id.get())
                        .map(|(_, delay)| delay),
                },
                publish_leader_hints.clone(),
            );
            data.insert(
                group_id.get(),
                DataGroup {
                    group_id,
                    raft,
                    reader,
                    publisher,
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
                topology: Some(ClusterTopology::try_new(
                    1,
                    [self.local.clone()],
                    [self.local.node_id()],
                )?),
                security: None,
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
            operational: RwLock::new(BTreeMap::new()),
            peer_topology: None,
            topology_manifest_dirty: AtomicBool::new(false),
            maintenance: Mutex::new(None),
            maintenance_unjoined: AtomicBool::new(false),
            administration_delay: self.verification_delays.0.map(|(_, delay)| delay),
            publish_leader_hints,
            security: self.security.clone(),
            lifecycle: self.lifecycle.clone(),
            export: ExportCoordinator::open(&self.data_dir, self.export_limits)?,
        })
    }

    async fn open_v2(
        &self,
        manifest: NodeManifestV2,
        security_transition: bool,
    ) -> Result<ActiveCluster, DomainError> {
        if !security_transition {
            manifest.validate_local(&self.local)?;
        }
        let active = self
            .create_v2(
                manifest.clone(),
                manifest.state != PersistedNodeState::Active,
            )
            .await?;
        if security_transition {
            let policy = active
                .control_reader
                .security_policy()?
                .ok_or(DomainError::SecurityPolicyStale)?;
            if self.security.bootstrap_policy() != Some(&policy) {
                return Err(DomainError::SecurityPolicyConflict);
            }
            self.security.validate_local_peer_policy(&policy)?;
            self.security.renew_policy(policy)?;
            let mut durable = active.manifest.write().await;
            let ActiveManifest::V2(durable) = &mut *durable else {
                return Err(DomainError::Storage {
                    reason: "secured transition requires a replicated manifest".to_owned(),
                });
            };
            durable.security = self.security.durable_profile();
            durable.validate_local(&self.local)?;
        }
        if manifest.state == PersistedNodeState::Active {
            if let crate::manifest::DurableSecurityProfile::Secured {
                minimum_policy_revision,
                ..
            } = manifest.security
            {
                let policy = active
                    .control_reader
                    .security_policy()?
                    .ok_or(DomainError::SecurityPolicyStale)?;
                if policy.revision() < minimum_policy_revision {
                    return Err(DomainError::SecurityPolicyStale);
                }
                self.security.validate_local_peer_policy(&policy)?;
                self.security.renew_policy(policy)?;
            }
            wait_active_local_recovery(&active, &manifest.formation.bootstrap, &manifest.topology)
                .await?;
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
        mut manifest: NodeManifestV2,
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
            open_control_store_with_topology(
                &control_path,
                &control_identity,
                self.receipt_window,
                budget,
                &manifest.topology,
            )
        }
        .map_err(storage_error)?;
        let control_reader = control_handles.reader.clone();
        if let Some(topology) = control_reader.cluster_topology()? {
            manifest.topology = topology;
        }
        let members = manifest
            .topology
            .authorized_nodes()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let publish_leader_hints = Arc::new(StdRwLock::new(leader_hints(members.iter())));
        self.peer_routes
            .validate_topology(self.local.node_id(), &members)?;
        let peer_topology = peer::PeerTopology::new(members);
        let control = Raft::new(
            self.local.node_id().get(),
            raft_config(format!("{}-control", manifest.formation.cluster_id), true)?,
            TonicNetworkFactory::<ControlRaftConfig>::with_topology(
                manifest.formation.cluster_id,
                CONTROL_GROUP_ID,
                self.local.node_id().get(),
                peer_topology.clone(),
                self.peer_routes.clone(),
                self.security.clone(),
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
                TonicNetworkFactory::<DataRaftConfig>::with_topology(
                    manifest.formation.cluster_id,
                    group_id.get(),
                    self.local.node_id().get(),
                    peer_topology.clone(),
                    self.peer_routes.clone(),
                    self.security.clone(),
                ),
                handles.log_store,
                handles.state_machine,
            )
            .await
            .map_err(raft_fatal)?;
            let publisher = PublishScheduler::spawn(
                raft.clone(),
                self.publish_scheduler,
                PublishVerificationDelays {
                    before_submit: self
                        .verification_delays
                        .0
                        .filter(|(delayed_group, _)| *delayed_group == group_id.get())
                        .map(|(_, delay)| delay),
                    after_commit: self
                        .verification_delays
                        .1
                        .filter(|(delayed_group, _)| *delayed_group == group_id.get())
                        .map(|(_, delay)| delay),
                },
                publish_leader_hints.clone(),
            );
            data.insert(
                group_id.get(),
                DataGroup {
                    group_id,
                    raft,
                    reader,
                    publisher,
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
            operational: RwLock::new(BTreeMap::new()),
            peer_topology: Some(peer_topology),
            topology_manifest_dirty: AtomicBool::new(false),
            maintenance: Mutex::new(None),
            maintenance_unjoined: AtomicBool::new(false),
            administration_delay: self.verification_delays.0.map(|(_, delay)| delay),
            publish_leader_hints,
            security: self.security.clone(),
            lifecycle: self.lifecycle.clone(),
            export: ExportCoordinator::open(&self.data_dir, self.export_limits)?,
        })
    }
}

fn readiness_probe_endpoint(
    peer_routes: &PeerRoutes,
    leader: u64,
    target: &NodeDescriptor,
) -> String {
    peer_routes
        .get(leader)
        .unwrap_or_else(|| target.peer_uri())
        .to_owned()
}

async fn sample_remote_authority(
    probes: Vec<AuthorityProbe>,
    policy: ReadinessProbePolicy,
) -> Vec<AuthorityProbeResult> {
    sample_remote_authority_with(probes, policy, |probe, timeout| async move {
        match peer::probe_write_authority_remote(
            probe.cluster,
            probe.group,
            probe.sender,
            &probe.endpoint,
            &probe.target,
            &probe.security,
            timeout,
        )
        .await
        {
            Ok(true) => AuthorityProbeOutcome::Ready,
            Ok(false) => AuthorityProbeOutcome::Stale,
            Err(status) if status.code() == tonic::Code::Unimplemented => {
                AuthorityProbeOutcome::Unsupported
            }
            Err(_) => AuthorityProbeOutcome::Stale,
        }
    })
    .await
}

async fn sample_remote_authority_with<F, Fut>(
    probes: Vec<AuthorityProbe>,
    policy: ReadinessProbePolicy,
    probe: F,
) -> Vec<AuthorityProbeResult>
where
    F: Fn(AuthorityProbe, Duration) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = AuthorityProbeOutcome> + Send + 'static,
{
    let mut results = probes
        .iter()
        .map(|probe| {
            (
                probe.order,
                AuthorityProbeResult {
                    order: probe.order,
                    group: probe.group,
                    outcome: AuthorityProbeOutcome::Stale,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    if policy.max_in_flight == 0 {
        return results.into_values().collect();
    }

    let mut pending = VecDeque::from(probes);
    let mut in_flight = JoinSet::new();
    while in_flight.len() < policy.max_in_flight {
        let Some(next) = pending.pop_front() else {
            break;
        };
        spawn_authority_probe(&mut in_flight, next, policy, probe.clone());
    }
    while let Some(joined) = in_flight.join_next().await {
        if let Ok(result) = joined {
            results.insert(result.order, result);
        }
        if let Some(next) = pending.pop_front() {
            spawn_authority_probe(&mut in_flight, next, policy, probe.clone());
        }
    }
    results.into_values().collect()
}

fn spawn_authority_probe<F, Fut>(
    in_flight: &mut JoinSet<AuthorityProbeResult>,
    authority: AuthorityProbe,
    policy: ReadinessProbePolicy,
    probe: F,
) where
    F: Fn(AuthorityProbe, Duration) -> Fut + Send + 'static,
    Fut: Future<Output = AuthorityProbeOutcome> + Send + 'static,
{
    let order = authority.order;
    let group = authority.group;
    in_flight.spawn(async move {
        let outcome = tokio::time::timeout(
            policy.per_probe_timeout,
            probe(authority, policy.per_probe_timeout),
        )
        .await
        .unwrap_or(AuthorityProbeOutcome::Stale);
        AuthorityProbeResult {
            order,
            group,
            outcome,
        }
    });
}

fn leader_hints<'a>(
    nodes: impl IntoIterator<Item = &'a NodeDescriptor>,
) -> BTreeMap<u64, LeaderHint> {
    nodes
        .into_iter()
        .map(|node| {
            (
                node.node_id().get(),
                LeaderHint::new(node.node_id(), node.public_uri()),
            )
        })
        .collect()
}

fn topology_from_formation(formation: &FormationSpec) -> Result<ClusterTopology, DomainError> {
    ClusterTopology::try_new(
        1,
        formation.members.clone(),
        formation.members.iter().map(|member| member.node_id()),
    )
}

async fn wait_active_local_recovery(
    active: &ActiveCluster,
    bootstrap: &BootstrapSpec,
    topology: &ClusterTopology,
) -> Result<(), DomainError> {
    let deadline = Instant::now() + FORMATION_TIMEOUT;
    loop {
        if active.control_reader.cluster_topology()?.is_none() {
            ensure_control_topology(active, topology).await?;
        }
        let effective_topology = active
            .control_reader
            .cluster_topology()?
            .unwrap_or_else(|| topology.clone());
        sync_active_topology(active, &effective_topology).await?;
        if prove_active(active, bootstrap, &effective_topology)
            .await
            .is_ok()
        {
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

async fn sync_active_topology(
    active: &ActiveCluster,
    topology: &ClusterTopology,
) -> Result<(), DomainError> {
    let mut manifest = active.manifest.write().await;
    let ActiveManifest::V2(manifest) = &mut *manifest else {
        return Ok(());
    };
    if &manifest.topology == topology {
        return Ok(());
    }
    if let Some(peers) = &active.peer_topology {
        peers.replace(topology.authorized_nodes().values().cloned())?;
    }
    *active
        .publish_leader_hints
        .write()
        .map_err(|_| DomainError::Storage {
            reason: "publish leader-hint lock is poisoned".to_owned(),
        })? = leader_hints(topology.authorized_nodes().values());
    manifest.topology = topology.clone();
    active
        .topology_manifest_dirty
        .store(true, Ordering::Release);
    Ok(())
}

async fn topology_sync_loop(active: Weak<ActiveCluster>, data_dir: PathBuf, mut stop: StopToken) {
    loop {
        tokio::select! {
            biased;
            _ = stop.cancelled() => return,
            _ = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
        let Some(active) = active.upgrade() else {
            return;
        };
        let topology = match active.control_reader.cluster_topology() {
            Ok(Some(topology)) => topology,
            Ok(None) => continue,
            Err(error) => {
                eprintln!(
                    "{}",
                    serde_json::json!({
                        "event": "topology_sync_read_failed",
                        "detail": error.to_string(),
                    })
                );
                continue;
            }
        };
        let changed = {
            let manifest = active.manifest.read().await;
            matches!(
                &*manifest,
                ActiveManifest::V2(manifest) if manifest.topology != topology
            )
        };
        if !changed && !active.topology_manifest_dirty.load(Ordering::Acquire) {
            continue;
        }
        if let Err(error) = sync_active_topology(&active, &topology).await {
            eprintln!(
                "{}",
                serde_json::json!({
                    "event": "topology_sync_failed",
                    "detail": error.to_string(),
                })
            );
            continue;
        }
        let manifest = active.manifest.read().await.clone();
        if let ActiveManifest::V2(manifest) = manifest {
            match write_manifest(&data_dir, &manifest) {
                Ok(()) => active
                    .topology_manifest_dirty
                    .store(false, Ordering::Release),
                Err(error) => {
                    eprintln!(
                        "{}",
                        serde_json::json!({
                            "event": "topology_manifest_write_failed",
                            "detail": error.to_string(),
                        })
                    );
                }
            }
        }
    }
}

async fn export_reconciler_loop(active: Weak<ActiveCluster>, mut stop: StopToken) {
    loop {
        tokio::select! {
            biased;
            _ = stop.cancelled() => return,
            _ = tokio::time::sleep(EXPORT_RECONCILE_INTERVAL) => {}
        }
        let Some(active) = active.upgrade() else {
            return;
        };
        if let Err(error) = reconcile_export_tick(&active, Some(&stop)).await {
            eprintln!(
                "{}",
                serde_json::json!({
                    "event": "export_reconciliation_failed",
                    "detail": error.to_string(),
                })
            );
        }
    }
}

async fn reconcile_export_tick(
    active: &Arc<ActiveCluster>,
    stop: Option<&StopToken>,
) -> Result<(), DomainError> {
    let Some(export) = active.control_reader.active_export()? else {
        let materialization = active.export.cancel_and_join_materialization().await;
        active.export.cleanup_spool_files(None)?;
        materialization.map_err(materialization_domain_error)?;
        match active.export.poll_proposal().await? {
            ProposalPoll::Idle => {}
            ProposalPoll::Pending(_) => return Ok(()),
            ProposalPoll::Resolved(result) => match *result {
                Ok(_) => {}
                Err(error) => return Err(error),
            },
        }
        return Ok(());
    };
    if stop_requested(stop) {
        return Ok(());
    }
    if matches!(
        export.phase(),
        ActiveExportPhase::Materializing(_) | ActiveExportPhase::Available(_)
    ) && !has_export_materialization_authority(active).await
    {
        active
            .export
            .cancel_and_join_materialization()
            .await
            .map_err(materialization_domain_error)?;
        return Ok(());
    }
    if !matches!(
        export.phase(),
        ActiveExportPhase::Materializing(_) | ActiveExportPhase::Available(_)
    ) {
        active
            .export
            .cancel_and_join_materialization()
            .await
            .map_err(materialization_domain_error)?;
    }
    match active.export.poll_proposal().await? {
        ProposalPoll::Idle => {}
        ProposalPoll::Pending(_) => return Ok(()),
        ProposalPoll::Resolved(result) => {
            return match *result {
                Ok(_) => Ok(()),
                Err(error) => Err(error),
            };
        }
    }
    if !matches!(
        export.phase(),
        ActiveExportPhase::Releasing(_) | ActiveExportPhase::Aborting(_)
    ) && export_deadline_observation()?.lower_bound_unix_ms()
        >= export.spec().deadline().upper_bound_unix_ms()
    {
        if is_control_leader(active) {
            propose_control_export(
                active,
                stop,
                ExportCommand::RequestAbort {
                    request: export.spec().request().clone(),
                    reason: ExportAbortReason::DeadlineExceeded,
                    observed_clock: export_deadline_observation()?,
                },
            )
            .await?;
        }
        return Ok(());
    }

    match export.phase() {
        ActiveExportPhase::Preparing(preparing) => {
            let missing = export
                .spec()
                .configured_data_groups()
                .iter()
                .filter(|group| !preparing.fenced_groups().contains_key(group))
                .copied()
                .collect::<Vec<_>>();
            if is_control_leader(active) {
                for group_id in &missing {
                    let Some(group) = active.data.get(&group_id.get()) else {
                        continue;
                    };
                    let fence = group.reader.mutation_fence_state()?;
                    let observation = ExportFenceObservation::new(
                        *group_id,
                        fence.through_epoch(),
                        fence.held().copied(),
                    )?;
                    if observation
                        .held()
                        .is_some_and(|held| held.token() == export.spec().token())
                    {
                        propose_control_export(
                            active,
                            stop,
                            ExportCommand::RecordFence {
                                token: export.spec().token(),
                                observation,
                            },
                        )
                        .await?;
                        break;
                    }
                }
            }
            for group_id in missing {
                let Some(group) = active.data.get(&group_id.get()) else {
                    continue;
                };
                let fence = group.reader.mutation_fence_state()?;
                if fence
                    .held()
                    .is_some_and(|held| held.token() == export.spec().token())
                {
                    continue;
                }
                if is_data_leader(group) {
                    propose_data_export(
                        active,
                        group,
                        stop,
                        ExportCommand::AcquireFence {
                            token: export.spec().token(),
                        },
                    )
                    .await?;
                }
            }
        }
        ActiveExportPhase::Frozen(_) => {
            if is_control_leader(active) {
                propose_control_export(
                    active,
                    stop,
                    ExportCommand::BeginMaterialization {
                        token: export.spec().token(),
                    },
                )
                .await?;
            }
        }
        ActiveExportPhase::Materializing(cut) => {
            if !has_export_materialization_authority(active).await {
                active
                    .export
                    .cancel_and_join_materialization()
                    .await
                    .map_err(materialization_domain_error)?;
                return Ok(());
            }
            let export_id = export.spec().export();
            let artifact = if active.export.owns_materialization(export_id).await {
                let Some(artifact) = poll_export_materialization(active, stop, &export).await?
                else {
                    return Ok(());
                };
                artifact
            } else {
                match active.export.recover_ready(export_id, None) {
                    Ok(Some(artifact)) => artifact,
                    Ok(None) => {
                        let Some(artifact) =
                            poll_export_materialization(active, stop, &export).await?
                        else {
                            return Ok(());
                        };
                        artifact
                    }
                    Err(ReadyArtifactError::Retryable(detail)) => {
                        return Err(storage_error(detail));
                    }
                    Err(ReadyArtifactError::Deterministic(detail)) => {
                        eprintln!(
                            "{}",
                            serde_json::json!({
                                "event": "export_materialization_invalid",
                                "detail": detail,
                            })
                        );
                        request_materialization_abort(active, stop, &export).await?;
                        return Ok(());
                    }
                    Err(ReadyArtifactError::Missing) => {
                        unreachable!("recovery maps missing to none")
                    }
                }
            };
            if stop_requested(stop) {
                return Ok(());
            }
            if export_deadline_expired(&export)? {
                request_deadline_abort(active, stop, &export).await?;
                return Ok(());
            }
            if !has_export_materialization_authority(active).await || stop_requested(stop) {
                active
                    .export
                    .cancel_and_join_materialization()
                    .await
                    .map_err(materialization_domain_error)?;
                return Ok(());
            }
            if export_deadline_expired(&export)? {
                request_deadline_abort(active, stop, &export).await?;
                return Ok(());
            }
            propose_control_export(
                active,
                stop,
                ExportCommand::PublishArtifact {
                    token: export.spec().token(),
                    artifact,
                    cut: cut.clone(),
                },
            )
            .await?;
        }
        ActiveExportPhase::Available(available) => {
            if !has_export_materialization_authority(active).await {
                active
                    .export
                    .cancel_and_join_materialization()
                    .await
                    .map_err(materialization_domain_error)?;
                return Ok(());
            }
            if active
                .export
                .owns_materialization(export.spec().export())
                .await
            {
                match poll_export_materialization(active, stop, &export).await? {
                    Some(artifact) if artifact == available.artifact() => {}
                    Some(_) => {
                        active.export.remove_ready(export.spec().export())?;
                        request_materialization_abort(active, stop, &export).await?;
                        return Ok(());
                    }
                    None => {
                        if !has_export_materialization_authority(active).await {
                            active
                                .export
                                .cancel_and_join_materialization()
                                .await
                                .map_err(materialization_domain_error)?;
                        }
                        return Ok(());
                    }
                }
            }
            match active
                .export
                .recover_ready(export.spec().export(), Some(available.artifact()))
            {
                Ok(Some(_)) => {}
                Ok(None) => {
                    if !has_export_materialization_authority(active).await {
                        active
                            .export
                            .cancel_and_join_materialization()
                            .await
                            .map_err(materialization_domain_error)?;
                        return Ok(());
                    }
                    match poll_export_materialization(active, stop, &export).await? {
                        Some(artifact) if artifact == available.artifact() => {
                            if export_deadline_expired(&export)? {
                                request_deadline_abort(active, stop, &export).await?;
                            }
                        }
                        Some(_) => {
                            eprintln!(
                                "{}",
                                serde_json::json!({
                                    "event": "export_rebuild_mismatch",
                                })
                            );
                            active.export.remove_ready(export.spec().export())?;
                            request_materialization_abort(active, stop, &export).await?;
                        }
                        None => {}
                    }
                }
                Err(ReadyArtifactError::Retryable(detail)) => {
                    return Err(storage_error(detail));
                }
                Err(ReadyArtifactError::Deterministic(detail)) => {
                    eprintln!(
                        "{}",
                        serde_json::json!({
                            "event": "export_artifact_invalid",
                            "detail": detail,
                        })
                    );
                    if is_control_leader(active) {
                        request_materialization_abort(active, stop, &export).await?;
                    } else {
                        return Err(storage_error(detail));
                    }
                }
                Err(ReadyArtifactError::Missing) => unreachable!("recovery maps missing to none"),
            }
            if !has_export_materialization_authority(active).await {
                active
                    .export
                    .cancel_and_join_materialization()
                    .await
                    .map_err(materialization_domain_error)?;
            }
        }
        ActiveExportPhase::Releasing(_) | ActiveExportPhase::Aborting(_) => {
            reconcile_export_release(active, &export, stop).await?;
        }
    }
    Ok(())
}

async fn reconcile_export_release(
    active: &Arc<ActiveCluster>,
    export: &ActiveExport,
    stop: Option<&StopToken>,
) -> Result<(), DomainError> {
    if export.is_ready_to_finish() {
        if is_control_leader(active) {
            let result = propose_control_export(
                active,
                stop,
                ExportCommand::Finish {
                    token: export.spec().token(),
                },
            )
            .await?;
            if matches!(
                result,
                ApplyResult::Export(ExportApplyResult::Status(ExportStatus::Terminal(ref receipt)))
                    if receipt.export() == export.spec().export()
                        && receipt.request() == export.spec().request()
            ) {
                active.export.remove_ready(export.spec().export())?;
            }
        }
        return Ok(());
    }
    for group_id in export.spec().configured_data_groups() {
        let Some(group) = active.data.get(&group_id.get()) else {
            continue;
        };
        let fence = group.reader.mutation_fence_state()?;
        let release_needed = fence
            .held()
            .is_some_and(|held| held.token() == export.spec().token())
            || (fence.held().is_none() && fence.through_epoch() < export.spec().epoch().get());
        if release_needed && is_data_leader(group) {
            propose_data_export(
                active,
                group,
                stop,
                ExportCommand::ReleaseFence {
                    token: export.spec().token(),
                },
            )
            .await?;
        }
    }
    if is_control_leader(active)
        && let Some(group_id) = active
            .export
            .next_release_candidate(
                export.spec().export(),
                export.spec().configured_data_groups(),
            )
            .await
        && let Some(group) = active.data.get(&group_id.get())
    {
        let fence = group.reader.mutation_fence_state()?;
        if fence.held().is_none() && fence.through_epoch() >= export.spec().epoch().get() {
            let observation = ExportFenceObservation::new(group_id, fence.through_epoch(), None)?;
            propose_control_export(
                active,
                stop,
                ExportCommand::RecordRelease {
                    token: export.spec().token(),
                    observation,
                },
            )
            .await?;
        }
    }
    Ok(())
}

fn stop_requested(stop: Option<&StopToken>) -> bool {
    stop.is_some_and(StopToken::is_stopping)
}

fn materialization_domain_error(error: MaterializationError) -> DomainError {
    match error {
        MaterializationError::Cancelled => storage_error("export materialization cancelled"),
        MaterializationError::Retryable(detail)
        | MaterializationError::Limit(detail)
        | MaterializationError::Deterministic(detail) => storage_error(detail),
    }
}

fn is_control_leader(active: &ActiveCluster) -> bool {
    active.control.metrics().borrow_watched().state == ServerState::Leader
}

fn is_data_leader(group: &DataGroup) -> bool {
    group.raft.metrics().borrow_watched().state == ServerState::Leader
}

async fn has_export_materialization_authority(active: &ActiveCluster) -> bool {
    let metrics = active.control.metrics().borrow_watched().clone();
    let proof = active
        .operational
        .read()
        .await
        .get(&CONTROL_GROUP_ID)
        .cloned();
    has_materialization_authority(
        &metrics,
        proof,
        NodeId::new(metrics.id).expect("Raft node ID is nonzero"),
    )
}

fn has_materialization_authority<C>(
    metrics: &RaftMetrics<C>,
    proof: Option<OperationalProof>,
    local: NodeId,
) -> bool
where
    C: openraft::RaftTypeConfig<NodeId = u64, Term = u64>,
{
    has_materialization_authority_evidence(
        has_recent_quorum(metrics),
        proof.is_some_and(|proof| {
            proof.matches(
                local,
                metrics.current_term,
                metrics.last_applied.as_ref().map_or(0, |value| value.index),
            )
        }),
    )
}

fn has_materialization_authority_evidence(recent_quorum: bool, operational: bool) -> bool {
    recent_quorum && operational
}

fn export_deadline_expired(export: &ActiveExport) -> Result<bool, DomainError> {
    Ok(export_deadline_observation()?.lower_bound_unix_ms()
        >= export.spec().deadline().upper_bound_unix_ms())
}

async fn request_deadline_abort(
    active: &Arc<ActiveCluster>,
    stop: Option<&StopToken>,
    export: &ActiveExport,
) -> Result<(), DomainError> {
    if is_control_leader(active) {
        propose_control_export(
            active,
            stop,
            ExportCommand::RequestAbort {
                request: export.spec().request().clone(),
                reason: ExportAbortReason::DeadlineExceeded,
                observed_clock: export_deadline_observation()?,
            },
        )
        .await?;
    }
    Ok(())
}

async fn request_materialization_abort(
    active: &Arc<ActiveCluster>,
    stop: Option<&StopToken>,
    export: &ActiveExport,
) -> Result<(), DomainError> {
    if is_control_leader(active) {
        propose_control_export(
            active,
            stop,
            ExportCommand::RequestAbort {
                request: export.spec().request().clone(),
                reason: ExportAbortReason::MaterializationFailed,
                observed_clock: export_deadline_observation()?,
            },
        )
        .await?;
    }
    Ok(())
}

async fn poll_export_materialization(
    active: &Arc<ActiveCluster>,
    stop: Option<&StopToken>,
    export: &ActiveExport,
) -> Result<Option<light_stream_core::ArtifactIdentity>, DomainError> {
    let readers = active
        .data
        .values()
        .map(|group| group.reader.clone())
        .collect();
    match active
        .export
        .poll_materialization(
            export.spec().export(),
            active.control_reader.clone(),
            readers,
        )
        .await
    {
        Ok(artifact) => Ok(artifact),
        Err(MaterializationError::Cancelled) => Ok(None),
        Err(MaterializationError::Retryable(detail)) => Err(storage_error(detail)),
        Err(MaterializationError::Limit(detail) | MaterializationError::Deterministic(detail)) => {
            eprintln!(
                "{}",
                serde_json::json!({
                    "event": "export_materialization_invalid",
                    "detail": detail,
                })
            );
            request_materialization_abort(active, stop, export).await?;
            Ok(None)
        }
    }
}

async fn propose_control_export(
    active: &Arc<ActiveCluster>,
    stop: Option<&StopToken>,
    command: ExportCommand,
) -> Result<ApplyResult, DomainError> {
    if stop_requested(stop) {
        return Err(DomainError::ShuttingDown {
            outcome: RequestOutcome::DefiniteNoCommit,
        });
    }
    export_control_write(active, GroupCommand::Export(command), None).await
}

async fn propose_data_export(
    active: &Arc<ActiveCluster>,
    group: &DataGroup,
    stop: Option<&StopToken>,
    command: ExportCommand,
) -> Result<ApplyResult, DomainError> {
    if stop_requested(stop) {
        return Err(DomainError::ShuttingDown {
            outcome: RequestOutcome::DefiniteNoCommit,
        });
    }
    export_data_write(active, group, GroupCommand::Export(command)).await
}

async fn administration_reconciler_loop(active: Weak<ActiveCluster>, mut stop: StopToken) {
    loop {
        tokio::select! {
            biased;
            _ = stop.cancelled() => return,
            _ = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
        let Some(active) = active.upgrade() else {
            return;
        };
        if matches!(
            &*active.manifest.read().await,
            ActiveManifest::V2(NodeManifestV2 {
                state: PersistedNodeState::Retired,
                ..
            })
        ) {
            return;
        }
        let operation = match active.control_reader.active_administration() {
            Ok(Some(operation)) => operation,
            Ok(_) => continue,
            Err(error) => {
                eprintln!(
                    "{}",
                    serde_json::json!({
                        "event": "administration_read_failed",
                        "detail": error.to_string(),
                    })
                );
                continue;
            }
        };
        if let Some(delay) = active.administration_delay {
            tokio::select! {
                biased;
                _ = stop.cancelled() => return,
                _ = tokio::time::sleep(delay) => {}
            }
        }
        if stop.is_stopping() {
            return;
        }
        if let Err(error) = reconcile_administration(&active, operation).await {
            eprintln!(
                "{}",
                serde_json::json!({
                    "event": "administration_reconcile_failed",
                    "detail": error.to_string(),
                })
            );
        }
    }
}

async fn reconcile_administration(
    active: &Arc<ActiveCluster>,
    operation: AdministrationOperation,
) -> Result<(), DomainError> {
    if matches!(
        operation.lifecycle(),
        AdministrationLifecycle::Aborted { .. }
    ) {
        let topology =
            active
                .control_reader
                .cluster_topology()?
                .ok_or_else(|| DomainError::Storage {
                    reason: "aborted administration topology is missing".to_owned(),
                })?;
        let desired = topology
            .desired_voters()
            .iter()
            .map(|node| node.get())
            .collect::<BTreeSet<_>>();
        reconcile_membership_target(active, &active.control, ConsensusGroup::Control, &desired)
            .await?;
        for group in active.data.values() {
            reconcile_membership_target(active, &group.raft, ConsensusGroup::Data, &desired)
                .await?;
        }
        if all_groups_have_membership(active, &desired)
            && active.control.metrics().borrow_watched().state == ServerState::Leader
        {
            control_administration_write(
                active,
                GroupCommand::FinishAdministrationAbort {
                    request: operation.intent().request(),
                },
                None,
            )
            .await?;
        }
        return Ok(());
    }
    match operation.intent() {
        AdministrationIntent::ReplaceVoter {
            request,
            expected_topology_revision,
            remove,
            add,
        } => {
            let topology =
                active
                    .control_reader
                    .cluster_topology()?
                    .ok_or_else(|| DomainError::Storage {
                        reason: "replacement topology is missing".to_owned(),
                    })?;
            let formation = {
                let manifest = active.manifest.read().await;
                let ActiveManifest::V2(manifest) = &*manifest else {
                    return Err(DomainError::UnsupportedOperation {
                        operation: "voter replacement".to_owned(),
                        available_phase: "LS06 replicated clusters".to_owned(),
                    });
                };
                manifest.formation.clone()
            };
            let control_metrics = active.control.metrics().borrow_watched().clone();
            peer::prepare_replacement_remote(
                &peer::ReplacementPreparation {
                    formation: formation.clone(),
                    topology: topology.clone(),
                },
                control_metrics.id,
                add,
                &active.security,
            )
            .await?;
            let desired = topology
                .desired_voters()
                .iter()
                .map(|node| node.get())
                .collect::<BTreeSet<_>>();
            reconcile_replacement_group(
                active,
                &active.control,
                ConsensusGroup::Control,
                &desired,
                *remove,
                add,
            )
            .await?;
            for group in active.data.values() {
                reconcile_replacement_group(
                    active,
                    &group.raft,
                    ConsensusGroup::Data,
                    &desired,
                    *remove,
                    add,
                )
                .await?;
            }

            if all_groups_have_membership(active, &desired)
                && active.control.metrics().borrow_watched().state == ServerState::Leader
            {
                let sender_node_id = active.control.metrics().borrow_watched().id;
                let final_topology = topology.replacement_complete(
                    *expected_topology_revision,
                    *remove,
                    add.clone(),
                )?;
                if let Some(removed) = topology.node(*remove) {
                    peer::retire_replacement_remote(
                        &peer::ReplacementRetirement {
                            formation: formation.clone(),
                            transitional_topology: topology.clone(),
                            final_topology: final_topology.clone(),
                        },
                        sender_node_id,
                        removed,
                        &active.security,
                    )
                    .await?;
                }
                peer::activate_replacement_remote(
                    &peer::ReplacementPreparation {
                        formation,
                        topology,
                    },
                    sender_node_id,
                    add,
                    &active.security,
                )
                .await?;
                control_administration_write(
                    active,
                    GroupCommand::CompleteAdministration { request: *request },
                    None,
                )
                .await?;
            }
        }
        AdministrationIntent::TransferLeader {
            request,
            group,
            target,
        } => {
            let complete =
                if group.get() == CONTROL_GROUP_ID {
                    reconcile_leader_transfer(&active.control, *target).await?;
                    leader_transfer_is_complete(&active.control, &active.control_reader, *target)?
                } else {
                    let data = active.data.get(&group.get()).ok_or_else(|| {
                        DomainError::InvalidIdentity {
                            kind: "leader transfer group".to_owned(),
                            reason: "group is not hosted by this node".to_owned(),
                        }
                    })?;
                    reconcile_leader_transfer(&data.raft, *target).await?;
                    leader_transfer_is_complete(&data.raft, &data.reader, *target)?
                };
            if complete && active.control.metrics().borrow_watched().state == ServerState::Leader {
                control_administration_write(
                    active,
                    GroupCommand::CompleteAdministration { request: *request },
                    None,
                )
                .await?;
            }
        }
    }
    Ok(())
}

async fn reconcile_membership_target<C>(
    active: &Arc<ActiveCluster>,
    raft: &Raft<C, RocksStateMachine<C>>,
    group: ConsensusGroup,
    desired: &BTreeSet<u64>,
) -> Result<(), DomainError>
where
    C: openraft::RaftTypeConfig<D = GroupCommand, R = ApplyResult, NodeId = u64, Node = BasicNode>,
    RocksStateMachine<C>: openraft::storage::RaftStateMachine<C, SnapshotData = SnapshotArtifact>,
{
    let metrics = raft.metrics().borrow_watched().clone();
    if metrics.state != ServerState::Leader || membership_is_exact(&metrics, desired) {
        return Ok(());
    }
    tokio::time::timeout(
        FORMATION_TIMEOUT,
        raft.change_membership(desired.clone(), false),
    )
    .await
    .map_err(|_| DomainError::QuorumUnavailable {
        group,
        outcome: RequestOutcome::AmbiguousCommit,
        request: None,
    })?
    .map_err(|error| map_write_error(error, active, group))?;
    Ok(())
}

async fn reconcile_replacement_group<C>(
    active: &Arc<ActiveCluster>,
    raft: &Raft<C, RocksStateMachine<C>>,
    group: ConsensusGroup,
    desired: &BTreeSet<u64>,
    remove: NodeId,
    add: &NodeDescriptor,
) -> Result<(), DomainError>
where
    C: openraft::RaftTypeConfig<D = GroupCommand, R = ApplyResult, NodeId = u64, Node = BasicNode>,
    RocksStateMachine<C>: openraft::storage::RaftStateMachine<C, SnapshotData = SnapshotArtifact>,
{
    let metrics = raft.metrics().borrow_watched().clone();
    if metrics.state != ServerState::Leader || membership_is_exact(&metrics, desired) {
        return Ok(());
    }
    if metrics
        .membership_config
        .membership()
        .get_node(&add.node_id().get())
        .is_none()
    {
        tokio::time::timeout(
            FORMATION_TIMEOUT,
            raft.add_learner(add.node_id().get(), BasicNode::new(add.peer_uri()), true),
        )
        .await
        .map_err(|_| DomainError::QuorumUnavailable {
            group,
            outcome: RequestOutcome::AmbiguousCommit,
            request: None,
        })?
        .map_err(|error| map_write_error(error, active, group))?;
        return Ok(());
    }
    if metrics.current_leader == Some(remove.get()) {
        let target = metrics
            .membership_config
            .membership()
            .voter_ids()
            .find(|node| *node != remove.get() && desired.contains(node))
            .ok_or_else(|| DomainError::InvalidRange {
                reason: "replacement has no surviving leadership target".to_owned(),
            })?;
        raft.trigger()
            .transfer_leader(target)
            .await
            .map_err(raft_fatal)?;
        return Ok(());
    }
    tokio::time::timeout(
        FORMATION_TIMEOUT,
        raft.change_membership(desired.clone(), false),
    )
    .await
    .map_err(|_| DomainError::QuorumUnavailable {
        group,
        outcome: RequestOutcome::AmbiguousCommit,
        request: None,
    })?
    .map_err(|error| map_write_error(error, active, group))?;
    Ok(())
}

async fn reconcile_leader_transfer<C>(
    raft: &Raft<C, RocksStateMachine<C>>,
    target: NodeId,
) -> Result<(), DomainError>
where
    C: openraft::RaftTypeConfig<D = GroupCommand, R = ApplyResult, NodeId = u64, Node = BasicNode>,
    RocksStateMachine<C>: openraft::storage::RaftStateMachine<C, SnapshotData = SnapshotArtifact>,
{
    let metrics = raft.metrics().borrow_watched().clone();
    if metrics.current_leader == Some(target.get()) {
        return Ok(());
    }
    if metrics.state == ServerState::Leader {
        raft.trigger()
            .transfer_leader(target.get())
            .await
            .map_err(raft_fatal)?;
    }
    Ok(())
}

fn all_groups_have_membership(active: &ActiveCluster, desired: &BTreeSet<u64>) -> bool {
    membership_is_exact(&active.control.metrics().borrow_watched(), desired)
        && active
            .data
            .values()
            .all(|group| membership_is_exact(&group.raft.metrics().borrow_watched(), desired))
}

fn leader_transfer_is_complete<C>(
    raft: &Raft<C, RocksStateMachine<C>>,
    reader: &CommittedStateReader,
    target: NodeId,
) -> Result<bool, DomainError>
where
    C: openraft::RaftTypeConfig<
            D = GroupCommand,
            R = ApplyResult,
            NodeId = u64,
            Node = BasicNode,
            Term = u64,
        >,
    RocksStateMachine<C>: openraft::storage::RaftStateMachine<C, SnapshotData = SnapshotArtifact>,
{
    let metrics = raft.metrics().borrow_watched().clone();
    let applied = metrics.last_applied.map_or(0, |log| log.index);
    Ok(metrics.current_leader == Some(target.get())
        && reader
            .operational_proof()?
            .is_some_and(|proof| proof.matches(target, metrics.current_term, applied)))
}

async fn ensure_control_topology(
    active: &ActiveCluster,
    topology: &ClusterTopology,
) -> Result<(), DomainError> {
    match active.control_reader.cluster_topology()? {
        Some(stored) if &stored == topology => return Ok(()),
        Some(_) => {
            return Err(DomainError::IdentityMismatch {
                reason: "control topology conflicts with the durable node manifest".to_owned(),
            });
        }
        None => {}
    }
    let metrics = active.control.metrics().borrow_watched().clone();
    if metrics.state != ServerState::Leader || !has_recent_quorum(&metrics) {
        return Ok(());
    }
    let response = active
        .control
        .client_write(GroupCommand::InitializeClusterTopology {
            topology: topology.clone(),
        })
        .await
        .map_err(raft_fatal)?;
    match response.data {
        ApplyResult::Noop => Ok(()),
        ApplyResult::Rejected(error) => Err(error),
        other => Err(DomainError::Storage {
            reason: format!("unexpected topology initialization result {other}"),
        }),
    }
}

async fn initialize_seed_if_pristine<C>(
    raft: &Raft<C, RocksStateMachine<C>>,
    node_id: u64,
    peer_uri: &str,
) -> Result<(), DomainError>
where
    C: openraft::RaftTypeConfig<
            D = GroupCommand,
            R = ApplyResult,
            NodeId = u64,
            Node = BasicNode,
            Term = u64,
        >,
    RocksStateMachine<C>: openraft::storage::RaftStateMachine<C, SnapshotData = SnapshotArtifact>,
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
    C: openraft::RaftTypeConfig<
            D = GroupCommand,
            R = ApplyResult,
            NodeId = u64,
            Node = BasicNode,
            Term = u64,
        >,
    RocksStateMachine<C>: openraft::storage::RaftStateMachine<C, SnapshotData = SnapshotArtifact>,
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
    RocksStateMachine<C>: openraft::storage::RaftStateMachine<C, SnapshotData = SnapshotArtifact>,
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
    RocksStateMachine<C>: openraft::storage::RaftStateMachine<C, SnapshotData = SnapshotArtifact>,
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
    RocksStateMachine<C>: openraft::storage::RaftStateMachine<C, SnapshotData = SnapshotArtifact>,
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
    RocksStateMachine<C>: openraft::storage::RaftStateMachine<C, SnapshotData = SnapshotArtifact>,
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
    RocksStateMachine<C>: openraft::storage::RaftStateMachine<C, SnapshotData = SnapshotArtifact>,
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
    bootstrap: &BootstrapSpec,
    topology: &ClusterTopology,
) -> Result<(), DomainError> {
    validate_bootstrap_readers(&active.control_reader, &active.data, bootstrap)?;
    if active.control_reader.cluster_topology()?.as_ref() != Some(topology) {
        return Err(DomainError::IdentityMismatch {
            reason: "control topology conflicts with the durable node manifest".to_owned(),
        });
    }
    if active
        .control_reader
        .active_administration()?
        .is_some_and(|operation| matches!(operation.lifecycle(), AdministrationLifecycle::Pending))
    {
        if membership_is_authorized(&active.control.metrics().borrow_watched(), topology)
            && active.data.values().all(|group| {
                membership_is_authorized(&group.raft.metrics().borrow_watched(), topology)
            })
        {
            return Ok(());
        }
        return Err(DomainError::ClusterForming);
    }
    let voters = topology
        .desired_voters()
        .iter()
        .map(|node| node.get())
        .collect::<BTreeSet<_>>();
    if !membership_is_exact(&active.control.metrics().borrow_watched(), &voters)
        || active
            .data
            .values()
            .any(|group| !membership_is_exact(&group.raft.metrics().borrow_watched(), &voters))
    {
        return Err(DomainError::ClusterForming);
    }

    fn membership_is_authorized<C>(
        metrics: &openraft::RaftMetrics<C>,
        topology: &ClusterTopology,
    ) -> bool
    where
        C: openraft::RaftTypeConfig<NodeId = u64>,
    {
        let authorized = topology
            .authorized_nodes()
            .keys()
            .map(|node| node.get())
            .collect::<BTreeSet<_>>();
        let effective = metrics.membership_config.membership();
        let committed = metrics.committed_membership_config.membership();
        effective.voter_ids().next().is_some()
            && committed.voter_ids().next().is_some()
            && effective
                .voter_ids()
                .chain(effective.learner_ids())
                .chain(committed.voter_ids())
                .chain(committed.learner_ids())
                .all(|node| authorized.contains(&node))
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
    validate_cluster(active, cluster).await?;
    let local_route = active
        .control_reader
        .route(partition.stream(), partition.partition())?;
    match (supplied_group, supplied_revision) {
        (Some(group), Some(revision))
            if group == local_route.group() && revision == local_route.route_revision() =>
        {
            return Ok(local_route);
        }
        (Some(_), Some(_)) | (Some(_), None) | (None, Some(_)) => {
            return Err(DomainError::StaleRoute);
        }
        (None, None) => {}
    }
    linearize_control(active).await?;
    let route = active
        .control_reader
        .route(partition.stream(), partition.partition())?;
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
    let group = GroupId::new(CONTROL_GROUP_ID).expect("control group ID is nonzero");
    require_operational_leader(
        active,
        group,
        &active.control,
        &active.control_reader,
        RequestOutcome::NotApplicable,
        None,
    )
    .await?;
    prove_linearizable(
        active,
        group,
        ConsensusGroup::Control,
        &active.control,
        &active.control_reader,
    )
    .await
}

#[cfg(test)]
async fn linearize_export_control(active: &Arc<ActiveCluster>) -> Result<(), DomainError> {
    prove_linearizable(
        active,
        GroupId::new(CONTROL_GROUP_ID).expect("control group ID is nonzero"),
        ConsensusGroup::Control,
        &active.control,
        &active.control_reader,
    )
    .await
}

async fn submitted_control_write(
    active: &Arc<ActiveCluster>,
    command: GroupCommand,
    permit: &MutationPermit,
    request: Option<AmbiguousRequest>,
) -> Result<ClientWriteResponse<ControlRaftConfig>, DomainError> {
    let raft = active.control.clone();
    let retained_permit = permit.clone();
    let mut submitted = tokio::spawn(async move {
        let _permit = retained_permit;
        raft.client_write(command).await
    });
    match tokio::time::timeout(OPERATION_TIMEOUT, &mut submitted).await {
        Ok(Ok(result)) => {
            result.map_err(|error| map_write_error(error, active, ConsensusGroup::Control))
        }
        Ok(Err(error)) => Err(DomainError::Storage {
            reason: format!("submitted control mutation task failed: {error}"),
        }),
        Err(_) => Err(DomainError::QuorumUnavailable {
            group: ConsensusGroup::Control,
            outcome: RequestOutcome::AmbiguousCommit,
            request,
        }),
    }
}

async fn export_control_write(
    active: &Arc<ActiveCluster>,
    command: GroupCommand,
    request: Option<AmbiguousRequest>,
) -> Result<ApplyResult, DomainError> {
    let raft = active.control.clone();
    let task_active = active.clone();
    let start = active
        .export
        .start_proposal(request.clone(), async move {
            raft.client_write(command)
                .await
                .map(|response| response.data)
                .map_err(|error| map_write_error(error, &task_active, ConsensusGroup::Control))
        })
        .await?;
    if let ProposalStart::Busy(retained) = start {
        return Err(if retained == request && request.is_some() {
            export_timeout_error(ConsensusGroup::Control, request)
        } else {
            export_busy_error(ConsensusGroup::Control, request)
        });
    }
    match active
        .export
        .wait_proposal_until(tokio::time::Instant::now() + OPERATION_TIMEOUT)
        .await?
    {
        ProposalPoll::Resolved(result) => *result,
        ProposalPoll::Pending(retained_request) => Err(export_timeout_error(
            ConsensusGroup::Control,
            retained_request.or(request),
        )),
        ProposalPoll::Idle => Err(storage_error(
            "submitted export control proposal lost coordinator ownership",
        )),
    }
}

async fn export_data_write(
    active: &Arc<ActiveCluster>,
    group: &DataGroup,
    command: GroupCommand,
) -> Result<ApplyResult, DomainError> {
    let raft = group.raft.clone();
    let task_active = active.clone();
    let start = active
        .export
        .start_proposal(None, async move {
            raft.client_write(command)
                .await
                .map(|response| response.data)
                .map_err(|error| map_write_error(error, &task_active, ConsensusGroup::Data))
        })
        .await?;
    if matches!(start, ProposalStart::Busy(_)) {
        return Err(export_busy_error(ConsensusGroup::Data, None));
    }
    match active
        .export
        .wait_proposal_until(tokio::time::Instant::now() + OPERATION_TIMEOUT)
        .await?
    {
        ProposalPoll::Resolved(result) => *result,
        ProposalPoll::Pending(request) => Err(export_timeout_error(ConsensusGroup::Data, request)),
        ProposalPoll::Idle => Err(storage_error(
            "submitted export data proposal lost coordinator ownership",
        )),
    }
}

fn export_timeout_error(group: ConsensusGroup, request: Option<AmbiguousRequest>) -> DomainError {
    DomainError::QuorumUnavailable {
        group,
        outcome: RequestOutcome::AmbiguousCommit,
        request,
    }
}

fn export_busy_error(group: ConsensusGroup, request: Option<AmbiguousRequest>) -> DomainError {
    DomainError::QuorumUnavailable {
        group,
        outcome: RequestOutcome::DefiniteNoCommit,
        request,
    }
}

#[cfg(test)]
fn export_status_from_apply(result: ApplyResult) -> Result<ExportStatus, DomainError> {
    match result {
        ApplyResult::Export(ExportApplyResult::Status(status)) => Ok(status),
        ApplyResult::Rejected(error) => Err(error),
        other => Err(DomainError::Storage {
            reason: format!("unexpected export apply result {other}"),
        }),
    }
}

#[cfg(test)]
fn ready_artifact_domain_error(error: ReadyArtifactError) -> DomainError {
    let reason = match error {
        ReadyArtifactError::Missing => "local export artifact is missing".to_owned(),
        ReadyArtifactError::Retryable(reason) | ReadyArtifactError::Deterministic(reason) => reason,
    };
    DomainError::Storage { reason }
}

async fn submitted_data_write(
    active: &Arc<ActiveCluster>,
    group: &DataGroup,
    command: GroupCommand,
    permit: &MutationPermit,
    request: Option<AmbiguousRequest>,
) -> Result<ClientWriteResponse<DataRaftConfig>, DomainError> {
    let raft = group.raft.clone();
    let retained_permit = permit.clone();
    let mut submitted = tokio::spawn(async move {
        let _permit = retained_permit;
        raft.client_write(command).await
    });
    match tokio::time::timeout(OPERATION_TIMEOUT, &mut submitted).await {
        Ok(Ok(result)) => {
            result.map_err(|error| map_write_error(error, active, ConsensusGroup::Data))
        }
        Ok(Err(error)) => Err(DomainError::Storage {
            reason: format!("submitted data mutation task failed: {error}"),
        }),
        Err(_) => Err(DomainError::QuorumUnavailable {
            group: ConsensusGroup::Data,
            outcome: RequestOutcome::AmbiguousCommit,
            request,
        }),
    }
}

async fn control_write(
    active: &Arc<ActiveCluster>,
    command: GroupCommand,
    permit: &MutationPermit,
) -> Result<StreamDescriptor, DomainError> {
    require_operational_leader(
        active,
        GroupId::new(CONTROL_GROUP_ID).expect("control group ID is nonzero"),
        &active.control,
        &active.control_reader,
        RequestOutcome::DefiniteNoCommit,
        None,
    )
    .await?;
    let response = submitted_control_write(active, command, permit, None).await?;
    match response.data {
        ApplyResult::Stream(value) => Ok(value),
        ApplyResult::Rejected(error) => Err(error),
        other => Err(DomainError::Storage {
            reason: format!("unexpected catalog apply result {other}"),
        }),
    }
}

async fn control_administration_write(
    active: &Arc<ActiveCluster>,
    command: GroupCommand,
    permit: Option<&MutationPermit>,
) -> Result<AdministrationOperation, DomainError> {
    require_operational_leader(
        active,
        GroupId::new(CONTROL_GROUP_ID).expect("control group ID is nonzero"),
        &active.control,
        &active.control_reader,
        RequestOutcome::DefiniteNoCommit,
        None,
    )
    .await?;
    let response = if let Some(permit) = permit {
        submitted_control_write(active, command, permit, None).await?
    } else {
        tokio::time::timeout(OPERATION_TIMEOUT, active.control.client_write(command))
            .await
            .map_err(|_| DomainError::QuorumUnavailable {
                group: ConsensusGroup::Control,
                outcome: RequestOutcome::AmbiguousCommit,
                request: None,
            })?
            .map_err(|error| map_write_error(error, active, ConsensusGroup::Control))?
    };
    match response.data {
        ApplyResult::Administration(operation) => Ok(operation),
        ApplyResult::Rejected(error) => Err(error),
        other => Err(DomainError::Storage {
            reason: format!("unexpected administration apply result {other}"),
        }),
    }
}

async fn linearize(active: &Arc<ActiveCluster>, group: &DataGroup) -> Result<(), DomainError> {
    require_operational_leader(
        active,
        group.group_id,
        &group.raft,
        &group.reader,
        RequestOutcome::NotApplicable,
        None,
    )
    .await?;
    linearize_data_raw(active, group).await
}

async fn linearize_data_raw(
    active: &Arc<ActiveCluster>,
    group: &DataGroup,
) -> Result<(), DomainError> {
    prove_linearizable(
        active,
        group.group_id,
        ConsensusGroup::Data,
        &group.raft,
        &group.reader,
    )
    .await
}

async fn prove_linearizable<C>(
    active: &Arc<ActiveCluster>,
    group: GroupId,
    group_kind: ConsensusGroup,
    raft: &Raft<C, RocksStateMachine<C>>,
    reader: &CommittedStateReader,
) -> Result<(), DomainError>
where
    C: openraft::RaftTypeConfig<
            D = GroupCommand,
            R = ApplyResult,
            NodeId = u64,
            Node = BasicNode,
            Term = u64,
        >,
    RocksStateMachine<C>: openraft::storage::RaftStateMachine<C, SnapshotData = SnapshotArtifact>,
{
    match tokio::time::timeout(
        READ_INDEX_FAST_PATH_TIMEOUT,
        raft.ensure_linearizable(ReadPolicy::ReadIndex),
    )
    .await
    {
        Ok(Ok(_)) => return Ok(()),
        Ok(Err(error)) if raft.metrics().borrow_watched().state != ServerState::Leader => {
            return Err(map_read_error(error, active, group_kind));
        }
        Ok(Err(error)) => {
            eprintln!(
                "{}",
                serde_json::json!({
                    "event": "read_index_fallback",
                    "group_id": group,
                    "detail": error.to_string(),
                })
            );
        }
        Err(error) => {
            eprintln!(
                "{}",
                serde_json::json!({
                    "event": "read_index_fallback",
                    "group_id": group,
                    "detail": error.to_string(),
                })
            );
        }
    }
    let response = match tokio::time::timeout(
        OPERATION_TIMEOUT,
        raft.client_write(GroupCommand::OperationalProbe { group }),
    )
    .await
    {
        Ok(Ok(response)) => response,
        Ok(Err(error)) => {
            eprintln!(
                "{}",
                serde_json::json!({
                    "event": "linearization_barrier_failed",
                    "group_id": group,
                    "detail": error.to_string(),
                })
            );
            return Err(map_write_error(error, active, group_kind));
        }
        Err(error) => {
            eprintln!(
                "{}",
                serde_json::json!({
                    "event": "linearization_barrier_failed",
                    "group_id": group,
                    "detail": error.to_string(),
                })
            );
            return Err(DomainError::QuorumUnavailable {
                group: group_kind,
                outcome: RequestOutcome::NotApplicable,
                request: None,
            });
        }
    };
    let ApplyResult::OperationalProof(proof) = response.data else {
        return Err(DomainError::Storage {
            reason: "linearization barrier returned an unexpected result".to_owned(),
        });
    };
    let metrics = raft.metrics().borrow_watched().clone();
    let leader = NodeId::new(metrics.id)?;
    let applied = metrics.last_applied.map_or(0, |value| value.index);
    if metrics.state != ServerState::Leader
        || !proof.matches(leader, metrics.current_term, applied)
        || reader.operational_proof().ok().flatten() != Some(proof)
    {
        return Err(DomainError::QuorumUnavailable {
            group: group_kind,
            outcome: RequestOutcome::NotApplicable,
            request: None,
        });
    }
    active.operational.write().await.insert(group.get(), proof);
    Ok(())
}

async fn require_operational_leader<C>(
    active: &Arc<ActiveCluster>,
    group: GroupId,
    raft: &Raft<C, RocksStateMachine<C>>,
    reader: &CommittedStateReader,
    outcome: RequestOutcome,
    request: Option<AmbiguousRequest>,
) -> Result<(), DomainError>
where
    C: openraft::RaftTypeConfig<
            D = GroupCommand,
            R = ApplyResult,
            NodeId = u64,
            Node = BasicNode,
            Term = u64,
        >,
    RocksStateMachine<C>: openraft::storage::RaftStateMachine<C, SnapshotData = SnapshotArtifact>,
{
    let metrics = raft.metrics().borrow_watched().clone();
    if metrics.state != ServerState::Leader {
        return Ok(());
    }
    let leader = NodeId::new(metrics.id)?;
    let applied = metrics.last_applied.map_or(0, |value| value.index);
    if active
        .operational
        .read()
        .await
        .get(&group.get())
        .is_some_and(|proof| proof.matches(leader, metrics.current_term, applied))
    {
        return Ok(());
    }
    if let Ok(Some(proof)) = reader.operational_proof()
        && proof.matches(leader, metrics.current_term, applied)
    {
        active.operational.write().await.insert(group.get(), proof);
        return Ok(());
    }
    let group_kind = if group.get() == CONTROL_GROUP_ID {
        ConsensusGroup::Control
    } else {
        ConsensusGroup::Data
    };
    if prove_linearizable(active, group, group_kind, raft, reader)
        .await
        .is_ok()
    {
        return Ok(());
    }
    Err(DomainError::QuorumUnavailable {
        group: group_kind,
        outcome,
        request,
    })
}

async fn maintain_group_retention(
    active: &Arc<ActiveCluster>,
    group: &DataGroup,
    partition: PartitionKey,
    permit: Option<&MutationPermit>,
) -> Result<RetentionStatus, DomainError> {
    for _ in 0..64 {
        let observation = lease_clock_observation()?;
        if !group
            .reader
            .retention_maintenance_needed(partition, observation.lower_bound())?
        {
            linearize_data_raw(active, group).await?;
            return group.reader.retention_status(partition);
        }
        let status = group.reader.retention_status(partition)?;
        let command = GroupCommand::MaintainRetention {
            partition,
            expected_cursor: status.reclaim_cursor(),
            max_records: 1024,
            max_payload_bytes: 8 * 1024 * 1024,
            clock: observation,
        };
        let response = if let Some(permit) = permit {
            submitted_data_write(active, group, command, permit, None).await?
        } else {
            tokio::time::timeout(OPERATION_TIMEOUT, group.raft.client_write(command))
                .await
                .map_err(|_| DomainError::QuorumUnavailable {
                    group: ConsensusGroup::Data,
                    outcome: RequestOutcome::AmbiguousCommit,
                    request: None,
                })?
                .map_err(|error| map_write_error(error, active, ConsensusGroup::Data))?
        };
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

async fn retention_maintenance_loop(active: Weak<ActiveCluster>, mut stop: StopToken) {
    loop {
        tokio::select! {
            biased;
            _ = stop.cancelled() => return,
            _ = tokio::time::sleep(Duration::from_millis(250)) => {}
        }
        let Some(active) = active.upgrade() else {
            return;
        };
        for group in active.data.values() {
            if stop.is_stopping() {
                return;
            }
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
                if stop.is_stopping() {
                    return;
                }
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
                        if stop.is_stopping() {
                            return;
                        }
                        if let Err(error) =
                            maintain_group_retention(&active, group, partition, None).await
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
}

async fn operational_probe_loop<C>(
    active: Weak<ActiveCluster>,
    group: GroupId,
    raft: Raft<C, RocksStateMachine<C>>,
    reader: CommittedStateReader,
    mut stop: StopToken,
) where
    C: openraft::RaftTypeConfig<
            D = GroupCommand,
            R = ApplyResult,
            NodeId = u64,
            Node = BasicNode,
            Term = u64,
        > + 'static,
    RocksStateMachine<C>:
        openraft::storage::RaftStateMachine<C, SnapshotData = SnapshotArtifact> + 'static,
{
    loop {
        tokio::select! {
            biased;
            _ = stop.cancelled() => return,
            _ = tokio::time::sleep(Duration::from_millis(25)) => {}
        }
        let Some(active) = active.upgrade() else {
            return;
        };
        if !active.manifest.read().await.is_application_active() {
            continue;
        }
        let metrics = raft.metrics().borrow_watched().clone();
        if metrics.state != ServerState::Leader || !has_recent_quorum(&metrics) {
            active.operational.write().await.remove(&group.get());
            continue;
        }
        let leader = match NodeId::new(metrics.id) {
            Ok(leader) => leader,
            Err(_) => continue,
        };
        let applied = metrics.last_applied.map_or(0, |value| value.index);
        if active
            .operational
            .read()
            .await
            .get(&group.get())
            .is_some_and(|proof| proof.matches(leader, metrics.current_term, applied))
        {
            continue;
        }
        active.operational.write().await.remove(&group.get());
        if let Ok(Some(proof)) = reader.operational_proof()
            && proof.matches(leader, metrics.current_term, applied)
            && matches!(
                tokio::time::timeout(
                    OPERATION_TIMEOUT,
                    raft.ensure_linearizable(ReadPolicy::ReadIndex),
                )
                .await,
                Ok(Ok(_))
            )
        {
            active.operational.write().await.insert(group.get(), proof);
            continue;
        }
        if stop.is_stopping() {
            return;
        }
        let response = tokio::time::timeout(
            OPERATION_TIMEOUT,
            raft.client_write(GroupCommand::OperationalProbe { group }),
        )
        .await;
        let Ok(Ok(response)) = response else {
            continue;
        };
        let ApplyResult::OperationalProof(proof) = response.data else {
            continue;
        };
        if !matches!(
            tokio::time::timeout(
                OPERATION_TIMEOUT,
                raft.ensure_linearizable(ReadPolicy::ReadIndex),
            )
            .await,
            Ok(Ok(_))
        ) {
            continue;
        }
        let metrics = raft.metrics().borrow_watched().clone();
        let applied = metrics.last_applied.map_or(0, |value| value.index);
        let stored = match reader.operational_proof() {
            Ok(Some(stored)) => stored,
            _ => continue,
        };
        if stored == proof
            && proof.matches(leader, metrics.current_term, applied)
            && metrics.state == ServerState::Leader
        {
            active.operational.write().await.insert(group.get(), proof);
        }
    }
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
    let member = manifest.topology.node(NodeId::new(leader_id).ok()?)?;
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
    RocksStateMachine<C>: openraft::storage::RaftStateMachine<C, SnapshotData = SnapshotArtifact>,
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
        publish_queue: None,
    }
}

async fn snapshot_raft_group<C>(
    raft: &Raft<C, RocksStateMachine<C>>,
    reader: &CommittedStateReader,
    group_id: u64,
    purge: bool,
) -> Result<SnapshotGroupResult, DomainError>
where
    C: openraft::RaftTypeConfig<D = GroupCommand, R = ApplyResult, NodeId = u64, Node = BasicNode>,
    RocksStateMachine<C>: openraft::storage::RaftStateMachine<C, SnapshotData = SnapshotArtifact>,
{
    let covered_index = raft
        .metrics()
        .borrow_watched()
        .last_applied
        .as_ref()
        .ok_or_else(|| DomainError::Storage {
            reason: format!("Raft group {group_id} has no applied log to snapshot"),
        })?
        .index;
    raft.trigger().snapshot().await.map_err(raft_fatal)?;
    let deadline = Instant::now() + SNAPSHOT_OPERATION_TIMEOUT;
    let snapshot_index = loop {
        let metrics = raft.metrics().borrow_watched().clone();
        if let Some(snapshot) = metrics.snapshot
            && snapshot.index >= covered_index
        {
            break snapshot.index;
        }
        if Instant::now() >= deadline {
            return Err(DomainError::Storage {
                reason: format!(
                    "timed out waiting for Raft group {group_id} snapshot through index {covered_index}"
                ),
            });
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    if !purge {
        return Ok(SnapshotGroupResult {
            group_id,
            snapshot_index,
            purged_index: None,
        });
    }
    reader
        .snapshot_artifact_bytes()?
        .ok_or_else(|| DomainError::Storage {
            reason: format!("Raft group {group_id} completed without a snapshot artifact"),
        })?;
    raft.trigger()
        .purge_log(snapshot_index)
        .await
        .map_err(raft_fatal)?;
    loop {
        let metrics = raft.metrics().borrow_watched().clone();
        if let Some(purged) = metrics.purged
            && purged.index >= snapshot_index
        {
            return Ok(SnapshotGroupResult {
                group_id,
                snapshot_index,
                purged_index: Some(purged.index),
            });
        }
        if Instant::now() >= deadline {
            return Err(DomainError::Storage {
                reason: format!(
                    "timed out waiting for Raft group {group_id} purge through index {snapshot_index}"
                ),
            });
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
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

fn validate_replacement_envelope(
    local: &NodeDescriptor,
    envelope: &wire::PeerEnvelope,
    preparation: &peer::ReplacementPreparation,
) -> Result<(), tonic::Status> {
    let local_stored = preparation
        .topology
        .node(local.node_id())
        .ok_or_else(|| tonic::Status::permission_denied("target is not authorized"))?;
    let sender = NodeId::new(envelope.sender_node_id)
        .ok()
        .and_then(|node| preparation.topology.node(node));
    if envelope.cluster_id != preparation.formation.cluster_id.to_string()
        || envelope.target_node_id != local.node_id().get()
        || local_stored != local
        || sender.is_none()
    {
        return Err(tonic::Status::permission_denied(
            "replacement envelope conflicts with the durable topology",
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
        election_timeout_max: RAFT_ELECTION_TIMEOUT_MAX_MS,
        heartbeat_interval: 75,
        enable_pre_vote: Some(true),
        snapshot_policy: if replicated {
            SnapshotPolicy::Never
        } else {
            SnapshotPolicy::LogsSinceLast(64)
        },
        max_in_snapshot_log_to_keep: if replicated { u64::MAX } else { 0 },
        install_snapshot_timeout: SNAPSHOT_TRANSFER_TIMEOUT.as_millis() as u64,
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
        PREVIOUS_NODE_MANIFEST_VERSION => {
            let mut manifest: NodeManifestV2 =
                serde_json::from_slice(&bytes).map_err(storage_error)?;
            manifest.format_version = NODE_MANIFEST_VERSION;
            Ok(Some(ActiveManifest::V2(manifest)))
        }
        LEGACY_NODE_MANIFEST_VERSION => {
            let legacy: LegacyNodeManifestV3 =
                serde_json::from_slice(&bytes).map_err(storage_error)?;
            let manifest = NodeManifestV2::from_legacy(legacy)?;
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
    vec!["independent_hosts:BLOCKED".to_owned()]
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

fn export_deadline_observation() -> Result<ExportDeadline, DomainError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| DomainError::LeaseClockUnavailable)?;
    let now = u64::try_from(now.as_millis()).map_err(|_| DomainError::LeaseClockUnavailable)?;
    let skew = u64::try_from(LEASE_CLOCK_SKEW.as_millis())
        .map_err(|_| DomainError::LeaseClockUnavailable)?;
    ExportDeadline::new(now.saturating_sub(skew), now.saturating_add(skew))
}

fn internal_status(error: impl std::fmt::Display) -> tonic::Status {
    tonic::Status::internal(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::{
        io::Read,
        sync::atomic::{AtomicUsize, Ordering as AtomicOrdering},
    };

    use uuid::Uuid;

    use super::*;

    struct ActiveProbe(Arc<AtomicUsize>);

    impl Drop for ActiveProbe {
        fn drop(&mut self) {
            self.0.fetch_sub(1, AtomicOrdering::AcqRel);
        }
    }

    fn local(node_id: u64) -> NodeDescriptor {
        NodeDescriptor::new(
            light_stream_core::NodeId::new(node_id).unwrap(),
            format!("http://127.0.0.1:{}", 7100 + node_id),
            format!("http://127.0.0.1:{}", 7200 + node_id),
        )
    }

    fn manager_config() -> ClusterManagerConfig {
        ClusterManagerConfig {
            receipt_window: 8,
            peer_routes: PeerRoutes::default(),
            group_pool: GroupPoolConfig::default(),
            publish_scheduler: PublishSchedulerConfig::default(),
            verification_delays: (None, None),
            export_limits: light_stream_export::ExportLimits::default(),
            security: RuntimeSecurityConfig::LocalInsecure,
            lifecycle: LifecycleController::starting(),
        }
    }

    fn authority_probe(order: usize, group: u64) -> AuthorityProbe {
        AuthorityProbe {
            order,
            cluster: ClusterId::from_uuid(Uuid::new_v4()),
            group: GroupId::new(group).unwrap(),
            sender: NodeId::new(1).unwrap(),
            endpoint: local(2).peer_uri().to_owned(),
            target: local(2),
            security: RuntimeSecurityConfig::LocalInsecure,
        }
    }

    fn export_request(sequence: u64) -> MutationRequestId {
        MutationRequestId::new(
            light_stream_core::PrincipalId::parse("export-test").unwrap(),
            light_stream_core::MutationSessionId::from_uuid(Uuid::from_u128(7)),
            light_stream_core::RequestSequence::new(sequence),
        )
    }

    fn future_export_deadline() -> ExportDeadline {
        let now = export_deadline_observation().unwrap().upper_bound_unix_ms();
        ExportDeadline::new(now + 60_000, now + 60_000).unwrap()
    }

    async fn stopped_standalone_export_manager(
        name: &str,
    ) -> (Arc<ClusterManager>, PathBuf, BootstrapSpec) {
        stopped_standalone_export_manager_with_config(name, manager_config()).await
    }

    async fn stopped_standalone_export_manager_with_config(
        name: &str,
        config: ClusterManagerConfig,
    ) -> (Arc<ClusterManager>, PathBuf, BootstrapSpec) {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/test-data/light-stream-server")
            .join(name);
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        let manager = Arc::new(
            ClusterManager::open(path.clone(), local(1), config)
                .await
                .unwrap(),
        );
        let spec = BootstrapSpec::new(
            ClusterId::from_uuid(Uuid::new_v4()),
            StreamId::from_uuid(Uuid::new_v4()),
            StreamName::parse("export-source").unwrap(),
        );
        manager
            .bootstrap(BootstrapCommand::standalone(spec.clone()))
            .await
            .unwrap();
        for _ in 0..100 {
            if manager.write_readiness().await.is_ready() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(manager.write_readiness().await.is_ready());
        let active = manager.active.read().await.as_ref().cloned().unwrap();
        if let Some(tasks) = active.maintenance.lock().await.take() {
            tasks
                .stop_and_join(tokio::time::Instant::now() + Duration::from_secs(2))
                .await
                .unwrap();
        }
        (manager, path, spec)
    }

    async fn reconcile_until_phase(
        manager: &ClusterManager,
        request: &MutationRequestId,
        expected: ExportStatusPhase,
    ) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if matches!(
                manager.export_status(request).await.unwrap(),
                Some(ExportStatus::Active(status)) if status.phase() == expected
            ) {
                return;
            }
            assert!(tokio::time::Instant::now() < deadline);
            manager.reconcile_export_once().await.unwrap();
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    async fn reconcile_until_terminal(
        manager: &ClusterManager,
        request: &MutationRequestId,
        disposition: light_stream_core::ExportTerminalDisposition,
    ) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if matches!(
                manager.export_status(request).await.unwrap(),
                Some(ExportStatus::Terminal(receipt))
                    if receipt.disposition() == disposition
            ) {
                return;
            }
            assert!(tokio::time::Instant::now() < deadline);
            manager.reconcile_export_once().await.unwrap();
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    #[tokio::test]
    async fn standalone_export_progresses_rebuilds_and_releases() {
        let (manager, path, spec) =
            stopped_standalone_export_manager("standalone-export-progression").await;
        let request = export_request(1);
        let intent = ExportIntent::new(
            request.clone(),
            spec.cluster(),
            light_stream_core::ExportSelection::try_new([spec.stream()]).unwrap(),
            light_stream_core::ExportFormatVersion::V1,
        );
        let export_id = intent.export_id();

        assert!(matches!(
            manager
                .begin_export(intent, future_export_deadline())
                .await
                .unwrap(),
            ExportStatus::Active(status) if status.phase() == ExportStatusPhase::Preparing
        ));
        assert_eq!(
            manager.write_readiness().await,
            WriteReadiness::NotReady {
                reasons: vec![ReadinessReason::ExportInProgress],
            }
        );

        manager.reconcile_export_once().await.unwrap();
        assert!(matches!(
            manager.export_status(&request).await.unwrap(),
            Some(ExportStatus::Active(status))
                if status.phase() == ExportStatusPhase::Preparing
        ));
        reconcile_until_phase(&manager, &request, ExportStatusPhase::Frozen).await;
        manager.reconcile_export_once().await.unwrap();
        assert!(matches!(
            manager.export_status(&request).await.unwrap(),
            Some(ExportStatus::Active(status))
                if status.phase() == ExportStatusPhase::Materializing
        ));
        reconcile_until_phase(&manager, &request, ExportStatusPhase::Available).await;
        let available = manager
            .active
            .read()
            .await
            .as_ref()
            .unwrap()
            .control_reader
            .active_export()
            .unwrap()
            .unwrap();
        let ActiveExportPhase::Available(available) = available.phase() else {
            panic!("expected available export");
        };
        let artifact = available.artifact();

        let mut opened = manager
            .open_export_artifact(export_id, artifact)
            .await
            .unwrap();
        let mut original = Vec::new();
        opened.read_to_end(&mut original).unwrap();
        assert!(
            manager
                .open_export_artifact(
                    export_id,
                    light_stream_core::ArtifactIdentity::new(1, [9; 32]).unwrap(),
                )
                .await
                .is_err()
        );

        let ready = path.join("exports").join(format!("{export_id}.ready"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&ready).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        let active = manager.active.read().await.as_ref().cloned().unwrap();
        assert!(matches!(
            active.export.recover_ready(
                export_id,
                Some(light_stream_core::ArtifactIdentity::new(1, [8; 32]).unwrap()),
            ),
            Ok(None)
        ));
        assert!(!ready.exists());
        let rebuild_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            manager.reconcile_export_once().await.unwrap();
            if fs::read(&ready).is_ok_and(|bytes| bytes == original) {
                break;
            }
            assert!(tokio::time::Instant::now() < rebuild_deadline);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(fs::read(&ready).unwrap(), original);
        manager
            .open_export_artifact(export_id, artifact)
            .await
            .unwrap();

        assert!(matches!(
            manager
                .request_export_completion(request.clone(), export_id, artifact)
                .await
                .unwrap(),
            ExportStatus::Active(status) if status.phase() == ExportStatusPhase::Releasing
        ));
        reconcile_until_terminal(
            &manager,
            &request,
            light_stream_core::ExportTerminalDisposition::Completed,
        )
        .await;
        assert!(!ready.exists());

        let abort_request = export_request(2);
        let abort_intent = ExportIntent::new(
            abort_request.clone(),
            spec.cluster(),
            light_stream_core::ExportSelection::try_new([spec.stream()]).unwrap(),
            light_stream_core::ExportFormatVersion::V1,
        );
        manager
            .begin_export(abort_intent, future_export_deadline())
            .await
            .unwrap();
        assert!(matches!(
            manager
                .request_export_abort(
                    abort_request.clone(),
                    ExportAbortReason::OperatorRequested,
                    export_deadline_observation().unwrap(),
                )
                .await
                .unwrap(),
            ExportStatus::Active(status) if status.phase() == ExportStatusPhase::Aborting
        ));
        reconcile_until_terminal(
            &manager,
            &abort_request,
            light_stream_core::ExportTerminalDisposition::Aborted,
        )
        .await;

        manager.shutdown().await.unwrap();
        let _ = fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn owned_reconciler_progresses_standalone_export_to_available() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/test-data/light-stream-server/owned-export-reconciler");
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        let manager = Arc::new(
            ClusterManager::open(path.clone(), local(1), manager_config())
                .await
                .unwrap(),
        );
        let spec = BootstrapSpec::new(
            ClusterId::from_uuid(Uuid::new_v4()),
            StreamId::from_uuid(Uuid::new_v4()),
            StreamName::parse("owned-export").unwrap(),
        );
        manager
            .bootstrap(BootstrapCommand::standalone(spec.clone()))
            .await
            .unwrap();
        let request = export_request(5);
        manager
            .begin_export(
                ExportIntent::new(
                    request.clone(),
                    spec.cluster(),
                    light_stream_core::ExportSelection::try_new([spec.stream()]).unwrap(),
                    light_stream_core::ExportFormatVersion::V1,
                ),
                future_export_deadline(),
            )
            .await
            .unwrap();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if matches!(
                manager.export_status(&request).await.unwrap(),
                Some(ExportStatus::Active(status))
                    if status.phase() == ExportStatusPhase::Available
            ) {
                break;
            }
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        manager
            .request_export_abort(
                request.clone(),
                ExportAbortReason::OperatorRequested,
                export_deadline_observation().unwrap(),
            )
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if matches!(
                manager.export_status(&request).await.unwrap(),
                Some(ExportStatus::Terminal(receipt))
                    if receipt.disposition()
                        == light_stream_core::ExportTerminalDisposition::Aborted
            ) {
                break;
            }
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        manager.shutdown().await.unwrap();
        let _ = fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn materialization_size_limit_requests_abort() {
        let mut config = manager_config();
        config.export_limits.max_artifact_bytes = 1;
        config.export_limits.max_manifest_bytes = 1;
        config.export_limits.max_section_bytes = 1;
        config.export_limits.max_payload_bytes = 1;
        let (manager, path, spec) =
            stopped_standalone_export_manager_with_config("export-materialization-limit", config)
                .await;
        let request = export_request(6);
        manager
            .begin_export(
                ExportIntent::new(
                    request.clone(),
                    spec.cluster(),
                    light_stream_core::ExportSelection::try_new([spec.stream()]).unwrap(),
                    light_stream_core::ExportFormatVersion::V1,
                ),
                future_export_deadline(),
            )
            .await
            .unwrap();
        reconcile_until_phase(&manager, &request, ExportStatusPhase::Materializing).await;

        reconcile_until_phase(&manager, &request, ExportStatusPhase::Aborting).await;
        reconcile_until_terminal(
            &manager,
            &request,
            light_stream_core::ExportTerminalDisposition::Aborted,
        )
        .await;
        manager.shutdown().await.unwrap();
        let _ = fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn stopping_maintenance_stops_export_progress() {
        let (manager, path, spec) =
            stopped_standalone_export_manager("stopped-export-reconciler").await;
        let request = export_request(3);
        let intent = ExportIntent::new(
            request.clone(),
            spec.cluster(),
            light_stream_core::ExportSelection::try_new([spec.stream()]).unwrap(),
            light_stream_core::ExportFormatVersion::V1,
        );
        manager
            .begin_export(intent, future_export_deadline())
            .await
            .unwrap();

        tokio::time::sleep(EXPORT_RECONCILE_INTERVAL * 3).await;

        assert!(matches!(
            manager.export_status(&request).await.unwrap(),
            Some(ExportStatus::Active(status))
                if status.phase() == ExportStatusPhase::Preparing
        ));
        manager.shutdown().await.unwrap();
        let _ = fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn elapsed_export_deadline_aborts_and_releases() {
        let (manager, path, spec) =
            stopped_standalone_export_manager("expired-export-deadline").await;
        let request = export_request(4);
        let intent = ExportIntent::new(
            request.clone(),
            spec.cluster(),
            light_stream_core::ExportSelection::try_new([spec.stream()]).unwrap(),
            light_stream_core::ExportFormatVersion::V1,
        );
        let now = export_deadline_observation().unwrap().lower_bound_unix_ms();
        manager
            .begin_export(intent, ExportDeadline::new(now - 1, now - 1).unwrap())
            .await
            .unwrap();

        manager.reconcile_export_once().await.unwrap();
        assert!(matches!(
            manager.export_status(&request).await.unwrap(),
            Some(ExportStatus::Active(status)) if status.phase() == ExportStatusPhase::Aborting
        ));
        reconcile_until_terminal(
            &manager,
            &request,
            light_stream_core::ExportTerminalDisposition::Aborted,
        )
        .await;

        manager.shutdown().await.unwrap();
        let _ = fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn completed_materialization_is_not_published_after_deadline() {
        let (manager, path, spec) =
            stopped_standalone_export_manager("post-build-expired-export").await;
        let request = export_request(7);
        let intent = ExportIntent::new(
            request.clone(),
            spec.cluster(),
            light_stream_core::ExportSelection::try_new([spec.stream()]).unwrap(),
            light_stream_core::ExportFormatVersion::V1,
        );
        let export_id = intent.export_id();
        let deadline_at = export_deadline_observation().unwrap().upper_bound_unix_ms() + 500;
        manager
            .begin_export(
                intent,
                ExportDeadline::new(deadline_at, deadline_at).unwrap(),
            )
            .await
            .unwrap();
        reconcile_until_phase(&manager, &request, ExportStatusPhase::Materializing).await;
        manager.reconcile_export_once().await.unwrap();
        let ready = path.join("exports").join(format!("{export_id}.ready"));
        let wait_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while !ready.exists() {
            assert!(tokio::time::Instant::now() < wait_deadline);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let expiry_wait = tokio::time::Instant::now() + Duration::from_secs(6);
        while export_deadline_observation().unwrap().lower_bound_unix_ms() < deadline_at {
            assert!(tokio::time::Instant::now() < expiry_wait);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        manager.reconcile_export_once().await.unwrap();

        assert!(matches!(
            manager.export_status(&request).await.unwrap(),
            Some(ExportStatus::Active(status)) if status.phase() == ExportStatusPhase::Aborting
        ));
        manager.shutdown().await.unwrap();
        let _ = fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn authoritative_probe_is_false_while_export_is_active() {
        let (manager, path, spec) =
            stopped_standalone_export_manager("active-export-authority-probe").await;
        let active = manager.active.read().await.as_ref().cloned().unwrap();
        let request = export_request(8);
        manager
            .begin_export(
                ExportIntent::new(
                    request,
                    spec.cluster(),
                    light_stream_core::ExportSelection::try_new([spec.stream()]).unwrap(),
                    light_stream_core::ExportFormatVersion::V1,
                ),
                future_export_deadline(),
            )
            .await
            .unwrap();

        assert!(
            !ClusterManager::active_group_write_authority(
                &active,
                manager.local.node_id(),
                GroupId::new(CONTROL_GROUP_ID).unwrap(),
            )
            .await
            .unwrap()
        );
        manager.shutdown().await.unwrap();
        let _ = fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn follower_and_stale_leader_do_not_start_materialization() {
        let (manager, path, spec) =
            stopped_standalone_export_manager("export-authority-gate").await;
        let active = manager.active.read().await.as_ref().cloned().unwrap();
        let request = export_request(10);
        let intent = ExportIntent::new(
            request.clone(),
            spec.cluster(),
            light_stream_core::ExportSelection::try_new([spec.stream()]).unwrap(),
            light_stream_core::ExportFormatVersion::V1,
        );
        let export_id = intent.export_id();
        manager
            .begin_export(intent, future_export_deadline())
            .await
            .unwrap();
        reconcile_until_phase(&manager, &request, ExportStatusPhase::Materializing).await;
        let exited = Arc::new(std::sync::atomic::AtomicBool::new(false));
        active
            .export
            .start_test_materialization(export_id, exited.clone())
            .await
            .unwrap();

        let metrics = active.control.metrics().borrow_watched().clone();
        let proof = active
            .operational
            .read()
            .await
            .get(&CONTROL_GROUP_ID)
            .cloned();
        let mut follower = metrics.clone();
        follower.state = ServerState::Follower;
        assert!(!has_materialization_authority(
            &follower,
            proof,
            manager.local.node_id(),
        ));
        assert!(!has_materialization_authority_evidence(false, true));
        active.operational.write().await.remove(&CONTROL_GROUP_ID);
        assert!(!has_export_materialization_authority(&active).await);

        manager.reconcile_export_once().await.unwrap();

        assert!(exited.load(std::sync::atomic::Ordering::Acquire));
        assert!(!active.export.materialization_is_running().await);
        assert!(
            !path
                .join("exports")
                .join(format!("{export_id}.ready"))
                .exists()
        );
        manager.shutdown().await.unwrap();
        let _ = fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn authority_loss_reaps_available_materialization() {
        let (manager, path, spec) =
            stopped_standalone_export_manager("available-export-authority-loss").await;
        let active = manager.active.read().await.as_ref().cloned().unwrap();
        let request = export_request(11);
        let intent = ExportIntent::new(
            request.clone(),
            spec.cluster(),
            light_stream_core::ExportSelection::try_new([spec.stream()]).unwrap(),
            light_stream_core::ExportFormatVersion::V1,
        );
        let export_id = intent.export_id();
        manager
            .begin_export(intent, future_export_deadline())
            .await
            .unwrap();
        reconcile_until_phase(&manager, &request, ExportStatusPhase::Available).await;
        let exited = Arc::new(std::sync::atomic::AtomicBool::new(false));
        active
            .export
            .start_test_materialization(export_id, exited.clone())
            .await
            .unwrap();
        active.operational.write().await.remove(&CONTROL_GROUP_ID);

        manager.reconcile_export_once().await.unwrap();

        assert!(exited.load(std::sync::atomic::Ordering::Acquire));
        assert!(!active.export.materialization_is_running().await);
        manager.shutdown().await.unwrap();
        let _ = fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn no_active_export_reaps_mismatched_task_and_cleans_regular_spool_files() {
        let (manager, path, _) =
            stopped_standalone_export_manager("terminal-export-spool-cleanup").await;
        let active = manager.active.read().await.as_ref().cloned().unwrap();
        let exited = Arc::new(std::sync::atomic::AtomicBool::new(false));
        active
            .export
            .start_test_materialization(ExportId::from_uuid(Uuid::from_u128(0x901)), exited.clone())
            .await
            .unwrap();
        let exports = path.join("exports");
        let stale_ready = exports.join(format!(
            "{}.ready",
            ExportId::from_uuid(Uuid::from_u128(0x902))
        ));
        let stale_building = exports.join(format!(
            "{}.building",
            ExportId::from_uuid(Uuid::from_u128(0x903))
        ));
        fs::write(&stale_ready, b"ready").unwrap();
        fs::write(&stale_building, b"building").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;
            let target = path.join("symlink-target");
            let link = exports.join("ignored.ready");
            fs::write(&target, b"preserve").unwrap();
            symlink(&target, &link).unwrap();
        }

        manager.reconcile_export_once().await.unwrap();

        assert!(exited.load(std::sync::atomic::Ordering::Acquire));
        assert!(!active.export.materialization_is_running().await);
        assert!(!stale_ready.exists());
        assert!(!stale_building.exists());
        #[cfg(unix)]
        assert_eq!(fs::read(path.join("symlink-target")).unwrap(), b"preserve");
        manager.shutdown().await.unwrap();
        let _ = fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn shutdown_joins_materialization_before_cluster_storage() {
        let (manager, path, _) =
            stopped_standalone_export_manager("shutdown-owned-materialization").await;
        let active = manager.active.read().await.as_ref().cloned().unwrap();
        let exited = Arc::new(std::sync::atomic::AtomicBool::new(false));
        active
            .export
            .start_test_materialization(ExportId::from_uuid(Uuid::from_u128(0x900)), exited.clone())
            .await
            .unwrap();

        manager.shutdown().await.unwrap();

        assert!(exited.load(std::sync::atomic::Ordering::Acquire));
        assert!(!active.export.materialization_is_running().await);
        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn timed_out_export_mutation_retains_request_identity() {
        let request = export_request(9);
        let error = export_timeout_error(
            ConsensusGroup::Control,
            Some(AmbiguousRequest::Mutation {
                request: request.clone(),
            }),
        );

        assert!(matches!(
            error,
            DomainError::QuorumUnavailable {
                request: Some(AmbiguousRequest::Mutation { request: actual }),
                ..
            } if actual == request
        ));
    }

    #[tokio::test]
    async fn remote_authority_sampling_is_bounded_timed_and_ordered() {
        let active = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let probes = (0..6)
            .map(|order| authority_probe(order, order as u64 + 1))
            .collect();
        let results = sample_remote_authority_with(
            probes,
            ReadinessProbePolicy {
                max_in_flight: 2,
                per_probe_timeout: Duration::from_millis(20),
            },
            {
                let active = active.clone();
                let maximum = maximum.clone();
                move |probe, _| {
                    let active = active.clone();
                    let maximum = maximum.clone();
                    async move {
                        let current = active.fetch_add(1, AtomicOrdering::AcqRel) + 1;
                        let _active_probe = ActiveProbe(active);
                        maximum.fetch_max(current, AtomicOrdering::AcqRel);
                        let delay = if probe.order == 3 { 50 } else { 5 };
                        tokio::time::sleep(Duration::from_millis(delay)).await;
                        if probe.order == 4 {
                            AuthorityProbeOutcome::Unsupported
                        } else {
                            AuthorityProbeOutcome::Ready
                        }
                    }
                }
            },
        )
        .await;

        assert!(maximum.load(AtomicOrdering::Acquire) <= 2);
        assert_eq!(
            results
                .iter()
                .map(|result| result.order)
                .collect::<Vec<_>>(),
            vec![0, 1, 2, 3, 4, 5]
        );
        assert_eq!(results[3].outcome, AuthorityProbeOutcome::Stale);
        assert_eq!(results[4].outcome, AuthorityProbeOutcome::Unsupported);
    }

    #[test]
    fn readiness_probe_uses_peer_route_without_changing_peer_identity() {
        let target = local(2);
        let routes = PeerRoutes::parse(vec!["2=http://127.0.0.1:8202".to_owned()], 1).unwrap();

        assert_eq!(
            readiness_probe_endpoint(&routes, 2, &target),
            "http://127.0.0.1:8202"
        );
        assert_eq!(target.peer_uri(), "http://127.0.0.1:7202");
    }

    #[tokio::test]
    async fn shutdown_blocks_late_cluster_activation() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/test-data/light-stream-server/shutdown-activation");
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        let manager = Arc::new(
            ClusterManager::open(path.clone(), local(1), manager_config())
                .await
                .unwrap(),
        );
        let held = manager.bootstrap_lock.lock().await;
        let shutdown_manager = manager.clone();
        let shutdown = tokio::spawn(async move {
            shutdown_manager
                .begin_shutdown(Duration::from_secs(1), Duration::from_secs(1))
                .await
        });
        tokio::task::yield_now().await;

        let spec = BootstrapSpec::new(
            ClusterId::from_uuid(Uuid::new_v4()),
            light_stream_core::StreamId::from_uuid(Uuid::new_v4()),
            light_stream_core::StreamName::parse("shutdown").unwrap(),
        );
        let bootstrap_manager = manager.clone();
        let bootstrap = tokio::spawn(async move {
            bootstrap_manager
                .bootstrap(BootstrapCommand::standalone(spec))
                .await
        });
        tokio::task::yield_now().await;
        drop(held);

        let (maintenance, drain) = shutdown.await.unwrap();
        maintenance.unwrap();
        assert_eq!(drain, DrainOutcome::Completed { accepted: 0 });
        assert!(matches!(
            bootstrap.await.unwrap(),
            Err(DomainError::ShuttingDown { .. })
        ));
        assert!(manager.active.read().await.is_none());
        manager.shutdown().await.unwrap();
        let _ = fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn shutdown_deadline_bounds_bootstrap_lock_wait() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/test-data/light-stream-server/shutdown-bootstrap-deadline");
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        let manager = Arc::new(
            ClusterManager::open(path.clone(), local(1), manager_config())
                .await
                .unwrap(),
        );
        let held = manager.bootstrap_lock.lock().await;
        let started = tokio::time::Instant::now();

        let (maintenance, drain) = manager
            .begin_shutdown(Duration::from_secs(1), Duration::from_millis(25))
            .await;

        assert!(matches!(
            maintenance,
            Err(ShutdownPreparationError::BootstrapDeadline)
        ));
        assert_eq!(drain, DrainOutcome::Completed { accepted: 0 });
        assert!(started.elapsed() < Duration::from_secs(1));
        drop(held);
        manager.shutdown().await.unwrap();
        let _ = fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn bootstrap_waiting_for_lock_is_not_admitted() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/test-data/light-stream-server/cancelled-bootstrap");
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        let manager = Arc::new(
            ClusterManager::open(path.clone(), local(1), manager_config())
                .await
                .unwrap(),
        );
        let held = manager.bootstrap_lock.lock().await;
        let spec = BootstrapSpec::new(
            ClusterId::from_uuid(Uuid::new_v4()),
            light_stream_core::StreamId::from_uuid(Uuid::new_v4()),
            light_stream_core::StreamName::parse("cancelled-bootstrap").unwrap(),
        );
        let bootstrap_manager = manager.clone();
        let bootstrap = tokio::spawn(async move {
            bootstrap_manager
                .bootstrap(BootstrapCommand::standalone(spec))
                .await
        });
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            manager
                .lifecycle
                .admission_drain_snapshot()
                .mutations_in_flight,
            0
        );
        bootstrap.abort();
        let _ = bootstrap.await;
        drop(held);
        assert!(manager.identity().await.is_none());
        manager.shutdown().await.unwrap();
        let _ = fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn bootstrap_is_explicit_and_idempotent() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/test-data/light-stream-server/bootstrap");
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        let manager = Arc::new(
            ClusterManager::open(path.clone(), local(1), manager_config())
                .await
                .unwrap(),
        );
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
        let reopened = ClusterManager::open(path.clone(), local(1), manager_config())
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
        let error = ClusterManager::open(path.clone(), local(1), manager_config())
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
            )
            .unwrap(),
        )
        .unwrap();
        let manager = ClusterManager::open(path.clone(), local(2), manager_config())
            .await
            .unwrap();
        assert!(path.join("groups/1/rocksdb").is_dir());
        assert!(path.join("groups/2/rocksdb").is_dir());
        assert!(manager.identity().await.is_none());
        manager.shutdown().await.unwrap();
        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn version_three_manifest_migration_is_not_published_before_open_succeeds() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/test-data/light-stream-server/manifest-v3-migration");
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
            &LegacyNodeManifestV3 {
                format_version: LEGACY_NODE_MANIFEST_VERSION,
                local_node_id: NodeId::new(2).unwrap(),
                formation,
                state: PersistedNodeState::Active,
            },
        )
        .unwrap();

        let Some(ActiveManifest::V2(manifest)) = read_manifest(&path).unwrap() else {
            panic!("expected migrated version 2 manifest");
        };

        assert_eq!(NODE_MANIFEST_VERSION, manifest.format_version);
        assert_eq!(3, manifest.topology.authorized_nodes().len());
        assert_eq!(3, manifest.topology.desired_voters().len());
        let stored_before: ManifestHeader =
            serde_json::from_slice(&fs::read(manifest_path(&path)).unwrap()).unwrap();
        assert_eq!(LEGACY_NODE_MANIFEST_VERSION, stored_before.format_version);
        write_manifest(&path, &manifest).unwrap();
        let stored_after: ManifestHeader =
            serde_json::from_slice(&fs::read(manifest_path(&path)).unwrap()).unwrap();
        assert_eq!(NODE_MANIFEST_VERSION, stored_after.format_version);
        let _ = fs::remove_dir_all(path);
    }
}
