mod retention;
mod snapshot;
pub use retention::ClockObservation;
use retention::{
    AdmissionState, LeaseBudget, PartitionRetentionState, RetentionLimits, SafeLeaseClock, admit,
    advance_floor, lease_is_effectively_active,
};
pub use snapshot::{SnapshotArtifact, SnapshotDigest};
use snapshot::{SnapshotCatalog, SnapshotRecord, StoredArtifactDescriptor, decode_snapshot_v3};

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    fmt::{self, Debug},
    fs, io,
    marker::PhantomData,
    ops::{Bound, RangeBounds},
    path::{Path, PathBuf},
    sync::{
        Arc, Condvar, Mutex, RwLock,
        atomic::{AtomicU64, Ordering},
    },
};

use crc32fast::Hasher as Crc32;
use futures_util::{Stream, StreamExt};
use light_stream_core::{
    AdministrationIntent, AdministrationOperation, AdministrationRequestId, BookmarkId,
    BookmarkName, BookmarkPage, BookmarkPageRequest, BookmarkPublicationSequence, BootstrapResult,
    BootstrapSpec, ByteCount, CatalogRequestId, CheckpointCasResult, CheckpointExpectation,
    CheckpointKey, CheckpointMutation, CheckpointRevision, ClusterId, ClusterTopology,
    CommittedBookmark, CommittedCheckpoint, CommittedCursor, CommittedRecord, CommittedRecordRange,
    CommittedStreamBookmark, CreateStreamSpec, DomainError, FetchPage, GroupId, LeaseDeadline,
    LeaseRelease, LeaseRenewal, MutationRequestId, NodeId, OperationalProof, PartitionId,
    PartitionKey, PartitionPlacement, PartitionRoute, ProducerRequestId, PublishBatch,
    PublishReceipt, RecordOffset, ReplayLease, ReplayLeaseId, ReplayLeaseRequest,
    ReplicatedPublishBatch, RetentionRequest, RetentionResult, RetentionStatus, SecurityMutation,
    SecurityPolicy, StreamBookmarkPage, StreamBookmarkPageRequest, StreamCursorVector,
    StreamDescriptor, StreamId, StreamLifecycle, StreamName,
};
use openraft::{
    BasicNode, EntryPayload,
    errors::{RPCError, ReplicationClosed, StreamingError, Unreachable},
    network::{RPCOption, RaftNetworkFactory, RaftNetworkV2},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
    },
    storage::{
        EntryResponder, IOFlushed, LogState, RaftLogReader, RaftLogStorage, RaftSnapshotBuilder,
        RaftStateMachine, Snapshot,
    },
    type_config::alias::{LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf, VoteOf},
};
use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamilyDescriptor, DB, Direction, IteratorMode, Options,
    WriteBatch, WriteOptions,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use thiserror::Error;

pub const STORAGE_FORMAT_VERSION: u32 = 1;
pub const CONTROL_GROUP_ID: u64 = 1;
pub const DATA_GROUP_ID: u64 = 2;
pub const DEFAULT_RECEIPT_WINDOW: usize = 4096;
pub const MAX_FETCH_RECORDS: u32 = 1024;
pub const MAX_FETCH_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_SNAPSHOT_BYTES: usize = 64 * 1024 * 1024;
pub const MIN_GROUP_CACHE_BYTES: usize = 1024 * 1024;
pub const MIN_GROUP_WRITE_BUFFER_BYTES: usize = 1024 * 1024;

const CF_META: &str = "ls_v1_meta";
const CF_RAFT_META: &str = "ls_v1_raft_meta";
const CF_RAFT_LOG: &str = "ls_v1_raft_log";
const CF_PAYLOAD: &str = "ls_v1_payload";
const CF_STATE: &str = "ls_v1_state";
const CF_STATE_A: &str = "ls_v2_state_a";
const CF_STATE_B: &str = "ls_v2_state_b";
const CF_SNAPSHOT: &str = "ls_v1_snapshot";
const COLUMN_FAMILIES: [&str; 8] = [
    CF_META,
    CF_RAFT_META,
    CF_RAFT_LOG,
    CF_PAYLOAD,
    CF_STATE,
    CF_STATE_A,
    CF_STATE_B,
    CF_SNAPSHOT,
];

const KEY_IDENTITY: &[u8] = b"identity";
const KEY_SCHEMA_VERSION: &[u8] = b"schema-version";
const KEY_SCHEMA_MIGRATION_CURSOR: &[u8] = b"schema-migration-cursor";
const KEY_ACTIVE_STATE_BANK: &[u8] = b"active-state-bank";
const KEY_VOTE: &[u8] = b"vote";
const KEY_COMMITTED: &[u8] = b"committed";
const KEY_PURGED: &[u8] = b"purged";
const KEY_APPLIED: &[u8] = b"applied";
const KEY_MEMBERSHIP: &[u8] = b"membership";
const KEY_BOOTSTRAP: &[u8] = b"bootstrap";
const KEY_CURRENT_SNAPSHOT: &[u8] = b"current";
const SNAPSHOT_MAGIC: &[u8; 8] = b"LSNP0002";
const PAYLOAD_MAGIC: &[u8; 8] = b"LSPY0002";
const KEY_NEXT_OFFSET: &[u8] = b"next-offset";
const KEY_DATA_GROUP_POOL: &[u8] = b"data-group-pool";
const KEY_MAX_STREAMS: &[u8] = b"max-streams";
const KEY_MAX_PARTITIONS: &[u8] = b"max-partitions";
const KEY_ASSIGNMENT_CURSOR: &[u8] = b"assignment-cursor";
const KEY_CATALOG_REVISION: &[u8] = b"catalog-revision";
const STREAM_PREFIX: &[u8] = b"stream/";
const STREAM_NAME_PREFIX: &[u8] = b"stream-name/";
const CREATE_INTENT_PREFIX: &[u8] = b"create-intent/";
const PAYLOAD_BYTES_PREFIX: &[u8] = b"bytes/";
const PAYLOAD_OWNERS_PREFIX: &[u8] = b"owners/";
const RECORD_PREFIX: &[u8] = b"record/";
const RECEIPT_PREFIX: &[u8] = b"receipt/";
const SESSION_PREFIX: &[u8] = b"session/";
const MUTATION_RECEIPT_PREFIX: &[u8] = b"mutation-receipt/";
const MUTATION_SESSION_PREFIX: &[u8] = b"mutation-session/";
const CHECKPOINT_PREFIX: &[u8] = b"checkpoint/";
const BOOKMARK_ID_PREFIX: &[u8] = b"bookmark/id/";
const BOOKMARK_NAME_PREFIX: &[u8] = b"bookmark/name/";
const BOOKMARK_ORDER_PREFIX: &[u8] = b"bookmark/order/";
const BOOKMARK_PUBLICATION_PREFIX: &[u8] = b"bookmark/publication/";
const STREAM_BOOKMARK_ID_PREFIX: &[u8] = b"stream-bookmark/id/";
const STREAM_BOOKMARK_NAME_PREFIX: &[u8] = b"stream-bookmark/name/";
const STREAM_BOOKMARK_ORDER_PREFIX: &[u8] = b"stream-bookmark/order/";
const STREAM_BOOKMARK_PUBLICATION_PREFIX: &[u8] = b"stream-bookmark/publication/";
const RETENTION_PREFIX: &[u8] = b"retention/";
const KEY_LEASE_CLOCK: &[u8] = b"lease-clock";
const KEY_LEASE_BUDGET: &[u8] = b"lease-budget";
const KEY_OPERATIONAL_PROOF: &[u8] = b"operational-proof";
const KEY_CLUSTER_TOPOLOGY: &[u8] = b"cluster-topology";
const KEY_SECURITY_POLICY: &[u8] = b"security-policy";
const KEY_ACTIVE_ADMINISTRATION: &[u8] = b"administration/active";
const ADMINISTRATION_REQUEST_PREFIX: &[u8] = b"administration/request/";
const LEASE_ID_PREFIX: &[u8] = b"lease/id/";
const LEASE_REQUEST_PREFIX: &[u8] = b"lease/request/";
const CURRENT_SCHEMA_VERSION: u32 = 2;
const LEGACY_SCHEMA_VERSION: u32 = 1;
const MIGRATION_BATCH_RECORDS: usize = 1024;

type GroupLeaderId = openraft::impls::leader_id_adv::LeaderId<u64, u64>;
pub type GroupLogId = openraft::LogId<GroupLeaderId>;
pub type GroupEntry = openraft::Entry<GroupLeaderId, GroupCommand, u64, BasicNode>;
type GroupMembership = openraft::StoredMembership<GroupLeaderId, u64, BasicNode>;
type GroupSnapshotMeta = openraft::storage::SnapshotMeta<GroupLeaderId, u64, BasicNode>;
type PayloadWrite = (Vec<u8>, Vec<u8>);
type ThinEntryWrite = (ThinEntry, Vec<PayloadWrite>);

openraft::declare_raft_types!(
    pub ControlRaftConfig:
        D = GroupCommand,
        R = ApplyResult,
        NodeId = u64,
        Node = BasicNode,
        Entry = GroupEntry,
);

openraft::declare_raft_types!(
    pub DataRaftConfig:
        D = GroupCommand,
        R = ApplyResult,
        NodeId = u64,
        Node = BasicNode,
        Entry = GroupEntry,
);

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GroupKind {
    Control,
    Data,
}

impl fmt::Display for GroupKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control => formatter.write_str("control"),
            Self::Data => formatter.write_str("data"),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GroupIdentity {
    pub format_version: u32,
    pub cluster_id: ClusterId,
    pub group_id: GroupId,
    pub kind: GroupKind,
}

impl GroupIdentity {
    pub fn new(cluster_id: ClusterId, group_id: GroupId, kind: GroupKind) -> Self {
        Self {
            format_version: STORAGE_FORMAT_VERSION,
            cluster_id,
            group_id,
            kind,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum GroupCommand {
    BootstrapControl {
        spec: BootstrapSpec,
        topology: Option<ClusterTopology>,
        #[serde(default)]
        security: Option<SecurityPolicy>,
        data_groups: Vec<GroupId>,
        max_streams: u32,
        max_partitions_per_stream: u32,
    },
    BootstrapData {
        spec: BootstrapSpec,
    },
    CreateStreamIntent {
        spec: CreateStreamSpec,
        stream_id: StreamId,
    },
    ReplicaReady {
        stream_id: StreamId,
        group_id: GroupId,
    },
    ActivateStream {
        stream_id: StreamId,
    },
    BeginDeleteStream {
        stream_id: StreamId,
    },
    FinishDeleteStream {
        stream_id: StreamId,
    },
    Publish {
        batch: PublishBatch,
    },
    PublishMany {
        batch: ReplicatedPublishBatch,
    },
    CompareAndSetCheckpoint {
        mutation: CheckpointMutation,
    },
    CreateBookmark {
        id: BookmarkId,
        partition: PartitionKey,
        name: BookmarkName,
        offset: RecordOffset,
    },
    DeleteBookmark {
        partition: PartitionKey,
        id: BookmarkId,
    },
    CreateStreamBookmark {
        id: BookmarkId,
        name: BookmarkName,
        vector: StreamCursorVector,
    },
    DeleteStreamBookmark {
        stream_id: StreamId,
        id: BookmarkId,
    },
    AdvanceRetention {
        request: RetentionRequest,
        clock: ClockObservation,
    },
    AdmitReplayLease {
        request: ReplayLeaseRequest,
        clock: ClockObservation,
    },
    RenewReplayLease {
        request: LeaseRenewal,
        clock: ClockObservation,
    },
    ReleaseReplayLease {
        request: LeaseRelease,
        clock: ClockObservation,
    },
    MaintainRetention {
        partition: PartitionKey,
        expected_cursor: RecordOffset,
        max_records: u32,
        max_payload_bytes: u64,
        clock: ClockObservation,
    },
    OperationalProbe {
        group: GroupId,
    },
    BeginAdministration {
        intent: AdministrationIntent,
    },
    CompleteAdministration {
        request: AdministrationRequestId,
    },
    AbortAdministration {
        request: AdministrationRequestId,
    },
    FinishAdministrationAbort {
        request: AdministrationRequestId,
    },
    InitializeClusterTopology {
        topology: ClusterTopology,
    },
    InitializeSecurityPolicy {
        policy: SecurityPolicy,
    },
    ApplySecurityMutation {
        mutation: SecurityMutation,
    },
    ActivateSecuredTransport {
        request: MutationRequestId,
        topology: ClusterTopology,
        policy: SecurityPolicy,
    },
}

impl fmt::Display for GroupCommand {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BootstrapControl { .. } => formatter.write_str("bootstrap-control"),
            Self::BootstrapData { .. } => formatter.write_str("bootstrap-data"),
            Self::CreateStreamIntent { .. } => formatter.write_str("create-stream-intent"),
            Self::ReplicaReady { .. } => formatter.write_str("replica-ready"),
            Self::ActivateStream { .. } => formatter.write_str("activate-stream"),
            Self::BeginDeleteStream { .. } => formatter.write_str("begin-delete-stream"),
            Self::FinishDeleteStream { .. } => formatter.write_str("finish-delete-stream"),
            Self::Publish { .. } => formatter.write_str("publish"),
            Self::PublishMany { .. } => formatter.write_str("publish-many"),
            Self::CompareAndSetCheckpoint { .. } => {
                formatter.write_str("compare-and-set-checkpoint")
            }
            Self::CreateBookmark { .. } => formatter.write_str("create-bookmark"),
            Self::DeleteBookmark { .. } => formatter.write_str("delete-bookmark"),
            Self::CreateStreamBookmark { .. } => formatter.write_str("create-stream-bookmark"),
            Self::DeleteStreamBookmark { .. } => formatter.write_str("delete-stream-bookmark"),
            Self::AdvanceRetention { .. } => formatter.write_str("advance-retention"),
            Self::AdmitReplayLease { .. } => formatter.write_str("admit-replay-lease"),
            Self::RenewReplayLease { .. } => formatter.write_str("renew-replay-lease"),
            Self::ReleaseReplayLease { .. } => formatter.write_str("release-replay-lease"),
            Self::MaintainRetention { .. } => formatter.write_str("maintain-retention"),
            Self::OperationalProbe { .. } => formatter.write_str("operational-probe"),
            Self::BeginAdministration { .. } => formatter.write_str("begin-administration"),
            Self::CompleteAdministration { .. } => formatter.write_str("complete-administration"),
            Self::AbortAdministration { .. } => formatter.write_str("abort-administration"),
            Self::FinishAdministrationAbort { .. } => {
                formatter.write_str("finish-administration-abort")
            }
            Self::InitializeClusterTopology { .. } => {
                formatter.write_str("initialize-cluster-topology")
            }
            Self::InitializeSecurityPolicy { .. } => {
                formatter.write_str("initialize-security-policy")
            }
            Self::ApplySecurityMutation { .. } => formatter.write_str("apply-security-mutation"),
            Self::ActivateSecuredTransport { .. } => {
                formatter.write_str("activate-secured-transport")
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ApplyResult {
    Bootstrapped(BootstrapResult),
    Stream(StreamDescriptor),
    Published(PublishReceipt),
    PublishedMany(PublishManyResult),
    Checkpoint(CheckpointCasResult),
    Bookmark(CommittedBookmark),
    StreamBookmark(CommittedStreamBookmark),
    Retention(RetentionResult),
    ReplayLease(ReplayLease),
    RetentionStatus(RetentionStatus),
    OperationalProof(OperationalProof),
    Administration(AdministrationOperation),
    SecurityPolicy(SecurityPolicy),
    Rejected(DomainError),
    Noop,
}

impl fmt::Display for ApplyResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bootstrapped(_) => formatter.write_str("bootstrapped"),
            Self::Stream(value) => write!(formatter, "stream {}", value.stream()),
            Self::Published(_) => formatter.write_str("published"),
            Self::PublishedMany(value) => {
                write!(formatter, "published {} requests", value.outcomes().len())
            }
            Self::Checkpoint(value) => write!(formatter, "checkpoint {:?}", value),
            Self::Bookmark(value) => write!(formatter, "bookmark {}", value.id()),
            Self::StreamBookmark(value) => write!(formatter, "stream bookmark {}", value.id()),
            Self::Retention(value) => {
                write!(formatter, "retention floor {}", value.floor().get())
            }
            Self::ReplayLease(value) => write!(formatter, "replay lease {}", value.id()),
            Self::RetentionStatus(value) => {
                write!(
                    formatter,
                    "retention cursor {}",
                    value.reclaim_cursor().get()
                )
            }
            Self::OperationalProof(value) => {
                write!(formatter, "operational proof {}", value.log_index())
            }
            Self::Administration(value) => {
                write!(formatter, "administration {:?}", value.lifecycle())
            }
            Self::SecurityPolicy(value) => {
                write!(formatter, "security policy {}", value.revision().get())
            }
            Self::Rejected(error) => write!(formatter, "rejected: {error}"),
            Self::Noop => formatter.write_str("noop"),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PublishItemOutcome {
    request: ProducerRequestId,
    result: Result<PublishReceipt, DomainError>,
}

impl PublishItemOutcome {
    pub fn new(request: ProducerRequestId, result: Result<PublishReceipt, DomainError>) -> Self {
        Self { request, result }
    }

    pub fn request(&self) -> &ProducerRequestId {
        &self.request
    }

    pub fn result(&self) -> &Result<PublishReceipt, DomainError> {
        &self.result
    }

    pub fn into_result(self) -> Result<PublishReceipt, DomainError> {
        self.result
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PublishManyResult {
    outcomes: Vec<PublishItemOutcome>,
}

impl PublishManyResult {
    pub fn new(outcomes: Vec<PublishItemOutcome>) -> Self {
        Self { outcomes }
    }

    pub fn outcomes(&self) -> &[PublishItemOutcome] {
        &self.outcomes
    }

    pub fn into_outcomes(self) -> Vec<PublishItemOutcome> {
        self.outcomes
    }
}

#[derive(Debug, Error)]
pub enum StorageOpenError {
    #[error("storage path {path} does not exist")]
    Missing { path: String },
    #[error("storage path {path} is not pristine")]
    NotPristine { path: String },
    #[error("storage identity conflict: expected {expected:?}, found {actual:?}")]
    IdentityConflict {
        expected: GroupIdentity,
        actual: GroupIdentity,
    },
    #[error("unsupported storage format {0}")]
    UnsupportedVersion(u32),
    #[error("storage error: {0}")]
    Storage(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GroupStorageBudget {
    pub cache_bytes: usize,
    pub write_buffer_bytes: usize,
}

impl GroupStorageBudget {
    pub fn new(cache_bytes: usize, write_buffer_bytes: usize) -> Result<Self, StorageOpenError> {
        if cache_bytes < MIN_GROUP_CACHE_BYTES || write_buffer_bytes < MIN_GROUP_WRITE_BUFFER_BYTES
        {
            return Err(StorageOpenError::Storage(
                "per-group RocksDB budget is below the 1 MiB minimum".to_owned(),
            ));
        }
        Ok(Self {
            cache_bytes,
            write_buffer_bytes,
        })
    }
}

#[derive(Clone)]
struct GroupDb {
    db: Arc<DB>,
    group_root: PathBuf,
    identity: GroupIdentity,
    receipt_window: usize,
    write_lane: Arc<OrderedWriteLane>,
    state_bank: Arc<RwLock<StateBank>>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
enum StateBank {
    A,
    B,
}

impl StateBank {
    const fn column_family(self) -> &'static str {
        match self {
            Self::A => CF_STATE_A,
            Self::B => CF_STATE_B,
        }
    }

    const fn inactive(self) -> Self {
        match self {
            Self::A => Self::B,
            Self::B => Self::A,
        }
    }

    const fn bit(self) -> u8 {
        match self {
            Self::A => 1,
            Self::B => 2,
        }
    }
}

#[derive(Debug, Default)]
struct OrderedWriteLane {
    next: AtomicU64,
    serving: Mutex<u64>,
    changed: Condvar,
}

struct OrderedWriteGuard<'a> {
    lane: &'a OrderedWriteLane,
}

impl OrderedWriteLane {
    fn enter(&self) -> io::Result<OrderedWriteGuard<'_>> {
        let ticket = self.next.fetch_add(1, Ordering::Relaxed);
        let mut serving = self
            .serving
            .lock()
            .map_err(|_| io_error("group write lane poisoned"))?;
        while *serving != ticket {
            serving = self
                .changed
                .wait(serving)
                .map_err(|_| io_error("group write lane poisoned"))?;
        }
        Ok(OrderedWriteGuard { lane: self })
    }
}

impl Drop for OrderedWriteGuard<'_> {
    fn drop(&mut self) {
        if let Ok(mut serving) = self.lane.serving.lock() {
            *serving += 1;
            self.lane.changed.notify_all();
        }
    }
}

#[derive(Clone)]
pub struct RocksLogStore<C> {
    db: GroupDb,
    marker: PhantomData<C>,
}

#[derive(Clone)]
pub struct RocksLogReader<C> {
    db: GroupDb,
    marker: PhantomData<C>,
}

#[derive(Clone)]
pub struct RocksStateMachine<C> {
    db: GroupDb,
    marker: PhantomData<C>,
}

#[derive(Clone)]
pub struct CommittedStateReader {
    db: GroupDb,
}

#[derive(Clone)]
pub struct GroupSnapshotBuilder<C> {
    db: GroupDb,
    marker: PhantomData<C>,
}

#[derive(Clone, Debug)]
pub struct StoreHandles<C> {
    pub log_store: RocksLogStore<C>,
    pub state_machine: RocksStateMachine<C>,
    pub reader: CommittedStateReader,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct StoredValue {
    version: u32,
    checksum: u32,
    body: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ThinEntry {
    log_id: GroupLogId,
    payload: ThinPayload,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
enum ThinPayload {
    Blank,
    Membership(openraft::Membership<u64, BasicNode>),
    Normal(ThinCommand),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
enum ThinCommand {
    BootstrapControl {
        spec: BootstrapSpec,
        #[serde(default)]
        topology: Option<ClusterTopology>,
        #[serde(default)]
        security: Option<Box<SecurityPolicy>>,
        data_groups: Vec<GroupId>,
        max_streams: u32,
        max_partitions_per_stream: u32,
    },
    BootstrapData {
        spec: BootstrapSpec,
    },
    CreateStreamIntent {
        spec: CreateStreamSpec,
        stream_id: StreamId,
    },
    ReplicaReady {
        stream_id: StreamId,
        group_id: GroupId,
    },
    ActivateStream {
        stream_id: StreamId,
    },
    BeginDeleteStream {
        stream_id: StreamId,
    },
    FinishDeleteStream {
        stream_id: StreamId,
    },
    Publish {
        cluster: ClusterId,
        partition: PartitionKey,
        request: ProducerRequestId,
        fingerprint: String,
        payload_keys: Vec<Vec<u8>>,
        bookmark: Option<BookmarkName>,
    },
    PublishMany {
        requests: Vec<ThinPublish>,
    },
    CompareAndSetCheckpoint {
        mutation: CheckpointMutation,
    },
    CreateBookmark {
        id: BookmarkId,
        partition: PartitionKey,
        name: BookmarkName,
        offset: RecordOffset,
    },
    DeleteBookmark {
        partition: PartitionKey,
        id: BookmarkId,
    },
    CreateStreamBookmark {
        id: BookmarkId,
        name: BookmarkName,
        vector: StreamCursorVector,
    },
    DeleteStreamBookmark {
        stream_id: StreamId,
        id: BookmarkId,
    },
    AdvanceRetention {
        request: RetentionRequest,
        clock: ClockObservation,
    },
    AdmitReplayLease {
        request: ReplayLeaseRequest,
        clock: ClockObservation,
    },
    RenewReplayLease {
        request: LeaseRenewal,
        clock: ClockObservation,
    },
    ReleaseReplayLease {
        request: LeaseRelease,
        clock: ClockObservation,
    },
    MaintainRetention {
        partition: PartitionKey,
        expected_cursor: RecordOffset,
        max_records: u32,
        max_payload_bytes: u64,
        clock: ClockObservation,
    },
    OperationalProbe {
        group: GroupId,
    },
    BeginAdministration {
        intent: AdministrationIntent,
    },
    CompleteAdministration {
        request: AdministrationRequestId,
    },
    AbortAdministration {
        request: AdministrationRequestId,
    },
    FinishAdministrationAbort {
        request: AdministrationRequestId,
    },
    InitializeClusterTopology {
        topology: ClusterTopology,
    },
    InitializeSecurityPolicy {
        policy: Box<SecurityPolicy>,
    },
    ApplySecurityMutation {
        mutation: Box<SecurityMutation>,
    },
    ActivateSecuredTransport {
        request: MutationRequestId,
        topology: ClusterTopology,
        policy: Box<SecurityPolicy>,
    },
}

struct BootstrapControlState {
    topology: Option<ClusterTopology>,
    security: Option<SecurityPolicy>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ThinPublish {
    cluster: ClusterId,
    partition: PartitionKey,
    request: ProducerRequestId,
    fingerprint: String,
    payload_keys: Vec<Vec<u8>>,
    bookmark: Option<BookmarkName>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
struct PayloadOwners {
    raft_log: bool,
    #[serde(default)]
    applied_state: bool,
    #[serde(default)]
    applied_banks: u8,
}

impl PayloadOwners {
    fn reachable(&self) -> bool {
        self.raft_log || self.applied_state || self.applied_banks != 0
    }

    fn applied_in(&self, bank: StateBank) -> bool {
        self.applied_banks & bank.bit() != 0
    }

    fn set_applied_in(&mut self, bank: StateBank, applied: bool) {
        if applied {
            self.applied_banks |= bank.bit();
        } else {
            self.applied_banks &= !bank.bit();
        }
        self.applied_state = false;
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct StoredRecord {
    payload_key: Vec<u8>,
    #[serde(default)]
    payload_bytes: u64,
    #[serde(default)]
    cumulative_end_bytes: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct StoredReceipt {
    fingerprint: String,
    receipt: PublishReceipt,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
enum StoredPublishResult {
    Published(PublishReceipt),
    Rejected(StoredPublishRejection),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum StoredPublishRejection {
    BookmarkNameConflict,
    InvalidRange(String),
}

impl StoredPublishResult {
    fn as_result(&self) -> Result<PublishReceipt, DomainError> {
        match self {
            Self::Published(receipt) => Ok(receipt.clone()),
            Self::Rejected(StoredPublishRejection::BookmarkNameConflict) => {
                Err(DomainError::BookmarkNameConflict)
            }
            Self::Rejected(StoredPublishRejection::InvalidRange(reason)) => {
                Err(DomainError::InvalidRange {
                    reason: reason.clone(),
                })
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct VersionedStoredPublishOutcome {
    fingerprint: String,
    result: StoredPublishResult,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(untagged)]
enum StoredPublishOutcome {
    Legacy(StoredReceipt),
    Versioned(VersionedStoredPublishOutcome),
}

struct PublishApplyTxn<'a> {
    db: &'a GroupDb,
    log_id: GroupLogId,
    write: &'a mut WriteBatch,
    active_bank: StateBank,
    next_offsets: HashMap<PartitionKey, u64>,
    retentions: HashMap<PartitionKey, PartitionRetentionState>,
    sessions: BTreeMap<Vec<u8>, ProducerSessionState>,
    outcomes: BTreeMap<Vec<u8>, VersionedStoredPublishOutcome>,
    bookmark_names: BTreeMap<Vec<u8>, Option<BookmarkId>>,
    bookmark_ids: BTreeMap<Vec<u8>, Option<CommittedBookmark>>,
    bookmark_publications: HashMap<PartitionKey, u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct StoredMutationReceipt {
    fingerprint: String,
    result: ApplyResult,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
struct ProducerSessionState {
    highest_sequence: Option<u64>,
    retained_sequences: VecDeque<u64>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
struct MutationSessionState {
    highest_sequence: Option<u64>,
    retained_sequences: VecDeque<u64>,
}

struct LeaseExpiryState {
    budget: LeaseBudget,
    active: Vec<ReplayLease>,
    retentions: Vec<(PartitionKey, PartitionRetentionState)>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct StoredCreateIntent {
    spec: CreateStreamSpec,
    stream_id: StreamId,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct SnapshotBundle {
    format_version: u32,
    identity: GroupIdentity,
    meta: GroupSnapshotMeta,
    state: Vec<(Vec<u8>, Vec<u8>)>,
    payloads: Vec<(Vec<u8>, Vec<u8>)>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct StoredCurrentSnapshot {
    artifact: StoredArtifactDescriptor,
    meta: GroupSnapshotMeta,
}

fn decode_snapshot_bundle(bytes: &[u8]) -> io::Result<SnapshotBundle> {
    if let Some(decoded) = decode_snapshot_v3(bytes)? {
        return Ok(SnapshotBundle {
            format_version: decoded.storage_format_version,
            identity: serde_json::from_slice(&decoded.identity_json).map_err(io_error)?,
            meta: serde_json::from_slice(&decoded.meta_json).map_err(io_error)?,
            state: decoded.state,
            payloads: decoded.payloads,
        });
    }
    if !bytes.starts_with(SNAPSHOT_MAGIC) {
        return decode(bytes);
    }
    if bytes.len() < SNAPSHOT_MAGIC.len() + 4 + 32 {
        return Err(io_error("snapshot artifact is truncated"));
    }
    let content_len = bytes.len() - 32;
    let (content, expected_digest) = bytes.split_at(content_len);
    if Sha256::digest(content).as_slice() != expected_digest {
        return Err(io_error("snapshot artifact checksum mismatch"));
    }
    let mut cursor = SnapshotCursor::new(content);
    cursor.expect(SNAPSHOT_MAGIC)?;
    let format_version = cursor.read_u32()?;
    let identity = cursor.read_json()?;
    let meta = cursor.read_json()?;
    let state = cursor.read_entries()?;
    let payloads = cursor.read_entries()?;
    if !cursor.is_finished() {
        return Err(io_error("snapshot artifact has trailing bytes"));
    }
    Ok(SnapshotBundle {
        format_version,
        identity,
        meta,
        state,
        payloads,
    })
}

fn encode_payload_value(payload: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(PAYLOAD_MAGIC.len() + payload.len());
    bytes.extend_from_slice(PAYLOAD_MAGIC);
    bytes.extend_from_slice(payload);
    bytes
}

fn decode_payload_value(bytes: &[u8]) -> io::Result<Vec<u8>> {
    if let Some(payload) = bytes.strip_prefix(PAYLOAD_MAGIC) {
        Ok(payload.to_vec())
    } else {
        decode(bytes)
    }
}

struct SnapshotCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> SnapshotCursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn expect(&mut self, expected: &[u8]) -> io::Result<()> {
        if self.take(expected.len())? != expected {
            return Err(io_error("snapshot artifact magic mismatch"));
        }
        Ok(())
    }

    fn read_u32(&mut self) -> io::Result<u32> {
        let bytes: [u8; 4] = self.take(4)?.try_into().map_err(io_error)?;
        Ok(u32::from_be_bytes(bytes))
    }

    fn read_u64(&mut self) -> io::Result<u64> {
        let bytes: [u8; 8] = self.take(8)?.try_into().map_err(io_error)?;
        Ok(u64::from_be_bytes(bytes))
    }

    fn read_json<T: DeserializeOwned>(&mut self) -> io::Result<T> {
        let len = usize::try_from(self.read_u32()?).map_err(io_error)?;
        serde_json::from_slice(self.take(len)?).map_err(io_error)
    }

    fn read_entries(&mut self) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let count = usize::try_from(self.read_u64()?).map_err(io_error)?;
        let mut entries = Vec::new();
        entries.try_reserve(count).map_err(io_error)?;
        for _ in 0..count {
            let key_len = usize::try_from(self.read_u32()?).map_err(io_error)?;
            let value_len = usize::try_from(self.read_u64()?).map_err(io_error)?;
            let key = self.take(key_len)?.to_vec();
            let value = self.take(value_len)?.to_vec();
            entries.push((key, value));
        }
        Ok(entries)
    }

    fn take(&mut self, len: usize) -> io::Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(len)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| io_error("snapshot artifact frame is truncated"))?;
        let value = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    const fn is_finished(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

impl<C> Debug for RocksLogStore<C> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RocksLogStore")
            .field("identity", &self.db.identity)
            .finish()
    }
}

impl<C> Debug for RocksStateMachine<C> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RocksStateMachine")
            .field("identity", &self.db.identity)
            .finish()
    }
}

impl Debug for CommittedStateReader {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CommittedStateReader")
            .field("identity", &self.db.identity)
            .finish()
    }
}

impl<C> StoreHandles<C> {
    fn new(db: GroupDb) -> Self {
        Self {
            log_store: RocksLogStore {
                db: db.clone(),
                marker: PhantomData,
            },
            state_machine: RocksStateMachine {
                db: db.clone(),
                marker: PhantomData,
            },
            reader: CommittedStateReader { db },
        }
    }
}

pub fn create_control_store(
    path: &Path,
    identity: GroupIdentity,
    receipt_window: usize,
    budget: GroupStorageBudget,
) -> Result<StoreHandles<ControlRaftConfig>, StorageOpenError> {
    create_store(path, identity, receipt_window, budget)
}

pub fn create_data_store(
    path: &Path,
    identity: GroupIdentity,
    receipt_window: usize,
    budget: GroupStorageBudget,
) -> Result<StoreHandles<DataRaftConfig>, StorageOpenError> {
    create_store(path, identity, receipt_window, budget)
}

pub fn open_control_store(
    path: &Path,
    identity: &GroupIdentity,
    receipt_window: usize,
    budget: GroupStorageBudget,
) -> Result<StoreHandles<ControlRaftConfig>, StorageOpenError> {
    open_store(path, identity, receipt_window, budget)
}

pub fn open_control_store_with_topology(
    path: &Path,
    identity: &GroupIdentity,
    receipt_window: usize,
    budget: GroupStorageBudget,
    topology: &ClusterTopology,
) -> Result<StoreHandles<ControlRaftConfig>, StorageOpenError> {
    let handles = open_store(path, identity, receipt_window, budget)?;
    handles
        .reader
        .db
        .initialize_control_topology(topology)
        .map_err(storage_open)?;
    Ok(handles)
}

pub fn open_data_store(
    path: &Path,
    identity: &GroupIdentity,
    receipt_window: usize,
    budget: GroupStorageBudget,
) -> Result<StoreHandles<DataRaftConfig>, StorageOpenError> {
    open_store(path, identity, receipt_window, budget)
}

fn create_store<C>(
    path: &Path,
    identity: GroupIdentity,
    receipt_window: usize,
    budget: GroupStorageBudget,
) -> Result<StoreHandles<C>, StorageOpenError> {
    if receipt_window == 0 {
        return Err(StorageOpenError::Storage(
            "receipt window must be greater than zero".to_owned(),
        ));
    }
    if path.exists() && path.read_dir().map_err(storage_open)?.next().is_some() {
        return open_store(path, &identity, receipt_window, budget);
    }
    fs::create_dir_all(path).map_err(storage_open)?;
    let db_path = path.join("rocksdb");
    let mut options = db_options(true, budget);
    options.create_missing_column_families(true);
    let descriptors = descriptors();
    let db = DB::open_cf_descriptors(&options, &db_path, descriptors).map_err(storage_open)?;
    let group_db = GroupDb {
        db: Arc::new(db),
        group_root: path.to_path_buf(),
        identity: identity.clone(),
        receipt_window,
        write_lane: Arc::new(OrderedWriteLane::default()),
        state_bank: Arc::new(RwLock::new(StateBank::A)),
    };
    group_db
        .put_sync(CF_META, KEY_IDENTITY, &identity)
        .map_err(storage_open)?;
    group_db
        .put_sync(CF_META, KEY_SCHEMA_VERSION, &CURRENT_SCHEMA_VERSION)
        .map_err(storage_open)?;
    group_db
        .put_sync(CF_META, KEY_ACTIVE_STATE_BANK, &StateBank::A)
        .map_err(storage_open)?;
    group_db
        .snapshot_catalog()
        .and_then(|catalog| catalog.reconcile_on_open())
        .map_err(storage_open)?;
    Ok(StoreHandles::new(group_db))
}

fn open_store<C>(
    path: &Path,
    expected: &GroupIdentity,
    receipt_window: usize,
    budget: GroupStorageBudget,
) -> Result<StoreHandles<C>, StorageOpenError> {
    let db_path = path.join("rocksdb");
    if !db_path.is_dir() {
        return Err(StorageOpenError::Missing {
            path: db_path.display().to_string(),
        });
    }
    let mut options = db_options(false, budget);
    options.create_missing_column_families(true);
    let actual_cfs = DB::list_cf(&options, &db_path).map_err(storage_open)?;
    let expected_cfs: BTreeSet<_> = std::iter::once("default")
        .chain(COLUMN_FAMILIES)
        .map(str::to_owned)
        .collect();
    let actual_cfs: BTreeSet<_> = actual_cfs.into_iter().collect();
    if !actual_cfs.is_subset(&expected_cfs) || !actual_cfs.contains(CF_STATE) {
        return Err(StorageOpenError::Storage(format!(
            "column family set contains unsupported entries: expected subset of {expected_cfs:?}, found {actual_cfs:?}"
        )));
    }
    let db = DB::open_cf_descriptors(&options, &db_path, descriptors()).map_err(storage_open)?;
    let group_db = GroupDb {
        db: Arc::new(db),
        group_root: path.to_path_buf(),
        identity: expected.clone(),
        receipt_window,
        write_lane: Arc::new(OrderedWriteLane::default()),
        state_bank: Arc::new(RwLock::new(StateBank::A)),
    };
    let actual = group_db
        .get::<GroupIdentity>(CF_META, KEY_IDENTITY)
        .map_err(storage_open)?
        .ok_or_else(|| StorageOpenError::Storage("group identity is missing".to_owned()))?;
    if actual.format_version != STORAGE_FORMAT_VERSION {
        return Err(StorageOpenError::UnsupportedVersion(actual.format_version));
    }
    if &actual != expected {
        return Err(StorageOpenError::IdentityConflict {
            expected: expected.clone(),
            actual,
        });
    }
    group_db
        .snapshot_catalog()
        .and_then(|catalog| catalog.reconcile_on_open())
        .map_err(storage_open)?;
    group_db.migrate_state_banks().map_err(storage_open)?;
    group_db.migrate_schema().map_err(storage_open)?;
    group_db
        .migrate_current_snapshot_artifact()
        .map_err(storage_open)?;
    Ok(StoreHandles::new(group_db))
}

fn db_options(create: bool, budget: GroupStorageBudget) -> Options {
    let mut options = Options::default();
    options.create_if_missing(create);
    options.set_atomic_flush(true);
    options.set_use_fsync(true);
    options.set_paranoid_checks(true);
    options.set_db_write_buffer_size(budget.write_buffer_bytes);
    let cache = Cache::new_lru_cache(budget.cache_bytes);
    let mut table = BlockBasedOptions::default();
    table.set_block_cache(&cache);
    options.set_block_based_table_factory(&table);
    options
}

fn descriptors() -> Vec<ColumnFamilyDescriptor> {
    COLUMN_FAMILIES
        .into_iter()
        .map(|name| ColumnFamilyDescriptor::new(name, Options::default()))
        .collect()
}

fn storage_open(error: impl fmt::Display) -> StorageOpenError {
    StorageOpenError::Storage(error.to_string())
}

fn io_error(error: impl fmt::Display) -> io::Error {
    io::Error::other(error.to_string())
}

fn encode<T: Serialize>(value: &T) -> io::Result<Vec<u8>> {
    let body = serde_json::to_vec(value).map_err(io_error)?;
    let mut checksum = Crc32::new();
    checksum.update(&body);
    serde_json::to_vec(&StoredValue {
        version: STORAGE_FORMAT_VERSION,
        checksum: checksum.finalize(),
        body,
    })
    .map_err(io_error)
}

fn decode<T: DeserializeOwned>(bytes: &[u8]) -> io::Result<T> {
    let stored: StoredValue = serde_json::from_slice(bytes).map_err(io_error)?;
    if stored.version != STORAGE_FORMAT_VERSION {
        return Err(io_error(format!(
            "unsupported stored value version {}",
            stored.version
        )));
    }
    let mut checksum = Crc32::new();
    checksum.update(&stored.body);
    if checksum.finalize() != stored.checksum {
        return Err(io_error("stored value checksum mismatch"));
    }
    serde_json::from_slice(&stored.body).map_err(io_error)
}

impl GroupDb {
    fn initialize_control_topology(&self, topology: &ClusterTopology) -> io::Result<()> {
        if self.identity.kind != GroupKind::Control {
            return Err(io_error("cluster topology belongs to the control group"));
        }
        match self.get::<ClusterTopology>(CF_STATE, KEY_CLUSTER_TOPOLOGY)? {
            Some(_) => {}
            None => self.put_sync(CF_STATE, KEY_CLUSTER_TOPOLOGY, topology)?,
        }
        self.migrate_control_snapshot_topology()?;
        Ok(())
    }

    fn migrate_control_snapshot_topology(&self) -> io::Result<()> {
        let topology = self
            .get::<ClusterTopology>(CF_STATE, KEY_CLUSTER_TOPOLOGY)?
            .ok_or_else(|| io_error("control topology is missing"))?;
        let Some((meta, artifact)) = self.current_snapshot_artifact()? else {
            return Ok(());
        };
        let topology_value = encode(&topology)?;
        let catalog = self.snapshot_catalog()?;
        let mut writer = match artifact.reader() {
            Ok(reader) => catalog.begin_artifact(
                reader.storage_format_version(),
                reader.identity_json(),
                reader.meta_json(),
            )?,
            Err(_) if artifact.len() <= MAX_SNAPSHOT_BYTES as u64 => {
                let bytes = artifact.read_all_limited(MAX_SNAPSHOT_BYTES)?;
                let mut bundle = decode_snapshot_bundle(&bytes)?;
                let identity = serde_json::to_vec(&bundle.identity).map_err(io_error)?;
                let metadata = serde_json::to_vec(&bundle.meta).map_err(io_error)?;
                let mut writer =
                    catalog.begin_artifact(bundle.format_version, &identity, &metadata)?;
                if bundle
                    .state
                    .iter()
                    .any(|(key, _)| key.as_slice() == KEY_CLUSTER_TOPOLOGY)
                {
                    return Ok(());
                }
                bundle
                    .state
                    .push((KEY_CLUSTER_TOPOLOGY.to_vec(), topology_value));
                bundle.state.sort_by(|left, right| left.0.cmp(&right.0));
                for (key, value) in bundle.state {
                    writer.write_state(&key, &value)?;
                }
                for (key, value) in bundle.payloads {
                    writer.write_payload(&key, &value)?;
                }
                let (_, descriptor) = writer.finish()?;
                self.put_sync(
                    CF_SNAPSHOT,
                    KEY_CURRENT_SNAPSHOT,
                    &StoredCurrentSnapshot {
                        artifact: descriptor.clone(),
                        meta,
                    },
                )?;
                catalog.collect_except(&descriptor)?;
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let mut reader = artifact.reader()?;
        let mut inserted = false;
        while let Some(record) = reader.next_record()? {
            match record {
                SnapshotRecord::State { key, value } => {
                    if key.as_slice() == KEY_CLUSTER_TOPOLOGY {
                        return Ok(());
                    }
                    if !inserted && key.as_slice() >= KEY_CLUSTER_TOPOLOGY {
                        writer.write_state(KEY_CLUSTER_TOPOLOGY, &topology_value)?;
                        inserted = true;
                    }
                    writer.write_state(&key, &value)?;
                }
                SnapshotRecord::Payload { key, value } => {
                    if !inserted {
                        writer.write_state(KEY_CLUSTER_TOPOLOGY, &topology_value)?;
                        inserted = true;
                    }
                    writer.write_payload(&key, &value)?;
                }
            }
        }
        if !inserted {
            writer.write_state(KEY_CLUSTER_TOPOLOGY, &topology_value)?;
        }
        let (_, descriptor) = writer.finish()?;
        self.put_sync(
            CF_SNAPSHOT,
            KEY_CURRENT_SNAPSHOT,
            &StoredCurrentSnapshot {
                artifact: descriptor.clone(),
                meta,
            },
        )?;
        catalog.collect_except(&descriptor)
    }

    fn cf(&self, name: &str) -> io::Result<Arc<rocksdb::BoundColumnFamily<'_>>> {
        let resolved = if name == CF_STATE {
            self.active_state_bank()?.column_family()
        } else {
            name
        };
        self.raw_cf(resolved)
    }

    fn raw_cf(&self, name: &str) -> io::Result<Arc<rocksdb::BoundColumnFamily<'_>>> {
        self.db
            .cf_handle(name)
            .ok_or_else(|| io_error(format!("missing column family {name}")))
    }

    fn active_state_bank(&self) -> io::Result<StateBank> {
        self.state_bank
            .read()
            .map(|bank| *bank)
            .map_err(|_| io_error("state bank lock poisoned"))
    }

    fn migrate_state_banks(&self) -> io::Result<()> {
        if let Some(bank) = self.get::<StateBank>(CF_META, KEY_ACTIVE_STATE_BANK)? {
            *self
                .state_bank
                .write()
                .map_err(|_| io_error("state bank lock poisoned"))? = bank;
            self.clear_legacy_state()?;
            return Ok(());
        }
        let legacy = self.raw_cf(CF_STATE)?;
        let target = self.raw_cf(CF_STATE_A)?;
        let mut batch = WriteBatch::default();
        let mut pending = 0_u32;
        for item in self
            .db
            .iterator_cf(&legacy, IteratorMode::From(b"", Direction::Forward))
        {
            let (key, value) = item.map_err(io_error)?;
            batch.put_cf(&target, key, value);
            pending += 1;
            if pending == 1024 {
                self.write_sync(std::mem::take(&mut batch))?;
                pending = 0;
            }
        }
        if pending != 0 {
            self.write_sync(batch)?;
        }
        self.migrate_payload_owner_banks()?;
        self.put_sync(CF_META, KEY_ACTIVE_STATE_BANK, &StateBank::A)?;
        *self
            .state_bank
            .write()
            .map_err(|_| io_error("state bank lock poisoned"))? = StateBank::A;
        self.clear_legacy_state()
    }

    fn migrate_payload_owner_banks(&self) -> io::Result<()> {
        let payload = self.raw_cf(CF_PAYLOAD)?;
        let mut batch = WriteBatch::default();
        let mut pending = 0_u32;
        for item in self.db.iterator_cf(
            &payload,
            IteratorMode::From(PAYLOAD_OWNERS_PREFIX, Direction::Forward),
        ) {
            let (key, value) = item.map_err(io_error)?;
            if !key.starts_with(PAYLOAD_OWNERS_PREFIX) {
                break;
            }
            let mut owners: PayloadOwners = decode(&value)?;
            if owners.applied_state {
                owners.set_applied_in(StateBank::A, true);
            }
            if owners.reachable() {
                batch.put_cf(&payload, key, encode(&owners)?);
            } else {
                let id = &key[PAYLOAD_OWNERS_PREFIX.len()..];
                batch.delete_cf(&payload, &key);
                batch.delete_cf(&payload, payload_bytes_key(id));
            }
            pending += 1;
            if pending == 1024 {
                self.write_sync(std::mem::take(&mut batch))?;
                pending = 0;
            }
        }
        if pending != 0 {
            self.write_sync(batch)?;
        }
        Ok(())
    }

    fn clear_legacy_state(&self) -> io::Result<()> {
        let legacy = self.raw_cf(CF_STATE)?;
        let mut batch = WriteBatch::default();
        let mut pending = 0_u32;
        for item in self
            .db
            .iterator_cf(&legacy, IteratorMode::From(b"", Direction::Forward))
        {
            let (key, _) = item.map_err(io_error)?;
            batch.delete_cf(&legacy, key);
            pending += 1;
            if pending == 1024 {
                self.write_sync(std::mem::take(&mut batch))?;
                pending = 0;
            }
        }
        if pending != 0 {
            self.write_sync(batch)?;
        }
        Ok(())
    }

    fn get<T: DeserializeOwned>(&self, cf: &str, key: &[u8]) -> io::Result<Option<T>> {
        let state_guard = if cf == CF_STATE {
            Some(
                self.state_bank
                    .read()
                    .map_err(|_| io_error("state bank lock poisoned"))?,
            )
        } else {
            None
        };
        let handle = match state_guard.as_deref() {
            Some(bank) => self.raw_cf(bank.column_family())?,
            None => self.raw_cf(cf)?,
        };
        self.db
            .get_cf(&handle, key)
            .map_err(io_error)?
            .map(|bytes| decode(&bytes))
            .transpose()
    }

    fn snapshot_catalog(&self) -> io::Result<SnapshotCatalog> {
        SnapshotCatalog::open(&self.group_root)
    }

    fn current_snapshot_value(&self) -> io::Result<Option<Vec<u8>>> {
        self.db
            .get_cf(&self.cf(CF_SNAPSHOT)?, KEY_CURRENT_SNAPSHOT)
            .map_err(io_error)
            .map(|value| value.map(|bytes| bytes.to_vec()))
    }

    fn current_snapshot_artifact(
        &self,
    ) -> io::Result<Option<(GroupSnapshotMeta, SnapshotArtifact)>> {
        let Some(bytes) = self.current_snapshot_value()? else {
            return Ok(None);
        };
        let current: StoredCurrentSnapshot = decode(&bytes)?;
        let artifact = self
            .snapshot_catalog()?
            .open_descriptor(&current.artifact)?;
        Ok(Some((current.meta, artifact)))
    }

    fn migrate_current_snapshot_artifact(&self) -> io::Result<()> {
        let Some(bytes) = self.current_snapshot_value()? else {
            return Ok(());
        };
        if let Ok(current) = decode::<StoredCurrentSnapshot>(&bytes) {
            self.snapshot_catalog()?
                .open_descriptor(&current.artifact)?;
            return Ok(());
        }
        let artifact_bytes = if bytes.starts_with(SNAPSHOT_MAGIC) {
            bytes
        } else {
            decode::<Vec<u8>>(&bytes)?
        };
        let bundle = decode_snapshot_bundle(&artifact_bytes)?;
        let (_, descriptor) = self.snapshot_catalog()?.store_bytes(&artifact_bytes)?;
        self.put_sync(
            CF_SNAPSHOT,
            KEY_CURRENT_SNAPSHOT,
            &StoredCurrentSnapshot {
                artifact: descriptor,
                meta: bundle.meta,
            },
        )
    }

    fn put_sync<T: Serialize>(&self, cf: &str, key: &[u8], value: &T) -> io::Result<()> {
        let _guard = self.write_lane.enter()?;
        let mut batch = WriteBatch::default();
        batch.put_cf(&self.cf(cf)?, key, encode(value)?);
        self.write_sync(batch)
    }

    fn write_sync(&self, batch: WriteBatch) -> io::Result<()> {
        let mut options = WriteOptions::default();
        options.disable_wal(false);
        options.set_sync(true);
        self.db.write_opt(batch, &options).map_err(io_error)
    }

    fn scan_prefix(&self, cf: &str, prefix: &[u8]) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let state_guard = if cf == CF_STATE {
            Some(
                self.state_bank
                    .read()
                    .map_err(|_| io_error("state bank lock poisoned"))?,
            )
        } else {
            None
        };
        let handle = match state_guard.as_deref() {
            Some(bank) => self.raw_cf(bank.column_family())?,
            None => self.raw_cf(cf)?,
        };
        let mut values = Vec::new();
        for item in self
            .db
            .iterator_cf(&handle, IteratorMode::From(prefix, Direction::Forward))
        {
            let (key, value) = item.map_err(io_error)?;
            if !key.starts_with(prefix) {
                break;
            }
            values.push((key.to_vec(), value.to_vec()));
        }
        Ok(values)
    }

    fn migrate_schema(&self) -> io::Result<()> {
        let schema = self
            .get::<u32>(CF_META, KEY_SCHEMA_VERSION)?
            .unwrap_or(LEGACY_SCHEMA_VERSION);
        if schema == CURRENT_SCHEMA_VERSION {
            return Ok(());
        }
        if schema != LEGACY_SCHEMA_VERSION {
            return Err(io_error(format!("unsupported record schema {schema}")));
        }
        loop {
            let cursor = self.get::<Vec<u8>>(CF_META, KEY_SCHEMA_MIGRATION_CURSOR)?;
            let records = self.migration_record_batch(cursor.as_deref())?;
            let _guard = self.write_lane.enter()?;
            let mut write = WriteBatch::default();
            let meta = self.cf(CF_META)?;
            if records.is_empty() {
                write.put_cf(&meta, KEY_SCHEMA_VERSION, encode(&CURRENT_SCHEMA_VERSION)?);
                write.delete_cf(&meta, KEY_SCHEMA_MIGRATION_CURSOR);
                self.write_sync(write)?;
                return Ok(());
            }
            let state = self.cf(CF_STATE)?;
            let mut partitions = Vec::<(PartitionKey, PartitionRetentionState)>::new();
            let mut last_key = None;
            for (key, value) in records {
                let partition = partition_from_record_key(&key)?;
                let index = match partitions
                    .iter()
                    .position(|(candidate, _)| *candidate == partition)
                {
                    Some(index) => index,
                    None => {
                        let retention = self
                            .get::<PartitionRetentionState>(CF_STATE, &retention_key(partition))?
                            .unwrap_or_default();
                        partitions.push((partition, retention));
                        partitions.len() - 1
                    }
                };
                let mut record = decode::<StoredRecord>(&value)?;
                let payload_bytes = if record.payload_bytes == 0 {
                    let stored = self
                        .db
                        .get_cf(
                            &self.cf(CF_PAYLOAD)?,
                            payload_bytes_key(&record.payload_key),
                        )
                        .map_err(io_error)?
                        .ok_or_else(|| io_error("migration record payload is missing"))?;
                    let bytes = decode_payload_value(&stored)?;
                    u64::try_from(bytes.len()).map_err(io_error)?
                } else {
                    record.payload_bytes
                };
                let retention = &mut partitions[index].1;
                retention.next_byte_position = retention
                    .next_byte_position
                    .checked_add(payload_bytes)
                    .ok_or_else(|| io_error("partition byte position overflow during migration"))?;
                record.payload_bytes = payload_bytes;
                record.cumulative_end_bytes = retention.next_byte_position;
                write.put_cf(&state, &key, encode(&record)?);
                last_key = Some(key);
            }
            for (partition, retention) in partitions {
                write.put_cf(&state, retention_key(partition), encode(&retention)?);
            }
            write.put_cf(
                &meta,
                KEY_SCHEMA_MIGRATION_CURSOR,
                encode(&last_key.expect("nonempty migration batch has a last key"))?,
            );
            self.write_sync(write)?;
        }
    }

    fn migration_record_batch(&self, cursor: Option<&[u8]>) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let state = self.cf(CF_STATE)?;
        let start = cursor.unwrap_or(RECORD_PREFIX);
        let mut records = Vec::with_capacity(MIGRATION_BATCH_RECORDS);
        for item in self
            .db
            .iterator_cf(&state, IteratorMode::From(start, Direction::Forward))
        {
            let (key, value) = item.map_err(io_error)?;
            if !key.starts_with(RECORD_PREFIX) {
                break;
            }
            if cursor.is_some_and(|cursor| key.as_ref() <= cursor) {
                continue;
            }
            records.push((key.to_vec(), value.to_vec()));
            if records.len() == MIGRATION_BATCH_RECORDS {
                break;
            }
        }
        Ok(records)
    }
}

fn partition_from_record_key(key: &[u8]) -> io::Result<PartitionKey> {
    let expected = RECORD_PREFIX.len() + 16 + 4 + 8;
    if key.len() != expected || !key.starts_with(RECORD_PREFIX) {
        return Err(io_error("invalid record key during schema migration"));
    }
    let stream_start = RECORD_PREFIX.len();
    let stream_end = stream_start + 16;
    let stream = StreamId::from_uuid(
        uuid::Uuid::from_slice(&key[stream_start..stream_end]).map_err(io_error)?,
    );
    let partition = u32::from_be_bytes(
        key[stream_end..stream_end + 4]
            .try_into()
            .map_err(io_error)?,
    );
    Ok(PartitionKey::new(stream, PartitionId::new(partition)))
}

fn partition_from_retention_key(key: &[u8]) -> io::Result<PartitionKey> {
    let expected = RETENTION_PREFIX.len() + 16 + 4;
    if key.len() != expected || !key.starts_with(RETENTION_PREFIX) {
        return Err(io_error("invalid retention key"));
    }
    let stream_start = RETENTION_PREFIX.len();
    let stream_end = stream_start + 16;
    let stream = StreamId::from_uuid(
        uuid::Uuid::from_slice(&key[stream_start..stream_end]).map_err(io_error)?,
    );
    let partition = u32::from_be_bytes(key[stream_end..].try_into().map_err(io_error)?);
    Ok(PartitionKey::new(stream, PartitionId::new(partition)))
}

fn legacy_fingerprint(batch: &PublishBatch) -> io::Result<String> {
    #[derive(Serialize)]
    struct Fingerprint<'a> {
        cluster: ClusterId,
        partition: PartitionKey,
        request: &'a ProducerRequestId,
        records: &'a [Vec<u8>],
    }
    let bytes = serde_json::to_vec(&Fingerprint {
        cluster: batch.cluster(),
        partition: batch.partition(),
        request: batch.request(),
        records: batch.records(),
    })
    .map_err(io_error)?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn fingerprint(batch: &PublishBatch) -> io::Result<String> {
    fn update_bytes(digest: &mut Sha256, value: &[u8]) -> io::Result<()> {
        digest.update(u64::try_from(value.len()).map_err(io_error)?.to_be_bytes());
        digest.update(value);
        Ok(())
    }

    let mut digest = Sha256::new();
    digest.update(b"light-stream-publish-v2");
    digest.update(batch.cluster().as_uuid().as_bytes());
    digest.update(batch.partition().stream().as_uuid().as_bytes());
    digest.update(batch.partition().partition().get().to_be_bytes());
    update_bytes(&mut digest, batch.request().principal().as_str().as_bytes())?;
    digest.update(batch.request().session().as_uuid().as_bytes());
    digest.update(batch.request().sequence().get().to_be_bytes());
    digest.update(
        u64::try_from(batch.records().len())
            .map_err(io_error)?
            .to_be_bytes(),
    );
    for record in batch.records() {
        update_bytes(&mut digest, record)?;
    }
    match batch.bookmark() {
        Some(bookmark) => {
            digest.update([1]);
            update_bytes(&mut digest, bookmark.as_str().as_bytes())?;
        }
        None => digest.update([0]),
    }
    Ok(format!("v2:{:x}", digest.finalize()))
}

fn retention_fingerprint(request: &RetentionRequest) -> io::Result<String> {
    let bytes = serde_json::to_vec(request).map_err(io_error)?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn mutation_fingerprint(value: &impl Serialize) -> io::Result<String> {
    let bytes = serde_json::to_vec(value).map_err(io_error)?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn payload_id(log_id: GroupLogId, slot: usize) -> Vec<u8> {
    let mut key = Vec::with_capacity(28);
    key.extend_from_slice(&log_id.leader_id.term.to_be_bytes());
    key.extend_from_slice(&log_id.leader_id.node_id.to_be_bytes());
    key.extend_from_slice(&log_id.index.to_be_bytes());
    key.extend_from_slice(&(slot as u32).to_be_bytes());
    key
}

fn payload_bytes_key(id: &[u8]) -> Vec<u8> {
    [PAYLOAD_BYTES_PREFIX, id].concat()
}

fn payload_owners_key(id: &[u8]) -> Vec<u8> {
    [PAYLOAD_OWNERS_PREFIX, id].concat()
}

fn record_key(partition: PartitionKey, offset: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(RECORD_PREFIX.len() + 16 + 4 + 8);
    key.extend_from_slice(RECORD_PREFIX);
    key.extend_from_slice(partition.stream().as_uuid().as_bytes());
    key.extend_from_slice(&partition.partition().get().to_be_bytes());
    key.extend_from_slice(&offset.to_be_bytes());
    key
}

fn record_prefix(partition: PartitionKey) -> Vec<u8> {
    let mut key = Vec::with_capacity(RECORD_PREFIX.len() + 16 + 4);
    key.extend_from_slice(RECORD_PREFIX);
    key.extend_from_slice(partition.stream().as_uuid().as_bytes());
    key.extend_from_slice(&partition.partition().get().to_be_bytes());
    key
}

fn next_offset_key(partition: PartitionKey) -> Vec<u8> {
    let mut key = Vec::with_capacity(KEY_NEXT_OFFSET.len() + 20);
    key.extend_from_slice(KEY_NEXT_OFFSET);
    key.extend_from_slice(partition.stream().as_uuid().as_bytes());
    key.extend_from_slice(&partition.partition().get().to_be_bytes());
    key
}

fn retention_key(partition: PartitionKey) -> Vec<u8> {
    let mut key = Vec::with_capacity(RETENTION_PREFIX.len() + 20);
    key.extend_from_slice(RETENTION_PREFIX);
    key.extend_from_slice(partition.stream().as_uuid().as_bytes());
    key.extend_from_slice(&partition.partition().get().to_be_bytes());
    key
}

fn stream_key(stream: StreamId) -> Vec<u8> {
    [STREAM_PREFIX, stream.as_uuid().as_bytes()].concat()
}

fn stream_name_key(name: &StreamName) -> Vec<u8> {
    [STREAM_NAME_PREFIX, name.as_str().as_bytes()].concat()
}

fn create_intent_key(request: CatalogRequestId) -> Vec<u8> {
    [CREATE_INTENT_PREFIX, request.as_uuid().as_bytes()].concat()
}

fn administration_request_key(request: AdministrationRequestId) -> Vec<u8> {
    [ADMINISTRATION_REQUEST_PREFIX, request.as_uuid().as_bytes()].concat()
}

fn receipt_key(partition: PartitionKey, request: &ProducerRequestId) -> io::Result<Vec<u8>> {
    let body = serde_json::to_vec(request).map_err(io_error)?;
    let mut digest = Sha256::new();
    digest.update(partition.stream().as_uuid().as_bytes());
    digest.update(partition.partition().get().to_be_bytes());
    digest.update(body);
    Ok([RECEIPT_PREFIX, digest.finalize().as_slice()].concat())
}

fn session_key(partition: PartitionKey, request: &ProducerRequestId) -> Vec<u8> {
    let mut digest = Sha256::new();
    digest.update(partition.stream().as_uuid().as_bytes());
    digest.update(partition.partition().get().to_be_bytes());
    digest.update(request.principal().as_str().as_bytes());
    digest.update(request.session().as_uuid().as_bytes());
    [SESSION_PREFIX, digest.finalize().as_slice()].concat()
}

fn mutation_receipt_key(request: &MutationRequestId) -> io::Result<Vec<u8>> {
    let body = serde_json::to_vec(request).map_err(io_error)?;
    let mut digest = Sha256::new();
    digest.update(body);
    Ok([MUTATION_RECEIPT_PREFIX, digest.finalize().as_slice()].concat())
}

fn mutation_session_key(request: &MutationRequestId) -> Vec<u8> {
    let mut digest = Sha256::new();
    digest.update(request.principal().as_str().as_bytes());
    digest.update(request.session().as_uuid().as_bytes());
    [MUTATION_SESSION_PREFIX, digest.finalize().as_slice()].concat()
}

fn checkpoint_key(key: &CheckpointKey) -> io::Result<Vec<u8>> {
    let consumer = key.consumer().as_str().as_bytes();
    let consumer_len = u32::try_from(consumer.len()).map_err(io_error)?;
    let mut stored = Vec::with_capacity(CHECKPOINT_PREFIX.len() + 16 + 4 + 4 + consumer.len());
    stored.extend_from_slice(CHECKPOINT_PREFIX);
    stored.extend_from_slice(key.partition().stream().as_uuid().as_bytes());
    stored.extend_from_slice(&key.partition().partition().get().to_be_bytes());
    stored.extend_from_slice(&consumer_len.to_be_bytes());
    stored.extend_from_slice(consumer);
    Ok(stored)
}

fn replay_lease_id_key(id: ReplayLeaseId) -> Vec<u8> {
    [LEASE_ID_PREFIX, id.as_uuid().as_bytes()].concat()
}

fn replay_lease_request_key(request: &MutationRequestId) -> io::Result<Vec<u8>> {
    let body = serde_json::to_vec(request).map_err(io_error)?;
    Ok([LEASE_REQUEST_PREFIX, Sha256::digest(body).as_slice()].concat())
}

fn replay_lease_id(request: &MutationRequestId) -> io::Result<ReplayLeaseId> {
    let body = serde_json::to_vec(request).map_err(io_error)?;
    let digest = Sha256::digest(body);
    let bytes: [u8; 16] = digest[..16]
        .try_into()
        .map_err(|_| io_error("replay lease digest width is invalid"))?;
    Ok(ReplayLeaseId::from_uuid(uuid::Uuid::from_bytes(bytes)))
}

fn bookmark_id_key(id: BookmarkId) -> Vec<u8> {
    [BOOKMARK_ID_PREFIX, id.as_uuid().as_bytes()].concat()
}

fn bookmark_name_prefix(partition: PartitionKey) -> Vec<u8> {
    let mut key = Vec::with_capacity(BOOKMARK_NAME_PREFIX.len() + 20);
    key.extend_from_slice(BOOKMARK_NAME_PREFIX);
    key.extend_from_slice(partition.stream().as_uuid().as_bytes());
    key.extend_from_slice(&partition.partition().get().to_be_bytes());
    key
}

fn bookmark_name_key(partition: PartitionKey, name: &BookmarkName) -> Vec<u8> {
    let mut key = bookmark_name_prefix(partition);
    key.extend_from_slice(name.as_str().as_bytes());
    key
}

fn bookmark_order_prefix(partition: PartitionKey) -> Vec<u8> {
    let mut key = Vec::with_capacity(BOOKMARK_ORDER_PREFIX.len() + 20);
    key.extend_from_slice(BOOKMARK_ORDER_PREFIX);
    key.extend_from_slice(partition.stream().as_uuid().as_bytes());
    key.extend_from_slice(&partition.partition().get().to_be_bytes());
    key
}

fn bookmark_order_key(
    partition: PartitionKey,
    publication: BookmarkPublicationSequence,
) -> Vec<u8> {
    let mut key = bookmark_order_prefix(partition);
    key.extend_from_slice(&publication.get().to_be_bytes());
    key
}

fn bookmark_publication_key(partition: PartitionKey) -> Vec<u8> {
    let mut key = Vec::with_capacity(BOOKMARK_PUBLICATION_PREFIX.len() + 20);
    key.extend_from_slice(BOOKMARK_PUBLICATION_PREFIX);
    key.extend_from_slice(partition.stream().as_uuid().as_bytes());
    key.extend_from_slice(&partition.partition().get().to_be_bytes());
    key
}

fn stream_bookmark_id_key(id: BookmarkId) -> Vec<u8> {
    [STREAM_BOOKMARK_ID_PREFIX, id.as_uuid().as_bytes()].concat()
}

fn stream_bookmark_name_key(stream: StreamId, name: &BookmarkName) -> Vec<u8> {
    let mut key = Vec::with_capacity(STREAM_BOOKMARK_NAME_PREFIX.len() + 16 + name.as_str().len());
    key.extend_from_slice(STREAM_BOOKMARK_NAME_PREFIX);
    key.extend_from_slice(stream.as_uuid().as_bytes());
    key.extend_from_slice(name.as_str().as_bytes());
    key
}

fn stream_bookmark_order_prefix(stream: StreamId) -> Vec<u8> {
    [STREAM_BOOKMARK_ORDER_PREFIX, stream.as_uuid().as_bytes()].concat()
}

fn stream_bookmark_order_key(
    stream: StreamId,
    publication: BookmarkPublicationSequence,
) -> Vec<u8> {
    let mut key = stream_bookmark_order_prefix(stream);
    key.extend_from_slice(&publication.get().to_be_bytes());
    key
}

fn stream_bookmark_publication_key(stream: StreamId) -> Vec<u8> {
    [
        STREAM_BOOKMARK_PUBLICATION_PREFIX,
        stream.as_uuid().as_bytes(),
    ]
    .concat()
}

fn bookmark_id(log_id: GroupLogId) -> BookmarkId {
    publish_bookmark_id(log_id, None)
}

fn publish_bookmark_id(log_id: GroupLogId, request_ordinal: Option<usize>) -> BookmarkId {
    let mut digest = Sha256::new();
    digest.update(log_id.leader_id.term.to_be_bytes());
    digest.update(log_id.leader_id.node_id.to_be_bytes());
    digest.update(log_id.index.to_be_bytes());
    digest.update(b"bookmark");
    if let Some(request_ordinal) = request_ordinal {
        digest.update(
            u64::try_from(request_ordinal)
                .expect("publish request ordinal fits in u64")
                .to_be_bytes(),
        );
    }
    let bytes: [u8; 16] = digest.finalize()[..16]
        .try_into()
        .expect("SHA-256 prefix has a fixed length");
    BookmarkId::from_uuid(uuid::Uuid::from_bytes(bytes))
}

fn range_start<R: RangeBounds<u64>>(range: &R) -> u64 {
    match range.start_bound() {
        Bound::Included(value) => *value,
        Bound::Excluded(value) => value.saturating_add(1),
        Bound::Unbounded => 0,
    }
}

fn range_end<R: RangeBounds<u64>>(range: &R) -> Option<u64> {
    match range.end_bound() {
        Bound::Included(value) => value.checked_add(1),
        Bound::Excluded(value) => Some(*value),
        Bound::Unbounded => None,
    }
}

fn thin_entry(entry: GroupEntry) -> io::Result<ThinEntryWrite> {
    let log_id = entry.log_id;
    match entry.payload {
        EntryPayload::Blank => Ok((
            ThinEntry {
                log_id,
                payload: ThinPayload::Blank,
            },
            Vec::new(),
        )),
        EntryPayload::Membership(membership) => Ok((
            ThinEntry {
                log_id,
                payload: ThinPayload::Membership(membership),
            },
            Vec::new(),
        )),
        EntryPayload::Normal(command) => match command {
            GroupCommand::BootstrapControl {
                spec,
                topology,
                security,
                data_groups,
                max_streams,
                max_partitions_per_stream,
            } => Ok((
                ThinEntry {
                    log_id,
                    payload: ThinPayload::Normal(ThinCommand::BootstrapControl {
                        spec,
                        topology,
                        security: security.map(Box::new),
                        data_groups,
                        max_streams,
                        max_partitions_per_stream,
                    }),
                },
                Vec::new(),
            )),
            GroupCommand::BootstrapData { spec } => Ok((
                ThinEntry {
                    log_id,
                    payload: ThinPayload::Normal(ThinCommand::BootstrapData { spec }),
                },
                Vec::new(),
            )),
            GroupCommand::CreateStreamIntent { spec, stream_id } => Ok((
                ThinEntry {
                    log_id,
                    payload: ThinPayload::Normal(ThinCommand::CreateStreamIntent {
                        spec,
                        stream_id,
                    }),
                },
                Vec::new(),
            )),
            GroupCommand::ReplicaReady {
                stream_id,
                group_id,
            } => Ok((
                ThinEntry {
                    log_id,
                    payload: ThinPayload::Normal(ThinCommand::ReplicaReady {
                        stream_id,
                        group_id,
                    }),
                },
                Vec::new(),
            )),
            GroupCommand::ActivateStream { stream_id } => Ok((
                ThinEntry {
                    log_id,
                    payload: ThinPayload::Normal(ThinCommand::ActivateStream { stream_id }),
                },
                Vec::new(),
            )),
            GroupCommand::BeginDeleteStream { stream_id } => Ok((
                ThinEntry {
                    log_id,
                    payload: ThinPayload::Normal(ThinCommand::BeginDeleteStream { stream_id }),
                },
                Vec::new(),
            )),
            GroupCommand::FinishDeleteStream { stream_id } => Ok((
                ThinEntry {
                    log_id,
                    payload: ThinPayload::Normal(ThinCommand::FinishDeleteStream { stream_id }),
                },
                Vec::new(),
            )),
            GroupCommand::Publish { batch } => {
                let digest = legacy_fingerprint(&batch)?;
                let mut payloads = Vec::with_capacity(batch.records().len());
                let mut keys = Vec::with_capacity(batch.records().len());
                for (slot, record) in batch.records().iter().enumerate() {
                    let key = payload_id(log_id, slot);
                    payloads.push((key.clone(), record.clone()));
                    keys.push(key);
                }
                Ok((
                    ThinEntry {
                        log_id,
                        payload: ThinPayload::Normal(ThinCommand::Publish {
                            cluster: batch.cluster(),
                            partition: batch.partition(),
                            request: batch.request().clone(),
                            fingerprint: digest,
                            payload_keys: keys,
                            bookmark: batch.bookmark().cloned(),
                        }),
                    },
                    payloads,
                ))
            }
            GroupCommand::PublishMany { batch } => {
                let mut payloads = Vec::with_capacity(batch.record_count());
                let mut requests = Vec::with_capacity(batch.requests().len());
                let mut slot = 0usize;
                for publish in batch.requests() {
                    let mut payload_keys = Vec::with_capacity(publish.records().len());
                    for record in publish.records() {
                        let key = payload_id(log_id, slot);
                        slot = slot
                            .checked_add(1)
                            .ok_or_else(|| io_error("publish payload slot overflow"))?;
                        payloads.push((key.clone(), record.clone()));
                        payload_keys.push(key);
                    }
                    requests.push(ThinPublish {
                        cluster: publish.cluster(),
                        partition: publish.partition(),
                        request: publish.request().clone(),
                        fingerprint: fingerprint(publish)?,
                        payload_keys,
                        bookmark: publish.bookmark().cloned(),
                    });
                }
                Ok((
                    ThinEntry {
                        log_id,
                        payload: ThinPayload::Normal(ThinCommand::PublishMany { requests }),
                    },
                    payloads,
                ))
            }
            GroupCommand::CompareAndSetCheckpoint { mutation } => Ok((
                ThinEntry {
                    log_id,
                    payload: ThinPayload::Normal(ThinCommand::CompareAndSetCheckpoint { mutation }),
                },
                Vec::new(),
            )),
            GroupCommand::CreateBookmark {
                id,
                partition,
                name,
                offset,
            } => Ok((
                ThinEntry {
                    log_id,
                    payload: ThinPayload::Normal(ThinCommand::CreateBookmark {
                        id,
                        partition,
                        name,
                        offset,
                    }),
                },
                Vec::new(),
            )),
            GroupCommand::DeleteBookmark { partition, id } => Ok((
                ThinEntry {
                    log_id,
                    payload: ThinPayload::Normal(ThinCommand::DeleteBookmark { partition, id }),
                },
                Vec::new(),
            )),
            GroupCommand::CreateStreamBookmark { id, name, vector } => Ok((
                ThinEntry {
                    log_id,
                    payload: ThinPayload::Normal(ThinCommand::CreateStreamBookmark {
                        id,
                        name,
                        vector,
                    }),
                },
                Vec::new(),
            )),
            GroupCommand::DeleteStreamBookmark { stream_id, id } => Ok((
                ThinEntry {
                    log_id,
                    payload: ThinPayload::Normal(ThinCommand::DeleteStreamBookmark {
                        stream_id,
                        id,
                    }),
                },
                Vec::new(),
            )),
            GroupCommand::AdvanceRetention { request, clock } => Ok((
                ThinEntry {
                    log_id,
                    payload: ThinPayload::Normal(ThinCommand::AdvanceRetention { request, clock }),
                },
                Vec::new(),
            )),
            GroupCommand::AdmitReplayLease { request, clock } => Ok((
                ThinEntry {
                    log_id,
                    payload: ThinPayload::Normal(ThinCommand::AdmitReplayLease { request, clock }),
                },
                Vec::new(),
            )),
            GroupCommand::RenewReplayLease { request, clock } => Ok((
                ThinEntry {
                    log_id,
                    payload: ThinPayload::Normal(ThinCommand::RenewReplayLease { request, clock }),
                },
                Vec::new(),
            )),
            GroupCommand::ReleaseReplayLease { request, clock } => Ok((
                ThinEntry {
                    log_id,
                    payload: ThinPayload::Normal(ThinCommand::ReleaseReplayLease {
                        request,
                        clock,
                    }),
                },
                Vec::new(),
            )),
            GroupCommand::MaintainRetention {
                partition,
                expected_cursor,
                max_records,
                max_payload_bytes,
                clock,
            } => Ok((
                ThinEntry {
                    log_id,
                    payload: ThinPayload::Normal(ThinCommand::MaintainRetention {
                        partition,
                        expected_cursor,
                        max_records,
                        max_payload_bytes,
                        clock,
                    }),
                },
                Vec::new(),
            )),
            GroupCommand::OperationalProbe { group } => Ok((
                ThinEntry {
                    log_id,
                    payload: ThinPayload::Normal(ThinCommand::OperationalProbe { group }),
                },
                Vec::new(),
            )),
            GroupCommand::BeginAdministration { intent } => Ok((
                ThinEntry {
                    log_id,
                    payload: ThinPayload::Normal(ThinCommand::BeginAdministration { intent }),
                },
                Vec::new(),
            )),
            GroupCommand::CompleteAdministration { request } => Ok((
                ThinEntry {
                    log_id,
                    payload: ThinPayload::Normal(ThinCommand::CompleteAdministration { request }),
                },
                Vec::new(),
            )),
            GroupCommand::AbortAdministration { request } => Ok((
                ThinEntry {
                    log_id,
                    payload: ThinPayload::Normal(ThinCommand::AbortAdministration { request }),
                },
                Vec::new(),
            )),
            GroupCommand::FinishAdministrationAbort { request } => Ok((
                ThinEntry {
                    log_id,
                    payload: ThinPayload::Normal(ThinCommand::FinishAdministrationAbort {
                        request,
                    }),
                },
                Vec::new(),
            )),
            GroupCommand::InitializeClusterTopology { topology } => Ok((
                ThinEntry {
                    log_id,
                    payload: ThinPayload::Normal(ThinCommand::InitializeClusterTopology {
                        topology,
                    }),
                },
                Vec::new(),
            )),
            GroupCommand::InitializeSecurityPolicy { policy } => Ok((
                ThinEntry {
                    log_id,
                    payload: ThinPayload::Normal(ThinCommand::InitializeSecurityPolicy {
                        policy: Box::new(policy),
                    }),
                },
                Vec::new(),
            )),
            GroupCommand::ApplySecurityMutation { mutation } => Ok((
                ThinEntry {
                    log_id,
                    payload: ThinPayload::Normal(ThinCommand::ApplySecurityMutation {
                        mutation: Box::new(mutation),
                    }),
                },
                Vec::new(),
            )),
            GroupCommand::ActivateSecuredTransport {
                request,
                topology,
                policy,
            } => Ok((
                ThinEntry {
                    log_id,
                    payload: ThinPayload::Normal(ThinCommand::ActivateSecuredTransport {
                        request,
                        topology,
                        policy: Box::new(policy),
                    }),
                },
                Vec::new(),
            )),
        },
    }
}

macro_rules! impl_log_storage {
    ($config:ty) => {
        impl RaftLogReader<$config> for RocksLogReader<$config> {
            async fn try_get_log_entries<RB>(
                &mut self,
                range: RB,
            ) -> Result<Vec<GroupEntry>, io::Error>
            where
                RB: RangeBounds<u64> + Clone + Debug + openraft::OptionalSend,
            {
                let start = range_start(&range);
                let end = range_end(&range);
                let handle = self.db.cf(CF_RAFT_LOG)?;
                let snapshot = self.db.db.snapshot();
                let mut entries = Vec::new();
                for item in snapshot.iterator_cf(
                    &handle,
                    IteratorMode::From(&start.to_be_bytes(), Direction::Forward),
                ) {
                    let (key, value) = item.map_err(io_error)?;
                    if key.len() != 8 {
                        return Err(io_error("invalid Raft log key"));
                    }
                    let index = u64::from_be_bytes(
                        key.as_ref()
                            .try_into()
                            .map_err(|_| io_error("invalid Raft log key width"))?,
                    );
                    if end.is_some_and(|end| index >= end) {
                        break;
                    }
                    let thin: ThinEntry = decode(&value)?;
                    let payload = match thin.payload {
                        ThinPayload::Blank => EntryPayload::Blank,
                        ThinPayload::Membership(membership) => EntryPayload::Membership(membership),
                        ThinPayload::Normal(command) => {
                            let hydrated = match command {
                                ThinCommand::BootstrapControl {
                                    spec,
                                    topology,
                                    security,
                                    data_groups,
                                    max_streams,
                                    max_partitions_per_stream,
                                } => GroupCommand::BootstrapControl {
                                    spec,
                                    topology,
                                    security: security.map(|policy| *policy),
                                    data_groups,
                                    max_streams,
                                    max_partitions_per_stream,
                                },
                                ThinCommand::BootstrapData { spec } => {
                                    GroupCommand::BootstrapData { spec }
                                }
                                ThinCommand::CreateStreamIntent { spec, stream_id } => {
                                    GroupCommand::CreateStreamIntent { spec, stream_id }
                                }
                                ThinCommand::ReplicaReady {
                                    stream_id,
                                    group_id,
                                } => GroupCommand::ReplicaReady {
                                    stream_id,
                                    group_id,
                                },
                                ThinCommand::ActivateStream { stream_id } => {
                                    GroupCommand::ActivateStream { stream_id }
                                }
                                ThinCommand::BeginDeleteStream { stream_id } => {
                                    GroupCommand::BeginDeleteStream { stream_id }
                                }
                                ThinCommand::FinishDeleteStream { stream_id } => {
                                    GroupCommand::FinishDeleteStream { stream_id }
                                }
                                ThinCommand::Publish {
                                    cluster,
                                    partition,
                                    request,
                                    fingerprint: _,
                                    payload_keys,
                                    bookmark,
                                } => {
                                    let payload_cf = self.db.cf(CF_PAYLOAD)?;
                                    let mut records = Vec::with_capacity(payload_keys.len());
                                    for payload_key in payload_keys {
                                        let bytes = snapshot
                                            .get_cf(&payload_cf, payload_bytes_key(&payload_key))
                                            .map_err(io_error)?
                                            .ok_or_else(|| {
                                                io_error("log entry references a missing payload")
                                            })?;
                                        records.push(decode_payload_value(&bytes)?);
                                    }
                                    let mut batch =
                                        PublishBatch::new(cluster, partition, request, records)
                                            .map_err(io_error)?;
                                    if let Some(bookmark) = bookmark {
                                        batch = batch.with_bookmark(bookmark);
                                    }
                                    GroupCommand::Publish { batch }
                                }
                                ThinCommand::PublishMany { requests } => {
                                    let payload_cf = self.db.cf(CF_PAYLOAD)?;
                                    let mut publishes = Vec::with_capacity(requests.len());
                                    for request in requests {
                                        let mut records =
                                            Vec::with_capacity(request.payload_keys.len());
                                        for payload_key in request.payload_keys {
                                            let bytes = snapshot
                                                .get_cf(
                                                    &payload_cf,
                                                    payload_bytes_key(&payload_key),
                                                )
                                                .map_err(io_error)?
                                                .ok_or_else(|| {
                                                    io_error(
                                                        "log entry references a missing payload",
                                                    )
                                                })?;
                                            records.push(decode_payload_value(&bytes)?);
                                        }
                                        let mut batch = PublishBatch::new(
                                            request.cluster,
                                            request.partition,
                                            request.request,
                                            records,
                                        )
                                        .map_err(io_error)?;
                                        if let Some(bookmark) = request.bookmark {
                                            batch = batch.with_bookmark(bookmark);
                                        }
                                        if fingerprint(&batch)? != request.fingerprint {
                                            return Err(io_error(
                                                "publish log fingerprint does not match payload",
                                            ));
                                        }
                                        publishes.push(batch);
                                    }
                                    GroupCommand::PublishMany {
                                        batch: ReplicatedPublishBatch::new(publishes)
                                            .map_err(io_error)?,
                                    }
                                }
                                ThinCommand::CompareAndSetCheckpoint { mutation } => {
                                    GroupCommand::CompareAndSetCheckpoint { mutation }
                                }
                                ThinCommand::CreateBookmark {
                                    id,
                                    partition,
                                    name,
                                    offset,
                                } => GroupCommand::CreateBookmark {
                                    id,
                                    partition,
                                    name,
                                    offset,
                                },
                                ThinCommand::DeleteBookmark { partition, id } => {
                                    GroupCommand::DeleteBookmark { partition, id }
                                }
                                ThinCommand::CreateStreamBookmark { id, name, vector } => {
                                    GroupCommand::CreateStreamBookmark { id, name, vector }
                                }
                                ThinCommand::DeleteStreamBookmark { stream_id, id } => {
                                    GroupCommand::DeleteStreamBookmark { stream_id, id }
                                }
                                ThinCommand::AdvanceRetention { request, clock } => {
                                    GroupCommand::AdvanceRetention { request, clock }
                                }
                                ThinCommand::AdmitReplayLease { request, clock } => {
                                    GroupCommand::AdmitReplayLease { request, clock }
                                }
                                ThinCommand::RenewReplayLease { request, clock } => {
                                    GroupCommand::RenewReplayLease { request, clock }
                                }
                                ThinCommand::ReleaseReplayLease { request, clock } => {
                                    GroupCommand::ReleaseReplayLease { request, clock }
                                }
                                ThinCommand::MaintainRetention {
                                    partition,
                                    expected_cursor,
                                    max_records,
                                    max_payload_bytes,
                                    clock,
                                } => GroupCommand::MaintainRetention {
                                    partition,
                                    expected_cursor,
                                    max_records,
                                    max_payload_bytes,
                                    clock,
                                },
                                ThinCommand::OperationalProbe { group } => {
                                    GroupCommand::OperationalProbe { group }
                                }
                                ThinCommand::BeginAdministration { intent } => {
                                    GroupCommand::BeginAdministration { intent }
                                }
                                ThinCommand::CompleteAdministration { request } => {
                                    GroupCommand::CompleteAdministration { request }
                                }
                                ThinCommand::AbortAdministration { request } => {
                                    GroupCommand::AbortAdministration { request }
                                }
                                ThinCommand::FinishAdministrationAbort { request } => {
                                    GroupCommand::FinishAdministrationAbort { request }
                                }
                                ThinCommand::InitializeClusterTopology { topology } => {
                                    GroupCommand::InitializeClusterTopology { topology }
                                }
                                ThinCommand::InitializeSecurityPolicy { policy } => {
                                    GroupCommand::InitializeSecurityPolicy { policy: *policy }
                                }
                                ThinCommand::ApplySecurityMutation { mutation } => {
                                    GroupCommand::ApplySecurityMutation {
                                        mutation: *mutation,
                                    }
                                }
                                ThinCommand::ActivateSecuredTransport {
                                    request,
                                    topology,
                                    policy,
                                } => GroupCommand::ActivateSecuredTransport {
                                    request,
                                    topology,
                                    policy: *policy,
                                },
                            };
                            EntryPayload::Normal(hydrated)
                        }
                    };
                    entries.push(GroupEntry {
                        log_id: thin.log_id,
                        payload,
                    });
                }
                Ok(entries)
            }

            async fn read_vote(&mut self) -> Result<Option<VoteOf<$config>>, io::Error> {
                self.db.get(CF_RAFT_META, KEY_VOTE)
            }

            async fn limited_get_log_entries(
                &mut self,
                start: u64,
                end: u64,
            ) -> Result<Vec<GroupEntry>, io::Error> {
                const LIMIT: usize = 8 * 1024 * 1024;
                let entries = self.try_get_log_entries(start..end).await?;
                let mut bytes = 0usize;
                let mut limited = Vec::new();
                for entry in entries {
                    let size = serde_json::to_vec(&entry).map_err(io_error)?.len();
                    if !limited.is_empty() && bytes.saturating_add(size) > LIMIT {
                        break;
                    }
                    bytes = bytes.saturating_add(size);
                    limited.push(entry);
                }
                Ok(limited)
            }
        }

        impl RaftLogStorage<$config> for RocksLogStore<$config> {
            type LogReader = RocksLogReader<$config>;

            async fn get_log_state(&mut self) -> Result<LogState<$config>, io::Error> {
                let last_purged_log_id = self.db.get(CF_RAFT_META, KEY_PURGED)?;
                let handle = self.db.cf(CF_RAFT_LOG)?;
                let last_log_id = match self
                    .db
                    .db
                    .iterator_cf(&handle, IteratorMode::End)
                    .next()
                    .transpose()
                    .map_err(io_error)?
                {
                    Some((_key, value)) => Some(decode::<ThinEntry>(&value)?.log_id),
                    None => last_purged_log_id,
                };
                Ok(LogState {
                    last_purged_log_id,
                    last_log_id,
                })
            }

            async fn get_log_reader(&mut self) -> Self::LogReader {
                RocksLogReader {
                    db: self.db.clone(),
                    marker: PhantomData,
                }
            }

            async fn save_vote(&mut self, vote: &VoteOf<$config>) -> Result<(), io::Error> {
                self.db.put_sync(CF_RAFT_META, KEY_VOTE, vote)
            }

            async fn save_committed(
                &mut self,
                committed: Option<LogIdOf<$config>>,
            ) -> Result<(), io::Error> {
                self.db.put_sync(CF_RAFT_META, KEY_COMMITTED, &committed)
            }

            async fn read_committed(&mut self) -> Result<Option<LogIdOf<$config>>, io::Error> {
                Ok(self.db.get(CF_RAFT_META, KEY_COMMITTED)?.flatten())
            }

            async fn append<I>(
                &mut self,
                entries: I,
                callback: IOFlushed<$config>,
            ) -> Result<(), io::Error>
            where
                I: IntoIterator<Item = GroupEntry> + openraft::OptionalSend,
                I::IntoIter: openraft::OptionalSend,
            {
                let result = (|| {
                    let _guard = self.db.write_lane.enter()?;
                    let mut write = WriteBatch::default();
                    let log_cf = self.db.cf(CF_RAFT_LOG)?;
                    let payload_cf = self.db.cf(CF_PAYLOAD)?;
                    for entry in entries {
                        let (thin, payloads) = thin_entry(entry)?;
                        for (key, bytes) in payloads {
                            write.put_cf(
                                &payload_cf,
                                payload_bytes_key(&key),
                                encode_payload_value(&bytes),
                            );
                            write.put_cf(
                                &payload_cf,
                                payload_owners_key(&key),
                                encode(&PayloadOwners {
                                    raft_log: true,
                                    applied_state: false,
                                    applied_banks: 0,
                                })?,
                            );
                        }
                        write.put_cf(&log_cf, thin.log_id.index.to_be_bytes(), encode(&thin)?);
                    }
                    self.db.write_sync(write)
                })();
                match result {
                    Ok(()) => {
                        callback.io_completed(Ok(()));
                        Ok(())
                    }
                    Err(error) => {
                        callback.io_completed(Err(io_error(error.to_string())));
                        Err(error)
                    }
                }
            }

            async fn truncate_after(
                &mut self,
                last_log_id: Option<LogIdOf<$config>>,
            ) -> Result<(), io::Error> {
                let start = last_log_id.map_or(0, |value| value.index.saturating_add(1));
                self.db.remove_log_range(start, None, None)
            }

            async fn purge(&mut self, log_id: LogIdOf<$config>) -> Result<(), io::Error> {
                self.db
                    .remove_log_range(0, Some(log_id.index.saturating_add(1)), Some(log_id))
            }
        }
    };
}

impl GroupDb {
    fn remove_log_range(
        &self,
        start: u64,
        end: Option<u64>,
        purged: Option<GroupLogId>,
    ) -> io::Result<()> {
        let _guard = self.write_lane.enter()?;
        let log_cf = self.cf(CF_RAFT_LOG)?;
        let payload_cf = self.cf(CF_PAYLOAD)?;
        let mut write = WriteBatch::default();
        let entries: Vec<_> = self
            .db
            .iterator_cf(
                &log_cf,
                IteratorMode::From(&start.to_be_bytes(), Direction::Forward),
            )
            .map(|item| item.map_err(io_error))
            .collect::<Result<_, _>>()?;
        for (key, value) in entries {
            let index = u64::from_be_bytes(
                key.as_ref()
                    .try_into()
                    .map_err(|_| io_error("invalid Raft log key width"))?,
            );
            if end.is_some_and(|end| index >= end) {
                break;
            }
            let thin: ThinEntry = decode(&value)?;
            let payload_keys = match thin.payload {
                ThinPayload::Normal(ThinCommand::Publish { payload_keys, .. }) => payload_keys,
                ThinPayload::Normal(ThinCommand::PublishMany { requests }) => requests
                    .into_iter()
                    .flat_map(|request| request.payload_keys)
                    .collect(),
                _ => Vec::new(),
            };
            for payload_key in payload_keys {
                let owners_key = payload_owners_key(&payload_key);
                let mut owners = self
                    .get::<PayloadOwners>(CF_PAYLOAD, &owners_key)?
                    .ok_or_else(|| io_error("missing payload ownership record"))?;
                owners.raft_log = false;
                if owners.reachable() {
                    write.put_cf(&payload_cf, owners_key, encode(&owners)?);
                } else {
                    write.delete_cf(&payload_cf, payload_bytes_key(&payload_key));
                    write.delete_cf(&payload_cf, owners_key);
                }
            }
            write.delete_cf(&log_cf, key);
        }
        if let Some(log_id) = purged {
            write.put_cf(&self.cf(CF_RAFT_META)?, KEY_PURGED, encode(&log_id)?);
        }
        self.write_sync(write)
    }
}

impl_log_storage!(ControlRaftConfig);
impl_log_storage!(DataRaftConfig);

macro_rules! impl_state_machine {
    ($config:ty) => {
        impl RaftStateMachine<$config> for RocksStateMachine<$config> {
            type SnapshotData = SnapshotArtifact;
            type SnapshotBuilder = GroupSnapshotBuilder<$config>;

            async fn applied_state(
                &mut self,
            ) -> Result<(Option<LogIdOf<$config>>, StoredMembershipOf<$config>), io::Error> {
                let applied = self.db.get(CF_STATE, KEY_APPLIED)?;
                let membership = self.db.get(CF_STATE, KEY_MEMBERSHIP)?.unwrap_or_default();
                Ok((applied, membership))
            }

            async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
            where
                Strm: Stream<Item = Result<EntryResponder<$config>, io::Error>>
                    + Unpin
                    + openraft::OptionalSend,
            {
                while let Some(item) = entries.next().await {
                    let (entry, responder) = item?;
                    let result = self.db.apply_entry(entry)?;
                    if let Some(responder) = responder {
                        responder.send(result);
                    }
                }
                Ok(())
            }

            async fn try_create_snapshot_builder(
                &mut self,
                _force: bool,
            ) -> Option<Self::SnapshotBuilder> {
                Some(GroupSnapshotBuilder {
                    db: self.db.clone(),
                    marker: PhantomData,
                })
            }

            async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
                GroupSnapshotBuilder {
                    db: self.db.clone(),
                    marker: PhantomData,
                }
            }

            async fn install_snapshot(
                &mut self,
                meta: &SnapshotMetaOf<$config>,
                snapshot: Self::SnapshotData,
            ) -> Result<(), io::Error> {
                self.db.install_snapshot(meta, &snapshot)
            }

            async fn get_current_snapshot(
                &mut self,
            ) -> Result<Option<SnapshotOf<$config, Self::SnapshotData>>, io::Error> {
                let Some((meta, artifact)) = self.db.current_snapshot_artifact()? else {
                    return Ok(None);
                };
                Ok(Some(Snapshot {
                    meta,
                    snapshot: artifact,
                }))
            }
        }

        impl RaftSnapshotBuilder<$config> for GroupSnapshotBuilder<$config> {
            type SnapshotData = SnapshotArtifact;

            async fn build_snapshot(
                &mut self,
            ) -> Result<SnapshotOf<$config, Self::SnapshotData>, io::Error> {
                let (meta, artifact) = self.db.build_snapshot().map_err(|error| {
                    eprintln!(
                        "{}",
                        serde_json::json!({
                            "event": "snapshot_build_failed",
                            "group_id": self.db.identity.group_id,
                            "error": error.to_string(),
                        })
                    );
                    error
                })?;
                Ok(Snapshot {
                    meta,
                    snapshot: artifact,
                })
            }
        }
    };
}

impl_state_machine!(ControlRaftConfig);
impl_state_machine!(DataRaftConfig);

impl<'a> PublishApplyTxn<'a> {
    fn new(db: &'a GroupDb, log_id: GroupLogId, write: &'a mut WriteBatch) -> io::Result<Self> {
        Ok(Self {
            db,
            log_id,
            write,
            active_bank: db.active_state_bank()?,
            next_offsets: HashMap::new(),
            retentions: HashMap::new(),
            sessions: BTreeMap::new(),
            outcomes: BTreeMap::new(),
            bookmark_names: BTreeMap::new(),
            bookmark_ids: BTreeMap::new(),
            bookmark_publications: HashMap::new(),
        })
    }

    fn prior_outcome(
        &self,
        batch: &PublishBatch,
        key: &[u8],
        fingerprint: &str,
    ) -> io::Result<Option<Result<PublishReceipt, DomainError>>> {
        if let Some(stored) = self.outcomes.get(key) {
            return Ok(Some(if stored.fingerprint == fingerprint {
                stored.result.as_result()
            } else {
                Err(DomainError::ReceiptConflict)
            }));
        }
        let Some(stored) = self.db.get::<StoredPublishOutcome>(CF_STATE, key)? else {
            return Ok(None);
        };
        match stored {
            StoredPublishOutcome::Versioned(stored) => {
                Ok(Some(if stored.fingerprint == fingerprint {
                    stored.result.as_result()
                } else {
                    Err(DomainError::ReceiptConflict)
                }))
            }
            StoredPublishOutcome::Legacy(stored) => {
                let same_bookmark =
                    stored.receipt.bookmark().map(CommittedBookmark::name) == batch.bookmark();
                Ok(Some(
                    if stored.fingerprint == legacy_fingerprint(batch)? && same_bookmark {
                        Ok(stored.receipt)
                    } else {
                        Err(DomainError::ReceiptConflict)
                    },
                ))
            }
        }
    }

    fn session(&mut self, key: &[u8]) -> io::Result<ProducerSessionState> {
        if let Some(session) = self.sessions.get(key) {
            return Ok(session.clone());
        }
        let session = self
            .db
            .get::<ProducerSessionState>(CF_STATE, key)?
            .unwrap_or_default();
        self.sessions.insert(key.to_vec(), session.clone());
        Ok(session)
    }

    fn store_outcome(
        &mut self,
        key: Vec<u8>,
        fingerprint: String,
        result: StoredPublishResult,
    ) -> io::Result<()> {
        let stored = VersionedStoredPublishOutcome {
            fingerprint,
            result,
        };
        self.write.put_cf(
            &self.db.cf(CF_STATE)?,
            &key,
            encode(&StoredPublishOutcome::Versioned(stored.clone()))?,
        );
        self.outcomes.insert(key, stored);
        Ok(())
    }

    fn advance_session(
        &mut self,
        batch: &PublishBatch,
        session_key: Vec<u8>,
        mut session: ProducerSessionState,
    ) -> io::Result<()> {
        let sequence = batch.request().sequence().get();
        session.highest_sequence = Some(sequence);
        session.retained_sequences.push_back(sequence);
        if session.retained_sequences.len() > self.db.receipt_window
            && let Some(expired) = session.retained_sequences.pop_front()
        {
            let expired_request = ProducerRequestId::new(
                batch.request().principal().clone(),
                batch.request().session(),
                light_stream_core::RequestSequence::new(expired),
            );
            let expired_key = receipt_key(batch.partition(), &expired_request)?;
            self.outcomes.remove(&expired_key);
            self.write.delete_cf(&self.db.cf(CF_STATE)?, expired_key);
        }
        self.write
            .put_cf(&self.db.cf(CF_STATE)?, &session_key, encode(&session)?);
        self.sessions.insert(session_key, session);
        Ok(())
    }

    fn bookmark_name_is_used(
        &mut self,
        partition: PartitionKey,
        name: &BookmarkName,
    ) -> io::Result<bool> {
        let key = bookmark_name_key(partition, name);
        if let Some(existing) = self.bookmark_names.get(&key) {
            return Ok(existing.is_some());
        }
        let existing = self.db.get::<BookmarkId>(CF_STATE, &key)?;
        self.bookmark_names.insert(key, existing);
        Ok(existing.is_some())
    }

    fn create_publish_bookmark(
        &mut self,
        request_ordinal: usize,
        partition: PartitionKey,
        name: BookmarkName,
        offset: RecordOffset,
    ) -> io::Result<Result<CommittedBookmark, DomainError>> {
        let id = publish_bookmark_id(self.log_id, Some(request_ordinal));
        let id_key = bookmark_id_key(id);
        let existing = if let Some(existing) = self.bookmark_ids.get(&id_key) {
            existing.clone()
        } else {
            let existing = self.db.get::<CommittedBookmark>(CF_STATE, &id_key)?;
            self.bookmark_ids.insert(id_key.clone(), existing.clone());
            existing
        };
        if existing.is_some() {
            return Ok(Err(DomainError::BookmarkNameConflict));
        }
        let publication =
            if let Some(publication) = self.bookmark_publications.get(&partition).copied() {
                publication
            } else {
                let publication = self
                    .db
                    .get::<u64>(CF_STATE, &bookmark_publication_key(partition))?
                    .unwrap_or_default();
                self.bookmark_publications.insert(partition, publication);
                publication
            }
            .checked_add(1)
            .ok_or_else(|| io_error("bookmark publication sequence overflow"))?;
        self.bookmark_publications.insert(partition, publication);
        let publication = BookmarkPublicationSequence::new(publication);
        let bookmark = CommittedBookmark::published(
            id,
            name.clone(),
            CommittedCursor::new(self.db.identity.cluster_id, partition, offset),
            publication,
        );
        let name_key = bookmark_name_key(partition, &name);
        let state = self.db.cf(CF_STATE)?;
        self.write.put_cf(&state, &id_key, encode(&bookmark)?);
        self.write.put_cf(&state, &name_key, encode(&id)?);
        self.write.put_cf(
            &state,
            bookmark_order_key(partition, publication),
            encode(&bookmark)?,
        );
        self.write.put_cf(
            &state,
            bookmark_publication_key(partition),
            encode(&publication.get())?,
        );
        self.bookmark_names.insert(name_key, Some(id));
        self.bookmark_ids.insert(id_key, Some(bookmark.clone()));
        Ok(Ok(bookmark))
    }

    fn apply_one(
        &mut self,
        request_ordinal: usize,
        first_payload_slot: usize,
        batch: PublishBatch,
    ) -> io::Result<PublishItemOutcome> {
        let request = batch.request().clone();
        if batch.cluster() != self.db.identity.cluster_id {
            return Ok(PublishItemOutcome::new(
                request,
                Err(DomainError::IdentityMismatch {
                    reason: "publish cluster does not match the data group".to_owned(),
                }),
            ));
        }

        let fingerprint = fingerprint(&batch)?;
        let stored_receipt_key = receipt_key(batch.partition(), batch.request())?;
        if let Some(result) = self.prior_outcome(&batch, &stored_receipt_key, &fingerprint)? {
            return Ok(PublishItemOutcome::new(request, result));
        }

        let producer_session_key = session_key(batch.partition(), batch.request());
        let session = self.session(&producer_session_key)?;
        let sequence = batch.request().sequence().get();
        if session
            .highest_sequence
            .is_some_and(|highest| sequence <= highest)
        {
            return Ok(PublishItemOutcome::new(
                request,
                Err(DomainError::ReceiptExpired),
            ));
        }

        if let Some(name) = batch.bookmark()
            && self.bookmark_name_is_used(batch.partition(), name)?
        {
            let error = DomainError::BookmarkNameConflict;
            self.store_outcome(
                stored_receipt_key,
                fingerprint,
                StoredPublishResult::Rejected(StoredPublishRejection::BookmarkNameConflict),
            )?;
            self.advance_session(&batch, producer_session_key, session)?;
            return Ok(PublishItemOutcome::new(request, Err(error)));
        }

        let first = if let Some(first) = self.next_offsets.get(&batch.partition()).copied() {
            first
        } else {
            let first = self
                .db
                .get::<u64>(CF_STATE, &next_offset_key(batch.partition()))?
                .unwrap_or_default();
            self.next_offsets.insert(batch.partition(), first);
            first
        };
        let count = u64::try_from(batch.records().len()).map_err(io_error)?;
        let range =
            match CommittedRecordRange::new(batch.partition(), RecordOffset::new(first), count) {
                Ok(range) => range,
                Err(DomainError::InvalidRange { reason }) => {
                    self.store_outcome(
                        stored_receipt_key,
                        fingerprint,
                        StoredPublishResult::Rejected(StoredPublishRejection::InvalidRange(
                            reason.clone(),
                        )),
                    )?;
                    self.advance_session(&batch, producer_session_key, session)?;
                    return Ok(PublishItemOutcome::new(
                        request,
                        Err(DomainError::InvalidRange { reason }),
                    ));
                }
                Err(error) => return Err(io_error(error)),
            };
        let mut retention =
            if let Some(retention) = self.retentions.get(&batch.partition()).cloned() {
                retention
            } else {
                let retention = self
                    .db
                    .get::<PartitionRetentionState>(CF_STATE, &retention_key(batch.partition()))?
                    .unwrap_or_default();
                self.retentions.insert(batch.partition(), retention);
                retention
            };
        let added_payload_bytes = batch.records().iter().try_fold(0u64, |total, record| {
            total
                .checked_add(u64::try_from(record.len()).map_err(io_error)?)
                .ok_or_else(|| io_error("publish payload byte count overflow"))
        })?;
        if retention
            .next_byte_position
            .checked_add(added_payload_bytes)
            .is_none()
        {
            let reason = "partition byte position overflow".to_owned();
            self.store_outcome(
                stored_receipt_key,
                fingerprint,
                StoredPublishResult::Rejected(StoredPublishRejection::InvalidRange(reason.clone())),
            )?;
            self.advance_session(&batch, producer_session_key, session)?;
            return Ok(PublishItemOutcome::new(
                request,
                Err(DomainError::InvalidRange { reason }),
            ));
        }
        let bookmark = match batch.bookmark().cloned() {
            Some(name) => match self.create_publish_bookmark(
                request_ordinal,
                batch.partition(),
                name,
                range.next(),
            )? {
                Ok(bookmark) => Some(bookmark),
                Err(error) => {
                    self.store_outcome(
                        stored_receipt_key,
                        fingerprint,
                        StoredPublishResult::Rejected(StoredPublishRejection::BookmarkNameConflict),
                    )?;
                    self.advance_session(&batch, producer_session_key, session)?;
                    return Ok(PublishItemOutcome::new(request, Err(error)));
                }
            },
            None => None,
        };
        let payload_cf = self.db.cf(CF_PAYLOAD)?;
        let state_cf = self.db.cf(CF_STATE)?;
        for (request_slot, record) in batch.records().iter().enumerate() {
            let flat_slot = first_payload_slot
                .checked_add(request_slot)
                .ok_or_else(|| io_error("publish payload slot overflow"))?;
            let payload_key = payload_id(self.log_id, flat_slot);
            let owners_key = payload_owners_key(&payload_key);
            let mut owners = self
                .db
                .get::<PayloadOwners>(CF_PAYLOAD, &owners_key)?
                .ok_or_else(|| io_error("applied entry payload is missing ownership"))?;
            owners.set_applied_in(self.active_bank, true);
            self.write.put_cf(&payload_cf, owners_key, encode(&owners)?);
            let offset = first + u64::try_from(request_slot).map_err(io_error)?;
            let payload_bytes = u64::try_from(record.len()).map_err(io_error)?;
            retention.next_byte_position = retention
                .next_byte_position
                .checked_add(payload_bytes)
                .ok_or_else(|| io_error("partition byte position overflow"))?;
            self.write.put_cf(
                &state_cf,
                record_key(batch.partition(), offset),
                encode(&StoredRecord {
                    payload_key,
                    payload_bytes,
                    cumulative_end_bytes: retention.next_byte_position,
                })?,
            );
        }
        self.write.put_cf(
            &state_cf,
            next_offset_key(batch.partition()),
            encode(&(first + count))?,
        );
        self.write.put_cf(
            &state_cf,
            retention_key(batch.partition()),
            encode(&retention)?,
        );
        self.next_offsets.insert(batch.partition(), first + count);
        self.retentions.insert(batch.partition(), retention);

        let receipt = PublishReceipt::new(request.clone(), range, bookmark);
        self.store_outcome(
            stored_receipt_key,
            fingerprint,
            StoredPublishResult::Published(receipt.clone()),
        )?;
        self.advance_session(&batch, producer_session_key, session)?;
        Ok(PublishItemOutcome::new(request, Ok(receipt)))
    }
}

impl GroupDb {
    fn apply_entry(&self, entry: GroupEntry) -> io::Result<ApplyResult> {
        let _guard = self.write_lane.enter()?;
        let mut write = WriteBatch::default();
        let state_cf = self.cf(CF_STATE)?;
        let result = match entry.payload {
            EntryPayload::Blank => ApplyResult::Noop,
            EntryPayload::Membership(membership) => {
                write.put_cf(
                    &state_cf,
                    KEY_MEMBERSHIP,
                    encode(&GroupMembership::new(Some(entry.log_id), membership))?,
                );
                ApplyResult::Noop
            }
            EntryPayload::Normal(command) => {
                self.apply_command(entry.log_id, command, &mut write)?
            }
        };
        write.put_cf(&state_cf, KEY_APPLIED, encode(&entry.log_id)?);
        self.write_sync(write)?;
        Ok(result)
    }

    fn apply_command(
        &self,
        log_id: GroupLogId,
        command: GroupCommand,
        write: &mut WriteBatch,
    ) -> io::Result<ApplyResult> {
        match command {
            GroupCommand::BootstrapControl {
                spec,
                topology,
                security,
                data_groups,
                max_streams,
                max_partitions_per_stream,
            } => self.apply_bootstrap_control(
                spec,
                BootstrapControlState { topology, security },
                data_groups,
                max_streams,
                max_partitions_per_stream,
                write,
            ),
            GroupCommand::BootstrapData { spec } => {
                self.apply_bootstrap(spec, GroupKind::Data, write)
            }
            GroupCommand::CreateStreamIntent { spec, stream_id } => {
                self.apply_create_stream(spec, stream_id, write)
            }
            GroupCommand::ReplicaReady {
                stream_id,
                group_id,
            } => self.apply_replica_ready(stream_id, group_id, write),
            GroupCommand::ActivateStream { stream_id } => {
                self.apply_activate_stream(stream_id, write)
            }
            GroupCommand::BeginDeleteStream { stream_id } => {
                self.apply_begin_delete(stream_id, write)
            }
            GroupCommand::FinishDeleteStream { stream_id } => {
                self.apply_finish_delete(stream_id, write)
            }
            GroupCommand::Publish { batch } => self.apply_publish(log_id, batch, write),
            GroupCommand::PublishMany { batch } => self.apply_publish_many(log_id, batch, write),
            GroupCommand::CompareAndSetCheckpoint { mutation } => {
                self.apply_checkpoint_mutation(mutation, write)
            }
            GroupCommand::CreateBookmark {
                id,
                partition,
                name,
                offset,
            } => self.apply_create_bookmark(id, partition, name, offset, write),
            GroupCommand::DeleteBookmark { partition, id } => {
                self.apply_delete_bookmark(partition, id, write)
            }
            GroupCommand::CreateStreamBookmark { id, name, vector } => {
                self.apply_create_stream_bookmark(id, name, vector, write)
            }
            GroupCommand::DeleteStreamBookmark { stream_id, id } => {
                self.apply_delete_stream_bookmark(stream_id, id, write)
            }
            GroupCommand::AdvanceRetention { request, clock } => {
                self.apply_advance_retention(request, clock, write)
            }
            GroupCommand::AdmitReplayLease { request, clock } => {
                self.apply_admit_replay_lease(request, clock, write)
            }
            GroupCommand::RenewReplayLease { request, clock } => {
                self.apply_renew_replay_lease(request, clock, write)
            }
            GroupCommand::ReleaseReplayLease { request, clock } => {
                self.apply_release_replay_lease(request, clock, write)
            }
            GroupCommand::MaintainRetention {
                partition,
                expected_cursor,
                max_records,
                max_payload_bytes,
                clock,
            } => self.apply_maintain_retention(
                partition,
                expected_cursor,
                max_records,
                max_payload_bytes,
                clock,
                write,
            ),
            GroupCommand::OperationalProbe { group } => {
                self.apply_operational_probe(log_id, group, write)
            }
            GroupCommand::BeginAdministration { intent } => {
                self.apply_begin_administration(intent, write)
            }
            GroupCommand::CompleteAdministration { request } => {
                self.apply_complete_administration(request, write)
            }
            GroupCommand::AbortAdministration { request } => {
                self.apply_abort_administration(request, write)
            }
            GroupCommand::FinishAdministrationAbort { request } => {
                self.apply_finish_administration_abort(request, write)
            }
            GroupCommand::InitializeClusterTopology { topology } => {
                self.apply_initialize_cluster_topology(topology, write)
            }
            GroupCommand::InitializeSecurityPolicy { policy } => {
                self.apply_initialize_security_policy(policy, write)
            }
            GroupCommand::ApplySecurityMutation { mutation } => {
                self.apply_security_mutation(mutation, write)
            }
            GroupCommand::ActivateSecuredTransport {
                request,
                topology,
                policy,
            } => self.apply_secured_transport(request, topology, policy, write),
        }
    }

    fn apply_begin_administration(
        &self,
        intent: AdministrationIntent,
        write: &mut WriteBatch,
    ) -> io::Result<ApplyResult> {
        if self.identity.kind != GroupKind::Control {
            return Ok(ApplyResult::Rejected(DomainError::IdentityMismatch {
                reason: "administration intent reached a data group".to_owned(),
            }));
        }
        let request_key = administration_request_key(intent.request());
        if let Some(existing) = self.get::<AdministrationOperation>(CF_STATE, &request_key)? {
            return if existing.intent() == &intent {
                Ok(ApplyResult::Administration(existing))
            } else {
                Ok(ApplyResult::Rejected(DomainError::MutationConflict))
            };
        }
        if self
            .get::<AdministrationOperation>(CF_STATE, KEY_ACTIVE_ADMINISTRATION)?
            .is_some()
        {
            return Ok(ApplyResult::Rejected(DomainError::ResourceLimit {
                resource: "active_administration_operations".to_owned(),
                limit: 1,
            }));
        }
        let topology = self
            .get::<ClusterTopology>(CF_STATE, KEY_CLUSTER_TOPOLOGY)?
            .ok_or_else(|| io_error("control topology is missing"))?;
        let transitional_topology = match &intent {
            AdministrationIntent::ReplaceVoter {
                expected_topology_revision,
                remove,
                add,
                ..
            } => {
                match topology.replacement_transition(
                    *expected_topology_revision,
                    *remove,
                    add.clone(),
                ) {
                    Ok(topology) => Some(topology),
                    Err(error) => return Ok(ApplyResult::Rejected(error)),
                }
            }
            AdministrationIntent::TransferLeader { group, target, .. } => {
                if !topology.desired_voters().contains(target) {
                    return Ok(ApplyResult::Rejected(DomainError::InvalidIdentity {
                        kind: "leader transfer target".to_owned(),
                        reason: "target is not a desired voter".to_owned(),
                    }));
                }
                let data_groups = self
                    .get::<Vec<GroupId>>(CF_STATE, KEY_DATA_GROUP_POOL)?
                    .unwrap_or_default();
                if group.get() != CONTROL_GROUP_ID && !data_groups.contains(group) {
                    return Ok(ApplyResult::Rejected(DomainError::InvalidIdentity {
                        kind: "leader transfer group".to_owned(),
                        reason: "group is not in the bounded group pool".to_owned(),
                    }));
                }
                None
            }
        };
        let operation = AdministrationOperation::pending(intent);
        let state = self.cf(CF_STATE)?;
        if let Some(topology) = transitional_topology {
            write.put_cf(&state, KEY_CLUSTER_TOPOLOGY, encode(&topology)?);
        }
        write.put_cf(&state, request_key, encode(&operation)?);
        write.put_cf(&state, KEY_ACTIVE_ADMINISTRATION, encode(&operation)?);
        Ok(ApplyResult::Administration(operation))
    }

    fn apply_complete_administration(
        &self,
        request: AdministrationRequestId,
        write: &mut WriteBatch,
    ) -> io::Result<ApplyResult> {
        if self.identity.kind != GroupKind::Control {
            return Ok(ApplyResult::Rejected(DomainError::IdentityMismatch {
                reason: "administration completion reached a data group".to_owned(),
            }));
        }
        let request_key = administration_request_key(request);
        let Some(operation) = self.get::<AdministrationOperation>(CF_STATE, &request_key)? else {
            return Ok(ApplyResult::Rejected(DomainError::InvalidIdentity {
                kind: "administration request".to_owned(),
                reason: "request was not found".to_owned(),
            }));
        };
        if !matches!(
            operation.lifecycle(),
            light_stream_core::AdministrationLifecycle::Pending
        ) {
            return Ok(ApplyResult::Administration(operation));
        }
        if operation.intent().request() != request {
            return Ok(ApplyResult::Rejected(DomainError::MutationConflict));
        }
        let Some(active) =
            self.get::<AdministrationOperation>(CF_STATE, KEY_ACTIVE_ADMINISTRATION)?
        else {
            return Ok(ApplyResult::Rejected(DomainError::MutationConflict));
        };
        if active.intent().request() != request {
            return Ok(ApplyResult::Rejected(DomainError::MutationConflict));
        }
        let current = self
            .get::<ClusterTopology>(CF_STATE, KEY_CLUSTER_TOPOLOGY)?
            .ok_or_else(|| io_error("control topology is missing"))?;
        let topology = match operation.intent() {
            AdministrationIntent::ReplaceVoter {
                expected_topology_revision,
                remove,
                add,
                ..
            } => current.replacement_complete(*expected_topology_revision, *remove, add.clone()),
            AdministrationIntent::TransferLeader { .. } => Ok(current),
        };
        let topology = match topology {
            Ok(topology) => topology,
            Err(error) => return Ok(ApplyResult::Rejected(error)),
        };
        let completed = operation.completed(topology.revision());
        let state = self.cf(CF_STATE)?;
        write.put_cf(&state, KEY_CLUSTER_TOPOLOGY, encode(&topology)?);
        write.put_cf(&state, request_key, encode(&completed)?);
        write.delete_cf(&state, KEY_ACTIVE_ADMINISTRATION);
        Ok(ApplyResult::Administration(completed))
    }

    fn apply_abort_administration(
        &self,
        request: AdministrationRequestId,
        write: &mut WriteBatch,
    ) -> io::Result<ApplyResult> {
        if self.identity.kind != GroupKind::Control {
            return Ok(ApplyResult::Rejected(DomainError::IdentityMismatch {
                reason: "administration abort reached a data group".to_owned(),
            }));
        }
        let request_key = administration_request_key(request);
        let Some(operation) = self.get::<AdministrationOperation>(CF_STATE, &request_key)? else {
            return Ok(ApplyResult::Rejected(DomainError::InvalidIdentity {
                kind: "administration request".to_owned(),
                reason: "request was not found".to_owned(),
            }));
        };
        if !matches!(
            operation.lifecycle(),
            light_stream_core::AdministrationLifecycle::Pending
        ) {
            return Ok(ApplyResult::Administration(operation));
        }
        let Some(active) =
            self.get::<AdministrationOperation>(CF_STATE, KEY_ACTIVE_ADMINISTRATION)?
        else {
            return Ok(ApplyResult::Rejected(DomainError::MutationConflict));
        };
        if active.intent().request() != request {
            return Ok(ApplyResult::Rejected(DomainError::MutationConflict));
        }
        let current = self
            .get::<ClusterTopology>(CF_STATE, KEY_CLUSTER_TOPOLOGY)?
            .ok_or_else(|| io_error("control topology is missing"))?;
        let topology_revision = match operation.intent() {
            AdministrationIntent::ReplaceVoter {
                expected_topology_revision,
                remove,
                add,
                ..
            } => {
                let topology = match current.replacement_abort(
                    *expected_topology_revision,
                    *remove,
                    add.clone(),
                ) {
                    Ok(topology) => topology,
                    Err(error) => return Ok(ApplyResult::Rejected(error)),
                };
                let revision = topology.revision();
                write.put_cf(
                    &self.cf(CF_STATE)?,
                    KEY_CLUSTER_TOPOLOGY,
                    encode(&topology)?,
                );
                revision
            }
            AdministrationIntent::TransferLeader { .. } => current.revision(),
        };
        let aborted = operation.aborted(topology_revision);
        let state = self.cf(CF_STATE)?;
        write.put_cf(&state, request_key, encode(&aborted)?);
        write.put_cf(&state, KEY_ACTIVE_ADMINISTRATION, encode(&aborted)?);
        Ok(ApplyResult::Administration(aborted))
    }

    fn apply_finish_administration_abort(
        &self,
        request: AdministrationRequestId,
        write: &mut WriteBatch,
    ) -> io::Result<ApplyResult> {
        let request_key = administration_request_key(request);
        let Some(operation) = self.get::<AdministrationOperation>(CF_STATE, &request_key)? else {
            return Ok(ApplyResult::Rejected(DomainError::InvalidIdentity {
                kind: "administration request".to_owned(),
                reason: "request was not found".to_owned(),
            }));
        };
        if !matches!(
            operation.lifecycle(),
            light_stream_core::AdministrationLifecycle::Aborted { .. }
        ) {
            return Ok(ApplyResult::Rejected(DomainError::MutationConflict));
        }
        let Some(active) =
            self.get::<AdministrationOperation>(CF_STATE, KEY_ACTIVE_ADMINISTRATION)?
        else {
            return Ok(ApplyResult::Administration(operation));
        };
        if active.intent().request() != request {
            return Ok(ApplyResult::Rejected(DomainError::MutationConflict));
        }
        write.delete_cf(&self.cf(CF_STATE)?, KEY_ACTIVE_ADMINISTRATION);
        Ok(ApplyResult::Administration(operation))
    }

    fn apply_initialize_cluster_topology(
        &self,
        topology: ClusterTopology,
        write: &mut WriteBatch,
    ) -> io::Result<ApplyResult> {
        if self.identity.kind != GroupKind::Control {
            return Ok(ApplyResult::Rejected(DomainError::IdentityMismatch {
                reason: "cluster topology initialization reached a data group".to_owned(),
            }));
        }
        if let Some(existing) = self.get::<ClusterTopology>(CF_STATE, KEY_CLUSTER_TOPOLOGY)? {
            return if existing == topology {
                Ok(ApplyResult::Noop)
            } else {
                Ok(ApplyResult::Rejected(DomainError::IdentityMismatch {
                    reason: "cluster topology conflicts with existing control state".to_owned(),
                }))
            };
        }
        write.put_cf(
            &self.cf(CF_STATE)?,
            KEY_CLUSTER_TOPOLOGY,
            encode(&topology)?,
        );
        Ok(ApplyResult::Noop)
    }

    fn apply_initialize_security_policy(
        &self,
        policy: SecurityPolicy,
        write: &mut WriteBatch,
    ) -> io::Result<ApplyResult> {
        if self.identity.kind != GroupKind::Control {
            return Ok(ApplyResult::Rejected(DomainError::IdentityMismatch {
                reason: "security policy initialization reached a data group".to_owned(),
            }));
        }
        if policy.cluster() != self.identity.cluster_id {
            return Ok(ApplyResult::Rejected(DomainError::IdentityMismatch {
                reason: "security policy cluster does not match the control group".to_owned(),
            }));
        }
        if let Some(existing) = self.get::<SecurityPolicy>(CF_STATE, KEY_SECURITY_POLICY)? {
            return if existing == policy {
                Ok(ApplyResult::SecurityPolicy(existing))
            } else {
                Ok(ApplyResult::Rejected(DomainError::SecurityPolicyConflict))
            };
        }
        write.put_cf(&self.cf(CF_STATE)?, KEY_SECURITY_POLICY, encode(&policy)?);
        Ok(ApplyResult::SecurityPolicy(policy))
    }

    fn apply_security_mutation(
        &self,
        mutation: SecurityMutation,
        write: &mut WriteBatch,
    ) -> io::Result<ApplyResult> {
        if self.identity.kind != GroupKind::Control {
            return Ok(ApplyResult::Rejected(DomainError::IdentityMismatch {
                reason: "security policy mutation reached a data group".to_owned(),
            }));
        }
        let fingerprint = mutation_fingerprint(&mutation)?;
        if let Some(result) = self.prior_mutation_result(mutation.request(), &fingerprint)? {
            return Ok(result);
        }
        let Some(policy) = self.get::<SecurityPolicy>(CF_STATE, KEY_SECURITY_POLICY)? else {
            return Ok(ApplyResult::Rejected(DomainError::SecurityPolicyConflict));
        };
        let request = mutation.request().clone();
        let result = match policy.apply(mutation) {
            Ok(policy) => {
                write.put_cf(&self.cf(CF_STATE)?, KEY_SECURITY_POLICY, encode(&policy)?);
                ApplyResult::SecurityPolicy(policy)
            }
            Err(error) => ApplyResult::Rejected(error),
        };
        self.store_mutation_result(&request, fingerprint, &result, write)?;
        Ok(result)
    }

    fn apply_secured_transport(
        &self,
        request: MutationRequestId,
        topology: ClusterTopology,
        policy: SecurityPolicy,
        write: &mut WriteBatch,
    ) -> io::Result<ApplyResult> {
        if self.identity.kind != GroupKind::Control {
            return Ok(ApplyResult::Rejected(DomainError::IdentityMismatch {
                reason: "secured transport activation reached a data group".to_owned(),
            }));
        }
        let fingerprint =
            mutation_fingerprint(&(request.clone(), topology.clone(), policy.clone()))?;
        if let Some(result) = self.prior_mutation_result(&request, &fingerprint)? {
            return Ok(result);
        }
        let current = self
            .get::<ClusterTopology>(CF_STATE, KEY_CLUSTER_TOPOLOGY)?
            .ok_or_else(|| io_error("control topology is missing"))?;
        if let Some(existing) = self.get::<SecurityPolicy>(CF_STATE, KEY_SECURITY_POLICY)? {
            return if existing == policy && current == topology {
                Ok(ApplyResult::SecurityPolicy(existing))
            } else {
                Ok(ApplyResult::Rejected(DomainError::SecurityPolicyConflict))
            };
        }
        if topology.revision() != current.revision().saturating_add(1)
            || topology.desired_voters() != current.desired_voters()
            || topology.authorized_nodes().keys().collect::<BTreeSet<_>>()
                != current.authorized_nodes().keys().collect::<BTreeSet<_>>()
            || topology.authorized_nodes().values().any(|node| {
                !node.public_uri().starts_with("https://")
                    || !node.peer_uri().starts_with("https://")
            })
            || policy.cluster() != self.identity.cluster_id
            || topology
                .authorized_nodes()
                .keys()
                .any(|node| !policy.has_active_peer(*node))
        {
            return Ok(ApplyResult::Rejected(DomainError::SecurityPolicyConflict));
        }
        let state = self.cf(CF_STATE)?;
        write.put_cf(&state, KEY_CLUSTER_TOPOLOGY, encode(&topology)?);
        write.put_cf(&state, KEY_SECURITY_POLICY, encode(&policy)?);
        let result = ApplyResult::SecurityPolicy(policy);
        self.store_mutation_result(&request, fingerprint, &result, write)?;
        Ok(result)
    }

    fn apply_operational_probe(
        &self,
        log_id: GroupLogId,
        group: GroupId,
        write: &mut WriteBatch,
    ) -> io::Result<ApplyResult> {
        if group != self.identity.group_id {
            return Ok(ApplyResult::Rejected(DomainError::IdentityMismatch {
                reason: "operational probe reached another group".to_owned(),
            }));
        }
        let proof = OperationalProof::new(
            group,
            NodeId::new(log_id.leader_id.node_id).map_err(io_error)?,
            log_id.leader_id.term,
            log_id.index,
        );
        write.put_cf(&self.cf(CF_STATE)?, KEY_OPERATIONAL_PROOF, encode(&proof)?);
        Ok(ApplyResult::OperationalProof(proof))
    }

    fn apply_bootstrap_control(
        &self,
        spec: BootstrapSpec,
        state: BootstrapControlState,
        data_groups: Vec<GroupId>,
        max_streams: u32,
        max_partitions: u32,
        write: &mut WriteBatch,
    ) -> io::Result<ApplyResult> {
        let BootstrapControlState { topology, security } = state;
        if data_groups.is_empty()
            || max_streams == 0
            || max_partitions == 0
            || data_groups
                .iter()
                .any(|group| group.get() == CONTROL_GROUP_ID)
        {
            return Ok(ApplyResult::Rejected(DomainError::InvalidRange {
                reason: "invalid bounded catalog configuration".to_owned(),
            }));
        }
        let result = self.apply_bootstrap(spec.clone(), GroupKind::Control, write)?;
        if matches!(result, ApplyResult::Rejected(_)) {
            return Ok(result);
        }
        if let Some(existing) = self.get::<Vec<GroupId>>(CF_STATE, KEY_DATA_GROUP_POOL)? {
            let stored_topology = self.get::<ClusterTopology>(CF_STATE, KEY_CLUSTER_TOPOLOGY)?;
            let stored_security = self.get::<SecurityPolicy>(CF_STATE, KEY_SECURITY_POLICY)?;
            if existing != data_groups
                || topology
                    .as_ref()
                    .is_some_and(|topology| stored_topology.as_ref() != Some(topology))
                || security
                    .as_ref()
                    .is_some_and(|security| stored_security.as_ref() != Some(security))
                || self.get::<u32>(CF_STATE, KEY_MAX_STREAMS)? != Some(max_streams)
                || self.get::<u32>(CF_STATE, KEY_MAX_PARTITIONS)? != Some(max_partitions)
            {
                return Ok(ApplyResult::Rejected(DomainError::BootstrapConflict {
                    reason: "bounded group pool or catalog limits differ".to_owned(),
                }));
            }
            return Ok(result);
        }
        let group = data_groups[0];
        let descriptor = StreamDescriptor::new(
            spec.cluster(),
            spec.stream(),
            spec.stream_name().clone(),
            StreamLifecycle::Active,
            vec![PartitionPlacement::new(PartitionId::new(0), group)],
            vec![group],
            1,
        );
        let state = self.cf(CF_STATE)?;
        write.put_cf(&state, KEY_DATA_GROUP_POOL, encode(&data_groups)?);
        if let Some(topology) = topology {
            write.put_cf(&state, KEY_CLUSTER_TOPOLOGY, encode(&topology)?);
        }
        if let Some(security) = security {
            if security.cluster() != spec.cluster() {
                return Ok(ApplyResult::Rejected(DomainError::IdentityMismatch {
                    reason: "bootstrap security policy cluster differs".to_owned(),
                }));
            }
            write.put_cf(&state, KEY_SECURITY_POLICY, encode(&security)?);
        }
        write.put_cf(&state, KEY_MAX_STREAMS, encode(&max_streams)?);
        write.put_cf(&state, KEY_MAX_PARTITIONS, encode(&max_partitions)?);
        write.put_cf(&state, KEY_ASSIGNMENT_CURSOR, encode(&1_u64)?);
        write.put_cf(&state, KEY_CATALOG_REVISION, encode(&1_u64)?);
        write.put_cf(&state, stream_key(spec.stream()), encode(&descriptor)?);
        write.put_cf(
            &state,
            stream_name_key(spec.stream_name()),
            encode(&spec.stream())?,
        );
        Ok(result)
    }

    fn apply_bootstrap(
        &self,
        spec: BootstrapSpec,
        expected_kind: GroupKind,
        write: &mut WriteBatch,
    ) -> io::Result<ApplyResult> {
        if self.identity.kind != expected_kind {
            return Ok(ApplyResult::Rejected(DomainError::IdentityMismatch {
                reason: format!(
                    "{} command reached {} group",
                    expected_kind, self.identity.kind
                ),
            }));
        }
        if spec.cluster() != self.identity.cluster_id {
            return Ok(ApplyResult::Rejected(DomainError::IdentityMismatch {
                reason: "bootstrap cluster does not match the group manifest".to_owned(),
            }));
        }
        if let Some(existing) = self.get::<BootstrapSpec>(CF_STATE, KEY_BOOTSTRAP)? {
            if existing != spec {
                return Ok(ApplyResult::Rejected(DomainError::BootstrapConflict {
                    reason: "bootstrap stream identity differs".to_owned(),
                }));
            }
        } else {
            write.put_cf(&self.cf(CF_STATE)?, KEY_BOOTSTRAP, encode(&spec)?);
        }
        Ok(ApplyResult::Bootstrapped(BootstrapResult::new(
            spec.cluster(),
            spec.stream(),
            spec.stream_name().clone(),
            GroupId::new(CONTROL_GROUP_ID).map_err(io_error)?,
            GroupId::new(DATA_GROUP_ID).map_err(io_error)?,
        )))
    }

    fn apply_create_stream(
        &self,
        spec: CreateStreamSpec,
        stream_id: StreamId,
        write: &mut WriteBatch,
    ) -> io::Result<ApplyResult> {
        if self.identity.kind != GroupKind::Control {
            return Ok(ApplyResult::Rejected(DomainError::IdentityMismatch {
                reason: "create stream intent reached a data group".to_owned(),
            }));
        }
        let Some(bootstrap) = self.get::<BootstrapSpec>(CF_STATE, KEY_BOOTSTRAP)? else {
            return Ok(ApplyResult::Rejected(DomainError::NotBootstrapped));
        };
        if let Some(existing) =
            self.get::<StoredCreateIntent>(CF_STATE, &create_intent_key(spec.request_id()))?
        {
            if existing.spec != spec {
                return Ok(ApplyResult::Rejected(DomainError::BootstrapConflict {
                    reason: "catalog request identity was reused with different intent".to_owned(),
                }));
            }
            return Ok(ApplyResult::Stream(
                self.get(CF_STATE, &stream_key(existing.stream_id))?
                    .ok_or_else(|| io_error("catalog intent references a missing stream"))?,
            ));
        }
        if self
            .get::<StreamId>(CF_STATE, &stream_name_key(spec.name()))?
            .is_some()
        {
            return Ok(ApplyResult::Rejected(DomainError::StreamNameConflict));
        }
        let max_streams = self
            .get::<u32>(CF_STATE, KEY_MAX_STREAMS)?
            .ok_or_else(|| io_error("catalog stream limit is missing"))?;
        let max_partitions = self
            .get::<u32>(CF_STATE, KEY_MAX_PARTITIONS)?
            .ok_or_else(|| io_error("catalog partition limit is missing"))?;
        if spec.partition_count() > max_partitions {
            return Ok(ApplyResult::Rejected(DomainError::ResourceLimit {
                resource: "partitions_per_stream".to_owned(),
                limit: u64::from(max_partitions),
            }));
        }
        let existing_count = self
            .scan_prefix(CF_STATE, STREAM_PREFIX)?
            .into_iter()
            .filter_map(|(_, value)| decode::<StreamDescriptor>(&value).ok())
            .filter(|value| value.lifecycle() != StreamLifecycle::Deleted)
            .count() as u64;
        if existing_count >= u64::from(max_streams) {
            return Ok(ApplyResult::Rejected(DomainError::ResourceLimit {
                resource: "streams".to_owned(),
                limit: u64::from(max_streams),
            }));
        }
        if self
            .get::<StreamDescriptor>(CF_STATE, &stream_key(stream_id))?
            .is_some()
        {
            return Ok(ApplyResult::Rejected(DomainError::IdentityMismatch {
                reason: "new stream ID already exists".to_owned(),
            }));
        }
        let groups = self
            .get::<Vec<GroupId>>(CF_STATE, KEY_DATA_GROUP_POOL)?
            .ok_or_else(|| io_error("data group pool is missing"))?;
        let cursor = self
            .get::<u64>(CF_STATE, KEY_ASSIGNMENT_CURSOR)?
            .unwrap_or_default();
        let revision = self.next_catalog_revision(write)?;
        let placements = (0..spec.partition_count())
            .map(|partition| {
                let index = (cursor + u64::from(partition)) % groups.len() as u64;
                PartitionPlacement::new(PartitionId::new(partition), groups[index as usize])
            })
            .collect();
        let descriptor = StreamDescriptor::new(
            bootstrap.cluster(),
            stream_id,
            spec.name().clone(),
            StreamLifecycle::Preparing,
            placements,
            Vec::new(),
            revision,
        );
        let state = self.cf(CF_STATE)?;
        write.put_cf(
            &state,
            create_intent_key(spec.request_id()),
            encode(&StoredCreateIntent {
                spec: spec.clone(),
                stream_id,
            })?,
        );
        write.put_cf(&state, stream_key(stream_id), encode(&descriptor)?);
        write.put_cf(&state, stream_name_key(spec.name()), encode(&stream_id)?);
        write.put_cf(
            &state,
            KEY_ASSIGNMENT_CURSOR,
            encode(&(cursor + u64::from(spec.partition_count())))?,
        );
        Ok(ApplyResult::Stream(descriptor))
    }

    fn load_stream_result(
        &self,
        stream_id: StreamId,
    ) -> io::Result<Result<StreamDescriptor, ApplyResult>> {
        Ok(self
            .get(CF_STATE, &stream_key(stream_id))?
            .ok_or(ApplyResult::Rejected(DomainError::StreamNotFound)))
    }

    fn apply_replica_ready(
        &self,
        stream_id: StreamId,
        group_id: GroupId,
        write: &mut WriteBatch,
    ) -> io::Result<ApplyResult> {
        let mut descriptor = match self.load_stream_result(stream_id)? {
            Ok(value) => value,
            Err(value) => return Ok(value),
        };
        if descriptor.lifecycle() != StreamLifecycle::Preparing {
            return Ok(ApplyResult::Stream(descriptor));
        }
        if !descriptor
            .placements()
            .iter()
            .any(|value| value.group() == group_id)
        {
            return Ok(ApplyResult::Rejected(DomainError::StaleRoute));
        }
        let mut ready = descriptor.ready_groups().to_vec();
        if !ready.contains(&group_id) {
            ready.push(group_id);
            ready.sort();
            descriptor = StreamDescriptor::new(
                descriptor.cluster(),
                descriptor.stream(),
                descriptor.name().clone(),
                descriptor.lifecycle(),
                descriptor.placements().to_vec(),
                ready,
                self.next_catalog_revision(write)?,
            );
            write.put_cf(
                &self.cf(CF_STATE)?,
                stream_key(stream_id),
                encode(&descriptor)?,
            );
        }
        Ok(ApplyResult::Stream(descriptor))
    }

    fn apply_activate_stream(
        &self,
        stream_id: StreamId,
        write: &mut WriteBatch,
    ) -> io::Result<ApplyResult> {
        let descriptor = match self.load_stream_result(stream_id)? {
            Ok(value) => value,
            Err(value) => return Ok(value),
        };
        if descriptor.lifecycle() == StreamLifecycle::Active {
            return Ok(ApplyResult::Stream(descriptor));
        }
        if descriptor.lifecycle() != StreamLifecycle::Preparing {
            return Ok(ApplyResult::Rejected(DomainError::StreamNotActive));
        }
        let required = descriptor
            .placements()
            .iter()
            .map(PartitionPlacement::group)
            .collect::<BTreeSet<_>>();
        let ready = descriptor
            .ready_groups()
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if required != ready {
            return Ok(ApplyResult::Rejected(DomainError::ClusterForming));
        }
        let active = StreamDescriptor::new(
            descriptor.cluster(),
            descriptor.stream(),
            descriptor.name().clone(),
            StreamLifecycle::Active,
            descriptor.placements().to_vec(),
            descriptor.ready_groups().to_vec(),
            self.next_catalog_revision(write)?,
        );
        write.put_cf(&self.cf(CF_STATE)?, stream_key(stream_id), encode(&active)?);
        Ok(ApplyResult::Stream(active))
    }

    fn apply_begin_delete(
        &self,
        stream_id: StreamId,
        write: &mut WriteBatch,
    ) -> io::Result<ApplyResult> {
        let descriptor = match self.load_stream_result(stream_id)? {
            Ok(value) => value,
            Err(value) => return Ok(value),
        };
        if matches!(
            descriptor.lifecycle(),
            StreamLifecycle::Deleting | StreamLifecycle::Deleted
        ) {
            return Ok(ApplyResult::Stream(descriptor));
        }
        if descriptor.lifecycle() != StreamLifecycle::Active {
            return Ok(ApplyResult::Rejected(DomainError::StreamNotActive));
        }
        let deleting = StreamDescriptor::new(
            descriptor.cluster(),
            descriptor.stream(),
            descriptor.name().clone(),
            StreamLifecycle::Deleting,
            descriptor.placements().to_vec(),
            descriptor.ready_groups().to_vec(),
            self.next_catalog_revision(write)?,
        );
        write.put_cf(
            &self.cf(CF_STATE)?,
            stream_key(stream_id),
            encode(&deleting)?,
        );
        Ok(ApplyResult::Stream(deleting))
    }

    fn apply_finish_delete(
        &self,
        stream_id: StreamId,
        write: &mut WriteBatch,
    ) -> io::Result<ApplyResult> {
        let descriptor = match self.load_stream_result(stream_id)? {
            Ok(value) => value,
            Err(value) => return Ok(value),
        };
        if descriptor.lifecycle() == StreamLifecycle::Deleted {
            return Ok(ApplyResult::Stream(descriptor));
        }
        if descriptor.lifecycle() != StreamLifecycle::Deleting {
            return Ok(ApplyResult::Rejected(DomainError::StreamNotActive));
        }
        let deleted = StreamDescriptor::new(
            descriptor.cluster(),
            descriptor.stream(),
            descriptor.name().clone(),
            StreamLifecycle::Deleted,
            descriptor.placements().to_vec(),
            descriptor.ready_groups().to_vec(),
            self.next_catalog_revision(write)?,
        );
        let state = self.cf(CF_STATE)?;
        write.put_cf(&state, stream_key(stream_id), encode(&deleted)?);
        write.delete_cf(&state, stream_name_key(descriptor.name()));
        Ok(ApplyResult::Stream(deleted))
    }

    fn next_catalog_revision(&self, write: &mut WriteBatch) -> io::Result<u64> {
        let revision = self
            .get::<u64>(CF_STATE, KEY_CATALOG_REVISION)?
            .unwrap_or_default()
            .saturating_add(1);
        write.put_cf(
            &self.cf(CF_STATE)?,
            KEY_CATALOG_REVISION,
            encode(&revision)?,
        );
        Ok(revision)
    }

    fn create_bookmark_record(
        &self,
        id: BookmarkId,
        partition: PartitionKey,
        name: BookmarkName,
        offset: RecordOffset,
        maximum_offset: RecordOffset,
        write: &mut WriteBatch,
    ) -> io::Result<ApplyResult> {
        if self.identity.kind != GroupKind::Data {
            return Ok(ApplyResult::Rejected(DomainError::IdentityMismatch {
                reason: "bookmark command reached the control group".to_owned(),
            }));
        }
        if offset.get() > maximum_offset.get() {
            return Ok(ApplyResult::Rejected(DomainError::InvalidRange {
                reason: format!(
                    "bookmark offset {} exceeds committed tail {}",
                    offset.get(),
                    maximum_offset.get()
                ),
            }));
        }
        let id_key = bookmark_id_key(id);
        if let Some(existing) = self.get::<CommittedBookmark>(CF_STATE, &id_key)? {
            return if existing.name() == &name
                && existing.cursor().partition() == partition
                && existing.cursor().next_offset() == offset
            {
                Ok(ApplyResult::Bookmark(existing))
            } else {
                Ok(ApplyResult::Rejected(DomainError::BookmarkNameConflict))
            };
        }
        let name_key = bookmark_name_key(partition, &name);
        if self.get::<BookmarkId>(CF_STATE, &name_key)?.is_some() {
            return Ok(ApplyResult::Rejected(DomainError::BookmarkNameConflict));
        }
        let publication_key = bookmark_publication_key(partition);
        let publication = BookmarkPublicationSequence::new(
            self.get::<u64>(CF_STATE, &publication_key)?
                .unwrap_or_default()
                .saturating_add(1),
        );
        let bookmark = CommittedBookmark::published(
            id,
            name,
            CommittedCursor::new(self.identity.cluster_id, partition, offset),
            publication,
        );
        let state = self.cf(CF_STATE)?;
        write.put_cf(&state, id_key, encode(&bookmark)?);
        write.put_cf(&state, name_key, encode(&id)?);
        write.put_cf(
            &state,
            bookmark_order_key(partition, publication),
            encode(&bookmark)?,
        );
        write.put_cf(&state, publication_key, encode(&publication.get())?);
        Ok(ApplyResult::Bookmark(bookmark))
    }

    fn apply_create_bookmark(
        &self,
        id: BookmarkId,
        partition: PartitionKey,
        name: BookmarkName,
        offset: RecordOffset,
        write: &mut WriteBatch,
    ) -> io::Result<ApplyResult> {
        let maximum = RecordOffset::new(
            self.get::<u64>(CF_STATE, &next_offset_key(partition))?
                .unwrap_or_default(),
        );
        self.create_bookmark_record(id, partition, name, offset, maximum, write)
    }

    fn apply_delete_bookmark(
        &self,
        partition: PartitionKey,
        id: BookmarkId,
        write: &mut WriteBatch,
    ) -> io::Result<ApplyResult> {
        let id_key = bookmark_id_key(id);
        let Some(mut bookmark) = self.get::<CommittedBookmark>(CF_STATE, &id_key)? else {
            return Ok(ApplyResult::Rejected(DomainError::BookmarkNotFound));
        };
        if bookmark.cursor().partition() != partition {
            return Ok(ApplyResult::Rejected(DomainError::IdentityMismatch {
                reason: "bookmark belongs to another partition".to_owned(),
            }));
        }
        if bookmark.lifecycle() == light_stream_core::BookmarkLifecycle::Deleted {
            return Ok(ApplyResult::Bookmark(bookmark));
        }
        let state = self.cf(CF_STATE)?;
        write.delete_cf(&state, bookmark_name_key(partition, bookmark.name()));
        write.delete_cf(
            &state,
            bookmark_order_key(partition, bookmark.publication()),
        );
        bookmark.mark_deleted();
        write.put_cf(&state, id_key, encode(&bookmark)?);
        Ok(ApplyResult::Bookmark(bookmark))
    }

    fn apply_create_stream_bookmark(
        &self,
        id: BookmarkId,
        name: BookmarkName,
        vector: StreamCursorVector,
        write: &mut WriteBatch,
    ) -> io::Result<ApplyResult> {
        if self.identity.kind != GroupKind::Control {
            return Ok(ApplyResult::Rejected(DomainError::IdentityMismatch {
                reason: "stream bookmark reached a data group".to_owned(),
            }));
        }
        if vector.cluster() != self.identity.cluster_id {
            return Ok(ApplyResult::Rejected(DomainError::IdentityMismatch {
                reason: "stream bookmark cluster does not match the control group".to_owned(),
            }));
        }
        let Some(stream) = self.get::<StreamDescriptor>(CF_STATE, &stream_key(vector.stream()))?
        else {
            return Ok(ApplyResult::Rejected(DomainError::StreamNotFound));
        };
        if stream.lifecycle() != StreamLifecycle::Active {
            return Ok(ApplyResult::Rejected(DomainError::StreamNotActive));
        }
        let wanted = stream
            .placements()
            .iter()
            .map(PartitionPlacement::partition)
            .collect::<BTreeSet<_>>();
        let actual = vector
            .positions()
            .iter()
            .map(|cursor| cursor.partition().partition())
            .collect::<BTreeSet<_>>();
        if wanted != actual || actual.len() != vector.positions().len() {
            return Ok(ApplyResult::Rejected(DomainError::InvalidRange {
                reason: "stream bookmark must contain one position for every stream partition"
                    .to_owned(),
            }));
        }
        let id_key = stream_bookmark_id_key(id);
        if let Some(existing) = self.get::<CommittedStreamBookmark>(CF_STATE, &id_key)? {
            return if existing.name() == &name && existing.vector() == &vector {
                Ok(ApplyResult::StreamBookmark(existing))
            } else {
                Ok(ApplyResult::Rejected(DomainError::BookmarkNameConflict))
            };
        }
        let name_key = stream_bookmark_name_key(vector.stream(), &name);
        if self.get::<BookmarkId>(CF_STATE, &name_key)?.is_some() {
            return Ok(ApplyResult::Rejected(DomainError::BookmarkNameConflict));
        }
        let publication_key = stream_bookmark_publication_key(vector.stream());
        let publication = BookmarkPublicationSequence::new(
            self.get::<u64>(CF_STATE, &publication_key)?
                .unwrap_or_default()
                .saturating_add(1),
        );
        let bookmark = CommittedStreamBookmark::published(id, name, vector, publication);
        let state = self.cf(CF_STATE)?;
        write.put_cf(&state, id_key, encode(&bookmark)?);
        write.put_cf(&state, name_key, encode(&id)?);
        write.put_cf(
            &state,
            stream_bookmark_order_key(bookmark.vector().stream(), publication),
            encode(&bookmark)?,
        );
        write.put_cf(&state, publication_key, encode(&publication.get())?);
        Ok(ApplyResult::StreamBookmark(bookmark))
    }

    fn apply_delete_stream_bookmark(
        &self,
        stream_id: StreamId,
        id: BookmarkId,
        write: &mut WriteBatch,
    ) -> io::Result<ApplyResult> {
        let id_key = stream_bookmark_id_key(id);
        let Some(mut bookmark) = self.get::<CommittedStreamBookmark>(CF_STATE, &id_key)? else {
            return Ok(ApplyResult::Rejected(DomainError::BookmarkNotFound));
        };
        if bookmark.vector().stream() != stream_id {
            return Ok(ApplyResult::Rejected(DomainError::BookmarkNotFound));
        }
        if bookmark.lifecycle() == light_stream_core::BookmarkLifecycle::Deleted {
            return Ok(ApplyResult::StreamBookmark(bookmark));
        }
        let state = self.cf(CF_STATE)?;
        write.delete_cf(&state, stream_bookmark_name_key(stream_id, bookmark.name()));
        write.delete_cf(
            &state,
            stream_bookmark_order_key(stream_id, bookmark.publication()),
        );
        bookmark.mark_deleted();
        write.put_cf(&state, id_key, encode(&bookmark)?);
        Ok(ApplyResult::StreamBookmark(bookmark))
    }

    fn apply_advance_retention(
        &self,
        request: RetentionRequest,
        observation: ClockObservation,
        write: &mut WriteBatch,
    ) -> io::Result<ApplyResult> {
        if self.identity.kind != GroupKind::Data {
            return Ok(ApplyResult::Rejected(DomainError::IdentityMismatch {
                reason: "retention command reached the control group".to_owned(),
            }));
        }
        let fingerprint = retention_fingerprint(&request)?;
        let receipt_key = mutation_receipt_key(request.request())?;
        if let Some(existing) = self.get::<StoredMutationReceipt>(CF_STATE, &receipt_key)? {
            return if existing.fingerprint == fingerprint {
                Ok(existing.result)
            } else {
                Ok(ApplyResult::Rejected(DomainError::MutationConflict))
            };
        }
        let session_key = mutation_session_key(request.request());
        let mut session = self
            .get::<MutationSessionState>(CF_STATE, &session_key)?
            .unwrap_or_default();
        let sequence = request.request().sequence().get();
        if session
            .highest_sequence
            .is_some_and(|highest| sequence <= highest)
        {
            return Ok(ApplyResult::Rejected(DomainError::MutationReceiptExpired));
        }
        let tail = RecordOffset::new(
            self.get::<u64>(CF_STATE, &next_offset_key(request.partition()))?
                .unwrap_or_default(),
        );
        let previous = self
            .get::<PartitionRetentionState>(CF_STATE, &retention_key(request.partition()))?
            .unwrap_or_default();
        let state = self.cf(CF_STATE)?;
        let result = match advance_floor(
            RecordOffset::new(previous.logical_floor),
            request.target_floor(),
            tail,
        ) {
            Ok(floor) => {
                let clock = match self.get::<SafeLeaseClock>(CF_STATE, KEY_LEASE_CLOCK)? {
                    Some(clock) => clock.advance(observation),
                    None => SafeLeaseClock::new(observation),
                };
                match clock {
                    Ok(clock) => {
                        let floor_byte_position = if floor.get() == 0 {
                            0
                        } else {
                            match self.get::<StoredRecord>(
                                CF_STATE,
                                &record_key(request.partition(), floor.get() - 1),
                            )? {
                                Some(record) => record.cumulative_end_bytes,
                                None if floor == RecordOffset::new(previous.logical_floor) => {
                                    previous.floor_byte_position
                                }
                                None => {
                                    return Ok(ApplyResult::Rejected(DomainError::Storage {
                                        reason: "retention floor byte boundary is unavailable"
                                            .to_owned(),
                                    }));
                                }
                            }
                        };
                        let status = PartitionRetentionState {
                            logical_floor: floor.get(),
                            floor_byte_position,
                            ..previous
                        };
                        write.put_cf(&state, retention_key(request.partition()), encode(&status)?);
                        write.put_cf(&state, KEY_LEASE_CLOCK, encode(&clock)?);
                        ApplyResult::Retention(RetentionResult::new(
                            request.request().clone(),
                            request.partition(),
                            RecordOffset::new(previous.logical_floor),
                            floor,
                        ))
                    }
                    Err(error) => ApplyResult::Rejected(error),
                }
            }
            Err(error) => ApplyResult::Rejected(error),
        };
        write.put_cf(
            &state,
            receipt_key,
            encode(&StoredMutationReceipt {
                fingerprint,
                result: result.clone(),
            })?,
        );
        session.highest_sequence = Some(sequence);
        session.retained_sequences.push_back(sequence);
        if session.retained_sequences.len() > self.receipt_window
            && let Some(expired) = session.retained_sequences.pop_front()
        {
            let expired_request = MutationRequestId::new(
                request.request().principal().clone(),
                request.request().session(),
                light_stream_core::RequestSequence::new(expired),
            );
            write.delete_cf(&state, mutation_receipt_key(&expired_request)?);
        }
        write.put_cf(&state, session_key, encode(&session)?);
        Ok(result)
    }

    fn prior_mutation_result(
        &self,
        request: &MutationRequestId,
        fingerprint: &str,
    ) -> io::Result<Option<ApplyResult>> {
        let receipt_key = mutation_receipt_key(request)?;
        if let Some(existing) = self.get::<StoredMutationReceipt>(CF_STATE, &receipt_key)? {
            return if existing.fingerprint == fingerprint {
                Ok(Some(existing.result))
            } else {
                Ok(Some(ApplyResult::Rejected(DomainError::MutationConflict)))
            };
        }
        let session = self
            .get::<MutationSessionState>(CF_STATE, &mutation_session_key(request))?
            .unwrap_or_default();
        if session
            .highest_sequence
            .is_some_and(|highest| request.sequence().get() <= highest)
        {
            return Ok(Some(ApplyResult::Rejected(
                DomainError::MutationReceiptExpired,
            )));
        }
        Ok(None)
    }

    fn store_mutation_result(
        &self,
        request: &MutationRequestId,
        fingerprint: String,
        result: &ApplyResult,
        write: &mut WriteBatch,
    ) -> io::Result<()> {
        let state = self.cf(CF_STATE)?;
        write.put_cf(
            &state,
            mutation_receipt_key(request)?,
            encode(&StoredMutationReceipt {
                fingerprint,
                result: result.clone(),
            })?,
        );
        let session_key = mutation_session_key(request);
        let mut session = self
            .get::<MutationSessionState>(CF_STATE, &session_key)?
            .unwrap_or_default();
        let sequence = request.sequence().get();
        session.highest_sequence = Some(sequence);
        session.retained_sequences.push_back(sequence);
        if session.retained_sequences.len() > self.receipt_window
            && let Some(expired) = session.retained_sequences.pop_front()
        {
            let expired_request = MutationRequestId::new(
                request.principal().clone(),
                request.session(),
                light_stream_core::RequestSequence::new(expired),
            );
            write.delete_cf(&state, mutation_receipt_key(&expired_request)?);
        }
        write.put_cf(&state, session_key, encode(&session)?);
        Ok(())
    }

    fn advance_lease_clock(
        &self,
        observation: ClockObservation,
    ) -> io::Result<Result<SafeLeaseClock, DomainError>> {
        Ok(
            match self.get::<SafeLeaseClock>(CF_STATE, KEY_LEASE_CLOCK)? {
                Some(clock) => clock.advance(observation),
                None => SafeLeaseClock::new(observation),
            },
        )
    }

    fn expire_due_leases(
        &self,
        clock: &SafeLeaseClock,
        write: &mut WriteBatch,
    ) -> io::Result<LeaseExpiryState> {
        let state = self.cf(CF_STATE)?;
        let leases = self
            .scan_prefix(CF_STATE, LEASE_ID_PREFIX)?
            .into_iter()
            .map(|(_, value)| decode::<ReplayLease>(&value))
            .collect::<io::Result<Vec<_>>>()?;
        let mut budget = self
            .get::<LeaseBudget>(CF_STATE, KEY_LEASE_BUDGET)?
            .unwrap_or_default();
        let mut active = Vec::new();
        let mut retentions = Vec::<(PartitionKey, PartitionRetentionState)>::new();
        let mut expired_any = false;
        for lease in leases {
            if lease_is_effectively_active(&lease, clock) {
                active.push(lease);
                continue;
            }
            if lease.lifecycle() != light_stream_core::ReplayLeaseLifecycle::Active {
                continue;
            }
            let range = lease.range();
            let protected_bytes = lease.protected_bytes().get();
            let expired = lease.expired();
            write.put_cf(&state, replay_lease_id_key(expired.id()), encode(&expired)?);
            budget.active_leases = budget.active_leases.saturating_sub(1);
            budget.reserved_bytes = budget.reserved_bytes.saturating_sub(protected_bytes);
            let index = match retentions
                .iter()
                .position(|(partition, _)| *partition == range.partition())
            {
                Some(index) => index,
                None => {
                    retentions.push((
                        range.partition(),
                        self.get::<PartitionRetentionState>(
                            CF_STATE,
                            &retention_key(range.partition()),
                        )?
                        .unwrap_or_default(),
                    ));
                    retentions.len() - 1
                }
            };
            let retention = &mut retentions[index].1;
            if range.start().get() < retention.logical_floor {
                retention.reclaim_cursor = retention.reclaim_cursor.min(range.start().get());
            }
            expired_any = true;
        }
        if expired_any {
            write.put_cf(&state, KEY_LEASE_BUDGET, encode(&budget)?);
            for (partition, retention) in &retentions {
                write.put_cf(&state, retention_key(*partition), encode(retention)?);
            }
        }
        Ok(LeaseExpiryState {
            budget,
            active,
            retentions,
        })
    }

    fn replay_range_bytes(
        &self,
        request: &ReplayLeaseRequest,
        retention: PartitionRetentionState,
    ) -> io::Result<Result<ByteCount, DomainError>> {
        let range = request.range();
        let start_bytes = if range.start().get() == retention.logical_floor {
            retention.floor_byte_position
        } else {
            let key = record_key(range.partition(), range.start().get() - 1);
            let Some(record) = self.get::<StoredRecord>(CF_STATE, &key)? else {
                return Ok(Err(DomainError::InvalidRange {
                    reason: "replay lease start boundary is unavailable".to_owned(),
                }));
            };
            record.cumulative_end_bytes
        };
        let Some(last_offset) = range.end().get().checked_sub(1) else {
            return Ok(Err(DomainError::InvalidRange {
                reason: "replay lease range is empty".to_owned(),
            }));
        };
        let Some(last) =
            self.get::<StoredRecord>(CF_STATE, &record_key(range.partition(), last_offset))?
        else {
            return Ok(Err(DomainError::InvalidRange {
                reason: "replay lease end boundary is unavailable".to_owned(),
            }));
        };
        Ok(last
            .cumulative_end_bytes
            .checked_sub(start_bytes)
            .map(ByteCount::new)
            .ok_or_else(|| DomainError::Storage {
                reason: "replay lease byte index is not monotonic".to_owned(),
            }))
    }

    fn apply_admit_replay_lease(
        &self,
        request: ReplayLeaseRequest,
        observation: ClockObservation,
        write: &mut WriteBatch,
    ) -> io::Result<ApplyResult> {
        let fingerprint = mutation_fingerprint(&request)?;
        if let Some(result) = self.prior_mutation_result(request.request(), &fingerprint)? {
            return Ok(result);
        }
        let result = match self.advance_lease_clock(observation)? {
            Ok(clock) => {
                let expiry = self.expire_due_leases(&clock, write)?;
                let retention = self
                    .get::<PartitionRetentionState>(
                        CF_STATE,
                        &retention_key(request.range().partition()),
                    )?
                    .unwrap_or_default();
                let tail = RecordOffset::new(
                    self.get::<u64>(CF_STATE, &next_offset_key(request.range().partition()))?
                        .unwrap_or_default(),
                );
                if request.range().start() < RecordOffset::new(retention.logical_floor) {
                    ApplyResult::Rejected(DomainError::CursorExpired {
                        requested: request.range().start(),
                        available_from: RecordOffset::new(retention.logical_floor),
                    })
                } else {
                    match self.replay_range_bytes(&request, retention)? {
                        Ok(bytes) => {
                            let budget = expiry.budget;
                            let limits = RetentionLimits::default();
                            match admit(
                                replay_lease_id(request.request())?,
                                request.clone(),
                                bytes,
                                AdmissionState {
                                    floor: RecordOffset::new(retention.logical_floor),
                                    tail,
                                    clock: &clock,
                                    limits: &limits,
                                    budget,
                                },
                            ) {
                                Ok(lease) => {
                                    let state = self.cf(CF_STATE)?;
                                    write.put_cf(
                                        &state,
                                        replay_lease_id_key(lease.id()),
                                        encode(&lease)?,
                                    );
                                    write.put_cf(
                                        &state,
                                        replay_lease_request_key(request.request())?,
                                        encode(&lease.id())?,
                                    );
                                    write.put_cf(
                                        &state,
                                        KEY_LEASE_BUDGET,
                                        encode(&LeaseBudget {
                                            active_leases: budget.active_leases.saturating_add(1),
                                            reserved_bytes: budget
                                                .reserved_bytes
                                                .saturating_add(bytes.get()),
                                        })?,
                                    );
                                    write.put_cf(&state, KEY_LEASE_CLOCK, encode(&clock)?);
                                    ApplyResult::ReplayLease(lease)
                                }
                                Err(error) => ApplyResult::Rejected(error),
                            }
                        }
                        Err(error) => ApplyResult::Rejected(error),
                    }
                }
            }
            Err(error) => ApplyResult::Rejected(error),
        };
        self.store_mutation_result(request.request(), fingerprint, &result, write)?;
        Ok(result)
    }

    fn apply_renew_replay_lease(
        &self,
        request: LeaseRenewal,
        observation: ClockObservation,
        write: &mut WriteBatch,
    ) -> io::Result<ApplyResult> {
        let fingerprint = mutation_fingerprint(&request)?;
        if let Some(result) = self.prior_mutation_result(request.request(), &fingerprint)? {
            return Ok(result);
        }
        let result = match self.advance_lease_clock(observation)? {
            Ok(clock) => {
                self.expire_due_leases(&clock, write)?;
                let id_key = replay_lease_id_key(request.lease());
                match self.get::<ReplayLease>(CF_STATE, &id_key)? {
                    Some(lease) if lease.range().partition() != request.partition() => {
                        ApplyResult::Rejected(DomainError::ReplayLeaseRangeViolation)
                    }
                    Some(lease) if lease_is_effectively_active(&lease, &clock) => {
                        let expires_at = clock
                            .upper_bound()
                            .checked_add(request.duration().as_millis())
                            .map(LeaseDeadline::new);
                        match expires_at {
                            Some(expires_at) => {
                                let hard_expires_at = lease.hard_expires_at();
                                match lease.renewed(expires_at, hard_expires_at) {
                                    Ok(lease) => {
                                        let state = self.cf(CF_STATE)?;
                                        write.put_cf(&state, id_key, encode(&lease)?);
                                        write.put_cf(&state, KEY_LEASE_CLOCK, encode(&clock)?);
                                        ApplyResult::ReplayLease(lease)
                                    }
                                    Err(error) => ApplyResult::Rejected(error),
                                }
                            }
                            None => ApplyResult::Rejected(DomainError::LeaseClockUnavailable),
                        }
                    }
                    Some(lease) => ApplyResult::Rejected(DomainError::ReplayLeaseInactive {
                        lease: lease.id(),
                        lifecycle: if lease.lifecycle()
                            == light_stream_core::ReplayLeaseLifecycle::Active
                        {
                            light_stream_core::ReplayLeaseLifecycle::Expired
                        } else {
                            lease.lifecycle()
                        },
                    }),
                    None => ApplyResult::Rejected(DomainError::ReplayLeaseNotFound {
                        lease: request.lease(),
                    }),
                }
            }
            Err(error) => ApplyResult::Rejected(error),
        };
        self.store_mutation_result(request.request(), fingerprint, &result, write)?;
        Ok(result)
    }

    fn apply_release_replay_lease(
        &self,
        request: LeaseRelease,
        observation: ClockObservation,
        write: &mut WriteBatch,
    ) -> io::Result<ApplyResult> {
        let fingerprint = mutation_fingerprint(&request)?;
        if let Some(result) = self.prior_mutation_result(request.request(), &fingerprint)? {
            return Ok(result);
        }
        let result = match self.advance_lease_clock(observation)? {
            Ok(clock) => {
                let expiry = self.expire_due_leases(&clock, write)?;
                let id_key = replay_lease_id_key(request.lease());
                match self.get::<ReplayLease>(CF_STATE, &id_key)? {
                    Some(lease) if lease.range().partition() != request.partition() => {
                        ApplyResult::Rejected(DomainError::ReplayLeaseRangeViolation)
                    }
                    Some(lease) if lease_is_effectively_active(&lease, &clock) => {
                        let released = lease.released();
                        let budget = expiry.budget;
                        let state = self.cf(CF_STATE)?;
                        write.put_cf(&state, id_key, encode(&released)?);
                        let partition = released.range().partition();
                        let mut retention = expiry
                            .retentions
                            .iter()
                            .find(|(candidate, _)| *candidate == partition)
                            .map(|(_, retention)| *retention)
                            .unwrap_or(
                                self.get::<PartitionRetentionState>(
                                    CF_STATE,
                                    &retention_key(partition),
                                )?
                                .unwrap_or_default(),
                            );
                        if released.range().start().get() < retention.logical_floor {
                            retention.reclaim_cursor =
                                retention.reclaim_cursor.min(released.range().start().get());
                            write.put_cf(&state, retention_key(partition), encode(&retention)?);
                        }
                        write.put_cf(
                            &state,
                            KEY_LEASE_BUDGET,
                            encode(&LeaseBudget {
                                active_leases: budget.active_leases.saturating_sub(1),
                                reserved_bytes: budget
                                    .reserved_bytes
                                    .saturating_sub(released.protected_bytes().get()),
                            })?,
                        );
                        write.put_cf(&state, KEY_LEASE_CLOCK, encode(&clock)?);
                        ApplyResult::ReplayLease(released)
                    }
                    Some(lease) => ApplyResult::Rejected(DomainError::ReplayLeaseInactive {
                        lease: lease.id(),
                        lifecycle: if lease.lifecycle()
                            == light_stream_core::ReplayLeaseLifecycle::Active
                        {
                            light_stream_core::ReplayLeaseLifecycle::Expired
                        } else {
                            lease.lifecycle()
                        },
                    }),
                    None => ApplyResult::Rejected(DomainError::ReplayLeaseNotFound {
                        lease: request.lease(),
                    }),
                }
            }
            Err(error) => ApplyResult::Rejected(error),
        };
        self.store_mutation_result(request.request(), fingerprint, &result, write)?;
        Ok(result)
    }

    fn apply_maintain_retention(
        &self,
        partition: PartitionKey,
        expected_cursor: RecordOffset,
        max_records: u32,
        max_payload_bytes: u64,
        observation: ClockObservation,
        write: &mut WriteBatch,
    ) -> io::Result<ApplyResult> {
        if max_records == 0 || max_payload_bytes == 0 {
            return Ok(ApplyResult::Rejected(DomainError::InvalidRange {
                reason: "retention maintenance limits must be greater than zero".to_owned(),
            }));
        }
        let mut retention = self
            .get::<PartitionRetentionState>(CF_STATE, &retention_key(partition))?
            .unwrap_or_default();
        if retention.reclaim_cursor != expected_cursor.get() {
            return Ok(ApplyResult::RetentionStatus(RetentionStatus::new(
                partition,
                RecordOffset::new(retention.logical_floor),
                RecordOffset::new(retention.reclaim_cursor),
                ByteCount::new(retention.logically_expired_bytes),
                ByteCount::new(retention.raft_only_bytes),
            )));
        }
        let clock = match self.advance_lease_clock(observation)? {
            Ok(clock) => clock,
            Err(error) => return Ok(ApplyResult::Rejected(error)),
        };
        let expiry = self.expire_due_leases(&clock, write)?;
        if let Some((_, updated)) = expiry
            .retentions
            .iter()
            .find(|(candidate, _)| *candidate == partition)
        {
            retention = *updated;
        }
        let state = self.cf(CF_STATE)?;
        let payload = self.cf(CF_PAYLOAD)?;
        let active_ranges = expiry
            .active
            .into_iter()
            .filter(|lease| lease.range().partition() == partition)
            .map(|lease| lease.range())
            .collect::<Vec<_>>();
        let mut examined = 0_u32;
        let mut reclaimed_bytes = 0_u64;
        while retention.reclaim_cursor < retention.logical_floor && examined < max_records {
            let offset = RecordOffset::new(retention.reclaim_cursor);
            if let Some(range) = active_ranges.iter().find(|range| range.contains(offset)) {
                retention.reclaim_cursor = range.end().get().min(retention.logical_floor);
                continue;
            }
            let record_key = record_key(partition, retention.reclaim_cursor);
            let Some(record) = self.get::<StoredRecord>(CF_STATE, &record_key)? else {
                retention.reclaim_cursor = retention.reclaim_cursor.saturating_add(1);
                examined = examined.saturating_add(1);
                continue;
            };
            if examined > 0
                && reclaimed_bytes.saturating_add(record.payload_bytes) > max_payload_bytes
            {
                break;
            }
            let owners_key = payload_owners_key(&record.payload_key);
            let mut owners = self
                .get::<PayloadOwners>(CF_PAYLOAD, &owners_key)?
                .ok_or_else(|| io_error("retained record payload is missing ownership"))?;
            owners.set_applied_in(self.active_state_bank()?, false);
            write.delete_cf(&state, record_key);
            if owners.reachable() {
                if owners.raft_log {
                    retention.raft_only_bytes = retention
                        .raft_only_bytes
                        .saturating_add(record.payload_bytes);
                }
                write.put_cf(&payload, owners_key, encode(&owners)?);
            } else {
                write.delete_cf(&payload, owners_key);
                write.delete_cf(&payload, payload_bytes_key(&record.payload_key));
            }
            retention.logically_expired_bytes = retention
                .logically_expired_bytes
                .saturating_add(record.payload_bytes);
            retention.reclaim_cursor = retention.reclaim_cursor.saturating_add(1);
            reclaimed_bytes = reclaimed_bytes.saturating_add(record.payload_bytes);
            examined = examined.saturating_add(1);
        }
        write.put_cf(&state, retention_key(partition), encode(&retention)?);
        write.put_cf(&state, KEY_LEASE_CLOCK, encode(&clock)?);
        Ok(ApplyResult::RetentionStatus(RetentionStatus::new(
            partition,
            RecordOffset::new(retention.logical_floor),
            RecordOffset::new(retention.reclaim_cursor),
            ByteCount::new(retention.logically_expired_bytes),
            ByteCount::new(retention.raft_only_bytes),
        )))
    }

    fn apply_checkpoint_mutation(
        &self,
        mutation: CheckpointMutation,
        write: &mut WriteBatch,
    ) -> io::Result<ApplyResult> {
        if self.identity.kind != GroupKind::Data {
            return Ok(ApplyResult::Rejected(DomainError::IdentityMismatch {
                reason: "checkpoint mutation reached the control group".to_owned(),
            }));
        }
        let Some(spec) = self.get::<BootstrapSpec>(CF_STATE, KEY_BOOTSTRAP)? else {
            return Ok(ApplyResult::Rejected(DomainError::NotBootstrapped));
        };
        let fingerprint = mutation_fingerprint(&mutation)?;
        if let Some(result) = self.prior_mutation_result(mutation.request(), &fingerprint)? {
            return Ok(result);
        }
        if mutation.key().cluster() != self.identity.cluster_id
            || mutation.key().cluster() != spec.cluster()
        {
            return Ok(ApplyResult::Rejected(DomainError::IdentityMismatch {
                reason: "checkpoint cluster does not match the data group".to_owned(),
            }));
        }
        let key = checkpoint_key(mutation.key())?;
        let current = self.get::<CommittedCheckpoint>(CF_STATE, &key)?;
        let expectation_matches = match (mutation.expected(), current.as_ref()) {
            (CheckpointExpectation::Missing, None) => true,
            (CheckpointExpectation::Revision(expected), Some(current)) => {
                expected == current.revision()
            }
            _ => false,
        };
        if !expectation_matches {
            let result = ApplyResult::Checkpoint(CheckpointCasResult::Conflict {
                request: mutation.request().clone(),
                current,
            });
            self.store_mutation_result(mutation.request(), fingerprint, &result, write)?;
            return Ok(result);
        }

        let tail = RecordOffset::new(
            self.get::<u64>(CF_STATE, &next_offset_key(mutation.key().partition()))?
                .unwrap_or_default(),
        );
        let candidate = mutation.candidate().next_offset();
        if candidate > tail {
            let result =
                ApplyResult::Rejected(DomainError::CheckpointAheadOfTail { candidate, tail });
            self.store_mutation_result(mutation.request(), fingerprint, &result, write)?;
            return Ok(result);
        }
        if let Some(current) = current.as_ref()
            && candidate < current.cursor().next_offset()
        {
            let result = ApplyResult::Rejected(DomainError::CheckpointRegression {
                current: current.cursor().next_offset(),
                candidate,
            });
            self.store_mutation_result(mutation.request(), fingerprint, &result, write)?;
            return Ok(result);
        }

        let revision = match current.as_ref() {
            Some(current) => current.revision().checked_next().map_err(io_error)?,
            None => CheckpointRevision::initial(),
        };
        let checkpoint =
            CommittedCheckpoint::new(mutation.key().clone(), mutation.candidate(), revision);
        let result = ApplyResult::Checkpoint(CheckpointCasResult::Advanced {
            request: mutation.request().clone(),
            previous: current,
            checkpoint: checkpoint.clone(),
        });
        write.put_cf(&self.cf(CF_STATE)?, key, encode(&checkpoint)?);
        self.store_mutation_result(mutation.request(), fingerprint, &result, write)?;
        Ok(result)
    }

    fn apply_publish_many(
        &self,
        log_id: GroupLogId,
        batch: ReplicatedPublishBatch,
        write: &mut WriteBatch,
    ) -> io::Result<ApplyResult> {
        if self.identity.kind != GroupKind::Data {
            return Ok(ApplyResult::Rejected(DomainError::IdentityMismatch {
                reason: "publish reached the control group".to_owned(),
            }));
        }
        let Some(spec) = self.get::<BootstrapSpec>(CF_STATE, KEY_BOOTSTRAP)? else {
            return Ok(ApplyResult::Rejected(DomainError::NotBootstrapped));
        };
        let mut transaction = PublishApplyTxn::new(self, log_id, write)?;
        let mut outcomes = Vec::with_capacity(batch.requests().len());
        let mut first_payload_slot = 0usize;
        for publish in batch.into_requests() {
            let record_count = publish.records().len();
            if publish.cluster() != spec.cluster() {
                outcomes.push(PublishItemOutcome::new(
                    publish.request().clone(),
                    Err(DomainError::IdentityMismatch {
                        reason: "publish cluster does not match the data group".to_owned(),
                    }),
                ));
            } else {
                outcomes.push(transaction.apply_one(
                    outcomes.len(),
                    first_payload_slot,
                    publish,
                )?);
            }
            first_payload_slot = first_payload_slot
                .checked_add(record_count)
                .ok_or_else(|| io_error("publish payload slot overflow"))?;
        }
        Ok(ApplyResult::PublishedMany(PublishManyResult::new(outcomes)))
    }

    fn apply_publish(
        &self,
        log_id: GroupLogId,
        batch: PublishBatch,
        write: &mut WriteBatch,
    ) -> io::Result<ApplyResult> {
        if self.identity.kind != GroupKind::Data {
            return Ok(ApplyResult::Rejected(DomainError::IdentityMismatch {
                reason: "publish reached the control group".to_owned(),
            }));
        }
        let Some(spec) = self.get::<BootstrapSpec>(CF_STATE, KEY_BOOTSTRAP)? else {
            return Ok(ApplyResult::Rejected(DomainError::NotBootstrapped));
        };
        if batch.cluster() != self.identity.cluster_id || batch.cluster() != spec.cluster() {
            return Ok(ApplyResult::Rejected(DomainError::IdentityMismatch {
                reason: "publish cluster does not match the data group".to_owned(),
            }));
        }

        let fingerprint = legacy_fingerprint(&batch)?;
        let stored_receipt_key = receipt_key(batch.partition(), batch.request())?;
        if let Some(existing) = self.get::<StoredReceipt>(CF_STATE, &stored_receipt_key)? {
            return if existing.fingerprint == fingerprint {
                Ok(ApplyResult::Published(existing.receipt))
            } else {
                Ok(ApplyResult::Rejected(DomainError::ReceiptConflict))
            };
        }

        let session_key = session_key(batch.partition(), batch.request());
        let mut session = self
            .get::<ProducerSessionState>(CF_STATE, &session_key)?
            .unwrap_or_default();
        let sequence = batch.request().sequence().get();
        if session
            .highest_sequence
            .is_some_and(|highest| sequence <= highest)
        {
            return Ok(ApplyResult::Rejected(DomainError::ReceiptExpired));
        }

        let first = self
            .get::<u64>(CF_STATE, &next_offset_key(batch.partition()))?
            .unwrap_or_default();
        let count = u64::try_from(batch.records().len()).map_err(io_error)?;
        let range = CommittedRecordRange::new(batch.partition(), RecordOffset::new(first), count)
            .map_err(io_error)?;
        let bookmark = match batch.bookmark().cloned() {
            Some(name) => match self.create_bookmark_record(
                bookmark_id(log_id),
                batch.partition(),
                name,
                range.next(),
                range.next(),
                write,
            )? {
                ApplyResult::Bookmark(value) => Some(value),
                ApplyResult::Rejected(error) => return Ok(ApplyResult::Rejected(error)),
                other => {
                    return Err(io_error(format!(
                        "unexpected bookmark apply result {other}"
                    )));
                }
            },
            None => None,
        };
        let payload_cf = self.cf(CF_PAYLOAD)?;
        let state_cf = self.cf(CF_STATE)?;
        let mut retention = self
            .get::<PartitionRetentionState>(CF_STATE, &retention_key(batch.partition()))?
            .unwrap_or_default();
        for (slot, record) in batch.records().iter().enumerate() {
            let payload_key = payload_id(log_id, slot);
            let owners_key = payload_owners_key(&payload_key);
            let mut owners = self
                .get::<PayloadOwners>(CF_PAYLOAD, &owners_key)?
                .ok_or_else(|| io_error("applied entry payload is missing ownership"))?;
            owners.set_applied_in(self.active_state_bank()?, true);
            write.put_cf(&payload_cf, owners_key, encode(&owners)?);
            let offset = first + u64::try_from(slot).map_err(io_error)?;
            let payload_bytes = u64::try_from(record.len()).map_err(io_error)?;
            retention.next_byte_position = retention
                .next_byte_position
                .checked_add(payload_bytes)
                .ok_or_else(|| io_error("partition byte position overflow"))?;
            write.put_cf(
                &state_cf,
                record_key(batch.partition(), offset),
                encode(&StoredRecord {
                    payload_key: payload_key.clone(),
                    payload_bytes,
                    cumulative_end_bytes: retention.next_byte_position,
                })?,
            );
        }
        write.put_cf(
            &state_cf,
            next_offset_key(batch.partition()),
            encode(&(first + count))?,
        );
        write.put_cf(
            &state_cf,
            retention_key(batch.partition()),
            encode(&retention)?,
        );
        let receipt = PublishReceipt::new(batch.request().clone(), range, bookmark);
        write.put_cf(
            &state_cf,
            &stored_receipt_key,
            encode(&StoredReceipt {
                fingerprint,
                receipt: receipt.clone(),
            })?,
        );

        session.highest_sequence = Some(sequence);
        session.retained_sequences.push_back(sequence);
        if session.retained_sequences.len() > self.receipt_window
            && let Some(expired) = session.retained_sequences.pop_front()
        {
            let expired_request = ProducerRequestId::new(
                batch.request().principal().clone(),
                batch.request().session(),
                light_stream_core::RequestSequence::new(expired),
            );
            write.delete_cf(&state_cf, receipt_key(batch.partition(), &expired_request)?);
        }
        write.put_cf(&state_cf, session_key, encode(&session)?);
        Ok(ApplyResult::Published(receipt))
    }

    fn build_snapshot(&self) -> io::Result<(GroupSnapshotMeta, SnapshotArtifact)> {
        let _guard = self.write_lane.enter()?;
        let applied = self.get::<GroupLogId>(CF_STATE, KEY_APPLIED)?;
        let membership = self
            .get::<GroupMembership>(CF_STATE, KEY_MEMBERSHIP)?
            .unwrap_or_default();
        let meta = GroupSnapshotMeta {
            last_log_id: applied,
            last_membership: membership,
        };
        let identity_json = serde_json::to_vec(&self.identity).map_err(io_error)?;
        let meta_json = serde_json::to_vec(&meta).map_err(io_error)?;
        let mut artifact = self.snapshot_catalog()?.begin_artifact(
            STORAGE_FORMAT_VERSION,
            &identity_json,
            &meta_json,
        )?;
        let state_cf = self.cf(CF_STATE)?;
        for item in self
            .db
            .iterator_cf(&state_cf, IteratorMode::From(b"", Direction::Forward))
        {
            let (key, value) = item.map_err(io_error)?;
            artifact.write_state(&key, &value)?;
        }
        let mut write = WriteBatch::default();
        let active_bank = self.active_state_bank()?;
        let payload_cf = self.cf(CF_PAYLOAD)?;
        for item in self.db.iterator_cf(
            &payload_cf,
            IteratorMode::From(PAYLOAD_OWNERS_PREFIX, Direction::Forward),
        ) {
            let (key, value) = item.map_err(io_error)?;
            if !key.starts_with(PAYLOAD_OWNERS_PREFIX) {
                break;
            }
            let owners: PayloadOwners = decode(&value)?;
            if owners.applied_in(active_bank) {
                let id = &key[PAYLOAD_OWNERS_PREFIX.len()..];
                let bytes_key = payload_bytes_key(id);
                let bytes = self
                    .db
                    .get_cf(&payload_cf, &bytes_key)
                    .map_err(io_error)?
                    .ok_or_else(|| io_error("snapshot payload bytes are missing"))?;
                artifact.write_payload(&bytes_key, &bytes)?;
            }
        }
        let (artifact, descriptor) = artifact.finish()?;
        write.put_cf(
            &self.cf(CF_SNAPSHOT)?,
            KEY_CURRENT_SNAPSHOT,
            encode(&StoredCurrentSnapshot {
                artifact: descriptor.clone(),
                meta: meta.clone(),
            })?,
        );
        self.write_sync(write)?;
        if let Err(error) = self.snapshot_catalog()?.collect_except(&descriptor) {
            eprintln!(
                "{}",
                serde_json::json!({
                    "event": "snapshot_artifact_cleanup_failed",
                    "group_id": self.identity.group_id,
                    "error": error.to_string(),
                })
            );
        }
        Ok((meta, artifact))
    }

    fn install_snapshot(
        &self,
        meta: &GroupSnapshotMeta,
        artifact: &SnapshotArtifact,
    ) -> io::Result<()> {
        let _write_guard = self.write_lane.enter()?;
        let mut bank = self
            .state_bank
            .write()
            .map_err(|_| io_error("state bank lock poisoned"))?;
        let active_bank = *bank;
        let target_bank = active_bank.inactive();
        let state_cf = self.raw_cf(target_bank.column_family())?;
        let payload_cf = self.cf(CF_PAYLOAD)?;
        let mut write = WriteBatch::default();
        let mut pending = 0_u32;
        let mut pending_bytes = 0_usize;
        match artifact.reader() {
            Ok(mut reader) => {
                let identity: GroupIdentity =
                    serde_json::from_slice(reader.identity_json()).map_err(io_error)?;
                let artifact_meta: GroupSnapshotMeta =
                    serde_json::from_slice(reader.meta_json()).map_err(io_error)?;
                if reader.storage_format_version() != STORAGE_FORMAT_VERSION
                    || identity != self.identity
                {
                    return Err(io_error("snapshot identity or format mismatch"));
                }
                if &artifact_meta != meta {
                    return Err(io_error("snapshot metadata mismatch"));
                }
                self.clear_state_bank(target_bank)?;
                while let Some(record) = reader.next_record()? {
                    match record {
                        SnapshotRecord::State { key, value } => {
                            pending_bytes = pending_bytes
                                .saturating_add(key.len())
                                .saturating_add(value.len());
                            write.put_cf(&state_cf, key, value);
                        }
                        SnapshotRecord::Payload {
                            key: bytes_key,
                            value,
                        } => {
                            pending_bytes = pending_bytes
                                .saturating_add(bytes_key.len())
                                .saturating_add(value.len());
                            let id = bytes_key
                                .strip_prefix(PAYLOAD_BYTES_PREFIX)
                                .ok_or_else(|| io_error("invalid snapshot payload key"))?
                                .to_vec();
                            write.put_cf(&payload_cf, &bytes_key, value);
                            let owners_key = payload_owners_key(&id);
                            let mut owners = self
                                .get::<PayloadOwners>(CF_PAYLOAD, &owners_key)?
                                .unwrap_or_default();
                            owners.set_applied_in(target_bank, true);
                            write.put_cf(&payload_cf, owners_key, encode(&owners)?);
                        }
                    }
                    pending += 1;
                    if pending == 1024 || pending_bytes >= 8 * 1024 * 1024 {
                        self.write_sync(std::mem::take(&mut write))?;
                        pending = 0;
                        pending_bytes = 0;
                    }
                }
            }
            Err(_) if artifact.len() <= MAX_SNAPSHOT_BYTES as u64 => {
                let bytes = artifact.read_all_limited(MAX_SNAPSHOT_BYTES)?;
                let bundle = decode_snapshot_bundle(&bytes)?;
                if bundle.format_version != STORAGE_FORMAT_VERSION
                    || bundle.identity != self.identity
                {
                    return Err(io_error("snapshot identity or format mismatch"));
                }
                if &bundle.meta != meta {
                    return Err(io_error("snapshot metadata mismatch"));
                }
                self.clear_state_bank(target_bank)?;
                for (key, value) in bundle.state {
                    pending_bytes = pending_bytes
                        .saturating_add(key.len())
                        .saturating_add(value.len());
                    write.put_cf(&state_cf, key, value);
                    pending += 1;
                    if pending == 1024 || pending_bytes >= 8 * 1024 * 1024 {
                        self.write_sync(std::mem::take(&mut write))?;
                        pending = 0;
                        pending_bytes = 0;
                    }
                }
                for (bytes_key, value) in bundle.payloads {
                    pending_bytes = pending_bytes
                        .saturating_add(bytes_key.len())
                        .saturating_add(value.len());
                    let id = bytes_key
                        .strip_prefix(PAYLOAD_BYTES_PREFIX)
                        .ok_or_else(|| io_error("invalid snapshot payload key"))?
                        .to_vec();
                    write.put_cf(&payload_cf, &bytes_key, value);
                    let owners_key = payload_owners_key(&id);
                    let mut owners = self
                        .get::<PayloadOwners>(CF_PAYLOAD, &owners_key)?
                        .unwrap_or_default();
                    owners.set_applied_in(target_bank, true);
                    write.put_cf(&payload_cf, owners_key, encode(&owners)?);
                    pending += 1;
                    if pending == 1024 || pending_bytes >= 8 * 1024 * 1024 {
                        self.write_sync(std::mem::take(&mut write))?;
                        pending = 0;
                        pending_bytes = 0;
                    }
                }
            }
            Err(error) => return Err(error),
        }
        if pending != 0 {
            self.write_sync(std::mem::take(&mut write))?;
        }
        let (_, descriptor) = self.snapshot_catalog()?.adopt(artifact)?;
        write.put_cf(
            &self.cf(CF_SNAPSHOT)?,
            KEY_CURRENT_SNAPSHOT,
            encode(&StoredCurrentSnapshot {
                artifact: descriptor.clone(),
                meta: meta.clone(),
            })?,
        );
        write.put_cf(
            &self.raw_cf(CF_META)?,
            KEY_ACTIVE_STATE_BANK,
            encode(&target_bank)?,
        );
        self.write_sync(write)?;
        *bank = target_bank;
        self.clear_state_bank(active_bank)?;
        drop(bank);
        if let Err(error) = self.snapshot_catalog()?.collect_except(&descriptor) {
            eprintln!(
                "{}",
                serde_json::json!({
                    "event": "snapshot_artifact_cleanup_failed",
                    "group_id": self.identity.group_id,
                    "error": error.to_string(),
                })
            );
        }
        Ok(())
    }

    fn clear_state_bank(&self, bank: StateBank) -> io::Result<()> {
        let state = self.raw_cf(bank.column_family())?;
        let mut write = WriteBatch::default();
        let mut pending = 0_u32;
        for item in self
            .db
            .iterator_cf(&state, IteratorMode::From(b"", Direction::Forward))
        {
            let (key, _) = item.map_err(io_error)?;
            write.delete_cf(&state, key);
            pending += 1;
            if pending == 1024 {
                self.write_sync(std::mem::take(&mut write))?;
                pending = 0;
            }
        }
        if pending != 0 {
            self.write_sync(std::mem::take(&mut write))?;
        }
        let payload = self.raw_cf(CF_PAYLOAD)?;
        pending = 0;
        for item in self.db.iterator_cf(
            &payload,
            IteratorMode::From(PAYLOAD_OWNERS_PREFIX, Direction::Forward),
        ) {
            let (key, value) = item.map_err(io_error)?;
            if !key.starts_with(PAYLOAD_OWNERS_PREFIX) {
                break;
            }
            let id = &key[PAYLOAD_OWNERS_PREFIX.len()..];
            let mut owners: PayloadOwners = decode(&value)?;
            if !owners.applied_in(bank) {
                continue;
            }
            owners.set_applied_in(bank, false);
            if owners.reachable() {
                write.put_cf(&payload, &key, encode(&owners)?);
            } else {
                write.delete_cf(&payload, &key);
                write.delete_cf(&payload, payload_bytes_key(id));
            }
            pending += 1;
            if pending == 1024 {
                self.write_sync(std::mem::take(&mut write))?;
                pending = 0;
            }
        }
        if pending != 0 {
            self.write_sync(write)?;
        }
        Ok(())
    }
}

impl CommittedStateReader {
    pub fn security_policy(&self) -> Result<Option<SecurityPolicy>, DomainError> {
        self.db
            .get(CF_STATE, KEY_SECURITY_POLICY)
            .map_err(storage_domain)
    }

    pub fn cluster_topology(&self) -> Result<Option<ClusterTopology>, DomainError> {
        self.db
            .get(CF_STATE, KEY_CLUSTER_TOPOLOGY)
            .map_err(storage_domain)
    }

    pub fn active_administration(&self) -> Result<Option<AdministrationOperation>, DomainError> {
        self.db
            .get(CF_STATE, KEY_ACTIVE_ADMINISTRATION)
            .map_err(storage_domain)
    }

    pub fn administration_operation(
        &self,
        request: AdministrationRequestId,
    ) -> Result<Option<AdministrationOperation>, DomainError> {
        self.db
            .get(CF_STATE, &administration_request_key(request))
            .map_err(storage_domain)
    }

    pub fn snapshot_artifact_bytes(&self) -> Result<Option<u64>, DomainError> {
        self.db
            .current_snapshot_artifact()
            .map(|value| value.map(|(_, artifact)| artifact.len()))
            .map_err(storage_domain)
    }

    pub fn bootstrap_spec(&self) -> Result<Option<BootstrapSpec>, DomainError> {
        self.db
            .get(CF_STATE, KEY_BOOTSTRAP)
            .map_err(|error| DomainError::Storage {
                reason: error.to_string(),
            })
    }

    pub fn operational_proof(&self) -> Result<Option<OperationalProof>, DomainError> {
        self.db
            .get(CF_STATE, KEY_OPERATIONAL_PROOF)
            .map_err(storage_domain)
    }

    pub fn receipt(
        &self,
        partition: PartitionKey,
        request: &ProducerRequestId,
    ) -> Result<PublishReceipt, DomainError> {
        let stored = self
            .db
            .get::<StoredPublishOutcome>(
                CF_STATE,
                &receipt_key(partition, request).map_err(|error| DomainError::Storage {
                    reason: error.to_string(),
                })?,
            )
            .map_err(|error| DomainError::Storage {
                reason: error.to_string(),
            })?
            .ok_or(DomainError::ReceiptNotFound)?;
        match stored {
            StoredPublishOutcome::Legacy(stored) => Ok(stored.receipt),
            StoredPublishOutcome::Versioned(stored) => stored.result.as_result(),
        }
    }

    pub fn checkpoint(&self, key: &CheckpointKey) -> Result<CommittedCheckpoint, DomainError> {
        if key.cluster() != self.db.identity.cluster_id {
            return Err(DomainError::IdentityMismatch {
                reason: "checkpoint cluster does not match the data group".to_owned(),
            });
        }
        self.db
            .get::<CommittedCheckpoint>(CF_STATE, &checkpoint_key(key).map_err(storage_domain)?)
            .map_err(storage_domain)?
            .ok_or(DomainError::CheckpointNotFound)
    }

    pub fn fetch(
        &self,
        cluster: ClusterId,
        partition: PartitionKey,
        offset: RecordOffset,
        limit: u32,
    ) -> Result<FetchPage, DomainError> {
        if limit == 0 || limit > MAX_FETCH_RECORDS {
            return Err(DomainError::InvalidRange {
                reason: format!("fetch limit must be between 1 and {MAX_FETCH_RECORDS}"),
            });
        }
        let spec = self.bootstrap_spec()?.ok_or(DomainError::NotBootstrapped)?;
        if cluster != self.db.identity.cluster_id || cluster != spec.cluster() {
            return Err(DomainError::IdentityMismatch {
                reason: "fetch cluster does not match the data group".to_owned(),
            });
        }
        let prefix = record_prefix(partition);
        let start = record_key(partition, offset.get());
        let state_bank = self
            .db
            .state_bank
            .read()
            .map_err(|_| DomainError::Storage {
                reason: "state bank lock poisoned".to_owned(),
            })?;
        let state_cf = self
            .db
            .raw_cf(state_bank.column_family())
            .map_err(storage_domain)?;
        let payload_cf = self.db.cf(CF_PAYLOAD).map_err(storage_domain)?;
        let snapshot = self.db.db.snapshot();
        drop(state_bank);
        let retention = snapshot
            .get_cf(&state_cf, retention_key(partition))
            .map_err(storage_domain)?
            .map(|value| decode::<PartitionRetentionState>(&value).map_err(storage_domain))
            .transpose()?
            .unwrap_or_default();
        let floor = RecordOffset::new(retention.logical_floor);
        if offset < floor {
            return Err(DomainError::CursorExpired {
                requested: offset,
                available_from: floor,
            });
        }
        let mut records = Vec::new();
        let mut payload_bytes = 0usize;
        for item in snapshot.iterator_cf(&state_cf, IteratorMode::From(&start, Direction::Forward))
        {
            let (key, value) = item.map_err(storage_domain)?;
            if !key.starts_with(&prefix) || records.len() >= limit as usize {
                break;
            }
            let offset_bytes: [u8; 8] =
                key[key.len() - 8..]
                    .try_into()
                    .map_err(|_| DomainError::Storage {
                        reason: "invalid record key".to_owned(),
                    })?;
            let stored: StoredRecord = decode(&value).map_err(storage_domain)?;
            let payload = snapshot
                .get_cf(&payload_cf, payload_bytes_key(&stored.payload_key))
                .map_err(storage_domain)?
                .ok_or_else(|| DomainError::Storage {
                    reason: "committed record payload is missing".to_owned(),
                })?;
            let payload = decode_payload_value(&payload).map_err(storage_domain)?;
            if !records.is_empty() && payload_bytes.saturating_add(payload.len()) > MAX_FETCH_BYTES
            {
                break;
            }
            payload_bytes = payload_bytes.saturating_add(payload.len());
            records.push(CommittedRecord::new(
                RecordOffset::new(u64::from_be_bytes(offset_bytes)),
                payload,
            ));
        }
        let next_offset = records.last().map_or(offset, |record| {
            RecordOffset::new(record.offset().get() + 1)
        });
        Ok(FetchPage::new(partition, records, next_offset))
    }

    pub fn partition_tail(&self, partition: PartitionKey) -> Result<RecordOffset, DomainError> {
        self.db
            .get::<u64>(CF_STATE, &next_offset_key(partition))
            .map_err(storage_domain)
            .map(|value| RecordOffset::new(value.unwrap_or_default()))
    }

    pub fn retention_status(
        &self,
        partition: PartitionKey,
    ) -> Result<RetentionStatus, DomainError> {
        let value = self
            .db
            .get::<PartitionRetentionState>(CF_STATE, &retention_key(partition))
            .map_err(storage_domain)?
            .unwrap_or_default();
        Ok(RetentionStatus::new(
            partition,
            RecordOffset::new(value.logical_floor),
            RecordOffset::new(value.reclaim_cursor),
            light_stream_core::ByteCount::new(value.logically_expired_bytes),
            light_stream_core::ByteCount::new(value.raft_only_bytes),
        ))
    }

    pub fn retention_maintenance_needed(
        &self,
        partition: PartitionKey,
        safe_lower_bound_unix_ms: u64,
    ) -> Result<bool, DomainError> {
        let status = self
            .db
            .get::<PartitionRetentionState>(CF_STATE, &retention_key(partition))
            .map_err(storage_domain)?
            .unwrap_or_default();
        if status.reclaim_cursor < status.logical_floor {
            return Ok(true);
        }
        let leases = self
            .db
            .scan_prefix(CF_STATE, LEASE_ID_PREFIX)
            .map_err(storage_domain)?
            .into_iter()
            .map(|(_, value)| decode::<ReplayLease>(&value).map_err(storage_domain))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(leases.into_iter().any(|lease| {
            lease.range().partition() == partition
                && lease.lifecycle() == light_stream_core::ReplayLeaseLifecycle::Active
                && (safe_lower_bound_unix_ms >= lease.expires_at().unix_millis()
                    || safe_lower_bound_unix_ms >= lease.hard_expires_at().unix_millis())
        }))
    }

    pub fn retention_partitions(&self) -> Result<Vec<PartitionKey>, DomainError> {
        self.db
            .scan_prefix(CF_STATE, RETENTION_PREFIX)
            .map_err(storage_domain)?
            .into_iter()
            .map(|(key, _)| partition_from_retention_key(&key).map_err(storage_domain))
            .collect()
    }

    pub fn retention_receipt(
        &self,
        request: &MutationRequestId,
    ) -> Result<RetentionResult, DomainError> {
        let receipt = self
            .db
            .get::<StoredMutationReceipt>(
                CF_STATE,
                &mutation_receipt_key(request).map_err(storage_domain)?,
            )
            .map_err(storage_domain)?
            .ok_or(DomainError::MutationReceiptExpired)?;
        match receipt.result {
            ApplyResult::Retention(result) => Ok(result),
            ApplyResult::Rejected(error) => Err(error),
            other => Err(DomainError::Storage {
                reason: format!("mutation receipt contains unexpected result {other}"),
            }),
        }
    }

    pub fn replay_lease_by_request(
        &self,
        request: &MutationRequestId,
    ) -> Result<ReplayLease, DomainError> {
        let id = self
            .db
            .get::<ReplayLeaseId>(
                CF_STATE,
                &replay_lease_request_key(request).map_err(storage_domain)?,
            )
            .map_err(storage_domain)?
            .ok_or(DomainError::MutationReceiptExpired)?;
        self.db
            .get::<ReplayLease>(CF_STATE, &replay_lease_id_key(id))
            .map_err(storage_domain)?
            .ok_or(DomainError::ReplayLeaseNotFound { lease: id })
    }

    pub fn replay_lease(
        &self,
        partition: PartitionKey,
        id: ReplayLeaseId,
    ) -> Result<ReplayLease, DomainError> {
        let lease = self
            .db
            .get::<ReplayLease>(CF_STATE, &replay_lease_id_key(id))
            .map_err(storage_domain)?
            .ok_or(DomainError::ReplayLeaseNotFound { lease: id })?;
        if lease.range().partition() != partition {
            return Err(DomainError::ReplayLeaseRangeViolation);
        }
        Ok(lease)
    }

    pub fn fetch_protected(
        &self,
        cluster: ClusterId,
        partition: PartitionKey,
        lease_id: ReplayLeaseId,
        offset: RecordOffset,
        limit: u32,
    ) -> Result<FetchPage, DomainError> {
        if limit == 0 || limit > MAX_FETCH_RECORDS {
            return Err(DomainError::InvalidRange {
                reason: format!("fetch limit must be between 1 and {MAX_FETCH_RECORDS}"),
            });
        }
        let spec = self.bootstrap_spec()?.ok_or(DomainError::NotBootstrapped)?;
        if cluster != self.db.identity.cluster_id || cluster != spec.cluster() {
            return Err(DomainError::IdentityMismatch {
                reason: "fetch cluster does not match the data group".to_owned(),
            });
        }
        let state_bank = self
            .db
            .state_bank
            .read()
            .map_err(|_| DomainError::Storage {
                reason: "state bank lock poisoned".to_owned(),
            })?;
        let state_cf = self
            .db
            .raw_cf(state_bank.column_family())
            .map_err(storage_domain)?;
        let payload_cf = self.db.cf(CF_PAYLOAD).map_err(storage_domain)?;
        let snapshot = self.db.db.snapshot();
        drop(state_bank);
        let lease = snapshot
            .get_cf(&state_cf, replay_lease_id_key(lease_id))
            .map_err(storage_domain)?
            .map(|value| decode::<ReplayLease>(&value).map_err(storage_domain))
            .transpose()?
            .ok_or(DomainError::ReplayLeaseNotFound { lease: lease_id })?;
        if lease.request().cluster() != cluster || lease.range().partition() != partition {
            return Err(DomainError::ReplayLeaseRangeViolation);
        }
        let clock = snapshot
            .get_cf(&state_cf, KEY_LEASE_CLOCK)
            .map_err(storage_domain)?
            .map(|value| decode::<SafeLeaseClock>(&value).map_err(storage_domain))
            .transpose()?
            .ok_or(DomainError::LeaseClockUnavailable)?;
        if !lease_is_effectively_active(&lease, &clock) {
            return Err(DomainError::ReplayLeaseInactive {
                lease: lease.id(),
                lifecycle: if lease.lifecycle() == light_stream_core::ReplayLeaseLifecycle::Active {
                    light_stream_core::ReplayLeaseLifecycle::Expired
                } else {
                    lease.lifecycle()
                },
            });
        }
        let range = lease.range();
        if offset == range.end() {
            return Ok(FetchPage::new(range.partition(), Vec::new(), offset));
        }
        if !range.contains(offset) {
            return Err(DomainError::ReplayLeaseRangeViolation);
        }
        let prefix = record_prefix(range.partition());
        let start = record_key(range.partition(), offset.get());
        let mut records = Vec::new();
        let mut payload_bytes = 0usize;
        for item in snapshot.iterator_cf(&state_cf, IteratorMode::From(&start, Direction::Forward))
        {
            let (key, value) = item.map_err(storage_domain)?;
            if !key.starts_with(&prefix) || records.len() >= limit as usize {
                break;
            }
            let offset_bytes: [u8; 8] =
                key[key.len() - 8..]
                    .try_into()
                    .map_err(|_| DomainError::Storage {
                        reason: "invalid record key".to_owned(),
                    })?;
            let record_offset = RecordOffset::new(u64::from_be_bytes(offset_bytes));
            if record_offset >= range.end() {
                break;
            }
            let stored: StoredRecord = decode(&value).map_err(storage_domain)?;
            let payload = snapshot
                .get_cf(&payload_cf, payload_bytes_key(&stored.payload_key))
                .map_err(storage_domain)?
                .ok_or_else(|| DomainError::Storage {
                    reason: "protected record payload is missing".to_owned(),
                })?;
            let payload = decode_payload_value(&payload).map_err(storage_domain)?;
            if !records.is_empty() && payload_bytes.saturating_add(payload.len()) > MAX_FETCH_BYTES
            {
                break;
            }
            payload_bytes = payload_bytes.saturating_add(payload.len());
            records.push(CommittedRecord::new(record_offset, payload));
        }
        let next_offset = records.last().map_or(offset, |record| {
            RecordOffset::new(record.offset().get() + 1)
        });
        Ok(FetchPage::new(range.partition(), records, next_offset))
    }

    pub fn resolve_bookmark(
        &self,
        partition: PartitionKey,
        name: &BookmarkName,
    ) -> Result<CommittedBookmark, DomainError> {
        let id = self
            .db
            .get::<BookmarkId>(CF_STATE, &bookmark_name_key(partition, name))
            .map_err(storage_domain)?
            .ok_or(DomainError::BookmarkNotFound)?;
        self.bookmark_by_id(partition, id)
    }

    pub fn bookmark_by_id(
        &self,
        partition: PartitionKey,
        id: BookmarkId,
    ) -> Result<CommittedBookmark, DomainError> {
        let bookmark = self
            .db
            .get::<CommittedBookmark>(CF_STATE, &bookmark_id_key(id))
            .map_err(storage_domain)?
            .ok_or(DomainError::BookmarkNotFound)?;
        if bookmark.cursor().partition() != partition {
            return Err(DomainError::BookmarkNotFound);
        }
        Ok(bookmark)
    }

    pub fn list_bookmarks(
        &self,
        request: &BookmarkPageRequest,
    ) -> Result<BookmarkPage, DomainError> {
        let partition = request.partition();
        let current = self
            .db
            .get::<u64>(CF_STATE, &bookmark_publication_key(partition))
            .map_err(storage_domain)?
            .unwrap_or_default();
        let ceiling = request
            .publication_ceiling()
            .unwrap_or_else(|| BookmarkPublicationSequence::new(current));
        let before = request
            .before()
            .map_or_else(|| ceiling.get().saturating_add(1), |value| value.get());
        let mut items = self
            .db
            .scan_prefix(CF_STATE, &bookmark_order_prefix(partition))
            .map_err(storage_domain)?
            .into_iter()
            .rev()
            .filter_map(|(_, value)| decode::<CommittedBookmark>(&value).ok())
            .filter(|bookmark| {
                bookmark.lifecycle() == light_stream_core::BookmarkLifecycle::Available
                    && bookmark.publication().get() <= ceiling.get()
                    && bookmark.publication().get() < before
            })
            .take(request.limit() as usize)
            .collect::<Vec<_>>();
        items.sort_by_key(|bookmark| std::cmp::Reverse(bookmark.publication()));
        let next_before = (items.len() == request.limit() as usize)
            .then(|| items.last().map(CommittedBookmark::publication))
            .flatten();
        Ok(BookmarkPage::new(items, ceiling, next_before))
    }

    pub fn resolve_stream_bookmark(
        &self,
        stream: StreamId,
        name: &BookmarkName,
    ) -> Result<CommittedStreamBookmark, DomainError> {
        let id = self
            .db
            .get::<BookmarkId>(CF_STATE, &stream_bookmark_name_key(stream, name))
            .map_err(storage_domain)?
            .ok_or(DomainError::BookmarkNotFound)?;
        self.stream_bookmark_by_id(stream, id)
    }

    pub fn stream_bookmark_by_id(
        &self,
        stream: StreamId,
        id: BookmarkId,
    ) -> Result<CommittedStreamBookmark, DomainError> {
        let bookmark = self
            .db
            .get::<CommittedStreamBookmark>(CF_STATE, &stream_bookmark_id_key(id))
            .map_err(storage_domain)?
            .ok_or(DomainError::BookmarkNotFound)?;
        if bookmark.vector().stream() != stream {
            return Err(DomainError::BookmarkNotFound);
        }
        Ok(bookmark)
    }

    pub fn list_stream_bookmarks(
        &self,
        request: &StreamBookmarkPageRequest,
    ) -> Result<StreamBookmarkPage, DomainError> {
        if request.cluster() != self.db.identity.cluster_id {
            return Err(DomainError::IdentityMismatch {
                reason: "stream bookmark cluster does not match the control group".to_owned(),
            });
        }
        let current = self
            .db
            .get::<u64>(CF_STATE, &stream_bookmark_publication_key(request.stream()))
            .map_err(storage_domain)?
            .unwrap_or_default();
        let ceiling = request
            .publication_ceiling()
            .unwrap_or_else(|| BookmarkPublicationSequence::new(current));
        let before = request
            .before()
            .map_or_else(|| ceiling.get().saturating_add(1), |value| value.get());
        let mut items = self
            .db
            .scan_prefix(CF_STATE, &stream_bookmark_order_prefix(request.stream()))
            .map_err(storage_domain)?
            .into_iter()
            .rev()
            .filter_map(|(_, value)| decode::<CommittedStreamBookmark>(&value).ok())
            .filter(|bookmark| {
                bookmark.lifecycle() == light_stream_core::BookmarkLifecycle::Available
                    && bookmark.publication().get() <= ceiling.get()
                    && bookmark.publication().get() < before
            })
            .take(request.limit() as usize)
            .collect::<Vec<_>>();
        items.sort_by_key(|bookmark| std::cmp::Reverse(bookmark.publication()));
        let next_before = (items.len() == request.limit() as usize)
            .then(|| items.last().map(CommittedStreamBookmark::publication))
            .flatten();
        Ok(StreamBookmarkPage::new(items, ceiling, next_before))
    }

    pub fn stream_by_id(
        &self,
        stream_id: StreamId,
    ) -> Result<Option<StreamDescriptor>, DomainError> {
        self.db
            .get(CF_STATE, &stream_key(stream_id))
            .map_err(storage_domain)
    }

    pub fn stream_by_name(
        &self,
        name: &StreamName,
    ) -> Result<Option<StreamDescriptor>, DomainError> {
        let Some(stream_id) = self
            .db
            .get::<StreamId>(CF_STATE, &stream_name_key(name))
            .map_err(storage_domain)?
        else {
            return Ok(None);
        };
        self.stream_by_id(stream_id)
    }

    pub fn active_streams(&self) -> Result<Vec<StreamDescriptor>, DomainError> {
        let mut values = self
            .db
            .scan_prefix(CF_STATE, STREAM_PREFIX)
            .map_err(storage_domain)?
            .into_iter()
            .map(|(_, value)| decode::<StreamDescriptor>(&value).map_err(storage_domain))
            .collect::<Result<Vec<_>, _>>()?;
        values.retain(|value| value.lifecycle() == StreamLifecycle::Active);
        values.sort_by(|left, right| left.name().cmp(right.name()));
        Ok(values)
    }

    pub fn route(
        &self,
        stream_id: StreamId,
        partition: PartitionId,
    ) -> Result<PartitionRoute, DomainError> {
        let descriptor = self
            .stream_by_id(stream_id)?
            .ok_or(DomainError::StreamNotFound)?;
        if descriptor.lifecycle() != StreamLifecycle::Active {
            return Err(DomainError::StreamNotActive);
        }
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

    pub fn data_group_pool(&self) -> Result<Vec<GroupId>, DomainError> {
        self.db
            .get(CF_STATE, KEY_DATA_GROUP_POOL)
            .map_err(storage_domain)?
            .ok_or_else(|| DomainError::Storage {
                reason: "control catalog data group pool is missing".to_owned(),
            })
    }
}

fn storage_domain(error: impl fmt::Display) -> DomainError {
    DomainError::Storage {
        reason: error.to_string(),
    }
}

#[derive(Clone, Debug, Default)]
pub struct NoRemoteNetworkFactory;

#[derive(Clone, Debug)]
pub struct NoRemoteNetwork {
    target: u64,
}

impl<C> RaftNetworkFactory<C> for NoRemoteNetworkFactory
where
    C: openraft::RaftTypeConfig<NodeId = u64, Node = BasicNode>,
{
    type Network = NoRemoteNetwork;

    async fn new_client(&mut self, target: u64, _node: &BasicNode) -> Self::Network {
        NoRemoteNetwork { target }
    }

    async fn new_heartbeat_client(&mut self, target: u64, _node: &BasicNode) -> Self::Network {
        NoRemoteNetwork { target }
    }

    async fn new_snapshot_client(&mut self, target: u64, _node: &BasicNode) -> Self::Network {
        NoRemoteNetwork { target }
    }
}

impl<C> RaftNetworkV2<C> for NoRemoteNetwork
where
    C: openraft::RaftTypeConfig<NodeId = u64, Node = BasicNode>,
{
    type SnapshotData = SnapshotArtifact;

    async fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<C>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<C>, RPCError<C>> {
        Err(RPCError::Unreachable(Unreachable::from_string(format!(
            "LS02a has no remote Raft transport for node {}",
            self.target
        ))))
    }

    async fn vote(
        &mut self,
        _rpc: VoteRequest<C>,
        _option: RPCOption,
    ) -> Result<VoteResponse<C>, RPCError<C>> {
        Err(RPCError::Unreachable(Unreachable::from_string(format!(
            "LS02a has no remote Raft transport for node {}",
            self.target
        ))))
    }

    async fn full_snapshot(
        &mut self,
        _vote: VoteOf<C>,
        _snapshot: SnapshotOf<C, Self::SnapshotData>,
        _cancel: impl std::future::Future<Output = ReplicationClosed> + openraft::OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<C>, StreamingError<C>> {
        Err(StreamingError::Unreachable(Unreachable::from_string(
            format!(
                "LS02a has no remote Raft snapshot transport for node {}",
                self.target
            ),
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };

    use openraft::{
        EntryPayload,
        storage::{IOFlushed, RaftLogStorage, RaftStateMachine},
        testing::log::{StoreBuilder, Suite},
        type_config::TypeConfigExt,
    };
    use uuid::Uuid;

    use super::*;

    static TEST_ID: AtomicU64 = AtomicU64::new(1);

    fn test_budget() -> GroupStorageBudget {
        GroupStorageBudget::new(8 * 1024 * 1024, 4 * 1024 * 1024).unwrap()
    }

    struct ProjectTestDir(PathBuf);

    impl ProjectTestDir {
        fn new(label: &str) -> Self {
            let id = TEST_ID.fetch_add(1, Ordering::Relaxed);
            let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../target/test-data/light-stream-storage")
                .join(format!("{label}-{}-{id}", std::process::id()));
            let _ = fs::remove_dir_all(&path);
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for ProjectTestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[derive(Clone, Default)]
    struct Builder;

    struct Guard(ProjectTestDir);

    impl
        StoreBuilder<
            ControlRaftConfig,
            RocksLogStore<ControlRaftConfig>,
            RocksStateMachine<ControlRaftConfig>,
            Guard,
        > for Builder
    {
        async fn build(
            &self,
        ) -> Result<
            (
                Guard,
                RocksLogStore<ControlRaftConfig>,
                RocksStateMachine<ControlRaftConfig>,
            ),
            openraft::StorageError<ControlRaftConfig>,
        > {
            let guard = Guard(ProjectTestDir::new("conformance"));
            let identity = GroupIdentity::new(
                "018f3f7e-5b3b-7c11-98f7-b65ac15f6501".parse().unwrap(),
                GroupId::new(CONTROL_GROUP_ID).unwrap(),
                GroupKind::Control,
            );
            let handles =
                create_control_store(&guard.0.0, identity, DEFAULT_RECEIPT_WINDOW, test_budget())
                    .map_err(|error| {
                    openraft::StorageError::read(ControlRaftConfig::err_from_string(
                        error.to_string(),
                    ))
                })?;
            Ok((guard, handles.log_store, handles.state_machine))
        }
    }

    #[tokio::test]
    async fn openraft_storage_conformance() {
        Suite::test_all(Builder).await.unwrap();
    }

    fn ids() -> (ClusterId, light_stream_core::StreamId) {
        (
            ClusterId::from_uuid(Uuid::new_v4()),
            light_stream_core::StreamId::from_uuid(Uuid::new_v4()),
        )
    }

    fn test_topology() -> ClusterTopology {
        let node = light_stream_core::NodeDescriptor::new(
            NodeId::new(1).unwrap(),
            "http://127.0.0.1:7101",
            "http://127.0.0.1:7201",
        );
        ClusterTopology::try_new(1, [node.clone()], [node.node_id()]).unwrap()
    }

    fn test_security_policy(
        cluster: ClusterId,
    ) -> (
        SecurityPolicy,
        light_stream_core::PrincipalId,
        light_stream_core::CredentialRef,
    ) {
        let principal = light_stream_core::PrincipalId::parse("security-admin").unwrap();
        let credential = light_stream_core::CredentialRef::new(
            light_stream_core::CredentialId::parse("admin").unwrap(),
            light_stream_core::CredentialGeneration::initial(),
        );
        let grants = BTreeMap::from([(
            principal.clone(),
            BTreeSet::from([
                light_stream_core::Grant::new(
                    light_stream_core::Permission::SecurityAdmin,
                    light_stream_core::ResourceScope::Cluster { cluster },
                ),
                light_stream_core::Grant::new(
                    light_stream_core::Permission::ClusterAdmin,
                    light_stream_core::ResourceScope::Cluster { cluster },
                ),
            ]),
        )]);
        let policy = SecurityPolicy::try_new(
            cluster,
            light_stream_core::PolicyRevision::initial(),
            light_stream_core::RevocationRevision::default(),
            grants,
            vec![light_stream_core::TokenVerifier::new(
                credential.clone(),
                principal.clone(),
                light_stream_core::TokenVerifierDigest::from_token_bytes(b"test-security-token"),
            )],
            BTreeMap::from([(
                NodeId::new(1).unwrap(),
                BTreeSet::from([light_stream_core::PeerCertificateBinding::new(
                    cluster,
                    NodeId::new(1).unwrap(),
                    light_stream_core::CredentialGeneration::initial(),
                    light_stream_core::CertificateFingerprint::from_der(b"test-peer-certificate"),
                )]),
            )]),
        )
        .unwrap();
        (policy, principal, credential)
    }

    #[test]
    fn legacy_bootstrap_log_without_topology_still_decodes() {
        let (cluster, stream) = ids();
        let command = ThinCommand::BootstrapControl {
            spec: BootstrapSpec::new(cluster, stream, StreamName::parse("bootstrap").unwrap()),
            topology: Some(test_topology()),
            security: None,
            data_groups: vec![GroupId::new(DATA_GROUP_ID).unwrap()],
            max_streams: 8,
            max_partitions_per_stream: 4,
        };
        let mut value = serde_json::to_value(command).unwrap();
        value
            .get_mut("BootstrapControl")
            .and_then(serde_json::Value::as_object_mut)
            .unwrap()
            .remove("topology");
        value
            .get_mut("BootstrapControl")
            .and_then(serde_json::Value::as_object_mut)
            .unwrap()
            .remove("security");

        let decoded: ThinCommand = serde_json::from_value(value).unwrap();

        assert!(matches!(
            decoded,
            ThinCommand::BootstrapControl { topology: None, .. }
        ));
    }

    #[test]
    fn security_policy_mutations_are_idempotent_and_snapshot_backed() {
        let directory = ProjectTestDir::new("security-policy");
        let (cluster, _) = ids();
        let identity = GroupIdentity::new(
            cluster,
            GroupId::new(CONTROL_GROUP_ID).unwrap(),
            GroupKind::Control,
        );
        let handles = create_control_store(
            &directory.0,
            identity,
            DEFAULT_RECEIPT_WINDOW,
            test_budget(),
        )
        .unwrap();
        let (policy, principal, _) = test_security_policy(cluster);
        let initialized = handles
            .state_machine
            .db
            .apply_entry(GroupEntry {
                log_id: GroupLogId::new(
                    GroupLeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    1,
                ),
                payload: EntryPayload::Normal(GroupCommand::InitializeSecurityPolicy {
                    policy: policy.clone(),
                }),
            })
            .unwrap();
        assert_eq!(initialized, ApplyResult::SecurityPolicy(policy.clone()));

        let request = MutationRequestId::new(
            principal.clone(),
            light_stream_core::MutationSessionId::from_uuid(Uuid::new_v4()),
            light_stream_core::RequestSequence::new(1),
        );
        let next_credential = light_stream_core::CredentialRef::new(
            light_stream_core::CredentialId::parse("admin").unwrap(),
            light_stream_core::CredentialGeneration::new(2).unwrap(),
        );
        let mutation = SecurityMutation::new(
            request,
            policy.revision(),
            light_stream_core::SecurityChange::AddTokenGeneration {
                verifier: light_stream_core::TokenVerifier::new(
                    next_credential,
                    principal,
                    light_stream_core::TokenVerifierDigest::from_token_bytes(
                        b"next-test-security-token",
                    ),
                ),
            },
        );
        let first = handles
            .state_machine
            .db
            .apply_entry(GroupEntry {
                log_id: GroupLogId::new(
                    GroupLeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    2,
                ),
                payload: EntryPayload::Normal(GroupCommand::ApplySecurityMutation {
                    mutation: mutation.clone(),
                }),
            })
            .unwrap();
        let retry = handles
            .state_machine
            .db
            .apply_entry(GroupEntry {
                log_id: GroupLogId::new(
                    GroupLeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    3,
                ),
                payload: EntryPayload::Normal(GroupCommand::ApplySecurityMutation { mutation }),
            })
            .unwrap();
        assert_eq!(first, retry);
        let committed = handles.reader.security_policy().unwrap().unwrap();
        assert_eq!(committed.revision().get(), 2);
        assert_eq!(committed.token_verifiers().len(), 2);

        let (_, artifact) = handles.state_machine.db.build_snapshot().unwrap();
        let mut artifact = artifact.reader().unwrap();
        let mut snapshotted = None;
        while let Some(record) = artifact.next_record().unwrap() {
            if let SnapshotRecord::State { key, value } = record
                && key == KEY_SECURITY_POLICY
            {
                snapshotted = Some(decode::<SecurityPolicy>(&value).unwrap());
            }
        }
        assert_eq!(snapshotted, Some(committed));
    }

    #[test]
    fn secured_transport_activation_updates_topology_and_policy_atomically() {
        let directory = ProjectTestDir::new("secured-transport");
        let (cluster, stream) = ids();
        let handles = create_control_store(
            &directory.0,
            GroupIdentity::new(
                cluster,
                GroupId::new(CONTROL_GROUP_ID).unwrap(),
                GroupKind::Control,
            ),
            DEFAULT_RECEIPT_WINDOW,
            test_budget(),
        )
        .unwrap();
        let initial_topology = test_topology();
        handles
            .state_machine
            .db
            .apply_entry(GroupEntry {
                log_id: GroupLogId::new(
                    GroupLeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    1,
                ),
                payload: EntryPayload::Normal(GroupCommand::BootstrapControl {
                    spec: BootstrapSpec::new(
                        cluster,
                        stream,
                        StreamName::parse("bootstrap").unwrap(),
                    ),
                    topology: Some(initial_topology.clone()),
                    security: None,
                    data_groups: vec![GroupId::new(DATA_GROUP_ID).unwrap()],
                    max_streams: 8,
                    max_partitions_per_stream: 4,
                }),
            })
            .unwrap();
        let secured_node = light_stream_core::NodeDescriptor::new(
            NodeId::new(1).unwrap(),
            "https://127.0.0.1:7101",
            "https://127.0.0.1:7201",
        );
        let secured_topology =
            ClusterTopology::try_new(2, [secured_node.clone()], [secured_node.node_id()]).unwrap();
        let (policy, principal, _) = test_security_policy(cluster);
        let request = MutationRequestId::new(
            principal,
            light_stream_core::MutationSessionId::from_uuid(Uuid::new_v4()),
            light_stream_core::RequestSequence::new(1),
        );
        let command = GroupCommand::ActivateSecuredTransport {
            request,
            topology: secured_topology.clone(),
            policy: policy.clone(),
        };
        let first = handles
            .state_machine
            .db
            .apply_entry(GroupEntry {
                log_id: GroupLogId::new(
                    GroupLeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    2,
                ),
                payload: EntryPayload::Normal(command.clone()),
            })
            .unwrap();
        let retry = handles
            .state_machine
            .db
            .apply_entry(GroupEntry {
                log_id: GroupLogId::new(
                    GroupLeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    3,
                ),
                payload: EntryPayload::Normal(command),
            })
            .unwrap();
        assert_eq!(first, retry);
        assert_eq!(
            handles.reader.cluster_topology().unwrap(),
            Some(secured_topology)
        );
        assert_eq!(handles.reader.security_policy().unwrap(), Some(policy));
    }

    #[test]
    fn secured_transport_activation_rejects_missing_peer_coverage_atomically() {
        let directory = ProjectTestDir::new("secured-transport-missing-peer");
        let (cluster, stream) = ids();
        let handles = create_control_store(
            &directory.0,
            GroupIdentity::new(
                cluster,
                GroupId::new(CONTROL_GROUP_ID).unwrap(),
                GroupKind::Control,
            ),
            DEFAULT_RECEIPT_WINDOW,
            test_budget(),
        )
        .unwrap();
        let initial_topology = test_topology();
        handles
            .state_machine
            .db
            .apply_entry(GroupEntry {
                log_id: GroupLogId::new(
                    GroupLeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    1,
                ),
                payload: EntryPayload::Normal(GroupCommand::BootstrapControl {
                    spec: BootstrapSpec::new(
                        cluster,
                        stream,
                        StreamName::parse("bootstrap").unwrap(),
                    ),
                    topology: Some(initial_topology.clone()),
                    security: None,
                    data_groups: vec![GroupId::new(DATA_GROUP_ID).unwrap()],
                    max_streams: 8,
                    max_partitions_per_stream: 4,
                }),
            })
            .unwrap();
        let secured_node = light_stream_core::NodeDescriptor::new(
            NodeId::new(1).unwrap(),
            "https://127.0.0.1:7101",
            "https://127.0.0.1:7201",
        );
        let secured_topology =
            ClusterTopology::try_new(2, [secured_node.clone()], [secured_node.node_id()]).unwrap();
        let (covered_policy, principal, _) = test_security_policy(cluster);
        let policy = SecurityPolicy::try_new(
            covered_policy.cluster(),
            covered_policy.revision(),
            covered_policy.revocation_revision(),
            covered_policy.grants().clone(),
            covered_policy.token_verifiers().to_vec(),
            BTreeMap::new(),
        )
        .unwrap();
        let result = handles
            .state_machine
            .db
            .apply_entry(GroupEntry {
                log_id: GroupLogId::new(
                    GroupLeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    2,
                ),
                payload: EntryPayload::Normal(GroupCommand::ActivateSecuredTransport {
                    request: MutationRequestId::new(
                        principal,
                        light_stream_core::MutationSessionId::from_uuid(Uuid::new_v4()),
                        light_stream_core::RequestSequence::new(1),
                    ),
                    topology: secured_topology,
                    policy,
                }),
            })
            .unwrap();

        assert_eq!(
            result,
            ApplyResult::Rejected(DomainError::SecurityPolicyConflict)
        );
        assert_eq!(
            handles.reader.cluster_topology().unwrap(),
            Some(initial_topology)
        );
        assert_eq!(handles.reader.security_policy().unwrap(), None);
    }

    #[tokio::test]
    async fn purge_keeps_applied_payload_readable() {
        let directory = ProjectTestDir::new("purge-retention");
        let (cluster, stream) = ids();
        let identity = GroupIdentity::new(
            cluster,
            GroupId::new(DATA_GROUP_ID).unwrap(),
            GroupKind::Data,
        );
        let handles = create_data_store(
            &directory.0,
            identity,
            DEFAULT_RECEIPT_WINDOW,
            test_budget(),
        )
        .unwrap();
        let mut log = handles.log_store;
        let mut state = handles.state_machine;
        let partition = PartitionKey::new(stream, light_stream_core::PartitionId::new(0));
        let spec = BootstrapSpec::new(
            cluster,
            stream,
            light_stream_core::StreamName::parse("bootstrap").unwrap(),
        );
        let bootstrap = GroupEntry {
            log_id: GroupLogId::new(
                GroupLeaderId {
                    term: 1,
                    node_id: 1,
                },
                1,
            ),
            payload: EntryPayload::Normal(GroupCommand::BootstrapData { spec }),
        };
        let publish = GroupEntry {
            log_id: GroupLogId::new(
                GroupLeaderId {
                    term: 1,
                    node_id: 1,
                },
                2,
            ),
            payload: EntryPayload::Normal(GroupCommand::Publish {
                batch: PublishBatch::new(
                    cluster,
                    partition,
                    ProducerRequestId::new(
                        light_stream_core::PrincipalId::parse("test").unwrap(),
                        light_stream_core::ProducerSessionId::from_uuid(Uuid::new_v4()),
                        light_stream_core::RequestSequence::new(1),
                    ),
                    vec![b"retained".to_vec()],
                )
                .unwrap(),
            }),
        };
        log.append([bootstrap.clone(), publish.clone()], IOFlushed::noop())
            .await
            .unwrap();
        state
            .apply(futures_util::stream::iter([
                Ok((bootstrap, None)),
                Ok((publish, None)),
            ]))
            .await
            .unwrap();
        log.purge(GroupLogId::new(
            GroupLeaderId {
                term: 1,
                node_id: 1,
            },
            2,
        ))
        .await
        .unwrap();
        let page = handles
            .reader
            .fetch(cluster, partition, RecordOffset::new(0), 10)
            .unwrap();
        assert_eq!(page.records()[0].payload(), b"retained");
    }

    #[tokio::test]
    async fn publish_many_preserves_request_receipt_and_bookmark_boundaries() {
        let directory = ProjectTestDir::new("publish-many");
        let (cluster, stream) = ids();
        let handles = create_data_store(
            &directory.0,
            GroupIdentity::new(
                cluster,
                GroupId::new(DATA_GROUP_ID).unwrap(),
                GroupKind::Data,
            ),
            DEFAULT_RECEIPT_WINDOW,
            test_budget(),
        )
        .unwrap();
        let mut log = handles.log_store;
        let mut state = handles.state_machine;
        let partition = PartitionKey::new(stream, PartitionId::new(0));
        let principal = light_stream_core::PrincipalId::parse("test").unwrap();
        let session = light_stream_core::ProducerSessionId::from_uuid(Uuid::new_v4());
        let first_request = ProducerRequestId::new(
            principal.clone(),
            session,
            light_stream_core::RequestSequence::new(1),
        );
        let second_request = ProducerRequestId::new(
            principal,
            session,
            light_stream_core::RequestSequence::new(2),
        );
        let first = PublishBatch::new(
            cluster,
            partition,
            first_request.clone(),
            vec![b"zero".to_vec(), b"one".to_vec()],
        )
        .unwrap()
        .with_bookmark(BookmarkName::parse("after-first").unwrap());
        let second = PublishBatch::new(
            cluster,
            partition,
            second_request.clone(),
            vec![b"two".to_vec()],
        )
        .unwrap()
        .with_bookmark(BookmarkName::parse("after-second").unwrap());
        let entries = vec![
            GroupEntry {
                log_id: GroupLogId::new(
                    GroupLeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    1,
                ),
                payload: EntryPayload::Normal(GroupCommand::BootstrapData {
                    spec: BootstrapSpec::new(
                        cluster,
                        stream,
                        StreamName::parse("bootstrap").unwrap(),
                    ),
                }),
            },
            GroupEntry {
                log_id: GroupLogId::new(
                    GroupLeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    2,
                ),
                payload: EntryPayload::Normal(GroupCommand::PublishMany {
                    batch: ReplicatedPublishBatch::new(vec![first.clone(), first, second]).unwrap(),
                }),
            },
        ];

        log.append(entries.clone(), IOFlushed::noop())
            .await
            .unwrap();
        state
            .apply(futures_util::stream::iter(
                entries.into_iter().map(|entry| Ok((entry, None))),
            ))
            .await
            .unwrap();

        let page = handles
            .reader
            .fetch(cluster, partition, RecordOffset::new(0), 10)
            .unwrap();
        assert_eq!(
            page.records()
                .iter()
                .map(|record| record.payload())
                .collect::<Vec<_>>(),
            vec![b"zero".as_slice(), b"one".as_slice(), b"two".as_slice()]
        );
        let first_receipt = handles.reader.receipt(partition, &first_request).unwrap();
        let second_receipt = handles.reader.receipt(partition, &second_request).unwrap();
        assert_eq!(first_receipt.range().first(), RecordOffset::new(0));
        assert_eq!(first_receipt.range().next(), RecordOffset::new(2));
        assert_eq!(second_receipt.range().first(), RecordOffset::new(2));
        assert_eq!(second_receipt.range().next(), RecordOffset::new(3));
        assert_eq!(
            handles
                .reader
                .resolve_bookmark(partition, &BookmarkName::parse("after-first").unwrap())
                .unwrap()
                .cursor()
                .next_offset(),
            RecordOffset::new(2)
        );
        assert_eq!(
            handles
                .reader
                .resolve_bookmark(partition, &BookmarkName::parse("after-second").unwrap())
                .unwrap()
                .cursor()
                .next_offset(),
            RecordOffset::new(3)
        );
    }

    #[tokio::test]
    async fn checkpoint_cas_preserves_conflicts_without_changing_bookmarks() {
        let directory = ProjectTestDir::new("checkpoint-cas");
        let (cluster, stream) = ids();
        let handles = create_data_store(
            &directory.0,
            GroupIdentity::new(
                cluster,
                GroupId::new(DATA_GROUP_ID).unwrap(),
                GroupKind::Data,
            ),
            DEFAULT_RECEIPT_WINDOW,
            test_budget(),
        )
        .unwrap();
        let mut log = handles.log_store;
        let mut state = handles.state_machine;
        let partition = PartitionKey::new(stream, PartitionId::new(0));
        let bootstrap = GroupEntry {
            log_id: GroupLogId::new(
                GroupLeaderId {
                    term: 1,
                    node_id: 1,
                },
                1,
            ),
            payload: EntryPayload::Normal(GroupCommand::BootstrapData {
                spec: BootstrapSpec::new(cluster, stream, StreamName::parse("bootstrap").unwrap()),
            }),
        };
        let publish = GroupEntry {
            log_id: GroupLogId::new(
                GroupLeaderId {
                    term: 1,
                    node_id: 1,
                },
                2,
            ),
            payload: EntryPayload::Normal(GroupCommand::PublishMany {
                batch: ReplicatedPublishBatch::new(vec![
                    PublishBatch::new(
                        cluster,
                        partition,
                        ProducerRequestId::new(
                            light_stream_core::PrincipalId::parse("producer").unwrap(),
                            light_stream_core::ProducerSessionId::from_uuid(Uuid::new_v4()),
                            light_stream_core::RequestSequence::new(1),
                        ),
                        vec![b"zero".to_vec(), b"one".to_vec(), b"two".to_vec()],
                    )
                    .unwrap()
                    .with_bookmark(BookmarkName::parse("shared").unwrap()),
                ])
                .unwrap(),
            }),
        };
        log.append([bootstrap.clone(), publish.clone()], IOFlushed::noop())
            .await
            .unwrap();
        state
            .apply(futures_util::stream::iter([
                Ok((bootstrap, None)),
                Ok((publish, None)),
            ]))
            .await
            .unwrap();
        let bookmarks_before = handles
            .reader
            .list_bookmarks(&BookmarkPageRequest::new(partition, 10, None, None).unwrap())
            .unwrap();
        let checkpoint_key = CheckpointKey::new(
            cluster,
            partition,
            light_stream_core::ConsumerId::parse("billing").unwrap(),
        );
        let mutation_session = light_stream_core::MutationSessionId::from_uuid(Uuid::new_v4());
        let mutation = |sequence, expected, offset| {
            CheckpointMutation::new(
                MutationRequestId::new(
                    light_stream_core::PrincipalId::parse("consumer").unwrap(),
                    mutation_session,
                    light_stream_core::RequestSequence::new(sequence),
                ),
                checkpoint_key.clone(),
                expected,
                CommittedCursor::new(cluster, partition, RecordOffset::new(offset)),
            )
            .unwrap()
        };
        let first = state
            .db
            .apply_entry(GroupEntry {
                log_id: GroupLogId::new(
                    GroupLeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    3,
                ),
                payload: EntryPayload::Normal(GroupCommand::CompareAndSetCheckpoint {
                    mutation: mutation(1, CheckpointExpectation::Missing, 1),
                }),
            })
            .unwrap();
        assert!(matches!(
            first,
            ApplyResult::Checkpoint(CheckpointCasResult::Advanced { .. })
        ));
        let second = state
            .db
            .apply_entry(GroupEntry {
                log_id: GroupLogId::new(
                    GroupLeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    4,
                ),
                payload: EntryPayload::Normal(GroupCommand::CompareAndSetCheckpoint {
                    mutation: mutation(
                        2,
                        CheckpointExpectation::Revision(CheckpointRevision::initial()),
                        2,
                    ),
                }),
            })
            .unwrap();
        assert!(matches!(
            second,
            ApplyResult::Checkpoint(CheckpointCasResult::Advanced { .. })
        ));
        let losing_mutation = mutation(
            3,
            CheckpointExpectation::Revision(CheckpointRevision::initial()),
            3,
        );
        let conflict = state
            .db
            .apply_entry(GroupEntry {
                log_id: GroupLogId::new(
                    GroupLeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    5,
                ),
                payload: EntryPayload::Normal(GroupCommand::CompareAndSetCheckpoint {
                    mutation: losing_mutation.clone(),
                }),
            })
            .unwrap();
        assert!(matches!(
            &conflict,
            ApplyResult::Checkpoint(CheckpointCasResult::Conflict {
                current: Some(current),
                ..
            }) if current.cursor().next_offset() == RecordOffset::new(2)
        ));
        let retry = state
            .db
            .apply_entry(GroupEntry {
                log_id: GroupLogId::new(
                    GroupLeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    6,
                ),
                payload: EntryPayload::Normal(GroupCommand::CompareAndSetCheckpoint {
                    mutation: losing_mutation,
                }),
            })
            .unwrap();
        assert_eq!(retry, conflict);
        assert_eq!(
            handles
                .reader
                .checkpoint(&checkpoint_key)
                .unwrap()
                .cursor()
                .next_offset(),
            RecordOffset::new(2)
        );
        assert_eq!(
            handles
                .reader
                .list_bookmarks(&BookmarkPageRequest::new(partition, 10, None, None).unwrap())
                .unwrap(),
            bookmarks_before
        );
    }

    #[tokio::test]
    async fn publish_many_replays_a_durable_bookmark_rejection() {
        let directory = ProjectTestDir::new("publish-many-rejection");
        let (cluster, stream) = ids();
        let handles = create_data_store(
            &directory.0,
            GroupIdentity::new(
                cluster,
                GroupId::new(DATA_GROUP_ID).unwrap(),
                GroupKind::Data,
            ),
            DEFAULT_RECEIPT_WINDOW,
            test_budget(),
        )
        .unwrap();
        let mut log = handles.log_store;
        let mut state = handles.state_machine;
        let partition = PartitionKey::new(stream, PartitionId::new(0));
        let bookmark_id = BookmarkId::from_uuid(Uuid::new_v4());
        let request = ProducerRequestId::new(
            light_stream_core::PrincipalId::parse("producer").unwrap(),
            light_stream_core::ProducerSessionId::from_uuid(Uuid::new_v4()),
            light_stream_core::RequestSequence::new(1),
        );
        let rejected = PublishBatch::new(
            cluster,
            partition,
            request.clone(),
            vec![b"rejected".to_vec()],
        )
        .unwrap()
        .with_bookmark(BookmarkName::parse("occupied").unwrap());
        let entries = vec![
            GroupEntry {
                log_id: GroupLogId::new(
                    GroupLeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    1,
                ),
                payload: EntryPayload::Normal(GroupCommand::BootstrapData {
                    spec: BootstrapSpec::new(
                        cluster,
                        stream,
                        StreamName::parse("bootstrap").unwrap(),
                    ),
                }),
            },
            GroupEntry {
                log_id: GroupLogId::new(
                    GroupLeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    2,
                ),
                payload: EntryPayload::Normal(GroupCommand::CreateBookmark {
                    id: bookmark_id,
                    partition,
                    name: BookmarkName::parse("occupied").unwrap(),
                    offset: RecordOffset::new(0),
                }),
            },
            GroupEntry {
                log_id: GroupLogId::new(
                    GroupLeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    3,
                ),
                payload: EntryPayload::Normal(GroupCommand::PublishMany {
                    batch: ReplicatedPublishBatch::new(vec![rejected.clone()]).unwrap(),
                }),
            },
            GroupEntry {
                log_id: GroupLogId::new(
                    GroupLeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    4,
                ),
                payload: EntryPayload::Normal(GroupCommand::DeleteBookmark {
                    partition,
                    id: bookmark_id,
                }),
            },
            GroupEntry {
                log_id: GroupLogId::new(
                    GroupLeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    5,
                ),
                payload: EntryPayload::Normal(GroupCommand::PublishMany {
                    batch: ReplicatedPublishBatch::new(vec![rejected]).unwrap(),
                }),
            },
        ];
        log.append(entries.clone(), IOFlushed::noop())
            .await
            .unwrap();
        state
            .apply(futures_util::stream::iter(
                entries.into_iter().map(|entry| Ok((entry, None))),
            ))
            .await
            .unwrap();

        assert_eq!(
            handles.reader.receipt(partition, &request).unwrap_err(),
            DomainError::BookmarkNameConflict
        );
        assert!(
            handles
                .reader
                .fetch(cluster, partition, RecordOffset::new(0), 10)
                .unwrap()
                .records()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn publish_bookmark_id_collision_rejects_without_poisoning_the_group() {
        let directory = ProjectTestDir::new("publish-bookmark-id-collision");
        let (cluster, stream) = ids();
        let handles = create_data_store(
            &directory.0,
            GroupIdentity::new(
                cluster,
                GroupId::new(DATA_GROUP_ID).unwrap(),
                GroupKind::Data,
            ),
            DEFAULT_RECEIPT_WINDOW,
            test_budget(),
        )
        .unwrap();
        let mut log = handles.log_store;
        let mut state = handles.state_machine;
        let partition = PartitionKey::new(stream, PartitionId::new(0));
        let publish_log_id = GroupLogId::new(
            GroupLeaderId {
                term: 1,
                node_id: 1,
            },
            3,
        );
        let colliding_id = publish_bookmark_id(publish_log_id, Some(0));
        let request = ProducerRequestId::new(
            light_stream_core::PrincipalId::parse("producer").unwrap(),
            light_stream_core::ProducerSessionId::from_uuid(Uuid::new_v4()),
            light_stream_core::RequestSequence::new(1),
        );
        let publish = PublishBatch::new(
            cluster,
            partition,
            request.clone(),
            vec![b"rejected".to_vec()],
        )
        .unwrap()
        .with_bookmark(BookmarkName::parse("publish-name").unwrap());
        let entries = vec![
            GroupEntry {
                log_id: GroupLogId::new(
                    GroupLeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    1,
                ),
                payload: EntryPayload::Normal(GroupCommand::BootstrapData {
                    spec: BootstrapSpec::new(
                        cluster,
                        stream,
                        StreamName::parse("bootstrap").unwrap(),
                    ),
                }),
            },
            GroupEntry {
                log_id: GroupLogId::new(
                    GroupLeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    2,
                ),
                payload: EntryPayload::Normal(GroupCommand::CreateBookmark {
                    id: colliding_id,
                    partition,
                    name: BookmarkName::parse("client-name").unwrap(),
                    offset: RecordOffset::new(0),
                }),
            },
            GroupEntry {
                log_id: publish_log_id,
                payload: EntryPayload::Normal(GroupCommand::PublishMany {
                    batch: ReplicatedPublishBatch::new(vec![publish]).unwrap(),
                }),
            },
        ];
        log.append(entries.clone(), IOFlushed::noop())
            .await
            .unwrap();
        state
            .apply(futures_util::stream::iter(
                entries.into_iter().map(|entry| Ok((entry, None))),
            ))
            .await
            .unwrap();

        assert_eq!(
            handles.reader.receipt(partition, &request).unwrap_err(),
            DomainError::BookmarkNameConflict
        );
        assert!(
            handles
                .reader
                .fetch(cluster, partition, RecordOffset::new(0), 10)
                .unwrap()
                .records()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn legacy_publish_retry_returns_its_original_receipt() {
        let directory = ProjectTestDir::new("legacy-publish-retry");
        let (cluster, stream) = ids();
        let handles = create_data_store(
            &directory.0,
            GroupIdentity::new(
                cluster,
                GroupId::new(DATA_GROUP_ID).unwrap(),
                GroupKind::Data,
            ),
            DEFAULT_RECEIPT_WINDOW,
            test_budget(),
        )
        .unwrap();
        let mut log = handles.log_store;
        let mut state = handles.state_machine;
        let partition = PartitionKey::new(stream, PartitionId::new(0));
        let request = ProducerRequestId::new(
            light_stream_core::PrincipalId::parse("legacy").unwrap(),
            light_stream_core::ProducerSessionId::from_uuid(Uuid::new_v4()),
            light_stream_core::RequestSequence::new(1),
        );
        let publish = PublishBatch::new(
            cluster,
            partition,
            request.clone(),
            vec![b"legacy".to_vec()],
        )
        .unwrap()
        .with_bookmark(BookmarkName::parse("legacy-bookmark").unwrap());
        let entries = vec![
            GroupEntry {
                log_id: GroupLogId::new(
                    GroupLeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    1,
                ),
                payload: EntryPayload::Normal(GroupCommand::BootstrapData {
                    spec: BootstrapSpec::new(
                        cluster,
                        stream,
                        StreamName::parse("bootstrap").unwrap(),
                    ),
                }),
            },
            GroupEntry {
                log_id: GroupLogId::new(
                    GroupLeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    2,
                ),
                payload: EntryPayload::Normal(GroupCommand::Publish {
                    batch: publish.clone(),
                }),
            },
            GroupEntry {
                log_id: GroupLogId::new(
                    GroupLeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    3,
                ),
                payload: EntryPayload::Normal(GroupCommand::Publish { batch: publish }),
            },
        ];
        log.append(entries.clone(), IOFlushed::noop())
            .await
            .unwrap();
        state
            .apply(futures_util::stream::iter(
                entries.into_iter().map(|entry| Ok((entry, None))),
            ))
            .await
            .unwrap();

        let receipt = handles.reader.receipt(partition, &request).unwrap();
        assert_eq!(receipt.range().first(), RecordOffset::new(0));
        assert_eq!(receipt.range().next(), RecordOffset::new(1));
        assert_eq!(
            handles
                .reader
                .fetch(cluster, partition, RecordOffset::new(0), 10)
                .unwrap()
                .records()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn bookmarks_are_atomic_ordered_and_reusable_by_name() {
        let directory = ProjectTestDir::new("bookmarks");
        let (cluster, stream) = ids();
        let identity = GroupIdentity::new(
            cluster,
            GroupId::new(DATA_GROUP_ID).unwrap(),
            GroupKind::Data,
        );
        let handles = create_data_store(
            &directory.0,
            identity,
            DEFAULT_RECEIPT_WINDOW,
            test_budget(),
        )
        .unwrap();
        let mut log = handles.log_store;
        let mut state = handles.state_machine;
        let partition = PartitionKey::new(stream, PartitionId::new(0));
        let bootstrap_spec =
            BootstrapSpec::new(cluster, stream, StreamName::parse("bootstrap").unwrap());
        let first_id = BookmarkId::from_uuid(Uuid::new_v4());
        let second_id = BookmarkId::from_uuid(Uuid::new_v4());
        let publish = PublishBatch::new(
            cluster,
            partition,
            ProducerRequestId::new(
                light_stream_core::PrincipalId::parse("test").unwrap(),
                light_stream_core::ProducerSessionId::from_uuid(Uuid::new_v4()),
                light_stream_core::RequestSequence::new(1),
            ),
            vec![b"first".to_vec()],
        )
        .unwrap()
        .with_bookmark(BookmarkName::parse("after-first").unwrap());
        let entries = vec![
            GroupEntry {
                log_id: GroupLogId::new(
                    GroupLeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    1,
                ),
                payload: EntryPayload::Normal(GroupCommand::BootstrapData {
                    spec: bootstrap_spec,
                }),
            },
            GroupEntry {
                log_id: GroupLogId::new(
                    GroupLeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    2,
                ),
                payload: EntryPayload::Normal(GroupCommand::Publish { batch: publish }),
            },
            GroupEntry {
                log_id: GroupLogId::new(
                    GroupLeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    3,
                ),
                payload: EntryPayload::Normal(GroupCommand::CreateBookmark {
                    id: first_id,
                    partition,
                    name: BookmarkName::parse("before-first").unwrap(),
                    offset: RecordOffset::new(0),
                }),
            },
            GroupEntry {
                log_id: GroupLogId::new(
                    GroupLeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    4,
                ),
                payload: EntryPayload::Normal(GroupCommand::DeleteBookmark {
                    partition,
                    id: first_id,
                }),
            },
            GroupEntry {
                log_id: GroupLogId::new(
                    GroupLeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    5,
                ),
                payload: EntryPayload::Normal(GroupCommand::CreateBookmark {
                    id: second_id,
                    partition,
                    name: BookmarkName::parse("before-first").unwrap(),
                    offset: RecordOffset::new(1),
                }),
            },
        ];
        log.append(entries.clone(), IOFlushed::noop())
            .await
            .unwrap();
        state
            .apply(futures_util::stream::iter(
                entries.into_iter().map(|entry| Ok((entry, None))),
            ))
            .await
            .unwrap();
        let atomic = handles
            .reader
            .resolve_bookmark(partition, &BookmarkName::parse("after-first").unwrap())
            .unwrap();
        assert_eq!(atomic.cursor().next_offset(), RecordOffset::new(1));
        assert_eq!(atomic.publication().get(), 1);
        let replacement = handles
            .reader
            .resolve_bookmark(partition, &BookmarkName::parse("before-first").unwrap())
            .unwrap();
        assert_eq!(replacement.id(), second_id);
        assert_eq!(replacement.publication().get(), 3);
        assert_eq!(
            handles
                .reader
                .bookmark_by_id(partition, first_id)
                .unwrap()
                .lifecycle(),
            light_stream_core::BookmarkLifecycle::Deleted
        );
        let page = handles
            .reader
            .list_bookmarks(&BookmarkPageRequest::new(partition, 10, None, None).unwrap())
            .unwrap();
        assert_eq!(
            page.items()
                .iter()
                .map(|bookmark| bookmark.id())
                .collect::<Vec<_>>(),
            vec![second_id, atomic.id()]
        );
    }

    #[tokio::test]
    async fn stream_bookmark_vectors_live_in_the_control_catalog() {
        let directory = ProjectTestDir::new("stream-bookmarks");
        let (cluster, stream) = ids();
        let identity = GroupIdentity::new(
            cluster,
            GroupId::new(CONTROL_GROUP_ID).unwrap(),
            GroupKind::Control,
        );
        let handles = create_control_store(
            &directory.0,
            identity,
            DEFAULT_RECEIPT_WINDOW,
            test_budget(),
        )
        .unwrap();
        let mut log = handles.log_store;
        let mut state = handles.state_machine;
        let spec = BootstrapSpec::new(cluster, stream, StreamName::parse("bootstrap").unwrap());
        let id = BookmarkId::from_uuid(Uuid::new_v4());
        let vector = StreamCursorVector::new(
            stream,
            vec![CommittedCursor::new(
                cluster,
                PartitionKey::new(stream, PartitionId::new(0)),
                RecordOffset::new(0),
            )],
        )
        .unwrap();
        let entries = vec![
            GroupEntry {
                log_id: GroupLogId::new(
                    GroupLeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    1,
                ),
                payload: EntryPayload::Normal(GroupCommand::BootstrapControl {
                    spec,
                    topology: Some(test_topology()),
                    security: None,
                    data_groups: vec![GroupId::new(DATA_GROUP_ID).unwrap()],
                    max_streams: 8,
                    max_partitions_per_stream: 4,
                }),
            },
            GroupEntry {
                log_id: GroupLogId::new(
                    GroupLeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    2,
                ),
                payload: EntryPayload::Normal(GroupCommand::CreateStreamBookmark {
                    id,
                    name: BookmarkName::parse("vector").unwrap(),
                    vector: vector.clone(),
                }),
            },
        ];
        log.append(entries.clone(), IOFlushed::noop())
            .await
            .unwrap();
        state
            .apply(futures_util::stream::iter(
                entries.into_iter().map(|entry| Ok((entry, None))),
            ))
            .await
            .unwrap();
        let bookmark = handles
            .reader
            .resolve_stream_bookmark(stream, &BookmarkName::parse("vector").unwrap())
            .unwrap();
        assert_eq!(bookmark.id(), id);
        assert_eq!(bookmark.vector(), &vector);
        assert_eq!(
            handles
                .reader
                .list_stream_bookmarks(
                    &StreamBookmarkPageRequest::new(cluster, stream, 10, None, None).unwrap()
                )
                .unwrap()
                .items(),
            &[bookmark]
        );
    }

    #[tokio::test]
    async fn administration_intents_are_idempotent_exclusive_and_restartable() {
        let directory = ProjectTestDir::new("administration-intents");
        let (cluster, stream) = ids();
        let identity = GroupIdentity::new(
            cluster,
            GroupId::new(CONTROL_GROUP_ID).unwrap(),
            GroupKind::Control,
        );
        let bootstrap =
            BootstrapSpec::new(cluster, stream, StreamName::parse("bootstrap").unwrap());
        let node1 = light_stream_core::NodeDescriptor::new(
            NodeId::new(1).unwrap(),
            "http://127.0.0.1:7101",
            "http://127.0.0.1:7201",
        );
        let node2 = light_stream_core::NodeDescriptor::new(
            NodeId::new(2).unwrap(),
            "http://127.0.0.1:7102",
            "http://127.0.0.1:7202",
        );
        let node3 = light_stream_core::NodeDescriptor::new(
            NodeId::new(3).unwrap(),
            "http://127.0.0.1:7103",
            "http://127.0.0.1:7203",
        );
        let topology = ClusterTopology::try_new(
            1,
            [node1.clone(), node2.clone(), node3.clone()],
            [node1.node_id(), node2.node_id(), node3.node_id()],
        )
        .unwrap();
        let request = AdministrationRequestId::from_uuid(Uuid::new_v4());
        let replacement = light_stream_core::NodeDescriptor::new(
            NodeId::new(4).unwrap(),
            "http://127.0.0.1:7104",
            "http://127.0.0.1:7204",
        );
        let intent = AdministrationIntent::ReplaceVoter {
            request,
            expected_topology_revision: 1,
            remove: node3.node_id(),
            add: replacement.clone(),
        };
        let other_request = AdministrationRequestId::from_uuid(Uuid::new_v4());
        {
            let handles = create_control_store(
                &directory.0,
                identity.clone(),
                DEFAULT_RECEIPT_WINDOW,
                test_budget(),
            )
            .unwrap();
            let mut state = handles.state_machine;
            let commands = [
                GroupCommand::BootstrapControl {
                    spec: bootstrap,
                    topology: Some(topology.clone()),
                    security: None,
                    data_groups: vec![GroupId::new(DATA_GROUP_ID).unwrap()],
                    max_streams: 8,
                    max_partitions_per_stream: 4,
                },
                GroupCommand::BeginAdministration {
                    intent: intent.clone(),
                },
                GroupCommand::BeginAdministration {
                    intent: intent.clone(),
                },
                GroupCommand::BeginAdministration {
                    intent: AdministrationIntent::TransferLeader {
                        request: other_request,
                        group: GroupId::new(DATA_GROUP_ID).unwrap(),
                        target: node2.node_id(),
                    },
                },
            ];
            let entries = commands
                .into_iter()
                .enumerate()
                .map(|(index, command)| GroupEntry {
                    log_id: GroupLogId::new(
                        GroupLeaderId {
                            term: 1,
                            node_id: 1,
                        },
                        index as u64 + 1,
                    ),
                    payload: EntryPayload::Normal(command),
                });
            state
                .apply(futures_util::stream::iter(
                    entries.map(|entry| Ok((entry, None))),
                ))
                .await
                .unwrap();
            let active = handles.reader.active_administration().unwrap().unwrap();
            assert_eq!(active.intent(), &intent);
            assert!(
                handles
                    .reader
                    .administration_operation(other_request)
                    .unwrap()
                    .is_none()
            );
            let conflict = handles
                .reader
                .db
                .apply_entry(GroupEntry {
                    log_id: GroupLogId::new(
                        GroupLeaderId {
                            term: 1,
                            node_id: 1,
                        },
                        5,
                    ),
                    payload: EntryPayload::Normal(GroupCommand::BeginAdministration {
                        intent: AdministrationIntent::TransferLeader {
                            request,
                            group: GroupId::new(DATA_GROUP_ID).unwrap(),
                            target: node2.node_id(),
                        },
                    }),
                })
                .unwrap();
            assert_eq!(
                ApplyResult::Rejected(DomainError::MutationConflict),
                conflict
            );
            let transitional_topology = topology
                .replacement_transition(1, node3.node_id(), replacement.clone())
                .unwrap();
            assert_eq!(
                handles.reader.cluster_topology().unwrap(),
                Some(transitional_topology.clone())
            );
            let final_topology = transitional_topology
                .replacement_complete(1, node3.node_id(), replacement)
                .unwrap();
            let complete = GroupEntry {
                log_id: GroupLogId::new(
                    GroupLeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    6,
                ),
                payload: EntryPayload::Normal(GroupCommand::CompleteAdministration { request }),
            };
            state
                .apply(futures_util::stream::iter([Ok((complete, None))]))
                .await
                .unwrap();
            assert!(handles.reader.active_administration().unwrap().is_none());
            assert_eq!(
                handles.reader.cluster_topology().unwrap(),
                Some(final_topology)
            );
            let stale_request = AdministrationRequestId::from_uuid(Uuid::new_v4());
            let stale = handles
                .reader
                .db
                .apply_entry(GroupEntry {
                    log_id: GroupLogId::new(
                        GroupLeaderId {
                            term: 1,
                            node_id: 1,
                        },
                        7,
                    ),
                    payload: EntryPayload::Normal(GroupCommand::BeginAdministration {
                        intent: AdministrationIntent::ReplaceVoter {
                            request: stale_request,
                            expected_topology_revision: 1,
                            remove: node2.node_id(),
                            add: light_stream_core::NodeDescriptor::new(
                                NodeId::new(5).unwrap(),
                                "http://127.0.0.1:7105",
                                "http://127.0.0.1:7205",
                            ),
                        },
                    }),
                })
                .unwrap();
            assert_eq!(ApplyResult::Rejected(DomainError::StaleRoute), stale);
            assert!(handles.reader.active_administration().unwrap().is_none());
            let invalid_group = handles
                .reader
                .db
                .apply_entry(GroupEntry {
                    log_id: GroupLogId::new(
                        GroupLeaderId {
                            term: 1,
                            node_id: 1,
                        },
                        8,
                    ),
                    payload: EntryPayload::Normal(GroupCommand::BeginAdministration {
                        intent: AdministrationIntent::TransferLeader {
                            request: AdministrationRequestId::from_uuid(Uuid::new_v4()),
                            group: GroupId::new(999).unwrap(),
                            target: node2.node_id(),
                        },
                    }),
                })
                .unwrap();
            assert!(matches!(
                invalid_group,
                ApplyResult::Rejected(DomainError::InvalidIdentity { .. })
            ));
            let transfer_request = AdministrationRequestId::from_uuid(Uuid::new_v4());
            let begin_transfer = handles
                .reader
                .db
                .apply_entry(GroupEntry {
                    log_id: GroupLogId::new(
                        GroupLeaderId {
                            term: 1,
                            node_id: 1,
                        },
                        9,
                    ),
                    payload: EntryPayload::Normal(GroupCommand::BeginAdministration {
                        intent: AdministrationIntent::TransferLeader {
                            request: transfer_request,
                            group: GroupId::new(DATA_GROUP_ID).unwrap(),
                            target: node2.node_id(),
                        },
                    }),
                })
                .unwrap();
            assert!(matches!(begin_transfer, ApplyResult::Administration(_)));
            let abort = handles
                .reader
                .db
                .apply_entry(GroupEntry {
                    log_id: GroupLogId::new(
                        GroupLeaderId {
                            term: 1,
                            node_id: 1,
                        },
                        10,
                    ),
                    payload: EntryPayload::Normal(GroupCommand::AbortAdministration {
                        request: transfer_request,
                    }),
                })
                .unwrap();
            assert!(matches!(
                abort,
                ApplyResult::Administration(operation)
                    if matches!(
                        operation.lifecycle(),
                        light_stream_core::AdministrationLifecycle::Aborted { .. }
                    )
            ));
            assert!(handles.reader.active_administration().unwrap().is_some());
            handles
                .reader
                .db
                .apply_entry(GroupEntry {
                    log_id: GroupLogId::new(
                        GroupLeaderId {
                            term: 1,
                            node_id: 1,
                        },
                        11,
                    ),
                    payload: EntryPayload::Normal(GroupCommand::FinishAdministrationAbort {
                        request: transfer_request,
                    }),
                })
                .unwrap();
            assert!(handles.reader.active_administration().unwrap().is_none());
            let next_request = AdministrationRequestId::from_uuid(Uuid::new_v4());
            handles
                .reader
                .db
                .apply_entry(GroupEntry {
                    log_id: GroupLogId::new(
                        GroupLeaderId {
                            term: 1,
                            node_id: 1,
                        },
                        12,
                    ),
                    payload: EntryPayload::Normal(GroupCommand::BeginAdministration {
                        intent: AdministrationIntent::TransferLeader {
                            request: next_request,
                            group: GroupId::new(DATA_GROUP_ID).unwrap(),
                            target: node2.node_id(),
                        },
                    }),
                })
                .unwrap();
            let complete_aborted = handles
                .reader
                .db
                .apply_entry(GroupEntry {
                    log_id: GroupLogId::new(
                        GroupLeaderId {
                            term: 1,
                            node_id: 1,
                        },
                        13,
                    ),
                    payload: EntryPayload::Normal(GroupCommand::CompleteAdministration {
                        request: transfer_request,
                    }),
                })
                .unwrap();
            assert_eq!(
                ApplyResult::Administration(
                    handles
                        .reader
                        .administration_operation(transfer_request)
                        .unwrap()
                        .unwrap()
                ),
                complete_aborted
            );
            assert_eq!(
                next_request,
                handles
                    .reader
                    .active_administration()
                    .unwrap()
                    .unwrap()
                    .intent()
                    .request()
            );
        }
        let handles = open_control_store(
            &directory.0,
            &identity,
            DEFAULT_RECEIPT_WINDOW,
            test_budget(),
        )
        .unwrap();
        let operation = handles
            .reader
            .administration_operation(request)
            .unwrap()
            .unwrap();
        assert!(matches!(
            operation.lifecycle(),
            light_stream_core::AdministrationLifecycle::Complete {
                completed_topology_revision: 3
            }
        ));
    }

    #[tokio::test]
    async fn committed_topology_wins_over_a_stale_manifest_topology() {
        let directory = ProjectTestDir::new("authoritative-control-topology");
        let (cluster, _stream) = ids();
        let identity = GroupIdentity::new(
            cluster,
            GroupId::new(CONTROL_GROUP_ID).unwrap(),
            GroupKind::Control,
        );
        let old_topology = ClusterTopology::try_new(
            1,
            [
                light_stream_core::NodeDescriptor::new(
                    NodeId::new(1).unwrap(),
                    "http://127.0.0.1:7101",
                    "http://127.0.0.1:7201",
                ),
                light_stream_core::NodeDescriptor::new(
                    NodeId::new(2).unwrap(),
                    "http://127.0.0.1:7102",
                    "http://127.0.0.1:7202",
                ),
                light_stream_core::NodeDescriptor::new(
                    NodeId::new(3).unwrap(),
                    "http://127.0.0.1:7103",
                    "http://127.0.0.1:7203",
                ),
            ],
            [
                NodeId::new(1).unwrap(),
                NodeId::new(2).unwrap(),
                NodeId::new(3).unwrap(),
            ],
        )
        .unwrap();
        let transition = old_topology
            .replacement_transition(
                1,
                NodeId::new(3).unwrap(),
                light_stream_core::NodeDescriptor::new(
                    NodeId::new(4).unwrap(),
                    "http://127.0.0.1:7104",
                    "http://127.0.0.1:7204",
                ),
            )
            .unwrap();
        {
            let handles = create_control_store(
                &directory.0,
                identity.clone(),
                DEFAULT_RECEIPT_WINDOW,
                test_budget(),
            )
            .unwrap();
            handles
                .reader
                .db
                .put_sync(CF_STATE, KEY_CLUSTER_TOPOLOGY, &transition)
                .unwrap();
        }

        let handles = open_control_store_with_topology(
            &directory.0,
            &identity,
            DEFAULT_RECEIPT_WINDOW,
            test_budget(),
            &old_topology,
        )
        .unwrap();

        assert_eq!(handles.reader.cluster_topology().unwrap(), Some(transition));
    }

    #[tokio::test]
    async fn legacy_control_snapshot_is_rebuilt_with_initialized_topology() {
        let source = ProjectTestDir::new("legacy-control-snapshot-topology");
        let (cluster, stream) = ids();
        let identity = GroupIdentity::new(
            cluster,
            GroupId::new(CONTROL_GROUP_ID).unwrap(),
            GroupKind::Control,
        );
        let topology = test_topology();
        {
            let handles = create_control_store(
                &source.0,
                identity.clone(),
                DEFAULT_RECEIPT_WINDOW,
                test_budget(),
            )
            .unwrap();
            let mut state = handles.state_machine;
            state
                .apply(futures_util::stream::iter([Ok((
                    GroupEntry {
                        log_id: GroupLogId::new(
                            GroupLeaderId {
                                term: 1,
                                node_id: 1,
                            },
                            1,
                        ),
                        payload: EntryPayload::Normal(GroupCommand::BootstrapControl {
                            spec: BootstrapSpec::new(
                                cluster,
                                stream,
                                StreamName::parse("bootstrap").unwrap(),
                            ),
                            topology: None,
                            security: None,
                            data_groups: vec![GroupId::new(DATA_GROUP_ID).unwrap()],
                            max_streams: 8,
                            max_partitions_per_stream: 4,
                        }),
                    },
                    None,
                ))]))
                .await
                .unwrap();
            state
                .get_snapshot_builder()
                .await
                .build_snapshot()
                .await
                .unwrap();
        }

        let handles = open_control_store_with_topology(
            &source.0,
            &identity,
            DEFAULT_RECEIPT_WINDOW,
            test_budget(),
            &topology,
        )
        .unwrap();
        let (_, artifact) = handles
            .reader
            .db
            .current_snapshot_artifact()
            .unwrap()
            .unwrap();
        let mut reader = artifact.reader().unwrap();
        let mut stored = None;
        while let Some(record) = reader.next_record().unwrap() {
            if let SnapshotRecord::State { key, value } = record
                && key == KEY_CLUSTER_TOPOLOGY
            {
                stored = Some(decode::<ClusterTopology>(&value).unwrap());
            }
        }
        assert_eq!(Some(topology), stored);
    }

    #[tokio::test]
    async fn retention_floor_expires_fetch_without_deleting_bookmarks() {
        let directory = ProjectTestDir::new("retention-floor");
        let (cluster, stream) = ids();
        let identity = GroupIdentity::new(
            cluster,
            GroupId::new(DATA_GROUP_ID).unwrap(),
            GroupKind::Data,
        );
        let handles = create_data_store(
            &directory.0,
            identity,
            DEFAULT_RECEIPT_WINDOW,
            test_budget(),
        )
        .unwrap();
        let mut log = handles.log_store;
        let mut state = handles.state_machine;
        let partition = PartitionKey::new(stream, PartitionId::new(0));
        let bookmark_id = BookmarkId::from_uuid(Uuid::new_v4());
        let mutation = MutationRequestId::new(
            light_stream_core::PrincipalId::parse("operator").unwrap(),
            light_stream_core::MutationSessionId::from_uuid(Uuid::new_v4()),
            light_stream_core::RequestSequence::new(1),
        );
        let retention_request =
            RetentionRequest::new(mutation.clone(), partition, RecordOffset::new(2));
        let entries = vec![
            GroupEntry {
                log_id: GroupLogId::new(
                    GroupLeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    1,
                ),
                payload: EntryPayload::Normal(GroupCommand::BootstrapData {
                    spec: BootstrapSpec::new(
                        cluster,
                        stream,
                        StreamName::parse("bootstrap").unwrap(),
                    ),
                }),
            },
            GroupEntry {
                log_id: GroupLogId::new(
                    GroupLeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    2,
                ),
                payload: EntryPayload::Normal(GroupCommand::Publish {
                    batch: PublishBatch::new(
                        cluster,
                        partition,
                        ProducerRequestId::new(
                            light_stream_core::PrincipalId::parse("test").unwrap(),
                            light_stream_core::ProducerSessionId::from_uuid(Uuid::new_v4()),
                            light_stream_core::RequestSequence::new(1),
                        ),
                        vec![b"zero".to_vec(), b"one".to_vec(), b"two".to_vec()],
                    )
                    .unwrap(),
                }),
            },
            GroupEntry {
                log_id: GroupLogId::new(
                    GroupLeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    3,
                ),
                payload: EntryPayload::Normal(GroupCommand::CreateBookmark {
                    id: bookmark_id,
                    partition,
                    name: BookmarkName::parse("old-boundary").unwrap(),
                    offset: RecordOffset::new(1),
                }),
            },
            GroupEntry {
                log_id: GroupLogId::new(
                    GroupLeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    4,
                ),
                payload: EntryPayload::Normal(GroupCommand::AdvanceRetention {
                    request: retention_request.clone(),
                    clock: ClockObservation::new(8_000, 12_000).unwrap(),
                }),
            },
        ];
        log.append(entries.clone(), IOFlushed::noop())
            .await
            .unwrap();
        state
            .apply(futures_util::stream::iter(
                entries.into_iter().map(|entry| Ok((entry, None))),
            ))
            .await
            .unwrap();
        let duplicate = GroupEntry {
            log_id: GroupLogId::new(
                GroupLeaderId {
                    term: 1,
                    node_id: 1,
                },
                5,
            ),
            payload: EntryPayload::Normal(GroupCommand::AdvanceRetention {
                request: retention_request,
                clock: ClockObservation::new(9_000, 13_000).unwrap(),
            }),
        };
        log.append([duplicate.clone()], IOFlushed::noop())
            .await
            .unwrap();
        state
            .apply(futures_util::stream::iter([Ok((duplicate, None))]))
            .await
            .unwrap();
        let retained_result = handles.reader.retention_receipt(&mutation).unwrap();
        assert_eq!(retained_result.previous_floor(), RecordOffset::new(0));
        assert_eq!(retained_result.floor(), RecordOffset::new(2));
        let conflict = GroupEntry {
            log_id: GroupLogId::new(
                GroupLeaderId {
                    term: 1,
                    node_id: 1,
                },
                6,
            ),
            payload: EntryPayload::Normal(GroupCommand::AdvanceRetention {
                request: RetentionRequest::new(mutation.clone(), partition, RecordOffset::new(3)),
                clock: ClockObservation::new(10_000, 14_000).unwrap(),
            }),
        };
        log.append([conflict.clone()], IOFlushed::noop())
            .await
            .unwrap();
        state
            .apply(futures_util::stream::iter([Ok((conflict, None))]))
            .await
            .unwrap();
        assert_eq!(
            handles.reader.retention_receipt(&mutation).unwrap().floor(),
            RecordOffset::new(2)
        );

        let error = handles
            .reader
            .fetch(cluster, partition, RecordOffset::new(1), 10)
            .unwrap_err();
        assert!(matches!(
            error,
            DomainError::CursorExpired {
                requested,
                available_from
            } if requested == RecordOffset::new(1) && available_from == RecordOffset::new(2)
        ));
        assert_eq!(
            handles
                .reader
                .fetch(cluster, partition, RecordOffset::new(2), 10)
                .unwrap()
                .records()[0]
                .payload(),
            b"two"
        );
        assert_eq!(
            handles
                .reader
                .resolve_bookmark(partition, &BookmarkName::parse("old-boundary").unwrap())
                .unwrap()
                .id(),
            bookmark_id
        );
        let status = handles.reader.retention_status(partition).unwrap();
        assert_eq!(status.logical_floor(), RecordOffset::new(2));
        assert_eq!(status.reclaim_cursor(), RecordOffset::new(0));
    }

    #[tokio::test]
    async fn replay_lease_preserves_a_bounded_island_below_the_floor() {
        let directory = ProjectTestDir::new("replay-lease");
        let (cluster, stream) = ids();
        let handles = create_data_store(
            &directory.0,
            GroupIdentity::new(
                cluster,
                GroupId::new(DATA_GROUP_ID).unwrap(),
                GroupKind::Data,
            ),
            DEFAULT_RECEIPT_WINDOW,
            test_budget(),
        )
        .unwrap();
        let mut log = handles.log_store;
        let mut state = handles.state_machine;
        let partition = PartitionKey::new(stream, PartitionId::new(0));
        let session = light_stream_core::MutationSessionId::from_uuid(Uuid::new_v4());
        let admission_id = MutationRequestId::new(
            light_stream_core::PrincipalId::parse("replay").unwrap(),
            session,
            light_stream_core::RequestSequence::new(1),
        );
        let lease_request = light_stream_core::ReplayLeaseRequest::new(
            admission_id.clone(),
            cluster,
            light_stream_core::ReplayRange::new(
                partition,
                RecordOffset::new(1),
                RecordOffset::new(3),
            )
            .unwrap(),
            light_stream_core::LeaseDuration::from_millis(30_000).unwrap(),
            light_stream_core::ByteLimit::new(1024).unwrap(),
        );
        let entries = vec![
            GroupEntry {
                log_id: GroupLogId::new(
                    GroupLeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    1,
                ),
                payload: EntryPayload::Normal(GroupCommand::BootstrapData {
                    spec: BootstrapSpec::new(
                        cluster,
                        stream,
                        StreamName::parse("bootstrap").unwrap(),
                    ),
                }),
            },
            GroupEntry {
                log_id: GroupLogId::new(
                    GroupLeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    2,
                ),
                payload: EntryPayload::Normal(GroupCommand::Publish {
                    batch: PublishBatch::new(
                        cluster,
                        partition,
                        ProducerRequestId::new(
                            light_stream_core::PrincipalId::parse("test").unwrap(),
                            light_stream_core::ProducerSessionId::from_uuid(Uuid::new_v4()),
                            light_stream_core::RequestSequence::new(1),
                        ),
                        vec![
                            b"zero".to_vec(),
                            b"one".to_vec(),
                            b"two".to_vec(),
                            b"three".to_vec(),
                        ],
                    )
                    .unwrap(),
                }),
            },
            GroupEntry {
                log_id: GroupLogId::new(
                    GroupLeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    3,
                ),
                payload: EntryPayload::Normal(GroupCommand::AdmitReplayLease {
                    request: lease_request,
                    clock: ClockObservation::new(8_000, 12_000).unwrap(),
                }),
            },
            GroupEntry {
                log_id: GroupLogId::new(
                    GroupLeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    4,
                ),
                payload: EntryPayload::Normal(GroupCommand::AdvanceRetention {
                    request: RetentionRequest::new(
                        MutationRequestId::new(
                            light_stream_core::PrincipalId::parse("operator").unwrap(),
                            light_stream_core::MutationSessionId::from_uuid(Uuid::new_v4()),
                            light_stream_core::RequestSequence::new(1),
                        ),
                        partition,
                        RecordOffset::new(4),
                    ),
                    clock: ClockObservation::new(9_000, 13_000).unwrap(),
                }),
            },
            GroupEntry {
                log_id: GroupLogId::new(
                    GroupLeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    5,
                ),
                payload: EntryPayload::Normal(GroupCommand::MaintainRetention {
                    partition,
                    expected_cursor: RecordOffset::new(0),
                    max_records: 128,
                    max_payload_bytes: 1024,
                    clock: ClockObservation::new(10_000, 14_000).unwrap(),
                }),
            },
        ];
        log.append(entries.clone(), IOFlushed::noop())
            .await
            .unwrap();
        state
            .apply(futures_util::stream::iter(
                entries.into_iter().map(|entry| Ok((entry, None))),
            ))
            .await
            .unwrap();
        let lease = handles
            .reader
            .replay_lease_by_request(&admission_id)
            .unwrap();
        let page = handles
            .reader
            .fetch_protected(cluster, partition, lease.id(), RecordOffset::new(1), 10)
            .unwrap();
        assert_eq!(
            page.records()
                .iter()
                .map(|record| record.payload())
                .collect::<Vec<_>>(),
            vec![b"one".as_slice(), b"two".as_slice()]
        );
        let other_partition = PartitionKey::new(stream, PartitionId::new(1));
        assert!(matches!(
            handles.reader.fetch_protected(
                cluster,
                other_partition,
                lease.id(),
                RecordOffset::new(1),
                10,
            ),
            Err(DomainError::ReplayLeaseRangeViolation)
        ));
        let mismatched_release = GroupEntry {
            log_id: GroupLogId::new(
                GroupLeaderId {
                    term: 1,
                    node_id: 1,
                },
                6,
            ),
            payload: EntryPayload::Normal(GroupCommand::ReleaseReplayLease {
                request: light_stream_core::LeaseRelease::new(
                    MutationRequestId::new(
                        light_stream_core::PrincipalId::parse("replay").unwrap(),
                        light_stream_core::MutationSessionId::from_uuid(Uuid::new_v4()),
                        light_stream_core::RequestSequence::new(1),
                    ),
                    other_partition,
                    lease.id(),
                ),
                clock: ClockObservation::new(11_000, 15_000).unwrap(),
            }),
        };
        log.append([mismatched_release.clone()], IOFlushed::noop())
            .await
            .unwrap();
        state
            .apply(futures_util::stream::iter([Ok((mismatched_release, None))]))
            .await
            .unwrap();
        assert_eq!(
            handles
                .reader
                .replay_lease(partition, lease.id())
                .unwrap()
                .lifecycle(),
            light_stream_core::ReplayLeaseLifecycle::Active
        );
        let expired_other = ReplayLease::admitted(
            ReplayLeaseId::from_uuid(Uuid::new_v4()),
            ReplayLeaseRequest::new(
                MutationRequestId::new(
                    light_stream_core::PrincipalId::parse("other").unwrap(),
                    light_stream_core::MutationSessionId::from_uuid(Uuid::new_v4()),
                    light_stream_core::RequestSequence::new(1),
                ),
                cluster,
                light_stream_core::ReplayRange::new(
                    other_partition,
                    RecordOffset::new(0),
                    RecordOffset::new(1),
                )
                .unwrap(),
                light_stream_core::LeaseDuration::from_millis(1).unwrap(),
                light_stream_core::ByteLimit::new(1).unwrap(),
            ),
            ByteCount::new(1),
            LeaseDeadline::new(10_000),
            LeaseDeadline::new(10_000),
        );
        let mut fixture = WriteBatch::default();
        fixture.put_cf(
            &handles.reader.db.cf(CF_STATE).unwrap(),
            replay_lease_id_key(expired_other.id()),
            encode(&expired_other).unwrap(),
        );
        fixture.put_cf(
            &handles.reader.db.cf(CF_STATE).unwrap(),
            KEY_LEASE_BUDGET,
            encode(&LeaseBudget {
                active_leases: 2,
                reserved_bytes: lease
                    .protected_bytes()
                    .get()
                    .saturating_add(expired_other.protected_bytes().get()),
            })
            .unwrap(),
        );
        handles.reader.db.write_sync(fixture).unwrap();
        assert!(matches!(
            handles
                .reader
                .fetch(cluster, partition, RecordOffset::new(1), 10)
                .unwrap_err(),
            DomainError::CursorExpired { .. }
        ));
        let status = handles.reader.retention_status(partition).unwrap();
        assert_eq!(status.reclaim_cursor(), RecordOffset::new(4));
        assert_eq!(status.logically_expired_bytes(), ByteCount::new(9));
        assert_eq!(status.raft_only_bytes(), ByteCount::new(9));

        let release = GroupEntry {
            log_id: GroupLogId::new(
                GroupLeaderId {
                    term: 1,
                    node_id: 1,
                },
                7,
            ),
            payload: EntryPayload::Normal(GroupCommand::ReleaseReplayLease {
                request: light_stream_core::LeaseRelease::new(
                    MutationRequestId::new(
                        light_stream_core::PrincipalId::parse("replay").unwrap(),
                        session,
                        light_stream_core::RequestSequence::new(2),
                    ),
                    partition,
                    lease.id(),
                ),
                clock: ClockObservation::new(12_000, 16_000).unwrap(),
            }),
        };
        log.append([release.clone()], IOFlushed::noop())
            .await
            .unwrap();
        state
            .apply(futures_util::stream::iter([Ok((release, None))]))
            .await
            .unwrap();
        assert_eq!(
            handles
                .reader
                .db
                .get::<LeaseBudget>(CF_STATE, KEY_LEASE_BUDGET)
                .unwrap(),
            Some(LeaseBudget::default())
        );
        assert_eq!(
            handles
                .reader
                .retention_status(partition)
                .unwrap()
                .reclaim_cursor(),
            RecordOffset::new(1)
        );
        let finish = GroupEntry {
            log_id: GroupLogId::new(
                GroupLeaderId {
                    term: 1,
                    node_id: 1,
                },
                8,
            ),
            payload: EntryPayload::Normal(GroupCommand::MaintainRetention {
                partition,
                expected_cursor: RecordOffset::new(1),
                max_records: 128,
                max_payload_bytes: 1024,
                clock: ClockObservation::new(13_000, 17_000).unwrap(),
            }),
        };
        log.append([finish.clone()], IOFlushed::noop())
            .await
            .unwrap();
        state
            .apply(futures_util::stream::iter([Ok((finish, None))]))
            .await
            .unwrap();
        let status = handles.reader.retention_status(partition).unwrap();
        assert_eq!(status.reclaim_cursor(), RecordOffset::new(4));
        assert_eq!(status.logically_expired_bytes(), ByteCount::new(15));
        assert_eq!(status.raft_only_bytes(), ByteCount::new(15));
        assert!(matches!(
            handles.reader.fetch_protected(
                cluster,
                partition,
                lease.id(),
                RecordOffset::new(1),
                10,
            ),
            Err(DomainError::ReplayLeaseInactive {
                lifecycle: light_stream_core::ReplayLeaseLifecycle::Released,
                ..
            })
        ));
        handles
            .reader
            .db
            .db
            .put_cf(
                &handles.reader.db.cf(CF_STATE).unwrap(),
                replay_lease_id_key(lease.id()),
                b"corrupt",
            )
            .unwrap();
        let corrupt_maintenance = GroupEntry {
            log_id: GroupLogId::new(
                GroupLeaderId {
                    term: 1,
                    node_id: 1,
                },
                9,
            ),
            payload: EntryPayload::Normal(GroupCommand::MaintainRetention {
                partition,
                expected_cursor: RecordOffset::new(4),
                max_records: 16,
                max_payload_bytes: 1024,
                clock: ClockObservation::new(14_000, 18_000).unwrap(),
            }),
        };
        log.append([corrupt_maintenance.clone()], IOFlushed::noop())
            .await
            .unwrap();
        assert!(
            state
                .apply(futures_util::stream::iter([Ok((
                    corrupt_maintenance,
                    None,
                ))]))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn legacy_record_schema_migrates_with_a_durable_byte_index() {
        #[derive(Serialize)]
        struct LegacyStoredRecord {
            payload_key: Vec<u8>,
        }

        let directory = ProjectTestDir::new("record-schema-migration");
        let (cluster, stream) = ids();
        let identity = GroupIdentity::new(
            cluster,
            GroupId::new(DATA_GROUP_ID).unwrap(),
            GroupKind::Data,
        );
        {
            let handles = create_data_store(
                &directory.0,
                identity.clone(),
                DEFAULT_RECEIPT_WINDOW,
                test_budget(),
            )
            .unwrap();
            let mut log = handles.log_store;
            let mut state_machine = handles.state_machine;
            let partition = PartitionKey::new(stream, PartitionId::new(0));
            let entries = vec![
                GroupEntry {
                    log_id: GroupLogId::new(
                        GroupLeaderId {
                            term: 1,
                            node_id: 1,
                        },
                        1,
                    ),
                    payload: EntryPayload::Normal(GroupCommand::BootstrapData {
                        spec: BootstrapSpec::new(
                            cluster,
                            stream,
                            StreamName::parse("bootstrap").unwrap(),
                        ),
                    }),
                },
                GroupEntry {
                    log_id: GroupLogId::new(
                        GroupLeaderId {
                            term: 1,
                            node_id: 1,
                        },
                        2,
                    ),
                    payload: EntryPayload::Normal(GroupCommand::Publish {
                        batch: PublishBatch::new(
                            cluster,
                            partition,
                            ProducerRequestId::new(
                                light_stream_core::PrincipalId::parse("test").unwrap(),
                                light_stream_core::ProducerSessionId::from_uuid(Uuid::new_v4()),
                                light_stream_core::RequestSequence::new(1),
                            ),
                            vec![b"legacy".to_vec()],
                        )
                        .unwrap(),
                    }),
                },
            ];
            log.append(entries.clone(), IOFlushed::noop())
                .await
                .unwrap();
            state_machine
                .apply(futures_util::stream::iter(
                    entries.into_iter().map(|entry| Ok((entry, None))),
                ))
                .await
                .unwrap();
            let record_key = record_key(partition, 0);
            let record = handles
                .reader
                .db
                .get::<StoredRecord>(CF_STATE, &record_key)
                .unwrap()
                .unwrap();
            let mut write = WriteBatch::default();
            write.put_cf(
                &handles.reader.db.cf(CF_STATE).unwrap(),
                record_key,
                encode(&LegacyStoredRecord {
                    payload_key: record.payload_key,
                })
                .unwrap(),
            );
            write.delete_cf(&handles.reader.db.cf(CF_META).unwrap(), KEY_SCHEMA_VERSION);
            write.delete_cf(
                &handles.reader.db.cf(CF_STATE).unwrap(),
                retention_key(partition),
            );
            handles.reader.db.write_sync(write).unwrap();
        }

        let handles = open_data_store(
            &directory.0,
            &identity,
            DEFAULT_RECEIPT_WINDOW,
            test_budget(),
        )
        .unwrap();
        let partition = PartitionKey::new(stream, PartitionId::new(0));
        let record = handles
            .reader
            .db
            .get::<StoredRecord>(CF_STATE, &record_key(partition, 0))
            .unwrap()
            .unwrap();
        assert_eq!(record.payload_bytes, 6);
        assert_eq!(record.cumulative_end_bytes, 6);
        assert_eq!(
            handles
                .reader
                .retention_status(partition)
                .unwrap()
                .logical_floor(),
            RecordOffset::new(0)
        );
        assert_eq!(
            handles
                .reader
                .db
                .get::<u32>(CF_META, KEY_SCHEMA_VERSION)
                .unwrap(),
            Some(CURRENT_SCHEMA_VERSION)
        );
    }

    #[tokio::test]
    async fn legacy_state_bank_migration_converts_payload_ownership_before_publication() {
        let directory = ProjectTestDir::new("state-bank-migration");
        let (cluster, stream) = ids();
        let identity = GroupIdentity::new(
            cluster,
            GroupId::new(DATA_GROUP_ID).unwrap(),
            GroupKind::Data,
        );
        let partition = PartitionKey::new(stream, PartitionId::new(0));
        {
            let handles = create_data_store(
                &directory.0,
                identity.clone(),
                DEFAULT_RECEIPT_WINDOW,
                test_budget(),
            )
            .unwrap();
            let mut log = handles.log_store;
            let mut state = handles.state_machine;
            let entries = [
                GroupEntry {
                    log_id: GroupLogId::new(
                        GroupLeaderId {
                            term: 1,
                            node_id: 1,
                        },
                        1,
                    ),
                    payload: EntryPayload::Normal(GroupCommand::BootstrapData {
                        spec: BootstrapSpec::new(
                            cluster,
                            stream,
                            StreamName::parse("bootstrap").unwrap(),
                        ),
                    }),
                },
                GroupEntry {
                    log_id: GroupLogId::new(
                        GroupLeaderId {
                            term: 1,
                            node_id: 1,
                        },
                        2,
                    ),
                    payload: EntryPayload::Normal(GroupCommand::Publish {
                        batch: PublishBatch::new(
                            cluster,
                            partition,
                            ProducerRequestId::new(
                                light_stream_core::PrincipalId::parse("migration").unwrap(),
                                light_stream_core::ProducerSessionId::from_uuid(Uuid::new_v4()),
                                light_stream_core::RequestSequence::new(1),
                            ),
                            vec![b"banked".to_vec()],
                        )
                        .unwrap(),
                    }),
                },
            ];
            log.append(entries.clone(), IOFlushed::noop())
                .await
                .unwrap();
            state
                .apply(futures_util::stream::iter(
                    entries.into_iter().map(|entry| Ok((entry, None))),
                ))
                .await
                .unwrap();
            let db = &handles.reader.db;
            let active = db.raw_cf(CF_STATE_A).unwrap();
            let legacy = db.raw_cf(CF_STATE).unwrap();
            let mut write = WriteBatch::default();
            for item in db
                .db
                .iterator_cf(&active, IteratorMode::From(b"", Direction::Forward))
            {
                let (key, value) = item.unwrap();
                write.put_cf(&legacy, &key, &value);
                write.delete_cf(&active, key);
            }
            let record = db
                .get::<StoredRecord>(CF_STATE_A, &record_key(partition, 0))
                .unwrap()
                .unwrap();
            let owners_key = payload_owners_key(&record.payload_key);
            let mut owners = db
                .get::<PayloadOwners>(CF_PAYLOAD, &owners_key)
                .unwrap()
                .unwrap();
            owners.applied_state = true;
            owners.applied_banks = 0;
            write.put_cf(
                &db.raw_cf(CF_PAYLOAD).unwrap(),
                owners_key,
                encode(&owners).unwrap(),
            );
            write.delete_cf(&db.raw_cf(CF_META).unwrap(), KEY_ACTIVE_STATE_BANK);
            db.write_sync(write).unwrap();
        }

        let handles = open_data_store(
            &directory.0,
            &identity,
            DEFAULT_RECEIPT_WINDOW,
            test_budget(),
        )
        .unwrap();
        let page = handles
            .reader
            .fetch(cluster, partition, RecordOffset::new(0), 1)
            .unwrap();
        assert_eq!(page.records()[0].payload(), b"banked");
        let record = handles
            .reader
            .db
            .get::<StoredRecord>(CF_STATE, &record_key(partition, 0))
            .unwrap()
            .unwrap();
        let owners = handles
            .reader
            .db
            .get::<PayloadOwners>(CF_PAYLOAD, &payload_owners_key(&record.payload_key))
            .unwrap()
            .unwrap();
        assert!(owners.applied_in(StateBank::A));
        assert!(!owners.applied_state);
    }

    #[tokio::test]
    async fn snapshot_contains_and_installs_payloads() {
        let source = ProjectTestDir::new("snapshot-source");
        let target = ProjectTestDir::new("snapshot-target");
        let (cluster, stream) = ids();
        let identity = GroupIdentity::new(
            cluster,
            GroupId::new(DATA_GROUP_ID).unwrap(),
            GroupKind::Data,
        );
        let source_handles = create_data_store(
            &source.0,
            identity.clone(),
            DEFAULT_RECEIPT_WINDOW,
            test_budget(),
        )
        .unwrap();
        let mut source_log = source_handles.log_store;
        let mut source_state = source_handles.state_machine;
        let partition = PartitionKey::new(stream, light_stream_core::PartitionId::new(0));
        let spec = BootstrapSpec::new(
            cluster,
            stream,
            light_stream_core::StreamName::parse("bootstrap").unwrap(),
        );
        let entries = [
            GroupEntry {
                log_id: GroupLogId::new(
                    GroupLeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    1,
                ),
                payload: EntryPayload::Normal(GroupCommand::BootstrapData { spec }),
            },
            GroupEntry {
                log_id: GroupLogId::new(
                    GroupLeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    2,
                ),
                payload: EntryPayload::Normal(GroupCommand::Publish {
                    batch: PublishBatch::new(
                        cluster,
                        partition,
                        ProducerRequestId::new(
                            light_stream_core::PrincipalId::parse("test").unwrap(),
                            light_stream_core::ProducerSessionId::from_uuid(Uuid::new_v4()),
                            light_stream_core::RequestSequence::new(1),
                        ),
                        vec![b"snapshot-payload".to_vec()],
                    )
                    .unwrap(),
                }),
            },
        ];
        source_log
            .append(entries.clone(), IOFlushed::noop())
            .await
            .unwrap();
        source_state
            .apply(futures_util::stream::iter(
                entries.into_iter().map(|entry| Ok((entry, None))),
            ))
            .await
            .unwrap();
        let mut builder = source_state.get_snapshot_builder().await;
        let snapshot = builder.build_snapshot().await.unwrap();
        let artifact_bytes = snapshot
            .snapshot
            .read_all_limited(MAX_SNAPSHOT_BYTES)
            .unwrap();
        assert!(artifact_bytes.starts_with(b"LSNP0003"));
        let mut corrupted = artifact_bytes;
        corrupted[b"LSNP0003".len() + 4] ^= 1;
        assert!(decode_snapshot_bundle(&corrupted).is_err());
        assert!(
            !source_handles
                .reader
                .db
                .current_snapshot_artifact()
                .unwrap()
                .unwrap()
                .1
                .is_empty()
        );
        let stored = source_handles
            .reader
            .db
            .get::<StoredRecord>(CF_STATE, &record_key(partition, 0))
            .unwrap()
            .unwrap();
        let retention_entries = [
            GroupEntry {
                log_id: GroupLogId::new(
                    GroupLeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    3,
                ),
                payload: EntryPayload::Normal(GroupCommand::AdvanceRetention {
                    request: RetentionRequest::new(
                        MutationRequestId::new(
                            light_stream_core::PrincipalId::parse("operator").unwrap(),
                            light_stream_core::MutationSessionId::from_uuid(Uuid::new_v4()),
                            light_stream_core::RequestSequence::new(1),
                        ),
                        partition,
                        RecordOffset::new(1),
                    ),
                    clock: ClockObservation::new(8_000, 12_000).unwrap(),
                }),
            },
            GroupEntry {
                log_id: GroupLogId::new(
                    GroupLeaderId {
                        term: 1,
                        node_id: 1,
                    },
                    4,
                ),
                payload: EntryPayload::Normal(GroupCommand::MaintainRetention {
                    partition,
                    expected_cursor: RecordOffset::new(0),
                    max_records: 16,
                    max_payload_bytes: 1024,
                    clock: ClockObservation::new(9_000, 13_000).unwrap(),
                }),
            },
        ];
        source_log
            .append(retention_entries.clone(), IOFlushed::noop())
            .await
            .unwrap();
        source_state
            .apply(futures_util::stream::iter(
                retention_entries.into_iter().map(|entry| Ok((entry, None))),
            ))
            .await
            .unwrap();
        source_log
            .purge(GroupLogId::new(
                GroupLeaderId {
                    term: 1,
                    node_id: 1,
                },
                4,
            ))
            .await
            .unwrap();
        let mut replacement_builder = source_state.get_snapshot_builder().await;
        replacement_builder.build_snapshot().await.unwrap();
        assert!(
            source_handles
                .reader
                .db
                .get::<PayloadOwners>(CF_PAYLOAD, &payload_owners_key(&stored.payload_key),)
                .unwrap()
                .is_none()
        );

        let target_handles =
            create_data_store(&target.0, identity, DEFAULT_RECEIPT_WINDOW, test_budget()).unwrap();
        let mut target_state = target_handles.state_machine;
        target_state
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .unwrap();
        let page = target_handles
            .reader
            .fetch(cluster, partition, RecordOffset::new(0), 10)
            .unwrap();
        assert_eq!(page.records()[0].payload(), b"snapshot-payload");
    }

    #[test]
    fn corrupted_identity_is_refused() {
        let directory = ProjectTestDir::new("corrupt-identity");
        let identity = GroupIdentity::new(
            ClusterId::from_uuid(Uuid::new_v4()),
            GroupId::new(DATA_GROUP_ID).unwrap(),
            GroupKind::Data,
        );
        let handles = create_data_store(
            &directory.0,
            identity.clone(),
            DEFAULT_RECEIPT_WINDOW,
            test_budget(),
        )
        .unwrap();
        let meta = handles.reader.db.cf(CF_META).unwrap();
        handles
            .reader
            .db
            .db
            .put_cf(&meta, KEY_IDENTITY, b"corrupt")
            .unwrap();
        drop(meta);
        drop(handles);
        let error = open_data_store(
            &directory.0,
            &identity,
            DEFAULT_RECEIPT_WINDOW,
            test_budget(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("storage error"));
    }
}
