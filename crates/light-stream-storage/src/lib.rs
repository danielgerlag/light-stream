use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fmt::{self, Debug},
    fs, io,
    marker::PhantomData,
    ops::{Bound, RangeBounds},
    path::Path,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use crc32fast::Hasher as Crc32;
use futures_util::{Stream, StreamExt};
use light_stream_core::{
    BookmarkId, BookmarkName, BookmarkPage, BookmarkPageRequest, BookmarkPublicationSequence,
    BootstrapResult, BootstrapSpec, CatalogRequestId, ClusterId, CommittedBookmark,
    CommittedCursor, CommittedRecord, CommittedRecordRange, CommittedStreamBookmark,
    CreateStreamSpec, DomainError, FetchPage, GroupId, PartitionId, PartitionKey,
    PartitionPlacement, PartitionRoute, ProducerRequestId, PublishBatch, PublishReceipt,
    RecordOffset, StreamBookmarkPage, StreamBookmarkPageRequest, StreamCursorVector,
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
const CF_SNAPSHOT: &str = "ls_v1_snapshot";
const COLUMN_FAMILIES: [&str; 6] = [
    CF_META,
    CF_RAFT_META,
    CF_RAFT_LOG,
    CF_PAYLOAD,
    CF_STATE,
    CF_SNAPSHOT,
];

const KEY_IDENTITY: &[u8] = b"identity";
const KEY_VOTE: &[u8] = b"vote";
const KEY_COMMITTED: &[u8] = b"committed";
const KEY_PURGED: &[u8] = b"purged";
const KEY_APPLIED: &[u8] = b"applied";
const KEY_MEMBERSHIP: &[u8] = b"membership";
const KEY_BOOTSTRAP: &[u8] = b"bootstrap";
const KEY_CURRENT_SNAPSHOT: &[u8] = b"current";
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
const BOOKMARK_ID_PREFIX: &[u8] = b"bookmark/id/";
const BOOKMARK_NAME_PREFIX: &[u8] = b"bookmark/name/";
const BOOKMARK_ORDER_PREFIX: &[u8] = b"bookmark/order/";
const BOOKMARK_PUBLICATION_PREFIX: &[u8] = b"bookmark/publication/";
const STREAM_BOOKMARK_ID_PREFIX: &[u8] = b"stream-bookmark/id/";
const STREAM_BOOKMARK_NAME_PREFIX: &[u8] = b"stream-bookmark/name/";
const STREAM_BOOKMARK_ORDER_PREFIX: &[u8] = b"stream-bookmark/order/";
const STREAM_BOOKMARK_PUBLICATION_PREFIX: &[u8] = b"stream-bookmark/publication/";

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

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
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
            Self::CreateBookmark { .. } => formatter.write_str("create-bookmark"),
            Self::DeleteBookmark { .. } => formatter.write_str("delete-bookmark"),
            Self::CreateStreamBookmark { .. } => formatter.write_str("create-stream-bookmark"),
            Self::DeleteStreamBookmark { .. } => formatter.write_str("delete-stream-bookmark"),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ApplyResult {
    Bootstrapped(BootstrapResult),
    Stream(StreamDescriptor),
    Published(PublishReceipt),
    Bookmark(CommittedBookmark),
    StreamBookmark(CommittedStreamBookmark),
    Rejected(DomainError),
    Noop,
}

impl fmt::Display for ApplyResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bootstrapped(_) => formatter.write_str("bootstrapped"),
            Self::Stream(value) => write!(formatter, "stream {}", value.stream()),
            Self::Published(_) => formatter.write_str("published"),
            Self::Bookmark(value) => write!(formatter, "bookmark {}", value.id()),
            Self::StreamBookmark(value) => write!(formatter, "stream bookmark {}", value.id()),
            Self::Rejected(error) => write!(formatter, "rejected: {error}"),
            Self::Noop => formatter.write_str("noop"),
        }
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
    identity: GroupIdentity,
    receipt_window: usize,
    write_lane: Arc<OrderedWriteLane>,
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
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
struct PayloadOwners {
    raft_log: bool,
    applied_state: bool,
    snapshot_artifact: bool,
}

impl PayloadOwners {
    fn reachable(&self) -> bool {
        self.raft_log || self.applied_state || self.snapshot_artifact
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct StoredRecord {
    payload_key: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct StoredReceipt {
    fingerprint: String,
    receipt: PublishReceipt,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
struct ProducerSessionState {
    highest_sequence: Option<u64>,
    retained_sequences: VecDeque<u64>,
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
        identity: identity.clone(),
        receipt_window,
        write_lane: Arc::new(OrderedWriteLane::default()),
    };
    group_db
        .put_sync(CF_META, KEY_IDENTITY, &identity)
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
    let options = db_options(false, budget);
    let actual_cfs = DB::list_cf(&options, &db_path).map_err(storage_open)?;
    let expected_cfs: BTreeSet<_> = std::iter::once("default")
        .chain(COLUMN_FAMILIES)
        .map(str::to_owned)
        .collect();
    let actual_cfs: BTreeSet<_> = actual_cfs.into_iter().collect();
    if actual_cfs != expected_cfs {
        return Err(StorageOpenError::Storage(format!(
            "column family set mismatch: expected {expected_cfs:?}, found {actual_cfs:?}"
        )));
    }
    let db = DB::open_cf_descriptors(&options, &db_path, descriptors()).map_err(storage_open)?;
    let group_db = GroupDb {
        db: Arc::new(db),
        identity: expected.clone(),
        receipt_window,
        write_lane: Arc::new(OrderedWriteLane::default()),
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
    fn cf(&self, name: &str) -> io::Result<Arc<rocksdb::BoundColumnFamily<'_>>> {
        self.db
            .cf_handle(name)
            .ok_or_else(|| io_error(format!("missing column family {name}")))
    }

    fn get<T: DeserializeOwned>(&self, cf: &str, key: &[u8]) -> io::Result<Option<T>> {
        let handle = self.cf(cf)?;
        self.db
            .get_cf(&handle, key)
            .map_err(io_error)?
            .map(|bytes| decode(&bytes))
            .transpose()
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
        let handle = self.cf(cf)?;
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
}

fn fingerprint(batch: &PublishBatch) -> io::Result<String> {
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

fn stream_key(stream: StreamId) -> Vec<u8> {
    [STREAM_PREFIX, stream.as_uuid().as_bytes()].concat()
}

fn stream_name_key(name: &StreamName) -> Vec<u8> {
    [STREAM_NAME_PREFIX, name.as_str().as_bytes()].concat()
}

fn create_intent_key(request: CatalogRequestId) -> Vec<u8> {
    [CREATE_INTENT_PREFIX, request.as_uuid().as_bytes()].concat()
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
    let mut digest = Sha256::new();
    digest.update(log_id.leader_id.term.to_be_bytes());
    digest.update(log_id.leader_id.node_id.to_be_bytes());
    digest.update(log_id.index.to_be_bytes());
    digest.update(b"bookmark");
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
                data_groups,
                max_streams,
                max_partitions_per_stream,
            } => Ok((
                ThinEntry {
                    log_id,
                    payload: ThinPayload::Normal(ThinCommand::BootstrapControl {
                        spec,
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
                let digest = fingerprint(&batch)?;
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
                                    data_groups,
                                    max_streams,
                                    max_partitions_per_stream,
                                } => GroupCommand::BootstrapControl {
                                    spec,
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
                                        records.push(decode::<Vec<u8>>(&bytes)?);
                                    }
                                    let mut batch =
                                        PublishBatch::new(cluster, partition, request, records)
                                            .map_err(io_error)?;
                                    if let Some(bookmark) = bookmark {
                                        batch = batch.with_bookmark(bookmark);
                                    }
                                    GroupCommand::Publish { batch }
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
                            write.put_cf(&payload_cf, payload_bytes_key(&key), encode(&bytes)?);
                            write.put_cf(
                                &payload_cf,
                                payload_owners_key(&key),
                                encode(&PayloadOwners {
                                    raft_log: true,
                                    applied_state: false,
                                    snapshot_artifact: false,
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
            if let ThinPayload::Normal(ThinCommand::Publish { payload_keys, .. }) = thin.payload {
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
            type SnapshotData = Vec<u8>;
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
                let Some(bytes) = self.db.get::<Vec<u8>>(CF_SNAPSHOT, KEY_CURRENT_SNAPSHOT)? else {
                    return Ok(None);
                };
                let bundle: SnapshotBundle = decode(&bytes)?;
                Ok(Some(Snapshot {
                    meta: bundle.meta,
                    snapshot: bytes,
                }))
            }
        }

        impl RaftSnapshotBuilder<$config> for GroupSnapshotBuilder<$config> {
            type SnapshotData = Vec<u8>;

            async fn build_snapshot(
                &mut self,
            ) -> Result<SnapshotOf<$config, Self::SnapshotData>, io::Error> {
                let bytes = self.db.build_snapshot()?;
                let bundle: SnapshotBundle = decode(&bytes)?;
                Ok(Snapshot {
                    meta: bundle.meta,
                    snapshot: bytes,
                })
            }
        }
    };
}

impl_state_machine!(ControlRaftConfig);
impl_state_machine!(DataRaftConfig);

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
                data_groups,
                max_streams,
                max_partitions_per_stream,
            } => self.apply_bootstrap_control(
                spec,
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
        }
    }

    fn apply_bootstrap_control(
        &self,
        spec: BootstrapSpec,
        data_groups: Vec<GroupId>,
        max_streams: u32,
        max_partitions: u32,
        write: &mut WriteBatch,
    ) -> io::Result<ApplyResult> {
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
            if existing != data_groups
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

        let fingerprint = fingerprint(&batch)?;
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
        for (slot, _record) in batch.records().iter().enumerate() {
            let payload_key = payload_id(log_id, slot);
            let owners_key = payload_owners_key(&payload_key);
            let mut owners = self
                .get::<PayloadOwners>(CF_PAYLOAD, &owners_key)?
                .ok_or_else(|| io_error("applied entry payload is missing ownership"))?;
            owners.applied_state = true;
            write.put_cf(&payload_cf, owners_key, encode(&owners)?);
            let offset = first + u64::try_from(slot).map_err(io_error)?;
            write.put_cf(
                &state_cf,
                record_key(batch.partition(), offset),
                encode(&StoredRecord {
                    payload_key: payload_key.clone(),
                })?,
            );
        }
        write.put_cf(
            &state_cf,
            next_offset_key(batch.partition()),
            encode(&(first + count))?,
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

    fn build_snapshot(&self) -> io::Result<Vec<u8>> {
        let _guard = self.write_lane.enter()?;
        let applied = self.get::<GroupLogId>(CF_STATE, KEY_APPLIED)?;
        let membership = self
            .get::<GroupMembership>(CF_STATE, KEY_MEMBERSHIP)?
            .unwrap_or_default();
        let meta = GroupSnapshotMeta {
            last_log_id: applied,
            last_membership: membership,
        };
        let state = self.scan_prefix(CF_STATE, b"")?;
        let mut payloads = Vec::new();
        let mut write = WriteBatch::default();
        let payload_cf = self.cf(CF_PAYLOAD)?;
        for (key, value) in self.scan_prefix(CF_PAYLOAD, PAYLOAD_OWNERS_PREFIX)? {
            let mut owners: PayloadOwners = decode(&value)?;
            if owners.applied_state {
                owners.snapshot_artifact = true;
                write.put_cf(&payload_cf, &key, encode(&owners)?);
                let id = &key[PAYLOAD_OWNERS_PREFIX.len()..];
                let bytes_key = payload_bytes_key(id);
                let bytes = self
                    .db
                    .get_cf(&payload_cf, &bytes_key)
                    .map_err(io_error)?
                    .ok_or_else(|| io_error("snapshot payload bytes are missing"))?;
                payloads.push((bytes_key, bytes.to_vec()));
            }
        }
        let bundle = SnapshotBundle {
            format_version: STORAGE_FORMAT_VERSION,
            identity: self.identity.clone(),
            meta,
            state,
            payloads,
        };
        let bytes = encode(&bundle)?;
        if bytes.len() > MAX_SNAPSHOT_BYTES {
            return Err(io_error("snapshot exceeds the LS02a byte limit"));
        }
        write.put_cf(
            &self.cf(CF_SNAPSHOT)?,
            KEY_CURRENT_SNAPSHOT,
            encode(&bytes)?,
        );
        self.write_sync(write)?;
        Ok(bytes)
    }

    fn install_snapshot(&self, meta: &GroupSnapshotMeta, bytes: &[u8]) -> io::Result<()> {
        if bytes.len() > MAX_SNAPSHOT_BYTES {
            return Err(io_error("snapshot exceeds the LS02a byte limit"));
        }
        let bundle: SnapshotBundle = decode(bytes)?;
        if bundle.format_version != STORAGE_FORMAT_VERSION || bundle.identity != self.identity {
            return Err(io_error("snapshot identity or format mismatch"));
        }
        if &bundle.meta != meta {
            return Err(io_error("snapshot metadata mismatch"));
        }
        let _guard = self.write_lane.enter()?;
        let state_cf = self.cf(CF_STATE)?;
        let payload_cf = self.cf(CF_PAYLOAD)?;
        let mut write = WriteBatch::default();
        for (key, _) in self.scan_prefix(CF_STATE, b"")? {
            write.delete_cf(&state_cf, key);
        }
        for (key, value) in &bundle.state {
            write.put_cf(&state_cf, key, value);
        }
        let snapshot_payloads: BTreeMap<Vec<u8>, Vec<u8>> =
            bundle.payloads.iter().cloned().collect();
        for (key, value) in self.scan_prefix(CF_PAYLOAD, PAYLOAD_OWNERS_PREFIX)? {
            let id = key[PAYLOAD_OWNERS_PREFIX.len()..].to_vec();
            let mut owners: PayloadOwners = decode(&value)?;
            owners.applied_state = false;
            owners.snapshot_artifact = false;
            if snapshot_payloads.contains_key(&payload_bytes_key(&id)) {
                owners.applied_state = true;
                owners.snapshot_artifact = true;
            }
            if owners.reachable() {
                write.put_cf(&payload_cf, key, encode(&owners)?);
            } else {
                write.delete_cf(&payload_cf, payload_bytes_key(&id));
                write.delete_cf(&payload_cf, key);
            }
        }
        for (bytes_key, value) in snapshot_payloads {
            let id = bytes_key[PAYLOAD_BYTES_PREFIX.len()..].to_vec();
            write.put_cf(&payload_cf, &bytes_key, value);
            let owners_key = payload_owners_key(&id);
            let mut owners = self
                .get::<PayloadOwners>(CF_PAYLOAD, &owners_key)?
                .unwrap_or_default();
            owners.applied_state = true;
            owners.snapshot_artifact = true;
            write.put_cf(&payload_cf, owners_key, encode(&owners)?);
        }
        write.put_cf(
            &self.cf(CF_SNAPSHOT)?,
            KEY_CURRENT_SNAPSHOT,
            encode(&bytes.to_vec())?,
        );
        self.write_sync(write)
    }
}

impl CommittedStateReader {
    pub fn bootstrap_spec(&self) -> Result<Option<BootstrapSpec>, DomainError> {
        self.db
            .get(CF_STATE, KEY_BOOTSTRAP)
            .map_err(|error| DomainError::Storage {
                reason: error.to_string(),
            })
    }

    pub fn receipt(
        &self,
        partition: PartitionKey,
        request: &ProducerRequestId,
    ) -> Result<PublishReceipt, DomainError> {
        self.db
            .get::<StoredReceipt>(
                CF_STATE,
                &receipt_key(partition, request).map_err(|error| DomainError::Storage {
                    reason: error.to_string(),
                })?,
            )
            .map_err(|error| DomainError::Storage {
                reason: error.to_string(),
            })?
            .map(|stored| stored.receipt)
            .ok_or(DomainError::ReceiptNotFound)
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
        let state_cf = self.db.cf(CF_STATE).map_err(storage_domain)?;
        let payload_cf = self.db.cf(CF_PAYLOAD).map_err(storage_domain)?;
        let snapshot = self.db.db.snapshot();
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
            let payload: Vec<u8> = decode(&payload).map_err(storage_domain)?;
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
    type SnapshotData = Vec<u8>;

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
