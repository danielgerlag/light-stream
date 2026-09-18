use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
    mem,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use light_stream_core::{
    ActiveExport, ActiveExportPhase, BookmarkLifecycle, BookmarkPublicationSequence, ClusterId,
    CommittedBookmark, CommittedRecord, CommittedStreamBookmark, ExportFenceToken, GroupCut,
    GroupId, PartitionKey, RecordOffset, StreamCursorVector, StreamDescriptor, StreamId,
    StreamLifecycle,
};
use light_stream_export::{
    ActiveStreamV1, ControlSectionV1, DataGroupSourceV1, DataGroupV1, ExportDocumentV1,
    ExportExclusionsV1, ExportIdV1, ExportLimits, PartitionV1, REQUIRED_FEATURES_V1,
};
use rocksdb::{Direction, IteratorMode, Snapshot};
use serde::de::DeserializeOwned;
use thiserror::Error;

use crate::{
    BOOKMARK_ID_PREFIX, CF_META, CF_PAYLOAD, CommittedStateReader, GroupIdentity, GroupKind,
    GroupLogId, KEY_ACTIVE_EXPORT, KEY_APPLIED, KEY_ASSIGNMENT_CURSOR, KEY_CATALOG_REVISION,
    KEY_DATA_GROUP_POOL, KEY_IDENTITY, KEY_MAX_PARTITIONS, KEY_MAX_STREAMS, KEY_MUTATION_FENCE,
    PAYLOAD_MAGIC, PartitionRetentionState, STORAGE_FORMAT_VERSION, STREAM_BOOKMARK_ID_PREFIX,
    StoredRecord, decode, payload_bytes_key, record_key, retention_key,
    stream_bookmark_publication_key, stream_key,
};

#[derive(Debug, Error)]
pub enum LogicalExportError {
    #[error("logical export was cancelled")]
    Cancelled,
    #[error("logical export requires a control-group reader, got {group}")]
    WrongControlGroup { group: GroupId },
    #[error("logical export requires a data-group reader, got {group}")]
    WrongDataGroup { group: GroupId },
    #[error("no rebuildable logical export is active")]
    NoRebuildableExport,
    #[error("logical export data group {group} is not planned")]
    UnplannedGroup { group: GroupId },
    #[error("logical export data group {group} was supplied or read more than once")]
    DuplicateGroup { group: GroupId },
    #[error("logical export has no reader for data group {group}")]
    MissingGroup { group: GroupId },
    #[error("logical export token mismatch for data group {group}")]
    WrongToken {
        group: GroupId,
        expected: ExportFenceToken,
        actual: ExportFenceToken,
    },
    #[error("logical export cut mismatch for data group {group}")]
    WrongCut {
        group: GroupId,
        expected: GroupCut,
        actual: GroupCut,
    },
    #[error("logical export group mismatch: expected {expected}, got {actual}")]
    WrongGroup { expected: GroupId, actual: GroupId },
    #[error("logical export cluster mismatch for data group {group}")]
    WrongCluster {
        group: GroupId,
        expected: ClusterId,
        actual: ClusterId,
    },
    #[error("logical export fence is unavailable for data group {group}")]
    Fence { group: GroupId },
    #[error("logical export storage is corrupt in data group {group}: {reason}")]
    Corruption { group: GroupId, reason: String },
    #[error("logical export record is missing at {partition:?} offset {offset:?}")]
    MissingRecord {
        partition: PartitionKey,
        offset: RecordOffset,
    },
    #[error("logical export payload is missing at {partition:?} offset {offset:?}")]
    MissingPayload {
        partition: PartitionKey,
        offset: RecordOffset,
    },
    #[error("logical export limit exceeded: {limit}")]
    Limit { limit: &'static str },
    #[error("logical export storage read failed for group {group}: {reason}")]
    Storage { group: GroupId, reason: String },
}

#[derive(Clone, Default)]
pub struct LogicalExportCancellation {
    cancelled: Arc<AtomicBool>,
}

impl LogicalExportCancellation {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    fn check(&self) -> Result<(), LogicalExportError> {
        if self.is_cancelled() {
            Err(LogicalExportError::Cancelled)
        } else {
            Ok(())
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControlPlanV1 {
    control: ControlSectionV1,
    partitions_by_group: BTreeMap<GroupId, Vec<PartitionKey>>,
}

impl ControlPlanV1 {
    pub const fn control(&self) -> &ControlSectionV1 {
        &self.control
    }

    pub const fn partitions_by_group(&self) -> &BTreeMap<GroupId, Vec<PartitionKey>> {
        &self.partitions_by_group
    }
}

pub struct PreparedLogicalExportV1 {
    document: ExportDocumentV1,
    source: LogicalExportSourceV1,
    limits: ExportLimits,
}

impl PreparedLogicalExportV1 {
    pub fn plan(&self) -> ControlPlanV1 {
        ControlPlanV1 {
            control: self.document.control.clone(),
            partitions_by_group: self
                .source
                .groups
                .iter()
                .map(|(group, planned)| (*group, planned.partitions.clone()))
                .collect(),
        }
    }

    pub fn document(&self) -> ExportDocumentV1 {
        self.document.clone()
    }

    pub const fn source_mut(&mut self) -> &mut LogicalExportSourceV1 {
        &mut self.source
    }

    pub const fn limits(&self) -> ExportLimits {
        self.limits
    }
}

pub struct LogicalExportSourceV1 {
    groups: BTreeMap<GroupId, PlannedGroupV1>,
    selected_streams: BTreeSet<StreamId>,
    source_cluster: ClusterId,
    export_id: ExportIdV1,
    token: ExportFenceToken,
    limits: ExportLimits,
    budget: ExportBudget,
    cancellation: LogicalExportCancellation,
}

struct PlannedGroupV1 {
    cut: GroupCut,
    partitions: Vec<PartitionKey>,
    delivery: GroupDeliveryV1,
}

enum GroupDeliveryV1 {
    Pending(CommittedStateReader),
    Consumed,
}

#[derive(Clone, Copy, Default)]
struct ExportBudget {
    streams: u64,
    partitions: u64,
    records: u64,
    bookmarks: u64,
    payload_bytes: u64,
}

struct ControlSnapshotV1<'a> {
    snapshot: Snapshot<'a>,
    state: Arc<rocksdb::BoundColumnFamily<'a>>,
    meta: Arc<rocksdb::BoundColumnFamily<'a>>,
}

struct DataSnapshotV1<'a> {
    snapshot: Snapshot<'a>,
    state: Arc<rocksdb::BoundColumnFamily<'a>>,
    meta: Arc<rocksdb::BoundColumnFamily<'a>>,
    payload: Arc<rocksdb::BoundColumnFamily<'a>>,
}

struct MaterializedPartitionV1 {
    retention_floor: RecordOffset,
    tail: RecordOffset,
    bookmark_publication_ceiling: BookmarkPublicationSequence,
    records: Vec<CommittedRecord>,
    bookmarks: Vec<CommittedBookmark>,
}

struct SectionBudget {
    used: u64,
    max: u64,
}

impl SectionBudget {
    fn new(base: u64, max: u64) -> Result<Self, LogicalExportError> {
        if base > max {
            return Err(LogicalExportError::Limit {
                limit: "section_bytes",
            });
        }
        Ok(Self { used: base, max })
    }

    fn charge(&mut self, amount: u64) -> Result<(), LogicalExportError> {
        self.used = self
            .used
            .checked_add(amount)
            .filter(|value| *value <= self.max)
            .ok_or(LogicalExportError::Limit {
                limit: "section_bytes",
            })?;
        Ok(())
    }

    fn charge_items(&mut self, count: usize, bytes: u64) -> Result<(), LogicalExportError> {
        self.charge(
            u64::try_from(count)
                .ok()
                .and_then(|count| count.checked_mul(bytes))
                .ok_or(LogicalExportError::Limit {
                    limit: "section_bytes",
                })?,
        )
    }
}

impl ExportBudget {
    fn charge_streams(
        &mut self,
        amount: u64,
        limits: &ExportLimits,
    ) -> Result<(), LogicalExportError> {
        charge(&mut self.streams, amount, limits.max_streams, "streams")
    }

    fn charge_partitions(
        &mut self,
        amount: u64,
        limits: &ExportLimits,
    ) -> Result<(), LogicalExportError> {
        charge(
            &mut self.partitions,
            amount,
            limits.max_partitions,
            "partitions",
        )
    }

    fn charge_records(
        &mut self,
        amount: u64,
        limits: &ExportLimits,
    ) -> Result<(), LogicalExportError> {
        charge(&mut self.records, amount, limits.max_records, "records")
    }

    fn charge_bookmarks(
        &mut self,
        amount: u64,
        limits: &ExportLimits,
    ) -> Result<(), LogicalExportError> {
        charge(
            &mut self.bookmarks,
            amount,
            limits.max_bookmarks,
            "bookmarks",
        )
    }

    fn charge_payload_bytes(
        &mut self,
        amount: u64,
        limits: &ExportLimits,
    ) -> Result<(), LogicalExportError> {
        charge(
            &mut self.payload_bytes,
            amount,
            limits.max_payload_bytes,
            "payload_bytes",
        )
    }

    fn ensure_payload_allocation(
        &self,
        amount: u64,
        limits: &ExportLimits,
    ) -> Result<(), LogicalExportError> {
        let next = self
            .payload_bytes
            .checked_add(amount)
            .ok_or(LogicalExportError::Limit {
                limit: "payload_bytes",
            })?;
        if next > limits.max_payload_bytes {
            return Err(LogicalExportError::Limit {
                limit: "payload_bytes",
            });
        }
        Ok(())
    }
}

impl LogicalExportSourceV1 {
    fn data_group_with_hook(
        &mut self,
        group: GroupId,
        cut: GroupCut,
        after_snapshot: impl FnOnce(),
    ) -> Result<Option<DataGroupV1>, LogicalExportError> {
        self.cancellation.check()?;
        let planned = self
            .groups
            .get_mut(&group)
            .ok_or(LogicalExportError::UnplannedGroup { group })?;
        if planned.cut != cut {
            return Err(LogicalExportError::WrongCut {
                group,
                expected: planned.cut,
                actual: cut,
            });
        }
        let reader = match mem::replace(&mut planned.delivery, GroupDeliveryV1::Consumed) {
            GroupDeliveryV1::Pending(reader) => reader,
            GroupDeliveryV1::Consumed => {
                return Err(LogicalExportError::DuplicateGroup { group });
            }
        };
        let budget_checkpoint = self.budget;
        let result = (|| {
            let planned = self
                .groups
                .get(&group)
                .expect("planned group remains available");
            let partition_count =
                u64::try_from(planned.partitions.len()).map_err(|_| LogicalExportError::Limit {
                    limit: "partitions",
                })?;
            self.budget
                .charge_partitions(partition_count, &self.limits)?;
            let partitions = planned.partitions.clone();

            let state_bank =
                reader
                    .db
                    .state_bank
                    .read()
                    .map_err(|_| LogicalExportError::Storage {
                        group,
                        reason: "state bank lock poisoned".to_owned(),
                    })?;
            let state = reader
                .db
                .raw_cf(state_bank.column_family())
                .map_err(|error| storage_error(group, error))?;
            let meta = reader
                .db
                .raw_cf(CF_META)
                .map_err(|error| storage_error(group, error))?;
            let payload = reader
                .db
                .raw_cf(CF_PAYLOAD)
                .map_err(|error| storage_error(group, error))?;
            let snapshot = DataSnapshotV1 {
                snapshot: reader.db.db.snapshot(),
                state,
                meta,
                payload,
            };
            drop(state_bank);
            after_snapshot();
            self.cancellation.check()?;

            self.materialize_data_group(&snapshot, group, cut, &partitions)
                .map(Some)
        })();
        match result {
            Ok(data) => Ok(data),
            Err(error) => {
                self.budget = budget_checkpoint;
                Err(error)
            }
        }
    }

    fn materialize_data_group(
        &mut self,
        snapshot: &DataSnapshotV1<'_>,
        group: GroupId,
        cut: GroupCut,
        planned_partitions: &[PartitionKey],
    ) -> Result<DataGroupV1, LogicalExportError> {
        self.cancellation.check()?;
        let identity: GroupIdentity =
            required_value(&snapshot.snapshot, &snapshot.meta, KEY_IDENTITY, group)?;
        if identity.format_version != STORAGE_FORMAT_VERSION {
            return Err(corruption(group, "group storage format is unsupported"));
        }
        if identity.kind != GroupKind::Data {
            return Err(LogicalExportError::WrongDataGroup {
                group: identity.group_id,
            });
        }
        if identity.group_id != group {
            return Err(LogicalExportError::WrongGroup {
                expected: group,
                actual: identity.group_id,
            });
        }
        if identity.cluster_id != self.source_cluster {
            return Err(LogicalExportError::WrongCluster {
                group,
                expected: self.source_cluster,
                actual: identity.cluster_id,
            });
        }
        require_applied(&snapshot.snapshot, &snapshot.state, group, cut)?;
        require_fence(&snapshot.snapshot, &snapshot.state, group, self.token, cut)?;

        let mut section = SectionBudget::new(80, self.limits.max_section_bytes)?;
        section.charge_items(planned_partitions.len(), 76)?;
        let planned = planned_partitions
            .iter()
            .map(|partition| (partition.stream(), partition.partition()))
            .collect::<BTreeSet<_>>();
        if planned.len() != planned_partitions.len() {
            return Err(corruption(group, "planned data-group partitions repeat"));
        }
        let mut materialized = BTreeMap::new();
        let mut tails = BTreeMap::new();
        for partition in planned_partitions {
            self.cancellation.check()?;
            let retention = snapshot_value::<PartitionRetentionState>(
                &snapshot.snapshot,
                &snapshot.state,
                &retention_key(*partition),
                group,
            )?
            .unwrap_or_default();
            let floor = RecordOffset::new(retention.logical_floor);
            let tail = RecordOffset::new(
                snapshot_value::<u64>(
                    &snapshot.snapshot,
                    &snapshot.state,
                    &crate::next_offset_key(*partition),
                    group,
                )?
                .unwrap_or_default(),
            );
            let record_count = tail
                .get()
                .checked_sub(floor.get())
                .ok_or_else(|| corruption(group, "partition retention floor exceeds its tail"))?;
            self.budget.charge_records(record_count, &self.limits)?;
            section.charge(
                record_count
                    .checked_mul(16)
                    .ok_or(LogicalExportError::Limit {
                        limit: "section_bytes",
                    })?,
            )?;
            let capacity = usize::try_from(record_count)
                .map_err(|_| LogicalExportError::Limit { limit: "records" })?;
            let mut records = Vec::with_capacity(capacity);
            for offset in floor.get()..tail.get() {
                self.cancellation.check()?;
                let stored: StoredRecord = snapshot_value(
                    &snapshot.snapshot,
                    &snapshot.state,
                    &record_key(*partition, offset),
                    group,
                )?
                .ok_or(LogicalExportError::MissingRecord {
                    partition: *partition,
                    offset: RecordOffset::new(offset),
                })?;
                let payload_value = snapshot
                    .snapshot
                    .get_pinned_cf(&snapshot.payload, payload_bytes_key(&stored.payload_key))
                    .map_err(|error| storage_error(group, error))?
                    .ok_or(LogicalExportError::MissingPayload {
                        partition: *partition,
                        offset: RecordOffset::new(offset),
                    })?;
                self.budget
                    .ensure_payload_allocation(stored.payload_bytes, &self.limits)?;
                section.charge(stored.payload_bytes)?;
                self.cancellation.check()?;
                let payload = if let Some(bytes) = payload_value.strip_prefix(PAYLOAD_MAGIC) {
                    Cow::Borrowed(bytes)
                } else {
                    if u64::try_from(payload_value.len())
                        .map_or(true, |length| length > self.limits.max_section_bytes)
                    {
                        return Err(LogicalExportError::Limit {
                            limit: "section_bytes",
                        });
                    }
                    Cow::Owned(
                        crate::decode_payload_value(payload_value.as_ref())
                            .map_err(|error| corruption(group, error.to_string()))?,
                    )
                };
                self.cancellation.check()?;
                let payload_bytes =
                    u64::try_from(payload.len()).map_err(|_| LogicalExportError::Limit {
                        limit: "payload_bytes",
                    })?;
                if payload_bytes != stored.payload_bytes {
                    return Err(corruption(
                        group,
                        "stored record payload length does not match its payload",
                    ));
                }
                self.budget
                    .charge_payload_bytes(payload_bytes, &self.limits)?;
                self.cancellation.check()?;
                records.push(CommittedRecord::new(
                    RecordOffset::new(offset),
                    payload.into_owned(),
                ));
            }
            let bookmark_publication_ceiling = BookmarkPublicationSequence::new(
                snapshot_value::<u64>(
                    &snapshot.snapshot,
                    &snapshot.state,
                    &crate::bookmark_publication_key(*partition),
                    group,
                )?
                .unwrap_or_default(),
            );
            let key = (partition.stream(), partition.partition());
            tails.insert(key, tail);
            materialized.insert(
                key,
                MaterializedPartitionV1 {
                    retention_floor: floor,
                    tail,
                    bookmark_publication_ceiling,
                    records,
                    bookmarks: Vec::new(),
                },
            );
        }

        for item in snapshot.snapshot.iterator_cf(
            &snapshot.state,
            IteratorMode::From(BOOKMARK_ID_PREFIX, Direction::Forward),
        ) {
            self.cancellation.check()?;
            let (key, value) = item.map_err(|error| storage_error(group, error))?;
            if !key.starts_with(BOOKMARK_ID_PREFIX) {
                break;
            }
            let bookmark: CommittedBookmark =
                decode(&value).map_err(|error| corruption(group, error.to_string()))?;
            if key.as_ref() != crate::bookmark_id_key(bookmark.id()).as_slice()
                || bookmark.cursor().cluster() != self.source_cluster
            {
                return Err(corruption(
                    group,
                    "partition bookmark key or cluster is invalid",
                ));
            }
            let partition = bookmark.cursor().partition();
            if !self.selected_streams.contains(&partition.stream()) {
                continue;
            }
            let target = (partition.stream(), partition.partition());
            let tail = tails.get(&target).ok_or_else(|| {
                corruption(
                    group,
                    "partition bookmark targets a selected partition in another group",
                )
            })?;
            if bookmark.cursor().next_offset() > *tail {
                return Err(corruption(
                    group,
                    "partition bookmark target exceeds the partition tail",
                ));
            }
            self.budget.charge_bookmarks(1, &self.limits)?;
            section.charge(
                73_u64
                    .checked_add(u64::try_from(bookmark.name().as_str().len()).map_err(|_| {
                        LogicalExportError::Limit {
                            limit: "section_bytes",
                        }
                    })?)
                    .ok_or(LogicalExportError::Limit {
                        limit: "section_bytes",
                    })?,
            )?;
            materialized
                .get_mut(&target)
                .expect("planned partition initialized its materialized state")
                .bookmarks
                .push(bookmark);
        }

        let mut partitions = Vec::with_capacity(planned_partitions.len());
        for partition in planned_partitions {
            self.cancellation.check()?;
            let key = (partition.stream(), partition.partition());
            let mut materialized = materialized
                .remove(&key)
                .expect("planned partition initialized its materialized state");
            materialized
                .bookmarks
                .sort_by_key(|bookmark| (bookmark.publication(), bookmark.id()));
            partitions.push(PartitionV1 {
                source_cluster: self.source_cluster,
                stream: partition.stream(),
                partition: partition.partition(),
                retention_floor: materialized.retention_floor,
                tail: materialized.tail,
                bookmark_publication_ceiling: materialized.bookmark_publication_ceiling,
                records: materialized.records,
                bookmarks: materialized.bookmarks,
            });
        }

        Ok(DataGroupV1 {
            source_cluster: self.source_cluster,
            export_id: self.export_id,
            group,
            cut,
            partitions,
        })
    }

    #[cfg(test)]
    fn data_group_after_snapshot(
        &mut self,
        group: GroupId,
        cut: GroupCut,
        after_snapshot: impl FnOnce(),
    ) -> Result<Option<DataGroupV1>, LogicalExportError> {
        self.data_group_with_hook(group, cut, after_snapshot)
    }
}

impl DataGroupSourceV1 for LogicalExportSourceV1 {
    type Error = LogicalExportError;

    fn data_group(
        &mut self,
        group: GroupId,
        cut: GroupCut,
    ) -> Result<Option<DataGroupV1>, Self::Error> {
        self.data_group_with_hook(group, cut, || {})
    }
}

impl CommittedStateReader {
    pub fn prepare_logical_export_v1(
        &self,
        data_groups: &[CommittedStateReader],
        limits: ExportLimits,
    ) -> Result<PreparedLogicalExportV1, LogicalExportError> {
        self.prepare_logical_export_v1_cancellable(
            data_groups,
            limits,
            LogicalExportCancellation::new(),
        )
    }

    pub fn prepare_logical_export_v1_cancellable(
        &self,
        data_groups: &[CommittedStateReader],
        limits: ExportLimits,
        cancellation: LogicalExportCancellation,
    ) -> Result<PreparedLogicalExportV1, LogicalExportError> {
        self.prepare_logical_export_v1_with_hook(data_groups, limits, cancellation, || {})
    }

    #[cfg(test)]
    fn prepare_logical_export_v1_after_control_snapshot(
        &self,
        data_groups: &[CommittedStateReader],
        limits: ExportLimits,
        after_snapshot: impl FnOnce(),
    ) -> Result<PreparedLogicalExportV1, LogicalExportError> {
        self.prepare_logical_export_v1_with_hook(
            data_groups,
            limits,
            LogicalExportCancellation::new(),
            after_snapshot,
        )
    }

    fn prepare_logical_export_v1_with_hook(
        &self,
        data_groups: &[CommittedStateReader],
        limits: ExportLimits,
        cancellation: LogicalExportCancellation,
        after_snapshot: impl FnOnce(),
    ) -> Result<PreparedLogicalExportV1, LogicalExportError> {
        cancellation.check()?;
        let group = self.db.identity.group_id;
        let state_bank = self
            .db
            .state_bank
            .read()
            .map_err(|_| LogicalExportError::Storage {
                group,
                reason: "state bank lock poisoned".to_owned(),
            })?;
        let state = self
            .db
            .raw_cf(state_bank.column_family())
            .map_err(|error| storage_error(group, error))?;
        let meta = self
            .db
            .raw_cf(CF_META)
            .map_err(|error| storage_error(group, error))?;
        let snapshot = ControlSnapshotV1 {
            snapshot: self.db.db.snapshot(),
            state,
            meta,
        };
        drop(state_bank);
        after_snapshot();
        cancellation.check()?;

        prepare_from_control_snapshot(&snapshot, data_groups, limits, group, cancellation)
    }
}

fn prepare_from_control_snapshot(
    snapshot: &ControlSnapshotV1<'_>,
    data_readers: &[CommittedStateReader],
    limits: ExportLimits,
    reader_group: GroupId,
    cancellation: LogicalExportCancellation,
) -> Result<PreparedLogicalExportV1, LogicalExportError> {
    cancellation.check()?;
    let identity: GroupIdentity = required_value(
        &snapshot.snapshot,
        &snapshot.meta,
        KEY_IDENTITY,
        reader_group,
    )?;
    if identity.format_version != STORAGE_FORMAT_VERSION {
        return Err(corruption(
            identity.group_id,
            "group storage format is unsupported",
        ));
    }
    if identity.kind != GroupKind::Control {
        return Err(LogicalExportError::WrongControlGroup {
            group: identity.group_id,
        });
    }
    if identity.group_id != reader_group {
        return Err(LogicalExportError::WrongGroup {
            expected: reader_group,
            actual: identity.group_id,
        });
    }

    let active: ActiveExport = snapshot_value(
        &snapshot.snapshot,
        &snapshot.state,
        KEY_ACTIVE_EXPORT,
        reader_group,
    )?
    .ok_or(LogicalExportError::NoRebuildableExport)?;
    let cut = match active.phase() {
        light_stream_core::ActiveExportPhase::Materializing(cut) => cut.clone(),
        ActiveExportPhase::Available(available) => available.cut().clone(),
        _ => return Err(LogicalExportError::NoRebuildableExport),
    };
    let spec = active.spec();
    if identity.cluster_id != spec.cluster() {
        return Err(LogicalExportError::WrongCluster {
            group: identity.group_id,
            expected: spec.cluster(),
            actual: identity.cluster_id,
        });
    }
    if cut.control().group() != identity.group_id {
        return Err(LogicalExportError::WrongGroup {
            expected: identity.group_id,
            actual: cut.control().group(),
        });
    }
    require_applied(
        &snapshot.snapshot,
        &snapshot.state,
        identity.group_id,
        cut.control(),
    )?;
    require_fence(
        &snapshot.snapshot,
        &snapshot.state,
        identity.group_id,
        spec.token(),
        cut.control(),
    )?;

    let mut configured_data_groups = spec
        .configured_data_groups()
        .iter()
        .copied()
        .collect::<Vec<_>>();
    configured_data_groups.sort_unstable();
    if configured_data_groups != cut.data().keys().copied().collect::<Vec<_>>() {
        return Err(corruption(
            identity.group_id,
            "active export configured groups do not match its cut",
        ));
    }
    let mut stored_groups: Vec<GroupId> = required_value(
        &snapshot.snapshot,
        &snapshot.state,
        KEY_DATA_GROUP_POOL,
        identity.group_id,
    )?;
    stored_groups.sort_unstable();
    if stored_groups.windows(2).any(|pair| pair[0] == pair[1])
        || stored_groups != configured_data_groups
    {
        return Err(corruption(
            identity.group_id,
            "configured data groups do not match the active export",
        ));
    }

    let catalog_revision = required_value(
        &snapshot.snapshot,
        &snapshot.state,
        KEY_CATALOG_REVISION,
        identity.group_id,
    )?;
    let assignment_cursor = required_value(
        &snapshot.snapshot,
        &snapshot.state,
        KEY_ASSIGNMENT_CURSOR,
        identity.group_id,
    )?;
    let max_streams = required_value(
        &snapshot.snapshot,
        &snapshot.state,
        KEY_MAX_STREAMS,
        identity.group_id,
    )?;
    let max_partitions_per_stream = required_value(
        &snapshot.snapshot,
        &snapshot.state,
        KEY_MAX_PARTITIONS,
        identity.group_id,
    )?;
    let mut budget = ExportBudget::default();
    let mut section = SectionBudget::new(104, limits.max_section_bytes)?;
    section.charge_items(configured_data_groups.len(), 8)?;
    let stream_count_per_collection = u64::try_from(spec.selection().as_slice().len())
        .map_err(|_| LogicalExportError::Limit { limit: "streams" })?;
    budget.charge_streams(stream_count_per_collection, &limits)?;
    budget.charge_streams(stream_count_per_collection, &limits)?;
    let selected_streams = spec.selection().as_slice().to_vec();
    let selected_set = selected_streams.iter().copied().collect::<BTreeSet<_>>();
    let mut descriptors = BTreeMap::new();
    let mut partitions_by_group = configured_data_groups
        .iter()
        .copied()
        .map(|group| (group, Vec::new()))
        .collect::<BTreeMap<_, _>>();
    let mut seen_partitions = BTreeSet::new();

    for stream in &selected_streams {
        cancellation.check()?;
        let stored: StreamDescriptor = required_value(
            &snapshot.snapshot,
            &snapshot.state,
            &stream_key(*stream),
            identity.group_id,
        )?;
        if stored.stream() != *stream
            || stored.cluster() != identity.cluster_id
            || stored.lifecycle() != StreamLifecycle::Active
        {
            return Err(corruption(
                identity.group_id,
                "selected stream descriptor identity or lifecycle is invalid",
            ));
        }
        let stream_bytes = 77_u64
            .checked_add(u64::try_from(stored.name().as_str().len()).map_err(|_| {
                LogicalExportError::Limit {
                    limit: "section_bytes",
                }
            })?)
            .and_then(|value| {
                u64::try_from(stored.placements().len())
                    .ok()
                    .and_then(|count| count.checked_mul(12))
                    .and_then(|bytes| value.checked_add(bytes))
            })
            .and_then(|value| {
                u64::try_from(stored.ready_groups().len())
                    .ok()
                    .and_then(|count| count.checked_mul(8))
                    .and_then(|bytes| value.checked_add(bytes))
            })
            .ok_or(LogicalExportError::Limit {
                limit: "section_bytes",
            })?;
        section.charge(stream_bytes)?;
        budget.charge_partitions(
            u64::try_from(stored.placements().len()).map_err(|_| LogicalExportError::Limit {
                limit: "partitions",
            })?,
            &limits,
        )?;
        let mut placements = stored.placements().to_vec();
        placements.sort_by_key(|placement| (placement.partition(), placement.group()));
        if placements
            .windows(2)
            .any(|pair| pair[0].partition() == pair[1].partition())
        {
            return Err(corruption(
                identity.group_id,
                "selected stream has duplicate partition placements",
            ));
        }
        let mut ready_groups = stored.ready_groups().to_vec();
        ready_groups.sort_unstable();
        if ready_groups.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(corruption(
                identity.group_id,
                "selected stream has duplicate ready groups",
            ));
        }
        for placement in &placements {
            let partition = PartitionKey::new(*stream, placement.partition());
            if !seen_partitions.insert((*stream, placement.partition())) {
                return Err(corruption(
                    identity.group_id,
                    "selected stream has duplicate partitions",
                ));
            }
            partitions_by_group
                .get_mut(&placement.group())
                .ok_or_else(|| {
                    corruption(
                        identity.group_id,
                        "selected partition targets an unconfigured data group",
                    )
                })?
                .push(partition);
        }
        descriptors.insert(
            *stream,
            StreamDescriptor::new(
                stored.cluster(),
                stored.stream(),
                stored.name().clone(),
                stored.lifecycle(),
                placements,
                ready_groups,
                stored.revision(),
            ),
        );
    }
    for partitions in partitions_by_group.values_mut() {
        cancellation.check()?;
        partitions.sort_by_key(|partition| (partition.stream(), partition.partition()));
    }

    let mut bookmarks_by_stream = selected_streams
        .iter()
        .copied()
        .map(|stream| (stream, Vec::new()))
        .collect::<BTreeMap<_, _>>();
    for item in snapshot.snapshot.iterator_cf(
        &snapshot.state,
        IteratorMode::From(STREAM_BOOKMARK_ID_PREFIX, Direction::Forward),
    ) {
        cancellation.check()?;
        let (key, value) = item.map_err(|error| storage_error(identity.group_id, error))?;
        if !key.starts_with(STREAM_BOOKMARK_ID_PREFIX) {
            break;
        }
        let stored: CommittedStreamBookmark =
            decode(&value).map_err(|error| corruption(identity.group_id, error.to_string()))?;
        let expected_key = crate::stream_bookmark_id_key(stored.id());
        let stream = stored.vector().stream();
        if key.as_ref() != expected_key.as_slice()
            || stored.vector().cluster() != identity.cluster_id
            || stored.vector().positions().iter().any(|position| {
                position.cluster() != identity.cluster_id || position.partition().stream() != stream
            })
        {
            return Err(corruption(
                identity.group_id,
                "stream bookmark key or identity is invalid",
            ));
        }
        if !selected_set.contains(&stream) {
            continue;
        }
        budget.charge_bookmarks(1, &limits)?;
        let descriptor = descriptors.get(&stream).ok_or_else(|| {
            corruption(
                identity.group_id,
                "stream bookmark targets a missing selected stream",
            )
        })?;
        budget.charge_partitions(
            u64::try_from(stored.vector().positions().len()).map_err(|_| {
                LogicalExportError::Limit {
                    limit: "partitions",
                }
            })?,
            &limits,
        )?;
        let bookmark_bytes = 69_u64
            .checked_add(u64::try_from(stored.name().as_str().len()).map_err(|_| {
                LogicalExportError::Limit {
                    limit: "section_bytes",
                }
            })?)
            .and_then(|value| {
                u64::try_from(stored.vector().positions().len())
                    .ok()
                    .and_then(|count| count.checked_mul(12))
                    .and_then(|bytes| value.checked_add(bytes))
            })
            .ok_or(LogicalExportError::Limit {
                limit: "section_bytes",
            })?;
        section.charge(bookmark_bytes)?;
        let mut positions = stored.vector().positions().to_vec();
        positions.sort_by_key(|position| position.partition().partition());
        if positions.len() != descriptor.placements().len()
            || positions
                .iter()
                .zip(descriptor.placements())
                .any(|(position, placement)| {
                    position.cluster() != identity.cluster_id
                        || position.partition().stream() != stream
                        || position.partition().partition() != placement.partition()
                })
        {
            return Err(corruption(
                identity.group_id,
                "stream bookmark targets do not match the selected stream",
            ));
        }
        let vector = StreamCursorVector::new(stream, positions)
            .map_err(|error| corruption(identity.group_id, error.to_string()))?;
        let mut bookmark = CommittedStreamBookmark::published(
            stored.id(),
            stored.name().clone(),
            vector,
            stored.publication(),
        );
        if stored.lifecycle() == BookmarkLifecycle::Deleted {
            bookmark.mark_deleted();
        }
        bookmarks_by_stream
            .get_mut(&stream)
            .expect("selected stream initialized its bookmark collection")
            .push(bookmark);
    }

    let mut streams = Vec::with_capacity(selected_streams.len());
    for stream in &selected_streams {
        cancellation.check()?;
        let mut bookmarks = bookmarks_by_stream
            .remove(stream)
            .expect("selected stream initialized its bookmark collection");
        bookmarks.sort_by_key(|bookmark| (bookmark.publication(), bookmark.id()));
        let bookmark_publication_ceiling = BookmarkPublicationSequence::new(
            snapshot_value::<u64>(
                &snapshot.snapshot,
                &snapshot.state,
                &stream_bookmark_publication_key(*stream),
                identity.group_id,
            )?
            .unwrap_or_default(),
        );
        streams.push(ActiveStreamV1 {
            descriptor: descriptors
                .remove(stream)
                .expect("selected stream descriptor was loaded"),
            bookmark_publication_ceiling,
            bookmarks,
        });
    }

    let control = ControlSectionV1 {
        source_cluster: identity.cluster_id,
        export_id: spec.export().into(),
        cut: cut.control(),
        catalog_revision,
        assignment_cursor,
        max_streams,
        max_partitions_per_stream,
        configured_data_groups: configured_data_groups.clone(),
        streams,
    };
    let mut readers = BTreeMap::new();
    for reader in data_readers {
        cancellation.check()?;
        let group = reader.db.identity.group_id;
        if reader.db.identity.kind != GroupKind::Data {
            return Err(LogicalExportError::WrongDataGroup { group });
        }
        if !partitions_by_group.contains_key(&group) {
            return Err(LogicalExportError::UnplannedGroup { group });
        }
        if readers.insert(group, reader.clone()).is_some() {
            return Err(LogicalExportError::DuplicateGroup { group });
        }
    }
    let mut groups = BTreeMap::new();
    for (group, partitions) in partitions_by_group {
        cancellation.check()?;
        let reader = readers
            .remove(&group)
            .ok_or(LogicalExportError::MissingGroup { group })?;
        let cut = *cut
            .data()
            .get(&group)
            .expect("configured groups match the quiescent cut");
        groups.insert(
            group,
            PlannedGroupV1 {
                cut,
                partitions,
                delivery: GroupDeliveryV1::Pending(reader),
            },
        );
    }

    let document = ExportDocumentV1 {
        source_cluster: identity.cluster_id,
        export_id: spec.export().into(),
        selected_streams,
        cut,
        control,
        required_features: REQUIRED_FEATURES_V1,
        exclusions: ExportExclusionsV1::v1(),
    };
    Ok(PreparedLogicalExportV1 {
        document,
        source: LogicalExportSourceV1 {
            groups,
            selected_streams: selected_set,
            source_cluster: identity.cluster_id,
            export_id: spec.export().into(),
            token: spec.token(),
            limits,
            budget,
            cancellation,
        },
        limits,
    })
}

fn require_applied(
    snapshot: &Snapshot<'_>,
    state: &Arc<rocksdb::BoundColumnFamily<'_>>,
    group: GroupId,
    cut: GroupCut,
) -> Result<(), LogicalExportError> {
    let applied: GroupLogId = required_value(snapshot, state, KEY_APPLIED, group)?;
    if applied.index < cut.applied_index() {
        return Err(corruption(
            group,
            "group has not applied the requested export cut",
        ));
    }
    Ok(())
}

fn require_fence(
    snapshot: &Snapshot<'_>,
    state: &Arc<rocksdb::BoundColumnFamily<'_>>,
    group: GroupId,
    token: ExportFenceToken,
    cut: GroupCut,
) -> Result<(), LogicalExportError> {
    let fence: light_stream_core::MutationFenceState =
        snapshot_value(snapshot, state, KEY_MUTATION_FENCE, group)?
            .ok_or(LogicalExportError::Fence { group })?;
    let held = fence
        .held()
        .copied()
        .ok_or(LogicalExportError::Fence { group })?;
    if held.token() != token {
        return Err(LogicalExportError::WrongToken {
            group,
            expected: token,
            actual: held.token(),
        });
    }
    if held.cut().group() != group {
        return Err(LogicalExportError::WrongGroup {
            expected: group,
            actual: held.cut().group(),
        });
    }
    if held.cut() != cut {
        return Err(LogicalExportError::WrongCut {
            group,
            expected: cut,
            actual: held.cut(),
        });
    }
    Ok(())
}

fn required_value<T: DeserializeOwned>(
    snapshot: &Snapshot<'_>,
    cf: &Arc<rocksdb::BoundColumnFamily<'_>>,
    key: &[u8],
    group: GroupId,
) -> Result<T, LogicalExportError> {
    snapshot_value(snapshot, cf, key, group)?
        .ok_or_else(|| corruption(group, format!("required key {:?} is missing", key)))
}

fn snapshot_value<T: DeserializeOwned>(
    snapshot: &Snapshot<'_>,
    cf: &Arc<rocksdb::BoundColumnFamily<'_>>,
    key: &[u8],
    group: GroupId,
) -> Result<Option<T>, LogicalExportError> {
    snapshot
        .get_cf(cf, key)
        .map_err(|error| storage_error(group, error))?
        .map(|value| decode(&value).map_err(|error| corruption(group, error.to_string())))
        .transpose()
}

fn charge(
    total: &mut u64,
    amount: u64,
    limit: u64,
    name: &'static str,
) -> Result<(), LogicalExportError> {
    let next = total
        .checked_add(amount)
        .ok_or(LogicalExportError::Limit { limit: name })?;
    if next > limit {
        return Err(LogicalExportError::Limit { limit: name });
    }
    *total = next;
    Ok(())
}

fn corruption(group: GroupId, reason: impl Into<String>) -> LogicalExportError {
    LogicalExportError::Corruption {
        group,
        reason: reason.into(),
    }
}

fn storage_error(group: GroupId, error: impl std::fmt::Display) -> LogicalExportError {
    LogicalExportError::Storage {
        group,
        reason: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cell::Cell,
        collections::BTreeMap,
        fs,
        io::Cursor,
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };

    use light_stream_core::{
        BookmarkId, BookmarkLifecycle, BookmarkName, CatalogRequestId, ClusterId, CommittedCursor,
        CreateStreamSpec, ExportDeadline, ExportEpoch, ExportFenceObservation, ExportFenceToken,
        ExportFormatVersion, ExportIntent, ExportSelection, GroupCut, GroupId, MutationFenceState,
        MutationRequestId, NodeId, PartitionId, PartitionKey, ProducerRequestId, PublishBatch,
        RecordOffset, RequestSequence, RetentionRequest, StreamCursorVector, StreamId,
        StreamLifecycle, StreamName,
    };
    use light_stream_export::{
        DataGroupSourceV1, ExportExclusionsV1, ExportLimits, REQUIRED_FEATURES_V1, write_v1,
    };
    use openraft::EntryPayload;
    use rocksdb::{IteratorMode, WriteBatch};
    use uuid::Uuid;

    use super::{LogicalExportCancellation, LogicalExportError, PreparedLogicalExportV1};
    use crate::{
        ApplyResult, BootstrapSpec, CF_META, CF_PAYLOAD, CF_STATE, CONTROL_GROUP_ID,
        ClockObservation, ControlRaftConfig, DATA_GROUP_ID, DEFAULT_RECEIPT_WINDOW,
        ExportApplyResult, ExportCommand, GroupCommand, GroupDb, GroupEntry, GroupIdentity,
        GroupKind, GroupLeaderId, GroupLogId, GroupStorageBudget, KEY_ACTIVE_EXPORT,
        KEY_ACTIVE_STATE_BANK, KEY_ASSIGNMENT_CURSOR, KEY_CATALOG_REVISION, KEY_DATA_GROUP_POOL,
        KEY_MAX_PARTITIONS, KEY_MAX_STREAMS, KEY_MUTATION_FENCE, StoreHandles, StoredRecord,
        create_control_store, create_data_store, encode, encode_payload_value, next_offset_key,
        payload_bytes_key, payload_owners_key, record_key, stream_key,
    };

    static TEST_ID: AtomicU64 = AtomicU64::new(1);

    struct ProjectTestDir(PathBuf);

    impl ProjectTestDir {
        fn new(label: &str) -> Self {
            let id = TEST_ID.fetch_add(1, Ordering::Relaxed);
            let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../target/test-data/light-stream-storage-logical-export")
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

    struct Fixture {
        _directory: ProjectTestDir,
        control: StoreHandles<ControlRaftConfig>,
        data: BTreeMap<GroupId, StoreHandles<crate::DataRaftConfig>>,
        cluster: ClusterId,
        selected_stream: StreamId,
        unselected_stream: StreamId,
        partition: PartitionKey,
        populated_group: GroupId,
        empty_group: GroupId,
        token: ExportFenceToken,
        populated_cut: GroupCut,
        empty_cut: GroupCut,
    }

    impl Fixture {
        fn new(label: &str) -> Self {
            let directory = ProjectTestDir::new(label);
            let cluster = ClusterId::from_uuid(Uuid::from_u128(0x100));
            let selected_stream = StreamId::from_uuid(Uuid::from_u128(0x101));
            let unselected_stream = StreamId::from_uuid(Uuid::from_u128(0x102));
            let populated_group = GroupId::new(DATA_GROUP_ID).unwrap();
            let empty_group = GroupId::new(DATA_GROUP_ID + 1).unwrap();
            let bootstrap = BootstrapSpec::new(
                cluster,
                selected_stream,
                StreamName::parse("selected").unwrap(),
            );
            let control = create_control_store(
                &directory.0.join("control"),
                GroupIdentity::new(
                    cluster,
                    GroupId::new(CONTROL_GROUP_ID).unwrap(),
                    GroupKind::Control,
                ),
                DEFAULT_RECEIPT_WINDOW,
                test_budget(),
            )
            .unwrap();
            apply(
                &control.reader.db,
                1,
                GroupCommand::BootstrapControl {
                    spec: bootstrap.clone(),
                    topology: None,
                    security: None,
                    data_groups: vec![populated_group, empty_group],
                    max_streams: 17,
                    max_partitions_per_stream: 5,
                },
            );
            apply(
                &control.reader.db,
                2,
                GroupCommand::CreateStreamIntent {
                    spec: CreateStreamSpec::new(
                        CatalogRequestId::from_uuid(Uuid::from_u128(0x103)),
                        StreamName::parse("unselected").unwrap(),
                        1,
                    )
                    .unwrap(),
                    stream_id: unselected_stream,
                },
            );
            apply(
                &control.reader.db,
                3,
                GroupCommand::ReplicaReady {
                    stream_id: unselected_stream,
                    group_id: empty_group,
                },
            );
            apply(
                &control.reader.db,
                4,
                GroupCommand::ActivateStream {
                    stream_id: unselected_stream,
                },
            );
            let partition = PartitionKey::new(selected_stream, PartitionId::new(0));
            let vector = || {
                StreamCursorVector::new(
                    selected_stream,
                    vec![CommittedCursor::new(
                        cluster,
                        partition,
                        RecordOffset::new(3),
                    )],
                )
                .unwrap()
            };
            apply(
                &control.reader.db,
                5,
                GroupCommand::CreateStreamBookmark {
                    id: BookmarkId::from_uuid(Uuid::from_u128(0x104)),
                    name: BookmarkName::parse("active-stream-bookmark").unwrap(),
                    vector: vector(),
                },
            );
            let deleted_stream_bookmark = BookmarkId::from_uuid(Uuid::from_u128(0x105));
            apply(
                &control.reader.db,
                6,
                GroupCommand::CreateStreamBookmark {
                    id: deleted_stream_bookmark,
                    name: BookmarkName::parse("deleted-stream-bookmark").unwrap(),
                    vector: vector(),
                },
            );
            apply(
                &control.reader.db,
                7,
                GroupCommand::DeleteStreamBookmark {
                    stream_id: selected_stream,
                    id: deleted_stream_bookmark,
                },
            );

            let populated = create_data_store(
                &directory.0.join("data-populated"),
                GroupIdentity::new(cluster, populated_group, GroupKind::Data),
                DEFAULT_RECEIPT_WINDOW,
                test_budget(),
            )
            .unwrap();
            apply(
                &populated.reader.db,
                1,
                GroupCommand::BootstrapData {
                    spec: bootstrap.clone(),
                },
            );
            apply(
                &populated.reader.db,
                2,
                GroupCommand::Publish {
                    batch: PublishBatch::new(
                        cluster,
                        partition,
                        ProducerRequestId::new(
                            light_stream_core::PrincipalId::parse("producer").unwrap(),
                            light_stream_core::ProducerSessionId::from_uuid(Uuid::from_u128(0x106)),
                            RequestSequence::new(1),
                        ),
                        vec![b"zero".to_vec(), b"one".to_vec(), b"two".to_vec()],
                    )
                    .unwrap(),
                },
            );
            apply(
                &populated.reader.db,
                3,
                GroupCommand::AdvanceRetention {
                    request: RetentionRequest::new(
                        mutation_request(20),
                        partition,
                        RecordOffset::new(1),
                    ),
                    clock: ClockObservation::new(1_000, 2_000).unwrap(),
                },
            );
            apply(
                &populated.reader.db,
                4,
                GroupCommand::MaintainRetention {
                    partition,
                    expected_cursor: RecordOffset::new(0),
                    max_records: 1,
                    max_payload_bytes: 1024,
                    clock: ClockObservation::new(2_000, 3_000).unwrap(),
                },
            );
            apply(
                &populated.reader.db,
                5,
                GroupCommand::CreateBookmark {
                    id: BookmarkId::from_uuid(Uuid::from_u128(0x107)),
                    partition,
                    name: BookmarkName::parse("active-partition-bookmark").unwrap(),
                    offset: RecordOffset::new(3),
                },
            );
            let deleted_partition_bookmark = BookmarkId::from_uuid(Uuid::from_u128(0x108));
            apply(
                &populated.reader.db,
                6,
                GroupCommand::CreateBookmark {
                    id: deleted_partition_bookmark,
                    partition,
                    name: BookmarkName::parse("deleted-partition-bookmark").unwrap(),
                    offset: RecordOffset::new(2),
                },
            );
            apply(
                &populated.reader.db,
                7,
                GroupCommand::DeleteBookmark {
                    partition,
                    id: deleted_partition_bookmark,
                },
            );

            let empty = create_data_store(
                &directory.0.join("data-empty"),
                GroupIdentity::new(cluster, empty_group, GroupKind::Data),
                DEFAULT_RECEIPT_WINDOW,
                test_budget(),
            )
            .unwrap();
            apply(
                &empty.reader.db,
                1,
                GroupCommand::BootstrapData { spec: bootstrap },
            );

            let intent = export_intent(cluster, selected_stream, 1);
            let token =
                ExportFenceToken::new(ExportEpoch::new(1).unwrap(), intent.request_digest());
            apply(
                &control.reader.db,
                8,
                GroupCommand::Export(ExportCommand::Begin {
                    intent,
                    deadline: ExportDeadline::new(10_000, 20_000).unwrap(),
                }),
            );
            let populated_observation = fence_observation(apply(
                &populated.reader.db,
                8,
                GroupCommand::Export(ExportCommand::AcquireFence { token }),
            ));
            let populated_cut = populated_observation.held().unwrap().cut();
            let empty_observation = fence_observation(apply(
                &empty.reader.db,
                2,
                GroupCommand::Export(ExportCommand::AcquireFence { token }),
            ));
            let empty_cut = empty_observation.held().unwrap().cut();
            apply(
                &control.reader.db,
                9,
                GroupCommand::Export(ExportCommand::RecordFence {
                    token,
                    observation: populated_observation,
                }),
            );
            apply(
                &control.reader.db,
                10,
                GroupCommand::Export(ExportCommand::RecordFence {
                    token,
                    observation: empty_observation,
                }),
            );
            apply(
                &control.reader.db,
                11,
                GroupCommand::Export(ExportCommand::BeginMaterialization { token }),
            );

            Self {
                _directory: directory,
                control,
                data: BTreeMap::from([(populated_group, populated), (empty_group, empty)]),
                cluster,
                selected_stream,
                unselected_stream,
                partition,
                populated_group,
                empty_group,
                token,
                populated_cut,
                empty_cut,
            }
        }

        fn readers(&self) -> Vec<crate::CommittedStateReader> {
            self.data
                .values()
                .map(|handles| handles.reader.clone())
                .collect()
        }

        fn prepare(&self, limits: ExportLimits) -> PreparedLogicalExportV1 {
            self.control
                .reader
                .prepare_logical_export_v1(&self.readers(), limits)
                .unwrap()
        }
    }

    #[test]
    fn prepares_control_catalog_and_canonical_group_plan() {
        let fixture = Fixture::new("control-plan");
        let prepared = fixture.prepare(ExportLimits::default());
        let document = prepared.document();

        assert_eq!(document.source_cluster, fixture.cluster);
        assert_eq!(document.export_id, fixture.token.export().into());
        assert_eq!(document.selected_streams, vec![fixture.selected_stream]);
        assert_eq!(document.required_features, REQUIRED_FEATURES_V1);
        assert_eq!(document.exclusions, ExportExclusionsV1::v1());
        assert_eq!(document.cut.control().applied_index(), 8);
        assert_eq!(document.control.catalog_revision, 4);
        assert_eq!(document.control.assignment_cursor, 2);
        assert_eq!(document.control.max_streams, 17);
        assert_eq!(document.control.max_partitions_per_stream, 5);
        assert_eq!(
            document.control.configured_data_groups,
            vec![fixture.populated_group, fixture.empty_group]
        );
        assert_eq!(document.control.streams.len(), 1);
        assert_eq!(
            document.control.streams[0].descriptor.stream(),
            fixture.selected_stream
        );
        assert!(
            document
                .control
                .streams
                .iter()
                .all(|stream| stream.descriptor.stream() != fixture.unselected_stream)
        );
        assert_eq!(
            document.control.streams[0].descriptor.lifecycle(),
            StreamLifecycle::Active
        );
        assert_eq!(
            prepared.plan().partitions_by_group(),
            &BTreeMap::from([
                (fixture.populated_group, vec![fixture.partition]),
                (fixture.empty_group, Vec::new()),
            ])
        );
    }

    #[test]
    fn cancelled_logical_export_stops_before_preparation() {
        let fixture = Fixture::new("cancelled-before-preparation");
        let cancellation = LogicalExportCancellation::new();
        cancellation.cancel();

        assert!(matches!(
            fixture
                .control
                .reader
                .prepare_logical_export_v1_cancellable(
                    &fixture.readers(),
                    ExportLimits::default(),
                    cancellation,
                ),
            Err(LogicalExportError::Cancelled)
        ));
    }

    #[test]
    fn cancelled_logical_export_stops_after_data_snapshot() {
        let fixture = Fixture::new("cancelled-after-data-snapshot");
        let cancellation = LogicalExportCancellation::new();
        let mut prepared = fixture
            .control
            .reader
            .prepare_logical_export_v1_cancellable(
                &fixture.readers(),
                ExportLimits::default(),
                cancellation.clone(),
            )
            .unwrap();

        assert!(matches!(
            prepared.source_mut().data_group_after_snapshot(
                fixture.populated_group,
                fixture.populated_cut,
                || cancellation.cancel(),
            ),
            Err(LogicalExportError::Cancelled)
        ));
    }

    #[test]
    fn reads_floor_to_tail_records_and_complete_bookmark_histories() {
        let fixture = Fixture::new("logical-data");
        let mut prepared = fixture.prepare(ExportLimits::default());
        let document = prepared.document();
        let stream = &document.control.streams[0];

        assert_eq!(stream.bookmark_publication_ceiling.get(), 2);
        assert_eq!(stream.bookmarks.len(), 2);
        assert_eq!(
            stream.bookmarks[0].id(),
            BookmarkId::from_uuid(Uuid::from_u128(0x104))
        );
        assert_eq!(stream.bookmarks[0].publication().get(), 1);
        assert_eq!(
            stream.bookmarks[0].lifecycle(),
            BookmarkLifecycle::Available
        );
        assert_eq!(
            stream.bookmarks[1].id(),
            BookmarkId::from_uuid(Uuid::from_u128(0x105))
        );
        assert_eq!(stream.bookmarks[1].publication().get(), 2);
        assert_eq!(stream.bookmarks[1].lifecycle(), BookmarkLifecycle::Deleted);

        let group = prepared
            .source_mut()
            .data_group(fixture.populated_group, fixture.populated_cut)
            .unwrap()
            .unwrap();
        assert_eq!(group.partitions.len(), 1);
        let partition = &group.partitions[0];
        assert_eq!(partition.retention_floor, RecordOffset::new(1));
        assert_eq!(partition.tail, RecordOffset::new(3));
        assert_eq!(
            partition
                .records
                .iter()
                .map(|record| (record.offset(), record.payload().to_vec()))
                .collect::<Vec<_>>(),
            vec![
                (RecordOffset::new(1), b"one".to_vec()),
                (RecordOffset::new(2), b"two".to_vec()),
            ]
        );
        assert_eq!(partition.bookmark_publication_ceiling.get(), 2);
        assert_eq!(partition.bookmarks.len(), 2);
        assert_eq!(
            partition.bookmarks[0].id(),
            BookmarkId::from_uuid(Uuid::from_u128(0x107))
        );
        assert_eq!(partition.bookmarks[0].publication().get(), 1);
        assert_eq!(
            partition.bookmarks[0].lifecycle(),
            BookmarkLifecycle::Available
        );
        assert_eq!(
            partition.bookmarks[1].id(),
            BookmarkId::from_uuid(Uuid::from_u128(0x108))
        );
        assert_eq!(partition.bookmarks[1].publication().get(), 2);
        assert_eq!(
            partition.bookmarks[1].lifecycle(),
            BookmarkLifecycle::Deleted
        );
    }

    #[test]
    fn source_enforces_the_planned_group_cut_exactly_once() {
        let fixture = Fixture::new("source-contract");
        let mut prepared = fixture.prepare(ExportLimits::default());
        let source = prepared.source_mut();
        let unknown_group = GroupId::new(99).unwrap();

        assert!(matches!(
            source.data_group(
                unknown_group,
                GroupCut::new(unknown_group, 1, NodeId::new(1).unwrap(), 1)
            ),
            Err(LogicalExportError::UnplannedGroup { group }) if group == unknown_group
        ));
        let wrong_cut = GroupCut::new(
            fixture.populated_group,
            fixture.populated_cut.term(),
            fixture.populated_cut.leader(),
            fixture.populated_cut.applied_index() + 1,
        );
        assert!(matches!(
            source.data_group(fixture.populated_group, wrong_cut),
            Err(LogicalExportError::WrongCut {
                group,
                expected,
                actual,
            }) if group == fixture.populated_group
                && expected == fixture.populated_cut
                && actual == wrong_cut
        ));
        assert!(
            source
                .data_group(fixture.populated_group, fixture.populated_cut)
                .unwrap()
                .is_some()
        );
        assert!(matches!(
            source.data_group(fixture.populated_group, fixture.populated_cut),
            Err(LogicalExportError::DuplicateGroup { group })
                if group == fixture.populated_group
        ));
        let empty = source
            .data_group(fixture.empty_group, fixture.empty_cut)
            .unwrap()
            .unwrap();
        assert!(empty.partitions.is_empty());
    }

    #[test]
    fn refuses_wrong_token_cut_group_and_cluster() {
        let wrong_token_fixture = Fixture::new("wrong-token");
        let wrong_intent = export_intent(
            wrong_token_fixture.cluster,
            wrong_token_fixture.selected_stream,
            99,
        );
        let wrong_token =
            ExportFenceToken::new(ExportEpoch::new(2).unwrap(), wrong_intent.request_digest());
        replace_fence(
            &wrong_token_fixture.data[&wrong_token_fixture.populated_group]
                .reader
                .db,
            wrong_token,
            wrong_token_fixture.populated_cut,
        );
        let mut prepared = wrong_token_fixture
            .control
            .reader
            .prepare_logical_export_v1(&wrong_token_fixture.readers(), ExportLimits::default())
            .unwrap();
        assert!(matches!(
            prepared.source_mut().data_group(
                wrong_token_fixture.populated_group,
                wrong_token_fixture.populated_cut
            ),
            Err(LogicalExportError::WrongToken {
                group,
                expected,
                actual,
            }) if group == wrong_token_fixture.populated_group
                && expected == wrong_token_fixture.token
                && actual == wrong_token
        ));

        let wrong_cut_fixture = Fixture::new("wrong-cut");
        let wrong_cut = GroupCut::new(
            wrong_cut_fixture.populated_group,
            wrong_cut_fixture.populated_cut.term(),
            wrong_cut_fixture.populated_cut.leader(),
            wrong_cut_fixture.populated_cut.applied_index() + 1,
        );
        replace_fence(
            &wrong_cut_fixture.data[&wrong_cut_fixture.populated_group]
                .reader
                .db,
            wrong_cut_fixture.token,
            wrong_cut,
        );
        let mut prepared = wrong_cut_fixture
            .control
            .reader
            .prepare_logical_export_v1(&wrong_cut_fixture.readers(), ExportLimits::default())
            .unwrap();
        assert!(matches!(
            prepared.source_mut().data_group(
                wrong_cut_fixture.populated_group,
                wrong_cut_fixture.populated_cut
            ),
            Err(LogicalExportError::WrongCut {
                group,
                expected,
                actual,
            }) if group == wrong_cut_fixture.populated_group
                && expected == wrong_cut_fixture.populated_cut
                && actual == wrong_cut
        ));

        let wrong_group_fixture = Fixture::new("wrong-group");
        replace_fence(
            &wrong_group_fixture.data[&wrong_group_fixture.populated_group]
                .reader
                .db,
            wrong_group_fixture.token,
            GroupCut::new(
                wrong_group_fixture.empty_group,
                wrong_group_fixture.populated_cut.term(),
                wrong_group_fixture.populated_cut.leader(),
                wrong_group_fixture.populated_cut.applied_index(),
            ),
        );
        let mut prepared = wrong_group_fixture
            .control
            .reader
            .prepare_logical_export_v1(&wrong_group_fixture.readers(), ExportLimits::default())
            .unwrap();
        assert!(matches!(
            prepared.source_mut().data_group(
                wrong_group_fixture.populated_group,
                wrong_group_fixture.populated_cut
            ),
            Err(LogicalExportError::WrongGroup {
                expected,
                actual,
            }) if expected == wrong_group_fixture.populated_group
                && actual == wrong_group_fixture.empty_group
        ));

        let missing_fence_fixture = Fixture::new("missing-fence");
        missing_fence_fixture.data[&missing_fence_fixture.populated_group]
            .reader
            .db
            .put_sync(CF_STATE, KEY_MUTATION_FENCE, &MutationFenceState::default())
            .unwrap();
        let mut prepared = missing_fence_fixture
            .control
            .reader
            .prepare_logical_export_v1(&missing_fence_fixture.readers(), ExportLimits::default())
            .unwrap();
        assert!(matches!(
            prepared.source_mut().data_group(
                missing_fence_fixture.populated_group,
                missing_fence_fixture.populated_cut
            ),
            Err(LogicalExportError::Fence { group })
                if group == missing_fence_fixture.populated_group
        ));

        let wrong_cluster_fixture = Fixture::new("wrong-cluster");
        let actual_cluster = ClusterId::from_uuid(Uuid::from_u128(0x999));
        let replacement = create_data_store(
            &wrong_cluster_fixture
                ._directory
                .0
                .join("wrong-cluster-reader"),
            GroupIdentity::new(
                actual_cluster,
                wrong_cluster_fixture.populated_group,
                GroupKind::Data,
            ),
            DEFAULT_RECEIPT_WINDOW,
            test_budget(),
        )
        .unwrap();
        replace_fence(
            &replacement.reader.db,
            wrong_cluster_fixture.token,
            wrong_cluster_fixture.populated_cut,
        );
        let readers = vec![
            replacement.reader,
            wrong_cluster_fixture.data[&wrong_cluster_fixture.empty_group]
                .reader
                .clone(),
        ];
        let mut prepared = wrong_cluster_fixture
            .control
            .reader
            .prepare_logical_export_v1(&readers, ExportLimits::default())
            .unwrap();
        assert!(matches!(
            prepared.source_mut().data_group(
                wrong_cluster_fixture.populated_group,
                wrong_cluster_fixture.populated_cut
            ),
            Err(LogicalExportError::WrongCluster {
                group,
                expected,
                actual,
            }) if group == wrong_cluster_fixture.populated_group
                && expected == wrong_cluster_fixture.cluster
                && actual == actual_cluster
        ));
    }

    #[test]
    fn refuses_invalid_reader_roles_and_absent_materializing_export() {
        let wrong_control = Fixture::new("wrong-control-reader");
        assert!(matches!(
            wrong_control.data[&wrong_control.populated_group]
                .reader
                .prepare_logical_export_v1(
                    &wrong_control.readers(),
                    ExportLimits::default()
                ),
            Err(LogicalExportError::WrongControlGroup { group })
                if group == wrong_control.populated_group
        ));

        let wrong_data = Fixture::new("wrong-data-reader");
        let mut readers = wrong_data.readers();
        readers.push(wrong_data.control.reader.clone());
        assert!(matches!(
            wrong_data
                .control
                .reader
                .prepare_logical_export_v1(&readers, ExportLimits::default()),
            Err(LogicalExportError::WrongDataGroup { group })
                if group == GroupId::new(CONTROL_GROUP_ID).unwrap()
        ));

        let no_export = Fixture::new("no-materializing-export");
        let state = no_export.control.reader.db.cf(CF_STATE).unwrap();
        no_export
            .control
            .reader
            .db
            .db
            .delete_cf(&state, KEY_ACTIVE_EXPORT)
            .unwrap();
        assert!(matches!(
            no_export
                .control
                .reader
                .prepare_logical_export_v1(&no_export.readers(), ExportLimits::default()),
            Err(LogicalExportError::NoRebuildableExport)
        ));
    }

    #[test]
    fn available_export_can_rebuild_a_missing_local_artifact() {
        let fixture = Fixture::new("available-rebuild");
        let active = fixture.control.reader.active_export().unwrap().unwrap();
        let cut = match active.phase() {
            light_stream_core::ActiveExportPhase::Materializing(cut) => cut.clone(),
            other => panic!("expected materializing export, got {other:?}"),
        };
        apply(
            &fixture.control.reader.db,
            12,
            GroupCommand::Export(ExportCommand::PublishArtifact {
                token: fixture.token,
                artifact: light_stream_core::ArtifactIdentity::new(1, [7; 32]).unwrap(),
                cut,
            }),
        );

        assert!(
            fixture
                .control
                .reader
                .prepare_logical_export_v1(&fixture.readers(), ExportLimits::default())
                .is_ok()
        );
    }

    #[test]
    fn refuses_duplicate_unplanned_and_missing_group_readers() {
        let fixture = Fixture::new("reader-set");
        let populated = fixture.data[&fixture.populated_group].reader.clone();
        assert!(matches!(
            fixture.control.reader.prepare_logical_export_v1(
                &[populated.clone(), populated],
                ExportLimits::default()
            ),
            Err(LogicalExportError::DuplicateGroup { group })
                if group == fixture.populated_group
        ));
        assert!(matches!(
            fixture.control.reader.prepare_logical_export_v1(
                &[fixture.data[&fixture.populated_group].reader.clone()],
                ExportLimits::default()
            ),
            Err(LogicalExportError::MissingGroup { group }) if group == fixture.empty_group
        ));

        let extra_group = GroupId::new(99).unwrap();
        let extra = create_data_store(
            &fixture._directory.0.join("extra-group"),
            GroupIdentity::new(fixture.cluster, extra_group, GroupKind::Data),
            DEFAULT_RECEIPT_WINDOW,
            test_budget(),
        )
        .unwrap();
        replace_fence(
            &extra.reader.db,
            fixture.token,
            GroupCut::new(extra_group, 1, NodeId::new(1).unwrap(), 1),
        );
        let mut readers = fixture.readers();
        readers.push(extra.reader);
        assert!(matches!(
            fixture
                .control
                .reader
                .prepare_logical_export_v1(&readers, ExportLimits::default()),
            Err(LogicalExportError::UnplannedGroup { group }) if group == extra_group
        ));
    }

    #[test]
    fn reports_missing_payload_record_gap_and_corruption() {
        let missing_payload = Fixture::new("missing-payload");
        delete_payload(&missing_payload, 1);
        let mut prepared = missing_payload.prepare(ExportLimits::default());
        assert!(matches!(
            prepared.source_mut().data_group(
                missing_payload.populated_group,
                missing_payload.populated_cut
            ),
            Err(LogicalExportError::MissingPayload { partition, offset })
                if partition == missing_payload.partition && offset == RecordOffset::new(1)
        ));

        let record_gap = Fixture::new("record-gap");
        delete_record(&record_gap, 1);
        let mut prepared = record_gap.prepare(ExportLimits::default());
        assert!(matches!(
            prepared.source_mut().data_group(
                record_gap.populated_group,
                record_gap.populated_cut
            ),
            Err(LogicalExportError::MissingRecord { partition, offset })
                if partition == record_gap.partition && offset == RecordOffset::new(1)
        ));

        let corruption = Fixture::new("corruption");
        let db = &corruption.data[&corruption.populated_group].reader.db;
        let state = db.cf(CF_STATE).unwrap();
        db.db
            .put_cf(&state, record_key(corruption.partition, 1), b"corrupt")
            .unwrap();
        let mut prepared = corruption.prepare(ExportLimits::default());
        assert!(matches!(
            prepared.source_mut().data_group(
                corruption.populated_group,
                corruption.populated_cut
            ),
            Err(LogicalExportError::Corruption { group, .. })
                if group == corruption.populated_group
        ));
    }

    #[test]
    fn data_group_materialization_failure_restores_all_budget_counters_and_consumes_group() {
        let fixture = Fixture::new("missing-payload-budget-rollback");
        delete_payload(&fixture, 1);
        let mut prepared = fixture.prepare(ExportLimits::default());
        let budget_before = (
            prepared.source.budget.streams,
            prepared.source.budget.partitions,
            prepared.source.budget.records,
            prepared.source.budget.bookmarks,
            prepared.source.budget.payload_bytes,
        );

        assert!(matches!(
            prepared
                .source_mut()
                .data_group(fixture.populated_group, fixture.populated_cut),
            Err(LogicalExportError::MissingPayload { partition, offset })
                if partition == fixture.partition && offset == RecordOffset::new(1)
        ));
        assert_eq!(
            (
                prepared.source.budget.streams,
                prepared.source.budget.partitions,
                prepared.source.budget.records,
                prepared.source.budget.bookmarks,
                prepared.source.budget.payload_bytes,
            ),
            budget_before
        );
        assert!(matches!(
            prepared
                .source_mut()
                .data_group(fixture.populated_group, fixture.populated_cut),
            Err(LogicalExportError::DuplicateGroup { group })
                if group == fixture.populated_group
        ));
    }

    #[test]
    fn excludes_non_logical_state_from_the_artifact() {
        let fixture = Fixture::new("excluded-state");
        let canary = "LS09_EXCLUDED_STATE_CANARY";
        fixture
            .control
            .reader
            .db
            .put_sync(CF_STATE, b"administration/excluded-canary", &canary)
            .unwrap();
        fixture.data[&fixture.populated_group]
            .reader
            .db
            .put_sync(CF_STATE, b"checkpoint/excluded-canary", &canary)
            .unwrap();
        fixture.data[&fixture.populated_group]
            .reader
            .db
            .put_sync(CF_PAYLOAD, b"bytes/excluded-canary", &canary)
            .unwrap();

        let bytes = artifact_bytes(fixture.prepare(ExportLimits::default()));
        assert!(
            !bytes
                .windows(canary.len())
                .any(|window| window == canary.as_bytes())
        );
    }

    #[test]
    fn repeated_preparations_are_byte_deterministic() {
        let fixture = Fixture::new("deterministic");
        let mut readers = fixture.readers();
        let first = artifact_bytes(
            fixture
                .control
                .reader
                .prepare_logical_export_v1(&readers, ExportLimits::default())
                .unwrap(),
        );
        readers.reverse();
        let second = artifact_bytes(
            fixture
                .control
                .reader
                .prepare_logical_export_v1(&readers, ExportLimits::default())
                .unwrap(),
        );

        assert_eq!(first, second);
    }

    #[test]
    fn data_group_snapshot_captures_live_state_at_call() {
        let fixture = Fixture::new("snapshot-mutation");
        let mut prepared = fixture.prepare(ExportLimits::default());
        fixture
            .control
            .reader
            .db
            .put_sync(CF_STATE, KEY_CATALOG_REVISION, &999_u64)
            .unwrap();
        let db = &fixture.data[&fixture.populated_group].reader.db;
        let stored = db
            .get::<StoredRecord>(CF_STATE, &record_key(fixture.partition, 1))
            .unwrap()
            .unwrap();
        let payload = db.cf(CF_PAYLOAD).unwrap();
        let payload_key = payload_bytes_key(&stored.payload_key);
        db.db
            .put_cf(&payload, &payload_key, encode_payload_value(b"now"))
            .unwrap();

        let group = prepared
            .source_mut()
            .data_group_after_snapshot(fixture.populated_group, fixture.populated_cut, || {
                db.db
                    .put_cf(&payload, &payload_key, encode_payload_value(b"new"))
                    .unwrap();
            })
            .unwrap()
            .unwrap();
        assert_eq!(prepared.document().control.catalog_revision, 4);
        assert_eq!(group.partitions[0].records[0].payload(), b"now");
    }

    #[test]
    fn data_group_snapshot_captures_active_bank_before_switch() {
        let fixture = Fixture::new("snapshot-bank-switch");
        let mut prepared = fixture.prepare(ExportLimits::default());

        let group = prepared
            .source_mut()
            .data_group_after_snapshot(fixture.populated_group, fixture.populated_cut, || {
                switch_to_divergent_bank(
                    &fixture.data[&fixture.populated_group].reader.db,
                    fixture.partition,
                );
            })
            .unwrap()
            .unwrap();
        assert_eq!(
            group.partitions[0]
                .records
                .iter()
                .map(|record| record.offset())
                .collect::<Vec<_>>(),
            vec![RecordOffset::new(1), RecordOffset::new(2)]
        );
    }

    #[test]
    fn control_plan_uses_one_snapshot_across_active_bank_switch() {
        let fixture = Fixture::new("control-snapshot-bank-switch");
        let readers = fixture.readers();
        let prepared = fixture
            .control
            .reader
            .prepare_logical_export_v1_after_control_snapshot(
                &readers,
                ExportLimits::default(),
                || {
                    switch_to_divergent_control_bank(
                        &fixture.control.reader.db,
                        fixture.selected_stream,
                        fixture.empty_group,
                    );
                },
            )
            .unwrap();
        let document = prepared.document();

        assert_eq!(document.source_cluster, fixture.cluster);
        assert_eq!(document.export_id, fixture.token.export().into());
        assert_eq!(document.selected_streams, vec![fixture.selected_stream]);
        assert_eq!(
            document.cut.control().group(),
            GroupId::new(CONTROL_GROUP_ID).unwrap()
        );
        assert_eq!(document.cut.control().term(), 1);
        assert_eq!(document.cut.control().leader(), NodeId::new(1).unwrap());
        assert_eq!(document.cut.control().applied_index(), 8);
        assert_eq!(
            document.cut.data(),
            &BTreeMap::from([
                (fixture.populated_group, fixture.populated_cut),
                (fixture.empty_group, fixture.empty_cut),
            ])
        );
        assert_eq!(document.control.catalog_revision, 4);
        assert_eq!(document.control.assignment_cursor, 2);
        assert_eq!(document.control.max_streams, 17);
        assert_eq!(document.control.max_partitions_per_stream, 5);
        assert_eq!(
            document.control.configured_data_groups,
            vec![fixture.populated_group, fixture.empty_group]
        );
        assert_eq!(document.control.streams.len(), 1);
        assert_eq!(
            document.control.streams[0].descriptor.stream(),
            fixture.selected_stream
        );
        assert_eq!(
            document.control.streams[0].descriptor.lifecycle(),
            StreamLifecycle::Active
        );
        assert_eq!(
            document.control.streams[0]
                .bookmark_publication_ceiling
                .get(),
            2
        );
        assert_eq!(document.control.streams[0].bookmarks.len(), 2);
        assert_eq!(
            prepared.plan().partitions_by_group(),
            &BTreeMap::from([
                (fixture.populated_group, vec![fixture.partition]),
                (fixture.empty_group, Vec::new()),
            ])
        );
    }

    #[test]
    fn refuses_limits_before_large_allocation_or_payload_copy() {
        let control_section_limit = Fixture::new("control-section-limit");
        assert!(matches!(
            control_section_limit
                .control
                .reader
                .prepare_logical_export_v1(
                    &control_section_limit.readers(),
                    ExportLimits {
                        max_section_bytes: 100,
                        ..ExportLimits::default()
                    },
                ),
            Err(LogicalExportError::Limit {
                limit: "section_bytes"
            })
        ));

        let data_section_limit = Fixture::new("data-section-limit");
        let db = &data_section_limit.data[&data_section_limit.populated_group]
            .reader
            .db;
        let mut stored = db
            .get::<StoredRecord>(CF_STATE, &record_key(data_section_limit.partition, 1))
            .unwrap()
            .unwrap();
        stored.payload_bytes = 1_024;
        db.put_sync(
            CF_STATE,
            &record_key(data_section_limit.partition, 1),
            &stored,
        )
        .unwrap();
        db.db
            .put_cf(
                &db.cf(CF_PAYLOAD).unwrap(),
                payload_bytes_key(&stored.payload_key),
                encode_payload_value(&vec![7; 1_024]),
            )
            .unwrap();
        let mut prepared = data_section_limit.prepare(ExportLimits {
            max_section_bytes: 512,
            ..ExportLimits::default()
        });
        assert!(matches!(
            prepared.source_mut().data_group(
                data_section_limit.populated_group,
                data_section_limit.populated_cut,
            ),
            Err(LogicalExportError::Limit {
                limit: "section_bytes"
            })
        ));

        let record_limit = Fixture::new("record-limit");
        record_limit.data[&record_limit.populated_group]
            .reader
            .db
            .put_sync(
                CF_STATE,
                &next_offset_key(record_limit.partition),
                &10_000_000_u64,
            )
            .unwrap();
        let limits = ExportLimits {
            max_records: 1,
            ..ExportLimits::default()
        };
        let mut prepared = record_limit.prepare(limits);
        assert!(matches!(
            prepared
                .source_mut()
                .data_group(record_limit.populated_group, record_limit.populated_cut),
            Err(LogicalExportError::Limit { limit: "records" })
        ));

        let payload_limit = Fixture::new("payload-limit");
        let limits = ExportLimits {
            max_payload_bytes: 1,
            ..ExportLimits::default()
        };
        let mut prepared = payload_limit.prepare(limits);
        assert!(matches!(
            prepared
                .source_mut()
                .data_group(payload_limit.populated_group, payload_limit.populated_cut),
            Err(LogicalExportError::Limit {
                limit: "payload_bytes"
            })
        ));
    }

    #[test]
    fn control_budget_counts_both_selected_stream_collections() {
        let fixture = Fixture::new("selected-stream-collections-limit");
        let limits = ExportLimits {
            max_streams: 1,
            ..ExportLimits::default()
        };

        assert!(matches!(
            fixture
                .control
                .reader
                .prepare_logical_export_v1(&fixture.readers(), limits),
            Err(LogicalExportError::Limit { limit: "streams" })
        ));
    }

    #[test]
    fn control_budget_counts_stream_bookmark_positions_before_copy() {
        let fixture = Fixture::new("control-partition-limit");
        let limits = ExportLimits {
            max_partitions: 1,
            ..ExportLimits::default()
        };

        assert!(matches!(
            fixture
                .control
                .reader
                .prepare_logical_export_v1(&fixture.readers(), limits),
            Err(LogicalExportError::Limit {
                limit: "partitions"
            })
        ));
    }

    #[test]
    fn data_group_budget_counts_emitted_partitions_before_allocation() {
        let fixture = Fixture::new("data-partition-limit");
        let limits = ExportLimits {
            max_partitions: 3,
            ..ExportLimits::default()
        };
        let mut prepared = fixture.prepare(limits);
        let after_snapshot_called = Cell::new(false);

        assert!(matches!(
            prepared.source_mut().data_group_after_snapshot(
                fixture.populated_group,
                fixture.populated_cut,
                || after_snapshot_called.set(true)
            ),
            Err(LogicalExportError::Limit {
                limit: "partitions"
            })
        ));
        assert!(!after_snapshot_called.get());
    }

    #[test]
    fn rejects_payload_length_that_disagrees_with_stored_record() {
        let fixture = Fixture::new("payload-length-mismatch");
        let db = &fixture.data[&fixture.populated_group].reader.db;
        let key = record_key(fixture.partition, 1);
        let mut stored = db.get::<StoredRecord>(CF_STATE, &key).unwrap().unwrap();
        stored.payload_bytes = 1;
        db.put_sync(CF_STATE, &key, &stored).unwrap();
        let mut prepared = fixture.prepare(ExportLimits::default());

        assert!(matches!(
            prepared
                .source_mut()
                .data_group(fixture.populated_group, fixture.populated_cut),
            Err(LogicalExportError::Corruption { group, .. })
                if group == fixture.populated_group
        ));
    }

    fn test_budget() -> GroupStorageBudget {
        GroupStorageBudget::new(8 * 1024 * 1024, 4 * 1024 * 1024).unwrap()
    }

    fn mutation_request(sequence: u64) -> MutationRequestId {
        MutationRequestId::new(
            light_stream_core::PrincipalId::parse("export-test").unwrap(),
            light_stream_core::MutationSessionId::from_uuid(Uuid::from_u128(0x200)),
            RequestSequence::new(sequence),
        )
    }

    fn export_intent(cluster: ClusterId, stream: StreamId, sequence: u64) -> ExportIntent {
        ExportIntent::new(
            mutation_request(sequence),
            cluster,
            ExportSelection::try_new([stream]).unwrap(),
            ExportFormatVersion::V1,
        )
    }

    fn apply(db: &GroupDb, index: u64, command: GroupCommand) -> ApplyResult {
        let entry = GroupEntry {
            log_id: GroupLogId::new(
                GroupLeaderId {
                    term: 1,
                    node_id: 1,
                },
                index,
            ),
            payload: EntryPayload::Normal(command),
        };
        let (_, payloads) = crate::thin_entry(entry.clone()).unwrap();
        if !payloads.is_empty() {
            let payload_cf = db.cf(CF_PAYLOAD).unwrap();
            let mut write = WriteBatch::default();
            for (key, payload) in payloads {
                write.put_cf(
                    &payload_cf,
                    payload_bytes_key(&key),
                    encode_payload_value(&payload),
                );
                write.put_cf(
                    &payload_cf,
                    payload_owners_key(&key),
                    encode(&crate::PayloadOwners {
                        raft_log: true,
                        applied_state: false,
                        applied_banks: 0,
                    })
                    .unwrap(),
                );
            }
            db.write_sync(write).unwrap();
        }
        db.apply_entry(entry).unwrap()
    }

    fn fence_observation(result: ApplyResult) -> ExportFenceObservation {
        match result {
            ApplyResult::Export(ExportApplyResult::Fence(observation)) => observation,
            other => panic!("expected export fence observation, got {other:?}"),
        }
    }

    fn replace_fence(db: &GroupDb, token: ExportFenceToken, cut: GroupCut) {
        let (state, _) = MutationFenceState::default().acquire(token, cut).unwrap();
        db.put_sync(CF_STATE, KEY_MUTATION_FENCE, &state).unwrap();
    }

    fn delete_record(fixture: &Fixture, offset: u64) {
        let db = &fixture.data[&fixture.populated_group].reader.db;
        let state = db.cf(CF_STATE).unwrap();
        db.db
            .delete_cf(&state, record_key(fixture.partition, offset))
            .unwrap();
    }

    fn delete_payload(fixture: &Fixture, offset: u64) {
        let db = &fixture.data[&fixture.populated_group].reader.db;
        let stored = db
            .get::<StoredRecord>(CF_STATE, &record_key(fixture.partition, offset))
            .unwrap()
            .unwrap();
        let payload = db.cf(CF_PAYLOAD).unwrap();
        db.db
            .delete_cf(&payload, payload_bytes_key(&stored.payload_key))
            .unwrap();
    }

    fn artifact_bytes(mut prepared: PreparedLogicalExportV1) -> Vec<u8> {
        let document = prepared.document();
        let limits = prepared.limits();
        let mut output = Cursor::new(Vec::new());
        write_v1(&mut output, &document, prepared.source_mut(), &limits).unwrap();
        output.into_inner()
    }

    fn switch_to_divergent_bank(db: &GroupDb, partition: PartitionKey) {
        let active = *db.state_bank.read().unwrap();
        let inactive = active.inactive();
        let source = db.raw_cf(active.column_family()).unwrap();
        let target = db.raw_cf(inactive.column_family()).unwrap();
        let mut write = WriteBatch::default();
        for item in db.db.iterator_cf(&source, IteratorMode::Start) {
            let (key, value) = item.unwrap();
            write.put_cf(&target, key, value);
        }
        write.delete_cf(&target, record_key(partition, 1));
        write.put_cf(
            &db.raw_cf(CF_META).unwrap(),
            KEY_ACTIVE_STATE_BANK,
            encode(&inactive).unwrap(),
        );
        db.write_sync(write).unwrap();
        *db.state_bank.write().unwrap() = inactive;
    }

    fn switch_to_divergent_control_bank(
        db: &GroupDb,
        selected_stream: StreamId,
        only_group: GroupId,
    ) {
        let active = *db.state_bank.read().unwrap();
        let inactive = active.inactive();
        let source = db.raw_cf(active.column_family()).unwrap();
        let target = db.raw_cf(inactive.column_family()).unwrap();
        let mut write = WriteBatch::default();
        for item in db.db.iterator_cf(&source, IteratorMode::Start) {
            let (key, value) = item.unwrap();
            write.put_cf(&target, key, value);
        }
        write.delete_cf(&target, KEY_ACTIVE_EXPORT);
        write.delete_cf(&target, stream_key(selected_stream));
        write.put_cf(
            &target,
            KEY_DATA_GROUP_POOL,
            encode(&vec![only_group]).unwrap(),
        );
        write.put_cf(&target, KEY_MAX_STREAMS, encode(&99_u32).unwrap());
        write.put_cf(&target, KEY_MAX_PARTITIONS, encode(&99_u32).unwrap());
        write.put_cf(&target, KEY_ASSIGNMENT_CURSOR, encode(&999_u64).unwrap());
        write.put_cf(&target, KEY_CATALOG_REVISION, encode(&999_u64).unwrap());
        write.put_cf(
            &db.raw_cf(CF_META).unwrap(),
            KEY_ACTIVE_STATE_BANK,
            encode(&inactive).unwrap(),
        );
        db.write_sync(write).unwrap();
        *db.state_bank.write().unwrap() = inactive;
    }
}
