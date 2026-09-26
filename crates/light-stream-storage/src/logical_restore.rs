use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, Write},
    path::{Path, PathBuf},
};

use light_stream_core::{
    ArtifactIdentity, BookmarkLifecycle, BookmarkPublicationSequence, BootstrapSpec, ClusterId,
    CommittedBookmark, CommittedCursor, CommittedRecord, CommittedStreamBookmark, GroupId, NodeId,
    PartitionKey, RecordOffset, StreamCursorVector, StreamDescriptor, StreamId,
};
use light_stream_export::{
    ControlSectionV1, DataGroupV1, ExportExclusionsV1, ExportLimits, ExportManifestV1,
    FORMAT_VERSION_V1, PartitionV1, REQUIRED_FEATURES_V1, VerifiedExport, VerifiedSectionV1,
    VerifyError, VisitError, verify,
};
use openraft::{BasicNode, EntryPayload, Membership};
use rocksdb::{Direction, IteratorMode, WriteBatch};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    ApplyResult, BOOKMARK_ID_PREFIX, CF_PAYLOAD, CF_RAFT_LOG, CF_STATE, CONTROL_GROUP_ID,
    DATA_GROUP_ID, GroupCommand, GroupDb, GroupEntry, GroupIdentity, GroupKind, GroupLogId,
    GroupStorageBudget, KEY_ASSIGNMENT_CURSOR, KEY_CATALOG_REVISION, KEY_DATA_GROUP_POOL,
    KEY_MAX_PARTITIONS, KEY_MAX_STREAMS, PartitionRetentionState, PayloadOwners,
    STREAM_BOOKMARK_ID_PREFIX, StateBank, StorageOpenError, StoredRecord, bookmark_id_key,
    bookmark_name_key, bookmark_order_key, bookmark_publication_key, create_control_store,
    create_data_store, decode, decode_payload_value, encode, encode_payload_value, next_offset_key,
    open_control_store, open_data_store, payload_bytes_key, payload_owners_key, record_key,
    retention_key, stream_bookmark_id_key, stream_bookmark_name_key, stream_bookmark_order_key,
    stream_bookmark_publication_key, stream_key, stream_name_key, thin_entry,
};

const RESTORE_FILE: &str = "RESTORE.json";
const IMPORT_BATCH_RECORDS: usize = 1024;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RestoreGroupPoolConfig {
    pub max_data_groups: u16,
    pub max_streams: u32,
    pub max_partitions_per_stream: u32,
    pub rocksdb_cache_bytes: usize,
    pub rocksdb_write_buffer_bytes: usize,
}

fn append_membership(db: &GroupDb, node_id: NodeId) -> Result<(), RestoreError> {
    let node = node_id.get();
    let membership = Membership::new(
        vec![BTreeSet::from([node])],
        BTreeMap::from([(node, BasicNode::new("local"))]),
    )
    .map_err(io::Error::other)?;
    let entry = GroupEntry {
        log_id: GroupLogId {
            leader_id: crate::GroupLeaderId {
                term: 1,
                node_id: node,
            },
            index: 0,
        },
        payload: EntryPayload::Membership(membership),
    };
    append_log_entry(db, entry.clone())?;
    match db.apply_entry(entry)? {
        ApplyResult::Noop => Ok(()),
        other => Err(RestoreError::AuditMismatch {
            scope: "membership".to_owned(),
            reason: format!("unexpected apply result {other}"),
        }),
    }
}

impl RestoreGroupPoolConfig {
    pub fn data_group_ids(&self) -> Result<Vec<GroupId>, RestoreError> {
        (0..self.max_data_groups)
            .map(|slot| GroupId::new(DATA_GROUP_ID + u64::from(slot)).map_err(RestoreError::Core))
            .collect()
    }

    pub fn per_group_budget(&self) -> Result<GroupStorageBudget, RestoreError> {
        let divisor = usize::from(self.max_data_groups) + 1;
        GroupStorageBudget::new(
            self.rocksdb_cache_bytes / divisor,
            self.rocksdb_write_buffer_bytes / divisor,
        )
        .map_err(RestoreError::Storage)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RestoreConfig {
    pub target_cluster: ClusterId,
    pub node_id: NodeId,
    pub data_dir: PathBuf,
    pub group_pool: RestoreGroupPoolConfig,
    pub receipt_window: usize,
    #[serde(default)]
    pub serve_config_digest: Option<[u8; 32]>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PreservedIdentities {
    pub control_group: GroupId,
    pub data_groups: Vec<GroupId>,
    pub streams: Vec<StreamId>,
    pub partitions: u64,
    pub records: u64,
    pub partition_bookmarks: u64,
    pub stream_bookmarks: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RestoreReceipt {
    pub artifact: ArtifactIdentity,
    pub source_cluster: ClusterId,
    pub target_cluster: ClusterId,
    pub preserved: PreservedIdentities,
    #[serde(with = "exclusions_serde")]
    pub exclusions: ExportExclusionsV1,
    pub target_group_pool: RestoreGroupPoolConfig,
    pub serve_config_digest: Option<[u8; 32]>,
}

#[derive(Debug, Error)]
pub enum RestoreError {
    #[error("restore destination {path} already exists")]
    DestinationExists { path: String },
    #[error("restore destination {path} is not absent")]
    DestinationNotAbsent { path: String },
    #[error("restore target cluster must differ from source cluster {0}")]
    SameCluster(ClusterId),
    #[error("restore artifact has unsupported format version {0}")]
    UnsupportedVersion(u32),
    #[error("restore artifact has unsupported required features {0:#x}")]
    UnsupportedFeatures(u64),
    #[error("restore export contains no streams")]
    EmptyExport,
    #[error("restore target group pool is missing preserved data group {0}")]
    MissingGroup(GroupId),
    #[error("restore input is not a regular file: {path}")]
    NotRegularFile { path: String },
    #[error("restore verification failed")]
    Verify(#[from] VerifyError),
    #[error("restore storage failed")]
    Storage(#[from] StorageOpenError),
    #[error("restore storage I/O failed")]
    Io(#[from] io::Error),
    #[error("restore domain value is invalid")]
    Core(#[from] light_stream_core::DomainError),
    #[error("restore section visit failed")]
    Visit(#[source] VisitError<Box<RestoreError>>),
    #[error("restore audit mismatch for {scope}: {reason}")]
    AuditMismatch { scope: String, reason: String },
    #[error("restore bootstrap command was rejected: {0}")]
    BootstrapRejected(light_stream_core::DomainError),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct RestoreRecord {
    receipt: RestoreReceipt,
}

#[derive(Clone, Debug)]
struct ControlAuditPlan {
    digest: [u8; 32],
    bootstrap: BootstrapSpec,
    streams: Vec<ActiveStreamAudit>,
    preserved_groups: Vec<GroupId>,
    catalog_revision: u64,
    assignment_cursor: u64,
}

#[derive(Clone, Debug)]
struct ActiveStreamAudit {
    descriptor: StreamDescriptor,
    bookmark_publication_ceiling: BookmarkPublicationSequence,
}

#[derive(Clone, Debug)]
struct DataGroupAuditPlan {
    digest: [u8; 32],
    partitions: Vec<PartitionAudit>,
}

#[derive(Clone, Debug)]
struct PartitionAudit {
    key: PartitionKey,
    retention_floor: RecordOffset,
    tail: RecordOffset,
    bookmark_publication_ceiling: BookmarkPublicationSequence,
}

pub fn restore_from_path(
    input: &Path,
    limits: &ExportLimits,
    config: RestoreConfig,
) -> Result<RestoreReceipt, RestoreError> {
    let metadata = fs::metadata(input)?;
    if !metadata.is_file() {
        return Err(RestoreError::NotRegularFile {
            path: input.display().to_string(),
        });
    }
    restore_from_verified(verify(File::open(input)?, limits)?, config)
}

pub fn restore_from_verified<R: Read + Seek>(
    verified: VerifiedExport<R>,
    config: RestoreConfig,
) -> Result<RestoreReceipt, RestoreError> {
    let manifest = verified.manifest().clone();
    let artifact = verified.artifact();
    if manifest.format_version != FORMAT_VERSION_V1 {
        return Err(RestoreError::UnsupportedVersion(manifest.format_version));
    }
    if manifest.required_features & !REQUIRED_FEATURES_V1 != 0 {
        return Err(RestoreError::UnsupportedFeatures(
            manifest.required_features,
        ));
    }
    if manifest.source_cluster == config.target_cluster {
        return Err(RestoreError::SameCluster(manifest.source_cluster));
    }
    if let Some(receipt) =
        matching_existing_receipt(&config.data_dir, artifact, &manifest, &config)?
    {
        return Ok(receipt);
    }
    if config.data_dir.exists() {
        return Err(RestoreError::DestinationExists {
            path: config.data_dir.display().to_string(),
        });
    }

    let data_groups = config.group_pool.data_group_ids()?;
    for group in manifest.cut.data().keys() {
        if !data_groups.contains(group) {
            return Err(RestoreError::MissingGroup(*group));
        }
    }

    let staging = staging_path(&config.data_dir)?;
    if staging.exists() {
        fs::remove_dir_all(&staging)?;
    }
    let result = restore_into_staging(
        verified,
        manifest,
        artifact,
        &config,
        &staging,
        &data_groups,
    );
    match result {
        Ok(receipt) => Ok(receipt),
        Err(error) => {
            let _ = fs::remove_dir_all(&staging);
            Err(error)
        }
    }
}

fn restore_into_staging<R: Read + Seek>(
    verified: VerifiedExport<R>,
    manifest: ExportManifestV1,
    artifact: ArtifactIdentity,
    config: &RestoreConfig,
    staging: &Path,
    data_groups: &[GroupId],
) -> Result<RestoreReceipt, RestoreError> {
    fs::create_dir_all(staging.join("groups"))?;
    let budget = config.group_pool.per_group_budget()?;
    let control_group = GroupId::new(CONTROL_GROUP_ID)?;
    fs::create_dir_all(group_path(staging, control_group))?;
    let control = create_control_store(
        &group_path(staging, control_group),
        GroupIdentity::new(config.target_cluster, control_group, GroupKind::Control),
        config.receipt_window,
        budget,
    )?;
    let mut data_handles = BTreeMap::new();
    for group in data_groups {
        fs::create_dir_all(group_path(staging, *group))?;
        data_handles.insert(
            *group,
            create_data_store(
                &group_path(staging, *group),
                GroupIdentity::new(config.target_cluster, *group, GroupKind::Data),
                config.receipt_window,
                budget,
            )?,
        );
    }

    let mut control_audit = None;
    let mut data_audits = BTreeMap::new();
    verified
        .visit_sections(|section| {
            let outcome = match section {
                VerifiedSectionV1::Control(control_section) => {
                    import_control_section(&control.reader.db, control_section, config, data_groups)
                        .and_then(|audit| {
                            for handles in data_handles.values() {
                                append_membership(&handles.reader.db, config.node_id)?;
                                append_and_apply(
                                    &handles.reader.db,
                                    1,
                                    config.node_id,
                                    GroupCommand::BootstrapData {
                                        spec: audit.bootstrap.clone(),
                                    },
                                )?;
                            }
                            control_audit = Some(audit);
                            Ok(())
                        })
                }
                VerifiedSectionV1::DataGroup(data_group) => {
                    let Some(handles) = data_handles.get(&data_group.group) else {
                        return Err(Box::new(RestoreError::MissingGroup(data_group.group)));
                    };
                    import_data_group(&handles.reader.db, data_group, config).map(|audit| {
                        data_audits.insert(data_group.group, audit);
                    })
                }
            };
            outcome.map_err(Box::new)
        })
        .map_err(RestoreError::Visit)?;

    let control_audit = control_audit.ok_or(RestoreError::EmptyExport)?;
    if control_audit.streams.is_empty() {
        return Err(RestoreError::EmptyExport);
    }

    flush_group(&control.reader.db)?;
    for handles in data_handles.values() {
        flush_group(&handles.reader.db)?;
    }
    drop(data_handles);
    drop(control);

    audit_reopened(staging, config, data_groups, &control_audit, &data_audits)?;

    let receipt = RestoreReceipt {
        artifact,
        source_cluster: manifest.source_cluster,
        target_cluster: config.target_cluster,
        preserved: PreservedIdentities {
            control_group,
            data_groups: control_audit.preserved_groups.clone(),
            streams: manifest.selected_streams.clone(),
            partitions: manifest.totals.partitions,
            records: manifest.totals.records,
            partition_bookmarks: manifest.totals.partition_bookmarks,
            stream_bookmarks: manifest.totals.stream_bookmarks,
        },
        exclusions: manifest.exclusions,
        target_group_pool: config.group_pool.clone(),
        serve_config_digest: config.serve_config_digest,
    };
    write_restore_record(staging, &receipt)?;
    sync_tree(staging)?;
    sync_parent(staging)?;
    if config.data_dir.exists() {
        return Err(RestoreError::DestinationNotAbsent {
            path: config.data_dir.display().to_string(),
        });
    }
    fs::rename(staging, &config.data_dir)?;
    sync_parent(&config.data_dir)?;
    Ok(receipt)
}

fn import_control_section(
    db: &GroupDb,
    section: &ControlSectionV1,
    config: &RestoreConfig,
    data_groups: &[GroupId],
) -> Result<ControlAuditPlan, RestoreError> {
    let bootstrap = choose_bootstrap(section, config.target_cluster)?;
    append_membership(db, config.node_id)?;
    append_and_apply(
        db,
        1,
        config.node_id,
        GroupCommand::BootstrapControl {
            spec: bootstrap.clone(),
            topology: None,
            security: None,
            data_groups: data_groups.to_vec(),
            max_streams: config.group_pool.max_streams,
            max_partitions_per_stream: config.group_pool.max_partitions_per_stream,
        },
    )?;

    let state = db.cf(CF_STATE)?;
    let mut batch = WriteBatch::default();
    batch.put_cf(
        &state,
        KEY_CATALOG_REVISION,
        encode(&section.catalog_revision)?,
    );
    batch.put_cf(
        &state,
        KEY_ASSIGNMENT_CURSOR,
        encode(&section.assignment_cursor)?,
    );
    batch.put_cf(&state, KEY_DATA_GROUP_POOL, encode(&data_groups.to_vec())?);
    batch.put_cf(
        &state,
        KEY_MAX_STREAMS,
        encode(&config.group_pool.max_streams)?,
    );
    batch.put_cf(
        &state,
        KEY_MAX_PARTITIONS,
        encode(&config.group_pool.max_partitions_per_stream)?,
    );
    let mut digest = Sha256::new();
    hash_item(&mut digest, "catalog_revision", &section.catalog_revision)?;
    hash_item(&mut digest, "assignment_cursor", &section.assignment_cursor)?;
    hash_item(
        &mut digest,
        "preserved_data_groups",
        &section.configured_data_groups,
    )?;

    let mut audits = Vec::with_capacity(section.streams.len());
    let mut streams = section.streams.clone();
    streams.sort_by_key(|stream| stream.descriptor.stream());
    for stream in streams {
        let descriptor = rewrite_stream_descriptor(&stream.descriptor, config.target_cluster);
        batch.put_cf(
            &state,
            stream_key(descriptor.stream()),
            encode(&descriptor)?,
        );
        batch.put_cf(
            &state,
            stream_name_key(descriptor.name()),
            encode(&descriptor.stream())?,
        );
        batch.put_cf(
            &state,
            stream_bookmark_publication_key(descriptor.stream()),
            encode(&stream.bookmark_publication_ceiling.get())?,
        );
        hash_item(&mut digest, "stream", &descriptor)?;
        hash_item(
            &mut digest,
            "stream_bookmark_publication_ceiling",
            &stream.bookmark_publication_ceiling,
        )?;
        let mut bookmarks = stream.bookmarks.clone();
        bookmarks.sort_by_key(|bookmark| (bookmark.publication(), bookmark.id()));
        for bookmark in bookmarks {
            let rewritten = rewrite_stream_bookmark(&bookmark, config.target_cluster)?;
            batch.put_cf(
                &state,
                stream_bookmark_id_key(rewritten.id()),
                encode(&rewritten)?,
            );
            if rewritten.lifecycle() == BookmarkLifecycle::Available {
                batch.put_cf(
                    &state,
                    stream_bookmark_name_key(descriptor.stream(), rewritten.name()),
                    encode(&rewritten.id())?,
                );
                batch.put_cf(
                    &state,
                    stream_bookmark_order_key(descriptor.stream(), rewritten.publication()),
                    encode(&rewritten)?,
                );
            }
            hash_item(&mut digest, "stream_bookmark", &rewritten)?;
        }
        audits.push(ActiveStreamAudit {
            descriptor,
            bookmark_publication_ceiling: stream.bookmark_publication_ceiling,
        });
    }
    db.write_sync(batch)?;
    Ok(ControlAuditPlan {
        digest: digest.finalize().into(),
        bootstrap,
        streams: audits,
        preserved_groups: section.configured_data_groups.clone(),
        catalog_revision: section.catalog_revision,
        assignment_cursor: section.assignment_cursor,
    })
}

fn import_data_group(
    db: &GroupDb,
    section: &DataGroupV1,
    config: &RestoreConfig,
) -> Result<DataGroupAuditPlan, RestoreError> {
    let mut digest = Sha256::new();
    let mut audits = Vec::with_capacity(section.partitions.len());
    let mut partitions = section.partitions.clone();
    partitions.sort_by_key(|partition| (partition.stream, partition.partition));
    for partition in partitions {
        import_partition(db, &partition, config.target_cluster, &mut digest)?;
        audits.push(PartitionAudit {
            key: PartitionKey::new(partition.stream, partition.partition),
            retention_floor: partition.retention_floor,
            tail: partition.tail,
            bookmark_publication_ceiling: partition.bookmark_publication_ceiling,
        });
    }
    Ok(DataGroupAuditPlan {
        digest: digest.finalize().into(),
        partitions: audits,
    })
}

fn import_partition(
    db: &GroupDb,
    partition: &PartitionV1,
    target_cluster: ClusterId,
    digest: &mut Sha256,
) -> Result<(), RestoreError> {
    let key = PartitionKey::new(partition.stream, partition.partition);
    hash_item(
        digest,
        "partition",
        &(key, partition.retention_floor, partition.tail),
    )?;
    hash_item(
        digest,
        "partition_bookmark_publication_ceiling",
        &partition.bookmark_publication_ceiling,
    )?;
    let state = db.cf(CF_STATE)?;
    let payload = db.cf(CF_PAYLOAD)?;
    let mut batch = WriteBatch::default();
    let mut next_byte_position = 0_u64;
    let mut records_in_batch = 0_usize;
    let mut records = partition.records.clone();
    records.sort_by_key(CommittedRecord::offset);
    for record in records {
        let payload_key = restore_payload_id(key, record.offset());
        let payload_bytes = u64::try_from(record.payload().len()).map_err(io::Error::other)?;
        next_byte_position = next_byte_position
            .checked_add(payload_bytes)
            .ok_or_else(|| io::Error::other("restored partition byte position overflow"))?;
        batch.put_cf(
            &state,
            record_key(key, record.offset().get()),
            encode(&StoredRecord {
                payload_key: payload_key.clone(),
                payload_bytes,
                cumulative_end_bytes: next_byte_position,
            })?,
        );
        batch.put_cf(
            &payload,
            payload_bytes_key(&payload_key),
            encode_payload_value(record.payload()),
        );
        batch.put_cf(
            &payload,
            payload_owners_key(&payload_key),
            encode(&PayloadOwners {
                raft_log: false,
                applied_state: false,
                applied_banks: StateBank::A.bit(),
            })?,
        );
        hash_item(digest, "record", &record)?;
        records_in_batch += 1;
        if records_in_batch >= IMPORT_BATCH_RECORDS {
            db.write_sync(batch)?;
            batch = WriteBatch::default();
            records_in_batch = 0;
        }
    }
    batch.put_cf(&state, next_offset_key(key), encode(&partition.tail.get())?);
    batch.put_cf(
        &state,
        retention_key(key),
        encode(&PartitionRetentionState {
            logical_floor: partition.retention_floor.get(),
            reclaim_cursor: partition.retention_floor.get(),
            logically_expired_bytes: 0,
            raft_only_bytes: 0,
            next_byte_position,
            floor_byte_position: 0,
        })?,
    );
    batch.put_cf(
        &state,
        bookmark_publication_key(key),
        encode(&partition.bookmark_publication_ceiling.get())?,
    );
    let mut bookmarks = partition.bookmarks.clone();
    bookmarks.sort_by_key(|bookmark| (bookmark.publication(), bookmark.id()));
    for bookmark in bookmarks {
        let rewritten = rewrite_bookmark(&bookmark, target_cluster);
        batch.put_cf(&state, bookmark_id_key(rewritten.id()), encode(&rewritten)?);
        if rewritten.lifecycle() == BookmarkLifecycle::Available {
            batch.put_cf(
                &state,
                bookmark_name_key(key, rewritten.name()),
                encode(&rewritten.id())?,
            );
            batch.put_cf(
                &state,
                bookmark_order_key(key, rewritten.publication()),
                encode(&rewritten)?,
            );
        }
        hash_item(digest, "partition_bookmark", &rewritten)?;
    }
    db.write_sync(batch)?;
    Ok(())
}

fn append_and_apply(
    db: &GroupDb,
    index: u64,
    node_id: NodeId,
    command: GroupCommand,
) -> Result<(), RestoreError> {
    let entry = GroupEntry {
        log_id: GroupLogId {
            leader_id: crate::GroupLeaderId {
                term: 1,
                node_id: node_id.get(),
            },
            index,
        },
        payload: EntryPayload::Normal(command),
    };
    append_log_entry(db, entry.clone())?;
    match db.apply_entry(entry)? {
        ApplyResult::Bootstrapped(_) => Ok(()),
        ApplyResult::Rejected(error) => Err(RestoreError::BootstrapRejected(error)),
        other => Err(RestoreError::AuditMismatch {
            scope: "bootstrap".to_owned(),
            reason: format!("unexpected apply result {other}"),
        }),
    }
}

fn append_log_entry(db: &GroupDb, entry: GroupEntry) -> Result<(), RestoreError> {
    let (thin, payloads) = thin_entry(entry)?;
    let log = db.cf(CF_RAFT_LOG)?;
    let payload = db.cf(CF_PAYLOAD)?;
    let mut batch = WriteBatch::default();
    for (key, bytes) in payloads {
        batch.put_cf(
            &payload,
            payload_bytes_key(&key),
            encode_payload_value(&bytes),
        );
        batch.put_cf(
            &payload,
            payload_owners_key(&key),
            encode(&PayloadOwners {
                raft_log: true,
                applied_state: false,
                applied_banks: 0,
            })?,
        );
    }
    batch.put_cf(&log, thin.log_id.index.to_be_bytes(), encode(&thin)?);
    db.write_sync(batch)?;
    Ok(())
}

fn choose_bootstrap(
    section: &ControlSectionV1,
    target_cluster: ClusterId,
) -> Result<BootstrapSpec, RestoreError> {
    let stream = section
        .streams
        .iter()
        .min_by_key(|stream| stream.descriptor.stream())
        .ok_or(RestoreError::EmptyExport)?;
    Ok(BootstrapSpec::new(
        target_cluster,
        stream.descriptor.stream(),
        stream.descriptor.name().clone(),
    ))
}

fn rewrite_stream_descriptor(
    source: &StreamDescriptor,
    target_cluster: ClusterId,
) -> StreamDescriptor {
    StreamDescriptor::new(
        target_cluster,
        source.stream(),
        source.name().clone(),
        source.lifecycle(),
        source.placements().to_vec(),
        source.ready_groups().to_vec(),
        source.revision(),
    )
}

fn rewrite_bookmark(source: &CommittedBookmark, target_cluster: ClusterId) -> CommittedBookmark {
    let mut bookmark = CommittedBookmark::published(
        source.id(),
        source.name().clone(),
        CommittedCursor::new(
            target_cluster,
            source.cursor().partition(),
            source.cursor().next_offset(),
        ),
        source.publication(),
    );
    if source.lifecycle() == BookmarkLifecycle::Deleted {
        bookmark.mark_deleted();
    }
    bookmark
}

fn rewrite_stream_bookmark(
    source: &CommittedStreamBookmark,
    target_cluster: ClusterId,
) -> Result<CommittedStreamBookmark, RestoreError> {
    let positions = source
        .vector()
        .positions()
        .iter()
        .map(|cursor| {
            CommittedCursor::new(target_cluster, cursor.partition(), cursor.next_offset())
        })
        .collect::<Vec<_>>();
    let vector = StreamCursorVector::new(source.vector().stream(), positions)?;
    let mut bookmark = CommittedStreamBookmark::published(
        source.id(),
        source.name().clone(),
        vector,
        source.publication(),
    );
    if source.lifecycle() == BookmarkLifecycle::Deleted {
        bookmark.mark_deleted();
    }
    Ok(bookmark)
}

fn restore_payload_id(partition: PartitionKey, offset: RecordOffset) -> Vec<u8> {
    let mut id = Vec::with_capacity(7 + 16 + 4 + 8);
    id.extend_from_slice(b"restore");
    id.extend_from_slice(partition.stream().as_uuid().as_bytes());
    id.extend_from_slice(&partition.partition().get().to_be_bytes());
    id.extend_from_slice(&offset.get().to_be_bytes());
    id
}

fn audit_reopened(
    staging: &Path,
    config: &RestoreConfig,
    data_groups: &[GroupId],
    expected_control: &ControlAuditPlan,
    expected_data: &BTreeMap<GroupId, DataGroupAuditPlan>,
) -> Result<(), RestoreError> {
    let budget = config.group_pool.per_group_budget()?;
    let control_group = GroupId::new(CONTROL_GROUP_ID)?;
    let control = open_control_store(
        &group_path(staging, control_group),
        &GroupIdentity::new(config.target_cluster, control_group, GroupKind::Control),
        config.receipt_window,
        budget,
    )?;
    let actual_control = digest_control(&control.reader.db, expected_control)?;
    if actual_control != expected_control.digest {
        return Err(RestoreError::AuditMismatch {
            scope: "control".to_owned(),
            reason: "logical digest differs".to_owned(),
        });
    }
    drop(control);

    for group in data_groups {
        let data = open_data_store(
            &group_path(staging, *group),
            &GroupIdentity::new(config.target_cluster, *group, GroupKind::Data),
            config.receipt_window,
            budget,
        )?;
        if let Some(expected) = expected_data.get(group) {
            let actual = digest_data_group(&data.reader.db, expected)?;
            if actual != expected.digest {
                return Err(RestoreError::AuditMismatch {
                    scope: format!("data group {group}"),
                    reason: "logical digest differs".to_owned(),
                });
            }
        } else if !data
            .reader
            .retention_partitions()
            .map_err(domain_storage)?
            .is_empty()
        {
            return Err(RestoreError::AuditMismatch {
                scope: format!("data group {group}"),
                reason: "unexpected logical partitions in unexported group".to_owned(),
            });
        }
        drop(data);
    }
    Ok(())
}

fn digest_control(db: &GroupDb, expected: &ControlAuditPlan) -> Result<[u8; 32], RestoreError> {
    let mut digest = Sha256::new();
    let catalog_revision: u64 = required_db_value(db, CF_STATE, KEY_CATALOG_REVISION)?;
    let assignment_cursor: u64 = required_db_value(db, CF_STATE, KEY_ASSIGNMENT_CURSOR)?;
    if catalog_revision != expected.catalog_revision
        || assignment_cursor != expected.assignment_cursor
    {
        return Err(RestoreError::AuditMismatch {
            scope: "control".to_owned(),
            reason: "catalog cursor or revision differs".to_owned(),
        });
    }
    let stored_groups: Vec<GroupId> = required_db_value(db, CF_STATE, KEY_DATA_GROUP_POOL)?;
    for group in &expected.preserved_groups {
        if !stored_groups.contains(group) {
            return Err(RestoreError::AuditMismatch {
                scope: "control".to_owned(),
                reason: format!("preserved group {group} is absent from target pool"),
            });
        }
    }
    hash_item(&mut digest, "catalog_revision", &catalog_revision)?;
    hash_item(&mut digest, "assignment_cursor", &assignment_cursor)?;
    hash_item(
        &mut digest,
        "preserved_data_groups",
        &expected.preserved_groups,
    )?;
    for stream in &expected.streams {
        let stored: StreamDescriptor =
            required_db_value(db, CF_STATE, &stream_key(stream.descriptor.stream()))?;
        hash_item(&mut digest, "stream", &stored)?;
        let ceiling = BookmarkPublicationSequence::new(
            db.get::<u64>(
                CF_STATE,
                &stream_bookmark_publication_key(stream.descriptor.stream()),
            )?
            .unwrap_or_default(),
        );
        if ceiling != stream.bookmark_publication_ceiling {
            return Err(RestoreError::AuditMismatch {
                scope: "control".to_owned(),
                reason: "stream bookmark ceiling differs".to_owned(),
            });
        }
        hash_item(&mut digest, "stream_bookmark_publication_ceiling", &ceiling)?;
        for bookmark in stream_bookmarks_for(db, stream.descriptor.stream())? {
            hash_item(&mut digest, "stream_bookmark", &bookmark)?;
        }
    }
    Ok(digest.finalize().into())
}

fn digest_data_group(
    db: &GroupDb,
    expected: &DataGroupAuditPlan,
) -> Result<[u8; 32], RestoreError> {
    let mut digest = Sha256::new();
    for partition in &expected.partitions {
        hash_item(
            &mut digest,
            "partition",
            &(partition.key, partition.retention_floor, partition.tail),
        )?;
        let retention: PartitionRetentionState =
            required_db_value(db, CF_STATE, &retention_key(partition.key))?;
        let tail: u64 = required_db_value(db, CF_STATE, &next_offset_key(partition.key))?;
        if retention.logical_floor != partition.retention_floor.get()
            || tail != partition.tail.get()
        {
            return Err(RestoreError::AuditMismatch {
                scope: "data".to_owned(),
                reason: "partition floor or tail differs".to_owned(),
            });
        }
        let ceiling = BookmarkPublicationSequence::new(
            db.get::<u64>(CF_STATE, &bookmark_publication_key(partition.key))?
                .unwrap_or_default(),
        );
        if ceiling != partition.bookmark_publication_ceiling {
            return Err(RestoreError::AuditMismatch {
                scope: "data".to_owned(),
                reason: "partition bookmark ceiling differs".to_owned(),
            });
        }
        hash_item(
            &mut digest,
            "partition_bookmark_publication_ceiling",
            &ceiling,
        )?;
        for offset in partition.retention_floor.get()..partition.tail.get() {
            hash_item(
                &mut digest,
                "record",
                &read_record(db, partition.key, offset)?,
            )?;
        }
        for bookmark in partition_bookmarks_for(db, partition.key)? {
            hash_item(&mut digest, "partition_bookmark", &bookmark)?;
        }
    }
    Ok(digest.finalize().into())
}

fn read_record(
    db: &GroupDb,
    partition: PartitionKey,
    offset: u64,
) -> Result<CommittedRecord, RestoreError> {
    let stored: StoredRecord = required_db_value(db, CF_STATE, &record_key(partition, offset))?;
    let payload = db
        .db
        .get_cf(&db.cf(CF_PAYLOAD)?, payload_bytes_key(&stored.payload_key))
        .map_err(io::Error::other)?
        .ok_or_else(|| io::Error::other("restored payload missing"))?;
    Ok(CommittedRecord::new(
        RecordOffset::new(offset),
        decode_payload_value(&payload)?,
    ))
}

fn stream_bookmarks_for(
    db: &GroupDb,
    stream: StreamId,
) -> Result<Vec<CommittedStreamBookmark>, RestoreError> {
    let state = db.cf(CF_STATE)?;
    let mut values = Vec::new();
    for item in db.db.iterator_cf(
        &state,
        IteratorMode::From(STREAM_BOOKMARK_ID_PREFIX, Direction::Forward),
    ) {
        let (key, value) = item.map_err(io::Error::other)?;
        if !key.starts_with(STREAM_BOOKMARK_ID_PREFIX) {
            break;
        }
        let bookmark: CommittedStreamBookmark = decode(&value)?;
        if key.as_ref() == stream_bookmark_id_key(bookmark.id()).as_slice()
            && bookmark.vector().stream() == stream
        {
            values.push(bookmark);
        }
    }
    values.sort_by_key(|bookmark| (bookmark.publication(), bookmark.id()));
    Ok(values)
}

fn partition_bookmarks_for(
    db: &GroupDb,
    partition: PartitionKey,
) -> Result<Vec<CommittedBookmark>, RestoreError> {
    let state = db.cf(CF_STATE)?;
    let mut values = Vec::new();
    for item in db.db.iterator_cf(
        &state,
        IteratorMode::From(BOOKMARK_ID_PREFIX, Direction::Forward),
    ) {
        let (key, value) = item.map_err(io::Error::other)?;
        if !key.starts_with(BOOKMARK_ID_PREFIX) {
            break;
        }
        let bookmark: CommittedBookmark = decode(&value)?;
        if key.as_ref() == bookmark_id_key(bookmark.id()).as_slice()
            && bookmark.cursor().partition() == partition
        {
            values.push(bookmark);
        }
    }
    values.sort_by_key(|bookmark| (bookmark.publication(), bookmark.id()));
    Ok(values)
}

fn required_db_value<T: DeserializeOwned>(
    db: &GroupDb,
    cf: &str,
    key: &[u8],
) -> Result<T, RestoreError> {
    db.get(cf, key)?.ok_or_else(|| RestoreError::AuditMismatch {
        scope: "storage".to_owned(),
        reason: format!("required key {:?} is missing", key),
    })
}

fn hash_item<T: Serialize>(
    digest: &mut Sha256,
    label: &str,
    value: &T,
) -> Result<(), RestoreError> {
    digest.update(label.as_bytes());
    digest.update([0]);
    let bytes = serde_json::to_vec(value).map_err(io::Error::other)?;
    digest.update((bytes.len() as u64).to_be_bytes());
    digest.update(bytes);
    Ok(())
}

fn matching_existing_receipt(
    destination: &Path,
    artifact: ArtifactIdentity,
    manifest: &ExportManifestV1,
    config: &RestoreConfig,
) -> Result<Option<RestoreReceipt>, RestoreError> {
    let path = destination.join(RESTORE_FILE);
    if !path.is_file() {
        return Ok(None);
    }
    let record: RestoreRecord =
        serde_json::from_slice(&fs::read(&path)?).map_err(io::Error::other)?;
    let receipt = record.receipt;
    if receipt.artifact == artifact
        && receipt.source_cluster == manifest.source_cluster
        && receipt.target_cluster == config.target_cluster
        && receipt.target_group_pool == config.group_pool
        && receipt.serve_config_digest == config.serve_config_digest
    {
        Ok(Some(receipt))
    } else {
        Ok(None)
    }
}

fn write_restore_record(staging: &Path, receipt: &RestoreReceipt) -> Result<(), RestoreError> {
    let path = staging.join(RESTORE_FILE);
    let temporary = staging.join(format!(".{RESTORE_FILE}.{}.tmp", Uuid::new_v4()));
    let bytes = serde_json::to_vec_pretty(&RestoreRecord {
        receipt: receipt.clone(),
    })
    .map_err(io::Error::other)?;
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&temporary, &path)?;
    sync_directory(staging)?;
    Ok(())
}

fn staging_path(destination: &Path) -> Result<PathBuf, RestoreError> {
    let parent = destination
        .parent()
        .ok_or_else(|| io::Error::other("restore destination has no parent"))?;
    let name = destination
        .file_name()
        .ok_or_else(|| io::Error::other("restore destination has no final component"))?
        .to_string_lossy();
    Ok(parent.join(format!("{name}.restore-staging-{}", Uuid::new_v4())))
}

fn group_path(root: &Path, group: GroupId) -> PathBuf {
    root.join("groups").join(group.get().to_string())
}

fn flush_group(db: &GroupDb) -> Result<(), RestoreError> {
    db.db.flush_wal(true).map_err(io::Error::other)?;
    db.db.flush().map_err(io::Error::other)?;
    sync_directory(&db.group_root)?;
    sync_directory(&db.group_root.join("rocksdb"))?;
    Ok(())
}

fn sync_tree(root: &Path) -> Result<(), RestoreError> {
    for entry in fs::read_dir(root)? {
        let path = entry?.path();
        if path.is_dir() {
            sync_tree(&path)?;
        } else if path.is_file() {
            match File::open(&path).and_then(|file| file.sync_all()) {
                Ok(()) => {}
                Err(error) if cfg!(windows) && error.kind() == io::ErrorKind::PermissionDenied => {}
                Err(error) => {
                    return Err(io::Error::new(
                        error.kind(),
                        format!("{}: {error}", path.display()),
                    )
                    .into());
                }
            }
        }
    }
    sync_directory(root)
}

fn sync_parent(path: &Path) -> Result<(), RestoreError> {
    if let Some(parent) = path.parent() {
        sync_directory(parent)?;
    }
    Ok(())
}

fn sync_directory(path: &Path) -> Result<(), RestoreError> {
    match open_directory(path).and_then(|file| file.sync_all()) {
        Ok(()) => {}
        Err(error) if cfg!(windows) && error.kind() == io::ErrorKind::PermissionDenied => {}
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

#[cfg(windows)]
fn open_directory(path: &Path) -> io::Result<File> {
    use std::os::windows::fs::OpenOptionsExt;

    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)
}

#[cfg(not(windows))]
fn open_directory(path: &Path) -> io::Result<File> {
    File::open(path)
}

fn domain_storage(error: light_stream_core::DomainError) -> RestoreError {
    RestoreError::AuditMismatch {
        scope: "storage".to_owned(),
        reason: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        fs,
        io::{self, Cursor},
        path::{Path, PathBuf},
        sync::atomic::{AtomicU64, Ordering},
    };

    use light_stream_core::{
        BookmarkId, BookmarkName, BookmarkPublicationSequence, CommittedCursor, GroupCut,
        PartitionId, PartitionPlacement, QuiescentCut, StreamLifecycle, StreamName,
    };
    use light_stream_export::{
        ActiveStreamV1, ControlSectionV1, DataGroupSourceV1, DataGroupV1, ExportDocumentV1,
        ExportExclusionsV1, ExportIdV1, ExportLimits, PartitionV1, write_v1,
    };

    use super::*;
    use crate::{DEFAULT_RECEIPT_WINDOW, MAX_FETCH_RECORDS};

    static TEST_ID: AtomicU64 = AtomicU64::new(1);

    struct ProjectTestDir(PathBuf);

    impl ProjectTestDir {
        fn new(label: &str) -> Self {
            let id = TEST_ID.fetch_add(1, Ordering::Relaxed);
            let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            let workspace = manifest_dir
                .parent()
                .and_then(Path::parent)
                .expect("crate lives under workspace/crates");
            let path = workspace
                .join("target/test-data/light-stream-storage-logical-restore")
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

    struct Source {
        groups: BTreeMap<GroupId, DataGroupV1>,
    }

    impl DataGroupSourceV1 for Source {
        type Error = io::Error;

        fn data_group(
            &mut self,
            group: GroupId,
            _cut: GroupCut,
        ) -> Result<Option<DataGroupV1>, Self::Error> {
            Ok(self.groups.get(&group).cloned())
        }
    }

    struct ArtifactFixture {
        bytes: Vec<u8>,
        source_cluster: ClusterId,
        target_cluster: ClusterId,
        stream: StreamId,
        partition: PartitionKey,
        data_group: GroupId,
        active_partition_bookmark: BookmarkId,
        deleted_partition_bookmark: BookmarkId,
        active_stream_bookmark: BookmarkId,
        deleted_stream_bookmark: BookmarkId,
    }

    fn fixture_artifact(configured_groups: Vec<GroupId>) -> ArtifactFixture {
        let source_cluster = ClusterId::from_uuid(Uuid::from_u128(0x9001));
        let target_cluster = ClusterId::from_uuid(Uuid::from_u128(0x9002));
        let stream = StreamId::from_uuid(Uuid::from_u128(0x9003));
        let data_group = configured_groups[0];
        let partition = PartitionKey::new(stream, PartitionId::new(0));
        let active_partition_bookmark = BookmarkId::from_uuid(Uuid::from_u128(0x9004));
        let deleted_partition_bookmark = BookmarkId::from_uuid(Uuid::from_u128(0x9005));
        let active_stream_bookmark = BookmarkId::from_uuid(Uuid::from_u128(0x9006));
        let deleted_stream_bookmark = BookmarkId::from_uuid(Uuid::from_u128(0x9007));
        let cut = QuiescentCut::try_new(
            GroupCut::new(
                GroupId::new(CONTROL_GROUP_ID).unwrap(),
                1,
                NodeId::new(1).unwrap(),
                11,
            ),
            [GroupCut::new(data_group, 1, NodeId::new(1).unwrap(), 7)],
        )
        .unwrap();
        let descriptor = StreamDescriptor::new(
            source_cluster,
            stream,
            StreamName::parse("restored").unwrap(),
            StreamLifecycle::Active,
            vec![PartitionPlacement::new(PartitionId::new(0), data_group)],
            vec![data_group],
            5,
        );
        let vector = || {
            StreamCursorVector::new(
                stream,
                vec![CommittedCursor::new(
                    source_cluster,
                    partition,
                    RecordOffset::new(4),
                )],
            )
            .unwrap()
        };
        let mut deleted_stream = CommittedStreamBookmark::published(
            deleted_stream_bookmark,
            BookmarkName::parse("deleted-stream").unwrap(),
            vector(),
            BookmarkPublicationSequence::new(2),
        );
        deleted_stream.mark_deleted();
        let control = ControlSectionV1 {
            source_cluster,
            export_id: ExportIdV1::from_bytes(*Uuid::from_u128(0x9008).as_bytes()),
            cut: cut.control(),
            catalog_revision: 5,
            assignment_cursor: 3,
            max_streams: 16,
            max_partitions_per_stream: 8,
            configured_data_groups: configured_groups.clone(),
            streams: vec![ActiveStreamV1 {
                descriptor,
                bookmark_publication_ceiling: BookmarkPublicationSequence::new(2),
                bookmarks: vec![
                    CommittedStreamBookmark::published(
                        active_stream_bookmark,
                        BookmarkName::parse("active-stream").unwrap(),
                        vector(),
                        BookmarkPublicationSequence::new(1),
                    ),
                    deleted_stream,
                ],
            }],
        };
        let mut deleted_partition = CommittedBookmark::published(
            deleted_partition_bookmark,
            BookmarkName::parse("deleted-partition").unwrap(),
            CommittedCursor::new(source_cluster, partition, RecordOffset::new(3)),
            BookmarkPublicationSequence::new(2),
        );
        deleted_partition.mark_deleted();
        let data = DataGroupV1 {
            source_cluster,
            export_id: control.export_id,
            group: data_group,
            cut: *cut.data().get(&data_group).unwrap(),
            partitions: vec![PartitionV1 {
                source_cluster,
                stream,
                partition: PartitionId::new(0),
                retention_floor: RecordOffset::new(2),
                tail: RecordOffset::new(4),
                bookmark_publication_ceiling: BookmarkPublicationSequence::new(2),
                records: vec![
                    CommittedRecord::new(RecordOffset::new(2), b"two".to_vec()),
                    CommittedRecord::new(RecordOffset::new(3), b"three".to_vec()),
                ],
                bookmarks: vec![
                    CommittedBookmark::published(
                        active_partition_bookmark,
                        BookmarkName::parse("active-partition").unwrap(),
                        CommittedCursor::new(source_cluster, partition, RecordOffset::new(4)),
                        BookmarkPublicationSequence::new(1),
                    ),
                    deleted_partition,
                ],
            }],
        };
        let document = ExportDocumentV1 {
            source_cluster,
            export_id: control.export_id,
            selected_streams: vec![stream],
            cut,
            control,
            required_features: 0,
            exclusions: ExportExclusionsV1::v1(),
        };
        let mut source = Source {
            groups: [(data_group, data)].into_iter().collect(),
        };
        let mut cursor = Cursor::new(Vec::new());
        write_v1(
            &mut cursor,
            &document,
            &mut source,
            &ExportLimits::default(),
        )
        .unwrap();
        ArtifactFixture {
            bytes: cursor.into_inner(),
            source_cluster,
            target_cluster,
            stream,
            partition,
            data_group,
            active_partition_bookmark,
            deleted_partition_bookmark,
            active_stream_bookmark,
            deleted_stream_bookmark,
        }
    }

    fn config(root: &Path, target_cluster: ClusterId, max_data_groups: u16) -> RestoreConfig {
        RestoreConfig {
            target_cluster,
            node_id: NodeId::new(1).unwrap(),
            data_dir: root.join("restored"),
            group_pool: RestoreGroupPoolConfig {
                max_data_groups,
                max_streams: 16,
                max_partitions_per_stream: 8,
                rocksdb_cache_bytes: 8 * 1024 * 1024 * (usize::from(max_data_groups) + 1),
                rocksdb_write_buffer_bytes: 4 * 1024 * 1024 * (usize::from(max_data_groups) + 1),
            },
            receipt_window: DEFAULT_RECEIPT_WINDOW,
            serve_config_digest: Some([7; 32]),
        }
    }

    #[test]
    fn restore_rewrites_cluster_and_preserves_logical_ledger() {
        let directory = ProjectTestDir::new("success");
        let fixture = fixture_artifact(vec![GroupId::new(DATA_GROUP_ID).unwrap()]);
        let verified =
            verify(Cursor::new(fixture.bytes.clone()), &ExportLimits::default()).unwrap();
        let receipt =
            restore_from_verified(verified, config(&directory.0, fixture.target_cluster, 1))
                .unwrap();
        assert_eq!(receipt.source_cluster, fixture.source_cluster);
        assert_eq!(receipt.target_cluster, fixture.target_cluster);
        assert!(directory.0.join("restored").join(RESTORE_FILE).is_file());

        let budget = receipt.target_group_pool.per_group_budget().unwrap();
        let control_group = GroupId::new(CONTROL_GROUP_ID).unwrap();
        let control = open_control_store(
            &group_path(&directory.0.join("restored"), control_group),
            &GroupIdentity::new(fixture.target_cluster, control_group, GroupKind::Control),
            DEFAULT_RECEIPT_WINDOW,
            budget,
        )
        .unwrap();
        let descriptor = control
            .reader
            .stream_by_id(fixture.stream)
            .unwrap()
            .unwrap();
        assert_eq!(descriptor.cluster(), fixture.target_cluster);
        let stream_bookmark = control
            .reader
            .stream_bookmark_by_id(fixture.stream, fixture.active_stream_bookmark)
            .unwrap();
        assert_eq!(stream_bookmark.vector().cluster(), fixture.target_cluster);
        let deleted_stream = control
            .reader
            .stream_bookmark_by_id(fixture.stream, fixture.deleted_stream_bookmark)
            .unwrap();
        assert_eq!(deleted_stream.lifecycle(), BookmarkLifecycle::Deleted);
        drop(control);

        let data = open_data_store(
            &group_path(&directory.0.join("restored"), fixture.data_group),
            &GroupIdentity::new(fixture.target_cluster, fixture.data_group, GroupKind::Data),
            DEFAULT_RECEIPT_WINDOW,
            budget,
        )
        .unwrap();
        let page = data
            .reader
            .fetch(
                fixture.target_cluster,
                fixture.partition,
                RecordOffset::new(2),
                MAX_FETCH_RECORDS,
            )
            .unwrap();
        assert_eq!(
            page.records()
                .iter()
                .map(|record| (record.offset().get(), record.payload().to_vec()))
                .collect::<Vec<_>>(),
            vec![(2, b"two".to_vec()), (3, b"three".to_vec())]
        );
        assert_eq!(
            data.reader.partition_tail(fixture.partition).unwrap(),
            RecordOffset::new(4)
        );
        assert_eq!(
            data.reader
                .retention_status(fixture.partition)
                .unwrap()
                .logical_floor(),
            RecordOffset::new(2)
        );
        let bookmark = data
            .reader
            .bookmark_by_id(fixture.partition, fixture.active_partition_bookmark)
            .unwrap();
        assert_eq!(bookmark.cursor().cluster(), fixture.target_cluster);
        let deleted = data
            .reader
            .bookmark_by_id(fixture.partition, fixture.deleted_partition_bookmark)
            .unwrap();
        assert_eq!(deleted.lifecycle(), BookmarkLifecycle::Deleted);
    }

    #[test]
    fn restore_rejects_existing_destination() {
        let directory = ProjectTestDir::new("exists");
        let fixture = fixture_artifact(vec![GroupId::new(DATA_GROUP_ID).unwrap()]);
        let cfg = config(&directory.0, fixture.target_cluster, 1);
        fs::create_dir_all(&cfg.data_dir).unwrap();
        let verified = verify(Cursor::new(fixture.bytes), &ExportLimits::default()).unwrap();
        assert!(matches!(
            restore_from_verified(verified, cfg),
            Err(RestoreError::DestinationExists { .. })
        ));
    }

    #[test]
    fn restore_rejects_same_cluster() {
        let directory = ProjectTestDir::new("same-cluster");
        let fixture = fixture_artifact(vec![GroupId::new(DATA_GROUP_ID).unwrap()]);
        let verified = verify(Cursor::new(fixture.bytes), &ExportLimits::default()).unwrap();
        assert!(matches!(
            restore_from_verified(verified, config(&directory.0, fixture.source_cluster, 1)),
            Err(RestoreError::SameCluster(_))
        ));
    }

    #[test]
    fn restore_rejects_missing_target_group() {
        let directory = ProjectTestDir::new("missing-group");
        let fixture = fixture_artifact(vec![GroupId::new(DATA_GROUP_ID + 1).unwrap()]);
        let verified = verify(Cursor::new(fixture.bytes), &ExportLimits::default()).unwrap();
        assert!(matches!(
            restore_from_verified(verified, config(&directory.0, fixture.target_cluster, 1)),
            Err(RestoreError::MissingGroup(_))
        ));
        assert!(!directory.0.join("restored").exists());
    }

    #[test]
    fn restore_from_path_rejects_corrupt_artifact_without_destination() {
        let directory = ProjectTestDir::new("corrupt");
        let input = directory.0.join("corrupt.lsexport");
        fs::write(&input, b"not an export").unwrap();
        let fixture = fixture_artifact(vec![GroupId::new(DATA_GROUP_ID).unwrap()]);
        assert!(matches!(
            restore_from_path(
                &input,
                &ExportLimits::default(),
                config(&directory.0, fixture.target_cluster, 1),
            ),
            Err(RestoreError::Verify(_))
        ));
        assert!(!directory.0.join("restored").exists());
    }
}

mod exclusions_serde {
    use light_stream_export::ExportExclusionsV1;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &ExportExclusionsV1, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(value.bits())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<ExportExclusionsV1, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(ExportExclusionsV1::from_bits(u64::deserialize(
            deserializer,
        )?))
    }
}
