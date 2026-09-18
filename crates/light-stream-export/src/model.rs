use std::error::Error;

use light_stream_core::{
    ArtifactIdentity, BookmarkPublicationSequence, ClusterId, CommittedBookmark, CommittedRecord,
    CommittedStreamBookmark, ExportId, GroupCut, GroupId, PartitionId, QuiescentCut, RecordOffset,
    StreamDescriptor, StreamId,
};

pub const MAGIC_V1: [u8; 8] = *b"LSEXPT01";
pub const TRAILER_MAGIC_V1: [u8; 8] = *b"LSEXEND1";
pub const FORMAT_VERSION_V1: u32 = 1;
pub const REQUIRED_FEATURES_V1: u64 = 0;
pub const SECTION_KIND_CONTROL_V1: u16 = 1;
pub const SECTION_KIND_DATA_GROUP_V1: u16 = 2;
pub const CONTROL_SECTION_VERSION_V1: u16 = 2;
pub const DATA_SECTION_VERSION_V1: u16 = 1;
pub const STREAM_LIFECYCLE_ACTIVE_V1: u8 = 1;
pub const BOOKMARK_LIFECYCLE_ACTIVE_V1: u8 = 1;
pub const BOOKMARK_LIFECYCLE_DELETED_V1: u8 = 2;
pub const OPTION_NONE_V1: u8 = 0;
pub const OPTION_SOME_V1: u8 = 1;
pub const TRAILER_BYTES_V1: u64 = 56;

pub const EXCLUSION_RAFT_LOGS_AND_SNAPSHOTS_V1: u64 = 1 << 0;
pub const EXCLUSION_NODE_MEMBERSHIP_ENDPOINTS_ADMIN_HISTORY_V1: u64 = 1 << 1;
pub const EXCLUSION_PRODUCER_SESSIONS_AND_RECEIPTS_V1: u64 = 1 << 2;
pub const EXCLUSION_CONSUMER_CHECKPOINTS_V1: u64 = 1 << 3;
pub const EXCLUSION_REPLAY_LEASES_AND_MAINTENANCE_CURSORS_V1: u64 = 1 << 4;
pub const EXCLUSION_SECURITY_POLICY_CREDENTIALS_CERTIFICATES_KEYS_TOKENS_V1: u64 = 1 << 5;
pub const EXCLUSION_UNSELECTED_STREAMS_V1: u64 = 1 << 6;
pub const EXCLUSION_DELETED_STREAM_CATALOG_ENTRIES_V1: u64 = 1 << 7;
pub const EXCLUSIONS_V1: u64 = (1 << 8) - 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExportExclusionsV1(u64);

impl ExportExclusionsV1 {
    pub const fn v1() -> Self {
        Self(EXCLUSIONS_V1)
    }

    pub const fn from_bits(bits: u64) -> Self {
        Self(bits)
    }

    pub const fn bits(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ExportIdV1([u8; 16]);

impl ExportIdV1 {
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl From<ExportId> for ExportIdV1 {
    fn from(value: ExportId) -> Self {
        Self(*value.as_uuid().as_bytes())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExportLimits {
    pub max_artifact_bytes: u64,
    pub max_manifest_bytes: u64,
    pub max_section_bytes: u64,
    pub max_streams: u64,
    pub max_partitions: u64,
    pub max_records: u64,
    pub max_bookmarks: u64,
    pub max_payload_bytes: u64,
}

impl Default for ExportLimits {
    fn default() -> Self {
        Self {
            max_artifact_bytes: 100 * 1024 * 1024 * 1024,
            max_manifest_bytes: 16 * 1024 * 1024,
            max_section_bytes: 1024 * 1024 * 1024,
            max_streams: 1_024,
            max_partitions: 131_072,
            max_records: 100_000_000,
            max_bookmarks: 10_000_000,
            max_payload_bytes: 100 * 1024 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExportDocumentV1 {
    pub source_cluster: ClusterId,
    pub export_id: ExportIdV1,
    pub selected_streams: Vec<StreamId>,
    pub cut: QuiescentCut,
    pub control: ControlSectionV1,
    pub required_features: u64,
    pub exclusions: ExportExclusionsV1,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControlSectionV1 {
    pub source_cluster: ClusterId,
    pub export_id: ExportIdV1,
    pub cut: GroupCut,
    pub catalog_revision: u64,
    pub assignment_cursor: u64,
    pub max_streams: u32,
    pub max_partitions_per_stream: u32,
    pub configured_data_groups: Vec<GroupId>,
    pub streams: Vec<ActiveStreamV1>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActiveStreamV1 {
    pub descriptor: StreamDescriptor,
    pub bookmark_publication_ceiling: BookmarkPublicationSequence,
    pub bookmarks: Vec<CommittedStreamBookmark>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataGroupV1 {
    pub source_cluster: ClusterId,
    pub export_id: ExportIdV1,
    pub group: GroupId,
    pub cut: GroupCut,
    pub partitions: Vec<PartitionV1>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartitionV1 {
    pub source_cluster: ClusterId,
    pub stream: StreamId,
    pub partition: PartitionId,
    pub retention_floor: RecordOffset,
    pub tail: RecordOffset,
    pub bookmark_publication_ceiling: BookmarkPublicationSequence,
    pub records: Vec<CommittedRecord>,
    pub bookmarks: Vec<CommittedBookmark>,
}

pub trait DataGroupSourceV1 {
    type Error: Error + Send + Sync + 'static;

    fn data_group(
        &mut self,
        group: GroupId,
        cut: GroupCut,
    ) -> Result<Option<DataGroupV1>, Self::Error>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SectionKindV1 {
    Control,
    DataGroup,
}

impl SectionKindV1 {
    pub const fn number(self) -> u16 {
        match self {
            Self::Control => SECTION_KIND_CONTROL_V1,
            Self::DataGroup => SECTION_KIND_DATA_GROUP_V1,
        }
    }

    pub const fn version(self) -> u16 {
        match self {
            Self::Control => CONTROL_SECTION_VERSION_V1,
            Self::DataGroup => DATA_SECTION_VERSION_V1,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SectionDescriptorV1 {
    pub ordinal: u64,
    pub kind: SectionKindV1,
    pub section_version: u16,
    pub group: Option<GroupId>,
    pub file_offset: u64,
    pub item_count: u64,
    pub payload_length: u64,
    pub payload_sha256: [u8; 32],
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ExportTotalsV1 {
    pub configured_data_groups: u64,
    pub streams: u64,
    pub partitions: u64,
    pub records: u64,
    pub payload_bytes: u64,
    pub partition_bookmarks: u64,
    pub stream_bookmarks: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExportManifestV1 {
    pub format_version: u32,
    pub required_features: u64,
    pub source_cluster: ClusterId,
    pub export_id: ExportIdV1,
    pub selected_streams: Vec<StreamId>,
    pub cut: QuiescentCut,
    pub sections: Vec<SectionDescriptorV1>,
    pub totals: ExportTotalsV1,
    pub exclusions: ExportExclusionsV1,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ArtifactDigestCoverage {
    pub start: u64,
    pub end: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExportInspection {
    pub artifact: ArtifactIdentity,
    pub manifest: ExportManifestV1,
    pub sections: Vec<SectionDescriptorV1>,
    pub artifact_digest_coverage: ArtifactDigestCoverage,
}

#[derive(Clone, Copy, Debug)]
pub enum VerifiedSectionV1<'a> {
    Control(&'a ControlSectionV1),
    DataGroup(&'a DataGroupV1),
}

pub struct VerifiedExport<R> {
    pub(crate) reader: R,
    pub(crate) inspection: ExportInspection,
    pub(crate) limits: ExportLimits,
    pub(crate) manifest_offset: u64,
}

impl<R> VerifiedExport<R> {
    pub fn manifest(&self) -> &ExportManifestV1 {
        &self.inspection.manifest
    }

    pub fn sections(&self) -> &[SectionDescriptorV1] {
        &self.inspection.sections
    }

    pub fn artifact(&self) -> ArtifactIdentity {
        self.inspection.artifact
    }

    pub fn into_reader(self) -> R {
        self.reader
    }
}
