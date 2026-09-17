use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    num::NonZeroU64,
};

use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    ClusterId, DomainError, GroupId, MAX_DATA_GROUPS, MutationRequestId, NodeId, StreamId,
};

pub const MAX_EXPORT_STREAMS: usize = 1_024;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct ExportId(Uuid);

impl ExportId {
    pub const fn from_uuid(value: Uuid) -> Self {
        Self(value)
    }

    pub const fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl<'de> Deserialize<'de> for ExportId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Self(Uuid::deserialize(deserializer)?))
    }
}

impl fmt::Display for ExportId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct ExportEpoch(NonZeroU64);

impl ExportEpoch {
    pub fn new(value: u64) -> Result<Self, DomainError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or_else(|| DomainError::InvalidIdentity {
                kind: "export epoch".to_owned(),
                reason: "zero is reserved".to_owned(),
            })
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

impl<'de> Deserialize<'de> for ExportEpoch {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(u64::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ExportRequestDigest([u8; 32]);

impl ExportRequestDigest {
    pub const fn from_bytes(value: [u8; 32]) -> Self {
        Self(value)
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub const fn into_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl PartialEq<[u8; 32]> for ExportRequestDigest {
    fn eq(&self, other: &[u8; 32]) -> bool {
        self.0 == *other
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ExportSelection {
    streams: Vec<StreamId>,
}

impl ExportSelection {
    pub fn try_new(streams: impl IntoIterator<Item = StreamId>) -> Result<Self, DomainError> {
        let mut streams = streams.into_iter().collect::<Vec<_>>();
        if streams.is_empty() {
            return Err(DomainError::InvalidRange {
                reason: "export selection must contain at least one stream".to_owned(),
            });
        }
        if streams.len() > MAX_EXPORT_STREAMS {
            return Err(DomainError::ResourceLimit {
                resource: "export_streams".to_owned(),
                limit: MAX_EXPORT_STREAMS as u64,
            });
        }
        streams.sort_unstable();
        if streams.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(DomainError::InvalidIdentity {
                kind: "export selection".to_owned(),
                reason: "stream IDs must be unique".to_owned(),
            });
        }
        Ok(Self { streams })
    }

    pub fn streams(&self) -> impl ExactSizeIterator<Item = StreamId> + '_ {
        self.streams.iter().copied()
    }

    pub fn as_slice(&self) -> &[StreamId] {
        &self.streams
    }
}

impl<'de> Deserialize<'de> for ExportSelection {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Wire {
            streams: Vec<StreamId>,
        }

        Self::try_new(Wire::deserialize(deserializer)?.streams).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct ExportDeadline {
    lower_bound_unix_ms: u64,
    upper_bound_unix_ms: u64,
}

impl ExportDeadline {
    pub fn new(lower_bound_unix_ms: u64, upper_bound_unix_ms: u64) -> Result<Self, DomainError> {
        if lower_bound_unix_ms > upper_bound_unix_ms {
            return Err(DomainError::InvalidRange {
                reason: "export deadline lower bound exceeds upper bound".to_owned(),
            });
        }
        Ok(Self {
            lower_bound_unix_ms,
            upper_bound_unix_ms,
        })
    }

    pub const fn lower_bound_unix_ms(self) -> u64 {
        self.lower_bound_unix_ms
    }

    pub const fn upper_bound_unix_ms(self) -> u64 {
        self.upper_bound_unix_ms
    }
}

impl<'de> Deserialize<'de> for ExportDeadline {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Wire {
            lower_bound_unix_ms: u64,
            upper_bound_unix_ms: u64,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::new(wire.lower_bound_unix_ms, wire.upper_bound_unix_ms)
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExportFormatVersion {
    V1,
}

impl ExportFormatVersion {
    pub const fn number(self) -> u32 {
        match self {
            Self::V1 => 1,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ExportIntent {
    request: MutationRequestId,
    cluster: ClusterId,
    selection: ExportSelection,
    format: ExportFormatVersion,
    request_digest: ExportRequestDigest,
    export: ExportId,
}

impl ExportIntent {
    pub fn new(
        request: MutationRequestId,
        cluster: ClusterId,
        selection: ExportSelection,
        format: ExportFormatVersion,
    ) -> Self {
        let request_digest = canonical_request_digest(&request, cluster, &selection, format);
        let export = export_id_from_digest(request_digest);
        Self {
            request,
            cluster,
            selection,
            format,
            request_digest,
            export,
        }
    }

    pub const fn request(&self) -> &MutationRequestId {
        &self.request
    }

    pub const fn cluster(&self) -> ClusterId {
        self.cluster
    }

    pub const fn selection(&self) -> &ExportSelection {
        &self.selection
    }

    pub const fn format(&self) -> ExportFormatVersion {
        self.format
    }

    pub const fn request_digest(&self) -> ExportRequestDigest {
        self.request_digest
    }

    pub const fn export_id(&self) -> ExportId {
        self.export
    }
}

impl<'de> Deserialize<'de> for ExportIntent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Wire {
            request: MutationRequestId,
            cluster: ClusterId,
            selection: ExportSelection,
            format: ExportFormatVersion,
            request_digest: ExportRequestDigest,
            export: ExportId,
        }

        let wire = Wire::deserialize(deserializer)?;
        let intent = Self::new(wire.request, wire.cluster, wire.selection, wire.format);
        if intent.request_digest != wire.request_digest || intent.export != wire.export {
            return Err(serde::de::Error::custom(
                "export intent identity does not match its canonical fields",
            ));
        }
        Ok(intent)
    }
}

fn canonical_request_digest(
    request: &MutationRequestId,
    cluster: ClusterId,
    selection: &ExportSelection,
    format: ExportFormatVersion,
) -> ExportRequestDigest {
    let principal = request.principal().as_str().as_bytes();
    let mut digest = Sha256::new();
    digest.update(b"light-stream/export-request/v1\0");
    digest.update(b"principal\0");
    digest.update((principal.len() as u32).to_be_bytes());
    digest.update(principal);
    digest.update(b"session\0");
    digest.update(request.session().as_uuid().as_bytes());
    digest.update(b"sequence\0");
    digest.update(request.sequence().get().to_be_bytes());
    digest.update(b"cluster\0");
    digest.update(cluster.as_uuid().as_bytes());
    digest.update(b"format\0");
    digest.update(format.number().to_be_bytes());
    digest.update(b"streams\0");
    digest.update((selection.as_slice().len() as u32).to_be_bytes());
    for stream in selection.as_slice() {
        digest.update(stream.as_uuid().as_bytes());
    }
    ExportRequestDigest::from_bytes(digest.finalize().into())
}

fn export_id_from_digest(digest: ExportRequestDigest) -> ExportId {
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    ExportId::from_uuid(Uuid::from_bytes(bytes))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct ExportFenceToken {
    epoch: ExportEpoch,
    export: ExportId,
    request_digest: ExportRequestDigest,
}

impl ExportFenceToken {
    pub fn new(epoch: ExportEpoch, request_digest: ExportRequestDigest) -> Self {
        Self {
            epoch,
            export: export_id_from_digest(request_digest),
            request_digest,
        }
    }

    pub const fn epoch(self) -> ExportEpoch {
        self.epoch
    }

    pub const fn export(self) -> ExportId {
        self.export
    }

    pub const fn request_digest(self) -> ExportRequestDigest {
        self.request_digest
    }
}

impl<'de> Deserialize<'de> for ExportFenceToken {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Wire {
            epoch: ExportEpoch,
            export: ExportId,
            request_digest: ExportRequestDigest,
        }

        let wire = Wire::deserialize(deserializer)?;
        let token = Self::new(wire.epoch, wire.request_digest);
        if token.export != wire.export {
            return Err(serde::de::Error::custom(
                "export fence token ID does not match its request digest",
            ));
        }
        Ok(token)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GroupCut {
    group: GroupId,
    term: u64,
    leader: NodeId,
    applied_index: u64,
}

impl GroupCut {
    pub const fn new(group: GroupId, term: u64, leader: NodeId, applied_index: u64) -> Self {
        Self {
            group,
            term,
            leader,
            applied_index,
        }
    }

    pub const fn group(self) -> GroupId {
        self.group
    }

    pub const fn term(self) -> u64 {
        self.term
    }

    pub const fn leader(self) -> NodeId {
        self.leader
    }

    pub const fn applied_index(self) -> u64 {
        self.applied_index
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct QuiescentCut {
    control: GroupCut,
    data: BTreeMap<GroupId, GroupCut>,
}

impl QuiescentCut {
    pub fn try_new(
        control: GroupCut,
        data: impl IntoIterator<Item = GroupCut>,
    ) -> Result<Self, DomainError> {
        let data = data.into_iter().collect::<Vec<_>>();
        let count = data.len();
        let data = data
            .into_iter()
            .map(|cut| (cut.group(), cut))
            .collect::<BTreeMap<_, _>>();
        if data.len() != count {
            return Err(DomainError::InvalidIdentity {
                kind: "quiescent cut".to_owned(),
                reason: "data group cuts must be unique".to_owned(),
            });
        }
        Ok(Self { control, data })
    }

    pub const fn control(&self) -> GroupCut {
        self.control
    }

    pub const fn data(&self) -> &BTreeMap<GroupId, GroupCut> {
        &self.data
    }
}

impl<'de> Deserialize<'de> for QuiescentCut {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Wire {
            control: GroupCut,
            data: BTreeMap<GroupId, GroupCut>,
        }

        let wire = Wire::deserialize(deserializer)?;
        if wire.data.iter().any(|(group, cut)| *group != cut.group()) {
            return Err(serde::de::Error::custom(
                "quiescent cut key does not match its group cut",
            ));
        }
        Self::try_new(wire.control, wire.data.into_values()).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ExportSpec {
    intent: ExportIntent,
    epoch: ExportEpoch,
    configured_data_groups: BTreeSet<GroupId>,
    deadline: ExportDeadline,
}

impl ExportSpec {
    pub fn try_new(
        intent: ExportIntent,
        epoch: ExportEpoch,
        configured_data_groups: impl IntoIterator<Item = GroupId>,
        deadline: ExportDeadline,
    ) -> Result<Self, DomainError> {
        let groups = configured_data_groups.into_iter().collect::<Vec<_>>();
        if groups.is_empty() {
            return Err(DomainError::InvalidRange {
                reason: "export requires at least one configured data group".to_owned(),
            });
        }
        if groups.len() > usize::from(MAX_DATA_GROUPS) {
            return Err(DomainError::ResourceLimit {
                resource: "export_data_groups".to_owned(),
                limit: u64::from(MAX_DATA_GROUPS),
            });
        }
        let count = groups.len();
        let configured_data_groups = groups.into_iter().collect::<BTreeSet<_>>();
        if configured_data_groups.len() != count {
            return Err(DomainError::InvalidIdentity {
                kind: "export data groups".to_owned(),
                reason: "configured data group IDs must be unique".to_owned(),
            });
        }
        Ok(Self {
            intent,
            epoch,
            configured_data_groups,
            deadline,
        })
    }

    pub const fn request(&self) -> &MutationRequestId {
        self.intent.request()
    }

    pub const fn export(&self) -> ExportId {
        self.intent.export_id()
    }

    pub const fn cluster(&self) -> ClusterId {
        self.intent.cluster()
    }

    pub const fn selection(&self) -> &ExportSelection {
        self.intent.selection()
    }

    pub const fn format(&self) -> ExportFormatVersion {
        self.intent.format()
    }

    pub const fn request_digest(&self) -> ExportRequestDigest {
        self.intent.request_digest()
    }

    pub const fn epoch(&self) -> ExportEpoch {
        self.epoch
    }

    pub const fn configured_data_groups(&self) -> &BTreeSet<GroupId> {
        &self.configured_data_groups
    }

    pub const fn deadline(&self) -> ExportDeadline {
        self.deadline
    }

    pub fn token(&self) -> ExportFenceToken {
        ExportFenceToken::new(self.epoch, self.request_digest())
    }
}

impl<'de> Deserialize<'de> for ExportSpec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Wire {
            intent: ExportIntent,
            epoch: ExportEpoch,
            configured_data_groups: Vec<GroupId>,
            deadline: ExportDeadline,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::try_new(
            wire.intent,
            wire.epoch,
            wire.configured_data_groups,
            wire.deadline,
        )
        .map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct ArtifactIdentity {
    length: NonZeroU64,
    sha256: [u8; 32],
}

impl ArtifactIdentity {
    pub fn new(length: u64, sha256: [u8; 32]) -> Result<Self, DomainError> {
        let length = NonZeroU64::new(length).ok_or_else(|| DomainError::InvalidRange {
            reason: "export artifact length must be greater than zero".to_owned(),
        })?;
        Ok(Self { length, sha256 })
    }

    pub const fn length(self) -> u64 {
        self.length.get()
    }

    pub const fn sha256(self) -> [u8; 32] {
        self.sha256
    }
}

impl<'de> Deserialize<'de> for ArtifactIdentity {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Wire {
            length: u64,
            sha256: [u8; 32],
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::new(wire.length, wire.sha256).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PreparingExport {
    control_cut: GroupCut,
    fenced_groups: BTreeMap<GroupId, GroupCut>,
}

impl PreparingExport {
    pub const fn control_cut(&self) -> GroupCut {
        self.control_cut
    }

    pub const fn fenced_groups(&self) -> &BTreeMap<GroupId, GroupCut> {
        &self.fenced_groups
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AvailableExport {
    cut: QuiescentCut,
    artifact: ArtifactIdentity,
}

impl AvailableExport {
    pub const fn cut(&self) -> &QuiescentCut {
        &self.cut
    }

    pub const fn artifact(&self) -> ArtifactIdentity {
        self.artifact
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReleasingExport {
    cut: QuiescentCut,
    artifact: ArtifactIdentity,
    released_groups: BTreeSet<GroupId>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AbortingExport {
    reason: ExportAbortReason,
    released_groups: BTreeSet<GroupId>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "phase", content = "state", rename_all = "snake_case")]
pub enum ActiveExportPhase {
    Preparing(PreparingExport),
    Frozen(QuiescentCut),
    Materializing(QuiescentCut),
    Available(AvailableExport),
    Releasing(ReleasingExport),
    Aborting(AbortingExport),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ActiveExport {
    spec: ExportSpec,
    phase: ActiveExportPhase,
}

impl ActiveExport {
    pub const fn preparing(spec: ExportSpec, control_cut: GroupCut) -> Self {
        Self {
            spec,
            phase: ActiveExportPhase::Preparing(PreparingExport {
                control_cut,
                fenced_groups: BTreeMap::new(),
            }),
        }
    }

    pub const fn spec(&self) -> &ExportSpec {
        &self.spec
    }

    pub const fn phase(&self) -> &ActiveExportPhase {
        &self.phase
    }

    pub fn record_fence(
        mut self,
        observation: ExportFenceObservation,
    ) -> Result<Self, DomainError> {
        let ActiveExportPhase::Preparing(preparing) = &mut self.phase else {
            return if self.cut().is_some_and(|cut| {
                cut.data().get(&observation.group())
                    == observation.held().map(HeldExportFence::cut).as_ref()
            }) {
                Ok(self)
            } else {
                Err(DomainError::ExportConflict)
            };
        };
        if !self
            .spec
            .configured_data_groups()
            .contains(&observation.group())
        {
            return Err(DomainError::InvalidIdentity {
                kind: "export fence observation".to_owned(),
                reason: "group is not in the configured data-group set".to_owned(),
            });
        }
        let held = observation
            .held()
            .filter(|held| held.token() == self.spec.token())
            .ok_or(DomainError::ExportConflict)?;
        if held.cut().group() != observation.group() {
            return Err(DomainError::InvalidIdentity {
                kind: "export fence observation".to_owned(),
                reason: "observation group does not match the held cut".to_owned(),
            });
        }
        if let Some(existing) = preparing.fenced_groups.get(&observation.group()) {
            return if *existing == held.cut() {
                Ok(self)
            } else {
                Err(DomainError::ExportConflict)
            };
        }
        preparing
            .fenced_groups
            .insert(observation.group(), held.cut());
        if preparing
            .fenced_groups
            .keys()
            .copied()
            .collect::<BTreeSet<_>>()
            == *self.spec.configured_data_groups()
        {
            self.phase = ActiveExportPhase::Frozen(QuiescentCut::try_new(
                preparing.control_cut,
                preparing.fenced_groups.values().copied(),
            )?);
        }
        Ok(self)
    }

    pub fn begin_materialization(mut self) -> Result<Self, DomainError> {
        match &self.phase {
            ActiveExportPhase::Frozen(cut) => {
                self.phase = ActiveExportPhase::Materializing(cut.clone());
                Ok(self)
            }
            ActiveExportPhase::Materializing(_) => Ok(self),
            _ => Err(DomainError::ExportConflict),
        }
    }

    pub fn publish_artifact(
        mut self,
        artifact: ArtifactIdentity,
        cut: QuiescentCut,
    ) -> Result<Self, DomainError> {
        match &self.phase {
            ActiveExportPhase::Materializing(expected) if expected == &cut => {
                self.phase = ActiveExportPhase::Available(AvailableExport { cut, artifact });
                Ok(self)
            }
            ActiveExportPhase::Available(existing)
                if existing.cut == cut && existing.artifact == artifact =>
            {
                Ok(self)
            }
            _ => Err(DomainError::ExportConflict),
        }
    }

    pub fn request_completion(mut self, artifact: ArtifactIdentity) -> Result<Self, DomainError> {
        match &self.phase {
            ActiveExportPhase::Available(available) if available.artifact == artifact => {
                self.phase = ActiveExportPhase::Releasing(ReleasingExport {
                    cut: available.cut.clone(),
                    artifact,
                    released_groups: BTreeSet::new(),
                });
                Ok(self)
            }
            ActiveExportPhase::Releasing(releasing) if releasing.artifact == artifact => Ok(self),
            _ => Err(DomainError::ExportConflict),
        }
    }

    pub fn request_abort(mut self, reason: ExportAbortReason) -> Result<Self, DomainError> {
        match &self.phase {
            ActiveExportPhase::Aborting(aborting) if aborting.reason == reason => Ok(self),
            ActiveExportPhase::Aborting(_) => Err(DomainError::ExportConflict),
            ActiveExportPhase::Releasing(_) => Err(DomainError::ExportConflict),
            _ => {
                self.phase = ActiveExportPhase::Aborting(AbortingExport {
                    reason,
                    released_groups: BTreeSet::new(),
                });
                Ok(self)
            }
        }
    }

    pub fn record_release(
        mut self,
        observation: ExportFenceObservation,
    ) -> Result<Self, DomainError> {
        if !self
            .spec
            .configured_data_groups()
            .contains(&observation.group())
        {
            return Err(DomainError::InvalidIdentity {
                kind: "export release observation".to_owned(),
                reason: "group is not in the configured data-group set".to_owned(),
            });
        }
        if observation.through_epoch() < self.spec.epoch().get() || observation.held().is_some() {
            return Err(DomainError::ExportConflict);
        }
        match &mut self.phase {
            ActiveExportPhase::Releasing(releasing) => {
                releasing.released_groups.insert(observation.group());
                Ok(self)
            }
            ActiveExportPhase::Aborting(aborting) => {
                aborting.released_groups.insert(observation.group());
                Ok(self)
            }
            _ => Err(DomainError::ExportConflict),
        }
    }

    pub fn is_ready_to_finish(&self) -> bool {
        let released = match &self.phase {
            ActiveExportPhase::Releasing(value) => &value.released_groups,
            ActiveExportPhase::Aborting(value) => &value.released_groups,
            _ => return false,
        };
        released == self.spec.configured_data_groups()
    }

    pub fn receipt(&self) -> Result<ExportReceipt, DomainError> {
        if !self.is_ready_to_finish() {
            return Err(DomainError::ExportConflict);
        }
        let outcome = match &self.phase {
            ActiveExportPhase::Releasing(value) => ExportReceiptOutcome::Completed(value.artifact),
            ActiveExportPhase::Aborting(value) => {
                ExportReceiptOutcome::Aborted(value.reason.clone())
            }
            _ => return Err(DomainError::ExportConflict),
        };
        Ok(ExportReceipt::new(
            self.spec.request().clone(),
            self.spec.epoch(),
            self.spec.request_digest(),
            outcome,
        ))
    }

    fn cut(&self) -> Option<&QuiescentCut> {
        match &self.phase {
            ActiveExportPhase::Frozen(cut) | ActiveExportPhase::Materializing(cut) => Some(cut),
            ActiveExportPhase::Available(value) => Some(&value.cut),
            ActiveExportPhase::Releasing(value) => Some(&value.cut),
            ActiveExportPhase::Preparing(_) | ActiveExportPhase::Aborting(_) => None,
        }
    }
}

impl<'de> Deserialize<'de> for ActiveExport {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Wire {
            spec: ExportSpec,
            phase: ActiveExportPhase,
        }

        let wire = Wire::deserialize(deserializer)?;
        let active = Self {
            spec: wire.spec,
            phase: wire.phase,
        };
        let configured = active.spec.configured_data_groups();
        let valid = match &active.phase {
            ActiveExportPhase::Preparing(value) => {
                value.fenced_groups.len() < configured.len()
                    && value
                        .fenced_groups
                        .iter()
                        .all(|(group, cut)| configured.contains(group) && *group == cut.group())
            }
            ActiveExportPhase::Frozen(cut)
            | ActiveExportPhase::Materializing(cut)
            | ActiveExportPhase::Available(AvailableExport { cut, .. }) => {
                cut.data().keys().copied().collect::<BTreeSet<_>>() == *configured
            }
            ActiveExportPhase::Releasing(value) => {
                value.cut.data().keys().copied().collect::<BTreeSet<_>>() == *configured
                    && value.released_groups.is_subset(configured)
            }
            ActiveExportPhase::Aborting(value) => value.released_groups.is_subset(configured),
        };
        if !valid {
            return Err(serde::de::Error::custom(
                "active export phase does not match its configured groups",
            ));
        }
        Ok(active)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExportAbortReason {
    OperatorRequested,
    DeadlineExceeded,
    MaterializationFailed,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExportTerminalDisposition {
    Completed,
    Aborted,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "outcome", content = "value", rename_all = "snake_case")]
pub enum ExportReceiptOutcome {
    Completed(ArtifactIdentity),
    Aborted(ExportAbortReason),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ExportReceipt {
    request: MutationRequestId,
    export: ExportId,
    epoch: ExportEpoch,
    request_digest: ExportRequestDigest,
    outcome: ExportReceiptOutcome,
}

impl ExportReceipt {
    pub fn new(
        request: MutationRequestId,
        epoch: ExportEpoch,
        request_digest: ExportRequestDigest,
        outcome: ExportReceiptOutcome,
    ) -> Self {
        Self {
            request,
            export: export_id_from_digest(request_digest),
            epoch,
            request_digest,
            outcome,
        }
    }

    pub const fn request(&self) -> &MutationRequestId {
        &self.request
    }

    pub const fn export(&self) -> ExportId {
        self.export
    }

    pub const fn epoch(&self) -> ExportEpoch {
        self.epoch
    }

    pub const fn request_digest(&self) -> ExportRequestDigest {
        self.request_digest
    }

    pub const fn outcome(&self) -> &ExportReceiptOutcome {
        &self.outcome
    }

    pub const fn disposition(&self) -> ExportTerminalDisposition {
        match self.outcome {
            ExportReceiptOutcome::Completed(_) => ExportTerminalDisposition::Completed,
            ExportReceiptOutcome::Aborted(_) => ExportTerminalDisposition::Aborted,
        }
    }
}

impl<'de> Deserialize<'de> for ExportReceipt {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Wire {
            request: MutationRequestId,
            export: ExportId,
            epoch: ExportEpoch,
            request_digest: ExportRequestDigest,
            outcome: ExportReceiptOutcome,
        }

        let wire = Wire::deserialize(deserializer)?;
        let receipt = Self::new(wire.request, wire.epoch, wire.request_digest, wire.outcome);
        if receipt.export != wire.export {
            return Err(serde::de::Error::custom(
                "export receipt ID does not match its request digest",
            ));
        }
        Ok(receipt)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExportStatusPhase {
    Preparing,
    Frozen,
    Materializing,
    Available,
    Releasing,
    Aborting,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ActiveExportStatus {
    export: ExportId,
    epoch: ExportEpoch,
    phase: ExportStatusPhase,
}

impl ActiveExportStatus {
    pub const fn export(self) -> ExportId {
        self.export
    }

    pub const fn epoch(self) -> ExportEpoch {
        self.epoch
    }

    pub const fn phase(self) -> ExportStatusPhase {
        self.phase
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "status", content = "value", rename_all = "snake_case")]
pub enum ExportStatus {
    Active(ActiveExportStatus),
    Terminal(ExportReceipt),
}

impl ExportStatus {
    pub fn active(active: &ActiveExport) -> Self {
        let phase = match active.phase() {
            ActiveExportPhase::Preparing(_) => ExportStatusPhase::Preparing,
            ActiveExportPhase::Frozen(_) => ExportStatusPhase::Frozen,
            ActiveExportPhase::Materializing(_) => ExportStatusPhase::Materializing,
            ActiveExportPhase::Available(_) => ExportStatusPhase::Available,
            ActiveExportPhase::Releasing(_) => ExportStatusPhase::Releasing,
            ActiveExportPhase::Aborting(_) => ExportStatusPhase::Aborting,
        };
        Self::Active(ActiveExportStatus {
            export: active.spec().export(),
            epoch: active.spec().epoch(),
            phase,
        })
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct HeldExportFence {
    token: ExportFenceToken,
    cut: GroupCut,
}

impl HeldExportFence {
    pub const fn new(token: ExportFenceToken, cut: GroupCut) -> Self {
        Self { token, cut }
    }

    pub const fn token(self) -> ExportFenceToken {
        self.token
    }

    pub const fn cut(self) -> GroupCut {
        self.cut
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub struct MutationFenceState {
    through_epoch: u64,
    held: Option<HeldExportFence>,
}

impl MutationFenceState {
    pub const fn through_epoch(&self) -> u64 {
        self.through_epoch
    }

    pub const fn held(&self) -> Option<&HeldExportFence> {
        self.held.as_ref()
    }

    pub fn acquire(
        mut self,
        token: ExportFenceToken,
        cut: GroupCut,
    ) -> Result<(Self, ExportFenceObservation), DomainError> {
        if token.epoch().get() <= self.through_epoch {
            let observation = self.observation(cut.group());
            return Ok((self, observation));
        }
        match self.held {
            Some(held) if held.token() == token => {
                let observation = self.observation(held.cut().group());
                return Ok((self, observation));
            }
            Some(_) => return Err(DomainError::ExportConflict),
            None => self.held = Some(HeldExportFence::new(token, cut)),
        }
        let observation = self.observation(cut.group());
        Ok((self, observation))
    }

    pub fn release_for_group(
        mut self,
        token: ExportFenceToken,
        group: GroupId,
    ) -> Result<(Self, ExportFenceObservation), DomainError> {
        if let Some(held) = self.held {
            if held.token() != token {
                return Err(DomainError::ExportConflict);
            }
            self.held = None;
        }
        self.through_epoch = self.through_epoch.max(token.epoch().get());
        let observation = self.observation(group);
        Ok((self, observation))
    }

    fn observation(&self, group: GroupId) -> ExportFenceObservation {
        ExportFenceObservation {
            group,
            through_epoch: self.through_epoch,
            held: self.held,
        }
    }
}

impl<'de> Deserialize<'de> for MutationFenceState {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Wire {
            through_epoch: u64,
            held: Option<HeldExportFence>,
        }

        let wire = Wire::deserialize(deserializer)?;
        if wire
            .held
            .is_some_and(|held| held.token().epoch().get() <= wire.through_epoch)
        {
            return Err(serde::de::Error::custom(
                "held export fence is not newer than the release watermark",
            ));
        }
        Ok(Self {
            through_epoch: wire.through_epoch,
            held: wire.held,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct ExportFenceObservation {
    group: GroupId,
    through_epoch: u64,
    held: Option<HeldExportFence>,
}

impl<'de> Deserialize<'de> for ExportFenceObservation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Wire {
            group: GroupId,
            through_epoch: u64,
            held: Option<HeldExportFence>,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::new(wire.group, wire.through_epoch, wire.held).map_err(serde::de::Error::custom)
    }
}

impl ExportFenceObservation {
    pub fn new(
        group: GroupId,
        through_epoch: u64,
        held: Option<HeldExportFence>,
    ) -> Result<Self, DomainError> {
        if held.is_some_and(|fence| fence.cut().group() != group) {
            return Err(DomainError::InvalidIdentity {
                kind: "export fence observation".to_owned(),
                reason: "held fence cut belongs to another group".to_owned(),
            });
        }
        if held.is_some_and(|fence| fence.token().epoch().get() <= through_epoch) {
            return Err(DomainError::InvalidIdentity {
                kind: "export fence observation".to_owned(),
                reason: "held fence is not newer than the release watermark".to_owned(),
            });
        }
        Ok(Self {
            group,
            through_epoch,
            held,
        })
    }

    pub const fn group(self) -> GroupId {
        self.group
    }

    pub const fn through_epoch(self) -> u64 {
        self.through_epoch
    }

    pub const fn held(self) -> Option<HeldExportFence> {
        self.held
    }
}

#[cfg(test)]
mod tests {
    use sha2::{Digest, Sha256};
    use uuid::Uuid;

    use super::*;
    use crate::{
        ClusterId, GroupId, MutationRequestId, MutationSessionId, NodeId, PrincipalId,
        RequestSequence, StreamId,
    };

    fn stream(value: u128) -> StreamId {
        StreamId::from_uuid(Uuid::from_u128(value))
    }

    fn request(principal: &str, session: u128, sequence: u64) -> MutationRequestId {
        MutationRequestId::new(
            PrincipalId::parse(principal).unwrap(),
            MutationSessionId::from_uuid(Uuid::from_u128(session)),
            RequestSequence::new(sequence),
        )
    }

    fn intent(
        request: MutationRequestId,
        cluster: ClusterId,
        streams: impl IntoIterator<Item = StreamId>,
    ) -> ExportIntent {
        ExportIntent::new(
            request,
            cluster,
            ExportSelection::try_new(streams).unwrap(),
            ExportFormatVersion::V1,
        )
    }

    #[test]
    fn export_selection_is_canonical_nonempty_bounded_and_unique() {
        let selection = ExportSelection::try_new([stream(3), stream(1), stream(2)]).unwrap();
        assert_eq!(
            selection.streams().collect::<Vec<_>>(),
            vec![stream(1), stream(2), stream(3)]
        );

        assert!(matches!(
            ExportSelection::try_new([]),
            Err(crate::DomainError::InvalidRange { .. })
        ));
        assert!(matches!(
            ExportSelection::try_new([stream(1), stream(1)]),
            Err(crate::DomainError::InvalidIdentity { .. })
        ));
        assert!(matches!(
            ExportSelection::try_new(
                (0..=MAX_EXPORT_STREAMS).map(|index| stream(index as u128 + 1))
            ),
            Err(crate::DomainError::ResourceLimit { .. })
        ));
    }

    #[test]
    fn export_digest_and_id_cover_the_canonical_intent_but_not_deadline() {
        let cluster = ClusterId::from_uuid(Uuid::from_u128(10));
        let base_request = request("operator-a", 20, 30);
        let first_intent = intent(base_request.clone(), cluster, [stream(2), stream(1)]);
        let same_intent = intent(base_request.clone(), cluster, [stream(1), stream(2)]);
        let first = ExportSpec::try_new(
            first_intent,
            ExportEpoch::new(1).unwrap(),
            [GroupId::new(2).unwrap(), GroupId::new(3).unwrap()],
            ExportDeadline::new(1_000, 2_000).unwrap(),
        )
        .unwrap();
        let retry = ExportSpec::try_new(
            same_intent,
            ExportEpoch::new(1).unwrap(),
            [GroupId::new(3).unwrap(), GroupId::new(2).unwrap()],
            ExportDeadline::new(9_000, 10_000).unwrap(),
        )
        .unwrap();

        assert_eq!(first.request_digest(), retry.request_digest());
        assert_eq!(first.export(), retry.export());
        let mut expected = Sha256::new();
        expected.update(b"light-stream/export-request/v1\0");
        expected.update(b"principal\0");
        expected.update((b"operator-a".len() as u32).to_be_bytes());
        expected.update(b"operator-a");
        expected.update(b"session\0");
        expected.update(Uuid::from_u128(20).as_bytes());
        expected.update(b"sequence\0");
        expected.update(30_u64.to_be_bytes());
        expected.update(b"cluster\0");
        expected.update(cluster.as_uuid().as_bytes());
        expected.update(b"format\0");
        expected.update(1_u32.to_be_bytes());
        expected.update(b"streams\0");
        expected.update(2_u32.to_be_bytes());
        expected.update(stream(1).as_uuid().as_bytes());
        expected.update(stream(2).as_uuid().as_bytes());
        let expected: [u8; 32] = expected.finalize().into();
        assert_eq!(first.request_digest(), expected);

        let changed_principal = intent(
            request("operator-b", 20, 30),
            cluster,
            [stream(1), stream(2)],
        );
        let changed_session = intent(
            request("operator-a", 21, 30),
            cluster,
            [stream(1), stream(2)],
        );
        let changed_sequence = intent(
            request("operator-a", 20, 31),
            cluster,
            [stream(1), stream(2)],
        );
        let changed_cluster = intent(
            base_request.clone(),
            ClusterId::from_uuid(Uuid::from_u128(11)),
            [stream(1), stream(2)],
        );
        let changed_streams = intent(base_request, cluster, [stream(1), stream(3)]);

        for changed in [
            changed_principal,
            changed_session,
            changed_sequence,
            changed_cluster,
            changed_streams,
        ] {
            assert_ne!(first.request_digest(), changed.request_digest());
            assert_ne!(first.export(), changed.export_id());
        }
        assert_eq!(first.format(), ExportFormatVersion::V1);
    }

    #[test]
    fn release_before_acquire_keeps_the_fence_open_through_that_epoch() {
        let token = ExportFenceToken::new(
            ExportEpoch::new(7).unwrap(),
            ExportRequestDigest::from_bytes([9; 32]),
        );
        let group = GroupId::new(2).unwrap();
        let cut = GroupCut::new(group, 3, NodeId::new(1).unwrap(), 99);

        let (released, release_observation) = MutationFenceState::default()
            .release_for_group(token, group)
            .unwrap();
        assert_eq!(released.through_epoch(), 7);
        assert!(released.held().is_none());
        assert!(release_observation.held().is_none());

        let (delayed, delayed_observation) = released.acquire(token, cut).unwrap();
        assert_eq!(delayed.through_epoch(), 7);
        assert!(delayed.held().is_none());
        assert!(delayed_observation.held().is_none());

        let older = ExportFenceToken::new(ExportEpoch::new(6).unwrap(), token.request_digest());
        let (still_open, older_observation) = delayed.acquire(older, cut).unwrap();
        assert_eq!(still_open.through_epoch(), 7);
        assert!(still_open.held().is_none());
        assert!(older_observation.held().is_none());
    }

    #[test]
    fn export_deserialization_rechecks_constructor_invariants() {
        assert!(serde_json::from_str::<ExportEpoch>("0").is_err());
        assert!(
            serde_json::from_value::<ExportSelection>(serde_json::json!({ "streams": [] }))
                .is_err()
        );
        assert!(
            serde_json::from_value::<ExportDeadline>(serde_json::json!({
                "lower_bound_unix_ms": 2,
                "upper_bound_unix_ms": 1
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<ArtifactIdentity>(serde_json::json!({
                "length": 0,
                "sha256": vec![0; 32]
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<ArtifactIdentity>(serde_json::json!({
                "length": 1,
                "sha256": vec![0; 31]
            }))
            .is_err()
        );

        let cluster = ClusterId::from_uuid(Uuid::from_u128(10));
        let spec = ExportSpec::try_new(
            intent(request("operator", 20, 30), cluster, [stream(1)]),
            ExportEpoch::new(1).unwrap(),
            [GroupId::new(2).unwrap()],
            ExportDeadline::new(1, 2).unwrap(),
        )
        .unwrap();
        let mut value = serde_json::to_value(spec).unwrap();
        value["configured_data_groups"] = serde_json::json!([2, 2]);
        assert!(serde_json::from_value::<ExportSpec>(value).is_err());

        let token = ExportFenceToken::new(
            ExportEpoch::new(1).unwrap(),
            ExportRequestDigest::from_bytes([1; 32]),
        );
        let observation = serde_json::json!({
            "group": 2,
            "through_epoch": 0,
            "held": {
                "token": token,
                "cut": GroupCut::new(
                    GroupId::new(3).unwrap(),
                    1,
                    NodeId::new(1).unwrap(),
                    1
                )
            }
        });
        assert!(serde_json::from_value::<ExportFenceObservation>(observation).is_err());

        let stale_held_fence = serde_json::json!({
            "group": 2,
            "through_epoch": 1,
            "held": {
                "token": token,
                "cut": GroupCut::new(
                    GroupId::new(2).unwrap(),
                    1,
                    NodeId::new(1).unwrap(),
                    1
                )
            }
        });
        assert!(
            serde_json::from_value::<ExportFenceObservation>(stale_held_fence.clone()).is_err()
        );
        assert!(
            ExportFenceObservation::new(
                GroupId::new(2).unwrap(),
                1,
                Some(serde_json::from_value(stale_held_fence["held"].clone()).unwrap()),
            )
            .is_err()
        );

        let configured_group = GroupId::new(2).unwrap();
        let control_cut = GroupCut::new(GroupId::new(1).unwrap(), 1, NodeId::new(1).unwrap(), 1);
        let data_cut = GroupCut::new(configured_group, 1, NodeId::new(1).unwrap(), 1);
        let spec = ExportSpec::try_new(
            intent(request("operator", 21, 31), cluster, [stream(1)]),
            ExportEpoch::new(2).unwrap(),
            [configured_group],
            ExportDeadline::new(1, 2).unwrap(),
        )
        .unwrap();
        let mut impossible_preparing =
            serde_json::to_value(ActiveExport::preparing(spec, control_cut)).unwrap();
        impossible_preparing["phase"]["state"]["fenced_groups"] =
            serde_json::to_value(BTreeMap::from([(configured_group, data_cut)])).unwrap();
        assert!(serde_json::from_value::<ActiveExport>(impossible_preparing).is_err());

        let receipt_intent = intent(request("operator", 22, 32), cluster, [stream(1)]);
        let receipt = ExportReceipt::new(
            receipt_intent.request().clone(),
            ExportEpoch::new(3).unwrap(),
            receipt_intent.request_digest(),
            ExportReceiptOutcome::Aborted(ExportAbortReason::OperatorRequested),
        );
        let mut invalid_receipt = serde_json::to_value(receipt).unwrap();
        invalid_receipt["export"] =
            serde_json::to_value(ExportId::from_uuid(Uuid::from_u128(999))).unwrap();
        assert!(serde_json::from_value::<ExportReceipt>(invalid_receipt).is_err());
    }

    #[test]
    fn export_fence_token_rejects_a_mismatched_export_id() {
        let intent = intent(
            request("operator", 1, 1),
            ClusterId::from_uuid(Uuid::from_u128(2)),
            [StreamId::from_uuid(Uuid::from_u128(3))],
        );
        let token = ExportFenceToken::new(ExportEpoch::new(1).unwrap(), intent.request_digest());
        let mut invalid = serde_json::to_value(token).unwrap();
        invalid["export"] =
            serde_json::to_value(ExportId::from_uuid(Uuid::from_u128(999))).unwrap();

        assert!(serde_json::from_value::<ExportFenceToken>(invalid).is_err());
    }

    #[test]
    fn abort_reason_cannot_change_after_closing_starts() {
        let cluster = ClusterId::from_uuid(Uuid::from_u128(10));
        let spec = ExportSpec::try_new(
            intent(request("operator", 20, 30), cluster, [stream(1)]),
            ExportEpoch::new(1).unwrap(),
            [GroupId::new(2).unwrap()],
            ExportDeadline::new(1, 2).unwrap(),
        )
        .unwrap();
        let active = ActiveExport::preparing(
            spec,
            GroupCut::new(GroupId::new(1).unwrap(), 1, NodeId::new(1).unwrap(), 1),
        )
        .request_abort(ExportAbortReason::OperatorRequested)
        .unwrap();

        assert_eq!(
            active.request_abort(ExportAbortReason::DeadlineExceeded),
            Err(DomainError::ExportConflict)
        );
    }
}
