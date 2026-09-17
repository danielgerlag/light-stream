use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    AmbiguousRequest, ConsensusGroup, ExportId, LeaderHint, RecordOffset, ReplayLeaseId,
    ReplayLeaseLifecycle, RequestOutcome,
};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    InvalidIdentity,
    InvalidName,
    InvalidRange,
    InvalidPayload,
    NotBootstrapped,
    BootstrapConflict,
    IdentityMismatch,
    ReceiptConflict,
    ReceiptExpired,
    ReceiptNotFound,
    Storage,
    NotLeader,
    QuorumUnavailable,
    ClusterForming,
    StreamNotFound,
    StreamNotActive,
    StreamNameConflict,
    BookmarkNotFound,
    BookmarkNameConflict,
    CheckpointNotFound,
    CheckpointAheadOfTail,
    CheckpointRegression,
    CursorExpired,
    ReplayLeaseNotFound,
    ReplayLeaseInactive,
    ReplayLeaseConflict,
    ReplayLeaseRangeViolation,
    ReplayLeaseLifetimeExhausted,
    MutationConflict,
    MutationReceiptExpired,
    ExportConflict,
    ExportInProgress,
    LeaseClockUnavailable,
    PublishOverloaded,
    ResourceLimit,
    SecurityAuthenticationFailed,
    SecurityPermissionDenied,
    SecurityPolicyConflict,
    SecurityPolicyStale,
    ShuttingDown,
    StaleRoute,
    UnsupportedOperation,
}

impl ErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidIdentity => "invalid_identity",
            Self::InvalidName => "invalid_name",
            Self::InvalidRange => "invalid_range",
            Self::InvalidPayload => "invalid_payload",
            Self::NotBootstrapped => "not_bootstrapped",
            Self::BootstrapConflict => "bootstrap_conflict",
            Self::IdentityMismatch => "identity_mismatch",
            Self::ReceiptConflict => "receipt_conflict",
            Self::ReceiptExpired => "receipt_expired",
            Self::ReceiptNotFound => "receipt_not_found",
            Self::Storage => "storage_error",
            Self::NotLeader => "not_leader",
            Self::QuorumUnavailable => "quorum_unavailable",
            Self::ClusterForming => "cluster_forming",
            Self::StreamNotFound => "stream_not_found",
            Self::StreamNotActive => "stream_not_active",
            Self::StreamNameConflict => "stream_name_conflict",
            Self::BookmarkNotFound => "bookmark_not_found",
            Self::BookmarkNameConflict => "bookmark_name_conflict",
            Self::CheckpointNotFound => "checkpoint_not_found",
            Self::CheckpointAheadOfTail => "checkpoint_ahead_of_tail",
            Self::CheckpointRegression => "checkpoint_regression",
            Self::CursorExpired => "cursor_expired",
            Self::ReplayLeaseNotFound => "replay_lease_not_found",
            Self::ReplayLeaseInactive => "replay_lease_inactive",
            Self::ReplayLeaseConflict => "replay_lease_conflict",
            Self::ReplayLeaseRangeViolation => "replay_lease_range_violation",
            Self::ReplayLeaseLifetimeExhausted => "replay_lease_lifetime_exhausted",
            Self::MutationConflict => "mutation_conflict",
            Self::MutationReceiptExpired => "mutation_receipt_expired",
            Self::ExportConflict => "export_conflict",
            Self::ExportInProgress => "export_in_progress",
            Self::LeaseClockUnavailable => "lease_clock_unavailable",
            Self::PublishOverloaded => "publish_overloaded",
            Self::ResourceLimit => "resource_limit",
            Self::SecurityAuthenticationFailed => "security_authentication_failed",
            Self::SecurityPermissionDenied => "security_permission_denied",
            Self::SecurityPolicyConflict => "security_policy_conflict",
            Self::SecurityPolicyStale => "security_policy_stale",
            Self::ShuttingDown => "shutting_down",
            Self::StaleRoute => "stale_route",
            Self::UnsupportedOperation => "unsupported_operation",
        }
    }
}

#[derive(Clone, Debug, Error, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "code", rename_all = "snake_case")]
pub enum DomainError {
    #[error("invalid {kind}: {reason}")]
    InvalidIdentity { kind: String, reason: String },
    #[error("invalid {kind}: {reason}")]
    InvalidName { kind: String, reason: String },
    #[error("invalid committed range: {reason}")]
    InvalidRange { reason: String },
    #[error("invalid publish payload: {reason}")]
    InvalidPayload { reason: String },
    #[error("cluster has not been bootstrapped")]
    NotBootstrapped,
    #[error("bootstrap conflicts with the existing cluster: {reason}")]
    BootstrapConflict { reason: String },
    #[error("stored identity does not match the requested identity: {reason}")]
    IdentityMismatch { reason: String },
    #[error("producer request identity was already used for another payload")]
    ReceiptConflict,
    #[error("producer request identity is older than the retained receipt window")]
    ReceiptExpired,
    #[error("producer receipt was not found")]
    ReceiptNotFound,
    #[error("durable storage failed: {reason}")]
    Storage { reason: String },
    #[error("the local node is not the current {group:?} leader")]
    NotLeader {
        group: ConsensusGroup,
        leader: Option<LeaderHint>,
    },
    #[error("the {group:?} group has no available quorum")]
    QuorumUnavailable {
        group: ConsensusGroup,
        outcome: RequestOutcome,
        request: Option<AmbiguousRequest>,
    },
    #[error("cluster formation is not complete")]
    ClusterForming,
    #[error("stream was not found")]
    StreamNotFound,
    #[error("stream is not active")]
    StreamNotActive,
    #[error("stream name is already active or fenced")]
    StreamNameConflict,
    #[error("bookmark was not found")]
    BookmarkNotFound,
    #[error("bookmark name is already active")]
    BookmarkNameConflict,
    #[error("consumer checkpoint was not found")]
    CheckpointNotFound,
    #[error("checkpoint offset {candidate:?} exceeds committed tail {tail:?}")]
    CheckpointAheadOfTail {
        candidate: RecordOffset,
        tail: RecordOffset,
    },
    #[error("checkpoint offset {candidate:?} precedes current offset {current:?}")]
    CheckpointRegression {
        current: RecordOffset,
        candidate: RecordOffset,
    },
    #[error("record offset {requested:?} expired; earliest available offset is {available_from:?}")]
    CursorExpired {
        requested: RecordOffset,
        available_from: RecordOffset,
    },
    #[error("replay lease {lease} was not found")]
    ReplayLeaseNotFound { lease: ReplayLeaseId },
    #[error("replay lease {lease} is {lifecycle:?}")]
    ReplayLeaseInactive {
        lease: ReplayLeaseId,
        lifecycle: ReplayLeaseLifecycle,
    },
    #[error("replay lease request identity conflicts with an existing request")]
    ReplayLeaseConflict,
    #[error("requested replay is outside the admitted lease range")]
    ReplayLeaseRangeViolation,
    #[error("replay lease reached its maximum total lifetime")]
    ReplayLeaseLifetimeExhausted,
    #[error("mutation request identity was already used for another operation")]
    MutationConflict,
    #[error("mutation request identity is older than the retained receipt window")]
    MutationReceiptExpired,
    #[error("export request identity was already used for another canonical intent")]
    ExportConflict,
    #[error("export {export} currently fences this mutation")]
    ExportInProgress {
        export: ExportId,
        outcome: RequestOutcome,
    },
    #[error("the configured lease clock guarantee is unavailable")]
    LeaseClockUnavailable,
    #[error("{resource} limit {limit} was exceeded before publish admission")]
    PublishOverloaded { resource: String, limit: u64 },
    #[error("{resource} limit {limit} was exceeded")]
    ResourceLimit { resource: String, limit: u64 },
    #[error("client authentication failed")]
    SecurityAuthenticationFailed,
    #[error("the authenticated principal is not authorized for this operation")]
    SecurityPermissionDenied,
    #[error("the security policy mutation conflicts with committed policy")]
    SecurityPolicyConflict,
    #[error("the local security policy is too stale to authorize work")]
    SecurityPolicyStale,
    #[error("the broker is draining and no longer accepts mutations")]
    ShuttingDown { outcome: RequestOutcome },
    #[error("routing metadata is stale")]
    StaleRoute,
    #[error("{operation} is not supported until {available_phase}")]
    UnsupportedOperation {
        operation: String,
        available_phase: String,
    },
}

impl DomainError {
    pub const fn code(&self) -> ErrorCode {
        match self {
            Self::InvalidIdentity { .. } => ErrorCode::InvalidIdentity,
            Self::InvalidName { .. } => ErrorCode::InvalidName,
            Self::InvalidRange { .. } => ErrorCode::InvalidRange,
            Self::InvalidPayload { .. } => ErrorCode::InvalidPayload,
            Self::NotBootstrapped => ErrorCode::NotBootstrapped,
            Self::BootstrapConflict { .. } => ErrorCode::BootstrapConflict,
            Self::IdentityMismatch { .. } => ErrorCode::IdentityMismatch,
            Self::ReceiptConflict => ErrorCode::ReceiptConflict,
            Self::ReceiptExpired => ErrorCode::ReceiptExpired,
            Self::ReceiptNotFound => ErrorCode::ReceiptNotFound,
            Self::Storage { .. } => ErrorCode::Storage,
            Self::NotLeader { .. } => ErrorCode::NotLeader,
            Self::QuorumUnavailable { .. } => ErrorCode::QuorumUnavailable,
            Self::ClusterForming => ErrorCode::ClusterForming,
            Self::StreamNotFound => ErrorCode::StreamNotFound,
            Self::StreamNotActive => ErrorCode::StreamNotActive,
            Self::StreamNameConflict => ErrorCode::StreamNameConflict,
            Self::BookmarkNotFound => ErrorCode::BookmarkNotFound,
            Self::BookmarkNameConflict => ErrorCode::BookmarkNameConflict,
            Self::CheckpointNotFound => ErrorCode::CheckpointNotFound,
            Self::CheckpointAheadOfTail { .. } => ErrorCode::CheckpointAheadOfTail,
            Self::CheckpointRegression { .. } => ErrorCode::CheckpointRegression,
            Self::CursorExpired { .. } => ErrorCode::CursorExpired,
            Self::ReplayLeaseNotFound { .. } => ErrorCode::ReplayLeaseNotFound,
            Self::ReplayLeaseInactive { .. } => ErrorCode::ReplayLeaseInactive,
            Self::ReplayLeaseConflict => ErrorCode::ReplayLeaseConflict,
            Self::ReplayLeaseRangeViolation => ErrorCode::ReplayLeaseRangeViolation,
            Self::ReplayLeaseLifetimeExhausted => ErrorCode::ReplayLeaseLifetimeExhausted,
            Self::MutationConflict => ErrorCode::MutationConflict,
            Self::MutationReceiptExpired => ErrorCode::MutationReceiptExpired,
            Self::ExportConflict => ErrorCode::ExportConflict,
            Self::ExportInProgress { .. } => ErrorCode::ExportInProgress,
            Self::LeaseClockUnavailable => ErrorCode::LeaseClockUnavailable,
            Self::PublishOverloaded { .. } => ErrorCode::PublishOverloaded,
            Self::ResourceLimit { .. } => ErrorCode::ResourceLimit,
            Self::SecurityAuthenticationFailed => ErrorCode::SecurityAuthenticationFailed,
            Self::SecurityPermissionDenied => ErrorCode::SecurityPermissionDenied,
            Self::SecurityPolicyConflict => ErrorCode::SecurityPolicyConflict,
            Self::SecurityPolicyStale => ErrorCode::SecurityPolicyStale,
            Self::ShuttingDown { .. } => ErrorCode::ShuttingDown,
            Self::StaleRoute => ErrorCode::StaleRoute,
            Self::UnsupportedOperation { .. } => ErrorCode::UnsupportedOperation,
        }
    }
}
