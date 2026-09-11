use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{ConsensusGroup, LeaderHint, ProducerRequestId, RequestOutcome};

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
    ResourceLimit,
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
            Self::ResourceLimit => "resource_limit",
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
        request: Option<ProducerRequestId>,
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
    #[error("{resource} limit {limit} was exceeded")]
    ResourceLimit { resource: String, limit: u64 },
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
            Self::ResourceLimit { .. } => ErrorCode::ResourceLimit,
            Self::StaleRoute => ErrorCode::StaleRoute,
            Self::UnsupportedOperation { .. } => ErrorCode::UnsupportedOperation,
        }
    }
}
