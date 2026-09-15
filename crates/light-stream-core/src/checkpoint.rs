use std::num::NonZeroU64;

use serde::{Deserialize, Serialize};

use crate::{ClusterId, CommittedCursor, ConsumerId, DomainError, MutationRequestId, PartitionKey};

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct CheckpointKey {
    cluster: ClusterId,
    partition: PartitionKey,
    consumer: ConsumerId,
}

impl CheckpointKey {
    pub const fn new(cluster: ClusterId, partition: PartitionKey, consumer: ConsumerId) -> Self {
        Self {
            cluster,
            partition,
            consumer,
        }
    }

    pub const fn cluster(&self) -> ClusterId {
        self.cluster
    }

    pub const fn partition(&self) -> PartitionKey {
        self.partition
    }

    pub fn consumer(&self) -> &ConsumerId {
        &self.consumer
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct CheckpointRevision(NonZeroU64);

impl CheckpointRevision {
    pub fn new(value: u64) -> Result<Self, DomainError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or_else(|| DomainError::InvalidRange {
                reason: "checkpoint revision must be greater than zero".to_owned(),
            })
    }

    pub const fn initial() -> Self {
        Self(NonZeroU64::MIN)
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }

    pub fn checked_next(self) -> Result<Self, DomainError> {
        self.get()
            .checked_add(1)
            .and_then(NonZeroU64::new)
            .map(Self)
            .ok_or_else(|| DomainError::InvalidRange {
                reason: "checkpoint revision overflow".to_owned(),
            })
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", content = "revision", rename_all = "snake_case")]
pub enum CheckpointExpectation {
    Missing,
    Revision(CheckpointRevision),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CheckpointMutation {
    request: MutationRequestId,
    key: CheckpointKey,
    expected: CheckpointExpectation,
    candidate: CommittedCursor,
}

impl CheckpointMutation {
    pub fn new(
        request: MutationRequestId,
        key: CheckpointKey,
        expected: CheckpointExpectation,
        candidate: CommittedCursor,
    ) -> Result<Self, DomainError> {
        if candidate.cluster() != key.cluster() || candidate.partition() != key.partition() {
            return Err(DomainError::IdentityMismatch {
                reason: "checkpoint candidate does not match its key".to_owned(),
            });
        }
        Ok(Self {
            request,
            key,
            expected,
            candidate,
        })
    }

    pub fn request(&self) -> &MutationRequestId {
        &self.request
    }

    pub fn key(&self) -> &CheckpointKey {
        &self.key
    }

    pub const fn expected(&self) -> CheckpointExpectation {
        self.expected
    }

    pub const fn candidate(&self) -> CommittedCursor {
        self.candidate
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CommittedCheckpoint {
    key: CheckpointKey,
    cursor: CommittedCursor,
    revision: CheckpointRevision,
}

impl CommittedCheckpoint {
    pub const fn new(
        key: CheckpointKey,
        cursor: CommittedCursor,
        revision: CheckpointRevision,
    ) -> Self {
        Self {
            key,
            cursor,
            revision,
        }
    }

    pub fn key(&self) -> &CheckpointKey {
        &self.key
    }

    pub const fn cursor(&self) -> CommittedCursor {
        self.cursor
    }

    pub const fn revision(&self) -> CheckpointRevision {
        self.revision
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CheckpointCasResult {
    Advanced {
        request: MutationRequestId,
        previous: Option<CommittedCheckpoint>,
        checkpoint: CommittedCheckpoint,
    },
    Conflict {
        request: MutationRequestId,
        current: Option<CommittedCheckpoint>,
    },
}

impl CheckpointCasResult {
    pub fn request(&self) -> &MutationRequestId {
        match self {
            Self::Advanced { request, .. } | Self::Conflict { request, .. } => request,
        }
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;
    use crate::{
        MutationSessionId, PartitionId, PrincipalId, RecordOffset, RequestSequence, StreamId,
    };

    #[test]
    fn checkpoint_mutation_requires_one_partition_identity() {
        let cluster = ClusterId::from_uuid(Uuid::new_v4());
        let partition = PartitionKey::new(StreamId::from_uuid(Uuid::new_v4()), PartitionId::new(0));
        let other = PartitionKey::new(StreamId::from_uuid(Uuid::new_v4()), PartitionId::new(0));
        let key = CheckpointKey::new(cluster, partition, ConsumerId::parse("consumer").unwrap());
        let request = MutationRequestId::new(
            PrincipalId::parse("consumer").unwrap(),
            MutationSessionId::from_uuid(Uuid::new_v4()),
            RequestSequence::new(1),
        );
        assert!(matches!(
            CheckpointMutation::new(
                request,
                key,
                CheckpointExpectation::Missing,
                CommittedCursor::new(cluster, other, RecordOffset::new(1)),
            ),
            Err(DomainError::IdentityMismatch { .. })
        ));
    }
}
