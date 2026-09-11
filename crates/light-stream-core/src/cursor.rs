use serde::{Deserialize, Serialize};

use crate::{ClusterId, DomainError, PartitionId, StreamId};

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct RecordOffset(u64);

impl RecordOffset {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub fn checked_add(self, count: u64) -> Result<Self, DomainError> {
        self.0
            .checked_add(count)
            .map(Self)
            .ok_or_else(|| DomainError::InvalidRange {
                reason: "record offset overflow".to_owned(),
            })
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct PartitionKey {
    stream: StreamId,
    partition: PartitionId,
}

impl PartitionKey {
    pub const fn new(stream: StreamId, partition: PartitionId) -> Self {
        Self { stream, partition }
    }

    pub const fn stream(self) -> StreamId {
        self.stream
    }

    pub const fn partition(self) -> PartitionId {
        self.partition
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct CommittedCursor {
    cluster: ClusterId,
    partition: PartitionKey,
    next_offset: RecordOffset,
}

impl CommittedCursor {
    pub const fn new(
        cluster: ClusterId,
        partition: PartitionKey,
        next_offset: RecordOffset,
    ) -> Self {
        Self {
            cluster,
            partition,
            next_offset,
        }
    }

    pub const fn cluster(self) -> ClusterId {
        self.cluster
    }

    pub const fn partition(self) -> PartitionKey {
        self.partition
    }

    pub const fn next_offset(self) -> RecordOffset {
        self.next_offset
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CommittedRecordRange {
    partition: PartitionKey,
    first: RecordOffset,
    count: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CommittedRecord {
    offset: RecordOffset,
    payload: Vec<u8>,
}

impl CommittedRecord {
    pub fn new(offset: RecordOffset, payload: Vec<u8>) -> Self {
        Self { offset, payload }
    }

    pub const fn offset(&self) -> RecordOffset {
        self.offset
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FetchPage {
    partition: PartitionKey,
    records: Vec<CommittedRecord>,
    next_offset: RecordOffset,
}

impl FetchPage {
    pub fn new(
        partition: PartitionKey,
        records: Vec<CommittedRecord>,
        next_offset: RecordOffset,
    ) -> Self {
        Self {
            partition,
            records,
            next_offset,
        }
    }

    pub const fn partition(&self) -> PartitionKey {
        self.partition
    }

    pub fn records(&self) -> &[CommittedRecord] {
        &self.records
    }

    pub const fn next_offset(&self) -> RecordOffset {
        self.next_offset
    }
}

impl CommittedRecordRange {
    pub fn new(
        partition: PartitionKey,
        first: RecordOffset,
        count: u64,
    ) -> Result<Self, DomainError> {
        if count == 0 {
            return Err(DomainError::InvalidRange {
                reason: "a committed range cannot be empty".to_owned(),
            });
        }
        first.checked_add(count)?;
        Ok(Self {
            partition,
            first,
            count,
        })
    }

    pub const fn partition(self) -> PartitionKey {
        self.partition
    }

    pub const fn first(self) -> RecordOffset {
        self.first
    }

    pub const fn count(self) -> u64 {
        self.count
    }

    pub fn next(self) -> RecordOffset {
        self.first
            .checked_add(self.count)
            .expect("constructor proves the committed range cannot overflow")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StreamId;

    fn partition() -> PartitionKey {
        PartitionKey::new(
            "018f3f7e-5b3b-7c11-98f7-b65ac15f65bf"
                .parse::<StreamId>()
                .unwrap(),
            PartitionId::new(2),
        )
    }

    #[test]
    fn committed_range_is_nonempty_and_ordered() {
        assert!(CommittedRecordRange::new(partition(), RecordOffset::new(7), 0).is_err());
        let range = CommittedRecordRange::new(partition(), RecordOffset::new(7), 3).unwrap();
        assert_eq!(range.next(), RecordOffset::new(10));
    }

    #[test]
    fn committed_range_rejects_offset_overflow() {
        assert!(CommittedRecordRange::new(partition(), RecordOffset::new(u64::MAX), 1).is_err());
    }
}
