use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{
    BookmarkId, BookmarkName, BookmarkPublicationSequence, ClusterId, CommittedCursor, DomainError,
    PartitionId, StreamId,
};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CommittedBookmark {
    id: BookmarkId,
    name: BookmarkName,
    cursor: CommittedCursor,
    publication: BookmarkPublicationSequence,
    lifecycle: BookmarkLifecycle,
}

impl CommittedBookmark {
    pub const fn new(id: BookmarkId, name: BookmarkName, cursor: CommittedCursor) -> Self {
        Self {
            id,
            name,
            cursor,
            publication: BookmarkPublicationSequence::new(1),
            lifecycle: BookmarkLifecycle::Available,
        }
    }

    pub const fn published(
        id: BookmarkId,
        name: BookmarkName,
        cursor: CommittedCursor,
        publication: BookmarkPublicationSequence,
    ) -> Self {
        Self {
            id,
            name,
            cursor,
            publication,
            lifecycle: BookmarkLifecycle::Available,
        }
    }

    pub const fn id(&self) -> BookmarkId {
        self.id
    }

    pub fn name(&self) -> &BookmarkName {
        &self.name
    }

    pub const fn cursor(&self) -> CommittedCursor {
        self.cursor
    }

    pub const fn publication(&self) -> BookmarkPublicationSequence {
        self.publication
    }

    pub const fn lifecycle(&self) -> BookmarkLifecycle {
        self.lifecycle
    }

    pub fn mark_deleted(&mut self) {
        self.lifecycle = BookmarkLifecycle::Deleted;
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BookmarkLifecycle {
    Available,
    Deleted,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CreateBookmarkSpec {
    id: BookmarkId,
    partition: crate::PartitionKey,
    name: BookmarkName,
    offset: crate::RecordOffset,
}

impl CreateBookmarkSpec {
    pub const fn new(
        id: BookmarkId,
        partition: crate::PartitionKey,
        name: BookmarkName,
        offset: crate::RecordOffset,
    ) -> Self {
        Self {
            id,
            partition,
            name,
            offset,
        }
    }

    pub const fn id(&self) -> BookmarkId {
        self.id
    }

    pub const fn partition(&self) -> crate::PartitionKey {
        self.partition
    }

    pub fn name(&self) -> &BookmarkName {
        &self.name
    }

    pub const fn offset(&self) -> crate::RecordOffset {
        self.offset
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BookmarkPageRequest {
    partition: crate::PartitionKey,
    limit: u32,
    publication_ceiling: Option<BookmarkPublicationSequence>,
    before: Option<BookmarkPublicationSequence>,
}

impl BookmarkPageRequest {
    pub fn new(
        partition: crate::PartitionKey,
        limit: u32,
        publication_ceiling: Option<BookmarkPublicationSequence>,
        before: Option<BookmarkPublicationSequence>,
    ) -> Result<Self, DomainError> {
        if limit == 0 || limit > 1000 {
            return Err(DomainError::InvalidRange {
                reason: "bookmark page limit must be between 1 and 1000".to_owned(),
            });
        }
        Ok(Self {
            partition,
            limit,
            publication_ceiling,
            before,
        })
    }

    pub const fn partition(&self) -> crate::PartitionKey {
        self.partition
    }

    pub const fn limit(&self) -> u32 {
        self.limit
    }

    pub const fn publication_ceiling(&self) -> Option<BookmarkPublicationSequence> {
        self.publication_ceiling
    }

    pub const fn before(&self) -> Option<BookmarkPublicationSequence> {
        self.before
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BookmarkPage {
    items: Vec<CommittedBookmark>,
    publication_ceiling: BookmarkPublicationSequence,
    next_before: Option<BookmarkPublicationSequence>,
}

impl BookmarkPage {
    pub fn new(
        items: Vec<CommittedBookmark>,
        publication_ceiling: BookmarkPublicationSequence,
        next_before: Option<BookmarkPublicationSequence>,
    ) -> Self {
        Self {
            items,
            publication_ceiling,
            next_before,
        }
    }

    pub fn items(&self) -> &[CommittedBookmark] {
        &self.items
    }

    pub const fn publication_ceiling(&self) -> BookmarkPublicationSequence {
        self.publication_ceiling
    }

    pub const fn next_before(&self) -> Option<BookmarkPublicationSequence> {
        self.next_before
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StreamCursorVector {
    cluster: ClusterId,
    stream: StreamId,
    positions: Vec<CommittedCursor>,
}

impl StreamCursorVector {
    pub fn new(stream: StreamId, positions: Vec<CommittedCursor>) -> Result<Self, DomainError> {
        if positions.is_empty() {
            return Err(DomainError::InvalidRange {
                reason: "a stream cursor vector needs at least one partition".to_owned(),
            });
        }
        let cluster = positions[0].cluster();
        let mut partitions = BTreeSet::<PartitionId>::new();
        for cursor in &positions {
            if cursor.cluster() != cluster {
                return Err(DomainError::InvalidRange {
                    reason: "every cursor must belong to the vector cluster".to_owned(),
                });
            }
            if cursor.partition().stream() != stream {
                return Err(DomainError::InvalidRange {
                    reason: "every cursor must belong to the vector stream".to_owned(),
                });
            }
            if !partitions.insert(cursor.partition().partition()) {
                return Err(DomainError::InvalidRange {
                    reason: "a stream cursor vector cannot repeat a partition".to_owned(),
                });
            }
        }
        Ok(Self {
            cluster,
            stream,
            positions,
        })
    }

    pub const fn cluster(&self) -> ClusterId {
        self.cluster
    }

    pub const fn stream(&self) -> StreamId {
        self.stream
    }

    pub fn positions(&self) -> &[CommittedCursor] {
        &self.positions
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CommittedStreamBookmark {
    id: BookmarkId,
    name: BookmarkName,
    vector: StreamCursorVector,
    publication: BookmarkPublicationSequence,
    lifecycle: BookmarkLifecycle,
}

impl CommittedStreamBookmark {
    pub const fn published(
        id: BookmarkId,
        name: BookmarkName,
        vector: StreamCursorVector,
        publication: BookmarkPublicationSequence,
    ) -> Self {
        Self {
            id,
            name,
            vector,
            publication,
            lifecycle: BookmarkLifecycle::Available,
        }
    }

    pub const fn id(&self) -> BookmarkId {
        self.id
    }

    pub fn name(&self) -> &BookmarkName {
        &self.name
    }

    pub const fn vector(&self) -> &StreamCursorVector {
        &self.vector
    }

    pub const fn publication(&self) -> BookmarkPublicationSequence {
        self.publication
    }

    pub const fn lifecycle(&self) -> BookmarkLifecycle {
        self.lifecycle
    }

    pub fn mark_deleted(&mut self) {
        self.lifecycle = BookmarkLifecycle::Deleted;
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StreamBookmarkPageRequest {
    cluster: ClusterId,
    stream: StreamId,
    limit: u32,
    publication_ceiling: Option<BookmarkPublicationSequence>,
    before: Option<BookmarkPublicationSequence>,
}

impl StreamBookmarkPageRequest {
    pub fn new(
        cluster: ClusterId,
        stream: StreamId,
        limit: u32,
        publication_ceiling: Option<BookmarkPublicationSequence>,
        before: Option<BookmarkPublicationSequence>,
    ) -> Result<Self, DomainError> {
        if limit == 0 || limit > 1000 {
            return Err(DomainError::InvalidRange {
                reason: "stream bookmark page limit must be between 1 and 1000".to_owned(),
            });
        }
        Ok(Self {
            cluster,
            stream,
            limit,
            publication_ceiling,
            before,
        })
    }

    pub const fn cluster(&self) -> ClusterId {
        self.cluster
    }

    pub const fn stream(&self) -> StreamId {
        self.stream
    }

    pub const fn limit(&self) -> u32 {
        self.limit
    }

    pub const fn publication_ceiling(&self) -> Option<BookmarkPublicationSequence> {
        self.publication_ceiling
    }

    pub const fn before(&self) -> Option<BookmarkPublicationSequence> {
        self.before
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StreamBookmarkPage {
    items: Vec<CommittedStreamBookmark>,
    publication_ceiling: BookmarkPublicationSequence,
    next_before: Option<BookmarkPublicationSequence>,
}

impl StreamBookmarkPage {
    pub fn new(
        items: Vec<CommittedStreamBookmark>,
        publication_ceiling: BookmarkPublicationSequence,
        next_before: Option<BookmarkPublicationSequence>,
    ) -> Self {
        Self {
            items,
            publication_ceiling,
            next_before,
        }
    }

    pub fn items(&self) -> &[CommittedStreamBookmark] {
        &self.items
    }

    pub const fn publication_ceiling(&self) -> BookmarkPublicationSequence {
        self.publication_ceiling
    }

    pub const fn next_before(&self) -> Option<BookmarkPublicationSequence> {
        self.next_before
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BookmarkTarget {
    Partition { cursor: CommittedCursor },
    IndependentVector { vector: StreamCursorVector },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{PartitionKey, RecordOffset};

    fn cursor(cluster: &str, stream: StreamId, partition: u32) -> CommittedCursor {
        CommittedCursor::new(
            cluster.parse().unwrap(),
            PartitionKey::new(stream, PartitionId::new(partition)),
            RecordOffset::new(10),
        )
    }

    #[test]
    fn stream_vector_requires_one_cluster_and_unique_partitions() {
        let stream: StreamId = "018f3f7e-5b3b-7c11-98f7-b65ac15f65bf".parse().unwrap();
        let first = cursor("018f3f7e-5b3b-7c11-98f7-b65ac15f65be", stream, 0);
        let another_cluster = cursor("018f3f7e-5b3b-7c11-98f7-b65ac15f65bd", stream, 1);
        assert!(StreamCursorVector::new(stream, vec![first, another_cluster]).is_err());
        assert!(StreamCursorVector::new(stream, vec![first, first]).is_err());
    }
}
