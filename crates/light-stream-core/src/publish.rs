use serde::{Deserialize, Serialize};

use crate::{
    ClusterId, CommittedBookmark, CommittedRecordRange, DomainError, PartitionKey, PrincipalId,
    ProducerSessionId, RequestSequence,
};

pub const MAX_RECORDS_PER_PUBLISH: usize = 128;
pub const MAX_RECORD_BYTES: usize = 1024 * 1024;
pub const MAX_PUBLISH_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_REQUESTS_PER_REPLICATED_PUBLISH: usize = 128;
pub const MAX_RECORDS_PER_REPLICATED_PUBLISH: usize = 1024;
pub const MAX_REPLICATED_PUBLISH_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct ProducerRequestId {
    principal: PrincipalId,
    session: ProducerSessionId,
    sequence: RequestSequence,
}

impl ProducerRequestId {
    pub const fn new(
        principal: PrincipalId,
        session: ProducerSessionId,
        sequence: RequestSequence,
    ) -> Self {
        Self {
            principal,
            session,
            sequence,
        }
    }

    pub fn principal(&self) -> &PrincipalId {
        &self.principal
    }

    pub const fn session(&self) -> ProducerSessionId {
        self.session
    }

    pub const fn sequence(&self) -> RequestSequence {
        self.sequence
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PublishProbe {
    cluster: ClusterId,
    partition: PartitionKey,
    request: ProducerRequestId,
    records: Vec<Vec<u8>>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PublishBatch {
    cluster: ClusterId,
    partition: PartitionKey,
    request: ProducerRequestId,
    records: Vec<Vec<u8>>,
    bookmark: Option<crate::BookmarkName>,
}

impl PublishBatch {
    pub fn new(
        cluster: ClusterId,
        partition: PartitionKey,
        request: ProducerRequestId,
        records: Vec<Vec<u8>>,
    ) -> Result<Self, DomainError> {
        validate_records(&records)?;
        let records = compact_records(records);
        Ok(Self {
            cluster,
            partition,
            request,
            records,
            bookmark: None,
        })
    }

    pub const fn cluster(&self) -> ClusterId {
        self.cluster
    }

    pub const fn partition(&self) -> PartitionKey {
        self.partition
    }

    pub fn request(&self) -> &ProducerRequestId {
        &self.request
    }

    pub fn records(&self) -> &[Vec<u8>] {
        &self.records
    }

    pub fn payload_bytes(&self) -> usize {
        self.records.iter().map(Vec::len).sum()
    }

    pub fn bookmark(&self) -> Option<&crate::BookmarkName> {
        self.bookmark.as_ref()
    }

    pub fn with_bookmark(mut self, bookmark: crate::BookmarkName) -> Self {
        self.bookmark = Some(bookmark);
        self
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReplicatedPublishBatch {
    requests: Vec<PublishBatch>,
}

impl ReplicatedPublishBatch {
    pub fn new(requests: Vec<PublishBatch>) -> Result<Self, DomainError> {
        if requests.is_empty() {
            return Err(DomainError::InvalidPayload {
                reason: "at least one publish request is required".to_owned(),
            });
        }
        if requests.len() > MAX_REQUESTS_PER_REPLICATED_PUBLISH {
            return Err(DomainError::InvalidPayload {
                reason: format!(
                    "publish request count exceeds {MAX_REQUESTS_PER_REPLICATED_PUBLISH}"
                ),
            });
        }
        let mut records = 0usize;
        let mut payload_bytes = 0usize;
        for request in &requests {
            records = records
                .checked_add(request.records().len())
                .ok_or_else(|| DomainError::InvalidPayload {
                    reason: "replicated publish record count overflow".to_owned(),
                })?;
            payload_bytes = payload_bytes
                .checked_add(request.payload_bytes())
                .ok_or_else(|| DomainError::InvalidPayload {
                    reason: "replicated publish payload byte count overflow".to_owned(),
                })?;
        }
        if records > MAX_RECORDS_PER_REPLICATED_PUBLISH {
            return Err(DomainError::InvalidPayload {
                reason: format!(
                    "replicated publish record count exceeds {MAX_RECORDS_PER_REPLICATED_PUBLISH}"
                ),
            });
        }
        if payload_bytes > MAX_REPLICATED_PUBLISH_BYTES {
            return Err(DomainError::InvalidPayload {
                reason: format!(
                    "replicated publish payload exceeds {MAX_REPLICATED_PUBLISH_BYTES} bytes"
                ),
            });
        }
        Ok(Self { requests })
    }

    pub fn requests(&self) -> &[PublishBatch] {
        &self.requests
    }

    pub fn into_requests(self) -> Vec<PublishBatch> {
        self.requests
    }

    pub fn record_count(&self) -> usize {
        self.requests
            .iter()
            .map(|request| request.records().len())
            .sum()
    }

    pub fn payload_bytes(&self) -> usize {
        self.requests.iter().map(PublishBatch::payload_bytes).sum()
    }
}

impl PublishProbe {
    pub fn new(
        cluster: ClusterId,
        partition: PartitionKey,
        request: ProducerRequestId,
        records: Vec<Vec<u8>>,
    ) -> Result<Self, DomainError> {
        validate_records(&records)?;
        let records = compact_records(records);
        Ok(Self {
            cluster,
            partition,
            request,
            records,
        })
    }

    pub const fn cluster(&self) -> ClusterId {
        self.cluster
    }

    pub const fn partition(&self) -> PartitionKey {
        self.partition
    }

    pub fn request(&self) -> &ProducerRequestId {
        &self.request
    }

    pub fn records(&self) -> &[Vec<u8>] {
        &self.records
    }
}

fn compact_records(records: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
    records
        .into_iter()
        .map(|record| record.into_boxed_slice().into_vec())
        .collect()
}

fn validate_records(records: &[Vec<u8>]) -> Result<(), DomainError> {
    if records.is_empty() {
        return Err(DomainError::InvalidPayload {
            reason: "at least one record is required".to_owned(),
        });
    }
    if records.len() > MAX_RECORDS_PER_PUBLISH {
        return Err(DomainError::InvalidPayload {
            reason: format!("record count exceeds {MAX_RECORDS_PER_PUBLISH}"),
        });
    }
    let mut total = 0usize;
    for record in records {
        if record.is_empty() {
            return Err(DomainError::InvalidPayload {
                reason: "records cannot be empty".to_owned(),
            });
        }
        if record.len() > MAX_RECORD_BYTES {
            return Err(DomainError::InvalidPayload {
                reason: format!("record exceeds {MAX_RECORD_BYTES} bytes"),
            });
        }
        total = total
            .checked_add(record.len())
            .ok_or_else(|| DomainError::InvalidPayload {
                reason: "payload byte count overflow".to_owned(),
            })?;
    }
    if total > MAX_PUBLISH_BYTES {
        return Err(DomainError::InvalidPayload {
            reason: format!("publish payload exceeds {MAX_PUBLISH_BYTES} bytes"),
        });
    }
    Ok(())
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PublishReceipt {
    request: ProducerRequestId,
    range: CommittedRecordRange,
    bookmark: Option<CommittedBookmark>,
}

impl PublishReceipt {
    pub const fn new(
        request: ProducerRequestId,
        range: CommittedRecordRange,
        bookmark: Option<CommittedBookmark>,
    ) -> Self {
        Self {
            request,
            range,
            bookmark,
        }
    }

    pub fn request(&self) -> &ProducerRequestId {
        &self.request
    }

    pub const fn range(&self) -> CommittedRecordRange {
        self.range
    }

    pub fn bookmark(&self) -> Option<&CommittedBookmark> {
        self.bookmark.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{PartitionId, StreamId};

    fn request() -> ProducerRequestId {
        ProducerRequestId::new(
            PrincipalId::parse("local-test").unwrap(),
            "018f3f7e-5b3b-7c11-98f7-b65ac15f65c0".parse().unwrap(),
            RequestSequence::new(1),
        )
    }

    fn partition() -> PartitionKey {
        PartitionKey::new(
            "018f3f7e-5b3b-7c11-98f7-b65ac15f65bf"
                .parse::<StreamId>()
                .unwrap(),
            PartitionId::new(0),
        )
    }

    fn cluster() -> ClusterId {
        "018f3f7e-5b3b-7c11-98f7-b65ac15f65be".parse().unwrap()
    }

    #[test]
    fn publish_probe_requires_bounded_nonempty_records() {
        assert!(PublishProbe::new(cluster(), partition(), request(), Vec::new()).is_err());
        assert!(PublishProbe::new(cluster(), partition(), request(), vec![Vec::new()]).is_err());
        assert!(
            PublishProbe::new(cluster(), partition(), request(), vec![b"record".to_vec()]).is_ok()
        );
    }

    #[test]
    fn replicated_publish_batch_requires_bounded_nonempty_work() {
        let batch = ReplicatedPublishBatch::new(vec![
            PublishBatch::new(
                cluster(),
                partition(),
                request(),
                vec![vec![1; MAX_RECORD_BYTES]],
            )
            .unwrap(),
        ])
        .unwrap();
        assert_eq!(batch.record_count(), 1);
        assert_eq!(batch.payload_bytes(), MAX_RECORD_BYTES);
        assert!(matches!(
            ReplicatedPublishBatch::new(Vec::new()),
            Err(DomainError::InvalidPayload { .. })
        ));
    }

    #[test]
    fn producer_request_round_trips() {
        let encoded = serde_json::to_string(&request()).unwrap();
        let decoded: ProducerRequestId = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, request());
    }

    #[test]
    fn receipt_round_trip_preserves_committed_range() {
        let bookmark = crate::CommittedBookmark::new(
            "018f3f7e-5b3b-7c11-98f7-b65ac15f65c1".parse().unwrap(),
            crate::BookmarkName::parse("after-import").unwrap(),
            crate::CommittedCursor::new(cluster(), partition(), crate::RecordOffset::new(6)),
        );
        let receipt = PublishReceipt::new(
            request(),
            CommittedRecordRange::new(partition(), crate::RecordOffset::new(4), 2).unwrap(),
            Some(bookmark),
        );
        let encoded = serde_json::to_string(&receipt).unwrap();
        let decoded: PublishReceipt = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, receipt);
    }
}
