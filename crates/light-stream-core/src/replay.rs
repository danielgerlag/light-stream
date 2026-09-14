use std::num::NonZeroU64;

use serde::{Deserialize, Serialize};

use crate::{
    ClusterId, DomainError, MutationSessionId, PartitionKey, PrincipalId, ProducerRequestId,
    RecordOffset, ReplayLeaseId, RequestSequence,
};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AmbiguousRequest {
    Publish { request: ProducerRequestId },
    Mutation { request: MutationRequestId },
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct MutationRequestId {
    principal: PrincipalId,
    session: MutationSessionId,
    sequence: RequestSequence,
}

impl MutationRequestId {
    pub const fn new(
        principal: PrincipalId,
        session: MutationSessionId,
        sequence: RequestSequence,
    ) -> Self {
        Self {
            principal,
            session,
            sequence,
        }
    }

    pub const fn principal(&self) -> &PrincipalId {
        &self.principal
    }

    pub const fn session(&self) -> MutationSessionId {
        self.session
    }

    pub const fn sequence(&self) -> RequestSequence {
        self.sequence
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct ByteCount(u64);

impl ByteCount {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct ByteLimit(NonZeroU64);

impl ByteLimit {
    pub fn new(value: u64) -> Result<Self, DomainError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or_else(|| DomainError::InvalidRange {
                reason: "byte limit must be greater than zero".to_owned(),
            })
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct LeaseDuration(NonZeroU64);

impl LeaseDuration {
    pub fn from_millis(value: u64) -> Result<Self, DomainError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or_else(|| DomainError::InvalidRange {
                reason: "lease duration must be greater than zero".to_owned(),
            })
    }

    pub const fn as_millis(self) -> u64 {
        self.0.get()
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct LeaseDeadline(u64);

impl LeaseDeadline {
    pub const fn new(unix_millis: u64) -> Self {
        Self(unix_millis)
    }

    pub const fn unix_millis(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct LeaseGeneration(u64);

impl LeaseGeneration {
    pub const fn initial() -> Self {
        Self(1)
    }

    pub const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub const fn new(value: u64) -> Self {
        Self(value)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReplayRange {
    partition: PartitionKey,
    start: RecordOffset,
    end: RecordOffset,
}

impl ReplayRange {
    pub fn new(
        partition: PartitionKey,
        start: RecordOffset,
        end: RecordOffset,
    ) -> Result<Self, DomainError> {
        if start >= end {
            return Err(DomainError::InvalidRange {
                reason: "replay range must be nonempty and ordered".to_owned(),
            });
        }
        Ok(Self {
            partition,
            start,
            end,
        })
    }

    pub const fn partition(self) -> PartitionKey {
        self.partition
    }

    pub const fn start(self) -> RecordOffset {
        self.start
    }

    pub const fn end(self) -> RecordOffset {
        self.end
    }

    pub const fn contains(self, offset: RecordOffset) -> bool {
        offset.get() >= self.start.get() && offset.get() < self.end.get()
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReplayLeaseRequest {
    request: MutationRequestId,
    cluster: ClusterId,
    range: ReplayRange,
    duration: LeaseDuration,
    max_bytes: ByteLimit,
}

impl ReplayLeaseRequest {
    pub const fn new(
        request: MutationRequestId,
        cluster: ClusterId,
        range: ReplayRange,
        duration: LeaseDuration,
        max_bytes: ByteLimit,
    ) -> Self {
        Self {
            request,
            cluster,
            range,
            duration,
            max_bytes,
        }
    }

    pub const fn request(&self) -> &MutationRequestId {
        &self.request
    }

    pub const fn cluster(&self) -> ClusterId {
        self.cluster
    }

    pub const fn range(&self) -> ReplayRange {
        self.range
    }

    pub const fn duration(&self) -> LeaseDuration {
        self.duration
    }

    pub const fn max_bytes(&self) -> ByteLimit {
        self.max_bytes
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayLeaseLifecycle {
    Active,
    Released,
    Expired,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReplayLease {
    id: ReplayLeaseId,
    request: ReplayLeaseRequest,
    protected_bytes: ByteCount,
    generation: LeaseGeneration,
    expires_at: LeaseDeadline,
    hard_expires_at: LeaseDeadline,
    lifecycle: ReplayLeaseLifecycle,
}

impl ReplayLease {
    pub const fn admitted(
        id: ReplayLeaseId,
        request: ReplayLeaseRequest,
        protected_bytes: ByteCount,
        expires_at: LeaseDeadline,
        hard_expires_at: LeaseDeadline,
    ) -> Self {
        Self {
            id,
            request,
            protected_bytes,
            generation: LeaseGeneration::initial(),
            expires_at,
            hard_expires_at,
            lifecycle: ReplayLeaseLifecycle::Active,
        }
    }

    pub const fn restored(
        id: ReplayLeaseId,
        request: ReplayLeaseRequest,
        protected_bytes: ByteCount,
        generation: LeaseGeneration,
        expires_at: LeaseDeadline,
        hard_expires_at: LeaseDeadline,
        lifecycle: ReplayLeaseLifecycle,
    ) -> Self {
        Self {
            id,
            request,
            protected_bytes,
            generation,
            expires_at,
            hard_expires_at,
            lifecycle,
        }
    }

    pub const fn id(&self) -> ReplayLeaseId {
        self.id
    }

    pub const fn request(&self) -> &ReplayLeaseRequest {
        &self.request
    }

    pub const fn range(&self) -> ReplayRange {
        self.request.range()
    }

    pub const fn protected_bytes(&self) -> ByteCount {
        self.protected_bytes
    }

    pub const fn generation(&self) -> LeaseGeneration {
        self.generation
    }

    pub const fn expires_at(&self) -> LeaseDeadline {
        self.expires_at
    }

    pub const fn hard_expires_at(&self) -> LeaseDeadline {
        self.hard_expires_at
    }

    pub const fn lifecycle(&self) -> ReplayLeaseLifecycle {
        self.lifecycle
    }

    pub fn renewed(
        mut self,
        expires_at: LeaseDeadline,
        hard_expires_at: LeaseDeadline,
    ) -> Result<Self, DomainError> {
        if self.lifecycle != ReplayLeaseLifecycle::Active {
            return Err(DomainError::InvalidRange {
                reason: "a terminal replay lease cannot be renewed".to_owned(),
            });
        }
        if expires_at <= self.expires_at || expires_at > hard_expires_at {
            return Err(DomainError::InvalidRange {
                reason: "renewal must extend the lease without exceeding its hard expiry"
                    .to_owned(),
            });
        }
        self.expires_at = expires_at;
        self.hard_expires_at = hard_expires_at;
        self.generation = self.generation.next();
        Ok(self)
    }

    pub fn released(mut self) -> Self {
        self.lifecycle = ReplayLeaseLifecycle::Released;
        self.generation = self.generation.next();
        self
    }

    pub fn expired(mut self) -> Self {
        self.lifecycle = ReplayLeaseLifecycle::Expired;
        self.generation = self.generation.next();
        self
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LeaseRenewal {
    request: MutationRequestId,
    partition: PartitionKey,
    lease: ReplayLeaseId,
    duration: LeaseDuration,
}

impl LeaseRenewal {
    pub const fn new(
        request: MutationRequestId,
        partition: PartitionKey,
        lease: ReplayLeaseId,
        duration: LeaseDuration,
    ) -> Self {
        Self {
            request,
            partition,
            lease,
            duration,
        }
    }

    pub const fn request(&self) -> &MutationRequestId {
        &self.request
    }

    pub const fn lease(&self) -> ReplayLeaseId {
        self.lease
    }

    pub const fn partition(&self) -> PartitionKey {
        self.partition
    }

    pub const fn duration(&self) -> LeaseDuration {
        self.duration
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LeaseRelease {
    request: MutationRequestId,
    partition: PartitionKey,
    lease: ReplayLeaseId,
}

impl LeaseRelease {
    pub const fn new(
        request: MutationRequestId,
        partition: PartitionKey,
        lease: ReplayLeaseId,
    ) -> Self {
        Self {
            request,
            partition,
            lease,
        }
    }

    pub const fn request(&self) -> &MutationRequestId {
        &self.request
    }

    pub const fn lease(&self) -> ReplayLeaseId {
        self.lease
    }

    pub const fn partition(&self) -> PartitionKey {
        self.partition
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RetentionRequest {
    request: MutationRequestId,
    partition: PartitionKey,
    target_floor: RecordOffset,
}

impl RetentionRequest {
    pub const fn new(
        request: MutationRequestId,
        partition: PartitionKey,
        target_floor: RecordOffset,
    ) -> Self {
        Self {
            request,
            partition,
            target_floor,
        }
    }

    pub const fn request(&self) -> &MutationRequestId {
        &self.request
    }

    pub const fn partition(&self) -> PartitionKey {
        self.partition
    }

    pub const fn target_floor(&self) -> RecordOffset {
        self.target_floor
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RetentionResult {
    request: MutationRequestId,
    partition: PartitionKey,
    previous_floor: RecordOffset,
    floor: RecordOffset,
}

impl RetentionResult {
    pub const fn new(
        request: MutationRequestId,
        partition: PartitionKey,
        previous_floor: RecordOffset,
        floor: RecordOffset,
    ) -> Self {
        Self {
            request,
            partition,
            previous_floor,
            floor,
        }
    }

    pub const fn request(&self) -> &MutationRequestId {
        &self.request
    }

    pub const fn partition(&self) -> PartitionKey {
        self.partition
    }

    pub const fn previous_floor(&self) -> RecordOffset {
        self.previous_floor
    }

    pub const fn floor(&self) -> RecordOffset {
        self.floor
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RetentionStatus {
    partition: PartitionKey,
    logical_floor: RecordOffset,
    reclaim_cursor: RecordOffset,
    logically_expired_bytes: ByteCount,
    raft_only_bytes: ByteCount,
}

impl RetentionStatus {
    pub const fn new(
        partition: PartitionKey,
        logical_floor: RecordOffset,
        reclaim_cursor: RecordOffset,
        logically_expired_bytes: ByteCount,
        raft_only_bytes: ByteCount,
    ) -> Self {
        Self {
            partition,
            logical_floor,
            reclaim_cursor,
            logically_expired_bytes,
            raft_only_bytes,
        }
    }

    pub const fn partition(&self) -> PartitionKey {
        self.partition
    }

    pub const fn logical_floor(&self) -> RecordOffset {
        self.logical_floor
    }

    pub const fn reclaim_cursor(&self) -> RecordOffset {
        self.reclaim_cursor
    }

    pub const fn logically_expired_bytes(&self) -> ByteCount {
        self.logically_expired_bytes
    }

    pub const fn raft_only_bytes(&self) -> ByteCount {
        self.raft_only_bytes
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayAvailability {
    Available,
    Expired { available_from: RecordOffset },
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProtectedFetchRequest {
    cluster: ClusterId,
    partition: PartitionKey,
    lease: ReplayLeaseId,
    offset: RecordOffset,
    limit: u32,
}

impl ProtectedFetchRequest {
    pub const fn new(
        cluster: ClusterId,
        partition: PartitionKey,
        lease: ReplayLeaseId,
        offset: RecordOffset,
        limit: u32,
    ) -> Self {
        Self {
            cluster,
            partition,
            lease,
            offset,
            limit,
        }
    }

    pub const fn cluster(self) -> ClusterId {
        self.cluster
    }

    pub const fn partition(self) -> PartitionKey {
        self.partition
    }

    pub const fn lease(self) -> ReplayLeaseId {
        self.lease
    }

    pub const fn offset(self) -> RecordOffset {
        self.offset
    }

    pub const fn limit(self) -> u32 {
        self.limit
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ClusterId, PartitionId, PartitionKey, PrincipalId, RecordOffset, RequestSequence, StreamId,
    };

    fn partition() -> PartitionKey {
        PartitionKey::new(
            "018f3f7e-5b3b-7c11-98f7-b65ac15f65bf"
                .parse::<StreamId>()
                .unwrap(),
            PartitionId::new(0),
        )
    }

    fn mutation(sequence: u64) -> MutationRequestId {
        MutationRequestId::new(
            PrincipalId::parse("retention-test").unwrap(),
            "018f3f7e-5b3b-7c11-98f7-b65ac15f65c0".parse().unwrap(),
            RequestSequence::new(sequence),
        )
    }

    #[test]
    fn replay_range_is_nonempty_and_partition_local() {
        let key = partition();
        assert!(ReplayRange::new(key, RecordOffset::new(2), RecordOffset::new(2)).is_err());
        let range = ReplayRange::new(key, RecordOffset::new(2), RecordOffset::new(5)).unwrap();
        assert_eq!(range.partition(), key);
        assert_eq!(range.start(), RecordOffset::new(2));
        assert_eq!(range.end(), RecordOffset::new(5));
    }

    #[test]
    fn replay_lease_terminal_states_cannot_be_renewed() {
        let cluster: ClusterId = "018f3f7e-5b3b-7c11-98f7-b65ac15f65be".parse().unwrap();
        let request = ReplayLeaseRequest::new(
            mutation(1),
            cluster,
            ReplayRange::new(partition(), RecordOffset::new(2), RecordOffset::new(5)).unwrap(),
            LeaseDuration::from_millis(30_000).unwrap(),
            ByteLimit::new(1_024).unwrap(),
        );
        let lease = ReplayLease::admitted(
            ReplayLeaseId::from_uuid(uuid::Uuid::new_v4()),
            request,
            ByteCount::new(300),
            LeaseDeadline::new(50_000),
            LeaseDeadline::new(500_000),
        );
        let released = lease.released();
        assert_eq!(released.lifecycle(), ReplayLeaseLifecycle::Released);
        assert!(
            released
                .renewed(LeaseDeadline::new(60_000), LeaseDeadline::new(500_000))
                .is_err()
        );
    }

    #[test]
    fn mutation_identity_round_trips_without_losing_sequence() {
        let request = mutation(42);
        let encoded = serde_json::to_string(&request).unwrap();
        let decoded: MutationRequestId = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, request);
        assert_eq!(decoded.sequence(), RequestSequence::new(42));
    }
}
