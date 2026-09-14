use light_stream_core::{
    ByteCount, DomainError, LeaseDeadline, RecordOffset, ReplayLease, ReplayLeaseId,
    ReplayLeaseLifecycle, ReplayLeaseRequest,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ClockObservation {
    lower_bound: u64,
    upper_bound: u64,
}

impl ClockObservation {
    pub fn new(lower_bound: u64, upper_bound: u64) -> Result<Self, DomainError> {
        if lower_bound > upper_bound {
            return Err(DomainError::LeaseClockUnavailable);
        }
        Ok(Self {
            lower_bound,
            upper_bound,
        })
    }

    pub const fn lower_bound(self) -> u64 {
        self.lower_bound
    }

    pub const fn upper_bound(self) -> u64 {
        self.upper_bound
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct SafeLeaseClock {
    lower_bound: u64,
    upper_bound: u64,
}

impl SafeLeaseClock {
    pub(crate) fn new(observation: ClockObservation) -> Result<Self, DomainError> {
        Ok(Self {
            lower_bound: observation.lower_bound(),
            upper_bound: observation.upper_bound(),
        })
    }

    pub(crate) fn advance(self, observation: ClockObservation) -> Result<Self, DomainError> {
        if observation.upper_bound() < self.lower_bound {
            return Err(DomainError::LeaseClockUnavailable);
        }
        Ok(Self {
            lower_bound: self.lower_bound.max(observation.lower_bound()),
            upper_bound: self.upper_bound.max(observation.upper_bound()),
        })
    }

    pub(crate) const fn lower_bound(self) -> u64 {
        self.lower_bound
    }

    pub(crate) const fn upper_bound(self) -> u64 {
        self.upper_bound
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct RetentionLimits {
    pub(crate) max_lease_bytes: u64,
    pub(crate) max_group_reserved_lease_bytes: u64,
    pub(crate) max_active_leases: u32,
    pub(crate) max_lease_duration_ms: u64,
    pub(crate) max_lease_lifetime_ms: u64,
}

impl Default for RetentionLimits {
    fn default() -> Self {
        Self {
            max_lease_bytes: 512 * 1024 * 1024,
            max_group_reserved_lease_bytes: 4 * 1024 * 1024 * 1024,
            max_active_leases: 256,
            max_lease_duration_ms: 60_000,
            max_lease_lifetime_ms: 15 * 60_000,
        }
    }
}

#[cfg(test)]
impl RetentionLimits {
    fn test_defaults() -> Self {
        Self::default()
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct LeaseBudget {
    pub(crate) active_leases: u32,
    pub(crate) reserved_bytes: u64,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct AdmissionState<'a> {
    pub(crate) floor: RecordOffset,
    pub(crate) tail: RecordOffset,
    pub(crate) clock: &'a SafeLeaseClock,
    pub(crate) limits: &'a RetentionLimits,
    pub(crate) budget: LeaseBudget,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct PartitionRetentionState {
    pub(crate) logical_floor: u64,
    pub(crate) reclaim_cursor: u64,
    pub(crate) logically_expired_bytes: u64,
    pub(crate) raft_only_bytes: u64,
    pub(crate) next_byte_position: u64,
    pub(crate) floor_byte_position: u64,
}

pub(crate) fn admit(
    id: ReplayLeaseId,
    request: ReplayLeaseRequest,
    protected_bytes: ByteCount,
    state: AdmissionState<'_>,
) -> Result<ReplayLease, DomainError> {
    let range = request.range();
    if range.start() < state.floor {
        return Err(DomainError::CursorExpired {
            requested: range.start(),
            available_from: state.floor,
        });
    }
    if range.end() > state.tail {
        return Err(DomainError::InvalidRange {
            reason: "replay lease range extends past the committed tail".to_owned(),
        });
    }
    if request.duration().as_millis() > state.limits.max_lease_duration_ms {
        return Err(DomainError::ResourceLimit {
            resource: "replay_lease_duration_ms".to_owned(),
            limit: state.limits.max_lease_duration_ms,
        });
    }
    if protected_bytes.get() > request.max_bytes().get()
        || protected_bytes.get() > state.limits.max_lease_bytes
    {
        return Err(DomainError::ResourceLimit {
            resource: "replay_lease_bytes".to_owned(),
            limit: request.max_bytes().get().min(state.limits.max_lease_bytes),
        });
    }
    if state.budget.active_leases >= state.limits.max_active_leases {
        return Err(DomainError::ResourceLimit {
            resource: "active_replay_leases".to_owned(),
            limit: u64::from(state.limits.max_active_leases),
        });
    }
    if state
        .budget
        .reserved_bytes
        .saturating_add(protected_bytes.get())
        > state.limits.max_group_reserved_lease_bytes
    {
        return Err(DomainError::ResourceLimit {
            resource: "group_replay_lease_bytes".to_owned(),
            limit: state.limits.max_group_reserved_lease_bytes,
        });
    }
    let expires_at = state
        .clock
        .upper_bound()
        .checked_add(request.duration().as_millis())
        .ok_or(DomainError::LeaseClockUnavailable)?;
    let hard_expires_at = state
        .clock
        .upper_bound()
        .checked_add(state.limits.max_lease_lifetime_ms)
        .ok_or(DomainError::LeaseClockUnavailable)?;
    Ok(ReplayLease::admitted(
        id,
        request,
        protected_bytes,
        LeaseDeadline::new(expires_at),
        LeaseDeadline::new(hard_expires_at),
    ))
}

pub(crate) fn lease_is_effectively_active(lease: &ReplayLease, clock: &SafeLeaseClock) -> bool {
    lease.lifecycle() == ReplayLeaseLifecycle::Active
        && clock.lower_bound() < lease.expires_at().unix_millis()
        && clock.lower_bound() < lease.hard_expires_at().unix_millis()
}

pub(crate) fn advance_floor(
    current: RecordOffset,
    requested: RecordOffset,
    tail: RecordOffset,
) -> Result<RecordOffset, DomainError> {
    if requested > tail {
        return Err(DomainError::InvalidRange {
            reason: "retention floor cannot advance past the committed tail".to_owned(),
        });
    }
    Ok(current.max(requested))
}

#[cfg(test)]
mod tests {
    use super::*;
    use light_stream_core::{
        ByteCount, ByteLimit, ClusterId, LeaseDuration, MutationRequestId, MutationSessionId,
        PartitionId, PartitionKey, PrincipalId, RecordOffset, ReplayLeaseId, ReplayLeaseRequest,
        ReplayRange, RequestSequence, StreamId,
    };
    use uuid::Uuid;

    fn partition() -> PartitionKey {
        PartitionKey::new(
            "018f3f7e-5b3b-7c11-98f7-b65ac15f65bf"
                .parse::<StreamId>()
                .unwrap(),
            PartitionId::new(0),
        )
    }

    fn request(start: u64, end: u64) -> ReplayLeaseRequest {
        ReplayLeaseRequest::new(
            MutationRequestId::new(
                PrincipalId::parse("retention-test").unwrap(),
                MutationSessionId::from_uuid(Uuid::new_v4()),
                RequestSequence::new(1),
            ),
            "018f3f7e-5b3b-7c11-98f7-b65ac15f65be"
                .parse::<ClusterId>()
                .unwrap(),
            ReplayRange::new(
                partition(),
                RecordOffset::new(start),
                RecordOffset::new(end),
            )
            .unwrap(),
            LeaseDuration::from_millis(30_000).unwrap(),
            ByteLimit::new(4_096).unwrap(),
        )
    }

    #[test]
    fn safe_time_never_moves_backward_or_expires_early() {
        let clock = SafeLeaseClock::new(ClockObservation::new(8_000, 12_000).unwrap()).unwrap();
        let lease = admit(
            ReplayLeaseId::from_uuid(Uuid::new_v4()),
            request(2, 5),
            ByteCount::new(300),
            AdmissionState {
                floor: RecordOffset::new(0),
                tail: RecordOffset::new(10),
                clock: &clock,
                limits: &RetentionLimits::test_defaults(),
                budget: LeaseBudget::default(),
            },
        )
        .unwrap();
        assert_eq!(lease.expires_at().unix_millis(), 42_000);
        let advanced = clock
            .advance(ClockObservation::new(7_000, 11_000).unwrap())
            .unwrap();
        assert_eq!(advanced.lower_bound(), 8_000);
        assert!(lease_is_effectively_active(&lease, &advanced));
    }

    #[test]
    fn floor_first_rejects_admission_without_resurrection() {
        let clock = SafeLeaseClock::new(ClockObservation::new(8_000, 12_000).unwrap()).unwrap();
        let error = admit(
            ReplayLeaseId::from_uuid(Uuid::new_v4()),
            request(2, 5),
            ByteCount::new(300),
            AdmissionState {
                floor: RecordOffset::new(4),
                tail: RecordOffset::new(10),
                clock: &clock,
                limits: &RetentionLimits::test_defaults(),
                budget: LeaseBudget::default(),
            },
        )
        .unwrap_err();
        assert!(matches!(
            error,
            light_stream_core::DomainError::CursorExpired {
                requested,
                available_from
            } if requested == RecordOffset::new(2) && available_from == RecordOffset::new(4)
        ));
    }

    #[test]
    fn admission_first_preserves_only_its_bounded_island() {
        let clock = SafeLeaseClock::new(ClockObservation::new(8_000, 12_000).unwrap()).unwrap();
        let lease = admit(
            ReplayLeaseId::from_uuid(Uuid::new_v4()),
            request(2, 5),
            ByteCount::new(300),
            AdmissionState {
                floor: RecordOffset::new(0),
                tail: RecordOffset::new(10),
                clock: &clock,
                limits: &RetentionLimits::test_defaults(),
                budget: LeaseBudget::default(),
            },
        )
        .unwrap();
        let floor = advance_floor(
            RecordOffset::new(0),
            RecordOffset::new(8),
            RecordOffset::new(10),
        )
        .unwrap();
        assert_eq!(floor, RecordOffset::new(8));
        assert!(lease.range().contains(RecordOffset::new(3)));
        assert!(!lease.range().contains(RecordOffset::new(7)));
    }
}
