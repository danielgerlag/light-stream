use std::sync::{Arc, Mutex};

use light_stream_core::{DomainError, NodePhase, ReadinessReason, RequestOutcome, WriteReadiness};
use tokio::{
    sync::{Notify, watch},
    time::Instant,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OperationalState {
    pub generation: u64,
    pub phase: NodePhase,
    pub readiness: WriteReadiness,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DrainOutcome {
    Completed { accepted: u64 },
    DeadlineExceeded { accepted: u64, unresolved: u64 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DrainTicket {
    accepted: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MutationAdmission {
    Open,
    Closed,
}

impl MutationAdmission {
    pub(crate) const fn is_open(self) -> bool {
        matches!(self, Self::Open)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AdmissionDrainSnapshot {
    pub(crate) admission: MutationAdmission,
    pub(crate) mutations_in_flight: u64,
}

#[derive(Clone)]
pub(crate) struct LifecycleController {
    state: watch::Sender<OperationalState>,
    gate: Arc<MutationGate>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ReadinessSampleTicket {
    generation: u64,
}

impl LifecycleController {
    pub(crate) fn starting() -> Self {
        let (state, _) = watch::channel(OperationalState {
            generation: 0,
            phase: NodePhase::Starting,
            readiness: WriteReadiness::NotReady {
                reasons: vec![ReadinessReason::Starting],
            },
        });
        Self {
            state,
            gate: Arc::new(MutationGate::open()),
        }
    }

    pub(crate) fn snapshot(&self) -> OperationalState {
        self.state.borrow().clone()
    }

    pub(crate) fn admission_drain_snapshot(&self) -> AdmissionDrainSnapshot {
        let state = self
            .gate
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        AdmissionDrainSnapshot {
            admission: state.admission,
            mutations_in_flight: state.in_flight,
        }
    }

    pub(crate) fn mark_running(&self, readiness: WriteReadiness) {
        self.state.send_modify(|state| {
            state.generation = state.generation.saturating_add(1);
            state.phase = NodePhase::Running;
            state.readiness = readiness.clone();
        });
    }

    pub(crate) fn begin_readiness_sample(&self) -> Option<ReadinessSampleTicket> {
        let state = self.state.borrow();
        (state.phase == NodePhase::Running).then_some(ReadinessSampleTicket {
            generation: state.generation,
        })
    }

    pub(crate) fn commit_readiness(
        &self,
        ticket: ReadinessSampleTicket,
        readiness: WriteReadiness,
    ) -> bool {
        let mut accepted = false;
        self.state.send_modify(|state| {
            if state.phase != NodePhase::Running || state.generation != ticket.generation {
                return;
            }
            accepted = true;
            if state.readiness != readiness {
                state.generation = state.generation.saturating_add(1);
                state.readiness = readiness.clone();
            }
        });
        accepted
    }

    pub(crate) fn try_admit_mutation(&self) -> Result<MutationPermit, DomainError> {
        self.gate.try_admit()
    }

    pub(crate) fn start_drain(&self) -> DrainTicket {
        let accepted = self.gate.close();
        self.state.send_modify(|state| {
            state.generation = state.generation.saturating_add(1);
            state.phase = NodePhase::Draining;
            state.readiness = WriteReadiness::NotReady {
                reasons: vec![ReadinessReason::Draining],
            };
        });
        DrainTicket { accepted }
    }

    pub(crate) async fn finish_drain(
        &self,
        ticket: DrainTicket,
        deadline: Instant,
    ) -> DrainOutcome {
        let unresolved = self.gate.wait_drained(deadline).await;
        if unresolved == 0 {
            DrainOutcome::Completed {
                accepted: ticket.accepted,
            }
        } else {
            DrainOutcome::DeadlineExceeded {
                accepted: ticket.accepted,
                unresolved,
            }
        }
    }

    #[cfg(test)]
    pub(crate) async fn begin_drain(&self, deadline: Instant) -> DrainOutcome {
        let ticket = self.start_drain();
        self.finish_drain(ticket, deadline).await
    }

    pub(crate) fn mark_stopping(&self) {
        self.state.send_modify(|state| {
            state.generation = state.generation.saturating_add(1);
            state.phase = NodePhase::Stopping;
        });
    }
}

struct MutationGate {
    state: Mutex<MutationGateState>,
    drained: Notify,
}

struct MutationGateState {
    admission: MutationAdmission,
    accepted: u64,
    in_flight: u64,
}

impl MutationGate {
    fn open() -> Self {
        Self {
            state: Mutex::new(MutationGateState {
                admission: MutationAdmission::Open,
                accepted: 0,
                in_flight: 0,
            }),
            drained: Notify::new(),
        }
    }

    fn try_admit(self: &Arc<Self>) -> Result<MutationPermit, DomainError> {
        let mut state = self.state.lock().map_err(|_| DomainError::Storage {
            reason: "mutation admission lock is poisoned".to_owned(),
        })?;
        if state.admission == MutationAdmission::Closed {
            return Err(DomainError::ShuttingDown {
                outcome: RequestOutcome::DefiniteNoCommit,
            });
        }
        state.accepted = state.accepted.saturating_add(1);
        state.in_flight = state.in_flight.saturating_add(1);
        Ok(MutationPermit {
            _lease: Arc::new(MutationLease { gate: self.clone() }),
        })
    }

    fn close(&self) -> u64 {
        let Ok(mut state) = self.state.lock() else {
            return 0;
        };
        state.admission = MutationAdmission::Closed;
        state.accepted
    }

    async fn wait_drained(&self, deadline: Instant) -> u64 {
        loop {
            let notified = self.drained.notified();
            let in_flight = self.state.lock().map_or(u64::MAX, |state| state.in_flight);
            if in_flight == 0 {
                return 0;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return self.state.lock().map_or(u64::MAX, |state| state.in_flight);
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct MutationPermit {
    _lease: Arc<MutationLease>,
}

struct MutationLease {
    gate: Arc<MutationGate>,
}

impl Drop for MutationLease {
    fn drop(&mut self) {
        let Ok(mut state) = self.gate.state.lock() else {
            return;
        };
        state.in_flight = state.in_flight.saturating_sub(1);
        if state.in_flight == 0 {
            self.gate.drained.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn admission_drain_snapshot_tracks_the_existing_gate() {
        let lifecycle = LifecycleController::starting();

        assert_eq!(
            lifecycle.admission_drain_snapshot(),
            AdmissionDrainSnapshot {
                admission: MutationAdmission::Open,
                mutations_in_flight: 0,
            }
        );

        let permit = lifecycle.try_admit_mutation().unwrap();
        assert_eq!(
            lifecycle.admission_drain_snapshot(),
            AdmissionDrainSnapshot {
                admission: MutationAdmission::Open,
                mutations_in_flight: 1,
            }
        );

        drop(permit);
        assert_eq!(lifecycle.admission_drain_snapshot().mutations_in_flight, 0);
    }

    #[test]
    fn submitted_mutation_clone_keeps_admission_in_flight() {
        let lifecycle = LifecycleController::starting();
        let request_permit = lifecycle.try_admit_mutation().unwrap();
        let submitted_permit = request_permit.clone();

        drop(request_permit);
        assert_eq!(lifecycle.admission_drain_snapshot().mutations_in_flight, 1);

        drop(submitted_permit);
        assert_eq!(lifecycle.admission_drain_snapshot().mutations_in_flight, 0);
    }

    #[tokio::test]
    async fn drain_rejects_new_mutations_and_waits_for_accepted_work() {
        let lifecycle = LifecycleController::starting();
        lifecycle.mark_running(WriteReadiness::Ready);
        let permit = lifecycle.try_admit_mutation().unwrap();
        let draining = {
            let lifecycle = lifecycle.clone();
            tokio::spawn(async move {
                lifecycle
                    .begin_drain(Instant::now() + Duration::from_secs(1))
                    .await
            })
        };
        tokio::task::yield_now().await;

        assert_eq!(
            lifecycle.try_admit_mutation().err().unwrap(),
            DomainError::ShuttingDown {
                outcome: RequestOutcome::DefiniteNoCommit,
            }
        );
        assert!(!draining.is_finished());
        drop(permit);
        assert_eq!(
            draining.await.unwrap(),
            DrainOutcome::Completed { accepted: 1 }
        );
        assert_eq!(lifecycle.snapshot().phase, NodePhase::Draining);
        assert_eq!(
            lifecycle.admission_drain_snapshot(),
            AdmissionDrainSnapshot {
                admission: MutationAdmission::Closed,
                mutations_in_flight: 0,
            }
        );
    }

    #[tokio::test]
    async fn deadline_exceeded_drain_preserves_unresolved_count() {
        let lifecycle = LifecycleController::starting();
        let permit = lifecycle.try_admit_mutation().unwrap();

        assert_eq!(
            lifecycle.begin_drain(Instant::now()).await,
            DrainOutcome::DeadlineExceeded {
                accepted: 1,
                unresolved: 1,
            }
        );
        assert_eq!(
            lifecycle.admission_drain_snapshot(),
            AdmissionDrainSnapshot {
                admission: MutationAdmission::Closed,
                mutations_in_flight: 1,
            }
        );

        drop(permit);
        assert_eq!(
            lifecycle.admission_drain_snapshot(),
            AdmissionDrainSnapshot {
                admission: MutationAdmission::Closed,
                mutations_in_flight: 0,
            }
        );
    }

    #[test]
    fn older_readiness_sample_cannot_overwrite_newer_state() {
        let lifecycle = LifecycleController::starting();
        lifecycle.mark_running(WriteReadiness::Ready);
        let older = lifecycle.begin_readiness_sample().unwrap();
        let newer = lifecycle.begin_readiness_sample().unwrap();
        let not_ready = WriteReadiness::NotReady {
            reasons: vec![ReadinessReason::SecurityPolicyStale],
        };

        assert!(lifecycle.commit_readiness(newer, not_ready.clone()));
        assert!(!lifecycle.commit_readiness(older, WriteReadiness::Ready));
        assert_eq!(lifecycle.snapshot().readiness, not_ready);
    }

    #[tokio::test]
    async fn readiness_sample_cannot_overwrite_drain() {
        let lifecycle = LifecycleController::starting();
        lifecycle.mark_running(WriteReadiness::Ready);
        let ticket = lifecycle.begin_readiness_sample().unwrap();

        assert_eq!(
            lifecycle.begin_drain(Instant::now()).await,
            DrainOutcome::Completed { accepted: 0 }
        );
        assert!(!lifecycle.commit_readiness(ticket, WriteReadiness::Ready));
        assert_eq!(lifecycle.snapshot().phase, NodePhase::Draining);
        assert_eq!(
            lifecycle.snapshot().readiness,
            WriteReadiness::NotReady {
                reasons: vec![ReadinessReason::Draining],
            }
        );
    }

    #[test]
    fn readiness_sample_cannot_overwrite_stopping() {
        let lifecycle = LifecycleController::starting();
        lifecycle.mark_running(WriteReadiness::Ready);
        let ticket = lifecycle.begin_readiness_sample().unwrap();

        lifecycle.mark_stopping();

        assert!(!lifecycle.commit_readiness(ticket, WriteReadiness::Ready));
        assert_eq!(lifecycle.snapshot().phase, NodePhase::Stopping);
    }
}
