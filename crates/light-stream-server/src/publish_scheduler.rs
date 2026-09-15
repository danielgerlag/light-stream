use std::{
    collections::{BTreeMap, VecDeque},
    mem::size_of,
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use light_stream_core::{
    AmbiguousRequest, ConsensusGroup, DomainError, LeaderHint, MAX_PUBLISH_BYTES,
    MAX_RECORDS_PER_PUBLISH, MAX_RECORDS_PER_REPLICATED_PUBLISH, MAX_REPLICATED_PUBLISH_BYTES,
    MAX_REQUESTS_PER_REPLICATED_PUBLISH, ProducerRequestId, PublishBatch, PublishReceipt,
    ReplicatedPublishBatch, RequestOutcome,
};
use light_stream_storage::{ApplyResult, GroupCommand};
use openraft::errors::{ClientWriteError, RaftError};
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
    time::{Instant, sleep, sleep_until},
};

use crate::runtime::DataRaft;

const OPERATION_TIMEOUT: Duration = Duration::from_secs(3);
const REQUEST_OVERHEAD_BYTES: usize = 256;
const RECORD_OVERHEAD_BYTES: usize = size_of::<Vec<u8>>() + 32;

#[derive(Clone, Copy, Debug)]
pub(crate) struct PublishSchedulerConfig {
    pub queue_requests: usize,
    pub queue_records: usize,
    pub queue_resident_bytes: usize,
    pub batch_requests: usize,
    pub batch_records: usize,
    pub batch_payload_bytes: usize,
    pub max_coalesce_delay: Duration,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct PublishVerificationDelays {
    pub before_submit: Option<Duration>,
    pub after_commit: Option<Duration>,
}

impl Default for PublishSchedulerConfig {
    fn default() -> Self {
        Self {
            queue_requests: 512,
            queue_records: 8_192,
            queue_resident_bytes: 32 * 1024 * 1024,
            batch_requests: 64,
            batch_records: 512,
            batch_payload_bytes: 8 * 1024 * 1024,
            max_coalesce_delay: Duration::from_micros(200),
        }
    }
}

impl PublishSchedulerConfig {
    pub fn try_new(
        queue_requests: usize,
        queue_records: usize,
        queue_resident_bytes: usize,
        batch_requests: usize,
        batch_records: usize,
        batch_payload_bytes: usize,
        max_coalesce_delay: Duration,
    ) -> Result<Self, String> {
        if queue_requests == 0
            || queue_records == 0
            || queue_resident_bytes == 0
            || batch_requests == 0
            || batch_records == 0
            || batch_payload_bytes == 0
        {
            return Err("publish queue and batch limits must be greater than zero".to_owned());
        }
        if max_coalesce_delay.is_zero() {
            return Err("publish coalesce delay must be greater than zero".to_owned());
        }
        if batch_requests > MAX_REQUESTS_PER_REPLICATED_PUBLISH
            || batch_records > MAX_RECORDS_PER_REPLICATED_PUBLISH
            || batch_payload_bytes > MAX_REPLICATED_PUBLISH_BYTES
        {
            return Err("publish batch limits exceed the replicated command limits".to_owned());
        }
        if batch_records < MAX_RECORDS_PER_PUBLISH || batch_payload_bytes < MAX_PUBLISH_BYTES {
            return Err("publish batch limits must fit one maximum-size request".to_owned());
        }
        let maximum_request_resident_bytes = MAX_PUBLISH_BYTES
            .saturating_add(MAX_RECORDS_PER_PUBLISH.saturating_mul(RECORD_OVERHEAD_BYTES))
            .saturating_add(REQUEST_OVERHEAD_BYTES);
        if queue_requests < batch_requests
            || queue_records < batch_records
            || queue_resident_bytes < maximum_request_resident_bytes
        {
            return Err("publish queue limits must contain one complete physical batch".to_owned());
        }
        Ok(Self {
            queue_requests,
            queue_records,
            queue_resident_bytes,
            batch_requests,
            batch_records,
            batch_payload_bytes,
            max_coalesce_delay,
        })
    }
}

#[derive(Clone, Copy)]
struct PublishCharge {
    records: usize,
    resident_bytes: usize,
}

impl PublishCharge {
    fn for_batch(batch: &PublishBatch) -> Result<Self, DomainError> {
        let payload_bytes = batch
            .records()
            .iter()
            .try_fold(0usize, |total, record| total.checked_add(record.len()))
            .ok_or_else(|| DomainError::InvalidPayload {
                reason: "publish resident byte count overflow".to_owned(),
            })?;
        let record_overhead = batch
            .records()
            .len()
            .checked_mul(RECORD_OVERHEAD_BYTES)
            .ok_or_else(|| DomainError::InvalidPayload {
                reason: "publish record overhead overflow".to_owned(),
            })?;
        let resident_bytes = payload_bytes
            .checked_add(record_overhead)
            .and_then(|total| total.checked_add(REQUEST_OVERHEAD_BYTES))
            .ok_or_else(|| DomainError::InvalidPayload {
                reason: "publish resident byte count overflow".to_owned(),
            })?;
        Ok(Self {
            records: batch.records().len(),
            resident_bytes,
        })
    }
}

#[derive(Default)]
struct AdmissionUsage {
    accepting: bool,
    requests: usize,
    records: usize,
    resident_bytes: usize,
}

struct Admission {
    usage: Mutex<AdmissionUsage>,
    config: PublishSchedulerConfig,
}

impl Admission {
    fn new(config: PublishSchedulerConfig) -> Self {
        Self {
            usage: Mutex::new(AdmissionUsage {
                accepting: true,
                ..AdmissionUsage::default()
            }),
            config,
        }
    }

    fn reserve(&self, charge: PublishCharge) -> Result<(), DomainError> {
        let mut usage = self.usage.lock().map_err(|_| DomainError::Storage {
            reason: "publish admission lock is poisoned".to_owned(),
        })?;
        if !usage.accepting {
            return Err(DomainError::ClusterForming);
        }
        let requests = usage.requests.saturating_add(1);
        let records = usage.records.saturating_add(charge.records);
        let resident_bytes = usage.resident_bytes.saturating_add(charge.resident_bytes);
        for (resource, used, limit) in [
            (
                "publish queue requests",
                requests,
                self.config.queue_requests,
            ),
            ("publish queue records", records, self.config.queue_records),
            (
                "publish queue resident bytes",
                resident_bytes,
                self.config.queue_resident_bytes,
            ),
        ] {
            if used > limit {
                return Err(DomainError::PublishOverloaded {
                    resource: resource.to_owned(),
                    limit: u64::try_from(limit).unwrap_or(u64::MAX),
                });
            }
        }
        usage.requests = requests;
        usage.records = records;
        usage.resident_bytes = resident_bytes;
        Ok(())
    }

    fn release(&self, charge: PublishCharge) {
        let Ok(mut usage) = self.usage.lock() else {
            return;
        };
        usage.requests = usage.requests.saturating_sub(1);
        usage.records = usage.records.saturating_sub(charge.records);
        usage.resident_bytes = usage.resident_bytes.saturating_sub(charge.resident_bytes);
    }

    fn close(&self) {
        if let Ok(mut usage) = self.usage.lock() {
            usage.accepting = false;
        }
    }
}

struct QueuedPublish {
    admitted_at: Instant,
    charge: PublishCharge,
    batch: PublishBatch,
    reply: oneshot::Sender<Result<PublishReceipt, DomainError>>,
}

struct PublishCompletion {
    charge: PublishCharge,
    request: ProducerRequestId,
    reply: Option<oneshot::Sender<Result<PublishReceipt, DomainError>>>,
}

pub(crate) struct PublishWaiter {
    reply: oneshot::Receiver<Result<PublishReceipt, DomainError>>,
}

impl PublishWaiter {
    pub async fn wait(self) -> Result<PublishReceipt, DomainError> {
        self.reply.await.unwrap_or_else(|_| {
            Err(DomainError::Storage {
                reason: "publish scheduler stopped before completing the request".to_owned(),
            })
        })
    }
}

pub(crate) struct PublishScheduler {
    admission: Arc<Admission>,
    sender: mpsc::Sender<QueuedPublish>,
    shutdown: watch::Sender<bool>,
    worker: tokio::sync::Mutex<Option<JoinHandle<()>>>,
}

impl PublishScheduler {
    pub fn spawn(
        raft: DataRaft,
        config: PublishSchedulerConfig,
        verification: PublishVerificationDelays,
        leader_hints: Arc<RwLock<BTreeMap<u64, LeaderHint>>>,
    ) -> Self {
        let admission = Arc::new(Admission::new(config));
        let (sender, receiver) = mpsc::channel(config.queue_requests);
        let (shutdown, shutdown_receiver) = watch::channel(false);
        let worker_admission = admission.clone();
        let worker = tokio::spawn(run_worker(
            raft,
            config,
            verification,
            leader_hints,
            worker_admission,
            receiver,
            shutdown_receiver,
        ));
        Self {
            admission,
            sender,
            shutdown,
            worker: tokio::sync::Mutex::new(Some(worker)),
        }
    }

    pub fn try_admit(&self, batch: PublishBatch) -> Result<PublishWaiter, DomainError> {
        let charge = PublishCharge::for_batch(&batch)?;
        self.admission.reserve(charge)?;
        let (reply, receiver) = oneshot::channel();
        let queued = QueuedPublish {
            admitted_at: Instant::now(),
            charge,
            batch,
            reply,
        };
        if self.sender.try_send(queued).is_err() {
            self.admission.release(charge);
            return Err(DomainError::PublishOverloaded {
                resource: "publish queue requests".to_owned(),
                limit: u64::try_from(self.admission.config.queue_requests).unwrap_or(u64::MAX),
            });
        }
        Ok(PublishWaiter { reply: receiver })
    }

    pub async fn shutdown(&self) {
        self.admission.close();
        let _ = self.shutdown.send(true);
        if let Some(worker) = self.worker.lock().await.take() {
            let _ = worker.await;
        }
    }
}

async fn run_worker(
    raft: DataRaft,
    config: PublishSchedulerConfig,
    verification: PublishVerificationDelays,
    leader_hints: Arc<RwLock<BTreeMap<u64, LeaderHint>>>,
    admission: Arc<Admission>,
    mut receiver: mpsc::Receiver<QueuedPublish>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut pending = VecDeque::new();
    loop {
        if *shutdown.borrow() {
            break;
        }
        if pending.is_empty() {
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
                queued = receiver.recv() => {
                    let Some(queued) = queued else {
                        break;
                    };
                    pending.push_back(queued);
                }
            }
        }
        discard_cancelled(&mut pending, &admission);
        let Some(oldest) = pending.front() else {
            continue;
        };
        let flush_at = oldest.admitted_at + config.max_coalesce_delay;
        let usage = batch_usage(&pending, config);
        if !usage.flush_now && Instant::now() < flush_at {
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
                queued = receiver.recv() => {
                    if let Some(queued) = queued {
                        pending.push_back(queued);
                    }
                }
                _ = sleep_until(flush_at) => {}
            }
            continue;
        }
        let selected = take_batch(&mut pending, config);
        if selected.is_empty() {
            continue;
        }
        submit_batch(
            &raft,
            verification.before_submit,
            verification.after_commit,
            &leader_hints,
            &admission,
            &mut shutdown,
            selected,
        )
        .await;
    }

    admission.close();
    for queued in pending {
        let _ = queued.reply.send(Err(DomainError::ClusterForming));
        admission.release(queued.charge);
    }
    while let Ok(queued) = receiver.try_recv() {
        let _ = queued.reply.send(Err(DomainError::ClusterForming));
        admission.release(queued.charge);
    }
}

struct BatchUsage {
    flush_now: bool,
}

fn batch_usage(pending: &VecDeque<QueuedPublish>, config: PublishSchedulerConfig) -> BatchUsage {
    let mut requests = 0usize;
    let mut records = 0usize;
    let mut payload_bytes = 0usize;
    let mut flush_now = false;
    for queued in pending {
        let next_records = records.saturating_add(queued.batch.records().len());
        let next_payload_bytes = payload_bytes
            .saturating_add(queued.batch.records().iter().map(Vec::len).sum::<usize>());
        if requests == config.batch_requests
            || next_records > config.batch_records
            || next_payload_bytes > config.batch_payload_bytes
        {
            flush_now = requests > 0;
            break;
        }
        requests += 1;
        records = next_records;
        payload_bytes = next_payload_bytes;
        if requests == config.batch_requests
            || records == config.batch_records
            || payload_bytes == config.batch_payload_bytes
        {
            flush_now = true;
            break;
        }
    }
    BatchUsage { flush_now }
}

fn take_batch(
    pending: &mut VecDeque<QueuedPublish>,
    config: PublishSchedulerConfig,
) -> Vec<QueuedPublish> {
    let mut selected = Vec::new();
    let mut records = 0usize;
    let mut payload_bytes = 0usize;
    while selected.len() < config.batch_requests {
        let Some(next) = pending.front() else {
            break;
        };
        let next_records = records.saturating_add(next.batch.records().len());
        let next_payload_bytes =
            payload_bytes.saturating_add(next.batch.records().iter().map(Vec::len).sum::<usize>());
        if !selected.is_empty()
            && (next_records > config.batch_records
                || next_payload_bytes > config.batch_payload_bytes)
        {
            break;
        }
        let next = pending.pop_front().expect("front item exists");
        records = next_records;
        payload_bytes = next_payload_bytes;
        selected.push(next);
    }
    selected
}

fn discard_cancelled(pending: &mut VecDeque<QueuedPublish>, admission: &Admission) {
    let mut retained = VecDeque::with_capacity(pending.len());
    while let Some(queued) = pending.pop_front() {
        if queued.reply.is_closed() {
            admission.release(queued.charge);
        } else {
            retained.push_back(queued);
        }
    }
    *pending = retained;
}

async fn submit_batch(
    raft: &DataRaft,
    verification_delay: Option<Duration>,
    verification_response_delay: Option<Duration>,
    leader_hints: &RwLock<BTreeMap<u64, LeaderHint>>,
    admission: &Admission,
    shutdown: &mut watch::Receiver<bool>,
    mut selected: Vec<QueuedPublish>,
) {
    selected.retain(|queued| {
        if queued.reply.is_closed() {
            admission.release(queued.charge);
            false
        } else {
            true
        }
    });
    if selected.is_empty() {
        return;
    }
    if let Some(delay) = verification_delay {
        sleep(delay).await;
        selected.retain(|queued| {
            if queued.reply.is_closed() {
                admission.release(queued.charge);
                false
            } else {
                true
            }
        });
    }
    if selected.is_empty() {
        return;
    }

    let mut requests = Vec::with_capacity(selected.len());
    let mut completions = Vec::with_capacity(selected.len());
    for queued in selected {
        completions.push(PublishCompletion {
            charge: queued.charge,
            request: queued.batch.request().clone(),
            reply: Some(queued.reply),
        });
        requests.push(queued.batch);
    }
    let batch = match ReplicatedPublishBatch::new(requests) {
        Ok(batch) => batch,
        Err(error) => {
            complete_all(completions, admission, error);
            return;
        }
    };

    let write = raft.client_write(GroupCommand::PublishMany { batch });
    tokio::pin!(write);
    tokio::select! {
        response = &mut write => {
            match response {
                Ok(response) => {
                    if let Some(delay) = verification_response_delay {
                        tokio::select! {
                            _ = sleep(delay) => {}
                            _ = shutdown.changed() => {}
                        }
                    }
                    complete_response(completions, admission, response.data)
                }
                Err(error) => complete_all(
                    completions,
                    admission,
                    map_write_error(error, leader_hints),
                ),
            }
        }
        _ = sleep(OPERATION_TIMEOUT) => {
            for completion in &mut completions {
                let _ = completion.reply.take().expect("reply is present").send(Err(DomainError::QuorumUnavailable {
                    group: ConsensusGroup::Data,
                    outcome: RequestOutcome::AmbiguousCommit,
                    request: Some(AmbiguousRequest::Publish {
                        request: completion.request.clone(),
                    }),
                }));
            }
            tokio::select! {
                _ = &mut write => {}
                _ = shutdown.changed() => {}
            }
            for completion in completions {
                admission.release(completion.charge);
            }
        }
        _ = shutdown.changed() => {
            for mut completion in completions {
                let _ = completion.reply.take().expect("reply is present").send(Err(
                    DomainError::QuorumUnavailable {
                        group: ConsensusGroup::Data,
                        outcome: RequestOutcome::AmbiguousCommit,
                        request: Some(AmbiguousRequest::Publish {
                            request: completion.request,
                        }),
                    },
                ));
                admission.release(completion.charge);
            }
        }
    }
}

fn complete_response(
    completions: Vec<PublishCompletion>,
    admission: &Admission,
    result: ApplyResult,
) {
    let ApplyResult::PublishedMany(result) = result else {
        complete_all(
            completions,
            admission,
            DomainError::Storage {
                reason: format!("unexpected publish apply result {result}"),
            },
        );
        return;
    };
    let outcomes = result.into_outcomes();
    if outcomes.len() != completions.len()
        || outcomes
            .iter()
            .zip(&completions)
            .any(|(outcome, completion)| outcome.request() != &completion.request)
    {
        complete_all(
            completions,
            admission,
            DomainError::Storage {
                reason: "publish batch returned mismatched outcomes".to_owned(),
            },
        );
        return;
    }
    for (mut completion, outcome) in completions.into_iter().zip(outcomes) {
        let _ = completion
            .reply
            .take()
            .expect("reply is present")
            .send(outcome.into_result());
        admission.release(completion.charge);
    }
}

fn complete_all(completions: Vec<PublishCompletion>, admission: &Admission, error: DomainError) {
    for mut completion in completions {
        let _ = completion
            .reply
            .take()
            .expect("reply is present")
            .send(Err(error.clone()));
        admission.release(completion.charge);
    }
}

fn map_write_error(
    error: RaftError<
        light_stream_storage::DataRaftConfig,
        ClientWriteError<light_stream_storage::DataRaftConfig>,
    >,
    leader_hints: &RwLock<BTreeMap<u64, LeaderHint>>,
) -> DomainError {
    match error {
        RaftError::APIError(ClientWriteError::ForwardToLeader(forward)) => DomainError::NotLeader {
            group: ConsensusGroup::Data,
            leader: forward.leader_id.and_then(|leader| {
                leader_hints
                    .read()
                    .ok()
                    .and_then(|hints| hints.get(&leader).cloned())
            }),
        },
        RaftError::APIError(ClientWriteError::ChangeMembershipError(error)) => {
            DomainError::Storage {
                reason: error.to_string(),
            }
        }

        RaftError::Fatal(error) => DomainError::Storage {
            reason: error.to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use light_stream_core::{
        ClusterId, PartitionId, PartitionKey, PrincipalId, ProducerSessionId, RequestSequence,
        StreamId,
    };
    use uuid::Uuid;

    use super::*;

    fn batch(sequence: u64, records: usize, bytes: usize) -> PublishBatch {
        PublishBatch::new(
            ClusterId::from_uuid(Uuid::new_v4()),
            PartitionKey::new(StreamId::from_uuid(Uuid::new_v4()), PartitionId::new(0)),
            ProducerRequestId::new(
                PrincipalId::parse("scheduler-test").unwrap(),
                ProducerSessionId::from_uuid(Uuid::new_v4()),
                RequestSequence::new(sequence),
            ),
            vec![vec![1; bytes]; records],
        )
        .unwrap()
    }

    fn queued(batch: PublishBatch) -> QueuedPublish {
        let charge = PublishCharge::for_batch(&batch).unwrap();
        let (reply, _) = oneshot::channel();
        QueuedPublish {
            admitted_at: Instant::now(),
            charge,
            batch,
            reply,
        }
    }

    #[test]
    fn admission_reserves_all_dimensions_without_waiting() {
        let config = PublishSchedulerConfig {
            queue_requests: 1,
            queue_records: 2,
            queue_resident_bytes: 1024,
            batch_requests: 1,
            batch_records: 2,
            batch_payload_bytes: 512,
            max_coalesce_delay: Duration::from_millis(1),
        };
        let admission = Admission::new(config);
        let charge = PublishCharge {
            records: 2,
            resident_bytes: 1024,
        };
        admission.reserve(charge).unwrap();
        assert!(matches!(
            admission.reserve(PublishCharge {
                records: 1,
                resident_bytes: 1
            }),
            Err(DomainError::PublishOverloaded { .. })
        ));
        admission.release(charge);
        admission
            .reserve(PublishCharge {
                records: 1,
                resident_bytes: 1,
            })
            .unwrap();
    }

    #[test]
    fn batch_limits_leave_non_fitting_work_queued() {
        let config = PublishSchedulerConfig {
            queue_requests: 8,
            queue_records: 8,
            queue_resident_bytes: 4096,
            batch_requests: 4,
            batch_records: 3,
            batch_payload_bytes: 3,
            max_coalesce_delay: Duration::from_millis(1),
        };
        let mut pending = VecDeque::from([queued(batch(1, 2, 1)), queued(batch(2, 2, 1))]);
        assert!(batch_usage(&pending, config).flush_now);
        let selected = take_batch(&mut pending, config);
        assert_eq!(selected.len(), 1);
        assert_eq!(pending.len(), 1);
    }
}
