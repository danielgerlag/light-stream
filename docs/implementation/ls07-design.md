# LS07 write path and consumer progress design

## Decision

LS07 adds one bounded publish scheduler to each data group. The scheduler combines independent producer requests into one Raft command without combining their identities, receipts, or bookmarks.

The Rust client uses one internal operation executor for deadlines, routing, retries, cancellation, and commit certainty. Consumer checkpoints use partition-scoped compare-and-set state in the owning data group. Checkpoints remain separate from bookmarks and do not pin retained records.

## Publish caller

The public publish call stays request-shaped:

```rust
let receipt = client.publish(batch).await?;

let receipt = client
    .publish_with(
        batch,
        PublishOptions::default()
            .resolve_ambiguous_receipt(true)
            .cancellation(cancel.clone()),
    )
    .await?;
```

One call still returns one `PublishReceipt`. Server batching remains private.

## Replicated publish batch

`GroupCommand::PublishMany` contains several complete `PublishBatch` values:

```rust
pub enum GroupCommand {
    Publish {
        batch: PublishBatch,
    },
    PublishMany {
        batch: ReplicatedPublishBatch,
    },
    CompareAndSetCheckpoint {
        mutation: CheckpointMutation,
    },
}

pub struct ReplicatedPublishBatch {
    requests: Vec<PublishBatch>,
}

pub struct ReplicatedPublishLimits {
    pub max_requests: usize,
    pub max_records: usize,
    pub max_payload_bytes: usize,
}

pub struct PublishManyResult {
    outcomes: Vec<PublishItemOutcome>,
}

pub struct PublishItemOutcome {
    request: ProducerRequestId,
    result: Result<PublishReceipt, DomainError>,
}
```

The result order matches the request order. Each outcome repeats its request ID so the scheduler can reject a corrupt or mismatched result before it replies to callers.

The state machine processes logical requests in command order. A successful request receives the next contiguous offsets for its partition. A duplicate or rejected request consumes no offsets. One rejected request does not reject its siblings.

`GroupCommand::Publish` remains decode-only for retained pre-LS07 log entries. New writes use `PublishMany`, including singleton writes during migration.

## Durable request outcomes

The current receipt record stores successful publishes. LS07 changes that record into a versioned durable outcome:

```rust
pub enum StoredPublishOutcome {
    Published(PublishReceipt),
    Rejected(DomainError),
}
```

The request fingerprint covers the complete semantic body:

- cluster;
- partition;
- producer request ID;
- ordered record boundaries and bytes;
- optional bookmark name.

An exact retry returns the original success or deterministic rejection. A reused request ID with another body returns `ReceiptConflict`.

Deterministic rejections consume the producer sequence and enter the receipt window. This rule prevents a lost bookmark-conflict response from becoming a different effect after the conflicting bookmark disappears.

Legacy fingerprints remain readable. New entries use the complete-body fingerprint. The upgrade does not scan and rewrite old receipts.

## Multi-request state application

The current single-request apply function reads RocksDB while it builds a `WriteBatch`. Repeating that function for one physical Raft entry would read stale tails, receipts, sessions, and bookmark indexes.

LS07 adds a bounded apply transaction:

```rust
struct PublishApplyTxn<'a> {
    machine: &'a RocksStateMachineInner,
    log_id: GroupLogId,
    partitions: HashMap<PartitionKey, PendingPartition>,
    outcomes: HashMap<Vec<u8>, StoredPublishOutcome>,
    producer_sessions: HashMap<Vec<u8>, ProducerSessionState>,
    bookmark_names: HashMap<Vec<u8>, BookmarkId>,
    bookmark_publications: HashMap<PartitionKey, BookmarkPublicationSequence>,
}

impl PublishApplyTxn<'_> {
    fn apply(
        machine: &RocksStateMachineInner,
        log_id: GroupLogId,
        requests: Vec<PublishBatch>,
        write: &mut WriteBatch,
    ) -> io::Result<PublishManyResult>;
}
```

The transaction loads each touched key once and then reads staged values first. Its memory is bounded by the replicated batch limits.

Record payloads keep the LS06 ownership model. Thin log entries flatten record slots across all logical requests. Payload IDs still derive from the Raft log ID and the flat slot. Only successful records gain applied-state ownership.

An optional publish bookmark stays atomic with its logical request. Bookmark IDs derive from the Raft log ID and the logical request ordinal. Legacy singleton commands keep their old bookmark ID derivation.

## Bounded group scheduler

Each `DataGroup` owns one `PublishScheduler` and one supervised worker:

```rust
pub struct PublishSchedulerConfig {
    pub queue_requests: usize,
    pub queue_records: usize,
    pub queue_resident_bytes: usize,
    pub batch_requests: usize,
    pub batch_records: usize,
    pub batch_payload_bytes: usize,
    pub max_coalesce_delay: Duration,
}

pub struct PublishScheduler {
    shared: Arc<Shared>,
}

impl PublishScheduler {
    pub fn try_admit(
        &self,
        batch: PublishBatch,
    ) -> Result<PublishWaiter, DomainError>;

    pub async fn shutdown(&self);
}
```

Admission uses one short mutex hold. It reserves request count, record count, and resident bytes as one operation. It never waits for capacity.

Resident-byte accounting includes:

- payload bytes;
- record descriptors;
- request metadata;
- the completion sender;
- the queue node.

Record payloads use boxed slices so spare vector capacity cannot escape the byte budget.

The scheduler keeps one FIFO queue and at most one physical Raft write in flight. It starts the flush timer from the oldest admitted request. New arrivals do not reset that timer. It flushes when any batch limit is reached or the timer expires.

The first release uses FIFO. Partition fairness adds enough policy to hide scheduler faults during the first performance pass. LS07 records per-partition wait time and adds fairness only if the evidence shows starvation.

Admission returns `PublishOverloaded` before Raft submission. The error always means `DefiniteNoCommit`. Capacity becomes available as soon as queued work cancels or submitted work completes.

## Cancellation certainty

The scheduler uses these states:

| State | Raft may commit | Cancellation result |
| --- | --- | --- |
| Not admitted | No | Definite no commit |
| Queued | No | Remove and return definite no commit |
| Submitted | Yes | Detach the caller and return ambiguous commit |
| Completed | Already decided | Return the durable result |

The `Queued` to `Submitted` transition occurs immediately before `raft.client_write`. The worker keeps submitted payloads and admission reservations until Openraft returns, even after the caller disconnects.

Client cancellation before any write RPC starts is definite no commit. Cancellation, deadline expiry, or connection loss after a write RPC starts is ambiguous until a receipt or mutation result resolves it.

Cancellation and deadline interruption are `ClientError` values. They are not `DomainError` values because they describe the caller's local lifecycle.

## Client retry control

Implementation kept the operation-specific typed response decoders. A generic tonic executor required lending boxed futures and made mutation certainty harder to inspect.

The client instead shares the invariants that cannot drift:

- `Deadline` supplies one decreasing budget.
- `resolve_route_until` refreshes metadata without replacing that budget.
- endpoint and leader-hint helpers preserve rotation order;
- publish tracks unresolved ambiguous attempts instead of treating every redirect as ambiguous;
- `Cancellation` and `ClientError::Interrupted` carry the local lifecycle and commit outcome;
- receipt resolution uses the remaining publish deadline and the original producer identity.

Checkpoint reads and mutations use the same route deadline for initial resolution, retries, and stale-route refresh. Existing operation-specific loops remain because their legal retry and result rules differ.

The CLI maps `SIGINT` to client cancellation. It waits briefly for a certainty-aware operation, prints one JSON object, and exits with code 130.

## Consumer checkpoint model

Checkpoint state belongs to the partition's data group:

```rust
pub struct CheckpointKey {
    cluster: ClusterId,
    partition: PartitionKey,
    consumer: ConsumerId,
}

pub struct CheckpointRevision(NonZeroU64);

pub enum CheckpointExpectation {
    Missing,
    Revision(CheckpointRevision),
}

pub struct CheckpointMutation {
    request: MutationRequestId,
    expected: CheckpointExpectation,
    candidate: CheckpointCandidate,
}

pub struct CommittedCheckpoint {
    key: CheckpointKey,
    cursor: CommittedCursor,
    revision: CheckpointRevision,
}

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
```

Checkpoint application:

1. Checks the mutation receipt and mutation-session sequence.
2. Checks cluster, partition, and data-group ownership.
3. Compares the expected revision with the stored checkpoint.
4. Rejects a candidate beyond the committed partition tail.
5. Rejects a candidate behind the current checkpoint.
6. Stores revision 1 for creation or increments the current revision.
7. Stores the result and the mutation receipt in the same RocksDB write.

Both success and conflict are stable mutation results. An exact retry returns the original result even if a later mutation changes the checkpoint.

Checkpoint CAS does not check the retention floor. A checkpoint can point to data that retention later removes. `checkpoint get` still returns the stored value, while `checkpoint fetch` returns the existing `CursorExpired` error.

Checkpoint updates do not read or write bookmark state. They do not create payload ownership, replay leases, or retention pins.

## Module ownership

| Module | Responsibility |
| --- | --- |
| `light-stream-core/src/checkpoint.rs` | Checkpoint key, revision, mutation, committed value, and CAS result |
| `light-stream-core/src/error.rs` | Publish overload and checkpoint errors |
| `light-stream-storage/src/publish_apply.rs` | Multi-request apply transaction and durable publish outcomes |
| `light-stream-storage/src/checkpoint.rs` | Checkpoint keys, CAS application, and committed reads |
| `light-stream-server/src/publish_scheduler.rs` | Admission, batching, cancellation boundary, result fan-out, and shutdown |
| `light-stream-server/src/runtime.rs` | Route requests to the data group and own scheduler lifecycle |
| `light-stream-client/src/executor.rs` | Deadline, routing, retries, cancellation, and certainty |
| `light-stream-client/src/operations.rs` | Operation-specific RPC and ambiguity policy |
| `light-stream-cli/src/main.rs` | Publish options, checkpoint commands, JSON, and exit codes |

## Implementation order

1. Add `PublishMany`, thin-entry support, durable publish outcomes, and the multi-request apply transaction. Send singleton `PublishMany` commands from the current direct runtime path.
2. Add the bounded per-group scheduler and switch runtime publish calls to it.
3. Add checkpoint domain types, storage CAS, RPCs, client calls, and CLI commands.
4. Share deadline, route refresh, leader hint, cancellation, and certainty helpers while retaining operation-specific typed result loops.
5. Add live LS07 verification, measure the release path with batching enabled, and tune only from saved evidence.

## Required evidence

LS07 must prove:

- one durable outcome for each producer request in a physical batch;
- no duplicate records after a dropped response;
- sparse traffic flushes within the configured delay;
- request, record, and byte admission limits never exceed their configured values;
- overload rejection is definite no commit and admission recovers after load falls;
- queued cancellation is definite and submitted cancellation is ambiguous;
- leader changes preserve request identities and results;
- competing checkpoint revisions produce one advance and one stable conflict;
- checkpoint changes leave bookmarks unchanged;
- receipts and checkpoints survive restart, failover, snapshot build, purge, and catch-up;
- CLI success, conflict, overload, expiration, and cancellation output remains parseable;
- B1 and B4 pass with batching enabled.
