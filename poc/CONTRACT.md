# High-volume experiment contract

This is a benchmark POC, not a production broker. It compares storage, batching, sharding, and replicated durable append.
The product design already has competing storage proposals. Reuse them as executable candidates instead of running another paper design arena.

## Completion predicate

Four Rust storage implementations run behind one batch format and storage contract.
Standalone and three-process TCP runs produce machine-readable throughput, latency, and data-audit results.
At least three repeated trials compare single-record writes, batched writes, and independent shards.
Stored payload checksums, bookmark positions, and reopened state match acknowledged client results.
Failure experiments demonstrate the fixed-leader quorum boundary without claiming automatic failover.
The workspace contains raw evidence, reproduction commands, and a revised high-volume design.

## Scope and limits

Use local processes with separate data directories and TCP listeners.
These are separate logical nodes on one host and filesystem, not independent failure domains.
Replication is an explicitly fixed-leader measurement protocol, not Raft.
It has no election, leader fencing, membership changes, or automatic replica catch-up.
Do not call it an HA implementation.
The three-node experiment measures the cost of durable fan-out and majority acknowledgement.
Replica stores may contain attempted but unacknowledged batches. Audits must distinguish them.

Use bounded workloads and queues. Run trials serially so candidates do not compete for the disk.
Do not alter system caches, CPU settings, other containers, or unrelated processes.
Disable storage compression for the comparison.
Every successful storage append includes payload and an after-batch bookmark in one durable operation.
OS synchronization is required. Process-crash evidence is not a hardware power-loss guarantee.

## Shared data shape

The `poc-common` crate owns the binary batch format and these types:

- `Batch { shard: u32, sequence: u64, records: u32, record_bytes: u32, payload: Vec<u8> }`.
- Batch sequence starts at zero independently within each shard.
- A batch's deterministic payload depends on its shard and sequence.
- `Batch::generate(shard, sequence, records, record_bytes) -> anyhow::Result<Batch>`.
- `Batch::encode(&self) -> Vec<u8>`.
- `Batch::decode(bytes: &[u8]) -> anyhow::Result<Batch>`.
- `Batch::validate_payload(&self) -> anyhow::Result<()>` checks actual bytes against the deterministic generator.
- `Batch::payload_checksum(&self) -> u32`.
- `Bookmark { sequence: u64, next_record: u64 }`.
- `Audit { batches: u64, records: u64, payload_bytes: u64, digest: u64, first_sequence: Option<u64>, last_sequence: Option<u64> }`.
- `Audit::observe(&mut self, batch: &Batch)` updates ordered cumulative counters and digest.

The storage crate owns `Engine`, implementing clap ValueEnum, serde serialization, and Display for `segment`, `redb`, `fjall`, `rocksdb`.
It exports:

```rust
pub trait Store: Send {
    fn append(&mut self, batch: &poc_common::Batch) -> anyhow::Result<()>;
    fn audit(&mut self) -> anyhow::Result<poc_common::Audit>;
    fn last_bookmarks(&mut self, limit: usize) -> anyhow::Result<Vec<poc_common::Bookmark>>;
    fn retain_from(&mut self, sequence: u64) -> anyhow::Result<()>;
}
pub fn open(engine: Engine, directory: &std::path::Path) -> anyhow::Result<Box<dyn Store>>;
```

Each Store owns one shard and rejects noncontiguous or conflicting appends.
Reopening reconstructs the next batch sequence and cumulative next-record offset.
`audit` scans persisted payloads, decodes checksums, validates payload contents, and reports retained data in sequence order.
`last_bookmarks` returns newest retained markers first without scanning payloads.
`retain_from` makes earlier batches and their POC marker entries logically unavailable and preserves absolute next-record positions of surviving bookmarks.
Retention is measured separately after ingestion, not silently omitted from the evidence.
This storage POC does not implement human bookmark names, independent bookmark lifetimes, pin leases, or consumer progress.
The full broker must preserve expired bookmark metadata under its separate policy.

## Node and driver interface

The runner crate produces a binary named `stream-poc`.
Command output goes to stdout as one JSON object, except node startup which prints a single readiness JSON line.
Diagnostics go to stderr. Any failed correctness assertion or command has a nonzero exit code.

Commands:

- `node --engine ENGINE --dir PATH --listen HOST:PORT --shards N [--peers ADDR,ADDR]`. A node with peers is the fixed leader. A node without peers is a standalone node or follower. The listener may use port zero and reports its resolved address under the JSON key `address`.
- `bench --address ADDR --shards N --batch-records N --record-bytes N --seconds FLOAT --max-batches N [--start-sequence N]`. One concurrent client per shard. A synchronized start excludes connection setup. Every request latency includes waiting for the acknowledgement. Stop on either duration or max batches per shard. No automatic retries that hide errors.
- `inspect --engine ENGINE --dir PATH --shards N [--retain-from N]`. Used only after the node stops. Reopen stores, audit bytes and bookmarks, measure reopen and replay time, optionally apply retention and audit again.
- `status --address ADDR`. Returns per-shard applied audits or counters and readiness. The orchestrator uses it for liveness, not as a substitute for the offline persisted-data audit.

The benchmark JSON includes:

- `elapsed_seconds`, `records`, `payload_bytes`, `batches`, `records_per_second`, `mib_per_second`.
- `latency_us` with `p50`, `p95`, `p99`, and `max`.
- `per_shard`, an array of the expected Audit for batches acknowledged to each client.
- `latencies_us`, the raw per-request samples.
- `errors`, zero on a successful run. A failure must not produce success-shaped output.

Inspect JSON includes `reopen_seconds`, `audit_seconds`, `per_shard`, `bookmarks`, and optional `after_retention`.
Bookmarks must match the acknowledged batch sequences and absolute next-record offsets.
Status JSON uses `per_shard` for an array of Audit-shaped counters if possible.

## Replication invariants

Use a serialized writer per shard with bounded admission.
A leader must complete its own durable append and receive at least one durable follower acknowledgement in a three-node run.
Follower operations for each shard must preserve sequence order, including when a slow follower falls behind.
A missing follower must not turn every healthy request into a timeout delay.
A missing majority must produce an explicit error, never a local-only success.
No client reads or production commit-index semantics are claimed by this measurement protocol.
Report attempted versus acknowledged tails during failure experiments.

## Ownership

- Parent owns root workspace files, `poc/common/`, `poc/scripts/`, reports, and design documents.
- Storage worker owns `poc/storage/` only.
- Runner worker owns `poc/runner/` only.
- All workers read this contract. Changes to its interfaces require explicit coordination.
- Workers may run targeted builds and tests, but performance trials run only after all builds stop.
