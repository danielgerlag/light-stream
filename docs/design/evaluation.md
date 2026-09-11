# Evaluation obligations

These are production design obligations. The [POC report](../experiments/high-volume.md) records the experiments already executed. A production Raft broker does not exist.

[Design index](README.md) | [Proposal](proposal.md) | [Storage arena](storage-arena.md)

## Correctness cases

The POCs cover durable positional batch markers, reopened payload bytes, retention, and fixed-leader quorum refusal.
They do not cover named bookmark lifetimes, leases, consumer progress, elections, fencing, or Raft snapshot installation.
The cases below remain the full product obligations.

| Scenario | Required observable behavior |
| --- | --- |
| Append and bookmark in one request | Both become visible after the same commit. Neither is published alone. |
| Durable quorum commits, acknowledgement is lost | A retry under a supported idempotency identity returns the original result, not a bookmark at the new head. |
| Old leader remains reachable after a network split | It cannot acknowledge successful writes or claim fresh linearizable reads without the required quorum authority. |
| One of three replicas fails | The other two can elect a leader and continue. Surviving acknowledged history remains readable within the stated storage failure assumptions. |
| Two of three replicas fail | Writes stop. The server does not silently accept weaker durability. |
| A bookmark points at an old but committed record | Creating the bookmark races with retention through one ordered decision. Creation does not resurrect logically expired data. |
| A live pin would exceed a quota | Reject the pin or apply documented backpressure. Never silently delete data still promised by an active lease. |
| Unpinned bookmark outlives its records | Keep metadata if its own lifetime permits, mark the position expired, and return an explicit resume error. Never jump to the earliest remaining record. |
| A replay job promises a complete interval | Resolve start and end positions and acquire a bounded protection lease at admission. Retention must not invalidate the promised interval while the lease remains valid. |
| Replay spans requests without a protection lease | Expiration between requests produces an explicit error, not a silently shortened successful replay. |
| A client reads slowly | Bounded fetches do not keep database read transactions open while the client processes messages. |
| Bookmarks are created while the user pages backward | A sequence-bound pagination cursor avoids duplicates caused by moving list offsets. Define deletion behavior separately. |
| A name is reused for a deleted and recreated stream | The old bookmark fails by stream identity or incarnation, not by name lookup into the new stream. |
| A partitioned checkpoint is assembled in a future design | The cursor vector denotes independently ordered positions unless an explicit barrier protocol established stronger consistency. |
| A node installs a Raft snapshot | The node obtains all retained replay history through crash-safe installation. Compact control state alone cannot stand in for missing messages. |
| A process restarts with large retained history | Normal recovery uses durable application state and bounded pending work, not a mandatory scan of every retained payload byte. Corruption needs an explicit slower recovery or repair path. |
| A Raft adapter removes old entries | Removing consensus replay requirements must not remove message history still covered by retention or pins. |
| A Kafka client enables an unsupported feature | The broker rejects or does not advertise the feature. It never fakes success for idempotence, transactions, group coordination, or security. |

## Proposed performance comparisons

Compare RocksDB, Fjall, redb, and append-only segments with the same record sizes, batch sizes, durability guarantees, retention workload, and concurrency.
Compare standalone with standalone, and a three-replica durable quorum with the same guarantee.
Keep TLS, compression, hardware, and filesystem settings explicit.

Record these dimensions separately:

- Compressed registry size and unpacked disk usage.
- Idle and loaded RSS, page-cache use, and CPU.
- Empty readiness, populated dirty recovery, and new-replica catch-up.
- Payload throughput, durable acknowledgement p99, and replay bandwidth.
- Device bytes written, write amplification, and retention cost.
- Bookmark lookup latency during ingest and retention.
- Snapshot duration, staging space, and interference with active traffic.
- Failover interruption under process failure and network partitions.

Include last-100 bookmark listing across 100,000 bookmarks.
Include sustained retention and replay during writes.
An empty append loop is not a streaming-service workload.
Do not compare an asynchronous local acknowledgement with a synchronously persisted quorum acknowledgement.

## Tentative budgets

| Metric | Proposed target | Scope |
| --- | --- | --- |
| Compressed image size | Below 50 MiB | Declared platform and enabled features. |
| Idle RSS | Below 128 MiB | Standalone with ten streams. |
| Empty readiness | Below one second | Standalone, ready to accept a durable append, on a declared Linux reference machine. |
| Populated crash recovery | Not yet set | Requires a retained-data size and crash scenario. |
| HA throughput and latency | Not yet set | Requires record sizes, batching, concurrency, hardware, and durability settings. |

These budgets are proposals, not results or accepted requirements.
Avoid arbitrary percentage thresholds for switching engines.
If redb meets the intended workload and custom segments offer little benefit, retain the simpler storage ownership.
If the global writer or retention cost misses the workload, revisit replication-group granularity as well as the storage engine.
