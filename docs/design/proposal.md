# High-volume streaming design direction

Status: revised after the 2026-09-10 POC experiments. This is a proposed production architecture, not an implemented HA broker.

[Measured evidence](../experiments/high-volume.md) | [Initial proposal](initial-proposal.md) | [Design index](README.md)

## What changed

The original proposal favored one redb database and one installation-wide Raft group for small deployments.
The requested target is now high-volume usage.
The experiments favor independent writer paths, substantial batching, and LSM storage candidates.

Retire the installation-wide writer as the high-volume default.
Use a bounded number of independently replicated partitions or shard groups.
Each group can host many logical streams, so stream count does not imply one database, thread, cache, and Raft group per stream.

Keep RocksDB and Fjall as the storage finalists.
Prefer RocksDB as the baseline for the next high-volume iteration.
At eight shards and 512-record batches it delivered about 96% of Fjall's median throughput, with median trial p99 near 92 ms rather than 895 ms.
It also led the tested large-record configurations.
Fjall leads several small-record configurations, but one longer attempt lost its acknowledgement quorum.
That failure remains unresolved and prevents selecting Fjall solely from its short-run lead.

The custom segment POC did not win.
Its conservative per-batch manifest publication requires several synchronization operations.
This argues against shipping that implementation for high volume, not against every possible append-only segment design.
redb can remain a metadata or small-deployment option, but no longer defines the high-volume payload path.

## What the measurements support

With three logical nodes, four shards, 128-record batches, and 1-KiB records, the median rates were approximately 4,692 records/s for segments, 9,811 for redb, 24,427 for Fjall, and 15,902 for RocksDB.

With eight shards and 512-record batches, the five-second comparison reached approximately 78,600 records/s for Fjall and 75,532 for RocksDB.
These are configuration results on one shared host.
Shard count and producer concurrency changed together.

Record size changes the comparison.
RocksDB led the tested 16-KiB-record configurations.
There is no evidence for one universally fastest backend.

The longer observations contain three successful RocksDB attempts and two successful Fjall attempts plus one failed Fjall attempt.
Successful-run throughput must be shown beside those outcomes.
Three attempts are not a reliability estimate or a production capacity certification.

The [evidence report](../experiments/high-volume.md) contains exact rates, latency, memory, raw paths, and limits.

## Partition ownership and data layout

A stream retains an immutable identity.
Ordering is guaranteed within a stream partition.
A cursor identifies the stream, partition, and next record offset.
Internal Raft indexes and physical file locations are not user cursors.

A shard group owns its partition logs, marker commands, producer-session state, retention state, and derived indexes.
One serialized writer per group removes concurrent mutation of that state.
Separate groups do not share a global append mutex or transaction.

A small control group owns names, placement, membership intent, and topology changes.
It is not on the ordinary payload append path.
Partition movement must transfer state and fence the old owner before routing new writes.

Use ordered numeric keys and batch-shaped payload storage for LSM candidates.
Keep the record index separate from mutable consumer progress.
Store payloads once logically where the chosen consensus adapter permits it.
Avoid interpreting one logical payload as one physical write.

Compression, cache sizes, memtables, and group count need explicit per-node budgets.
The POC used disabled compression, 16-MiB caches, and 32-MiB LSM memtables per shard.
Those settings are experimental controls, not universal production defaults.

## Batching and flow control

Batch by bytes and time, not only by a fixed record count.
The measured 512-record batches were 512 KiB for 1-KiB records.
The same record count would be a different memory and latency decision for larger records.

Use the tested 512-KiB size as an initial experiment point, not a fixed product promise.
The flush timer still needs low-traffic latency experiments.
Client-supplied batch results do not prove a server-side group-commit policy.

Bound admission, batch bytes, outstanding producer work, and replication windows.
Backpressure must be visible to the client before unbounded memory or disk queues accumulate.
Reserve capacity for recovery, snapshots, and retention progress.

The baseline POC exposed an important replication coupling.
It enqueued work to both followers before consuming the first successful acknowledgement.
A full slow-follower queue could therefore delay an otherwise healthy durable majority.

Production replication must maintain an independent cursor and bounded window for each follower.
A lagging follower should resume from durable history without blocking the majority or skipping entries.
Raft replication and snapshot catch-up must provide that behavior.
The POC's permanent peer exclusion is an experiment, not a substitute for production catch-up.

The controlled delayed-follower experiment completed the same 24 batches in a median 0.245 seconds with isolation versus 4.920 seconds with blocking admission.
Both modes required a durable leader and follower.
The excluded slow peer retained only five batches, so this is not a comparison of three fully caught-up copies.
The result supports removing queue coupling from the high-volume design, not claiming a general 20x throughput improvement.

Peer diagnostics need shard, sequence, timing, queue state, and failure cause.
A transport deadline must not be treated as proof that a machine or storage engine failed.
The sustained failure showed why a short acknowledgement deadline and permanent disable policy are insufficient for a production recovery story.

## Durability and HA

Use an established Raft implementation for each group.
Deploy three replicas across independent failure domains.
Keep votes, terms, log entries, membership, and snapshot state durable according to that library's storage contract.

A successful append requires local durability, a durable replication majority, and committed application of the command.
Application of a batch and its bookmark must be deterministic.
A returned result includes the stable record range and bookmark identity.

The Raft-persistence milestone and committed-application milestone remain distinct.
Batch or checkpoint derived state only when the recovery contract proves it can be rebuilt safely.
Do not disable an engine WAL or discard consensus history merely to improve a benchmark.

Serve current reads only after quorum-confirmed leadership and application through the read barrier.
An isolated former leader cannot acknowledge new committed writes or claim fresh state.
Use learners and safe membership changes for node replacement.

Snapshots must preserve retained messages as well as compact control state.
Logical Raft-prefix removal does not authorize deleting payloads still covered by retention or a lease.
Installation needs checksums, atomic publication, and temporary disk headroom.

The current POCs do not implement Raft, elections, fencing, membership changes, or automatic catch-up.
Their fixed-leader failure experiments establish a narrower durable acknowledgement boundary.
They do not prove the production architecture above.

## Bookmarks at high volume

A bookmark has an immutable ID, a human name, one or more stable cursors, and a publication sequence.
Its position does not move as consumers make progress.
Consumer offsets remain separate mutable state.

Within one partition, append-plus-bookmark is one committed domain command.
An after-batch bookmark resumes at the next record.
Retries within a documented producer-session window return the original result.
Conflicting reuse and expired retry identities fail explicitly.

List recent bookmarks by publication order.
Do not sort them by client time or the offset they happen to target.
Index that order so listing N markers does not scan record payloads.

A partitioned bookmark contains one cursor per partition.
An independently sampled cursor vector is not an atomic or causally consistent cut.
A stronger checkpoint requires a barrier or transaction protocol and must be represented as a distinct guarantee.

Named replay ranges remain the main product distinction.
Listing markers and replaying records between boundaries are separate operations.
N completed intervals require N+1 distinct position-ordered boundaries.

Expired marker metadata must outlive its payload when the bookmark policy requires it.
Resume returns an explicit expiration error rather than jumping to a newer record.
The POC deliberately removes positional markers with retained batches and does not implement this production lifetime policy.

Protection leases need explicit ranges, deadlines, byte budgets, and admission rules.
A complete replay promise acquires protection before data is returned.
Replay across unleased requests can fail on expiration between requests.
No lease can silently prevent all reclamation until the disk fills.

## Retention, recovery, and operational limits

High volume makes retention and storage amplification part of capacity planning.
Measure sustained ingest with continuous retention and readers before publishing a capacity number.
The current POCs measure retention after ingestion.

Time, byte, consumer, and pin policies must produce a clear logical retention floor.
Replicas apply that decision consistently.
Physical reclamation can lag logical deletion, especially with LSM tombstones, active readers, and snapshots.

Bound the work required after a restart.
For LSM stores, include journal replay, table metadata, and shard count.
For a future segment design, persist compact indexes and a bounded recovery tail rather than scanning every payload.
Corruption repair is a separate, potentially slower operation.

Object storage is not required for the first implementation.
Long retention at high ingestion rates may justify it later.
That choice needs an explicit remote-history and recovery contract, not only an upload loop.

## API and product scope

Use a native batched streaming API first, with byte-bounded flow control and authentication.
Keep the transport decision separate from the storage decision.
The POC binary protocol is a measurement tool, not the proposed public API.

Kafka compatibility remains a distinct milestone.
Partition identities fit Kafka better than one global ordered stream, but parsing Produce and Fetch is insufficient.
Consumer groups, producer epochs, transactions, offsets, error behavior, authentication, and acknowledgement semantics remain separate commitments.

Keep bookmark discovery and protection leases on the native API.
Ordinary Kafka clients can seek to resolved offsets but do not automatically acquire bookmark capabilities.
Use bridges before promising more broker wire protocols.

The useful differentiators remain named replay ranges, retention-aware bookmarks, checkpoint manifests, and portable replay bundles.
An external snapshot reference does not imply an atomic transaction with that external system.

## Gates before production commitments

The next production-capacity claims require real Raft, independent hosts and disks, long soaks, concurrent retention and readers, and explicit failure recovery.
A cold-replay experiment and an open-loop overload experiment are also still needed.

The earlier image, idle-memory, and startup budgets remain tentative.
The benchmark executable bundles four engines with debug symbols, so its file size does not answer the single-engine container question.
No independent-host throughput target has been met or claimed.
