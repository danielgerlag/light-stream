# A lightweight broker built around bookmarks

Status: historical small-installation proposal from 2026-09-10. Superseded by the [high-volume direction](proposal.md) after the POC experiments.

[Design index](README.md) | [Storage arena](storage-arena.md) | [Sources](sources.md) | [Evaluation](evaluation.md)

## Product direction

Build for application teams that need durable event history, named checkpoints, and controlled replay without a large operational footprint.
For the first release, prefer a Rust binary with redb, one installation-wide Raft group, and native bookmark APIs.
Treat that storage choice as a hypothesis, not a measured performance result.
This targets small installations, not Kafka-scale horizontal throughput.

## The current baseline

[Kafka 4.0 removed ZooKeeper mode](https://kafka.apache.org/40/getting-started/upgrade/).
The old Kafka-plus-ZooKeeper image total is not the current minimum deployment.
Docker Hub reported roughly [239 MB for Kafka 4.3.1](https://hub.docker.com/v2/repositories/apache/kafka/tags/4.3.1) and [11.1 MB for nats:alpine](https://hub.docker.com/v2/repositories/library/nats/tags/alpine), both Linux amd64, on 2026-09-10.
These are registry sizes, not runtime memory.
The exact platform digests are in [Sources](sources.md).

[JetStream already provides replicated streams](https://docs.nats.io/learn/topologies/jetstream-in-a-cluster).
[Redpanda already combines Kafka APIs with Raft](https://docs.redpanda.com/current/get-started/architecture/).
[Apache Iggy already targets streaming in Rust](https://iggy.apache.org/).
Small deployment and Rust alone are not a sufficient distinction.

## Storage recommendation

Two arena candidates chose custom segments and one chose redb.
The independent judge selected redb for fewer coordination boundaries and less custom crash-consistency code.
The lead initially preferred segments, then accepted the judge's first-release argument.
Segments remain the main performance challenger.

| Design | Main attraction | Main cost | Recommendation |
| --- | --- | --- | --- |
| RocksDB for records and metadata | Established ordered storage and atomic batches make bookmark indexes straightforward. | Native integration, memory budgets, compaction, and retention tuning. Amplification depends on layout and workload. | Keep as a serious benchmark candidate, especially if keyed state or compaction becomes central. |
| Append-only segments with checkpointed metadata | Matches append, sequential replay, and whole-segment retention. Can share payload bytes with replication. | The broker owns crash-safe manifests, tail repair, index recovery, and snapshot transfer. | Main challenger if transactional storage misses the runtime budgets. |
| Fjall for records and metadata | A Rust LSM with atomic operations across keyspaces and reverse iteration. | Still has LSM buffers and background maintenance. | Strong Rust-native alternative to the custom log. |
| redb for records and metadata | Rust transactions and one storage owner reduce custom crash-consistency work. | Copy-on-write pages, a single database writer, and record reclamation can limit sustained ingestion. | First-release choice for small installations. The choice needs workload evidence before implementation commitment. |

[RocksDB range deletion](https://rocksdb.org/blog/2018/11/21/delete-range.html), [Fjall's design](https://docs.rs/fjall/latest/fjall/), and [redb's design](https://docs.rs/redb/latest/redb/) support the mechanism descriptions.
The expected workload consequences remain judgments.

One redb database holds payload batches, Raft entries, committed record indexes, bookmarks, and producer sessions.
Raft history and committed record indexes reference the same logical batch.
This avoids a second application payload copy, not physical write amplification.
Keep cache and metadata budgets explicit.

## Bookmark contract

A stream has an immutable UUID and one ordered record sequence.
A cursor contains that UUID and the next record offset to read.
The cursor after record 99 names record 100.
It never contains a physical file address or leader term.

A bookmark has an immutable ID, human name, cursor, and publication sequence.
Its cursor never moves.
The server may report its target as available, expired, or deleted.
Consumer progress is a separate mutable value.

Support publishing a bookmark at a retained committed cursor and capturing the current tail.
An atomic append-and-bookmark command publishes a batch and an after-batch checkpoint together.
Resolve validation, sequence assignment, duplicate requests, and name conflicts in ordered application.
Leader prechecks alone do not establish atomicity.
Clients receive the batch start and end cursors.

An ordered bookmark index returns recent markers without scanning record payloads.
Use publication sequence for recency, not client clocks or target offsets.
Backward pagination includes an initial publication ceiling and an exclusive continuation sequence.
Concurrent deletion can remove an item between pages. This is not historical snapshot isolation.

Listing N bookmarks returns metadata.
Replaying between two cursors returns the half-open record range.
N completed intervals need N+1 distinct position-ordered boundaries.
Backdated bookmarks make creation order and position order different.

Bounded fetches copy a byte-limited batch under a short read transaction.
Retention cannot reclaim the database pages before that transaction releases them.
Do not keep read transactions open while a slow client processes messages.
An unleased replay across several requests can still encounter expiration between requests.
A replay job that promises a complete interval must first acquire a bounded range lease.
Keep that stronger promise out of the first release unless the lease is implemented.

If retention passes an unpinned cursor, return an explicit expired-position error.
Never move the cursor to the earliest remaining record.
Recreating a stream changes its UUID, so old bookmarks cannot silently attach to new data.
Deleted bookmark IDs remain terminal.
Name reuse creates a new ID and must not recreate an old result during a duplicate retry.
Bound tombstone and producer-session history, and reject requests older than the supported retry window.

Keep streams unpartitioned in the first release.
Different streams share the installation's write leader and database writer.
Concurrent clients and batching do not remove that aggregate scaling limit.
A future partitioned checkpoint needs one cursor per partition.
A consistent cross-partition checkpoint needs an explicit coordination protocol.

## HA without external coordination

The proposed stack is Rust, Tokio, redb, an existing Raft implementation such as Openraft, and a native streaming API.
Use one binary in standalone and three-replica modes.
Standalone has local durability but cannot survive loss of its only disk.
HA replicas belong on independent hosts or failure domains.

One Raft group owns stream creation, deletion, records, bookmarks, and retention.
This removes cross-group catalog coordination from the first release.
All three nodes hold the full installation.
Adding replicas does not shard the data or increase write capacity.

Persist a command's payload and Raft entry durably before counting each replica's acknowledgement.
After a durable majority commits the command, apply it in another atomic database transaction.
That transaction assigns record offsets and updates the bookmark, retry result, and applied index together.
Only then return success to the client.
These are two durable broker phases, even when each payload is stored once logically.
Every replica must apply the same command deterministically.
Reads that claim current state require a quorum-confirmed leader and application through its read barrier.
One failed replica leaves a majority. Two failures stop writes.
Never silently downgrade durability.

Persist producer session identity, request sequence, and recent results.
A retry after a lost reply returns the original result within the advertised window.
Start with one outstanding request per producer session.
This does not promise exactly-once external side effects.

Raft prefix purging removes log references, not retained messages.
Reclaim a payload only when neither committed records nor required consensus history reference it.
Readers and snapshots can delay physical page reuse.
Logical record deletion does not promise immediate shrinking of the database file.
[Openraft's storage contract](https://docs.rs/openraft/latest/openraft/storage/trait.RaftLogStorage.html) requires durable writes, serialized storage changes, and no holes in retained consensus history.

A recovering follower needs retained messages as well as compact Raft state.
Snapshots contain all retained payloads, bookmark state, producer sessions, retention state, membership, and the included log position.
Transfer a consistent applied-state snapshot, not an arbitrary copy of a live database file.
Stage and synchronize the received state before atomic installation, preserving any newer local Raft term and vote.
Snapshot transfer needs temporary disk headroom and can take time proportional to retained history.
Use learner catch-up before membership replacement.
Keep backup and disaster recovery separate from the claim of surviving one replica failure.

Use durable transactions with redb's quick-repair option throughout.
The [redb design](https://raw.githubusercontent.com/cberner/redb/master/docs/design.md) explains that quick-repair preserves allocator state and uses two-phase commit.
This trades extra commit work for avoiding a full allocator reconstruction after a crash.
Do not hide that cost in a throughput claim.

Restart loads committed application state and resolves the remaining Raft suffix.
Bound the leader's queued and committed-but-unapplied work with backpressure.
Use snapshot catch-up when a follower falls too far behind.
An empty process start, dirty local recovery, and a new replica receiving retained history are different readiness measurements.

## Compatibility boundary

Start with a native API, with gRPC streaming as a reasonable first candidate.
Use bounded batches, pull-based flow control, TLS, and stream authorization.
Do not design a bespoke transport solely on an unmeasured speed assumption.

A later Kafka listener maps an ordered stream to a one-partition topic.
Describe it as a supported client profile, not drop-in compatibility.
Kafka's [protocol specification](https://kafka.apache.org/43/design/protocol/) includes version negotiation and leader discovery.
Client support also requires the relevant batch encodings, errors, compression, offsets, authentication, and retry behavior.
Consumer groups, idempotent producers, and transactions are additional semantic commitments.

Keep bookmark operations on the native endpoint.
Ordinary Kafka clients can seek to resolved offsets but cannot discover a new bookmark feature automatically.
Document acknowledgement semantics instead of assuming that a Raft quorum and Kafka's in-sync replica policy are identical.
Use bridges before promising NATS, MQTT, or RabbitMQ wire compatibility.

If unchanged Kafka applications are the primary audience, compatibility becomes foundational and the first-release scope needs to change.

## Differentiation worth considering

| Feature | What users gain |
| --- | --- |
| Named replay ranges | Replay an import or deployment interval without changing production consumer progress. |
| Retention-aware bookmarks | See whether replay is possible, when protection expires, and how much data a protection lease retains. |
| Checkpoint manifests | Associate a position with a schema version, deployment version, or external state snapshot reference. |
| Portable replay bundles | Export a bounded event interval and its bookmarks for local debugging or reproducible input datasets. |

RabbitMQ already has [server-side named offset tracking](https://www.rabbitmq.com/blog/2021/09/13/rabbitmq-streams-offset-tracking).
The distinction is the combined immutable bookmark history, atomic boundaries, and replay workflow, not naming an offset.
The product must earn adoption against a thin bookmark layer on an existing broker.

Pins can follow the first release.
A pin lease must specify its protected range or suffix, duration, and byte budget.
Serialize pin admission, expiry, and retention changes.
Reject new work rather than silently break an active promise when disk limits are reached.
An external snapshot reference does not imply an atomic transaction with the external system.

## First-release scope

The first release contains ordered streams, durable append, bounded fetch, atomic bookmarks, recent-marker listing, explicit retention errors, quotas, TLS, and three-replica failover.
Defer consumer-group balancing, Kafka certification, distributed transactions, compaction, automatic repartitioning, and object storage.

Tentative budgets are a compressed image below 50 MiB, standalone idle RSS below 128 MiB for ten streams, and standalone empty ready-to-append startup below one second on a declared Linux reference machine.
These are proposed targets, not results.
Populated crash recovery and three-node resource use need separate budgets.

The storage decision depends on equal-durability comparisons under sustained writes, retention, replay, and bookmark traffic.
Include device write amplification and recovery cost, not just a short append benchmark.
If redb meets the budgets and a custom log offers little benefit, keep the transactional engine.
If sustained ingestion, retention cost, or the global writer misses the intended workload, revisit storage and replication-group granularity together.

The remaining product choices are small installations versus scale-out throughput, and unchanged Kafka clients versus a better checkpoint workflow.
This recommendation prioritizes small installations and the checkpoint workflow.
