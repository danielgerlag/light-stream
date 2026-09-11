# Sources and baseline observations

Sources consulted on 2026-09-10. Product documentation and registry observations are distinct from measured broker performance.

[Design index](README.md) | [Proposal](proposal.md) | [Storage arena](storage-arena.md)

## Apache Kafka

The [Kafka 4.0 upgrade guide](https://kafka.apache.org/40/getting-started/upgrade/) states that Kafka 4.0 supports only KRaft and removes ZooKeeper mode.
A new service should not be compared solely against a mandatory Kafka-plus-ZooKeeper deployment.

The [Docker documentation](https://kafka.apache.org/43/getting-started/docker/) describes JVM and GraalVM-native images.
The native image is explicitly experimental and not recommended for production.
Image comparisons need a version, platform, enabled features, and a distinction between registry bytes and unpacked bytes.

[Docker Hub's Kafka 4.3.1 tag metadata](https://hub.docker.com/v2/repositories/apache/kafka/tags/4.3.1) reported 238,867,857 bytes for Linux amd64, approximately 239 decimal MB.
The platform image digest was `sha256:ccd1314e47ec76909e01f86308b4dcf2064f19f7c89759234322314b0e319e26`.
This is a registry size, not runtime memory or locally unpacked disk usage.

The [Kafka protocol specification](https://kafka.apache.org/43/design/protocol/) describes a versioned binary protocol over TCP.
Clients use metadata to find partition leaders, negotiate API versions, and refresh metadata after leader changes.
Compatibility includes observable broker behavior, not just request decoding.

## Redpanda

The [Redpanda architecture documentation](https://docs.redpanda.com/current/get-started/architecture/) describes Kafka APIs, a Raft group per topic partition, and a controller partition with snapshots.
Redpanda supports tiered storage and retains retrievable history independently of compact control state.
It is a relevant baseline for Kafka compatibility without a JVM or ZooKeeper.

## NATS JetStream

The [first-stream documentation](https://docs.nats.io/learn/jetstream/your-first-stream) describes file streams that retain messages for rereading.
It names a server-provided message sequence and a two-minute default duplicate window.

The [cluster documentation](https://docs.nats.io/learn/topologies/jetstream-in-a-cluster) describes a metadata group, per-stream replication groups, and replicated consumer state.
R3 streams tolerate one node failure when replicas occupy independent failure domains.
A cluster alone does not make an R1 stream highly available.
No claim is made here about acknowledgement fsync defaults or runtime memory.

[Docker Hub's nats:alpine tag metadata](https://hub.docker.com/v2/repositories/library/nats/tags/alpine) reported 11,104,030 bytes for Linux amd64, approximately 11.1 decimal MB.
The platform image digest was `sha256:065e8355c20a5575b3c77224be1855e8103fd148b68fba05130b9b8ddfa40ccc`.
The tag is mutable. The digest anchors the observation.
This is a registry size, not a statement about memory under JetStream workloads.

## RabbitMQ Streams

The [streams documentation](https://www.rabbitmq.com/docs/streams) describes persistent replicated append-only logs with non-destructive replay and a dedicated binary stream protocol.

The [offset-tracking article](https://www.rabbitmq.com/blog/2021/09/13/rabbitmq-streams-offset-tracking) describes storing and retrieving an application offset under a stable tracking reference.
This is a mutable consumer cursor, not the same contract as an immutable, shareable history of named bookmarks.
Offset tracking writes are stored in the stream log, so high-frequency tracking has overhead.
Resuming by a named offset should not be marketed as a new invention.

## Apache Iggy

The [Iggy website](https://iggy.apache.org/) advertises a Rust implementation, partitioning, consumer groups, multiple transports, and retention.
The site claims high throughput, but no independent performance numbers were measured during this discussion.
Rust plus lightweight streaming is not an unoccupied category.

## Storage engines

The [RocksDB overview](https://rocksdb.org/docs/getting-started.html) describes ordered keys and arbitrary byte values.
The [range-deletion article](https://rocksdb.org/blog/2018/11/21/delete-range.html) describes range tombstones, WAL persistence, flushes, and compaction.
Logical range deletion does not by itself establish immediate physical space reclamation.
`DeleteFilesInRange` can remove fully covered files subject to snapshot constraints.
Different append-only RocksDB layouts need not have the same write amplification.

The [Fjall documentation](https://docs.rs/fjall/latest/fjall/) describes a Rust LSM with forward and reverse range iteration and atomic operations across keyspaces.
It performs background maintenance. Removing C++ does not remove LSM tradeoffs.

The [redb documentation](https://docs.rs/redb/latest/redb/) describes a Rust store based on copy-on-write B+trees with ACID transactions and crash safety.
It is not an LSM engine.
The [redb design document](https://raw.githubusercontent.com/cberner/redb/master/docs/design.md) describes the single writer, MVCC, quick-repair, and allocator recovery.
Quick-repair preserves allocator state and uses two-phase commit.
Without a usable quick-repair state, crash recovery can require walking database trees.

## Consensus

The [Openraft getting-started guide](https://docs.rs/openraft/latest/openraft/docs/getting_started/index.html) separates Raft log storage, state-machine storage, and networking.
It explicitly requires completed storage writes to be durable.
The [log-storage contract](https://docs.rs/openraft/latest/openraft/storage/trait.RaftLogStorage.html) requires serialized write I/O and no holes in retained consensus history.
Using a Raft library does not implement the broker's storage, snapshot transfer, or retained-history recovery.

## Evidence limits

Some older NATS reference URLs redirect to rewritten tutorials.
An AI search summary asserted that native bookmark pinning was absent, but primary sources did not establish that absence.
That assertion is not part of the recommendation.

The source material does not establish exclusive novelty for the proposed bookmark workflow.
It also does not establish the speed, memory use, startup time, or storage amplification of the proposed service.
