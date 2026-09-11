# Storage arena decision record

Status: provisional recommendation from the 2026-09-10 discussion.

This is the historical paper arena. The [measured POCs](../experiments/high-volume.md) and [high-volume proposal](proposal.md) supersede its small-installation selection.

[Design index](README.md) | [Proposal](proposal.md) | [Sources](sources.md) | [Evaluation](evaluation.md)

Three independent candidates received the same task and research contract.
Each compared RocksDB, custom segments, Fjall, and redb.
An independent judge scored the completed proposals.
This was a design arena, not an implemented storage benchmark.
There were no dropouts.

## Candidate directions

| Candidate | Preferred design | Main argument | Distinctive tradeoff |
| --- | --- | --- | --- |
| 1 | Append-only segments shared by replication and replay, with per-stream Raft groups and a catalog group. | Sequential data and whole-segment retention fit an event log. | The broker owns storage recovery and cross-group lifecycle coordination. Combined bookmarks point after the batch. |
| 2 | Append-only segments shared by replication and replay, with checkpointed metadata and per-stream Raft groups. | Bound recovery work and avoid a second payload representation. | Includes a constrained Kafka profile early. Combined bookmarks point at the batch start. |
| 3 | One redb database per node and one installation-wide Raft group. | Delegate local transactional safety to an existing engine and reduce coordination boundaries. | One database writer and one leader limit aggregate throughput. Combined bookmarks point after the batch. |

## Scores and selection

Each criterion received a score from 0 to 5.
Scores describe architectural coherence, not performance or implementation correctness.

| Criterion | Lead C1 | Lead C2 | Lead C3 | Judge C1 | Judge C2 | Judge C3 |
| --- | --- | --- | --- | --- | --- | --- |
| Bookmark correctness | 5 | 5 | 5 | 4 | 4 | 4 |
| Storage fit | 5 | 4 | 4 | 4 | 5 | 4 |
| HA correctness | 5 | 4 | 4 | 5 | 5 | 5 |
| Lightweight scope | 4 | 4 | 4 | 4 | 3 | 5 |
| Compatibility and differentiation | 4 | 4 | 5 | 4 | 4 | 5 |
| Extensibility and evidence | 4 | 4 | 4 | 4 | 4 | 4 |
| Total | 27 | 25 | 26 | 25 | 25 | 27 |

The lead initially chose candidate 1 for sequential log fit.
The judge chose candidate 3 for one transactional store and one coordination group.
The difference exposed a scope assumption.
The lead emphasized eventual high-throughput independent streams, while the judge emphasized a small-deployment first release.
That first-release target is a proposed product scope, not a requirement the user already settled.

The synthesized proposal adopts candidate 3 for that explicitly limited first release.
redb owns local transactional crash safety, and one Raft group removes coordination between stream groups and a catalog group.
This gives the next maintainer fewer custom crash-consistency rules.
It does not establish that redb is faster or uses less memory.
The global writer, full replication on every node, and copy-on-write costs are explicit limits.
Custom segments remain the main challenger if measured ingestion and retention justify their engineering cost.

## Ideas incorporated from the other candidates

Candidate 1 contributed explicit pin admission, protection scope, expiry, quota exhaustion, and disk headroom requirements.
It also contributed one outstanding request per producer session and an explicit distinction between HA and disaster recovery.

Candidate 2 contributed ordered validation of historical cursors, names, quotas, tail capture, and duplicate requests.
It also contributed bounded queued and unapplied work, plus bookmark listing under concurrent ingest and retention as an evaluation workload.

The judge and lead identified a replay-retention gap shared by all candidates.
The synthesis distinguishes short bounded reads, abortable replay across multiple requests, and lease-protected complete replay.
A complete replay promise requires a bounded range lease at admission.

The synthesis keeps after-batch bookmark semantics.
It does not silently combine candidate 2's before-batch convention with the other candidates.

## Rejected combinations and claims

Combining the single-group redb design with per-stream segment groups would reintroduce both coordination and storage complexity.
That combination is not part of the proposal.

A logical single payload does not imply a single physical disk write.
Raft persistence and committed state application remain two durable phases.
redb quick-repair adds its own local commit work.

An experimental Kafka listener is outside the initial native-HA release.
Protocol parsing does not establish support for consumer groups, transactions, or idempotence.
Kafka's in-sync replica semantics are not identical to a majority vote.

Deleted human bookmark names do not need permanent reservation.
Immutable IDs remain terminal, name reuse is explicit, and requests outside a bounded receipt window fail.

Arbitrary 20 percent performance switching thresholds were rejected.
Target workloads and resource budgets must determine which engine wins.

A stored external snapshot reference does not establish an atomic external transaction.
Named offsets and replay are not exclusively novel features.
The historical Kafka-plus-ZooKeeper image sum is not the current minimum deployment.

## Evidence limits

The lead read all three candidate artifacts and the judge's rationale.
Primary sources support the KRaft correction, registry-size observations, storage-engine mechanisms, and Openraft durability obligations.
The redb quick-repair and allocator-recovery design was also inspected.

The service remains unimplemented and its performance remains unmeasured.
The [evaluation cases](evaluation.md) describe obligations for a future implementation, not completed tests.

## Principles that changed decisions

| Principle | Decision it changed |
| --- | --- |
| Model the Domain | Separate immutable bookmarks from mutable consumer progress and make cursor expiry explicit. |
| Laziness Protocol | Prefer one transactional store and one Raft group over custom storage and per-stream lifecycle coordination initially. |
| Build the Lever | Give every candidate the same research recipe and comparison contract. |
| Redesign from First Principles | Keep bookmark publication, append results, and retry receipts inside one committed state transition. |
| Prove It Works | Keep documented mechanisms separate from unmeasured resource and speed targets. |
