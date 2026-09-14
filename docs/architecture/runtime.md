# Production runtime architecture

Status: implementation contract for LS01 to LS06. LS05 is implemented.

The arena result is preserved in `artifacts/LS02b/design/synthesis.md`.
The independent judge scored the selected design 28/30 and preferred its auditable payload ownership and exact Openraft 0.10 API mapping.
Pin `openraft = "=0.10.0-alpha.34"`.
This is an exact prerelease dependency, not a stable API.
The adapter boundary and persisted format must isolate future upgrades.

## Caller and module shape

The public surface is a Rust client, `light-streamctl`, and versioned tonic/prost services.
Callers use streams, partitions, cursors, receipts, bookmarks, replay leases, and consumer checkpoints.
They never use Raft indexes, terms, RocksDB keys, group IDs, or generated protobuf types as domain values.

Use distinct control and data Raft type configurations.
The control group owns stream names, immutable identities, placement, membership intent, and lifecycle intents.
Data groups own records, offsets, producer receipts, partition-local bookmarks, retention, replay leases, and consumer progress.

Use one supervised actor per open group.
A bounded registry reserves a slot and node-wide RocksDB memory budget before opening or activating a replica.
Peer Raft RPCs route directly to the Openraft handle.
Application commands, admission, current reads, and lifecycle operations go through the group actor.

LS02a uses one process-local ticketed write lane per group database. Tickets preserve submission order across cloned Openraft storage handles. Each mutation uses a synchronous WAL write. LS03 can replace this lane with the planned bounded actor when admission and several groups exist. The storage and runtime interfaces do not expose the temporary implementation choice.

## RocksDB and payload ownership

Each group owns one RocksDB database with column families for Raft metadata, Raft log descriptors, immutable payload objects, committed record indexes, receipts, bookmarks, retention, leases, progress, and snapshot metadata.
One ordered write lane serializes votes, log changes, application, snapshot publication, and payload ownership transitions.

Normal operation stores each payload once per replica.
The persisted Raft log contains a compact descriptor that references an immutable payload object.
The committed state stores offset-to-payload references.
The Openraft reader hydrates entries from descriptors and payloads.

Derive each payload object identity from the unique Raft log position and mutation slot.
Do not use producer request identity as the object key.
A valid duplicate can create a redundant object for that log entry, which remains reachable until purge.
This avoids shared mutable reference accounting across duplicate entries.

Track independent reachability from the Raft log, the active applied generation, the current snapshot, incoming snapshots, and active replay leases.
Do not encode these simultaneous roots as one lifecycle enum.
Raft truncation or purge removes only its ownership.
Retention removes applied-state ownership only after the logical floor advances and no lease protects the records.
Garbage collection deletes an object only when every root is absent.

## Snapshots and recovery

Use file-backed, checksummed snapshot artifacts.
The artifact contains complete retained state and all required payload objects.
A metadata-only snapshot is invalid.

Build a snapshot from a consistent RocksDB view.
Receive into a hidden staging generation.
Verify cluster, group, format, length, and digest before publication.
Install by atomically switching the active generation and current snapshot pointer.
Do not overwrite a newer local vote.

Use ordinary log suffix catch-up while required entries remain.
Use snapshot catch-up after the purge frontier.
Use learners and safe membership changes before a replacement becomes a voter.

## Openraft 0.10 boundary

Keep Openraft code inside storage and peer-network adapters.
Use the 0.10 `RaftLogStorage`, `RaftLogReader`, streamed `RaftStateMachine::apply`, file-backed snapshot types, `RaftNetworkV2`, and dedicated replication, heartbeat, and snapshot clients.

The first compile gate proves the exact method signatures against `=0.10.0-alpha.34`.
Do not mix 0.9 and 0.10 trait signatures.
Run the Openraft storage suite plus product-specific purge, retention, snapshot, and crash tests.

Persist vote and log writes in strict order.
Call `IOFlushed` only after the RocksDB durability boundary.
Commit application state before sending the application result.
Current reads require Openraft's linearizable read barrier.

## Staged implementation

1. LS01 creates typed contracts, generated APIs, closed server/client/CLI shells, verification profiles, and an exact Openraft compile spike.
2. LS02a implements one voter, one control group, one data group, RocksDB storage, append, fetch, and receipts.
3. LS02b implements three voters, tonic Raft RPCs, election, quorum refusal, suffix catch-up, and lost-response retry.
4. LS03 adds bounded group actors, lifecycle intents, placement, and independent data paths.
5. LS04 adds partition-local bookmarks and independent stream-level cursor vectors.
6. LS05 adds retention, replay leases, reclamation, and bounded reads.
7. LS06 adds snapshots, suffix and snapshot catch-up, learners, membership changes, and leader transfer.

Every stage leaves runnable release binaries and evidence from the independent oracle.

## LS02a implementation

`crates/light-stream-storage` implements the exact Openraft `0.10.0-alpha.34` log, state-machine, snapshot, and no-remote network traits. The Openraft storage conformance suite runs in `openraft_storage_conformance`.

The process opens no group database on pristine startup. The explicit bootstrap RPC creates cluster manifest version `1`, control group `1`, data group `2`, and one bootstrap stream partition. A matching retry is idempotent. A conflicting request fails.

The data log stores compact descriptors. Publish payload IDs contain the Raft term, leader node, log index, and mutation slot. Applied records and receipts refer to the same payload object. Snapshot bytes contain the complete retained state and payload set and are capped at 64 MiB.

LS02a has no remote Raft transport. Its version `1` manifest and standalone commands remain compatible.

## LS02b implementation

Each process has a node ID, an advertised public URI, an advertised peer URI, and its own data directory. Public and peer tonic services use separate listeners. The peer service accepts bounded version `1` envelopes with codec version `1`. The envelope binds the cluster, group, sender, and target before the server decodes an Openraft request.

Manifest version `2` stores the fixed formation and the local lifecycle state. A missing manifest with group storage fails startup. A joining or forming restart opens or recreates only its authorized empty group stores and never calls `initialize`. An active restart requires both existing stores, then proves the persisted bootstrap identity and exact local memberships before it reports ready.

The seed prepares both peers, initializes control group `1` and data group `2` with itself, commits the bootstrap commands, and calls `add_learner(..., true)` for each peer. It then waits until every leader replication position equals the leader's last log. Both groups change to the uniform voter set `{1, 2, 3}`.

Application writes use the data Raft handle. Fetch and receipt reads call `ensure_linearizable(ReadPolicy::ReadIndex)` before RocksDB reads. Openraft `ForwardToLeader` and quorum variants map by enum variant. A leader hint is public only when the Openraft `BasicNode` address matches the durable peer descriptor.

Each public client operation creates one absolute deadline. Endpoint connection, retry delay, and RPC time all consume the same budget. Publish expiry preserves the producer request ID and reports `ambiguous_commit` after any RPC was sent.

Each append, vote, and pre-vote peer call creates one absolute deadline from `RPCOption::soft_ttl()`. The connection and tonic request consume the same budget.

New LS02b groups set `SnapshotPolicy::Never` and retain their logs. `full_snapshot` returns `full_snapshot is unsupported until LS06`. LS02b does not claim snapshot catch-up after purge.

Diagnostics read Openraft metrics without triggering elections, membership changes, snapshots, purges, or writes. They report both memberships, commit frontiers, application progress, replication progress, durable peers, and unsupported claims.

The local verifier starts three release processes. It records exact control and data memberships on every running node. It validates typed leader hints against diagnostics and topology. Catch-up requires leader-side replication equality.

During majority loss, the verifier leaves the diagnosed old leader alive and requires publish, fetch, and receipt refusal. This proves minority refusal but does not test a selective partition with a healthy remote majority.

For lost-response recovery, the verifier drops the first response, kills that data leader, waits for another leader, and reads the receipt before retrying the publish. The retry and full restart must preserve the same range. Independent-host HA evidence remains blocked.

## LS03 implementation

Cluster formation creates a fixed, configured pool of data groups and one control group. The local profile uses groups 2 through 5. Logical streams and partitions map onto these groups through committed catalog entries. Stream count does not allocate additional databases.

The control group applies idempotent create requests, activation, deletion, name reuse, and route revisions. Data requests carry the expected group and route revision. A mismatch returns `stale_route`, and the client resolves the current route before retrying the same producer identity.

Diagnostics report slot counts, per-group leaders and progress, and divided RocksDB cache and write-buffer budgets. A verification-only delay hook can target one group and is disabled by default.

The LS03 verifier proves bounded stores, routes across four groups, independent byte ledgers, response-loss create idempotency, quotas, deleted identity rejection, stale-route refresh, unrelated-group progress during a targeted delay, and full-cluster restart.

Single-process failure while several data groups are active remains deferred to LS06. The LS03 verifier does not claim that partial-node multi-group recovery is complete.

## LS04 implementation

Each data group indexes partition bookmarks by immutable ID, active name, and monotonically increasing publication sequence. Atomic publish-plus-bookmark derives the bookmark ID from the committed Raft log position and stores records, the producer receipt, and the bookmark in one synchronous state-machine write. A name conflict rejects the whole application result without advancing the partition offset.

Deletion removes the active name and publication-order entries but retains the old ID as a deleted tombstone. A later bookmark may reuse the name with a new ID. Retrying the old create request returns the deleted old object and cannot rebind it.

Newest-first listing reads only the bookmark order index. The first page fixes a publication ceiling. Later pages use that ceiling and an exclusive publication sequence, so concurrent creation cannot duplicate or insert entries into the captured traversal.

Stream bookmarks live in the control group as vectors with one committed position per stream partition. The server performs a linearizable tail check in each owning data group before it commits the vector. Positions are independent and the API makes no cross-group consistent-cut guarantee.

The LS04 verifier uses three release processes. It proves exact resume, lost-response retry, conflict atomicity, backdated ordering, stable pagination, deletion and name reuse, stream vectors, indexed last-100 lookup over 10,000 records, and full-cluster restart. Partial-node recovery remains deferred to LS06.

## LS05 implementation

Each data group owns a monotonic retention floor and durable replay leases. Lease admission and floor advancement share one Raft order. Admission first protects an exact range below a later floor. Floor advancement first rejects a lease that starts below the floor.

The leader writes a bounded wall-clock observation into each retention command. The state machine advances a monotonic safe lower bound and never reads the local clock. The local profile declares a two-second maximum clock error. A hard total lifetime prevents endless renewal.

Retention maintenance is a bounded replicated command. It skips active lease ranges, deletes expired record indexes, clears applied-state ownership, and advances a durable cursor. Release or expiry moves the cursor back to the protected range so maintenance can revisit it.

A leader-only worker runs every 250 milliseconds only when a partition needs expiry or reclaim work. A deposed leader stops proposing because it no longer reports `ServerState::Leader`.

Logical expiry does not imply physical payload deletion. Retained Raft entries still own payloads because production groups use `SnapshotPolicy::Never`. LS05 reports those bytes as `raft_only_bytes`. Raft-owned payload reclamation remains LS06.

Legacy record values migrate to schema version 2 before the store opens. The migration adds payload length and cumulative partition bytes in bounded synchronous batches.

## Rejected combinations

Do not combine the 0.10 adapter with 0.9 method signatures or snapshot semantics.
Do not use the fixed-leader POC as a compatibility path.
Do not make payload history live only in the purgeable Raft log.
Do not store a second permanent payload value during apply.
Do not derive payload object identity only from the producer request.
Do not put one RocksDB instance or Raft group behind every logical stream.
Do not route heartbeats, votes, or snapshot data through the public application mailbox.
Do not add security internals before LS08, but keep public and peer transport-security boundaries separate from LS01.
