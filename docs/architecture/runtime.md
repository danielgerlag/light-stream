# Production runtime architecture

Status: implemented through LS07.

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
8. LS07 adds bounded group commit, explicit publish admission, client cancellation and receipt resolution, and mutable consumer checkpoints.

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

## LS06 recovery checkpoint

Each group requires a durable operational probe from the current leader term. Reads use `ReadIndex` when it completes promptly and otherwise commit another operational probe as the linearization barrier. Request handling can adopt an already committed proof synchronously, so recovery does not depend on the background probe task winning a scheduling race.

Replicated groups keep automatic snapshots disabled. The maintenance API builds a complete snapshot, waits for its covered index, and purges only through that index. A follower behind the purge frontier receives the artifact through dedicated begin, chunk, and finish RPCs.

The receiver persists an intent, partial artifact, and independently synced progress offset. Restart truncates any unacknowledged tail and resumes from the last acknowledged byte. The receiver validates the artifact digest, identity, vote relation, and local committed index before calling Openraft's assertion-bearing install path.

Snapshot artifacts and payload values use checksummed binary framing instead of nested JSON byte arrays. In the local LS06 recovery fixture, 4 MiB of random payload produced a 4.2 MiB artifact rather than the previous 44.9 MiB artifact.

Openraft, the snapshot sender, and receiver installation exchange an immutable `SnapshotArtifact` backed by an open file. Current snapshot bytes live in content-addressed files, and RocksDB stores a small descriptor. Existing inline snapshots migrate on open. The sender performs bounded positional reads, and receiver finish hashes the staged file without `read_to_end`.

Snapshot construction streams ordered state and payload records into `LSNP0003`. Installation streams records into the inactive `ls_v2_state_a` or `ls_v2_state_b` column family with byte-bounded RocksDB batches. One synchronous metadata batch publishes the new bank and snapshot descriptor. Readers select one bank through the storage boundary. Legacy state migrates to bank A before normal startup.

The local B7 run retained exactly 1 GiB, built a 1,075,598,548-byte artifact, interrupted transfer at 64 MiB, resumed and installed 1,026 chunks, and verified 1,025 records from the stopped repaired node. The repaired-node RSS delta was 138,264,576 bytes under a locked 178,274,304-byte budget. Independent-host capacity remains blocked.

## LS06 administration

The control group stores a validated `ClusterTopology` and one durable administration operation. The topology separates authorized nodes from desired voters. Replacement first authorizes both the incoming learner and the outgoing voter, then commits a final topology that removes the retired node.

Every node runs an awaited administration reconciler unless its manifest is `retired`. Group leaders prepare the incoming node, add it as a learner, transfer leadership away from the outgoing voter to a surviving effective voter, and call native Openraft membership change. The control leader completes the operation only after every group reports the exact uniform target membership and the outgoing node durably records `retired`.

Administration request IDs are idempotent. Reuse with another body fails. Only one operation is active. Abort restores the prior topology and keeps the slot occupied until every group removes a learner that raced with cancellation. The public client follows control-leader hints and rotates seeds within one deadline.

Node manifest version 4 persists the topology. Version 3 manifests and bootstrap log entries migrate without rewriting the manifest before storage recovery succeeds. Committed control topology is authoritative when the manifest write lags or fails.

## LS07 batching and consumer progress

Each data group owns one bounded publish scheduler. Admission reserves request count, record count, and resident bytes without waiting. A full queue returns `publish_overloaded` with `definite_no_commit`.

The scheduler combines complete `PublishBatch` requests into one `GroupCommand::PublishMany`. It never combines producer identities. The state machine returns one ordered outcome per request and assigns offsets only to new successful requests.

The scheduler keeps one Raft write in flight. The oldest admitted request fixes the coalescing deadline. New arrivals do not reset it. The default delay is 200 microseconds. Server flags set queue and physical-batch limits and use microseconds for the timer.

Publish receipts store versioned success or rejection outcomes. The complete-body fingerprint covers records and the optional bookmark. A lost deterministic rejection cannot become a later success after state changes.

Mutable consumer checkpoints live in the partition's data group. The key contains the cluster, partition, and `ConsumerId`. Creation expects a missing value. Updates compare an exact `CheckpointRevision`. Success and conflict results use `MutationRequestId` receipts, so an exact retry returns the original ordered result.

Checkpoint updates cannot exceed the committed tail or move behind the current checkpoint. They do not inspect the retention floor, change bookmarks, or pin payloads. A retained checkpoint can later point below the retention floor. Fetch then returns `cursor_expired`.

The client uses one absolute deadline for publish and checkpoint route resolution, retries, and RPCs. Publish accepts a cancellation token and can resolve an ambiguous transport result through the durable receipt. `SIGINT` prints a machine-readable certainty result before `light-streamctl` exits with code 130.

## Rejected combinations

Do not combine the 0.10 adapter with 0.9 method signatures or snapshot semantics.
Do not use the fixed-leader POC as a compatibility path.
Do not make payload history live only in the purgeable Raft log.
Do not store a second permanent payload value during apply.
Do not derive payload object identity only from the producer request.
Do not put one RocksDB instance or Raft group behind every logical stream.
Do not route heartbeats, votes, or snapshot data through the public application mailbox.
Do not add security internals before LS08, but keep public and peer transport-security boundaries separate from LS01.
