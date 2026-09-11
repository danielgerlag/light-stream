# Production module map

## Problem

The workspace contains the production service and a separate fixed-leader POC control. Production callers use product identities and committed offsets. Tonic, prost, Openraft, RocksDB keys, and Raft log indexes remain inside adapters.

## Usage

An operator starts one process with explicit public and peer addresses and an owned data directory.

```sh
light-streamd \
  --data-dir .local/light-stream/node-1 \
  --public-listen 127.0.0.1:7101 \
  --peer-listen 127.0.0.1:7201 \
  --security-mode local-insecure
```

Automation selects the endpoint on every CLI call.

```sh
light-streamctl --endpoint http://127.0.0.1:7101 health
light-streamctl --endpoint http://127.0.0.1:7101 cluster bootstrap \
  --cluster-id 018f3f7e-5b3b-7c11-98f7-b65ac15f6501 \
  --stream-id 018f3f7e-5b3b-7c11-98f7-b65ac15f6502 \
  --stream-name bootstrap
```

The health command returns readiness, the build revision, the security mode, bootstrap state, and capabilities. Publish and fetch use explicit cluster, stream, partition, and producer identities.

Rust callers use `light_stream_client::Client`. The client converts generated messages into `light_stream_core` values before returning.

## Shape

`light-stream-core` owns the stable domain model. Identity newtypes have private fields. Checked constructors own string, address-independent, and range invariants. `CommittedRecordRange` stores a first offset and a nonzero count, so an empty or reversed committed range cannot be constructed. `SecurityMode` is an exhaustive enum.

`light-stream-proto` owns generated public messages and explicit conversions between protobuf messages and domain values. Core does not depend on protobuf code.

`light-stream-storage` owns the versioned RocksDB column families, payload ownership, Openraft log and state-machine traits, bounded snapshots, and the LS02a no-remote compatibility adapter.

`light-stream-server` owns process configuration, the data-directory lock, both tonic listeners, the versioned node manifest, one control Raft group, a bounded pool of data Raft groups, tonic peer transport, formation, catalog routing, recovery checks, diagnostics, and linearizable reads.

`light-stream-client` owns endpoint construction, decreasing retry deadlines, seed fallback, typed leader hints, and typed bootstrap, publish, fetch, receipt, bookmark, health, capability, and diagnostics calls. It does not expose tonic status or generated responses.

`light-stream-cli` owns stable JSON output and process exit codes. It always requires an explicit endpoint.

`light-stream-testkit` owns the independent ledger checks and Rust client driver. It does not import server internals.

`scripts/verify.py` owns builds, process lifecycle, isolated ports and data directories, fingerprints, command capture, verdicts, retained logs, and cleanup.

The public interface is small. A caller can inspect health and diagnostics, bootstrap a standalone or three-voter cluster, create and route streams, publish a bounded batch, fetch committed records, resolve a receipt, or manage partition and stream bookmarks. The server hides group placement, elections, replication, recovery, storage ordering, payload ownership, and transport conversion.

## LS03 implementation

Manifest version `3` stores a bounded group-pool configuration. Cluster formation creates one control database and the configured data-group databases on every node. The local profile uses four data groups, with process-wide RocksDB cache and write-buffer budgets divided across the five databases.

The control state machine owns idempotent create requests, stream lifecycle, partition placement, route revisions, and deletion. Data groups own payloads, offsets, and receipts. A verification-only group delay is disabled unless the explicit fault-hook flag and target group are both supplied.

The client resolves routes through the control group, follows the data leader, and refreshes a stale group or catalog revision. The CLI can supply an explicit cached route to verify that behavior.

## LS04 implementation

Data groups store immutable partition bookmarks by ID, active name, and publication order. A publish can add an after-batch bookmark to the same RocksDB state-machine write as records and its producer receipt. Matching retries return the original bookmark ID.

The control group stores stream bookmarks as explicitly independent partition positions. Creation requires one position for every stream partition and rejects a position past its current committed tail. The type and API do not represent a cross-group consistent cut.

## Synthesis decision

The repository selected the design recorded in `artifacts/LS02b/design/synthesis.md`. LS02a implements its per-group RocksDB database, compact log descriptors, single payload objects, separate control and data configurations, explicit bootstrap, and current-read barrier.

## Tradeoffs accepted

- We retain `publish-probe` as a separate LS01 compatibility path. It stays unsupported and never targets durable state.
- We use a ticketed synchronous lane as the LS02a ordered write path. LS03 adds the bounded actor when the runtime has several groups and admission pressure.
- We accept an embedded source revision supplied at build time in exchange for detecting stale release binaries without a Git repository.
- We accept a no-remote network adapter until LS02b adds tonic replication.

## Alternatives considered

A single listener would reduce one address, but it would mix public and peer admission and weaken the LS08 security boundary.

Returning tonic `UNIMPLEMENTED` for publish would be shorter, but it would leave automation dependent on a framework status instead of the versioned typed error contract.

Putting generated messages in core would remove conversion code, but it would make domain callers depend on protobuf representation and versioning.

Creating an LS01 Raft node would make the Openraft dependency more visible, but it would claim runtime consensus before storage, bootstrap, and recovery contracts exist.

## Open questions and risks

- The exact Openraft version is a prerelease. An upgrade requires a storage and network adapter review.
- LS08 must replace the local security adapter without changing core commands or public result types.
- The build revision is a source fingerprint when the verifier builds the binaries. Manual builds report `UNVERSIONED`.

## LS02b implementation

`light-stream-server::manifest` validates the durable local descriptor and exact three-member formation. Manifest version `1` remains the LS02a standalone format. Manifest version `2` records joining, forming, or active state for a fixed three-voter topology.

`light-stream-server::peer` implements `RaftNetworkV2` and `RaftNetworkFactory` for both existing Openraft configurations. Unary tonic RPCs carry bounded JSON encodings of the exact alpha.34 append, vote, pre-vote, and response types.

The seed initializes both local groups as one voter. It prepares both peers, adds them as blocking learners, waits for exact leader-side replication equality, changes both memberships to `{1, 2, 3}`, and asks each peer to prove its local activation state.

The data leader owns application writes and current reads. Followers return a public URI from the durable descriptor. The client retries the same write identity and bytes within one deadline.

## Openraft compile mapping

The exact `0.10.0-alpha.34` crate exposes projected types under `openraft::type_config::alias` when the `type-alias` feature is enabled. The production adapter uses `VoteOf`, `LogIdOf`, `SnapshotMetaOf`, `SnapshotOf`, and `StoredMembershipOf` from that module.

`light-stream-storage` implements `RaftLogReader`, `RaftLogStorage`, `RaftSnapshotBuilder`, and `RaftStateMachine`. `light-stream-server::peer` implements `RaftNetworkV2` and `RaftNetworkFactory`. The server constructs two real `Raft` values and checks durable local recovery before it announces readiness.
