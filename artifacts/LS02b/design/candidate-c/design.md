# LS02b candidate C: staged three-voter formation and direct leader routing

## Caller usage

The operator starts three separately addressed processes. Each process owns one data directory and advertises stable public and peer addresses. A listen address may use port `0` in tests, but a three-voter bootstrap request uses the resolved advertised addresses from each ready announcement.

```sh
light-streamd \
  --node-id 1 \
  --data-dir artifacts/run/scratch/node-1 \
  --public-listen 127.0.0.1:7101 \
  --peer-listen 127.0.0.1:7201 \
  --advertise-public http://127.0.0.1:7101 \
  --advertise-peer http://127.0.0.1:7201 \
  --security-mode local-insecure

light-streamd \
  --node-id 2 \
  --data-dir artifacts/run/scratch/node-2 \
  --public-listen 127.0.0.1:7102 \
  --peer-listen 127.0.0.1:7202 \
  --advertise-public http://127.0.0.1:7102 \
  --advertise-peer http://127.0.0.1:7202 \
  --security-mode local-insecure

light-streamd \
  --node-id 3 \
  --data-dir artifacts/run/scratch/node-3 \
  --public-listen 127.0.0.1:7103 \
  --peer-listen 127.0.0.1:7203 \
  --advertise-public http://127.0.0.1:7103 \
  --advertise-peer http://127.0.0.1:7203 \
  --security-mode local-insecure
```

The operator sends one formation request to the declared bootstrap node. The server prepares the other two nodes as learners, catches up both groups, and changes both memberships to `{1, 2, 3}`. The command returns only after both committed memberships are uniform and every node can recover the bootstrap stream.

```sh
light-streamctl \
  --endpoint http://127.0.0.1:7101 \
  cluster bootstrap \
  --cluster-id 018f3f7e-5b3b-7c11-98f7-b65ac15f6501 \
  --stream-id 018f3f7e-5b3b-7c11-98f7-b65ac15f6502 \
  --stream-name bootstrap \
  --bootstrap-node-id 1 \
  --node 1,http://127.0.0.1:7101,http://127.0.0.1:7201 \
  --node 2,http://127.0.0.1:7102,http://127.0.0.1:7202 \
  --node 3,http://127.0.0.1:7103,http://127.0.0.1:7203
```

Existing one-voter usage remains valid. Omitting `--node` keeps the LS02a standalone formation and uses the local node descriptor.

Callers can seed the client with one or more public endpoints. The client sends each application request to a real group leader. A follower returns a typed leader hint. The client reconnects to that endpoint and resends the same request identity. No server proxies an application request.

```rust
use std::time::Duration;
use light_stream_client::{Client, ClientOptions, RetryPolicy};

let client = Client::connect_cluster(
    [
        "http://127.0.0.1:7101",
        "http://127.0.0.1:7102",
        "http://127.0.0.1:7103",
    ],
    ClientOptions {
        request_timeout: Duration::from_secs(2),
        operation_timeout: Duration::from_secs(10),
        retry_policy: RetryPolicy::LeaderHintsAndSeeds,
    },
).await?;

let receipt = client.publish(batch).await?;
let page = client.fetch(cluster, partition, receipt.range().first(), 128).await?;
```

The existing constructor remains a one-seed convenience:

```rust
let client = Client::connect("http://127.0.0.1:7102").await?;
```

The CLI accepts repeated `--endpoint` values. One value preserves the current grammar. Verification can disable retries to test the exact endpoint response from an isolated old leader.

```sh
light-streamctl \
  --endpoint http://127.0.0.1:7103 \
  --no-retry \
  diagnostics
```

`diagnostics` is observational. It reports local Openraft state and replication progress. It cannot elect a leader, change membership, append a record, install a snapshot, or repair storage.

## Problem and constraints

LS02a already has durable one-voter control and data groups, compact payload descriptors, complete local snapshots, linearizable reads, and durable producer receipts. LS02b must add a real three-voter path without replacing those storage decisions.

The non-obvious part is cluster formation. Empty followers need durable cluster and group identity before their peer handlers can accept Openraft RPCs. Openraft requires each future voter to exist as a learner before `change_membership`. At the same time, LS02b must not introduce LS03 placement, LS06 replacement, or LS08 peer identity.

The chosen design uses one bounded formation operation:

1. Persist the intended topology.
2. Prepare the two remote nodes as empty learners.
3. Initialize only the bootstrap node as a one-voter cluster.
4. Add both learners to both groups with `blocking = true`.
5. Change the data membership to three voters.
6. Change the control membership to three voters last.
7. Activate each node only after local facts prove that both groups reached the target.

The control group stays one-voter until the data group has completed its membership change. This ordering keeps the bootstrap coordinator the control leader during every step that needs coordination. After activation, elections are unrestricted.

The accepted LS02a evidence at source revision `cea7171efcd004fa3c5e7c244a3a3f1779b3fc701f75d283abf278e616ba3b2e` is the regression floor. `artifacts/LS02a/final-3/result.json` is `PASS`. The storage suite ran Openraft conformance and the product snapshot and corruption tests. The lost-response fixture returned the original offset `2`, and the conflicting retry returned `receipt_conflict`. That run explicitly reported three-node replication, election failover, and quorum refusal as LS02b work, snapshot catch-up as LS06 work, secured mode as LS08 work, and independent hosts as blocked.

## Scope and claims

LS02b supports:

- one control group with ID `1`;
- one data group with ID `2`;
- one bootstrap stream partition;
- either one voter or exactly three voters;
- direct tonic peer RPCs for append, vote, and pre-vote;
- real learner catch-up and joint-consensus membership changes;
- client leader-hint retries without server-side proxying;
- quorum refusal, local partitions, slow-follower recovery, log-suffix catch-up, and lost-response retry;
- complete E02 through E07 and E09 local evidence;
- the existing LS02a local snapshot build, install, and corruption tests.

LS02b does not claim:

- snapshot transfer or catch-up after Raft log purge;
- node replacement, voter removal, leader transfer, or topology changes after activation;
- more than one data group or any LS03 placement;
- peer authentication, TLS, authorization, or secured mode;
- independent-host high availability or capacity;
- retention, replay leases, bookmarks, or consumer progress.

E08 remains `UNSUPPORTED_LS06_SNAPSHOT_AFTER_PURGE`. Independent-host parts of E03, E04, E06, and E08 remain `BLOCKED_REFERENCE_HOSTS`.

## Concrete Rust types

### Stable domain additions in `light-stream-core`

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsensusGroup {
    Control,
    Data,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LeaderHint {
    group: ConsensusGroup,
    node_id: NodeId,
    public_endpoint: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestOutcome {
    DefiniteNoCommit,
    AmbiguousCommit,
    NotApplicable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FormationState {
    Pristine,
    Joining,
    Forming,
    Active,
}
```

`LeaderHint.public_endpoint` stays private and passes a checked constructor. Core validates its length, scheme syntax, and absence of user information. Server configuration validates loopback policy. LS08 later adds trust policy.

`DomainError` gains structured variants. Existing variants and error codes remain stable.

```rust
pub enum DomainError {
    // Existing LS02a variants remain.
    ClusterForming,
    BootstrapWrongNode { expected: NodeId },
    JoinConflict { reason: String },
    NotLeader {
        group: ConsensusGroup,
        leader: Option<LeaderHint>,
    },
    QuorumUnavailable {
        group: ConsensusGroup,
        outcome: RequestOutcome,
    },
    RequestDeadline {
        operation: String,
        outcome: RequestOutcome,
    },
    PeerUnavailable {
        node: NodeId,
        reason: String,
    },
}
```

Expected errors remain typed application results. Tonic statuses report malformed protobuf, oversized messages, and peer transport failures.

### Server-only topology types

Addresses do not move into `light-stream-core`. `light-stream-server` owns runtime topology and transport parsing.

```rust
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct PublicEndpoint(url::Url);

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct PeerEndpoint(url::Url);

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct NodeDescriptor {
    pub id: NodeId,
    pub public: PublicEndpoint,
    pub peer: PeerEndpoint,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct StandaloneTopology {
    pub local: NodeDescriptor,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ThreeVoterTopology {
    pub bootstrap_node: NodeId,
    pub nodes: [NodeDescriptor; 3],
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) enum ClusterTopology {
    Standalone(StandaloneTopology),
    ThreeVoter(ThreeVoterTopology),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct FormationSpec {
    pub bootstrap: BootstrapSpec,
    pub topology: ClusterTopology,
    pub topology_digest: [u8; 32],
}
```

Only constructors can create `ThreeVoterTopology`. They sort by `NodeId` and reject:

- a node count other than three;
- duplicate node IDs;
- node ID `0`;
- duplicate public endpoints;
- duplicate peer endpoints;
- one endpoint used as both public and peer transport;
- a missing bootstrap node;
- a local descriptor that differs from startup configuration;
- a non-loopback endpoint in `local-insecure` mode without the existing explicit override;
- a digest that does not match the canonical sorted representation.

The `[NodeDescriptor; 3]` field makes a two-voter or four-voter LS02b topology unrepresentable after boundary parsing.

### Durable node manifest

Pristine storage has no manifest. Every non-pristine state has one complete, checksummed manifest.

```rust
pub(crate) const NODE_MANIFEST_VERSION: u32 = 2;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct NodeManifest {
    pub format_version: u32,
    pub checksum: [u8; 32],
    pub state: PersistedNodeState,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub(crate) enum PersistedNodeState {
    Joining(JoiningManifest),
    Forming(FormingManifest),
    Active(ActiveManifest),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct JoiningManifest {
    pub local_node: NodeId,
    pub formation: FormationSpec,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct FormingManifest {
    pub local_node: NodeId,
    pub formation: FormationSpec,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ActiveManifest {
    pub local_node: NodeId,
    pub formation: FormationSpec,
}
```

`cluster.json` remains the root path so existing operational tooling has one place to inspect. The writer serializes a versioned envelope, syncs the file, renames it, and syncs the parent directory as LS02a does.

The reader supports exactly two formats:

- version `1`, the current LS02a `ClusterManifest`, imported once as `Active(Standalone)`;
- version `2`, the state enum above.

The version `1` import requires the configured node ID to match, the existing control and data group IDs to remain `1` and `2`, and both stored bootstrap specs to match. It then writes version `2`. This is a narrow LS02a compatibility path, not the general upgrade system planned for LS09.

No manifest stores an asserted current leader, Raft term, commit index, or membership progress. Those values come from Openraft and RocksDB during reconciliation. A stale local step counter therefore cannot authorize activation.

### Separate control and data handles

```rust
pub(crate) struct ControlGroup {
    pub raft: openraft::Raft<
        ControlRaftConfig,
        RocksStateMachine<ControlRaftConfig>,
    >,
    pub reader: CommittedStateReader,
}

pub(crate) struct DataGroup {
    pub raft: openraft::Raft<
        DataRaftConfig,
        RocksStateMachine<DataRaftConfig>,
    >,
    pub reader: CommittedStateReader,
}

pub(crate) struct ReplicaHandles {
    pub control: Arc<ControlGroup>,
    pub data: Arc<DataGroup>,
}

pub(crate) enum ValidatedPeerRoute {
    Control {
        sender: NodeId,
        handle: Arc<ControlGroup>,
    },
    Data {
        sender: NodeId,
        handle: Arc<DataGroup>,
    },
}
```

There is no `Raft<C>` getter keyed by a raw `u64`. Peer services choose `Control` or `Data` from their generated service type, then validate the fixed group ID before they can obtain a handle. This prevents a decoded data request from reaching the control handle.

## Protobuf RPCs

### Public API changes

Existing field numbers do not change. The new fields extend current messages.

```proto
enum ConsensusGroup {
  CONSENSUS_GROUP_UNSPECIFIED = 0;
  CONSENSUS_GROUP_CONTROL = 1;
  CONSENSUS_GROUP_DATA = 2;
}

enum RequestOutcome {
  REQUEST_OUTCOME_UNSPECIFIED = 0;
  REQUEST_OUTCOME_DEFINITE_NO_COMMIT = 1;
  REQUEST_OUTCOME_AMBIGUOUS_COMMIT = 2;
  REQUEST_OUTCOME_NOT_APPLICABLE = 3;
}

enum FormationState {
  FORMATION_STATE_UNSPECIFIED = 0;
  FORMATION_STATE_PRISTINE = 1;
  FORMATION_STATE_JOINING = 2;
  FORMATION_STATE_FORMING = 3;
  FORMATION_STATE_ACTIVE = 4;
}

message NodeDescriptor {
  uint64 node_id = 1;
  string public_endpoint = 2;
  string peer_endpoint = 3;
}

message LeaderHint {
  ConsensusGroup group = 1;
  uint64 node_id = 2;
  string public_endpoint = 3;
}

message BootstrapRequest {
  string cluster_id = 1;
  string stream_id = 2;
  string stream_name = 3;
  repeated NodeDescriptor initial_nodes = 4;
  uint64 bootstrap_node_id = 5;
}

message ErrorResult {
  string code = 1;
  string message = 2;
  bool retryable = 3;
  RequestOutcome outcome = 4;
  LeaderHint leader_hint = 5;
  ConsensusGroup group = 6;
}

message HealthResponse {
  bool ready = 1;
  string revision = 2;
  SecurityMode security_mode = 3;
  string public_address = 4;
  string peer_address = 5;
  bool bootstrapped = 6;
  string cluster_id = 7;
  uint64 node_id = 8;
  FormationState formation_state = 9;
  bool application_ready = 10;
}
```

Diagnostics use one read-only RPC:

```proto
message DiagnosticsRequest {}

message ReplicationProgress {
  uint64 node_id = 1;
  uint64 matched_index = 2;
  bool matched_index_known = 3;
}

message GroupDiagnostics {
  ConsensusGroup group = 1;
  uint64 group_id = 2;
  string server_state = 3;
  uint64 current_term = 4;
  uint64 leader_node_id = 5;
  bool leader_known = 6;
  repeated uint64 voters = 7;
  repeated uint64 learners = 8;
  uint64 last_log_index = 9;
  bool last_log_known = 10;
  uint64 cluster_committed_index = 11;
  bool cluster_committed_known = 12;
  uint64 last_applied_index = 13;
  bool last_applied_known = 14;
  uint64 snapshot_index = 15;
  bool snapshot_known = 16;
  uint64 purged_index = 17;
  bool purged_known = 18;
  repeated ReplicationProgress replication = 19;
  uint64 last_quorum_acked_age_ms = 20;
  bool last_quorum_acked_known = 21;
}

message DiagnosticsResponse {
  uint64 node_id = 1;
  string cluster_id = 2;
  FormationState formation_state = 3;
  bytes topology_digest = 4;
  GroupDiagnostics control = 5;
  GroupDiagnostics data = 6;
  repeated string unsupported_claims = 7;
}

service LightStream {
  // Existing methods remain.
  rpc Diagnostics(DiagnosticsRequest) returns (DiagnosticsResponse);
}
```

Diagnostics do not promise a consistent cut across both groups. Each `GroupDiagnostics` value is one clone of that group's latest `RaftMetrics`.

### Peer API

The peer schema has separate control and data services. The service path supplies the first routing type check. The envelope supplies cluster, group, sender, and recipient checks before deserialization.

```proto
enum PeerGroupKind {
  PEER_GROUP_KIND_UNSPECIFIED = 0;
  PEER_GROUP_KIND_CONTROL = 1;
  PEER_GROUP_KIND_DATA = 2;
}

message PeerFrame {
  uint32 protocol_version = 1;
  string cluster_id = 2;
  uint64 group_id = 3;
  PeerGroupKind group_kind = 4;
  uint64 sender_node_id = 5;
  uint64 recipient_node_id = 6;
  bytes topology_digest = 7;
  bytes payload = 8;
  uint64 payload_length = 9;
  bytes payload_sha256 = 10;
}

message RaftReply {
  PeerFrame frame = 1;
}

message PrepareJoinRequest {
  uint32 protocol_version = 1;
  string cluster_id = 2;
  uint64 bootstrap_node_id = 3;
  repeated NodeDescriptor nodes = 4;
  bytes topology_digest = 5;
  string stream_id = 6;
  string stream_name = 7;
}

message PrepareJoinResponse {
  uint64 node_id = 1;
  bytes topology_digest = 2;
  FormationState formation_state = 3;
}

message ActivateRequest {
  uint32 protocol_version = 1;
  string cluster_id = 2;
  bytes topology_digest = 3;
}

message ActivateResponse {
  uint64 node_id = 1;
  FormationState formation_state = 2;
}

service PeerLifecycle {
  rpc Probe(PeerProbeRequest) returns (PeerProbeResponse);
  rpc PrepareJoin(PrepareJoinRequest) returns (PrepareJoinResponse);
  rpc Activate(ActivateRequest) returns (ActivateResponse);
}

service ControlRaftPeer {
  rpc AppendEntries(PeerFrame) returns (RaftReply);
  rpc Vote(PeerFrame) returns (RaftReply);
  rpc PreVote(PeerFrame) returns (RaftReply);
}

service DataRaftPeer {
  rpc AppendEntries(PeerFrame) returns (RaftReply);
  rpc Vote(PeerFrame) returns (RaftReply);
  rpc PreVote(PeerFrame) returns (RaftReply);
}
```

LS02b deliberately defines no snapshot RPC. `RaftNetworkV2::full_snapshot` returns an explicit `StreamingError::Unreachable` whose message is `snapshot transport is unsupported until LS06`. Automatic snapshots and purge are disabled in the LS02b runtime, so E02 through E07 never depend on that path.

The wire codec is `bincode 2` with its serde adapter and a fixed version-1 configuration. JSON is rejected for peer RPCs because an 8 MiB `Vec<u8>` becomes a much larger JSON integer array. The peer schema version isolates the codec from future Openraft upgrades.

`RaftReply.frame.payload` contains the encoded `Result<Response, RaftError<C>>`. Routing failures use tonic status and never enter Openraft. A valid remote Openraft error becomes `RPCError::RemoteError(RemoteError::new_with_node(...))`.

## Exact Openraft 0.10.0-alpha.34 mapping

The design is based on the local crate at:

```text
/Users/danielgerlag/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/openraft-0.10.0-alpha.34
```

`Cargo.lock` pins checksum `06fe14d529f8fc098fbb4b9655d5db9b7ee0594e6a9d4e220aa931618de17b1f`.

The implementation must compile these exact calls:

```rust
raft.initialize(BTreeMap::from([
    (local_id, BasicNode::new(local_peer_endpoint)),
])).await?;

raft.add_learner(node_id, BasicNode::new(peer_endpoint), true).await?;

raft.change_membership(
    BTreeSet::from([node_1, node_2, node_3]),
    false,
).await?;

raft.ensure_linearizable(ReadPolicy::ReadIndex).await?;

raft.append_entries(request).await?;
raft.vote(request).await?;
raft.pre_vote(request).await?;
raft.install_full_snapshot(vote, snapshot).await?;
```

The relevant exact signatures are:

```rust
pub async fn add_learner(
    &self,
    id: C::NodeId,
    node: C::Node,
    blocking: bool,
) -> Result<ClientWriteResponse<C>, RaftError<C, ClientWriteError<C>>>;

pub async fn change_membership(
    &self,
    members: impl Into<ChangeMembers<C::NodeId, C::Node>>,
    retain: bool,
) -> Result<ClientWriteResponse<C>, RaftError<C, ClientWriteError<C>>>;

pub async fn ensure_linearizable(
    &self,
    read_policy: ReadPolicy,
) -> Result<ReadLogId<C>, RaftError<C, LinearizableReadError<C>>>;
```

Passing a `BTreeSet<NodeId>` to `change_membership` converts to `ChangeMembers::ReplaceAllVoters`. Openraft rejects a target voter that is not already a learner. `change_membership` writes a joint membership and then a uniform membership.

The network adapter implements all required `RaftNetworkV2` methods:

```rust
impl<C> RaftNetworkV2<C> for TonicRaftNetwork<C>
where
    C: PeerRaftConfig,
{
    type SnapshotData = Vec<u8>;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<C>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<C>, RPCError<C>>;

    async fn vote(
        &mut self,
        rpc: VoteRequest<C>,
        option: RPCOption,
    ) -> Result<VoteResponse<C>, RPCError<C>>;

    async fn pre_vote(
        &mut self,
        rpc: VoteRequest<C>,
        option: RPCOption,
    ) -> Result<VoteResponse<C>, RPCError<C>>;

    async fn full_snapshot(
        &mut self,
        vote: VoteOf<C>,
        snapshot: SnapshotOf<C, Vec<u8>>,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<C>, StreamingError<C>>;
}
```

`pre_vote` is implemented even if LS02b leaves `Config::enable_pre_vote` at its default. The Openraft default grants pre-vote without transport, which is unsuitable for a production adapter if the option is enabled later.

`RaftNetworkFactory::Network` is one type, so traffic isolation is a value carried by that type:

```rust
enum PeerTrafficClass {
    Replication,
    Heartbeat,
    Snapshot,
}

struct TonicRaftNetwork<C> {
    local: NodeId,
    target: NodeId,
    target_node: BasicNode,
    group: PeerGroup,
    traffic: PeerTrafficClass,
    channels: PeerChannelPool,
    marker: PhantomData<C>,
}
```

`new_client`, `new_heartbeat_client`, and `new_snapshot_client` return the same network type with different `PeerTrafficClass` values and distinct tonic channels. Snapshot clients exist to satisfy the exact factory contract, but `full_snapshot` refuses before sending data.

The receiver maps generated methods directly to:

```rust
ControlRaftPeer::append_entries -> ControlGroup.raft.append_entries(...)
ControlRaftPeer::vote           -> ControlGroup.raft.vote(...)
ControlRaftPeer::pre_vote       -> ControlGroup.raft.pre_vote(...)

DataRaftPeer::append_entries    -> DataGroup.raft.append_entries(...)
DataRaftPeer::vote              -> DataGroup.raft.vote(...)
DataRaftPeer::pre_vote          -> DataGroup.raft.pre_vote(...)
```

`RaftMetrics` supplies the diagnostics fields used here: `state`, `current_term`, `current_leader`, `last_log_index`, `cluster_committed`, `last_applied`, `snapshot`, `purged`, `membership_config`, `committed_membership_config`, `replication`, and `last_quorum_acked`.

## Bounded transport and deadlines

The public API keeps `MAX_PUBLIC_MESSAGE_BYTES = 9 MiB`. The server and client set both tonic encoding and decoding limits.

Peer services use these hard limits:

| Service | Maximum decoded request or response |
| --- | ---: |
| `PeerLifecycle` | 256 KiB |
| `ControlRaftPeer` | 1 MiB |
| `DataRaftPeer` | 10 MiB |

The data limit holds one maximum 8 MiB publish batch plus Openraft and codec overhead. The encoder checks the serialized payload before constructing `PeerFrame`. The decoder checks `payload_length`, the service limit, and SHA-256 before bincode decoding.

Openraft passes `RPCOption` to network calls. The client sets the tonic request timeout to `option.soft_ttl()`. It maps a deadline to:

```rust
RPCError::Timeout(openraft::errors::Timeout {
    action,
    id: local_id,
    target,
    timeout: option.soft_ttl(),
})
```

Connection refusal and a closed channel become `RPCError::Unreachable`. A transient HTTP/2 stream failure after connection becomes `RPCError::Network`. A decoded remote `RaftError` becomes `RPCError::RemoteError`.

The server also caps work so a client cannot request an unbounded handler lifetime:

| Operation | Server cap |
| --- | ---: |
| vote or pre-vote | 1 second |
| heartbeat or append | 2 seconds |
| prepare join | 10 seconds |
| activate | 10 seconds |
| public publish, fetch, or receipt | 2 seconds |
| public bootstrap | 30 seconds |

The 30-second bootstrap cap is an API attempt deadline, not the whole formation lifetime. If the call times out, the durable `Forming` manifest lets the same request resume. The client reports an ambiguous bootstrap result and resolves it with health and diagnostics.

Openraft uses separate heartbeat clients. LS02b creates a separate tonic channel for them, so an 8 MiB data append cannot queue ahead of a linearizable-read heartbeat on the same HTTP/2 connection. The first implementation uses Openraft's default sequential `stream_append` adapter. Native bidirectional streaming is deferred until evidence shows that the sequential path misses a later performance budget.

## Startup, bootstrap, and join state machine

### Startup

```text
No cluster.json and no nonempty group directory
    -> Pristine
    -> listeners serve Health, Diagnostics, Bootstrap, and PrepareJoin
    -> publish, fetch, and receipt return not_bootstrapped

No cluster.json with a nonempty group directory
    -> startup failure
    -> refuse to infer or recreate cluster identity

Joining manifest
    -> open empty or existing control and data stores
    -> construct both Raft handles without initialize()
    -> serve peer RPCs immediately
    -> reconcile local membership and bootstrap application
    -> Active when both groups prove the target state

Forming manifest
    -> open both stores and Raft handles
    -> resume the idempotent formation algorithm
    -> Active when both groups prove the target state

Active manifest
    -> open both stores
    -> validate group identity and bootstrap application
    -> wait_for_recovery(10 seconds) on both groups
    -> application_ready

Invalid, conflicting, or unsupported manifest
    -> startup failure
    -> no storage reinitialization
```

`ready` continues to mean that the process can answer health and bootstrap. `application_ready` means that the node is `Active` and both groups completed recovery. This preserves the LS02a pristine startup contract without letting a joining node claim write readiness.

### Three-voter bootstrap

`ClusterManager::bootstrap` runs this idempotent algorithm:

1. Parse and validate `FormationSpec`.
2. Require `bootstrap_node_id == local_node_id`.
3. If the manifest is `Active` with the same digest and bootstrap spec, return the existing result.
4. If the manifest is `Forming` with the same digest, resume.
5. Reject every other non-pristine state as `bootstrap_conflict`.
6. Persist `FormingManifest` before creating group storage.
7. Call `PeerLifecycle.PrepareJoin` on nodes `2` and `3`.
8. Each follower validates its local descriptor, persists `JoiningManifest`, opens both empty group stores, creates both Raft handles, and returns only when peer routing is ready.
9. Create or open the bootstrap node's control and data stores.
10. Initialize each pristine local group with only the bootstrap node:

   ```rust
   raft.initialize(BTreeMap::from([(
       local_id,
       BasicNode::new(local_peer_endpoint),
   )])).await?;
   ```

11. Commit `BootstrapControl` and `BootstrapData` if each state machine lacks the exact bootstrap spec.
12. For each remote node in ascending `NodeId`, call `control.add_learner(id, node, true)`.
13. For each remote node in ascending `NodeId`, call `data.add_learner(id, node, true)`.
14. Wait until both effective and committed memberships show one voter plus both learners.
15. Call `data.change_membership({1, 2, 3}, false)` and wait until both data memberships are the same uniform voter set.
16. Call `control.change_membership({1, 2, 3}, false)` last and wait until both control memberships are the same uniform voter set.
17. Verify that both local state machines contain the exact bootstrap spec.
18. Persist `ActiveManifest`.
19. Send best-effort `Activate` requests to the other nodes.
20. Return `BootstrapResult`.

Every retry reads Openraft facts before taking an action:

- `is_initialized()` decides whether `initialize` is needed.
- committed state decides whether bootstrap application commands are needed.
- current and committed membership decide whether a learner or membership action is needed.
- `add_learner(..., true)` may be repeated for the same node and address.
- `change_membership` is skipped when the uniform target is already committed.
- `ChangeMembershipError::InProgress` causes a bounded wait for the existing change, then a fresh observation.

The server never treats the local manifest as proof that an Openraft action completed.

### Why data membership changes first

Both groups begin with the bootstrap node as their only voter. Learners cannot become leaders. After data membership becomes three-voter, data leadership may move. No later formation step needs a data-group write. The control group remains one-voter until its final membership change, so the formation coordinator remains its only possible leader.

If the bootstrap process fails before control promotion, formation pauses until that process restarts. This is an accepted bootstrap limitation. It does not create a runtime fixed leader because the limitation ends before `Active`.

If the process fails after control promotion, every required consensus action has completed. Any node can derive `Active` from its local committed memberships and bootstrap state.

### Follower activation

`PrepareJoin` is legal only from `Pristine`, a matching `Joining` state, or a matching `Active` state. It never overwrites a conflicting manifest.

An `Activate` request is a hint to reconcile, not authority to mark the node active. A joining node persists `ActiveManifest` only when:

- both group identities match the manifest;
- both state machines contain the exact bootstrap spec;
- both effective memberships equal their committed memberships;
- both voter sets equal the three IDs in the topology;
- `wait_for_recovery(Some(Duration::from_secs(10)))` succeeds for both groups.

Startup performs the same check. A lost `Activate` response cannot strand a fully joined node.

## Function signatures

### Manifest and topology

```rust
impl ThreeVoterTopology {
    pub(crate) fn parse(
        bootstrap_node: NodeId,
        nodes: Vec<NodeDescriptor>,
        local: &LocalNodeConfig,
        security: SecurityMode,
        allow_insecure_non_loopback: bool,
    ) -> Result<Self, DomainError>;

    pub(crate) fn node(&self, id: NodeId) -> Option<&NodeDescriptor>;
    pub(crate) fn voter_ids(&self) -> BTreeSet<u64>;
    pub(crate) fn openraft_nodes(&self) -> BTreeMap<u64, BasicNode>;
}

pub(crate) fn read_node_manifest(
    data_dir: &Path,
    local: &LocalNodeConfig,
) -> Result<Option<NodeManifest>, StartupError>;

pub(crate) fn write_node_manifest(
    data_dir: &Path,
    state: &PersistedNodeState,
) -> Result<(), StartupError>;
```

### Runtime

```rust
impl ClusterManager {
    pub(crate) async fn open(
        config: LocalNodeConfig,
        receipt_window: usize,
        peer_channels: PeerChannelPool,
    ) -> Result<Self, DomainError>;

    pub(crate) async fn bootstrap(
        &self,
        formation: FormationSpec,
        deadline: Instant,
    ) -> Result<BootstrapResult, DomainError>;

    pub(crate) async fn prepare_join(
        &self,
        offer: JoinOffer,
        deadline: Instant,
    ) -> Result<JoinAcceptance, DomainError>;

    pub(crate) async fn activate(
        &self,
        cluster: ClusterId,
        topology_digest: [u8; 32],
        deadline: Instant,
    ) -> Result<FormationState, DomainError>;

    pub(crate) async fn publish(
        &self,
        batch: PublishBatch,
        deadline: Instant,
    ) -> Result<PublishReceipt, DomainError>;

    pub(crate) async fn fetch(
        &self,
        cluster: ClusterId,
        partition: PartitionKey,
        offset: RecordOffset,
        limit: u32,
        deadline: Instant,
    ) -> Result<FetchPage, DomainError>;

    pub(crate) async fn receipt(
        &self,
        cluster: ClusterId,
        partition: PartitionKey,
        request: &ProducerRequestId,
        deadline: Instant,
    ) -> Result<PublishReceipt, DomainError>;

    pub(crate) fn diagnostics(&self) -> NodeDiagnostics;
}
```

`publish`, `fetch`, and `receipt` select only `DataGroup`. Formation code names `ControlGroup` and `DataGroup` explicitly. There is no raw group parameter on an application method.

### Formation helpers

```rust
async fn resume_three_voter_formation(
    local: &LocalNodeConfig,
    formation: &FormationSpec,
    replicas: &ReplicaHandles,
    lifecycle: &PeerLifecycleClient,
    deadline: Instant,
) -> Result<(), DomainError>;

async fn ensure_learner<C>(
    raft: &Raft<C, RocksStateMachine<C>>,
    node: &NodeDescriptor,
    deadline: Instant,
) -> Result<(), DomainError>
where
    C: PeerRaftConfig;

async fn ensure_uniform_voters<C>(
    raft: &Raft<C, RocksStateMachine<C>>,
    voters: &BTreeSet<u64>,
    deadline: Instant,
) -> Result<(), DomainError>
where
    C: PeerRaftConfig;

async fn wait_uniform_membership<C>(
    raft: &Raft<C, RocksStateMachine<C>>,
    voters: &BTreeSet<u64>,
    deadline: Instant,
) -> Result<(), DomainError>
where
    C: PeerRaftConfig;
```

The generic helpers hide repeated Openraft mechanics. They cannot choose a group or a state-machine command. The caller still holds a typed control or data handle.

### Peer routing and codec

```rust
pub(crate) trait PeerRaftConfig:
    openraft::RaftTypeConfig<
        D = GroupCommand,
        R = ApplyResult,
        NodeId = u64,
        Node = BasicNode,
    >
{
    const GROUP: ConsensusGroup;
    const GROUP_ID: u64;
    const MAX_MESSAGE_BYTES: usize;
}

fn validate_frame<C: PeerRaftConfig>(
    frame: PeerFrame,
    local: &ActiveOrJoiningNode,
) -> Result<ValidatedPeerFrame<C>, PeerRouteError>;

fn encode_frame<C: PeerRaftConfig, T: Serialize>(
    route: &PeerRoute<C>,
    value: &T,
) -> Result<PeerFrame, PeerCodecError>;

fn decode_frame<C: PeerRaftConfig, T: DeserializeOwned>(
    frame: ValidatedPeerFrame<C>,
) -> Result<T, PeerCodecError>;
```

`ControlRaftConfig` implements `PeerRaftConfig` with group ID `1` and a 1 MiB limit. `DataRaftConfig` implements it with group ID `2` and a 10 MiB limit.

## Peer routing validation

The peer boundary applies checks in this order:

1. Tonic rejects a message over the service limit.
2. The handler requires protocol version `1`.
3. The service type fixes the expected group kind.
4. `group_id` must match that kind.
5. `recipient_node_id` must equal the configured local node ID.
6. `cluster_id` and `topology_digest` must match the durable `Joining`, `Forming`, or `Active` manifest.
7. `sender_node_id` must exist in that manifest's topology.
8. `payload_length` must equal the received byte count and stay within the group limit.
9. `payload_sha256` must match.
10. Bincode decodes the exact Openraft request type for that service.
11. The request reaches only the typed handle selected by the service.

`Pristine` nodes reject Raft RPCs and accept only `Probe` and `PrepareJoin`. `Joining` nodes accept Raft RPCs for the offered topology. `Active` nodes accept only their active topology.

These checks prevent accidental cross-cluster, cross-group, wrong-recipient, stale-topology, oversized, and corrupt routing. They do not authenticate the claimed sender. LS02b reports that limit in diagnostics and evidence. LS08 replaces claimed identity with mutual TLS identity without changing the validated route type.

## Linearizable reads

`fetch` and `receipt` run:

```rust
data.raft
    .ensure_linearizable(ReadPolicy::ReadIndex)
    .await?;

data.reader.fetch(...)?;
```

The server does not serve current data after only checking `current_leader()`. Openraft documents that `current_leader()` is suitable for routing but not for guarding a read.

`LinearizableReadError::ForwardToLeader` maps to typed `not_leader` with a data-group hint. `LinearizableReadError::QuorumNotEnough` maps to `quorum_unavailable` with `RequestOutcome::NotApplicable`.

Diagnostics do not use a read barrier because their values are explicitly local observations.

## Leader hints and client retries

The server builds a public leader hint only when all of these values agree:

- Openraft returned `ForwardToLeader.leader_id`;
- the node ID exists in the durable topology;
- `ForwardToLeader.leader_node.addr` equals that descriptor's peer endpoint;
- the descriptor has a valid public endpoint.

If any check fails, the server returns `not_leader` without a hint and records the mismatch in diagnostics. It never copies the Openraft peer address into a public redirect.

The client uses one operation budget and several request attempts:

```rust
pub struct ClientOptions {
    pub request_timeout: Duration,
    pub operation_timeout: Duration,
    pub retry_policy: RetryPolicy,
}

pub enum RetryPolicy {
    NoRetry,
    LeaderHintsAndSeeds,
}

async fn execute_with_routing<T>(
    &self,
    group: ConsensusGroup,
    operation: OperationKind,
    call: impl FnMut(&mut LightStreamClient<Channel>) -> CallFuture<T>,
) -> Result<T, ClientError>;
```

Rules:

1. Use a cached leader endpoint for the group when present. Otherwise use the next seed.
2. Set a per-attempt tonic timeout to the smaller of `request_timeout` and the remaining operation budget.
3. On `not_leader` with a validated hint, cache the hint and connect directly.
4. On `not_leader` without a hint, try each unused seed once.
5. On connection failure or `UNAVAILABLE`, try another endpoint while the operation budget remains.
6. Bound attempts to `2 * endpoint_count + 2`. Reject hint loops.
7. Never change `ProducerRequestId`, payload bytes, cluster, stream, or partition between publish attempts.
8. On a publish timeout, treat the result as ambiguous. Retry the same request identity.
9. If the operation budget expires after any ambiguous publish attempt, return `ClientError::AmbiguousCommit`. The caller resolves it with `receipt`.
10. A `receipt_conflict` is final.
11. A linearizable `receipt_not_found` is final only after quorum is available.

The client does not retry validation, identity, bootstrap conflict, receipt conflict, or unsupported errors.

The server wraps `client_write` in a deadline. A timeout maps to `quorum_unavailable` with `AmbiguousCommit`, because cancellation does not prove that Openraft failed to commit. A follower's `ForwardToLeader` maps to `DefiniteNoCommit` for that attempt.

## Durable receipt semantics

LS02b keeps the LS02a state-machine transaction unchanged:

- the payload fingerprint includes cluster, partition, producer request identity, and every record byte;
- the state machine checks an existing receipt before allocating offsets;
- a matching retry returns the stored `PublishReceipt`;
- a conflicting retry returns `receipt_conflict`;
- records, next offset, receipt, session state, and applied log ID commit in one synchronous RocksDB batch;
- the apply responder receives the result only after that batch succeeds.

Raft may contain duplicate log entries for one producer identity after a lost response and a leader change. Only the first applied command changes offsets and records. Later matching commands return the original durable receipt.

## Snapshot handling choice

LS02b preserves the LS02a storage implementation and tests for complete snapshots. It changes runtime policy:

```rust
Config {
    snapshot_policy: SnapshotPolicy::Never,
    max_in_snapshot_log_to_keep: 0,
    ..ls02_timing_and_bounds()
}
```

The local `RaftStateMachine` snapshot methods remain valid. Product tests continue to prove that a snapshot contains retained state and payload bytes, installs atomically at the storage boundary, and rejects a conflicting identity.

Network snapshot transfer is not implemented. This choice keeps the three-voter work focused on elections, quorum, suffix replication, and receipts. It also prevents Openraft from purging the suffix needed by E07 while `full_snapshot` is unavailable.

Consequences:

- E07 must prove that `purged` does not advance and the returning follower catches up by log suffix.
- E08 is unsupported, not skipped or passed.
- LS02b evidence makes no bounded-disk, retention, or long-run claim.
- LS06 must add file-backed staged receive, digest validation, a dedicated snapshot transport, and purge-after-snapshot verification before it re-enables automatic snapshots.

The 64 MiB LS02a byte-bundle cap remains for local tests. LS06 is still responsible for replacing the in-memory network representation with file-backed streaming.

## Diagnostics shape and meaning

`ClusterManager::diagnostics` reads the latest watch value from each typed Raft handle. It returns:

- the durable formation state and topology digest;
- local node ID and cluster ID;
- each group's Openraft server state and term;
- each group's observed leader;
- effective and committed voter and learner sets;
- last log, cluster-committed, applied, snapshot, and purged indexes;
- leader-only per-node matched indexes;
- leader-only age of the last quorum acknowledgement;
- explicit unsupported claims.

The verifier uses diagnostics to choose fault targets and to wait for convergence. It never treats diagnostics as a data oracle. Record bytes and receipts still come through the public API and compare with external ledgers.

Diagnostics report an unhealthy invariant when effective membership differs from committed membership beyond the active operation deadline. Health does not rewrite or finish that membership.

## Module ownership

| Module | Ownership |
| --- | --- |
| `light-stream-core` | Product identities, application commands and results, `ConsensusGroup`, structured leader hints, outcomes, and public error semantics. No addresses other than checked public hint text. |
| `light-stream-proto` | Public protobuf generation and conversion. No Openraft types. |
| `light-stream-storage` | Existing RocksDB log, state machine, receipts, payload ownership, and local snapshots. Adds only the marker trait implementations needed by the peer adapter if they cannot live in server. |
| `light-stream-server/src/config.rs` | Local listen and advertised addresses, node identity, local-insecure boundary checks, and finite timeout configuration. |
| `light-stream-server/src/manifest.rs` | Topology constructors, version-1 import, version-2 manifest validation, canonical digest, and synced replacement. |
| `light-stream-server/src/runtime.rs` | `ClusterManager`, typed replica handles, startup reconciliation, application operations, and activation checks. |
| `light-stream-server/src/formation.rs` | The bounded bootstrap and learner-to-voter algorithm. It owns action ordering and idempotent observation. |
| `light-stream-server/src/peer.rs` | Peer codec, frame validation, channel pool, exact Openraft network factories, and generated peer services. |
| `light-stream-server/src/service.rs` | Public tonic boundary, method deadlines, typed error conversion, health, and diagnostics. |
| `light-stream-client` | Seed channels, leader caches, bounded retries, deadline accounting, and domain conversion. It does not expose tonic or generated responses. |
| `light-stream-cli` | Repeated endpoint parsing, topology argument parsing, `--no-retry`, stable JSON, and exit codes. |
| `light-stream-testkit` | Independent application ledger, process and proxy controls, diagnostics polling, and scenario-specific assertions. It does not import server internals. |
| `scripts/verify.py` | Release build, owned lifecycle, scenario state machine, evidence publication, cleanup, and explicit blocked claims. |

The public application interface remains bootstrap, publish, fetch, receipt, health, capabilities, and diagnostics. Formation and peer complexity stays behind those calls.

## Error mapping

### Openraft application errors

| Source | Public code | Retry | Outcome |
| --- | --- | --- | --- |
| `ClientWriteError::ForwardToLeader` | `not_leader` | another endpoint | `definite_no_commit` for this attempt |
| `LinearizableReadError::ForwardToLeader` | `not_leader` | another endpoint | `not_applicable` |
| `LinearizableReadError::QuorumNotEnough` | `quorum_unavailable` | after backoff | `not_applicable` |
| public write deadline while awaiting `client_write` | `quorum_unavailable` | same request identity | `ambiguous_commit` |
| `ChangeMembershipError::InProgress` during matching formation | internal bounded wait | same formation | `not_applicable` |
| `ChangeMembershipError::LearnerNotFound` | `storage_error` plus failed formation | no blind retry | `not_applicable` |
| `RaftError::Fatal` | `storage_error` and node not application-ready | no | `ambiguous_commit` for a write |
| state-machine `Rejected` | existing domain code | only as specified | definite |

The implementation matches variants. It does not inspect error strings as LS02a's temporary `raft_error` helper does.

### Peer transport errors

| Tonic or codec result | Openraft network error |
| --- | --- |
| deadline exceeded | `RPCError::Timeout` |
| connection refused, route closed, or peer unavailable | `RPCError::Unreachable` |
| transient HTTP/2 stream failure | `RPCError::Network` |
| decoded remote `RaftError` | `RPCError::RemoteError` |
| wrong cluster, group, recipient, topology, or sender | `RPCError::Unreachable` with permanent-route detail |
| corrupt or oversized payload | `RPCError::Unreachable`; receiver also records a protocol diagnostic |

### CLI exit codes

Existing codes remain. Code `5` covers `not_leader`, `quorum_unavailable`, `cluster_forming`, ambiguous commit, and storage failure. JSON contains the structured outcome and leader hint, so automation does not parse text.

## Verifier state machine

The verifier uses a run-state enum instead of a long sequence whose partial completion is implicit.

```rust
enum SuiteState {
    Prepared,
    BinariesBuilt,
    ProxiesReady,
    NodesReady,
    ClusterActive,
    ScenarioRunning { id: ScenarioId, step: ScenarioStep },
    OracleChecking,
    CleaningUp,
    Passed,
    Failed,
}

enum ScenarioId {
    E02,
    E03,
    E04,
    E05,
    E06,
    E07,
    E08,
    E09,
}

enum ScenarioStep {
    Arrange,
    ConfirmFault,
    DriveTraffic,
    Heal,
    AwaitConvergence,
    CompareLedger,
    RecordVerdict,
}
```

Each transition appends one state record before starting the next action. A fault lease owns every proxy rule and process stop. Cleanup removes the rule or restarts the process before the scenario can record `PASS`.

The testkit adds a group-aware peer proxy. It forwards real HTTP/2 bytes and can delay or drop traffic by upstream, downstream, and gRPC method path. It never decodes an Openraft response, returns a synthetic response, changes a payload, forwards a public application call, or writes broker state.

### E02: three-node application journey

1. Start three peer proxies and three release servers.
2. Bootstrap through node `1`.
3. Wait until all diagnostics report `Active` and both committed voter sets are `{1, 2, 3}`.
4. If both groups have the same leader, isolate that node only for `DataRaftPeer` traffic.
5. Wait for the other two voters to elect a data leader while control leadership stays unchanged.
6. Heal the data links and wait for one stable data leader.
7. Assert that control and data leaders differ.
8. Publish through one follower endpoint with ordinary client retries.
9. Fetch through a different follower endpoint.
10. Compare every byte, offset, request identity, and receipt with the independent ledger.
11. Save `e02.json`, diagnostics snapshots, proxy rules, commands, and ledgers.

The production client follows a leader hint to the real leader. The proxy only creates the election needed by the scenario.

### E03: leader crash during traffic

1. Record and sync an acknowledged prefix.
2. Identify the data leader from diagnostics.
3. Send SIGTERM to that owned process and record its exit.
4. Continue bounded publish attempts through surviving endpoints.
5. Wait at most 10 seconds for a new data leader and successful writes.
6. Restart the failed node from the same data directory.
7. Read every acknowledged byte and receipt.
8. Save election duration and the old and new leader IDs.

Local functional evidence can pass. Independent-host HA remains blocked.

### E04: live old leader in a minority

1. Identify the current data leader.
2. Isolate only that process's data-group links from the other two voters.
3. Keep its public listener live.
4. Use `RetryPolicy::NoRetry` against the old leader.
5. Require publish to return `not_leader`, `quorum_unavailable`, or an ambiguous deadline. It must not return a receipt.
6. Require fetch and receipt not to return fresh success.
7. Publish and read through the healthy majority.
8. Heal the links.
9. Wait until the old leader reports follower state and an applied index equal to the leader.
10. Compare the complete ledger.

The scenario does not accept an automatically rerouted client success as proof that the old endpoint was safe.

### E05: majority unavailable

1. Isolate one data leader from both other voters, or stop two voters.
2. Send new request identities with `NoRetry`.
3. Require zero successful acknowledgements.
4. Before healing, require every current read or receipt lookup to fail with `not_leader`, `quorum_unavailable`, or a deadline. No node may expose a local-only result.
5. Heal the quorum.
6. Resolve every ambiguous request with a linearizable receipt lookup.
7. Accept either `receipt_not_found` or one later quorum-committed receipt. A later commit remains unacknowledged during the outage and enters the oracle as an ambiguous result resolved after recovery.
8. Fail on a duplicate effect, an acknowledgement during the outage, or a record that became visible without a restored quorum.

### E06: slow minority

1. Choose one non-leader follower.
2. Delay only data-group traffic on links to that follower.
3. Keep the leader and the other follower healthy.
4. Publish the fixed bounded workload and require majority progress within B1.
5. Record the slow follower's matched and applied indexes.
6. Remove the delay.
7. Wait until matched and applied indexes reach the leader's committed index.
8. Fetch the complete acknowledged range through normal routing.

The proxy rule applies to one follower, not all links.

### E07: log-suffix catch-up

1. Stop one follower.
2. Record `snapshot` and `purged` diagnostics on the leader.
3. Publish a bounded suffix.
4. Restart the follower from its durable store.
5. Wait until its applied index reaches the leader's committed index.
6. Require the leader's `purged` index not to advance.
7. Compare all bytes and receipts.

This proves suffix catch-up only.

### E08: snapshot catch-up

Write:

```json
{
  "scenario": "E08",
  "verdict": "UNSUPPORTED",
  "reason": "LS02b disables automatic purge and has no network snapshot transport",
  "available_phase": "LS06",
  "independent_host": "BLOCKED"
}
```

The suite does not count this record as a pass.

### E09: lost acknowledgement and retry

1. Put a response-dropping public proxy in front of one endpoint.
2. Forward one publish request and allow the upstream server to finish.
3. Drop the response bytes and record confirmation from the proxy.
4. Retry the same producer request through another endpoint.
5. Require the original offset and count.
6. Retry with a different payload and require `receipt_conflict`.
7. Fetch and prove one committed effect.
8. Restart all nodes and repeat the receipt lookup.

This replaces LS02a's discarded stdout with an actual response-path fault.

### Evidence and cleanup

Each scenario writes:

- `attempts.jsonl`;
- `acks.jsonl`;
- `errors.jsonl`;
- `reads.jsonl`;
- `faults.jsonl`;
- `diagnostics.jsonl`;
- `topology.json`;
- `commands.jsonl`;
- one `e0N.json` verdict;
- node and proxy logs.

The external oracle rejects duplicate acknowledgements, digest mismatch, missing acknowledged outcomes, offset disagreement, or a receipt range that differs across retries.

The final suite records:

- E02, E03, E04, E05, E06, E07, and E09 as `PASS` only after their local predicates pass;
- E08 as `UNSUPPORTED_LS06`;
- independent-host claims as `BLOCKED_REFERENCE_HOSTS`;
- secured mode as `UNSUPPORTED_LS08`.

## Illegal states prevented by types and boundaries

| Illegal state | Prevention |
| --- | --- |
| Two or four LS02b voters | `ThreeVoterTopology` owns `[NodeDescriptor; 3]`; the only other topology is standalone. |
| Node ID `0` | `NodeId` checked constructor. |
| Duplicate node or endpoint | `ThreeVoterTopology::parse`. |
| Bootstrap sent to an undeclared node | `bootstrap_node_id` and local descriptor validation. |
| Follower accepts Raft before join preparation | no manifest means `Pristine`, which rejects Raft services. |
| Control RPC reaches data Raft | separate generated service, fixed group ID, and typed handle. |
| Wrong cluster or stale topology reaches Openraft | manifest and digest checks precede decode. |
| Remote request targets another node | `recipient_node_id` check. |
| Membership promotes an unknown node | formation always calls `add_learner(..., true)` before `change_membership`; Openraft also returns `LearnerNotFound`. |
| Local manifest claims completed membership | activation derives completion from current and committed Openraft memberships. |
| Joining node serves application data | `application_ready` requires `Active` and recovery. |
| Follower serves a current read | `ensure_linearizable(ReadIndex)` precedes state access. |
| Retry allocates a second offset | durable receipt check precedes offset allocation in state-machine apply. |
| Client silently proxies through a follower | server returns a hint; client opens a direct channel. |
| E07 accidentally uses a snapshot | automatic snapshots are disabled and the verifier asserts no purge. |
| E08 is reported as passed | scenario state is explicit `UNSUPPORTED`. |

## Synthesis decision

Candidate C is the base.

Candidate A initialized all three nodes directly as voters. It was smaller on paper, but it violated the required learner-first flow and made conflicting multi-node initialization a split-brain risk.

Candidate B required operators to install an identical static cluster file on every node before startup. It prevented early misrouting, but it exposed storage format and formation ordering to callers. It also made LS02a standalone compatibility harder.

Candidate C keeps caller work to one bootstrap request. `PrepareJoin` makes follower state durable before Openraft traffic, then the bootstrap node uses the exact learner and membership APIs. Data-first and control-last promotion avoids a bootstrap-only forwarding service and avoids a runtime fixed leader.

The design also takes one useful part from candidate B: every node persists the canonical topology digest and validates peer routes against it.

## Tradeoffs accepted

- We accept that formation pauses if the bootstrap node fails before control promotion. In exchange, LS02b avoids replicated lifecycle machinery that belongs in LS03.
- We accept retained Raft logs for LS02b functional runs. In exchange, E07 has a real suffix-only proof and the product cannot accidentally depend on an unimplemented snapshot transport.
- We accept bincode as one small peer-only dependency. In exchange, maximum public publish batches fit inside a bounded peer message without JSON expansion.
- We accept one extra read-only public RPC. In exchange, the verifier chooses real leaders and proves catch-up without reading storage or adding mutating test controls.
- We accept unauthenticated claimed peer IDs in `local-insecure`. In exchange, LS02b does not pre-implement LS08. Evidence states this limit.

## Alternatives considered and rejected

### Initialize all three voters at once

Openraft permits `initialize` with a three-node map, but this path does not prove learner catch-up before voting. Calling it on several nodes with differing maps can split the cluster. It lost despite its short implementation.

### Pre-provision a static cluster manifest on every node

This shape makes startup deterministic, but the caller must understand internal group IDs, manifest versions, and storage ownership. It is a shallow interface. The chosen bootstrap call hides those rules.

### Let pristine nodes create groups on the first AppendEntries RPC

This removes `PrepareJoin`, but an unauthenticated peer frame could choose cluster identity and create durable storage. It also makes cross-cluster mistakes destructive. The explicit preparation boundary is worth one lifecycle RPC.

### Proxy application requests through followers

Server proxying gives a one-endpoint demo, but it hides actual routing, doubles request paths, complicates ambiguous outcomes, and can make E04 pass through the healthy majority while the old endpoint is unsafe. Typed hints and direct client retry are clearer.

### Implement snapshot transfer now

A correct path needs staged file receive, cancellation, digest checks, crash recovery, and purge coordination. A unary 64 MiB byte bundle would only imitate that work. LS06 owns the complete path.

### Add a mutating diagnostics or force-leader RPC

That would make E02 deterministic by bypassing normal elections. The group-aware proxy instead creates a real minority and observes the resulting election.

## Open questions and risks

- Is a 30-second bootstrap attempt deadline enough for three local RocksDB stores under sanitizer or debug builds? Verification should record actual learner catch-up time before locking the value.
- Is 10 MiB enough for the worst bincode encoding of one maximum `PublishBatch` plus Openraft metadata? A compile-time fixture must encode the maximum legal batch and assert the bound before implementation accepts the constant.
- Does tonic create physically separate HTTP/2 connections for the three traffic-class channels under the selected connector? A transport test must prove that a saturated replication channel does not delay heartbeat RPCs.
- Does `add_learner(..., true)` return after the learner has the membership entry needed by the following `change_membership` in this exact prerelease? The local Openraft source says it waits until logs are up to date. An integration test must lock that behavior.
- Can a node fail after data promotion but before control promotion with data leadership elsewhere? Recovery must prove that no further data-group mutation is required before continuing control promotion.

## Self-grade

| Criterion | Grade | Reason |
| --- | ---: | --- |
| Exact Openraft `0.10.0-alpha.34` compatibility | 6/6 | The design uses the exact local signatures, 0.10 `ReadLogId`, `RaftNetworkV2`, separate factory clients, `add_learner`, joint membership, structured errors, and metrics fields. |
| Illegal routing and bootstrap states prevented by types and boundaries | 6/6 | Fixed topology types, durable state variants, service-level group typing, manifest digests, and activation from Openraft facts block the listed illegal states. |
| Minimal maintainable implementation | 5/6 | The design adds lifecycle, diagnostics, and peer codec modules, but each hides required policy. Snapshot transport, dynamic placement, and generalized lifecycle intents remain out of scope. |
| Complete E02 through E09 local verification | 6/6 | E02 through E07 and E09 have executable local state machines. E08 and independent-host claims are explicit blocked results, as requested. |
| No hidden fixed leader or proxy shortcut | 6/6 | Only pre-activation control formation depends on the bootstrap voter. Active leadership is elected. Public requests reconnect directly, and fault proxies only alter transport. |
| Total | 29/30 | The remaining point reflects bootstrap coordination's deliberate dependency on the bootstrap node before activation. |

## First implementation step

Implement the versioned topology and node-manifest types with boundary tests, then replace `NoRemoteNetworkFactory` with compile-only control and data tonic adapters before changing runtime formation.
