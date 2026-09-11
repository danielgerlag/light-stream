# LS02b candidate B: typed dual-group Raft with client-side leader routing

Status: design only. This document does not authorize production code changes.

## Caller usage comes first

LS02b keeps the LS02a concepts. Callers still publish to a stream partition, fetch committed records, and resolve a producer receipt. They do not choose a Raft group, inspect a term, or send a peer RPC.

### Rust client

The existing one-endpoint call remains valid for standalone nodes:

```rust
let client = Client::connect("http://127.0.0.1:7101").await?;

let receipt = client.publish(batch.clone()).await?;
let page = client
    .fetch(
        batch.cluster(),
        batch.partition(),
        receipt.range().first(),
        128,
    )
    .await?;
```

A three-voter caller supplies seeds, not a leader:

```rust
let client = Client::connect_cluster(ClientConfig {
    seeds: vec![
        "http://127.0.0.1:7101".parse()?,
        "http://127.0.0.1:7102".parse()?,
        "http://127.0.0.1:7103".parse()?,
    ],
    attempt_timeout: Duration::from_secs(2),
    operation_timeout: Duration::from_secs(10),
    max_leader_redirects: 4,
})
.await?;

let receipt = client.publish(batch.clone()).await?;
```

`Client::publish` preserves `ProducerRequestId` across every attempt. A follower returns a typed leader hint. The client validates the hint against the cluster directory, connects to that public endpoint, and retries directly. A server never proxies the publish.

If an attempt times out after submission, the client treats the result as unknown and retries the same identity while the operation budget remains. If the budget expires, the error contains the original request identity so the caller can run `receipt`.

```rust
match client.publish(batch.clone()).await {
    Ok(receipt) => persist_ack(receipt),
    Err(ClientError::OutcomeUnknown { request_id, .. }) => {
        let receipt = client
            .receipt(batch.cluster(), batch.partition(), request_id)
            .await?;
        persist_ack(receipt);
    }
    Err(error) => return Err(error),
}
```

Fetch and receipt reads are linearizable. The client may enter through any node, but the read runs only after the data leader completes `ensure_linearizable(ReadPolicy::ReadIndex)`.

### CLI

Standalone bootstrap keeps the LS02a grammar:

```sh
light-streamctl \
  --endpoint http://127.0.0.1:7101 \
  cluster bootstrap \
  --cluster-id "$CLUSTER_ID" \
  --stream-id "$STREAM_ID" \
  --stream-name bootstrap
```

Three-voter bootstrap adds exactly three voter descriptors:

```sh
light-streamctl \
  --endpoint http://127.0.0.1:7101 \
  cluster bootstrap \
  --cluster-id "$CLUSTER_ID" \
  --stream-id "$STREAM_ID" \
  --stream-name bootstrap \
  --voter 1,127.0.0.1:7101,127.0.0.1:7201 \
  --voter 2,127.0.0.1:7102,127.0.0.1:7202 \
  --voter 3,127.0.0.1:7103,127.0.0.1:7203
```

The endpoint receiving this request is the bootstrap coordinator only while the cluster forms. It initializes each Raft group with itself as the sole voter, prepares the other nodes, adds both as blocking learners, and then calls `change_membership` with all three voter IDs. No node ID is special after activation.

The same bootstrap request is idempotent after any process restart. A request with a different cluster, stream, node set, or endpoint fails with `bootstrap_conflict`.

Application commands keep their LS02a grammar:

```sh
light-streamctl --endpoint http://127.0.0.1:7102 publish \
  --cluster-id "$CLUSTER_ID" \
  --stream-id "$STREAM_ID" \
  --principal verify \
  --session "$SESSION_ID" \
  --sequence 7 \
  --file record.bin

light-streamctl --endpoint http://127.0.0.1:7103 fetch \
  --cluster-id "$CLUSTER_ID" \
  --stream-id "$STREAM_ID" \
  --offset 0 \
  --limit 128
```

The CLI uses the Rust client's retry policy. JSON output identifies the endpoint that accepted the request and the number of direct attempts. It does not expose Raft terms or log IDs.

### Operator diagnostics

```sh
light-streamctl \
  --endpoint http://127.0.0.1:7102 \
  cluster status
```

The response reports the local node state, the current leader for each group, voters, learners, write readiness, and per-peer lag in entries. Diagnostics are read-only. They cannot trigger an election, alter membership, repair storage, or advance a commit index.

## Design decision

Candidate B uses separate control and data types from the public API through the peer service:

- `ControlGroupHandle` accepts only `ControlCommand`.
- `DataGroupHandle` accepts only `DataCommand`.
- `ControlAppendEntries` and `ControlVote` decode only `ControlRaftConfig`.
- `DataAppendEntries` and `DataVote` decode only `DataRaftConfig`.
- the bootstrap boundary is the only code that can create a provisioning manifest, and `FormationReconciler` is the only code that can run membership changes.

This is slightly more code than one generic `GroupHandle`, but it removes the easiest illegal states. A publish cannot reach the control Raft instance through a generic enum and a mismatched peer request cannot be decoded before the RPC method chooses its group kind.

The public surface remains small. `Client` hides endpoint selection, leader hints, deadlines, and safe retries. `ClusterRuntime` hides startup, two Raft instances, manifest recovery, learner admission, and membership promotion.

## Concrete Rust data types

### Node and topology types

These types belong in `light-stream-core`. Fields stay private. Constructors perform all boundary validation.

```rust
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NodeDescriptor {
    node_id: NodeId,
    public_address: SocketAddr,
    peer_address: SocketAddr,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum BootstrapTopology {
    Standalone {
        voter: NodeId,
    },
    ThreeVoter(ThreeVoterTopology),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ThreeVoterTopology {
    coordinator: NodeId,
    voters: [NodeDescriptor; 3],
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BootstrapPlan {
    bootstrap: BootstrapSpec,
    topology: BootstrapTopology,
}
```

`ThreeVoterTopology::try_new` enforces:

- exactly three distinct, nonzero node IDs;
- three distinct public addresses and three distinct peer addresses;
- no public address equals any peer address;
- the coordinator appears exactly once;
- the local node descriptor matches the configured node ID and advertised addresses;
- every address is loopback unless the existing `--allow-insecure-non-loopback` boundary permits it.

There is no constructor that accepts an arbitrary `Vec<NodeDescriptor>` and stores it unchecked.

Openraft node metadata needs the public address for `ForwardToLeader` and the peer address for `RaftNetworkFactory`:

```rust
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RaftNode {
    // Keep `addr` for JSON compatibility with Openraft BasicNode in LS02a data.
    addr: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    public_addr: Option<String>,
}

impl RaftNode {
    pub fn from_descriptor(node: &NodeDescriptor) -> Self;
    pub fn peer_address(&self) -> Result<SocketAddr, PeerAddressError>;
    pub fn public_address(&self) -> Option<Result<SocketAddr, PeerAddressError>>;
}
```

An LS02a membership encoded as `{"addr":"127.0.0.1:7201"}` decodes as `RaftNode { addr, public_addr: None }`. A new three-voter membership always has `public_addr: Some`. This keeps the RocksDB and snapshot format at version `1`.

### Separate control and data commands

The current `GroupCommand` permits a control command to be constructed for a data Raft type and rejects it only during apply. LS02b replaces that in-memory shape:

```rust
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ControlCommand {
    Bootstrap { spec: BootstrapSpec },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ControlApplyResult {
    Bootstrapped(BootstrapResult),
    Rejected(DomainError),
    Noop,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum DataCommand {
    Bootstrap { spec: BootstrapSpec },
    Publish { batch: PublishBatch },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum DataApplyResult {
    Bootstrapped(BootstrapResult),
    Published(PublishReceipt),
    Rejected(DomainError),
    Noop,
}
```

The exact Openraft declarations are:

```rust
openraft::declare_raft_types!(
    pub ControlRaftConfig:
        D = ControlCommand,
        R = ControlApplyResult,
        NodeId = u64,
        Node = RaftNode,
        LeaderId = openraft::impls::leader_id_adv::LeaderId<u64, u64>,
        Entry = openraft::Entry<
            <Self::LeaderId as openraft::vote::RaftLeaderId>::Committed,
            Self::D,
            Self::NodeId,
            Self::Node,
        >,
);

openraft::declare_raft_types!(
    pub DataRaftConfig:
        D = DataCommand,
        R = DataApplyResult,
        NodeId = u64,
        Node = RaftNode,
        LeaderId = openraft::impls::leader_id_adv::LeaderId<u64, u64>,
        Entry = openraft::Entry<
            <Self::LeaderId as openraft::vote::RaftLeaderId>::Committed,
            Self::D,
            Self::NodeId,
            Self::Node,
        >,
);
```

The explicit advanced `LeaderId` preserves LS02a's `(term, node_id, index)` log identity. The persisted thin entry remains format `1`. The decoder maps only these old variants:

```rust
enum PersistedThinCommandV1 {
    BootstrapControl { spec: BootstrapSpec },
    BootstrapData { spec: BootstrapSpec },
    Publish {
        cluster: ClusterId,
        partition: PartitionKey,
        request: ProducerRequestId,
        fingerprint: String,
        payload_keys: Vec<Vec<u8>>,
    },
}
```

The control reader rejects `BootstrapData` and `Publish` as corruption. The data reader rejects `BootstrapControl`. Existing valid LS02a entries decode without rewriting payload objects, offsets, receipts, or snapshots.

### Request and routing errors

`DomainError` remains for deterministic domain decisions. Leadership and transport do not become domain errors.

```rust
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LeaderHint {
    pub group: GroupId,
    pub node: NodeDescriptor,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutcomeCertainty {
    DefiniteNoEffect,
    Unknown,
}

#[derive(Debug, Error)]
pub enum GroupRequestError {
    #[error(transparent)]
    Domain(#[from] DomainError),

    #[error("request must run on the group leader")]
    NotLeader {
        group: GroupId,
        hint: Option<LeaderHint>,
    },

    #[error("a quorum did not confirm the operation")]
    QuorumUnavailable {
        group: GroupId,
        certainty: OutcomeCertainty,
    },

    #[error("operation exceeded its server deadline")]
    Deadline {
        operation: &'static str,
        certainty: OutcomeCertainty,
    },

    #[error("the local Raft task stopped: {reason}")]
    RaftFatal { group: GroupId, reason: String },
}
```

`NotLeader` means Openraft rejected the request before accepting it as a leader operation. Its certainty is `DefiniteNoEffect`. A write deadline is `Unknown` because the command may commit after the public future is dropped.

### Durable node manifest

The root manifest moves from the untagged LS02a struct to a versioned state enum:

```rust
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum NodeManifestV2 {
    Provisioning(ProvisioningManifest),
    Active(ActiveManifest),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProvisioningManifest {
    format_version: u32,              // 2
    storage_format_version: u32,      // 1
    local_node_id: NodeId,
    plan: BootstrapPlan,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ActiveManifest {
    format_version: u32,              // 2
    storage_format_version: u32,      // 1
    local_node_id: NodeId,
    plan: BootstrapPlan,
    activation: ActivationProof,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ActivationProof {
    Standalone {
        control_voter: NodeId,
        data_voter: NodeId,
    },
    ThreeVoter {
        control_voters: [NodeId; 3],
        data_voters: [NodeId; 3],
    },
}
```

`ActivationProof` has no public constructor. `ClusterRuntime::prove_activation` creates it only after:

- both local stores contain the requested cluster and bootstrap catalog;
- both effective memberships are uniform, not joint;
- both committed memberships equal their effective memberships;
- both voter sets equal the requested topology;
- every stored `RaftNode` matches the durable topology;
- neither group contains an extra learner or voter.

The proof variant must match `BootstrapTopology`. An active state therefore cannot represent three voters in one group and one voter in the other.

Manifest writes keep the LS02a durability sequence: write a sibling file, `sync_all`, rename, then sync the parent directory. Startup refuses group directories without a manifest. It never treats them as pristine.

The loader accepts the LS02a `cluster.json` shape as `NodeManifestV1`. It verifies both stores and rewrites it atomically as an active standalone V2 manifest. It does not change storage format `1`.

## Protobuf RPCs

### Public API additions

Existing field numbers and methods remain unchanged. An empty `voters` field preserves LS02a standalone bootstrap.

```proto
message NodeDescriptor {
  uint64 node_id = 1;
  string public_address = 2;
  string peer_address = 3;
}

message BootstrapRequest {
  string cluster_id = 1;
  string stream_id = 2;
  string stream_name = 3;
  repeated NodeDescriptor voters = 4; // empty or exactly three
}

message BootstrapSuccess {
  string cluster_id = 1;
  string stream_id = 2;
  string stream_name = 3;
  uint64 control_group_id = 4;
  uint64 data_group_id = 5;
  repeated NodeDescriptor voters = 6;
}

enum OutcomeCertainty {
  OUTCOME_CERTAINTY_UNSPECIFIED = 0;
  OUTCOME_CERTAINTY_DEFINITE_NO_EFFECT = 1;
  OUTCOME_CERTAINTY_UNKNOWN = 2;
}

message LeaderHint {
  uint64 group_id = 1;
  NodeDescriptor leader = 2;
}

message ErrorResult {
  string code = 1;
  string message = 2;
  bool retryable = 3;
  OutcomeCertainty certainty = 4;
  LeaderHint leader_hint = 5;
}

message ClusterStatusRequest {}

message PeerProgress {
  uint64 node_id = 1;
  bool recently_reachable = 2;
  optional uint64 lag_entries = 3;
}

enum GroupRole {
  GROUP_ROLE_UNSPECIFIED = 0;
  GROUP_ROLE_LEARNER = 1;
  GROUP_ROLE_FOLLOWER = 2;
  GROUP_ROLE_CANDIDATE = 3;
  GROUP_ROLE_LEADER = 4;
  GROUP_ROLE_SHUTDOWN = 5;
}

message GroupStatus {
  uint64 group_id = 1;
  string kind = 2; // "control" or "data"
  GroupRole local_role = 3;
  optional uint64 current_leader_id = 4;
  repeated uint64 voter_ids = 5;
  repeated uint64 learner_ids = 6;
  bool membership_committed = 7;
  bool local_state_applied = 8;
  bool write_ready = 9;
  repeated PeerProgress peers = 10;
  string snapshot_mode = 11; // "standalone_ls02a" or "disabled_ls02b_no_purge"
}

message ClusterStatusResponse {
  string revision = 1;
  uint64 local_node_id = 2;
  string lifecycle = 3; // pristine, provisioning, active, or failed
  repeated NodeDescriptor nodes = 4;
  repeated GroupStatus groups = 5;
  repeated string unsupported_claims = 6;
}

service LightStream {
  // Existing methods stay in place.
  rpc ClusterStatus(ClusterStatusRequest) returns (ClusterStatusResponse);
}
```

`ErrorResult` keeps fields `1` and `2`, so LS02a clients can still read the code and message. New clients use the remaining fields.

### Peer service

The RPC method fixes the group kind before decoding the Openraft request.

```proto
message RaftRequestHeader {
  uint32 protocol_version = 1;
  uint32 encoding_version = 2;
  string cluster_id = 3;
  uint64 group_id = 4;
  uint64 sender_node_id = 5;
  uint64 target_node_id = 6;
}

message RaftJsonRequest {
  RaftRequestHeader header = 1;
  bytes body_json = 2;
}

message RaftJsonResponse {
  RaftRequestHeader header = 1;
  bytes body_json = 2;
}

message PrepareJoinRequest {
  uint32 protocol_version = 1;
  uint64 target_node_id = 2;
  uint64 coordinator_node_id = 3;
  string cluster_id = 4;
  string stream_id = 5;
  string stream_name = 6;
  repeated NodeDescriptor voters = 7;
}

message PrepareJoinResponse {
  uint64 node_id = 1;
  string state = 2; // prepared or already_prepared
}

message FinalizeJoinRequest {
  uint32 protocol_version = 1;
  uint64 target_node_id = 2;
  string cluster_id = 3;
  repeated uint64 expected_voter_ids = 4;
}

message FinalizeJoinResponse {
  uint64 node_id = 1;
  string state = 2; // active or already_active
}

service PeerService {
  rpc Probe(PeerProbeRequest) returns (PeerProbeResponse);
  rpc PrepareJoin(PrepareJoinRequest) returns (PrepareJoinResponse);
  rpc FinalizeJoin(FinalizeJoinRequest) returns (FinalizeJoinResponse);

  rpc ControlAppendEntries(RaftJsonRequest) returns (RaftJsonResponse);
  rpc ControlVote(RaftJsonRequest) returns (RaftJsonResponse);
  rpc DataAppendEntries(RaftJsonRequest) returns (RaftJsonResponse);
  rpc DataVote(RaftJsonRequest) returns (RaftJsonResponse);
}
```

`body_json` uses `serde_json` only inside `light-stream-server::peer::codec`. The peer protocol version owns this Openraft prerelease coupling. No generated peer type enters `light-stream-core`, `light-stream-client`, or storage.

The peer service caps encoded and decoded messages at 40 MiB. The Openraft configuration sets `max_payload_entries = 1`. The existing public command cap is 8 MiB, so one worst-case JSON-encoded byte array remains below the peer limit. The peer codec rejects any encoded request over the limit before tonic allocates a response body.

Snapshots are not sent through `body_json`.

## Function signatures

### Public runtime

```rust
pub struct ClusterRuntime {
    state: RwLock<RuntimeState>,
    bootstrap_lock: Mutex<()>,
}

enum RuntimeState {
    Pristine(PristineRuntime),
    Provisioning(Arc<ProvisioningRuntime>),
    Active(Arc<ActiveRuntime>),
    Failed(StartupFailure),
}

pub struct ActiveRuntime {
    manifest: ActiveManifest,
    control: ControlGroupHandle,
    data: DataGroupHandle,
}

impl ClusterRuntime {
    pub async fn open(config: LocalNodeConfig) -> Result<Self, StartupError>;

    pub async fn bootstrap(
        &self,
        plan: BootstrapPlan,
        deadline: Instant,
    ) -> Result<BootstrapResult, GroupRequestError>;

    pub async fn publish(
        &self,
        batch: PublishBatch,
        deadline: Instant,
    ) -> Result<PublishReceipt, GroupRequestError>;

    pub async fn fetch(
        &self,
        cluster: ClusterId,
        partition: PartitionKey,
        offset: RecordOffset,
        limit: u32,
        deadline: Instant,
    ) -> Result<FetchPage, GroupRequestError>;

    pub async fn receipt(
        &self,
        cluster: ClusterId,
        partition: PartitionKey,
        request: ProducerRequestId,
        deadline: Instant,
    ) -> Result<PublishReceipt, GroupRequestError>;

    pub async fn status(&self) -> ClusterStatus;
    pub async fn shutdown(&self) -> Result<(), ShutdownError>;
}
```

`ClusterRuntime` completes each application operation. `PublicApi` only parses protobuf, attaches a deadline, calls one runtime method, and converts the typed result.

### Separate group handles

```rust
pub(crate) struct ControlGroupHandle {
    raft: Raft<ControlRaftConfig, RocksStateMachine<ControlRaftConfig>>,
    reader: ControlStateReader,
}

pub(crate) struct DataGroupHandle {
    raft: Raft<DataRaftConfig, RocksStateMachine<DataRaftConfig>>,
    reader: CommittedStateReader,
}

impl ControlGroupHandle {
    pub(crate) async fn bootstrap(
        &self,
        spec: BootstrapSpec,
        deadline: Instant,
    ) -> Result<BootstrapResult, GroupRequestError>;

    pub(crate) async fn add_learner(
        &self,
        node: NodeDescriptor,
        deadline: Instant,
    ) -> Result<(), BootstrapError>;

    pub(crate) async fn promote_voters(
        &self,
        voters: [NodeId; 3],
        deadline: Instant,
    ) -> Result<(), BootstrapError>;

    pub(crate) fn status(&self) -> GroupStatus;
}

impl DataGroupHandle {
    pub(crate) async fn bootstrap(
        &self,
        spec: BootstrapSpec,
        deadline: Instant,
    ) -> Result<BootstrapResult, GroupRequestError>;

    pub(crate) async fn publish(
        &self,
        batch: PublishBatch,
        deadline: Instant,
    ) -> Result<PublishReceipt, GroupRequestError>;

    pub(crate) async fn fetch_linearizable(
        &self,
        query: FetchQuery,
        deadline: Instant,
    ) -> Result<FetchPage, GroupRequestError>;

    pub(crate) async fn receipt_linearizable(
        &self,
        query: ReceiptQuery,
        deadline: Instant,
    ) -> Result<PublishReceipt, GroupRequestError>;

    pub(crate) async fn add_learner(
        &self,
        node: NodeDescriptor,
        deadline: Instant,
    ) -> Result<(), BootstrapError>;

    pub(crate) async fn promote_voters(
        &self,
        voters: [NodeId; 3],
        deadline: Instant,
    ) -> Result<(), BootstrapError>;

    pub(crate) fn status(&self) -> GroupStatus;
}
```

The two implementations may share private generic helpers. No public `GroupHandle<C>` lets callers choose a type parameter or group kind.

`fetch_linearizable` and `receipt_linearizable` run:

```rust
raft.ensure_linearizable(ReadPolicy::ReadIndex).await?;
reader.fetch(...);
```

The call is wrapped in the remaining server deadline. The code matches `LinearizableReadError` variants. It does not search error strings.

### Bootstrap reconciliation

```rust
impl ProvisioningRuntime {
    async fn reconcile(
        self: Arc<Self>,
        deadline: Instant,
    ) -> Result<Arc<ActiveRuntime>, BootstrapError>;

    async fn prepare_joiners(
        &self,
        topology: &ThreeVoterTopology,
        deadline: Instant,
    ) -> Result<(), BootstrapError>;

    async fn reconcile_control_group(
        &self,
        deadline: Instant,
    ) -> Result<(), BootstrapError>;

    async fn reconcile_data_group(
        &self,
        deadline: Instant,
    ) -> Result<(), BootstrapError>;

    async fn finalize_joiners(
        &self,
        deadline: Instant,
    ) -> Result<(), BootstrapError>;

    fn prove_activation(&self) -> Result<ActivationProof, BootstrapError>;
}
```

Every provisioning node runs this reconciler. A node mutates a group only when its local handle is that group's leader. Every method inspects durable Openraft state before acting. A saved boolean never claims that `add_learner` or `change_membership` committed.

### Exact Openraft calls

For each group, the coordinator uses this sequence:

```rust
if !raft.is_initialized().await? {
    raft.initialize(BTreeMap::from([(
        local_id,
        RaftNode::from_descriptor(local_node),
    )]))
    .await?;
}

raft.client_write(group_bootstrap_command).await?;

raft.add_learner(
    node_2.id(),
    RaftNode::from_descriptor(&node_2),
    true,
)
.await?;

raft.add_learner(
    node_3.id(),
    RaftNode::from_descriptor(&node_3),
    true,
)
.await?;

raft.change_membership(
    BTreeSet::from([local_id, node_2.id(), node_3.id()]),
    false,
)
.await?;
```

`blocking = true` means Openraft waits until the learner is caught up before returning. `change_membership` performs Openraft's joint configuration followed by its uniform configuration. The reconciler then waits until:

```rust
metrics.membership_config == metrics.committed_membership_config
```

and the uniform voter set is exactly the requested three nodes.

The design never calls `initialize` with all three nodes. Doing so would skip the required learner path.

### Peer network adapters

```rust
pub struct ControlNetworkFactory {
    transport: PeerTransport,
}

pub struct DataNetworkFactory {
    transport: PeerTransport,
}

pub struct ControlNetwork {
    target: u64,
    node: RaftNode,
    channel: Channel,
}

pub struct DataNetwork {
    target: u64,
    node: RaftNode,
    channel: Channel,
}
```

Both implement the exact Openraft 0.10 traits:

```rust
impl RaftNetworkFactory<ControlRaftConfig> for ControlNetworkFactory {
    type Network = ControlNetwork;

    async fn new_client(&mut self, target: u64, node: &RaftNode) -> Self::Network;
    async fn new_heartbeat_client(
        &mut self,
        target: u64,
        node: &RaftNode,
    ) -> Self::Network;
    async fn new_snapshot_client(
        &mut self,
        target: u64,
        node: &RaftNode,
    ) -> Self::Network;
}

impl RaftNetworkV2<ControlRaftConfig> for ControlNetwork {
    type SnapshotData = Vec<u8>;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<ControlRaftConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<ControlRaftConfig>, RPCError<ControlRaftConfig>>;

    async fn vote(
        &mut self,
        rpc: VoteRequest<ControlRaftConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<ControlRaftConfig>, RPCError<ControlRaftConfig>>;

    async fn pre_vote(
        &mut self,
        rpc: VoteRequest<ControlRaftConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<ControlRaftConfig>, RPCError<ControlRaftConfig>>;

    async fn full_snapshot(
        &mut self,
        vote: VoteOf<ControlRaftConfig>,
        snapshot: SnapshotOf<ControlRaftConfig, Vec<u8>>,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<ControlRaftConfig>, StreamingError<ControlRaftConfig>>;
}
```

The data implementation has the same signatures with `DataRaftConfig`.

`new_client`, `new_heartbeat_client`, and `new_snapshot_client` return distinct lazy tonic channels. A slow append cannot hold the heartbeat channel for that peer. No shared mutex spans a network await.

Each tonic request uses `option.soft_ttl()` as its request timeout. Connection errors and `Unavailable` map to `RPCError::Unreachable`. Local serialization and transient HTTP/2 errors map to `RPCError::Network`. A tonic deadline maps to an Openraft `Timeout` with the correct source node, target node, and RPC kind.

`pre_vote` returns an explicit `Unreachable` error. The Raft config sets `enable_pre_vote = Some(false)`, so Openraft does not call it. This override prevents a later config change from silently using Openraft 0.10's default granting implementation without a real peer RPC.

`full_snapshot` returns `StreamingError::Unreachable` with `snapshot transport is unsupported until LS06`. The three-voter LS02b config cannot request a snapshot because automatic snapshots and purge are disabled.

## Module ownership

```text
crates/light-stream-core/
  src/node.rs                 NodeDescriptor, ThreeVoterTopology
  src/bootstrap.rs            BootstrapPlan and BootstrapResult
  src/error.rs                deterministic DomainError only

crates/light-stream-proto/
  src/convert.rs              public protobuf boundary
  src/peer_convert.rs         join and diagnostics boundary

crates/light-stream-storage/
  src/lib.rs                  public storage re-exports
  src/format_v1.rs            existing keys, checksums, thin entries, snapshots
  src/control.rs              ControlCommand codec and ControlStateReader
  src/data.rs                 DataCommand codec and CommittedStateReader
  src/rocks.rs                ordered RocksDB write lane and Openraft traits

crates/light-stream-server/
  src/manifest.rs             V1 load, V2 validation, atomic transitions
  src/runtime.rs              ClusterRuntime and runtime state enum
  src/bootstrap.rs            idempotent learner and membership reconciler
  src/groups/control.rs       ControlGroupHandle
  src/groups/data.rs          DataGroupHandle
  src/peer/codec.rs           versioned Openraft JSON encoding
  src/peer/network.rs         two RaftNetworkFactory implementations
  src/peer/service.rs         typed control and data RPC dispatch
  src/service.rs              public API adapter
  src/diagnostics.rs          read-only status projection

crates/light-stream-client/
  src/lib.rs                  stable public Client methods
  src/directory.rs            validated node directory
  src/retry.rs                deadline and leader-hint state machine

crates/light-stream-cli/
  src/main.rs                 voter parsing and cluster status output

crates/light-stream-testkit/
  src/ls02b.rs                E02-E09 verifier state machine
  src/fault_proxy.rs          owned group-aware public and peer fault proxies
```

`ClusterRuntime`, the group handles, and `Client` are the deep modules. The protobuf, Openraft, and RocksDB adapters remain private.

## Startup, bootstrap, and join state machine

### Node startup

```text
Acquire data-directory lock
        |
        v
Load and validate manifest
        |
        +-- no manifest, no group directories --> Pristine
        |
        +-- no manifest, group directories ----> Failed(orphan_group_storage)
        |
        +-- V1 standalone ----------------------> verify stores, rewrite V2 Active
        |
        +-- V2 Provisioning --------------------> open both groups, peer-ready, reconcile
        |
        +-- V2 Active --------------------------> open both groups, wait_for_recovery,
                                                  verify activation proof, app-ready
```

`Raft::new` returns before restart recovery is complete. An active node does not serve fetch, receipt, or publish until both groups pass `wait_for_recovery` and the manifest proof still matches storage.

A pristine node serves health, capabilities, probe, bootstrap, and `PrepareJoin`. Application operations return `not_bootstrapped`.

A provisioning joiner starts its peer service before waiting for replicated logs. Otherwise the coordinator could never add it as a learner. Its public application operations return `cluster_forming`.

Every provisioning node also starts `FormationReconciler`. A learner only observes progress. If a joint voter configuration survives a coordinator crash and another voter becomes leader, that leader can finish the uniform membership from the same durable plan.

### Three-voter bootstrap

The bootstrap lock serializes exact-plan reconciliation on the coordinator.

```text
Pristine coordinator
  1. validate BootstrapPlan and local descriptor
  2. durably write ProvisioningManifest
  3. create control and data stores
  4. create both Raft handles with typed network factories
  5. initialize control with coordinator only
  6. commit ControlCommand::Bootstrap
  7. initialize data with coordinator only
  8. commit DataCommand::Bootstrap
  9. PrepareJoin node 2 and node 3
 10. control.add_learner(node 2, blocking=true)
 11. control.add_learner(node 3, blocking=true)
 12. control.change_membership({1,2,3}, retain=false)
 13. data.add_learner(node 2, blocking=true)
 14. data.add_learner(node 3, blocking=true)
 15. data.change_membership({1,2,3}, retain=false)
 16. prove exact committed memberships and catalogs
 17. atomically write local ActiveManifest
 18. FinalizeJoin node 2 and node 3
 19. return BootstrapResult
```

Control and data membership changes are sequential. This avoids two concurrent bootstrap workflows mutating the same remote manifests and makes evidence easier to interpret. Normal Raft operation remains independent by group.

### Retry after a crash

The manifest records the requested end state, not transient completion flags.

On an exact retry, the reconciler:

1. opens both durable groups;
2. calls `is_initialized`;
3. reads effective and committed memberships;
4. waits if a joint membership is still committing;
5. rejects any unknown node or changed endpoint;
6. prepares only missing joiners;
7. calls `add_learner(..., true)` only for a node absent from the group;
8. calls `change_membership` only when all requested nodes are present as either voters or caught-up learners;
9. proves both final memberships before writing `Active`.

Calling `initialize` on a non-pristine group is never part of recovery.

If the coordinator dies while a group still has only one voter, the operator restarts that durable node and retries the exact bootstrap request. No other node can safely elect itself at that point. If a joint or uniform configuration already gives the group a viable quorum, whichever node Openraft elects can finish reconciliation from the same durable plan. LS02b does not claim coordinator failover before the first viable voter promotion. No active application path depends on the original coordinator.

### Joiner transitions

```text
Pristine
  |
  | PrepareJoin, exact local descriptor
  v
Provisioning(peer-ready, app-not-ready)
  |
  | replicated bootstrap catalog and exact committed
  | three-voter memberships in both local groups
  v
Active
```

`FinalizeJoin` asks the target to verify local durable state. It does not tell the node to trust the coordinator's claim.

## Peer routing validation

Each peer RPC performs these checks before decoding or dispatch:

1. tonic enforces the 40 MiB service limit;
2. `protocol_version == 1` and `encoding_version == 1`;
3. `target_node_id` equals the configured local node ID;
4. a provisioning or active manifest exists;
5. `cluster_id` equals the durable manifest cluster;
6. the RPC method's fixed group kind matches the fixed group ID;
7. `sender_node_id` appears in the durable three-voter topology;
8. the local typed handle exists;
9. JSON decoding consumes the full body and stays within a decode-depth limit;
10. an append sender equals `rpc.vote.leader_id.node_id`;
11. a vote sender equals `rpc.vote.leader_id.node_id`.

Only then does the service call:

```rust
control.raft.append_entries(request).await
control.raft.vote(request).await
data.raft.append_entries(request).await
data.raft.vote(request).await
```

The peer path never enters the public application service or an application mailbox.

These checks prevent accidental cross-cluster, cross-node, cross-group, and cross-config routing. They do not authenticate a peer. In `local-insecure`, a process that can reach the peer listener can forge a sender ID. Peer authentication remains explicitly unsupported until LS08.

## Leader hints and client retry semantics

### Server behavior

For writes, the group handle matches:

```rust
RaftError::APIError(ClientWriteError::ForwardToLeader(forward))
```

For reads, it matches:

```rust
RaftError::APIError(LinearizableReadError::ForwardToLeader(forward))
RaftError::APIError(LinearizableReadError::QuorumNotEnough(_))
RaftError::Fatal(fatal)
```

If `ForwardToLeader` contains both an ID and a `RaftNode.public_addr`, the server returns a `LeaderHint`. If either is absent, it returns a retryable `not_leader` without a hint.

The server does not open a public client to another node. This avoids hidden proxy retries and keeps one operation deadline visible to the caller.

### Client behavior

Each operation owns one decreasing deadline budget.

```rust
enum RetryState {
    SelectEndpoint,
    Attempt { node: NodeId, number: u8 },
    FollowHint { hint: LeaderHint },
    RefreshDirectory,
    ResolveUnknownWrite,
    Complete,
}
```

Rules:

- use the cached leader first, then rotate through seeds;
- set a tonic timeout to `min(attempt_timeout, remaining_operation_budget)`;
- accept a hint only when its node ID and public address match the validated directory;
- reject repeated `(group, node)` hints after four redirects;
- refresh the directory from `ClusterStatus` when a hint is absent or invalid;
- retry publish only with the original `ProducerRequestId` and identical fingerprint;
- retry bootstrap only with the exact `BootstrapPlan`;
- retry fetch and receipt because they have no side effect;
- return `OutcomeUnknown` when a write budget expires after any submitted attempt;
- never convert a timeout into a definite rejection;
- never invent a default leader or fall back to node `1`.

Receipt durability remains the LS02a state-machine rule. A successful publish response is sent only after RocksDB has durably applied the receipt. Every voter applies the same fingerprint and range. A new leader therefore returns the original range after response loss.

## Diagnostics shape

`ClusterStatus` is a projection of:

- the durable manifest lifecycle;
- `RaftMetrics.state`;
- `RaftMetrics.current_leader`;
- effective and committed membership;
- `last_log_index`, `last_applied`, and leader replication progress.

The public result does not expose terms, votes, or absolute Raft log IDs. It derives:

- `membership_committed` from effective membership equality with committed membership;
- `local_state_applied` from local applied progress reaching local committed progress;
- `write_ready` only for an active local leader with a recent quorum acknowledgement;
- `lag_entries` from the leader's local last log index minus the peer's matched index;
- `recently_reachable` from heartbeat metrics and the configured election timeout.

Followers can report their local role and current leader. Per-peer progress is present only on the leader because Openraft exposes replication metrics only there.

The response always includes:

```text
unsupported_claims:
  - snapshot_after_purge_until_ls06
  - peer_authentication_until_ls08
  - independent_host_ha_not_verified
```

Diagnostics are for evidence and operations. Application routing uses leader hints and the validated node directory, not scraped diagnostic text.

## Snapshot handling choice

LS02b chooses log-suffix catch-up only for new three-voter clusters:

```rust
Config {
    snapshot_policy: SnapshotPolicy::Never,
    max_in_snapshot_log_to_keep: u64::MAX,
    replication_lag_threshold: u64::MAX,
    max_payload_entries: 1,
    enable_pre_vote: Some(false),
    ..validated_ls02_timing()
}
```

Openraft never purges logs that are not in a snapshot. With `SnapshotPolicy::Never`, a new LS02b three-voter cluster retains the full Raft log and can complete E04, E06, and E07 through ordinary suffix replication.

The existing LS02a storage snapshot builder, installer, payload ownership, 64 MiB cap, and tests remain unchanged. Existing active standalone nodes retain their LS02a behavior. LS02b does not convert an old standalone cluster into a new three-voter cluster.

`RaftNetworkV2::full_snapshot` returns an explicit unsupported transport error. No snapshot RPC is declared. This prevents a unary 64 MiB snapshot from slipping into the ordinary append path.

E08, snapshot catch-up after purge, is `BLOCKED_LS06_SNAPSHOT_AFTER_PURGE`. LS06 must add a dedicated chunked snapshot RPC, staged file receipt, digest verification, atomic publication, and then enable purge. A storage unit test that installs a `Vec<u8>` snapshot is not E08 evidence.

This choice trades disk growth for a smaller correct LS02b. It avoids claiming crash-safe remote snapshot installation before that path exists.

## Openraft 0.10.0-alpha.34 compatibility

The design was checked against the exact local source at:

```text
/Users/danielgerlag/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/
  openraft-0.10.0-alpha.34/
```

| Design use | Exact local source fact |
| --- | --- |
| `Raft::new` | `src/raft/mod.rs:469` takes `id`, `Arc<Config>`, a `RaftNetworkFactory`, `RaftLogStorage`, and `RaftStateMachine`. Its network snapshot data must equal the state-machine snapshot data. |
| Log identity | `src/vote/leader_id/leader_id_adv.rs` defines the `(term, node_id)` leader ID used by LS02a. Both new type configs select it explicitly. |
| Protocol dispatch | `src/raft/mod.rs:901`, `981`, and `1033` expose `append_entries`, `vote`, and `install_full_snapshot`. |
| Linearizable reads | `src/raft/mod.rs:1088` returns `Result<ReadLogId<C>, RaftError<C, LinearizableReadError<C>>>`; candidate B uses `ReadPolicy::ReadIndex`. |
| Application writes | `src/raft/mod.rs:1207` returns `ClientWriteResponse<C>` with the applied response data. |
| Pristine check | `src/raft/mod.rs:1338` exposes `is_initialized`. |
| Initialization | `src/raft/mod.rs:1392` accepts `IntoNodes`. Candidate B passes only the coordinator. |
| Learner flow | `src/raft/impl_raft_blocking_write.rs:108` exposes `add_learner(id, node, blocking)`. |
| Membership flow | `src/raft/impl_raft_blocking_write.rs:69` exposes `change_membership(members, retain)` and performs joint then uniform consensus. |
| Network factory | `src/network/factory.rs:28` requires a network type and has separate ordinary, heartbeat, and snapshot constructors. |
| Network V2 | `src/network/v2/network.rs:106` requires `append_entries`, `vote`, and `full_snapshot`; `pre_vote` has a granting default. Candidate B overrides it and disables pre-vote. |
| RPC deadlines | `src/network/rpc_option.rs` says transports use `soft_ttl()` and Openraft owns the hard TTL. |
| Leader hints | `src/errors/mod.rs:363` exposes `ForwardToLeader { leader_id, leader_node }`. |
| Read quorum failure | `src/errors/linearizable_read_error.rs` distinguishes `ForwardToLeader` from `QuorumNotEnough`. |
| Metrics | `src/metrics/raft_metrics.rs` exposes state, current leader, effective and committed memberships, quorum acknowledgement, and leader-only replication progress. |
| Snapshot policy | `src/config/config.rs` defines `SnapshotPolicy::Never`; logs outside a snapshot are not eligible for purge. |

The design does not use Openraft 0.9 chunk RPCs, `InstallSnapshotRequest`, or 0.9 storage signatures.

## Error mapping

### Public application mapping

| Source | Public code | Retryable | Certainty | Client action |
| --- | --- | --- | --- | --- |
| invalid protobuf or bounded domain value | `invalid_argument` | no | definite no effect | return |
| no manifest | `not_bootstrapped` | no | definite no effect | bootstrap |
| provisioning manifest | `cluster_forming` | yes | definite no effect | retry exact plan or wait |
| conflicting manifest or topology | `bootstrap_conflict` | no | definite no effect | operator correction |
| `ForwardToLeader` | `not_leader` | yes | definite no effect | validate and follow hint |
| `LinearizableReadError::QuorumNotEnough` | `quorum_unavailable` | yes | definite no read result | try another endpoint until deadline |
| server deadline during publish | `deadline_exceeded` | yes | unknown | retry same identity or query receipt |
| server deadline during read | `deadline_exceeded` | yes | definite no read result | retry |
| deterministic receipt mismatch | `receipt_conflict` | no | definite no new effect | return |
| missing retained receipt | `receipt_not_found` or `receipt_expired` | no | definite | return |
| Openraft fatal or RocksDB failure | `storage_error` | no | unknown for writes | stop local node and preserve store |

Tonic status carries boundary failures such as malformed protobuf and oversized messages. Valid application failures remain typed protobuf `ErrorResult` values so the CLI retains stable error codes.

### Peer transport mapping

| Tonic result | Openraft result |
| --- | --- |
| response before `soft_ttl` | decoded Openraft response |
| tonic `DeadlineExceeded` | `RPCError::Timeout` |
| connect refused, channel closed, tonic `Unavailable` | `RPCError::Unreachable` |
| local codec or transient HTTP/2 write failure | `RPCError::Network` |
| bad protocol, cluster, group, target, or sender | `RPCError::Unreachable` with bounded diagnostic text |
| local peer Raft fatal | tonic `Unavailable`, then `RPCError::Unreachable` |

Error text never determines application behavior.

## Verifier state machine

The verifier runs release binaries through the Rust client and CLI. It uses three processes, three data directories, stable ports, and owned group-aware proxies. Each E02-E09 scenario gets a fresh artifact directory and independent ledger.

```rust
enum Ls02bSuiteState {
    BuildRelease,
    Run { scenario: ScenarioId },
    Record { scenario: ScenarioId, verdict: Verdict },
    Complete,
    Failed,
}

enum ScenarioState {
    Allocate,
    StartNodes,
    WaitPeerReady,
    Bootstrap,
    AssertMembership,
    DriveTraffic,
    ApplyFault,
    ConfirmFault,
    AwaitRecovery,
    ReadAcknowledgedLedger,
    ResolveUnknownWrites,
    StopNodes,
    PersistResult,
}
```

The state machine cannot record `PASS` before `ConfirmFault`, `ReadAcknowledgedLedger`, and `PersistResult`.

### E02: three-node application journey

1. Start all three pristine nodes behind peer proxies.
2. Bootstrap through node 1.
3. Assert both groups have the exact three committed voters and no learners.
4. Partition node 1 only for control-group peer RPCs.
5. Wait for nodes 2 or 3 to become control leader while node 1 remains data leader.
6. Heal the control partition.
7. Publish through node 2 and fetch through node 3.
8. Assert client attempt logs show direct leader-hint routing.
9. Compare all bytes and offsets with the external ledger.

The proxy reads the peer RPC method and header. It does not alter production state or elect a leader through an admin shortcut.

### E03: leader crash during traffic

1. Record the data leader from diagnostics.
2. Publish and sync the acknowledgement ledger.
3. Kill that exact owned process.
4. Confirm another voter becomes data leader within 10 seconds.
5. Continue publishes through a different endpoint.
6. Read every acknowledged byte and receipt.
7. Restart the old leader and verify it returns as a follower.

### E04: live old leader in a minority

1. Keep the old data leader process and public listener alive.
2. Block only its data-group peer links in both directions.
3. Confirm the majority elects another data leader.
4. Send publish, fetch, and receipt requests directly to the old leader.
5. Require no successful publish and no supposedly fresh read.
6. Heal the links.
7. Wait for zero data-group lag and compare the committed ledger on all nodes.

The old leader may have an uncommitted local suffix. The verifier does not count it as acknowledged data.

### E05: majority unavailable

1. Block data-group peer links so no node can contact a majority.
2. Confirm the fault through proxy counters.
3. Attempt writes through every public endpoint with finite deadlines.
4. Require `not_leader`, `quorum_unavailable`, or unknown write deadline results, and zero successful acknowledgements.
5. Confirm linearizable fetch cannot report fresh success.
6. Heal the cluster.
7. Resolve every unknown request by receipt before the final ledger check.

The scenario permits an unknown timed-out write to commit after healing. It forbids a local-only success response.

### E06: slow minority

1. Delay data-group RPCs only to node 3 beyond the per-RPC soft TTL.
2. Publish through nodes 1 and 2 at the pinned low-load profile.
3. Require the healthy majority to keep acknowledging.
4. Confirm heartbeat traffic to node 2 is not blocked by node 3's slow append path.
5. Remove the delay.
6. Wait for node 3's lag to reach zero without removing it from membership.
7. Compare node 3's committed bytes with the ledger.

### E07: log-suffix catch-up

1. Stop node 3.
2. Publish enough records through the remaining majority.
3. Restart node 3 with the same store while `snapshot_mode` is `disabled_ls02b_no_purge`.
4. Wait for lag to reach zero.
5. Compare offsets, payload bytes, and receipts.
6. Assert no snapshot RPC was attempted.

### E08: snapshot catch-up

Record:

```json
{
  "scenario": "E08",
  "verdict": "BLOCKED",
  "reason": "BLOCKED_LS06_SNAPSHOT_AFTER_PURGE",
  "unsupported_claim": "LS02b disables three-voter snapshots and purge"
}
```

No other scenario may be relabeled as E08.

### E09: lost acknowledgement and retry

1. Send a publish through a public fault proxy.
2. Let the proxy observe the complete successful server response, then discard it before the client receives it.
3. Retry the same `ProducerRequestId` through another endpoint.
4. Require the original offset range.
5. Query the receipt from the new data leader.
6. Submit the same identity with different bytes and require `receipt_conflict`.
7. Assert exactly one committed record range in the external ledger.

### Evidence and claims

Every scenario saves commands, attempts, acknowledgements, unknown outcomes, reads, fault confirmations, node logs, proxy counters, diagnostics, and `result.json`.

The local suite may claim:

- real same-host elections;
- real three-voter durable majority commits;
- minority fencing through failed commit and `ReadIndex`;
- real log-suffix catch-up;
- durable lost-response receipts.

It must report:

- snapshot-after-purge: `BLOCKED_LS06_SNAPSHOT_AFTER_PURGE`;
- independent-host HA and capacity: `BLOCKED_REFERENCE_HOSTS`;
- secured peer identity: `UNSUPPORTED_LS08`;
- leader transfer and node replacement: `UNSUPPORTED_LS06`;
- pre-vote: `UNSUPPORTED_LS02B`;
- multi-group placement: `UNSUPPORTED_LS03`.

## Alternatives considered and rejected

### One generic peer RPC and `GroupHandle<C>`

This had the smallest line count. It also required a runtime group-kind switch before generic decoding, kept `GroupCommand` legal for both configs, and made a cross-group dispatch error possible in every caller. Candidate B accepts four Raft RPC methods to remove that state.

### Initialize all three voters on the coordinator

Openraft permits `initialize` with multiple nodes, but this skips `add_learner(..., true)` and the required learner-to-voter proof. It also asks fresh remote stores to participate before the coordinator has prepared them. Rejected.

### Server-side forwarding to the leader

Forwarding hides retries inside the server, creates nested deadlines, and makes a lost response harder to classify. It can also turn one client attempt into multiple writes unless every proxy preserves the exact identity. Candidate B returns a hint and lets the client connect directly.

### Fixed node 1 leader

The POC used this shape, but it has no election or quorum fencing. Candidate B uses node 1 only as an explicit bootstrap coordinator. Active control and data leaders come only from Openraft elections.

### Enable snapshots and purge in LS02b

The current in-memory snapshot format passes storage tests, but remote transfer, staged receipt, interrupted installation, and corrupt transfer evidence do not exist. A unary snapshot would also share the ordinary peer message budget. Candidate B keeps logs and defers the complete remote path to LS06.

### One RPC service per group instance

Two services per node would make control and data separation obvious, but LS03 would need one service registration per data group. The chosen method-per-kind shape keeps one peer listener and lets future data groups route by `group_id`.

### Pre-vote without a peer RPC

Openraft 0.10's default `pre_vote` implementation grants. Enabling it without overriding the network method would misrepresent unreachable peers. Candidate B disables it and overrides the method to fail closed.

## Tradeoffs accepted

- We accept an explicit bootstrap coordinator in exchange for a small crash-recoverable formation workflow. Failover is not claimed while a group still has only that voter.
- We accept 40 MiB bounded JSON peer messages in exchange for reusing the pinned Openraft serde shapes without a second binary codec. LS06 can replace the peer encoding behind protocol version `2`.
- We accept full Raft-log retention for new three-voter clusters in exchange for correct suffix catch-up without an incomplete snapshot transport.
- We accept four typed Raft RPC methods in exchange for preventing control and data request confusion at the service boundary.
- We accept client-visible leader hints in exchange for no server proxy and one visible retry budget.
- We accept same-host functional evidence in exchange for making no independent failure-domain or capacity claim.

## Self-grade

| Criterion | Grade | Reason |
| --- | --- | --- |
| Exact Openraft 0.10 compatibility | A | The design uses the local alpha.34 signatures for `Raft::new`, `RaftNetworkFactory`, `RaftNetworkV2`, `is_initialized`, `add_learner`, `change_membership`, `client_write`, and `ensure_linearizable`. It accounts for the granting default of `pre_vote`. |
| Illegal routing and bootstrap states prevented | A- | Separate commands, handles, RPC methods, manifest states, and private activation proofs prevent the main illegal states. Local-insecure peer spoofing remains unsupported until LS08 and is stated. |
| Minimal maintainable surface | A- | The public client and runtime remain small. The peer service adds four typed methods, but that code replaces repeated runtime kind checks. Snapshot transfer and LS03 routing stay out. |
| Complete E02-E09 local verification | A- | E02-E07 and E09 have concrete release-binary scenarios and fault oracles. E08 is explicitly blocked until LS06, as required. Independent-host claims remain blocked. |
| No hidden fixed leader or proxy shortcut | A | Openraft elects both active leaders. Servers never forward application operations. The only coordinator is explicit, durable, and limited to formation. |

Overall: **A-**. The main cost is retained Raft-log growth until LS06. That cost is visible and safer than a partial snapshot claim.

## First implementation step if this design is approved

Split the in-memory control and data command types while preserving persisted format `1`, then add compile-only Openraft boundary tests for both typed network adapters before changing startup behavior.
