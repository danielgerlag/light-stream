# LS02b candidate A design

## Caller usage

LS02b keeps the LS02a standalone command unchanged:

```sh
light-streamd \
	--node-id 1 \
	--public-listen 127.0.0.1:7101 \
	--peer-listen 127.0.0.1:7201 \
	--data-dir run/node-1

light-streamctl --endpoint http://127.0.0.1:7101 cluster bootstrap \
	--cluster-id e71f2179-3cb5-c6c0-ea5f-716fbf8fa223 \
	--stream-id a8fc25d5-4e16-4eb9-92ce-f794f7456485 \
	--stream-name bootstrap
```

Omitting `--node` keeps the one-voter LS02a behavior. A three-voter bootstrap adds exactly three node descriptors:

```sh
light-streamctl --endpoint http://127.0.0.1:7101 cluster bootstrap \
	--cluster-id e71f2179-3cb5-c6c0-ea5f-716fbf8fa223 \
	--stream-id a8fc25d5-4e16-4eb9-92ce-f794f7456485 \
	--stream-name bootstrap \
	--node 1,http://127.0.0.1:7101,127.0.0.1:7201 \
	--node 2,http://127.0.0.1:7102,127.0.0.1:7202 \
	--node 3,http://127.0.0.1:7103,127.0.0.1:7203
```

The operator starts all three pristine processes before this command. The endpoint that receives the command coordinates bootstrap. It does not remain a preferred leader.

For three voters, the coordinator owns the initial control-group bootstrap. The next node by `NodeId` owns the initial data-group bootstrap. Each owner initializes a one-voter group, adds the other two replicas as learners with `add_learner(..., true)`, and then commits the uniform three-voter membership with `change_membership(..., false)`. This gives E02 different control and data leaders without a fixed-leader rule or a leader-transfer test hook.

The Rust client keeps the single-endpoint constructor and adds a multi-seed constructor:

```rust
let client = Client::connect("http://127.0.0.1:7103").await?;

let receipt = client.publish(batch.clone()).await?;
let page = client
	.fetch(cluster_id, partition, receipt.range().first(), 128)
	.await?;
```

If node 3 is not the data leader, it returns a typed leader hint. The client opens a direct connection to that leader and retries the same request. The server never forwards the application call.

Callers that want several initial routes use:

```rust
let client = Client::connect_many([
	"http://127.0.0.1:7101",
	"http://127.0.0.1:7102",
	"http://127.0.0.1:7103",
])
.await?;
```

The client keeps one short-lived leader cache per group kind. A hint replaces the cached endpoint. A failed cached endpoint falls back to the configured seeds. No node ID is special after bootstrap.

The CLI follows the same retry policy:

```sh
light-streamctl --endpoint http://127.0.0.1:7101 publish \
	--cluster-id e71f2179-3cb5-c6c0-ea5f-716fbf8fa223 \
	--stream-id a8fc25d5-4e16-4eb9-92ce-f794f7456485 \
	--principal verify \
	--session eff6d7c6-fa33-856f-ee69-80f64ca8c28c \
	--sequence 9 \
	--file record.bin
```

If the response is lost after commit, `Client::publish` retries the same `ProducerRequestId` and the same payload. The replicated receipt returns the original offset range. If the overall deadline expires, the client returns `ClientError::AmbiguousWrite` with the request ID. It does not claim that the write failed.

Diagnostics are read-only:

```sh
light-streamctl --endpoint http://127.0.0.1:7101 diagnostics
```

The JSON result names the local startup state, both group leaders, voter sets, commit and apply positions, and leader-side follower match positions. It also states that snapshot catch-up, node replacement, operator-directed leader transfer, and secured transport remain unsupported.

## Problem

LS02a already has durable one-voter control and data groups, compact Raft log descriptors, one stored payload per replica, linearizable reads, durable receipts, complete local snapshots, and exact Openraft `0.10.0-alpha.34` storage traits. LS02b must add real tonic Raft traffic, elections, quorum refusal, suffix catch-up, and lost-response retry without changing those storage rules. The hard part is bootstrap. Three pristine processes must reach a uniform three-voter membership through learners, while restart and retry cannot initialize a second cluster or route one group's RPC into the other group.

LS02b does not add LS03 placement, bounded group actors, stream lifecycle, or independent data paths. It does not add LS08 authentication or transport security. It retains all Raft logs for new LS02b clusters, so snapshot-after-purge catch-up remains an explicit LS06 claim.

## Grounded constraints

The design follows these existing facts:

- `docs/architecture/runtime.md` requires distinct control and data Raft configurations, direct peer routing to Openraft, dedicated replication and heartbeat clients, linearizable reads, and exact Openraft `=0.10.0-alpha.34`.
- `crates/light-stream-storage/src/lib.rs` persists `BasicNode`, `GroupCommand`, thin log entries, payload ownership, applied records, receipts, and complete `Vec<u8>` snapshots under storage format version 1.
- `crates/light-stream-server/src/runtime.rs` currently owns explicit bootstrap, fixed group IDs 1 and 2, and the durable root manifest.
- `crates/light-stream-client/src/lib.rs` currently connects to one endpoint and maps every Openraft failure through string inspection. LS02b replaces that string inspection with typed mapping.
- `artifacts/LS02a/final-3/result.json` is `PASS`. `durable-journey.json`, `receipt-evidence.json`, and `storage-test-evidence.json` prove restart durability, exact retry receipts, payload snapshots, and storage conformance.
- `artifacts/LS02a/final-3/unsupported.json` names three-voter replication, elections, and quorum refusal as LS02b work. It names snapshot catch-up as LS06 and secured mode as LS08.

## Design shape

### Domain types

These types belong in `light-stream-core`. Their fields stay private. Constructors perform all parsing and cross-field validation.

```rust
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NodeDescriptor {
	id: NodeId,
	public_endpoint: PublicEndpoint,
	peer_endpoint: PeerEndpoint,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PublicEndpoint {
	uri: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PeerEndpoint {
	address: SocketAddr,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum BootstrapTopology {
	Standalone(StandaloneTopology),
	ThreeVoter(ThreeVoterTopology),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StandaloneTopology {
	local: NodeDescriptor,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ThreeVoterTopology {
	coordinator: NodeId,
	nodes: [NodeDescriptor; 3],
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum GroupKind {
	Control,
	Data,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LeaderHint {
	group: GroupKind,
	node_id: NodeId,
	public_endpoint: PublicEndpoint,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BootstrapPlan {
	spec: BootstrapSpec,
	topology: BootstrapTopology,
	control_owner: NodeId,
	data_owner: NodeId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BootstrapProposal {
	spec: BootstrapSpec,
	nodes: Vec<NodeDescriptor>,
}
```

`BootstrapTopology::parse` accepts only these shapes:

- No wire nodes means standalone on the contacted node. This preserves LS02a.
- One explicit node means standalone. The descriptor must match the contacted process.
- Three explicit nodes means three voters.
- Every node ID is nonzero and unique.
- Every public endpoint and peer endpoint is unique.
- No endpoint contains a zero port.
- The contacted node appears exactly once.
- In local-insecure mode, existing loopback rules still apply.

The owner selection is deterministic:

```rust
impl BootstrapPlan {
	pub fn materialize_new(
		proposal: BootstrapProposal,
		contacted_node: NodeId,
	) -> Result<Self, DomainError>;

	pub fn accepts(&self, proposal: &BootstrapProposal) -> bool;
	pub const fn control_owner(&self) -> NodeId;
	pub const fn data_owner(&self) -> NodeId;
	pub fn voters(&self) -> BTreeSet<u64>;
	pub fn node(&self, id: NodeId) -> Option<&NodeDescriptor>;
}
```

The prost boundary creates `BootstrapProposal`. A pristine node calls `materialize_new`. A prepared or active node compares the proposal with its stored plan and reuses the stored coordinator and owners. This lets the same request resume through any node without changing the plan.

For standalone, both owners are the local node. For three voters, the control owner is the first contacted node and the data owner is the next node in sorted `NodeId` order. This choice exists only in the durable bootstrap intent. Current leaders always come from Openraft metrics.

### Durable node manifest

The root manifest moves from version 1 to version 2. The group databases remain at storage format version 1.

```rust
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "format_version")]
enum StoredNodeManifest {
	#[serde(rename = "1")]
	V1(LegacyClusterManifestV1),
	#[serde(rename = "2")]
	V2(NodeManifestV2),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct NodeManifestV2 {
	cluster_id: ClusterId,
	local_node: NodeId,
	bootstrap: BootstrapSpec,
	topology: BootstrapTopology,
	groups: GroupManifestSet,
	phase: DurableNodePhase,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct GroupManifestSet {
	control: GroupManifest<ControlGroup>,
	data: GroupManifest<DataGroup>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct GroupManifest<K> {
	group_id: GroupId,
	bootstrap_owner: NodeId,
	#[serde(skip)]
	kind: PhantomData<K>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
enum DurableNodePhase {
	Prepared {
		coordinator: NodeId,
	},
	Active {
		control_membership_log: PersistedLogPosition,
		data_membership_log: PersistedLogPosition,
	},
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct PersistedLogPosition {
	term: u64,
	leader_node_id: u64,
	index: u64,
}
```

`Prepared` means both group stores exist and both Raft handles can receive peer traffic. It does not mean that application traffic is allowed. `Active` means the local state machines have applied uniform membership entries with the exact voter set for both groups.

The manifest writer keeps the LS02a durability sequence:

```rust
fn write_manifest_atomic(
	data_dir: &Path,
	manifest: &NodeManifestV2,
) -> Result<(), ManifestError>;
```

It writes `cluster.json.tmp`, calls `sync_all` on the file, renames it to `cluster.json`, and calls `sync_all` on the parent directory.

Version 1 migration is narrow:

- A version 1 manifest opens as an active standalone topology.
- The migration creates a version 2 manifest with only the local node.
- The migration does not rewrite group storage or membership entries.
- Standalone listeners may change across restart, which preserves the LS02a port-zero verifier.
- A version 1 standalone cluster cannot expand to three voters in LS02b. That requires snapshot-aware replacement and remains unsupported until LS06.

For a version 2 three-voter manifest, the configured local node ID and advertised endpoints must match the stored descriptor exactly. A restart with changed addresses fails before either Raft handle starts.

### Runtime states

The server owns one state value:

```rust
enum NodeRuntime {
	Pristine(PristineNode),
	Prepared(PreparedNode),
	Active(Arc<ActiveNode>),
	Faulted(NodeFault),
}

struct PristineNode {
	local: NodeDescriptor,
}

struct PreparedNode {
	manifest: Arc<NodeManifestV2>,
	control: ControlHandle,
	data: DataHandle,
}

struct ActiveNode {
	manifest: Arc<NodeManifestV2>,
	control: ControlHandle,
	data: DataHandle,
}

pub struct ClusterManager {
	state: RwLock<NodeRuntime>,
	transition: Mutex<()>,
	manifest_store: ManifestStore,
	peer_directory: PeerDirectory,
}
```

Only `ClusterManager` changes `NodeRuntime`. Public and peer services borrow typed capabilities from it:

```rust
impl ClusterManager {
	pub async fn open(config: RuntimeConfig) -> Result<Self, StartupError>;

	pub async fn bootstrap(
		&self,
		request: BootstrapPlan,
	) -> Result<BootstrapResult, DomainError>;

	pub async fn public_data(&self) -> Result<ActiveDataRef<'_>, DomainError>;

	pub async fn route_control(
		&self,
		route: ValidatedPeerRoute<ControlGroup>,
	) -> Result<ControlHandle, PeerError>;

	pub async fn route_data(
		&self,
		route: ValidatedPeerRoute<DataGroup>,
	) -> Result<DataHandle, PeerError>;

	pub async fn diagnostics(&self) -> NodeDiagnostics;
}
```

`PristineNode` can serve health, capabilities, bootstrap, peer probe, and `PrepareReplica`. `PreparedNode` can serve Raft and bootstrap-management RPCs. It rejects publish, fetch, and receipt with `cluster_preparing`. `ActiveNode` serves all LS02a application calls and all LS02b peer calls. `Faulted` serves diagnostics and rejects every mutation.

### Separate control and data handles

The raw `Raft` fields stay private:

```rust
type ControlRaft =
	openraft::Raft<ControlRaftConfig, RocksStateMachine<ControlRaftConfig>>;

type DataRaft =
	openraft::Raft<DataRaftConfig, RocksStateMachine<DataRaftConfig>>;

#[derive(Clone)]
struct ControlHandle {
	raft: ControlRaft,
	route: GroupRoute<ControlGroup>,
}

#[derive(Clone)]
struct DataHandle {
	raft: DataRaft,
	reader: CommittedStateReader,
	route: GroupRoute<DataGroup>,
}

enum ControlGroup {}
enum DataGroup {}

#[derive(Clone)]
struct GroupRoute<K> {
	cluster_id: ClusterId,
	group_id: GroupId,
	local_node: NodeId,
	kind: PhantomData<K>,
}
```

Application code cannot obtain the raw control Raft handle. It cannot publish into the control group. The control handle exposes only bootstrap and membership operations:

```rust
impl ControlHandle {
	async fn bootstrap_catalog(
		&self,
		spec: BootstrapSpec,
	) -> Result<BootstrapResult, RaftCallError>;

	async fn ensure_voters(
		&self,
		plan: &BootstrapPlan,
	) -> Result<MembershipReceipt, RaftCallError>;
}

impl DataHandle {
	async fn bootstrap_partition(
		&self,
		spec: BootstrapSpec,
	) -> Result<BootstrapResult, RaftCallError>;

	async fn ensure_voters(
		&self,
		plan: &BootstrapPlan,
	) -> Result<MembershipReceipt, RaftCallError>;

	async fn publish(
		&self,
		batch: PublishBatch,
		deadline: Instant,
	) -> Result<PublishReceipt, DomainError>;

	async fn fetch(
		&self,
		cluster: ClusterId,
		partition: PartitionKey,
		offset: RecordOffset,
		limit: u32,
	) -> Result<FetchPage, DomainError>;

	async fn receipt(
		&self,
		cluster: ClusterId,
		partition: PartitionKey,
		request: &ProducerRequestId,
	) -> Result<PublishReceipt, DomainError>;
}
```

Both handles still instantiate the existing shared storage adapter and `GroupCommand`. This preserves the LS02a log and snapshot format. The typed handle boundary prevents new application code from constructing an invalid group command.

### Exact Openraft 0.10 calls

The design targets the local source at:

```text
/Users/danielgerlag/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/openraft-0.10.0-alpha.34
```

The lockfile pins checksum `06fe14d529f8fc098fbb4b9655d5db9b7ee0594e6a9d4e220aa931618de17b1f`.

The implementation must use these exact signatures:

```rust
Raft::new(
	node_id: u64,
	config: Arc<openraft::Config>,
	network: TonicNetworkFactory<C>,
	log_store: RocksLogStore<C>,
	state_machine: RocksStateMachine<C>,
) -> Result<Raft<C, RocksStateMachine<C>>, _>;

raft.initialize(BTreeMap<u64, BasicNode>)
	.await;

raft.add_learner(node_id, BasicNode::new(peer_address), true)
	.await;

raft.change_membership(BTreeSet<u64>, false)
	.await;

raft.client_write(GroupCommand::Publish { batch })
	.await;

raft.ensure_linearizable(openraft::ReadPolicy::ReadIndex)
	.await;

raft.wait_for_recovery(Some(timeout))
	.await;

raft.metrics();
raft.current_leader().await;
raft.shutdown().await;
```

`add_learner` and `change_membership` both return:

```rust
Result<
	openraft::raft::ClientWriteResponse<C>,
	openraft::errors::RaftError<C, openraft::errors::ClientWriteError<C>>,
>
```

The design does not use a 0.9 install-snapshot request, a chunk offset API, or a 0.9 storage signature.

### Openraft configuration

LS02b uses:

```rust
openraft::Config {
	cluster_name,
	election_timeout_min: 750,
	election_timeout_max: 1_500,
	heartbeat_interval: 200,
	max_payload_entries: 1,
	max_append_entries: Some(64),
	snapshot_policy: SnapshotPolicy::Never,
	max_in_snapshot_log_to_keep: 0,
	enable_pre_vote: Some(true),
	..Default::default()
}
```

The values are proposed local defaults, not capacity claims. The timeout relation passes `Config::validate`. `max_payload_entries = 1` bounds a replicated application entry to the existing 8 MiB publish limit plus protocol overhead. `RocksLogReader::limited_get_log_entries` keeps its 8 MiB read cap.

`SnapshotPolicy::Never` is deliberate. Openraft does not purge logs that are not included in a snapshot. E07 can therefore prove real suffix catch-up. LS02b does not accidentally enter a remote snapshot path that it cannot complete.

Pre-vote requires a real peer RPC. Openraft's default `RaftNetworkV2::pre_vote` grants when unimplemented, so LS02b overrides it. A disconnected node must return an error, not a synthetic grant.

## Protobuf RPCs

The public schema keeps all existing field numbers. It adds topology, hints, diagnostics, and retry metadata.

```proto
syntax = "proto3";

package lightstream.v1;

message BootstrapNode {
	uint64 node_id = 1;
	string public_endpoint = 2;
	string peer_endpoint = 3;
}

message BootstrapRequest {
	string cluster_id = 1;
	string stream_id = 2;
	string stream_name = 3;
	repeated BootstrapNode nodes = 4;
}

message LeaderHint {
	string group_kind = 1;
	uint64 node_id = 2;
	string public_endpoint = 3;
}

enum RetryDisposition {
	RETRY_DISPOSITION_UNSPECIFIED = 0;
	RETRY_DISPOSITION_DO_NOT_RETRY = 1;
	RETRY_DISPOSITION_RETRY_SAME_REQUEST = 2;
	RETRY_DISPOSITION_RETRY_AFTER_BACKOFF = 3;
}

enum CommitStatus {
	COMMIT_STATUS_UNSPECIFIED = 0;
	COMMIT_STATUS_NOT_COMMITTED = 1;
	COMMIT_STATUS_COMMITTED = 2;
	COMMIT_STATUS_UNKNOWN = 3;
}

message ErrorResult {
	string code = 1;
	string message = 2;
	LeaderHint leader_hint = 3;
	RetryDisposition retry = 4;
	CommitStatus commit_status = 5;
}

message DiagnosticsRequest {}

enum NodeStartupState {
	NODE_STARTUP_STATE_UNSPECIFIED = 0;
	NODE_STARTUP_STATE_PRISTINE = 1;
	NODE_STARTUP_STATE_PREPARED = 2;
	NODE_STARTUP_STATE_ACTIVE = 3;
	NODE_STARTUP_STATE_FAULTED = 4;
}

message LogPosition {
	bool known = 1;
	uint64 term = 2;
	uint64 leader_node_id = 3;
	uint64 index = 4;
}

message PeerProgress {
	uint64 node_id = 1;
	LogPosition matched = 2;
}

message GroupDiagnostics {
	uint64 group_id = 1;
	string group_kind = 2;
	string raft_state = 3;
	uint64 current_term = 4;
	uint64 current_leader_id = 5;
	repeated uint64 voters = 6;
	repeated uint64 learners = 7;
	LogPosition local_committed = 8;
	LogPosition cluster_committed = 9;
	LogPosition last_applied = 10;
	LogPosition snapshot = 11;
	LogPosition purged = 12;
	repeated PeerProgress replication = 13;
	string running_error = 14;
}

message UnsupportedClaim {
	string claim = 1;
	string available_phase = 2;
	string reason = 3;
}

message DiagnosticsResponse {
	uint64 node_id = 1;
	NodeStartupState startup_state = 2;
	string cluster_id = 3;
	repeated GroupDiagnostics groups = 4;
	repeated UnsupportedClaim unsupported = 5;
}

service LightStream {
	rpc Health(HealthRequest) returns (HealthResponse);
	rpc Capabilities(CapabilitiesRequest) returns (CapabilitiesResponse);
	rpc Bootstrap(BootstrapRequest) returns (BootstrapResponse);
	rpc Publish(PublishRequest) returns (PublishResponse);
	rpc CommitPublish(PublishRequest) returns (PublishResponse);
	rpc Fetch(FetchRequest) returns (FetchResponse);
	rpc GetReceipt(ReceiptRequest) returns (ReceiptResponse);
	rpc Diagnostics(DiagnosticsRequest) returns (DiagnosticsResponse);
}
```

The peer schema uses protobuf-native Openraft fields. It does not encode Openraft values as JSON.

```proto
syntax = "proto3";

package lightstream.peer.v1;

message PeerRoute {
	uint32 protocol_version = 1;
	string cluster_id = 2;
	uint64 group_id = 3;
	uint64 sender_node_id = 4;
	uint64 target_node_id = 5;
}

message RaftVote {
	uint64 term = 1;
	uint64 node_id = 2;
	bool committed = 3;
}

message RaftLogId {
	uint64 term = 1;
	uint64 node_id = 2;
	uint64 index = 3;
}

message OptionalLogId {
	bool present = 1;
	RaftLogId value = 2;
}

message VoterSet {
	repeated uint64 node_ids = 1;
}

message MembershipNode {
	uint64 node_id = 1;
	string peer_endpoint = 2;
}

message MembershipEntry {
	repeated VoterSet configs = 1;
	repeated MembershipNode nodes = 2;
}

message BootstrapCommand {
	string cluster_id = 1;
	string stream_id = 2;
	string stream_name = 3;
}

message ProducerRequestId {
	string principal_id = 1;
	string producer_session_id = 2;
	uint64 sequence = 3;
}

message PublishCommand {
	string cluster_id = 1;
	string stream_id = 2;
	uint32 partition_id = 3;
	ProducerRequestId request_id = 4;
	repeated bytes records = 5;
}

message RaftEntry {
	RaftLogId log_id = 1;
	oneof payload {
		bool blank = 2;
		MembershipEntry membership = 3;
		BootstrapCommand bootstrap_control = 4;
		BootstrapCommand bootstrap_data = 5;
		PublishCommand publish = 6;
	}
}

message AppendEntriesRequest {
	PeerRoute route = 1;
	RaftVote vote = 2;
	OptionalLogId prev_log_id = 3;
	repeated RaftEntry entries = 4;
	OptionalLogId leader_commit = 5;
}

message PartialSuccess {
	OptionalLogId matched = 1;
}

message AppendEntriesResponse {
	oneof result {
		bool success = 1;
		PartialSuccess partial_success = 2;
		bool conflict = 3;
		RaftVote higher_vote = 4;
	}
}

message VoteRequest {
	PeerRoute route = 1;
	RaftVote vote = 2;
	OptionalLogId last_log_id = 3;
	bool leadership_transfer = 4;
}

message VoteResponse {
	RaftVote vote = 1;
	bool vote_granted = 2;
	OptionalLogId last_log_id = 3;
}

message PrepareReplicaRequest {
	uint32 protocol_version = 1;
	uint64 sender_node_id = 2;
	uint64 target_node_id = 3;
	bytes canonical_bootstrap_plan = 4;
}

message PrepareReplicaResponse {
	bool prepared = 1;
}

message BootstrapGroupRequest {
	PeerRoute route = 1;
	string group_kind = 2;
	bytes canonical_bootstrap_plan = 3;
}

message BootstrapGroupResponse {
	oneof result {
		RaftLogId committed_membership = 1;
		ManagementLeaderHint leader_hint = 2;
		ManagementError error = 3;
	}
}

message ManagementLeaderHint {
	uint64 node_id = 1;
	string peer_endpoint = 2;
}

message ManagementError {
	string code = 1;
	string message = 2;
}

message FinalizeReplicaRequest {
	uint32 protocol_version = 1;
	uint64 sender_node_id = 2;
	uint64 target_node_id = 3;
	string cluster_id = 4;
	RaftLogId control_membership = 5;
	RaftLogId data_membership = 6;
}

message FinalizeReplicaResponse {
	bool active = 1;
}

message PeerProbeRequest {
	uint32 protocol_version = 1;
	uint64 target_node_id = 2;
}

message PeerProbeResponse {
	uint32 protocol_version = 1;
	string revision = 2;
	uint64 node_id = 3;
}

service PeerService {
	rpc Probe(PeerProbeRequest) returns (PeerProbeResponse);
	rpc PrepareReplica(PrepareReplicaRequest) returns (PrepareReplicaResponse);
	rpc BootstrapGroup(BootstrapGroupRequest) returns (BootstrapGroupResponse);
	rpc FinalizeReplica(FinalizeReplicaRequest) returns (FinalizeReplicaResponse);
	rpc AppendEntries(AppendEntriesRequest) returns (AppendEntriesResponse);
	rpc Vote(VoteRequest) returns (VoteResponse);
	rpc PreVote(VoteRequest) returns (VoteResponse);
}
```

`canonical_bootstrap_plan` is the checksum-wrapped persisted representation produced by the manifest module. The peer proto module treats it as opaque bytes, then the manifest boundary decodes and validates it. This keeps one authoritative bootstrap representation instead of duplicating topology rules in prost conversion code.

There is no snapshot RPC in LS02b. Adding a dormant RPC would enlarge the peer interface without producing a supported path.

## Peer transport

### Exact network traits

One generic adapter implements the exact `RaftNetworkV2<C>` trait:

```rust
struct TonicNetworkFactory<C> {
	local: NodeId,
	group: GroupRouteMarker<C>,
	directory: Arc<PeerDirectory>,
}

struct TonicRaftNetwork<C> {
	local: NodeId,
	target: NodeId,
	group: GroupRouteMarker<C>,
	lane: PeerLane,
	directory: Arc<PeerDirectory>,
}

enum PeerLane {
	Replication,
	Heartbeat,
	Snapshot,
}
```

```rust
impl<C> openraft::network::RaftNetworkFactory<C> for TonicNetworkFactory<C>
where
	C: SupportedGroupConfig,
{
	type Network = TonicRaftNetwork<C>;

	async fn new_client(
		&mut self,
		target: u64,
		node: &BasicNode,
	) -> Self::Network;

	async fn new_heartbeat_client(
		&mut self,
		target: u64,
		node: &BasicNode,
	) -> Self::Network;

	async fn new_snapshot_client(
		&mut self,
		target: u64,
		node: &BasicNode,
	) -> Self::Network;
}
```

Each constructor validates `node.addr` against the durable topology before it returns a client. The replication and heartbeat lanes use separate tonic `Channel` values. The snapshot lane has no channel.

```rust
impl<C> openraft::network::RaftNetworkV2<C> for TonicRaftNetwork<C>
where
	C: SupportedGroupConfig,
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
		cancel: impl Future<Output = ReplicationClosed>
			+ openraft::OptionalSend
			+ 'static,
		option: RPCOption,
	) -> Result<SnapshotResponse<C>, StreamingError<C>>;
}
```

`full_snapshot` returns `StreamingError::Unreachable` with the stable reason `snapshot_catch_up_unsupported_until_LS06`. `SnapshotPolicy::Never` prevents normal LS02b operation from calling it.

The adapter uses Openraft's default sequential `stream_append`. LS02b does not add a custom pipelined stream.

### Message bounds and deadlines

The constants are:

```rust
const MAX_PUBLIC_MESSAGE_BYTES: usize = 9 * 1024 * 1024;
const MAX_PEER_MESSAGE_BYTES: usize = 10 * 1024 * 1024;
const PUBLIC_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(2);
const PUBLIC_OVERALL_TIMEOUT: Duration = Duration::from_secs(8);
const BOOTSTRAP_OVERALL_TIMEOUT: Duration = Duration::from_secs(30);
```

Both tonic servers and every generated client set maximum encoding and decoding sizes. Every outbound Raft request calls:

```rust
request.set_timeout(option.soft_ttl());
```

The adapter maps tonic `DeadlineExceeded` to `RPCError::Timeout`, connection refusal and route absence to `RPCError::Unreachable`, and a broken established HTTP2 exchange to `RPCError::Network`. A remote Raft fatal error becomes `Unreachable`, because `RaftNetworkV2::append_entries` and `vote` use `RPCError<C>` with no application error type.

The public client gives every attempt the smaller of `PUBLIC_ATTEMPT_TIMEOUT` and the remaining overall budget. Bootstrap uses its separate overall budget.

### Dial overrides for local fault verification

`PeerDirectory` separates the identity endpoint from the dial endpoint:

```rust
struct PeerDirectory {
	topology: BootstrapTopology,
	dial_overrides: BTreeMap<NodeId, PeerEndpoint>,
}

impl PeerDirectory {
	fn resolve(
		&self,
		local: NodeId,
		target: NodeId,
		openraft_node: &BasicNode,
	) -> Result<DialTarget, PeerRouteError>;
}
```

`openraft_node.addr` must equal the durable descriptor's peer endpoint. Only then may a local `--peer-route NODE_ID=ADDRESS` override change where the socket connects. The verifier uses one directed proxy per local-node and target-node pair. Production defaults have no overrides and dial the durable endpoint directly.

The override cannot change the target identity. Every request carries `target_node_id`, and the receiver rejects a request for another node.

## Peer routing validation

The prost boundary converts an untrusted `PeerRoute` into one of two typed routes:

```rust
struct ValidatedPeerRoute<K> {
	cluster: ClusterId,
	group: GroupId,
	sender: NodeId,
	target: NodeId,
	kind: PhantomData<K>,
}

enum RoutedGroup {
	Control(ValidatedPeerRoute<ControlGroup>),
	Data(ValidatedPeerRoute<DataGroup>),
}

fn validate_peer_route(
	wire: peer::PeerRoute,
	manifest: &NodeManifestV2,
) -> Result<RoutedGroup, PeerRouteError>;
```

Validation occurs before decoding the Raft body:

1. `protocol_version` must equal 1.
2. `target_node_id` must equal the local node.
3. `cluster_id` must equal the durable manifest.
4. `group_id` must equal the control group ID or the data group ID.
5. `sender_node_id` must exist in the durable topology.
6. The Raft vote's leader node must equal `sender_node_id`.
7. Every membership node address must match the durable topology.
8. The decoded message must fit the route's exact Openraft configuration.

`PrepareReplica` has a separate boundary because a pristine node has no cluster manifest. It accepts only a plan whose target descriptor matches the running process. A retry must byte-match the stored prepared manifest. A different cluster, topology, coordinator, or bootstrap stream returns `bootstrap_conflict`.

These checks prevent control and data RPC swaps, target-node mistakes, stale address injection, and a second bootstrap plan. They do not authenticate the sender. Authentication remains LS08.

## Bootstrap and join state machine

### Startup

```text
No cluster.json
  -> Pristine

cluster.json v1
  -> validate LS02a stores
  -> migrate to v2 standalone Active
  -> open control and data handles
  -> wait_for_recovery

cluster.json v2 Prepared
  -> validate local identity and endpoints
  -> open control and data handles without initialize
  -> serve peer Raft and bootstrap-management RPCs
  -> reject application data calls

cluster.json v2 Active
  -> validate both durable group identities
  -> open both handles
  -> wait_for_recovery on both
  -> verify exact uniform voter sets
  -> serve application calls
```

Startup never calls `initialize` based only on an empty log. Only the durable group bootstrap owner may call it while the manifest is `Prepared`.

### Three-voter bootstrap

The public bootstrap call runs this state machine:

```text
Validate BootstrapPlan
  -> lock ClusterManager transition
  -> if Active and exact match, return the existing BootstrapResult
  -> if Active and mismatch, return bootstrap_conflict
  -> persist local Prepared manifest
  -> PrepareReplica on both remote nodes
  -> BootstrapGroup(Control) on control_owner
  -> BootstrapGroup(Data) on data_owner
  -> FinalizeReplica on all three nodes
  -> persist local Active manifest
  -> return BootstrapResult
```

`PrepareReplica` is idempotent:

```rust
async fn prepare_replica(
	&self,
	plan: BootstrapPlan,
) -> Result<(), BootstrapError>;
```

It writes `Prepared` before it creates the group stores. On recovery, a prepared manifest with a missing pristine group directory creates that directory. A non-pristine mismatched directory faults the node.

`BootstrapGroup` is group-specific:

```rust
async fn bootstrap_control_group(
	handle: &ControlHandle,
	plan: &BootstrapPlan,
) -> Result<MembershipReceipt, BootstrapError>;

async fn bootstrap_data_group(
	handle: &DataHandle,
	plan: &BootstrapPlan,
) -> Result<MembershipReceipt, BootstrapError>;
```

Each function:

1. If the group is pristine, verifies that the local node is the durable owner. No other node may call `initialize`.
2. Reads current and committed membership from `RaftMetrics`.
3. If the group is initialized and the local node is not leader, returns a typed peer leader hint.
4. If the group is pristine, calls `initialize` with only the owner and its `BasicNode`.
5. Waits for the owner to become leader.
6. Writes the existing LS02a bootstrap command if the state machine does not already contain it.
7. Calls `add_learner(id, BasicNode::new(peer), true)` for each missing non-owner node.
8. Waits until every learner has matched the leader through the blocking Openraft call.
9. Calls `change_membership(expected_voters, false)` only when the committed uniform voter set differs.
10. Waits until `membership_config` and `committed_membership_config` name the same one-set uniform membership.
11. Returns the committed membership log ID.

If a membership is joint after a crash, the function waits for Openraft to finish it. It does not issue another change while `membership_config.log_id()` differs from `committed_membership_config.log_id()`.

`FinalizeReplica` checks local evidence:

```rust
async fn finalize_replica(
	&self,
	expected_control: PersistedLogPosition,
	expected_data: PersistedLogPosition,
) -> Result<(), BootstrapError>;
```

For both groups, the local committed membership must be uniform, must contain exactly the planned voters, and must have reached at least the expected membership log. The local state machine must have applied that membership. Only then does the node persist `Active`.

### Bootstrap retry and crash cases

- A retry before `PrepareReplica` finishes repeats the same requests.
- A retry after one group becomes three-voter resumes the other group.
- A retry after both groups become three-voter repeats finalization.
- A coordinator crash does not elect a special replacement coordinator. Any node can accept the same public bootstrap request, read the durable plan, and resume calls to the two recorded group owners.
- A group owner crash before adding any learner blocks that group's bootstrap until the owner returns. No other node has a quorum at that point.
- A group owner crash after the three-voter membership commits allows normal election. The resuming coordinator sends `BootstrapGroup` to the current group leader by following the typed peer leader hint.
- A conflicting bootstrap request never calls Openraft.

## Application routing and linearizable reads

The public server never proxies application operations.

`DataHandle::publish` calls `client_write` on the local data Raft. It maps `ClientWriteError::ForwardToLeader` to a typed `not_leader` result with the public endpoint from the durable topology.

`DataHandle::fetch` and `DataHandle::receipt` call:

```rust
self.raft
	.ensure_linearizable(openraft::ReadPolicy::ReadIndex)
	.await?;
```

Only after the barrier succeeds do they read `CommittedStateReader`. A follower returns a leader hint. A minority old leader receives `LinearizableReadError::QuorumNotEnough` and returns `quorum_unavailable`. It never serves a local supposedly current read.

The server wraps application calls in finite deadlines. A write deadline produces:

```text
code = quorum_unavailable
retry = RETRY_SAME_REQUEST
commit_status = COMMIT_STATUS_UNKNOWN
```

The unknown status matters. A timed-out entry may commit after connectivity returns. The caller resolves it with the same publish request or `GetReceipt`.

A read quorum failure produces:

```text
code = quorum_unavailable
retry = RETRY_AFTER_BACKOFF
commit_status = COMMIT_STATUS_NOT_COMMITTED
```

The commit field describes the read operation, not the data already stored.

## Leader hints and client retry semantics

```rust
#[derive(Clone, Debug)]
pub struct ClientOptions {
	seeds: Vec<PublicEndpoint>,
	attempt_timeout: Duration,
	overall_timeout: Duration,
	max_attempts: NonZeroUsize,
}

#[derive(Clone)]
pub struct Client {
	options: Arc<ClientOptions>,
	channels: Arc<ChannelPool>,
	leaders: Arc<RwLock<LeaderCache>>,
}

#[derive(Default)]
struct LeaderCache {
	control: Option<CachedLeader>,
	data: Option<CachedLeader>,
}

struct CachedLeader {
	node_id: NodeId,
	endpoint: PublicEndpoint,
	observed_at: Instant,
}
```

The public methods remain domain-typed:

```rust
impl Client {
	pub async fn connect(
		endpoint: impl Into<String>,
	) -> Result<Self, ClientError>;

	pub async fn connect_many<I, S>(
		endpoints: I,
	) -> Result<Self, ClientError>
	where
		I: IntoIterator<Item = S>,
		S: Into<String>;

	pub async fn bootstrap(
		&self,
		plan: BootstrapPlan,
	) -> Result<BootstrapResult, ClientError>;

	pub async fn publish(
		&self,
		batch: PublishBatch,
	) -> Result<PublishReceipt, ClientError>;

	pub async fn fetch(
		&self,
		cluster: ClusterId,
		partition: PartitionKey,
		offset: RecordOffset,
		limit: u32,
	) -> Result<FetchPage, ClientError>;

	pub async fn receipt(
		&self,
		cluster: ClusterId,
		partition: PartitionKey,
		request: ProducerRequestId,
	) -> Result<PublishReceipt, ClientError>;
}
```

Retry rules:

1. Start with the unexpired group leader cache. If none exists, start with the caller's seed.
2. On `not_leader` with a valid hint, replace the cache and retry that endpoint.
3. On `not_leader` without a hint, connection failure, or retryable unavailability, rotate through seeds.
4. Never retry a validation error, identity mismatch, bootstrap conflict, or receipt conflict.
5. Retry a publish only with the same `PublishBatch`. The client does not rebuild the request ID.
6. Stop at `max_attempts` or the overall deadline.
7. If any publish attempt may have reached a server, return `AmbiguousWrite` when the budget expires.
8. If no publish attempt connected, return `Unavailable`.

The client validates a leader hint before use. Its endpoint must parse, its node ID must be nonzero, and the group kind must match the operation. The server is still the authority for cluster identity.

## Durable receipts

LS02b keeps the LS02a receipt algorithm:

- The Raft state machine computes the publish fingerprint from cluster, partition, request identity, and records.
- The replicated receipt stores the original committed offset range.
- A matching duplicate returns the stored receipt without assigning offsets.
- A conflicting duplicate returns `receipt_conflict`.
- The client retries after response loss with the same request identity.
- Quorum loss never falls back to a local RocksDB write outside `client_write`.

No receipt is stored in the root manifest or a client cache. Every successful receipt comes from applied replicated state.

## Diagnostics

`NodeDiagnostics` is a stable domain structure. Prost conversion owns the wire details.

```rust
pub struct NodeDiagnostics {
	pub node_id: NodeId,
	pub startup_state: DiagnosticsStartupState,
	pub cluster_id: Option<ClusterId>,
	pub groups: Vec<GroupDiagnostics>,
	pub unsupported: Vec<UnsupportedClaim>,
}

pub struct GroupDiagnostics {
	pub identity: DiagnosticGroupIdentity,
	pub raft_state: String,
	pub term: u64,
	pub leader: Option<NodeId>,
	pub voters: BTreeSet<NodeId>,
	pub learners: BTreeSet<NodeId>,
	pub local_committed: Option<DiagnosticLogPosition>,
	pub cluster_committed: Option<DiagnosticLogPosition>,
	pub last_applied: Option<DiagnosticLogPosition>,
	pub snapshot: Option<DiagnosticLogPosition>,
	pub purged: Option<DiagnosticLogPosition>,
	pub replication: BTreeMap<NodeId, Option<DiagnosticLogPosition>>,
	pub running_error: Option<String>,
}
```

The adapter reads `RaftMetrics` fields that exist in alpha.34:

- `running_state`
- `current_term`
- `state`
- `current_leader`
- `membership_config`
- `committed_membership_config`
- `local_committed`
- `cluster_committed`
- `last_applied`
- `snapshot`
- `purged`
- `replication`

Diagnostics do not call `client_write`, `initialize`, `add_learner`, `change_membership`, `elect`, `purge_log`, or snapshot triggers.

## Snapshot choice

LS02b preserves the LS02a storage snapshot implementation and its tests. The snapshot contains complete state and retained payload objects and remains capped at 64 MiB.

LS02b disables automatic snapshot creation for new three-voter clusters with `SnapshotPolicy::Never`. It does not expose a peer snapshot RPC. It retains all Raft history, so a stopped or slow follower catches up through ordinary AppendEntries.

The following claims are explicit:

| Claim | LS02b status | Reason |
| --- | --- | --- |
| Local snapshot build and install in the storage adapter | Supported, inherited from LS02a | Required by Openraft storage conformance and existing product tests. |
| Log-suffix follower catch-up | Supported | LS02b retains the required log history. |
| Snapshot catch-up after Raft history purge | `UNSUPPORTED_LS06` | There is no peer snapshot RPC and LS02b does not purge new cluster history. |
| Expanding an old standalone cluster after its history was purged | `UNSUPPORTED_LS06` | Safe expansion may require snapshot transfer. |
| Node replacement | `UNSUPPORTED_LS06` | Replacement needs snapshot-aware learner handling and lifecycle policy. |

This is safer than wiring an incomplete snapshot stream that can pass metadata while omitting payload bytes.

## Verifier state machine

`scripts/verify.py` remains the evidence coordinator. `light-stream-testkit` remains the independent data oracle and adds diagnostics polling plus owned fault proxies.

```rust
enum ScenarioState {
	Preparing,
	StartingNodes,
	Bootstrapping,
	WaitingForTopology,
	RunningWorkload,
	InjectingFault,
	WaitingForRecovery,
	ReconcilingAmbiguousWrites,
	ValidatingLedger,
	StoppingNodes,
	Passed,
	Failed,
	Unsupported,
	Blocked,
}
```

Every transition writes an evidence event before the next action. A fault transition records the proxy rule or process signal and confirms that it took effect.

The local LS02b run uses three release `light-streamd` processes with fixed public and peer ports. Each process receives per-target `--peer-route` addresses for directed proxies. The verifier drives application work through both `light-streamctl` and the Rust client.

### E02

1. Start three pristine nodes.
2. Bootstrap the exact three-node plan through node 1.
3. Wait until diagnostics show uniform voters `{1, 2, 3}` for both groups on all nodes.
4. Require control leader 1 and data leader 2 immediately after bootstrap.
5. Publish through node 1. The client must follow a data-leader hint to node 2.
6. Fetch through node 3. The client must follow a leader hint.
7. Compare every byte, offset, request ID, endpoint, and retry event with the external ledger.

### E03

1. Run acknowledged traffic.
2. Stop the current data leader process.
3. Keep the other two voters connected.
4. Continue with the same client and seeds.
5. Require a new leader and successful traffic within B2's 10 seconds.
6. Read every acknowledged byte.

This is local functional evidence. Independent-host HA remains `BLOCKED`.

### E04

1. Leave the old data leader's public listener running.
2. Block all four directed peer routes between it and the other two voters.
3. Send a publish and a fetch to the old leader.
4. Require no successful write acknowledgement.
5. Require the fetch to fail with `quorum_unavailable` or a valid new-leader hint. It must not return a supposedly current page from local state.
6. Publish through the majority.
7. Heal all links and wait until the old leader's match and applied positions reach the majority.
8. Read the majority's acknowledged data through the healed node.

Independent-host partition tolerance remains `BLOCKED`.

### E05

1. Stop two voters.
2. Send writes to the remaining node for longer than one application attempt timeout.
3. Require `quorum_unavailable`, `RETRY_SAME_REQUEST`, and `COMMIT_STATUS_UNKNOWN`.
4. Require zero successful acknowledgements during the fault.
5. Restart the voters.
6. Resolve every ambiguous request through retry or receipt before judging presence.
7. Require no local-only acknowledgement path.

### E06

1. Delay both directed routes to and from voter 3.
2. Keep voters 1 and 2 healthy.
3. Publish a recorded workload.
4. Require the healthy majority to progress within B1.
5. Remove the delay.
6. Wait until voter 3's diagnostics show the same committed and applied positions.
7. Read identical bytes through voter 3.

This is local slow-minority evidence. Independent disks and hosts remain `BLOCKED`.

### E07

1. Stop voter 3.
2. Publish enough records to create several Raft entries.
3. Assert that diagnostics report no purge frontier beyond voter 3's match point.
4. Restart voter 3.
5. Require AppendEntries suffix catch-up.
6. Require identical records, offsets, receipts, committed position, and applied position.

### E08

Record:

```json
{
	"scenario": "E08",
	"verdict": "UNSUPPORTED",
	"code": "snapshot_catch_up_unsupported_until_LS06",
	"reason": "LS02b retains Raft history and has no peer snapshot RPC"
}
```

The verifier must not count the existing storage-only snapshot unit test as E08.

### E09

1. Send a publish through a response-dropping public proxy.
2. Let the request reach the data leader and commit.
3. Drop the HTTP2 response.
4. Retry the same `PublishBatch` through another endpoint.
5. Require the original offsets and request identity.
6. Send the same request with different bytes and require `receipt_conflict`.
7. Restart all nodes and require the receipt again.

### Evidence outputs

The run writes:

```text
manifest.json
result.json
attempts.jsonl
acks.jsonl
errors.jsonl
reads.jsonl
faults.jsonl
routing.jsonl
diagnostics.jsonl
e02.json
e03.json
e04.json
e05.json
e06.json
e07.json
e08.json
e09.json
unsupported.json
```

`result.json` passes only when E02, E03, E04, E05, E06, E07, and E09 pass, E08 has the exact unsupported record, the LS02a suite passes, and every ledger discrepancy count is zero.

## Error mapping

### Server domain errors

```rust
pub enum DomainError {
	InvalidIdentity { kind: String, reason: String },
	InvalidName { kind: String, reason: String },
	InvalidRange { reason: String },
	InvalidPayload { reason: String },
	InvalidTopology { reason: String },
	NotBootstrapped,
	ClusterPreparing,
	BootstrapConflict { reason: String },
	IdentityMismatch { reason: String },
	ReceiptConflict,
	ReceiptExpired,
	ReceiptNotFound,
	NotLeader { hint: Option<LeaderHint> },
	QuorumUnavailable { group: GroupKind },
	Storage { reason: String },
	UnsupportedOperation {
		operation: String,
		available_phase: String,
	},
}
```

Typed Openraft mapping:

| Openraft result | Public code | Retry | Commit status |
| --- | --- | --- | --- |
| `ClientWriteError::ForwardToLeader` | `not_leader` | same request | unknown |
| `LinearizableReadError::ForwardToLeader` | `not_leader` | retry after hint | not committed |
| `LinearizableReadError::QuorumNotEnough` | `quorum_unavailable` | backoff | not committed |
| Write deadline elapsed | `quorum_unavailable` | same request | unknown |
| `RaftError::Fatal(Fatal::StorageError)` | `storage_error` | do not retry automatically | unknown for writes |
| `RaftError::Fatal(Fatal::Panicked)` | `storage_error` and node faults | do not retry automatically | unknown |
| `ChangeMembershipError::InProgress` | internal bootstrap retry | backoff | not applicable |
| `ChangeMembershipError::LearnerNotFound` | `bootstrap_conflict` and node faults | do not retry with another plan | not applicable |
| `InitializeError::NotAllowed` | exact-plan recovery check | do not call initialize again | not applicable |

The mapper matches enum variants. It never searches error strings for `ForwardToLeader`.

### Peer tonic status

Peer RPCs use tonic status for transport and protocol failures:

| Condition | tonic code |
| --- | --- |
| Missing field, invalid ID, invalid address | `InvalidArgument` |
| Protocol version, cluster, group, sender, or target mismatch | `FailedPrecondition` |
| Message exceeds the peer bound | `ResourceExhausted` |
| Local group is not prepared | `Unavailable` |
| Local Raft stopped or storage failed | `Unavailable` |
| Deadline elapsed | `DeadlineExceeded` |

No peer failure becomes an application receipt.

### Client errors

```rust
pub enum ClientError {
	InvalidEndpoint { endpoint: String, reason: String },
	Connection(String),
	Protocol(String),
	Domain(DomainError),
	Unavailable {
		attempted: Vec<PublicEndpoint>,
	},
	AmbiguousWrite {
		request: ProducerRequestId,
		last_error: String,
	},
}
```

CLI exit codes retain existing meanings where possible:

- 2 for invalid local input.
- 3 for unsupported operations.
- 4 for conflicts.
- 5 for unavailable, not leader after retry exhaustion, quorum unavailable, and ambiguous write.
- 1 for protocol or unexpected client failures.

The JSON error includes `retry`, `commit_status`, and `leader_hint` when present.

## Module ownership

```text
crates/light-stream-core/
  src/bootstrap.rs       BootstrapPlan and topology types
  src/error.rs           Domain error additions
  src/identity.rs        Existing NodeId and GroupId
  src/diagnostics.rs     Stable diagnostic domain types

crates/light-stream-proto/
  src/convert.rs         Public wire conversion and validation
  proto public schema    Existing public RPCs plus topology and diagnostics

crates/light-stream-storage/
  src/lib.rs             Existing storage format and receipt logic
                         No transport or topology ownership

crates/light-stream-server/
  src/manifest.rs        Versioned root manifest and atomic migration
  src/runtime.rs         NodeRuntime transitions and application entry points
  src/group.rs           Typed ControlHandle and DataHandle
  src/bootstrap.rs       Prepare, group bootstrap, finalize, and resume
  src/peer/convert.rs    Protobuf and Openraft conversion
  src/peer/network.rs    RaftNetworkFactory and RaftNetworkV2
  src/peer/service.rs    Inbound validation and typed Raft dispatch
  src/peer/directory.rs  Durable identity endpoints and local dial overrides
  src/service.rs         Public tonic service only

crates/light-stream-client/
  src/lib.rs             Public Client methods
  src/retry.rs           Leader cache, deadlines, and retry policy

crates/light-stream-cli/
  src/main.rs            Repeated --node parsing and diagnostics command

crates/light-stream-testkit/
  src/oracle.rs          Independent ledger checks
  src/fault_proxy.rs     Owned directed TCP proxies and response drop
  src/scenario.rs        E02 to E09 driver state machine

scripts/verify.py         Process ownership, scenario coordination, evidence
verification/            LS02b profiles and scenario predicates
```

Storage does not know tonic addresses. The proto crate does not own retry policy. The client does not inspect Openraft values. The verifier does not call storage helpers.

The public interface stays small. Application callers still use `bootstrap`, `publish`, `fetch`, `receipt`, and diagnostics. Bootstrap management, Raft traffic, topology reconciliation, and membership recovery remain behind the server boundary.

## Explicit unsupported claims

LS02b must report these claims without implying a pass:

- `snapshot_catch_up`: `UNSUPPORTED_LS06`
- `snapshot_after_purge`: `UNSUPPORTED_LS06`
- `node_replacement`: `UNSUPPORTED_LS06`
- `standalone_to_three_voter_expansion`: `UNSUPPORTED_LS06`
- `operator_leader_transfer`: `UNSUPPORTED_LS06`
- `secured_public_transport`: `UNSUPPORTED_LS08`
- `secured_peer_transport`: `UNSUPPORTED_LS08`
- `independent_host_high_availability`: `BLOCKED_REFERENCE_HOSTS`
- `independent_disk_failure_domains`: `BLOCKED_REFERENCE_HOSTS`
- `LS03_partition_placement`: `UNSUPPORTED_LS03`

The local E03, E04, and E06 results prove process and link behavior on one host. They do not prove independent-host availability.

## Synthesis decision

I compared three whole designs without spawning agents.

Candidate A uses deterministic per-group bootstrap owners, real learner promotion, follower leader hints, and direct client retries. It became the base because it guarantees different initial group leaders for E02 while leaving Openraft in full control after bootstrap.

Candidate B initializes both groups on one coordinator and adds server-side forwarding. It has a smaller bootstrap protocol, but every follower becomes an application proxy. That hides quorum and leader behavior from the client and violates the no-proxy requirement.

Candidate C initializes both groups on one coordinator and adds a verifier-only election or transfer RPC to split leaders. It preserves direct client routing, but the test hook mutates consensus state and advances leader transfer ahead of its supported phase.

Candidate A takes the manifest migration and typed control and data handles that all candidates need. It rejects server forwarding, direct three-voter initialization, test-only elections, and a dormant snapshot RPC.

## Alternatives considered and rejected

### Initialize all three voters directly

Rejected. `Raft::initialize` can accept three members, but it skips the required learner catch-up proof. A bad address can enter the voter set before the node has any log.

### One bootstrap owner for both groups

Rejected as the complete design. It starts both groups with the same leader and makes E02's different-leader predicate depend on random elections or an extra transfer mechanism.

### Store public and peer addresses in a custom Openraft node type

Rejected for LS02b. Replacing `BasicNode` changes serialized membership entries and snapshots. The root manifest can map a leader node ID to its public endpoint without changing the LS02a group format.

### Server-side application forwarding

Rejected. A follower proxy adds another timeout and ambiguity boundary, hides the contacted leader from evidence, and can become an accidental fixed gateway.

### A fixed node 1 client target

Rejected. It fails after node 1 stops and does not exercise Openraft leader hints.

### Automatic snapshots with one large unary RPC

Rejected. A 64 MiB unary message conflicts with the bounded peer-message requirement. It also advances snapshot-after-purge semantics without staged-file verification.

### Chunked remote snapshots in LS02b

Rejected. A correct path needs a staging file, digest checks, cancellation cleanup, atomic publication, and payload-root accounting. That is LS06 work. Retaining logs is smaller and makes E07 complete.

### Custom pipelined AppendEntries streaming

Rejected. Alpha.34 supplies a sequential default over `append_entries`. One bounded entry per RPC is enough for the first real three-voter path.

## Tradeoffs accepted

- We accept two bootstrap group owners in exchange for deterministic different leaders without leader transfer.
- We accept three internal bootstrap-management RPCs in exchange for restartable, typed transitions across three processes.
- We accept retained Raft history in exchange for excluding incomplete remote snapshot handling.
- We accept per-process peer dial overrides in exchange for deterministic E04 and E06 link faults through real tonic traffic.
- We accept a root manifest version migration in exchange for durable topology and restart validation.
- We accept `BasicNode` carrying only the peer address in exchange for LS02a storage compatibility.

## Design red-flag review

The design avoids a shallow public module. Five application operations hide leader discovery, retries, membership, manifests, and peer routing.

Topology knowledge has one owner, the manifest and peer-directory module. Storage does not parse endpoints. The client sees only public leader hints.

Modules follow owned knowledge rather than execution order. `manifest.rs` owns every manifest transition. `bootstrap.rs` owns the complete distributed bootstrap operation.

The proposed methods add policy or type conversion. There is no public wrapper that only forwards the same arguments to another layer.

## Open questions and risks

- Do the proposed 750 to 1,500 millisecond election bounds meet B2 on every supported local platform without causing avoidable elections under Clippy and release-build load?
- Does tonic's encoded size for one maximum 8 MiB publish remain below the proposed 10 MiB peer limit with the concrete prost schema?
- Should the local fault profile expose `--peer-route` as a repeated CLI option or load the same typed map from a verifier-owned JSON file?
- Does alpha.34's blocking `add_learner` return only after the learner has applied the bootstrap commands, or only after log matching? Finalization still checks local apply, but the implementation test should pin this behavior.
- Can a coordinator safely resume `BootstrapGroup` through a new group leader after the original bootstrap owner has joined the final voter set? The compile and live spike must prove the exact forwarding path.

## Self-grade

| Criterion | Grade | Reason |
| --- | --- | --- |
| Exact Openraft 0.10 compatibility | 5/5 | The design uses alpha.34 `RaftNetworkV2`, `RaftNetworkFactory`, `add_learner`, `change_membership`, `ensure_linearizable`, metrics, streamed state-machine assumptions, and `full_snapshot` signature. It excludes 0.9 APIs. |
| Illegal routing and bootstrap states prevented by types and boundaries | 5/5 | Typed routes, typed handles, exact manifest phases, exact-plan retries, target validation, and uniform-membership finalization prevent the named invalid states. |
| Minimal maintainable interface | 4/5 | The application interface stays small. The distributed bootstrap needs three internal management RPCs and fault-route overrides. Removing either would weaken determinism or recovery. |
| Complete E02 to E09 local verification within declared scope | 4.5/5 | E02 to E07 and E09 have release-binary scenarios. E08 is explicitly unsupported because snapshot-after-purge belongs to LS06. Independent-host claims are blocked rather than inferred from local runs. |
| No hidden fixed leader or proxy shortcut | 5/5 | Bootstrap owners only establish initial groups. Openraft elects all later leaders. Followers return hints, and clients connect directly to leaders. |

Total: **23.5/25**.

## First implementation step

Add compile-only type sketches and prost conversions for the exact alpha.34 peer requests and responses, then prove one control-group AppendEntries and vote round trip before changing bootstrap.
