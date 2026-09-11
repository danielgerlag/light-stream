# LS02b implementation design

## Caller behavior

Three processes start with stable node IDs, public endpoints, peer endpoints, and separate data directories. Startup does not create group storage or initialize Raft.

One bootstrap request names either the local standalone node or exactly three nodes. For three nodes, the contacted seed prepares the two empty peers, initializes control group `1` and data group `2` with itself as the sole voter, adds the other nodes as blocking learners, waits for exact replication, and changes both memberships to voters `{1, 2, 3}`.

Application requests may enter through any public endpoint. A nonleader returns a typed data-group leader hint. The client connects to that public endpoint and retries within one decreasing deadline. The server does not proxy the request.

Fetch and receipt reads run `ensure_linearizable(ReadPolicy::ReadIndex)` on the data leader before reading RocksDB.

## Data shape

The implementation uses one typed topology and one runtime state machine.

```rust
pub struct NodeDescriptor {
    node_id: NodeId,
    public_uri: PublicUri,
    peer_uri: PeerUri,
}

pub enum ClusterTopology {
    Standalone(NodeDescriptor),
    ThreeVoter(ThreeVoterTopology),
}

pub struct ThreeVoterTopology {
    seed: NodeId,
    nodes: [NodeDescriptor; 3],
}

pub enum NodeRuntime {
    Pristine,
    Joining(ActiveGroups),
    Forming(ActiveGroups),
    Active(ActiveGroups),
    Faulted(String),
}

pub struct ActiveGroups {
    manifest: NodeManifestV2,
    control: ControlHandle,
    data: DataHandle,
}
```

`ThreeVoterTopology::try_new` sorts nodes by ID and rejects zero IDs, duplicate IDs, duplicate endpoints, a missing seed, and a local descriptor that conflicts with process configuration.

The control and data handles remain distinct. Both can use the existing `GroupCommand` and persisted storage format. Public application methods expose only the data handle. Bootstrap code names both handles explicitly.

## Durable manifest

`cluster.json` becomes a versioned node manifest.

```rust
pub enum PersistedNodeState {
    Joining(FormationSpec),
    Forming(FormationSpec),
    Active(FormationSpec),
}

pub struct NodeManifestV2 {
    format_version: u32,
    local_node_id: NodeId,
    formation: FormationSpec,
    state: PersistedNodeState,
}
```

The manifest stores the cluster identity, bootstrap stream identity, fixed group IDs, the seed ID, and every node descriptor. It does not store an asserted leader or transient catch-up step.

The loader accepts the LS02a version `1` manifest as an active standalone cluster. It does not rewrite group storage.

Startup rules are strict.

1. No manifest and no group directories means `Pristine`.
2. No manifest with group data is a startup error.
3. `Joining` and `Forming` open existing local stores without calling `initialize`.
4. `Active` opens existing stores, waits for recovery, and verifies the stored cluster and group identities.
5. A configured node ID or advertised endpoint that conflicts with the manifest is a startup error.

## Peer protocol

The existing peer listener gains bounded unary RPCs for append entries, vote, pre-vote, and lifecycle preparation. The envelope carries:

- protocol version;
- codec version;
- cluster ID;
- group ID;
- sender node ID;
- target node ID;
- encoded Openraft request.

The peer codec uses `serde_json` because Openraft already derives serde for the required request and response types. The peer request and response limit is finite and large enough for the current maximum publish batch. A boundary test encodes the maximum legal batch and proves the configured limit.

The receiver validates the envelope before decoding the Openraft body. It rejects a wrong protocol version, codec version, cluster, group, sender, target, or message size. A joining node accepts only the cluster and topology stored in its durable manifest.

The network adapter implements the exact `openraft = 0.10.0-alpha.34` traits:

```rust
impl<C> RaftNetworkFactory<C> for TonicNetworkFactory<C> {
    type Network = TonicRaftNetwork<C>;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network;
    async fn new_heartbeat_client(&mut self, target: u64, node: &BasicNode) -> Self::Network;
    async fn new_snapshot_client(&mut self, target: u64, node: &BasicNode) -> Self::Network;
}

impl<C> RaftNetworkV2<C> for TonicRaftNetwork<C> {
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
}
```

Factory methods cannot fail. They create a fail-closed client if the Openraft node metadata conflicts with the durable peer directory.

Each tonic request uses `RPCOption::soft_ttl()` as its deadline. A timeout maps to `RPCError::Timeout`. Connection refusal maps to `RPCError::Unreachable`. A transient HTTP/2 failure maps to `RPCError::Network`. The adapter never permanently removes a peer.

## Snapshot choice

LS02b sets `SnapshotPolicy::Never` for new three-voter groups and retains their logs. `full_snapshot` returns an explicit unsupported transport error. Ordinary follower recovery uses append entries while the log suffix exists.

The LS02a storage snapshot implementation and tests remain. LS02b does not claim remote snapshot catch-up or catch-up after purge. E08 records `UNSUPPORTED_LS06`.

## Bootstrap and join

The public bootstrap request is idempotent.

1. Parse a standalone or exact three-voter topology.
2. Reject a conflicting existing manifest.
3. Persist `Forming` on the seed before creating local stores.
4. Send `PrepareJoin` to each remote node.
5. Each remote node validates its local descriptor, persists `Joining`, opens empty group stores, and starts peer Raft handlers without calling `initialize`.
6. The seed initializes each pristine local group with only itself.
7. The seed commits the existing bootstrap command in each group.
8. The seed calls `add_learner(id, BasicNode::new(peer_uri), true)` for each remote node.
9. The seed waits until leader replication metrics show each learner exactly matches the leader's last log. The code does not assume that blocking learner admission means exact equality.
10. The seed calls `change_membership({1, 2, 3}, false)` for each group.
11. Every node becomes active only after effective and committed memberships are equal, uniform, contain exactly the intended voters, and both state machines contain the bootstrap identity.

A retry observes Openraft state before every action. It never calls `initialize` on a nonpristine group. An activation message asks a node to reconcile. It cannot force the node active.

## Leader routing and errors

Openraft errors map by enum variant. No code searches error strings.

```rust
pub enum RequestOutcome {
    DefiniteNoCommit,
    AmbiguousCommit,
    NotApplicable,
}

pub enum DomainError {
    NotLeader {
        group: ConsensusGroup,
        leader: Option<LeaderHint>,
    },
    QuorumUnavailable {
        group: ConsensusGroup,
        outcome: RequestOutcome,
    },
    ClusterForming,
    BootstrapConflict,
    // Existing variants remain.
}
```

`ForwardToLeader` becomes a public leader hint only when the node ID and peer address agree with the durable topology. The server supplies the matching public endpoint from the manifest.

A follower rejection is definite for that attempt. A write timeout is ambiguous. The client retries only the same producer request identity and bytes. If its overall deadline expires, it returns the ambiguous request identity for receipt resolution.

## Diagnostics

One read-only public status RPC reports:

- local node ID and lifecycle state;
- peer descriptors;
- group kind and group ID;
- local Raft role;
- current leader;
- effective voters and learners;
- committed voters and learners;
- last log index;
- local and cluster committed indexes;
- last applied index;
- leader-side replication progress;
- snapshot and purge indexes;
- unsupported claims.

Diagnostics never initialize, elect, change membership, append, purge, or install a snapshot.

## Verification

The verifier starts three release processes and uses stable ports, separate stores, and owned TCP proxies. It polls diagnostics instead of sleeping for progress.

It proves:

- E02 with exact three-voter membership, publish through a follower, fetch through another endpoint, and byte comparison against random external data;
- E03 by killing the diagnosed data leader and requiring a different elected leader within the declared deadline;
- E04 by isolating the old leader, disabling client retries for direct endpoint assertions, requiring no successful write or fresh read, healing, and waiting for catch-up;
- E05 by stopping two voters and requiring zero successful acknowledgements;
- E06 by delaying one follower while the majority progresses, then waiting for exact catch-up;
- E07 by stopping and restarting one follower while the leader retains the suffix;
- E09 by letting a proxy observe a successful upstream response, dropping that response, retrying through another endpoint, checking the original receipt, rejecting conflicting bytes, and checking the receipt after full restart.

E08 records snapshot catch-up after purge as unsupported until LS06. Independent-host results remain blocked. Secured mode remains unsupported until LS08.

## Module ownership

- `light-stream-core` owns topology-independent public group, leader hint, and request outcome types.
- `light-stream-storage` keeps the current format, payload ownership, receipts, and snapshot implementation.
- `light-stream-server::manifest` owns topology parsing and durable lifecycle state.
- `light-stream-server::peer` owns the Openraft codec, network factory, and inbound dispatch.
- `light-stream-server::runtime` owns the two group handles, formation, startup reconciliation, and application calls.
- `light-stream-client` owns the decreasing retry deadline and leader cache.
- `scripts/verify.py` owns process lifecycle, proxies, faults, ledgers, and evidence.

## Arena decision

Candidate A had the best storage compatibility and direct-routing design. Its two bootstrap owners were rejected because the requirement says the seed initializes both groups and because different group leaders are not required.

Candidate B contributed activation proof derived from Openraft facts. Its custom Raft node type and split persisted commands were rejected because they increase migration risk without helping LS02b.

Candidate C contributed explicit no-retry fault checks and restart-all receipt verification. Its topology digest and separate generated peer services were rejected as more code than the route envelope needs.

The synthesis keeps `BasicNode`, `GroupCommand`, and storage format `1`. It adds only the durable node topology, peer transport, membership flow, typed routing errors, diagnostics, client retry, and verification needed for LS02b.

## Verification status

The design matches the exact alpha.34 network, membership, read, and metrics APIs found in the local cargo registry. Implementation must compile the peer adapter before the runtime switches from `NoRemoteNetworkFactory`.
