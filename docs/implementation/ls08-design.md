# LS08 secured runtime design

## Decision

The control group owns the cluster's non-secret security policy. Local files hold raw bearer tokens, TLS private keys, and certificate chains. Raft stores token verifier digests, grants, active peer certificate fingerprints, policy revisions, and revocation revisions.

Each public request enters through one `SecurityRuntime` call and receives a typed `Permit<Action>`. Each peer request derives its node identity from mutual TLS before Light Stream decodes the peer envelope. Outbound peer connections require the exact target node URI and an active certificate fingerprint. A short, leader-confirmed policy lease bounds stale public authorization to five seconds.

This design uses Candidate A as the base. Candidate B supplied typed permits and control-group-only recovery traffic. Candidate C supplied immutable `StreamId` scopes and raw plus encoded secret-canary scans.

## Operator usage

Local development remains explicit:

```sh
light-streamd \
  --security-mode local-insecure \
  --public-listen 127.0.0.1:7101 \
  --peer-listen 127.0.0.1:7201
```

Secured mode reads one path-only configuration:

```sh
light-streamd \
  --security-mode secured \
  --security-config /etc/light-stream/node-1/security.json \
  --public-listen 0.0.0.0:7101 \
  --peer-listen 0.0.0.0:7201
```

```json
{
  "version": 1,
  "cluster_id": "018f3f7e-5b3b-7c11-98f7-b65ac15f6501",
  "public_tls": {
    "certificate_chain_file": "/etc/light-stream/public-cert.pem",
    "private_key_file": "/run/light-stream/public-key.pem"
  },
  "peer_tls": {
    "certificate_chain_file": "/etc/light-stream/peer-cert.pem",
    "private_key_file": "/run/light-stream/peer-key.pem",
    "trust_roots_file": "/etc/light-stream/peer-roots.pem"
  },
  "bootstrap_policy_file": "/etc/light-stream/bootstrap-policy.json",
  "maximum_policy_staleness_ms": 5000
}
```

The command line contains paths only. Secret files must be regular files owned by the service user with mode `0600`. A failed security load stops startup before either listener serves.

The client reads its trust root and token from protected files:

```sh
light-streamctl \
  --endpoint https://node-1.example.test:7101 \
  --security-config /run/light-stream/client.json \
  health
```

```rust
let client = Client::connect(ClientConfig {
    endpoints,
    deadline: Duration::from_secs(5),
    retry: true,
    security: ClientSecurity::secured_from_file(
        "/run/light-stream/client.json",
    )?,
}).await?;
```

An `https` endpoint without secured client configuration fails. Secured configuration with an `http` endpoint also fails.

## Policy types

`light-stream-core::security` owns the stable, non-secret policy model:

```rust
pub struct PolicyRevision(NonZeroU64);
pub struct RevocationRevision(u64);
pub struct CredentialId(String);
pub struct CredentialGeneration(NonZeroU64);

pub struct CredentialRef {
    id: CredentialId,
    generation: CredentialGeneration,
}

pub struct TokenVerifierDigest([u8; 32]);
pub struct CertificateFingerprint([u8; 32]);

pub struct TokenVerifier {
    credential: CredentialRef,
    principal: PrincipalId,
    digest: TokenVerifierDigest,
    status: CredentialStatus,
}

pub struct PeerCertificateBinding {
    cluster: ClusterId,
    node: NodeId,
    generation: CredentialGeneration,
    fingerprint: CertificateFingerprint,
    status: CredentialStatus,
}

pub enum CredentialStatus {
    Active,
    Revoked { at: RevocationRevision },
}

pub enum Permission {
    ClusterObserve,
    ClusterBootstrap,
    StreamDiscover,
    StreamCreate,
    StreamDescribe,
    StreamDelete,
    RouteResolve,
    Publish,
    Fetch,
    ReceiptRead,
    BookmarkRead,
    BookmarkManage,
    RetentionRead,
    RetentionManage,
    ReplayRead,
    ReplayManage,
    CheckpointRead,
    CheckpointManage,
    SnapshotManage,
    ClusterAdmin,
    SecurityObserve,
    SecurityAdmin,
}

pub enum ResourceScope {
    Cluster(ClusterId),
    AllStreams(ClusterId),
    Stream {
        cluster: ClusterId,
        stream: StreamId,
    },
}

pub struct Grant {
    permission: Permission,
    scope: ResourceScope,
}

pub struct SecurityPolicy {
    cluster: ClusterId,
    revision: PolicyRevision,
    revocation_revision: RevocationRevision,
    grants: BTreeMap<PrincipalId, BTreeSet<Grant>>,
    token_verifiers: BTreeMap<CredentialRef, TokenVerifier>,
    peer_certificates: BTreeMap<NodeId, BTreeSet<PeerCertificateBinding>>,
}
```

Grants use immutable `StreamId`. A recreated stream name does not inherit an old grant.

Token verifier digests are SHA-256 digests of at least 256 random token bits. Token comparison uses `subtle::ConstantTimeEq`. Light Stream does not accept passwords and does not implement custom cryptography.

## Policy mutation

The control group stores `SecurityPolicy` and applies compare-and-set mutations:

```rust
pub struct SecurityMutation {
    request: MutationRequestId,
    expected_revision: PolicyRevision,
    change: SecurityChange,
}

pub enum SecurityChange {
    ReplacePrincipalGrants {
        principal: PrincipalId,
        grants: BTreeSet<Grant>,
    },
    AddTokenGeneration {
        verifier: TokenVerifier,
    },
    RevokeTokenGeneration {
        credential: CredentialRef,
    },
    AddPeerCertificate {
        binding: PeerCertificateBinding,
    },
    RevokePeerCertificate {
        node: NodeId,
        generation: CredentialGeneration,
    },
}

pub enum GroupCommand {
    ApplySecurityMutation {
        mutation: SecurityMutation,
    },
}
```

The state machine checks the expected revision, monotonic credential generations, cluster identity, peer binding identity, and the final active `SecurityAdmin`. An exact retry returns the original result through the existing mutation receipt mechanism.

Raw tokens and private keys cannot inhabit these types. They cannot enter Raft or a snapshot through this command.

## Public request boundary

`SecurityRuntime` is the only component that creates authorization permits:

```rust
pub struct SecurityRuntime {
    mode: SecurityMode,
    policy: PolicyCache,
    tls: TlsMaterialStore,
    events: SecurityEventSink,
}

pub struct AuthenticatedPrincipal {
    principal: PrincipalId,
    credential: CredentialRef,
    policy_revision: PolicyRevision,
    revocation_revision: RevocationRevision,
}

pub struct Permit<A> {
    principal: Option<AuthenticatedPrincipal>,
    resource: AuthorizedResource,
    action: PhantomData<A>,
}

impl SecurityRuntime {
    pub async fn load_before_bind(
        config: RuntimeSecurityConfig,
        node: NodeId,
        durable: DurableSecurityState,
    ) -> Result<Arc<Self>, StartupError>;

    pub async fn admit_public<C: PublicCall>(
        &self,
        request: &Request<C::Request>,
    ) -> Result<Permit<C::Action>, Status>;

    pub async fn admit_peer<T>(
        &self,
        request: &Request<T>,
        claimed: ClaimedPeerIdentity,
    ) -> Result<AuthenticatedPeer, Status>;

    pub async fn reload_local_material(
        &self,
    ) -> Result<TlsGeneration, SecurityReloadError>;
}
```

Every secured handler calls `admit_public` before it reads cached state or calls `ClusterManager`. Runtime methods that expose protected data or change state accept the action-specific permit.

```rust
pub async fn receipt(
    &self,
    permit: &Permit<action::ReceiptRead>,
    cluster: ClusterId,
    partition: PartitionKey,
    request: &ProducerRequestId,
) -> Result<PublishReceipt, DomainError>;
```

The permit boundary prevents a new handler from bypassing authorization by calling a protected runtime method directly.

## Route coverage

A sealed `PublicCall` implementation defines the permission and resource for each protobuf method. A descriptor-set test compares the registry with every `LightStream` RPC. A new RPC fails the test until it has a policy.

Authorization runs before any receipt, bookmark, lease, checkpoint, diagnostic, snapshot, or administration record is read.

| RPC | Permission | Resource |
| --- | --- | --- |
| `Health`, `Capabilities` | `ClusterObserve` | Cluster |
| `Bootstrap` | `ClusterBootstrap` | Requested cluster |
| `CreateStream` | `StreamCreate` | Cluster |
| `DescribeStream`, `ListStreams` | `StreamDescribe` | Stream or all streams |
| `DeleteStream` | `StreamDelete` | Stream |
| `ResolveRoute` | `RouteResolve` | Stream |
| `Publish`, `CommitPublish` | `Publish` | Stream |
| `Fetch` | `Fetch` | Stream |
| `GetReceipt` | `ReceiptRead` | Stream and request owner |
| Bookmark writes | `BookmarkManage` | Stream |
| Bookmark reads | `BookmarkRead` | Stream |
| `AdvanceRetention` | `RetentionManage` | Stream |
| `GetRetentionStatus` | `RetentionRead` | Stream |
| Replay writes | `ReplayManage` | Stream and request owner |
| Replay reads | `ReplayRead` | Stream and lease owner |
| `GetCheckpoint` | `CheckpointRead` | Stream |
| `CompareAndSetCheckpoint` | `CheckpointManage` | Stream and request owner |
| `Diagnostics` | `ClusterObserve` | Cluster |
| `SnapshotGroup` | `SnapshotManage` | Cluster |
| Administration writes and reads | `ClusterAdmin` | Cluster |
| Security policy read | `SecurityObserve` | Cluster |
| Security policy mutation | `SecurityAdmin` | Cluster |

Name-only stream requests require `StreamDiscover` before catalog resolution. The resolved immutable `StreamId` must then match the operation grant.

## Principal binding

Wire principal fields remain for `local-insecure` compatibility.

```rust
pub fn bind_producer(
    permit: &Permit<action::Publish>,
    wire: v1::ProducerRequestId,
) -> Result<ProducerRequestId, Status>;

pub fn bind_mutation<A>(
    permit: &Permit<A>,
    wire: v1::MutationRequestId,
) -> Result<MutationRequestId, Status>;
```

In secured mode, the server derives the principal from the token. An empty body principal is accepted. An equal body principal is accepted for compatibility. A different principal fails before any receipt lookup or mutation.

The credential ID and generation are absent from `ProducerRequestId` and `MutationRequestId`. Token rotation for the same principal preserves durable receipts, leases, and checkpoints.

## Public TLS

The public listener uses server-authenticated TLS. Startup verifies:

- the certificate and private key match;
- the certificate is valid for server authentication;
- the advertised host appears in a DNS or IP subject alternative name;
- the trust and key files satisfy the file-permission rules.

Clients verify the configured trust root and the endpoint hostname. Public clients use bearer tokens, not client certificates.

## Peer mutual TLS

Each peer certificate has client and server authentication usage. It contains exactly one URI subject alternative name:

```text
spiffe://light-stream/cluster/<cluster-id>/node/<node-id>
```

Inbound peer processing follows this order:

1. Rustls verifies the trust chain and client usage.
2. Light Stream parses the URI subject alternative name.
3. The certificate cluster must equal the durable cluster.
4. The certificate node must equal the envelope sender.
5. The envelope target must equal the local node.
6. The fingerprint must be active for that node.
7. The node must exist in `ClusterTopology::authorized_nodes`.
8. Only then may Light Stream decode the Raft or snapshot payload.

Outbound peer TLS verifies both the endpoint hostname and the peer URI identity. A `PeerRoutes` override changes the dial address but never the expected node identity.

A bearer token cannot authenticate to the peer listener. A peer certificate grants no public permission.

## Cold restart

A secured cluster needs peer traffic to recover a control quorum before it can confirm policy freshness. A strict fresh-lease requirement would deadlock restart.

For at most five seconds after a successful local material load, the peer listener accepts only vote, pre-vote, append, and snapshot traffic that passes all of these checks:

- mutual TLS is valid;
- the certificate identity matches the peer envelope;
- the peer exists in the locally durable topology;
- the certificate fingerprint is active in the locally applied policy;
- the group is locally authorized.

The recovery exception permits only control-group append, vote, pre-vote, and snapshot traffic that matches the locally applied policy and durable topology. It lasts for at most one additional policy-staleness interval. It permits no public work, lifecycle RPC, administration RPC, data-group traffic, or new application proposal. This narrow exception lets an isolated follower apply the policy revision that ends its stale state.

## Policy freshness

Each node keeps a leader-confirmed `PolicyLease`:

```rust
pub struct PolicyLease {
    policy: SecurityPolicy,
    valid_until: Instant,
}
```

The refresh worker runs once per second. The control leader renews its lease after a linearizable policy read. Followers renew their lease when they accept control-group `AppendEntries` and can read the applied policy. The lease expires after at most five seconds. Once expired, public and normal peer work returns `security_policy_stale`.

Each RPC checks revocation against the current lease. A policy update therefore reaches public authorization when the node renews that lease.

## Rotation

Token rotation uses additive generations:

1. Add the new token verifier for the same principal.
2. Move clients to the new protected token file.
3. Revoke the old generation.
4. Confirm that all nodes reached the new revocation revision.

Both generations map to one principal. Durable producer and mutation IDs do not change.

Peer certificate rotation follows the same overlap:

1. Add the new fingerprint for the existing node ID.
2. Confirm that the target node applied the new policy revision.
3. Stop the target node.
4. Replace its certificate and key files.
5. Restart the node and validate peer traffic.
6. Revoke the old fingerprint.

LS08 uses a rolling restart. It does not hot-reload TLS material. An offline node must first restart with its currently active certificate and catch up before the operator activates a new certificate generation for that node.

## Bootstrap

A secured bootstrap policy must:

- match the requested cluster ID;
- bind each formation node to an active peer certificate;
- contain at least one active token generation;
- grant one principal `ClusterBootstrap`, `ClusterAdmin`, and `SecurityAdmin`;
- have the same digest on every forming node.

Only an unbootstrapped node may use the local bootstrap policy. `BootstrapControl` commits the normalized policy alongside topology and catalog initialization. After the committed policy appears, the bootstrap file cannot authorize public work.

## Manifest migration

Manifest version 5 stores only the selected security profile and non-secret bootstrap policy digest:

```rust
pub enum DurableSecurityProfile {
    LocalInsecure,
    Secured {
        bootstrap_policy_digest: [u8; 32],
        minimum_policy_revision: PolicyRevision,
    },
}
```

Version 4 continues to open in `local-insecure`. `security activate-transport` commits the HTTPS topology and initial policy in one control-group write. The command uses a `MutationRequestId`, so an exact retry returns the original result.

After the command succeeds, the operator stops all voters and restarts them with `--security-mode secured`. Each node reads the committed topology and policy before it changes its local manifest. The transition rejects changed node IDs, changed desired voters, stale topology revisions, invalid HTTPS endpoints, or a policy that lacks an active peer binding for any voter.

Mixed plaintext and mutual-TLS peers are never accepted. After the secured policy commits, returning to insecure mode requires a future authenticated transition. Startup flags cannot downgrade the cluster.

## Secrets and events

Secret-bearing types implement neither `Serialize` nor content-revealing `Debug` or `Display`. Raw tokens and private keys use zeroizing wrappers.

Security events contain the principal, action, redacted target, allow or deny result, policy revision, revocation revision, and durable request identity. They omit tokens, authorization headers, token digests, certificate bytes, private-key paths, payloads, and metadata maps.

The verifier generates synthetic secrets at runtime outside the repository and data directory. It scans logs, command capture, manifests, RocksDB values, snapshots, source snapshots, and JSON evidence for raw and encoded canaries.

## Module ownership

| Module | Responsibility |
| --- | --- |
| `light-stream-core/src/security.rs` | Policy, permissions, grants, revisions, verifier digests, certificate bindings, and redacted summaries |
| `light-stream-storage/src/lib.rs` | Control policy apply, snapshots, and committed reads |
| `light-stream-server/src/security.rs` | Protected file loading, TLS configuration, token and peer authentication, typed permits, policy leases, and security events |
| `light-stream-server/src/service.rs` | Admit the request before conversion and runtime access |
| `light-stream-server/src/peer.rs` | Admit the peer before payload decode |
| `light-stream-server/src/runtime.rs` | Policy reads, mutations, lease renewal, and secured transport activation |
| `light-stream-server/src/manifest.rs` | Version 5 durable security profile |
| `light-stream-client/src/lib.rs` | Client trust roots, token files, TLS, and metadata injection |
| `light-stream-cli/src/main.rs` | Security config paths and policy administration |

## Synthesis decision

Candidate A is the base because it gives the control group one policy authority and covers every public route. Candidate B supplied typed permits and the control-group-only recovery rule. Candidate C supplied immutable stream scopes and raw plus encoded canary scans.

The design rejects local policy bundles as cluster authority. It also rejects scheduled activation, detached operator proofs, public-client certificates, and mixed-mode rolling migration.

## Tradeoffs

- We accept replicated token verifier digests and certificate fingerprints in exchange for one policy authority on every leader.
- We accept secured unavailability after five seconds without a leader-confirmed policy lease in exchange for a real revocation bound.
- We accept a coordinated first transition to secured peer transport in exchange for no plaintext downgrade path.
- We accept explicit credential overlap in exchange for deterministic rotation without clock-driven policy changes.
- We accept typed permits on protected runtime methods in exchange for compile-time visibility of missing authorization.

## First implementation unit

Implement the pure policy model and token authentication boundary first. Add policy types, permission evaluation, token verification, principal binding, redacted summaries, and serialization tests that prove raw tokens and private keys cannot enter the committed policy.
