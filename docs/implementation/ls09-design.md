# LS09 package and operations design

## Decision

LS09 adds four contracts:

1. A deterministic release archive and a single-engine Linux image.
2. One runtime lifecycle model for liveness, write readiness, metrics, and drain.
3. A broker-managed logical export with a durable cluster-wide mutation fence.
4. An offline restore that publishes a fresh standalone cluster with one directory rename.

The export format contains selected retained records and bookmark state. It does not contain RocksDB keys, Raft logs, topology, credentials, receipts, checkpoints, replay leases, or security policy.

Candidate 1 is the arena base. The cross-judge scored it 29 out of 30. Candidate 3 supplied derived liveness and bounded readiness probes. Candidate 2 reinforced the separate `light-stream-export` crate.

The synthesis rejects two arena recommendations. Version 1 fences every configured group instead of adding route-aware partial admission. It also keeps the current node manifest versions. A manifest version 6 would force an unrelated standalone and three-voter redesign.

## Operator workflows

### Build a release archive

```sh
python3 scripts/package_release.py \
  --target aarch64-apple-darwin \
  --revision "$(git rev-parse HEAD)" \
  --output dist
```

The script writes one archive and one manifest:

```text
dist/
  light-stream-0.1.0-aarch64-apple-darwin.tar.gz
  light-stream-0.1.0-aarch64-apple-darwin.release.json
```

The archive contains:

```text
light-stream-0.1.0-aarch64-apple-darwin/
  bin/light-streamd
  bin/light-streamctl
  config/server.example.json
  release.json
  SHA256SUMS
```

`release.json` records the package version, source revision, Rust target, binary digests, dynamic libraries, and supported protocol and storage versions. The archive contains no source tree, compiler, Cargo files, testkit, token, private key, or test certificate.

Version 1 release metadata is not a signature or publisher attestation. It proves file identity after the operator obtains the package through a trusted channel.

### Run the Linux image

```sh
docker build \
  --file packaging/Containerfile \
  --tag light-stream:local \
  .

docker run --rm \
  --read-only \
  --volume light-stream-data:/var/lib/light-stream \
  --publish 7101:7101 \
  --publish 7201:7201 \
  --publish 9102:9102 \
  light-stream:local \
  --operations-listen 0.0.0.0:9102
```

The runtime image contains only `light-streamd`, its declared shared libraries, and CA roots. It runs as a non-root user. Secured deployments mount their path-only configuration and private files at runtime.

### Configure a node

`light-streamd` accepts either the existing flags or one versioned JSON file. Mixing a configuration file with server-setting flags is an error. Clap fields become optional, and `ServerConfig` applies defaults only after it selects the source.

```sh
light-streamd --config /etc/light-stream/server.json
```

```json
{
  "version": 1,
  "node_id": 1,
  "data_dir": "/var/lib/light-stream",
  "public_listen": "0.0.0.0:7101",
  "peer_listen": "0.0.0.0:7201",
  "operations_listen": "127.0.0.1:9102",
  "security_mode": "secured",
  "security_config": "/run/secrets/light-stream/server.json",
  "shutdown_grace_ms": 30000,
  "max_export_bytes": 107374182400
}
```

The file may contain paths to private files. It may not contain token bytes, private keys, certificates, or authorization headers.

### Probe a node

```sh
curl --fail http://127.0.0.1:9102/livez
curl --fail http://127.0.0.1:9102/readyz
curl --fail http://127.0.0.1:9102/metrics
```

`/livez` reports whether the process can serve its operations listener. It remains live during quorum loss, export, and drain.

`/readyz` reports cluster-wide write readiness from this endpoint. It returns HTTP 503 when any active group lacks recent write authority, when secured policy is stale, when export holds the mutation fence, or when drain has started.

The operations listener is unauthenticated and redacted. It defaults to loopback. An explicit non-loopback bind exposes only closed reason codes, group IDs, node IDs, counters, and build identity. It never exposes application identities, endpoints, paths, or security material. Read-only load balancers that must retain nodes during export use `/livez`; write-routing load balancers use `/readyz`.

The existing authenticated `health` RPC adds `live`, `write_ready`, lifecycle generation, and readiness reasons. Its existing `ready` field keeps its LS08 meaning. It remains true while the public server is available, including quorum loss and export.

### Stop a node

The first `SIGINT` or `SIGTERM` starts one drain:

1. Change the lifecycle to `Draining`.
2. Close mutation admission.
3. Reject new mutations with `shutting_down` and `definite_no_commit`.
4. Stop maintenance tasks from proposing new work.
5. Keep public reads and Raft peer traffic available.
6. Wait for accepted mutations until `shutdown_grace_ms`.
7. Stop public acceptance, peer acceptance, Raft groups, storage, and the operations listener.

A repeated signal observes the same drain. A missed deadline exits nonzero and reports unresolved accepted work. It never reports a clean shutdown.

`ActiveCluster` retains every maintenance task handle. Drain closes public mutation admission first. It then joins maintenance tasks and waits for accepted mutations within the same deadline. Submitted Raft writes and bootstrap retain owned mutation permits after an RPC timeout or cancellation. `OperationalProbe` is outside the mutation gate, but its task stops before Raft shutdown.

### Export selected streams

```sh
light-streamctl \
  --endpoint https://node-1.example.test:7101 \
  --security-config /run/light-stream/client.json \
  export create \
  --cluster-id 018f3f7e-5b3b-7c11-98f7-b65ac15f6501 \
  --request-id 018f3f7e-5b3b-7c11-98f7-b65ac15fa001 \
  --stream-id 018f3f7e-5b3b-7c11-98f7-b65ac15f6502 \
  --output ./orders.lsexport
```

The client hides prepare, materialization, resumable download, digest verification, local file publication, and fence release. It writes `orders.lsexport.part` with mode `0600`, syncs it, verifies the complete artifact, renames it, syncs the parent directory, and then confirms completion.

Export version 1 pauses every logical cluster mutation. Reads and Raft recovery continue. The broad pause is deliberate. It gives the first format one mutation gate and one cut rule.

A retry with the same request ID and stream set resumes the durable operation. Reusing the request ID with another stream set returns `export_conflict`.

### Restore an export

```sh
light-streamd restore \
  --config /etc/light-stream/restored-server.json \
  --input ./orders.lsexport \
  --expected-sha256 SHA256 \
  --expected-source-cluster-id 018f3f7e-5b3b-7c11-98f7-b65ac15f6501 \
  --target-cluster-id 018f3f7e-5b3b-7c11-98f7-b65ac15fb001
```

Restore version 1 creates a local-insecure standalone cluster. The target configuration supplies the node ID, data directory, advertised public and peer URIs, group pool, catalog limits, and RocksDB memory budgets. The destination must not exist. The target cluster ID must differ from the source cluster ID.

The restored server keeps the existing local-insecure loopback rules. Version 1 does not restore a secured or three-voter cluster. Secured and replicated restore remain deferred until Light Stream has an explicit standalone-to-cluster formation path.

Restore preserves stream IDs, group IDs, partition IDs, bookmark IDs, offsets, publication sequences, and names. It rewrites cluster-scoped values to the target cluster ID. The result states that receipts, producer sessions, checkpoints, replay leases, topology, and security state were not restored.

## Runtime lifecycle

One `LifecycleController` owns lifecycle and mutation admission:

```rust
pub enum NodePhase {
    Starting { stage: StartupStage },
    Running,
    Draining { drain: DrainId, deadline_unix_ms: u64 },
    Stopping { outcome: DrainOutcome },
    Failed { reason: LifecycleFailure },
}

pub struct OperationalState {
    pub generation: u64,
    pub phase: NodePhase,
    pub write_readiness: WriteReadiness,
}

pub enum WriteReadiness {
    Ready,
    NotReady { reasons: Vec<ReadinessReason> },
}

pub enum ReadinessReason {
    Starting,
    NotBootstrapped,
    Forming,
    Retired,
    GroupLeaderUnknown { group: GroupId },
    GroupAuthorityStale { group: GroupId },
    ProbeUnsupported { group: GroupId },
    SecurityPolicyStale,
    ExportInProgress { export: ExportId },
    Draining,
    StorageFailure,
}

pub struct MutationGate {
    state: Mutex<MutationGateState>,
    drained: Notify,
}

pub struct MutationPermit {
    gate: Arc<MutationGate>,
}
```

`live` derives from `NodePhase`. The code does not store another liveness boolean. `MutationGate` owns the admission mode and the accepted mutation count. The lifecycle does not duplicate those fields.

Every public mutation acquires a permit after authentication and boundary validation. It acquires the permit before route lookup, queue admission, or a Raft proposal. Maintenance writers that change exported state use the same gate. Reads, operational proofs, security changes, topology changes, administration recovery, and peer Raft traffic do not.

Valid lifecycle transitions are:

```text
Starting -> Running
Starting -> Failed
Running -> Draining
Running -> Failed
Draining -> Stopping
Draining -> Failed
```

No transition returns to `Running` after drain starts.

## Write readiness

The readiness worker runs at a fixed interval with at most four concurrent probes. Each remote probe has a 100 ms deadline. The complete sample has a 600 ms deadline. When the complete sample expires, readiness fails closed with stale control-group authority.

A leader proves write authority with the existing operational-proof rule. A follower asks the current leader through one additive authenticated `ProbeWriteAuthority` peer RPC. The RPC reports an existing proof only after it matches the current leader, term, applied index, and durable state. It never invokes the write fallback that creates an `OperationalProbe`.

The RPC uses peer protocol and codec version 1. A cluster with mixed LS08 and LS09 binaries can therefore report `probe_unsupported`; LS09 does not claim mixed-version readiness.

The probe does not append a record, repair storage, or change membership.

Write readiness requires:

- lifecycle `Running`;
- mutation admission open;
- an active cluster;
- a current security policy in secured mode;
- no active export;
- a recent write-authority proof for the control group;
- a recent write-authority proof for every data group that owns an active partition;
- no latched storage failure.

Queue saturation does not make readiness false. It is bounded overload and remains visible through queue metrics.

## Bounded metrics

The operations listener builds one bounded `OperationalSnapshot` from atomics, scheduler state, watched Raft metrics, readiness samples, and lifecycle state. It does not scan RocksDB or perform a linearizable read during a scrape.

The first metric set is:

```text
light_stream_live
light_stream_write_ready
light_stream_lifecycle_state{state}
light_stream_readiness_reason{reason,group_id}
light_stream_mutation_admission_open
light_stream_mutations_in_flight
light_stream_group_has_leader{kind,group_id}
light_stream_group_local_commit_index{kind,group_id}
light_stream_group_cluster_commit_index{kind,group_id}
light_stream_group_applied_index{kind,group_id}
light_stream_group_replication_lag_entries{kind,group_id,target_node_id}
light_stream_publish_queue_requests{group_id}
light_stream_publish_queue_records{group_id}
light_stream_publish_queue_resident_bytes{group_id}
light_stream_publish_queue_limit{group_id,resource}
light_stream_publish_rejections_total{group_id,reason}
light_stream_export_state{state}
```

Labels use closed enums, configured group IDs, and configured node IDs. They never contain stream IDs, names, endpoints, filesystem paths, principals, request IDs, payloads, tokens, certificate subjects, or error strings.

## Durable export state

The control group stores one active export and a bounded terminal receipt history:

```rust
pub struct ExportSpec {
    pub request: MutationRequestId,
    pub export: ExportId,
    pub cluster: ClusterId,
    pub streams: NonEmptyBoundedSet<StreamId>,
    pub format: ExportFormatVersion,
    pub deadline: ClockObservation,
}

pub enum ExportLifecycle {
    Preparing { fenced_groups: BTreeMap<GroupId, GroupCut> },
    Frozen { cut: QuiescentCut },
    Materializing { cut: QuiescentCut },
    Available { descriptor: ExportDescriptor },
    Releasing { descriptor: ExportDescriptor },
    Completed { descriptor: ExportDescriptor },
    Aborting { reason: ExportAbortReason },
    Aborted { reason: ExportAbortReason },
}

pub struct QuiescentCut {
    pub control: GroupCut,
    pub data: BTreeMap<GroupId, GroupCut>,
}

pub enum MutationFence {
    Open,
    ExportHeld { export: ExportId, request_digest: [u8; 32] },
}
```

The control group and every configured data group apply the matching fence. The fence orders after earlier included-state mutations and rejects later included-state mutations.

The fence blocks these operations:

- stream create, activation, deletion, and name reuse;
- publish and atomic publish-plus-bookmark;
- partition bookmark create and delete;
- stream bookmark create and delete;
- retention advancement and retention reclaim.

The fence permits operational proofs, export progress, export release, export abort, replay-lease lifecycle, checkpoint updates, security policy changes, topology changes, and administration recovery. These operations do not change version 1 export contents.

Fence, release, abort, and status commands are idempotent.

Export prepare refuses to start while a membership administration operation is active. New membership operations are refused while export is active. A membership operation that was already committed may finish so a failed node cannot strand the export.

Every node runs a reconciler. A node proposes a group fence only when it leads that group. The control leader records `Frozen` only after its local readers have applied every declared fence. If a leader changes, the next leader resumes from durable state.

The control leader materializes the artifact after `Frozen`. It waits until its local copies have applied every cut, then reads one RocksDB snapshot per group. If the leader fails before `Available`, the next leader rebuilds the same canonical bytes.

The fence remains until the client verifies and publishes its local file. This keeps failover rebuild possible. A control leader can rebuild the artifact from current group snapshots because no included state changes after the fence.

`AbortExport` carries a bounded `ClockObservation` and is exempt from the fence. The control leader proposes it after the fixed export deadline. If the control group has no leader, the export remains fenced until quorum returns or an operator submits `export abort`.

## Export format version 1

`light-stream-export` owns the `.lsexport` format. It has no RocksDB, Raft, server, or transport dependency.

The file is uncompressed and deterministic:

```text
prologue
  magic "LSEXPT01"
  format_version u32
  required_features u64

section*
  kind u16
  section_version u16
  item_count u64
  encoded_length u64
  payload_sha256[32]
  payload

manifest
  encoded_length u64
  canonical manifest bytes

trailer
  manifest_offset u64
  artifact_length u64
  artifact_sha256[32]
  magic "LSEXEND1"
```

Integers use big-endian encoding. The artifact digest covers the prologue, sections, and manifest. Each section has its own digest. The decoder rejects trailing bytes.

The decoder checks every length before allocation. Limits cap the artifact, manifest, section, stream, partition, record, marker, and payload counts. The export byte limit also bounds failover re-materialization time and spool disk use. Export and restore use bounded buffers.

The logical sections contain:

- selected active stream descriptors and placements;
- retained records from each retention floor through the cut tail;
- exact next offsets and retention floors;
- active partition bookmarks;
- deleted partition bookmark tombstones;
- active stream bookmarks;
- deleted stream bookmark tombstones;
- publication counters and ceilings.

The manifest declares every exclusion. Version 1 excludes:

- Raft logs and Raft snapshots;
- node membership, endpoints, and administration history;
- producer sessions and receipts;
- consumer checkpoints;
- replay leases and maintenance cursors;
- security policy, token verifiers, certificates, keys, and tokens;
- unselected streams and deleted stream catalog entries.

Restore never rewrites opaque persisted values. The export decoder validates the source cluster on each logical type. The restore builder constructs each target type with the target cluster ID. This finite format schema is the cluster-identity rewrite inventory.

## Restore publication

Restore has one trust boundary:

```rust
pub struct VerifiedExport {
    manifest: ExportManifestV1,
    sections: BTreeMap<SectionId, VerifiedSection>,
    file: File,
}

pub struct RestorePlan {
    pub source_cluster: ClusterId,
    pub target_cluster: ClusterId,
    pub node: NodeId,
    pub group_pool: GroupPoolConfig,
    pub artifact: ArtifactIdentity,
}

pub struct RestoreReceipt {
    pub artifact: ArtifactIdentity,
    pub source_cluster: ClusterId,
    pub target_cluster: ClusterId,
    pub preserved: PreservedIdentities,
    pub excluded: ExportExclusions,
}
```

Only the decoder can construct `VerifiedExport`. Restore accepts that type instead of an untrusted path and manifest pair.

Restore follows this order:

1. Open a bounded regular input file and reject unsupported format features.
2. Verify the complete artifact, identities, ordering, counts, digests, cursor bounds, marker targets, and declared exclusions.
3. Require an absent destination and a target cluster ID that differs from the source.
4. Create a sibling staging directory on the destination filesystem.
5. Require that the target group pool contains every preserved source group ID.
6. Choose the first exported stream by ID as the target `BootstrapSpec`.
7. Build a current `NodeManifestV1` from the target config.
8. Create the target control topology from the configured local node and advertised URIs.
9. Restore the data-group pool, catalog limits, catalog revision, assignment cursor, stream routes, and publication counters.
10. Import logical state in bounded batches and rewrite only the cluster ID.
11. Initialize fresh standalone Raft state. Do not import source votes, terms, indexes, logs, snapshots, receipts, leases, checkpoints, topology, or security state.
12. Reopen every staged group through the normal storage path and compare the logical ledger with the export manifest.
13. Write `RESTORE.json` and the node manifest under staging.
14. Sync every database, file, directory, staging root, and the destination parent.
15. Close all handles and rename the staging root to the destination.
16. Sync the destination parent.

A crash before the rename leaves no destination. A crash after the rename exposes the complete staged tree. A retry returns the original receipt only when `RESTORE.json`, the artifact digest, and the target identity match.

`RESTORE.json` records the exact target `GroupPoolConfig` and the serve configuration digest. A later `serve` command must use the same group pool and memory budgets.

Restore does not bump the node manifest version. Version 1 produces the current local-insecure standalone manifest. Manifest versions 3, 4, and 5 remain the supported replicated restart and migration fixtures.

## Module ownership

| Path | Responsibility |
| --- | --- |
| `crates/light-stream-core/src/operations.rs` | Lifecycle, readiness, export identities, cuts, receipts, and closed error reasons |
| `crates/light-stream-export/` | Framing, canonical ordering, limits, digests, version dispatch, inspection, and `VerifiedExport` |
| `crates/light-stream-storage/src/logical_export.rs` | Logical group enumeration under a verified cut |
| `crates/light-stream-storage/src/logical_restore.rs` | Fresh group construction, bounded import, identity rewrite, and staged audit |
| `crates/light-stream-server/src/lifecycle.rs` | `LifecycleController`, `MutationGate`, readiness samples, and drain |
| `crates/light-stream-server/src/operations.rs` | `/livez`, `/readyz`, and `/metrics` |
| `crates/light-stream-server/src/export.rs` | Durable export reconciliation, artifact spool, expiry, download, release, and abort |
| `crates/light-stream-server/src/runtime.rs` | Group handles, operational proofs, export commands, and shutdown ordering |
| `crates/light-stream-server/src/service.rs` | Authentication, authorization, mutation permits, health, and export RPCs |
| `crates/light-stream-server/src/main.rs` | Serve, version, inspect, and restore command dispatch |
| `crates/light-stream-client/src/lib.rs` | Expanded health and one resumable `export_to` workflow |
| `crates/light-stream-cli/src/main.rs` | Stable JSON commands and local file publication |
| `scripts/package_release.py` | Deterministic archive, checksums, dependency inventory, and secret scan |
| `packaging/Containerfile` | Pinned multi-stage Linux build and non-root runtime |
| `scripts/verify.py` | LS09 package, runtime, export, restore, version, and performance evidence |

## Compatibility

The compatibility report lists these versions:

| Boundary | LS09 rule |
| --- | --- |
| Package | Semantic version plus immutable source revision |
| Public API | Additive `lightstream.v1` fields and RPCs |
| Peer API | Exact peer protocol and codec version 1 |
| Standalone node manifest | Current version 1 |
| Replicated node manifest | Read versions 3, 4, and 5; write version 5 |
| Group storage | Read and write version 1 |
| Record schema | Read supported legacy values; write the current schema |
| Export | Read and write version 1 |

Unknown required versions fail before listener startup, restore publication, or storage mutation. Startup and restore never reset storage to recover from a version error.

Older clients do not understand `shutting_down` commit certainty. Operators must use an LS09 client or CLI when they depend on `definite_no_commit` during drain.

## Verification

Every lane runs packaged release binaries:

| Lane | Pass rule |
| --- | --- |
| `l01.json` | Base and head packages preserve the same standalone and three-voter ledger. |
| `l02.json` | The extracted archive and OCI image complete E25 without a checkout or undeclared runtime dependency. |
| `l03.json` | Liveness remains available without quorum while readiness becomes false, names the failed groups, and recovers within B0. |
| `l04.json` | Drain rejects new mutations, resolves accepted work, exits within its deadline, and preserves acknowledgements after restart. |
| `l05.json` | Export records exact streams, group cuts, floors, tails, markers, counts, exclusions, length, and digest. A leader failure during download rebuilds the same digest and resumes at the saved offset. |
| `l06.json` | Restore publishes all groups at once and reproduces every declared byte, cursor, marker, and identity rule. |
| `l07.json` | Corruption, truncation, excess lengths, duplicate items, wrong identities, and unsupported features publish no destination. |
| `l08.json` | Supported manifest and storage fixtures preserve data. Unsupported versions leave bytes unchanged. |
| `l09.json` | Metrics expose lifecycle, readiness, lag, queue use, limits, overload, export, and drain without secret canaries. |
| `l10.json` | The secured package uses mounted private files, fails closed when they are missing, and leaks no secret bytes. |

LS09 records B0 liveness and readiness, B4 base and head performance, local startup, idle RSS, archive bytes, compressed OCI bytes, metrics overhead, export duration, restore duration, peak restore RSS, and staging disk use.

Docker is available on the local ARM64 host, so the 50 MiB Linux image target is measured here. Independent-host claims remain `BLOCKED`.

`release-journey.json` indexes the package, clean-environment, lifecycle, export, restore, version, secured-mode, and performance evidence.

## Implementation sequence

### Unit 1: Build the package verifier

Add `scripts/package_release.py`, `packaging/Containerfile`, the LS09 verifier dispatcher, and the LS09 entry in the repository verification skill. Normalize archive ordering, timestamps, owner IDs, and modes. Build with `SOURCE_DATE_EPOCH`, path remapping, and stripped release binaries. Build the same source twice and compare both archive and OCI digests.

Package the current binaries and prove that the extracted archive runs without the checkout. Record current lifecycle, metrics, export, and restore gaps as failing observations.

### Unit 2: Add lifecycle, readiness, metrics, and drain

Add the lifecycle types, mutation gate, operations listener, bounded readiness probes, scheduler metrics, and drain order. Pass lanes 3, 4, and 9 before export code starts.

### Unit 3: Add durable export and the format crate

Add the export domain, group fences, reconciler, logical storage readers, canonical format, client download, and corruption self-tests. Pass lane 5 and the export half of lane 7.

### Unit 4: Add offline restore and compatibility checks

Add staged logical import, cluster ID rewrite, standalone Raft initialization, restore receipt, complete corruption cases, and supported version fixtures. Pass lanes 6, 7, and 8.

### Unit 5: Close package and performance evidence

Run all ten lanes from the archive and the image. Record B0, B4, local B6 measurements, secured packaging, and the final cleanup result. Update operator docs and the repository verification skill.

## Rejected alternatives

Raw RocksDB copies and Raft snapshots expose internal recovery state and cannot support a selected-stream identity rewrite.

Client-composed exports cannot order a durable cut across leaders or recover from a lost response.

Offline export without a replicated fence cannot prove which acknowledged writes belong to the artifact.

Offline stopped-node materialization keeps the operator inside the coordination protocol. Broker-managed materialization can resume after leader change and gives `light-streamctl` one command.

Live restore cannot publish several independent groups atomically. The first format restores only to an absent local destination.

A manifest version 6 for restore provenance adds a migration without changing startup correctness. It also conflicts with the current separate standalone and three-voter manifest shapes. `RESTORE.json` records provenance inside the atomically published destination.

Fencing only selected groups needs route-aware process admission and creates two meanings for write readiness. Version 1 fences every configured data group and states the resulting write pause.

Compression adds decompression limits and another integrity boundary. Version 1 stays uncompressed.
