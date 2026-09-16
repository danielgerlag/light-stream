# Light Stream API invariants

Status: implemented through LS08.

## Identities

- `ClusterId`, `NodeId`, `GroupId`, `StreamId`, `PartitionId`, `PrincipalId`, `ProducerSessionId`, `RequestSequence`, `BookmarkId`, and `ConsumerId` are distinct domain types.
- Human names are labels. Reusing a stream or bookmark name does not reuse its immutable identity.
- A partition cursor contains the cluster, stream, partition, and next record offset.
- The next offset is inclusive when reading.
- Raft log indexes and physical storage keys are not user cursors.

## Commands

- One data group serializes commands for the partitions it owns.
- An append and its optional partition-local bookmark commit as one command.
- The bookmark points after the appended batch.
- A stream-level cursor vector contains independently committed positions. It is not an atomic or causally consistent cut.
- Mutable consumer progress is separate from immutable bookmarks.
- A consumer checkpoint is keyed by cluster, partition, and `ConsumerId`.
- Checkpoint creation expects no current value. Checkpoint updates compare an exact revision.
- A checkpoint can outlive retained records. It does not pin payloads or move the retention floor.

## Retry receipts

- A producer session belongs to one principal, cluster, and partition.
- Request sequences are monotone within a session.
- A valid duplicate returns the original result.
- Reusing a request sequence with another payload fails.
- An expired or unknown retry identity fails instead of becoming a new write.
- Deleting a bookmark does not change a previously committed receipt.
- LS02a retains 4096 receipt identities per producer session by default.

## Visibility

- Clients read only committed and applied records.
- A failed validation or authorization request has no committed side effect.
- A timeout or lost response is ambiguous until the receipt or committed data resolves it.
- Publish overload is rejected before admission and always means `definite_no_commit`.
- Cancellation after a publish RPC starts remains ambiguous until receipt resolution.
- Current reads require quorum-confirmed leadership.

## Bootstrap

- Startup opens an existing cluster or remains pristine. Startup never creates a cluster.
- `cluster bootstrap` is the only LS02a cluster creation path.
- Repeating the same bootstrap request returns the same cluster and stream identities.
- A bootstrap request with different identities fails.
- LS02a creates control group `1`, data group `2`, and partition `0` of the bootstrap stream.
- LS02b accepts exactly three unique node IDs, public URIs, and peer URIs.
- The contacted seed prepares both pristine peers before it adds either peer as a learner.
- A node becomes application-active only after both state machines contain the bootstrap identity and both groups have the same exact committed and effective voter set.
- Startup opens only groups authorized by `cluster.json`. It never initializes an existing manifest silently.

## Consensus routing

- Public application requests execute only on the current data-group leader.
- A follower returns a typed leader hint only when Openraft's node address matches the durable peer descriptor.
- A publish deadline before any RPC is sent is a definite non-commit and retains the producer request identity.
- A publish deadline after an RPC could reach a server is ambiguous and retains the producer request identity.
- Fetch and receipt deadlines have `not_applicable` commit outcomes.
- Current fetch and receipt reads use `ensure_linearizable(ReadPolicy::ReadIndex)`.
- Peer envelopes bind the protocol and codec versions, cluster, group, sender, and target before decoding the Openraft body.

## Storage

- Each group owns one RocksDB database and a versioned column-family set.
- Raft log descriptors and committed record indexes refer to one immutable payload object.
- A payload object has independent Raft-log, applied-state, and snapshot reachability flags.
- Raft log purge cannot remove a payload that committed records or the current snapshot still reference.
- Votes, committed indexes, log writes, state application, and snapshots use synchronous WAL writes.
- A state-machine response is sent only after the applied state and the receipt are durable.
- New LS02b groups use `SnapshotPolicy::Never`. Logs remain available for ordinary suffix catch-up.
- Remote `full_snapshot` returns an explicit unsupported error. Snapshot catch-up after purge remains `UNSUPPORTED_LS06`.

## Retention and replay

- Retention advances a monotonically increasing logical floor per partition.
- Bookmark metadata can outlive its payload under the configured policy.
- Resuming an expired cursor fails explicitly and never skips to a newer offset.
- A protected replay is partition-local and requires an admitted lease with a deadline and byte budget.
- An unleased replay across requests can fail on expiration between requests.

## Security

- `local-insecure` binds loopback by default and reports its mode.
- `secured` requires public TLS, bearer-token authentication, permission checks, and peer mutual TLS.
- A client identity is not a node identity.
- Security configuration errors fail startup and never enable a fallback mode.
- The control group stores grants, token verifier digests, peer certificate fingerprints, and policy revisions. It never stores raw tokens or private keys.
- Every public RPC has one registered permission and authenticates before request conversion or runtime access.
- Name-based stream reads require `StreamDiscover`, then reauthorize the resolved immutable `StreamId`.
- Producer receipts, replay leases, checkpoints, and security mutations remain bound to their authenticated principal.
- Peer certificates bind the cluster and node in one URI SAN. The certificate identity must match the peer envelope and an active policy fingerprint before payload decoding.
- Public work requires a policy lease no older than `maximum_policy_staleness_ms`. Expired leases fail with `security_policy_stale`.
- Token and peer certificate generations only increase. Rotation adds the new generation before revoking the old generation.
- Manifest version 5 records either `local-insecure` or `secured`. Startup flags cannot downgrade a secured manifest.
- `ActivateSecuredTransport` commits the HTTPS topology and initial policy in one idempotent control-group command.
