# Light Stream implementation plan

Build a high-volume Rust broker with RocksDB, real Raft, bounded independent writer groups, and a native API.
Deliver immutable named bookmarks, durable retry receipts, retention, and protected replay.
Implement configurable runtime security and an optional independent security review.
Keep the POCs as controls, not production implementations.
Execute LS01, LS02, LS03, LS04, LS05, LS06, LS07, LS08, LS09, and LS10 in order.
Stop at an evidenced implementation or release-candidate verdict. The operator owns merge and deployment.

## How to read this

One box is one unit of work. Every box names the evidence that checks it. A nested box is a sub-step of the box above it. Check a box only when its evidence exists, a file, a log, a test result, or an immutable revision. The body is a how-to. The appendices explain and record.

The program follows `playbooks/autopilot-stack.md` from the installed `poteto-mode` skill when available.
Without those plugins, follow the same ownership, dependency, evidence, and operator-approval rules using ordinary agent controls.
One coding agent may execute the sequence serially.
The operator merges every PR. No owner enables auto-merge or deploys.

Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked.

Use the [verification contract](verification.md) for profiles, scenario IDs, budgets B0 to B7, evidence, and verdicts.
Use the [security contract](security.md) for runtime modes and the independent review switch.
All `crates/light-stream-*`, `scripts/verify.py`, and `verification/` paths below are planned deliverables, not existing tools.
The existing commands are documented in the root README and `poc/README.md`.

Treat the PR IDs as work-package IDs until a real checkout and forge are available.
Do not invent a base SHA, initialize or publish a repository without authority, or claim a PR exists.
Local work can use immutable source snapshots while publication is blocked.
Do not call that state merge-ready.

This service and CLI plan uses API transcripts, JSON receipts, and process logs as authoritative live evidence.
A screenshot or terminal video can supplement an operator review, but cannot replace those records.
Do not manufacture browser screenshots for a service with no browser UI.

## Program checklist

### Arm the program

- [ ] Present this plan and its defaults to the operator. Save the approval or hold decision in `artifacts/program/authorization.json`. Do not start implementation on a planning request alone.
- [ ] On explicit go, set `/autopilot` or the agent's equivalent objective to execute LS01 through LS10, satisfy unit/live/perf gates, preserve failed evidence, and stop before merge or deployment. Save the objective.
- [ ] Resolve optional plugin paths with `/skills info poteto-mode`. Read the selected execution playbook and applicable principles. If unavailable, record the ordinary-command workflow instead of blocking on a plugin.
- [ ] Read `README.md`, `docs/design/proposal.md`, `docs/experiments/high-volume.md`, this plan, and its verification and security companions. Save the input revisions in the program manifest.
- [ ] Record `independent_security_review`, the security deployment profile, base revision, artifact root, resource limits, and approved infrastructure in `artifacts/program/manifest.json`.
- [ ] Lock the proposed numeric budgets and workload profile in LS01 before comparing implementation results. Save operator changes as a new profile revision.
- [ ] Schedule a 30-minute audit using `/every 30m` or the agent environment's scheduler. Save the schedule or explicit manual-resume mechanism.
- [ ] Use this tick prompt verbatim. "Re-read the execution objective and plan. Check every active owner, revision, live lane, and evidence path. Stop scope drift. Report actual completed artifacts, verdicts, operator gates, and blockers. Send a status message even when nothing changed."
- [ ] On hold or stand-down, stop new writes and new fault actions, stop owned workloads safely, and save a resume manifest. Do not terminate unrelated processes.

### Spawn owners

- [ ] Start an owner only when its dependencies are ready. Save its ID, revision, allowed files, and acceptance criteria in the program manifest.
- [ ] Follow the chain `LS01 -> LS02 -> LS03 -> LS04 -> LS05 -> LS06 -> LS07 -> LS08 -> LS09 -> LS10`. Base a stack child on its verified parent.
- [ ] Keep one writer for root manifests, lockfiles, protocol schemas, and stack topology. Let each owner delegate only disjoint files under its own work package.
- [ ] Record the throughput checkpoint before each work package. Name blocking prerequisites, independent work, shared mutable state, and the smallest safe decomposition.
- [ ] Run independent functional lanes concurrently only with isolated stores, ports, and fault controls. Serialize performance runs on shared hardware.
- [ ] Hold operator review for LS01, LS04, LS05, LS07, LS08, and LS09. Save the operator's disposition at the reviewed revision.

### PR mechanics, for every PR

- [ ] Resolve the approved forge once. Use `gh` when available; use an approved `origin` CLI only if it resolves the repository. Record unavailable publication as blocked.
- [ ] Open the PR against its verified parent only after publication authority exists. Include scope, exact revision, unit/live/perf evidence, and unproven claims.
- [ ] Run the applicable existing format, compile, lint, and test gates. Add new tooling only when the implementation needs it, and document the command.
- [ ] Apply technical-writing and unslop rules to documentation and commits. Review comments and suppressions before review.
- [ ] Triage automated review claims against the code and evidence. Do not accept or dismiss security findings by reputation alone.
- [ ] Rebase only within the approved branch workflow. Never reset unrelated work or force-push shared branches without authorization.

### Verdict and merge, for every PR

- [ ] At the exact candidate revision, run independent gates, the ten live lanes below, the performance comparison, and a receipts audit. Use `/swarm` when available or equivalent isolated verifiers.
- [ ] Accept `PASS` only when the artifacts satisfy their predicates. A timeout, missing host, unsupported base feature, or absent artifact is not a pass.
- [ ] Send findings back to the owner. A changed patch invalidates its verdict. Rebuild and rerun affected gates at the new revision.
- [ ] Append only verified changes to the proposed stack. The operator owns landing. Compare patch identity after rebases and refresh runtime evidence at the final revision.

### Boot recipe, for every live lane

- [ ] Materialize the exact revision in an isolated worktree or approved immutable source snapshot. Save its identity and dependency lockfile.
- [ ] Build the actual release artifacts using the planned verification runner. Record binary hashes, compiler, build flags, and source snapshots.
- [ ] Start only the required owned nodes, clients, and fault proxies. Wait for service readiness, then prove a real request succeeds rather than trusting an open port.
- [ ] Deliver ordinary input through the public client or CLI. Use read-only diagnostics for observation and owned controls for faults.
- [ ] Save evidence under `artifacts/<revision>/<phase>/<lane>/<run-id>/`. The abbreviated paths in each lane are relative to that run directory.
- [ ] Preserve failed stores and logs in restricted local diagnostics. Publish only redacted evidence. Stop owned processes and record cleanup.

## Establish contracts and the verification runner (LS01)

**Depends on.** None.

**Files.**

- [ ] Create `proto/lightstream/v1/`, `crates/light-stream-core/`, `crates/light-stream-proto/`, and initial server, client, CLI, and testkit crates. Save the module map.
- [ ] Create `scripts/verify.py`, `verification/profiles/`, and `verification/scenarios/`. Update the root Cargo workspace without changing historical POC evidence.

**Build.**

- [ ] Define stable identities, partition cursors, domain commands, receipts, bookmark targets, error classes, and security-mode types. Record invariants and serialization versions in `docs/api/`.
- [ ] Define the versioned native gRPC API and CLI JSON grammar. Start with Tokio, tonic/prost, and Openraft unless the early compatibility/conformance investigation justifies a recorded change.
- [ ] Build a real server health/capability endpoint, CLI health command, process harness, source capture, and independent acknowledgement ledger. Unsupported data operations must fail explicitly.
- [ ] Lock local and reference profiles, proposed budgets, and optional-review input. Record any unresolved infrastructure as blocked.

**You see.**

- [ ] The built server answers a real CLI request, identifies its revision and capabilities, and never reports an unimplemented publish as successful. Save `contract-demo.json`.

**Verify, unit.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked.

- [ ] Add domain round-trip, invalid-boundary, profile, and oracle-negative tests. Run `cargo test --workspace` and the new harness tests; retain the existing POC tests.

**Verify, live.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked. Ten independent lanes at the PR head.

- [ ] Lane 1. Regression lane against trunk. Run existing POC smoke controls at base and head; record the absent production feature. Save `l01.json`. Pass when controls remain valid and head health is real.
- [ ] Lane 2. Boot the release server and call health through the CLI. Save `l02.json`. Pass when revision and capabilities match the binary.
- [ ] Lane 3. Request an unimplemented data operation. Save `l03.json`. Pass when it returns a definite unsupported error with no acknowledgement.
- [ ] Lane 4. Submit invalid configuration and malformed bounded input. Save `l04.json`. Pass when startup or ingress fails explicitly without an unsafe listener.
- [ ] Lane 5. Start two isolated run directories. Save `l05.json`. Pass when ports, stores, and ledgers cannot collide.
- [ ] Lane 6. Hold a store lock and start a second owner. Save `l06.json`. Pass when the second owner is refused without reinitialization.
- [ ] Lane 7. Change source after a previous build. Save `l07.json`. Pass when the runner rebuilds or rejects a stale executable.
- [ ] Lane 8. Inject a false acknowledgement into an oracle fixture. Save `l08.json`. Pass when the verifier reports failure rather than a count-only success.
- [ ] Lane 9. Force a child-process failure. Save `l09.json`. Pass when logs and owned scratch data survive and unrelated resources remain untouched.
- [ ] Lane 10. Resume a recorded run. Save `l10.json`. Pass when its revision, seed, profile, and prior failures remain explicit.

**Verify, perf.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked.

- [ ] Metric. Measure harness overhead and unchanged POC control throughput at base and head; measure new health latency and process startup separately.
- [ ] Probe. Interleave five equivalent control runs through the same observer. Run the new health probe against the real head server.
- [ ] Baseline. Save the base control values first and mark production health unsupported on base.
- [ ] Rule. Apply B4 to equivalent controls. Require B0 and health p99 at or below 100 ms for the new endpoint.

**Review gate.** The operator reviews the public contracts before merge.

- [ ] Present schemas, error semantics, security defaults, budgets, and actual CLI transcripts. Save the operator decision.
- [ ] Use a terminal screenshot or video only as supplemental review evidence; JSON and logs remain authoritative.

**Merge.**

- [ ] Record the clean verdict, operator contract approval, exact revision, and publication status. Stop at merge-ready.

## Implement the durable Raft vertical slice (LS02)

**Depends on.** LS01.

**Files.**

- [ ] Create `crates/light-stream-storage/` and server replication, state-machine, and data-RPC modules. Add scenario fixtures under `verification/`.

**Build.**

- [ ] Implement RocksDB log, hard-state, and committed application storage for the selected Raft library. Keep log persistence and committed application distinct.
- [ ] Start one control group and one data group with an explicit bootstrap stream identity. Support one-voter standalone and a real three-voter topology without a temporary fixed-leader path.
- [ ] Implement committed append, bounded fetch, stable offsets, and minimal durable producer request receipts. Do not expose uncommitted tails.
- [ ] Run the Raft library's storage conformance suite. Preserve Apple full-sync normalization and record platform durability semantics.

**You see.**

- [ ] CLI and client publish arbitrary bytes, read the committed result, restart nodes, and read the same bytes. Save `durable-journey.json`.

**Verify, unit.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked.

- [ ] Add storage adapter, state-machine determinism, receipt, corruption, and crash-boundary tests. Run the targeted core, storage, and server tests.

**Verify, live.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked. Ten independent lanes at the PR head.

- [ ] Lane 1. Regression lane against trunk. Preserve LS01 and POC controls; record absent append on base. Save `l01.json`. Pass when head E01 completes without a fabricated ratio.
- [ ] Lane 2. Run E01 with variable opaque records. Save `l02.json`. Pass when CLI and client reads match the independent ledger.
- [ ] Lane 3. Run E02 with three real voters. Save `l03.json`. Pass when durable majority acknowledgement and committed reads agree.
- [ ] Lane 4. Kill the data leader after an acknowledged batch. Save `l04.json`. Pass when E03 preserves that batch after election.
- [ ] Lane 5. Remove the majority. Save `l05.json`. Pass when E05 returns the defined error with zero new acknowledgements.
- [ ] Lane 6. Crash between log persistence and application. Save `l06.json`. Pass when recovery resolves the committed prefix without duplicate effects.
- [ ] Lane 7. Drop an acknowledgement and retry its identity. Save `l07.json`. Pass when E09 returns the original result once.
- [ ] Lane 8. Submit a conflicting retry payload. Save `l08.json`. Pass when it is rejected without changing committed state.
- [ ] Lane 9. Corrupt a committed storage fixture. Save `l09.json`. Pass when E19 reports corruption or approved replica repair, not silent truncation.
- [ ] Lane 10. Restart all nodes from durable stores. Save `l10.json`. Pass when the cluster identity, stream identity, offsets, and receipt ledger survive.

**Verify, perf.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked.

- [ ] Metric. Measure unchanged health overhead on base and head, plus new durable append/fetch latency and throughput.
- [ ] Probe. Run interleaved health controls and a pinned low-load append/fetch workload through the actual client.
- [ ] Baseline. Record base health first. Record the absence of production Raft append instead of using POC rates as a production baseline.
- [ ] Rule. Require B0, B1, B2, and zero ledger discrepancies. Apply B4 only to equivalent existing behavior.

**Review gate.** None. LS02 is not review-gated beyond its approved API contract and evidence.

**Merge.**

- [ ] Record storage-conformance and live-ledger evidence at the exact revision. Stop at merge-ready.

## Distribute stream partitions across bounded groups (LS03)

**Depends on.** LS02.

**Files.**

- [ ] Edit server catalog, group registry, routing, and placement modules plus core topology types and metadata RPCs.

**Build.**

- [ ] Implement stable stream and partition identities, a bounded group registry, and metadata routing without a global payload transaction.
- [ ] Implement recoverable create, activate, and delete intents. Never expose a name before its data group is ready or reuse an old identity.
- [ ] Support several groups with distributed leaders and explicit replica placement. Keep automatic repartitioning outside v1.
- [ ] Enforce per-node resource budgets across groups rather than allocating unrestricted caches and threads per stream.

**You see.**

- [ ] Independent streams and partitions accept traffic through different leaders, while logical stream count does not create an unbounded number of stores. Save `partition-journey.json`.

**Verify, unit.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked.

- [ ] Add catalog-intent, routing, identity, placement, and shared-budget tests. Run targeted core and server tests.

**Verify, live.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked. Ten independent lanes at the PR head.

- [ ] Lane 1. Regression lane against trunk. Repeat the one-group append journey at base and head. Save `l01.json`. Pass when the same workload preserves its ledger and budgets.
- [ ] Lane 2. Create many streams mapped to bounded groups. Save `l02.json`. Pass when configured store and worker limits hold.
- [ ] Lane 3. Write four partitions through different endpoints. Save `l03.json`. Pass when routing reaches their current leaders and preserves per-partition order.
- [ ] Lane 4. Kill the control leader during a create intent. Save `l04.json`. Pass when retry converges to one active stream identity.
- [ ] Lane 5. Interrupt deletion and restart. Save `l05.json`. Pass when the old group is fenced before name reuse.
- [ ] Lane 6. Run E17. Save `l06.json`. Pass when old cursors and receipts never attach to a recreated stream.
- [ ] Lane 7. Submit stale routing metadata. Save `l07.json`. Pass when the client refreshes or receives an explicit routing error.
- [ ] Lane 8. Slow one group while loading another. Save `l08.json`. Pass when no global append lock stalls unrelated groups.
- [ ] Lane 9. Exceed stream and group quotas. Save `l09.json`. Pass when admission fails explicitly without partial activation.
- [ ] Lane 10. Restart the catalog and all groups. Save `l10.json`. Pass when placement, identities, and committed data agree.

**Verify, perf.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked.

- [ ] Metric. Compare one-group performance at fixed client concurrency, then measure the new multi-group configuration curve separately.
- [ ] Probe. Interleave equivalent base/head one-group workloads and run one/two/four/eight groups with total producer concurrency recorded.
- [ ] Baseline. Save the one-group base first. Record unsupported group counts rather than treating them as zero throughput.
- [ ] Rule. Apply B4 to the equivalent path and B0/B1 to new metadata operations. Fail any unbounded resource growth.

**Review gate.** None. LS03 is not review-gated beyond the LS01 topology contract.

**Merge.**

- [ ] Record independent-group and catalog-recovery evidence. Stop at merge-ready.

## Add immutable named bookmarks and recent listing (LS04)

**Depends on.** LS03.

**Files.**

- [x] Edit core bookmark commands, storage indexes, bookmark RPCs, client methods, and CLI commands.

**Build.**

- [x] Implement partition-local named bookmarks in the data group's state machine, with immutable IDs, after-batch cursors, and publication sequence.
- [x] Implement atomic append-plus-bookmark, backdated committed targets, terminal deletion, explicit name reuse, and receipt-safe retry behavior.
- [x] Implement indexed newest-N listing and stable backward pagination.
- [x] Implement stream-level named cursor vectors as explicitly independent positions in the control catalog. Do not promise cross-group append atomicity or a consistent cut.

**You see.**

- [x] A user names an import boundary, lists recent markers, and resumes the exact record after that boundary. Save `bookmark-journey.json`.

**Verify, unit.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked.

- [x] Add boundary, publication-order, pagination, name-lifecycle, vector, and retry tests. Run targeted core/storage/server/client tests.

**Verify, live.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked. Ten independent lanes at the PR head.

- [x] Lane 1. Regression lane against trunk. Preserve ordinary append/fetch and gate the new marker journey. Save `l01.json`. Pass when base behavior survives and head E10 succeeds.
- [x] Lane 2. Publish a marker after a batch and resume. Save `l02.json`. Pass when the next record is exact and no boundary record is duplicated.
- [x] Lane 3. Crash around combined publication. Save `l03.json`. Pass when E11 never exposes only the batch or only its marker.
- [x] Lane 4. Drop the combined response and retry. Save `l04.json`. Pass when the receipt returns identical offsets and marker ID.
- [x] Lane 5. Create a marker targeting an older offset. Save `l05.json`. Pass when recent listing still follows publication order.
- [x] Lane 6. Create markers while paging backward. Save `l06.json`. Pass when the captured listing ceiling prevents duplicate pages.
- [x] Lane 7. Delete a marker, reuse its name, and retry the old request. Save `l07.json`. Pass when the old ID is not resurrected or rebound.
- [x] Lane 8. Run E18 across partitions. Save `l08.json`. Pass when vector positions are valid and the API exposes no false consistent-cut guarantee.
- [x] Lane 9. List 100 markers from 10,000 entries. Save `l09.json`. Pass when results are correct and read instrumentation shows no payload scan.
- [ ] Lane 10. Fail over and restart before resolving a marker. Save `l10.json`. Pass when name, ID, cursor, and publication sequence remain unchanged.

**Verify, perf.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked.

- [ ] Metric. Compare append/fetch on both revisions; measure new marker publication and indexed last-100 lookup separately.
- [ ] Probe. Interleave unchanged workloads and query the head's 10,000-entry fixture during writes.
- [ ] Baseline. Save base append/fetch first and record that named markers are absent.
- [ ] Rule. Apply B4 to unchanged traffic. Require marker operations within B1 and last-100 local lookup at or below 100 ms p99 without payload scans.

**Review gate.** The operator reviews bookmark semantics before merge.

- [ ] Present CLI transcripts and vector/partition examples, including deletion and expiration expectations. Save approval.
- [ ] Supply a terminal screenshot or video only where it helps the operator inspect the actual interaction.

**Merge.**

- [ ] Record the exact-revision verdict and approved bookmark contract. Stop at merge-ready.

## Implement retention and bounded replay protection (LS05)

**Depends on.** LS04.

**Files.**

- [x] Edit retention, lease, bounded-read, bookmark-availability, and storage-reclamation modules.

**Build.**

- [x] Replicate retention-floor changes and preserve bookmark metadata under its independent lifetime policy.
- [x] Implement partition-local range leases with durable admission, byte limits, expiry, renewal, release, and clock assumptions.
- [x] Admit protected replay before promising completeness. Keep unleased replay explicitly abortable on expiry.
- [x] Use short read transactions and bounded batches. Add logical space accounting and reserve configured recovery headroom. Physical bytes owned by retained Raft entries remain LS06.

**You see.**

- [x] A protected replay completes while retention runs; an unprotected expired cursor produces a clear error rather than skipped data. Save `retention-journey.json`.

**Verify, unit.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked.

- [x] Add ordered retention/pin races, lease lifecycle, quota, clock-bound, and record-base tests. Run targeted core/storage/server tests.

**Verify, live.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked. Ten independent lanes at the PR head.

- [x] Lane 1. Regression lane against trunk. Replay retained data with retention disabled on both revisions. Save `l01.json`. Pass when byte and marker results remain identical.
- [x] Lane 2. Expire payloads while keeping markers. Save `l02.json`. Pass when E12 returns visible expired metadata and an explicit resume error.
- [x] Lane 3. Replay under an admitted lease during retention. Save `l03.json`. Pass when E13 returns the entire promised range.
- [x] Lane 4. Replay without a lease during expiry. Save `l04.json`. Pass when E14 reports an explicit terminal error rather than a shortened success.
- [x] Lane 5. Race pin admission with retention. Save `l05.json`. Pass when E15 has one valid ordered result with no resurrection.
- [x] Lane 6. Exhaust pin and disk budgets. Save `l06.json`. Pass when new work is rejected before active protection is violated.
- [x] Lane 7. Crash during renewal or release. Save `l07.json`. Pass when retry converges and no orphan lease blocks reclamation forever.
- [x] Lane 8. Exercise the declared clock-skew boundary. Save `l08.json`. Pass when expiry follows the documented guarantee without silently shortening a lease.
- [x] Lane 9. Stall a reader. Save `l09.json`. Pass when E16 keeps transactions and memory bounded.
- [x] Lane 10. Restart after logical deletion and partial reclamation. Save `l10.json`. Pass when the retained floor and marker availability remain correct.

**Verify, perf.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked.

- [x] Metric. Compare retention-disabled traffic, then measure new lease latency, reclaim cost, retained bytes, and reader interference.
- [x] Probe. Interleave the unchanged path and run a pinned publish/fetch comparison plus the LS05 retention journey.
- [x] Baseline. Record the LS04 publish/fetch behavior and mark lease protection unsupported there.
- [x] Rule. Apply B4 to equivalent traffic and B1 to lease operations. Fail any lease violation, unbounded queue, or disk-budget overrun.

**Review gate.** The operator reviews resource and replay promises before merge.

- [ ] Present protected and unprotected CLI transcripts, quotas, and clock assumptions. Save approval.
- [ ] Use a screenshot or video only to supplement the operator's review of the real replay behavior.

**Merge.**

- [ ] Record the retention-race and replay verdicts at the reviewed revision. Stop at merge-ready.

## Complete recovery, catch-up, and safe membership changes (LS06)

**Depends on.** LS05.

**Files.**

- [x] Edit Raft transport, per-peer replication progress, snapshot storage/transfer, membership administration, and recovery diagnostics.

**Build.**

- [x] Replace every POC-style permanent peer exclusion with independent replication progress and bounded catch-up from durable history.
- [x] Implement log-suffix and snapshot catch-up with complete retained group state and included log identity. The 1 GiB file-backed fixture passes.
- [x] Stage and verify snapshots before installation. Preserve newer local term and vote state.
- [x] Implement learner admission, safe voter changes, leader transfer, cancellation, retirement, and recovery from interrupted operations.

**You see.**

- [x] A slow or stopped replica returns to service without losing entries or requiring a forced reset, while a healthy majority continues. Save `ls06/snapshot-recovery.json`.

**Verify, unit.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked.

- [x] Extend Raft conformance, snapshot integrity, membership, fencing, and cancellation tests. Run targeted storage, server, client, and testkit tests.

**Verify, live.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked. Ten independent lanes at the PR head.

- [ ] Lane 1. Regression lane against trunk. Repeat the healthy and single-leader-crash journeys. Save `l01.json`. Pass when prior guarantees and committed ledgers remain intact.
- [ ] Lane 2. Isolate a live old leader. Save `l02.json`. Pass when E04 refuses stale success and heals through Raft.
- [ ] Lane 3. Delay a minority follower. Save `l03.json`. Pass when E06 preserves majority progress and eventually catches up that follower.
- [x] Lane 4. Restore a stopped follower within retained log history. Covered by the fresh LS02b regression.
- [x] Lane 5. Restore a follower beyond the purge frontier. Save `ls06/snapshot-recovery.json`. Pass when E08 restores retained payloads and metadata.
- [ ] Lane 6. Crash halfway through snapshot installation. Save `l06.json`. Pass when restart selects a valid state without partial publication.
- [ ] Lane 7. Send a corrupt or wrong-cluster snapshot. Save `l07.json`. Pass when E19 rejects it without overwriting valid state.
- [x] Lane 8. Replace a voter through a learner. Save `ls06/membership-recovery.json`. Pass when E20 changes membership only after catch-up.
- [x] Lane 9. Fail a leader during membership change. Save `ls06/membership-recovery.json`. Pass when recovery converges without forced stale promotion.
- [x] Lane 10. Catch up groups while another is busy. Save `ls06/membership-recovery.json`. Pass when group 3 remains usable during replacement.

**Verify, perf.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked.

- [ ] Metric. Compare healthy throughput and p99, then measure failover pause, peer lag, snapshot time, and temporary disk usage.
- [x] Probe. Run the pinned fault and B7 recovery fixtures. Trunk does not support the B7 snapshot path.
- [x] Baseline. Record existing recovery first and mark unsupported snapshot and membership cases honestly.
- [x] Rule. Require the predeclared B7 deadlines, disk headroom, and locked RSS budget. Fail lost acknowledgements, permanent exclusion, exact-byte mismatch, or an unbounded catch-up backlog.

**Review gate.** None. LS06 is not review-gated beyond its approved administration and recovery contracts.

**Merge.**

- [x] Record old-leader fencing, catch-up, and membership verdicts. Stop at merge-ready.

## Finish batching, client workflows, and mutable progress (LS07)

**Depends on.** LS06.

**Files.**

- [x] Edit ingest scheduling, producer receipt handling, consumer checkpoints, `crates/light-stream-client/`, and `crates/light-stream-cli/`.

**Build.**

- [x] Implement byte/time-bounded server batching and explicit admission/backpressure without changing request identities.
- [x] Implement metadata refresh, reconnect, bounded retry, cancellation, and receipt resolution in the Rust client.
- [x] Add complete CLI workflows for streams, publish/read, bookmarks, replay leases, and compare-and-set consumer progress.
- [x] Keep record offsets, mutable consumer progress, and shared immutable markers distinct across retries and failover.

**You see.**

- [x] A user completes the application journey from the CLI and SDK while leadership changes, with stable receipts and bounded completion. Save `client-workflows.json`.

**Verify, unit.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked.

- [x] Add batch-timer, retry-budget, cancellation, routing, checkpoint-CAS, and CLI error/output tests. Run targeted client/CLI/server tests.

**Verify, live.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked. Ten independent lanes at the PR head.

- [x] Lane 1. Regression lane against trunk. Run the same client request identities and retained replay on both revisions. Save `l01.json`. Pass when results remain compatible.
- [x] Lane 2. Run E01 and E02 using only the shipped CLI and SDK. Save `l02.json`. Pass when both match the independent data ledger.
- [x] Lane 3. Change leaders during client traffic. Save `l03.json`. Pass when metadata refresh and bounded retries preserve exactly one committed result.
- [x] Lane 4. Drop a response after commit. Save `l04.json`. Pass when E09 resolves the original receipt without duplicate records.
- [x] Lane 5. Cancel a submitted request. Save `l05.json`. Pass when the client reports ambiguity and the durable receipt resolves the result.
- [x] Lane 6. Publish sparse traffic through the batch timer. Save `l06.json`. Pass when the declared flush deadline is honored.
- [x] Lane 7. Exceed offered capacity. Save `l07.json`. Pass when E29 rejects work explicitly and recovers after load falls.
- [x] Lane 8. Advance a consumer checkpoint with competing revisions. Save `l08.json`. Pass when E21 preserves CAS and leaves bookmarks unchanged.
- [x] Lane 9. Restart after progress and receipt updates. Save `l09.json`. Pass when both survive recovery and failover.
- [x] Lane 10. Run CLI failures in automation. Save `l10.json`. Pass when exit codes, JSON, and partial or ambiguous results are machine-readable.

**Verify, perf.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked.

- [x] Metric. Compare equivalent client-visible traffic on both revisions. Measure batch wait, completion, overload rejection, and queue recovery separately.
- [x] Probe. Interleave fixed-concurrency workloads and add overload and sparse-arrival probes with deterministic identities.
- [x] Baseline. Save base client-visible latency, including serialization and retries, before tuning head.
- [x] Rule. Apply B4 and B1 to equivalent healthy traffic. Fail any configured queue limit, retry deadline, or batching-time bound violation.

**Review gate.** The operator reviews the client and CLI workflow before merge.

- [ ] Present actual transcripts covering success, expiration, overload, cancellation, and retry. Save the operator decision.
- [ ] Include a terminal screenshot or video when it clarifies the interaction, without replacing raw command evidence.

**Merge.**

- [ ] Record both CLI and SDK verdicts and the approved output contract. Stop at merge-ready.

## Implement optional runtime security (LS08)

**Depends on.** LS07.

**Files.**

- [x] Edit server transport/security modules, centralized authorization policy, credential configuration, client/CLI credential handling, and security fixtures.

**Build.**

- [x] Implement `local-insecure` and `secured` profiles as specified in the security contract. Reject invalid secure configuration and unintended insecure remote exposure.
- [x] Add verified client TLS, high-entropy token authentication, stream-scoped permissions, and peer mutual TLS tied to cluster membership.
- [x] Bind producer sessions and receipts to principals. Cover every existing unary, streaming, peer, and administrative route.
- [x] Implement bounded revocation, credential rotation, secret redaction, and security events without adding a password system or identity-provider service.

**You see.**

- [x] The complete application workflow works in both selected modes; denied requests have no committed effects, and secure startup never falls back. Save `security-journey.json`.

**Verify, unit.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked.

- [x] Add mode parsing, policy coverage, identity binding, redaction, certificate, and revocation-bound tests. Run targeted server/client/CLI tests.

**Verify, live.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked. Ten independent lanes at the PR head.

- [x] Lane 1. Regression lane against trunk. Preserve the approved local workflow and record secured mode absent on base. Save `l01.json`. Pass when head supports both without silent mode changes.
- [x] Lane 2. Run E22 on loopback. Save `l02.json`. Pass when no-credential work succeeds and the mode is explicit.
- [x] Lane 3. Attempt unintended insecure remote binding. Save `l03.json`. Pass when startup refuses it before serving traffic.
- [x] Lane 4. Run E23 with valid client and peer identities. Save `l04.json`. Pass when secured publication, replay, and failover preserve the ledger.
- [x] Lane 5. Use missing or invalid client credentials. Save `l05.json`. Pass when access is denied and state is unchanged.
- [x] Lane 6. Use a valid principal against unauthorized streams and receipts. Save `l06.json`. Pass when every route enforces its scope.
- [x] Lane 7. Present invalid server or peer certificates. Save `l07.json`. Pass when trust, hostname, cluster, and membership checks reject them without fallback.
- [x] Lane 8. Rotate and revoke credentials during traffic. Save `l08.json`. Pass when E24 honors the declared overlap and revocation bound.
- [x] Lane 9. Stop authorization refresh and restart secured nodes. Save `l09.json`. Pass when expired policy or missing trust material fails closed.
- [x] Lane 10. Search captured artifacts for synthetic secret canaries. Save `l10.json`. Pass when logs, manifests, command capture, and snapshots contain no secret material.

**Verify, perf.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked.

- [ ] Metric. Compare insecure-local traffic on both revisions and measure secured steady-state latency, connection setup, CPU, and memory separately.
- [x] Probe. Interleave same-mode controls. Run TLS and authorization workloads with pinned certificate, token, and connection-reuse settings.
- [x] Baseline. Record the insecure base first; label secured behavior absent rather than claiming a security speedup.
- [x] Rule. Apply B4 to equivalent local traffic and B1 to secured low-load operations. Require the declared revocation bound and zero authorization bypasses.

**Review gate.** The operator reviews security defaults and exposure before merge.

- [ ] Present mode, permission, rotation, and failure transcripts. Save explicit approval of the insecure override and revocation policy.
- [ ] Use a screenshot or video only as supplemental operator evidence; never expose secret values in media.

**Merge.**

- [ ] Record mandatory functional-security verdicts regardless of the optional independent-review flag. Stop at merge-ready.

## Package and operate the actual broker (LS09)

**Depends on.** LS08.

**Files.**

- [ ] Add release/container definitions, operator configuration, supported export/restore commands, health/metrics, and operator documentation.

**Build.**

- [ ] Produce a stripped single-engine release artifact with versioned storage and protocol compatibility.
- [ ] Add readiness distinct from liveness, per-group leadership/lag/queue metrics, graceful shutdown, and bounded operational diagnostics.
- [ ] Implement a declared quiescent export/restore contract. Establish a healthy quiescent cut and include required data and marker state; do not claim an online atomic cross-group backup.
- [ ] Refuse unsupported storage versions and invalid restore identities. Keep private credentials outside exported data.

**You see.**

- [ ] A clean environment starts the packaged broker, completes the user journey, and restores the supported artifact without a development checkout. Save `release-journey.json`.

**Verify, unit.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked.

- [ ] Add config, readiness, manifest, version, shutdown, export, and restore tests. Run the relevant workspace and packaging checks.

**Verify, live.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked. Ten independent lanes at the PR head.

- [ ] Lane 1. Regression lane against trunk. Run the same application journey from base and head release binaries. Save `l01.json`. Pass when package changes preserve the ledger.
- [ ] Lane 2. Run E25 in a clean environment. Save `l02.json`. Pass when no development checkout or undeclared runtime dependency is needed.
- [ ] Lane 3. Observe liveness without a write quorum. Save `l03.json`. Pass when readiness does not falsely advertise writable service.
- [ ] Lane 4. Drain accepted writes during shutdown. Save `l04.json`. Pass when acknowledged work survives and admission closes explicitly.
- [ ] Lane 5. Run the supported quiescent E26 export. Save `l05.json`. Pass when the artifact declares its exact cut, identities, and scope.
- [ ] Lane 6. Restore into empty storage. Save `l06.json`. Pass when the declared data and marker ledger match through the public API.
- [ ] Lane 7. Corrupt or truncate the export. Save `l07.json`. Pass when restore fails without publishing partial state.
- [ ] Lane 8. Run E27 for supported version fixtures. Save `l08.json`. Pass when data survives and unsupported formats are refused without reset.
- [ ] Lane 9. Inspect metrics under a slow follower and overload. Save `l09.json`. Pass when group lag, queue pressure, and rejection are observable without secrets.
- [ ] Lane 10. Exercise secured configuration in the package. Save `l10.json`. Pass when certificate mounts, redaction, and startup refusal match LS08.

**Verify, perf.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked.

- [ ] Metric. Compare the same broker workload in base/head release artifacts; measure package bytes, startup, idle RSS, diagnostics overhead, and restore duration.
- [ ] Probe. Interleave release runs and execute clean-start and B7 recovery fixtures with recorded cache treatment.
- [ ] Baseline. Save base release values first and record unsupported export operations separately.
- [ ] Rule. Apply B4, B0, the 50-MiB image budget, and the approved local recovery budget. Record B6 startup/RSS measurements here and enforce the full reference budget in LS10. Fail undeclared dependencies or secret-containing artifacts.

**Review gate.** The operator reviews the operational contract before merge.

- [ ] Present actual boot, shutdown, export, restore, and configuration transcripts. Save the operator's acceptance of supported recovery scope.
- [ ] Add a screenshot or video only where it helps the operator inspect the real command workflow.

**Merge.**

- [ ] Record packaged-artifact evidence and approved operations documentation. Stop at merge-ready.

## Complete end-to-end and independent-host acceptance (LS10)

**Depends on.** LS09.

**Files.**

- [ ] Complete `verification/` profiles and scenarios, CI entry points, generated acceptance reports, and the optional security-review artifact index.

**Build.**

- [ ] Run the complete suite against packaged artifacts through both CLI and client, with security disabled locally and enabled on reference hosts.
- [ ] Run long ingest/read/retention, cold replay, open-loop overload, election, partition, and catch-up workloads on approved independent hosts.
- [ ] If `independent_security_review` is true, run the read-only specialist workflow and resolve its release-blocking findings. Otherwise record `NOT_REQUESTED`.
- [ ] Generate one acceptance index that includes failures, blocked infrastructure, exact revisions, all raw evidence, and separate local/independent-host verdicts.

**You see.**

- [ ] The report proves the complete user journey and fault behavior through packaged binaries, or names the exact blocked or failed gate. Save `acceptance-index.json`.

**Verify, unit.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked.

- [ ] Run the full production and retained POC test suites, oracle-negative tests, and evidence-schema tests. Save actual command output.

**Verify, live.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked. Ten independent lanes at the PR head.

- [ ] Lane 1. Regression lane against trunk. Run the complete supported packaged journey at base and head. Save `l01.json`. Pass when existing behavior and newly added acceptance assertions agree.
- [ ] Lane 2. Run standalone CLI and SDK journeys in both security modes. Save `l02.json`. Pass when records, bookmarks, progress, and restart results match.
- [ ] Lane 3. Run the three-independent-host journey. Save `l03.json`. Pass when machine/disk independence is established and all acknowledged bytes remain readable.
- [ ] Lane 4. Crash the leader during sustained traffic. Save `l04.json`. Pass when E03 meets its recovery deadline without losing acknowledged work.
- [ ] Lane 5. Keep an old leader alive in a minority, then heal. Save `l05.json`. Pass when E04 fences stale success and catch-up completes.
- [ ] Lane 6. Delay a minority and then force snapshot catch-up. Save `l06.json`. Pass when E06 and E08 preserve majority progress and retained history.
- [ ] Lane 7. Run E28 with markers and protected/unprotected replay. Save `l07.json`. Pass when retention, leases, and reader outcomes match the oracle throughout.
- [ ] Lane 8. Run E29 and E30. Save `l08.json`. Pass when overload is bounded, service recovers, and cold replay returns exact bytes.
- [ ] Lane 9. Run security denial, rotation, and peer-identity cases across failover. Save `l09.json`. Pass when security guarantees survive topology changes.
- [ ] Lane 10. Restore the supported export and audit optional-review disposition. Save `l10.json`. Pass when E26 succeeds and review is either completed or explicitly not requested.

**Verify, perf.** Tests alone are not sufficient verification. A PR is verified only when its unit, live, and perf boxes are all checked.

- [ ] Metric. Measure equivalent base/head throughput and end-to-end p99, plus the approved long-run capacity, retention, resource, failover, and cold-replay metrics.
- [ ] Probe. Run interleaved reference-host comparisons and the full one-hour B5 workload with concurrent reads and retention. Preserve every failed attempt.
- [ ] Baseline. Measure the base packaged broker first under the same secured profile. Never substitute POC or same-host numbers.
- [ ] Rule. Require B3, B4, B5, B6, B7, and zero ledger or security violations. Missing independent hosts is `BLOCKED`, not a waived pass.

**Review gate.** None. LS10 is not interaction review-gated; the operator still owns release and deployment approval.

**Merge.**

- [ ] Record the exact-revision final verdict, security-review disposition, outstanding risks, and operator-owned landing steps. Do not deploy.

## Close the program

- [ ] Mark implementation-ready only when LS01 through LS09 and the complete local LS10 suite have passing evidence. Report publication or infrastructure blockers separately.
- [ ] Mark release-candidate-ready only when independent-host LS10 gates also pass and any selected security review is complete.
- [ ] Never claim security-reviewed when the independent review was not requested.
- [ ] Confirm every checked box has evidence, all failed attempts remain visible, and no owned workloads remain running unintentionally.
- [ ] Deliver the verified revision or stack, generated report index, artifact paths, security configuration, optional-review outcome, and unresolved risks. The operator lands and deploys.

## Appendix A. Prototype evidence

The [POC report](../experiments/high-volume.md) records executable measurements, not hypothetical rates.
Its main, scale, and record-size matrices completed 138 successful trials.
The sustained observations include five successes and one failed Fjall attempt.
The controlled slow-minority comparison completed six trials.

RocksDB delivered roughly 96% of Fjall's largest short-run median throughput with substantially lower batch p99.
The result supports RocksDB as the next baseline, not as a universally fastest engine.
The tested custom segment implementation paid several synchronization operations per batch.

The POCs also found a macOS sync mismatch and a stale-binary verification gap.
The code now normalizes the native full-sync path, builds before runs, and retains source and failure evidence.
Their fixed leader, shared disk, positional-only markers, and permanent isolation do not establish production Raft or named-bookmark behavior.

There was no Git repository or branch SHA for the original POCs.
Use their recorded binary/source hashes and [source checkpoint](../../evidence/source-checkpoint/README.md), not an invented commit.
Historical Python versions were not fully recovered.
The original failed Fjall stores and memory samples are unavailable, so its underlying cause and post-failure prefix survival remain unproven.

## Appendix B. Alternatives rejected

One installation-wide redb writer is not the high-volume default.
Keep redb and Fjall in the POC controls, but do not ship a multi-engine production abstraction without evidence that it is needed.

Do not promote the fixed-leader replication loop or permanent peer exclusion.
Production needs real consensus, fencing, independent replication progress, and catch-up.

Do not launch one database, cache, thread, or consensus group for every logical stream.
Bound groups and map many streams onto them.

Do not promise a globally consistent bookmark from independently collected partition cursors.
Do not implement distributed transactions, automatic repartitioning, Kafka compatibility, tiered storage, an identity provider, or online cross-group backup in this first program.

Do not make TLS/auth correctness optional merely because the independent review is optional.
Do not use screenshots, unit tests, healthy counters, or unlike benchmark ratios as substitutes for the end-to-end oracle.

## Appendix C. Risks

| Risk | Owner and required response |
| --- | --- |
| Raft storage ordering or snapshot assumptions are wrong. | LS02 and LS06 own conformance, crash-boundary evidence, and retained-payload recovery. |
| Metadata and data-group lifecycle diverge. | LS03 owns recoverable intents, identities, activation, deletion, and routing. |
| Marker semantics become cross-group transactions accidentally. | LS04 owns explicit partition-local versus independent-vector guarantees. |
| Lease expiry or retention breaks a replay promise. | LS05 owns ordered admission, clock assumptions, physical headroom, and race scenarios. |
| A slow minority throttles a majority or is excluded forever. | LS06 owns per-peer progress and real catch-up rather than the POC shortcut. |
| Retry, cancellation, or security identity changes duplicate effects. | LS02, LS07, and LS08 own stable receipts and principal binding. |
| Secured and insecure paths drift or secrets enter evidence. | LS08 owns route coverage, fail-closed startup, redaction, and both-mode E2E. |
| Export implies a stronger consistency scope than implemented. | LS09 owns the supported quiescent cut, identity policy, and exact restore oracle. |
| Short same-host results are sold as production capacity. | LS10 owns independent hosts, long runs, explicit budgets, and blocked verdicts. |
| CI or a coding agent lacks the required harness or hardware. | LS01 builds the harness; LS10 records external-resource blockers without substituting a proxy. |

## Appendix D. Links and reading list

- [Current proposal](../design/proposal.md).
- [Measured POC evidence](../experiments/high-volume.md).
- [Production verification obligations](../design/evaluation.md).
- [POC implementation contract](../../poc/CONTRACT.md).
- [POC storage mechanisms](../../poc/storage/README.md).
- [Independent POC review](../../evidence/review.md).
- [End-to-end contract for this program](verification.md).
- [Security contract for this program](security.md).
- [Coding-agent handoff](agent-task.md).

Existing code worth reading includes `poc/common/src/lib.rs`, `poc/storage/src/model.rs`, `poc/storage/src/rocksdb_store.rs`, `poc/runner/src/node.rs`, and `poc/scripts/run.py`.
Reuse proven ideas and tests, not the POC's protocol or synthetic-data assumptions.

Use `how` for the touched subsystem before each implementation package.
Use `architect` when the initial Raft adapter or domain boundary needs a real design decision.
Use `interrogate` for contested durability, lifecycle, or security decisions.
Use `show-me-your-work` for one canonical program decision trail.
When those tools are absent, perform their concrete investigation, independent critique, and evidence duties without inventing tool executions.

## Appendix E. Contracts to freeze in LS01

These are proposed implementation defaults.
Record operator changes before implementation, rather than letting different owners make incompatible choices.

### Create these planned modules

| Module | Responsibility |
| --- | --- |
| `crates/light-stream-core` | Stable domain identities, commands, deterministic state transitions, receipts, cursor semantics, and error classes. |
| `crates/light-stream-proto` and `proto/lightstream/v1` | Authoritative wire schemas, generated types, protocol versioning, and bounded boundary conversion. |
| `crates/light-stream-storage` | RocksDB Raft storage, committed application state, indexes, retention, and snapshot persistence. |
| `crates/light-stream-server` | Group runtime, catalog, routing, public/peer transports, batching, security, and operational controls. |
| `crates/light-stream-client` | Routing, sessions, bounded retry, pull reads, receipts, and bookmark workflows. |
| `crates/light-stream-cli` | The scriptable `light-streamctl` interface and stable JSON/error output. |
| `crates/light-stream-testkit` | Independent input generation, client/CLI driving, expected-data oracle, and workload measurement. |
| `scripts/verify.py` and `verification/` | Process/container/host lifecycle, faults, profiles, evidence capture, and verdict aggregation. |

Keep domain logic out of protocol adapters.
Keep the test generator and oracle out of production storage.
Use existing library contracts instead of designing another consensus protocol.

### Name the cursor and bookmark guarantees

A cursor includes cluster identity, immutable stream identity, partition identity, and the next record offset.
The next offset is inclusive when reading.
Offsets and published IDs never change after commit.
Reusing a human stream name does not reuse its identity.

Partition-local bookmark names and publication order belong to that partition's data group.
An atomic append-plus-bookmark is partition-local.
Do not send every such marker through the control group.

Stream-level named cursor vectors belong to a separate control-catalog namespace.
Their positions are independently committed observations, not a simultaneous or causally consistent cut.
Creation and listing must expose the target kind and scope.
Do not merge partition-local and stream-level publication sequences into an invented global order.

For a one-partition stream, the CLI may default to the partition scope.
For partitioned streams, require an explicit partition or independent-vector choice.
Protected replay is partition-local in v1.
Reject requests for a protected cross-group atomic replay guarantee rather than pretending several leases establish one.

### Bound retry and progress state

Producer sessions are bound to a principal, cluster, and partition.
Use server-issued session identities and monotone request sequences.
Store a payload fingerprint and stable result within a bounded receipt window.
Reject conflicting reuse, unknown sessions, and requests older than the window.
Do not transparently turn an expired retry into a new write.

Return the original receipt for a valid duplicate, even when the referenced marker was subsequently deleted.
Resolving that deleted marker remains a separate terminal result.
Apply authorization before serving cached receipts.

Consumer checkpoints are partition-scoped, mutable compare-and-set progress.
They do not move bookmarks, pin records implicitly, or promise exactly-once effects in an external database.
Automatic consumer-group balancing remains outside this program.

### Limit export and restore promises

LS09's first supported artifact is a quiescent logical export of selected streams, retained records, and bookmark metadata.
Enforce and record the quiescent scope.
Abort rather than publish an incomplete export when a required group is unavailable.

Restore into a fresh cluster by default and return an explicit identity mapping.
Do not silently bind old cursors or producer sessions to the new cluster.
Verify payload bytes, record order, and marker positions under the declared mapping.
Do not claim producer-session continuity, credential restoration, an online cross-group consistent cut, or a full-cluster backup.

This export contract does not weaken LS06.
Raft snapshots used for replica recovery must still include all state required to continue the same group, including retained history, receipts, leases, and progress.
