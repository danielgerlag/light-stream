# Coding-agent task

Implement the high-volume streaming broker described in [the plan](plan.md), using [the proposal](../design/proposal.md) and [the measured evidence](../experiments/high-volume.md).
Treat this task as authorization to implement only after the operator explicitly says to execute it.

Read the entire plan, [verification contract](verification.md), and [security contract](security.md) before editing.
Work through LS01 to LS10 in dependency order.
One coding agent can own the sequence.
Delegate only disjoint work or independent verification.
Do not require Copilot-specific plugins when equivalent agent controls and ordinary commands are available.

Use RocksDB as the initial production storage implementation and an established Raft library.
Keep a bounded number of independent writer and replication groups.
Do not promote the fixed-leader POC, permanent peer isolation, deterministic test payload restriction, or POC marker-lifetime policy into the broker.
Preserve `poc/` and historical `evidence/`.

Implement a native versioned API, a Rust client, and a CLI.
Deliver durable append, bounded reads, real failover and catch-up, producer retry receipts, immutable named bookmarks, recent-bookmark listing, retention, and bounded replay protection.
Support configurable TLS, authentication, and authorization.
Security-disabled mode is for explicit trusted development use, not a silent fallback.

Create the real end-to-end harness before the data path.
Drive the actual built server through the client and CLI.
Capture attempted requests, durable client acknowledgements, read-back bytes, process logs, fault actions, source snapshots, and raw performance samples.
Use independent expected-data generation and a SHA-256 ledger.
Do not treat health endpoints, live counters, unit tests, or an open TCP port as end-to-end proof.

The execution inputs are the base branch, artifact directory, local or independent-host profile, and `independent_security_review`.
The review flag defaults to false.
Its value does not disable functional security tests.
Confirm the Git checkout and publication authority before branch or remote operations.

Advance a work package only when its unit, live, and performance gates have evidence at the exact revision.
Retain failed attempts and their data.
Report `PASS`, `FAIL`, `BLOCKED`, or `INCONCLUSIVE` honestly.
Never raise timeouts or lower performance thresholds after a failure without an explicit, recorded plan revision.

Stop at implementation-ready or release-candidate-ready as defined in the plan.
If independent hosts are unavailable, finish the local work and report the independent-host gate as blocked.
Do not substitute containers on one host or the existing POC numbers for that gate.
Return the changed files, exact revision, evidence index, outstanding gates, and security-review disposition.
Do not merge or deploy without separate operator authorization.
