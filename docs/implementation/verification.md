# End-to-end verification contract

This document specifies verification to implement during LS01 to LS10.
The production commands, files, and profiles below are planned interfaces.
They do not exist yet.
The existing [POC commands](../../poc/README.md) remain separate controls.

## Verification boundary

Run the release server binary, not a storage library substituted for the broker.
Drive ordinary application work through the versioned public API, Rust client, and CLI.
Drive peer behavior through the real Raft transport.
Use mocks only in unit tests.

Read-only diagnostics may observe status, group leadership, replication lag, queue depth, and storage statistics.
Diagnostics must not insert records, repair data, advance commit indexes, or bypass authorization to make a live scenario pass.
Apply faults only to owned processes, containers, network proxies, and scratch devices.

Use a separate run directory for each revision, scenario, seed, and security profile.
One functional lane may run beside another only when their processes, ports, stores, and fault controls are isolated.
Run performance trials serially on shared hardware.
Ten logical live lanes do not authorize ten concurrent disk benchmarks on one laptop.

## Harness to create

Implement `scripts/verify.py` as the lifecycle and evidence coordinator.
Reuse the POC's safe process ownership, refusal to overwrite evidence, build-before-run gate, and failed-data preservation.
Do not copy its fixed-leader assumptions or synthetic-only store validation.

Implement a Rust verification driver under `crates/light-stream-testkit`.
Use the actual Rust client and direct CLI invocations as independent entry points.
Keep the oracle independent of server state-machine helpers.
Place scenarios and pinned workload profiles under `verification/`.

The planned interface is:

```sh
python3 scripts/verify.py --phase LS04 --lane 02 --profile local --revision HEAD --artifacts artifacts/run-id
python3 scripts/verify.py --suite e2e --profile local --security all --artifacts artifacts/run-id
python3 scripts/verify.py --suite perf --profile local --base BASE_SHA --head HEAD_SHA --artifacts artifacts/run-id
python3 scripts/verify.py --suite acceptance --profile reference-hosts --security secured --artifacts artifacts/run-id
```

Reject unknown scenarios and incomplete profiles.
Resolve revisions and build before starting a run.
Record `UNSUPPORTED` when a baseline revision lacks a feature.
That is not a passing execution of the feature and is not a zero-valued performance result.

The production CLI is planned as `light-streamctl`.
Its JSON output is the automation contract.
The exact command grammar must be settled and recorded in LS01 before the client and CLI diverge.

## Independent data oracle

Generate arbitrary binary and structured payloads, including variable record sizes, keys, and headers.
Use a recorded seed or saved input corpus.
The server must not know the generator rule.
Assign each attempted operation a stable producer identity, request sequence, and payload digest.

Persist these ledgers outside the broker's data directories:

| Ledger | Contents |
| --- | --- |
| `attempts.jsonl` | Every attempted operation, its identity, input digest, and client timestamp. |
| `acks.jsonl` | Successful results, partition offsets, bookmark IDs, and client-observed completion time. |
| `errors.jsonl` | Error class, attempt duration, retry identity, and whether the outcome is definite or ambiguous. |
| `reads.jsonl` | Returned offsets, record identities, payload digests, and terminal read status. |
| `faults.jsonl` | Target resource, action, timestamp, and confirmation that the fault actually occurred. |

Synchronize the acknowledgement ledger before injecting a fault that depends on a successful write.
After restart or election, read the acknowledged data through the public API.
Compare every acknowledged record's bytes and identity, not only a count.
Validate partition order and immutable offset assignment.

A timeout or lost response is ambiguous.
It may have committed.
Resolve it through the producer receipt or an authorized read after quorum returns.
Do not assert that every failed call is absent.
Definite validation and authorization rejections must have no committed side effects.

Require at most one committed result for the same producer request identity.
A retry with a conflicting payload must fail.
Retrying an operation whose bookmark was later deleted must not recreate that bookmark.
An extra unacknowledged tail must not bypass acknowledged-prefix validation.

## Topologies

| Profile | Topology and permitted claim |
| --- | --- |
| `local` | One-node standalone and three separately addressed processes or containers on one host. Functional and local regression evidence only. |
| `local-faults` | Three owned nodes behind per-link fault proxies, plus a replacement learner. Real elections, partitions, and catch-up on shared hardware. |
| `reference-hosts` | Three distinct Linux hosts with independent local disks and a separate load generator. Required before independent-host HA or capacity claims. |

The initial local resource envelope is one CPU and 1 GiB per node, with a separately budgeted driver.
Do not treat it as the capacity reference.
The proposed reference envelope is eight CPUs, 16 GiB RAM, local NVMe, and 10-Gbit networking per node, plus an adequately provisioned separate driver.
The operator must approve or replace that envelope before LS10.
Record the actual machines and storage, not merely their requested specifications.

Do not provision paid hosts or modify shared host firewalls without authorization.
If the independent-host profile cannot run, mark that gate `BLOCKED`.
Local functional E2E remains mandatory.

## Scenario catalog

Every scenario saves a summary, ledgers, logs, and the actual control actions.
The plan's per-PR lanes select and extend these scenarios.

| ID | Scenario | Pass predicate |
| --- | --- | --- |
| E01 | Standalone application journey | Create a stream, publish arbitrary records, consume through CLI and SDK, restart, and read identical committed bytes. |
| E02 | Three-node application journey | Publish and consume through different endpoints while leaders differ by group; routing and results agree. |
| E03 | Leader crash during traffic | The remaining quorum elects a leader and resumes within the approved deadline; all acknowledged data remains readable. |
| E04 | Live old leader in a minority | The isolated old leader cannot acknowledge writes or serve a supposedly fresh read; healing restores it through real catch-up. |
| E05 | Majority unavailable | New writes return the specified quorum error with zero successful acknowledgements; there is no local-only durability fallback. |
| E06 | Slow minority | Delay one follower without delaying the other links; the healthy majority progresses, then the lagging follower catches up without permanent exclusion or missing entries. |
| E07 | Log-suffix catch-up | A stopped follower returns while its missing history is still retained and reaches the same committed state. |
| E08 | Snapshot catch-up | A follower missing purged Raft history installs a snapshot containing all retained payloads and bookmark state, not metadata alone. |
| E09 | Lost acknowledgement and retry | Commit a request but drop its response; a retry returns the original offsets and IDs exactly once. |
| E10 | Named bookmark and pagination | Publish, list, and resume from markers; backdated targets do not change publication ordering; concurrent creation does not duplicate pages. |
| E11 | Atomic append and marker | A failed or retried operation never exposes only half of its committed batch-and-bookmark result. |
| E12 | Expired data and retained marker | Payload expiry leaves permitted bookmark metadata visible; resume fails explicitly without skipping to a newer offset. |
| E13 | Protected replay | Admit a bounded range lease, run retention concurrently, and return the complete promised range before expiry. |
| E14 | Unleased replay | Retention may interrupt a multi-request replay, but the client receives explicit expiration rather than a shortened success. |
| E15 | Retention and pin admission race | One ordered group decision prevents a pin from resurrecting logically expired data or retention from violating an active promise. |
| E16 | Quotas and slow readers | Queues and memory remain bounded; rejected work is visible; read transactions do not remain open while clients process data. |
| E17 | Stream recreation | Reusing a name creates a new identity; old cursors, markers, and producer state cannot silently bind to it. |
| E18 | Multi-partition bookmark vector | Each position is valid for its partition; the result explicitly lacks a globally consistent-cut guarantee. |
| E19 | Storage and snapshot corruption | Invalid checksums, identities, versions, or incomplete snapshot installation produce explicit failure or replica repair, never silent truncation of committed data. |
| E20 | Node replacement | A learner catches up before becoming a voter; membership changes and leader transfer preserve the acknowledged ledger. |
| E21 | Mutable consumer checkpoint | Compare-and-set progress survives restart and failover without moving shared bookmarks. |
| E22 | Security disabled | The approved local profile works without credentials and reports its mode; unintended insecure remote binding is refused. |
| E23 | Secured client and peer traffic | Valid identities work; invalid tokens, certificates, roles, and peer identities fail without side effects. |
| E24 | Security rotation and revocation | The approved rotation window works, revoked credentials stop working within the declared bound, and no plaintext fallback occurs. |
| E25 | Release artifact | A clean machine or container starts the packaged binary and completes E01 and E02 without a development checkout. |
| E26 | Quiescent backup or export restore | Restore the supported artifact into empty storage and compare its declared data, marker, and identity scope exactly. |
| E27 | Upgrade and restart | Supported version transitions preserve data and metadata; unsupported formats are refused without reinitializing storage. |
| E28 | Concurrent ingest, reads, and retention | A long run preserves the live retained window, marker policy, and bounded resources under simultaneous work. |
| E29 | Overload and recovery | Open-loop offered load above capacity causes bounded, explicit rejection; reducing load restores service without corruption. |
| E30 | Cold replay | On a controlled dedicated host or fresh volume, the requested history is read correctly and the cache treatment is recorded. |

## Performance protocol

The current POC rates are evidence for design choices, not an apples-to-apples production Raft baseline.
Never claim a speedup by comparing the production broker with a fixed-leader POC or by changing durability.

For an existing behavior, measure the base revision first, then run at least five interleaved base/head repetitions on the same hardware.
Pin security mode, sync semantics, compression, record distribution, batch limits, producer count, reader count, partition count, retained data, and offered load.
Keep total producer concurrency fixed when isolating a partitioning change.
Report configuration-scaling curves separately.

Record throughput, per-request latency, end-to-end latency including batching and retries, queue wait, error counts, timeout counts, CPU, RSS, device writes where observable, and disk footprint.
Record scheduled offered load, actual sends, scheduling lag, and successful completion rate.
Do not pass a capacity gate by undersending, silently dropping work, or returning fast rejections.
Do not equate file size with write amplification.
Do not pool only successful sustained attempts.
Save raw samples and their chronological timing.

If the base lacks the feature, record that absence.
Measure shared work on both revisions where meaningful.
Apply an absolute budget to the new operation and its complete user-visible result.
Do not invent a ratio between unlike operations.

These are proposed execution budgets to lock or revise in LS01 before seeing implementation results:

| Budget | Initial rule |
| --- | --- |
| B0 | Local process liveness within 5 seconds; broker write-readiness within 10 seconds once the required quorum exists. |
| B1 | Local healthy API probes at the pinned low offered load have p99 at or below 250 ms; each request has a finite deadline. |
| B2 | Local leader-loss recovery completes within 10 seconds after a viable quorum remains; no acknowledged record is lost. |
| B3 | Listing the newest 100 markers from 100,000 entries has p99 at or below 100 ms on the reference profile and performs no payload scan. |
| B4 | Compared with an equivalent supported base scenario, fail a median throughput decrease above 10% or median trial p99 increase above 20%. |
| B5 | Capacity profile schedules 50,000 1-KiB records/s for one hour across eight groups with bounded batches, concurrent readers, and continuous retention. Mean successful completion is at least 49,500 records/s, all scheduled records resolve successfully within the bounded drain, and end-to-end p99 is at most 150 ms. Healthy-run timeouts, rate-limit rejections, unavailable responses, and silent drops fail this gate. |
| B6 | Empty standalone reference startup is at most 1 second, idle RSS at most 128 MiB for ten streams mapped to bounded groups, and compressed single-engine Linux image at most 50 MiB. |
| B7 | Recovery and catch-up fixture contains at least 1 GiB retained per group; its explicit deadlines and disk headroom are recorded before the run. |

B1's initial low-load fixture uses 2,000 1-KiB records/s, batches capped at 128 records, four producers, and one reader.
The fixture can have fewer groups in early phases, but its exact topology is recorded.
LS09 measures startup and RSS locally; LS10 enforces B6 on the approved reference profile.

B5 and B6 are proposed acceptance targets, not established capabilities.
If the operator changes them, record the profile revision before implementation or comparison.
Do not relax them silently after failures.
If noise prevents a conclusion, report `INCONCLUSIVE` and investigate the observation method.

## Evidence and verdicts

Each run saves:

```text
artifacts/<revision>/<phase>/<lane>/<run-id>/
  manifest.json
  source/
  commands.jsonl
  attempts.jsonl
  acks.jsonl
  errors.jsonl
  reads.jsonl
  faults.jsonl
  node-logs/
  samples/
  result.json
```

The manifest includes exact code and binary identity, dependency lockfile, build flags, environment, security profile, workload, seed, resource limits, and fault schedule.
Do not archive private keys, tokens, authorization headers, or customer data.
Preserve failed stores under a restricted local diagnostic directory and record the path separately from publishable evidence.

Use `PASS`, `FAIL`, `BLOCKED`, or `INCONCLUSIVE`.
An optional independent security review that was not selected uses `NOT_REQUESTED`, never `PASS`.
A missing live or performance gate prevents the PR verdict.
Independent-host gates remain distinct from same-host gates in the final report.
