# LS03 evidence

LS03 implements a bounded pool of data Raft groups, a durable stream catalog, partition routing, stream lifecycle, stale-route refresh, and process-wide RocksDB budgets.

## Accepted run

- `final-2/result.json` is the final passing real release-binary run.
- `final-2/partition-journey.json` records four data groups, routed partitions, committed byte digests, leaders, and independent progress.
- `final-2/l02.json` through `l10.json` record bounded stores, routing, create idempotency, deletion and name reuse, stale-route refresh, targeted group delay, quotas, and restart.
- `final-2/cleanup.json` confirms disposable stores were removed and evidence retained.
- `final-2/unsupported.json` records LS04, LS05, LS06, LS08, and independent-host gaps.
- `skill-bounded-groups-final/` is the mapped verification-skill execution against the final source.

## Retained failed runs

`continued-1` through `continued-11` preserve reconstruction and verifier failures. They do not count as passing evidence.

## Limitation

The create response-loss path is verified. Single-process recovery with all bounded data groups active exposed incomplete readiness and is deferred to LS06. The passing LS03 run performs a clean full-cluster restart and verifies catalog and data afterward.
