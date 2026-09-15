---
name: verify-light-stream
description: Run Light Stream's real release binaries and preserve evidence for LS01 through LS05, including retention and protected replay.
---

# Verify Light Stream

Use the repository root as the working directory.

Read `feature-map.json` and select one feature. Use a new artifact directory for every run because the verifier refuses to overwrite evidence.

Run the mapped command exactly, replacing only the final artifact directory with a path that does not exist.

For the full LS01 phase, run:

```sh
python3 scripts/verify.py \
  --phase LS01 \
  --profile local \
  --security all \
  --artifacts artifacts/LS01/skill-full
```

For the implemented end-to-end suite, run:

```sh
python3 scripts/verify.py \
  --suite e2e \
  --profile local \
  --security all \
  --artifacts artifacts/LS01/skill-e2e
```

For the LS02a durable journey, run:

```sh
python3 scripts/verify.py \
  --phase LS02a \
  --profile local \
  --artifacts artifacts/LS02a/skill-full
```

For the LS02b three-voter journey, run:

```sh
python3 scripts/verify.py \
  --phase LS02b \
  --scenario three-voter \
  --profile local \
  --artifacts artifacts/LS02b/skill-three-voter
```

For the LS03 bounded-group journey, run:

```sh
python3 scripts/verify.py \
  --phase LS03 \
  --profile local \
  --security all \
  --artifacts artifacts/LS03/skill-bounded-groups
```

For the LS04 bookmark journey, run:

```sh
python3 scripts/verify.py \
  --phase LS04 \
  --scenario bookmarks \
  --profile local \
  --security all \
  --artifacts artifacts/LS04/skill-bookmarks-final
```

For the LS05 retention and protected replay journey, run:

```sh
python3 scripts/verify.py \
  --phase LS05 \
  --scenario retention-replay \
  --profile local \
  --security all \
  --artifacts artifacts/LS05/skill-retention-replay-final-3
```

For the LS06 snapshot recovery journey, run:

```sh
python3 scripts/verify.py \
  --phase LS06 \
  --profile local \
  --security all \
  --artifacts artifacts/LS06/skill-banked-install-final
```

For the LS06 B7 recovery journey, run:

```sh
python3 scripts/verify.py \
  --phase LS06 \
  --scenario b7-snapshot \
  --profile local \
  --security local-insecure \
  --artifacts artifacts/LS06/b7-1g-4
```

For LS06 voter replacement and leader transfer, run:

```sh
python3 scripts/verify.py \
  --phase LS06 \
  --scenario membership \
  --profile local \
  --security all \
  --artifacts artifacts/LS06/membership-multigroup-3
```

Inspect `result.json`. Accept only `PASS`. Inspect `cleanup.json` and confirm that `success_data_removed` is `true`. For the evidence-preservation feature, also confirm that `failed_data_retained` is `true`.

For LS02a, inspect `durable-journey.json`, `receipt-evidence.json`, and `storage-test-evidence.json`. Confirm that the CLI and Rust client match the independent ledger before and after restart. Confirm that the discarded response retry returns offset `2` and the conflicting retry returns `receipt_conflict`.

For LS02b, inspect `ls02b-summary.json` and `e02.json` through `e09.json`. Confirm exact committed and effective memberships for both groups on every recorded node. Confirm empty learner sets and typed data-leader hints that match diagnostics and topology. Confirm that catch-up includes exact leader replication progress for a non-leader target.

Inspect `e04.json` and `e05.json`. Confirm that the live old leader returns no publish acknowledgement and refuses fetch and receipt calls after the other two voters stop. Treat the selective-partition case with a healthy remote majority as `NOT_TESTED_LOCAL`.

Inspect `e09.json`. Confirm that the verifier drops the first response, kills the diagnosed data leader, elects a different leader, reads the exact receipt before retry, and receives the same range from the retry and after full restart. `e08.json` must report `UNSUPPORTED_LS06`.

For LS03, inspect `partition-journey.json` and `l02.json` through `l10.json`. Confirm five databases per node, four bounded data groups, partition routes spanning groups 2 through 5, byte-for-byte reads, response-loss create idempotency, quota rejection, name reuse with a new stream identity, stale-route refresh, group-local delay isolation, and full-cluster restart. `l04.json` records single-process multi-group recovery as `DEFERRED_LS06`.

For LS04, inspect `bookmark-journey.json` and `l01.json` through `l10.json`. Confirm atomic publish-plus-bookmark, exact resume, response-loss retry with one bookmark ID, backdated publication order, stable pagination, terminal deletion and name reuse, independent stream cursor vectors, a last-100 lookup below 100 ms over 10,000 records, and full-cluster restart. `l10.json` records partial-node failover as `DEFERRED_LS06`.

For LS05, inspect `retention-journey.json` and `l01.json` through `l10.json`. Confirm that bookmark metadata survives payload expiry, ordinary fetch returns `cursor_expired`, protected replay returns the exact admitted range, renewal and release survive dropped responses, idle maintenance expires abandoned leases, reclaim work stays bounded, and full restart preserves floors and lease lifecycle. Treat `raft_only_bytes` as retained by the Raft log. Filesystem payload reclamation and partial-node recovery remain `DEFERRED_LS06`.

For LS06, inspect `ls06/snapshot-recovery.json`. Confirm that a stopped follower falls behind a completed snapshot and purge, crashes after an acknowledged chunk, resumes from the durable offset, reaches the leader's committed and applied index, preserves exact record bytes, and removes completed staging files.

For B7, inspect `ls06/b7-plan.json`, `ls06/b7-calibration.json`, and `ls06/b7-recovery.json`. Confirm at least 1 GiB retained, the locked post-load RSS budget, the 64 MiB interrupted offset, the resumed 1,026-chunk transfer, all 1,025 records verified from the stopped repaired node, and successful scratch cleanup. Keep independent-host verification `BLOCKED`.

For membership administration, inspect `ls06/membership-recovery.json`. Confirm that the coordinator dies after the durable intent, voter 4 joins through learner catch-up, every group converges to `{1,2,4}`, voter 3 restarts as `retired`, data leadership moves to the requested voter, an unreachable replacement aborts at topology revision 5, group 3 serves traffic during replacement, and the final membership survives a full restart.

Return the command, verdict, source revision, binary fingerprints, evidence files, and cleanup result. Report secured mode as `UNSUPPORTED_LS08`. Report independent-host verification as `BLOCKED`.
