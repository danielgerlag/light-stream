# LS04 evidence

LS04 implements immutable partition bookmarks, atomic publish-plus-bookmark, stable recent listing, deletion and name reuse, and independent stream cursor vectors.

## Accepted runs

- `final-3/` is the final release-binary run for the implemented source.
- `skill-bookmarks-final/` is the mapped repository verification-skill run.
- `bookmark-journey.json` records the exact resume cursor, response-loss bookmark identity, stream vector, and full restart.
- `l01.json` through `l10.json` cover ordinary publish and fetch, atomicity, response loss, backdated order, stable pagination, deletion, vectors, the 10,000-record lookup, and restart.
- `cleanup.json` confirms that disposable RocksDB stores were removed.

## Retained iterations

`iteration-1` proved partition bookmarks before stream vectors were complete. `iteration-2` and `iteration-3` preserved verifier failures that exposed incomplete wire-error classification. `iteration-4` is the first complete passing run. `final/` preserves a performance-gate failure caused by treating the maximum of 20 samples as p99. `final-2` and `skill-bookmarks` passed before the verification profile label was advanced from LS03 to LS04.

## Limitation

Clean full-cluster restart is verified. Partial-node failover with the current grouped manifest remains deferred to LS06 and is not claimed by LS04.
