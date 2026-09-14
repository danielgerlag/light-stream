# LS05 evidence

LS05 implements monotonic retention floors, bounded replay leases, explicit cursor expiry, safe-time lease expiry, durable mutation retries, bounded logical reclaim, and legacy record migration.

## Accepted runs

- `final-4/` is the final release-binary run.
- `skill-retention-replay-final-3/` is the mapped verification-skill run.
- `retention-journey.json` records the floor, lease latency, logical reclaimed bytes, and Raft-owned bytes.
- `l01.json` through `l10.json` cover unchanged reads, expired bookmarks, protected and unprotected replay, ordering, quotas, response loss, idle expiry, stalled readers, and restart.
- `perf-final-2/result.json` compares five interleaved LS04 and LS05 trials. LS05 retained 102.0% of baseline publish throughput. Publish p99 was 102.8% of baseline, and fetch p99 was 93.9%.

## Retained iterations

- `iteration-1` exposed a missing cumulative byte boundary after reclaim.
- `iteration-2` passed before live response-loss coverage.
- `iteration-3` exposed a nested route field in the fault command.
- `iteration-4` added live dropped-response retries.
- `iteration-5` added autonomous idle maintenance.
- `final` passed before the first independent code review.
- `final-2` passed after the first review fixes.
- `final-3` passed before the cross-partition quota regression was added.

## Limitation

LS05 proves logical reclaim. Retained Raft log entries still own expired payloads. Filesystem payload reclamation, snapshot catch-up, and partial-node recovery remain LS06.
