# High-volume POC evidence

Captured on 2026-09-10. These are real Rust storage and TCP replication experiments, not a shipping broker.

## Outcome

The evidence no longer supports one installation-wide redb writer as the high-volume default.
Batched, independent writer paths outperform that small-installation proposal in the tested configurations.
Fjall and RocksDB are the storage finalists. The custom segment POC's per-batch manifest synchronization is expensive.

Fjall rose from **12,097 records/s** with one shard and 128-record batches
to **78,600 records/s** with eight shards and 512-record batches.
That is **6.50x** across those configurations, not an isolated sharding effect.

**A sustained Fjall attempt failed.** Two Fjall attempts completed and one lost the fixed leader's acknowledgement quorum.
The failure remains part of the result. Successful-run throughput is not evidence of sustained reliability.

RocksDB is the recommended baseline for the next high-volume iteration.
At eight shards and 512-record batches it delivered **96.1%** of Fjall's median throughput,
with median trial p99 **92.31 ms**, versus **895.38 ms** for Fjall.
Fjall remains a useful challenger, not the default selected from throughput alone.

## What ran

| Experiment | Completed successful trials | Other outcomes |
| --- | --- | --- |
| Four engines, single/batched writes, one/four shards, one/three nodes | 72 | Three repeats per configuration. |
| Fjall/RocksDB, one/two/four/eight shards, 128/512-record batches, three nodes | 48 | Three repeats per configuration. |
| 64-byte, 1-KiB, and 16-KiB records with three nodes | 18 | Three repeats per engine and size. |
| 30-second sustained attempts with eight shards and three nodes | 5 | One failed attempt; the interrupted schedule is not represented as complete. |
| Controlled slow-peer block/isolate comparison | 6 | Three repeats per admission policy. |
| Follower loss, majority loss, process-kill recovery | 4 engine scenarios | See the dedicated failure evidence and its gate revision. |

Host: **Apple M5**, **10 logical CPUs**,
**32 GiB RAM**, `rustc 1.96.1 (31fca3adb 2026-06-26)`.
Platform: `macOS-26.6.2-arm64-arm-64bit`.

Nodes are separate OS processes with separate stores over loopback TCP.
They share one physical host and filesystem. No independent-host throughput or fault-domain claim follows.
The comparison executable uses a fixed leader, not Raft. It has no election, fencing, or automatic replica catch-up.

## Four storage implementations

Three nodes, four shards, 128 records per batch, 1 KiB per record. Medians of three trials.

| Engine | Records/s | Payload MiB/s | Median trial p99, ms per batch | Peak summed node RSS, MiB |
| --- | --- | --- | --- | --- |
| segment | 4,692 | 4.58 | 133.95 | 31.1 |
| redb | 9,811 | 9.58 | 107.61 | 173.8 |
| fjall | 24,427 | 23.85 | 31.99 | 197.6 |
| rocksdb | 15,902 | 15.53 | 45.39 | 143.1 |

The segment implementation synchronizes data, a replacement manifest, and the directory for each batch.
Its result applies to that conservative implementation, not to all possible segment logs.
redb enables immediate durability and quick-repair. Fjall and RocksDB use synchronous journals with compression disabled.
All four atomically store the payload and an after-batch positional marker.

## Batching and independent writer paths

Three nodes and 1-KiB records. Each shard also adds one concurrent producer.

| Shards | Records per batch | Fjall records/s | Fjall p99 ms | RocksDB records/s | RocksDB p99 ms |
| --- | --- | --- | --- | --- | --- |
| 1 | 128 | 12,097 | 22.12 | 10,670 | 24.01 |
| 1 | 512 | 34,297 | 54.09 | 33,304 | 37.15 |
| 2 | 128 | 14,490 | 33.91 | 11,227 | 30.09 |
| 2 | 512 | 48,107 | 87.25 | 40,833 | 44.81 |
| 4 | 128 | 24,077 | 29.68 | 15,822 | 44.35 |
| 4 | 512 | 57,695 | 198.75 | 55,409 | 62.18 |
| 8 | 128 | 39,439 | 40.03 | 21,832 | 71.18 |
| 8 | 512 | 78,600 | 895.38 | 75,532 | 92.31 |

This is whole-configuration scaling. Shard count and client concurrency change together.
Larger batches amortize persistence costs but alter request latency and memory use.
These client-supplied batches do not measure a server-side group-commit batching timer.

## Record size changes the ranking

Three nodes, four shards, 128 records per batch.

| Record bytes | Fjall records/s | Fjall MiB/s | RocksDB records/s | RocksDB MiB/s |
| --- | --- | --- | --- | --- |
| 64 | 24,773 | 1.51 | 17,924 | 1.09 |
| 1024 | 23,195 | 22.65 | 15,961 | 15.59 |
| 16384 | 7,134 | 111.46 | 9,133 | 142.70 |

## Sustained attempts and the failure

The longer workload uses eight shards, 512-record batches, three nodes, a 30-second limit, and a 3-GiB logical byte cap.
Per-shard traffic exceeds the configured memory buffers and exercises LSM flushes.
This is longer ingestion, not a steady-state concurrent-retention workload.

| Engine | Successful attempts | Failed attempts | Successful-run median records/s | Successful-run median p99 ms |
| --- | --- | --- | --- | --- |
| fjall | 2 | 1 | 71,383 | 231.17 |
| rocksdb | 3 | 0 | 59,815 | 112.61 |

The failed Fjall attempt stopped after about 7.43 seconds near 39 MiB of acknowledged payload per shard.
Successful acknowledgements before the failure reached 4.81 seconds.
The fixed leader timed out follower exchanges and permanently disabled the affected peer workers.
The logs establish timeout-driven loss of the acknowledgement quorum, not a specific storage-engine or hardware root cause.
A fresh 30-second Fjall attempt completed, so the failure is intermittent in this evidence.

The original failed attempt did not retain its scratch stores or memory samples.
Its acknowledged-prefix survival was not audited after failure. Later orchestration preserves failed data and memory.
The two successful Fjall durations do not erase that missing evidence.

## Controlled slow-minority experiment

One real follower delays acknowledgements by 250 ms after durable storage.
Each trial appends the same 24 batches of 128 1-KiB records through three logical nodes.
The block control waits for capacity in every peer queue.
The isolate alternative permanently excludes a peer when its bounded queue fills, while still requiring local durability and the fast follower.

| Admission | Trials | Median elapsed seconds | Median records/s | Median trial p99 ms | Slow follower stored batches, min-max |
| --- | --- | --- | --- | --- | --- |
| block | 3 | 4.920 | 624 | 263.59 | 24-24 |
| isolate | 3 | 0.245 | 12,547 | 13.03 | 5-5 |

The isolate POC completed this controlled workload **20.09x** faster.
Both the leader and fast follower were reopened and matched the acknowledged payloads.
After isolation, removing the fast follower produced an explicit quorum error with zero new acknowledgements.
The slow follower does not receive the complete interval under isolation. It is explicitly degraded, not a third fully caught-up copy.
Permanent exclusion is only the POC mechanism. Production needs independent replication cursors and catch-up from durable history.
This demonstrates the queue-coupling defect. It does not establish the cause of the earlier intermittent sustained Fjall failure.

## Durability normalization and rejected evidence

The first smoke run made RocksDB appear dramatically faster because its native build used plain macOS `fsync`.
Rust's synchronization used `F_FULLFSYNC`.
The workspace now enables RocksDB's existing `HAVE_FULLFSYNC` path on Apple targets.
Both the build flag and the linked executable's full-sync call are captured.
No engine's synchronization was weakened to improve its result.

The first main run also stopped on a stale executable's old bookmark limit.
The orchestrator now builds before each run by default.
Neither rejected directory contributes to the tables above.

## What the evidence does and does not establish

The main, scaling, size, and successful sustained trials compare client-expected batch counts and digests with bytes reopened from all three replicas or the standalone store.
The scanner decodes checksums, regenerates deterministic payloads for byte comparison, and verifies absolute marker offsets.
Those storage trials also apply retention and reopen the retained state.
The marker is positional. Human names, independent bookmark lifetimes, leases, and consumer progress are not implemented.

Latency is closed-loop, not an open-loop overload SLO. p99 is per batch, not per individual record.
Main trials are about two seconds. Scale trials target five seconds. Byte caps and actual sample counts remain in raw files.
Caches are not cleared. Replay is warm-cache validation and throughput, not a cold-storage result.
Retention runs after ingest. Database file size is not device write amplification.
Summed RSS can count shared pages repeatedly, and excludes the benchmark client's memory.
OS synchronization and process-kill recovery do not prove hardware power-loss survival.

## Reproduce and inspect

- [POC commands and limits](../../poc/README.md).
- [Main raw evidence and summary](../../evidence/main-v2/summary.md).
- [Shard/batch scaling evidence](../../evidence/scale-v1/summary.md).
- [Message-size evidence](../../evidence/message-sizes/summary.md).
- [Interrupted sustained run](../../evidence/sustained-v1/run.json).
- [Fresh Fjall sustained attempt](../../evidence/sustained-repro/summary.md).
- [Third RocksDB sustained attempt](../../evidence/sustained-rocks-completion/summary.md).
- [Follower/majority failure scenarios with stronger gates](../../evidence/failures-v2/run.json).
- [Synchronization investigation](../../evidence/durability-review.md).
- [Controlled isolation raw results](../../evidence/slow-peer-v1/run.json).
- [Decision trail](../../evidence/decisions.tsv).
- [Independent review and remaining limits](../../evidence/review.md).
- [High-volume design direction](../design/proposal.md).

Regenerate this report with `python3 poc/scripts/report.py --slow-peer-run evidence/slow-peer-v1`.
