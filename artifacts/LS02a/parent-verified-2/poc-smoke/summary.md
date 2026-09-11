# Measured results

Medians across repeated trials. Latency is the median of each trial's p99.
These are same-host fixed-leader POCs, not independent-host Raft results.

| Case | Engine | Trials | Records/s median | MiB/s median | p99 us | RPS min | RPS max |
| --- | --- | --- | --- | --- | --- | --- | --- |
| smoke-r1 | fjall | 1 | 4250 | 4.15 | 8072 | 4250 | 4250 |
| smoke-r1 | redb | 1 | 1639 | 1.60 | 20382 | 1639 | 1639 |
| smoke-r1 | rocksdb | 1 | 3925 | 3.83 | 8984 | 3925 | 3925 |
| smoke-r1 | segment | 1 | 1281 | 1.25 | 31060 | 1281 | 1281 |
| smoke-r3 | fjall | 1 | 2026 | 1.98 | 18020 | 2026 | 2026 |
| smoke-r3 | redb | 1 | 755 | 0.74 | 52062 | 755 | 755 |
| smoke-r3 | rocksdb | 1 | 1385 | 1.35 | 34398 | 1385 | 1385 |
| smoke-r3 | segment | 1 | 446 | 0.44 | 83111 | 446 | 446 |
