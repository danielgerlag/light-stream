# Measured results

Medians across repeated trials. Latency is the median of each trial's p99.
These are same-host fixed-leader POCs, not independent-host Raft results.

| Case | Engine | Trials | Records/s median | MiB/s median | p99 us | RPS min | RPS max |
| --- | --- | --- | --- | --- | --- | --- | --- |
| smoke-r1 | fjall | 1 | 4478 | 4.37 | 8903 | 4478 | 4478 |
| smoke-r1 | redb | 1 | 1640 | 1.60 | 20463 | 1640 | 1640 |
| smoke-r1 | rocksdb | 1 | 3980 | 3.89 | 8270 | 3980 | 3980 |
| smoke-r1 | segment | 1 | 1297 | 1.27 | 31679 | 1297 | 1297 |
| smoke-r3 | fjall | 1 | 2123 | 2.07 | 16245 | 2123 | 2123 |
| smoke-r3 | redb | 1 | 738 | 0.72 | 48274 | 738 | 738 |
| smoke-r3 | rocksdb | 1 | 1488 | 1.45 | 28195 | 1488 | 1488 |
| smoke-r3 | segment | 1 | 474 | 0.46 | 80109 | 474 | 474 |
