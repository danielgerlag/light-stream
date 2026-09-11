# Measured results

Medians across repeated trials. Latency is the median of each trial's p99.
These are same-host fixed-leader POCs, not independent-host Raft results.

| Case | Engine | Trials | Records/s median | MiB/s median | p99 us | RPS min | RPS max |
| --- | --- | --- | --- | --- | --- | --- | --- |
| smoke-r1 | fjall | 1 | 4206 | 4.11 | 8734 | 4206 | 4206 |
| smoke-r1 | redb | 1 | 1646 | 1.61 | 20343 | 1646 | 1646 |
| smoke-r1 | rocksdb | 1 | 4010 | 3.92 | 8098 | 4010 | 4010 |
| smoke-r1 | segment | 1 | 1316 | 1.29 | 27216 | 1316 | 1316 |
| smoke-r3 | fjall | 1 | 2252 | 2.20 | 16336 | 2252 | 2252 |
| smoke-r3 | redb | 1 | 743 | 0.73 | 47038 | 743 | 743 |
| smoke-r3 | rocksdb | 1 | 1409 | 1.38 | 28901 | 1409 | 1409 |
| smoke-r3 | segment | 1 | 485 | 0.47 | 75982 | 485 | 485 |
