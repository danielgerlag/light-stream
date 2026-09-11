# Measured results

Medians across repeated trials. Latency is the median of each trial's p99.
These are same-host fixed-leader POCs, not independent-host Raft results.

| Case | Engine | Trials | Records/s median | MiB/s median | p99 us | RPS min | RPS max |
| --- | --- | --- | --- | --- | --- | --- | --- |
| smoke-r1 | fjall | 1 | 3934 | 3.84 | 9820 | 3934 | 3934 |
| smoke-r1 | redb | 1 | 1684 | 1.64 | 20532 | 1684 | 1684 |
| smoke-r1 | rocksdb | 1 | 3591 | 3.51 | 10371 | 3591 | 3591 |
| smoke-r1 | segment | 1 | 1296 | 1.27 | 29253 | 1296 | 1296 |
| smoke-r3 | fjall | 1 | 2188 | 2.14 | 16880 | 2188 | 2188 |
| smoke-r3 | redb | 1 | 758 | 0.74 | 49266 | 758 | 758 |
| smoke-r3 | rocksdb | 1 | 1463 | 1.43 | 27126 | 1463 | 1463 |
| smoke-r3 | segment | 1 | 478 | 0.47 | 75330 | 478 | 478 |
