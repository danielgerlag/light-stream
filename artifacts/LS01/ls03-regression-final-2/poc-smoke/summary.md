# Measured results

Medians across repeated trials. Latency is the median of each trial's p99.
These are same-host fixed-leader POCs, not independent-host Raft results.

| Case | Engine | Trials | Records/s median | MiB/s median | p99 us | RPS min | RPS max |
| --- | --- | --- | --- | --- | --- | --- | --- |
| smoke-r1 | fjall | 1 | 3925 | 3.83 | 8981 | 3925 | 3925 |
| smoke-r1 | redb | 1 | 1641 | 1.60 | 20326 | 1641 | 1641 |
| smoke-r1 | rocksdb | 1 | 4003 | 3.91 | 8054 | 4003 | 4003 |
| smoke-r1 | segment | 1 | 1349 | 1.32 | 24444 | 1349 | 1349 |
| smoke-r3 | fjall | 1 | 1980 | 1.93 | 20176 | 1980 | 1980 |
| smoke-r3 | redb | 1 | 740 | 0.72 | 51464 | 740 | 740 |
| smoke-r3 | rocksdb | 1 | 1470 | 1.44 | 33605 | 1470 | 1470 |
| smoke-r3 | segment | 1 | 448 | 0.44 | 106312 | 448 | 448 |
