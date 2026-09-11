# Measured results

Medians across repeated trials. Latency is the median of each trial's p99.
These are same-host fixed-leader POCs, not independent-host Raft results.

| Case | Engine | Trials | Records/s median | MiB/s median | p99 us | RPS min | RPS max |
| --- | --- | --- | --- | --- | --- | --- | --- |
| smoke-r1 | fjall | 1 | 4068 | 3.97 | 11145 | 4068 | 4068 |
| smoke-r1 | redb | 1 | 1648 | 1.61 | 20423 | 1648 | 1648 |
| smoke-r1 | rocksdb | 1 | 4023 | 3.93 | 8144 | 4023 | 4023 |
| smoke-r1 | segment | 1 | 1321 | 1.29 | 25009 | 1321 | 1321 |
| smoke-r3 | fjall | 1 | 2124 | 2.07 | 16507 | 2124 | 2124 |
| smoke-r3 | redb | 1 | 741 | 0.72 | 48422 | 741 | 741 |
| smoke-r3 | rocksdb | 1 | 1408 | 1.38 | 25006 | 1408 | 1408 |
| smoke-r3 | segment | 1 | 482 | 0.47 | 76439 | 482 | 482 |
