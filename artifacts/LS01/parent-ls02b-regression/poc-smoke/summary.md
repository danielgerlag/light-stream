# Measured results

Medians across repeated trials. Latency is the median of each trial's p99.
These are same-host fixed-leader POCs, not independent-host Raft results.

| Case | Engine | Trials | Records/s median | MiB/s median | p99 us | RPS min | RPS max |
| --- | --- | --- | --- | --- | --- | --- | --- |
| smoke-r1 | fjall | 1 | 4289 | 4.19 | 8113 | 4289 | 4289 |
| smoke-r1 | redb | 1 | 1789 | 1.75 | 19826 | 1789 | 1789 |
| smoke-r1 | rocksdb | 1 | 3992 | 3.90 | 8206 | 3992 | 3992 |
| smoke-r1 | segment | 1 | 1321 | 1.29 | 25515 | 1321 | 1321 |
| smoke-r3 | fjall | 1 | 2537 | 2.48 | 20088 | 2537 | 2537 |
| smoke-r3 | redb | 1 | 768 | 0.75 | 53390 | 768 | 768 |
| smoke-r3 | rocksdb | 1 | 1561 | 1.52 | 24108 | 1561 | 1561 |
| smoke-r3 | segment | 1 | 447 | 0.44 | 82828 | 447 | 447 |
