# Measured results

Medians across repeated trials. Latency is the median of each trial's p99.
These are same-host fixed-leader POCs, not independent-host Raft results.

| Case | Engine | Trials | Records/s median | MiB/s median | p99 us | RPS min | RPS max |
| --- | --- | --- | --- | --- | --- | --- | --- |
| smoke-r1 | fjall | 1 | 4603 | 4.50 | 9094 | 4603 | 4603 |
| smoke-r1 | redb | 1 | 1653 | 1.61 | 20340 | 1653 | 1653 |
| smoke-r1 | rocksdb | 1 | 3680 | 3.59 | 13583 | 3680 | 3680 |
| smoke-r1 | segment | 1 | 1222 | 1.19 | 42279 | 1222 | 1222 |
| smoke-r3 | fjall | 1 | 1980 | 1.93 | 20152 | 1980 | 1980 |
| smoke-r3 | redb | 1 | 760 | 0.74 | 47126 | 760 | 760 |
| smoke-r3 | rocksdb | 1 | 1479 | 1.44 | 24183 | 1479 | 1479 |
| smoke-r3 | segment | 1 | 498 | 0.49 | 83954 | 498 | 498 |
