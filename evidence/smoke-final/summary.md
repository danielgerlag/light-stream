# Measured results

Medians across repeated trials. Latency is the median of each trial's p99.
These are same-host fixed-leader POCs, not independent-host Raft results.

| Case | Engine | Trials | Records/s median | MiB/s median | p99 us | RPS min | RPS max |
| --- | --- | --- | --- | --- | --- | --- | --- |
| smoke-r1 | fjall | 1 | 3754 | 3.67 | 15157 | 3754 | 3754 |
| smoke-r1 | redb | 1 | 1652 | 1.61 | 20295 | 1652 | 1652 |
| smoke-r1 | rocksdb | 1 | 3969 | 3.88 | 8468 | 3969 | 3969 |
| smoke-r1 | segment | 1 | 1288 | 1.26 | 28068 | 1288 | 1288 |
| smoke-r3 | fjall | 1 | 2911 | 2.84 | 18525 | 2911 | 2911 |
| smoke-r3 | redb | 1 | 726 | 0.71 | 48806 | 726 | 726 |
| smoke-r3 | rocksdb | 1 | 1429 | 1.40 | 26052 | 1429 | 1429 |
| smoke-r3 | segment | 1 | 480 | 0.47 | 69297 | 480 | 480 |
