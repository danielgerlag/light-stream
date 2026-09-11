# Measured results

Medians across repeated trials. Latency is the median of each trial's p99.
These are same-host fixed-leader POCs, not independent-host Raft results.

| Case | Engine | Trials | Records/s median | MiB/s median | p99 us | RPS min | RPS max |
| --- | --- | --- | --- | --- | --- | --- | --- |
| smoke-r1 | fjall | 1 | 5239 | 5.12 | 8027 | 5239 | 5239 |
| smoke-r1 | redb | 1 | 1841 | 1.80 | 19152 | 1841 | 1841 |
| smoke-r1 | rocksdb | 1 | 3849 | 3.76 | 10316 | 3849 | 3849 |
| smoke-r1 | segment | 1 | 1328 | 1.30 | 24766 | 1328 | 1328 |
| smoke-r3 | fjall | 1 | 2101 | 2.05 | 17092 | 2101 | 2101 |
| smoke-r3 | redb | 1 | 769 | 0.75 | 47198 | 769 | 769 |
| smoke-r3 | rocksdb | 1 | 1542 | 1.51 | 25145 | 1542 | 1542 |
| smoke-r3 | segment | 1 | 483 | 0.47 | 75666 | 483 | 483 |
