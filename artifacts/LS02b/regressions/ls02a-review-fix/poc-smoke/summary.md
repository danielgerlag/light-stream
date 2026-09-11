# Measured results

Medians across repeated trials. Latency is the median of each trial's p99.
These are same-host fixed-leader POCs, not independent-host Raft results.

| Case | Engine | Trials | Records/s median | MiB/s median | p99 us | RPS min | RPS max |
| --- | --- | --- | --- | --- | --- | --- | --- |
| smoke-r1 | fjall | 1 | 3518 | 3.44 | 12919 | 3518 | 3518 |
| smoke-r1 | redb | 1 | 1721 | 1.68 | 20363 | 1721 | 1721 |
| smoke-r1 | rocksdb | 1 | 4721 | 4.61 | 8040 | 4721 | 4721 |
| smoke-r1 | segment | 1 | 1303 | 1.27 | 26606 | 1303 | 1303 |
| smoke-r3 | fjall | 1 | 1730 | 1.69 | 24096 | 1730 | 1730 |
| smoke-r3 | redb | 1 | 753 | 0.74 | 48218 | 753 | 753 |
| smoke-r3 | rocksdb | 1 | 1476 | 1.44 | 33879 | 1476 | 1476 |
| smoke-r3 | segment | 1 | 464 | 0.45 | 75811 | 464 | 464 |
