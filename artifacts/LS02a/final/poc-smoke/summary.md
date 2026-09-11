# Measured results

Medians across repeated trials. Latency is the median of each trial's p99.
These are same-host fixed-leader POCs, not independent-host Raft results.

| Case | Engine | Trials | Records/s median | MiB/s median | p99 us | RPS min | RPS max |
| --- | --- | --- | --- | --- | --- | --- | --- |
| smoke-r1 | fjall | 1 | 4210 | 4.11 | 9998 | 4210 | 4210 |
| smoke-r1 | redb | 1 | 1563 | 1.53 | 23732 | 1563 | 1563 |
| smoke-r1 | rocksdb | 1 | 3931 | 3.84 | 9388 | 3931 | 3931 |
| smoke-r1 | segment | 1 | 1238 | 1.21 | 32329 | 1238 | 1238 |
| smoke-r3 | fjall | 1 | 1931 | 1.89 | 24208 | 1931 | 1931 |
| smoke-r3 | redb | 1 | 755 | 0.74 | 48994 | 755 | 755 |
| smoke-r3 | rocksdb | 1 | 1482 | 1.45 | 24966 | 1482 | 1482 |
| smoke-r3 | segment | 1 | 477 | 0.47 | 75834 | 477 | 477 |
