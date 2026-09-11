# Measured results

Medians across repeated trials. Latency is the median of each trial's p99.
These are same-host fixed-leader POCs, not independent-host Raft results.

| Case | Engine | Trials | Records/s median | MiB/s median | p99 us | RPS min | RPS max |
| --- | --- | --- | --- | --- | --- | --- | --- |
| smoke-r1 | fjall | 1 | 4137 | 4.04 | 8328 | 4137 | 4137 |
| smoke-r1 | redb | 1 | 1760 | 1.72 | 20476 | 1760 | 1760 |
| smoke-r1 | rocksdb | 1 | 3960 | 3.87 | 8971 | 3960 | 3960 |
| smoke-r1 | segment | 1 | 1307 | 1.28 | 26015 | 1307 | 1307 |
| smoke-r3 | fjall | 1 | 2000 | 1.95 | 19574 | 2000 | 2000 |
| smoke-r3 | redb | 1 | 751 | 0.73 | 48178 | 751 | 751 |
| smoke-r3 | rocksdb | 1 | 1500 | 1.47 | 26064 | 1500 | 1500 |
| smoke-r3 | segment | 1 | 468 | 0.46 | 74189 | 468 | 468 |
