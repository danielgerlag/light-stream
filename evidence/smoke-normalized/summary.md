# Measured results

Medians across repeated trials. Latency is the median of each trial's p99.
These are same-host fixed-leader POCs, not independent-host Raft results.

| Case | Engine | Trials | Records/s median | MiB/s median | p99 us | RPS min | RPS max |
| --- | --- | --- | --- | --- | --- | --- | --- |
| smoke-r1 | fjall | 1 | 6501 | 6.35 | 7837 | 6501 | 6501 |
| smoke-r1 | redb | 1 | 1654 | 1.62 | 20360 | 1654 | 1654 |
| smoke-r1 | rocksdb | 1 | 4243 | 4.14 | 8187 | 4243 | 4243 |
| smoke-r1 | segment | 1 | 1304 | 1.27 | 28319 | 1304 | 1304 |
| smoke-r3 | fjall | 1 | 2045 | 2.00 | 17167 | 2045 | 2045 |
| smoke-r3 | redb | 1 | 752 | 0.73 | 53034 | 752 | 752 |
| smoke-r3 | rocksdb | 1 | 1451 | 1.42 | 28264 | 1451 | 1451 |
| smoke-r3 | segment | 1 | 499 | 0.49 | 71991 | 499 | 499 |
