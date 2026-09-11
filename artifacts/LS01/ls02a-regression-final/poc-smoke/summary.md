# Measured results

Medians across repeated trials. Latency is the median of each trial's p99.
These are same-host fixed-leader POCs, not independent-host Raft results.

| Case | Engine | Trials | Records/s median | MiB/s median | p99 us | RPS min | RPS max |
| --- | --- | --- | --- | --- | --- | --- | --- |
| smoke-r1 | fjall | 1 | 3941 | 3.85 | 9043 | 3941 | 3941 |
| smoke-r1 | redb | 1 | 2039 | 1.99 | 19765 | 2039 | 2039 |
| smoke-r1 | rocksdb | 1 | 3956 | 3.86 | 8772 | 3956 | 3956 |
| smoke-r1 | segment | 1 | 1322 | 1.29 | 28085 | 1322 | 1322 |
| smoke-r3 | fjall | 1 | 2091 | 2.04 | 22550 | 2091 | 2091 |
| smoke-r3 | redb | 1 | 751 | 0.73 | 50296 | 751 | 751 |
| smoke-r3 | rocksdb | 1 | 1520 | 1.48 | 24063 | 1520 | 1520 |
| smoke-r3 | segment | 1 | 470 | 0.46 | 81076 | 470 | 470 |
