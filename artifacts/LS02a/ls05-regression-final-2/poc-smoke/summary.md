# Measured results

Medians across repeated trials. Latency is the median of each trial's p99.
These are same-host fixed-leader POCs, not independent-host Raft results.

| Case | Engine | Trials | Records/s median | MiB/s median | p99 us | RPS min | RPS max |
| --- | --- | --- | --- | --- | --- | --- | --- |
| smoke-r1 | fjall | 1 | 4236 | 4.14 | 9010 | 4236 | 4236 |
| smoke-r1 | redb | 1 | 1715 | 1.67 | 20712 | 1715 | 1715 |
| smoke-r1 | rocksdb | 1 | 3765 | 3.68 | 11921 | 3765 | 3765 |
| smoke-r1 | segment | 1 | 1217 | 1.19 | 30009 | 1217 | 1217 |
| smoke-r3 | fjall | 1 | 2103 | 2.05 | 20246 | 2103 | 2103 |
| smoke-r3 | redb | 1 | 774 | 0.76 | 48950 | 774 | 774 |
| smoke-r3 | rocksdb | 1 | 1497 | 1.46 | 23960 | 1497 | 1497 |
| smoke-r3 | segment | 1 | 487 | 0.48 | 76105 | 487 | 487 |
