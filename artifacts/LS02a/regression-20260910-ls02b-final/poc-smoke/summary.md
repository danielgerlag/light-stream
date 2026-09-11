# Measured results

Medians across repeated trials. Latency is the median of each trial's p99.
These are same-host fixed-leader POCs, not independent-host Raft results.

| Case | Engine | Trials | Records/s median | MiB/s median | p99 us | RPS min | RPS max |
| --- | --- | --- | --- | --- | --- | --- | --- |
| smoke-r1 | fjall | 1 | 4108 | 4.01 | 9175 | 4108 | 4108 |
| smoke-r1 | redb | 1 | 1607 | 1.57 | 24093 | 1607 | 1607 |
| smoke-r1 | rocksdb | 1 | 3951 | 3.86 | 11937 | 3951 | 3951 |
| smoke-r1 | segment | 1 | 1211 | 1.18 | 29313 | 1211 | 1211 |
| smoke-r3 | fjall | 1 | 1994 | 1.95 | 16773 | 1994 | 1994 |
| smoke-r3 | redb | 1 | 742 | 0.72 | 53797 | 742 | 742 |
| smoke-r3 | rocksdb | 1 | 1260 | 1.23 | 65187 | 1260 | 1260 |
| smoke-r3 | segment | 1 | 446 | 0.44 | 88250 | 446 | 446 |
