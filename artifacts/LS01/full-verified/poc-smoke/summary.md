# Measured results

Medians across repeated trials. Latency is the median of each trial's p99.
These are same-host fixed-leader POCs, not independent-host Raft results.

| Case | Engine | Trials | Records/s median | MiB/s median | p99 us | RPS min | RPS max |
| --- | --- | --- | --- | --- | --- | --- | --- |
| smoke-r1 | fjall | 1 | 4130 | 4.03 | 8933 | 4130 | 4130 |
| smoke-r1 | redb | 1 | 1665 | 1.63 | 20236 | 1665 | 1665 |
| smoke-r1 | rocksdb | 1 | 3943 | 3.85 | 8885 | 3943 | 3943 |
| smoke-r1 | segment | 1 | 1327 | 1.30 | 26339 | 1327 | 1327 |
| smoke-r3 | fjall | 1 | 1903 | 1.86 | 21334 | 1903 | 1903 |
| smoke-r3 | redb | 1 | 753 | 0.74 | 50717 | 753 | 753 |
| smoke-r3 | rocksdb | 1 | 1511 | 1.48 | 24538 | 1511 | 1511 |
| smoke-r3 | segment | 1 | 481 | 0.47 | 76982 | 481 | 481 |
