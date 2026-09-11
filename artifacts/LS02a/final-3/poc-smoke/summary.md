# Measured results

Medians across repeated trials. Latency is the median of each trial's p99.
These are same-host fixed-leader POCs, not independent-host Raft results.

| Case | Engine | Trials | Records/s median | MiB/s median | p99 us | RPS min | RPS max |
| --- | --- | --- | --- | --- | --- | --- | --- |
| smoke-r1 | fjall | 1 | 4400 | 4.30 | 8774 | 4400 | 4400 |
| smoke-r1 | redb | 1 | 1774 | 1.73 | 20055 | 1774 | 1774 |
| smoke-r1 | rocksdb | 1 | 3982 | 3.89 | 8275 | 3982 | 3982 |
| smoke-r1 | segment | 1 | 1306 | 1.28 | 27331 | 1306 | 1306 |
| smoke-r3 | fjall | 1 | 2182 | 2.13 | 17217 | 2182 | 2182 |
| smoke-r3 | redb | 1 | 884 | 0.86 | 40976 | 884 | 884 |
| smoke-r3 | rocksdb | 1 | 1447 | 1.41 | 24696 | 1447 | 1447 |
| smoke-r3 | segment | 1 | 457 | 0.45 | 87831 | 457 | 457 |
