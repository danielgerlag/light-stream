# Measured results

Medians across repeated trials. Latency is the median of each trial's p99.
These are same-host fixed-leader POCs, not independent-host Raft results.

| Case | Engine | Trials | Records/s median | MiB/s median | p99 us | RPS min | RPS max |
| --- | --- | --- | --- | --- | --- | --- | --- |
| smoke-r1 | fjall | 1 | 4315 | 4.21 | 9870 | 4315 | 4315 |
| smoke-r1 | redb | 1 | 1621 | 1.58 | 24066 | 1621 | 1621 |
| smoke-r1 | rocksdb | 1 | 3886 | 3.79 | 15218 | 3886 | 3886 |
| smoke-r1 | segment | 1 | 1330 | 1.30 | 26464 | 1330 | 1330 |
| smoke-r3 | fjall | 1 | 2106 | 2.06 | 18046 | 2106 | 2106 |
| smoke-r3 | redb | 1 | 758 | 0.74 | 48089 | 758 | 758 |
| smoke-r3 | rocksdb | 1 | 1505 | 1.47 | 28866 | 1505 | 1505 |
| smoke-r3 | segment | 1 | 491 | 0.48 | 72178 | 491 | 491 |
