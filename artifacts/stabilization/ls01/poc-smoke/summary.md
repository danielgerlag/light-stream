# Measured results

Medians across repeated trials. Latency is the median of each trial's p99.
These are same-host fixed-leader POCs, not independent-host Raft results.

| Case | Engine | Trials | Records/s median | MiB/s median | p99 us | RPS min | RPS max |
| --- | --- | --- | --- | --- | --- | --- | --- |
| smoke-r1 | fjall | 1 | 4235 | 4.14 | 8094 | 4235 | 4235 |
| smoke-r1 | redb | 1 | 1668 | 1.63 | 23536 | 1668 | 1668 |
| smoke-r1 | rocksdb | 1 | 3978 | 3.89 | 8328 | 3978 | 3978 |
| smoke-r1 | segment | 1 | 1527 | 1.49 | 26243 | 1527 | 1527 |
| smoke-r3 | fjall | 1 | 2486 | 2.43 | 16226 | 2486 | 2486 |
| smoke-r3 | redb | 1 | 787 | 0.77 | 47957 | 787 | 787 |
| smoke-r3 | rocksdb | 1 | 1417 | 1.38 | 25791 | 1417 | 1417 |
| smoke-r3 | segment | 1 | 460 | 0.45 | 74066 | 460 | 460 |
