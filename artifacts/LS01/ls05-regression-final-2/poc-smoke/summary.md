# Measured results

Medians across repeated trials. Latency is the median of each trial's p99.
These are same-host fixed-leader POCs, not independent-host Raft results.

| Case | Engine | Trials | Records/s median | MiB/s median | p99 us | RPS min | RPS max |
| --- | --- | --- | --- | --- | --- | --- | --- |
| smoke-r1 | fjall | 1 | 4752 | 4.64 | 9789 | 4752 | 4752 |
| smoke-r1 | redb | 1 | 1693 | 1.65 | 20486 | 1693 | 1693 |
| smoke-r1 | rocksdb | 1 | 3967 | 3.87 | 12218 | 3967 | 3967 |
| smoke-r1 | segment | 1 | 1319 | 1.29 | 28044 | 1319 | 1319 |
| smoke-r3 | fjall | 1 | 2115 | 2.07 | 18238 | 2115 | 2115 |
| smoke-r3 | redb | 1 | 761 | 0.74 | 52492 | 761 | 761 |
| smoke-r3 | rocksdb | 1 | 1524 | 1.49 | 24238 | 1524 | 1524 |
| smoke-r3 | segment | 1 | 472 | 0.46 | 74405 | 472 | 472 |
