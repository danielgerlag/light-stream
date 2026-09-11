# Measured results

Medians across repeated trials. Latency is the median of each trial's p99.
These are same-host fixed-leader POCs, not independent-host Raft results.

| Case | Engine | Trials | Records/s median | MiB/s median | p99 us | RPS min | RPS max |
| --- | --- | --- | --- | --- | --- | --- | --- |
| smoke-r1 | fjall | 1 | 4494 | 4.39 | 11272 | 4494 | 4494 |
| smoke-r1 | redb | 1 | 1675 | 1.64 | 20525 | 1675 | 1675 |
| smoke-r1 | rocksdb | 1 | 3974 | 3.88 | 8420 | 3974 | 3974 |
| smoke-r1 | segment | 1 | 1326 | 1.30 | 25020 | 1326 | 1326 |
| smoke-r3 | fjall | 1 | 2802 | 2.74 | 16419 | 2802 | 2802 |
| smoke-r3 | redb | 1 | 749 | 0.73 | 52033 | 749 | 749 |
| smoke-r3 | rocksdb | 1 | 1478 | 1.44 | 27142 | 1478 | 1478 |
| smoke-r3 | segment | 1 | 476 | 0.47 | 74011 | 476 | 476 |
