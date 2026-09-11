# Measured results

Medians across repeated trials. Latency is the median of each trial's p99.
These are same-host fixed-leader POCs, not independent-host Raft results.

| Case | Engine | Trials | Records/s median | MiB/s median | p99 us | RPS min | RPS max |
| --- | --- | --- | --- | --- | --- | --- | --- |
| smoke-r1 | fjall | 1 | 5370 | 5.24 | 8307 | 5370 | 5370 |
| smoke-r1 | redb | 1 | 1803 | 1.76 | 22582 | 1803 | 1803 |
| smoke-r1 | rocksdb | 1 | 3977 | 3.88 | 8272 | 3977 | 3977 |
| smoke-r1 | segment | 1 | 1306 | 1.28 | 31932 | 1306 | 1306 |
| smoke-r3 | fjall | 1 | 2239 | 2.19 | 17270 | 2239 | 2239 |
| smoke-r3 | redb | 1 | 746 | 0.73 | 47866 | 746 | 746 |
| smoke-r3 | rocksdb | 1 | 1540 | 1.50 | 28640 | 1540 | 1540 |
| smoke-r3 | segment | 1 | 509 | 0.50 | 67667 | 509 | 509 |
