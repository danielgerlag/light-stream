# Measured results

Medians across repeated trials. Latency is the median of each trial's p99.
These are same-host fixed-leader POCs, not independent-host Raft results.

| Case | Engine | Trials | Records/s median | MiB/s median | p99 us | RPS min | RPS max |
| --- | --- | --- | --- | --- | --- | --- | --- |
| r3-s8-b512-z1024-sustained | fjall | 1 | 74115 | 72.38 | 228227 | 74115 | 74115 |
| r3-s8-b512-z1024-sustained | rocksdb | 2 | 61741 | 60.29 | 121129 | 59527 | 63955 |
