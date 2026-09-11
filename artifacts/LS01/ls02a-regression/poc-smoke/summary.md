# Measured results

Medians across repeated trials. Latency is the median of each trial's p99.
These are same-host fixed-leader POCs, not independent-host Raft results.

| Case | Engine | Trials | Records/s median | MiB/s median | p99 us | RPS min | RPS max |
| --- | --- | --- | --- | --- | --- | --- | --- |
| smoke-r1 | fjall | 1 | 4196 | 4.10 | 8853 | 4196 | 4196 |
| smoke-r1 | redb | 1 | 1578 | 1.54 | 24581 | 1578 | 1578 |
| smoke-r1 | rocksdb | 1 | 4042 | 3.95 | 13409 | 4042 | 4042 |
| smoke-r1 | segment | 1 | 1322 | 1.29 | 26257 | 1322 | 1322 |
| smoke-r3 | fjall | 1 | 2126 | 2.08 | 16336 | 2126 | 2126 |
| smoke-r3 | redb | 1 | 719 | 0.70 | 60202 | 719 | 719 |
| smoke-r3 | rocksdb | 1 | 1479 | 1.44 | 24174 | 1479 | 1479 |
| smoke-r3 | segment | 1 | 472 | 0.46 | 73071 | 472 | 472 |
