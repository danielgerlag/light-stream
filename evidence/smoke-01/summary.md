# Measured results

Medians across repeated trials. Latency is the median of each trial's p99.
These are same-host fixed-leader POCs, not independent-host Raft results.

| Case | Engine | Trials | Records/s median | MiB/s median | p99 us | RPS min | RPS max |
| --- | --- | --- | --- | --- | --- | --- | --- |
| smoke-r1 | fjall | 1 | 5055 | 4.94 | 8102 | 5055 | 5055 |
| smoke-r1 | redb | 1 | 1587 | 1.55 | 25034 | 1587 | 1587 |
| smoke-r1 | rocksdb | 1 | 72061 | 70.37 | 812 | 72061 | 72061 |
| smoke-r1 | segment | 1 | 1300 | 1.27 | 28703 | 1300 | 1300 |
| smoke-r3 | fjall | 1 | 2098 | 2.05 | 20047 | 2098 | 2098 |
| smoke-r3 | redb | 1 | 815 | 0.80 | 46383 | 815 | 815 |
| smoke-r3 | rocksdb | 1 | 30799 | 30.08 | 2157 | 30799 | 30799 |
| smoke-r3 | segment | 1 | 457 | 0.45 | 70013 | 457 | 457 |
