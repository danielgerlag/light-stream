# Measured results

Medians across repeated trials. Latency is the median of each trial's p99.
These are same-host fixed-leader POCs, not independent-host Raft results.

| Case | Engine | Trials | Records/s median | MiB/s median | p99 us | RPS min | RPS max |
| --- | --- | --- | --- | --- | --- | --- | --- |
| smoke-r1 | fjall | 1 | 7960 | 7.77 | 4239 | 7960 | 7960 |
| smoke-r1 | redb | 1 | 1725 | 1.68 | 20213 | 1725 | 1725 |
| smoke-r1 | rocksdb | 1 | 3891 | 3.80 | 9106 | 3891 | 3891 |
| smoke-r1 | segment | 1 | 1266 | 1.24 | 30010 | 1266 | 1266 |
| smoke-r3 | fjall | 1 | 2059 | 2.01 | 18010 | 2059 | 2059 |
| smoke-r3 | redb | 1 | 751 | 0.73 | 48397 | 751 | 751 |
| smoke-r3 | rocksdb | 1 | 1555 | 1.52 | 24067 | 1555 | 1555 |
| smoke-r3 | segment | 1 | 369 | 0.36 | 140631 | 369 | 369 |
