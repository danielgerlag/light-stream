# Measured results

Medians across repeated trials. Latency is the median of each trial's p99.
These are same-host fixed-leader POCs, not independent-host Raft results.

| Case | Engine | Trials | Records/s median | MiB/s median | p99 us | RPS min | RPS max |
| --- | --- | --- | --- | --- | --- | --- | --- |
| smoke-r1 | fjall | 1 | 4889 | 4.77 | 8181 | 4889 | 4889 |
| smoke-r1 | redb | 1 | 1555 | 1.52 | 27621 | 1555 | 1555 |
| smoke-r1 | rocksdb | 1 | 3843 | 3.75 | 9244 | 3843 | 3843 |
| smoke-r1 | segment | 1 | 1276 | 1.25 | 29617 | 1276 | 1276 |
| smoke-r3 | fjall | 1 | 2049 | 2.00 | 21469 | 2049 | 2049 |
| smoke-r3 | redb | 1 | 760 | 0.74 | 45934 | 760 | 760 |
| smoke-r3 | rocksdb | 1 | 1387 | 1.35 | 26219 | 1387 | 1387 |
| smoke-r3 | segment | 1 | 433 | 0.42 | 97904 | 433 | 433 |
