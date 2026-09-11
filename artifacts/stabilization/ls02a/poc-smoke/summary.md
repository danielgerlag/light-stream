# Measured results

Medians across repeated trials. Latency is the median of each trial's p99.
These are same-host fixed-leader POCs, not independent-host Raft results.

| Case | Engine | Trials | Records/s median | MiB/s median | p99 us | RPS min | RPS max |
| --- | --- | --- | --- | --- | --- | --- | --- |
| smoke-r1 | fjall | 1 | 4222 | 4.12 | 8212 | 4222 | 4222 |
| smoke-r1 | redb | 1 | 1759 | 1.72 | 20210 | 1759 | 1759 |
| smoke-r1 | rocksdb | 1 | 4142 | 4.05 | 8122 | 4142 | 4142 |
| smoke-r1 | segment | 1 | 1317 | 1.29 | 26547 | 1317 | 1317 |
| smoke-r3 | fjall | 1 | 2046 | 2.00 | 20130 | 2046 | 2046 |
| smoke-r3 | redb | 1 | 756 | 0.74 | 47771 | 756 | 756 |
| smoke-r3 | rocksdb | 1 | 1413 | 1.38 | 29949 | 1413 | 1413 |
| smoke-r3 | segment | 1 | 469 | 0.46 | 89871 | 469 | 469 |
