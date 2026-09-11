# Measured results

Medians across repeated trials. Latency is the median of each trial's p99.
These are same-host fixed-leader POCs, not independent-host Raft results.

| Case | Engine | Trials | Records/s median | MiB/s median | p99 us | RPS min | RPS max |
| --- | --- | --- | --- | --- | --- | --- | --- |
| smoke-r1 | fjall | 1 | 4244 | 4.14 | 8122 | 4244 | 4244 |
| smoke-r1 | redb | 1 | 1619 | 1.58 | 20634 | 1619 | 1619 |
| smoke-r1 | rocksdb | 1 | 3943 | 3.85 | 9108 | 3943 | 3943 |
| smoke-r1 | segment | 1 | 1274 | 1.24 | 32083 | 1274 | 1274 |
| smoke-r3 | fjall | 1 | 2359 | 2.30 | 16194 | 2359 | 2359 |
| smoke-r3 | redb | 1 | 796 | 0.78 | 47403 | 796 | 796 |
| smoke-r3 | rocksdb | 1 | 1428 | 1.39 | 25845 | 1428 | 1428 |
| smoke-r3 | segment | 1 | 486 | 0.47 | 73891 | 486 | 486 |
