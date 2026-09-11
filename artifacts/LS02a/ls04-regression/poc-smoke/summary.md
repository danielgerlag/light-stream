# Measured results

Medians across repeated trials. Latency is the median of each trial's p99.
These are same-host fixed-leader POCs, not independent-host Raft results.

| Case | Engine | Trials | Records/s median | MiB/s median | p99 us | RPS min | RPS max |
| --- | --- | --- | --- | --- | --- | --- | --- |
| smoke-r1 | fjall | 1 | 4236 | 4.14 | 8131 | 4236 | 4236 |
| smoke-r1 | redb | 1 | 1690 | 1.65 | 20225 | 1690 | 1690 |
| smoke-r1 | rocksdb | 1 | 3991 | 3.90 | 12022 | 3991 | 3991 |
| smoke-r1 | segment | 1 | 1331 | 1.30 | 24594 | 1331 | 1331 |
| smoke-r3 | fjall | 1 | 2050 | 2.00 | 17755 | 2050 | 2050 |
| smoke-r3 | redb | 1 | 763 | 0.74 | 46780 | 763 | 763 |
| smoke-r3 | rocksdb | 1 | 1546 | 1.51 | 24070 | 1546 | 1546 |
| smoke-r3 | segment | 1 | 456 | 0.45 | 94179 | 456 | 456 |
