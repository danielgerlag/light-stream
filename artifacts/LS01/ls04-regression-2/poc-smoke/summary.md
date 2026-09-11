# Measured results

Medians across repeated trials. Latency is the median of each trial's p99.
These are same-host fixed-leader POCs, not independent-host Raft results.

| Case | Engine | Trials | Records/s median | MiB/s median | p99 us | RPS min | RPS max |
| --- | --- | --- | --- | --- | --- | --- | --- |
| smoke-r1 | fjall | 1 | 4142 | 4.04 | 8713 | 4142 | 4142 |
| smoke-r1 | redb | 1 | 1640 | 1.60 | 20197 | 1640 | 1640 |
| smoke-r1 | rocksdb | 1 | 4239 | 4.14 | 8256 | 4239 | 4239 |
| smoke-r1 | segment | 1 | 1316 | 1.29 | 25487 | 1316 | 1316 |
| smoke-r3 | fjall | 1 | 2064 | 2.02 | 16558 | 2064 | 2064 |
| smoke-r3 | redb | 1 | 736 | 0.72 | 48235 | 736 | 736 |
| smoke-r3 | rocksdb | 1 | 1453 | 1.42 | 24416 | 1453 | 1453 |
| smoke-r3 | segment | 1 | 466 | 0.45 | 77985 | 466 | 466 |
