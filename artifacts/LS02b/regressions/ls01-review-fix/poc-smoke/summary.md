# Measured results

Medians across repeated trials. Latency is the median of each trial's p99.
These are same-host fixed-leader POCs, not independent-host Raft results.

| Case | Engine | Trials | Records/s median | MiB/s median | p99 us | RPS min | RPS max |
| --- | --- | --- | --- | --- | --- | --- | --- |
| smoke-r1 | fjall | 1 | 4046 | 3.95 | 9087 | 4046 | 4046 |
| smoke-r1 | redb | 1 | 1636 | 1.60 | 20354 | 1636 | 1636 |
| smoke-r1 | rocksdb | 1 | 4453 | 4.35 | 8041 | 4453 | 4453 |
| smoke-r1 | segment | 1 | 1047 | 1.02 | 36258 | 1047 | 1047 |
| smoke-r3 | fjall | 1 | 1771 | 1.73 | 28681 | 1771 | 1771 |
| smoke-r3 | redb | 1 | 697 | 0.68 | 48908 | 697 | 697 |
| smoke-r3 | rocksdb | 1 | 1633 | 1.59 | 20707 | 1633 | 1633 |
| smoke-r3 | segment | 1 | 435 | 0.42 | 82912 | 435 | 435 |
