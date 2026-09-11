# Measured results

Medians across repeated trials. Latency is the median of each trial's p99.
These are same-host fixed-leader POCs, not independent-host Raft results.

| Case | Engine | Trials | Records/s median | MiB/s median | p99 us | RPS min | RPS max |
| --- | --- | --- | --- | --- | --- | --- | --- |
| r3-s4-b128-z1024 | fjall | 3 | 23195 | 22.65 | 36833 | 21419 | 23300 |
| r3-s4-b128-z1024 | rocksdb | 3 | 15961 | 15.59 | 42918 | 15480 | 15977 |
| r3-s4-b128-z16384 | fjall | 3 | 7134 | 111.46 | 765420 | 6739 | 7554 |
| r3-s4-b128-z16384 | rocksdb | 3 | 9133 | 142.70 | 116186 | 9039 | 10004 |
| r3-s4-b128-z64 | fjall | 3 | 24773 | 1.51 | 33974 | 24062 | 24847 |
| r3-s4-b128-z64 | rocksdb | 3 | 17924 | 1.09 | 40133 | 17556 | 18068 |
