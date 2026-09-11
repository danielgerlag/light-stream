# Measured results

Medians across repeated trials. Latency is the median of each trial's p99.
These are same-host fixed-leader POCs, not independent-host Raft results.

| Case | Engine | Trials | Records/s median | MiB/s median | p99 us | RPS min | RPS max |
| --- | --- | --- | --- | --- | --- | --- | --- |
| smoke-r1 | fjall | 1 | 4345 | 4.24 | 9948 | 4345 | 4345 |
| smoke-r1 | redb | 1 | 1732 | 1.69 | 20098 | 1732 | 1732 |
| smoke-r1 | rocksdb | 1 | 3853 | 3.76 | 9043 | 3853 | 3853 |
| smoke-r1 | segment | 1 | 1501 | 1.47 | 24041 | 1501 | 1501 |
| smoke-r3 | fjall | 1 | 1837 | 1.79 | 19570 | 1837 | 1837 |
| smoke-r3 | redb | 1 | 821 | 0.80 | 43985 | 821 | 821 |
| smoke-r3 | rocksdb | 1 | 1508 | 1.47 | 25936 | 1508 | 1508 |
| smoke-r3 | segment | 1 | 485 | 0.47 | 73897 | 485 | 485 |
