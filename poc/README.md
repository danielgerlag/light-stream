# High-volume streaming experiments

The POCs compare four durable storage engines under a shared batch and bookmark contract.
The same executable runs standalone nodes and a three-process TCP topology.
The POC marker is an after-batch position, not the proposed human-named bookmark API.
The experiment retains marker entries with their batches.
Independent bookmark lifetimes, pin leases, and consumer progress remain broker requirements outside this storage experiment.

This is a fixed-leader replication experiment, not a Raft implementation.
It does not implement elections, fencing, membership changes, consumer delivery semantics, or replica catch-up.
Three processes on one machine share a CPU, memory, and disk.
Their results do not establish independent-host scalability or production HA.

## Run the experiments

Build and exercise correctness before measuring:

```sh
cargo test --workspace
cargo build --release -p stream-poc
python3 poc/scripts/run.py --matrix smoke --repeats 1 --output evidence/smoke
```

Run the repeated comparison:

```sh
python3 poc/scripts/run.py --matrix main --repeats 3 --seconds 2 --max-mib 128 --output evidence/main
```

Run longer shard-scaling experiments on selected engines:

```sh
python3 poc/scripts/run.py --matrix scale --engines segment,redb --repeats 3 --seconds 5 --max-mib 512 --output evidence/scale
```

Exercise one-follower loss, majority loss, and reopened data:

```sh
python3 poc/scripts/run.py --matrix failure --engines segment --output evidence/failure
```

Measure a dense bookmark index:

```sh
python3 poc/scripts/run.py --matrix density --repeats 1 --seconds 300 --max-mib 8 --output evidence/density
```

That workload caps at 100,000 durable single-record batches per engine.
The raw count shows whether it reached the cap before the time limit.

Run beyond the per-shard memory buffers:

```sh
python3 poc/scripts/run.py --matrix sustained --engines fjall,rocksdb --repeats 3 --seconds 30 --max-mib 3072 --output evidence/sustained
```

This bounds each trial to 3 GiB of logical payload, replicated to three nodes.
It is a longer ingest experiment. Retention is still measured after ingest, not concurrently.

Compare slow-minority admission policies:

```sh
python3 poc/scripts/run.py --matrix slow-peer --engines fjall --repeats 3 --output evidence/slow-peer
```

One real follower delays each replication acknowledgement by 250 ms after durable storage.
The control blocks on every peer queue. The alternative isolates a peer when its bounded queue fills.
Both still require local durability and a durable follower.
The isolation POC permanently excludes that lagging peer and cannot catch it up.
The experiment also removes the remaining fast follower and requires a zero-ack quorum rejection.

The script refuses to overwrite an existing evidence directory.
It runs a release build before capturing evidence so a stale executable cannot silently stand in for current source.
`--skip-build` is available for an explicitly supplied historical binary, but marks its source correspondence unverified.
It starts only its own child processes on loopback ephemeral ports.
It stops those processes and removes only the scratch directories it created.
Pass `--keep-data` to preserve those directories for manual inspection.
Failed trials retain their scratch data and memory samples for diagnosis.
Their `failure.json` records the exact data directory.

## Evidence files

Each run records its arguments, environment, source fingerprints, binary fingerprint, and randomized trial order.
New runs also copy the fingerprinted source files into their `source/` directory.
Each trial records node logs, raw request latencies, expected acknowledged data, offline reopened-data audits, retention results, memory samples, and disk footprint.
`summary.csv` and `summary.md` summarize the raw trial files.

Latency is closed-loop request latency, not an open-loop overload SLO.
Payload generation contributes to elapsed throughput time.
Node startup and offline inspection do not contribute to ingestion throughput.
The byte cap can end a trial before its duration limit. Both elapsed duration and operation count remain visible.

The script waits for healthy followers to catch up before stopping nodes.
Offline inspection decodes checksums and compares deterministic payload bytes, rather than trusting live counters.
Retention removes the first half of the common acknowledged sequence range and checks remaining records and absolute bookmark positions.
OS caches are not cleared. Replay is a warm-cache experiment unless stated otherwise.

OS synchronization and process-kill recovery do not prove survival of hardware power loss.
On macOS, Rust's standard-library synchronization uses `F_FULLFSYNC`.
The bundled RocksDB build otherwise omits its `HAVE_FULLFSYNC` macro and calls plain `fsync`.
The workspace Cargo configuration enables that macro for Apple targets so the comparison does not reward weaker synchronization.
Database file size is not device write amplification.
Summed process RSS is not unique physical memory because shared pages can be counted more than once.
The comparison executable links all four engines and retains release debug symbols.
Its file size is not the size of a stripped single-engine broker or a container image.
Shard comparisons also change the number of concurrent clients because the driver uses one client per shard.
They measure whole-configuration scaling, not an isolated estimate of the effect of storage sharding.

See [the implementation contract](CONTRACT.md) and [the design discussion](../docs/design/README.md).
