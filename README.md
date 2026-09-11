# Light Stream

Light Stream contains a production Rust service and the original high-volume storage POCs.
LS02a adds explicit cluster bootstrap, one-voter Openraft control and data groups, RocksDB persistence, committed publish and fetch, durable retry receipts, and complete bounded snapshots.
Three-voter replication starts in LS02b.

Start with the [measured evidence](docs/experiments/high-volume.md) and the [revised design](docs/design/proposal.md).
The [POC guide](poc/README.md) contains the full experiment commands and limitations.
The [coding-agent implementation plan](docs/implementation/README.md) defines the production work, mandatory end-to-end gates, and optional security review.
The [production module map](docs/api/module-map.md) explains the crate boundaries.
The [CLI reference](docs/api/cli.md) defines the JSON and exit-code contract.

## Build

The workspace vendors `protoc` through a Rust build dependency.
The POC dependencies still require a C++ toolchain with libclang for RocksDB.
The recorded experiments used Rust 1.96.1 and Apple Clang 21 on an Apple M5.

```sh
cargo test --workspace
cargo build --release -p light-stream-server -p light-stream-cli -p light-stream-testkit
```

Start the server. Startup does not create a cluster.

```sh
target/release/light-streamd \
  --data-dir /tmp/light-stream-node-1 \
  --public-listen 127.0.0.1:7101 \
  --peer-listen 127.0.0.1:7201

target/release/light-streamctl \
  --endpoint http://127.0.0.1:7101 \
  health

target/release/light-streamctl \
  --endpoint http://127.0.0.1:7101 \
  cluster bootstrap \
  --cluster-id 018f3f7e-5b3b-7c11-98f7-b65ac15f6501 \
  --stream-id 018f3f7e-5b3b-7c11-98f7-b65ac15f6502 \
  --stream-name bootstrap

target/release/light-streamctl \
  --endpoint http://127.0.0.1:7101 \
  publish \
  --cluster-id 018f3f7e-5b3b-7c11-98f7-b65ac15f6501 \
  --stream-id 018f3f7e-5b3b-7c11-98f7-b65ac15f6502 \
  --principal local \
  --session 018f3f7e-5b3b-7c11-98f7-b65ac15f6503 \
  --sequence 1 \
  --payload hello
```

Run the LS02a verification:

```sh
python3 scripts/verify.py \
  --phase LS02a \
  --profile local \
  --artifacts artifacts/LS02a/manual-run
```

## POC controls

```sh
cargo build --release -p stream-poc --jobs 4
python3 poc/scripts/run.py --matrix smoke --repeats 1 --output evidence/my-smoke
```

The experiment runner refuses to overwrite an existing output directory.
It starts its own loopback nodes and stores raw measurements, logs, and audits.
The three-node topology uses separate processes and data directories on one host.

The fixed-leader POC is not Raft and does not implement automatic failover, fencing, or replica catch-up.
The evidence includes unsuccessful attempts and the corrections made to the measurement setup.
