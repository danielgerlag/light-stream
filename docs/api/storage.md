# LS02a storage reference

`light-stream-storage` implements the exact Openraft `0.10.0-alpha.34` storage traits.

## Group directories

Each group owns one directory.

```text
<data-dir>/groups/<group-id>/rocksdb/
```

Control group `1` and data group `2` use separate RocksDB databases. Each database contains these versioned column families:

- `ls_v1_meta`
- `ls_v1_raft_meta`
- `ls_v1_raft_log`
- `ls_v1_payload`
- `ls_v1_state`
- `ls_v1_snapshot`

The `ls_v1_meta` column family stores the format version, cluster ID, group ID, and group kind. Startup rejects an unknown version, a missing column family, a checksum error, or an identity mismatch.

## Durability

A ticketed lane orders all writes for one group. Votes, committed indexes, log appends, truncation, purge, application, and snapshot publication use RocksDB WAL writes with `sync=true`. RocksDB enables paranoid checks and `use_fsync`. `.cargo/config.toml` keeps the Apple `HAVE_FULLFSYNC=1` build setting.

The log store calls `IOFlushed::io_completed` only after the synchronous write returns. The state machine writes the application result, offsets, payload ownership, receipt, and applied log ID in one batch before it sends the Openraft response.

## Payload ownership

A publish payload key contains the Raft term, leader node ID, log index, and record slot. The Raft log stores a descriptor with these keys. The applied record index stores the same keys. RocksDB stores each normal-operation payload body once.

Each payload ownership value has separate flags for:

- the Raft log
- the applied state
- the current snapshot

Log truncation and purge clear only the Raft-log flag. A payload is deleted only when no flag remains.

## Snapshots

LS02a snapshots are versioned byte bundles capped at 64 MiB. A bundle contains the group identity, Openraft snapshot metadata, every committed state entry, and every payload that the applied state references.

Snapshot installation checks the format, group identity, cluster identity, and Openraft metadata before it replaces committed state. The current snapshot bytes remain durable in the snapshot column family.

LS02a proves local build, install, and current-snapshot behavior. It does not claim network snapshot catch-up.

LS02b keeps this storage format and these unit tests. New three-voter groups set `SnapshotPolicy::Never`, so Openraft does not create automatic snapshots or purge logs covered by them. The peer adapter rejects `full_snapshot` until LS06. Ordinary follower recovery uses retained log suffixes.

## Tests

Run:

```sh
cargo test -p light-stream-storage
```

The command runs Openraft's `testing::log::Suite::test_all` plus product tests for purge-safe payload retention, complete snapshot installation, and corrupt identity refusal.
