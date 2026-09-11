# Storage candidates

`poc-storage` implements the `Engine`, `Store`, and `open` interfaces in
`../CONTRACT.md`. All four engines always compile. These are single-shard,
single-writer storage experiments, not Raft state machines.

## Shared behavior

The first batch chooses the shard. Sequence numbers start at zero, increase by
one, and never reset after retention. Duplicate batches, gaps, another shard,
invalid payloads, and offset overflow are rejected before a write starts.
An exclusive directory lock prevents concurrent opens, including different
engines. A checksummed engine marker prevents accidentally opening existing
data with another engine.

Each successful append durably commits the encoded batch, its after-batch
bookmark, and the absolute next-sequence and next-record state. A write error
invalidates the live handle. Close it and reopen to resolve an uncertain commit
before trying another operation. A failed operation can have reached durable
storage, so failure does not prove absence.

`audit` reads retained encoded payloads in sequence order, calls `Batch::decode`
and `Batch::validate_payload`, and compares sequences, shards, and cumulative
record offsets against persisted bookmarks and state. It does not reconstruct
payloads from counters. `last_bookmarks` reads only marker entries or the
segment metadata index. Its results are newest first.

`retain_from(n)` removes existing batches and markers below `n`. The cutoff is
clamped to the next sequence, so retaining beyond the current tail removes all
current data without hiding future appends. Retention never moves backward.
The retained record base preserves absolute offsets, including when no batches
remain. This marker lifetime is specific to the experiment. It does not model
human bookmark names, pins, leases, or consumer progress.

All engines disable storage compression. The synchronization calls request OS
durability, not a hardware power-loss guarantee. On macOS this crate does not
add an `F_FULLFSYNC` call beyond the database and standard-library behavior.

## Segment log

Segments rotate at a 64 MiB target. Frames are never split across files.
Each file has a checksummed identifier header. Each frame has a bounded batch
length, sequence, absolute after-batch record offset, batch CRC32, and header
CRC32. The batch format supplies another payload checksum. CRC32 detects
accidental corruption, not malicious changes.

A checksummed manifest records the committed byte extent of every segment and
the shard's sequence, record, and retention state. An append writes the frame,
synchronizes the segment, writes and synchronizes a pending manifest, renames
it over the manifest, then synchronizes the directory. Segment creation adds
a directory synchronization before manifest publication. This deliberately
costs more than one file synchronization per batch. The durable manifest makes
committed truncation distinguishable from an uncommitted torn write.

Reopen reads frame headers, not payload bodies, to rebuild a retained
`BTreeMap` of bookmarks and byte locations. Its memory and startup work scale
with the retained index and the headers still physically present in partially
retained segments. The manifest is limited to 16 MiB.

Reopen automatically truncates bytes beyond committed extents, removes
unreferenced segment files, and discards an unpublished pending manifest.
Repairs are synchronized and reported on stderr. A truncated committed extent,
corrupt manifest, corrupt committed header, or missing committed file causes
an error. The store never guesses a new commit boundary from a plausible
checksum. Payload corruption is detected by the explicit audit, not the
metadata-only reopen. There is no salvage API for corrupt committed data.

Retention publishes the new manifest before unlinking wholly expired
segments, then synchronizes the directory. A partially expired segment remains
on disk, but its expired frames disappear from audit and bookmark results.
The active segment is kept even when completely expired. It can be reclaimed
after a later rotation and an advancing retention call. Reopen cleans up files
left behind by a crash between manifest publication and unlinking.

## redb

redb 4.2 uses separate payload, bookmark, and metadata tables, with numeric
sequence keys. Every mutation uses one transaction with `Durability::Immediate`
and `set_quick_repair(true)`. The quick-repair setting persists allocator
recovery information at commit time. The page cache is 16 MiB per shard.
redb does not compress values. Retention deletes both tables' entries and
updates the record base in the same transaction. Freed pages can be reused;
retention does not promise to shrink the database file.

## Fjall

Fjall 3.1 uses one keyspace with disjoint payload, bookmark, and state key
prefixes. Sequence suffixes use big-endian encoding for ordered scans.
Every write batch explicitly selects `PersistMode::SyncAll`, including
initialization and retention. Automatic buffered journal persistence is
disabled. Journal, data-block, and index-block compression are disabled.
Key-value separation is disabled. The block cache is 16 MiB and the keyspace
memtable limit is 32 MiB per shard.

Retention enumerates marker keys and commits payload and marker tombstones
with the new state in one batch. This deletion batch uses memory proportional
to the number of expired batches. Compaction reclaims physical space later.

## RocksDB

RocksDB 0.25 uses the same key prefixes and big-endian sequence keys as Fjall.
One `WriteBatch` commits payload, marker, and state with WAL enabled and
`WriteOptions::set_sync(true)`. `set_use_fsync(true)` and paranoid checks are
enabled. Normal and bottommost compression are disabled, and compression
libraries are excluded from the crate features.

The block cache is 16 MiB per shard. Write buffers are 32 MiB, with at most two
buffers. Retention commits payload and marker range tombstones and new state
in one synchronized batch. Physical reclamation happens during compaction.
The bundled native build needs a C++ toolchain and libclang for bindgen.

## Verification

Run `cargo test -p poc-storage` from the workspace root. Tests use small data
under `poc/storage/.test-data`, never a system temporary directory. Each engine
covers append ordering, shard ownership, bookmarks, reopen, actual-payload
audits, retention, and continued append with absolute offsets. Child-process
tests exit without database destructors after append and retention, then audit
the reopened files. Segment tests also exercise partial retention, whole-file
reclamation, uncommitted-tail repair, and committed metadata or length
corruption.
