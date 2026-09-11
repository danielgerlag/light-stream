# macOS synchronization normalization

The first smoke run produced approximately 72,061 records/s for RocksDB and 1,300 records/s for segments in the two-shard standalone case.
The samples were short, and the synchronization paths differed.
Those numbers are not a fair engine ranking.

## Root cause

Rust 1.96.1's `library/std/src/sys/fs/unix.rs`, lines 1391-1411, calls `fcntl(F_FULLFSYNC)` for Apple file synchronization.
redb delegates its file synchronization to the standard library.
Fjall and the segment POC also use standard-library synchronization.

The bundled RocksDB `env/io_posix.cc`, lines 1840-1850, selects `fcntl(F_FULLFSYNC)` only when `HAVE_FULLFSYNC` is defined.
Otherwise it uses `fsync`.
librocksdb-sys 0.19.0+11.8.1's `build.rs`, lines 239-242, sets the Darwin platform macros but does not set `HAVE_FULLFSYNC`.

## Correction

The workspace `.cargo/config.toml` sets the target-specific C++ flags for Apple arm64 and x86_64 to define `HAVE_FULLFSYNC=1`.
This selects RocksDB's existing full-synchronization implementation.
No storage acknowledgements or Rust engine durability settings are weakened.

This is still an OS synchronization request, not proof that a particular device survives power loss.
Engines can require different numbers of synchronization operations for one atomic append.
That cost remains part of their measured implementation.

The evidence script includes `.cargo/config.toml` in its source fingerprint.
The first smoke directory remains available with its interpretation note.
