# Smoke-run interpretation

All eight smoke configurations matched acknowledged data against reopened payload bytes and bookmarks.
These short runs establish integration behavior only.
They are not the storage-ranking evidence.

The large apparent RocksDB advantage triggered a synchronization investigation.
Rust 1.96.1 uses `fcntl(F_FULLFSYNC)` for `File::sync_all` and `File::sync_data` on Apple platforms.
The bundled librocksdb-sys 0.19.0+11.8.1 build did not define `HAVE_FULLFSYNC`.
Its `PosixWritableFile::Fsync` therefore used plain `fsync`.

The workspace now defines `HAVE_FULLFSYNC=1` for Apple C++ builds in `.cargo/config.toml`.
The release executable must be rebuilt before ranking engines.
The raw smoke results remain unchanged so the misleading first observation is visible.
