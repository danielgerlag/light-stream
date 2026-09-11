use crate::model::{
    BOOKMARK_PREFIX, Backend, PAYLOAD_PREFIX, PayloadVisitor, STATE_KEY, State, decode_bookmark,
    encode_bookmark, encode_metadata, key, sequence_from_key,
};
use anyhow::{Context, Result, ensure};
use poc_common::{Batch, Bookmark};
use rocksdb::{
    BlockBasedOptions, Cache, DB, DBCompressionType, Direction, IteratorMode, Options, WriteBatch,
    WriteOptions,
};
use std::path::Path;

pub(crate) struct Rocks {
    db: DB,
}

impl Rocks {
    pub fn open(directory: &Path) -> Result<(Self, State)> {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_compression_type(DBCompressionType::None);
        options.set_bottommost_compression_type(DBCompressionType::None);
        options.set_use_fsync(true);
        options.set_paranoid_checks(true);
        options.set_write_buffer_size(32 * 1024 * 1024);
        options.set_max_write_buffer_number(2);
        let mut table = BlockBasedOptions::default();
        table.set_block_cache(&Cache::new_lru_cache(16 * 1024 * 1024));
        options.set_block_based_table_factory(&table);
        let db = DB::open(&options, directory.join("rocksdb"))?;
        let backend = Self { db };
        let state = if let Some(value) = backend.db.get(STATE_KEY)? {
            State::decode(&value)?
        } else {
            ensure!(
                backend.db.iterator(IteratorMode::Start).next().is_none(),
                "missing RocksDB state in nonempty store"
            );
            let state = State::default();
            let mut write = WriteBatch::default();
            write.put(STATE_KEY, encode_metadata(&state)?);
            backend.commit(write)?;
            state
        };
        Ok((backend, state))
    }

    fn commit(&self, write: WriteBatch) -> Result<()> {
        let mut options = WriteOptions::default();
        options.set_sync(true);
        options.disable_wal(false);
        self.db.write_opt(write, &options)?;
        Ok(())
    }
}

impl Backend for Rocks {
    fn append(&mut self, batch: &Batch, bookmark: &Bookmark, state: &State) -> Result<()> {
        let mut write = WriteBatch::default();
        write.put(key(PAYLOAD_PREFIX, batch.sequence), batch.encode());
        write.put(
            key(BOOKMARK_PREFIX, batch.sequence),
            encode_bookmark(bookmark),
        );
        write.put(STATE_KEY, encode_metadata(state)?);
        self.commit(write)
    }

    fn visit(&self, visitor: &mut PayloadVisitor<'_>) -> Result<()> {
        for item in self.db.iterator(IteratorMode::From(
            &key(PAYLOAD_PREFIX, 0),
            Direction::Forward,
        )) {
            let (key, bytes) = item?;
            if key.first() != Some(&PAYLOAD_PREFIX) {
                break;
            }
            let sequence = sequence_from_key(PAYLOAD_PREFIX, &key)?;
            visitor(sequence, &bytes, self.bookmark(sequence)?)?;
        }
        Ok(())
    }

    fn last_bookmarks(&self, limit: usize) -> Result<Vec<Bookmark>> {
        let mut bookmarks = Vec::new();
        for item in self
            .db
            .iterator(IteratorMode::From(
                &key(BOOKMARK_PREFIX, u64::MAX),
                Direction::Reverse,
            ))
            .take(limit)
        {
            let (key, value) = item?;
            if key.first() != Some(&BOOKMARK_PREFIX) {
                break;
            }
            bookmarks.push(decode_bookmark(
                sequence_from_key(BOOKMARK_PREFIX, &key)?,
                &value,
            )?);
        }
        Ok(bookmarks)
    }

    fn bookmark(&self, sequence: u64) -> Result<Bookmark> {
        let bytes = self
            .db
            .get(key(BOOKMARK_PREFIX, sequence))?
            .context("missing RocksDB bookmark")?;
        decode_bookmark(sequence, &bytes)
    }

    fn retain(&mut self, state: &State) -> Result<()> {
        let mut write = WriteBatch::default();
        write.delete_range(
            key(PAYLOAD_PREFIX, 0),
            key(PAYLOAD_PREFIX, state.retained_from),
        );
        write.delete_range(
            key(BOOKMARK_PREFIX, 0),
            key(BOOKMARK_PREFIX, state.retained_from),
        );
        write.put(STATE_KEY, encode_metadata(state)?);
        self.commit(write)
    }
}
