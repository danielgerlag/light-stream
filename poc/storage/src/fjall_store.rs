use crate::model::{
    BOOKMARK_PREFIX, Backend, PAYLOAD_PREFIX, PayloadVisitor, STATE_KEY, State, decode_bookmark,
    encode_bookmark, encode_metadata, key, sequence_from_key,
};
use anyhow::{Context, Result, ensure};
use fjall::{CompressionType, Database, Keyspace, KeyspaceCreateOptions, PersistMode};
use poc_common::{Batch, Bookmark};
use std::path::Path;

pub(crate) struct Fjall {
    db: Database,
    data: Keyspace,
}

impl Fjall {
    pub fn open(directory: &Path) -> Result<(Self, State)> {
        let db = Database::builder(directory.join("fjall"))
            .cache_size(16 * 1024 * 1024)
            .journal_compression(CompressionType::None)
            .manual_journal_persist(true)
            .open()?;
        let data = db.keyspace("store", || {
            KeyspaceCreateOptions::default()
                .max_memtable_size(32 * 1024 * 1024)
                .data_block_compression_policy(fjall::config::CompressionPolicy::disabled())
                .index_block_compression_policy(fjall::config::CompressionPolicy::disabled())
                .with_kv_separation(None)
        })?;
        let state = if let Some(value) = data.get(STATE_KEY)? {
            State::decode(&value)?
        } else {
            ensure!(
                data.iter().next().is_none(),
                "missing Fjall state in nonempty store"
            );
            let state = State::default();
            let mut write = db.batch().durability(Some(PersistMode::SyncAll));
            write.insert(&data, STATE_KEY, encode_metadata(&state)?);
            write.commit()?;
            state
        };
        Ok((Self { db, data }, state))
    }
}

impl Backend for Fjall {
    fn append(&mut self, batch: &Batch, bookmark: &Bookmark, state: &State) -> Result<()> {
        let mut write = self.db.batch().durability(Some(PersistMode::SyncAll));
        write.insert(
            &self.data,
            key(PAYLOAD_PREFIX, batch.sequence),
            batch.encode(),
        );
        write.insert(
            &self.data,
            key(BOOKMARK_PREFIX, batch.sequence),
            encode_bookmark(bookmark),
        );
        write.insert(&self.data, STATE_KEY, encode_metadata(state)?);
        write.commit()?;
        Ok(())
    }

    fn visit(&self, visitor: &mut PayloadVisitor<'_>) -> Result<()> {
        for item in self.data.prefix([PAYLOAD_PREFIX]) {
            let (key, bytes) = item.into_inner()?;
            let sequence = sequence_from_key(PAYLOAD_PREFIX, &key)?;
            visitor(sequence, &bytes, self.bookmark(sequence)?)?;
        }
        Ok(())
    }

    fn last_bookmarks(&self, limit: usize) -> Result<Vec<Bookmark>> {
        self.data
            .prefix([BOOKMARK_PREFIX])
            .rev()
            .take(limit)
            .map(|item| {
                let (key, value) = item.into_inner()?;
                decode_bookmark(sequence_from_key(BOOKMARK_PREFIX, &key)?, &value)
            })
            .collect()
    }

    fn bookmark(&self, sequence: u64) -> Result<Bookmark> {
        let bytes = self
            .data
            .get(key(BOOKMARK_PREFIX, sequence))?
            .context("missing Fjall bookmark")?;
        decode_bookmark(sequence, &bytes)
    }

    fn retain(&mut self, state: &State) -> Result<()> {
        let mut write = self.db.batch().durability(Some(PersistMode::SyncAll));
        for item in self
            .data
            .range(key(BOOKMARK_PREFIX, 0)..key(BOOKMARK_PREFIX, state.retained_from))
        {
            let bytes = item.key()?;
            let sequence = sequence_from_key(BOOKMARK_PREFIX, &bytes)?;
            write.remove(&self.data, bytes);
            write.remove(&self.data, key(PAYLOAD_PREFIX, sequence));
        }
        write.insert(&self.data, STATE_KEY, encode_metadata(state)?);
        write.commit()?;
        Ok(())
    }
}
