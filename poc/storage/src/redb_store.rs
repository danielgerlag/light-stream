use crate::model::{
    Backend, PayloadVisitor, State, decode_bookmark, encode_bookmark, encode_metadata,
};
use anyhow::{Context, Result};
use poc_common::{Batch, Bookmark};
use redb::{
    Database, Durability, ReadableDatabase, ReadableTable, TableDefinition, WriteTransaction,
};
use std::path::Path;

const PAYLOADS: TableDefinition<u64, &[u8]> = TableDefinition::new("payloads");
const BOOKMARKS: TableDefinition<u64, &[u8]> = TableDefinition::new("bookmarks");
const META: TableDefinition<&str, &[u8]> = TableDefinition::new("metadata");

pub(crate) struct Redb {
    db: Database,
}

impl Redb {
    pub fn open(directory: &Path) -> Result<(Self, State)> {
        let path = directory.join("redb.db");
        let db = Database::builder()
            .set_cache_size(16 * 1024 * 1024)
            .create(path)?;
        let backend = Self { db };
        let new = backend.db.begin_read()?.list_tables()?.next().is_none();
        if new {
            let transaction = backend.transaction()?;
            transaction.open_table(PAYLOADS)?;
            transaction.open_table(BOOKMARKS)?;
            transaction
                .open_table(META)?
                .insert("state", encode_metadata(&State::default())?.as_slice())?;
            transaction.commit()?;
        }
        let transaction = backend.db.begin_read()?;
        let metadata = transaction.open_table(META)?;
        let value = metadata.get("state")?.context("missing redb state")?;
        let state = State::decode(value.value())?;
        drop(value);
        drop(metadata);
        drop(transaction);
        Ok((backend, state))
    }

    fn transaction(&self) -> Result<WriteTransaction> {
        let mut transaction = self.db.begin_write()?;
        transaction.set_durability(Durability::Immediate)?;
        transaction.set_quick_repair(true);
        Ok(transaction)
    }
}

impl Backend for Redb {
    fn append(&mut self, batch: &Batch, bookmark: &Bookmark, state: &State) -> Result<()> {
        let transaction = self.transaction()?;
        transaction
            .open_table(PAYLOADS)?
            .insert(batch.sequence, batch.encode().as_slice())?;
        transaction
            .open_table(BOOKMARKS)?
            .insert(batch.sequence, encode_bookmark(bookmark).as_slice())?;
        transaction
            .open_table(META)?
            .insert("state", encode_metadata(state)?.as_slice())?;
        transaction.commit()?;
        Ok(())
    }

    fn visit(&self, visitor: &mut PayloadVisitor<'_>) -> Result<()> {
        let transaction = self.db.begin_read()?;
        let payloads = transaction.open_table(PAYLOADS)?;
        let bookmarks = transaction.open_table(BOOKMARKS)?;
        for item in payloads.iter()? {
            let (sequence, bytes) = item?;
            let sequence = sequence.value();
            let bookmark = bookmarks.get(sequence)?.context("missing redb bookmark")?;
            visitor(
                sequence,
                bytes.value(),
                decode_bookmark(sequence, bookmark.value())?,
            )?;
        }
        Ok(())
    }

    fn last_bookmarks(&self, limit: usize) -> Result<Vec<Bookmark>> {
        let transaction = self.db.begin_read()?;
        let bookmarks = transaction.open_table(BOOKMARKS)?;
        bookmarks
            .iter()?
            .rev()
            .take(limit)
            .map(|item| {
                let (sequence, value) = item?;
                decode_bookmark(sequence.value(), value.value())
            })
            .collect()
    }

    fn bookmark(&self, sequence: u64) -> Result<Bookmark> {
        let transaction = self.db.begin_read()?;
        let bookmarks = transaction.open_table(BOOKMARKS)?;
        let value = bookmarks.get(sequence)?.context("missing redb bookmark")?;
        decode_bookmark(sequence, value.value())
    }

    fn retain(&mut self, state: &State) -> Result<()> {
        let transaction = self.transaction()?;
        {
            let mut payloads = transaction.open_table(PAYLOADS)?;
            let mut bookmarks = transaction.open_table(BOOKMARKS)?;
            let keys = bookmarks
                .range(..state.retained_from)?
                .map(|entry| entry.map(|(key, _)| key.value()))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            for sequence in keys {
                payloads.remove(sequence)?;
                bookmarks.remove(sequence)?;
            }
        }
        transaction
            .open_table(META)?
            .insert("state", encode_metadata(state)?.as_slice())?;
        transaction.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Engine, tests::TestDirectory};

    #[test]
    fn resumes_initialization_of_an_empty_database_file() -> Result<()> {
        let directory = TestDirectory::new("redb-initialization")?;
        drop(Database::create(directory.0.join("redb.db"))?);
        let mut store = crate::open(Engine::Redb, &directory.0)?;
        store.append(&Batch::generate(0, 0, 1, 16)?)?;
        assert_eq!(store.audit()?.batches, 1);
        Ok(())
    }
}
