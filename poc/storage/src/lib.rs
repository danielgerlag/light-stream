mod fjall_store;
mod model;
mod redb_store;
mod rocksdb_store;
mod segment;

use anyhow::{Context, Result, ensure};
use clap::ValueEnum;
use model::{Backend, DirectoryLease, Scanner, State};
use poc_common::{Audit, Batch, Bookmark};
use serde::{Deserialize, Serialize};
use std::{fmt, path::Path};

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Engine {
    Segment,
    Redb,
    Fjall,
    Rocksdb,
}

impl fmt::Display for Engine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Segment => "segment",
            Self::Redb => "redb",
            Self::Fjall => "fjall",
            Self::Rocksdb => "rocksdb",
        })
    }
}

pub trait Store: Send {
    fn append(&mut self, batch: &Batch) -> Result<()>;
    fn audit(&mut self) -> Result<Audit>;
    fn last_bookmarks(&mut self, limit: usize) -> Result<Vec<Bookmark>>;
    fn retain_from(&mut self, sequence: u64) -> Result<()>;
}

pub fn open(engine: Engine, directory: &Path) -> Result<Box<dyn Store>> {
    let lease = DirectoryLease::acquire(directory, engine)?;
    let (backend, state): (Box<dyn Backend>, State) = match engine {
        Engine::Segment => {
            let (backend, state) = segment::Segment::open(directory)?;
            (Box::new(backend), state)
        }
        Engine::Redb => {
            let (backend, state) = redb_store::Redb::open(directory)?;
            (Box::new(backend), state)
        }
        Engine::Fjall => {
            let (backend, state) = fjall_store::Fjall::open(directory)?;
            (Box::new(backend), state)
        }
        Engine::Rocksdb => {
            let (backend, state) = rocksdb_store::Rocks::open(directory)?;
            (Box::new(backend), state)
        }
    };
    model::sync_directory(directory)?;
    let mut store = ShardStore {
        backend,
        state: Some(state),
        _lease: lease,
    };
    store.last_bookmarks(1)?;
    Ok(Box::new(store))
}

struct ShardStore {
    backend: Box<dyn Backend>,
    // A failed commit can have reached disk. Reopening resolves that uncertainty.
    state: Option<State>,
    _lease: DirectoryLease,
}

impl ShardStore {
    fn state(&self) -> Result<&State> {
        self.state
            .as_ref()
            .context("storage operation failed; close and reopen this store before continuing")
    }
}

impl Store for ShardStore {
    fn append(&mut self, batch: &Batch) -> Result<()> {
        let next = self.state()?.after_append(batch)?;
        let bookmark = Bookmark {
            sequence: batch.sequence,
            next_record: next.next_record,
        };
        self.state = None;
        self.backend.append(batch, &bookmark, &next)?;
        self.state = Some(next);
        Ok(())
    }

    fn audit(&mut self) -> Result<Audit> {
        let mut scanner = Scanner::new(self.state()?);
        self.backend
            .visit(&mut |sequence, bytes, bookmark| scanner.observe(sequence, bytes, bookmark))?;
        scanner.finish()
    }

    fn last_bookmarks(&mut self, limit: usize) -> Result<Vec<Bookmark>> {
        let state = self.state()?;
        let bookmarks = self.backend.last_bookmarks(limit)?;
        let expected = (state.next_sequence - state.retained_from).min(limit as u64);
        ensure!(
            bookmarks.len() as u64 == expected,
            "bookmark count mismatch"
        );
        let mut next_record = None;
        for (index, bookmark) in bookmarks.iter().enumerate() {
            ensure!(
                bookmark.sequence == state.next_sequence - 1 - index as u64,
                "bookmark sequence mismatch"
            );
            if let Some(previous) = next_record {
                ensure!(
                    bookmark.next_record < previous,
                    "bookmark offsets out of order"
                );
            } else {
                ensure!(
                    bookmark.next_record == state.next_record,
                    "last bookmark offset differs from durable state"
                );
            }
            ensure!(
                bookmark.next_record > state.retained_record,
                "bookmark precedes retained record offset"
            );
            next_record = Some(bookmark.next_record);
        }
        Ok(bookmarks)
    }

    fn retain_from(&mut self, sequence: u64) -> Result<()> {
        let state = self.state()?;
        let cutoff = sequence.min(state.next_sequence);
        if cutoff <= state.retained_from {
            return Ok(());
        }
        let marker = self.backend.bookmark(cutoff - 1)?;
        ensure!(marker.sequence == cutoff - 1, "retention bookmark mismatch");
        ensure!(
            marker.next_record > state.retained_record && marker.next_record <= state.next_record,
            "retention bookmark offset mismatch"
        );
        let mut next = state.clone();
        next.retained_from = cutoff;
        next.retained_record = marker.next_record;
        next.validate()?;
        self.state = None;
        self.backend.retain(&next)?;
        self.state = Some(next);
        Ok(())
    }
}

#[cfg(test)]
mod tests;
