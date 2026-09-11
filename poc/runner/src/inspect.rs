use std::time::Instant;

use anyhow::{Result, ensure};
use poc_common::{Audit, Bookmark};
use poc_storage::Store;
use serde::Serialize;

use crate::cli::{InspectOptions, MAX_SHARDS};
use crate::directory;

pub const BOOKMARK_LIMIT: usize = 100;

#[derive(Debug, Serialize)]
pub struct Retained {
    pub retain_from: u64,
    pub retention_seconds: f64,
    pub audit_seconds: f64,
    pub bookmark_lookup_seconds: f64,
    pub per_shard: Vec<Audit>,
    pub bookmarks: Vec<Vec<Bookmark>>,
}

#[derive(Debug, Serialize)]
pub struct Report {
    pub reopen_seconds: f64,
    pub audit_seconds: f64,
    pub bookmark_lookup_seconds: f64,
    pub per_shard: Vec<Audit>,
    pub bookmarks: Vec<Vec<Bookmark>>,
    pub bookmark_limit: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after_retention: Option<Retained>,
}

fn audits(stores: &mut [Box<dyn Store>]) -> Result<(Vec<Audit>, f64)> {
    let before = Instant::now();
    let audits = stores
        .iter_mut()
        .map(|store| store.audit())
        .collect::<Result<Vec<_>>>()?;
    Ok((audits, before.elapsed().as_secs_f64()))
}

fn bookmarks(stores: &mut [Box<dyn Store>]) -> Result<(Vec<Vec<Bookmark>>, f64)> {
    let before = Instant::now();
    let bookmarks = stores
        .iter_mut()
        .map(|store| store.last_bookmarks(BOOKMARK_LIMIT))
        .collect::<Result<Vec<_>>>()?;
    Ok((bookmarks, before.elapsed().as_secs_f64()))
}

pub fn run(options: InspectOptions) -> Result<Report> {
    ensure!(
        (1..=MAX_SHARDS).contains(&options.shards),
        "invalid shard count"
    );
    let _lock = directory::lock(&options.dir, false)?;
    for shard in 0..options.shards {
        ensure!(
            directory::shard_path(&options.dir, shard).is_dir(),
            "shard {shard} does not exist; inspect does not initialize empty stores"
        );
    }
    let before = Instant::now();
    let mut stores = Vec::new();
    for shard in 0..options.shards {
        stores.push(poc_storage::open(
            options.engine,
            &directory::shard_path(&options.dir, shard),
        )?);
    }
    let reopen_seconds = before.elapsed().as_secs_f64();
    let (per_shard, audit_seconds) = audits(&mut stores)?;
    let (before_bookmarks, bookmark_lookup_seconds) = bookmarks(&mut stores)?;
    let after_retention = if let Some(retain_from) = options.retain_from {
        let before = Instant::now();
        for store in &mut stores {
            store.retain_from(retain_from)?;
        }
        let retention_seconds = before.elapsed().as_secs_f64();
        let (per_shard, audit_seconds) = audits(&mut stores)?;
        let (bookmarks, bookmark_lookup_seconds) = bookmarks(&mut stores)?;
        Some(Retained {
            retain_from,
            retention_seconds,
            audit_seconds,
            bookmark_lookup_seconds,
            per_shard,
            bookmarks,
        })
    } else {
        None
    };
    Ok(Report {
        reopen_seconds,
        audit_seconds,
        bookmark_lookup_seconds,
        per_shard,
        bookmarks: before_bookmarks,
        bookmark_limit: BOOKMARK_LIMIT,
        after_retention,
    })
}
