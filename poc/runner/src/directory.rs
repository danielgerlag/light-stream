use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, ensure};
use fs2::FileExt;

pub fn lock(directory: &Path, create: bool) -> Result<File> {
    if create {
        std::fs::create_dir_all(directory)?;
    }
    ensure!(
        directory.is_dir(),
        "store directory does not exist: {}",
        directory.display()
    );
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(directory.join(".stream-poc.lock"))?;
    file.try_lock_exclusive().with_context(|| {
        format!(
            "store is in use; stop its node before inspecting {}",
            directory.display()
        )
    })?;
    Ok(file)
}

pub fn shard_path(directory: &Path, shard: u32) -> PathBuf {
    directory.join(format!("shard-{shard}"))
}
