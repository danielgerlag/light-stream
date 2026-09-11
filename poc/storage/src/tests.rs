use crate::{
    Engine,
    model::{Backend, State, create_directory},
    open,
};
use anyhow::Result;
use poc_common::{Audit, Batch, Bookmark};
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

pub(crate) struct TestDirectory(pub PathBuf);

impl TestDirectory {
    pub fn new(label: &str) -> Result<Self> {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join(".test-data")
            .join(format!(
                "{label}-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
        create_directory(&path)?;
        Ok(Self(path))
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn lifecycle(engine: Engine) -> Result<()> {
    let directory = TestDirectory::new(&format!("{engine}-lifecycle"))?;
    let batches = [
        Batch::generate(13, 0, 1, 17)?,
        Batch::generate(13, 1, 3, 17)?,
        Batch::generate(13, 2, 2, 17)?,
    ];
    let mut store = open(engine, &directory.0)?;
    assert_eq!(store.audit()?, Audit::default());
    assert!(store.last_bookmarks(100)?.is_empty());
    store.retain_from(u64::MAX)?;
    assert!(store.append(&batches[1]).is_err());
    store.append(&batches[0])?;
    assert!(store.append(&batches[0]).is_err());
    assert!(store.append(&batches[2]).is_err());
    assert!(store.append(&Batch::generate(14, 1, 1, 17)?).is_err());
    let mut invalid = batches[1].clone();
    invalid.payload[0] ^= 1;
    assert!(store.append(&invalid).is_err());
    store.append(&batches[1])?;
    store.append(&batches[2])?;
    let mut expected = Audit::default();
    for batch in &batches {
        expected.observe(batch);
    }
    assert_eq!(store.audit()?, expected);
    let bookmarks = vec![
        Bookmark {
            sequence: 2,
            next_record: 6,
        },
        Bookmark {
            sequence: 1,
            next_record: 4,
        },
        Bookmark {
            sequence: 0,
            next_record: 1,
        },
    ];
    assert_eq!(store.last_bookmarks(usize::MAX)?, bookmarks);
    assert_eq!(store.last_bookmarks(1)?, bookmarks[..1]);
    assert!(store.last_bookmarks(0)?.is_empty());
    assert!(open(engine, &directory.0).is_err());
    drop(store);

    let other = if engine == Engine::Segment {
        Engine::Redb
    } else {
        Engine::Segment
    };
    assert!(open(other, &directory.0).is_err());
    let mut store = open(engine, &directory.0)?;
    assert_eq!(store.audit()?, expected);
    assert_eq!(store.last_bookmarks(10)?, bookmarks);
    store.retain_from(1)?;
    store.retain_from(0)?;
    let mut retained = Audit::default();
    retained.observe(&batches[1]);
    retained.observe(&batches[2]);
    assert_eq!(store.audit()?, retained);
    assert_eq!(store.last_bookmarks(10)?, bookmarks[..2]);
    drop(store);

    let mut store = open(engine, &directory.0)?;
    assert_eq!(store.audit()?, retained);
    assert!(store.append(&batches[0]).is_err());
    store.append(&Batch::generate(13, 3, 4, 17)?)?;
    assert_eq!(store.last_bookmarks(1)?[0].next_record, 10);
    store.retain_from(3)?;
    assert_eq!(store.audit()?.first_sequence, Some(3));
    drop(store);

    let mut store = open(engine, &directory.0)?;
    store.append(&Batch::generate(13, 4, 2, 17)?)?;
    assert_eq!(store.last_bookmarks(1)?[0].next_record, 12);
    store.retain_from(100)?;
    assert_eq!(store.audit()?, Audit::default());
    assert!(store.last_bookmarks(10)?.is_empty());
    drop(store);

    let mut store = open(engine, &directory.0)?;
    assert_eq!(store.audit()?, Audit::default());
    assert!(store.append(&Batch::generate(14, 5, 1, 17)?).is_err());
    store.append(&Batch::generate(13, 5, 2, 17)?)?;
    assert_eq!(store.audit()?.first_sequence, Some(5));
    assert_eq!(
        store.last_bookmarks(10)?,
        vec![Bookmark {
            sequence: 5,
            next_record: 14
        }]
    );
    drop(store);
    let mut store = open(engine, &directory.0)?;
    assert_eq!(store.audit()?.records, 2);
    assert_eq!(store.last_bookmarks(1)?[0].next_record, 14);
    Ok(())
}

fn audits_actual_payloads(engine: Engine) -> Result<()> {
    for malformed_dimensions in [false, true] {
        let directory = TestDirectory::new(&format!("{engine}-audit"))?;
        let (mut backend, state): (Box<dyn Backend>, State) = match engine {
            Engine::Segment => {
                let (backend, state) = crate::segment::Segment::open(&directory.0)?;
                (Box::new(backend), state)
            }
            Engine::Redb => {
                let (backend, state) = crate::redb_store::Redb::open(&directory.0)?;
                (Box::new(backend), state)
            }
            Engine::Fjall => {
                let (backend, state) = crate::fjall_store::Fjall::open(&directory.0)?;
                (Box::new(backend), state)
            }
            Engine::Rocksdb => {
                let (backend, state) = crate::rocksdb_store::Rocks::open(&directory.0)?;
                (Box::new(backend), state)
            }
        };
        let mut batch = Batch::generate(3, 0, 2, 17)?;
        let state = state.after_append(&batch)?;
        let marker = Bookmark {
            sequence: 0,
            next_record: 2,
        };
        if malformed_dimensions {
            batch.record_bytes += 1;
        } else {
            batch.payload[0] ^= 1;
            assert!(Batch::decode(&batch.encode()).is_ok());
        }
        backend.append(&batch, &marker, &state)?;
        drop(backend);
        let mut store = open(engine, &directory.0)?;
        assert_eq!(store.last_bookmarks(10)?, vec![marker]);
        let error = store
            .audit()
            .expect_err("audit must read and validate persisted payload")
            .to_string();
        assert!(
            error.contains(if malformed_dimensions {
                "batch payload length mismatch"
            } else {
                "persisted payload differs"
            }),
            "{error}"
        );
    }
    Ok(())
}

#[test]
fn durable_child() -> Result<()> {
    let Some(directory) = std::env::var_os("POC_STORAGE_DURABILITY_DIRECTORY") else {
        return Ok(());
    };
    let engine = <Engine as clap::ValueEnum>::from_str(
        &std::env::var("POC_STORAGE_DURABILITY_ENGINE")?,
        false,
    )
    .map_err(anyhow::Error::msg)?;
    let mut store = open(engine, &PathBuf::from(directory))?;
    store.append(&Batch::generate(9, 0, 2, 19)?)?;
    store.append(&Batch::generate(9, 1, 3, 19)?)?;
    store.retain_from(1)?;
    store.append(&Batch::generate(9, 2, 4, 19)?)?;
    // Bypass database destructors and their final journal flushes.
    std::process::exit(0);
}

fn survives_without_clean_shutdown(engine: Engine) -> Result<()> {
    let directory = TestDirectory::new(&format!("{engine}-durable-exit"))?;
    let output = std::process::Command::new(std::env::current_exe()?)
        .args(["--exact", "tests::durable_child", "--nocapture"])
        .env("POC_STORAGE_DURABILITY_DIRECTORY", &directory.0)
        .env("POC_STORAGE_DURABILITY_ENGINE", engine.to_string())
        .output()?;
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let mut store = open(engine, &directory.0)?;
    let mut expected = Audit::default();
    expected.observe(&Batch::generate(9, 1, 3, 19)?);
    expected.observe(&Batch::generate(9, 2, 4, 19)?);
    assert_eq!(store.audit()?, expected);
    assert_eq!(
        store.last_bookmarks(10)?,
        vec![
            Bookmark {
                sequence: 2,
                next_record: 9,
            },
            Bookmark {
                sequence: 1,
                next_record: 5,
            },
        ]
    );
    store.append(&Batch::generate(9, 3, 1, 19)?)?;
    assert_eq!(store.last_bookmarks(1)?[0].next_record, 10);
    Ok(())
}

macro_rules! engine_tests {
    ($name:ident, $engine:expr) => {
        mod $name {
            use super::*;

            #[test]
            fn append_order_bookmarks_reopen_retention_and_resume() -> Result<()> {
                lifecycle($engine)
            }

            #[test]
            fn audit_decodes_and_validates_persisted_payload_without_bookmark_scans() -> Result<()>
            {
                audits_actual_payloads($engine)
            }

            #[test]
            fn append_and_retention_survive_without_clean_shutdown() -> Result<()> {
                survives_without_clean_shutdown($engine)
            }
        }
    };
}

engine_tests!(segment, Engine::Segment);
engine_tests!(redb, Engine::Redb);
engine_tests!(fjall, Engine::Fjall);
engine_tests!(rocksdb, Engine::Rocksdb);
