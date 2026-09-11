use crate::Engine;
use anyhow::{Context, Result, ensure};
use poc_common::{Audit, Batch, Bookmark};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::Path,
};

pub(crate) const STATE_KEY: &[u8] = b"s";
pub(crate) const PAYLOAD_PREFIX: u8 = b'p';
pub(crate) const BOOKMARK_PREFIX: u8 = b'b';
const MAX_METADATA_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct State {
    pub shard: Option<u32>,
    pub next_sequence: u64,
    pub next_record: u64,
    pub retained_from: u64,
    pub retained_record: u64,
}

impl State {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.retained_from <= self.next_sequence && self.retained_record <= self.next_record,
            "invalid retention state"
        );
        ensure!(
            (self.next_sequence == 0) == self.shard.is_none(),
            "invalid shard state"
        );
        ensure!(
            self.next_record >= self.next_sequence
                && self.retained_record >= self.retained_from
                && self.next_record - self.retained_record
                    >= self.next_sequence - self.retained_from
                && (self.retained_from != 0 || self.retained_record == 0)
                && (self.next_sequence != 0 || self.next_record == 0),
            "invalid absolute record offsets"
        );
        ensure!(
            (self.retained_from == self.next_sequence)
                == (self.retained_record == self.next_record),
            "empty retention state mismatch"
        );
        Ok(())
    }

    pub fn after_append(&self, batch: &Batch) -> Result<Self> {
        ensure!(
            batch.sequence == self.next_sequence,
            "noncontiguous batch: expected {}, received {}",
            self.next_sequence,
            batch.sequence
        );
        ensure!(
            self.shard.is_none_or(|shard| shard == batch.shard),
            "store belongs to another shard"
        );
        batch.validate_payload()?;
        let mut next = self.clone();
        next.shard = Some(batch.shard);
        next.next_sequence = batch.sequence.checked_add(1).context("sequence overflow")?;
        next.next_record = self
            .next_record
            .checked_add(u64::from(batch.records))
            .context("record offset overflow")?;
        Ok(next)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let state: Self = decode_metadata(bytes)?;
        state.validate()?;
        Ok(state)
    }
}

pub(crate) type PayloadVisitor<'a> = dyn FnMut(u64, &[u8], Bookmark) -> Result<()> + 'a;

pub(crate) trait Backend: Send {
    fn append(&mut self, batch: &Batch, bookmark: &Bookmark, state: &State) -> Result<()>;
    fn visit(&self, visitor: &mut PayloadVisitor<'_>) -> Result<()>;
    fn last_bookmarks(&self, limit: usize) -> Result<Vec<Bookmark>>;
    fn bookmark(&self, sequence: u64) -> Result<Bookmark>;
    fn retain(&mut self, state: &State) -> Result<()>;
}

pub(crate) struct Scanner<'a> {
    state: &'a State,
    sequence: u64,
    next_record: u64,
    audit: Audit,
}

impl<'a> Scanner<'a> {
    pub fn new(state: &'a State) -> Self {
        Self {
            state,
            sequence: state.retained_from,
            next_record: state.retained_record,
            audit: Audit::default(),
        }
    }

    pub fn observe(&mut self, key: u64, bytes: &[u8], bookmark: Bookmark) -> Result<()> {
        let batch = Batch::decode(bytes)?;
        batch.validate_payload()?;
        ensure!(
            batch.sequence == key && key == self.sequence,
            "persisted payload sequence mismatch"
        );
        ensure!(
            Some(batch.shard) == self.state.shard,
            "persisted shard mismatch"
        );
        self.next_record = self
            .next_record
            .checked_add(u64::from(batch.records))
            .context("persisted record offset overflow")?;
        ensure!(
            bookmark.sequence == key && bookmark.next_record == self.next_record,
            "persisted bookmark does not match its payload"
        );
        self.sequence = self.sequence.checked_add(1).context("sequence overflow")?;
        self.audit.observe(&batch);
        Ok(())
    }

    pub fn finish(self) -> Result<Audit> {
        ensure!(
            self.sequence == self.state.next_sequence && self.next_record == self.state.next_record,
            "persisted payload tail differs from durable state"
        );
        Ok(self.audit)
    }
}

pub(crate) fn key(prefix: u8, sequence: u64) -> [u8; 9] {
    let mut bytes = [0; 9];
    bytes[0] = prefix;
    bytes[1..].copy_from_slice(&sequence.to_be_bytes());
    bytes
}

pub(crate) fn sequence_from_key(prefix: u8, bytes: &[u8]) -> Result<u64> {
    ensure!(
        bytes.len() == 9 && bytes[0] == prefix,
        "invalid storage key"
    );
    Ok(u64::from_be_bytes(bytes[1..].try_into()?))
}

pub(crate) fn encode_bookmark(bookmark: &Bookmark) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(20);
    bytes.extend_from_slice(&bookmark.sequence.to_le_bytes());
    bytes.extend_from_slice(&bookmark.next_record.to_le_bytes());
    bytes.extend_from_slice(&crc32fast::hash(&bytes).to_le_bytes());
    bytes
}

pub(crate) fn decode_bookmark(sequence: u64, bytes: &[u8]) -> Result<Bookmark> {
    ensure!(bytes.len() == 20, "invalid bookmark length");
    ensure!(
        crc32fast::hash(&bytes[..16]) == u32::from_le_bytes(bytes[16..].try_into()?),
        "bookmark checksum mismatch"
    );
    let bookmark = Bookmark {
        sequence: u64::from_le_bytes(bytes[..8].try_into()?),
        next_record: u64::from_le_bytes(bytes[8..16].try_into()?),
    };
    ensure!(bookmark.sequence == sequence, "bookmark key mismatch");
    Ok(bookmark)
}

pub(crate) fn encode_metadata(value: &impl Serialize) -> Result<Vec<u8>> {
    let mut bytes = b"LSM1".to_vec();
    serde_json::to_writer(&mut bytes, value)?;
    bytes.extend_from_slice(&crc32fast::hash(&bytes).to_le_bytes());
    ensure!(
        bytes.len() as u64 <= MAX_METADATA_BYTES,
        "metadata too large"
    );
    Ok(bytes)
}

pub(crate) fn decode_metadata<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    ensure!(
        bytes.len() >= 8 && bytes.len() as u64 <= MAX_METADATA_BYTES && &bytes[..4] == b"LSM1",
        "invalid metadata format or length"
    );
    let end = bytes.len() - 4;
    ensure!(
        crc32fast::hash(&bytes[..end]) == u32::from_le_bytes(bytes[end..].try_into()?),
        "metadata checksum mismatch"
    );
    Ok(serde_json::from_slice(&bytes[4..end])?)
}

pub(crate) fn read_metadata<T: DeserializeOwned>(path: &Path) -> Result<T> {
    ensure!(
        fs::metadata(path)?.len() <= MAX_METADATA_BYTES,
        "metadata too large"
    );
    decode_metadata(&fs::read(path)?)
}

pub(crate) fn sync_directory(directory: &Path) -> Result<()> {
    File::open(directory)?.sync_all()?;
    Ok(())
}

pub(crate) fn create_directory(directory: &Path) -> Result<()> {
    if directory.is_dir() {
        return Ok(());
    }
    let parent = directory
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    create_directory(parent)?;
    match fs::create_dir(directory) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists && directory.is_dir() => {}
        Err(error) => return Err(error.into()),
    }
    sync_directory(parent)?;
    Ok(())
}

pub(crate) fn publish_metadata(directory: &Path, name: &str, value: &impl Serialize) -> Result<()> {
    let bytes = encode_metadata(value)?;
    let pending = directory.join(format!(".{name}.pending"));
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&pending)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(&pending, directory.join(name))?;
    sync_directory(directory)
}

pub(crate) struct DirectoryLease {
    _file: File,
}

impl DirectoryLease {
    pub fn acquire(directory: &Path, engine: Engine) -> Result<Self> {
        create_directory(directory)?;
        let marker = directory.join("ENGINE");
        if marker.exists() {
            let existing: Engine = read_metadata(&marker)?;
            ensure!(
                existing == engine,
                "store directory uses {existing}, not {engine}"
            );
        }
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(directory.join("STORE.lock"))?;
        fs2::FileExt::try_lock_exclusive(&file)
            .context("store directory is already open by another writer")?;
        let lease = Self { _file: file };
        if marker.exists() {
            let existing: Engine = read_metadata(&marker)?;
            ensure!(
                existing == engine,
                "store directory uses {existing}, not {engine}"
            );
        } else {
            publish_metadata(directory, "ENGINE", &engine)?;
        }
        sync_directory(directory)?;
        Ok(lease)
    }
}
