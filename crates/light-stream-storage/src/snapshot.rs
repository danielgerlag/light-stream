use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use crc32fast::Hasher as Crc32;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

const SNAPSHOT_V3_MAGIC: &[u8; 8] = b"LSNP0003";
const SNAPSHOT_V3_FOOTER: &[u8; 8] = b"LSNF0003";
const SNAPSHOT_V3_FORMAT: u32 = 3;
const STATE_RECORD: u8 = 1;
const PAYLOAD_RECORD: u8 = 2;
const MAX_SNAPSHOT_FRAME_BYTES: usize = 8 * 1024 * 1024;

pub(crate) enum SnapshotRecord {
    State { key: Vec<u8>, value: Vec<u8> },
    Payload { key: Vec<u8>, value: Vec<u8> },
}

pub(crate) struct SnapshotArtifactReader {
    reader: BufReader<File>,
    body_digest: Sha256,
    storage_format_version: u32,
    identity_json: Vec<u8>,
    meta_json: Vec<u8>,
    state_count: u64,
    payload_count: u64,
    last_state: Option<Vec<u8>>,
    last_payload: Option<Vec<u8>>,
    finished: bool,
}

pub(crate) struct DecodedSnapshotV3 {
    pub storage_format_version: u32,
    pub identity_json: Vec<u8>,
    pub meta_json: Vec<u8>,
    pub state: Vec<(Vec<u8>, Vec<u8>)>,
    pub payloads: Vec<(Vec<u8>, Vec<u8>)>,
}

pub(crate) fn decode_snapshot_v3(bytes: &[u8]) -> io::Result<Option<DecodedSnapshotV3>> {
    if !bytes.starts_with(SNAPSHOT_V3_MAGIC) {
        return Ok(None);
    }
    let mut cursor = ByteCursor::new(bytes);
    cursor.expect(SNAPSHOT_V3_MAGIC)?;
    if cursor.read_u32()? != SNAPSHOT_V3_FORMAT {
        return Err(io::Error::other(
            "unsupported streamed snapshot artifact version",
        ));
    }
    let storage_format_version = cursor.read_u32()?;
    let identity_len =
        usize::try_from(cursor.read_u32()?).map_err(|error| io::Error::other(error.to_string()))?;
    let meta_len =
        usize::try_from(cursor.read_u32()?).map_err(|error| io::Error::other(error.to_string()))?;
    let identity_json = cursor.take(identity_len)?.to_vec();
    let meta_json = cursor.take(meta_len)?.to_vec();
    let header_end = cursor.offset;
    let expected_header_crc = cursor.read_u32()?;
    let mut header_crc = Crc32::new();
    header_crc.update(&bytes[..header_end]);
    if header_crc.finalize() != expected_header_crc {
        return Err(io::Error::other(
            "snapshot artifact header checksum mismatch",
        ));
    }
    let mut state = Vec::new();
    let mut payloads = Vec::new();
    let mut last_state = None;
    let mut last_payload = None;
    loop {
        if cursor.remaining().starts_with(SNAPSHOT_V3_FOOTER) {
            let footer_start = cursor.offset;
            cursor.expect(SNAPSHOT_V3_FOOTER)?;
            let state_count = cursor.read_u64()?;
            let payload_count = cursor.read_u64()?;
            let expected_body_digest = cursor.take(32)?;
            let footer_crc_end = cursor.offset;
            let expected_footer_crc = cursor.read_u32()?;
            let mut footer_crc = Crc32::new();
            footer_crc.update(&bytes[footer_start..footer_crc_end]);
            if footer_crc.finalize() != expected_footer_crc {
                return Err(io::Error::other(
                    "snapshot artifact footer checksum mismatch",
                ));
            }
            if Sha256::digest(&bytes[..footer_start]).as_slice() != expected_body_digest {
                return Err(io::Error::other("snapshot artifact body digest mismatch"));
            }
            if state_count != state.len() as u64 || payload_count != payloads.len() as u64 {
                return Err(io::Error::other("snapshot artifact record counts mismatch"));
            }
            if !cursor.is_finished() {
                return Err(io::Error::other("snapshot artifact has trailing bytes"));
            }
            break;
        }
        let record_start = cursor.offset;
        let tag = cursor.read_u8()?;
        let key_len = usize::try_from(cursor.read_u32()?)
            .map_err(|error| io::Error::other(error.to_string()))?;
        let value_len = usize::try_from(cursor.read_u64()?)
            .map_err(|error| io::Error::other(error.to_string()))?;
        let key = cursor.take(key_len)?.to_vec();
        let value = cursor.take(value_len)?.to_vec();
        let record_crc_end = cursor.offset;
        let expected_crc = cursor.read_u32()?;
        let mut crc = Crc32::new();
        crc.update(&bytes[record_start..record_crc_end]);
        if crc.finalize() != expected_crc {
            return Err(io::Error::other(
                "snapshot artifact record checksum mismatch",
            ));
        }
        match tag {
            STATE_RECORD => {
                if !payloads.is_empty()
                    || last_state
                        .as_ref()
                        .is_some_and(|previous: &Vec<u8>| previous >= &key)
                {
                    return Err(io::Error::other(
                        "snapshot state records are not strictly ordered",
                    ));
                }
                last_state = Some(key.clone());
                state.push((key, value));
            }
            PAYLOAD_RECORD => {
                if last_payload
                    .as_ref()
                    .is_some_and(|previous: &Vec<u8>| previous >= &key)
                {
                    return Err(io::Error::other(
                        "snapshot payload records are not strictly ordered",
                    ));
                }
                last_payload = Some(key.clone());
                payloads.push((key, value));
            }
            _ => return Err(io::Error::other("unknown snapshot artifact record tag")),
        }
    }
    Ok(Some(DecodedSnapshotV3 {
        storage_format_version,
        identity_json,
        meta_json,
        state,
        payloads,
    }))
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct SnapshotDigest([u8; 32]);

impl SnapshotDigest {
    pub fn from_slice(value: &[u8]) -> io::Result<Self> {
        let bytes: [u8; 32] = value
            .try_into()
            .map_err(|_| io::Error::other("snapshot digest must contain 32 bytes"))?;
        Ok(Self(bytes))
    }

    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

#[derive(Clone, Debug)]
pub struct SnapshotArtifact {
    inner: Arc<SnapshotArtifactInner>,
}

#[derive(Debug)]
struct SnapshotArtifactInner {
    file: Mutex<File>,
    byte_len: u64,
    digest: SnapshotDigest,
}

impl SnapshotArtifact {
    pub fn open_verified(path: PathBuf, byte_len: u64, digest: SnapshotDigest) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).open(&path)?;
        if file.metadata()?.len() != byte_len {
            return Err(io::Error::other(
                "snapshot artifact length does not match its descriptor",
            ));
        }
        Ok(Self {
            inner: Arc::new(SnapshotArtifactInner {
                file: Mutex::new(file),
                byte_len,
                digest,
            }),
        })
    }

    pub fn len(&self) -> u64 {
        self.inner.byte_len
    }

    pub fn is_empty(&self) -> bool {
        self.inner.byte_len == 0
    }

    pub fn digest(&self) -> SnapshotDigest {
        self.inner.digest
    }

    pub fn read_chunk(&self, offset: u64, max_bytes: usize) -> io::Result<Vec<u8>> {
        if offset > self.inner.byte_len {
            return Err(io::Error::other(
                "snapshot offset exceeds the artifact length",
            ));
        }
        let remaining = self.inner.byte_len - offset;
        let len = usize::try_from(remaining.min(max_bytes as u64))
            .map_err(|error| io::Error::other(error.to_string()))?;
        let mut bytes = vec![0; len];
        let mut file = self
            .inner
            .file
            .lock()
            .map_err(|_| io::Error::other("snapshot artifact file lock poisoned"))?;
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(&mut bytes)?;
        Ok(bytes)
    }

    pub fn read_all_limited(&self, max_bytes: usize) -> io::Result<Vec<u8>> {
        let len = usize::try_from(self.inner.byte_len)
            .map_err(|error| io::Error::other(error.to_string()))?;
        if len > max_bytes {
            return Err(io::Error::other(
                "snapshot artifact exceeds the transitional install limit",
            ));
        }
        self.read_chunk(0, len)
    }

    pub(crate) fn reader(&self) -> io::Result<SnapshotArtifactReader> {
        let mut file = self
            .inner
            .file
            .lock()
            .map_err(|_| io::Error::other("snapshot artifact file lock poisoned"))?
            .try_clone()?;
        file.seek(SeekFrom::Start(0))?;
        SnapshotArtifactReader::open(file)
    }
}

impl SnapshotArtifactReader {
    fn open(file: File) -> io::Result<Self> {
        let mut value = Self {
            reader: BufReader::with_capacity(1024 * 1024, file),
            body_digest: Sha256::new(),
            storage_format_version: 0,
            identity_json: Vec::new(),
            meta_json: Vec::new(),
            state_count: 0,
            payload_count: 0,
            last_state: None,
            last_payload: None,
            finished: false,
        };
        let mut header = Vec::new();
        let mut magic = [0_u8; 8];
        value.read_body(&mut magic)?;
        header.extend_from_slice(&magic);
        if &magic != SNAPSHOT_V3_MAGIC {
            return Err(io::Error::other(
                "streaming snapshot reader requires LSNP0003",
            ));
        }
        let format = value.read_body_u32(&mut header)?;
        if format != SNAPSHOT_V3_FORMAT {
            return Err(io::Error::other(
                "unsupported streamed snapshot artifact version",
            ));
        }
        value.storage_format_version = value.read_body_u32(&mut header)?;
        let identity_len = usize::try_from(value.read_body_u32(&mut header)?)
            .map_err(|error| io::Error::other(error.to_string()))?;
        let meta_len = usize::try_from(value.read_body_u32(&mut header)?)
            .map_err(|error| io::Error::other(error.to_string()))?;
        if identity_len + meta_len > MAX_SNAPSHOT_FRAME_BYTES {
            return Err(io::Error::other("snapshot artifact header is oversized"));
        }
        let mut identity_json = vec![0; identity_len];
        value.read_body(&mut identity_json)?;
        header.extend_from_slice(&identity_json);
        value.identity_json = identity_json;
        let mut meta_json = vec![0; meta_len];
        value.read_body(&mut meta_json)?;
        header.extend_from_slice(&meta_json);
        value.meta_json = meta_json;
        let expected_crc = value.read_body_u32(&mut Vec::new())?;
        let mut crc = Crc32::new();
        crc.update(&header);
        if crc.finalize() != expected_crc {
            return Err(io::Error::other(
                "snapshot artifact header checksum mismatch",
            ));
        }
        Ok(value)
    }

    pub(crate) const fn storage_format_version(&self) -> u32 {
        self.storage_format_version
    }

    pub(crate) fn identity_json(&self) -> &[u8] {
        &self.identity_json
    }

    pub(crate) fn meta_json(&self) -> &[u8] {
        &self.meta_json
    }

    pub(crate) fn next_record(&mut self) -> io::Result<Option<SnapshotRecord>> {
        if self.finished {
            return Ok(None);
        }
        let mut first = [0_u8; 1];
        self.reader.read_exact(&mut first)?;
        if first[0] == SNAPSHOT_V3_FOOTER[0] {
            let mut footer_magic = [0_u8; 8];
            footer_magic[0] = first[0];
            self.reader.read_exact(&mut footer_magic[1..])?;
            if &footer_magic != SNAPSHOT_V3_FOOTER {
                return Err(io::Error::other("snapshot artifact footer magic mismatch"));
            }
            let state_count = read_u64(&mut self.reader)?;
            let payload_count = read_u64(&mut self.reader)?;
            let mut expected_body = [0_u8; 32];
            self.reader.read_exact(&mut expected_body)?;
            let expected_footer_crc = read_u32(&mut self.reader)?;
            let mut footer = Vec::with_capacity(56);
            footer.extend_from_slice(&footer_magic);
            footer.extend_from_slice(&state_count.to_be_bytes());
            footer.extend_from_slice(&payload_count.to_be_bytes());
            footer.extend_from_slice(&expected_body);
            let mut crc = Crc32::new();
            crc.update(&footer);
            if crc.finalize() != expected_footer_crc {
                return Err(io::Error::other(
                    "snapshot artifact footer checksum mismatch",
                ));
            }
            if self.body_digest.clone().finalize().as_slice() != expected_body {
                return Err(io::Error::other("snapshot artifact body digest mismatch"));
            }
            if state_count != self.state_count || payload_count != self.payload_count {
                return Err(io::Error::other("snapshot artifact record counts mismatch"));
            }
            let mut trailing = [0_u8; 1];
            if self.reader.read(&mut trailing)? != 0 {
                return Err(io::Error::other("snapshot artifact has trailing bytes"));
            }
            self.finished = true;
            return Ok(None);
        }
        self.body_digest.update(first);
        let tag = first[0];
        let mut header = [0_u8; 12];
        self.read_body(&mut header)?;
        let key_len = usize::try_from(u32::from_be_bytes(header[..4].try_into().map_err(
            |error: std::array::TryFromSliceError| io::Error::other(error.to_string()),
        )?))
        .map_err(|error| io::Error::other(error.to_string()))?;
        let value_len = usize::try_from(u64::from_be_bytes(header[4..].try_into().map_err(
            |error: std::array::TryFromSliceError| io::Error::other(error.to_string()),
        )?))
        .map_err(|error| io::Error::other(error.to_string()))?;
        if key_len
            .checked_add(value_len)
            .is_none_or(|len| len > MAX_SNAPSHOT_FRAME_BYTES)
        {
            return Err(io::Error::other("snapshot artifact record is oversized"));
        }
        let mut key = vec![0; key_len];
        self.read_body(&mut key)?;
        let mut value = vec![0; value_len];
        self.read_body(&mut value)?;
        let expected_crc = self.read_body_u32(&mut Vec::new())?;
        let mut crc = Crc32::new();
        crc.update(&first);
        crc.update(&header);
        crc.update(&key);
        crc.update(&value);
        if crc.finalize() != expected_crc {
            return Err(io::Error::other(
                "snapshot artifact record checksum mismatch",
            ));
        }
        match tag {
            STATE_RECORD => {
                if self.payload_count != 0
                    || self
                        .last_state
                        .as_ref()
                        .is_some_and(|previous| previous >= &key)
                {
                    return Err(io::Error::other(
                        "snapshot state records are not strictly ordered",
                    ));
                }
                self.last_state = Some(key.clone());
                self.state_count += 1;
                Ok(Some(SnapshotRecord::State { key, value }))
            }
            PAYLOAD_RECORD => {
                if self
                    .last_payload
                    .as_ref()
                    .is_some_and(|previous| previous >= &key)
                {
                    return Err(io::Error::other(
                        "snapshot payload records are not strictly ordered",
                    ));
                }
                self.last_payload = Some(key.clone());
                self.payload_count += 1;
                Ok(Some(SnapshotRecord::Payload { key, value }))
            }
            _ => Err(io::Error::other("unknown snapshot artifact record tag")),
        }
    }

    fn read_body(&mut self, bytes: &mut [u8]) -> io::Result<()> {
        self.reader.read_exact(bytes)?;
        self.body_digest.update(&*bytes);
        Ok(())
    }

    fn read_body_u32(&mut self, capture: &mut Vec<u8>) -> io::Result<u32> {
        let mut bytes = [0_u8; 4];
        self.read_body(&mut bytes)?;
        capture.extend_from_slice(&bytes);
        Ok(u32::from_be_bytes(bytes))
    }
}

fn read_u32(reader: &mut impl Read) -> io::Result<u32> {
    let mut bytes = [0_u8; 4];
    reader.read_exact(&mut bytes)?;
    Ok(u32::from_be_bytes(bytes))
}

fn read_u64(reader: &mut impl Read) -> io::Result<u64> {
    let mut bytes = [0_u8; 8];
    reader.read_exact(&mut bytes)?;
    Ok(u64::from_be_bytes(bytes))
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct StoredArtifactDescriptor {
    pub format_version: u32,
    pub file_name: String,
    pub byte_len: u64,
    pub digest: SnapshotDigest,
}

#[derive(Clone, Debug)]
pub(crate) struct SnapshotCatalog {
    objects: PathBuf,
}

impl SnapshotCatalog {
    pub(crate) fn open(group_root: &Path) -> io::Result<Self> {
        let objects = group_root.join("snapshots/objects");
        fs::create_dir_all(&objects)?;
        Ok(Self { objects })
    }

    pub(crate) fn reconcile_on_open(&self) -> io::Result<()> {
        for entry in fs::read_dir(&self.objects)? {
            let entry = entry?;
            let name = entry.file_name();
            if (name.to_string_lossy().starts_with(".build-")
                || name.to_string_lossy().starts_with(".adopt-"))
                && name.to_string_lossy().ends_with(".part")
            {
                fs::remove_file(entry.path())?;
            }
        }
        Ok(())
    }

    pub(crate) fn store_bytes(
        &self,
        bytes: &[u8],
    ) -> io::Result<(SnapshotArtifact, StoredArtifactDescriptor)> {
        let digest = SnapshotDigest::from_slice(&Sha256::digest(bytes))?;
        let file_name = format!("{}.lsnap", hex_digest(digest));
        let final_path = self.objects.join(&file_name);
        if !final_path.exists() {
            let temporary = self.objects.join(format!(".{file_name}.tmp"));
            let mut file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&temporary)?;
            file.write_all(bytes)?;
            file.sync_all()?;
            match fs::rename(&temporary, &final_path) {
                Ok(()) => {}
                Err(error) if final_path.exists() => {
                    let _ = fs::remove_file(&temporary);
                    if error.kind() != io::ErrorKind::AlreadyExists {
                        verify_file(&final_path, bytes.len() as u64, digest)?;
                    }
                }
                Err(error) => return Err(error),
            }
            File::open(&self.objects)?.sync_all()?;
        }
        verify_file(&final_path, bytes.len() as u64, digest)?;
        let artifact = SnapshotArtifact::open_verified(final_path, bytes.len() as u64, digest)?;
        Ok((
            artifact,
            StoredArtifactDescriptor {
                format_version: 1,
                file_name,
                byte_len: bytes.len() as u64,
                digest,
            },
        ))
    }

    pub(crate) fn begin_artifact(
        &self,
        storage_format_version: u32,
        identity_json: &[u8],
        meta_json: &[u8],
    ) -> io::Result<SnapshotArtifactWriter> {
        let temporary = self.objects.join(format!(".build-{}.part", Uuid::new_v4()));
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)?;
        let mut writer = SnapshotArtifactWriter {
            catalog: self.clone(),
            temporary,
            file: Some(BufWriter::with_capacity(1024 * 1024, file)),
            digest: Sha256::new(),
            byte_len: 0,
            state_count: 0,
            payload_count: 0,
            finished: false,
        };
        let identity_len = u32::try_from(identity_json.len())
            .map_err(|error| io::Error::other(error.to_string()))?;
        let meta_len =
            u32::try_from(meta_json.len()).map_err(|error| io::Error::other(error.to_string()))?;
        let mut header = Vec::with_capacity(
            SNAPSHOT_V3_MAGIC.len() + 20 + identity_json.len() + meta_json.len(),
        );
        header.extend_from_slice(SNAPSHOT_V3_MAGIC);
        header.extend_from_slice(&SNAPSHOT_V3_FORMAT.to_be_bytes());
        header.extend_from_slice(&storage_format_version.to_be_bytes());
        header.extend_from_slice(&identity_len.to_be_bytes());
        header.extend_from_slice(&meta_len.to_be_bytes());
        header.extend_from_slice(identity_json);
        header.extend_from_slice(meta_json);
        let mut checksum = Crc32::new();
        checksum.update(&header);
        header.extend_from_slice(&checksum.finalize().to_be_bytes());
        writer.write_hashed(&header)?;
        Ok(writer)
    }

    pub(crate) fn adopt(
        &self,
        artifact: &SnapshotArtifact,
    ) -> io::Result<(SnapshotArtifact, StoredArtifactDescriptor)> {
        let prefix = artifact.read_chunk(0, SNAPSHOT_V3_MAGIC.len())?;
        if prefix.as_slice() != SNAPSHOT_V3_MAGIC {
            let bytes = artifact.read_all_limited(super::MAX_SNAPSHOT_BYTES)?;
            return self.store_bytes(&bytes);
        }
        let digest = artifact.digest();
        let byte_len = artifact.len();
        let file_name = format!("{}.lsnap", hex_digest(digest));
        let final_path = self.objects.join(&file_name);
        if !final_path.exists() {
            let temporary = self.objects.join(format!(".adopt-{}.part", Uuid::new_v4()));
            let result = (|| {
                let mut file = BufWriter::with_capacity(
                    1024 * 1024,
                    OpenOptions::new()
                        .create_new(true)
                        .write(true)
                        .open(&temporary)?,
                );
                let mut offset = 0;
                while offset < byte_len {
                    let chunk = artifact.read_chunk(offset, 1024 * 1024)?;
                    if chunk.is_empty() {
                        return Err(io::Error::other(
                            "snapshot artifact ended before its declared length",
                        ));
                    }
                    file.write_all(&chunk)?;
                    offset += chunk.len() as u64;
                }
                file.flush()?;
                file.get_ref().sync_all()?;
                drop(file);
                match fs::rename(&temporary, &final_path) {
                    Ok(()) => File::open(&self.objects)?.sync_all()?,
                    Err(error) if final_path.exists() => {
                        if error.kind() != io::ErrorKind::AlreadyExists {
                            verify_file(&final_path, byte_len, digest)?;
                        }
                    }
                    Err(error) => return Err(error),
                }
                Ok(())
            })();
            if result.is_err() || final_path.exists() {
                let _ = fs::remove_file(&temporary);
            }
            result?;
        }
        verify_file(&final_path, byte_len, digest)?;
        let adopted = SnapshotArtifact::open_verified(final_path, byte_len, digest)?;
        Ok((
            adopted,
            StoredArtifactDescriptor {
                format_version: SNAPSHOT_V3_FORMAT,
                file_name,
                byte_len,
                digest,
            },
        ))
    }

    pub(crate) fn open_descriptor(
        &self,
        descriptor: &StoredArtifactDescriptor,
    ) -> io::Result<SnapshotArtifact> {
        if !matches!(descriptor.format_version, 1 | SNAPSHOT_V3_FORMAT)
            || descriptor.file_name != format!("{}.lsnap", hex_digest(descriptor.digest))
        {
            return Err(io::Error::other(
                "invalid stored snapshot artifact descriptor",
            ));
        }
        let path = self.objects.join(&descriptor.file_name);
        verify_file(&path, descriptor.byte_len, descriptor.digest)?;
        SnapshotArtifact::open_verified(path, descriptor.byte_len, descriptor.digest)
    }

    pub(crate) fn collect_except(&self, current: &StoredArtifactDescriptor) -> io::Result<()> {
        for entry in fs::read_dir(&self.objects)? {
            let entry = entry?;
            let path = entry.path();
            let name = entry.file_name();
            if name.to_string_lossy() == current.file_name {
                continue;
            }
            let is_snapshot = path.extension().and_then(|value| value.to_str()) == Some("lsnap");
            let is_temporary = name.to_string_lossy().ends_with(".tmp");
            if !is_snapshot && !is_temporary {
                continue;
            }
            match fs::remove_file(&path) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {}
                Err(error) => return Err(error),
            }
        }

        Ok(())
    }
}

pub(crate) struct SnapshotArtifactWriter {
    catalog: SnapshotCatalog,
    temporary: PathBuf,
    file: Option<BufWriter<File>>,
    digest: Sha256,
    byte_len: u64,
    state_count: u64,
    payload_count: u64,
    finished: bool,
}

impl SnapshotArtifactWriter {
    pub(crate) fn write_state(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        self.write_record(STATE_RECORD, key, value)?;
        self.state_count = self
            .state_count
            .checked_add(1)
            .ok_or_else(|| io::Error::other("snapshot state count overflow"))?;
        Ok(())
    }

    pub(crate) fn write_payload(&mut self, id: &[u8], value: &[u8]) -> io::Result<()> {
        self.write_record(PAYLOAD_RECORD, id, value)?;
        self.payload_count = self
            .payload_count
            .checked_add(1)
            .ok_or_else(|| io::Error::other("snapshot payload count overflow"))?;
        Ok(())
    }

    pub(crate) fn finish(mut self) -> io::Result<(SnapshotArtifact, StoredArtifactDescriptor)> {
        let body_digest = self.digest.clone().finalize();
        let mut footer = Vec::with_capacity(SNAPSHOT_V3_FOOTER.len() + 52);
        footer.extend_from_slice(SNAPSHOT_V3_FOOTER);
        footer.extend_from_slice(&self.state_count.to_be_bytes());
        footer.extend_from_slice(&self.payload_count.to_be_bytes());
        footer.extend_from_slice(&body_digest);
        let mut checksum = Crc32::new();
        checksum.update(&footer);
        footer.extend_from_slice(&checksum.finalize().to_be_bytes());
        self.write_hashed(&footer)?;
        let digest = SnapshotDigest::from_slice(&self.digest.clone().finalize())?;
        let byte_len = self.byte_len;
        let mut file = self
            .file
            .take()
            .ok_or_else(|| io::Error::other("snapshot artifact writer is closed"))?;
        file.flush()?;
        file.get_ref().sync_all()?;
        drop(file);
        let file_name = format!("{}.lsnap", hex_digest(digest));
        let final_path = self.catalog.objects.join(&file_name);
        if final_path.exists() {
            verify_file(&final_path, byte_len, digest)?;
            fs::remove_file(&self.temporary)?;
        } else {
            fs::rename(&self.temporary, &final_path)?;
            File::open(&self.catalog.objects)?.sync_all()?;
        }
        self.finished = true;
        let artifact = SnapshotArtifact::open_verified(final_path, byte_len, digest)?;
        Ok((
            artifact,
            StoredArtifactDescriptor {
                format_version: SNAPSHOT_V3_FORMAT,
                file_name,
                byte_len,
                digest,
            },
        ))
    }

    fn write_record(&mut self, tag: u8, key: &[u8], value: &[u8]) -> io::Result<()> {
        let key_len =
            u32::try_from(key.len()).map_err(|error| io::Error::other(error.to_string()))?;
        let value_len =
            u64::try_from(value.len()).map_err(|error| io::Error::other(error.to_string()))?;
        let mut header = [0_u8; 13];
        header[0] = tag;
        header[1..5].copy_from_slice(&key_len.to_be_bytes());
        header[5..13].copy_from_slice(&value_len.to_be_bytes());
        let mut checksum = Crc32::new();
        checksum.update(&header);
        checksum.update(key);
        checksum.update(value);
        self.write_hashed(&header)?;
        self.write_hashed(key)?;
        self.write_hashed(value)?;
        self.write_hashed(&checksum.finalize().to_be_bytes())
    }

    fn write_hashed(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.file
            .as_mut()
            .ok_or_else(|| io::Error::other("snapshot artifact writer is closed"))?
            .write_all(bytes)?;
        self.digest.update(bytes);
        self.byte_len = self
            .byte_len
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| io::Error::other("snapshot artifact length overflow"))?;
        Ok(())
    }
}

impl Drop for SnapshotArtifactWriter {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.file.take();
            let _ = fs::remove_file(&self.temporary);
        }
    }
}

struct ByteCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> ByteCursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn expect(&mut self, expected: &[u8]) -> io::Result<()> {
        if self.take(expected.len())? != expected {
            return Err(io::Error::other("snapshot artifact magic mismatch"));
        }
        Ok(())
    }

    fn read_u8(&mut self) -> io::Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn read_u32(&mut self) -> io::Result<u32> {
        let bytes: [u8; 4] = self
            .take(4)?
            .try_into()
            .map_err(|error: std::array::TryFromSliceError| io::Error::other(error.to_string()))?;
        Ok(u32::from_be_bytes(bytes))
    }

    fn read_u64(&mut self) -> io::Result<u64> {
        let bytes: [u8; 8] = self
            .take(8)?
            .try_into()
            .map_err(|error: std::array::TryFromSliceError| io::Error::other(error.to_string()))?;
        Ok(u64::from_be_bytes(bytes))
    }

    fn take(&mut self, len: usize) -> io::Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(len)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| io::Error::other("snapshot artifact frame is truncated"))?;
        let value = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    fn remaining(&self) -> &'a [u8] {
        &self.bytes[self.offset..]
    }

    fn is_finished(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

fn verify_file(path: &Path, byte_len: u64, digest: SnapshotDigest) -> io::Result<()> {
    let mut file = File::open(path)?;
    if file.metadata()?.len() != byte_len {
        return Err(io::Error::other("snapshot artifact length mismatch"));
    }
    let mut hasher = Sha256::new();
    let mut buffer = vec![0; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    if SnapshotDigest::from_slice(&hasher.finalize())? != digest {
        return Err(io::Error::other("snapshot artifact digest mismatch"));
    }
    Ok(())
}

fn hex_digest(digest: SnapshotDigest) -> String {
    let mut value = String::with_capacity(64);
    for byte in digest.as_bytes() {
        use std::fmt::Write as _;
        write!(&mut value, "{byte:02x}").expect("writing to String cannot fail");
    }
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn streaming_writer_seals_artifact_beyond_the_legacy_limit() {
        let directory = tempfile::tempdir().unwrap();
        let catalog = SnapshotCatalog::open(directory.path()).unwrap();
        let mut writer = catalog
            .begin_artifact(1, br#"{"group":2}"#, br#"{"last_log":42}"#)
            .unwrap();
        let payload = vec![7; 1024 * 1024];
        for index in 0_u64..65 {
            writer
                .write_payload(&index.to_be_bytes(), &payload)
                .unwrap();
        }

        let (artifact, descriptor) = writer.finish().unwrap();

        assert!(artifact.len() > super::super::MAX_SNAPSHOT_BYTES as u64);
        assert_eq!(artifact.len(), descriptor.byte_len);
        assert_eq!(artifact.digest(), descriptor.digest);
        assert_eq!(artifact.read_chunk(0, 8).unwrap(), b"LSNP0003");
        let mut reader = artifact.reader().unwrap();
        let mut payload_count = 0;
        while let Some(record) = reader.next_record().unwrap() {
            if matches!(record, SnapshotRecord::Payload { .. }) {
                payload_count += 1;
            }
        }
        assert_eq!(65, payload_count);
    }

    #[test]
    fn catalog_open_removes_abandoned_streaming_builds() {
        let directory = tempfile::tempdir().unwrap();
        let objects = directory.path().join("snapshots/objects");
        fs::create_dir_all(&objects).unwrap();
        let abandoned = objects.join(".build-abandoned.part");
        fs::write(&abandoned, b"partial").unwrap();

        SnapshotCatalog::open(directory.path())
            .unwrap()
            .reconcile_on_open()
            .unwrap();

        assert!(!abandoned.exists());
    }
}
