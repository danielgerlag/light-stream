use crate::model::{
    Backend, PayloadVisitor, State, publish_metadata, read_metadata, sync_directory,
};
use anyhow::{Context, Result, ensure};
use poc_common::{Batch, Bookmark, MAX_PAYLOAD_BYTES};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

const SEGMENT_TARGET_BYTES: u64 = 64 * 1024 * 1024;
const SEGMENT_HEADER_BYTES: u64 = 16;
const FRAME_HEADER_BYTES: u64 = 32;
const BATCH_HEADER_BYTES: usize = 28;
const MANIFEST: &str = "manifest";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Descriptor {
    id: u64,
    first_sequence: u64,
    next_sequence: u64,
    committed_len: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    state: State,
    segments: Vec<Descriptor>,
}

impl Manifest {
    fn validate(&self) -> Result<()> {
        self.state.validate()?;
        for segment in &self.segments {
            ensure!(
                segment.first_sequence < segment.next_sequence
                    && segment.committed_len > SEGMENT_HEADER_BYTES + FRAME_HEADER_BYTES,
                "invalid segment descriptor"
            );
        }
        for pair in self.segments.windows(2) {
            ensure!(
                pair[0].id < pair[1].id && pair[0].next_sequence == pair[1].first_sequence,
                "noncontiguous segment descriptors"
            );
        }
        if let (Some(first), Some(last)) = (self.segments.first(), self.segments.last()) {
            ensure!(
                first.first_sequence <= self.state.retained_from
                    && last.next_sequence == self.state.next_sequence,
                "segment descriptors differ from durable state"
            );
        } else {
            ensure!(self.state.next_sequence == 0, "missing committed segments");
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct Location {
    segment: u64,
    offset: u64,
    payload_len: usize,
    bookmark: Bookmark,
}

struct FrameHeader {
    payload_len: usize,
    bookmark: Bookmark,
    checksum: u32,
}

impl FrameHeader {
    fn encode(payload: &[u8], bookmark: &Bookmark) -> [u8; 32] {
        let mut bytes = [0; 32];
        bytes[..4].copy_from_slice(b"LSF1");
        bytes[4..8].copy_from_slice(&(payload.len() as u32).to_le_bytes());
        bytes[8..16].copy_from_slice(&bookmark.sequence.to_le_bytes());
        bytes[16..24].copy_from_slice(&bookmark.next_record.to_le_bytes());
        bytes[24..28].copy_from_slice(&crc32fast::hash(payload).to_le_bytes());
        let checksum = crc32fast::hash(&bytes[..28]);
        bytes[28..].copy_from_slice(&checksum.to_le_bytes());
        bytes
    }

    fn read(file: &mut File) -> Result<Self> {
        let mut bytes = [0; 32];
        file.read_exact(&mut bytes)
            .context("truncated committed frame header")?;
        ensure!(&bytes[..4] == b"LSF1", "invalid committed frame magic");
        ensure!(
            crc32fast::hash(&bytes[..28]) == u32::from_le_bytes(bytes[28..].try_into()?),
            "committed frame header checksum mismatch"
        );
        let payload_len = u32::from_le_bytes(bytes[4..8].try_into()?) as usize;
        ensure!(
            (BATCH_HEADER_BYTES + 1..=BATCH_HEADER_BYTES + MAX_PAYLOAD_BYTES)
                .contains(&payload_len),
            "invalid committed frame length"
        );
        Ok(Self {
            payload_len,
            bookmark: Bookmark {
                sequence: u64::from_le_bytes(bytes[8..16].try_into()?),
                next_record: u64::from_le_bytes(bytes[16..24].try_into()?),
            },
            checksum: u32::from_le_bytes(bytes[24..28].try_into()?),
        })
    }
}

pub(crate) struct Segment {
    directory: PathBuf,
    manifest: Manifest,
    index: BTreeMap<u64, Location>,
    active: Option<File>,
    target_bytes: u64,
}

impl Segment {
    pub fn open(directory: &Path) -> Result<(Self, State)> {
        Self::open_with_target(directory, SEGMENT_TARGET_BYTES)
    }

    fn open_with_target(directory: &Path, target_bytes: u64) -> Result<(Self, State)> {
        let path = directory.join(MANIFEST);
        let manifest: Manifest = if path.exists() {
            read_metadata(&path)?
        } else {
            ensure!(
                segment_files(directory)?.is_empty(),
                "segment files exist without a manifest; refusing to guess committed state"
            );
            let manifest = Manifest::default();
            publish_metadata(directory, MANIFEST, &manifest)?;
            manifest
        };
        manifest.validate()?;
        let mut index = BTreeMap::new();
        let mut repairs = Vec::new();
        let mut previous_record = None;
        for descriptor in &manifest.segments {
            let mut file = File::open(segment_path(directory, descriptor.id))
                .context("missing committed segment")?;
            let length = file.metadata()?.len();
            ensure!(
                length >= descriptor.committed_len,
                "committed segment is truncated; automatic repair would discard committed data"
            );
            read_segment_header(&mut file, descriptor.id)?;
            let mut offset = SEGMENT_HEADER_BYTES;
            let mut sequence = descriptor.first_sequence;
            while offset < descriptor.committed_len {
                file.seek(SeekFrom::Start(offset))?;
                let header = FrameHeader::read(&mut file)?;
                ensure!(
                    header.bookmark.sequence == sequence,
                    "committed frame sequence mismatch"
                );
                if let Some(previous) = previous_record {
                    ensure!(
                        header.bookmark.next_record > previous,
                        "frame record offset mismatch"
                    );
                }
                previous_record = Some(header.bookmark.next_record);
                let end = offset
                    .checked_add(FRAME_HEADER_BYTES + header.payload_len as u64)
                    .context("frame offset overflow")?;
                ensure!(
                    end <= descriptor.committed_len,
                    "frame exceeds committed segment length"
                );
                if sequence.checked_add(1) == Some(manifest.state.retained_from) {
                    ensure!(
                        header.bookmark.next_record == manifest.state.retained_record,
                        "retention offset differs from segment bookmark"
                    );
                }
                if sequence >= manifest.state.retained_from {
                    index.insert(
                        sequence,
                        Location {
                            segment: descriptor.id,
                            offset,
                            payload_len: header.payload_len,
                            bookmark: header.bookmark,
                        },
                    );
                }
                offset = end;
                sequence = sequence.checked_add(1).context("frame sequence overflow")?;
            }
            ensure!(
                offset == descriptor.committed_len && sequence == descriptor.next_sequence,
                "committed segment extent mismatch"
            );
            if length > descriptor.committed_len {
                repairs.push((descriptor.id, descriptor.committed_len, length));
            }
        }
        ensure!(
            previous_record.unwrap_or(0) == manifest.state.next_record,
            "last segment bookmark differs from durable state"
        );
        ensure!(
            index.len() as u64 == manifest.state.next_sequence - manifest.state.retained_from,
            "retained segment index count mismatch"
        );

        for (id, committed_len, actual_len) in repairs {
            let file = OpenOptions::new()
                .write(true)
                .open(segment_path(directory, id))?;
            file.set_len(committed_len)?;
            file.sync_all()?;
            eprintln!(
                "segment repair: removed {} uncommitted tail bytes from segment {id} in {}",
                actual_len - committed_len,
                directory.display()
            );
        }
        let referenced = manifest
            .segments
            .iter()
            .map(|s| s.id)
            .collect::<BTreeSet<_>>();
        let mut changed = false;
        for id in segment_files(directory)? {
            if !referenced.contains(&id) {
                fs::remove_file(segment_path(directory, id))?;
                eprintln!(
                    "segment repair: removed unreferenced segment {id} in {}",
                    directory.display()
                );
                changed = true;
            }
        }
        let pending = directory.join(".manifest.pending");
        if pending.exists() {
            fs::remove_file(pending)?;
            changed = true;
        }
        if changed {
            sync_directory(directory)?;
        }
        let active = manifest
            .segments
            .last()
            .map(|last| {
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(segment_path(directory, last.id))
            })
            .transpose()?;
        let state = manifest.state.clone();
        Ok((
            Self {
                directory: directory.to_path_buf(),
                manifest,
                index,
                active,
                target_bytes,
            },
            state,
        ))
    }
}

impl Backend for Segment {
    fn append(&mut self, batch: &Batch, bookmark: &Bookmark, state: &State) -> Result<()> {
        let payload = batch.encode();
        let header = FrameHeader::encode(&payload, bookmark);
        let frame_len = FRAME_HEADER_BYTES + payload.len() as u64;
        let mut manifest = self.manifest.clone();
        let rotate = manifest
            .segments
            .last()
            .is_none_or(|last| last.committed_len.saturating_add(frame_len) > self.target_bytes);
        if rotate {
            let id = match manifest.segments.last() {
                Some(last) => last
                    .id
                    .checked_add(1)
                    .context("segment identifier overflow")?,
                None => 0,
            };
            let mut file = OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(segment_path(&self.directory, id))?;
            file.write_all(&segment_header(id))?;
            self.active = Some(file);
            manifest.segments.push(Descriptor {
                id,
                first_sequence: batch.sequence,
                next_sequence: batch.sequence,
                committed_len: SEGMENT_HEADER_BYTES,
            });
        }
        let descriptor = manifest
            .segments
            .last_mut()
            .context("missing active descriptor")?;
        let offset = descriptor.committed_len;
        let file = self.active.as_mut().context("missing active segment")?;
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(&header)?;
        file.write_all(&payload)?;
        file.sync_all()?;
        if rotate {
            sync_directory(&self.directory)?;
        }
        descriptor.next_sequence = state.next_sequence;
        descriptor.committed_len = offset
            .checked_add(frame_len)
            .context("segment length overflow")?;
        let segment = descriptor.id;
        manifest.state = state.clone();
        publish_metadata(&self.directory, MANIFEST, &manifest)?;
        self.manifest = manifest;
        self.index.insert(
            batch.sequence,
            Location {
                segment,
                offset,
                payload_len: payload.len(),
                bookmark: bookmark.clone(),
            },
        );
        Ok(())
    }

    fn visit(&self, visitor: &mut PayloadVisitor<'_>) -> Result<()> {
        let mut current: Option<(u64, File)> = None;
        for (&sequence, location) in &self.index {
            if current
                .as_ref()
                .is_none_or(|(id, _)| *id != location.segment)
            {
                current = Some((
                    location.segment,
                    File::open(segment_path(&self.directory, location.segment))?,
                ));
            }
            let (_, file) = current.as_mut().context("missing audit segment")?;
            file.seek(SeekFrom::Start(location.offset))?;
            let header = FrameHeader::read(file)?;
            ensure!(
                header.payload_len == location.payload_len && header.bookmark == location.bookmark,
                "persisted frame header differs from indexed metadata"
            );
            let mut payload = vec![0; header.payload_len];
            file.read_exact(&mut payload)
                .context("truncated committed payload")?;
            ensure!(
                crc32fast::hash(&payload) == header.checksum,
                "segment payload checksum mismatch"
            );
            visitor(sequence, &payload, header.bookmark)?;
        }
        Ok(())
    }

    fn last_bookmarks(&self, limit: usize) -> Result<Vec<Bookmark>> {
        Ok(self
            .index
            .values()
            .rev()
            .take(limit)
            .map(|location| location.bookmark.clone())
            .collect())
    }

    fn bookmark(&self, sequence: u64) -> Result<Bookmark> {
        Ok(self
            .index
            .get(&sequence)
            .context("missing segment bookmark")?
            .bookmark
            .clone())
    }

    fn retain(&mut self, state: &State) -> Result<()> {
        let mut manifest = self.manifest.clone();
        manifest.state = state.clone();
        let active = manifest.segments.last().map(|s| s.id);
        let mut removed = Vec::new();
        manifest.segments.retain(|segment| {
            let keep = Some(segment.id) == active || segment.next_sequence > state.retained_from;
            if !keep {
                removed.push(segment.id);
            }
            keep
        });
        // Publish logical retention before unlinking files that an old manifest still needs.
        publish_metadata(&self.directory, MANIFEST, &manifest)?;
        self.manifest = manifest;
        self.index = self.index.split_off(&state.retained_from);
        for id in removed {
            fs::remove_file(segment_path(&self.directory, id))?;
        }
        sync_directory(&self.directory)
    }
}

fn segment_path(directory: &Path, id: u64) -> PathBuf {
    directory.join(format!("segment-{id:020}.log"))
}

fn segment_files(directory: &Path) -> Result<Vec<u64>> {
    let mut ids = Vec::new();
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if let Some(id) = name
            .strip_prefix("segment-")
            .and_then(|s| s.strip_suffix(".log"))
        {
            let id: u64 = id.parse().context("invalid segment filename")?;
            ensure!(
                name == format!("segment-{id:020}.log"),
                "noncanonical segment filename"
            );
            ids.push(id);
        }
    }
    ids.sort_unstable();
    Ok(ids)
}

fn segment_header(id: u64) -> [u8; 16] {
    let mut bytes = [0; 16];
    bytes[..4].copy_from_slice(b"LSG1");
    bytes[4..12].copy_from_slice(&id.to_le_bytes());
    let checksum = crc32fast::hash(&bytes[..12]);
    bytes[12..].copy_from_slice(&checksum.to_le_bytes());
    bytes
}

fn read_segment_header(file: &mut File, id: u64) -> Result<()> {
    let mut bytes = [0; 16];
    file.read_exact(&mut bytes)?;
    ensure!(
        bytes == segment_header(id),
        "segment header checksum or identifier mismatch"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DirectoryLease, Engine, ShardStore, Store, tests::TestDirectory};

    #[test]
    fn partial_retention_and_whole_segment_reclamation() -> Result<()> {
        let directory = TestDirectory::new("segment-rollover")?;
        let lease = DirectoryLease::acquire(&directory.0, Engine::Segment)?;
        let (backend, state) = Segment::open_with_target(&directory.0, 260)?;
        let mut store = ShardStore {
            backend: Box::new(backend),
            state: Some(state),
            _lease: lease,
        };
        for sequence in 0..6 {
            store.append(&Batch::generate(7, sequence, 2, 16)?)?;
        }
        assert_eq!(segment_files(&directory.0)?.len(), 3);
        store.retain_from(1)?;
        assert_eq!(segment_files(&directory.0)?.len(), 3);
        assert_eq!(store.audit()?.first_sequence, Some(1));
        store.retain_from(3)?;
        assert_eq!(segment_files(&directory.0)?.len(), 2);
        assert_eq!(store.last_bookmarks(10)?[2].next_record, 8);
        drop(store);
        let mut reopened = crate::open(Engine::Segment, &directory.0)?;
        assert_eq!(reopened.audit()?.first_sequence, Some(3));
        reopened.append(&Batch::generate(7, 6, 3, 16)?)?;
        assert_eq!(reopened.last_bookmarks(1)?[0].next_record, 15);
        reopened.retain_from(u64::MAX)?;
        assert_eq!(segment_files(&directory.0)?.len(), 1);
        drop(reopened);
        let mut reopened = crate::open(Engine::Segment, &directory.0)?;
        reopened.append(&Batch::generate(7, 7, 1, 16)?)?;
        assert_eq!(reopened.audit()?.first_sequence, Some(7));
        assert_eq!(reopened.last_bookmarks(1)?[0].next_record, 16);
        Ok(())
    }

    #[test]
    fn repairs_only_uncommitted_tails_and_orphan_segments() -> Result<()> {
        let directory = TestDirectory::new("segment-tail")?;
        let mut store = crate::open(Engine::Segment, &directory.0)?;
        store.append(&Batch::generate(1, 0, 1, 16)?)?;
        drop(store);
        let path = segment_path(&directory.0, 0);
        let committed = fs::metadata(&path)?.len();
        let mut file = OpenOptions::new().append(true).open(&path)?;
        let uncommitted = Batch::generate(1, 1, 1, 16)?.encode();
        file.write_all(&FrameHeader::encode(
            &uncommitted,
            &Bookmark {
                sequence: 1,
                next_record: 2,
            },
        ))?;
        file.write_all(&uncommitted)?;
        file.write_all(b"LSF1torn-uncommitted-frame")?;
        file.sync_all()?;
        fs::write(segment_path(&directory.0, 1), b"orphan")?;
        fs::write(
            directory.0.join(".manifest.pending"),
            b"unpublished metadata",
        )?;
        let mut store = crate::open(Engine::Segment, &directory.0)?;
        assert_eq!(fs::metadata(&path)?.len(), committed);
        assert_eq!(segment_files(&directory.0)?, vec![0]);
        assert_eq!(store.audit()?.batches, 1);
        store.append(&Batch::generate(1, 1, 1, 16)?)?;
        drop(store);
        let file = OpenOptions::new().write(true).open(&path)?;
        file.set_len(fs::metadata(&path)?.len() - 1)?;
        file.sync_all()?;
        assert!(crate::open(Engine::Segment, &directory.0).is_err());
        Ok(())
    }

    #[test]
    fn rejects_corrupt_metadata_and_committed_frame_lengths() -> Result<()> {
        let directory = TestDirectory::new("segment-checksums")?;
        let mut store = crate::open(Engine::Segment, &directory.0)?;
        store.append(&Batch::generate(0, 0, 1, 16)?)?;
        drop(store);
        let manifest_path = directory.0.join(MANIFEST);
        let original = fs::read(&manifest_path)?;
        let mut corrupt = original.clone();
        corrupt[8] ^= 1;
        fs::write(&manifest_path, corrupt)?;
        assert!(crate::open(Engine::Segment, &directory.0).is_err());
        fs::write(&manifest_path, original)?;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(segment_path(&directory.0, 0))?;
        file.seek(SeekFrom::Start(SEGMENT_HEADER_BYTES))?;
        let mut header = [0; 32];
        file.read_exact(&mut header)?;
        header[4..8].copy_from_slice(&u32::MAX.to_le_bytes());
        let checksum = crc32fast::hash(&header[..28]);
        header[28..].copy_from_slice(&checksum.to_le_bytes());
        file.seek(SeekFrom::Start(SEGMENT_HEADER_BYTES))?;
        file.write_all(&header)?;
        file.sync_all()?;
        assert!(crate::open(Engine::Segment, &directory.0).is_err());
        Ok(())
    }
}
