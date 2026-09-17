use std::{
    convert::Infallible,
    io::{self, Read, Seek, SeekFrom},
};

use light_stream_core::{
    ArtifactIdentity, BookmarkLifecycle, BookmarkPublicationSequence, CommittedBookmark,
    CommittedCursor, CommittedRecord, CommittedStreamBookmark, GroupCut, MAX_DATA_GROUPS,
    PartitionId, PartitionKey, PartitionPlacement, QuiescentCut, RecordOffset, StreamCursorVector,
    StreamDescriptor, StreamLifecycle,
};
use sha2::{Digest, Sha256};

use crate::{
    ActiveStreamV1, ArtifactDigestCoverage, BOOKMARK_LIFECYCLE_ACTIVE_V1,
    BOOKMARK_LIFECYCLE_DELETED_V1, ControlSectionV1, DataGroupV1, EXCLUSIONS_V1,
    ExportExclusionsV1, ExportInspection, ExportLimits, ExportManifestV1, ExportTotalsV1,
    FORMAT_VERSION_V1, MAGIC_V1, OPTION_NONE_V1, OPTION_SOME_V1, PartitionV1, REQUIRED_FEATURES_V1,
    SECTION_KIND_CONTROL_V1, SECTION_KIND_DATA_GROUP_V1, SECTION_VERSION_V1,
    STREAM_LIFECYCLE_ACTIVE_V1, SectionDescriptorV1, SectionKindV1, TRAILER_BYTES_V1,
    TRAILER_MAGIC_V1, VerifiedExport, VerifiedSectionV1, VerifyError, VisitError,
    codec::Decoder,
    validate::{
        DataValidation, ModelError, ValidationContext, finish_validation, validate_control,
        validate_data_group,
    },
};

const PROLOGUE_BYTES: u64 = 20;
const SECTION_HEADER_BYTES: u64 = 52;

pub(crate) struct DecodeBudget {
    max_streams: u64,
    max_partitions: u64,
    max_records: u64,
    max_bookmarks: u64,
    max_payload_bytes: u64,
    streams: u64,
    partitions: u64,
    records: u64,
    bookmarks: u64,
    payload_bytes: u64,
}

impl DecodeBudget {
    pub(crate) fn new(limits: &ExportLimits) -> Self {
        Self {
            max_streams: limits.max_streams,
            max_partitions: limits.max_partitions,
            max_records: limits.max_records,
            max_bookmarks: limits.max_bookmarks,
            max_payload_bytes: limits.max_payload_bytes,
            streams: 0,
            partitions: 0,
            records: 0,
            bookmarks: 0,
            payload_bytes: 0,
        }
    }

    pub(crate) fn reserve_streams(&mut self, count: u64) -> Result<(), VerifyError> {
        reserve(&mut self.streams, count, self.max_streams, "streams")
    }

    pub(crate) fn reserve_partitions(&mut self, count: u64) -> Result<(), VerifyError> {
        reserve(
            &mut self.partitions,
            count,
            self.max_partitions,
            "partitions",
        )
    }

    pub(crate) fn reserve_records(&mut self, count: u64) -> Result<(), VerifyError> {
        reserve(&mut self.records, count, self.max_records, "records")
    }

    pub(crate) fn reserve_bookmarks(&mut self, count: u64) -> Result<(), VerifyError> {
        reserve(&mut self.bookmarks, count, self.max_bookmarks, "bookmarks")
    }

    pub(crate) fn reserve_payload_bytes(&mut self, count: u64) -> Result<(), VerifyError> {
        reserve(
            &mut self.payload_bytes,
            count,
            self.max_payload_bytes,
            "payload bytes",
        )
    }

    pub(crate) fn reserve_manifest_model(
        &mut self,
        selected_streams: &[light_stream_core::StreamId],
    ) -> Result<(), VerifyError> {
        self.reserve_streams(model_count(selected_streams, "streams")?)
    }

    pub(crate) fn reserve_control_model(
        &mut self,
        control: &ControlSectionV1,
    ) -> Result<(), VerifyError> {
        self.reserve_streams(model_count(&control.streams, "streams")?)?;
        for stream in &control.streams {
            self.reserve_partitions(model_count(stream.descriptor.placements(), "partitions")?)?;
            self.reserve_bookmarks(model_count(&stream.bookmarks, "bookmarks")?)?;
            for bookmark in &stream.bookmarks {
                self.reserve_partitions(model_count(bookmark.vector().positions(), "partitions")?)?;
            }
        }
        Ok(())
    }

    pub(crate) fn reserve_data_group_model(
        &mut self,
        data: &DataGroupV1,
    ) -> Result<(), VerifyError> {
        self.reserve_partitions(model_count(&data.partitions, "partitions")?)?;
        for partition in &data.partitions {
            self.reserve_records(model_count(&partition.records, "records")?)?;
            for record in &partition.records {
                self.reserve_payload_bytes(model_count(record.payload(), "payload bytes")?)?;
            }
            self.reserve_bookmarks(model_count(&partition.bookmarks, "bookmarks")?)?;
        }
        Ok(())
    }
}

fn reserve(used: &mut u64, count: u64, limit: u64, name: &'static str) -> Result<(), VerifyError> {
    let next = used
        .checked_add(count)
        .ok_or(VerifyError::Limit { limit: name })?;
    if next > limit {
        return Err(VerifyError::Limit { limit: name });
    }
    *used = next;
    Ok(())
}

fn model_count<T>(values: &[T], limit: &'static str) -> Result<u64, VerifyError> {
    u64::try_from(values.len()).map_err(|_| VerifyError::Limit { limit })
}

pub(crate) fn verify<R: Read + Seek>(
    mut reader: R,
    limits: &ExportLimits,
) -> Result<VerifiedExport<R>, VerifyError> {
    let actual_length = reader.seek(SeekFrom::End(0))?;
    if actual_length > limits.max_artifact_bytes {
        return Err(VerifyError::Limit {
            limit: "artifact bytes",
        });
    }
    let minimum = PROLOGUE_BYTES
        .checked_add(SECTION_HEADER_BYTES)
        .and_then(|value| value.checked_add(8))
        .and_then(|value| value.checked_add(TRAILER_BYTES_V1))
        .ok_or(VerifyError::Limit {
            limit: "artifact bytes",
        })?;
    if actual_length < minimum {
        return Err(VerifyError::Truncated);
    }
    let trailer_start = actual_length
        .checked_sub(TRAILER_BYTES_V1)
        .ok_or(VerifyError::Truncated)?;
    reader.seek(SeekFrom::Start(trailer_start))?;
    let mut trailer = [0_u8; TRAILER_BYTES_V1 as usize];
    reader.read_exact(&mut trailer).map_err(map_eof)?;
    let mut trailer_decoder = Decoder::new(&trailer);
    let manifest_offset = trailer_decoder.u64()?;
    let declared_length = trailer_decoder.u64()?;
    let expected_digest: [u8; 32] = trailer_decoder
        .take(32)?
        .try_into()
        .map_err(|_| VerifyError::Truncated)?;
    if trailer_decoder.take(8)? != TRAILER_MAGIC_V1 {
        return Err(VerifyError::Invalid {
            field: "trailer magic",
        });
    }
    trailer_decoder.finish("trailer")?;
    if declared_length < actual_length {
        return Err(VerifyError::TrailingBytes);
    }
    if declared_length > actual_length {
        return Err(VerifyError::Truncated);
    }
    let manifest_end = trailer_start;
    if manifest_offset < PROLOGUE_BYTES || manifest_offset >= manifest_end {
        return Err(VerifyError::Invalid {
            field: "manifest offset",
        });
    }

    reader.seek(SeekFrom::Start(manifest_offset))?;
    let mut manifest_length_bytes = [0_u8; 8];
    reader
        .read_exact(&mut manifest_length_bytes)
        .map_err(map_eof)?;
    let manifest_length = u64::from_be_bytes(manifest_length_bytes);
    if manifest_length > limits.max_manifest_bytes {
        return Err(VerifyError::Limit {
            limit: "manifest bytes",
        });
    }
    let expected_manifest_end = manifest_offset
        .checked_add(8)
        .and_then(|value| value.checked_add(manifest_length))
        .ok_or(VerifyError::Limit {
            limit: "manifest bytes",
        })?;
    if expected_manifest_end != manifest_end {
        return Err(if expected_manifest_end < manifest_end {
            VerifyError::Invalid {
                field: "manifest length",
            }
        } else {
            VerifyError::Truncated
        });
    }
    let manifest_size = usize::try_from(manifest_length).map_err(|_| VerifyError::Limit {
        limit: "manifest bytes",
    })?;
    let mut manifest_bytes = vec![0_u8; manifest_size];
    reader.read_exact(&mut manifest_bytes).map_err(map_eof)?;
    let mut budget = DecodeBudget::new(limits);
    let manifest = decode_manifest(&manifest_bytes, limits, &mut budget)?;
    validate_manifest(&manifest, limits)?;

    reader.seek(SeekFrom::Start(0))?;
    let mut hashing_reader = HashingReader::new(&mut reader, manifest_end);
    let mut prologue = [0_u8; PROLOGUE_BYTES as usize];
    hashing_reader.read_exact(&mut prologue).map_err(map_eof)?;
    let mut prologue_decoder = Decoder::new(&prologue);
    if prologue_decoder.take(8)? != MAGIC_V1 {
        return Err(VerifyError::Invalid {
            field: "prologue magic",
        });
    }
    if prologue_decoder.u32()? != FORMAT_VERSION_V1 {
        return Err(VerifyError::Unsupported {
            field: "format version",
        });
    }
    let prologue_features = prologue_decoder.u64()?;
    if prologue_features & !REQUIRED_FEATURES_V1 != 0 {
        return Err(VerifyError::Unsupported {
            field: "required feature bits",
        });
    }
    prologue_decoder.finish("prologue")?;
    if manifest.required_features != prologue_features {
        return Err(VerifyError::Inconsistent {
            field: "required feature bits",
        });
    }

    let mut visitor = |_section: VerifiedSectionV1<'_>| Ok::<(), Infallible>(());
    match walk_sections(
        &mut hashing_reader,
        &manifest,
        manifest_offset,
        limits,
        &mut budget,
        &mut visitor,
    ) {
        Ok(()) => {}
        Err(VisitError::Verify(error)) => return Err(error),
        Err(VisitError::Visitor(error)) => match error {},
    }
    let mut verified_manifest_length_bytes = [0_u8; 8];
    hashing_reader
        .read_exact(&mut verified_manifest_length_bytes)
        .map_err(map_eof)?;
    if verified_manifest_length_bytes != manifest_length_bytes {
        return Err(VerifyError::Digest { scope: "artifact" });
    }
    compare_bytes(&mut hashing_reader, &manifest_bytes)?;
    let actual_digest = hashing_reader.finalize();
    if actual_digest != expected_digest {
        return Err(VerifyError::Digest { scope: "artifact" });
    }

    let artifact = ArtifactIdentity::new(actual_length, expected_digest).map_err(|_| {
        VerifyError::Invalid {
            field: "artifact identity",
        }
    })?;
    let sections = manifest.sections.clone();
    Ok(VerifiedExport {
        reader,
        limits: *limits,
        manifest_offset,
        inspection: ExportInspection {
            artifact,
            manifest,
            sections,
            artifact_digest_coverage: ArtifactDigestCoverage {
                start: 0,
                end: manifest_end,
            },
        },
    })
}

struct HashingReader<'a, R> {
    reader: &'a mut R,
    digest: Sha256,
    position: u64,
    end: u64,
}

impl<'a, R> HashingReader<'a, R> {
    fn new(reader: &'a mut R, end: u64) -> Self {
        Self {
            reader,
            digest: Sha256::new(),
            position: 0,
            end,
        }
    }

    fn finalize(self) -> [u8; 32] {
        self.digest.finalize().into()
    }
}

impl<R: Read> Read for HashingReader<'_, R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() || self.position == self.end {
            return Ok(0);
        }
        let remaining = self.end - self.position;
        let limit = usize::try_from(remaining)
            .unwrap_or(usize::MAX)
            .min(output.len());
        let amount = self.reader.read(&mut output[..limit])?;
        if amount > limit {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "reader returned too many bytes",
            ));
        }
        self.position = self
            .position
            .checked_add(u64::try_from(amount).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "reader position overflow")
            })?)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "reader position overflow")
            })?;
        self.digest.update(&output[..amount]);
        Ok(amount)
    }
}

impl<R> Seek for HashingReader<'_, R> {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        let target = match position {
            SeekFrom::Start(value) => i128::from(value),
            SeekFrom::End(value) => i128::from(self.end) + i128::from(value),
            SeekFrom::Current(value) => i128::from(self.position) + i128::from(value),
        };
        if target == i128::from(self.position) {
            Ok(self.position)
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "hashed input may only seek to its current position",
            ))
        }
    }
}

impl<R: Read + Seek> VerifiedExport<R> {
    pub fn visit_sections<E, F>(self, mut visitor: F) -> Result<R, VisitError<E>>
    where
        F: for<'a> FnMut(VerifiedSectionV1<'a>) -> Result<(), E>,
    {
        let VerifiedExport {
            mut reader,
            inspection,
            limits,
            manifest_offset,
        } = self;
        let mut budget = DecodeBudget::new(&limits);
        walk_sections(
            &mut reader,
            &inspection.manifest,
            manifest_offset,
            &limits,
            &mut budget,
            &mut visitor,
        )?;
        Ok(reader)
    }
}

fn walk_sections<R: Read + Seek, E, F>(
    reader: &mut R,
    manifest: &ExportManifestV1,
    manifest_offset: u64,
    limits: &ExportLimits,
    budget: &mut DecodeBudget,
    visitor: &mut F,
) -> Result<(), VisitError<E>>
where
    F: for<'a> FnMut(VerifiedSectionV1<'a>) -> Result<(), E>,
{
    let expected_sections = manifest
        .cut
        .data()
        .len()
        .checked_add(1)
        .ok_or(VerifyError::Limit {
            limit: "section count",
        })?;
    if manifest.sections.len() != expected_sections {
        return Err(VerifyError::Inconsistent {
            field: "section count",
        }
        .into());
    }

    let mut offset = PROLOGUE_BYTES;
    let control_descriptor = &manifest.sections[0];
    let (control_payload, next_offset) = read_section(
        reader,
        control_descriptor,
        ExpectedSection {
            ordinal: 0,
            kind: SectionKindV1::Control,
            group: None,
        },
        offset,
        manifest_offset,
        limits,
    )?;
    let control = decode_control(&control_payload, limits, budget)?;
    if u64::try_from(control.streams.len()).map_err(|_| VerifyError::Limit { limit: "streams" })?
        != control_descriptor.item_count
    {
        return Err(VerifyError::Inconsistent {
            field: "control item count",
        }
        .into());
    }
    offset = next_offset;

    let context = ValidationContext::from_manifest(manifest, &control);
    let catalog = validate_control(&context, limits).map_err(map_model)?;
    visitor(VerifiedSectionV1::Control(&control)).map_err(VisitError::Visitor)?;
    let mut validation = DataValidation::default();

    for (index, (expected_group, expected_cut)) in manifest.cut.data().iter().enumerate() {
        let section_index = index.checked_add(1).ok_or(VerifyError::Limit {
            limit: "section count",
        })?;
        let descriptor = manifest
            .sections
            .get(section_index)
            .ok_or(VerifyError::Inconsistent {
                field: "section count",
            })?;
        let ordinal = u64::try_from(section_index).map_err(|_| VerifyError::Limit {
            limit: "section count",
        })?;
        let (payload, next_offset) = read_section(
            reader,
            descriptor,
            ExpectedSection {
                ordinal,
                kind: SectionKindV1::DataGroup,
                group: Some(*expected_group),
            },
            offset,
            manifest_offset,
            limits,
        )?;
        let data = decode_data_group(&payload, limits, budget)?;
        if u64::try_from(data.partitions.len()).map_err(|_| VerifyError::Limit {
            limit: "partitions",
        })? != descriptor.item_count
        {
            return Err(VerifyError::Inconsistent {
                field: "data item count",
            }
            .into());
        }
        validate_data_group(
            &context,
            *expected_group,
            *expected_cut,
            &data,
            &catalog,
            &mut validation,
            limits,
        )
        .map_err(map_model)?;
        visitor(VerifiedSectionV1::DataGroup(&data)).map_err(VisitError::Visitor)?;
        offset = next_offset;
    }

    if offset != manifest_offset {
        return Err(VerifyError::Invalid {
            field: "section extent",
        }
        .into());
    }
    finish_validation(&context, &catalog, &mut validation, limits).map_err(map_model)?;
    if validation.totals != manifest.totals {
        return Err(VerifyError::Inconsistent {
            field: "manifest totals",
        }
        .into());
    }
    Ok(())
}

struct ExpectedSection {
    ordinal: u64,
    kind: SectionKindV1,
    group: Option<light_stream_core::GroupId>,
}

fn read_section<R: Read + Seek>(
    reader: &mut R,
    descriptor: &SectionDescriptorV1,
    expected: ExpectedSection,
    offset: u64,
    manifest_offset: u64,
    limits: &ExportLimits,
) -> Result<(Vec<u8>, u64), VerifyError> {
    if descriptor.ordinal != expected.ordinal || descriptor.file_offset != offset {
        return Err(VerifyError::NonCanonical {
            field: "section descriptors",
        });
    }
    if descriptor.kind != expected.kind
        || descriptor.group != expected.group
        || descriptor.section_version != SECTION_VERSION_V1
    {
        return Err(VerifyError::Inconsistent {
            field: "section order",
        });
    }
    reader.seek(SeekFrom::Start(offset))?;
    let mut header = [0_u8; SECTION_HEADER_BYTES as usize];
    reader.read_exact(&mut header).map_err(map_eof)?;
    let mut header_decoder = Decoder::new(&header);
    let kind = header_decoder.u16()?;
    let version = header_decoder.u16()?;
    let item_count = header_decoder.u64()?;
    let payload_length = header_decoder.u64()?;
    let payload_digest: [u8; 32] = header_decoder
        .take(32)?
        .try_into()
        .map_err(|_| VerifyError::Truncated)?;
    header_decoder.finish("section header")?;
    if kind != descriptor.kind.number()
        || version != descriptor.section_version
        || item_count != descriptor.item_count
        || payload_length != descriptor.payload_length
        || payload_digest != descriptor.payload_sha256
    {
        return Err(VerifyError::Inconsistent {
            field: "section descriptor",
        });
    }
    if payload_length > limits.max_section_bytes {
        return Err(VerifyError::Limit {
            limit: "section bytes",
        });
    }
    let payload_start = offset
        .checked_add(SECTION_HEADER_BYTES)
        .ok_or(VerifyError::Limit {
            limit: "section bytes",
        })?;
    let payload_end = payload_start
        .checked_add(payload_length)
        .ok_or(VerifyError::Limit {
            limit: "section bytes",
        })?;
    if payload_end > manifest_offset {
        return Err(VerifyError::Truncated);
    }
    let payload_size = usize::try_from(payload_length).map_err(|_| VerifyError::Limit {
        limit: "section bytes",
    })?;
    let mut payload = vec![0_u8; payload_size];
    reader.read_exact(&mut payload).map_err(map_eof)?;
    if <[u8; 32]>::from(Sha256::digest(&payload)) != payload_digest {
        return Err(VerifyError::Digest {
            scope: match expected.kind {
                SectionKindV1::Control => "control section",
                SectionKindV1::DataGroup => "data section",
            },
        });
    }
    Ok((payload, payload_end))
}

fn decode_control(
    payload: &[u8],
    limits: &ExportLimits,
    budget: &mut DecodeBudget,
) -> Result<ControlSectionV1, VerifyError> {
    let mut decoder = Decoder::new(payload);
    let source_cluster = decoder.cluster()?;
    let export_id = decoder.export()?;
    let cut = decode_cut(&mut decoder)?;
    let group_count = count(&mut decoder, u64::from(MAX_DATA_GROUPS), "data groups", 8)?;
    let mut configured_data_groups = Vec::with_capacity(capacity(group_count, "data groups")?);
    for _ in 0..group_count {
        configured_data_groups.push(decoder.group()?);
    }
    let stream_count = count(&mut decoder, limits.max_streams, "streams", 77)?;
    budget.reserve_streams(stream_count)?;
    let mut streams = Vec::with_capacity(capacity(stream_count, "streams")?);
    for _ in 0..stream_count {
        streams.push(decode_stream(&mut decoder, limits, budget)?);
    }
    decoder.finish("control payload")?;
    Ok(ControlSectionV1 {
        source_cluster,
        export_id,
        cut,
        configured_data_groups,
        streams,
    })
}

fn decode_stream(
    decoder: &mut Decoder<'_>,
    limits: &ExportLimits,
    budget: &mut DecodeBudget,
) -> Result<ActiveStreamV1, VerifyError> {
    let cluster = decoder.cluster()?;
    let stream = decoder.stream()?;
    let name = decoder.stream_name()?;
    if decoder.u8()? != STREAM_LIFECYCLE_ACTIVE_V1 {
        return Err(VerifyError::Invalid {
            field: "stream lifecycle",
        });
    }
    let revision = decoder.u64()?;
    let placement_count = count(decoder, limits.max_partitions, "partitions", 12)?;
    budget.reserve_partitions(placement_count)?;
    let mut placements = Vec::with_capacity(capacity(placement_count, "partitions")?);
    for _ in 0..placement_count {
        placements.push(PartitionPlacement::new(
            PartitionId::new(decoder.u32()?),
            decoder.group()?,
        ));
    }
    let ready_count = count(decoder, u64::from(MAX_DATA_GROUPS), "ready data groups", 8)?;
    let mut ready_groups = Vec::with_capacity(capacity(ready_count, "ready data groups")?);
    for _ in 0..ready_count {
        ready_groups.push(decoder.group()?);
    }
    let bookmark_publication_ceiling = BookmarkPublicationSequence::new(decoder.u64()?);
    let bookmark_count = count(decoder, limits.max_bookmarks, "bookmarks", 81)?;
    budget.reserve_bookmarks(bookmark_count)?;
    let mut bookmarks = Vec::with_capacity(capacity(bookmark_count, "bookmarks")?);
    for _ in 0..bookmark_count {
        bookmarks.push(decode_stream_bookmark(decoder, limits, budget)?);
    }
    Ok(ActiveStreamV1 {
        descriptor: StreamDescriptor::new(
            cluster,
            stream,
            name,
            StreamLifecycle::Active,
            placements,
            ready_groups,
            revision,
        ),
        bookmark_publication_ceiling,
        bookmarks,
    })
}

fn decode_stream_bookmark(
    decoder: &mut Decoder<'_>,
    limits: &ExportLimits,
    budget: &mut DecodeBudget,
) -> Result<CommittedStreamBookmark, VerifyError> {
    let cluster = decoder.cluster()?;
    let stream = decoder.stream()?;
    let id = decoder.bookmark()?;
    let name = decoder.bookmark_name()?;
    let publication = BookmarkPublicationSequence::new(decoder.u64()?);
    let lifecycle = decode_lifecycle(decoder.u8()?)?;
    let position_count = count(
        decoder,
        limits.max_partitions,
        "stream bookmark positions",
        12,
    )?;
    budget.reserve_partitions(position_count)?;
    let mut positions = Vec::with_capacity(capacity(position_count, "stream bookmark positions")?);
    for _ in 0..position_count {
        positions.push(CommittedCursor::new(
            cluster,
            PartitionKey::new(stream, PartitionId::new(decoder.u32()?)),
            RecordOffset::new(decoder.u64()?),
        ));
    }
    if positions
        .windows(2)
        .any(|pair| pair[0].partition().partition() >= pair[1].partition().partition())
    {
        return Err(VerifyError::NonCanonical {
            field: "stream bookmark positions",
        });
    }
    let vector = StreamCursorVector::new(stream, positions).map_err(|_| VerifyError::Invalid {
        field: "stream bookmark positions",
    })?;
    let mut bookmark = CommittedStreamBookmark::published(id, name, vector, publication);
    if lifecycle == BookmarkLifecycle::Deleted {
        bookmark.mark_deleted();
    }
    Ok(bookmark)
}

fn decode_data_group(
    payload: &[u8],
    limits: &ExportLimits,
    budget: &mut DecodeBudget,
) -> Result<DataGroupV1, VerifyError> {
    let mut decoder = Decoder::new(payload);
    let source_cluster = decoder.cluster()?;
    let export_id = decoder.export()?;
    let group = decoder.group()?;
    let cut = decode_cut(&mut decoder)?;
    let partition_count = count(&mut decoder, limits.max_partitions, "partitions", 76)?;
    budget.reserve_partitions(partition_count)?;
    let mut partitions = Vec::with_capacity(capacity(partition_count, "partitions")?);
    for _ in 0..partition_count {
        partitions.push(decode_partition(&mut decoder, limits, budget)?);
    }
    decoder.finish("data payload")?;
    Ok(DataGroupV1 {
        source_cluster,
        export_id,
        group,
        cut,
        partitions,
    })
}

fn decode_partition(
    decoder: &mut Decoder<'_>,
    limits: &ExportLimits,
    budget: &mut DecodeBudget,
) -> Result<PartitionV1, VerifyError> {
    let source_cluster = decoder.cluster()?;
    let stream = decoder.stream()?;
    let partition = PartitionId::new(decoder.u32()?);
    let retention_floor = RecordOffset::new(decoder.u64()?);
    let tail = RecordOffset::new(decoder.u64()?);
    let bookmark_publication_ceiling = BookmarkPublicationSequence::new(decoder.u64()?);
    let record_count = count(decoder, limits.max_records, "records", 16)?;
    budget.reserve_records(record_count)?;
    let mut records = Vec::with_capacity(capacity(record_count, "records")?);
    for _ in 0..record_count {
        let offset = RecordOffset::new(decoder.u64()?);
        let payload_length = decoder.u64()?;
        if payload_length > limits.max_payload_bytes {
            return Err(VerifyError::Limit {
                limit: "payload bytes",
            });
        }
        let payload_size = capacity(payload_length, "payload bytes")?;
        if payload_size > decoder.remaining() {
            return Err(VerifyError::Truncated);
        }
        budget.reserve_payload_bytes(payload_length)?;
        let payload = decoder.take(payload_size)?.to_vec();
        records.push(CommittedRecord::new(offset, payload));
    }
    let bookmark_count = count(decoder, limits.max_bookmarks, "bookmarks", 73)?;
    budget.reserve_bookmarks(bookmark_count)?;
    let mut bookmarks = Vec::with_capacity(capacity(bookmark_count, "bookmarks")?);
    for _ in 0..bookmark_count {
        bookmarks.push(decode_partition_bookmark(decoder)?);
    }
    Ok(PartitionV1 {
        source_cluster,
        stream,
        partition,
        retention_floor,
        tail,
        bookmark_publication_ceiling,
        records,
        bookmarks,
    })
}

fn decode_partition_bookmark(decoder: &mut Decoder<'_>) -> Result<CommittedBookmark, VerifyError> {
    let cluster = decoder.cluster()?;
    let stream = decoder.stream()?;
    let partition = PartitionId::new(decoder.u32()?);
    let id = decoder.bookmark()?;
    let name = decoder.bookmark_name()?;
    let next_offset = RecordOffset::new(decoder.u64()?);
    let publication = BookmarkPublicationSequence::new(decoder.u64()?);
    let lifecycle = decode_lifecycle(decoder.u8()?)?;
    let mut bookmark = CommittedBookmark::published(
        id,
        name,
        CommittedCursor::new(cluster, PartitionKey::new(stream, partition), next_offset),
        publication,
    );
    if lifecycle == BookmarkLifecycle::Deleted {
        bookmark.mark_deleted();
    }
    Ok(bookmark)
}

fn decode_manifest(
    bytes: &[u8],
    limits: &ExportLimits,
    budget: &mut DecodeBudget,
) -> Result<ExportManifestV1, VerifyError> {
    let mut decoder = Decoder::new(bytes);
    let format_version = decoder.u32()?;
    let required_features = decoder.u64()?;
    let source_cluster = decoder.cluster()?;
    let export_id = decoder.export()?;
    let selected_count = count(&mut decoder, limits.max_streams, "streams", 16)?;
    budget.reserve_streams(selected_count)?;
    let mut selected_streams = Vec::with_capacity(capacity(selected_count, "streams")?);
    for _ in 0..selected_count {
        selected_streams.push(decoder.stream()?);
    }
    let control_cut = decode_cut(&mut decoder)?;
    let data_count = count(&mut decoder, u64::from(MAX_DATA_GROUPS), "data groups", 32)?;
    let mut data_cuts = Vec::with_capacity(capacity(data_count, "data groups")?);
    for _ in 0..data_count {
        data_cuts.push(decode_cut(&mut decoder)?);
    }
    let data_groups = data_cuts.iter().map(|cut| cut.group()).collect::<Vec<_>>();
    require_strict(&data_groups, "data cuts")?;
    let cut = QuiescentCut::try_new(control_cut, data_cuts)
        .map_err(|_| VerifyError::NonCanonical { field: "data cuts" })?;
    let max_sections = u64::from(MAX_DATA_GROUPS)
        .checked_add(1)
        .ok_or(VerifyError::Limit {
            limit: "section count",
        })?;
    let section_count = count(&mut decoder, max_sections, "section count", 69)?;
    let mut sections = Vec::with_capacity(capacity(section_count, "section count")?);
    for _ in 0..section_count {
        let ordinal = decoder.u64()?;
        let kind = match decoder.u16()? {
            SECTION_KIND_CONTROL_V1 => SectionKindV1::Control,
            SECTION_KIND_DATA_GROUP_V1 => SectionKindV1::DataGroup,
            _ => {
                return Err(VerifyError::Unsupported {
                    field: "section kind",
                });
            }
        };
        let section_version = decoder.u16()?;
        let group = match decoder.u8()? {
            OPTION_NONE_V1 => None,
            OPTION_SOME_V1 => Some(decoder.group()?),
            _ => {
                return Err(VerifyError::Invalid {
                    field: "section group option",
                });
            }
        };
        let file_offset = decoder.u64()?;
        let item_count = decoder.u64()?;
        let payload_length = decoder.u64()?;
        let payload_sha256 = decoder
            .take(32)?
            .try_into()
            .map_err(|_| VerifyError::Truncated)?;
        sections.push(SectionDescriptorV1 {
            ordinal,
            kind,
            section_version,
            group,
            file_offset,
            item_count,
            payload_length,
            payload_sha256,
        });
    }
    let totals = ExportTotalsV1 {
        configured_data_groups: decoder.u64()?,
        streams: decoder.u64()?,
        partitions: decoder.u64()?,
        records: decoder.u64()?,
        payload_bytes: decoder.u64()?,
        partition_bookmarks: decoder.u64()?,
        stream_bookmarks: decoder.u64()?,
    };
    let exclusions = ExportExclusionsV1::from_bits(decoder.u64()?);
    decoder.finish("manifest")?;
    Ok(ExportManifestV1 {
        format_version,
        required_features,
        source_cluster,
        export_id,
        selected_streams,
        cut,
        sections,
        totals,
        exclusions,
    })
}

fn validate_manifest(
    manifest: &ExportManifestV1,
    limits: &ExportLimits,
) -> Result<(), VerifyError> {
    if manifest.format_version != FORMAT_VERSION_V1 {
        return Err(VerifyError::Unsupported {
            field: "format version",
        });
    }
    if manifest.required_features & !REQUIRED_FEATURES_V1 != 0 {
        return Err(VerifyError::Unsupported {
            field: "required feature bits",
        });
    }
    if manifest.exclusions.bits() != EXCLUSIONS_V1 {
        return Err(VerifyError::Inconsistent {
            field: "fixed exclusions",
        });
    }
    require_strict(&manifest.selected_streams, "selected streams")?;
    check_limit(manifest.totals.streams, limits.max_streams, "streams")?;
    check_limit(
        manifest.totals.partitions,
        limits.max_partitions,
        "partitions",
    )?;
    check_limit(manifest.totals.records, limits.max_records, "records")?;
    check_limit(
        manifest.totals.payload_bytes,
        limits.max_payload_bytes,
        "payload bytes",
    )?;
    let bookmarks = manifest
        .totals
        .partition_bookmarks
        .checked_add(manifest.totals.stream_bookmarks)
        .ok_or(VerifyError::Limit { limit: "bookmarks" })?;
    check_limit(bookmarks, limits.max_bookmarks, "bookmarks")?;
    if manifest.totals.configured_data_groups
        != u64::try_from(manifest.cut.data().len()).map_err(|_| VerifyError::Limit {
            limit: "data groups",
        })?
    {
        return Err(VerifyError::Inconsistent {
            field: "configured data groups",
        });
    }
    Ok(())
}

fn decode_cut(decoder: &mut Decoder<'_>) -> Result<GroupCut, VerifyError> {
    Ok(GroupCut::new(
        decoder.group()?,
        decoder.u64()?,
        decoder.node()?,
        decoder.u64()?,
    ))
}

fn decode_lifecycle(value: u8) -> Result<BookmarkLifecycle, VerifyError> {
    match value {
        BOOKMARK_LIFECYCLE_ACTIVE_V1 => Ok(BookmarkLifecycle::Available),
        BOOKMARK_LIFECYCLE_DELETED_V1 => Ok(BookmarkLifecycle::Deleted),
        _ => Err(VerifyError::Invalid {
            field: "bookmark lifecycle",
        }),
    }
}

fn count(
    decoder: &mut Decoder<'_>,
    limit: u64,
    name: &'static str,
    minimum_item_bytes: u64,
) -> Result<u64, VerifyError> {
    let value = decoder.u64()?;
    if value > limit {
        return Err(VerifyError::Limit { limit: name });
    }
    let minimum = value
        .checked_mul(minimum_item_bytes)
        .ok_or(VerifyError::Limit { limit: name })?;
    let remaining =
        u64::try_from(decoder.remaining()).map_err(|_| VerifyError::Limit { limit: name })?;
    if minimum > remaining {
        return Err(VerifyError::Truncated);
    }
    Ok(value)
}

fn capacity(value: u64, name: &'static str) -> Result<usize, VerifyError> {
    usize::try_from(value).map_err(|_| VerifyError::Limit { limit: name })
}

fn require_strict<T: Ord>(values: &[T], field: &'static str) -> Result<(), VerifyError> {
    if values.windows(2).any(|pair| pair[0] >= pair[1]) {
        Err(VerifyError::NonCanonical { field })
    } else {
        Ok(())
    }
}

fn check_limit(value: u64, limit: u64, name: &'static str) -> Result<(), VerifyError> {
    if value > limit {
        Err(VerifyError::Limit { limit: name })
    } else {
        Ok(())
    }
}

fn compare_bytes<R: Read>(reader: &mut R, expected: &[u8]) -> Result<(), VerifyError> {
    let mut buffer = [0_u8; 64 * 1024];
    let mut offset = 0;
    while offset < expected.len() {
        let amount = (expected.len() - offset).min(buffer.len());
        reader.read_exact(&mut buffer[..amount]).map_err(map_eof)?;
        if buffer[..amount] != expected[offset..offset + amount] {
            return Err(VerifyError::Digest { scope: "artifact" });
        }
        offset += amount;
    }
    Ok(())
}

fn map_eof(error: std::io::Error) -> VerifyError {
    if error.kind() == std::io::ErrorKind::UnexpectedEof {
        VerifyError::Truncated
    } else {
        VerifyError::Io(error)
    }
}

fn map_model(error: ModelError) -> VerifyError {
    match error {
        ModelError::Limit(limit) => VerifyError::Limit { limit },
        ModelError::NonCanonical(field) => VerifyError::NonCanonical { field },
        ModelError::Inconsistent(field) => VerifyError::Inconsistent { field },
    }
}
