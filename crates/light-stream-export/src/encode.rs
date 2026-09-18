use std::io::{Seek, SeekFrom, Write};

use light_stream_core::{
    ArtifactIdentity, BookmarkLifecycle, CommittedBookmark, CommittedStreamBookmark, GroupCut,
};
use sha2::{Digest, Sha256};

use crate::{
    ActiveStreamV1, BOOKMARK_LIFECYCLE_ACTIVE_V1, BOOKMARK_LIFECYCLE_DELETED_V1, DataGroupSourceV1,
    DataGroupV1, ExportDocumentV1, ExportLimits, ExportManifestV1, ExportWriteError,
    FORMAT_VERSION_V1, MAGIC_V1, OPTION_NONE_V1, OPTION_SOME_V1, SECTION_KIND_CONTROL_V1,
    SECTION_KIND_DATA_GROUP_V1, STREAM_LIFECYCLE_ACTIVE_V1, SectionDescriptorV1, SectionKindV1,
    TRAILER_BYTES_V1, TRAILER_MAGIC_V1,
    codec::{EncodeLimit, Encoder},
    decode::DecodeBudget,
    validate::{
        DataValidation, ModelError, ValidationContext, finish_validation, validate_control,
        validate_data_group,
    },
};

pub(crate) fn write_v1<W, S>(
    writer: &mut W,
    document: &ExportDocumentV1,
    source: &mut S,
    limits: &ExportLimits,
) -> Result<ArtifactIdentity, ExportWriteError<S::Error>>
where
    W: Write + Seek,
    S: DataGroupSourceV1,
{
    let context = ValidationContext::from_document(document);
    let catalog = validate_control(&context, limits).map_err(map_model)?;
    if writer.seek(SeekFrom::End(0))? != 0 {
        return Err(ExportWriteError::Inconsistent {
            field: "output must be empty",
        });
    }
    writer.seek(SeekFrom::Start(0))?;
    let mut budget = DecodeBudget::new(limits);
    budget
        .reserve_manifest_model(&document.selected_streams)
        .map_err(map_decode_budget)?;
    let mut output = Output::new(writer, limits.max_artifact_bytes);
    output.hashed(&MAGIC_V1)?;
    output.hashed(&FORMAT_VERSION_V1.to_be_bytes())?;
    output.hashed(&document.required_features.to_be_bytes())?;

    let mut sections = Vec::with_capacity(document.cut.data().len().checked_add(1).ok_or(
        ExportWriteError::Limit {
            limit: "data groups",
        },
    )?);
    budget
        .reserve_control_model(&document.control)
        .map_err(map_decode_budget)?;
    let control_payload = encode_control(document, limits.max_section_bytes).map_err(|_| {
        ExportWriteError::Limit {
            limit: "section bytes",
        }
    })?;
    let control_count = u64::try_from(document.control.streams.len())
        .map_err(|_| ExportWriteError::Limit { limit: "streams" })?;
    sections.push(output.section(
        0,
        SectionKindV1::Control,
        None,
        control_count,
        &control_payload,
    )?);

    let mut validation = DataValidation::default();
    for (index, (group, cut)) in document.cut.data().iter().enumerate() {
        let data = source
            .data_group(*group, *cut)
            .map_err(ExportWriteError::Source)?
            .ok_or(ExportWriteError::Inconsistent {
                field: "missing data group",
            })?;
        validate_data_group(
            &context,
            *group,
            *cut,
            &data,
            &catalog,
            &mut validation,
            limits,
        )
        .map_err(map_model)?;
        budget
            .reserve_data_group_model(&data)
            .map_err(map_decode_budget)?;
        let payload = encode_data_group(&data, limits.max_section_bytes).map_err(|_| {
            ExportWriteError::Limit {
                limit: "section bytes",
            }
        })?;
        let item_count =
            u64::try_from(data.partitions.len()).map_err(|_| ExportWriteError::Limit {
                limit: "partitions",
            })?;
        let ordinal = u64::try_from(index)
            .ok()
            .and_then(|value| value.checked_add(1))
            .ok_or(ExportWriteError::Limit {
                limit: "section count",
            })?;
        sections.push(output.section(
            ordinal,
            SectionKindV1::DataGroup,
            Some(*group),
            item_count,
            &payload,
        )?);
    }
    finish_validation(&context, &catalog, &mut validation, limits).map_err(map_model)?;

    let manifest = ExportManifestV1 {
        format_version: FORMAT_VERSION_V1,
        required_features: document.required_features,
        source_cluster: document.source_cluster,
        export_id: document.export_id,
        selected_streams: document.selected_streams.clone(),
        cut: document.cut.clone(),
        sections: sections.clone(),
        totals: validation.totals,
        exclusions: document.exclusions,
    };
    let manifest_bytes = encode_manifest(&manifest, limits.max_manifest_bytes).map_err(|_| {
        ExportWriteError::Limit {
            limit: "manifest bytes",
        }
    })?;
    let manifest_offset = output.offset;
    let manifest_length =
        u64::try_from(manifest_bytes.len()).map_err(|_| ExportWriteError::Limit {
            limit: "manifest bytes",
        })?;
    output.hashed(&manifest_length.to_be_bytes())?;
    output.hashed(&manifest_bytes)?;
    let manifest_end = output.offset;
    let artifact_length =
        manifest_end
            .checked_add(TRAILER_BYTES_V1)
            .ok_or(ExportWriteError::Limit {
                limit: "artifact bytes",
            })?;
    if artifact_length > limits.max_artifact_bytes {
        return Err(ExportWriteError::Limit {
            limit: "artifact bytes",
        });
    }
    let digest: [u8; 32] = output.digest.clone().finalize().into();
    output.plain(&manifest_offset.to_be_bytes())?;
    output.plain(&artifact_length.to_be_bytes())?;
    output.plain(&digest)?;
    output.plain(&TRAILER_MAGIC_V1)?;
    ArtifactIdentity::new(artifact_length, digest).map_err(|_| ExportWriteError::Inconsistent {
        field: "artifact identity",
    })
}

fn encode_control(document: &ExportDocumentV1, max: u64) -> Result<Vec<u8>, EncodeLimit> {
    let control = &document.control;
    let mut encoder = Encoder::new(max);
    encoder.cluster(control.source_cluster)?;
    encoder.export(control.export_id)?;
    encode_cut(&mut encoder, control.cut)?;
    // V1 payload offsets 64..72 are catalog revision, 72..80 assignment cursor,
    // 80..84 max streams, and 84..88 max partitions per stream.
    encoder.u64(control.catalog_revision)?;
    encoder.u64(control.assignment_cursor)?;
    encoder.u32(control.max_streams)?;
    encoder.u32(control.max_partitions_per_stream)?;
    encoder.u64(len_u64(&control.configured_data_groups)?)?;
    for group in &control.configured_data_groups {
        encoder.u64(group.get())?;
    }
    encoder.u64(len_u64(&control.streams)?)?;
    for stream in &control.streams {
        encode_stream(&mut encoder, stream)?;
    }
    Ok(encoder.into_bytes())
}

fn encode_stream(encoder: &mut Encoder, stream: &ActiveStreamV1) -> Result<(), EncodeLimit> {
    let descriptor = &stream.descriptor;
    encoder.cluster(descriptor.cluster())?;
    encoder.stream(descriptor.stream())?;
    encoder.string(descriptor.name().as_str())?;
    encoder.u8(STREAM_LIFECYCLE_ACTIVE_V1)?;
    encoder.u64(descriptor.revision())?;
    encoder.u64(len_u64(descriptor.placements())?)?;
    for placement in descriptor.placements() {
        encoder.u32(placement.partition().get())?;
        encoder.u64(placement.group().get())?;
    }
    encoder.u64(len_u64(descriptor.ready_groups())?)?;
    for group in descriptor.ready_groups() {
        encoder.u64(group.get())?;
    }
    encoder.u64(stream.bookmark_publication_ceiling.get())?;
    encoder.u64(len_u64(&stream.bookmarks)?)?;
    for bookmark in &stream.bookmarks {
        encode_stream_bookmark(encoder, bookmark)?;
    }
    Ok(())
}

fn encode_stream_bookmark(
    encoder: &mut Encoder,
    bookmark: &CommittedStreamBookmark,
) -> Result<(), EncodeLimit> {
    encoder.cluster(bookmark.vector().cluster())?;
    encoder.stream(bookmark.vector().stream())?;
    encoder.bookmark(bookmark.id())?;
    encoder.string(bookmark.name().as_str())?;
    encoder.u64(bookmark.publication().get())?;
    encoder.u8(lifecycle_tag(bookmark.lifecycle()))?;
    encoder.u64(len_u64(bookmark.vector().positions())?)?;
    for position in bookmark.vector().positions() {
        encoder.u32(position.partition().partition().get())?;
        encoder.u64(position.next_offset().get())?;
    }
    Ok(())
}

fn encode_data_group(data: &DataGroupV1, max: u64) -> Result<Vec<u8>, EncodeLimit> {
    let mut encoder = Encoder::new(max);
    encoder.cluster(data.source_cluster)?;
    encoder.export(data.export_id)?;
    encoder.u64(data.group.get())?;
    encode_cut(&mut encoder, data.cut)?;
    encoder.u64(len_u64(&data.partitions)?)?;
    for partition in &data.partitions {
        encoder.cluster(partition.source_cluster)?;
        encoder.stream(partition.stream)?;
        encoder.u32(partition.partition.get())?;
        encoder.u64(partition.retention_floor.get())?;
        encoder.u64(partition.tail.get())?;
        encoder.u64(partition.bookmark_publication_ceiling.get())?;
        encoder.u64(len_u64(&partition.records)?)?;
        for record in &partition.records {
            encoder.u64(record.offset().get())?;
            encoder.bytes(record.payload())?;
        }
        encoder.u64(len_u64(&partition.bookmarks)?)?;
        for bookmark in &partition.bookmarks {
            encode_partition_bookmark(&mut encoder, bookmark)?;
        }
    }
    Ok(encoder.into_bytes())
}

fn encode_partition_bookmark(
    encoder: &mut Encoder,
    bookmark: &CommittedBookmark,
) -> Result<(), EncodeLimit> {
    let cursor = bookmark.cursor();
    encoder.cluster(cursor.cluster())?;
    encoder.stream(cursor.partition().stream())?;
    encoder.u32(cursor.partition().partition().get())?;
    encoder.bookmark(bookmark.id())?;
    encoder.string(bookmark.name().as_str())?;
    encoder.u64(cursor.next_offset().get())?;
    encoder.u64(bookmark.publication().get())?;
    encoder.u8(lifecycle_tag(bookmark.lifecycle()))
}

fn encode_manifest(manifest: &ExportManifestV1, max: u64) -> Result<Vec<u8>, EncodeLimit> {
    let mut encoder = Encoder::new(max);
    encoder.u32(manifest.format_version)?;
    encoder.u64(manifest.required_features)?;
    encoder.cluster(manifest.source_cluster)?;
    encoder.export(manifest.export_id)?;
    encoder.u64(len_u64(&manifest.selected_streams)?)?;
    for stream in &manifest.selected_streams {
        encoder.stream(*stream)?;
    }
    encode_cut(&mut encoder, manifest.cut.control())?;
    encoder.u64(u64::try_from(manifest.cut.data().len()).map_err(|_| EncodeLimit)?)?;
    for cut in manifest.cut.data().values() {
        encode_cut(&mut encoder, *cut)?;
    }
    encoder.u64(len_u64(&manifest.sections)?)?;
    for section in &manifest.sections {
        encoder.u64(section.ordinal)?;
        encoder.u16(section.kind.number())?;
        encoder.u16(section.section_version)?;
        match section.group {
            None => encoder.u8(OPTION_NONE_V1)?,
            Some(group) => {
                encoder.u8(OPTION_SOME_V1)?;
                encoder.u64(group.get())?;
            }
        }
        encoder.u64(section.file_offset)?;
        encoder.u64(section.item_count)?;
        encoder.u64(section.payload_length)?;
        encoder.raw(&section.payload_sha256)?;
    }
    encoder.u64(manifest.totals.configured_data_groups)?;
    encoder.u64(manifest.totals.streams)?;
    encoder.u64(manifest.totals.partitions)?;
    encoder.u64(manifest.totals.records)?;
    encoder.u64(manifest.totals.payload_bytes)?;
    encoder.u64(manifest.totals.partition_bookmarks)?;
    encoder.u64(manifest.totals.stream_bookmarks)?;
    encoder.u64(manifest.exclusions.bits())?;
    Ok(encoder.into_bytes())
}

fn encode_cut(encoder: &mut Encoder, cut: GroupCut) -> Result<(), EncodeLimit> {
    encoder.u64(cut.group().get())?;
    encoder.u64(cut.term())?;
    encoder.u64(cut.leader().get())?;
    encoder.u64(cut.applied_index())
}

fn len_u64<T>(values: &[T]) -> Result<u64, EncodeLimit> {
    u64::try_from(values.len()).map_err(|_| EncodeLimit)
}

const fn lifecycle_tag(lifecycle: BookmarkLifecycle) -> u8 {
    match lifecycle {
        BookmarkLifecycle::Available => BOOKMARK_LIFECYCLE_ACTIVE_V1,
        BookmarkLifecycle::Deleted => BOOKMARK_LIFECYCLE_DELETED_V1,
    }
}

fn map_model<E>(error: ModelError) -> ExportWriteError<E>
where
    E: std::error::Error + Send + Sync + 'static,
{
    match error {
        ModelError::Limit(limit) => ExportWriteError::Limit { limit },
        ModelError::NonCanonical(field) => ExportWriteError::NonCanonical { field },
        ModelError::Inconsistent(field) => ExportWriteError::Inconsistent { field },
    }
}

fn map_decode_budget<E>(error: crate::VerifyError) -> ExportWriteError<E>
where
    E: std::error::Error + Send + Sync + 'static,
{
    let crate::VerifyError::Limit { limit } = error else {
        unreachable!("decode budget only returns limit errors");
    };
    ExportWriteError::Limit { limit }
}

struct Output<'a, W> {
    writer: &'a mut W,
    digest: Sha256,
    offset: u64,
    max: u64,
}

impl<'a, W: Write> Output<'a, W> {
    fn new(writer: &'a mut W, max: u64) -> Self {
        Self {
            writer,
            digest: Sha256::new(),
            offset: 0,
            max,
        }
    }

    fn section<E>(
        &mut self,
        ordinal: u64,
        kind: SectionKindV1,
        group: Option<light_stream_core::GroupId>,
        item_count: u64,
        payload: &[u8],
    ) -> Result<SectionDescriptorV1, ExportWriteError<E>>
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        let file_offset = self.offset;
        let payload_length = u64::try_from(payload.len()).map_err(|_| ExportWriteError::Limit {
            limit: "section bytes",
        })?;
        let payload_sha256: [u8; 32] = Sha256::digest(payload).into();
        let kind_number = match kind {
            SectionKindV1::Control => SECTION_KIND_CONTROL_V1,
            SectionKindV1::DataGroup => SECTION_KIND_DATA_GROUP_V1,
        };
        let section_version = kind.version();
        self.hashed(&kind_number.to_be_bytes())?;
        self.hashed(&section_version.to_be_bytes())?;
        self.hashed(&item_count.to_be_bytes())?;
        self.hashed(&payload_length.to_be_bytes())?;
        self.hashed(&payload_sha256)?;
        self.hashed(payload)?;
        Ok(SectionDescriptorV1 {
            ordinal,
            kind,
            section_version,
            group,
            file_offset,
            item_count,
            payload_length,
            payload_sha256,
        })
    }

    fn hashed<E>(&mut self, bytes: &[u8]) -> Result<(), ExportWriteError<E>>
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        self.write(bytes)?;
        self.digest.update(bytes);
        Ok(())
    }

    fn plain<E>(&mut self, bytes: &[u8]) -> Result<(), ExportWriteError<E>>
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        self.write(bytes)
    }

    fn write<E>(&mut self, bytes: &[u8]) -> Result<(), ExportWriteError<E>>
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        let length = u64::try_from(bytes.len()).map_err(|_| ExportWriteError::Limit {
            limit: "artifact bytes",
        })?;
        let next = self
            .offset
            .checked_add(length)
            .ok_or(ExportWriteError::Limit {
                limit: "artifact bytes",
            })?;
        if next > self.max {
            return Err(ExportWriteError::Limit {
                limit: "artifact bytes",
            });
        }
        self.writer.write_all(bytes)?;
        self.offset = next;
        Ok(())
    }
}
