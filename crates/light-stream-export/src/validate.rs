use std::collections::{BTreeMap, BTreeSet};

use light_stream_core::{
    BookmarkId, BookmarkLifecycle, BookmarkPublicationSequence, ClusterId, GroupCut, GroupId,
    MAX_DATA_GROUPS, PartitionId, QuiescentCut, RecordOffset, StreamId, StreamLifecycle,
};

use crate::{
    ControlSectionV1, DataGroupV1, EXCLUSIONS_V1, ExportDocumentV1, ExportExclusionsV1, ExportIdV1,
    ExportLimits, ExportManifestV1, ExportTotalsV1, REQUIRED_FEATURES_V1,
};

#[derive(Clone, Copy, Debug)]
pub(crate) enum ModelError {
    Limit(&'static str),
    NonCanonical(&'static str),
    Inconsistent(&'static str),
}

#[derive(Clone, Debug)]
pub(crate) struct CatalogValidation {
    pub(crate) partitions: BTreeMap<(StreamId, PartitionId), GroupId>,
}

#[derive(Default)]
pub(crate) struct DataValidation {
    pub(crate) seen: BTreeSet<(StreamId, PartitionId)>,
    pub(crate) tails: BTreeMap<(StreamId, PartitionId), RecordOffset>,
    pub(crate) totals: ExportTotalsV1,
}

#[derive(Clone, Copy)]
pub(crate) struct ValidationContext<'a> {
    source_cluster: ClusterId,
    export_id: ExportIdV1,
    selected_streams: &'a [StreamId],
    cut: &'a QuiescentCut,
    control: &'a ControlSectionV1,
    required_features: u64,
    exclusions: ExportExclusionsV1,
}

impl<'a> ValidationContext<'a> {
    pub(crate) fn from_document(document: &'a ExportDocumentV1) -> Self {
        Self {
            source_cluster: document.source_cluster,
            export_id: document.export_id,
            selected_streams: &document.selected_streams,
            cut: &document.cut,
            control: &document.control,
            required_features: document.required_features,
            exclusions: document.exclusions,
        }
    }

    pub(crate) fn from_manifest(
        manifest: &'a ExportManifestV1,
        control: &'a ControlSectionV1,
    ) -> Self {
        Self {
            source_cluster: manifest.source_cluster,
            export_id: manifest.export_id,
            selected_streams: &manifest.selected_streams,
            cut: &manifest.cut,
            control,
            required_features: manifest.required_features,
            exclusions: manifest.exclusions,
        }
    }
}

pub(crate) fn validate_control(
    context: &ValidationContext<'_>,
    limits: &ExportLimits,
) -> Result<CatalogValidation, ModelError> {
    if context.required_features != REQUIRED_FEATURES_V1 {
        return Err(ModelError::Inconsistent("required feature bits"));
    }
    if context.exclusions.bits() != EXCLUSIONS_V1 {
        return Err(ModelError::Inconsistent("fixed exclusions"));
    }
    if context.control.source_cluster != context.source_cluster {
        return Err(ModelError::Inconsistent("control source cluster"));
    }
    if context.control.export_id != context.export_id {
        return Err(ModelError::Inconsistent("control export ID"));
    }
    if context.control.cut != context.cut.control() {
        return Err(ModelError::Inconsistent("control cut"));
    }
    if context.selected_streams.is_empty() {
        return Err(ModelError::Inconsistent("selected streams"));
    }
    require_strict(context.selected_streams, "selected streams")?;
    check_count(
        context.selected_streams.len(),
        limits.max_streams,
        "streams",
    )?;
    let cut_groups = context.cut.data().keys().copied().collect::<Vec<_>>();
    if context.control.configured_data_groups != cut_groups {
        return Err(ModelError::Inconsistent("configured data groups"));
    }
    if cut_groups.is_empty() || cut_groups.len() > usize::from(MAX_DATA_GROUPS) {
        return Err(ModelError::Inconsistent("configured data groups"));
    }
    require_strict(
        &context.control.configured_data_groups,
        "configured data groups",
    )?;
    let control_streams = context
        .control
        .streams
        .iter()
        .map(|stream| stream.descriptor.stream())
        .collect::<Vec<_>>();
    require_strict(&control_streams, "streams")?;
    if control_streams != context.selected_streams {
        return Err(ModelError::Inconsistent("selected streams"));
    }

    let configured = context
        .control
        .configured_data_groups
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let mut partitions = BTreeMap::new();
    let mut bookmark_count = 0_u64;
    let mut stream_names = BTreeSet::new();
    let mut stream_bookmark_ids = BTreeSet::new();
    for stream in &context.control.streams {
        let descriptor = &stream.descriptor;
        if descriptor.cluster() != context.source_cluster {
            return Err(ModelError::Inconsistent("stream source cluster"));
        }
        if descriptor.lifecycle() != StreamLifecycle::Active {
            return Err(ModelError::Inconsistent("stream lifecycle"));
        }
        if !stream_names.insert(descriptor.name().as_str()) {
            return Err(ModelError::NonCanonical("stream names"));
        }
        if descriptor.placements().is_empty() {
            return Err(ModelError::Inconsistent("stream placements"));
        }
        let placement_ids = descriptor
            .placements()
            .iter()
            .map(|placement| placement.partition())
            .collect::<Vec<_>>();
        require_strict(&placement_ids, "placements")?;
        for placement in descriptor.placements() {
            if !configured.contains(&placement.group()) {
                return Err(ModelError::Inconsistent("placement data group"));
            }
            if partitions
                .insert(
                    (descriptor.stream(), placement.partition()),
                    placement.group(),
                )
                .is_some()
            {
                return Err(ModelError::NonCanonical("placements"));
            }
        }
        require_strict(descriptor.ready_groups(), "ready groups")?;
        if descriptor
            .ready_groups()
            .iter()
            .any(|group| !configured.contains(group))
        {
            return Err(ModelError::Inconsistent("ready data group"));
        }
        let placement_groups = descriptor
            .placements()
            .iter()
            .map(|placement| placement.group())
            .collect::<BTreeSet<_>>();
        let ready_groups = descriptor
            .ready_groups()
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if placement_groups != ready_groups {
            return Err(ModelError::Inconsistent("active stream ready groups"));
        }
        validate_stream_bookmarks(
            context,
            descriptor.stream(),
            descriptor.placements(),
            stream.bookmark_publication_ceiling,
            &stream.bookmarks,
            &mut stream_bookmark_ids,
        )?;
        bookmark_count = bookmark_count
            .checked_add(
                u64::try_from(stream.bookmarks.len())
                    .map_err(|_| ModelError::Limit("bookmarks"))?,
            )
            .ok_or(ModelError::Limit("bookmarks"))?;
    }
    check_u64(bookmark_count, limits.max_bookmarks, "bookmarks")?;
    check_count(partitions.len(), limits.max_partitions, "partitions")?;
    Ok(CatalogValidation { partitions })
}

pub(crate) fn validate_data_group(
    context: &ValidationContext<'_>,
    expected_group: GroupId,
    expected_cut: GroupCut,
    group: &DataGroupV1,
    catalog: &CatalogValidation,
    state: &mut DataValidation,
    limits: &ExportLimits,
) -> Result<(), ModelError> {
    if group.source_cluster != context.source_cluster {
        return Err(ModelError::Inconsistent("data source cluster"));
    }
    if group.export_id != context.export_id {
        return Err(ModelError::Inconsistent("data export ID"));
    }
    if group.group != expected_group
        || group.cut != expected_cut
        || group.cut.group() != group.group
    {
        return Err(ModelError::Inconsistent("data group cut"));
    }
    let keys = group
        .partitions
        .iter()
        .map(|partition| (partition.stream, partition.partition))
        .collect::<Vec<_>>();
    require_strict(&keys, "partitions")?;
    let mut bookmark_ids = BTreeSet::new();
    for partition in &group.partitions {
        let key = (partition.stream, partition.partition);
        if partition.source_cluster != context.source_cluster {
            return Err(ModelError::Inconsistent("partition source cluster"));
        }
        if catalog.partitions.get(&key) != Some(&group.group) {
            return Err(ModelError::Inconsistent("partition placement"));
        }
        if !state.seen.insert(key) {
            return Err(ModelError::NonCanonical("partitions"));
        }
        if partition.retention_floor > partition.tail {
            return Err(ModelError::Inconsistent("partition floor and tail"));
        }
        let expected_records = partition
            .tail
            .get()
            .checked_sub(partition.retention_floor.get())
            .ok_or(ModelError::Inconsistent("partition floor and tail"))?;
        if u64::try_from(partition.records.len()).map_err(|_| ModelError::Limit("records"))?
            != expected_records
        {
            return Err(ModelError::Inconsistent("partition record range"));
        }
        let mut expected_offset = partition.retention_floor.get();
        for record in &partition.records {
            if record.offset().get() != expected_offset {
                return Err(ModelError::NonCanonical("records"));
            }
            expected_offset = expected_offset
                .checked_add(1)
                .ok_or(ModelError::Limit("records"))?;
            let payload = u64::try_from(record.payload().len())
                .map_err(|_| ModelError::Limit("payload bytes"))?;
            state.totals.payload_bytes = state
                .totals
                .payload_bytes
                .checked_add(payload)
                .ok_or(ModelError::Limit("payload bytes"))?;
        }
        validate_partition_bookmarks(
            context,
            partition.stream,
            partition.partition,
            partition.tail,
            partition.bookmark_publication_ceiling,
            &partition.bookmarks,
            &mut bookmark_ids,
        )?;
        state.totals.partitions = checked_increment(state.totals.partitions, "partitions")?;
        state.totals.records = state
            .totals
            .records
            .checked_add(expected_records)
            .ok_or(ModelError::Limit("records"))?;
        state.totals.partition_bookmarks = state
            .totals
            .partition_bookmarks
            .checked_add(
                u64::try_from(partition.bookmarks.len())
                    .map_err(|_| ModelError::Limit("bookmarks"))?,
            )
            .ok_or(ModelError::Limit("bookmarks"))?;
        state.tails.insert(key, partition.tail);
    }
    check_u64(state.totals.partitions, limits.max_partitions, "partitions")?;
    check_u64(state.totals.records, limits.max_records, "records")?;
    check_u64(
        state.totals.payload_bytes,
        limits.max_payload_bytes,
        "payload bytes",
    )?;
    let bookmarks = state
        .totals
        .partition_bookmarks
        .checked_add(state.totals.stream_bookmarks)
        .ok_or(ModelError::Limit("bookmarks"))?;
    check_u64(bookmarks, limits.max_bookmarks, "bookmarks")
}

pub(crate) fn finish_validation(
    context: &ValidationContext<'_>,
    catalog: &CatalogValidation,
    state: &mut DataValidation,
    limits: &ExportLimits,
) -> Result<(), ModelError> {
    if state.seen.len() != catalog.partitions.len()
        || catalog
            .partitions
            .keys()
            .any(|partition| !state.seen.contains(partition))
    {
        return Err(ModelError::Inconsistent("descriptor partitions"));
    }
    for stream in &context.control.streams {
        for bookmark in &stream.bookmarks {
            for position in bookmark.vector().positions() {
                let key = (stream.descriptor.stream(), position.partition().partition());
                let Some(tail) = state.tails.get(&key) else {
                    return Err(ModelError::Inconsistent("stream bookmark partition"));
                };
                if position.next_offset() > *tail {
                    return Err(ModelError::Inconsistent("stream bookmark target"));
                }
            }
        }
    }
    state.totals.configured_data_groups =
        u64::try_from(context.cut.data().len()).map_err(|_| ModelError::Limit("data groups"))?;
    state.totals.streams =
        u64::try_from(context.control.streams.len()).map_err(|_| ModelError::Limit("streams"))?;
    state.totals.stream_bookmarks =
        context
            .control
            .streams
            .iter()
            .try_fold(0_u64, |total, stream| {
                total
                    .checked_add(
                        u64::try_from(stream.bookmarks.len())
                            .map_err(|_| ModelError::Limit("bookmarks"))?,
                    )
                    .ok_or(ModelError::Limit("bookmarks"))
            })?;
    check_u64(state.totals.streams, limits.max_streams, "streams")?;
    let bookmarks = state
        .totals
        .partition_bookmarks
        .checked_add(state.totals.stream_bookmarks)
        .ok_or(ModelError::Limit("bookmarks"))?;
    check_u64(bookmarks, limits.max_bookmarks, "bookmarks")
}

fn validate_stream_bookmarks(
    context: &ValidationContext<'_>,
    stream: StreamId,
    placements: &[light_stream_core::PartitionPlacement],
    ceiling: BookmarkPublicationSequence,
    bookmarks: &[light_stream_core::CommittedStreamBookmark],
    identities: &mut BTreeSet<BookmarkId>,
) -> Result<(), ModelError> {
    let mut expected_publication = 1_u64;
    let mut active_names = BTreeSet::new();
    for bookmark in bookmarks {
        if !identities.insert(bookmark.id()) {
            return Err(ModelError::NonCanonical("stream bookmark IDs"));
        }
        if bookmark.publication().get() != expected_publication {
            return Err(ModelError::NonCanonical("stream bookmarks"));
        }
        expected_publication = expected_publication
            .checked_add(1)
            .ok_or(ModelError::Limit("bookmarks"))?;
        if bookmark.lifecycle() == BookmarkLifecycle::Available
            && !active_names.insert(bookmark.name().as_str())
        {
            return Err(ModelError::NonCanonical("stream bookmark names"));
        }
        if bookmark.vector().cluster() != context.source_cluster
            || bookmark.vector().stream() != stream
        {
            return Err(ModelError::Inconsistent("stream bookmark identity"));
        }
        let positions = bookmark.vector().positions();
        if positions
            .windows(2)
            .any(|pair| pair[0].partition().partition() >= pair[1].partition().partition())
        {
            return Err(ModelError::NonCanonical("stream bookmark positions"));
        }
        if positions.len() != placements.len()
            || positions
                .iter()
                .zip(placements)
                .any(|(position, placement)| {
                    position.partition().partition() != placement.partition()
                })
        {
            return Err(ModelError::Inconsistent("stream bookmark partitions"));
        }
        if positions.iter().any(|position| {
            position.cluster() != context.source_cluster || position.partition().stream() != stream
        }) {
            return Err(ModelError::Inconsistent("stream bookmark position"));
        }
        validate_lifecycle(bookmark.lifecycle())?;
    }
    let expected_ceiling = expected_publication.saturating_sub(1);
    if ceiling.get() != expected_ceiling {
        return Err(ModelError::Inconsistent(
            "stream bookmark publication ceiling",
        ));
    }
    Ok(())
}

fn validate_partition_bookmarks(
    context: &ValidationContext<'_>,
    stream: StreamId,
    partition: PartitionId,
    tail: RecordOffset,
    ceiling: BookmarkPublicationSequence,
    bookmarks: &[light_stream_core::CommittedBookmark],
    identities: &mut BTreeSet<BookmarkId>,
) -> Result<(), ModelError> {
    let mut expected_publication = 1_u64;
    let mut active_names = BTreeSet::new();
    for bookmark in bookmarks {
        if !identities.insert(bookmark.id()) {
            return Err(ModelError::NonCanonical("partition bookmark IDs"));
        }
        if bookmark.publication().get() != expected_publication {
            return Err(ModelError::NonCanonical("partition bookmarks"));
        }
        expected_publication = expected_publication
            .checked_add(1)
            .ok_or(ModelError::Limit("bookmarks"))?;
        if bookmark.lifecycle() == BookmarkLifecycle::Available
            && !active_names.insert(bookmark.name().as_str())
        {
            return Err(ModelError::NonCanonical("partition bookmark names"));
        }
        let cursor = bookmark.cursor();
        if cursor.cluster() != context.source_cluster
            || cursor.partition().stream() != stream
            || cursor.partition().partition() != partition
        {
            return Err(ModelError::Inconsistent("partition bookmark identity"));
        }
        if cursor.next_offset() > tail {
            return Err(ModelError::Inconsistent("partition bookmark target"));
        }
        validate_lifecycle(bookmark.lifecycle())?;
    }
    let expected_ceiling = expected_publication.saturating_sub(1);
    if ceiling.get() != expected_ceiling {
        return Err(ModelError::Inconsistent(
            "partition bookmark publication ceiling",
        ));
    }
    Ok(())
}

fn validate_lifecycle(lifecycle: BookmarkLifecycle) -> Result<(), ModelError> {
    match lifecycle {
        BookmarkLifecycle::Available | BookmarkLifecycle::Deleted => Ok(()),
    }
}

fn require_strict<T: Ord>(values: &[T], field: &'static str) -> Result<(), ModelError> {
    if values.windows(2).any(|pair| pair[0] >= pair[1]) {
        Err(ModelError::NonCanonical(field))
    } else {
        Ok(())
    }
}

fn check_count(count: usize, limit: u64, name: &'static str) -> Result<(), ModelError> {
    check_u64(
        u64::try_from(count).map_err(|_| ModelError::Limit(name))?,
        limit,
        name,
    )
}

fn check_u64(value: u64, limit: u64, name: &'static str) -> Result<(), ModelError> {
    if value > limit {
        Err(ModelError::Limit(name))
    } else {
        Ok(())
    }
}

fn checked_increment(value: u64, name: &'static str) -> Result<u64, ModelError> {
    value.checked_add(1).ok_or(ModelError::Limit(name))
}
