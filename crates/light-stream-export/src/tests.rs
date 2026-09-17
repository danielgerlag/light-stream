use std::{
    cell::RefCell,
    convert::Infallible,
    io::{self, Cursor, Read, Seek, SeekFrom},
    rc::Rc,
};

use light_stream_core::{
    BookmarkName, BookmarkPublicationSequence, ClusterId, CommittedBookmark, CommittedCursor,
    CommittedRecord, CommittedStreamBookmark, GroupCut, GroupId, PartitionId, PartitionKey,
    PartitionPlacement, RecordOffset, StreamCursorVector, StreamDescriptor, StreamId,
    StreamLifecycle, StreamName,
};
use sha2::{Digest, Sha256};

use crate::{
    EXCLUSIONS_V1, ExportExclusionsV1, ExportLimits, ExportWriteError, FORMAT_VERSION_V1, MAGIC_V1,
    REQUIRED_FEATURES_V1, SECTION_VERSION_V1, SectionKindV1, TRAILER_BYTES_V1, TRAILER_MAGIC_V1,
    VerifyError,
    fixtures::{FixtureV1, canonical_v1},
    inspect, verify, write_v1,
};

const SECTION_HEADER_BYTES: usize = 52;

#[test]
fn deterministic_documents_produce_identical_bytes_and_identity() {
    let limits = ExportLimits::default();
    let mut first = canonical_v1();
    let mut second = canonical_v1();
    let (first_bytes, first_identity) = write_fixture(&mut first, &limits);
    let (second_bytes, second_identity) = write_fixture(&mut second, &limits);

    assert_eq!(first_identity, second_identity);
    assert_eq!(first_bytes, second_bytes);
    assert_eq!(
        first.source.requests,
        vec![group(1), group(2)],
        "the writer owns ascending group sequencing"
    );
}

#[test]
fn logical_round_trip_is_redacted_and_preserves_tombstones() {
    let limits = ExportLimits::default();
    let mut fixture = canonical_v1();
    let (bytes, identity) = write_fixture(&mut fixture, &limits);

    let verified = verify(Cursor::new(bytes), &limits).unwrap();
    let inspection = inspect(&verified);

    assert_eq!(inspection.artifact, identity);
    assert_eq!(
        inspection.manifest.selected_streams,
        fixture.document.selected_streams
    );
    assert_eq!(inspection.manifest.totals.configured_data_groups, 2);
    assert_eq!(inspection.manifest.totals.streams, 1);
    assert_eq!(inspection.manifest.totals.partitions, 1);
    assert_eq!(inspection.manifest.totals.records, 2);
    assert_eq!(inspection.manifest.totals.payload_bytes, 9);
    assert_eq!(inspection.manifest.totals.partition_bookmarks, 2);
    assert_eq!(inspection.manifest.totals.stream_bookmarks, 2);
}

#[test]
fn verified_section_visitor_reads_in_order_and_rechecks_mutated_bytes() {
    let limits = ExportLimits::default();
    let mut fixture = canonical_v1();
    let expected_control = fixture.document.control.clone();
    let expected_groups = fixture.source.groups.clone();
    let (bytes, _) = write_fixture(&mut fixture, &limits);
    let verified = verify(Cursor::new(bytes), &limits).unwrap();
    let mut visited = Vec::new();
    verified
        .visit_sections(|section| {
            visited.push(match section {
                crate::VerifiedSectionV1::Control(control) => {
                    assert_eq!(control, &expected_control);
                    None
                }
                crate::VerifiedSectionV1::DataGroup(group) => {
                    assert_eq!(Some(group), expected_groups.get(&group.group));
                    Some(group.group)
                }
            });
            Ok::<_, Infallible>(())
        })
        .unwrap();
    assert_eq!(visited, [None, Some(group(1)), Some(group(2))]);

    let mut fixture = canonical_v1();
    let (bytes, _) = write_fixture(&mut fixture, &limits);
    let reader = SharedCursor::new(bytes);
    let verified = verify(reader.clone(), &limits).unwrap();
    let data_payload = verified.sections()[1].file_offset as usize + SECTION_HEADER_BYTES;
    reader.mutate(data_payload);
    let mut visited = Vec::new();
    let error = verified
        .visit_sections(|section| {
            visited.push(match section {
                crate::VerifiedSectionV1::Control(_) => None,
                crate::VerifiedSectionV1::DataGroup(group) => Some(group.group),
            });
            Ok::<_, Infallible>(())
        })
        .unwrap_err();
    assert!(matches!(
        error,
        crate::VisitError::Verify(VerifyError::Digest {
            scope: "data section"
        })
    ));
    assert_eq!(visited, [None]);

    let mut fixture = canonical_v1();
    let (bytes, _) = write_fixture(&mut fixture, &limits);
    let reader = SharedCursor::new(bytes);
    let verified = verify(reader.clone(), &limits).unwrap();
    let data_header_item_count = verified.sections()[1].file_offset as usize + 4;
    reader.mutate(data_header_item_count);
    let mut visited = Vec::new();
    let error = verified
        .visit_sections(|section| {
            visited.push(match section {
                crate::VerifiedSectionV1::Control(_) => None,
                crate::VerifiedSectionV1::DataGroup(group) => Some(group.group),
            });
            Ok::<_, Infallible>(())
        })
        .unwrap_err();
    assert!(matches!(
        error,
        crate::VisitError::Verify(VerifyError::Inconsistent {
            field: "section descriptor"
        })
    ));
    assert_eq!(visited, [None]);
}

#[test]
fn verifier_hashes_the_same_bytes_it_decodes() {
    let limits = ExportLimits::default();
    let mut original = canonical_v1();
    let (original_bytes, _) = write_fixture(&mut original, &limits);
    let mut replacement = canonical_v1();
    data_one(&mut replacement).partitions[0].records[0] =
        CommittedRecord::new(RecordOffset::new(2), b"omega".to_vec());
    let (replacement_bytes, _) = write_fixture(&mut replacement, &limits);
    assert_eq!(original_bytes.len(), replacement_bytes.len());

    assert!(
        verify(
            SwapAfterManifestRead::new(original_bytes, replacement_bytes),
            &limits
        )
        .is_err(),
        "verification accepted bytes that changed between covered reads"
    );
}

#[test]
fn exact_v1_envelope_order_endianness_manifest_and_digest_coverage() {
    let limits = ExportLimits::default();
    let mut fixture = canonical_v1();
    let (bytes, identity) = write_fixture(&mut fixture, &limits);
    let inspection = inspect(&verify(Cursor::new(bytes.clone()), &limits).unwrap());

    assert_eq!(&bytes[..8], &MAGIC_V1);
    assert_eq!(be_u32(&bytes[8..12]), FORMAT_VERSION_V1);
    assert_eq!(be_u64(&bytes[12..20]), REQUIRED_FEATURES_V1);
    assert_eq!(inspection.sections.len(), 3);
    assert_eq!(inspection.sections[0].kind, SectionKindV1::Control);
    assert_eq!(inspection.sections[1].kind, SectionKindV1::DataGroup);
    assert_eq!(inspection.sections[1].group, Some(group(1)));
    assert_eq!(inspection.sections[2].kind, SectionKindV1::DataGroup);
    assert_eq!(inspection.sections[2].group, Some(group(2)));
    assert_eq!(inspection.sections[2].item_count, 0);

    for section in &inspection.sections {
        let offset = section.file_offset as usize;
        assert_eq!(be_u16(&bytes[offset..offset + 2]), section.kind.number());
        assert_eq!(be_u16(&bytes[offset + 2..offset + 4]), SECTION_VERSION_V1);
        assert_eq!(be_u64(&bytes[offset + 4..offset + 12]), section.item_count);
        assert_eq!(
            be_u64(&bytes[offset + 12..offset + 20]),
            section.payload_length
        );
    }

    let control_payload = inspection.sections[0].file_offset as usize + SECTION_HEADER_BYTES;
    assert_eq!(
        &bytes[control_payload..control_payload + 16],
        fixture.document.source_cluster.as_uuid().as_bytes()
    );
    assert_eq!(
        &bytes[control_payload + 16..control_payload + 32],
        fixture.document.export_id.as_bytes()
    );
    assert_eq!(
        be_u64(&bytes[control_payload + 64..control_payload + 72]),
        2
    );
    assert_eq!(
        be_u64(&bytes[control_payload + 72..control_payload + 80]),
        1
    );
    assert_eq!(
        be_u64(&bytes[control_payload + 80..control_payload + 88]),
        2
    );

    let manifest_offset = trailer_u64(&bytes, 0) as usize;
    let manifest_length = be_u64(&bytes[manifest_offset..manifest_offset + 8]) as usize;
    let manifest_start = manifest_offset + 8;
    let manifest_end = manifest_start + manifest_length;
    assert_eq!(
        be_u32(&bytes[manifest_start..manifest_start + 4]),
        FORMAT_VERSION_V1
    );
    assert_eq!(
        be_u64(&bytes[manifest_start + 4..manifest_start + 12]),
        REQUIRED_FEATURES_V1
    );
    assert_eq!(
        &bytes[manifest_start + 12..manifest_start + 28],
        fixture.document.source_cluster.as_uuid().as_bytes()
    );
    assert_eq!(
        &bytes[manifest_start + 28..manifest_start + 44],
        fixture.document.export_id.as_bytes()
    );
    assert_eq!(be_u64(&bytes[manifest_start + 44..manifest_start + 52]), 1);

    let trailer_start = bytes.len() - TRAILER_BYTES_V1 as usize;
    assert_eq!(manifest_end, trailer_start);
    assert_eq!(trailer_u64(&bytes, 8), bytes.len() as u64);
    assert_eq!(&bytes[bytes.len() - 8..], &TRAILER_MAGIC_V1);
    let digest: [u8; 32] = Sha256::digest(&bytes[..manifest_end]).into();
    assert_eq!(&bytes[trailer_start + 16..trailer_start + 48], &digest);
    assert_eq!(identity.sha256(), digest);
    assert_eq!(
        inspection.artifact_digest_coverage,
        crate::ArtifactDigestCoverage {
            start: 0,
            end: manifest_end as u64
        }
    );
}

#[test]
fn verifier_rejects_control_data_and_artifact_digest_corruption() {
    let limits = ExportLimits::default();
    let mut fixture = canonical_v1();
    let (bytes, _) = write_fixture(&mut fixture, &limits);
    let inspection = inspect(&verify(Cursor::new(bytes.clone()), &limits).unwrap());

    let mut control = bytes.clone();
    let control_payload = inspection.sections[0].file_offset as usize + SECTION_HEADER_BYTES;
    control[control_payload] ^= 1;
    recompute_artifact_digest(&mut control);
    assert!(matches!(
        verify(Cursor::new(control), &limits),
        Err(VerifyError::Digest {
            scope: "control section"
        })
    ));

    let mut data = bytes.clone();
    let data_payload = inspection.sections[1].file_offset as usize + SECTION_HEADER_BYTES;
    data[data_payload] ^= 1;
    recompute_artifact_digest(&mut data);
    assert!(matches!(
        verify(Cursor::new(data), &limits),
        Err(VerifyError::Digest {
            scope: "data section"
        })
    ));

    let mut artifact = bytes;
    let digest_offset = artifact.len() - TRAILER_BYTES_V1 as usize + 16;
    artifact[digest_offset] ^= 1;
    assert!(matches!(
        verify(Cursor::new(artifact), &limits),
        Err(VerifyError::Digest { scope: "artifact" })
    ));
}

#[test]
fn verifier_rejects_truncation_at_fixture_boundaries_and_trailing_bytes() {
    let limits = ExportLimits::default();
    let mut fixture = canonical_v1();
    let (bytes, _) = write_fixture(&mut fixture, &limits);
    let inspection = inspect(&verify(Cursor::new(bytes.clone()), &limits).unwrap());
    let manifest_offset = trailer_u64(&bytes, 0) as usize;
    let trailer_start = bytes.len() - TRAILER_BYTES_V1 as usize;
    let mut boundaries = vec![
        0,
        1,
        7,
        8,
        11,
        12,
        19,
        20,
        manifest_offset,
        manifest_offset + 7,
    ];
    for section in &inspection.sections {
        let start = section.file_offset as usize;
        boundaries.push(start + 1);
        boundaries.push(start + SECTION_HEADER_BYTES - 1);
        boundaries.push(start + SECTION_HEADER_BYTES + section.payload_length as usize - 1);
    }
    boundaries.extend([trailer_start - 1, trailer_start, bytes.len() - 1]);
    boundaries.sort_unstable();
    boundaries.dedup();
    for length in boundaries {
        assert!(
            verify(Cursor::new(bytes[..length].to_vec()), &limits).is_err(),
            "truncation at {length} unexpectedly verified"
        );
    }
    let mut trailing = bytes;
    trailing.push(0);
    assert!(verify(Cursor::new(trailing), &limits).is_err());
}

#[test]
fn verifier_rejects_unknown_required_feature_bit() {
    let limits = ExportLimits::default();
    let mut fixture = canonical_v1();
    let (mut bytes, _) = write_fixture(&mut fixture, &limits);
    let unknown = 1_u64 << 63;
    bytes[12..20].copy_from_slice(&unknown.to_be_bytes());
    let manifest_start = trailer_u64(&bytes, 0) as usize + 8;
    bytes[manifest_start + 4..manifest_start + 12].copy_from_slice(&unknown.to_be_bytes());
    recompute_artifact_digest(&mut bytes);
    assert!(matches!(
        verify(Cursor::new(bytes), &limits),
        Err(VerifyError::Unsupported {
            field: "required feature bits"
        })
    ));
}

#[test]
fn verifier_enforces_every_declared_limit_before_decoding_items() {
    let limits = ExportLimits::default();
    let mut fixture = canonical_v1();
    let (bytes, _) = write_fixture(&mut fixture, &limits);
    let inspection = inspect(&verify(Cursor::new(bytes.clone()), &limits).unwrap());
    let manifest_offset = trailer_u64(&bytes, 0) as usize;
    let manifest_length = be_u64(&bytes[manifest_offset..manifest_offset + 8]);

    let cases = [
        ExportLimits {
            max_artifact_bytes: bytes.len() as u64 - 1,
            ..limits
        },
        ExportLimits {
            max_manifest_bytes: manifest_length - 1,
            ..limits
        },
        ExportLimits {
            max_section_bytes: inspection.sections[0].payload_length - 1,
            ..limits
        },
        ExportLimits {
            max_streams: 0,
            ..limits
        },
        ExportLimits {
            max_partitions: 0,
            ..limits
        },
        ExportLimits {
            max_records: 1,
            ..limits
        },
        ExportLimits {
            max_bookmarks: 3,
            ..limits
        },
        ExportLimits {
            max_payload_bytes: 8,
            ..limits
        },
    ];
    for restricted in cases {
        assert!(matches!(
            verify(Cursor::new(bytes.clone()), &restricted),
            Err(VerifyError::Limit { .. })
        ));
    }
}

#[test]
fn writer_rejects_duplicate_and_out_of_order_control_collections() {
    let mut duplicate_stream = canonical_v1();
    duplicate_stream
        .document
        .selected_streams
        .push(duplicate_stream.document.selected_streams[0]);
    duplicate_stream
        .document
        .control
        .streams
        .push(duplicate_stream.document.control.streams[0].clone());
    assert_noncanonical(duplicate_stream);

    let mut out_of_order_stream = canonical_v1();
    let original = out_of_order_stream.document.control.streams[0].clone();
    let later: StreamId = id("018f3f7e-5b3b-7c11-98f7-b65ac15f6511");
    let later_stream = crate::ActiveStreamV1 {
        descriptor: StreamDescriptor::new(
            out_of_order_stream.document.source_cluster,
            later,
            original.descriptor.name().clone(),
            StreamLifecycle::Active,
            Vec::new(),
            Vec::new(),
            original.descriptor.revision(),
        ),
        bookmark_publication_ceiling: BookmarkPublicationSequence::new(0),
        bookmarks: Vec::new(),
    };
    out_of_order_stream.document.selected_streams = vec![later, original.descriptor.stream()];
    out_of_order_stream.document.control.streams = vec![later_stream, original];
    assert_noncanonical(out_of_order_stream);

    let mut placements = canonical_v1();
    let stream = &mut placements.document.control.streams[0];
    let descriptor = &stream.descriptor;
    stream.descriptor = StreamDescriptor::new(
        descriptor.cluster(),
        descriptor.stream(),
        descriptor.name().clone(),
        descriptor.lifecycle(),
        vec![
            PartitionPlacement::new(PartitionId::new(1), group(1)),
            PartitionPlacement::new(PartitionId::new(0), group(1)),
        ],
        descriptor.ready_groups().to_vec(),
        descriptor.revision(),
    );
    assert_noncanonical(placements);

    let mut duplicate_placements = canonical_v1();
    let stream = &mut duplicate_placements.document.control.streams[0];
    let descriptor = &stream.descriptor;
    let placement = descriptor.placements()[0];
    stream.descriptor = StreamDescriptor::new(
        descriptor.cluster(),
        descriptor.stream(),
        descriptor.name().clone(),
        descriptor.lifecycle(),
        vec![placement, placement],
        descriptor.ready_groups().to_vec(),
        descriptor.revision(),
    );
    assert_noncanonical(duplicate_placements);

    let mut ready = canonical_v1();
    let stream = &mut ready.document.control.streams[0];
    let descriptor = &stream.descriptor;
    stream.descriptor = StreamDescriptor::new(
        descriptor.cluster(),
        descriptor.stream(),
        descriptor.name().clone(),
        descriptor.lifecycle(),
        descriptor.placements().to_vec(),
        vec![group(2), group(1)],
        descriptor.revision(),
    );
    assert_noncanonical(ready);

    let mut duplicate_ready = canonical_v1();
    let stream = &mut duplicate_ready.document.control.streams[0];
    let descriptor = &stream.descriptor;
    stream.descriptor = StreamDescriptor::new(
        descriptor.cluster(),
        descriptor.stream(),
        descriptor.name().clone(),
        descriptor.lifecycle(),
        descriptor.placements().to_vec(),
        vec![group(1), group(1)],
        descriptor.revision(),
    );
    assert_noncanonical(duplicate_ready);

    let mut stream_bookmarks = canonical_v1();
    stream_bookmarks.document.control.streams[0]
        .bookmarks
        .reverse();
    assert_noncanonical(stream_bookmarks);

    let mut duplicate_stream_bookmark = canonical_v1();
    let bookmark = duplicate_stream_bookmark.document.control.streams[0].bookmarks[0].clone();
    duplicate_stream_bookmark.document.control.streams[0]
        .bookmarks
        .insert(1, bookmark);
    assert_noncanonical(duplicate_stream_bookmark);

    let mut repeated_stream_bookmark_id = canonical_v1();
    let first = repeated_stream_bookmark_id.document.control.streams[0].bookmarks[0].clone();
    let second = repeated_stream_bookmark_id.document.control.streams[0].bookmarks[1].clone();
    repeated_stream_bookmark_id.document.control.streams[0].bookmarks[1] =
        CommittedStreamBookmark::published(
            first.id(),
            second.name().clone(),
            second.vector().clone(),
            second.publication(),
        );
    assert_noncanonical(repeated_stream_bookmark_id);

    let mut positions = canonical_v1();
    let cluster = positions.document.source_cluster;
    let stream_id = positions.document.selected_streams[0];
    let original = &positions.document.control.streams[0].bookmarks[0];
    positions.document.control.streams[0].bookmarks[0] = CommittedStreamBookmark::published(
        original.id(),
        original.name().clone(),
        StreamCursorVector::new(
            stream_id,
            vec![
                CommittedCursor::new(
                    cluster,
                    PartitionKey::new(stream_id, PartitionId::new(1)),
                    RecordOffset::new(0),
                ),
                CommittedCursor::new(
                    cluster,
                    PartitionKey::new(stream_id, PartitionId::new(0)),
                    RecordOffset::new(4),
                ),
            ],
        )
        .unwrap(),
        original.publication(),
    );
    assert_noncanonical(positions);

    let key = PartitionKey::new(stream_id, PartitionId::new(0));
    assert!(
        StreamCursorVector::new(
            stream_id,
            vec![
                CommittedCursor::new(cluster, key, RecordOffset::new(3)),
                CommittedCursor::new(cluster, key, RecordOffset::new(4)),
            ],
        )
        .is_err(),
        "the reused core vector makes duplicate positions unrepresentable"
    );
}

#[test]
fn writer_rejects_duplicate_and_out_of_order_data_collections() {
    let mut duplicate_partitions = canonical_v1();
    let data = data_one(&mut duplicate_partitions);
    data.partitions.push(data.partitions[0].clone());
    assert_noncanonical(duplicate_partitions);

    let mut partitions = canonical_v1();
    add_second_placement(&mut partitions);
    clear_bookmarks(&mut partitions);
    let data = data_one(&mut partitions);
    let mut second = data.partitions[0].clone();
    second.partition = PartitionId::new(1);
    data.partitions.insert(0, second);
    assert_noncanonical(partitions);

    let mut records = canonical_v1();
    data_one(&mut records).partitions[0].records.reverse();
    assert_noncanonical(records);

    let mut duplicate_records = canonical_v1();
    let records = &mut data_one(&mut duplicate_records).partitions[0].records;
    records[1] = records[0].clone();
    assert_noncanonical(duplicate_records);

    let mut bookmarks = canonical_v1();
    data_one(&mut bookmarks).partitions[0].bookmarks.reverse();
    assert_noncanonical(bookmarks);

    let mut duplicate_bookmarks = canonical_v1();
    let bookmarks = &mut data_one(&mut duplicate_bookmarks).partitions[0].bookmarks;
    bookmarks[1] = bookmarks[0].clone();
    assert_noncanonical(duplicate_bookmarks);

    let mut repeated_bookmark_id = canonical_v1();
    let bookmarks = &mut data_one(&mut repeated_bookmark_id).partitions[0].bookmarks;
    let first = bookmarks[0].clone();
    let second = bookmarks[1].clone();
    bookmarks[1] = CommittedBookmark::published(
        first.id(),
        second.name().clone(),
        second.cursor(),
        second.publication(),
    );
    assert_noncanonical(repeated_bookmark_id);
}

#[test]
fn writer_rejects_identity_cut_group_and_selection_mismatches() {
    let other_cluster: ClusterId = id("018f3f7e-5b3b-7c11-98f7-b65ac15f6510");
    let other_stream: StreamId = id("018f3f7e-5b3b-7c11-98f7-b65ac15f6511");

    let mut control_cluster = canonical_v1();
    control_cluster.document.control.source_cluster = other_cluster;
    assert_inconsistent(control_cluster);

    let mut export = canonical_v1();
    export.document.control.export_id = crate::ExportIdV1::from_bytes([9; 16]);
    assert_inconsistent(export);

    let mut selection = canonical_v1();
    selection.document.selected_streams = vec![other_stream];
    assert_inconsistent(selection);

    let mut descriptor_cluster = canonical_v1();
    let value = &mut descriptor_cluster.document.control.streams[0];
    let descriptor = &value.descriptor;
    value.descriptor = StreamDescriptor::new(
        other_cluster,
        descriptor.stream(),
        descriptor.name().clone(),
        descriptor.lifecycle(),
        descriptor.placements().to_vec(),
        descriptor.ready_groups().to_vec(),
        descriptor.revision(),
    );
    assert_inconsistent(descriptor_cluster);

    let mut data_cluster = canonical_v1();
    data_one(&mut data_cluster).source_cluster = other_cluster;
    assert_inconsistent(data_cluster);

    let mut data_export = canonical_v1();
    data_one(&mut data_export).export_id = crate::ExportIdV1::from_bytes([8; 16]);
    assert_inconsistent(data_export);

    let mut data_group = canonical_v1();
    data_one(&mut data_group).group = group(2);
    assert_inconsistent(data_group);

    let mut data_cut = canonical_v1();
    let old = data_one(&mut data_cut).cut;
    data_one(&mut data_cut).cut = GroupCut::new(
        old.group(),
        old.term() + 1,
        old.leader(),
        old.applied_index(),
    );
    assert_inconsistent(data_cut);
}

#[test]
fn writer_rejects_floor_tail_gaps_and_extras() {
    let mut inversion = canonical_v1();
    data_one(&mut inversion).partitions[0].retention_floor = RecordOffset::new(5);
    assert_inconsistent(inversion);

    let mut gap = canonical_v1();
    data_one(&mut gap).partitions[0].records[1] =
        CommittedRecord::new(RecordOffset::new(4), b"beta".to_vec());
    assert_noncanonical(gap);

    let mut missing = canonical_v1();
    data_one(&mut missing).partitions[0].records.pop();
    assert_inconsistent(missing);

    let mut extra = canonical_v1();
    data_one(&mut extra).partitions[0]
        .records
        .push(CommittedRecord::new(
            RecordOffset::new(4),
            b"extra".to_vec(),
        ));
    assert_inconsistent(extra);
}

#[test]
fn writer_rejects_bookmark_targets_and_publications_past_cut() {
    let mut partition_target = canonical_v1();
    let cluster = partition_target.document.source_cluster;
    let stream = partition_target.document.selected_streams[0];
    let bookmark = &data_one(&mut partition_target).partitions[0].bookmarks[0];
    data_one(&mut partition_target).partitions[0].bookmarks[0] = CommittedBookmark::published(
        bookmark.id(),
        bookmark.name().clone(),
        CommittedCursor::new(
            cluster,
            PartitionKey::new(stream, PartitionId::new(0)),
            RecordOffset::new(5),
        ),
        bookmark.publication(),
    );
    assert_inconsistent(partition_target);

    let mut stream_target = canonical_v1();
    let bookmark = &stream_target.document.control.streams[0].bookmarks[0];
    stream_target.document.control.streams[0].bookmarks[0] = CommittedStreamBookmark::published(
        bookmark.id(),
        bookmark.name().clone(),
        StreamCursorVector::new(
            stream,
            vec![CommittedCursor::new(
                cluster,
                PartitionKey::new(stream, PartitionId::new(0)),
                RecordOffset::new(5),
            )],
        )
        .unwrap(),
        bookmark.publication(),
    );
    assert_inconsistent(stream_target);

    let mut partition_publication = canonical_v1();
    data_one(&mut partition_publication).partitions[0].bookmark_publication_ceiling =
        BookmarkPublicationSequence::new(1);
    assert_inconsistent(partition_publication);

    let mut stream_publication = canonical_v1();
    stream_publication.document.control.streams[0].bookmark_publication_ceiling =
        BookmarkPublicationSequence::new(1);
    assert_inconsistent(stream_publication);
}

#[test]
fn writer_rejects_partial_stream_bookmark_vectors() {
    let mut fixture = canonical_v1();
    add_second_partition(&mut fixture);

    assert_inconsistent(fixture);
}

#[test]
fn writer_rejects_duplicates_at_storage_scopes() {
    let mut stream_names = canonical_v1();
    add_second_stream(&mut stream_names, group(2), "orders");
    assert_noncanonical(stream_names);

    let mut stream_bookmark_ids = canonical_v1();
    let second_stream = add_second_stream(&mut stream_bookmark_ids, group(2), "payments");
    let duplicate_id = stream_bookmark_ids.document.control.streams[0].bookmarks[0].id();
    let bookmark = stream_bookmark(
        &stream_bookmark_ids,
        second_stream,
        duplicate_id,
        "second-stream",
        1,
    );
    let stream = stream_bookmark_ids
        .document
        .control
        .streams
        .last_mut()
        .unwrap();
    stream.bookmark_publication_ceiling = BookmarkPublicationSequence::new(1);
    stream.bookmarks.push(bookmark);
    assert_noncanonical(stream_bookmark_ids);

    let mut partition_bookmark_ids = canonical_v1();
    let second_stream = add_second_stream(&mut partition_bookmark_ids, group(1), "payments");
    let duplicate_id = data_one(&mut partition_bookmark_ids).partitions[0].bookmarks[0].id();
    let cluster = partition_bookmark_ids.document.source_cluster;
    let second_partition = data_one(&mut partition_bookmark_ids)
        .partitions
        .iter_mut()
        .find(|partition| partition.stream == second_stream)
        .unwrap();
    second_partition.bookmark_publication_ceiling = BookmarkPublicationSequence::new(1);
    second_partition
        .bookmarks
        .push(CommittedBookmark::published(
            duplicate_id,
            BookmarkName::parse("second-partition").unwrap(),
            CommittedCursor::new(
                cluster,
                PartitionKey::new(second_stream, PartitionId::new(0)),
                RecordOffset::new(0),
            ),
            BookmarkPublicationSequence::new(1),
        ));
    assert_noncanonical(partition_bookmark_ids);

    let mut stream_names = canonical_v1();
    let first = stream_names.document.control.streams[0].bookmarks[0].clone();
    let second = stream_names.document.control.streams[0].bookmarks[1].clone();
    stream_names.document.control.streams[0].bookmarks[1] = CommittedStreamBookmark::published(
        second.id(),
        first.name().clone(),
        second.vector().clone(),
        second.publication(),
    );
    assert_noncanonical(stream_names);

    let mut partition_names = canonical_v1();
    let bookmarks = &mut data_one(&mut partition_names).partitions[0].bookmarks;
    let first = bookmarks[0].clone();
    let second = bookmarks[1].clone();
    bookmarks[1] = CommittedBookmark::published(
        second.id(),
        first.name().clone(),
        second.cursor(),
        second.publication(),
    );
    assert_noncanonical(partition_names);
}

#[test]
fn writer_rejects_duplicate_publications_and_inexact_ceilings() {
    let mut stream_publications = canonical_v1();
    let first = stream_publications.document.control.streams[0].bookmarks[0].clone();
    let second = stream_publications.document.control.streams[0].bookmarks[1].clone();
    let mut duplicate = CommittedStreamBookmark::published(
        second.id(),
        second.name().clone(),
        second.vector().clone(),
        first.publication(),
    );
    duplicate.mark_deleted();
    stream_publications.document.control.streams[0].bookmarks[1] = duplicate;
    stream_publications.document.control.streams[0].bookmark_publication_ceiling =
        first.publication();
    assert_noncanonical(stream_publications);

    let mut partition_publications = canonical_v1();
    let bookmarks = &mut data_one(&mut partition_publications).partitions[0].bookmarks;
    let first = bookmarks[0].clone();
    let second = bookmarks[1].clone();
    let mut duplicate = CommittedBookmark::published(
        second.id(),
        second.name().clone(),
        second.cursor(),
        first.publication(),
    );
    duplicate.mark_deleted();
    bookmarks[1] = duplicate;
    data_one(&mut partition_publications).partitions[0].bookmark_publication_ceiling =
        first.publication();
    assert_noncanonical(partition_publications);

    let mut stream_above = canonical_v1();
    stream_above.document.control.streams[0].bookmark_publication_ceiling =
        BookmarkPublicationSequence::new(5);
    assert_inconsistent(stream_above);

    let mut partition_above = canonical_v1();
    data_one(&mut partition_above).partitions[0].bookmark_publication_ceiling =
        BookmarkPublicationSequence::new(3);
    assert_inconsistent(partition_above);

    let mut stream_gap = canonical_v1();
    let bookmark = stream_gap.document.control.streams[0].bookmarks[0].clone();
    stream_gap.document.control.streams[0].bookmarks[0] = CommittedStreamBookmark::published(
        bookmark.id(),
        bookmark.name().clone(),
        bookmark.vector().clone(),
        BookmarkPublicationSequence::new(2),
    );
    let bookmark = stream_gap.document.control.streams[0].bookmarks[1].clone();
    let mut deleted = CommittedStreamBookmark::published(
        bookmark.id(),
        bookmark.name().clone(),
        bookmark.vector().clone(),
        BookmarkPublicationSequence::new(3),
    );
    deleted.mark_deleted();
    stream_gap.document.control.streams[0].bookmarks[1] = deleted;
    stream_gap.document.control.streams[0].bookmark_publication_ceiling =
        BookmarkPublicationSequence::new(3);
    assert_noncanonical(stream_gap);

    let mut partition_gap = canonical_v1();
    let bookmark = data_one(&mut partition_gap).partitions[0].bookmarks[0].clone();
    data_one(&mut partition_gap).partitions[0].bookmarks[0] = CommittedBookmark::published(
        bookmark.id(),
        bookmark.name().clone(),
        bookmark.cursor(),
        BookmarkPublicationSequence::new(2),
    );
    let bookmark = data_one(&mut partition_gap).partitions[0].bookmarks[1].clone();
    let mut deleted = CommittedBookmark::published(
        bookmark.id(),
        bookmark.name().clone(),
        bookmark.cursor(),
        BookmarkPublicationSequence::new(3),
    );
    deleted.mark_deleted();
    data_one(&mut partition_gap).partitions[0].bookmarks[1] = deleted;
    data_one(&mut partition_gap).partitions[0].bookmark_publication_ceiling =
        BookmarkPublicationSequence::new(3);
    assert_noncanonical(partition_gap);

    let mut empty_stream = canonical_v1();
    empty_stream.document.control.streams[0].bookmarks.clear();
    empty_stream.document.control.streams[0].bookmark_publication_ceiling =
        BookmarkPublicationSequence::new(1);
    assert_inconsistent(empty_stream);

    let mut empty_partition = canonical_v1();
    data_one(&mut empty_partition).partitions[0]
        .bookmarks
        .clear();
    data_one(&mut empty_partition).partitions[0].bookmark_publication_ceiling =
        BookmarkPublicationSequence::new(1);
    assert_inconsistent(empty_partition);
}

#[test]
fn verifier_reserves_nested_aggregate_limits_before_allocation() {
    let limits = ExportLimits::default();

    let mut streams = canonical_v1();
    clear_bookmarks(&mut streams);
    add_second_stream(&mut streams, group(2), "payments");
    let (bytes, _) = write_fixture(&mut streams, &limits);
    let restricted = ExportLimits {
        max_streams: 2,
        ..limits
    };
    let error = verify(Cursor::new(bytes), &restricted)
        .err()
        .expect("aggregate streams must be rejected");
    assert!(
        matches!(error, VerifyError::Limit { limit: "streams" }),
        "{error:?}"
    );

    let mut partitions = canonical_v1();
    clear_bookmarks(&mut partitions);
    add_second_stream(&mut partitions, group(2), "payments");
    let (mut bytes, _) = write_fixture(&mut partitions, &limits);
    set_manifest_total(&mut bytes, 2, 1);
    let restricted = ExportLimits {
        max_partitions: 1,
        ..limits
    };
    let error = verify(Cursor::new(bytes), &restricted)
        .err()
        .expect("aggregate partitions must be rejected");
    assert!(
        matches!(
            error,
            VerifyError::Limit {
                limit: "partitions"
            }
        ),
        "{error:?}"
    );

    let mut bookmarks = canonical_v1();
    retain_one_bookmark_per_parent(&mut bookmarks);
    let second_stream = add_second_stream(&mut bookmarks, group(2), "payments");
    let second_bookmark = stream_bookmark(
        &bookmarks,
        second_stream,
        id("018f3f7e-5b3b-7c11-98f7-b65ac15f6520"),
        "second-stream",
        1,
    );
    let stream = bookmarks.document.control.streams.last_mut().unwrap();
    stream.bookmark_publication_ceiling = BookmarkPublicationSequence::new(1);
    stream.bookmarks.push(second_bookmark);
    let cluster = bookmarks.document.source_cluster;
    let second_partition = bookmarks
        .source
        .groups
        .get_mut(&group(2))
        .unwrap()
        .partitions
        .last_mut()
        .unwrap();
    second_partition.bookmark_publication_ceiling = BookmarkPublicationSequence::new(1);
    second_partition
        .bookmarks
        .push(CommittedBookmark::published(
            id("018f3f7e-5b3b-7c11-98f7-b65ac15f6521"),
            BookmarkName::parse("second-partition").unwrap(),
            CommittedCursor::new(
                cluster,
                PartitionKey::new(second_stream, PartitionId::new(0)),
                RecordOffset::new(0),
            ),
            BookmarkPublicationSequence::new(1),
        ));
    let (mut bytes, _) = write_fixture(&mut bookmarks, &limits);
    set_manifest_total(&mut bytes, 5, 0);
    set_manifest_total(&mut bytes, 6, 1);
    let restricted = ExportLimits {
        max_bookmarks: 1,
        ..limits
    };
    let error = verify(Cursor::new(bytes), &restricted)
        .err()
        .expect("aggregate bookmarks must be rejected");
    assert!(
        matches!(error, VerifyError::Limit { limit: "bookmarks" }),
        "{error:?}"
    );

    let mut records = two_record_parents();
    let (mut bytes, _) = write_fixture(&mut records, &limits);
    set_manifest_total(&mut bytes, 3, 1);
    let restricted = ExportLimits {
        max_records: 1,
        ..limits
    };
    let error = verify(Cursor::new(bytes), &restricted)
        .err()
        .expect("aggregate records must be rejected");
    assert!(
        matches!(error, VerifyError::Limit { limit: "records" }),
        "{error:?}"
    );

    let mut payloads = two_record_parents();
    let (mut bytes, _) = write_fixture(&mut payloads, &limits);
    set_manifest_total(&mut bytes, 4, 4);
    let restricted = ExportLimits {
        max_payload_bytes: 4,
        ..limits
    };
    let error = verify(Cursor::new(bytes), &restricted)
        .err()
        .expect("aggregate payload bytes must be rejected");
    assert!(
        matches!(
            error,
            VerifyError::Limit {
                limit: "payload bytes"
            }
        ),
        "{error:?}"
    );
}

#[test]
fn decode_budget_reserves_aggregate_nested_allocations() {
    let limits = ExportLimits {
        max_streams: 1,
        max_partitions: 1,
        max_records: 1,
        max_bookmarks: 1,
        max_payload_bytes: 4,
        ..ExportLimits::default()
    };
    let mut budget = crate::decode::DecodeBudget::new(&limits);

    assert!(budget.reserve_streams(1).is_ok());
    assert!(matches!(
        budget.reserve_streams(1),
        Err(VerifyError::Limit { limit: "streams" })
    ));
    assert!(budget.reserve_partitions(1).is_ok());
    assert!(matches!(
        budget.reserve_partitions(1),
        Err(VerifyError::Limit {
            limit: "partitions"
        })
    ));
    assert!(budget.reserve_records(1).is_ok());
    assert!(matches!(
        budget.reserve_records(1),
        Err(VerifyError::Limit { limit: "records" })
    ));
    assert!(budget.reserve_bookmarks(1).is_ok());
    assert!(matches!(
        budget.reserve_bookmarks(1),
        Err(VerifyError::Limit { limit: "bookmarks" })
    ));
    assert!(budget.reserve_payload_bytes(3).is_ok());
    assert!(matches!(
        budget.reserve_payload_bytes(2),
        Err(VerifyError::Limit {
            limit: "payload bytes"
        })
    ));
}

#[test]
fn writer_rejects_limits_that_cannot_decode_its_output() {
    let mut streams = canonical_v1();
    let mut output = Cursor::new(Vec::new());
    let limits = ExportLimits {
        max_streams: 1,
        ..ExportLimits::default()
    };
    assert!(matches!(
        write_v1(&mut output, &streams.document, &mut streams.source, &limits),
        Err(ExportWriteError::<Infallible>::Limit { limit: "streams" })
    ));

    let mut partitions = canonical_v1();
    let mut output = Cursor::new(Vec::new());
    let limits = ExportLimits {
        max_partitions: 3,
        ..ExportLimits::default()
    };
    assert!(matches!(
        write_v1(
            &mut output,
            &partitions.document,
            &mut partitions.source,
            &limits
        ),
        Err(ExportWriteError::<Infallible>::Limit {
            limit: "partitions"
        })
    ));
}

#[test]
fn writer_and_verifier_require_exact_v1_exclusions() {
    for bits in [EXCLUSIONS_V1 & !(1 << 2), EXCLUSIONS_V1 | (1 << 8)] {
        let mut fixture = canonical_v1();
        fixture.document.exclusions = ExportExclusionsV1::from_bits(bits);
        assert_inconsistent(fixture);
    }

    let limits = ExportLimits::default();
    for bits in [EXCLUSIONS_V1 & !(1 << 2), EXCLUSIONS_V1 | (1 << 8)] {
        let mut fixture = canonical_v1();
        let (mut bytes, _) = write_fixture(&mut fixture, &limits);
        let exclusions_offset = bytes.len() - TRAILER_BYTES_V1 as usize - 8;
        bytes[exclusions_offset..exclusions_offset + 8].copy_from_slice(&bits.to_be_bytes());
        recompute_artifact_digest(&mut bytes);
        assert!(matches!(
            verify(Cursor::new(bytes), &limits),
            Err(VerifyError::Inconsistent {
                field: "fixed exclusions"
            })
        ));
    }
}

#[test]
fn configured_empty_group_has_a_real_verified_data_section() {
    let limits = ExportLimits::default();
    let mut fixture = canonical_v1();
    let (bytes, _) = write_fixture(&mut fixture, &limits);
    let inspection = inspect(&verify(Cursor::new(bytes), &limits).unwrap());
    let empty = &inspection.sections[2];
    assert_eq!(empty.kind, SectionKindV1::DataGroup);
    assert_eq!(empty.group, Some(group(2)));
    assert_eq!(empty.item_count, 0);
    assert!(empty.payload_length > 0);
}

fn write_fixture(
    fixture: &mut FixtureV1,
    limits: &ExportLimits,
) -> (Vec<u8>, light_stream_core::ArtifactIdentity) {
    let mut output = Cursor::new(Vec::new());
    let identity = write_v1(&mut output, &fixture.document, &mut fixture.source, limits).unwrap();
    (output.into_inner(), identity)
}

fn assert_noncanonical(mut fixture: FixtureV1) {
    let mut output = Cursor::new(Vec::new());
    assert!(matches!(
        write_v1(
            &mut output,
            &fixture.document,
            &mut fixture.source,
            &ExportLimits::default()
        ),
        Err(ExportWriteError::<Infallible>::NonCanonical { .. })
    ));
}

fn assert_inconsistent(mut fixture: FixtureV1) {
    let mut output = Cursor::new(Vec::new());
    assert!(matches!(
        write_v1(
            &mut output,
            &fixture.document,
            &mut fixture.source,
            &ExportLimits::default()
        ),
        Err(ExportWriteError::<Infallible>::Inconsistent { .. })
    ));
}

fn data_one(fixture: &mut FixtureV1) -> &mut crate::DataGroupV1 {
    fixture.source.groups.get_mut(&group(1)).unwrap()
}

fn add_second_placement(fixture: &mut FixtureV1) {
    let stream = &mut fixture.document.control.streams[0];
    let descriptor = &stream.descriptor;
    stream.descriptor = StreamDescriptor::new(
        descriptor.cluster(),
        descriptor.stream(),
        descriptor.name().clone(),
        StreamLifecycle::Active,
        vec![
            PartitionPlacement::new(PartitionId::new(0), group(1)),
            PartitionPlacement::new(PartitionId::new(1), group(1)),
        ],
        descriptor.ready_groups().to_vec(),
        descriptor.revision(),
    );
}

fn add_second_partition(fixture: &mut FixtureV1) {
    add_second_placement(fixture);
    let source_cluster = fixture.document.source_cluster;
    let stream = fixture.document.selected_streams[0];
    data_one(fixture).partitions.push(crate::PartitionV1 {
        source_cluster,
        stream,
        partition: PartitionId::new(1),
        retention_floor: RecordOffset::new(0),
        tail: RecordOffset::new(0),
        bookmark_publication_ceiling: BookmarkPublicationSequence::new(0),
        records: Vec::new(),
        bookmarks: Vec::new(),
    });
}

fn add_second_stream(fixture: &mut FixtureV1, group: GroupId, name: &str) -> StreamId {
    let stream: StreamId = id("018f3f7e-5b3b-7c11-98f7-b65ac15f6511");
    fixture.document.selected_streams.push(stream);
    fixture
        .document
        .control
        .streams
        .push(crate::ActiveStreamV1 {
            descriptor: StreamDescriptor::new(
                fixture.document.source_cluster,
                stream,
                StreamName::parse(name).unwrap(),
                StreamLifecycle::Active,
                vec![PartitionPlacement::new(PartitionId::new(0), group)],
                vec![group],
                1,
            ),
            bookmark_publication_ceiling: BookmarkPublicationSequence::new(0),
            bookmarks: Vec::new(),
        });
    fixture
        .source
        .groups
        .get_mut(&group)
        .unwrap()
        .partitions
        .push(crate::PartitionV1 {
            source_cluster: fixture.document.source_cluster,
            stream,
            partition: PartitionId::new(0),
            retention_floor: RecordOffset::new(0),
            tail: RecordOffset::new(0),
            bookmark_publication_ceiling: BookmarkPublicationSequence::new(0),
            records: Vec::new(),
            bookmarks: Vec::new(),
        });
    stream
}

fn stream_bookmark(
    fixture: &FixtureV1,
    stream: StreamId,
    bookmark: light_stream_core::BookmarkId,
    name: &str,
    publication: u64,
) -> CommittedStreamBookmark {
    CommittedStreamBookmark::published(
        bookmark,
        BookmarkName::parse(name).unwrap(),
        StreamCursorVector::new(
            stream,
            vec![CommittedCursor::new(
                fixture.document.source_cluster,
                PartitionKey::new(stream, PartitionId::new(0)),
                RecordOffset::new(0),
            )],
        )
        .unwrap(),
        BookmarkPublicationSequence::new(publication),
    )
}

fn clear_bookmarks(fixture: &mut FixtureV1) {
    for stream in &mut fixture.document.control.streams {
        stream.bookmarks.clear();
        stream.bookmark_publication_ceiling = BookmarkPublicationSequence::new(0);
    }
    for data in fixture.source.groups.values_mut() {
        for partition in &mut data.partitions {
            partition.bookmarks.clear();
            partition.bookmark_publication_ceiling = BookmarkPublicationSequence::new(0);
        }
    }
}

fn retain_one_bookmark_per_parent(fixture: &mut FixtureV1) {
    fixture.document.control.streams[0].bookmarks.truncate(1);
    fixture.document.control.streams[0].bookmark_publication_ceiling =
        fixture.document.control.streams[0].bookmarks[0].publication();
    let partition = &mut data_one(fixture).partitions[0];
    partition.bookmarks.truncate(1);
    partition.bookmark_publication_ceiling = partition.bookmarks[0].publication();
}

fn two_record_parents() -> FixtureV1 {
    let mut fixture = canonical_v1();
    clear_bookmarks(&mut fixture);
    add_second_partition(&mut fixture);
    let cluster = fixture.document.source_cluster;
    let stream = fixture.document.selected_streams[0];
    let partitions = &mut data_one(&mut fixture).partitions;
    partitions[0].retention_floor = RecordOffset::new(0);
    partitions[0].tail = RecordOffset::new(1);
    partitions[0].records = vec![CommittedRecord::new(RecordOffset::new(0), b"aaaa".to_vec())];
    partitions[1] = crate::PartitionV1 {
        source_cluster: cluster,
        stream,
        partition: PartitionId::new(1),
        retention_floor: RecordOffset::new(0),
        tail: RecordOffset::new(1),
        bookmark_publication_ceiling: BookmarkPublicationSequence::new(0),
        records: vec![CommittedRecord::new(RecordOffset::new(0), b"bbbb".to_vec())],
        bookmarks: Vec::new(),
    };
    fixture
}

fn set_manifest_total(bytes: &mut [u8], index: usize, value: u64) {
    let start = manifest_totals_offset(bytes) + index * 8;
    bytes[start..start + 8].copy_from_slice(&value.to_be_bytes());
    recompute_artifact_digest(bytes);
}

fn manifest_totals_offset(bytes: &[u8]) -> usize {
    let mut offset = trailer_u64(bytes, 0) as usize + 8;
    offset += 4 + 8 + 16 + 16;
    let selected = be_u64(&bytes[offset..offset + 8]) as usize;
    offset += 8 + selected * 16;
    offset += 32;
    let data_cuts = be_u64(&bytes[offset..offset + 8]) as usize;
    offset += 8 + data_cuts * 32;
    let sections = be_u64(&bytes[offset..offset + 8]) as usize;
    offset += 8;
    for _ in 0..sections {
        offset += 8 + 2 + 2;
        let group = bytes[offset];
        offset += 1;
        if group == crate::OPTION_SOME_V1 {
            offset += 8;
        }
        offset += 8 + 8 + 8 + 32;
    }
    offset
}

#[derive(Clone, Debug)]
struct SharedCursor {
    bytes: Rc<RefCell<Vec<u8>>>,
    position: u64,
}

impl SharedCursor {
    fn new(bytes: Vec<u8>) -> Self {
        Self {
            bytes: Rc::new(RefCell::new(bytes)),
            position: 0,
        }
    }

    fn mutate(&self, offset: usize) {
        self.bytes.borrow_mut()[offset] ^= 1;
    }
}

impl Read for SharedCursor {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let bytes = self.bytes.borrow();
        let start = usize::try_from(self.position)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "cursor position"))?;
        let remaining = bytes.get(start..).unwrap_or_default();
        let length = remaining.len().min(output.len());
        output[..length].copy_from_slice(&remaining[..length]);
        self.position =
            self.position
                .checked_add(u64::try_from(length).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidInput, "cursor read length")
                })?)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "cursor position"))?;
        Ok(length)
    }
}

impl Seek for SharedCursor {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        let length = i128::try_from(self.bytes.borrow().len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "cursor length"))?;
        let current = i128::from(self.position);
        let next = match position {
            SeekFrom::Start(value) => i128::from(value),
            SeekFrom::End(value) => length + i128::from(value),
            SeekFrom::Current(value) => current + i128::from(value),
        };
        self.position = u64::try_from(next)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "cursor position"))?;
        Ok(self.position)
    }
}

struct SwapAfterManifestRead {
    bytes: Vec<u8>,
    replacement: Option<Vec<u8>>,
    position: u64,
    manifest_end: u64,
    manifest_was_read: bool,
}

impl SwapAfterManifestRead {
    fn new(bytes: Vec<u8>, replacement: Vec<u8>) -> Self {
        let manifest_end =
            u64::try_from(bytes.len() - TRAILER_BYTES_V1 as usize).expect("artifact length");
        Self {
            bytes,
            replacement: Some(replacement),
            position: 0,
            manifest_end,
            manifest_was_read: false,
        }
    }
}

impl Read for SwapAfterManifestRead {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let start = usize::try_from(self.position)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "cursor position"))?;
        let remaining = self.bytes.get(start..).unwrap_or_default();
        let length = remaining.len().min(output.len());
        output[..length].copy_from_slice(&remaining[..length]);
        self.position =
            self.position
                .checked_add(u64::try_from(length).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidInput, "cursor read length")
                })?)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "cursor position"))?;
        if u64::try_from(start)
            .is_ok_and(|start| start < self.manifest_end && self.position >= self.manifest_end)
        {
            self.manifest_was_read = true;
        }
        Ok(length)
    }
}

impl Seek for SwapAfterManifestRead {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        let length = i128::try_from(self.bytes.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "cursor length"))?;
        let current = i128::from(self.position);
        let next = match position {
            SeekFrom::Start(0) => {
                if self.manifest_was_read
                    && let Some(replacement) = self.replacement.take()
                {
                    self.bytes = replacement;
                }
                0
            }
            SeekFrom::Start(value) => i128::from(value),
            SeekFrom::End(value) => length + i128::from(value),
            SeekFrom::Current(value) => current + i128::from(value),
        };
        self.position = u64::try_from(next)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "cursor position"))?;
        Ok(self.position)
    }
}

fn recompute_artifact_digest(bytes: &mut [u8]) {
    let trailer_start = bytes.len() - TRAILER_BYTES_V1 as usize;
    let digest: [u8; 32] = Sha256::digest(&bytes[..trailer_start]).into();
    bytes[trailer_start + 16..trailer_start + 48].copy_from_slice(&digest);
}

fn trailer_u64(bytes: &[u8], relative: usize) -> u64 {
    let start = bytes.len() - TRAILER_BYTES_V1 as usize + relative;
    be_u64(&bytes[start..start + 8])
}

fn be_u16(bytes: &[u8]) -> u16 {
    u16::from_be_bytes(bytes.try_into().unwrap())
}

fn be_u32(bytes: &[u8]) -> u32 {
    u32::from_be_bytes(bytes.try_into().unwrap())
}

fn be_u64(bytes: &[u8]) -> u64 {
    u64::from_be_bytes(bytes.try_into().unwrap())
}

fn group(value: u64) -> GroupId {
    GroupId::new(value).unwrap()
}

fn id<T>(value: &str) -> T
where
    T: std::str::FromStr,
    T::Err: std::fmt::Debug,
{
    value.parse().unwrap()
}
