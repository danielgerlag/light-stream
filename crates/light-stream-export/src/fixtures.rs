use std::{collections::BTreeMap, convert::Infallible};

use light_stream_core::{
    BookmarkName, BookmarkPublicationSequence, CommittedBookmark, CommittedCursor, CommittedRecord,
    CommittedStreamBookmark, ExportDeadline, ExportEpoch, ExportFormatVersion, ExportIntent,
    ExportSelection, ExportSpec, GroupCut, GroupId, MutationRequestId, NodeId, PartitionId,
    PartitionKey, PartitionPlacement, PrincipalId, RecordOffset, RequestSequence,
    StreamCursorVector, StreamDescriptor, StreamLifecycle, StreamName,
};

use crate::{
    ActiveStreamV1, ControlSectionV1, DataGroupSourceV1, DataGroupV1, ExportDocumentV1,
    ExportExclusionsV1, PartitionV1, REQUIRED_FEATURES_V1,
};

#[derive(Clone)]
pub struct FixtureV1 {
    pub document: ExportDocumentV1,
    pub source: FixtureSourceV1,
}

#[derive(Clone, Debug)]
pub struct FixtureSourceV1 {
    pub groups: BTreeMap<GroupId, DataGroupV1>,
    pub requests: Vec<GroupId>,
}

impl DataGroupSourceV1 for FixtureSourceV1 {
    type Error = Infallible;

    fn data_group(
        &mut self,
        group: GroupId,
        _cut: GroupCut,
    ) -> Result<Option<DataGroupV1>, Self::Error> {
        self.requests.push(group);
        Ok(self.groups.remove(&group))
    }
}

pub fn canonical_v1() -> FixtureV1 {
    let cluster = id("018f3f7e-5b3b-7c11-98f7-b65ac15f6501");
    let stream = id("018f3f7e-5b3b-7c11-98f7-b65ac15f6502");
    let selected = ExportSelection::try_new([stream]).expect("fixture selection");
    let request = MutationRequestId::new(
        PrincipalId::parse("export-fixture").expect("fixture principal"),
        id("018f3f7e-5b3b-7c11-98f7-b65ac15f6503"),
        RequestSequence::new(7),
    );
    let intent = ExportIntent::new(request, cluster, selected, ExportFormatVersion::V1);
    let group_one = GroupId::new(1).expect("fixture group");
    let group_two = GroupId::new(2).expect("fixture group");
    let control_group = GroupId::new(100).expect("fixture control group");
    let leader = NodeId::new(1).expect("fixture leader");
    let control_cut = GroupCut::new(control_group, 3, leader, 41);
    let data_one_cut = GroupCut::new(group_one, 4, leader, 51);
    let data_two_cut = GroupCut::new(group_two, 5, leader, 61);
    let cut = light_stream_core::QuiescentCut::try_new(control_cut, [data_one_cut, data_two_cut])
        .expect("fixture cut");
    let spec = ExportSpec::try_new(
        intent,
        ExportEpoch::new(9).expect("fixture epoch"),
        [group_one, group_two],
        ExportDeadline::new(1, 2).expect("fixture deadline"),
    )
    .expect("fixture spec");
    let partition = PartitionId::new(0);
    let key = PartitionKey::new(stream, partition);
    let descriptor = StreamDescriptor::new(
        cluster,
        stream,
        StreamName::parse("orders").expect("fixture stream name"),
        StreamLifecycle::Active,
        vec![PartitionPlacement::new(partition, group_one)],
        vec![group_one],
        12,
    );
    let stream_bookmark = CommittedStreamBookmark::published(
        id("018f3f7e-5b3b-7c11-98f7-b65ac15f6504"),
        BookmarkName::parse("published").expect("fixture bookmark name"),
        StreamCursorVector::new(
            stream,
            vec![CommittedCursor::new(cluster, key, RecordOffset::new(4))],
        )
        .expect("fixture vector"),
        BookmarkPublicationSequence::new(1),
    );
    let mut deleted_stream_bookmark = CommittedStreamBookmark::published(
        id("018f3f7e-5b3b-7c11-98f7-b65ac15f6506"),
        BookmarkName::parse("deleted-published").expect("fixture bookmark name"),
        StreamCursorVector::new(
            stream,
            vec![CommittedCursor::new(cluster, key, RecordOffset::new(3))],
        )
        .expect("fixture vector"),
        BookmarkPublicationSequence::new(2),
    );
    deleted_stream_bookmark.mark_deleted();
    let partition_bookmark = CommittedBookmark::published(
        id("018f3f7e-5b3b-7c11-98f7-b65ac15f6505"),
        BookmarkName::parse("retained").expect("fixture bookmark name"),
        CommittedCursor::new(cluster, key, RecordOffset::new(4)),
        BookmarkPublicationSequence::new(1),
    );
    let mut deleted_partition_bookmark = CommittedBookmark::published(
        id("018f3f7e-5b3b-7c11-98f7-b65ac15f6507"),
        BookmarkName::parse("deleted-retained").expect("fixture bookmark name"),
        CommittedCursor::new(cluster, key, RecordOffset::new(3)),
        BookmarkPublicationSequence::new(2),
    );
    deleted_partition_bookmark.mark_deleted();
    let control = ControlSectionV1 {
        source_cluster: cluster,
        export_id: spec.export().into(),
        cut: control_cut,
        configured_data_groups: vec![group_one, group_two],
        streams: vec![ActiveStreamV1 {
            descriptor,
            bookmark_publication_ceiling: BookmarkPublicationSequence::new(2),
            bookmarks: vec![stream_bookmark, deleted_stream_bookmark],
        }],
    };
    let data_one = DataGroupV1 {
        source_cluster: cluster,
        export_id: spec.export().into(),
        group: group_one,
        cut: data_one_cut,
        partitions: vec![PartitionV1 {
            source_cluster: cluster,
            stream,
            partition,
            retention_floor: RecordOffset::new(2),
            tail: RecordOffset::new(4),
            bookmark_publication_ceiling: BookmarkPublicationSequence::new(2),
            records: vec![
                CommittedRecord::new(RecordOffset::new(2), b"alpha".to_vec()),
                CommittedRecord::new(RecordOffset::new(3), b"beta".to_vec()),
            ],
            bookmarks: vec![partition_bookmark, deleted_partition_bookmark],
        }],
    };
    let data_two = DataGroupV1 {
        source_cluster: cluster,
        export_id: spec.export().into(),
        group: group_two,
        cut: data_two_cut,
        partitions: Vec::new(),
    };
    FixtureV1 {
        document: ExportDocumentV1 {
            source_cluster: cluster,
            export_id: spec.export().into(),
            selected_streams: vec![stream],
            cut,
            control,
            required_features: REQUIRED_FEATURES_V1,
            exclusions: ExportExclusionsV1::v1(),
        },
        source: FixtureSourceV1 {
            groups: BTreeMap::from([(group_one, data_one), (group_two, data_two)]),
            requests: Vec::new(),
        },
    }
}

fn id<T>(value: &str) -> T
where
    T: std::str::FromStr,
    T::Err: std::fmt::Debug,
{
    value.parse().expect("fixture identity")
}
