use light_stream_core::{
    AmbiguousRequest, BookmarkId, BookmarkLifecycle, BookmarkName, BookmarkPage,
    BookmarkPageRequest, BookmarkPublicationSequence, BootstrapCommand, BootstrapResult,
    BootstrapSpec, ByteCount, ByteLimit, CapabilityReport, CapabilitySupport, CatalogRequestId,
    CheckpointCasResult, CheckpointExpectation, CheckpointKey, CheckpointMutation,
    CheckpointRevision, ClusterId, CommittedBookmark, CommittedCheckpoint, CommittedCursor,
    CommittedStreamBookmark, ConsensusGroup, ConsumerId, CreateBookmarkSpec, CreateStreamSpec,
    DomainError, FetchPage, GroupId, HealthStatus, LeaderHint, LeaseDeadline, LeaseDuration,
    LeaseGeneration, LeaseRelease, LeaseRenewal, MutationRequestId, MutationSessionId,
    NodeDescriptor, NodeId, PartitionId, PartitionKey, PartitionPlacement, PartitionRoute,
    PrincipalId, ProducerRequestId, ProducerSessionId, ProtectedFetchRequest, PublishBatch,
    PublishProbe, PublishReceipt, RecordOffset, ReplayLease, ReplayLeaseId, ReplayLeaseLifecycle,
    ReplayLeaseRequest, ReplayRange, RequestOutcome, RequestSequence, RetentionRequest,
    RetentionResult, RetentionStatus, SecurityMode, StreamBookmarkPage, StreamBookmarkPageRequest,
    StreamCursorVector, StreamDescriptor, StreamId, StreamLifecycle, StreamName,
};

use crate::v1;

pub type FetchRequestParts = (
    ClusterId,
    PartitionKey,
    RecordOffset,
    u32,
    Option<GroupId>,
    Option<u64>,
);

pub type ReceiptRequestParts = (
    ClusterId,
    PartitionKey,
    ProducerRequestId,
    Option<GroupId>,
    Option<u64>,
);

pub fn health_to_wire(
    status: &HealthStatus,
    public_address: impl Into<String>,
    peer_address: impl Into<String>,
    bootstrapped: bool,
    cluster_id: Option<ClusterId>,
) -> v1::HealthResponse {
    v1::HealthResponse {
        ready: status.ready(),
        revision: status.revision().to_owned(),
        security_mode: security_mode_to_wire(status.security_mode()) as i32,
        public_address: public_address.into(),
        peer_address: peer_address.into(),
        bootstrapped,
        cluster_id: cluster_id.map_or_else(String::new, |value| value.to_string()),
    }
}

pub fn capabilities_to_wire(
    revision: impl Into<String>,
    reports: &[CapabilityReport],
) -> v1::CapabilitiesResponse {
    v1::CapabilitiesResponse {
        revision: revision.into(),
        capabilities: reports
            .iter()
            .map(|report| {
                let (state, available_phase) = match report.support() {
                    CapabilitySupport::Available => (v1::CapabilityState::Available, String::new()),
                    CapabilitySupport::Unsupported { available_phase } => {
                        (v1::CapabilityState::Unsupported, available_phase.clone())
                    }
                };
                v1::Capability {
                    name: report.capability().as_str().to_owned(),
                    state: state as i32,
                    available_phase,
                }
            })
            .collect(),
    }
}

pub fn publish_probe_from_wire(request: v1::PublishRequest) -> Result<PublishProbe, DomainError> {
    let cluster = request.cluster_id.parse::<ClusterId>()?;
    let stream = request.stream_id.parse::<StreamId>()?;
    let request_id = request
        .request_id
        .ok_or_else(|| DomainError::InvalidIdentity {
            kind: "producer request ID".to_owned(),
            reason: "request_id is required".to_owned(),
        })?;
    let producer_request = ProducerRequestId::new(
        PrincipalId::parse(request_id.principal_id)?,
        request_id
            .producer_session_id
            .parse::<ProducerSessionId>()?,
        RequestSequence::new(request_id.sequence),
    );
    let partition = PartitionKey::new(stream, PartitionId::new(request.partition_id));
    PublishProbe::new(cluster, partition, producer_request, request.records)
}

pub fn publish_batch_from_wire(request: v1::PublishRequest) -> Result<PublishBatch, DomainError> {
    let bookmark = (!request.bookmark_name.is_empty())
        .then(|| BookmarkName::parse(request.bookmark_name.clone()))
        .transpose()?;
    let probe = publish_probe_from_wire(request)?;
    let mut batch = PublishBatch::new(
        probe.cluster(),
        probe.partition(),
        probe.request().clone(),
        probe.records().to_vec(),
    )?;
    if let Some(bookmark) = bookmark {
        batch = batch.with_bookmark(bookmark);
    }
    Ok(batch)
}

pub fn publish_batch_and_route_from_wire(
    request: v1::PublishRequest,
) -> Result<(PublishBatch, Option<GroupId>, Option<u64>), DomainError> {
    let group = (request.route_group_id != 0)
        .then(|| GroupId::new(request.route_group_id))
        .transpose()?;
    let revision = (request.route_revision != 0).then_some(request.route_revision);
    Ok((publish_batch_from_wire(request)?, group, revision))
}

pub fn bootstrap_from_wire(request: v1::BootstrapRequest) -> Result<BootstrapCommand, DomainError> {
    let spec = BootstrapSpec::new(
        request.cluster_id.parse::<ClusterId>()?,
        request.stream_id.parse::<StreamId>()?,
        StreamName::parse(request.stream_name)?,
    );
    if request.members.is_empty() {
        if request.seed_node_id != 0 {
            return Err(DomainError::InvalidIdentity {
                kind: "bootstrap topology".to_owned(),
                reason: "standalone bootstrap must omit seed_node_id".to_owned(),
            });
        }
        return Ok(BootstrapCommand::standalone(spec));
    }
    if request.members.len() != 3 {
        return Err(DomainError::InvalidIdentity {
            kind: "bootstrap topology".to_owned(),
            reason: "three-voter bootstrap requires exactly three members".to_owned(),
        });
    }
    let seed_node_id = NodeId::new(request.seed_node_id)?;
    let members = request
        .members
        .into_iter()
        .map(|member| {
            validate_uri("public URI", &member.public_uri)?;
            validate_uri("peer URI", &member.peer_uri)?;
            Ok(NodeDescriptor::new(
                NodeId::new(member.node_id)?,
                member.public_uri,
                member.peer_uri,
            ))
        })
        .collect::<Result<Vec<_>, DomainError>>()?;
    Ok(BootstrapCommand::three_voter(spec, seed_node_id, members))
}

pub fn bootstrap_to_wire(result: &BootstrapResult) -> v1::BootstrapResponse {
    v1::BootstrapResponse {
        result: Some(v1::bootstrap_response::Result::Success(
            v1::BootstrapSuccess {
                cluster_id: result.cluster().to_string(),
                stream_id: result.stream().to_string(),
                stream_name: result.stream_name().to_string(),
                control_group_id: result.control_group().get(),
                data_group_id: result.data_group().get(),
            },
        )),
    }
}

pub fn publish_receipt_to_wire(receipt: &PublishReceipt) -> v1::PublishSuccess {
    v1::PublishSuccess {
        request_id: Some(v1::ProducerRequestId {
            principal_id: receipt.request().principal().to_string(),
            producer_session_id: receipt.request().session().to_string(),
            sequence: receipt.request().sequence().get(),
        }),
        range: Some(v1::CommittedRecordRange {
            stream_id: receipt.range().partition().stream().to_string(),
            partition_id: receipt.range().partition().partition().get(),
            first_offset: receipt.range().first().get(),
            count: receipt.range().count(),
        }),
        bookmark: receipt.bookmark().map(bookmark_to_wire),
    }
}

pub fn bookmark_to_wire(bookmark: &CommittedBookmark) -> v1::Bookmark {
    v1::Bookmark {
        bookmark_id: bookmark.id().to_string(),
        name: bookmark.name().to_string(),
        cluster_id: bookmark.cursor().cluster().to_string(),
        stream_id: bookmark.cursor().partition().stream().to_string(),
        partition_id: bookmark.cursor().partition().partition().get(),
        next_offset: bookmark.cursor().next_offset().get(),
        publication_sequence: bookmark.publication().get(),
        lifecycle: match bookmark.lifecycle() {
            BookmarkLifecycle::Available => "available",
            BookmarkLifecycle::Deleted => "deleted",
        }
        .to_owned(),
    }
}

pub fn bookmark_from_wire(value: v1::Bookmark) -> Result<CommittedBookmark, DomainError> {
    let lifecycle = match value.lifecycle.as_str() {
        "available" => BookmarkLifecycle::Available,
        "deleted" => BookmarkLifecycle::Deleted,
        other => {
            return Err(DomainError::InvalidName {
                kind: "bookmark lifecycle".to_owned(),
                reason: format!("unknown lifecycle {other:?}"),
            });
        }
    };
    let mut bookmark = CommittedBookmark::published(
        value.bookmark_id.parse()?,
        BookmarkName::parse(value.name)?,
        CommittedCursor::new(
            value.cluster_id.parse()?,
            PartitionKey::new(
                value.stream_id.parse()?,
                PartitionId::new(value.partition_id),
            ),
            RecordOffset::new(value.next_offset),
        ),
        BookmarkPublicationSequence::new(value.publication_sequence),
    );
    if lifecycle == BookmarkLifecycle::Deleted {
        bookmark.mark_deleted();
    }
    Ok(bookmark)
}

pub type BookmarkRoute = (ClusterId, PartitionKey, Option<GroupId>, Option<u64>);

fn bookmark_route(
    cluster_id: String,
    stream_id: String,
    partition_id: u32,
    route_group_id: u64,
    route_revision: u64,
) -> Result<BookmarkRoute, DomainError> {
    Ok((
        cluster_id.parse()?,
        PartitionKey::new(stream_id.parse()?, PartitionId::new(partition_id)),
        (route_group_id != 0)
            .then(|| GroupId::new(route_group_id))
            .transpose()?,
        (route_revision != 0).then_some(route_revision),
    ))
}

pub type CreateBookmarkParts = (ClusterId, CreateBookmarkSpec, Option<GroupId>, Option<u64>);

pub fn create_bookmark_from_wire(
    request: v1::CreateBookmarkRequest,
) -> Result<CreateBookmarkParts, DomainError> {
    let (cluster, partition, group, revision) = bookmark_route(
        request.cluster_id,
        request.stream_id,
        request.partition_id,
        request.route_group_id,
        request.route_revision,
    )?;
    Ok((
        cluster,
        CreateBookmarkSpec::new(
            request.bookmark_id.parse()?,
            partition,
            BookmarkName::parse(request.name)?,
            RecordOffset::new(request.next_offset),
        ),
        group,
        revision,
    ))
}

pub type ResolveBookmarkParts = (
    ClusterId,
    PartitionKey,
    BookmarkName,
    Option<GroupId>,
    Option<u64>,
);

pub fn resolve_bookmark_from_wire(
    request: v1::ResolveBookmarkRequest,
) -> Result<ResolveBookmarkParts, DomainError> {
    let (cluster, partition, group, revision) = bookmark_route(
        request.cluster_id,
        request.stream_id,
        request.partition_id,
        request.route_group_id,
        request.route_revision,
    )?;
    Ok((
        cluster,
        partition,
        BookmarkName::parse(request.name)?,
        group,
        revision,
    ))
}

pub type DeleteBookmarkParts = (
    ClusterId,
    PartitionKey,
    BookmarkId,
    Option<GroupId>,
    Option<u64>,
);

pub fn delete_bookmark_from_wire(
    request: v1::DeleteBookmarkRequest,
) -> Result<DeleteBookmarkParts, DomainError> {
    let (cluster, partition, group, revision) = bookmark_route(
        request.cluster_id,
        request.stream_id,
        request.partition_id,
        request.route_group_id,
        request.route_revision,
    )?;
    Ok((
        cluster,
        partition,
        request.bookmark_id.parse()?,
        group,
        revision,
    ))
}

pub type ListBookmarksParts = (ClusterId, BookmarkPageRequest, Option<GroupId>, Option<u64>);

pub fn list_bookmarks_from_wire(
    request: v1::ListBookmarksRequest,
) -> Result<ListBookmarksParts, DomainError> {
    let (cluster, partition, group, revision) = bookmark_route(
        request.cluster_id,
        request.stream_id,
        request.partition_id,
        request.route_group_id,
        request.route_revision,
    )?;
    Ok((
        cluster,
        BookmarkPageRequest::new(
            partition,
            request.limit,
            (request.publication_ceiling != 0)
                .then(|| BookmarkPublicationSequence::new(request.publication_ceiling)),
            (request.before_publication != 0)
                .then(|| BookmarkPublicationSequence::new(request.before_publication)),
        )?,
        group,
        revision,
    ))
}

pub fn bookmark_page_to_wire(page: &BookmarkPage) -> v1::ListBookmarksResponse {
    v1::ListBookmarksResponse {
        result: Some(v1::list_bookmarks_response::Result::Page(
            v1::BookmarkPage {
                bookmarks: page.items().iter().map(bookmark_to_wire).collect(),
                publication_ceiling: page.publication_ceiling().get(),
                next_before: page.next_before().map_or(0, |value| value.get()),
            },
        )),
    }
}

pub fn stream_bookmark_to_wire(bookmark: &CommittedStreamBookmark) -> v1::StreamBookmark {
    v1::StreamBookmark {
        bookmark_id: bookmark.id().to_string(),
        name: bookmark.name().to_string(),
        cluster_id: bookmark.vector().cluster().to_string(),
        stream_id: bookmark.vector().stream().to_string(),
        positions: bookmark
            .vector()
            .positions()
            .iter()
            .map(|cursor| v1::StreamBookmarkPosition {
                partition_id: cursor.partition().partition().get(),
                next_offset: cursor.next_offset().get(),
            })
            .collect(),
        publication_sequence: bookmark.publication().get(),
        lifecycle: match bookmark.lifecycle() {
            BookmarkLifecycle::Available => "available",
            BookmarkLifecycle::Deleted => "deleted",
        }
        .to_owned(),
    }
}

pub fn stream_bookmark_from_wire(
    value: v1::StreamBookmark,
) -> Result<CommittedStreamBookmark, DomainError> {
    let cluster = value.cluster_id.parse::<ClusterId>()?;
    let stream = value.stream_id.parse::<StreamId>()?;
    let vector = StreamCursorVector::new(
        stream,
        value
            .positions
            .into_iter()
            .map(|position| {
                CommittedCursor::new(
                    cluster,
                    PartitionKey::new(stream, PartitionId::new(position.partition_id)),
                    RecordOffset::new(position.next_offset),
                )
            })
            .collect(),
    )?;
    let mut bookmark = CommittedStreamBookmark::published(
        value.bookmark_id.parse()?,
        BookmarkName::parse(value.name)?,
        vector,
        BookmarkPublicationSequence::new(value.publication_sequence),
    );
    match value.lifecycle.as_str() {
        "available" => {}
        "deleted" => bookmark.mark_deleted(),
        other => {
            return Err(DomainError::InvalidName {
                kind: "stream bookmark lifecycle".to_owned(),
                reason: format!("unknown lifecycle {other:?}"),
            });
        }
    }
    Ok(bookmark)
}

pub type CreateStreamBookmarkParts = (ClusterId, BookmarkId, BookmarkName, StreamCursorVector);

pub fn create_stream_bookmark_from_wire(
    request: v1::CreateStreamBookmarkRequest,
) -> Result<CreateStreamBookmarkParts, DomainError> {
    let cluster = request.cluster_id.parse::<ClusterId>()?;
    let stream = request.stream_id.parse::<StreamId>()?;
    let vector = StreamCursorVector::new(
        stream,
        request
            .positions
            .into_iter()
            .map(|position| {
                CommittedCursor::new(
                    cluster,
                    PartitionKey::new(stream, PartitionId::new(position.partition_id)),
                    RecordOffset::new(position.next_offset),
                )
            })
            .collect(),
    )?;
    Ok((
        cluster,
        request.bookmark_id.parse()?,
        BookmarkName::parse(request.name)?,
        vector,
    ))
}

pub fn resolve_stream_bookmark_from_wire(
    request: v1::ResolveStreamBookmarkRequest,
) -> Result<(ClusterId, StreamId, BookmarkName), DomainError> {
    Ok((
        request.cluster_id.parse()?,
        request.stream_id.parse()?,
        BookmarkName::parse(request.name)?,
    ))
}

pub fn delete_stream_bookmark_from_wire(
    request: v1::DeleteStreamBookmarkRequest,
) -> Result<(ClusterId, StreamId, BookmarkId), DomainError> {
    Ok((
        request.cluster_id.parse()?,
        request.stream_id.parse()?,
        request.bookmark_id.parse()?,
    ))
}

pub fn list_stream_bookmarks_from_wire(
    request: v1::ListStreamBookmarksRequest,
) -> Result<StreamBookmarkPageRequest, DomainError> {
    StreamBookmarkPageRequest::new(
        request.cluster_id.parse()?,
        request.stream_id.parse()?,
        request.limit,
        (request.publication_ceiling != 0)
            .then(|| BookmarkPublicationSequence::new(request.publication_ceiling)),
        (request.before_publication != 0)
            .then(|| BookmarkPublicationSequence::new(request.before_publication)),
    )
}

pub fn stream_bookmark_page_to_wire(page: &StreamBookmarkPage) -> v1::ListStreamBookmarksResponse {
    v1::ListStreamBookmarksResponse {
        result: Some(v1::list_stream_bookmarks_response::Result::Page(
            v1::StreamBookmarkPage {
                bookmarks: page.items().iter().map(stream_bookmark_to_wire).collect(),
                publication_ceiling: page.publication_ceiling().get(),
                next_before: page.next_before().map_or(0, |value| value.get()),
            },
        )),
    }
}

pub type AdvanceRetentionParts = (ClusterId, RetentionRequest, Option<GroupId>, Option<u64>);

pub fn advance_retention_from_wire(
    request: v1::AdvanceRetentionRequest,
) -> Result<AdvanceRetentionParts, DomainError> {
    let request_id = request
        .request_id
        .ok_or_else(|| DomainError::InvalidIdentity {
            kind: "mutation request ID".to_owned(),
            reason: "request_id is required".to_owned(),
        })?;
    Ok((
        request.cluster_id.parse()?,
        RetentionRequest::new(
            mutation_request_id_from_wire(request_id)?,
            PartitionKey::new(
                request.stream_id.parse()?,
                PartitionId::new(request.partition_id),
            ),
            RecordOffset::new(request.target_floor),
        ),
        (request.route_group_id != 0)
            .then(|| GroupId::new(request.route_group_id))
            .transpose()?,
        (request.route_revision != 0).then_some(request.route_revision),
    ))
}

pub fn retention_result_to_wire(result: &RetentionResult) -> v1::RetentionAdvance {
    v1::RetentionAdvance {
        request_id: Some(mutation_request_id_to_wire(result.request())),
        stream_id: result.partition().stream().to_string(),
        partition_id: result.partition().partition().get(),
        previous_floor: result.previous_floor().get(),
        floor: result.floor().get(),
    }
}

pub fn retention_result_from_wire(
    result: v1::RetentionAdvance,
) -> Result<RetentionResult, DomainError> {
    Ok(RetentionResult::new(
        mutation_request_id_from_wire(result.request_id.ok_or_else(|| {
            DomainError::InvalidIdentity {
                kind: "mutation request ID".to_owned(),
                reason: "request_id is required".to_owned(),
            }
        })?)?,
        PartitionKey::new(
            result.stream_id.parse()?,
            PartitionId::new(result.partition_id),
        ),
        RecordOffset::new(result.previous_floor),
        RecordOffset::new(result.floor),
    ))
}

pub type RetentionStatusParts = (ClusterId, PartitionKey, Option<GroupId>, Option<u64>);

pub fn retention_status_from_wire(
    request: v1::RetentionStatusRequest,
) -> Result<RetentionStatusParts, DomainError> {
    Ok((
        request.cluster_id.parse()?,
        PartitionKey::new(
            request.stream_id.parse()?,
            PartitionId::new(request.partition_id),
        ),
        (request.route_group_id != 0)
            .then(|| GroupId::new(request.route_group_id))
            .transpose()?,
        (request.route_revision != 0).then_some(request.route_revision),
    ))
}

pub fn retention_status_to_wire(status: &RetentionStatus) -> v1::RetentionStatus {
    v1::RetentionStatus {
        stream_id: status.partition().stream().to_string(),
        partition_id: status.partition().partition().get(),
        logical_floor: status.logical_floor().get(),
        reclaim_cursor: status.reclaim_cursor().get(),
        logically_expired_bytes: status.logically_expired_bytes().get(),
        raft_only_bytes: status.raft_only_bytes().get(),
    }
}

pub fn retention_status_from_response(
    status: v1::RetentionStatus,
) -> Result<RetentionStatus, DomainError> {
    Ok(RetentionStatus::new(
        PartitionKey::new(
            status.stream_id.parse()?,
            PartitionId::new(status.partition_id),
        ),
        RecordOffset::new(status.logical_floor),
        RecordOffset::new(status.reclaim_cursor),
        light_stream_core::ByteCount::new(status.logically_expired_bytes),
        light_stream_core::ByteCount::new(status.raft_only_bytes),
    ))
}

fn replay_range_from_wire(value: v1::ReplayRange) -> Result<ReplayRange, DomainError> {
    ReplayRange::new(
        PartitionKey::new(
            value.stream_id.parse()?,
            PartitionId::new(value.partition_id),
        ),
        RecordOffset::new(value.start_offset),
        RecordOffset::new(value.end_offset),
    )
}

fn replay_range_to_wire(value: ReplayRange) -> v1::ReplayRange {
    v1::ReplayRange {
        stream_id: value.partition().stream().to_string(),
        partition_id: value.partition().partition().get(),
        start_offset: value.start().get(),
        end_offset: value.end().get(),
    }
}

pub fn replay_lease_to_wire(value: &ReplayLease) -> v1::ReplayLease {
    v1::ReplayLease {
        lease_id: value.id().to_string(),
        admission_request: Some(mutation_request_id_to_wire(value.request().request())),
        cluster_id: value.request().cluster().to_string(),
        range: Some(replay_range_to_wire(value.range())),
        protected_bytes: value.protected_bytes().get(),
        generation: value.generation().get(),
        expires_at_unix_ms: value.expires_at().unix_millis(),
        hard_expires_at_unix_ms: value.hard_expires_at().unix_millis(),
        lifecycle: match value.lifecycle() {
            ReplayLeaseLifecycle::Active => "active",
            ReplayLeaseLifecycle::Released => "released",
            ReplayLeaseLifecycle::Expired => "expired",
        }
        .to_owned(),
        requested_duration_ms: value.request().duration().as_millis(),
        requested_max_bytes: value.request().max_bytes().get(),
    }
}

pub fn replay_lease_from_wire(value: v1::ReplayLease) -> Result<ReplayLease, DomainError> {
    let request = ReplayLeaseRequest::new(
        mutation_request_id_from_wire(value.admission_request.ok_or_else(|| {
            DomainError::InvalidIdentity {
                kind: "mutation request ID".to_owned(),
                reason: "admission_request is required".to_owned(),
            }
        })?)?,
        value.cluster_id.parse()?,
        replay_range_from_wire(value.range.ok_or_else(|| DomainError::InvalidRange {
            reason: "replay range is required".to_owned(),
        })?)?,
        LeaseDuration::from_millis(value.requested_duration_ms)?,
        ByteLimit::new(value.requested_max_bytes)?,
    );
    let lifecycle = match value.lifecycle.as_str() {
        "active" => ReplayLeaseLifecycle::Active,
        "released" => ReplayLeaseLifecycle::Released,
        "expired" => ReplayLeaseLifecycle::Expired,
        other => {
            return Err(DomainError::InvalidName {
                kind: "replay lease lifecycle".to_owned(),
                reason: format!("unknown lifecycle {other:?}"),
            });
        }
    };
    Ok(ReplayLease::restored(
        value.lease_id.parse()?,
        request,
        ByteCount::new(value.protected_bytes),
        LeaseGeneration::new(value.generation),
        LeaseDeadline::new(value.expires_at_unix_ms),
        LeaseDeadline::new(value.hard_expires_at_unix_ms),
        lifecycle,
    ))
}

pub type AdmitReplayLeaseParts = (ClusterId, ReplayLeaseRequest, Option<GroupId>, Option<u64>);

pub fn admit_replay_lease_from_wire(
    request: v1::AdmitReplayLeaseRequest,
) -> Result<AdmitReplayLeaseParts, DomainError> {
    Ok((
        request.cluster_id.parse()?,
        ReplayLeaseRequest::new(
            mutation_request_id_from_wire(request.request_id.ok_or_else(|| {
                DomainError::InvalidIdentity {
                    kind: "mutation request ID".to_owned(),
                    reason: "request_id is required".to_owned(),
                }
            })?)?,
            request.cluster_id.parse()?,
            replay_range_from_wire(request.range.ok_or_else(|| DomainError::InvalidRange {
                reason: "replay range is required".to_owned(),
            })?)?,
            LeaseDuration::from_millis(request.duration_ms)?,
            ByteLimit::new(request.max_bytes)?,
        ),
        (request.route_group_id != 0)
            .then(|| GroupId::new(request.route_group_id))
            .transpose()?,
        (request.route_revision != 0).then_some(request.route_revision),
    ))
}

pub type RenewReplayLeaseParts = (ClusterId, LeaseRenewal, Option<GroupId>, Option<u64>);

pub fn renew_replay_lease_from_wire(
    request: v1::RenewReplayLeaseRequest,
) -> Result<RenewReplayLeaseParts, DomainError> {
    let partition = PartitionKey::new(
        request.stream_id.parse()?,
        PartitionId::new(request.partition_id),
    );
    Ok((
        request.cluster_id.parse()?,
        LeaseRenewal::new(
            mutation_request_id_from_wire(request.request_id.ok_or_else(|| {
                DomainError::InvalidIdentity {
                    kind: "mutation request ID".to_owned(),
                    reason: "request_id is required".to_owned(),
                }
            })?)?,
            partition,
            request.lease_id.parse()?,
            LeaseDuration::from_millis(request.duration_ms)?,
        ),
        (request.route_group_id != 0)
            .then(|| GroupId::new(request.route_group_id))
            .transpose()?,
        (request.route_revision != 0).then_some(request.route_revision),
    ))
}

pub type ReleaseReplayLeaseParts = (ClusterId, LeaseRelease, Option<GroupId>, Option<u64>);

pub fn release_replay_lease_from_wire(
    request: v1::ReleaseReplayLeaseRequest,
) -> Result<ReleaseReplayLeaseParts, DomainError> {
    let partition = PartitionKey::new(
        request.stream_id.parse()?,
        PartitionId::new(request.partition_id),
    );
    Ok((
        request.cluster_id.parse()?,
        LeaseRelease::new(
            mutation_request_id_from_wire(request.request_id.ok_or_else(|| {
                DomainError::InvalidIdentity {
                    kind: "mutation request ID".to_owned(),
                    reason: "request_id is required".to_owned(),
                }
            })?)?,
            partition,
            request.lease_id.parse()?,
        ),
        (request.route_group_id != 0)
            .then(|| GroupId::new(request.route_group_id))
            .transpose()?,
        (request.route_revision != 0).then_some(request.route_revision),
    ))
}

pub type ReplayLeaseReadParts = (
    ClusterId,
    PartitionKey,
    ReplayLeaseId,
    Option<GroupId>,
    Option<u64>,
);

pub fn get_replay_lease_from_wire(
    request: v1::GetReplayLeaseRequest,
) -> Result<ReplayLeaseReadParts, DomainError> {
    Ok((
        request.cluster_id.parse()?,
        PartitionKey::new(
            request.stream_id.parse()?,
            PartitionId::new(request.partition_id),
        ),
        request.lease_id.parse()?,
        (request.route_group_id != 0)
            .then(|| GroupId::new(request.route_group_id))
            .transpose()?,
        (request.route_revision != 0).then_some(request.route_revision),
    ))
}

pub type FetchProtectedParts = (ProtectedFetchRequest, Option<GroupId>, Option<u64>);

pub fn fetch_protected_from_wire(
    request: v1::FetchProtectedRequest,
) -> Result<FetchProtectedParts, DomainError> {
    Ok((
        ProtectedFetchRequest::new(
            request.cluster_id.parse()?,
            PartitionKey::new(
                request.stream_id.parse()?,
                PartitionId::new(request.partition_id),
            ),
            request.lease_id.parse()?,
            RecordOffset::new(request.offset),
            request.limit,
        ),
        (request.route_group_id != 0)
            .then(|| GroupId::new(request.route_group_id))
            .transpose()?,
        (request.route_revision != 0).then_some(request.route_revision),
    ))
}

pub fn fetch_from_wire(request: v1::FetchRequest) -> Result<FetchRequestParts, DomainError> {
    Ok((
        request.cluster_id.parse::<ClusterId>()?,
        PartitionKey::new(
            request.stream_id.parse::<StreamId>()?,
            PartitionId::new(request.partition_id),
        ),
        RecordOffset::new(request.offset),
        request.limit,
        (request.route_group_id != 0)
            .then(|| GroupId::new(request.route_group_id))
            .transpose()?,
        (request.route_revision != 0).then_some(request.route_revision),
    ))
}

pub fn fetch_to_wire(page: &FetchPage) -> v1::FetchResponse {
    v1::FetchResponse {
        result: Some(v1::fetch_response::Result::Success(v1::FetchSuccess {
            stream_id: page.partition().stream().to_string(),
            partition_id: page.partition().partition().get(),
            records: page
                .records()
                .iter()
                .map(|record| v1::Record {
                    offset: record.offset().get(),
                    payload: record.payload().to_vec(),
                })
                .collect(),
            next_offset: page.next_offset().get(),
        })),
    }
}

pub fn receipt_from_wire(request: v1::ReceiptRequest) -> Result<ReceiptRequestParts, DomainError> {
    let request_id = request
        .request_id
        .ok_or_else(|| DomainError::InvalidIdentity {
            kind: "producer request ID".to_owned(),
            reason: "request_id is required".to_owned(),
        })?;
    Ok((
        request.cluster_id.parse::<ClusterId>()?,
        PartitionKey::new(
            request.stream_id.parse::<StreamId>()?,
            PartitionId::new(request.partition_id),
        ),
        ProducerRequestId::new(
            PrincipalId::parse(request_id.principal_id)?,
            request_id
                .producer_session_id
                .parse::<ProducerSessionId>()?,
            RequestSequence::new(request_id.sequence),
        ),
        (request.route_group_id != 0)
            .then(|| GroupId::new(request.route_group_id))
            .transpose()?,
        (request.route_revision != 0).then_some(request.route_revision),
    ))
}

pub fn create_stream_from_wire(
    request: v1::CreateStreamRequest,
) -> Result<(ClusterId, CreateStreamSpec), DomainError> {
    Ok((
        request.cluster_id.parse()?,
        CreateStreamSpec::new(
            request.request_id.parse::<CatalogRequestId>()?,
            StreamName::parse(request.stream_name)?,
            request.partition_count,
        )?,
    ))
}

pub fn stream_selector_from_wire(
    cluster_id: String,
    stream_id: String,
    stream_name: String,
) -> Result<(ClusterId, Option<StreamId>, Option<StreamName>), DomainError> {
    let cluster = cluster_id.parse()?;
    match (stream_id.is_empty(), stream_name.is_empty()) {
        (false, true) => Ok((cluster, Some(stream_id.parse()?), None)),
        (true, false) => Ok((cluster, None, Some(StreamName::parse(stream_name)?))),
        _ => Err(DomainError::InvalidIdentity {
            kind: "stream selector".to_owned(),
            reason: "provide exactly one of stream_id or stream_name".to_owned(),
        }),
    }
}

pub fn stream_to_wire(stream: &StreamDescriptor) -> v1::StreamDescription {
    v1::StreamDescription {
        cluster_id: stream.cluster().to_string(),
        stream_id: stream.stream().to_string(),
        stream_name: stream.name().to_string(),
        lifecycle: match stream.lifecycle() {
            StreamLifecycle::Preparing => "preparing",
            StreamLifecycle::Active => "active",
            StreamLifecycle::Deleting => "deleting",
            StreamLifecycle::Deleted => "deleted",
        }
        .to_owned(),
        placements: stream
            .placements()
            .iter()
            .map(|value| v1::PartitionPlacement {
                partition_id: value.partition().get(),
                group_id: value.group().get(),
            })
            .collect(),
        ready_group_ids: stream
            .ready_groups()
            .iter()
            .map(|value| value.get())
            .collect(),
        revision: stream.revision(),
    }
}

pub fn stream_from_wire(value: v1::StreamDescription) -> Result<StreamDescriptor, DomainError> {
    let lifecycle = match value.lifecycle.as_str() {
        "preparing" => StreamLifecycle::Preparing,
        "active" => StreamLifecycle::Active,
        "deleting" => StreamLifecycle::Deleting,
        "deleted" => StreamLifecycle::Deleted,
        other => {
            return Err(DomainError::InvalidName {
                kind: "stream lifecycle".to_owned(),
                reason: format!("unknown lifecycle {other:?}"),
            });
        }
    };
    Ok(StreamDescriptor::new(
        value.cluster_id.parse()?,
        value.stream_id.parse()?,
        StreamName::parse(value.stream_name)?,
        lifecycle,
        value
            .placements
            .into_iter()
            .map(|item| {
                Ok(PartitionPlacement::new(
                    PartitionId::new(item.partition_id),
                    GroupId::new(item.group_id)?,
                ))
            })
            .collect::<Result<Vec<_>, DomainError>>()?,
        value
            .ready_group_ids
            .into_iter()
            .map(GroupId::new)
            .collect::<Result<Vec<_>, DomainError>>()?,
        value.revision,
    ))
}

pub fn route_to_wire(route: &PartitionRoute, leader: Option<&LeaderHint>) -> v1::PartitionRoute {
    v1::PartitionRoute {
        cluster_id: route.cluster().to_string(),
        stream_id: route.stream().to_string(),
        stream_name: route.stream_name().to_string(),
        partition_id: route.partition().get(),
        group_id: route.group().get(),
        route_revision: route.route_revision(),
        leader: leader.map(leader_hint_to_wire),
    }
}

pub fn route_from_wire(value: v1::PartitionRoute) -> Result<PartitionRoute, DomainError> {
    Ok(PartitionRoute::new(
        value.cluster_id.parse()?,
        value.stream_id.parse()?,
        StreamName::parse(value.stream_name)?,
        PartitionId::new(value.partition_id),
        GroupId::new(value.group_id)?,
        value.route_revision,
    ))
}

pub type GetCheckpointParts = (CheckpointKey, Option<GroupId>, Option<u64>);

pub fn get_checkpoint_from_wire(
    request: v1::GetCheckpointRequest,
) -> Result<GetCheckpointParts, DomainError> {
    let key =
        checkpoint_key_from_wire(request.key.ok_or_else(|| DomainError::InvalidIdentity {
            kind: "checkpoint key".to_owned(),
            reason: "key is required".to_owned(),
        })?)?;
    Ok((
        key,
        (request.route_group_id != 0)
            .then(|| GroupId::new(request.route_group_id))
            .transpose()?,
        (request.route_revision != 0).then_some(request.route_revision),
    ))
}

pub type CompareAndSetCheckpointParts = (CheckpointMutation, Option<GroupId>, Option<u64>);

pub fn compare_and_set_checkpoint_from_wire(
    request: v1::CompareAndSetCheckpointRequest,
) -> Result<CompareAndSetCheckpointParts, DomainError> {
    let key =
        checkpoint_key_from_wire(request.key.ok_or_else(|| DomainError::InvalidIdentity {
            kind: "checkpoint key".to_owned(),
            reason: "key is required".to_owned(),
        })?)?;
    let expected = request
        .expected
        .and_then(|expected| expected.value)
        .ok_or_else(|| DomainError::InvalidRange {
            reason: "checkpoint expectation is required".to_owned(),
        })
        .and_then(|expected| match expected {
            v1::checkpoint_expectation::Value::Missing(true) => Ok(CheckpointExpectation::Missing),
            v1::checkpoint_expectation::Value::Missing(false) => Err(DomainError::InvalidRange {
                reason: "checkpoint missing expectation must be true".to_owned(),
            }),
            v1::checkpoint_expectation::Value::Revision(revision) => {
                CheckpointRevision::new(revision).map(CheckpointExpectation::Revision)
            }
        })?;
    let mutation = CheckpointMutation::new(
        mutation_request_id_from_wire(request.request_id.ok_or_else(|| {
            DomainError::InvalidIdentity {
                kind: "mutation request ID".to_owned(),
                reason: "request_id is required".to_owned(),
            }
        })?)?,
        key.clone(),
        expected,
        CommittedCursor::new(
            key.cluster(),
            key.partition(),
            RecordOffset::new(request.candidate_next_offset),
        ),
    )?;
    Ok((
        mutation,
        (request.route_group_id != 0)
            .then(|| GroupId::new(request.route_group_id))
            .transpose()?,
        (request.route_revision != 0).then_some(request.route_revision),
    ))
}

pub fn checkpoint_key_to_wire(key: &CheckpointKey) -> v1::CheckpointKey {
    v1::CheckpointKey {
        cluster_id: key.cluster().to_string(),
        stream_id: key.partition().stream().to_string(),
        partition_id: key.partition().partition().get(),
        consumer_id: key.consumer().to_string(),
    }
}

pub fn checkpoint_key_from_wire(value: v1::CheckpointKey) -> Result<CheckpointKey, DomainError> {
    Ok(CheckpointKey::new(
        value.cluster_id.parse()?,
        PartitionKey::new(
            value.stream_id.parse()?,
            PartitionId::new(value.partition_id),
        ),
        ConsumerId::parse(value.consumer_id)?,
    ))
}

pub fn checkpoint_to_wire(value: &CommittedCheckpoint) -> v1::ConsumerCheckpoint {
    v1::ConsumerCheckpoint {
        key: Some(checkpoint_key_to_wire(value.key())),
        next_offset: value.cursor().next_offset().get(),
        revision: value.revision().get(),
    }
}

pub fn checkpoint_from_wire(
    value: v1::ConsumerCheckpoint,
) -> Result<CommittedCheckpoint, DomainError> {
    let key = checkpoint_key_from_wire(value.key.ok_or_else(|| DomainError::InvalidIdentity {
        kind: "checkpoint key".to_owned(),
        reason: "key is required".to_owned(),
    })?)?;
    Ok(CommittedCheckpoint::new(
        key.clone(),
        CommittedCursor::new(
            key.cluster(),
            key.partition(),
            RecordOffset::new(value.next_offset),
        ),
        CheckpointRevision::new(value.revision)?,
    ))
}

pub fn checkpoint_cas_to_wire(
    value: &CheckpointCasResult,
) -> v1::compare_and_set_checkpoint_response::Result {
    match value {
        CheckpointCasResult::Advanced {
            request,
            previous,
            checkpoint,
        } => v1::compare_and_set_checkpoint_response::Result::Advanced(v1::CheckpointAdvanced {
            request_id: Some(mutation_request_id_to_wire(request)),
            previous: previous.as_ref().map(checkpoint_to_wire),
            checkpoint: Some(checkpoint_to_wire(checkpoint)),
        }),
        CheckpointCasResult::Conflict { request, current } => {
            v1::compare_and_set_checkpoint_response::Result::Conflict(v1::CheckpointConflict {
                request_id: Some(mutation_request_id_to_wire(request)),
                current: current.as_ref().map(checkpoint_to_wire),
            })
        }
    }
}

pub fn checkpoint_cas_from_wire(
    value: v1::compare_and_set_checkpoint_response::Result,
) -> Result<CheckpointCasResult, DomainError> {
    match value {
        v1::compare_and_set_checkpoint_response::Result::Advanced(value) => {
            Ok(CheckpointCasResult::Advanced {
                request: mutation_request_id_from_wire(value.request_id.ok_or_else(|| {
                    DomainError::InvalidIdentity {
                        kind: "mutation request ID".to_owned(),
                        reason: "request_id is required".to_owned(),
                    }
                })?)?,
                previous: value.previous.map(checkpoint_from_wire).transpose()?,
                checkpoint: checkpoint_from_wire(value.checkpoint.ok_or_else(|| {
                    DomainError::InvalidIdentity {
                        kind: "consumer checkpoint".to_owned(),
                        reason: "checkpoint is required".to_owned(),
                    }
                })?)?,
            })
        }
        v1::compare_and_set_checkpoint_response::Result::Conflict(value) => {
            Ok(CheckpointCasResult::Conflict {
                request: mutation_request_id_from_wire(value.request_id.ok_or_else(|| {
                    DomainError::InvalidIdentity {
                        kind: "mutation request ID".to_owned(),
                        reason: "request_id is required".to_owned(),
                    }
                })?)?,
                current: value.current.map(checkpoint_from_wire).transpose()?,
            })
        }
        v1::compare_and_set_checkpoint_response::Result::Error(value) => {
            Err(domain_error_from_wire(value)?)
        }
    }
}

pub fn domain_error_to_wire(error: &DomainError) -> v1::ErrorResult {
    let (group, leader, outcome, request_id, mutation_request_json) = match error {
        DomainError::NotLeader { group, leader } => (
            consensus_group_to_wire(*group) as i32,
            leader.as_ref().map(leader_hint_to_wire),
            v1::RequestOutcome::Unspecified as i32,
            None,
            String::new(),
        ),
        DomainError::QuorumUnavailable {
            group,
            outcome,
            request,
        } => {
            let (request_id, mutation_request_json) = match request {
                Some(AmbiguousRequest::Publish { request }) => {
                    (Some(request_id_to_wire(request)), String::new())
                }
                Some(AmbiguousRequest::Mutation { request }) => (
                    None,
                    serde_json::to_string(request).expect("mutation request IDs serialize"),
                ),
                None => (None, String::new()),
            };
            (
                consensus_group_to_wire(*group) as i32,
                None,
                request_outcome_to_wire(*outcome) as i32,
                request_id,
                mutation_request_json,
            )
        }
        DomainError::PublishOverloaded { .. } => (
            v1::ConsensusGroup::Data as i32,
            None,
            v1::RequestOutcome::DefiniteNoCommit as i32,
            None,
            String::new(),
        ),
        _ => (
            v1::ConsensusGroup::Unspecified as i32,
            None,
            v1::RequestOutcome::Unspecified as i32,
            None,
            String::new(),
        ),
    };
    v1::ErrorResult {
        code: error.code().as_str().to_owned(),
        message: error.to_string(),
        group,
        leader,
        outcome,
        request_id,
        mutation_request_json,
        detail_json: serde_json::to_string(error).expect("domain errors serialize"),
    }
}

pub fn domain_error_from_wire(value: v1::ErrorResult) -> Result<DomainError, DomainError> {
    match value.code.as_str() {
        "invalid_identity" => Ok(DomainError::InvalidIdentity {
            kind: "remote identity".to_owned(),
            reason: value.message,
        }),
        "invalid_name" => Ok(DomainError::InvalidName {
            kind: "remote name".to_owned(),
            reason: value.message,
        }),
        "invalid_range" => Ok(DomainError::InvalidRange {
            reason: value
                .message
                .strip_prefix("invalid committed range: ")
                .unwrap_or(&value.message)
                .to_owned(),
        }),
        "invalid_payload" => Ok(DomainError::InvalidPayload {
            reason: value
                .message
                .strip_prefix("invalid publish payload: ")
                .unwrap_or(&value.message)
                .to_owned(),
        }),
        "not_bootstrapped" => Ok(DomainError::NotBootstrapped),
        "bootstrap_conflict" => Ok(DomainError::BootstrapConflict {
            reason: value.message,
        }),
        "identity_mismatch" => Ok(DomainError::IdentityMismatch {
            reason: value.message,
        }),
        "receipt_conflict" => Ok(DomainError::ReceiptConflict),
        "receipt_expired" => Ok(DomainError::ReceiptExpired),
        "receipt_not_found" => Ok(DomainError::ReceiptNotFound),
        "storage_error" => Ok(DomainError::Storage {
            reason: value.message,
        }),
        "cluster_forming" => Ok(DomainError::ClusterForming),
        "stream_not_found" => Ok(DomainError::StreamNotFound),
        "stream_not_active" => Ok(DomainError::StreamNotActive),
        "stream_name_conflict" => Ok(DomainError::StreamNameConflict),
        "bookmark_not_found" => Ok(DomainError::BookmarkNotFound),
        "bookmark_name_conflict" => Ok(DomainError::BookmarkNameConflict),
        "checkpoint_not_found" => Ok(DomainError::CheckpointNotFound),
        "cursor_expired"
        | "checkpoint_ahead_of_tail"
        | "checkpoint_regression"
        | "replay_lease_not_found"
        | "replay_lease_inactive"
        | "replay_lease_conflict"
        | "replay_lease_range_violation"
        | "replay_lease_lifetime_exhausted"
        | "mutation_conflict"
        | "mutation_receipt_expired"
        | "lease_clock_unavailable"
        | "publish_overloaded"
        | "resource_limit" => decode_domain_error_detail(&value.detail_json, &value.code),
        "stale_route" => Ok(DomainError::StaleRoute),
        "unsupported_operation" => Ok(DomainError::UnsupportedOperation {
            operation: "remote operation".to_owned(),
            available_phase: "unknown".to_owned(),
        }),
        "not_leader" => Ok(DomainError::NotLeader {
            group: consensus_group_from_wire(value.group)?,
            leader: value
                .leader
                .map(|leader| {
                    Ok(LeaderHint::new(
                        NodeId::new(leader.node_id)?,
                        leader.public_uri,
                    ))
                })
                .transpose()?,
        }),
        "quorum_unavailable" => Ok(DomainError::QuorumUnavailable {
            group: consensus_group_from_wire(value.group)?,
            outcome: request_outcome_from_wire(value.outcome)?,
            request: match (value.request_id, value.mutation_request_json.is_empty()) {
                (Some(request), true) => Some(AmbiguousRequest::Publish {
                    request: request_id_from_wire(request)?,
                }),
                (None, false) => Some(AmbiguousRequest::Mutation {
                    request: serde_json::from_str(&value.mutation_request_json).map_err(
                        |error| DomainError::InvalidIdentity {
                            kind: "mutation request ID".to_owned(),
                            reason: error.to_string(),
                        },
                    )?,
                }),
                (None, true) => None,
                (Some(_), false) => {
                    return Err(DomainError::InvalidIdentity {
                        kind: "ambiguous request".to_owned(),
                        reason: "wire error contained two request identities".to_owned(),
                    });
                }
            },
        }),
        _ => Ok(DomainError::Storage {
            reason: value.message,
        }),
    }
}

fn decode_domain_error_detail(
    detail_json: &str,
    expected_code: &str,
) -> Result<DomainError, DomainError> {
    let error = serde_json::from_str::<DomainError>(detail_json).map_err(|error| {
        DomainError::InvalidName {
            kind: "domain error detail".to_owned(),
            reason: error.to_string(),
        }
    })?;
    if error.code().as_str() != expected_code {
        return Err(DomainError::InvalidIdentity {
            kind: "domain error detail".to_owned(),
            reason: "detail code does not match the error envelope".to_owned(),
        });
    }
    Ok(error)
}

pub fn unsupported_publish_to_wire() -> v1::PublishResponse {
    v1::PublishResponse {
        result: Some(v1::publish_response::Result::Unsupported(
            v1::UnsupportedResult {
                code: "unsupported_operation".to_owned(),
                operation: "publish".to_owned(),
                available_phase: "LS02".to_owned(),
                message: "publish is not implemented in LS01".to_owned(),
            },
        )),
    }
}

pub fn security_mode_from_wire(value: i32) -> Result<SecurityMode, DomainError> {
    match v1::SecurityMode::try_from(value).ok() {
        Some(v1::SecurityMode::LocalInsecure) => Ok(SecurityMode::LocalInsecure),
        Some(v1::SecurityMode::Secured) => Ok(SecurityMode::Secured),
        _ => Err(DomainError::InvalidName {
            kind: "security mode".to_owned(),
            reason: format!("unknown protobuf value {value}"),
        }),
    }
}

fn security_mode_to_wire(mode: SecurityMode) -> v1::SecurityMode {
    match mode {
        SecurityMode::LocalInsecure => v1::SecurityMode::LocalInsecure,
        SecurityMode::Secured => v1::SecurityMode::Secured,
    }
}

fn consensus_group_to_wire(group: ConsensusGroup) -> v1::ConsensusGroup {
    match group {
        ConsensusGroup::Control => v1::ConsensusGroup::Control,
        ConsensusGroup::Data => v1::ConsensusGroup::Data,
    }
}

fn consensus_group_from_wire(value: i32) -> Result<ConsensusGroup, DomainError> {
    match v1::ConsensusGroup::try_from(value).ok() {
        Some(v1::ConsensusGroup::Control) => Ok(ConsensusGroup::Control),
        Some(v1::ConsensusGroup::Data) => Ok(ConsensusGroup::Data),
        _ => Err(DomainError::InvalidName {
            kind: "consensus group".to_owned(),
            reason: format!("unknown protobuf value {value}"),
        }),
    }
}

fn request_outcome_to_wire(outcome: RequestOutcome) -> v1::RequestOutcome {
    match outcome {
        RequestOutcome::DefiniteNoCommit => v1::RequestOutcome::DefiniteNoCommit,
        RequestOutcome::AmbiguousCommit => v1::RequestOutcome::AmbiguousCommit,
        RequestOutcome::NotApplicable => v1::RequestOutcome::NotApplicable,
    }
}

fn request_outcome_from_wire(value: i32) -> Result<RequestOutcome, DomainError> {
    match v1::RequestOutcome::try_from(value).ok() {
        Some(v1::RequestOutcome::DefiniteNoCommit) => Ok(RequestOutcome::DefiniteNoCommit),
        Some(v1::RequestOutcome::AmbiguousCommit) => Ok(RequestOutcome::AmbiguousCommit),
        Some(v1::RequestOutcome::NotApplicable) => Ok(RequestOutcome::NotApplicable),
        _ => Err(DomainError::InvalidName {
            kind: "request outcome".to_owned(),
            reason: format!("unknown protobuf value {value}"),
        }),
    }
}

fn leader_hint_to_wire(hint: &LeaderHint) -> v1::LeaderHint {
    v1::LeaderHint {
        node_id: hint.node_id().get(),
        public_uri: hint.public_uri().to_owned(),
    }
}

fn request_id_from_wire(value: v1::ProducerRequestId) -> Result<ProducerRequestId, DomainError> {
    Ok(ProducerRequestId::new(
        PrincipalId::parse(value.principal_id)?,
        value.producer_session_id.parse()?,
        RequestSequence::new(value.sequence),
    ))
}

fn request_id_to_wire(value: &ProducerRequestId) -> v1::ProducerRequestId {
    v1::ProducerRequestId {
        principal_id: value.principal().to_string(),
        producer_session_id: value.session().to_string(),
        sequence: value.sequence().get(),
    }
}

pub fn mutation_request_id_from_wire(
    value: v1::MutationRequestId,
) -> Result<MutationRequestId, DomainError> {
    Ok(MutationRequestId::new(
        PrincipalId::parse(value.principal_id)?,
        value.mutation_session_id.parse::<MutationSessionId>()?,
        RequestSequence::new(value.sequence),
    ))
}

pub fn mutation_request_id_to_wire(value: &MutationRequestId) -> v1::MutationRequestId {
    v1::MutationRequestId {
        principal_id: value.principal().to_string(),
        mutation_session_id: value.session().to_string(),
        sequence: value.sequence().get(),
    }
}

fn validate_uri(kind: &str, value: &str) -> Result<(), DomainError> {
    if !value.starts_with("http://")
        || tonic::transport::Endpoint::from_shared(value.to_owned()).is_err()
    {
        return Err(DomainError::InvalidName {
            kind: kind.to_owned(),
            reason: "expected a valid http:// URI".to_owned(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_conversion_validates_the_boundary() {
        let request = v1::PublishRequest {
            cluster_id: "018f3f7e-5b3b-7c11-98f7-b65ac15f65be".to_owned(),
            stream_id: "018f3f7e-5b3b-7c11-98f7-b65ac15f65bf".to_owned(),
            partition_id: 0,
            request_id: Some(v1::ProducerRequestId {
                principal_id: "local-test".to_owned(),
                producer_session_id: "018f3f7e-5b3b-7c11-98f7-b65ac15f65c0".to_owned(),
                sequence: 1,
            }),
            records: vec![b"probe".to_vec()],
            route_group_id: 0,
            route_revision: 0,
            bookmark_name: String::new(),
        };
        assert_eq!(
            publish_probe_from_wire(request)
                .unwrap()
                .records()
                .first()
                .unwrap(),
            b"probe"
        );
    }

    #[test]
    fn publish_conversion_rejects_invalid_wire_data() {
        let request = v1::PublishRequest {
            cluster_id: "not-a-uuid".to_owned(),
            stream_id: String::new(),
            partition_id: 0,
            request_id: None,
            records: Vec::new(),
            route_group_id: 0,
            route_revision: 0,
            bookmark_name: String::new(),
        };
        assert!(publish_probe_from_wire(request).is_err());
    }

    #[test]
    fn domain_error_round_trip_preserves_client_classification() {
        let errors = [
            DomainError::InvalidRange {
                reason: "past tail".to_owned(),
            },
            DomainError::BookmarkNameConflict,
            DomainError::BookmarkNotFound,
        ];
        for error in errors {
            let decoded = domain_error_from_wire(domain_error_to_wire(&error)).unwrap();
            assert_eq!(decoded.code(), error.code());
        }
    }
}
