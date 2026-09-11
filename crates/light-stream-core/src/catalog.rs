use serde::{Deserialize, Serialize};

use crate::{CatalogRequestId, ClusterId, DomainError, GroupId, PartitionId, StreamId, StreamName};

pub const DEFAULT_MAX_DATA_GROUPS: u16 = 4;
pub const MIN_DATA_GROUPS: u16 = 1;
pub const MAX_DATA_GROUPS: u16 = 32;
pub const DEFAULT_MAX_STREAMS: u32 = 128;
pub const DEFAULT_MAX_PARTITIONS_PER_STREAM: u32 = 128;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CreateStreamSpec {
    request_id: CatalogRequestId,
    name: StreamName,
    partition_count: u32,
}

impl CreateStreamSpec {
    pub fn new(
        request_id: CatalogRequestId,
        name: StreamName,
        partition_count: u32,
    ) -> Result<Self, DomainError> {
        if partition_count == 0 {
            return Err(DomainError::InvalidRange {
                reason: "stream partition count must be greater than zero".to_owned(),
            });
        }
        Ok(Self {
            request_id,
            name,
            partition_count,
        })
    }
    pub const fn request_id(&self) -> CatalogRequestId {
        self.request_id
    }
    pub const fn name(&self) -> &StreamName {
        &self.name
    }
    pub const fn partition_count(&self) -> u32 {
        self.partition_count
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamLifecycle {
    Preparing,
    Active,
    Deleting,
    Deleted,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PartitionPlacement {
    partition: PartitionId,
    group: GroupId,
}

impl PartitionPlacement {
    pub const fn new(partition: PartitionId, group: GroupId) -> Self {
        Self { partition, group }
    }
    pub const fn partition(&self) -> PartitionId {
        self.partition
    }
    pub const fn group(&self) -> GroupId {
        self.group
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StreamDescriptor {
    cluster: ClusterId,
    stream: StreamId,
    name: StreamName,
    lifecycle: StreamLifecycle,
    placements: Vec<PartitionPlacement>,
    ready_groups: Vec<GroupId>,
    revision: u64,
}

impl StreamDescriptor {
    pub fn new(
        cluster: ClusterId,
        stream: StreamId,
        name: StreamName,
        lifecycle: StreamLifecycle,
        placements: Vec<PartitionPlacement>,
        ready_groups: Vec<GroupId>,
        revision: u64,
    ) -> Self {
        Self {
            cluster,
            stream,
            name,
            lifecycle,
            placements,
            ready_groups,
            revision,
        }
    }
    pub const fn cluster(&self) -> ClusterId {
        self.cluster
    }
    pub const fn stream(&self) -> StreamId {
        self.stream
    }
    pub const fn name(&self) -> &StreamName {
        &self.name
    }
    pub const fn lifecycle(&self) -> StreamLifecycle {
        self.lifecycle
    }
    pub fn placements(&self) -> &[PartitionPlacement] {
        &self.placements
    }
    pub fn ready_groups(&self) -> &[GroupId] {
        &self.ready_groups
    }
    pub const fn revision(&self) -> u64 {
        self.revision
    }
    pub fn placement(&self, partition: PartitionId) -> Option<PartitionPlacement> {
        self.placements
            .iter()
            .copied()
            .find(|value| value.partition == partition)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PartitionRoute {
    cluster: ClusterId,
    stream: StreamId,
    stream_name: StreamName,
    partition: PartitionId,
    group: GroupId,
    route_revision: u64,
}

impl PartitionRoute {
    pub const fn new(
        cluster: ClusterId,
        stream: StreamId,
        stream_name: StreamName,
        partition: PartitionId,
        group: GroupId,
        route_revision: u64,
    ) -> Self {
        Self {
            cluster,
            stream,
            stream_name,
            partition,
            group,
            route_revision,
        }
    }
    pub const fn cluster(&self) -> ClusterId {
        self.cluster
    }
    pub const fn stream(&self) -> StreamId {
        self.stream
    }
    pub const fn stream_name(&self) -> &StreamName {
        &self.stream_name
    }
    pub const fn partition(&self) -> PartitionId {
        self.partition
    }
    pub const fn group(&self) -> GroupId {
        self.group
    }
    pub const fn route_revision(&self) -> u64 {
        self.route_revision
    }
}
