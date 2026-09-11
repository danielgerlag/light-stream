use serde::{Deserialize, Serialize};

use crate::{ClusterId, GroupId, NodeId, StreamId, StreamName};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BootstrapSpec {
    cluster: ClusterId,
    stream: StreamId,
    stream_name: StreamName,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct NodeDescriptor {
    node_id: NodeId,
    public_uri: String,
    peer_uri: String,
}

impl NodeDescriptor {
    pub fn new(
        node_id: NodeId,
        public_uri: impl Into<String>,
        peer_uri: impl Into<String>,
    ) -> Self {
        Self {
            node_id,
            public_uri: public_uri.into(),
            peer_uri: peer_uri.into(),
        }
    }

    pub const fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn public_uri(&self) -> &str {
        &self.public_uri
    }

    pub fn peer_uri(&self) -> &str {
        &self.peer_uri
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum BootstrapTopology {
    Standalone,
    ThreeVoter {
        seed_node_id: NodeId,
        members: Vec<NodeDescriptor>,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BootstrapCommand {
    spec: BootstrapSpec,
    topology: BootstrapTopology,
}

impl BootstrapCommand {
    pub const fn standalone(spec: BootstrapSpec) -> Self {
        Self {
            spec,
            topology: BootstrapTopology::Standalone,
        }
    }

    pub fn three_voter(
        spec: BootstrapSpec,
        seed_node_id: NodeId,
        members: Vec<NodeDescriptor>,
    ) -> Self {
        Self {
            spec,
            topology: BootstrapTopology::ThreeVoter {
                seed_node_id,
                members,
            },
        }
    }

    pub const fn spec(&self) -> &BootstrapSpec {
        &self.spec
    }

    pub const fn topology(&self) -> &BootstrapTopology {
        &self.topology
    }
}

impl BootstrapSpec {
    pub const fn new(cluster: ClusterId, stream: StreamId, stream_name: StreamName) -> Self {
        Self {
            cluster,
            stream,
            stream_name,
        }
    }

    pub const fn cluster(&self) -> ClusterId {
        self.cluster
    }

    pub const fn stream(&self) -> StreamId {
        self.stream
    }

    pub fn stream_name(&self) -> &StreamName {
        &self.stream_name
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BootstrapResult {
    cluster: ClusterId,
    stream: StreamId,
    stream_name: StreamName,
    control_group: GroupId,
    data_group: GroupId,
}

impl BootstrapResult {
    pub const fn new(
        cluster: ClusterId,
        stream: StreamId,
        stream_name: StreamName,
        control_group: GroupId,
        data_group: GroupId,
    ) -> Self {
        Self {
            cluster,
            stream,
            stream_name,
            control_group,
            data_group,
        }
    }

    pub const fn cluster(&self) -> ClusterId {
        self.cluster
    }

    pub const fn stream(&self) -> StreamId {
        self.stream
    }

    pub fn stream_name(&self) -> &StreamName {
        &self.stream_name
    }

    pub const fn control_group(&self) -> GroupId {
        self.control_group
    }

    pub const fn data_group(&self) -> GroupId {
        self.data_group
    }
}
