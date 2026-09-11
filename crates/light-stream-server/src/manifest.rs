use std::collections::BTreeSet;

use light_stream_core::{
    BootstrapSpec, ClusterId, DEFAULT_MAX_DATA_GROUPS, DEFAULT_MAX_PARTITIONS_PER_STREAM,
    DEFAULT_MAX_STREAMS, DomainError, GroupId, MAX_DATA_GROUPS, MIN_DATA_GROUPS, NodeDescriptor,
    NodeId,
};
use serde::{Deserialize, Serialize};
use tonic::transport::Endpoint;

use light_stream_storage::{
    CONTROL_GROUP_ID, DATA_GROUP_ID, GroupStorageBudget, MIN_GROUP_CACHE_BYTES,
    MIN_GROUP_WRITE_BUFFER_BYTES, STORAGE_FORMAT_VERSION,
};

pub const NODE_MANIFEST_VERSION: u32 = 3;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GroupPoolConfig {
    pub max_data_groups: u16,
    pub max_streams: u32,
    pub max_partitions_per_stream: u32,
    pub rocksdb_cache_bytes: usize,
    pub rocksdb_write_buffer_bytes: usize,
}

impl GroupPoolConfig {
    pub fn try_new(
        max_data_groups: u16,
        max_streams: u32,
        max_partitions_per_stream: u32,
        rocksdb_cache_bytes: usize,
        rocksdb_write_buffer_bytes: usize,
    ) -> Result<Self, DomainError> {
        if !(MIN_DATA_GROUPS..=MAX_DATA_GROUPS).contains(&max_data_groups) {
            return Err(DomainError::ResourceLimit {
                resource: "data_group_slots".to_owned(),
                limit: u64::from(MAX_DATA_GROUPS),
            });
        }
        if max_streams == 0 || max_partitions_per_stream == 0 {
            return Err(DomainError::InvalidRange {
                reason: "catalog limits must be greater than zero".to_owned(),
            });
        }
        GroupStorageBudget::new(
            rocksdb_cache_bytes / (usize::from(max_data_groups) + 1),
            rocksdb_write_buffer_bytes / (usize::from(max_data_groups) + 1),
        )
        .map_err(|error| DomainError::Storage {
            reason: error.to_string(),
        })?;
        Ok(Self {
            max_data_groups,
            max_streams,
            max_partitions_per_stream,
            rocksdb_cache_bytes,
            rocksdb_write_buffer_bytes,
        })
    }
    pub fn data_group_ids(&self) -> Result<Vec<GroupId>, DomainError> {
        (0..self.max_data_groups)
            .map(|slot| GroupId::new(DATA_GROUP_ID + u64::from(slot)))
            .collect()
    }
    pub fn per_group_budget(&self) -> Result<GroupStorageBudget, DomainError> {
        GroupStorageBudget::new(
            self.rocksdb_cache_bytes / (usize::from(self.max_data_groups) + 1),
            self.rocksdb_write_buffer_bytes / (usize::from(self.max_data_groups) + 1),
        )
        .map_err(|error| DomainError::Storage {
            reason: error.to_string(),
        })
    }
}

impl Default for GroupPoolConfig {
    fn default() -> Self {
        Self {
            max_data_groups: DEFAULT_MAX_DATA_GROUPS,
            max_streams: DEFAULT_MAX_STREAMS,
            max_partitions_per_stream: DEFAULT_MAX_PARTITIONS_PER_STREAM,
            rocksdb_cache_bytes: 256 * 1024 * 1024,
            rocksdb_write_buffer_bytes: 128 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FormationSpec {
    pub cluster_id: ClusterId,
    pub bootstrap: BootstrapSpec,
    pub control_group_id: GroupId,
    pub data_group_id: GroupId,
    pub seed_node_id: NodeId,
    pub members: Vec<NodeDescriptor>,
    pub group_pool: GroupPoolConfig,
}

impl FormationSpec {
    pub fn try_new(
        bootstrap: BootstrapSpec,
        seed_node_id: u64,
        members: Vec<NodeDescriptor>,
        group_pool: GroupPoolConfig,
    ) -> Result<Self, DomainError> {
        if members.len() != 3 {
            return Err(DomainError::BootstrapConflict {
                reason: "three-voter bootstrap requires exactly three members".to_owned(),
            });
        }
        let seed_node_id = NodeId::new(seed_node_id)?;
        let mut members = members;
        for member in &members {
            validate_uri("public URI", member.public_uri())?;
            validate_uri("peer URI", member.peer_uri())?;
            if member.public_uri() == member.peer_uri() {
                return Err(DomainError::InvalidName {
                    kind: "node descriptor".to_owned(),
                    reason: "public and peer URIs must differ".to_owned(),
                });
            }
        }
        members.sort_by_key(|member| member.node_id());
        let node_ids = members
            .iter()
            .map(|member| member.node_id())
            .collect::<BTreeSet<_>>();
        let public_uris = members
            .iter()
            .map(|member| member.public_uri())
            .collect::<BTreeSet<_>>();
        let peer_uris = members
            .iter()
            .map(|member| member.peer_uri())
            .collect::<BTreeSet<_>>();
        let all_uris = members
            .iter()
            .flat_map(|member| [member.public_uri(), member.peer_uri()])
            .collect::<BTreeSet<_>>();
        if node_ids.len() != members.len()
            || public_uris.len() != members.len()
            || peer_uris.len() != members.len()
            || all_uris.len() != members.len() * 2
        {
            return Err(DomainError::BootstrapConflict {
                reason: "node IDs and advertised endpoints must be unique".to_owned(),
            });
        }
        if !node_ids.contains(&seed_node_id) {
            return Err(DomainError::BootstrapConflict {
                reason: "seed node is not present in the member set".to_owned(),
            });
        }
        Ok(Self {
            cluster_id: bootstrap.cluster(),
            bootstrap,
            control_group_id: GroupId::new(CONTROL_GROUP_ID)?,
            data_group_id: GroupId::new(DATA_GROUP_ID)?,
            seed_node_id,
            members,
            group_pool,
        })
    }

    pub fn local(&self, node_id: NodeId) -> Option<&NodeDescriptor> {
        self.members
            .iter()
            .find(|member| member.node_id() == node_id)
    }

    pub fn member(&self, node_id: u64) -> Option<&NodeDescriptor> {
        self.members
            .iter()
            .find(|member| member.node_id().get() == node_id)
    }

    pub fn voter_ids(&self) -> BTreeSet<u64> {
        self.members
            .iter()
            .map(|member| member.node_id().get())
            .collect()
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PersistedNodeState {
    Joining,
    Forming,
    Active,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct NodeManifestV2 {
    pub format_version: u32,
    pub local_node_id: NodeId,
    pub formation: FormationSpec,
    pub state: PersistedNodeState,
}

impl NodeManifestV2 {
    pub fn new(local_node_id: NodeId, formation: FormationSpec, state: PersistedNodeState) -> Self {
        Self {
            format_version: NODE_MANIFEST_VERSION,
            local_node_id,
            formation,
            state,
        }
    }

    pub fn validate_local(&self, configured: &NodeDescriptor) -> Result<(), DomainError> {
        if self.format_version != NODE_MANIFEST_VERSION {
            return Err(DomainError::Storage {
                reason: format!("unsupported node manifest version {}", self.format_version),
            });
        }
        let normalized = FormationSpec::try_new(
            self.formation.bootstrap.clone(),
            self.formation.seed_node_id.get(),
            self.formation.members.clone(),
            self.formation.group_pool.clone(),
        )?;
        if normalized != self.formation {
            return Err(DomainError::IdentityMismatch {
                reason: "durable formation contains inconsistent cluster or group identities"
                    .to_owned(),
            });
        }
        let stored = self.formation.local(self.local_node_id).ok_or_else(|| {
            DomainError::IdentityMismatch {
                reason: "manifest local node is absent from its topology".to_owned(),
            }
        })?;
        if stored != configured {
            return Err(DomainError::IdentityMismatch {
                reason: "configured node descriptor conflicts with the durable manifest".to_owned(),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct NodeManifestV1 {
    pub format_version: u32,
    pub cluster_id: ClusterId,
    pub bootstrap: BootstrapSpec,
    pub node_id: u64,
    pub control_group_id: u64,
    pub data_group_id: u64,
    #[serde(default)]
    pub group_pool: GroupPoolConfig,
}

impl NodeManifestV1 {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.format_version != STORAGE_FORMAT_VERSION
            || self.control_group_id != CONTROL_GROUP_ID
            || self.data_group_id != DATA_GROUP_ID
        {
            return Err(DomainError::Storage {
                reason: "unsupported LS02a manifest layout".to_owned(),
            });
        }
        if self.cluster_id != self.bootstrap.cluster() {
            return Err(DomainError::IdentityMismatch {
                reason: "cluster manifest and bootstrap cluster differ".to_owned(),
            });
        }
        if self.group_pool.max_data_groups < MIN_DATA_GROUPS
            || self.group_pool.max_data_groups > MAX_DATA_GROUPS
            || self.group_pool.rocksdb_cache_bytes
                < (usize::from(self.group_pool.max_data_groups) + 1) * MIN_GROUP_CACHE_BYTES
            || self.group_pool.rocksdb_write_buffer_bytes
                < (usize::from(self.group_pool.max_data_groups) + 1) * MIN_GROUP_WRITE_BUFFER_BYTES
        {
            return Err(DomainError::Storage {
                reason: "invalid bounded group pool in standalone manifest".to_owned(),
            });
        }
        Ok(())
    }
}

fn validate_uri(kind: &str, value: &str) -> Result<(), DomainError> {
    if !value.starts_with("http://") {
        return Err(DomainError::InvalidName {
            kind: kind.to_owned(),
            reason: "local-insecure endpoints must use http://".to_owned(),
        });
    }
    Endpoint::from_shared(value.to_owned()).map_err(|error| DomainError::InvalidName {
        kind: kind.to_owned(),
        reason: error.to_string(),
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use light_stream_core::{StreamId, StreamName};
    use uuid::Uuid;

    use super::*;

    #[test]
    fn three_voter_topology_rejects_duplicate_endpoints() {
        let bootstrap = BootstrapSpec::new(
            ClusterId::from_uuid(Uuid::new_v4()),
            StreamId::from_uuid(Uuid::new_v4()),
            StreamName::parse("bootstrap").unwrap(),
        );
        let members = vec![
            NodeDescriptor::new(
                NodeId::new(1).unwrap(),
                "http://127.0.0.1:7101",
                "http://127.0.0.1:7201",
            ),
            NodeDescriptor::new(
                NodeId::new(2).unwrap(),
                "http://127.0.0.1:7101",
                "http://127.0.0.1:7202",
            ),
            NodeDescriptor::new(
                NodeId::new(3).unwrap(),
                "http://127.0.0.1:7103",
                "http://127.0.0.1:7203",
            ),
        ];
        assert!(FormationSpec::try_new(bootstrap, 1, members, GroupPoolConfig::default()).is_err());
    }

    #[test]
    fn durable_manifest_rejects_a_changed_local_descriptor() {
        let bootstrap = BootstrapSpec::new(
            ClusterId::from_uuid(Uuid::new_v4()),
            StreamId::from_uuid(Uuid::new_v4()),
            StreamName::parse("bootstrap").unwrap(),
        );
        let members = vec![
            NodeDescriptor::new(
                NodeId::new(1).unwrap(),
                "http://127.0.0.1:7101",
                "http://127.0.0.1:7201",
            ),
            NodeDescriptor::new(
                NodeId::new(2).unwrap(),
                "http://127.0.0.1:7102",
                "http://127.0.0.1:7202",
            ),
            NodeDescriptor::new(
                NodeId::new(3).unwrap(),
                "http://127.0.0.1:7103",
                "http://127.0.0.1:7203",
            ),
        ];
        let formation =
            FormationSpec::try_new(bootstrap, 1, members, GroupPoolConfig::default()).unwrap();
        let manifest = NodeManifestV2::new(
            NodeId::new(1).unwrap(),
            formation,
            PersistedNodeState::Active,
        );
        let changed = NodeDescriptor::new(
            NodeId::new(1).unwrap(),
            "http://127.0.0.1:7199",
            "http://127.0.0.1:7201",
        );
        assert!(manifest.validate_local(&changed).is_err());
    }
}
