use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Deserializer, Serialize};

use crate::{AdministrationRequestId, DomainError, GroupId, NodeDescriptor, NodeId};

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ClusterTopology {
    revision: u64,
    authorized_nodes: BTreeMap<NodeId, NodeDescriptor>,
    desired_voters: BTreeSet<NodeId>,
}

#[derive(Deserialize)]
struct ClusterTopologyWire {
    revision: u64,
    authorized_nodes: BTreeMap<NodeId, NodeDescriptor>,
    desired_voters: BTreeSet<NodeId>,
}

impl<'de> Deserialize<'de> for ClusterTopology {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ClusterTopologyWire::deserialize(deserializer)?;
        Self::try_new(
            wire.revision,
            wire.authorized_nodes.into_values(),
            wire.desired_voters,
        )
        .map_err(serde::de::Error::custom)
    }
}

impl ClusterTopology {
    pub fn try_new(
        revision: u64,
        authorized_nodes: impl IntoIterator<Item = NodeDescriptor>,
        desired_voters: impl IntoIterator<Item = NodeId>,
    ) -> Result<Self, DomainError> {
        let authorized_nodes = authorized_nodes.into_iter().collect::<Vec<_>>();
        let authorized_count = authorized_nodes.len();
        let authorized_nodes = authorized_nodes
            .into_iter()
            .map(|node| (node.node_id(), node))
            .collect::<BTreeMap<_, _>>();
        let desired_voters = desired_voters.into_iter().collect::<BTreeSet<_>>();
        if authorized_nodes.is_empty() || desired_voters.is_empty() {
            return Err(DomainError::InvalidRange {
                reason: "cluster topology requires authorized nodes and desired voters".to_owned(),
            });
        }
        if authorized_nodes.len() != authorized_count {
            return Err(DomainError::InvalidIdentity {
                kind: "cluster topology".to_owned(),
                reason: "authorized node IDs must be unique".to_owned(),
            });
        }
        let mut endpoints = BTreeSet::new();
        for node in authorized_nodes.values() {
            for endpoint in [node.public_uri(), node.peer_uri()] {
                let uri =
                    endpoint
                        .parse::<http::Uri>()
                        .map_err(|error| DomainError::InvalidName {
                            kind: "node endpoint".to_owned(),
                            reason: error.to_string(),
                        })?;
                if uri.scheme_str() != Some("http") || uri.authority().is_none() {
                    return Err(DomainError::InvalidName {
                        kind: "node endpoint".to_owned(),
                        reason: "endpoint must be an absolute HTTP URI".to_owned(),
                    });
                }
                if !endpoints.insert(endpoint) {
                    return Err(DomainError::InvalidIdentity {
                        kind: "cluster topology".to_owned(),
                        reason: "node endpoints must be globally unique".to_owned(),
                    });
                }
            }
        }
        if !desired_voters
            .iter()
            .all(|node| authorized_nodes.contains_key(node))
        {
            return Err(DomainError::InvalidIdentity {
                kind: "cluster topology".to_owned(),
                reason: "every desired voter must be an authorized node".to_owned(),
            });
        }
        Ok(Self {
            revision,
            authorized_nodes,
            desired_voters,
        })
    }

    pub const fn revision(&self) -> u64 {
        self.revision
    }

    pub fn authorized_nodes(&self) -> &BTreeMap<NodeId, NodeDescriptor> {
        &self.authorized_nodes
    }

    pub fn desired_voters(&self) -> &BTreeSet<NodeId> {
        &self.desired_voters
    }

    pub fn node(&self, id: NodeId) -> Option<&NodeDescriptor> {
        self.authorized_nodes.get(&id)
    }

    pub fn replacement_transition(
        &self,
        expected_revision: u64,
        remove: NodeId,
        add: NodeDescriptor,
    ) -> Result<Self, DomainError> {
        if self.revision != expected_revision {
            return Err(DomainError::StaleRoute);
        }
        if !self.desired_voters.contains(&remove) {
            return Err(DomainError::InvalidIdentity {
                kind: "replacement voter".to_owned(),
                reason: "removed node is not a desired voter".to_owned(),
            });
        }
        if add.node_id() == remove || self.authorized_nodes.contains_key(&add.node_id()) {
            return Err(DomainError::InvalidIdentity {
                kind: "replacement voter".to_owned(),
                reason: "replacement node ID must be new".to_owned(),
            });
        }
        let add_id = add.node_id();
        let mut authorized_nodes = self.authorized_nodes.clone();
        authorized_nodes.insert(add_id, add);
        let mut desired_voters = self.desired_voters.clone();
        desired_voters.remove(&remove);
        desired_voters.insert(add_id);
        Self::try_new(
            self.revision
                .checked_add(1)
                .ok_or_else(|| DomainError::InvalidRange {
                    reason: "cluster topology revision overflow".to_owned(),
                })?,
            authorized_nodes.into_values(),
            desired_voters,
        )
    }

    pub fn replacement_complete(
        &self,
        expected_revision: u64,
        remove: NodeId,
        add: NodeDescriptor,
    ) -> Result<Self, DomainError> {
        if self.revision != expected_revision.saturating_add(1)
            || self.authorized_nodes.get(&add.node_id()) != Some(&add)
            || !self.authorized_nodes.contains_key(&remove)
            || self.desired_voters.contains(&remove)
            || !self.desired_voters.contains(&add.node_id())
        {
            return Err(DomainError::MutationConflict);
        }
        let mut authorized_nodes = self.authorized_nodes.clone();
        authorized_nodes.remove(&remove);
        Self::try_new(
            self.revision
                .checked_add(1)
                .ok_or_else(|| DomainError::InvalidRange {
                    reason: "cluster topology revision overflow".to_owned(),
                })?,
            authorized_nodes.into_values(),
            self.desired_voters.clone(),
        )
    }

    pub fn replacement_abort(
        &self,
        expected_revision: u64,
        remove: NodeId,
        add: NodeDescriptor,
    ) -> Result<Self, DomainError> {
        if self.revision != expected_revision.saturating_add(1)
            || self.authorized_nodes.get(&add.node_id()) != Some(&add)
            || !self.authorized_nodes.contains_key(&remove)
            || self.desired_voters.contains(&remove)
            || !self.desired_voters.contains(&add.node_id())
        {
            return Err(DomainError::MutationConflict);
        }
        let mut authorized_nodes = self.authorized_nodes.clone();
        authorized_nodes.remove(&add.node_id());
        let mut desired_voters = self.desired_voters.clone();
        desired_voters.remove(&add.node_id());
        desired_voters.insert(remove);
        Self::try_new(
            self.revision
                .checked_add(1)
                .ok_or_else(|| DomainError::InvalidRange {
                    reason: "cluster topology revision overflow".to_owned(),
                })?,
            authorized_nodes.into_values(),
            desired_voters,
        )
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AdministrationIntent {
    ReplaceVoter {
        request: AdministrationRequestId,
        expected_topology_revision: u64,
        remove: NodeId,
        add: NodeDescriptor,
    },
    TransferLeader {
        request: AdministrationRequestId,
        group: GroupId,
        target: NodeId,
    },
}

impl AdministrationIntent {
    pub const fn request(&self) -> AdministrationRequestId {
        match self {
            Self::ReplaceVoter { request, .. } | Self::TransferLeader { request, .. } => *request,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum AdministrationLifecycle {
    Pending,
    Complete { completed_topology_revision: u64 },
    Aborted { completed_topology_revision: u64 },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AdministrationOperation {
    intent: AdministrationIntent,
    lifecycle: AdministrationLifecycle,
}

impl AdministrationOperation {
    pub const fn pending(intent: AdministrationIntent) -> Self {
        Self {
            intent,
            lifecycle: AdministrationLifecycle::Pending,
        }
    }

    pub const fn intent(&self) -> &AdministrationIntent {
        &self.intent
    }

    pub const fn lifecycle(&self) -> &AdministrationLifecycle {
        &self.lifecycle
    }

    pub fn completed(self, completed_topology_revision: u64) -> Self {
        Self {
            intent: self.intent,
            lifecycle: AdministrationLifecycle::Complete {
                completed_topology_revision,
            },
        }
    }

    pub fn aborted(self, completed_topology_revision: u64) -> Self {
        Self {
            intent: self.intent,
            lifecycle: AdministrationLifecycle::Aborted {
                completed_topology_revision,
            },
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OperationalProof {
    group: GroupId,
    leader: NodeId,
    term: u64,
    log_index: u64,
}

impl OperationalProof {
    pub const fn new(group: GroupId, leader: NodeId, term: u64, log_index: u64) -> Self {
        Self {
            group,
            leader,
            term,
            log_index,
        }
    }

    pub const fn group(self) -> GroupId {
        self.group
    }

    pub const fn leader(self) -> NodeId {
        self.leader
    }

    pub const fn term(self) -> u64 {
        self.term
    }

    pub const fn log_index(self) -> u64 {
        self.log_index
    }

    pub const fn matches(self, leader: NodeId, term: u64, applied_index: u64) -> bool {
        self.leader.get() == leader.get() && self.term == term && self.log_index <= applied_index
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operational_proof_is_bound_to_one_leader_term_and_applied_index() {
        let proof = OperationalProof::new(GroupId::new(2).unwrap(), NodeId::new(1).unwrap(), 7, 42);
        assert!(proof.matches(NodeId::new(1).unwrap(), 7, 42));
        assert!(!proof.matches(NodeId::new(1).unwrap(), 8, 42));
        assert!(!proof.matches(NodeId::new(2).unwrap(), 7, 42));
        assert!(!proof.matches(NodeId::new(1).unwrap(), 7, 41));
    }

    #[test]
    fn topology_replacement_authorizes_transition_and_keeps_three_voters() {
        let nodes = [1, 2, 3].map(|id| {
            NodeDescriptor::new(
                NodeId::new(id).unwrap(),
                format!("http://127.0.0.1:71{id:02}"),
                format!("http://127.0.0.1:72{id:02}"),
            )
        });
        let topology =
            ClusterTopology::try_new(4, nodes.clone(), nodes.map(|node| node.node_id())).unwrap();
        let replacement = NodeDescriptor::new(
            NodeId::new(4).unwrap(),
            "http://127.0.0.1:7104",
            "http://127.0.0.1:7204",
        );
        let next = topology
            .replacement_transition(4, NodeId::new(3).unwrap(), replacement.clone())
            .unwrap();
        assert_eq!(5, next.revision());
        assert_eq!(4, next.authorized_nodes().len());
        assert_eq!(
            BTreeSet::from([
                NodeId::new(1).unwrap(),
                NodeId::new(2).unwrap(),
                NodeId::new(4).unwrap(),
            ]),
            *next.desired_voters()
        );
        let final_topology = next
            .replacement_complete(4, NodeId::new(3).unwrap(), replacement)
            .unwrap();
        assert_eq!(6, final_topology.revision());
        assert_eq!(3, final_topology.authorized_nodes().len());
        assert!(final_topology.node(NodeId::new(3).unwrap()).is_none());
        let aborted_topology = next
            .replacement_abort(
                4,
                NodeId::new(3).unwrap(),
                NodeDescriptor::new(
                    NodeId::new(4).unwrap(),
                    "http://127.0.0.1:7104",
                    "http://127.0.0.1:7204",
                ),
            )
            .unwrap();
        assert_eq!(6, aborted_topology.revision());
        assert!(aborted_topology.node(NodeId::new(4).unwrap()).is_none());
        assert!(
            aborted_topology
                .desired_voters()
                .contains(&NodeId::new(3).unwrap())
        );
    }

    #[test]
    fn topology_deserialization_rejects_missing_desired_voters() {
        let invalid = serde_json::json!({
            "revision": 1,
            "authorized_nodes": {
                "1": {
                    "node_id": 1,
                    "public_uri": "http://127.0.0.1:7101",
                    "peer_uri": "http://127.0.0.1:7201"
                }
            },
            "desired_voters": []
        });
        assert!(serde_json::from_value::<ClusterTopology>(invalid).is_err());
    }
}
