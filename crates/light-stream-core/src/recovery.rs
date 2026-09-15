use serde::{Deserialize, Serialize};

use crate::{GroupId, NodeId};

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
}
