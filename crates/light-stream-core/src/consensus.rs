use serde::{Deserialize, Serialize};

use crate::NodeId;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsensusGroup {
    Control,
    Data,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LeaderHint {
    node_id: NodeId,
    public_uri: String,
}

impl LeaderHint {
    pub fn new(node_id: NodeId, public_uri: impl Into<String>) -> Self {
        Self {
            node_id,
            public_uri: public_uri.into(),
        }
    }

    pub const fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn public_uri(&self) -> &str {
        &self.public_uri
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestOutcome {
    DefiniteNoCommit,
    AmbiguousCommit,
    NotApplicable,
}
