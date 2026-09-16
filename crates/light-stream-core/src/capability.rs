use serde::{Deserialize, Serialize};

use crate::SecurityMode;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Capability {
    Health,
    Bootstrap,
    Publish,
    Fetch,
    Receipt,
    Diagnostics,
    Bookmarks,
    Retention,
    ProtectedReplay,
    ConsumerCheckpoints,
    Security,
}

impl Capability {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Health => "health",
            Self::Bootstrap => "bootstrap",
            Self::Publish => "publish",
            Self::Fetch => "fetch",
            Self::Receipt => "receipt",
            Self::Diagnostics => "diagnostics",
            Self::Bookmarks => "bookmarks",
            Self::Retention => "retention",
            Self::ProtectedReplay => "protected_replay",
            Self::ConsumerCheckpoints => "consumer_checkpoints",
            Self::Security => "security",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "health" => Some(Self::Health),
            "bootstrap" => Some(Self::Bootstrap),
            "publish" => Some(Self::Publish),
            "fetch" => Some(Self::Fetch),
            "receipt" => Some(Self::Receipt),
            "diagnostics" => Some(Self::Diagnostics),
            "bookmarks" => Some(Self::Bookmarks),
            "retention" => Some(Self::Retention),
            "protected_replay" => Some(Self::ProtectedReplay),
            "consumer_checkpoints" => Some(Self::ConsumerCheckpoints),
            "security" => Some(Self::Security),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum CapabilitySupport {
    Available,
    Unsupported { available_phase: String },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CapabilityReport {
    capability: Capability,
    support: CapabilitySupport,
}

impl CapabilityReport {
    pub const fn new(capability: Capability, support: CapabilitySupport) -> Self {
        Self {
            capability,
            support,
        }
    }

    pub const fn capability(&self) -> Capability {
        self.capability
    }

    pub const fn support(&self) -> &CapabilitySupport {
        &self.support
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct HealthStatus {
    ready: bool,
    revision: String,
    security_mode: SecurityMode,
}

impl HealthStatus {
    pub fn new(ready: bool, revision: impl Into<String>, security_mode: SecurityMode) -> Self {
        Self {
            ready,
            revision: revision.into(),
            security_mode,
        }
    }

    pub const fn ready(&self) -> bool {
        self.ready
    }

    pub fn revision(&self) -> &str {
        &self.revision
    }

    pub const fn security_mode(&self) -> SecurityMode {
        self.security_mode
    }
}
