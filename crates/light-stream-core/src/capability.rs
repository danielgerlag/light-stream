use serde::{Deserialize, Serialize};

use crate::{GroupId, SecurityMode};

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
    phase: NodePhase,
    generation: u64,
    write_readiness: WriteReadiness,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NodePhase {
    Starting,
    Running,
    Draining,
    Stopping,
    Failed,
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(tag = "code", rename_all = "snake_case")]
pub enum ReadinessReason {
    Starting,
    NotBootstrapped,
    Forming,
    Retired,
    GroupLeaderUnknown { group: GroupId },
    GroupAuthorityStale { group: GroupId },
    ProbeUnsupported { group: GroupId },
    SecurityPolicyStale,
    Draining,
    StorageFailure,
}

impl ReadinessReason {
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::NotBootstrapped => "not_bootstrapped",
            Self::Forming => "forming",
            Self::Retired => "retired",
            Self::GroupLeaderUnknown { .. } => "group_leader_unknown",
            Self::GroupAuthorityStale { .. } => "group_authority_stale",
            Self::ProbeUnsupported { .. } => "probe_unsupported",
            Self::SecurityPolicyStale => "security_policy_stale",
            Self::Draining => "draining",
            Self::StorageFailure => "storage_failure",
        }
    }

    pub fn from_parts(code: &str, group: Option<GroupId>) -> Option<Self> {
        match code {
            "starting" if group.is_none() => Some(Self::Starting),
            "not_bootstrapped" if group.is_none() => Some(Self::NotBootstrapped),
            "forming" if group.is_none() => Some(Self::Forming),
            "retired" if group.is_none() => Some(Self::Retired),
            "group_leader_unknown" => Some(Self::GroupLeaderUnknown { group: group? }),
            "group_authority_stale" => Some(Self::GroupAuthorityStale { group: group? }),
            "probe_unsupported" => Some(Self::ProbeUnsupported { group: group? }),
            "security_policy_stale" if group.is_none() => Some(Self::SecurityPolicyStale),
            "draining" if group.is_none() => Some(Self::Draining),
            "storage_failure" if group.is_none() => Some(Self::StorageFailure),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum WriteReadiness {
    Ready,
    NotReady { reasons: Vec<ReadinessReason> },
}

impl WriteReadiness {
    pub const fn is_ready(&self) -> bool {
        matches!(self, Self::Ready)
    }
}

impl HealthStatus {
    pub fn new(ready: bool, revision: impl Into<String>, security_mode: SecurityMode) -> Self {
        Self {
            ready,
            revision: revision.into(),
            security_mode,
            phase: NodePhase::Running,
            generation: 0,
            write_readiness: if ready {
                WriteReadiness::Ready
            } else {
                WriteReadiness::NotReady {
                    reasons: vec![ReadinessReason::Starting],
                }
            },
        }
    }

    pub fn with_operational(
        mut self,
        phase: NodePhase,
        generation: u64,
        write_readiness: WriteReadiness,
    ) -> Self {
        self.phase = phase;
        self.generation = generation;
        self.write_readiness = write_readiness;
        self
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

    pub const fn phase(&self) -> NodePhase {
        self.phase
    }

    pub const fn generation(&self) -> u64 {
        self.generation
    }

    pub const fn write_ready(&self) -> bool {
        self.write_readiness.is_ready()
    }

    pub const fn readiness(&self) -> &WriteReadiness {
        &self.write_readiness
    }

    pub fn readiness_reasons(&self) -> &[ReadinessReason] {
        match &self.write_readiness {
            WriteReadiness::Ready => &[],
            WriteReadiness::NotReady { reasons } => reasons,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readiness_reason_codes_round_trip() {
        let group = GroupId::new(7).unwrap();
        let reasons = [
            ReadinessReason::Starting,
            ReadinessReason::NotBootstrapped,
            ReadinessReason::Forming,
            ReadinessReason::Retired,
            ReadinessReason::GroupLeaderUnknown { group },
            ReadinessReason::GroupAuthorityStale { group },
            ReadinessReason::ProbeUnsupported { group },
            ReadinessReason::SecurityPolicyStale,
            ReadinessReason::Draining,
            ReadinessReason::StorageFailure,
        ];

        for reason in reasons {
            let group = match &reason {
                ReadinessReason::GroupLeaderUnknown { group }
                | ReadinessReason::GroupAuthorityStale { group }
                | ReadinessReason::ProbeUnsupported { group } => Some(*group),
                _ => None,
            };
            assert_eq!(
                ReadinessReason::from_parts(reason.code(), group),
                Some(reason)
            );
        }
    }

    #[test]
    fn readiness_reason_parts_enforce_group_requirements() {
        let group = GroupId::new(7).unwrap();

        for code in [
            "group_leader_unknown",
            "group_authority_stale",
            "probe_unsupported",
        ] {
            assert_eq!(ReadinessReason::from_parts(code, None), None);
        }
        assert_eq!(ReadinessReason::from_parts("starting", Some(group)), None);
        assert_eq!(ReadinessReason::from_parts("unknown", None), None);
    }

    #[test]
    fn health_separates_service_availability_from_write_readiness() {
        let status = HealthStatus::new(true, "revision", SecurityMode::LocalInsecure)
            .with_operational(
                NodePhase::Running,
                7,
                WriteReadiness::NotReady {
                    reasons: vec![ReadinessReason::NotBootstrapped],
                },
            );

        assert!(status.ready());
        assert!(!status.write_ready());
        assert_eq!(status.phase(), NodePhase::Running);
        assert_eq!(status.generation(), 7);
        assert_eq!(
            status.readiness(),
            &WriteReadiness::NotReady {
                reasons: vec![ReadinessReason::NotBootstrapped],
            }
        );
    }
}
