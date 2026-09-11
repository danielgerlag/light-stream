use std::{fmt, num::NonZeroU64, str::FromStr};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::DomainError;

macro_rules! uuid_id {
    ($name:ident, $kind:literal) => {
        #[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
        pub struct $name(Uuid);

        impl $name {
            pub const fn from_uuid(value: Uuid) -> Self {
                Self(value)
            }

            pub const fn as_uuid(&self) -> &Uuid {
                &self.0
            }
        }

        impl FromStr for $name {
            type Err = DomainError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Uuid::parse_str(value)
                    .map(Self)
                    .map_err(|error| DomainError::InvalidIdentity {
                        kind: $kind.to_owned(),
                        reason: error.to_string(),
                    })
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

uuid_id!(ClusterId, "cluster ID");
uuid_id!(StreamId, "stream ID");
uuid_id!(CatalogRequestId, "catalog request ID");
uuid_id!(ProducerSessionId, "producer session ID");
uuid_id!(BookmarkId, "bookmark ID");

macro_rules! nonzero_id {
    ($name:ident, $kind:literal) => {
        #[derive(
            Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
        )]
        pub struct $name(NonZeroU64);

        impl $name {
            pub fn new(value: u64) -> Result<Self, DomainError> {
                NonZeroU64::new(value)
                    .map(Self)
                    .ok_or_else(|| DomainError::InvalidIdentity {
                        kind: $kind.to_owned(),
                        reason: "zero is reserved".to_owned(),
                    })
            }

            pub const fn get(self) -> u64 {
                self.0.get()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

nonzero_id!(NodeId, "node ID");
nonzero_id!(GroupId, "group ID");

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct PartitionId(u32);

impl PartitionId {
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct RequestSequence(u64);

impl RequestSequence {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct BookmarkPublicationSequence(u64);

impl BookmarkPublicationSequence {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

macro_rules! bounded_text {
    ($name:ident, $kind:literal, $max:expr) => {
        #[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
        pub struct $name(String);

        impl $name {
            pub fn parse(value: impl Into<String>) -> Result<Self, DomainError> {
                let value = value.into();
                if value.is_empty() {
                    return Err(DomainError::InvalidName {
                        kind: $kind.to_owned(),
                        reason: "value is empty".to_owned(),
                    });
                }
                if value.len() > $max {
                    return Err(DomainError::InvalidName {
                        kind: $kind.to_owned(),
                        reason: format!("value exceeds {} UTF-8 bytes", $max),
                    });
                }
                if value.chars().any(char::is_control) {
                    return Err(DomainError::InvalidName {
                        kind: $kind.to_owned(),
                        reason: "control characters are not allowed".to_owned(),
                    });
                }
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

bounded_text!(PrincipalId, "principal ID", 128);
bounded_text!(ConsumerId, "consumer ID", 128);
bounded_text!(StreamName, "stream name", 255);
bounded_text!(BookmarkName, "bookmark name", 255);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_round_trip_preserves_semantics() {
        let cluster: ClusterId = "018f3f7e-5b3b-7c11-98f7-b65ac15f65bf".parse().unwrap();
        let encoded = serde_json::to_string(&cluster).unwrap();
        let decoded: ClusterId = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, cluster);
    }

    #[test]
    fn checked_identity_constructors_reject_invalid_values() {
        assert!(NodeId::new(0).is_err());
        assert!(PrincipalId::parse("").is_err());
        assert!(BookmarkName::parse("line\nbreak").is_err());
    }
}
