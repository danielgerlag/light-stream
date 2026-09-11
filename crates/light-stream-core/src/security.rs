use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};

use crate::DomainError;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum SecurityMode {
    LocalInsecure,
    Secured,
}

impl FromStr for SecurityMode {
    type Err = DomainError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "local-insecure" => Ok(Self::LocalInsecure),
            "secured" => Ok(Self::Secured),
            _ => Err(DomainError::InvalidName {
                kind: "security mode".to_owned(),
                reason: format!("unknown value {value:?}"),
            }),
        }
    }
}

impl fmt::Display for SecurityMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LocalInsecure => formatter.write_str("local-insecure"),
            Self::Secured => formatter.write_str("secured"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn security_mode_round_trips_through_text_and_json() {
        let mode: SecurityMode = "local-insecure".parse().unwrap();
        assert_eq!(mode.to_string(), "local-insecure");
        let encoded = serde_json::to_string(&mode).unwrap();
        let decoded: SecurityMode = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, mode);
    }
}
