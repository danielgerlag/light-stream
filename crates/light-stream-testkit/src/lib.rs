use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Attempt {
    pub request_id: String,
    pub payload_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Acknowledgement {
    pub request_id: String,
    pub payload_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Outcome {
    pub request_id: String,
    pub status: OutcomeStatus,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OutcomeStatus {
    Acknowledged,
    Unsupported,
    Rejected,
}

#[derive(Debug, Error)]
pub enum OracleError {
    #[error("failed to read {path}: {source}")]
    Read {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid JSON at {path}:{line}: {source}")]
    Json {
        path: String,
        line: usize,
        #[source]
        source: serde_json::Error,
    },
    #[error("duplicate {kind} request ID {request_id}")]
    Duplicate {
        kind: &'static str,
        request_id: String,
    },
    #[error("acknowledgement {request_id} has no matching attempt")]
    AckWithoutAttempt { request_id: String },
    #[error("acknowledgement {request_id} payload digest differs from the attempt")]
    AckDigestMismatch { request_id: String },
    #[error("acknowledgement {request_id} has no acknowledged outcome")]
    FalseAcknowledgement { request_id: String },
}

pub fn validate_oracle(
    attempts_path: &Path,
    acknowledgements_path: &Path,
    outcomes_path: &Path,
) -> Result<(), OracleError> {
    let attempts = read_json_lines::<Attempt>(attempts_path)?;
    let acknowledgements = read_json_lines::<Acknowledgement>(acknowledgements_path)?;
    let outcomes = read_json_lines::<Outcome>(outcomes_path)?;
    let attempts = unique_map(attempts, "attempt", |item| item.request_id.clone())?;
    let outcomes = unique_map(outcomes, "outcome", |item| item.request_id.clone())?;
    let mut seen_acknowledgements = BTreeSet::new();
    for acknowledgement in acknowledgements {
        if !seen_acknowledgements.insert(acknowledgement.request_id.clone()) {
            return Err(OracleError::Duplicate {
                kind: "acknowledgement",
                request_id: acknowledgement.request_id,
            });
        }
        let attempt = attempts.get(&acknowledgement.request_id).ok_or_else(|| {
            OracleError::AckWithoutAttempt {
                request_id: acknowledgement.request_id.clone(),
            }
        })?;
        if acknowledgement.payload_sha256 != attempt.payload_sha256 {
            return Err(OracleError::AckDigestMismatch {
                request_id: acknowledgement.request_id,
            });
        }
        if outcomes
            .get(&acknowledgement.request_id)
            .map(|value| value.status)
            != Some(OutcomeStatus::Acknowledged)
        {
            return Err(OracleError::FalseAcknowledgement {
                request_id: acknowledgement.request_id,
            });
        }
    }
    Ok(())
}

fn unique_map<T>(
    values: Vec<T>,
    kind: &'static str,
    key: impl Fn(&T) -> String,
) -> Result<BTreeMap<String, T>, OracleError> {
    let mut result = BTreeMap::new();
    for value in values {
        let request_id = key(&value);
        if result.insert(request_id.clone(), value).is_some() {
            return Err(OracleError::Duplicate { kind, request_id });
        }
    }
    Ok(result)
}

fn read_json_lines<T: DeserializeOwned>(path: &Path) -> Result<Vec<T>, OracleError> {
    let file = File::open(path).map_err(|source| OracleError::Read {
        path: path.display().to_string(),
        source,
    })?;
    BufReader::new(file)
        .lines()
        .enumerate()
        .map(|(index, line)| {
            let line = line.map_err(|source| OracleError::Read {
                path: path.display().to_string(),
                source,
            })?;
            serde_json::from_str(&line).map_err(|source| OracleError::Json {
                path: path.display().to_string(),
                line: index + 1,
                source,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    #[test]
    fn false_acknowledgement_fails_the_oracle() {
        let directory = tempfile::tempdir().unwrap();
        let attempts = directory.path().join("attempts.jsonl");
        let acknowledgements = directory.path().join("acks.jsonl");
        let outcomes = directory.path().join("outcomes.jsonl");
        writeln!(
            File::create(&attempts).unwrap(),
            r#"{{"request_id":"request-1","payload_sha256":"abc"}}"#
        )
        .unwrap();
        writeln!(
            File::create(&acknowledgements).unwrap(),
            r#"{{"request_id":"request-1","payload_sha256":"abc"}}"#
        )
        .unwrap();
        writeln!(
            File::create(&outcomes).unwrap(),
            r#"{{"request_id":"request-1","status":"unsupported"}}"#
        )
        .unwrap();
        assert!(matches!(
            validate_oracle(&attempts, &acknowledgements, &outcomes),
            Err(OracleError::FalseAcknowledgement { .. })
        ));
    }
}
