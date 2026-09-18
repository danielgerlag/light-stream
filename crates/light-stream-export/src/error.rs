use std::io;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ExportWriteError<E>
where
    E: std::error::Error + Send + Sync + 'static,
{
    #[error("export output I/O failed")]
    Io(#[from] io::Error),
    #[error("export source failed")]
    Source(#[source] E),
    #[error("export limit exceeded: {limit}")]
    Limit { limit: &'static str },
    #[error("export input is not canonical: {field}")]
    NonCanonical { field: &'static str },
    #[error("export input is inconsistent: {field}")]
    Inconsistent { field: &'static str },
}

#[derive(Debug, Error)]
pub enum VerifyError {
    #[error("export verification cancelled")]
    Cancelled,
    #[error("export input I/O failed")]
    Io(#[from] io::Error),
    #[error("export artifact is truncated")]
    Truncated,
    #[error("export artifact has trailing bytes")]
    TrailingBytes,
    #[error("export artifact has invalid {field}")]
    Invalid { field: &'static str },
    #[error("export artifact has unsupported {field}")]
    Unsupported { field: &'static str },
    #[error("export limit exceeded: {limit}")]
    Limit { limit: &'static str },
    #[error("export artifact is not canonical: {field}")]
    NonCanonical { field: &'static str },
    #[error("export artifact cross-reference mismatch: {field}")]
    Inconsistent { field: &'static str },
    #[error("export digest mismatch: {scope}")]
    Digest { scope: &'static str },
}

#[derive(Debug, Error)]
pub enum VisitError<E> {
    #[error(transparent)]
    Verify(#[from] VerifyError),
    #[error("export section visitor failed")]
    Visitor(E),
}
