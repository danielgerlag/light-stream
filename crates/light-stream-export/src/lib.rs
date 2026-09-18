mod codec;
mod decode;
mod encode;
mod error;
mod model;
mod validate;

use std::io::{Read, Seek, Write};

pub use error::{ExportWriteError, VerifyError, VisitError};
pub use model::*;

#[cfg(test)]
mod fixtures;

pub fn write_v1<W, S>(
    writer: &mut W,
    document: &ExportDocumentV1,
    source: &mut S,
    limits: &ExportLimits,
) -> Result<light_stream_core::ArtifactIdentity, ExportWriteError<S::Error>>
where
    W: Write + Seek,
    S: DataGroupSourceV1,
{
    encode::write_v1(writer, document, source, limits)
}

pub fn verify<R: Read + Seek>(
    reader: R,
    limits: &ExportLimits,
) -> Result<VerifiedExport<R>, VerifyError> {
    decode::verify(reader, limits)
}

pub fn verify_cancellable<R, C>(
    reader: R,
    limits: &ExportLimits,
    cancelled: C,
) -> Result<VerifiedExport<R>, VerifyError>
where
    R: Read + Seek,
    C: FnMut() -> bool,
{
    decode::verify_cancellable(reader, limits, cancelled)
}

pub fn inspect<R>(verified: &VerifiedExport<R>) -> ExportInspection {
    verified.inspection.clone()
}

#[cfg(test)]
mod tests;
