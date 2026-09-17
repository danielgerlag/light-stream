use std::str::FromStr;

use light_stream_core::{
    BookmarkId, BookmarkName, ClusterId, GroupId, NodeId, StreamId, StreamName,
};

use crate::{ExportIdV1, VerifyError};

const MAX_NAME_BYTES: usize = 255;

#[derive(Clone, Copy, Debug)]
pub(crate) struct EncodeLimit;

pub(crate) struct Encoder {
    bytes: Vec<u8>,
    max: u64,
}

impl Encoder {
    pub(crate) fn new(max: u64) -> Self {
        Self {
            bytes: Vec::new(),
            max,
        }
    }

    pub(crate) fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    pub(crate) fn u8(&mut self, value: u8) -> Result<(), EncodeLimit> {
        self.raw(&[value])
    }

    pub(crate) fn u16(&mut self, value: u16) -> Result<(), EncodeLimit> {
        self.raw(&value.to_be_bytes())
    }

    pub(crate) fn u32(&mut self, value: u32) -> Result<(), EncodeLimit> {
        self.raw(&value.to_be_bytes())
    }

    pub(crate) fn u64(&mut self, value: u64) -> Result<(), EncodeLimit> {
        self.raw(&value.to_be_bytes())
    }

    pub(crate) fn cluster(&mut self, value: ClusterId) -> Result<(), EncodeLimit> {
        self.raw(value.as_uuid().as_bytes())
    }

    pub(crate) fn stream(&mut self, value: StreamId) -> Result<(), EncodeLimit> {
        self.raw(value.as_uuid().as_bytes())
    }

    pub(crate) fn bookmark(&mut self, value: BookmarkId) -> Result<(), EncodeLimit> {
        self.raw(value.as_uuid().as_bytes())
    }

    pub(crate) fn export(&mut self, value: ExportIdV1) -> Result<(), EncodeLimit> {
        self.raw(value.as_bytes())
    }

    pub(crate) fn string(&mut self, value: &str) -> Result<(), EncodeLimit> {
        let length = u32::try_from(value.len()).map_err(|_| EncodeLimit)?;
        self.u32(length)?;
        self.raw(value.as_bytes())
    }

    pub(crate) fn bytes(&mut self, value: &[u8]) -> Result<(), EncodeLimit> {
        let length = u64::try_from(value.len()).map_err(|_| EncodeLimit)?;
        self.u64(length)?;
        self.raw(value)
    }

    pub(crate) fn raw(&mut self, value: &[u8]) -> Result<(), EncodeLimit> {
        let current = u64::try_from(self.bytes.len()).map_err(|_| EncodeLimit)?;
        let added = u64::try_from(value.len()).map_err(|_| EncodeLimit)?;
        if current
            .checked_add(added)
            .is_none_or(|length| length > self.max)
        {
            return Err(EncodeLimit);
        }
        self.bytes.extend_from_slice(value);
        Ok(())
    }
}

pub(crate) struct Decoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Decoder<'a> {
    pub(crate) const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    pub(crate) fn finish(self, field: &'static str) -> Result<(), VerifyError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(VerifyError::Invalid { field })
        }
    }

    pub(crate) fn remaining(&self) -> usize {
        self.bytes.len() - self.offset
    }

    pub(crate) fn u8(&mut self) -> Result<u8, VerifyError> {
        Ok(self.take(1)?[0])
    }

    pub(crate) fn u16(&mut self) -> Result<u16, VerifyError> {
        Ok(u16::from_be_bytes(
            self.take(2)?
                .try_into()
                .map_err(|_| VerifyError::Truncated)?,
        ))
    }

    pub(crate) fn u32(&mut self) -> Result<u32, VerifyError> {
        Ok(u32::from_be_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| VerifyError::Truncated)?,
        ))
    }

    pub(crate) fn u64(&mut self) -> Result<u64, VerifyError> {
        Ok(u64::from_be_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| VerifyError::Truncated)?,
        ))
    }

    pub(crate) fn cluster(&mut self) -> Result<ClusterId, VerifyError> {
        parse_uuid(self.take(16)?, "source cluster")
    }

    pub(crate) fn stream(&mut self) -> Result<StreamId, VerifyError> {
        parse_uuid(self.take(16)?, "stream ID")
    }

    pub(crate) fn bookmark(&mut self) -> Result<BookmarkId, VerifyError> {
        parse_uuid(self.take(16)?, "bookmark ID")
    }

    pub(crate) fn export(&mut self) -> Result<ExportIdV1, VerifyError> {
        let bytes = self
            .take(16)?
            .try_into()
            .map_err(|_| VerifyError::Truncated)?;
        Ok(ExportIdV1::from_bytes(bytes))
    }

    pub(crate) fn group(&mut self) -> Result<GroupId, VerifyError> {
        GroupId::new(self.u64()?).map_err(|_| VerifyError::Invalid { field: "group ID" })
    }

    pub(crate) fn node(&mut self) -> Result<NodeId, VerifyError> {
        NodeId::new(self.u64()?).map_err(|_| VerifyError::Invalid { field: "node ID" })
    }

    fn string(&mut self, field: &'static str) -> Result<String, VerifyError> {
        let length =
            usize::try_from(self.u32()?).map_err(|_| VerifyError::Limit { limit: "string" })?;
        if length > MAX_NAME_BYTES {
            return Err(VerifyError::Limit { limit: field });
        }
        let bytes = self.take(length)?;
        String::from_utf8(bytes.to_vec()).map_err(|_| VerifyError::Invalid {
            field: "UTF-8 string",
        })
    }

    pub(crate) fn stream_name(&mut self) -> Result<StreamName, VerifyError> {
        StreamName::parse(self.string("stream name bytes")?).map_err(|_| VerifyError::Invalid {
            field: "stream name",
        })
    }

    pub(crate) fn bookmark_name(&mut self) -> Result<BookmarkName, VerifyError> {
        BookmarkName::parse(self.string("bookmark name bytes")?).map_err(|_| VerifyError::Invalid {
            field: "bookmark name",
        })
    }

    pub(crate) fn take(&mut self, length: usize) -> Result<&'a [u8], VerifyError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(VerifyError::Truncated)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(VerifyError::Truncated)?;
        self.offset = end;
        Ok(value)
    }
}

fn parse_uuid<T>(bytes: &[u8], field: &'static str) -> Result<T, VerifyError>
where
    T: FromStr,
{
    let value: [u8; 16] = bytes.try_into().map_err(|_| VerifyError::Truncated)?;
    let text = format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        value[0],
        value[1],
        value[2],
        value[3],
        value[4],
        value[5],
        value[6],
        value[7],
        value[8],
        value[9],
        value[10],
        value[11],
        value[12],
        value[13],
        value[14],
        value[15]
    );
    text.parse().map_err(|_| VerifyError::Invalid { field })
}
