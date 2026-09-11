use anyhow::{Result, bail, ensure};
use serde::{Deserialize, Serialize};

pub const MAX_PAYLOAD_BYTES: usize = 8 * 1024 * 1024;
const HEADER_BYTES: usize = 28;

#[derive(Clone, Debug)]
pub struct Batch {
    pub shard: u32,
    pub sequence: u64,
    pub records: u32,
    pub record_bytes: u32,
    pub payload: Vec<u8>,
}

impl Batch {
    pub fn generate(shard: u32, sequence: u64, records: u32, record_bytes: u32) -> Result<Self> {
        let size = payload_size(records, record_bytes)?;
        let mut payload = vec![0; size];
        fill_payload(shard, sequence, &mut payload);
        Ok(Self {
            shard,
            sequence,
            records,
            record_bytes,
            payload,
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(HEADER_BYTES + self.payload.len());
        bytes.extend_from_slice(b"LSP1");
        bytes.extend_from_slice(&self.shard.to_le_bytes());
        bytes.extend_from_slice(&self.sequence.to_le_bytes());
        bytes.extend_from_slice(&self.records.to_le_bytes());
        bytes.extend_from_slice(&self.record_bytes.to_le_bytes());
        bytes.extend_from_slice(&self.payload_checksum().to_le_bytes());
        bytes.extend_from_slice(&self.payload);
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        ensure!(bytes.len() >= HEADER_BYTES, "truncated batch header");
        ensure!(&bytes[..4] == b"LSP1", "unsupported batch format");
        let shard = u32::from_le_bytes(bytes[4..8].try_into()?);
        let sequence = u64::from_le_bytes(bytes[8..16].try_into()?);
        let records = u32::from_le_bytes(bytes[16..20].try_into()?);
        let record_bytes = u32::from_le_bytes(bytes[20..24].try_into()?);
        let checksum = u32::from_le_bytes(bytes[24..28].try_into()?);
        let size = payload_size(records, record_bytes)?;
        ensure!(
            bytes.len() == HEADER_BYTES + size,
            "batch payload length mismatch"
        );
        let payload = bytes[HEADER_BYTES..].to_vec();
        ensure!(
            crc32fast::hash(&payload) == checksum,
            "batch checksum mismatch"
        );
        Ok(Self {
            shard,
            sequence,
            records,
            record_bytes,
            payload,
        })
    }

    pub fn validate_payload(&self) -> Result<()> {
        ensure!(
            self.payload.len() == payload_size(self.records, self.record_bytes)?,
            "batch payload length mismatch"
        );
        let mut expected = vec![0; self.payload.len()];
        fill_payload(self.shard, self.sequence, &mut expected);
        ensure!(
            self.payload == expected,
            "persisted payload differs from generated input"
        );
        Ok(())
    }

    pub fn payload_checksum(&self) -> u32 {
        crc32fast::hash(&self.payload)
    }
}

fn payload_size(records: u32, record_bytes: u32) -> Result<usize> {
    ensure!(
        records > 0 && record_bytes > 0,
        "record dimensions must be positive"
    );
    let size = u64::from(records) * u64::from(record_bytes);
    if size > MAX_PAYLOAD_BYTES as u64 {
        bail!("batch exceeds {} bytes", MAX_PAYLOAD_BYTES);
    }
    Ok(usize::try_from(size)?)
}

fn fill_payload(shard: u32, sequence: u64, output: &mut [u8]) {
    let mut state = sequence
        .wrapping_add(0x9e3779b97f4a7c15)
        .wrapping_add(u64::from(shard).wrapping_mul(0xd1b54a32d192ed03));
    for chunk in output.chunks_mut(8) {
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        let bytes = state.wrapping_mul(0x2545f4914f6cdd1d).to_le_bytes();
        chunk.copy_from_slice(&bytes[..chunk.len()]);
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Bookmark {
    pub sequence: u64,
    pub next_record: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Audit {
    pub batches: u64,
    pub records: u64,
    pub payload_bytes: u64,
    pub digest: u64,
    pub first_sequence: Option<u64>,
    pub last_sequence: Option<u64>,
}

impl Audit {
    pub fn observe(&mut self, batch: &Batch) {
        self.batches += 1;
        self.records += u64::from(batch.records);
        self.payload_bytes += batch.payload.len() as u64;
        self.digest = self.digest.rotate_left(7)
            ^ u64::from(batch.payload_checksum())
            ^ batch.sequence.wrapping_mul(0x9e3779b97f4a7c15)
            ^ u64::from(batch.shard);
        self.first_sequence.get_or_insert(batch.sequence);
        self.last_sequence = Some(batch.sequence);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_and_actual_payload_validation() -> Result<()> {
        let batch = Batch::generate(3, 42, 128, 1024)?;
        let restored = Batch::decode(&batch.encode())?;
        restored.validate_payload()?;
        assert_eq!(batch.payload, restored.payload);
        assert_eq!(batch.sequence, restored.sequence);
        Ok(())
    }

    #[test]
    fn rejects_corruption_and_invalid_lengths() -> Result<()> {
        let mut bytes = Batch::generate(0, 0, 1, 64)?.encode();
        bytes[28] ^= 1;
        assert!(Batch::decode(&bytes).is_err());
        assert!(Batch::decode(&bytes[..12]).is_err());
        assert!(Batch::generate(0, 0, 0, 64).is_err());
        assert!(Batch::generate(0, 0, u32::MAX, u32::MAX).is_err());
        Ok(())
    }

    #[test]
    fn digest_detects_order_and_shard_changes() -> Result<()> {
        let a = Batch::generate(0, 0, 1, 17)?;
        let b = Batch::generate(0, 1, 1, 17)?;
        let mut forward = Audit::default();
        forward.observe(&a);
        forward.observe(&b);
        let mut reverse = Audit::default();
        reverse.observe(&b);
        reverse.observe(&a);
        assert_ne!(forward.digest, reverse.digest);
        assert_eq!(forward.payload_bytes, 34);
        assert_ne!(a.payload, Batch::generate(1, 0, 1, 17)?.payload);
        Ok(())
    }
}
