use anyhow::{Context, Result, bail, ensure};
use poc_common::{Audit, Batch, MAX_PAYLOAD_BYTES};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::time::{Duration, timeout};

use crate::cli::PeerAdmission;

pub const IO_TIMEOUT: Duration = Duration::from_secs(3);
pub const CLIENT_TIMEOUT: Duration = Duration::from_secs(15);
pub const MAX_FRAME_BYTES: usize = MAX_PAYLOAD_BYTES + 29;
pub const APPEND: u8 = 1;
pub const STATUS: u8 = 2;
pub const REPLICATE: u8 = 3;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Response {
    Ack {
        shard: u32,
        sequence: u64,
    },
    Status {
        ready: bool,
        per_shard: Vec<Audit>,
        #[serde(default)]
        peer_admission: PeerAdmission,
        #[serde(default)]
        replication_delay_ms: u64,
        #[serde(default)]
        peer_queue_capacity: usize,
        #[serde(default)]
        replication_peers: Vec<PeerStatus>,
    },
    Error {
        error: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PeerStatus {
    pub shard: u32,
    pub address: String,
    pub accepting: bool,
}

#[derive(Debug, Serialize)]
pub struct StatusReport {
    pub ready: bool,
    pub per_shard: Vec<Audit>,
    pub peer_admission: PeerAdmission,
    pub replication_delay_ms: u64,
    pub peer_queue_capacity: usize,
    pub replication_peers: Vec<PeerStatus>,
}

pub async fn connect(address: impl ToSocketAddrs) -> Result<TcpStream> {
    let stream = timeout(IO_TIMEOUT, TcpStream::connect(address))
        .await
        .context("TCP connect timed out")??;
    stream.set_nodelay(true)?;
    Ok(stream)
}

pub async fn read_frame(reader: &mut (impl AsyncRead + Unpin)) -> Result<Option<Vec<u8>>> {
    let mut header = [0u8; 4];
    if reader.read(&mut header[..1]).await? == 0 {
        return Ok(None);
    }
    reader
        .read_exact(&mut header[1..])
        .await
        .context("truncated frame length")?;
    let length = u32::from_be_bytes(header) as usize;
    ensure!(
        (1..=MAX_FRAME_BYTES).contains(&length),
        "invalid frame length {length}"
    );
    let mut payload = vec![0u8; length];
    reader
        .read_exact(&mut payload)
        .await
        .context("truncated frame payload")?;
    Ok(Some(payload))
}

pub async fn write_frame(writer: &mut (impl AsyncWrite + Unpin), bytes: &[u8]) -> Result<()> {
    ensure!(
        (1..=MAX_FRAME_BYTES).contains(&bytes.len()),
        "invalid outgoing frame length"
    );
    writer
        .write_all(&(bytes.len() as u32).to_be_bytes())
        .await?;
    writer.write_all(bytes).await?;
    Ok(())
}

pub fn batch_request(kind: u8, batch: &Batch) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(batch.payload.len() + 29);
    bytes.push(kind);
    bytes.extend_from_slice(&batch.encode());
    bytes
}

pub async fn exchange(stream: &mut TcpStream, request: &[u8]) -> Result<Response> {
    write_frame(stream, request).await?;
    let reply = read_frame(stream)
        .await?
        .context("node closed without an acknowledgement")?;
    serde_json::from_slice(&reply).context("invalid node response")
}

pub fn require_ack(response: Response, shard: u32, sequence: u64) -> Result<()> {
    match response {
        Response::Ack {
            shard: actual_shard,
            sequence: actual_sequence,
        } => {
            ensure!(
                (actual_shard, actual_sequence) == (shard, sequence),
                "acknowledgement identifies the wrong batch"
            );
            Ok(())
        }
        Response::Error { error } => bail!("node refused append: {error}"),
        Response::Status { .. } => bail!("expected append acknowledgement, received status"),
    }
}

pub async fn status(address: &str) -> Result<StatusReport> {
    let mut stream = connect(address).await?;
    match timeout(CLIENT_TIMEOUT, exchange(&mut stream, &[STATUS])).await?? {
        Response::Status {
            ready,
            per_shard,
            peer_admission,
            replication_delay_ms,
            peer_queue_capacity,
            replication_peers,
        } => Ok(StatusReport {
            ready,
            per_shard,
            peer_admission,
            replication_delay_ms,
            peer_queue_capacity,
            replication_peers,
        }),
        Response::Error { error } => bail!("status failed: {error}"),
        Response::Ack { .. } => bail!("unexpected acknowledgement for status"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    #[tokio::test]
    async fn framing_round_trips_and_distinguishes_clean_eof() -> Result<()> {
        let (mut sender, mut receiver) = duplex(128);
        write_frame(&mut sender, b"actual\0binary\xff").await?;
        sender.shutdown().await?;
        assert_eq!(
            read_frame(&mut receiver).await?.unwrap(),
            b"actual\0binary\xff"
        );
        assert!(read_frame(&mut receiver).await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn framing_rejects_zero_oversize_and_truncation() -> Result<()> {
        for bytes in [
            0u32.to_be_bytes().to_vec(),
            (MAX_FRAME_BYTES as u32 + 1).to_be_bytes().to_vec(),
            vec![0, 0],
            vec![0, 0, 0, 4, 1, 2],
        ] {
            let (mut sender, mut receiver) = duplex(128);
            sender.write_all(&bytes).await?;
            sender.shutdown().await?;
            assert!(read_frame(&mut receiver).await.is_err());
        }
        let (mut sender, _) = duplex(128);
        assert!(write_frame(&mut sender, &[]).await.is_err());
        Ok(())
    }
}
