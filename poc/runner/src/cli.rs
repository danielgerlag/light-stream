use std::path::PathBuf;

use anyhow::{Result, ensure};
use clap::{Args, Parser, Subcommand, ValueEnum};
use poc_common::MAX_PAYLOAD_BYTES;
use poc_storage::Engine;
use serde::{Deserialize, Serialize};

pub const MAX_SHARDS: u32 = 32;
pub const MAX_SAMPLES: u64 = 1_000_000;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PeerAdmission {
    #[default]
    Block,
    Isolate,
}

#[derive(Debug, Parser)]
#[command(
    name = "stream-poc",
    version,
    about = "Fixed-leader durable append measurement, not Raft or HA"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    Node(NodeOptions),
    Bench(BenchOptions),
    Inspect(InspectOptions),
    Status {
        #[arg(long)]
        address: String,
    },
}

#[derive(Debug, Args)]
pub struct NodeOptions {
    #[arg(long, value_enum)]
    pub engine: Engine,
    #[arg(long)]
    pub dir: PathBuf,
    #[arg(long)]
    pub listen: String,
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..=MAX_SHARDS as i64))]
    pub shards: u32,
    #[arg(long, value_delimiter = ',')]
    pub peers: Vec<String>,
    #[arg(
        long,
        value_enum,
        default_value = "block",
        help = "Full peer queue: block this shard or permanently isolate this shard/peer"
    )]
    pub peer_admission: PeerAdmission,
    #[arg(
        long,
        default_value_t = 0,
        value_parser = clap::value_parser!(u64).range(0..=60_000),
        help = "Follower-only replication acknowledgement delay after durable persistence (0..60000 ms)"
    )]
    pub replication_delay_ms: u64,
}

impl NodeOptions {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            (1..=MAX_SHARDS).contains(&self.shards),
            "invalid shard count"
        );
        ensure!(
            self.peers.is_empty() || self.peers.len() == 2,
            "a leader requires exactly two peers; omit --peers for standalone/follower"
        );
        if self.peers.len() == 2 {
            ensure!(self.peers[0] != self.peers[1], "peers must be distinct");
        }
        ensure!(
            self.peers.is_empty() || self.replication_delay_ms == 0,
            "--replication-delay-ms is follower-only and cannot delay a leader"
        );
        Ok(())
    }
}

#[derive(Debug, Args)]
pub struct BenchOptions {
    #[arg(long)]
    pub address: String,
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..=MAX_SHARDS as i64))]
    pub shards: u32,
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..=MAX_PAYLOAD_BYTES as i64))]
    pub batch_records: u32,
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..=MAX_PAYLOAD_BYTES as i64))]
    pub record_bytes: u32,
    #[arg(long, value_parser = positive_seconds)]
    pub seconds: f64,
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..=MAX_SAMPLES))]
    pub max_batches: u64,
    #[arg(long, default_value_t = 0)]
    pub start_sequence: u64,
}

fn positive_seconds(value: &str) -> std::result::Result<f64, String> {
    let seconds: f64 = value.parse().map_err(|_| "seconds must be a number")?;
    if seconds.is_finite() && seconds > 0.0 && seconds <= 3600.0 {
        Ok(seconds)
    } else {
        Err("seconds must be finite and in (0, 3600]".to_owned())
    }
}

impl BenchOptions {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            (1..=MAX_SHARDS).contains(&self.shards),
            "invalid shard count"
        );
        positive_seconds(&self.seconds.to_string()).map_err(anyhow::Error::msg)?;
        ensure!(
            self.batch_records > 0 && self.record_bytes > 0,
            "record dimensions must be positive"
        );
        ensure!(
            u64::from(self.batch_records) * u64::from(self.record_bytes)
                <= MAX_PAYLOAD_BYTES as u64,
            "batch exceeds {MAX_PAYLOAD_BYTES} payload bytes"
        );
        ensure!(
            self.max_batches > 0 && self.max_batches <= MAX_SAMPLES / u64::from(self.shards),
            "shards * max-batches must be in 1..={MAX_SAMPLES}; raw samples are never dropped"
        );
        ensure!(
            self.start_sequence.checked_add(self.max_batches).is_some(),
            "batch sequence would overflow"
        );
        Ok(())
    }
}

#[derive(Debug, Args)]
pub struct InspectOptions {
    #[arg(long, value_enum)]
    pub engine: Engine,
    #[arg(long)]
    pub dir: PathBuf,
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..=MAX_SHARDS as i64))]
    pub shards: u32,
    #[arg(long)]
    pub retain_from: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bench_args() -> Vec<&'static str> {
        vec![
            "stream-poc",
            "bench",
            "--address",
            "127.0.0.1:1",
            "--shards",
            "1",
            "--batch-records",
            "1",
            "--record-bytes",
            "16",
            "--seconds",
            "0.1",
            "--max-batches",
            "10",
        ]
    }

    #[test]
    fn rejects_invalid_input_and_caps_samples() {
        for value in ["0", "NaN", "inf", "3601"] {
            let mut args = bench_args();
            args[11] = value;
            assert!(Cli::try_parse_from(args).is_err(), "{value}");
        }
        for index in [5, 7, 9, 13] {
            let mut args = bench_args();
            args[index] = "0";
            assert!(Cli::try_parse_from(args).is_err());
        }
        let Command::Bench(mut options) = Cli::try_parse_from(bench_args()).unwrap().command else {
            unreachable!()
        };
        options.shards = 2;
        options.max_batches = MAX_SAMPLES;
        assert!(options.validate().is_err());
        options.max_batches = 10;
        options.batch_records = MAX_PAYLOAD_BYTES as u32;
        options.record_bytes = 2;
        assert!(options.validate().is_err());
        options.batch_records = 1;
        options.start_sequence = u64::MAX;
        assert!(options.validate().is_err());
    }

    #[test]
    fn peer_admission_defaults_to_block_and_delay_is_follower_only() -> Result<()> {
        let args = [
            "stream-poc",
            "node",
            "--engine",
            "segment",
            "--dir",
            "unused",
            "--listen",
            "127.0.0.1:0",
            "--shards",
            "1",
        ];
        let Command::Node(defaults) = Cli::try_parse_from(args)?.command else {
            unreachable!()
        };
        assert_eq!(defaults.peer_admission, PeerAdmission::Block);
        assert_eq!(defaults.replication_delay_ms, 0);
        let Command::Node(mut follower) = Cli::try_parse_from(args.into_iter().chain([
            "--peer-admission",
            "isolate",
            "--replication-delay-ms",
            "100",
        ]))?
        .command
        else {
            unreachable!()
        };
        assert_eq!(follower.peer_admission, PeerAdmission::Isolate);
        follower.validate()?;
        follower.peers = vec!["127.0.0.1:1".to_owned(), "127.0.0.1:2".to_owned()];
        assert!(follower.validate().is_err());
        follower.replication_delay_ms = 0;
        follower.validate()?;
        assert!(
            Cli::try_parse_from(args.into_iter().chain(["--peer-admission", "retry"])).is_err()
        );
        assert!(
            Cli::try_parse_from(args.into_iter().chain(["--replication-delay-ms", "60001"]))
                .is_err()
        );
        Ok(())
    }
}
