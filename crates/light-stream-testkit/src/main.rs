use std::{path::PathBuf, time::Duration};

use clap::{Parser, Subcommand};
use light_stream_client::{Client, default_probe};
use light_stream_core::{
    ClusterId, PartitionId, PartitionKey, PrincipalId, ProducerRequestId, ProducerSessionId,
    PublishBatch, RecordOffset, RequestSequence, StreamId,
};
use serde_json::json;

#[derive(Debug, Parser)]
#[command(name = "light-stream-testkit", version)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Health {
        #[arg(long)]
        endpoint: String,
    },
    HealthSamples {
        #[arg(long)]
        endpoint: String,
        #[arg(long, default_value_t = 20)]
        count: u32,
    },
    PublishProbe {
        #[arg(long)]
        endpoint: String,
        #[arg(long)]
        payload: String,
    },
    ClientPublish {
        #[arg(long)]
        endpoint: String,
        #[arg(long)]
        cluster_id: String,
        #[arg(long)]
        stream_id: String,
        #[arg(long, default_value_t = 0)]
        partition: u32,
        #[arg(long)]
        principal: String,
        #[arg(long)]
        session: String,
        #[arg(long)]
        sequence: u64,
        #[arg(long)]
        file: PathBuf,
        #[arg(long = "seed")]
        seeds: Vec<String>,
        #[arg(long, default_value_t = 5000)]
        deadline_ms: u64,
        #[arg(long)]
        no_retry: bool,
    },
    ClientFetch {
        #[arg(long)]
        endpoint: String,
        #[arg(long)]
        cluster_id: String,
        #[arg(long)]
        stream_id: String,
        #[arg(long, default_value_t = 0)]
        partition: u32,
        #[arg(long, default_value_t = 0)]
        offset: u64,
        #[arg(long, default_value_t = 128)]
        limit: u32,
        #[arg(long = "seed")]
        seeds: Vec<String>,
        #[arg(long, default_value_t = 5000)]
        deadline_ms: u64,
        #[arg(long)]
        no_retry: bool,
    },
    OracleCheck {
        #[arg(long)]
        attempts: PathBuf,
        #[arg(long)]
        acknowledgements: PathBuf,
        #[arg(long)]
        outcomes: PathBuf,
    },
}

#[tokio::main]
async fn main() {
    let result = run(Args::parse()).await;
    match result {
        Ok(value) => println!(
            "{}",
            serde_json::to_string(&value).expect("JSON value serializes")
        ),
        Err(error) => {
            println!(
                "{}",
                serde_json::to_string(&json!({
                    "ok": false,
                    "error": error,
                }))
                .expect("JSON value serializes")
            );
            std::process::exit(1);
        }
    }
}

async fn run(args: Args) -> Result<serde_json::Value, String> {
    match args.command {
        Command::Health { endpoint } => {
            let client = Client::connect(endpoint.clone())
                .await
                .map_err(|error| error.to_string())?;
            let health = client.health().await.map_err(|error| error.to_string())?;
            Ok(json!({
                "command": "health",
                "entrypoint": "rust-client",
                "endpoint": endpoint,
                "ok": health.status.ready(),
                "health": health,
            }))
        }
        Command::HealthSamples { endpoint, count } => {
            if count == 0 || count > 10_000 {
                return Err("count must be between 1 and 10000".to_owned());
            }
            let client = Client::connect(endpoint.clone())
                .await
                .map_err(|error| error.to_string())?;
            let mut samples_ms = Vec::with_capacity(count as usize);
            let mut health = None;
            for _ in 0..count {
                let started = std::time::Instant::now();
                health = Some(client.health().await.map_err(|error| error.to_string())?);
                samples_ms.push(started.elapsed().as_secs_f64() * 1000.0);
            }
            Ok(json!({
                "command": "health-samples",
                "entrypoint": "rust-client",
                "endpoint": endpoint,
                "ok": true,
                "health": health,
                "samples_ms": samples_ms,
            }))
        }
        Command::PublishProbe { endpoint, payload } => {
            let client = Client::connect(endpoint.clone())
                .await
                .map_err(|error| error.to_string())?;
            match client
                .publish_probe(
                    default_probe(payload.into_bytes()).map_err(|error| error.to_string())?,
                )
                .await
            {
                Ok(receipt) => Ok(json!({
                    "command": "publish-probe",
                    "entrypoint": "rust-client",
                    "endpoint": endpoint,
                    "ok": true,
                    "receipt": receipt,
                })),
                Err(error) => Err(error.to_string()),
            }
        }
        Command::ClientPublish {
            endpoint,
            cluster_id,
            stream_id,
            partition,
            principal,
            session,
            sequence,
            file,
            seeds,
            deadline_ms,
            no_retry,
        } => {
            let client = Client::connect_with_options(
                endpoint.clone(),
                seeds,
                Duration::from_millis(deadline_ms),
                !no_retry,
            )
            .await
            .map_err(|error| error.to_string())?;
            let receipt = client
                .publish(
                    PublishBatch::new(
                        cluster_id
                            .parse::<ClusterId>()
                            .map_err(|error| error.to_string())?,
                        PartitionKey::new(
                            stream_id
                                .parse::<StreamId>()
                                .map_err(|error| error.to_string())?,
                            PartitionId::new(partition),
                        ),
                        ProducerRequestId::new(
                            PrincipalId::parse(principal).map_err(|error| error.to_string())?,
                            session
                                .parse::<ProducerSessionId>()
                                .map_err(|error| error.to_string())?,
                            RequestSequence::new(sequence),
                        ),
                        vec![std::fs::read(file).map_err(|error| error.to_string())?],
                    )
                    .map_err(|error| error.to_string())?,
                )
                .await
                .map_err(|error| error.to_string())?;
            Ok(json!({
                "command": "client-publish",
                "entrypoint": "rust-client",
                "endpoint": endpoint,
                "ok": true,
                "receipt": receipt,
            }))
        }
        Command::ClientFetch {
            endpoint,
            cluster_id,
            stream_id,
            partition,
            offset,
            limit,
            seeds,
            deadline_ms,
            no_retry,
        } => {
            let client = Client::connect_with_options(
                endpoint.clone(),
                seeds,
                Duration::from_millis(deadline_ms),
                !no_retry,
            )
            .await
            .map_err(|error| error.to_string())?;
            let page = client
                .fetch(
                    cluster_id
                        .parse::<ClusterId>()
                        .map_err(|error| error.to_string())?,
                    PartitionKey::new(
                        stream_id
                            .parse::<StreamId>()
                            .map_err(|error| error.to_string())?,
                        PartitionId::new(partition),
                    ),
                    RecordOffset::new(offset),
                    limit,
                )
                .await
                .map_err(|error| error.to_string())?;
            Ok(json!({
                "command": "client-fetch",
                "entrypoint": "rust-client",
                "endpoint": endpoint,
                "ok": true,
                "page": page,
            }))
        }
        Command::OracleCheck {
            attempts,
            acknowledgements,
            outcomes,
        } => {
            light_stream_testkit::validate_oracle(&attempts, &acknowledgements, &outcomes)
                .map_err(|error| error.to_string())?;
            Ok(json!({
                "command": "oracle-check",
                "ok": true,
            }))
        }
    }
}
