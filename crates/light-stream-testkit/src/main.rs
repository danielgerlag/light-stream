use std::{path::PathBuf, time::Duration};

use clap::{Parser, Subcommand};
use light_stream_client::{Client, default_probe};
use light_stream_core::{
    ClusterId, GroupId, PartitionId, PartitionKey, PrincipalId, ProducerRequestId,
    ProducerSessionId, PublishBatch, RecordOffset, RequestSequence, StreamId,
};
use light_stream_storage::{
    DEFAULT_RECEIPT_WINDOW, GroupIdentity, GroupKind, GroupStorageBudget, open_data_store,
};
use openraft::{
    EntryPayload,
    storage::{RaftLogReader, RaftLogStorage, RaftStateMachine},
};
use serde_json::json;
use sha2::{Digest, Sha256};

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
    InspectDataGroup {
        #[arg(long)]
        path: PathBuf,
        #[arg(long)]
        cluster_id: String,
        #[arg(long)]
        group_id: u64,
        #[arg(long)]
        stream_id: Option<String>,
        #[arg(long, default_value_t = 0)]
        record_limit: u32,
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
        Command::InspectDataGroup {
            path,
            cluster_id,
            group_id,
            stream_id,
            record_limit,
        } => {
            let cluster = cluster_id
                .parse::<ClusterId>()
                .map_err(|error| error.to_string())?;
            let identity = GroupIdentity::new(
                cluster,
                GroupId::new(group_id).map_err(|error| error.to_string())?,
                GroupKind::Data,
            );
            let handles = open_data_store(
                &path,
                &identity,
                DEFAULT_RECEIPT_WINDOW,
                GroupStorageBudget::new(8 * 1024 * 1024, 4 * 1024 * 1024)
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string())?;
            let records = match stream_id {
                Some(stream_id) => {
                    let stream = stream_id
                        .parse::<StreamId>()
                        .map_err(|error| error.to_string())?;
                    let partition = PartitionKey::new(stream, PartitionId::new(0));
                    let mut offset = RecordOffset::new(0);
                    let mut records = Vec::new();
                    while records.len() < record_limit as usize {
                        let remaining = record_limit as usize - records.len();
                        let page = handles
                            .reader
                            .fetch(
                                cluster,
                                partition,
                                offset,
                                u32::try_from(remaining.min(1024))
                                    .map_err(|error| error.to_string())?,
                            )
                            .map_err(|error| error.to_string())?;
                        if page.records().is_empty() {
                            break;
                        }
                        records.extend(page.records().iter().map(|record| {
                            json!({
                                "offset": record.offset().get(),
                                "bytes": record.payload().len(),
                                "sha256": format!("{:x}", Sha256::digest(record.payload())),
                            })
                        }));
                        offset = page.next_offset();
                    }
                    records
                }
                None => Vec::new(),
            };
            let operational_proof = handles
                .reader
                .operational_proof()
                .map_err(|error| error.to_string())?;
            let mut log = handles.log_store;
            let mut state = handles.state_machine;
            let entries = log
                .get_log_reader()
                .await
                .try_get_log_entries(0..u64::MAX)
                .await
                .map_err(|error| error.to_string())?;
            let (applied, membership) = state
                .applied_state()
                .await
                .map_err(|error| error.to_string())?;
            Ok(json!({
                "command": "inspect-data-group",
                "ok": true,
                "identity": identity,
                "applied": applied,
                "membership": format!("{membership:?}"),
                "operational_proof": operational_proof,
                "records": records,
                "entries": entries.into_iter().map(|entry| {
                    let command = match entry.payload {
                        EntryPayload::Blank => "blank".to_owned(),
                        EntryPayload::Membership(_) => "membership".to_owned(),
                        EntryPayload::Normal(command) => command.to_string(),
                    };
                    json!({
                        "term": entry.log_id.leader_id.term,
                        "leader": entry.log_id.leader_id.node_id,
                        "index": entry.log_id.index,
                        "command": command,
                    })
                }).collect::<Vec<_>>(),
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
