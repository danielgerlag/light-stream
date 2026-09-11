use std::{fs, path::PathBuf, time::Duration};

use clap::{Args as ClapArgs, Parser, Subcommand};
use light_stream_client::{Client, ClientError, default_probe};
use light_stream_core::{
    BookmarkId, BookmarkName, BookmarkPageRequest, BookmarkPublicationSequence, BootstrapSpec,
    CatalogRequestId, ClusterId, CommittedCursor, CreateStreamSpec, DomainError, GroupId,
    NodeDescriptor, NodeId, PartitionId, PartitionKey, PrincipalId, ProducerRequestId,
    ProducerSessionId, PublishBatch, RecordOffset, RequestSequence, StreamBookmarkPageRequest,
    StreamCursorVector, StreamId, StreamName,
};
use serde_json::json;

#[derive(Debug, Parser)]
#[command(
    name = "light-streamctl",
    version,
    about = "Light Stream command line client"
)]
struct Args {
    #[arg(long)]
    endpoint: String,
    #[arg(long = "seed")]
    seeds: Vec<String>,
    #[arg(long, default_value_t = 5000)]
    deadline_ms: u64,
    #[arg(long)]
    no_retry: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Health,
    Capabilities,
    Diagnostics,
    Cluster {
        #[command(subcommand)]
        command: ClusterCommand,
    },
    Stream {
        #[command(subcommand)]
        command: StreamCommand,
    },
    Bookmark {
        #[command(subcommand)]
        command: BookmarkCommand,
    },
    Publish(PublishArgs),
    Fetch(FetchArgs),
    Receipt(ReceiptArgs),
    PublishProbe {
        #[arg(long)]
        payload: String,
    },
}

#[derive(Debug, Subcommand)]
enum ClusterCommand {
    Bootstrap {
        #[arg(long)]
        cluster_id: String,
        #[arg(long)]
        stream_id: String,
        #[arg(long)]
        stream_name: String,
        #[arg(long)]
        seed_node_id: Option<u64>,
        #[arg(long = "member")]
        members: Vec<String>,
    },
}

#[derive(Debug, Subcommand)]
enum StreamCommand {
    Create {
        #[arg(long)]
        cluster_id: String,
        #[arg(long)]
        request_id: String,
        #[arg(long)]
        name: String,
        #[arg(long)]
        partitions: u32,
    },
    Describe {
        #[arg(long)]
        cluster_id: String,
        #[arg(long, conflicts_with = "name")]
        stream_id: Option<String>,
        #[arg(long, conflicts_with = "stream_id")]
        name: Option<String>,
    },
    List {
        #[arg(long)]
        cluster_id: String,
    },
    Delete {
        #[arg(long)]
        cluster_id: String,
        #[arg(long)]
        stream_id: String,
    },
    Route {
        #[arg(long)]
        cluster_id: String,
        #[arg(long, conflicts_with = "name")]
        stream_id: Option<String>,
        #[arg(long, conflicts_with = "stream_id")]
        name: Option<String>,
        #[arg(long)]
        partition: u32,
    },
}

#[derive(Debug, Subcommand)]
enum BookmarkCommand {
    Create {
        #[command(flatten)]
        target: TargetArgs,
        #[arg(long)]
        bookmark_id: String,
        #[arg(long)]
        name: String,
        #[arg(long)]
        offset: u64,
    },
    Resolve {
        #[command(flatten)]
        target: TargetArgs,
        #[arg(long)]
        name: String,
    },
    Delete {
        #[command(flatten)]
        target: TargetArgs,
        #[arg(long)]
        bookmark_id: String,
    },
    List {
        #[command(flatten)]
        target: TargetArgs,
        #[arg(long, default_value_t = 100)]
        limit: u32,
        #[arg(long)]
        publication_ceiling: Option<u64>,
        #[arg(long)]
        before: Option<u64>,
    },
    StreamCreate {
        #[arg(long)]
        cluster_id: String,
        #[arg(long)]
        stream_id: String,
        #[arg(long)]
        bookmark_id: String,
        #[arg(long)]
        name: String,
        #[arg(long, value_parser = parse_stream_position)]
        position: Vec<(u32, u64)>,
    },
    StreamResolve {
        #[arg(long)]
        cluster_id: String,
        #[arg(long)]
        stream_id: String,
        #[arg(long)]
        name: String,
    },
    StreamDelete {
        #[arg(long)]
        cluster_id: String,
        #[arg(long)]
        stream_id: String,
        #[arg(long)]
        bookmark_id: String,
    },
    StreamList {
        #[arg(long)]
        cluster_id: String,
        #[arg(long)]
        stream_id: String,
        #[arg(long, default_value_t = 100)]
        limit: u32,
        #[arg(long)]
        publication_ceiling: Option<u64>,
        #[arg(long)]
        before: Option<u64>,
    },
}

#[derive(Debug, ClapArgs)]
struct TargetArgs {
    #[arg(long)]
    cluster_id: String,
    #[arg(long)]
    stream_id: String,
    #[arg(long, default_value_t = 0)]
    partition: u32,
}

#[derive(Debug, ClapArgs)]
struct ProducerArgs {
    #[arg(long)]
    principal: String,
    #[arg(long)]
    session: String,
    #[arg(long)]
    sequence: u64,
}

#[derive(Debug, ClapArgs)]
struct PublishArgs {
    #[command(flatten)]
    target: TargetArgs,
    #[command(flatten)]
    producer: ProducerArgs,
    #[arg(long, conflicts_with = "file")]
    payload: Option<String>,
    #[arg(long, conflicts_with = "payload")]
    file: Vec<PathBuf>,
    #[arg(long, requires = "route_revision")]
    route_group_id: Option<u64>,
    #[arg(long, requires = "route_group_id")]
    route_revision: Option<u64>,
    #[arg(long)]
    bookmark: Option<String>,
}

#[derive(Debug, ClapArgs)]
struct FetchArgs {
    #[command(flatten)]
    target: TargetArgs,
    #[arg(long, default_value_t = 0)]
    offset: u64,
    #[arg(long, default_value_t = 128)]
    limit: u32,
    #[arg(long, requires = "route_revision")]
    route_group_id: Option<u64>,
    #[arg(long, requires = "route_group_id")]
    route_revision: Option<u64>,
}

#[derive(Debug, ClapArgs)]
struct ReceiptArgs {
    #[command(flatten)]
    target: TargetArgs,
    #[command(flatten)]
    producer: ProducerArgs,
    #[arg(long, requires = "route_revision")]
    route_group_id: Option<u64>,
    #[arg(long, requires = "route_group_id")]
    route_revision: Option<u64>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let result = run(args).await;
    match result {
        Ok(value) => println!(
            "{}",
            serde_json::to_string(&value).expect("JSON value serializes")
        ),
        Err(error) => {
            println!(
                "{}",
                serde_json::to_string(&error_json(&error)).expect("JSON value serializes")
            );
            std::process::exit(error.exit_code());
        }
    }
}

async fn run(args: Args) -> Result<serde_json::Value, ClientError> {
    let endpoint = args.endpoint.clone();
    let seeds = args.seeds.clone();
    let deadline = Duration::from_millis(args.deadline_ms);
    let retry = !args.no_retry;
    match args.command {
        Command::Health => {
            let client =
                Client::connect_with_options(endpoint.clone(), seeds, deadline, retry).await?;
            let health = client.health().await?;
            let capabilities = client.capabilities().await?;
            Ok(json!({
                "command": "health",
                "ok": health.status.ready(),
                "endpoint": endpoint,
                "health": health,
                "capabilities": capabilities,
            }))
        }
        Command::Capabilities => {
            let client =
                Client::connect_with_options(endpoint.clone(), seeds, deadline, retry).await?;
            let capabilities = client.capabilities().await?;
            Ok(json!({
                "command": "capabilities",
                "ok": true,
                "endpoint": endpoint,
                "capabilities": capabilities,
            }))
        }
        Command::Diagnostics => {
            let client =
                Client::connect_with_options(endpoint.clone(), seeds, deadline, retry).await?;
            let diagnostics = client.diagnostics().await?;
            Ok(json!({
                "command": "diagnostics",
                "ok": true,
                "endpoint": endpoint,
                "diagnostics": diagnostics,
            }))
        }
        Command::Cluster {
            command:
                ClusterCommand::Bootstrap {
                    cluster_id,
                    stream_id,
                    stream_name,
                    seed_node_id,
                    members,
                },
        } => {
            let spec = BootstrapSpec::new(
                cluster_id.parse::<ClusterId>()?,
                stream_id.parse::<StreamId>()?,
                StreamName::parse(stream_name)?,
            );
            let client =
                Client::connect_with_options(endpoint.clone(), seeds, deadline, retry).await?;
            let bootstrap = match (seed_node_id, members.is_empty()) {
                (None, true) => client.bootstrap(spec).await?,
                (Some(seed_node_id), false) => {
                    let members = members
                        .into_iter()
                        .map(|member| parse_member(&member))
                        .collect::<Result<Vec<_>, DomainError>>()?;
                    client
                        .bootstrap_three_voter(spec, seed_node_id, &members)
                        .await?
                }
                _ => {
                    return Err(DomainError::InvalidIdentity {
                        kind: "bootstrap topology".to_owned(),
                        reason: "use both --seed-node-id and three --member values, or neither"
                            .to_owned(),
                    }
                    .into());
                }
            };
            Ok(json!({
                "command": "cluster-bootstrap",
                "ok": true,
                "endpoint": endpoint,
                "cluster": bootstrap,
            }))
        }
        Command::Stream { command } => {
            let client =
                Client::connect_with_options(endpoint.clone(), seeds, deadline, retry).await?;
            match command {
                StreamCommand::Create {
                    cluster_id,
                    request_id,
                    name,
                    partitions,
                } => {
                    let stream = client
                        .create_stream(
                            cluster_id.parse()?,
                            CreateStreamSpec::new(
                                request_id.parse::<CatalogRequestId>()?,
                                StreamName::parse(name)?,
                                partitions,
                            )?,
                        )
                        .await?;
                    Ok(
                        json!({"command":"stream-create","ok":true,"endpoint":endpoint,"stream":stream}),
                    )
                }
                StreamCommand::Describe {
                    cluster_id,
                    stream_id,
                    name,
                } => {
                    let name = name.as_deref().map(StreamName::parse).transpose()?;
                    let stream = client
                        .describe_stream(
                            cluster_id.parse()?,
                            stream_id.map(|value| value.parse()).transpose()?,
                            name.as_ref(),
                        )
                        .await?;
                    Ok(
                        json!({"command":"stream-describe","ok":true,"endpoint":endpoint,"stream":stream}),
                    )
                }
                StreamCommand::List { cluster_id } => {
                    let streams = client.list_streams(cluster_id.parse()?).await?;
                    Ok(
                        json!({"command":"stream-list","ok":true,"endpoint":endpoint,"streams":streams}),
                    )
                }
                StreamCommand::Delete {
                    cluster_id,
                    stream_id,
                } => {
                    let stream = client
                        .delete_stream(cluster_id.parse()?, stream_id.parse()?)
                        .await?;
                    Ok(
                        json!({"command":"stream-delete","ok":true,"endpoint":endpoint,"stream":stream}),
                    )
                }
                StreamCommand::Route {
                    cluster_id,
                    stream_id,
                    name,
                    partition,
                } => {
                    let name = name.as_deref().map(StreamName::parse).transpose()?;
                    let route = client
                        .resolve_route(
                            cluster_id.parse()?,
                            stream_id.map(|value| value.parse()).transpose()?,
                            name.as_ref(),
                            PartitionId::new(partition),
                        )
                        .await?;
                    Ok(
                        json!({"command":"stream-route","ok":true,"endpoint":endpoint,"route":route}),
                    )
                }
            }
        }
        Command::Bookmark { command } => {
            let client =
                Client::connect_with_options(endpoint.clone(), seeds, deadline, retry).await?;
            match command {
                BookmarkCommand::Create {
                    target,
                    bookmark_id,
                    name,
                    offset,
                } => {
                    let cluster = target.cluster_id.parse::<ClusterId>()?;
                    let bookmark = client
                        .create_bookmark(
                            cluster,
                            parse_target(&target)?,
                            bookmark_id.parse::<BookmarkId>()?,
                            BookmarkName::parse(name)?,
                            RecordOffset::new(offset),
                        )
                        .await?;
                    Ok(
                        json!({"command":"bookmark-create","ok":true,"endpoint":endpoint,"bookmark":bookmark}),
                    )
                }
                BookmarkCommand::Resolve { target, name } => {
                    let cluster = target.cluster_id.parse::<ClusterId>()?;
                    let bookmark = client
                        .resolve_bookmark(
                            cluster,
                            parse_target(&target)?,
                            BookmarkName::parse(name)?,
                        )
                        .await?;
                    Ok(
                        json!({"command":"bookmark-resolve","ok":true,"endpoint":endpoint,"bookmark":bookmark}),
                    )
                }
                BookmarkCommand::Delete {
                    target,
                    bookmark_id,
                } => {
                    let cluster = target.cluster_id.parse::<ClusterId>()?;
                    let bookmark = client
                        .delete_bookmark(
                            cluster,
                            parse_target(&target)?,
                            bookmark_id.parse::<BookmarkId>()?,
                        )
                        .await?;
                    Ok(
                        json!({"command":"bookmark-delete","ok":true,"endpoint":endpoint,"bookmark":bookmark}),
                    )
                }
                BookmarkCommand::List {
                    target,
                    limit,
                    publication_ceiling,
                    before,
                } => {
                    let cluster = target.cluster_id.parse::<ClusterId>()?;
                    let page = client
                        .list_bookmarks(
                            cluster,
                            BookmarkPageRequest::new(
                                parse_target(&target)?,
                                limit,
                                publication_ceiling.map(BookmarkPublicationSequence::new),
                                before.map(BookmarkPublicationSequence::new),
                            )?,
                        )
                        .await?;
                    Ok(json!({"command":"bookmark-list","ok":true,"endpoint":endpoint,"page":page}))
                }
                BookmarkCommand::StreamCreate {
                    cluster_id,
                    stream_id,
                    bookmark_id,
                    name,
                    position,
                } => {
                    let cluster = cluster_id.parse::<ClusterId>()?;
                    let stream = stream_id.parse::<StreamId>()?;
                    let vector = StreamCursorVector::new(
                        stream,
                        position
                            .into_iter()
                            .map(|(partition, offset)| {
                                CommittedCursor::new(
                                    cluster,
                                    PartitionKey::new(stream, PartitionId::new(partition)),
                                    RecordOffset::new(offset),
                                )
                            })
                            .collect(),
                    )?;
                    let bookmark = client
                        .create_stream_bookmark(
                            cluster,
                            bookmark_id.parse::<BookmarkId>()?,
                            BookmarkName::parse(name)?,
                            vector,
                        )
                        .await?;
                    Ok(
                        json!({"command":"bookmark-stream-create","ok":true,"endpoint":endpoint,"bookmark":bookmark}),
                    )
                }
                BookmarkCommand::StreamResolve {
                    cluster_id,
                    stream_id,
                    name,
                } => {
                    let bookmark = client
                        .resolve_stream_bookmark(
                            cluster_id.parse()?,
                            stream_id.parse()?,
                            BookmarkName::parse(name)?,
                        )
                        .await?;
                    Ok(
                        json!({"command":"bookmark-stream-resolve","ok":true,"endpoint":endpoint,"bookmark":bookmark}),
                    )
                }
                BookmarkCommand::StreamDelete {
                    cluster_id,
                    stream_id,
                    bookmark_id,
                } => {
                    let bookmark = client
                        .delete_stream_bookmark(
                            cluster_id.parse()?,
                            stream_id.parse()?,
                            bookmark_id.parse()?,
                        )
                        .await?;
                    Ok(
                        json!({"command":"bookmark-stream-delete","ok":true,"endpoint":endpoint,"bookmark":bookmark}),
                    )
                }
                BookmarkCommand::StreamList {
                    cluster_id,
                    stream_id,
                    limit,
                    publication_ceiling,
                    before,
                } => {
                    let page = client
                        .list_stream_bookmarks(StreamBookmarkPageRequest::new(
                            cluster_id.parse()?,
                            stream_id.parse()?,
                            limit,
                            publication_ceiling.map(BookmarkPublicationSequence::new),
                            before.map(BookmarkPublicationSequence::new),
                        )?)
                        .await?;
                    Ok(
                        json!({"command":"bookmark-stream-list","ok":true,"endpoint":endpoint,"page":page}),
                    )
                }
            }
        }
        Command::Publish(value) => {
            let partition = parse_target(&value.target)?;
            let request = parse_producer(&value.producer)?;
            let payloads = match (value.payload, value.file) {
                (Some(payload), files) if files.is_empty() => vec![payload.into_bytes()],
                (None, files) if !files.is_empty() => files
                    .into_iter()
                    .map(|path| {
                        fs::read(path).map_err(|error| {
                            ClientError::Domain(DomainError::InvalidPayload {
                                reason: error.to_string(),
                            })
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                (None, _) => {
                    return Err(DomainError::InvalidPayload {
                        reason: "one of --payload or --file is required".to_owned(),
                    }
                    .into());
                }
                (Some(_), _) => unreachable!("clap rejects conflicting payload inputs"),
            };
            let mut batch = PublishBatch::new(
                value.target.cluster_id.parse()?,
                partition,
                request,
                payloads,
            )?;
            if let Some(bookmark) = value.bookmark {
                batch = batch.with_bookmark(BookmarkName::parse(bookmark)?);
            }
            let client =
                Client::connect_with_options(endpoint.clone(), seeds, deadline, retry).await?;
            let receipt = match (value.route_group_id, value.route_revision) {
                (Some(group), Some(revision)) => {
                    client
                        .publish_with_route_hint(batch, GroupId::new(group)?, revision)
                        .await?
                }
                (None, None) => client.publish(batch).await?,
                _ => unreachable!("clap requires both route hint fields"),
            };
            Ok(json!({
                "command": "publish",
                "ok": true,
                "endpoint": endpoint,
                "receipt": receipt,
            }))
        }
        Command::Fetch(value) => {
            let cluster = value.target.cluster_id.parse::<ClusterId>()?;
            let partition = parse_target(&value.target)?;
            let client =
                Client::connect_with_options(endpoint.clone(), seeds, deadline, retry).await?;
            let page = match (value.route_group_id, value.route_revision) {
                (Some(group), Some(revision)) => {
                    client
                        .fetch_with_route_hint(
                            cluster,
                            partition,
                            RecordOffset::new(value.offset),
                            value.limit,
                            GroupId::new(group)?,
                            revision,
                        )
                        .await?
                }
                (None, None) => {
                    client
                        .fetch(
                            cluster,
                            partition,
                            RecordOffset::new(value.offset),
                            value.limit,
                        )
                        .await?
                }
                _ => unreachable!("clap requires both route hint fields"),
            };
            Ok(json!({
                "command": "fetch",
                "ok": true,
                "endpoint": endpoint,
                "page": page,
            }))
        }
        Command::Receipt(value) => {
            let cluster = value.target.cluster_id.parse::<ClusterId>()?;
            let partition = parse_target(&value.target)?;
            let request = parse_producer(&value.producer)?;
            let client =
                Client::connect_with_options(endpoint.clone(), seeds, deadline, retry).await?;
            let receipt = match (value.route_group_id, value.route_revision) {
                (Some(group), Some(revision)) => {
                    client
                        .receipt_with_route_hint(
                            cluster,
                            partition,
                            request,
                            GroupId::new(group)?,
                            revision,
                        )
                        .await?
                }
                (None, None) => client.receipt(cluster, partition, request).await?,
                _ => unreachable!("clap requires both route hint fields"),
            };
            Ok(json!({
                "command": "receipt",
                "ok": true,
                "endpoint": endpoint,
                "receipt": receipt,
            }))
        }
        Command::PublishProbe { payload } => {
            let probe = default_probe(payload.into_bytes())?;
            let client =
                Client::connect_with_options(endpoint.clone(), seeds, deadline, retry).await?;
            let receipt = client.publish_probe(probe).await?;
            Ok(json!({
                "command": "publish-probe",
                "ok": true,
                "endpoint": endpoint,
                "receipt": receipt,
            }))
        }
    }
}

fn parse_target(value: &TargetArgs) -> Result<PartitionKey, DomainError> {
    Ok(PartitionKey::new(
        value.stream_id.parse::<StreamId>()?,
        PartitionId::new(value.partition),
    ))
}

fn parse_producer(value: &ProducerArgs) -> Result<ProducerRequestId, DomainError> {
    Ok(ProducerRequestId::new(
        PrincipalId::parse(&value.principal)?,
        value.session.parse::<ProducerSessionId>()?,
        RequestSequence::new(value.sequence),
    ))
}

fn parse_member(value: &str) -> Result<NodeDescriptor, DomainError> {
    let parts = value.splitn(3, ',').collect::<Vec<_>>();
    if parts.len() != 3 {
        return Err(DomainError::InvalidName {
            kind: "member descriptor".to_owned(),
            reason: "expected NODE_ID,PUBLIC_URI,PEER_URI".to_owned(),
        });
    }
    Ok(NodeDescriptor::new(
        NodeId::new(
            parts[0]
                .parse::<u64>()
                .map_err(|error| DomainError::InvalidIdentity {
                    kind: "node ID".to_owned(),
                    reason: error.to_string(),
                })?,
        )?,
        parts[1],
        parts[2],
    ))
}

fn parse_stream_position(value: &str) -> Result<(u32, u64), String> {
    let (partition, offset) = value
        .split_once(':')
        .ok_or_else(|| "position must be PARTITION:OFFSET".to_owned())?;
    Ok((
        partition
            .parse()
            .map_err(|error| format!("invalid partition: {error}"))?,
        offset
            .parse()
            .map_err(|error| format!("invalid offset: {error}"))?,
    ))
}

fn error_json(error: &ClientError) -> serde_json::Value {
    match error {
        ClientError::Domain(DomainError::UnsupportedOperation {
            operation,
            available_phase,
        }) => json!({
            "command": "publish-probe",
            "ok": false,
            "error": {
                "code": "unsupported_operation",
                "message": error.to_string(),
                "operation": operation,
                "available_phase": available_phase,
            }
        }),
        ClientError::Domain(domain) => json!({
            "command": "request",
            "ok": false,
            "error": {
                "code": domain.code().as_str(),
                "message": domain.to_string(),
                "detail": domain,
            }
        }),
        ClientError::InvalidEndpoint { .. } => json!({
            "ok": false,
            "error": {
                "code": "invalid_endpoint",
                "message": error.to_string(),
            }
        }),
        _ => json!({
            "ok": false,
            "error": {
                "code": "client_error",
                "message": error.to_string(),
            }
        }),
    }
}
