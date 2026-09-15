use std::{fs, path::PathBuf, time::Duration};

use clap::{Args as ClapArgs, Parser, Subcommand};
use light_stream_client::{
    Cancellation, Client, ClientError, Interruption, PublishOptions, default_probe,
};
use light_stream_core::{
    BookmarkId, BookmarkName, BookmarkPageRequest, BookmarkPublicationSequence, BootstrapSpec,
    ByteLimit, CatalogRequestId, CheckpointExpectation, CheckpointKey, CheckpointMutation,
    CheckpointRevision, ClusterId, CommittedCursor, ConsumerId, CreateStreamSpec, DomainError,
    GroupId, LeaseDuration, LeaseRelease, LeaseRenewal, MutationRequestId, MutationSessionId,
    NodeDescriptor, NodeId, PartitionId, PartitionKey, PrincipalId, ProducerRequestId,
    ProducerSessionId, PublishBatch, RecordOffset, ReplayLeaseId, ReplayLeaseRequest, ReplayRange,
    RequestSequence, RetentionRequest, StreamBookmarkPageRequest, StreamCursorVector, StreamId,
    StreamName,
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
    Maintenance {
        #[command(subcommand)]
        command: MaintenanceCommand,
    },
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
    Retention {
        #[command(subcommand)]
        command: RetentionCommand,
    },
    Replay {
        #[command(subcommand)]
        command: ReplayCommand,
    },
    Checkpoint {
        #[command(subcommand)]
        command: CheckpointCommand,
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
enum MaintenanceCommand {
    Snapshot {
        #[arg(long)]
        cluster_id: String,
        #[arg(long)]
        group_id: u64,
        #[arg(long)]
        purge: bool,
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
    ReplaceVoter {
        #[arg(long)]
        cluster_id: String,
        #[arg(long)]
        request_id: String,
        #[arg(long)]
        expected_topology_revision: u64,
        #[arg(long)]
        remove_node_id: u64,
        #[arg(long)]
        add: String,
    },
    TransferLeader {
        #[arg(long)]
        cluster_id: String,
        #[arg(long)]
        request_id: String,
        #[arg(long)]
        group_id: u64,
        #[arg(long)]
        target_node_id: u64,
    },
    Operation {
        #[arg(long)]
        cluster_id: String,
        #[arg(long)]
        request_id: String,
    },
    Abort {
        #[arg(long)]
        cluster_id: String,
        #[arg(long)]
        request_id: String,
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

#[derive(Debug, Subcommand)]
enum RetentionCommand {
    Advance {
        #[command(flatten)]
        target: TargetArgs,
        #[command(flatten)]
        mutation: MutationArgs,
        #[arg(long)]
        floor: u64,
    },
    Status {
        #[command(flatten)]
        target: TargetArgs,
    },
}

#[derive(Debug, Subcommand)]
enum ReplayCommand {
    Admit {
        #[command(flatten)]
        target: TargetArgs,
        #[command(flatten)]
        mutation: MutationArgs,
        #[arg(long)]
        start: u64,
        #[arg(long)]
        end: u64,
        #[arg(long)]
        duration_ms: u64,
        #[arg(long)]
        max_bytes: u64,
    },
    Renew {
        #[command(flatten)]
        target: TargetArgs,
        #[command(flatten)]
        mutation: MutationArgs,
        #[arg(long)]
        lease_id: String,
        #[arg(long)]
        duration_ms: u64,
        #[arg(long, requires = "route_revision")]
        route_group_id: Option<u64>,
        #[arg(long, requires = "route_group_id")]
        route_revision: Option<u64>,
    },
    Release {
        #[command(flatten)]
        target: TargetArgs,
        #[command(flatten)]
        mutation: MutationArgs,
        #[arg(long)]
        lease_id: String,
        #[arg(long, requires = "route_revision")]
        route_group_id: Option<u64>,
        #[arg(long, requires = "route_group_id")]
        route_revision: Option<u64>,
    },
    Status {
        #[command(flatten)]
        target: TargetArgs,
        #[arg(long)]
        lease_id: String,
    },
    Fetch {
        #[command(flatten)]
        target: TargetArgs,
        #[arg(long)]
        lease_id: String,
        #[arg(long)]
        offset: u64,
        #[arg(long, default_value_t = 128)]
        limit: u32,
    },
}

#[derive(Debug, Subcommand)]
enum CheckpointCommand {
    Get {
        #[command(flatten)]
        target: TargetArgs,
        #[arg(long)]
        consumer: String,
    },
    Advance {
        #[command(flatten)]
        target: TargetArgs,
        #[command(flatten)]
        mutation: MutationArgs,
        #[arg(long)]
        consumer: String,
        #[arg(
            long,
            conflicts_with = "expected_revision",
            required_unless_present = "expected_revision"
        )]
        expect_missing: bool,
        #[arg(
            long,
            conflicts_with = "expect_missing",
            required_unless_present = "expect_missing"
        )]
        expected_revision: Option<u64>,
        #[arg(long)]
        offset: u64,
    },
    Fetch {
        #[command(flatten)]
        target: TargetArgs,
        #[arg(long)]
        consumer: String,
        #[arg(long, default_value_t = 128)]
        limit: u32,
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
struct MutationArgs {
    #[arg(long)]
    principal: String,
    #[arg(long)]
    mutation_session: String,
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
    #[arg(long)]
    resolve_receipt: bool,
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
    let cancellation = Cancellation::new();
    let run = run(args, cancellation.clone());
    tokio::pin!(run);
    let result = tokio::select! {
        result = &mut run => result,
        signal = tokio::signal::ctrl_c() => {
            if signal.is_err() {
                run.await
            } else {
                cancellation.cancel();
                tokio::select! {
                    result = &mut run => result,
                    () = tokio::time::sleep(Duration::from_millis(250)) => {
                        Err(ClientError::Interrupted {
                            reason: Interruption::Cancelled,
                            outcome: light_stream_core::RequestOutcome::NotApplicable,
                            request: None,
                        })
                    }
                }
            }
        }
    };
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

async fn run(args: Args, cancellation: Cancellation) -> Result<serde_json::Value, ClientError> {
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
        Command::Maintenance {
            command:
                MaintenanceCommand::Snapshot {
                    cluster_id,
                    group_id,
                    purge,
                },
        } => {
            let client =
                Client::connect_with_options(endpoint.clone(), seeds, deadline, retry).await?;
            let snapshot = client
                .snapshot_group(cluster_id.parse::<ClusterId>()?, group_id, purge)
                .await?;
            Ok(json!({
                "command": "maintenance-snapshot",
                "ok": true,
                "endpoint": endpoint,
                "snapshot": snapshot,
            }))
        }
        Command::Cluster { command } => {
            let client =
                Client::connect_with_options(endpoint.clone(), seeds, deadline, retry).await?;
            match command {
                ClusterCommand::Bootstrap {
                    cluster_id,
                    stream_id,
                    stream_name,
                    seed_node_id,
                    members,
                } => {
                    let spec = BootstrapSpec::new(
                        cluster_id.parse::<ClusterId>()?,
                        stream_id.parse::<StreamId>()?,
                        StreamName::parse(stream_name)?,
                    );
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
                                reason:
                                    "use both --seed-node-id and three --member values, or neither"
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
                ClusterCommand::ReplaceVoter {
                    cluster_id,
                    request_id,
                    expected_topology_revision,
                    remove_node_id,
                    add,
                } => {
                    let add = parse_member(&add)?;
                    let operation = client
                        .replace_voter(
                            cluster_id.parse()?,
                            &request_id,
                            expected_topology_revision,
                            remove_node_id,
                            &add,
                        )
                        .await?;
                    Ok(json!({
                        "command": "cluster-replace-voter",
                        "ok": true,
                        "endpoint": endpoint,
                        "operation": operation,
                    }))
                }
                ClusterCommand::TransferLeader {
                    cluster_id,
                    request_id,
                    group_id,
                    target_node_id,
                } => {
                    let operation = client
                        .transfer_leadership(
                            cluster_id.parse()?,
                            &request_id,
                            group_id,
                            target_node_id,
                        )
                        .await?;
                    Ok(json!({
                        "command": "cluster-transfer-leader",
                        "ok": true,
                        "endpoint": endpoint,
                        "operation": operation,
                    }))
                }
                ClusterCommand::Operation {
                    cluster_id,
                    request_id,
                } => {
                    let operation = client
                        .administration_status(cluster_id.parse()?, &request_id)
                        .await?;
                    Ok(json!({
                        "command": "cluster-operation",
                        "ok": true,
                        "endpoint": endpoint,
                        "operation": operation,
                    }))
                }
                ClusterCommand::Abort {
                    cluster_id,
                    request_id,
                } => {
                    let operation = client
                        .abort_administration(cluster_id.parse()?, &request_id)
                        .await?;
                    Ok(json!({
                        "command": "cluster-abort",
                        "ok": true,
                        "endpoint": endpoint,
                        "operation": operation,
                    }))
                }
            }
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
        Command::Retention { command } => {
            let client =
                Client::connect_with_options(endpoint.clone(), seeds, deadline, retry).await?;
            match command {
                RetentionCommand::Advance {
                    target,
                    mutation,
                    floor,
                } => {
                    let cluster = target.cluster_id.parse::<ClusterId>()?;
                    let result = client
                        .advance_retention(
                            cluster,
                            RetentionRequest::new(
                                parse_mutation(&mutation)?,
                                parse_target(&target)?,
                                RecordOffset::new(floor),
                            ),
                        )
                        .await?;
                    Ok(
                        json!({"command":"retention-advance","ok":true,"endpoint":endpoint,"retention":result}),
                    )
                }
                RetentionCommand::Status { target } => {
                    let cluster = target.cluster_id.parse::<ClusterId>()?;
                    let status = client
                        .retention_status(cluster, parse_target(&target)?)
                        .await?;
                    Ok(
                        json!({"command":"retention-status","ok":true,"endpoint":endpoint,"retention":status}),
                    )
                }
            }
        }
        Command::Replay { command } => {
            let client =
                Client::connect_with_options(endpoint.clone(), seeds, deadline, retry).await?;
            match command {
                ReplayCommand::Admit {
                    target,
                    mutation,
                    start,
                    end,
                    duration_ms,
                    max_bytes,
                } => {
                    let cluster = target.cluster_id.parse::<ClusterId>()?;
                    let partition = parse_target(&target)?;
                    let lease = client
                        .admit_replay_lease(ReplayLeaseRequest::new(
                            parse_mutation(&mutation)?,
                            cluster,
                            ReplayRange::new(
                                partition,
                                RecordOffset::new(start),
                                RecordOffset::new(end),
                            )?,
                            LeaseDuration::from_millis(duration_ms)?,
                            ByteLimit::new(max_bytes)?,
                        ))
                        .await?;
                    Ok(
                        json!({"command":"replay-admit","ok":true,"endpoint":endpoint,"lease":lease}),
                    )
                }
                ReplayCommand::Renew {
                    target,
                    mutation,
                    lease_id,
                    duration_ms,
                    route_group_id,
                    route_revision,
                } => {
                    let cluster = target.cluster_id.parse::<ClusterId>()?;
                    let partition = parse_target(&target)?;
                    let request = LeaseRenewal::new(
                        parse_mutation(&mutation)?,
                        partition,
                        lease_id.parse::<ReplayLeaseId>()?,
                        LeaseDuration::from_millis(duration_ms)?,
                    );
                    let lease = match (route_group_id, route_revision) {
                        (Some(group), Some(revision)) => {
                            client
                                .renew_replay_lease_with_route_hint(
                                    cluster,
                                    request,
                                    GroupId::new(group)?,
                                    revision,
                                )
                                .await?
                        }
                        (None, None) => client.renew_replay_lease(cluster, request).await?,
                        _ => unreachable!("clap requires both route hint fields"),
                    };
                    Ok(
                        json!({"command":"replay-renew","ok":true,"endpoint":endpoint,"lease":lease}),
                    )
                }
                ReplayCommand::Release {
                    target,
                    mutation,
                    lease_id,
                    route_group_id,
                    route_revision,
                } => {
                    let cluster = target.cluster_id.parse::<ClusterId>()?;
                    let partition = parse_target(&target)?;
                    let request = LeaseRelease::new(
                        parse_mutation(&mutation)?,
                        partition,
                        lease_id.parse::<ReplayLeaseId>()?,
                    );
                    let lease = match (route_group_id, route_revision) {
                        (Some(group), Some(revision)) => {
                            client
                                .release_replay_lease_with_route_hint(
                                    cluster,
                                    request,
                                    GroupId::new(group)?,
                                    revision,
                                )
                                .await?
                        }
                        (None, None) => client.release_replay_lease(cluster, request).await?,
                        _ => unreachable!("clap requires both route hint fields"),
                    };
                    Ok(
                        json!({"command":"replay-release","ok":true,"endpoint":endpoint,"lease":lease}),
                    )
                }
                ReplayCommand::Status { target, lease_id } => {
                    let lease = client
                        .replay_lease(
                            target.cluster_id.parse()?,
                            parse_target(&target)?,
                            lease_id.parse()?,
                        )
                        .await?;
                    Ok(
                        json!({"command":"replay-status","ok":true,"endpoint":endpoint,"lease":lease}),
                    )
                }
                ReplayCommand::Fetch {
                    target,
                    lease_id,
                    offset,
                    limit,
                } => {
                    let cluster = target.cluster_id.parse::<ClusterId>()?;
                    let partition = parse_target(&target)?;
                    let lease = client
                        .replay_lease(cluster, partition, lease_id.parse()?)
                        .await?;
                    let page = client
                        .fetch_protected(&lease, RecordOffset::new(offset), limit)
                        .await?;
                    Ok(
                        json!({"command":"replay-fetch","ok":true,"endpoint":endpoint,"page":page,"lease":lease}),
                    )
                }
            }
        }
        Command::Checkpoint { command } => {
            let client =
                Client::connect_with_options(endpoint.clone(), seeds, deadline, retry).await?;
            match command {
                CheckpointCommand::Get { target, consumer } => {
                    let key = CheckpointKey::new(
                        target.cluster_id.parse()?,
                        parse_target(&target)?,
                        ConsumerId::parse(consumer)?,
                    );
                    let checkpoint = client.checkpoint(key).await?;
                    Ok(json!({
                        "command": "checkpoint-get",
                        "ok": true,
                        "endpoint": endpoint,
                        "checkpoint": checkpoint,
                    }))
                }
                CheckpointCommand::Advance {
                    target,
                    mutation,
                    consumer,
                    expect_missing,
                    expected_revision,
                    offset,
                } => {
                    let cluster = target.cluster_id.parse()?;
                    let partition = parse_target(&target)?;
                    let expected = match (expect_missing, expected_revision) {
                        (true, None) => CheckpointExpectation::Missing,
                        (false, Some(revision)) => {
                            CheckpointExpectation::Revision(CheckpointRevision::new(revision)?)
                        }
                        _ => unreachable!("clap requires one checkpoint expectation"),
                    };
                    let update = CheckpointMutation::new(
                        parse_mutation(&mutation)?,
                        CheckpointKey::new(cluster, partition, ConsumerId::parse(consumer)?),
                        expected,
                        CommittedCursor::new(cluster, partition, RecordOffset::new(offset)),
                    )?;
                    let result = client.compare_and_set_checkpoint(update).await?;
                    Ok(json!({
                        "command": "checkpoint-advance",
                        "ok": true,
                        "endpoint": endpoint,
                        "result": result,
                    }))
                }
                CheckpointCommand::Fetch {
                    target,
                    consumer,
                    limit,
                } => {
                    let cluster = target.cluster_id.parse()?;
                    let partition = parse_target(&target)?;
                    let checkpoint = client
                        .checkpoint(CheckpointKey::new(
                            cluster,
                            partition,
                            ConsumerId::parse(consumer)?,
                        ))
                        .await?;
                    let page = client
                        .fetch(cluster, partition, checkpoint.cursor().next_offset(), limit)
                        .await?;
                    Ok(json!({
                        "command": "checkpoint-fetch",
                        "ok": true,
                        "endpoint": endpoint,
                        "checkpoint": checkpoint,
                        "page": page,
                    }))
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
                        .publish_with_route_hint_and_options(
                            batch,
                            GroupId::new(group)?,
                            revision,
                            PublishOptions::default()
                                .cancellation(cancellation)
                                .resolve_ambiguous_receipt(value.resolve_receipt),
                        )
                        .await?
                }
                (None, None) => {
                    client
                        .publish_with(
                            batch,
                            PublishOptions::default()
                                .cancellation(cancellation)
                                .resolve_ambiguous_receipt(value.resolve_receipt),
                        )
                        .await?
                }
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

fn parse_mutation(value: &MutationArgs) -> Result<MutationRequestId, DomainError> {
    Ok(MutationRequestId::new(
        PrincipalId::parse(&value.principal)?,
        value.mutation_session.parse::<MutationSessionId>()?,
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
        ClientError::Domain(DomainError::PublishOverloaded { resource, limit }) => json!({
            "command": "publish",
            "ok": false,
            "error": {
                "code": "publish_overloaded",
                "message": error.to_string(),
                "outcome": "definite_no_commit",
                "resource": resource,
                "limit": limit,
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
        ClientError::Interrupted {
            reason,
            outcome,
            request,
        } => json!({
            "command": "request",
            "ok": false,
            "error": {
                "code": match reason {
                    Interruption::Cancelled => "cancelled",
                    Interruption::Deadline => "deadline",
                    Interruption::Transport => "transport",
                },
                "message": error.to_string(),
                "outcome": outcome,
                "request": request,
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
