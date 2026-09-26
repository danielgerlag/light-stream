use std::{fs, path::PathBuf, process::ExitCode};

use clap::{Args, Parser, Subcommand};
use light_stream_export::{ExportLimits, FORMAT_VERSION_V1, inspect, verify};
use light_stream_server::{BUILD_REVISION, ServerArgs, ServerConfig};
use light_stream_storage::{RestoreConfig, STORAGE_FORMAT_VERSION, restore_from_path};
use serde_json::json;

type CliError = Box<dyn std::error::Error>;

#[derive(Parser)]
#[command(
    name = "light-streamd",
    version,
    about = "Light Stream server",
    args_conflicts_with_subcommands = true
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
    #[command(flatten)]
    serve: ServerArgs,
}

#[allow(clippy::large_enum_variant)]
#[derive(Subcommand)]
enum Command {
    /// Run the broker. This is the default when no subcommand is given.
    Serve(ServerArgs),
    /// Restore a supported export artifact into a fresh standalone cluster.
    Restore(RestoreCommandArgs),
    /// Inspect an export artifact manifest without publishing anything.
    Inspect(InspectCommandArgs),
    /// Report storage and protocol version information.
    Version,
}

#[derive(Args)]
struct RestoreCommandArgs {
    /// Path to a JSON restore configuration describing the target cluster.
    #[arg(long)]
    config: PathBuf,
    /// Path to the export artifact to restore.
    #[arg(long)]
    input: PathBuf,
}

#[derive(Args)]
struct InspectCommandArgs {
    /// Path to the export artifact to inspect.
    #[arg(long)]
    input: PathBuf,
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = match cli.command {
        None => serve(cli.serve).await,
        Some(Command::Serve(args)) => serve(args).await,
        Some(Command::Restore(args)) => run_restore(args),
        Some(Command::Inspect(args)) => run_inspect(args),
        Some(Command::Version) => run_version(),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

async fn serve(args: ServerArgs) -> Result<(), CliError> {
    let config = ServerConfig::try_from(args)?;
    light_stream_server::run(config).await?;
    Ok(())
}

fn run_restore(args: RestoreCommandArgs) -> Result<(), CliError> {
    let config: RestoreConfig = serde_json::from_slice(&fs::read(&args.config)?)?;
    let limits = ExportLimits::default();
    let receipt = restore_from_path(&args.input, &limits, config)?;
    println!("{}", serde_json::to_string_pretty(&receipt)?);
    Ok(())
}

fn run_inspect(args: InspectCommandArgs) -> Result<(), CliError> {
    let limits = ExportLimits::default();
    let verified = verify(fs::File::open(&args.input)?, &limits)?;
    let report = inspect(&verified);
    let manifest = &report.manifest;
    let value = json!({
        "artifact": {
            "length": report.artifact.length(),
            "sha256": hex_encode(&report.artifact.sha256()),
        },
        "artifact_digest_coverage": {
            "start": report.artifact_digest_coverage.start,
            "end": report.artifact_digest_coverage.end,
        },
        "format_version": manifest.format_version,
        "required_features": manifest.required_features,
        "source_cluster": format!("{:?}", manifest.source_cluster),
        "export_id": format!("{:?}", manifest.export_id),
        "selected_streams": manifest
            .selected_streams
            .iter()
            .map(|stream| format!("{stream:?}"))
            .collect::<Vec<_>>(),
        "cut": format!("{:?}", manifest.cut),
        "exclusions": format!("{:?}", manifest.exclusions),
        "totals": {
            "configured_data_groups": manifest.totals.configured_data_groups,
            "streams": manifest.totals.streams,
            "partitions": manifest.totals.partitions,
            "records": manifest.totals.records,
            "payload_bytes": manifest.totals.payload_bytes,
            "partition_bookmarks": manifest.totals.partition_bookmarks,
            "stream_bookmarks": manifest.totals.stream_bookmarks,
        },
        "sections": report
            .sections
            .iter()
            .map(|section| json!({
                "ordinal": section.ordinal,
                "kind": format!("{:?}", section.kind),
                "section_version": section.section_version,
                "group": section.group.map(|group| format!("{group:?}")),
                "item_count": section.item_count,
                "payload_length": section.payload_length,
            }))
            .collect::<Vec<_>>(),
    });
    println!("{}", serde_json::to_string_pretty(&value)?);
    Ok(())
}

fn run_version() -> Result<(), CliError> {
    let value = json!({
        "build_revision": BUILD_REVISION,
        "protocol": "lightstream.v1",
        "storage_format_version": STORAGE_FORMAT_VERSION,
        "export_format_version": FORMAT_VERSION_V1,
        "supported": {
            "storage_format_versions": [STORAGE_FORMAT_VERSION],
            "export_format_versions": [FORMAT_VERSION_V1],
        },
    });
    println!("{}", serde_json::to_string_pretty(&value)?);
    Ok(())
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}
