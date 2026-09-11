mod bench;
mod cli;
mod directory;
mod inspect;
mod node;
mod wire;

use anyhow::Result;
use clap::Parser;
use cli::{Cli, Command};
use serde::Serialize;

fn print_json(value: &impl Serialize) -> Result<()> {
    use std::io::Write;
    let stdout = std::io::stdout();
    let mut output = stdout.lock();
    serde_json::to_writer(&mut output, value)?;
    writeln!(output)?;
    output.flush()?;
    Ok(())
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error) if error.use_stderr() => {
            let _ = print_json(&serde_json::json!({"errors": 1, "error": error.to_string()}));
            return std::process::ExitCode::FAILURE;
        }
        Err(error) => {
            let _ = error.print();
            return std::process::ExitCode::SUCCESS;
        }
    };
    let result = execute(cli.command).await;
    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            if let Some(failure) = error.downcast_ref::<bench::FailureReport>() {
                let _ = print_json(failure);
            } else {
                let _ =
                    print_json(&serde_json::json!({"errors": 1, "error": format!("{error:#}")}));
            }
            std::process::ExitCode::FAILURE
        }
    }
}

async fn execute(command: Command) -> Result<()> {
    match command {
        Command::Node(options) => node::run(options).await,
        Command::Bench(options) => print_json(&bench::run(options).await?),
        Command::Inspect(options) => print_json(&inspect::run(options)?),
        Command::Status { address } => print_json(&wire::status(&address).await?),
    }
}
