use clap::Parser;
use light_stream_server::{ServerArgs, ServerConfig};

#[tokio::main]
async fn main() {
    let result = match ServerConfig::try_from(ServerArgs::parse()) {
        Ok(config) => light_stream_server::run(config).await,
        Err(error) => Err(error),
    };
    if let Err(error) = result {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
