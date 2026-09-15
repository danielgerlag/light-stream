mod config;
mod manifest;
mod openraft_boundary;
mod peer;
mod publish_scheduler;
mod runtime;
mod service;

use std::{
    fs::{File, OpenOptions},
    io::{self, Write},
    net::SocketAddr,
    path::Path,
    sync::Arc,
};

use fs2::FileExt;
use light_stream_core::{MAX_PUBLIC_MESSAGE_BYTES, NodeDescriptor, NodeId, SecurityMode};
use light_stream_proto::v1::light_stream_server::LightStreamServer;
use serde::Serialize;
use thiserror::Error;
use tokio::{net::TcpListener, sync::watch};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

pub use config::{ServerArgs, ServerConfig};
use peer::PeerApi;
use runtime::ClusterManager;
use service::PublicApi;

pub const BUILD_REVISION: &str = match option_env!("LIGHT_STREAM_BUILD_REVISION") {
    Some(value) => value,
    None => "UNVERSIONED",
};

#[derive(Debug, Error)]
pub enum StartupError {
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
    #[error("secured mode is not implemented until LS08")]
    SecuredModeUnsupported,
    #[error("data directory is already owned by another light-streamd process: {0}")]
    DataDirectoryLocked(String),
    #[error("failed to prepare data directory {path}: {source}")]
    DataDirectory {
        path: String,
        #[source]
        source: io::Error,
    },
    #[error("failed to bind {listener} listener at {address}: {source}")]
    Bind {
        listener: &'static str,
        address: SocketAddr,
        #[source]
        source: io::Error,
    },
    #[error("server failed: {0}")]
    Serve(#[from] tonic::transport::Error),
    #[error("shutdown signal failed: {0}")]
    Signal(io::Error),
    #[error("cluster runtime failed: {0}")]
    Cluster(#[from] light_stream_core::DomainError),
}

#[derive(Debug, Serialize)]
pub struct ReadyAnnouncement {
    pub ready: bool,
    pub revision: String,
    pub security_mode: String,
    pub public_address: String,
    pub peer_address: String,
    pub advertised_public_uri: String,
    pub advertised_peer_uri: String,
    pub data_directory: String,
}

struct DataDirectoryLock {
    _file: File,
}

impl DataDirectoryLock {
    fn acquire(path: &Path) -> Result<Self, StartupError> {
        std::fs::create_dir_all(path).map_err(|source| StartupError::DataDirectory {
            path: path.display().to_string(),
            source,
        })?;
        let lock_path = path.join("LOCK");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|source| StartupError::DataDirectory {
                path: lock_path.display().to_string(),
                source,
            })?;
        file.try_lock_exclusive()
            .map_err(|_| StartupError::DataDirectoryLocked(path.display().to_string()))?;
        Ok(Self { _file: file })
    }
}

pub async fn run(config: ServerConfig) -> Result<(), StartupError> {
    openraft_boundary::assert_compile_boundary();
    let _data_lock = DataDirectoryLock::acquire(config.data_dir())?;
    let public_listener = TcpListener::bind(config.public_listen())
        .await
        .map_err(|source| StartupError::Bind {
            listener: "public",
            address: config.public_listen(),
            source,
        })?;
    let peer_listener = TcpListener::bind(config.peer_listen())
        .await
        .map_err(|source| StartupError::Bind {
            listener: "peer",
            address: config.peer_listen(),
            source,
        })?;
    let public_address = public_listener
        .local_addr()
        .map_err(|source| StartupError::Bind {
            listener: "public",
            address: config.public_listen(),
            source,
        })?;
    let peer_address = peer_listener
        .local_addr()
        .map_err(|source| StartupError::Bind {
            listener: "peer",
            address: config.peer_listen(),
            source,
        })?;
    let advertised_public_uri = config
        .advertise_public_uri()
        .map(str::to_owned)
        .unwrap_or_else(|| format!("http://{public_address}"));
    let advertised_peer_uri = config
        .advertise_peer_uri()
        .map(str::to_owned)
        .unwrap_or_else(|| format!("http://{peer_address}"));
    let local = NodeDescriptor::new(
        NodeId::new(config.node_id()).map_err(StartupError::Cluster)?,
        advertised_public_uri.clone(),
        advertised_peer_uri.clone(),
    );

    let cluster = Arc::new(
        ClusterManager::open(
            config.data_dir().to_path_buf(),
            local,
            config.receipt_window(),
            config.peer_routes().clone(),
            config.group_pool().clone(),
            config.publish_scheduler(),
            (
                config.verification_delay(),
                config.verification_response_delay(),
            ),
        )
        .await?,
    );
    let public_api = PublicApi::new(
        cluster.clone(),
        config.security_mode(),
        advertised_public_uri.clone(),
        advertised_peer_uri.clone(),
    );
    let peer_api = PeerApi::new(cluster.clone());

    println!(
        "{}",
        serde_json::to_string(&ReadyAnnouncement {
            ready: true,
            revision: BUILD_REVISION.to_owned(),
            security_mode: SecurityMode::LocalInsecure.to_string(),
            public_address: public_address.to_string(),
            peer_address: peer_address.to_string(),
            advertised_public_uri,
            advertised_peer_uri,
            data_directory: config.data_dir().display().to_string(),
        })
        .expect("ready announcement is serializable")
    );
    io::stdout()
        .flush()
        .map_err(|source| StartupError::DataDirectory {
            path: "stdout".to_owned(),
            source,
        })?;

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let public_shutdown = shutdown_rx.clone();
    let peer_shutdown = shutdown_rx;
    let public_server = Server::builder()
        .add_service(
            LightStreamServer::new(public_api)
                .max_decoding_message_size(MAX_PUBLIC_MESSAGE_BYTES)
                .max_encoding_message_size(MAX_PUBLIC_MESSAGE_BYTES),
        )
        .serve_with_incoming_shutdown(
            TcpListenerStream::new(public_listener),
            wait_for_shutdown(public_shutdown),
        );
    let peer_server = Server::builder()
        .add_service(
            peer::wire::peer_service_server::PeerServiceServer::new(peer_api)
                .max_decoding_message_size(peer::MAX_PEER_MESSAGE_BYTES)
                .max_encoding_message_size(peer::MAX_PEER_MESSAGE_BYTES),
        )
        .serve_with_incoming_shutdown(
            TcpListenerStream::new(peer_listener),
            wait_for_shutdown(peer_shutdown),
        );

    let servers = async { tokio::try_join!(public_server, peer_server) };
    tokio::pin!(servers);
    let serve_result: Result<(), StartupError> = tokio::select! {
        result = &mut servers => {
            result.map(|_| ()).map_err(StartupError::from)
        }
        signal = shutdown_signal() => {
            match signal {
                Ok(()) => {
                    let _ = shutdown_tx.send(true);
                    servers.await.map(|_| ()).map_err(StartupError::from)
                }
                Err(error) => Err(error),
            }
        }
    };
    cluster.shutdown().await?;
    serve_result?;
    Ok(())
}

async fn wait_for_shutdown(mut receiver: watch::Receiver<bool>) {
    while !*receiver.borrow() {
        if receiver.changed().await.is_err() {
            break;
        }
    }
}

async fn shutdown_signal() -> Result<(), StartupError> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .map_err(StartupError::Signal)?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result.map_err(StartupError::Signal),
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await.map_err(StartupError::Signal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_directory_lock_refuses_a_second_owner() {
        let directory = tempfile::tempdir().unwrap();
        let _first = DataDirectoryLock::acquire(directory.path()).unwrap();
        assert!(matches!(
            DataDirectoryLock::acquire(directory.path()),
            Err(StartupError::DataDirectoryLocked(_))
        ));
    }
}
