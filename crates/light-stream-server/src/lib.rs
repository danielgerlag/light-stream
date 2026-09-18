mod config;
mod export;
mod lifecycle;
mod manifest;
mod openraft_boundary;
mod operations;
mod peer;
mod publish_scheduler;
mod runtime;
mod security;
mod service;
mod tasks;

use std::{
    fs::{File, OpenOptions},
    io::{self, Write},
    net::SocketAddr,
    path::Path,
    sync::Arc,
    time::Duration,
};

use fs2::FileExt;
use light_stream_core::{MAX_PUBLIC_MESSAGE_BYTES, NodeDescriptor, NodeId, SecurityMode};
use light_stream_proto::v1::light_stream_server::LightStreamServer;
use serde::Serialize;
use thiserror::Error;
use tokio::{
    net::TcpListener,
    sync::watch,
    task::{JoinError, JoinSet},
    time::{Instant, MissedTickBehavior},
};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

pub use config::{ServerArgs, ServerConfig};
use lifecycle::LifecycleController;
use peer::PeerApi;
use runtime::{ClusterManager, ClusterManagerConfig, ShutdownPreparationError};
use security::RuntimeSecurityConfig;
use service::PublicApi;
use tasks::{StopToken, TaskGroup, TaskGroupError};

pub const BUILD_REVISION: &str = match option_env!("LIGHT_STREAM_BUILD_REVISION") {
    Some(value) => value,
    None => "UNVERSIONED",
};

const BACKGROUND_TASK_STOP_TIMEOUT: Duration = Duration::from_secs(5);
const READINESS_REFRESH_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Debug, Error)]
pub enum StartupError {
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
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
    #[error("operations server failed: {0}")]
    Operations(io::Error),
    #[error("server task failed: {0}")]
    Task(String),
    #[error("shutdown signal failed: {0}")]
    Signal(io::Error),
    #[error(
        "shutdown drain deadline expired after accepting {accepted} mutations with {unresolved} unresolved"
    )]
    DrainDeadline { accepted: u64, unresolved: u64 },
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
    pub operations_address: String,
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

struct RefreshTasks {
    tasks: TaskGroup,
}

enum ServerEvent {
    Public(Result<(), StartupError>),
    Peer(Result<(), StartupError>),
    Operations(Result<(), StartupError>),
}

impl RefreshTasks {
    fn start(
        cluster: Arc<ClusterManager>,
        security: RuntimeSecurityConfig,
        lifecycle: LifecycleController,
    ) -> Self {
        let mut tasks = TaskGroup::new();
        if security.mode() == SecurityMode::Secured {
            let cluster = cluster.clone();
            tasks.spawn("security-refresh", move |stop| {
                security_refresh_loop(cluster, security, stop)
            });
        }
        tasks.spawn("readiness-refresh", move |stop| {
            readiness_refresh_loop(cluster, lifecycle, stop)
        });
        Self { tasks }
    }

    async fn stop_and_join(self, deadline: Instant) -> Result<(), TaskGroupError> {
        self.tasks.stop_and_join(deadline).await
    }
}

async fn security_refresh_loop(
    cluster: Arc<ClusterManager>,
    security: RuntimeSecurityConfig,
    mut stop: StopToken,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            _ = stop.cancelled() => return,
            _ = interval.tick() => {}
        }
        let policy = tokio::select! {
            biased;
            _ = stop.cancelled() => return,
            policy = cluster.confirmed_security_policy() => policy,
        };
        if let Ok(policy) = policy {
            let _ = security.renew_policy(policy);
        }
    }
}

async fn readiness_refresh_loop(
    cluster: Arc<ClusterManager>,
    lifecycle: LifecycleController,
    mut stop: StopToken,
) {
    let mut interval = tokio::time::interval(READINESS_REFRESH_INTERVAL);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            _ = stop.cancelled() => return,
            _ = interval.tick() => {}
        }
        let Some(ticket) = lifecycle.begin_readiness_sample() else {
            continue;
        };
        let readiness = tokio::select! {
            biased;
            _ = stop.cancelled() => return,
            readiness = cluster.write_readiness() => readiness,
        };
        lifecycle.commit_readiness(ticket, readiness);
    }
}

fn server_task_result(result: Result<ServerEvent, JoinError>) -> Result<(), StartupError> {
    match result.map_err(|error| StartupError::Task(error.to_string()))? {
        ServerEvent::Public(result)
        | ServerEvent::Peer(result)
        | ServerEvent::Operations(result) => result,
    }
}

async fn join_server_tasks(
    servers: &mut JoinSet<ServerEvent>,
    deadline: Instant,
) -> Result<(), StartupError> {
    let mut failures = Vec::new();
    loop {
        match tokio::time::timeout_at(deadline, servers.join_next()).await {
            Ok(Some(result)) => {
                if let Err(error) = server_task_result(result) {
                    failures.push(error);
                }
            }
            Ok(None) => break,
            Err(_) => {
                failures.push(StartupError::Task(
                    "server shutdown deadline expired".to_owned(),
                ));
                servers.abort_all();
                while let Some(result) = servers.join_next().await {
                    match result {
                        Ok(event) => {
                            if let Err(error) = server_task_result(Ok(event)) {
                                failures.push(error);
                            }
                        }
                        Err(error) if error.is_cancelled() => {}
                        Err(error) => failures.push(StartupError::Task(error.to_string())),
                    }
                }
                break;
            }
        }
    }
    finish_failures(failures)
}

async fn drain_and_stop_servers(
    cluster: &Arc<ClusterManager>,
    lifecycle: &LifecycleController,
    servers: &mut JoinSet<ServerEvent>,
    shutdown_tx: &watch::Sender<bool>,
    maintenance_timeout: Duration,
    drain_grace: Duration,
    mut server_failures: Vec<StartupError>,
) -> (Result<(), StartupError>, bool) {
    let shutdown = cluster.begin_shutdown(maintenance_timeout, drain_grace);
    tokio::pin!(shutdown);
    let (maintenance, drain) = loop {
        tokio::select! {
            result = &mut shutdown => break result,
            result = servers.join_next(), if !servers.is_empty() => {
                match result {
                    Some(result) => {
                        if let Err(error) = server_task_result(result) {
                            server_failures.push(error);
                        }
                    }
                    None => server_failures.push(StartupError::Task(
                        "server task set ended during drain".to_owned(),
                    )),
                }
            }
        }
    };
    let teardown_safe = maintenance
        .as_ref()
        .err()
        .is_none_or(ShutdownPreparationError::teardown_safe)
        && matches!(drain, lifecycle::DrainOutcome::Completed { .. });
    lifecycle.mark_stopping();
    let _ = shutdown_tx.send(true);
    if let Err(error) =
        join_server_tasks(servers, Instant::now() + BACKGROUND_TASK_STOP_TIMEOUT).await
    {
        server_failures.push(error);
    }
    let server_result = finish_failures(server_failures);
    (
        finish_shutdown(maintenance, drain, server_result),
        teardown_safe,
    )
}

fn finish_failures(mut failures: Vec<StartupError>) -> Result<(), StartupError> {
    match failures.len() {
        0 => Ok(()),
        1 => Err(failures.pop().expect("one failure is present")),
        _ => Err(StartupError::Task(
            failures
                .into_iter()
                .map(|error| error.to_string())
                .collect::<Vec<_>>()
                .join("; "),
        )),
    }
}

pub async fn run(config: ServerConfig) -> Result<(), StartupError> {
    openraft_boundary::assert_compile_boundary();
    let lifecycle = LifecycleController::starting();
    let security = config.security().clone();
    let scheme = match security.mode() {
        SecurityMode::LocalInsecure => "http",
        SecurityMode::Secured => "https",
    };
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
    let operations_listener = TcpListener::bind(config.operations_listen())
        .await
        .map_err(|source| StartupError::Bind {
            listener: "operations",
            address: config.operations_listen(),
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
    let operations_address =
        operations_listener
            .local_addr()
            .map_err(|source| StartupError::Bind {
                listener: "operations",
                address: config.operations_listen(),
                source,
            })?;
    let advertised_public_uri = config
        .advertise_public_uri()
        .map(str::to_owned)
        .unwrap_or_else(|| format!("{scheme}://{public_address}"));
    let advertised_peer_uri = config
        .advertise_peer_uri()
        .map(str::to_owned)
        .unwrap_or_else(|| format!("{scheme}://{peer_address}"));
    security.validate_advertised_uris(&advertised_public_uri, &advertised_peer_uri)?;
    let local = NodeDescriptor::new(
        NodeId::new(config.node_id()).map_err(StartupError::Cluster)?,
        advertised_public_uri.clone(),
        advertised_peer_uri.clone(),
    );

    let cluster = Arc::new(
        ClusterManager::open(
            config.data_dir().to_path_buf(),
            local,
            ClusterManagerConfig {
                receipt_window: config.receipt_window(),
                peer_routes: config.peer_routes().clone(),
                group_pool: config.group_pool().clone(),
                publish_scheduler: config.publish_scheduler(),
                verification_delays: (
                    config.verification_delay(),
                    config.verification_response_delay(),
                ),
                export_limits: config.export_limits(),
                security: security.clone(),
                lifecycle: lifecycle.clone(),
            },
        )
        .await?,
    );
    let public_api = PublicApi::new(
        cluster.clone(),
        security.clone(),
        advertised_public_uri.clone(),
        advertised_peer_uri.clone(),
        lifecycle.clone(),
    );
    let peer_api = PeerApi::new(cluster.clone());
    lifecycle.mark_running(cluster.write_readiness().await);
    let refresh_tasks = RefreshTasks::start(cluster.clone(), security.clone(), lifecycle.clone());
    let mut public_builder = Server::builder();
    if let Some(tls) = security.public_tls() {
        public_builder = public_builder.tls_config(tls)?;
    }
    let mut peer_builder = Server::builder();
    if let Some(tls) = security.peer_tls() {
        peer_builder = peer_builder.tls_config(tls)?;
    }

    println!(
        "{}",
        serde_json::to_string(&ReadyAnnouncement {
            ready: true,
            revision: BUILD_REVISION.to_owned(),
            security_mode: security.mode().to_string(),
            public_address: public_address.to_string(),
            peer_address: peer_address.to_string(),
            operations_address: operations_address.to_string(),
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
    let peer_shutdown = shutdown_rx.clone();
    let operations_shutdown = shutdown_rx;
    let public_server = public_builder
        .add_service(
            LightStreamServer::new(public_api)
                .max_decoding_message_size(MAX_PUBLIC_MESSAGE_BYTES)
                .max_encoding_message_size(MAX_PUBLIC_MESSAGE_BYTES),
        )
        .serve_with_incoming_shutdown(
            TcpListenerStream::new(public_listener),
            wait_for_shutdown(public_shutdown),
        );
    let peer_server = peer_builder
        .add_service(
            peer::wire::peer_service_server::PeerServiceServer::new(peer_api)
                .max_decoding_message_size(peer::MAX_PEER_MESSAGE_BYTES)
                .max_encoding_message_size(peer::MAX_PEER_MESSAGE_BYTES),
        )
        .serve_with_incoming_shutdown(
            TcpListenerStream::new(peer_listener),
            wait_for_shutdown(peer_shutdown),
        );
    let operations_server = operations::serve(
        operations_listener,
        lifecycle.clone(),
        cluster.clone(),
        operations_shutdown,
    );

    let mut servers = JoinSet::new();
    servers
        .spawn(async move { ServerEvent::Public(public_server.await.map_err(StartupError::from)) });
    servers.spawn(async move { ServerEvent::Peer(peer_server.await.map_err(StartupError::from)) });
    servers.spawn(async move {
        ServerEvent::Operations(operations_server.await.map_err(StartupError::Operations))
    });
    let (serve_result, mut cluster_shutdown_safe) = tokio::select! {
        result = servers.join_next() => {
            let first = result
                .ok_or_else(|| StartupError::Task("server task set ended unexpectedly".to_owned()))
                .and_then(server_task_result);
            drain_and_stop_servers(
                &cluster,
                &lifecycle,
                &mut servers,
                &shutdown_tx,
                BACKGROUND_TASK_STOP_TIMEOUT,
                config.shutdown_grace(),
                first.err().into_iter().collect(),
            )
            .await
        }
        signal = shutdown_signal() => {
            drain_and_stop_servers(
                &cluster,
                &lifecycle,
                &mut servers,
                &shutdown_tx,
                BACKGROUND_TASK_STOP_TIMEOUT,
                config.shutdown_grace(),
                signal.err().into_iter().collect(),
            )
            .await
        }
    };
    let mut failures = Vec::new();
    if let Err(error) = serve_result {
        failures.push(error);
    }
    let refresh_shutdown_safe = if let Err(error) = refresh_tasks
        .stop_and_join(Instant::now() + BACKGROUND_TASK_STOP_TIMEOUT)
        .await
    {
        let all_joined = error.all_joined();
        failures.push(StartupError::Task(error.to_string()));
        all_joined
    } else {
        true
    };
    cluster_shutdown_safe &= refresh_shutdown_safe;
    if cluster_shutdown_safe {
        if let Err(error) = cluster.shutdown().await {
            failures.push(StartupError::Cluster(error));
        }
    } else {
        failures.push(StartupError::Task(
            "cluster shutdown skipped because lifecycle tasks remained active".to_owned(),
        ));
    }
    finish_failures(failures)
}

fn finish_shutdown(
    maintenance: Result<(), ShutdownPreparationError>,
    drain: lifecycle::DrainOutcome,
    servers: Result<(), StartupError>,
) -> Result<(), StartupError> {
    let mut failures = Vec::new();
    if let Err(error) = maintenance {
        failures.push(StartupError::Task(error.to_string()));
    }
    if let lifecycle::DrainOutcome::DeadlineExceeded {
        accepted,
        unresolved,
    } = drain
    {
        failures.push(StartupError::DrainDeadline {
            accepted,
            unresolved,
        });
    }
    if let Err(error) = servers {
        failures.push(error);
    }
    finish_failures(failures)
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
    use std::{future, time::Duration};

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

    #[test]
    fn multiple_failures_are_accumulated() {
        let error = finish_failures(vec![
            StartupError::Task("public failed".to_owned()),
            StartupError::Task("peer failed".to_owned()),
        ])
        .unwrap_err()
        .to_string();

        assert!(error.contains("public failed"));
        assert!(error.contains("peer failed"));
    }

    #[test]
    fn single_shutdown_failure_preserves_its_typed_error() {
        assert!(matches!(
            finish_shutdown(
                Ok(()),
                lifecycle::DrainOutcome::DeadlineExceeded {
                    accepted: 3,
                    unresolved: 1,
                },
                Ok(()),
            ),
            Err(StartupError::DrainDeadline {
                accepted: 3,
                unresolved: 1,
            })
        ));
    }

    #[tokio::test]
    async fn server_shutdown_deadline_aborts_and_joins_unfinished_tasks() {
        let mut servers = JoinSet::new();
        servers.spawn(future::pending::<ServerEvent>());

        let error = join_server_tasks(&mut servers, Instant::now() + Duration::from_millis(10))
            .await
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("server shutdown deadline expired")
        );
        assert!(servers.is_empty());
    }
}
