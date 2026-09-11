use std::fs::File;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock, mpsc as blocking_mpsc};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail, ensure};
use poc_common::{Audit, Batch};
use poc_storage::Store;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, mpsc, oneshot, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{Duration, timeout};

use crate::cli::{NodeOptions, PeerAdmission};
use crate::wire::{self, APPEND, IO_TIMEOUT, REPLICATE, Response, STATUS};
use crate::{directory, print_json};

const WRITER_QUEUE: usize = 4;
const PEER_QUEUE: usize = 4;
const MAX_CONNECTIONS: usize = 128;

struct Append {
    batch: Arc<Batch>,
    reply: oneshot::Sender<Result<()>>,
}

struct Replication {
    batch: Arc<Batch>,
    reply: blocking_mpsc::Sender<Result<()>>,
}

struct Peer {
    address: SocketAddr,
    accepting: Arc<AtomicBool>,
    queue: Option<mpsc::Sender<Replication>>,
    admission: PeerAdmission,
}

impl Peer {
    fn enqueue(&mut self, job: Replication) -> Result<()> {
        if !self.accepting.load(Ordering::Acquire) {
            self.queue.take();
            bail!("peer {} is disabled", self.address);
        }
        let shard = job.batch.shard;
        let sequence = job.batch.sequence;
        let queue = self.queue.as_ref().context("peer queue is closed")?;
        let admitted = match self.admission {
            PeerAdmission::Block => queue
                .blocking_send(job)
                .map_err(|_| "replication_queue_closed"),
            PeerAdmission::Isolate => queue.try_send(job).map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => "queue_saturated_permanent_isolation",
                mpsc::error::TrySendError::Closed(_) => "replication_queue_closed",
            }),
        };
        if let Err(cause) = admitted {
            disable_peer(&self.accepting, self.address, shard, Some(sequence), cause);
            self.queue.take();
            bail!(
                "peer {} disabled for shard {shard} sequence {sequence}: {cause}",
                self.address
            );
        }
        Ok(())
    }
}

fn disable_peer(
    accepting: &AtomicBool,
    address: SocketAddr,
    shard: u32,
    sequence: Option<u64>,
    cause: &str,
) {
    accepting.store(false, Ordering::Release);
    eprintln!(
        "{}",
        serde_json::json!({
            "event": "peer_disabled",
            "timestamp_unix_ms": SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis(),
            "peer": address.to_string(),
            "shard": shard,
            "sequence": sequence,
            "cause": cause,
        })
    );
}

struct Writer {
    store: Box<dyn Store>,
    applied: Arc<RwLock<Audit>>,
    peers: Vec<Peer>,
    stopped: Option<String>,
}

impl Writer {
    fn append(&mut self, batch: Arc<Batch>) -> Result<()> {
        if let Some(reason) = &self.stopped {
            bail!("shard requires restart after previous append failure: {reason}");
        }
        let result = self.append_inner(batch);
        if let Err(error) = &result {
            self.stopped = Some(format!("{error:#}"));
        }
        result
    }

    fn append_inner(&mut self, batch: Arc<Batch>) -> Result<()> {
        self.store
            .append(&batch)
            .context("local durable append failed")?;
        self.applied
            .write()
            .expect("audit lock poisoned")
            .observe(&batch);
        if self.peers.is_empty() {
            return Ok(());
        }

        let (reply, results) = blocking_mpsc::channel();
        let mut pending = 0;
        let mut failures = Vec::new();
        for peer in &mut self.peers {
            match peer.enqueue(Replication {
                batch: batch.clone(),
                reply: reply.clone(),
            }) {
                Ok(()) => pending += 1,
                Err(error) => failures.push(error.to_string()),
            }
        }
        drop(reply);
        for _ in 0..pending {
            match results.recv_timeout(IO_TIMEOUT + Duration::from_secs(1)) {
                Ok(Ok(())) => return Ok(()),
                Ok(Err(error)) => failures.push(format!("{error:#}")),
                Err(error) => {
                    failures.push(format!(
                        "durable follower acknowledgement unavailable: {error}"
                    ));
                    break;
                }
            }
        }
        bail!(
            "quorum unavailable: local append is durable but no durable follower acknowledged; {}",
            failures.join("; ")
        )
    }

    fn run(mut self, mut requests: mpsc::Receiver<Append>) {
        while let Some(request) = requests.blocking_recv() {
            let _ = request.reply.send(self.append(request.batch));
        }
    }
}

async fn replicate(
    address: SocketAddr,
    shard: u32,
    mut jobs: mpsc::Receiver<Replication>,
    accepting: Arc<AtomicBool>,
    ready: oneshot::Sender<()>,
) {
    let mut stream = match wire::connect(address).await {
        Ok(stream) => stream,
        Err(error) => {
            let cause = format!("connection failed: {error:#}");
            disable_peer(&accepting, address, shard, None, &cause);
            let _ = ready.send(());
            cancel_replication_jobs(&mut jobs, &cause).await;
            return;
        }
    };
    let _ = ready.send(());
    while let Some(job) = jobs.recv().await {
        let request = wire::batch_request(REPLICATE, &job.batch);
        let result = async {
            let response = timeout(IO_TIMEOUT, wire::exchange(&mut stream, &request))
                .await
                .context("replication acknowledgement timed out")??;
            wire::require_ack(response, job.batch.shard, job.batch.sequence)
        }
        .await;
        let failure = result.as_ref().err().map(|error| format!("{error:#}"));
        if let Some(cause) = &failure {
            disable_peer(&accepting, address, shard, Some(job.batch.sequence), cause);
        }
        let _ = job.reply.send(result);
        if let Some(cause) = failure {
            cancel_replication_jobs(&mut jobs, &cause).await;
            break;
        }
    }
}

async fn cancel_replication_jobs(jobs: &mut mpsc::Receiver<Replication>, cause: &str) {
    jobs.close();
    while let Some(job) = jobs.recv().await {
        let _ = job.reply.send(Err(anyhow::anyhow!(
            "replication cancelled for shard {} sequence {}: {cause}",
            job.batch.shard,
            job.batch.sequence
        )));
    }
}

struct PeerHealth {
    shard: u32,
    address: SocketAddr,
    accepting: Arc<AtomicBool>,
}

struct Shared {
    writers: Vec<mpsc::Sender<Append>>,
    applied: Vec<Arc<RwLock<Audit>>>,
    leader: bool,
    peer_admission: PeerAdmission,
    replication_delay_ms: u64,
    replication_peers: Vec<PeerHealth>,
}

async fn dispatch(frame: &[u8], state: &Shared) -> Result<Response> {
    match frame.first().copied() {
        Some(STATUS) => {
            ensure!(frame.len() == 1, "status request must not have a payload");
            Ok(Response::Status {
                ready: true,
                per_shard: state
                    .applied
                    .iter()
                    .map(|audit| audit.read().expect("audit lock poisoned").clone())
                    .collect(),
                peer_admission: state.peer_admission,
                replication_delay_ms: state.replication_delay_ms,
                peer_queue_capacity: PEER_QUEUE,
                replication_peers: state
                    .replication_peers
                    .iter()
                    .map(|peer| wire::PeerStatus {
                        shard: peer.shard,
                        address: peer.address.to_string(),
                        accepting: peer.accepting.load(Ordering::Acquire),
                    })
                    .collect(),
            })
        }
        Some(kind @ (APPEND | REPLICATE)) => {
            ensure!(
                !(kind == REPLICATE && state.leader),
                "a fixed leader cannot accept replication"
            );
            let batch = Batch::decode(&frame[1..])?;
            let writer = state
                .writers
                .get(batch.shard as usize)
                .context("unknown shard")?;
            let shard = batch.shard;
            let sequence = batch.sequence;
            let (reply, result) = oneshot::channel();
            writer
                .try_send(Append {
                    batch: Arc::new(batch),
                    reply,
                })
                .map_err(|_| {
                    anyhow::anyhow!("shard admission queue is full or closed; no retry performed")
                })?;
            result
                .await
                .context("shard writer stopped without acknowledgement")??;
            if kind == REPLICATE && state.replication_delay_ms > 0 {
                tokio::time::sleep(Duration::from_millis(state.replication_delay_ms)).await;
            }
            Ok(Response::Ack { shard, sequence })
        }
        _ => bail!("unknown request type"),
    }
}

async fn send_response(stream: &mut TcpStream, response: &Response) -> Result<()> {
    timeout(
        IO_TIMEOUT,
        wire::write_frame(stream, &serde_json::to_vec(response)?),
    )
    .await
    .context("response write timed out")?
}

async fn connection(mut stream: TcpStream, state: Arc<Shared>, mut stop: watch::Receiver<bool>) {
    loop {
        if *stop.borrow() {
            return;
        }
        let frame = tokio::select! {
            biased;
            _ = stop.changed() => return,
            frame = wire::read_frame(&mut stream) => frame,
        };
        let response = match frame {
            Ok(Some(frame)) => match dispatch(&frame, &state).await {
                Ok(response) => response,
                Err(error) => Response::Error {
                    error: format!("{error:#}"),
                },
            },
            Ok(None) => return,
            Err(error) => {
                let _ = send_response(
                    &mut stream,
                    &Response::Error {
                        error: format!("{error:#}"),
                    },
                )
                .await;
                return;
            }
        };
        if send_response(&mut stream, &response).await.is_err() || *stop.borrow() {
            return;
        }
    }
}

async fn serve(listener: TcpListener, state: Arc<Shared>, mut stop: watch::Receiver<bool>) {
    let slots = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            _ = stop.changed() => break,
            Some(result) = connections.join_next(), if !connections.is_empty() => {
                if let Err(error) = result {
                    eprintln!("connection task failed: {error}");
                }
            }
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, _)) => {
                        if stream.set_nodelay(true).is_err() {
                            continue;
                        }
                        let Ok(slot) = slots.clone().try_acquire_owned() else {
                            drop(stream);
                            continue;
                        };
                        let state = state.clone();
                        let stop = stop.clone();
                        connections.spawn(async move {
                            let _slot = slot;
                            connection(stream, state, stop).await;
                        });
                    }
                    Err(error) => {
                        eprintln!("accept failed: {error}");
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                }
            }
        }
    }
    drop(listener);
    while let Some(result) = connections.join_next().await {
        if let Err(error) = result {
            eprintln!("connection task failed during shutdown: {error}");
        }
    }
}

pub struct RunningNode {
    pub address: SocketAddr,
    stop: watch::Sender<bool>,
    listener: JoinHandle<()>,
    writers: Vec<mpsc::Sender<Append>>,
    writer_threads: Vec<thread::JoinHandle<()>>,
    peers: Vec<JoinHandle<()>>,
    _directory_lock: File,
}

impl RunningNode {
    pub async fn start(options: NodeOptions) -> Result<Self> {
        options.validate()?;
        let directory_lock = directory::lock(&options.dir, true)?;
        let listener = TcpListener::bind(&options.listen).await?;
        let address = listener.local_addr()?;
        let mut peer_addresses = Vec::new();
        for peer in &options.peers {
            let peer_address = timeout(IO_TIMEOUT, tokio::net::lookup_host(peer))
                .await
                .context("peer address resolution timed out")??
                .next()
                .context("peer did not resolve to an address")?;
            ensure!(
                peer_address != address
                    && !(address.ip().is_unspecified() && peer_address.port() == address.port()),
                "a node cannot be its own peer"
            );
            ensure!(
                !peer_addresses.contains(&peer_address),
                "peers resolve to the same address"
            );
            peer_addresses.push(peer_address);
        }

        let mut stores = Vec::new();
        let mut applied = Vec::new();
        for shard in 0..options.shards {
            let mut store =
                poc_storage::open(options.engine, &directory::shard_path(&options.dir, shard))?;
            applied.push(Arc::new(RwLock::new(store.audit()?)));
            stores.push(store);
        }

        let mut writers = Vec::new();
        let mut writer_threads = Vec::new();
        let mut peer_tasks = Vec::new();
        let mut peer_ready = Vec::new();
        let mut peer_health = Vec::new();
        for (shard, store) in stores.into_iter().enumerate() {
            let mut peers = Vec::new();
            for &peer_address in &peer_addresses {
                let (queue, jobs) = mpsc::channel(PEER_QUEUE);
                let accepting = Arc::new(AtomicBool::new(true));
                let (ready, connected) = oneshot::channel();
                peer_tasks.push(tokio::spawn(replicate(
                    peer_address,
                    shard as u32,
                    jobs,
                    accepting.clone(),
                    ready,
                )));
                peer_ready.push(connected);
                peer_health.push(PeerHealth {
                    shard: shard as u32,
                    address: peer_address,
                    accepting: accepting.clone(),
                });
                peers.push(Peer {
                    address: peer_address,
                    accepting,
                    queue: Some(queue),
                    admission: options.peer_admission,
                });
            }
            let (queue, requests) = mpsc::channel(WRITER_QUEUE);
            let writer = Writer {
                store,
                applied: applied[shard].clone(),
                peers,
                stopped: None,
            };
            writer_threads.push(thread::spawn(move || writer.run(requests)));
            writers.push(queue);
        }
        let (stop, stopped) = watch::channel(false);
        let shared = Arc::new(Shared {
            writers: writers.clone(),
            applied,
            leader: !peer_addresses.is_empty(),
            peer_admission: options.peer_admission,
            replication_delay_ms: options.replication_delay_ms,
            replication_peers: peer_health,
        });
        let listener = tokio::spawn(serve(listener, shared, stopped));
        for connected in peer_ready {
            let _ = connected.await;
        }
        Ok(Self {
            address,
            stop,
            listener,
            writers,
            writer_threads,
            peers: peer_tasks,
            _directory_lock: directory_lock,
        })
    }

    pub async fn shutdown(self) -> Result<()> {
        let _ = self.stop.send(true);
        let listener_result = self.listener.await;
        drop(self.writers);
        let writers_result = tokio::task::spawn_blocking(move || {
            let mut panicked = false;
            for writer in self.writer_threads {
                panicked |= writer.join().is_err();
            }
            ensure!(!panicked, "shard writer panicked");
            Ok::<(), anyhow::Error>(())
        })
        .await;
        let mut peer_error = None;
        for peer in self.peers {
            if let Err(error) = peer.await {
                peer_error = Some(error);
            }
        }
        listener_result?;
        writers_result??;
        if let Some(error) = peer_error {
            return Err(error.into());
        }
        Ok(())
    }
}

pub async fn run(options: NodeOptions) -> Result<()> {
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    #[cfg(unix)]
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let engine = options.engine.to_string();
    let shards = options.shards;
    let leader = !options.peers.is_empty();
    let peer_admission = options.peer_admission;
    let replication_delay_ms = options.replication_delay_ms;
    let node = RunningNode::start(options).await?;
    let output = print_json(&serde_json::json!({
        "ready": true,
        "address": node.address.to_string(),
        "engine": engine,
        "shards": shards,
        "protocol": "fixed-leader-durable-append",
        "leader": leader,
        "peer_admission": peer_admission,
        "replication_delay_ms": replication_delay_ms,
        "peer_queue_capacity": PEER_QUEUE,
    }));
    if output.is_ok() {
        #[cfg(unix)]
        tokio::select! {
            _ = terminate.recv() => {},
            _ = interrupt.recv() => {},
        }
        #[cfg(not(unix))]
        tokio::signal::ctrl_c().await?;
    }
    let shutdown = node.shutdown().await;
    output?;
    shutdown
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_admission_isolate_never_resumes_after_full_queue() -> Result<()> {
        let (queue, mut jobs) = mpsc::channel(1);
        let accepting = Arc::new(AtomicBool::new(true));
        let mut peer = Peer {
            address: "127.0.0.1:1".parse()?,
            accepting: accepting.clone(),
            queue: Some(queue),
            admission: PeerAdmission::Isolate,
        };
        let (reply, _results) = blocking_mpsc::channel();
        let job = |sequence| -> Result<Replication> {
            Ok(Replication {
                batch: Arc::new(Batch::generate(0, sequence, 1, 17)?),
                reply: reply.clone(),
            })
        };
        peer.enqueue(job(0)?)?;
        assert!(peer.enqueue(job(1)?).is_err());
        assert!(!accepting.load(Ordering::Acquire));
        assert_eq!(jobs.try_recv()?.batch.sequence, 0);
        assert!(peer.enqueue(job(2)?).is_err());
        assert!(matches!(
            jobs.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn peer_admission_cancelled_jobs_receive_explicit_errors() -> Result<()> {
        let (queue, mut jobs) = mpsc::channel(2);
        let (reply, results) = blocking_mpsc::channel();
        for sequence in 0..2 {
            queue
                .try_send(Replication {
                    batch: Arc::new(Batch::generate(0, sequence, 1, 17)?),
                    reply: reply.clone(),
                })
                .map_err(|_| anyhow::anyhow!("test queue unexpectedly full"))?;
        }
        cancel_replication_jobs(&mut jobs, "test timeout").await;
        for sequence in 0..2 {
            let error = results
                .try_recv()?
                .expect_err("cancelled replication cannot acknowledge");
            assert!(
                error
                    .to_string()
                    .contains(&format!("sequence {sequence}: test timeout"))
            );
        }
        assert!(queue.is_closed());
        Ok(())
    }
}
