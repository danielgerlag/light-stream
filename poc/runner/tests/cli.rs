#![cfg(unix)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, ensure};
use poc_common::{Audit, Batch};
use poc_storage::Engine;
use serde_json::Value;

const BINARY: &str = env!("CARGO_BIN_EXE_stream-poc");
static TEST_NUMBER: AtomicU64 = AtomicU64::new(0);
static SERIAL: Mutex<()> = Mutex::new(());

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Result<Self> {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".test-data");
        let path = root.join(format!(
            "{}-{}",
            std::process::id(),
            TEST_NUMBER.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path)?;
        Ok(Self(path))
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
        if let Some(root) = self.0.parent() {
            let _ = std::fs::remove_dir(root);
        }
    }
}

struct Node {
    child: Child,
    address: String,
    readiness: Value,
}

impl Node {
    fn start(directory: &Path, shards: u32, peers: &[&str]) -> Result<Self> {
        Self::start_with_options(directory, shards, peers, &[])
    }

    fn start_with_options(
        directory: &Path,
        shards: u32,
        peers: &[&str],
        options: &[&str],
    ) -> Result<Self> {
        let mut command = Command::new(BINARY);
        command.args(["node", "--engine", "segment", "--dir"]);
        command
            .arg(directory)
            .args(["--listen", "127.0.0.1:0", "--shards", &shards.to_string()]);
        if !peers.is_empty() {
            command.args(["--peers", &peers.join(",")]);
        }
        command.args(options);
        let child = command
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        let mut node = Self {
            child,
            address: String::new(),
            readiness: Value::Null,
        };
        let stdout = node.child.stdout.take().context("missing node stdout")?;
        let (send, receive) = mpsc::channel();
        thread::spawn(move || {
            let mut line = String::new();
            let result = BufReader::new(stdout).read_line(&mut line).map(|_| line);
            let _ = send.send(result);
        });
        let line = receive.recv_timeout(Duration::from_secs(20))??;
        let ready: Value = serde_json::from_str(&line)?;
        ensure!(ready["ready"] == true, "node did not become ready: {ready}");
        node.address = ready["address"]
            .as_str()
            .context("missing resolved address")?
            .to_owned();
        ensure!(
            node.address.parse::<SocketAddr>()?.port() != 0,
            "port zero was not resolved"
        );
        node.readiness = ready;
        Ok(node)
    }

    fn stop(&mut self) -> Result<()> {
        ensure!(
            Command::new("kill")
                .args(["-TERM", &self.child.id().to_string()])
                .status()?
                .success(),
            "could not send SIGTERM"
        );
        let before = Instant::now();
        loop {
            if let Some(status) = self.child.try_wait()? {
                ensure!(status.success(), "node shutdown failed: {status}");
                return Ok(());
            }
            ensure!(
                before.elapsed() < Duration::from_secs(20),
                "graceful shutdown timed out"
            );
            thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn parse(output: &Output) -> Result<Value> {
    serde_json::from_slice(&output.stdout)
        .with_context(|| format!("invalid JSON: {}", String::from_utf8_lossy(&output.stdout)))
}

fn successful(args: &[&str]) -> Result<Value> {
    let output = Command::new(BINARY).args(args).output()?;
    let value = parse(&output)?;
    ensure!(
        output.status.success(),
        "command failed: {value}; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(value)
}

fn bench(address: &str, shards: u32, start: u64, count: u64) -> Result<Output> {
    Command::new(BINARY)
        .args([
            "bench",
            "--address",
            address,
            "--shards",
            &shards.to_string(),
            "--batch-records",
            "3",
            "--record-bytes",
            "17",
            "--seconds",
            "5",
            "--max-batches",
            &count.to_string(),
            "--start-sequence",
            &start.to_string(),
        ])
        .output()
        .map_err(Into::into)
}

fn inspect(directory: &Path, shards: u32, retain: Option<u64>) -> Result<Value> {
    let mut command = Command::new(BINARY);
    command.args(["inspect", "--engine", "segment", "--dir"]);
    command
        .arg(directory)
        .args(["--shards", &shards.to_string()]);
    if let Some(retain) = retain {
        command.args(["--retain-from", &retain.to_string()]);
    }
    let output = command.output()?;
    let result = parse(&output)?;
    ensure!(output.status.success(), "inspect failed: {result}");
    Ok(result)
}

fn expected(shards: u32, start: u64, end: u64) -> Result<Value> {
    let mut audits = Vec::new();
    for shard in 0..shards {
        let mut audit = Audit::default();
        for sequence in start..end {
            audit.observe(&Batch::generate(shard, sequence, 3, 17)?);
        }
        audits.push(audit);
    }
    Ok(serde_json::to_value(audits)?)
}

fn wait_applied(address: &str, audits: &Value) -> Result<()> {
    let before = Instant::now();
    loop {
        let status = successful(&["status", "--address", address])?;
        if &status["per_shard"] == audits {
            return Ok(());
        }
        ensure!(
            before.elapsed() < Duration::from_secs(5),
            "replica did not apply expected prefix"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn standalone_binary_audits_retains_reopens_and_rejects_online_inspect() -> Result<()> {
    let _serial = SERIAL.lock().unwrap_or_else(|error| error.into_inner());
    let directory = TestDirectory::new()?;
    let mut node = Node::start(&directory.0, 2, &[])?;
    let output = bench(&node.address, 2, 0, 6)?;
    ensure!(output.status.success(), "bench failed: {}", parse(&output)?);
    let report = parse(&output)?;
    let audits = expected(2, 0, 6)?;
    assert_eq!(report["per_shard"], audits);
    assert_eq!(report["records"], 36);
    assert_eq!(report["payload_bytes"], 612);
    assert_eq!(report["batches"], 12);
    assert_eq!(report["latencies_us"].as_array().unwrap().len(), 12);
    assert_eq!(report["errors"], 0);
    let rate = report["records_per_second"].as_f64().unwrap();
    let elapsed = report["elapsed_seconds"].as_f64().unwrap();
    assert!((rate * elapsed - 36.0).abs() < 0.000001);
    wait_applied(&node.address, &audits)?;

    let locked = Command::new(BINARY)
        .args(["inspect", "--engine", "segment", "--dir"])
        .arg(&directory.0)
        .args(["--shards", "2"])
        .output()?;
    assert!(!locked.status.success());
    assert_eq!(parse(&locked)?["errors"], 1);
    assert!(parse(&locked)?["per_shard"].is_null());
    node.stop()?;

    let inspection = inspect(&directory.0, 2, Some(3))?;
    assert_eq!(inspection["per_shard"], audits);
    assert!(inspection["reopen_seconds"].as_f64().unwrap() >= 0.0);
    assert!(inspection["audit_seconds"].as_f64().unwrap() >= 0.0);
    assert_eq!(inspection["bookmarks"][0][0]["sequence"], 5);
    assert_eq!(inspection["bookmarks"][0][0]["next_record"], 18);
    assert_eq!(
        inspection["after_retention"]["per_shard"],
        expected(2, 3, 6)?
    );
    assert_eq!(
        inspection["after_retention"]["bookmarks"][1][2]["next_record"],
        12
    );

    let mut reopened = Node::start(&directory.0, 2, &[])?;
    let output = bench(&reopened.address, 2, 6, 2)?;
    ensure!(
        output.status.success(),
        "resumed bench failed: {}",
        parse(&output)?
    );
    assert_eq!(parse(&output)?["per_shard"], expected(2, 6, 8)?);
    reopened.stop()?;
    assert_eq!(
        inspect(&directory.0, 2, None)?["per_shard"],
        expected(2, 3, 8)?
    );
    Ok(())
}

#[test]
fn inspect_caps_recent_markers_and_preserves_absolute_offsets() -> Result<()> {
    let _serial = SERIAL.lock().unwrap_or_else(|error| error.into_inner());
    let directory = TestDirectory::new()?;
    {
        let mut store = poc_storage::open(Engine::Segment, &directory.0.join("shard-0"))?;
        for sequence in 0..104 {
            store.append(&Batch::generate(0, sequence, 3, 17)?)?;
        }
    }
    let report = inspect(&directory.0, 1, Some(101))?;
    assert_eq!(report["bookmark_limit"], 100);
    assert_eq!(report["per_shard"], expected(1, 0, 104)?);
    let shards = report["bookmarks"].as_array().unwrap();
    assert_eq!(shards.len(), 1);
    let markers = shards[0].as_array().unwrap();
    assert_eq!(markers.len(), 100);
    for (index, marker) in markers.iter().enumerate() {
        let sequence = 103 - index as u64;
        assert_eq!(marker["sequence"], sequence);
        assert_eq!(marker["next_record"], (sequence + 1) * 3);
    }
    let retained = &report["after_retention"];
    assert_eq!(retained["per_shard"], expected(1, 101, 104)?);
    assert_eq!(retained["bookmarks"][0].as_array().unwrap().len(), 3);
    assert_eq!(retained["bookmarks"][0][0]["next_record"], 312);
    assert_eq!(retained["bookmarks"][0][2]["next_record"], 306);
    let reopened = inspect(&directory.0, 1, None)?;
    assert_eq!(reopened["per_shard"], retained["per_shard"]);
    assert_eq!(reopened["bookmarks"], retained["bookmarks"]);
    Ok(())
}

#[test]
fn isolated_peer_queue_preserves_healthy_quorum_without_resuming_slow_peer() -> Result<()> {
    let _serial = SERIAL.lock().unwrap_or_else(|error| error.into_inner());
    let directory = TestDirectory::new()?;
    let slow_dir = directory.0.join("slow");
    let fast_dir = directory.0.join("fast");
    let leader_dir = directory.0.join("leader");
    let mut slow =
        Node::start_with_options(&slow_dir, 1, &[], &["--replication-delay-ms", "1500"])?;
    let mut fast = Node::start(&fast_dir, 1, &[])?;
    let mut leader = Node::start_with_options(
        &leader_dir,
        1,
        &[&slow.address, &fast.address],
        &["--peer-admission", "isolate"],
    )?;
    assert_eq!(slow.readiness["replication_delay_ms"], 1500);
    assert_eq!(fast.readiness["peer_admission"], "block");
    assert_eq!(leader.readiness["peer_admission"], "isolate");
    assert_eq!(leader.readiness["peer_queue_capacity"], 4);
    let output = bench(&leader.address, 1, 0, 16)?;
    let report = parse(&output)?;
    ensure!(
        output.status.success(),
        "isolated benchmark failed: {report}"
    );
    assert_eq!(report["per_shard"], expected(1, 0, 16)?);
    ensure!(
        report["elapsed_seconds"].as_f64().unwrap() < 1.5,
        "healthy majority waited for delayed follower admission: {report}"
    );
    let status = successful(&["status", "--address", &leader.address])?;
    assert_eq!(status["peer_admission"], "isolate");
    let peers = status["replication_peers"].as_array().unwrap();
    let isolated = peers
        .iter()
        .find(|peer| peer["address"] == slow.address)
        .unwrap();
    let healthy = peers
        .iter()
        .find(|peer| peer["address"] == fast.address)
        .unwrap();
    assert_eq!(isolated["shard"], 0);
    assert_eq!(isolated["accepting"], false);
    assert_eq!(healthy["accepting"], true);
    let slow_status = successful(&["status", "--address", &slow.address])?;
    assert_eq!(slow_status["replication_delay_ms"], 1500);
    let slow_applied = slow_status["per_shard"][0]["batches"].as_u64().unwrap();
    ensure!(
        (1..=5).contains(&slow_applied),
        "slow follower did not persist a bounded prefix"
    );

    fast.stop()?;
    let refused = bench(&leader.address, 1, 16, 1)?;
    let error = parse(&refused)?;
    assert!(!refused.status.success());
    assert!(
        error["error"]
            .as_str()
            .unwrap()
            .contains("quorum unavailable")
    );
    assert_eq!(error["acknowledged_per_shard"], expected(1, 0, 0)?);
    leader.stop()?;
    slow.stop()?;
    assert_eq!(
        inspect(&leader_dir, 1, None)?["per_shard"],
        expected(1, 0, 17)?
    );
    assert_eq!(
        inspect(&fast_dir, 1, None)?["per_shard"],
        expected(1, 0, 16)?
    );
    let slow_inspection = inspect(&slow_dir, 1, None)?;
    let drained = slow_inspection["per_shard"][0]["batches"].as_u64().unwrap();
    ensure!(
        (slow_applied..=5).contains(&drained),
        "isolated peer resumed or lost its admitted prefix"
    );
    assert_eq!(slow_inspection["per_shard"], expected(1, 0, drained)?);
    assert_eq!(slow_inspection["bookmarks"][0][0]["sequence"], drained - 1);
    assert_eq!(
        slow_inspection["bookmarks"][0][0]["next_record"],
        drained * 3
    );
    Ok(())
}

#[test]
fn three_nodes_refuse_missing_majority_and_preserve_attempted_tail() -> Result<()> {
    let _serial = SERIAL.lock().unwrap_or_else(|error| error.into_inner());
    let directory = TestDirectory::new()?;
    let leader_dir = directory.0.join("leader");
    let a_dir = directory.0.join("a");
    let b_dir = directory.0.join("b");
    let mut a = Node::start(&a_dir, 2, &[])?;
    let mut b = Node::start(&b_dir, 2, &[])?;
    let mut leader = Node::start(&leader_dir, 2, &[&a.address, &b.address])?;
    let baseline = bench(&leader.address, 2, 0, 4)?;
    ensure!(
        baseline.status.success(),
        "baseline failed: {}",
        parse(&baseline)?
    );
    assert_eq!(parse(&baseline)?["per_shard"], expected(2, 0, 4)?);
    wait_applied(&a.address, &expected(2, 0, 4)?)?;
    wait_applied(&b.address, &expected(2, 0, 4)?)?;
    a.stop()?;

    let degraded = bench(&leader.address, 2, 4, 4)?;
    ensure!(
        degraded.status.success(),
        "one-follower quorum failed: {}",
        parse(&degraded)?
    );
    assert_eq!(parse(&degraded)?["per_shard"], expected(2, 4, 8)?);
    b.stop()?;
    let unavailable = bench(&leader.address, 1, 8, 1)?;
    assert!(!unavailable.status.success());
    let error = parse(&unavailable)?;
    assert_eq!(error["errors"], 1);
    assert_eq!(error["acknowledged_per_shard"], expected(1, 0, 0)?);
    assert_eq!(
        error["acknowledged_latencies_us"].as_array().unwrap().len(),
        0
    );
    assert!(
        error["error"]
            .as_str()
            .unwrap()
            .contains("quorum unavailable")
    );
    assert!(error["records_per_second"].is_null());
    let again = bench(&leader.address, 1, 9, 1)?;
    assert!(!again.status.success());
    leader.stop()?;

    let leader_audit = inspect(&leader_dir, 2, None)?;
    assert_eq!(leader_audit["per_shard"][0], expected(1, 0, 9)?[0]);
    assert_eq!(leader_audit["per_shard"][1], expected(2, 0, 8)?[1]);
    assert_eq!(inspect(&a_dir, 2, None)?["per_shard"], expected(2, 0, 4)?);
    assert_eq!(inspect(&b_dir, 2, None)?["per_shard"], expected(2, 0, 8)?);
    Ok(())
}

#[test]
fn quorum_return_does_not_cancel_or_reorder_a_slow_durable_follower() -> Result<()> {
    let _serial = SERIAL.lock().unwrap_or_else(|error| error.into_inner());
    let directory = TestDirectory::new()?;
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let slow_address = listener.local_addr()?.to_string();
    let slow_directory = directory.0.join("slow");
    let (release, gate) = mpsc::channel();
    let slow = thread::spawn(move || -> Result<Audit> {
        let mut store = poc_storage::open(Engine::Segment, &slow_directory)?;
        let (mut stream, _) = listener.accept()?;
        stream.set_read_timeout(Some(Duration::from_secs(10)))?;
        stream.set_write_timeout(Some(Duration::from_secs(10)))?;
        for sequence in 0..15 {
            let mut length = [0; 4];
            stream.read_exact(&mut length)?;
            let length = u32::from_be_bytes(length) as usize;
            ensure!((1..=1024).contains(&length), "unexpected test frame size");
            let mut request = vec![0; length];
            stream.read_exact(&mut request)?;
            assert_eq!(request[0], 3);
            let batch = Batch::decode(&request[1..])?;
            assert_eq!(batch.sequence, sequence);
            store.append(&batch)?;
            if sequence == 0 {
                gate.recv_timeout(Duration::from_secs(10))?;
            }
            thread::sleep(Duration::from_millis(5));
            let response = serde_json::to_vec(&serde_json::json!({
                "type": "ack", "shard": 0, "sequence": sequence,
            }))?;
            stream.write_all(&(response.len() as u32).to_be_bytes())?;
            stream.write_all(&response)?;
        }
        store.audit()
    });
    let mut fast = Node::start(&directory.0.join("fast"), 1, &[])?;
    let mut leader = Node::start(
        &directory.0.join("leader"),
        1,
        &[&fast.address, &slow_address],
    )?;
    let output = bench(&leader.address, 1, 0, 3)?;
    ensure!(
        output.status.success(),
        "quorum should not wait for slow follower: {}",
        parse(&output)?
    );
    release.send(())?;
    let queued = bench(&leader.address, 1, 3, 12)?;
    ensure!(
        queued.status.success(),
        "bounded queues must preserve the slow follower: {}",
        parse(&queued)?
    );
    leader.stop()?;
    fast.stop()?;
    let slow_audit = slow
        .join()
        .map_err(|_| anyhow::anyhow!("slow follower panicked"))??;
    assert_eq!(serde_json::to_value(slow_audit)?, expected(1, 0, 15)?[0]);
    Ok(())
}

#[test]
fn invalid_cli_is_nonzero_json_without_success_fields() -> Result<()> {
    let output = Command::new(BINARY)
        .args([
            "bench",
            "--address",
            "127.0.0.1:1",
            "--shards",
            "1",
            "--batch-records",
            "0",
            "--record-bytes",
            "16",
            "--seconds",
            "1",
            "--max-batches",
            "1",
        ])
        .output()?;
    assert!(!output.status.success());
    let error = parse(&output)?;
    assert_eq!(error["errors"], 1);
    assert!(error["elapsed_seconds"].is_null());
    Ok(())
}
