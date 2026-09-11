use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use anyhow::{Context, Result, ensure};
use poc_common::{Audit, Batch};
use serde::Serialize;
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::{Duration, timeout};

use crate::cli::BenchOptions;
use crate::wire::{self, APPEND, CLIENT_TIMEOUT};

#[derive(Debug, Serialize)]
pub struct Latency {
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub max: f64,
}

#[derive(Debug, Serialize)]
pub struct Report {
    pub elapsed_seconds: f64,
    pub records: u64,
    pub payload_bytes: u64,
    pub batches: u64,
    pub records_per_second: f64,
    pub mib_per_second: f64,
    pub latency_us: Latency,
    pub per_shard: Vec<Audit>,
    pub latencies_us: Vec<f64>,
    pub errors: u64,
    pub throughput_definition: &'static str,
    pub latency_definition: &'static str,
    pub latency_sample_order: &'static str,
}

#[derive(Debug, Serialize)]
pub struct FailureReport {
    pub error: String,
    pub errors: usize,
    pub acknowledged_per_shard: Vec<Audit>,
    pub acknowledged_latencies_us: Vec<f64>,
}

impl std::fmt::Display for FailureReport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.error)
    }
}

impl std::error::Error for FailureReport {}

struct ShardResult {
    shard: u32,
    audit: Audit,
    latencies: Vec<f64>,
    last_ack: Instant,
    error: Option<String>,
}

async fn client(
    mut stream: TcpStream,
    shard: u32,
    options: Arc<BenchOptions>,
    mut start: watch::Receiver<Option<Instant>>,
    failed: Arc<AtomicBool>,
) -> Result<ShardResult> {
    start
        .changed()
        .await
        .context("benchmark start was cancelled")?;
    let started = start.borrow().context("benchmark start time was not set")?;
    let mut result = ShardResult {
        shard,
        audit: Audit::default(),
        latencies: Vec::with_capacity(options.max_batches as usize),
        last_ack: started,
        error: None,
    };
    let writes: Result<()> = async {
        for offset in 0..options.max_batches {
            if failed.load(Ordering::Acquire)
                || started.elapsed() >= Duration::from_secs_f64(options.seconds)
            {
                break;
            }
            let sequence = options.start_sequence + offset;
            let batch =
                Batch::generate(shard, sequence, options.batch_records, options.record_bytes)?;
            let request = wire::batch_request(APPEND, &batch);
            let before = Instant::now();
            let response = timeout(CLIENT_TIMEOUT, wire::exchange(&mut stream, &request))
                .await
                .context("client acknowledgement timed out; append outcome may be unknown")??;
            wire::require_ack(response, shard, sequence)?;
            let acknowledged = Instant::now();
            result
                .latencies
                .push((acknowledged - before).as_secs_f64() * 1_000_000.0);
            result.last_ack = acknowledged;
            result.audit.observe(&batch);
        }
        Ok(())
    }
    .await;
    if let Err(error) = writes {
        failed.store(true, Ordering::Release);
        result.error = Some(format!("shard {shard}: {error:#}"));
    }
    Ok(result)
}

pub fn summarize(samples: &[f64]) -> Latency {
    if samples.is_empty() {
        return Latency {
            p50: 0.0,
            p95: 0.0,
            p99: 0.0,
            max: 0.0,
        };
    }
    let mut sorted = samples.to_vec();
    sorted.sort_by(f64::total_cmp);
    let percentile = |percent: usize| {
        let rank = (sorted.len() * percent).div_ceil(100);
        sorted[rank.saturating_sub(1)]
    };
    Latency {
        p50: percentile(50),
        p95: percentile(95),
        p99: percentile(99),
        max: sorted[sorted.len() - 1],
    }
}

pub async fn run(options: BenchOptions) -> Result<Report> {
    options.validate()?;
    let options = Arc::new(options);
    let mut streams = Vec::new();
    for shard in 0..options.shards {
        streams.push((
            shard,
            wire::connect(options.address.as_str())
                .await
                .with_context(|| format!("connecting shard {shard} before benchmark start"))?,
        ));
    }
    let (start, started) = watch::channel(None);
    let failed = Arc::new(AtomicBool::new(false));
    let mut clients = JoinSet::new();
    for (shard, stream) in streams {
        let options = options.clone();
        let started = started.clone();
        let failed = failed.clone();
        clients.spawn(async move {
            let result = client(stream, shard, options, started, failed.clone()).await;
            if result.is_err() {
                failed.store(true, Ordering::Release);
            }
            result
        });
    }
    let before = Instant::now();
    start.send(Some(before))?;
    let mut results = Vec::new();
    let mut errors = Vec::new();
    while let Some(result) = clients.join_next().await {
        match result {
            Ok(Ok(result)) => {
                if let Some(error) = &result.error {
                    errors.push(error.clone());
                }
                results.push(result);
            }
            Ok(Err(error)) => errors.push(format!("{error:#}")),
            Err(error) => {
                failed.store(true, Ordering::Release);
                errors.push(format!("benchmark client task failed: {error}"));
            }
        }
    }
    results.sort_by_key(|result| result.shard);
    let finished = results
        .iter()
        .map(|result| result.last_ack)
        .max()
        .unwrap_or(before);
    let elapsed_seconds = (finished - before).as_secs_f64();
    let mut per_shard = vec![Audit::default(); options.shards as usize];
    let mut latencies_us = Vec::new();
    for result in results {
        per_shard[result.shard as usize] = result.audit;
        latencies_us.extend(result.latencies);
    }
    if !errors.is_empty() {
        return Err(FailureReport {
            error: format!(
                "benchmark failed without retries; attempted writes may exist beyond acknowledged data: {}",
                errors.join("; ")
            ),
            errors: errors.len(),
            acknowledged_per_shard: per_shard,
            acknowledged_latencies_us: latencies_us,
        }
        .into());
    }
    ensure!(
        elapsed_seconds > 0.0,
        "duration expired before any request was acknowledged"
    );
    let records = per_shard.iter().map(|audit| audit.records).sum();
    let payload_bytes = per_shard.iter().map(|audit| audit.payload_bytes).sum();
    let batches = per_shard.iter().map(|audit| audit.batches).sum();
    Ok(Report {
        elapsed_seconds,
        records,
        payload_bytes,
        batches,
        records_per_second: records as f64 / elapsed_seconds,
        mib_per_second: payload_bytes as f64 / (1024.0 * 1024.0) / elapsed_seconds,
        latency_us: summarize(&latencies_us),
        per_shard,
        latencies_us,
        errors: 0,
        throughput_definition: "acknowledged records or payload MiB / wall seconds from synchronized release to final acknowledgement; includes payload generation and encoding, excludes connection setup",
        latency_definition: "microseconds from sending each pre-generated binary request to its matching acknowledgement; nearest-rank percentiles",
        latency_sample_order: "shard index, then request sequence",
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentiles_use_nearest_rank_without_off_by_one() {
        let samples: Vec<f64> = (1..=100).rev().map(f64::from).collect();
        let result = summarize(&samples);
        assert_eq!(
            (result.p50, result.p95, result.p99, result.max),
            (50.0, 95.0, 99.0, 100.0)
        );
        let one = summarize(&[7.5]);
        assert_eq!((one.p50, one.p95, one.p99, one.max), (7.5, 7.5, 7.5, 7.5));
        assert_eq!(summarize(&[1.0, 2.0, 3.0]).p50, 2.0);
        assert_eq!(summarize(&[]).max, 0.0);
    }

    #[tokio::test]
    async fn failed_benchmark_preserves_only_acknowledged_evidence() -> Result<()> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?.to_string();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            for sequence in 0..2 {
                let request = wire::read_frame(&mut stream)
                    .await?
                    .context("missing request")?;
                let batch = Batch::decode(&request[1..])?;
                ensure!(
                    batch.sequence == sequence,
                    "client retried or reordered a batch"
                );
                let reply = if sequence == 0 {
                    wire::Response::Ack { shard: 0, sequence }
                } else {
                    wire::Response::Error {
                        error: "quorum unavailable".to_owned(),
                    }
                };
                wire::write_frame(&mut stream, &serde_json::to_vec(&reply)?).await?;
            }
            Ok::<(), anyhow::Error>(())
        });
        let error = run(BenchOptions {
            address,
            shards: 1,
            batch_records: 2,
            record_bytes: 31,
            seconds: 5.0,
            max_batches: 2,
            start_sequence: 0,
        })
        .await
        .expect_err("a refused write must fail the benchmark");
        server.await??;
        let failure = error
            .downcast_ref::<FailureReport>()
            .context("missing failure evidence")?;
        let mut expected = Audit::default();
        expected.observe(&Batch::generate(0, 0, 2, 31)?);
        assert_eq!(failure.acknowledged_per_shard, vec![expected]);
        assert_eq!(failure.acknowledged_latencies_us.len(), 1);
        assert_eq!(failure.errors, 1);
        assert!(serde_json::to_value(failure)?["records_per_second"].is_null());
        Ok(())
    }
}
