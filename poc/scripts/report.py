#!/usr/bin/env python3
"""Generate the POC evidence report from successful and failed trial artifacts."""

import argparse
import json
import statistics
from pathlib import Path

from run import ROOT


def read(path):
    return json.loads(path.read_text())


def trials(directory):
    return [read(path) for path in sorted((directory / "trials").glob("*/result.json"))]


def median(items, field):
    return statistics.median(item["bench"][field] for item in items)


def p99_ms(items):
    return statistics.median(item["bench"]["latency_us"]["p99"] for item in items) / 1000


def group(items, name, engine):
    selected = [item for item in items if item["case"]["name"] == name and item["engine"] == engine]
    if len(selected) != 3:
        raise ValueError(f"{name}/{engine} requires three successful trials, found {len(selected)}")
    return selected


def slow_peer_section(directory):
    status = read(directory / "run.json")
    if status["verdict"] != "VERIFIED" or status["completed"] != 6:
        raise ValueError("slow-peer comparison requires all six successful trials")
    observations = trials(directory)
    lines = [
        "## Controlled slow-minority experiment", "",
        "One real follower delays acknowledgements by 250 ms after durable storage.",
        "Each trial appends the same 24 batches of 128 1-KiB records through three logical nodes.",
        "The block control waits for capacity in every peer queue.",
        "The isolate alternative permanently excludes a peer when its bounded queue fills, while still requiring local durability and the fast follower.", "",
        "| Admission | Trials | Median elapsed seconds | Median records/s | Median trial p99 ms | Slow follower stored batches, min-max |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    by_policy = {}
    for policy in ("block", "isolate"):
        items = [item for item in observations if item["peer_admission"] == policy]
        if len(items) != 3:
            raise ValueError(f"slow-peer policy {policy} requires three trials")
        by_policy[policy] = items
        counts = [item["recovered"]["slow"]["batches"] for item in items]
        lines.append(f"| {policy} | 3 | {median(items, 'elapsed_seconds'):.3f} | "
                     f"{median(items, 'records_per_second'):,.0f} | {p99_ms(items):.2f} | "
                     f"{min(counts)}-{max(counts)} |")
    ratio = median(by_policy["block"], "elapsed_seconds") / median(by_policy["isolate"], "elapsed_seconds")
    lines += [
        "", f"The isolate POC completed this controlled workload **{ratio:.2f}x** faster.",
        "Both the leader and fast follower were reopened and matched the acknowledged payloads.",
        "After isolation, removing the fast follower produced an explicit quorum error with zero new acknowledgements.",
        "The slow follower does not receive the complete interval under isolation. It is explicitly degraded, not a third fully caught-up copy.",
        "Permanent exclusion is only the POC mechanism. Production needs independent replication cursors and catch-up from durable history.",
        "This demonstrates the queue-coupling defect. It does not establish the cause of the earlier intermittent sustained Fjall failure.", "",
    ]
    return lines


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "docs/experiments/high-volume.md")
    parser.add_argument("--slow-peer-run", type=Path)
    args = parser.parse_args()
    main_dir = ROOT / "evidence/main-v2"
    scale_dir = ROOT / "evidence/scale-v1"
    sizes_dir = ROOT / "evidence/message-sizes"
    for path, count in [(main_dir, 72), (scale_dir, 48), (sizes_dir, 18)]:
        status = read(path / "run.json")
        if status["verdict"] != "VERIFIED" or status["completed"] != count:
            raise ValueError(f"incomplete primary comparison {path}")
    main_trials, scale_trials, size_trials = map(trials, (main_dir, scale_dir, sizes_dir))
    host = read(main_dir / "environment.json")
    sustained_directories = [
        ROOT / "evidence/sustained-v1",
        ROOT / "evidence/sustained-repro",
        ROOT / "evidence/sustained-rocks-completion",
    ]
    sustained = [item for directory in sustained_directories for item in trials(directory)]
    failed = []
    for directory in sustained_directories:
        for failure in (directory / "trials").glob("*/failure.json"):
            bench = read(failure.parent / "bench.json")
            failed.append({
                "engine": "fjall" if "-fjall-" in failure.parent.name else "rocksdb",
                "error": bench["error"], "file": str(failure.relative_to(ROOT)),
            })
    if len(sustained) != 5 or len(failed) != 1:
        raise ValueError("sustained report expects all five successful attempts and the failed attempt")
    fjall_base = group(scale_trials, "r3-s1-b128-z1024", "fjall")
    fjall_scale = group(scale_trials, "r3-s8-b512-z1024", "fjall")
    rocks_scale = group(scale_trials, "r3-s8-b512-z1024", "rocksdb")
    lines = [
        "# High-volume POC evidence", "",
        "Captured on 2026-09-10. These are real Rust storage and TCP replication experiments, not a shipping broker.", "",
        "## Outcome", "",
        "The evidence no longer supports one installation-wide redb writer as the high-volume default.",
        "Batched, independent writer paths outperform that small-installation proposal in the tested configurations.",
        "Fjall and RocksDB are the storage finalists. The custom segment POC's per-batch manifest synchronization is expensive.", "",
        f"Fjall rose from **{median(fjall_base, 'records_per_second'):,.0f} records/s** with one shard and 128-record batches",
        f"to **{median(fjall_scale, 'records_per_second'):,.0f} records/s** with eight shards and 512-record batches.",
        f"That is **{median(fjall_scale, 'records_per_second') / median(fjall_base, 'records_per_second'):.2f}x** across those configurations, not an isolated sharding effect.", "",
        "**A sustained Fjall attempt failed.** Two Fjall attempts completed and one lost the fixed leader's acknowledgement quorum.",
        "The failure remains part of the result. Successful-run throughput is not evidence of sustained reliability.", "",
        "RocksDB is the recommended baseline for the next high-volume iteration.",
        f"At eight shards and 512-record batches it delivered **{100 * median(rocks_scale, 'records_per_second') / median(fjall_scale, 'records_per_second'):.1f}%** of Fjall's median throughput,",
        f"with median trial p99 **{p99_ms(rocks_scale):.2f} ms**, versus **{p99_ms(fjall_scale):.2f} ms** for Fjall.",
        "Fjall remains a useful challenger, not the default selected from throughput alone.", "",
        "## What ran", "",
        "| Experiment | Completed successful trials | Other outcomes |",
        "| --- | --- | --- |",
        "| Four engines, single/batched writes, one/four shards, one/three nodes | 72 | Three repeats per configuration. |",
        "| Fjall/RocksDB, one/two/four/eight shards, 128/512-record batches, three nodes | 48 | Three repeats per configuration. |",
        "| 64-byte, 1-KiB, and 16-KiB records with three nodes | 18 | Three repeats per engine and size. |",
        "| 30-second sustained attempts with eight shards and three nodes | 5 | One failed attempt; the interrupted schedule is not represented as complete. |",
        "| Follower loss, majority loss, process-kill recovery | 4 engine scenarios | See the dedicated failure evidence and its gate revision. |", "",
        f"Host: **{host.get('cpu_model', host['machine'])}**, **{host['logical_cpus']} logical CPUs**,",
        f"**{host['memory_bytes'] / 1073741824:.0f} GiB RAM**, `{host['rustc']}`.",
        f"Platform: `{host['platform']}`.", "",
        "Nodes are separate OS processes with separate stores over loopback TCP.",
        "They share one physical host and filesystem. No independent-host throughput or fault-domain claim follows.",
        "The comparison executable uses a fixed leader, not Raft. It has no election, fencing, or automatic replica catch-up.", "",
        "## Four storage implementations", "",
        "Three nodes, four shards, 128 records per batch, 1 KiB per record. Medians of three trials.", "",
        "| Engine | Records/s | Payload MiB/s | Median trial p99, ms per batch | Peak summed node RSS, MiB |",
        "| --- | --- | --- | --- | --- |",
    ]
    for engine in ("segment", "redb", "fjall", "rocksdb"):
        items = group(main_trials, "r3-s4-b128-z1024", engine)
        rss = max(item["peak_summed_node_rss_bytes"] for item in items) / 1048576
        lines.append(f"| {engine} | {median(items, 'records_per_second'):,.0f} | "
                     f"{median(items, 'mib_per_second'):.2f} | {p99_ms(items):.2f} | {rss:.1f} |")
    lines += [
        "", "The segment implementation synchronizes data, a replacement manifest, and the directory for each batch.",
        "Its result applies to that conservative implementation, not to all possible segment logs.",
        "redb enables immediate durability and quick-repair. Fjall and RocksDB use synchronous journals with compression disabled.",
        "All four atomically store the payload and an after-batch positional marker.", "",
        "## Batching and independent writer paths", "",
        "Three nodes and 1-KiB records. Each shard also adds one concurrent producer.", "",
        "| Shards | Records per batch | Fjall records/s | Fjall p99 ms | RocksDB records/s | RocksDB p99 ms |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for shard in (1, 2, 4, 8):
        for batch in (128, 512):
            name = f"r3-s{shard}-b{batch}-z1024"
            f = group(scale_trials, name, "fjall")
            r = group(scale_trials, name, "rocksdb")
            lines.append(f"| {shard} | {batch} | {median(f, 'records_per_second'):,.0f} | "
                         f"{p99_ms(f):.2f} | {median(r, 'records_per_second'):,.0f} | {p99_ms(r):.2f} |")
    lines += [
        "", "This is whole-configuration scaling. Shard count and client concurrency change together.",
        "Larger batches amortize persistence costs but alter request latency and memory use.",
        "These client-supplied batches do not measure a server-side group-commit batching timer.", "",
        "## Record size changes the ranking", "",
        "Three nodes, four shards, 128 records per batch.", "",
        "| Record bytes | Fjall records/s | Fjall MiB/s | RocksDB records/s | RocksDB MiB/s |",
        "| --- | --- | --- | --- | --- |",
    ]
    for size in (64, 1024, 16384):
        name = f"r3-s4-b128-z{size}"
        f = group(size_trials, name, "fjall")
        r = group(size_trials, name, "rocksdb")
        lines.append(f"| {size} | {median(f, 'records_per_second'):,.0f} | "
                     f"{median(f, 'mib_per_second'):.2f} | {median(r, 'records_per_second'):,.0f} | "
                     f"{median(r, 'mib_per_second'):.2f} |")
    lines += [
        "", "## Sustained attempts and the failure", "",
        "The longer workload uses eight shards, 512-record batches, three nodes, a 30-second limit, and a 3-GiB logical byte cap.",
        "Per-shard traffic exceeds the configured memory buffers and exercises LSM flushes.",
        "This is longer ingestion, not a steady-state concurrent-retention workload.", "",
        "| Engine | Successful attempts | Failed attempts | Successful-run median records/s | Successful-run median p99 ms |",
        "| --- | --- | --- | --- | --- |",
    ]
    for engine in ("fjall", "rocksdb"):
        items = [item for item in sustained if item["engine"] == engine]
        failures = [item for item in failed if item["engine"] == engine]
        lines.append(f"| {engine} | {len(items)} | {len(failures)} | "
                     f"{median(items, 'records_per_second'):,.0f} | {p99_ms(items):.2f} |")
    lines += [
        "", "The failed Fjall attempt stopped after about 7.43 seconds near 39 MiB of acknowledged payload per shard.",
        "Successful acknowledgements before the failure reached 4.81 seconds.",
        "The fixed leader timed out follower exchanges and permanently disabled the affected peer workers.",
        "The logs establish timeout-driven loss of the acknowledgement quorum, not a specific storage-engine or hardware root cause.",
        "A fresh 30-second Fjall attempt completed, so the failure is intermittent in this evidence.", "",
        "The original failed attempt did not retain its scratch stores or memory samples.",
        "Its acknowledged-prefix survival was not audited after failure. Later orchestration preserves failed data and memory.",
        "The two successful Fjall durations do not erase that missing evidence.", "",
        "## Durability normalization and rejected evidence", "",
        "The first smoke run made RocksDB appear dramatically faster because its native build used plain macOS `fsync`.",
        "Rust's synchronization used `F_FULLFSYNC`.",
        "The workspace now enables RocksDB's existing `HAVE_FULLFSYNC` path on Apple targets.",
        "Both the build flag and the linked executable's full-sync call are captured.",
        "No engine's synchronization was weakened to improve its result.", "",
        "The first main run also stopped on a stale executable's old bookmark limit.",
        "The orchestrator now builds before each run by default.",
        "Neither rejected directory contributes to the tables above.", "",
        "## What the evidence does and does not establish", "",
        "The main, scaling, size, and successful sustained trials compare client-expected batch counts and digests with bytes reopened from all three replicas or the standalone store.",
        "The scanner decodes checksums, regenerates deterministic payloads for byte comparison, and verifies absolute marker offsets.",
        "Those storage trials also apply retention and reopen the retained state.",
        "The marker is positional. Human names, independent bookmark lifetimes, leases, and consumer progress are not implemented.", "",
        "Latency is closed-loop, not an open-loop overload SLO. p99 is per batch, not per individual record.",
        "Main trials are about two seconds. Scale trials target five seconds. Byte caps and actual sample counts remain in raw files.",
        "Caches are not cleared. Replay is warm-cache validation and throughput, not a cold-storage result.",
        "Retention runs after ingest. Database file size is not device write amplification.",
        "Summed RSS can count shared pages repeatedly, and excludes the benchmark client's memory.",
        "OS synchronization and process-kill recovery do not prove hardware power-loss survival.", "",
        "## Reproduce and inspect", "",
        "- [POC commands and limits](../../poc/README.md).",
        "- [Main raw evidence and summary](../../evidence/main-v2/summary.md).",
        "- [Shard/batch scaling evidence](../../evidence/scale-v1/summary.md).",
        "- [Message-size evidence](../../evidence/message-sizes/summary.md).",
        "- [Interrupted sustained run](../../evidence/sustained-v1/run.json).",
        "- [Fresh Fjall sustained attempt](../../evidence/sustained-repro/summary.md).",
        "- [Third RocksDB sustained attempt](../../evidence/sustained-rocks-completion/summary.md).",
        "- [Follower/majority failure scenarios with stronger gates](../../evidence/failures-v2/run.json).",
        "- [Synchronization investigation](../../evidence/durability-review.md).",
        "- [Decision trail](../../evidence/decisions.tsv).",
        "- [Independent review and remaining limits](../../evidence/review.md).",
        "- [High-volume design direction](../design/proposal.md).", "",
        ("Regenerate this report with `python3 poc/scripts/report.py --slow-peer-run evidence/slow-peer-v1`."
         if args.slow_peer_run is not None else
         "Regenerate this report with `python3 poc/scripts/report.py`."),
    ]
    if args.slow_peer_run is not None:
        index = lines.index("| Follower loss, majority loss, process-kill recovery | 4 engine scenarios | See the dedicated failure evidence and its gate revision. |")
        lines.insert(index, "| Controlled slow-peer block/isolate comparison | 6 | Three repeats per admission policy. |")
        index = lines.index("## Durability normalization and rejected evidence")
        lines[index:index] = slow_peer_section(args.slow_peer_run.resolve())
        index = lines.index("- [Decision trail](../../evidence/decisions.tsv).")
        lines.insert(index, "- [Controlled isolation raw results](../../evidence/slow-peer-v1/run.json).")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text("\n".join(lines) + "\n")
    print(args.output.relative_to(ROOT))


if __name__ == "__main__":
    main()
