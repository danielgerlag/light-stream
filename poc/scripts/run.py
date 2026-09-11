#!/usr/bin/env python3
"""Run bounded, serial streaming POC trials and retain their raw evidence."""

import argparse
import csv
import hashlib
import json
import math
import os
import platform
import random
import shutil
import statistics
import subprocess
import sys
import threading
import time
import uuid
import zlib
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
MASK64 = (1 << 64) - 1


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def run_command(command, timeout=120):
    result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, timeout=timeout)
    if result.returncode != 0:
        raise RuntimeError(
            f"Command failed ({result.returncode}): {command}\n{result.stdout}\n{result.stderr}"
        )
    return result.stdout.strip()


def command_json(command, path, timeout=120):
    started = time.monotonic()
    result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, timeout=timeout)
    write_json(path.with_suffix(".command.json"), {
        "command": [str(arg) for arg in command],
        "returncode": result.returncode,
        "wall_seconds": time.monotonic() - started,
        "stderr": result.stderr,
    })
    path.write_text(result.stdout)
    if result.returncode != 0:
        raise RuntimeError(f"Command failed; see {path} and {path.with_suffix('.command.json')}")
    value = json.loads(result.stdout)
    if not isinstance(value, dict):
        raise ValueError(f"Expected a JSON object in {path}")
    return value


def fingerprint(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def environment(binary):
    sources = [ROOT / "Cargo.toml", ROOT / "Cargo.lock", ROOT / ".cargo/config.toml"]
    sources.extend(sorted((ROOT / "poc").rglob("*.rs")))
    sources.extend(sorted((ROOT / "poc").rglob("*.toml")))
    sources.extend(sorted((ROOT / "poc").rglob("*.md")))
    sources.extend(sorted((ROOT / "poc" / "scripts").glob("*.py")))
    host = {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "logical_cpus": os.cpu_count(),
        "load_average_before": list(os.getloadavg()),
        "filesystem": run_command(["df", "-h", str(ROOT)]),
        "disk_free_bytes_before": shutil.disk_usage(ROOT).free,
        "python": sys.version,
        "rustc": run_command(["rustc", "--version"]),
        "cargo": run_command(["cargo", "--version"]),
        "binary_sha256": fingerprint(binary),
        "binary_bytes": binary.stat().st_size,
        "source_sha256": {str(p.relative_to(ROOT)): fingerprint(p) for p in sources},
        "node_placement": "separate processes, shared host and filesystem, loopback TCP",
        "replication": "fixed leader, local durable append plus one of two durable followers",
        "cache_policy": "OS caches not cleared",
    }
    if platform.system() == "Darwin":
        host["cpu_model"] = run_command(["sysctl", "-n", "machdep.cpu.brand_string"])
        host["memory_bytes"] = int(run_command(["sysctl", "-n", "hw.memsize"]))
    return host


def snapshot_sources(host, output):
    destination = output / "source"
    for relative, expected in host["source_sha256"].items():
        source = ROOT / relative
        if fingerprint(source) != expected:
            raise RuntimeError(f"source changed during evidence capture: {relative}")
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
        if fingerprint(target) != expected:
            raise RuntimeError(f"source snapshot checksum mismatch: {relative}")


class Node:
    def __init__(self, binary, engine, directory, log_directory, label, shards, peers=(),
                 peer_admission=None, replication_delay_ms=0):
        self.directory = directory
        self.label = label
        self.stdout_path = log_directory / f"{label}.stdout.log"
        self.stderr_path = log_directory / f"{label}.stderr.log"
        self.stdout = self.stdout_path.open("w")
        self.stderr = self.stderr_path.open("w")
        self.command = [
            str(binary), "node", "--engine", engine, "--dir", str(directory),
            "--listen", "127.0.0.1:0", "--shards", str(shards),
        ]
        if peers:
            self.command.extend(["--peers", ",".join(peers)])
        if peer_admission is not None:
            self.command.extend(["--peer-admission", peer_admission])
        if replication_delay_ms:
            self.command.extend(["--replication-delay-ms", str(replication_delay_ms)])
        self.started = time.monotonic()
        self.process = subprocess.Popen(
            self.command, cwd=ROOT, stdout=self.stdout, stderr=self.stderr
        )
        self.address = None
        try:
            while time.monotonic() - self.started < 60:
                lines = self.stdout_path.read_text().splitlines()
                if lines:
                    ready = json.loads(lines[0])
                    self.address = ready["address"]
                    self.ready_seconds = time.monotonic() - self.started
                    break
                if self.process.poll() is not None:
                    raise RuntimeError(f"{label} exited: {self.stderr_path.read_text()}")
                time.sleep(0.02)
            if self.address is None:
                raise TimeoutError(f"{label} did not announce readiness")
        except BaseException:
            self.stop()
            raise

    def stop(self, crash=False):
        if self.process.poll() is None:
            if crash:
                self.process.kill()
            else:
                self.process.terminate()
            try:
                self.process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=15)
        self.stdout.close()
        self.stderr.close()

    def description(self):
        return {
            "label": self.label, "pid": self.process.pid, "address": self.address,
            "ready_seconds": self.ready_seconds, "command": self.command,
        }


class MemorySampler:
    def __init__(self, nodes):
        self.pids = [node.process.pid for node in nodes]
        self.samples = []
        self.errors = []
        self.done = threading.Event()
        self.thread = threading.Thread(target=self.sample, daemon=True)

    def sample(self):
        while not self.done.is_set():
            result = subprocess.run(
                ["ps", "-o", "pid=,rss=", "-p", ",".join(map(str, self.pids))],
                capture_output=True, text=True,
            )
            if result.returncode == 0:
                rss = {}
                for line in result.stdout.splitlines():
                    pid, kib = line.split()
                    rss[pid] = int(kib) * 1024
                self.samples.append({"monotonic": time.monotonic(), "rss_bytes": rss})
            else:
                self.errors.append({"returncode": result.returncode, "stderr": result.stderr})
            self.done.wait(0.1)

    def start(self):
        self.thread.start()

    def stop(self):
        self.done.set()
        self.thread.join(timeout=10)
        if self.thread.is_alive():
            raise RuntimeError("memory sampler failed to stop")

    def peak(self):
        if not self.samples:
            return None
        return max(sum(sample["rss_bytes"].values()) for sample in self.samples)


def wait_for_audits(binary, nodes, expected, output):
    deadline = time.monotonic() + 60
    last = {}
    while time.monotonic() < deadline:
        complete = True
        for node in nodes:
            value = json.loads(run_command([str(binary), "status", "--address", node.address], 10))
            last[node.label] = value
            if value["per_shard"] != expected:
                complete = False
        if complete:
            write_json(output, last)
            return
        time.sleep(0.05)
    write_json(output, last)
    raise AssertionError(f"Followers did not match acknowledged input; see {output}")


def check_bookmarks(bookmarks, audit, batch_records):
    expected_count = min(100, audit["batches"])
    if len(bookmarks) != expected_count:
        raise AssertionError(f"Expected {expected_count} recent bookmarks, got {len(bookmarks)}")
    for index, bookmark in enumerate(bookmarks):
        sequence = audit["last_sequence"] - index
        if bookmark != {"sequence": sequence, "next_record": (sequence + 1) * batch_records}:
            raise AssertionError(f"Incorrect bookmark {bookmark} at reverse position {index}")


def disk_footprint(path):
    files = [p.stat() for p in path.rglob("*") if p.is_file()]
    return {
        "file_bytes": sum(item.st_size for item in files),
        "allocated_bytes": sum(item.st_blocks * 512 for item in files),
        "files": len(files),
    }


def trial(binary, case, engine, repeat, args, output, scratch):
    trial_id = f"{case['name']}-{engine}-r{repeat}"
    directory = output / "trials" / trial_id
    directory.mkdir(parents=True)
    data = scratch / trial_id
    data.mkdir()
    nodes = []
    sampler = None
    failed = False
    try:
        peers = []
        if case["replicas"] == 3:
            for i in range(2):
                follower = Node(binary, engine, data / f"follower-{i}", directory,
                                f"follower-{i}", case["shards"])
                nodes.append(follower)
                peers.append(follower.address)
        leader = Node(binary, engine, data / "leader", directory, "leader", case["shards"], peers)
        nodes.append(leader)
        write_json(directory / "nodes.json", [node.description() for node in nodes])
        command_json([str(binary), "status", "--address", leader.address],
                     directory / "ready-status.json")
        max_batches = max(1, int(
            args.max_mib * 1024 * 1024
            / (case["shards"] * case["batch_records"] * case["record_bytes"])
        ))
        if args.matrix == "smoke":
            max_batches = min(max_batches, 8)
        if "max_batches" in case:
            max_batches = min(max_batches, case["max_batches"])
        command = [
            str(binary), "bench", "--address", leader.address,
            "--shards", str(case["shards"]), "--batch-records", str(case["batch_records"]),
            "--record-bytes", str(case["record_bytes"]), "--seconds", str(args.seconds),
            "--max-batches", str(max_batches),
        ]
        sampler = MemorySampler(nodes)
        sampler.start()
        bench = command_json(command, directory / "bench.json", timeout=args.seconds + 120)
        sampler.stop()
        write_json(directory / "memory.json", {
            "samples": sampler.samples, "errors": sampler.errors,
            "peak_summed_node_rss_bytes": sampler.peak(),
        })
        if bench["errors"] != 0 or bench["batches"] < 1:
            raise AssertionError("benchmark reported errors or no acknowledged batches")
        expected = bench["per_shard"]
        if len(expected) != case["shards"]:
            raise AssertionError("benchmark returned the wrong number of shards")
        if sum(item["records"] for item in expected) != bench["records"]:
            raise AssertionError("acknowledged records do not sum to benchmark total")
        if len(bench["latencies_us"]) != bench["batches"]:
            raise AssertionError("raw latency count differs from acknowledged batch count")
        wait_for_audits(binary, nodes, expected, directory / "drained-status.json")
        for node in reversed(nodes):
            node.stop()
        inspections = []
        for node in nodes:
            inspection = command_json([
                str(binary), "inspect", "--engine", engine, "--dir", str(node.directory),
                "--shards", str(case["shards"]),
            ], directory / f"{node.label}.inspect.json")
            if inspection["per_shard"] != expected:
                raise AssertionError(f"{node.label} persisted data differs from acknowledged data")
            if len(inspection["bookmarks"]) != case["shards"]:
                raise AssertionError("inspection returned the wrong number of bookmark lists")
            for markers, audit in zip(inspection["bookmarks"], expected):
                check_bookmarks(markers, audit, case["batch_records"])
            inspections.append(inspection)
        footprint = disk_footprint(data)
        retain_from = min(item["batches"] for item in expected) // 2
        retention = command_json([
            str(binary), "inspect", "--engine", engine, "--dir", str(leader.directory),
            "--shards", str(case["shards"]), "--retain-from", str(retain_from),
        ], directory / "retention.json")
        after = retention["after_retention"]
        if isinstance(after, dict):
            after = after["per_shard"]
        if len(after) != case["shards"]:
            raise AssertionError("retention audit returned the wrong number of shards")
        for old, retained in zip(expected, after):
            remaining = old["batches"] - retain_from
            if (retained["batches"] != remaining
                    or retained["records"] != remaining * case["batch_records"]
                    or retained["first_sequence"] != retain_from
                    or retained["last_sequence"] != old["last_sequence"]):
                raise AssertionError("retention did not preserve the expected sequence interval")
        reopened_retention = command_json([
            str(binary), "inspect", "--engine", engine, "--dir", str(leader.directory),
            "--shards", str(case["shards"]),
        ], directory / "retention-reopened.json")
        if reopened_retention["per_shard"] != after:
            raise AssertionError("retention state changed after reopen")
        for markers, audit in zip(reopened_retention["bookmarks"], after):
            check_bookmarks(markers, audit, case["batch_records"])
        result = {
            "trial": trial_id, "case": case, "engine": engine, "repeat": repeat,
            "verdict": "VERIFIED", "bench": {k: v for k, v in bench.items() if k != "latencies_us"},
            "peak_summed_node_rss_bytes": sampler.peak(), "disk": footprint,
            "reopen_seconds": [item["reopen_seconds"] for item in inspections],
            "audit_seconds": [item["audit_seconds"] for item in inspections],
            "retention_from": retain_from,
            "source": f"trials/{trial_id}/bench.json",
        }
        write_json(directory / "result.json", result)
        return result
    except BaseException as error:
        failed = True
        if sampler is not None:
            if sampler.thread.is_alive():
                sampler.stop()
            write_json(directory / "memory.json", {
                "samples": sampler.samples, "errors": sampler.errors,
                "peak_summed_node_rss_bytes": sampler.peak(),
            })
        write_json(directory / "failure.json", {
            "verdict": "NOT VERIFIED", "error": str(error),
            "retained_data_directory": str(data),
        })
        raise
    finally:
        if sampler is not None and sampler.thread.is_alive():
            sampler.stop()
        for node in reversed(nodes):
            node.stop()
        if not args.keep_data and not failed:
            if data.parent != scratch or data.is_symlink():
                raise RuntimeError(f"Refusing cleanup of unexpected path {data}")
            shutil.rmtree(data)


def rotate_left(value, amount):
    amount %= 64
    return ((value << amount) | (value >> ((64 - amount) % 64))) & MASK64


def combine(a, b):
    if a["last_sequence"] + 1 != b["first_sequence"]:
        raise AssertionError("failure workload did not continue the acknowledged sequence")
    return {
        "batches": a["batches"] + b["batches"],
        "records": a["records"] + b["records"],
        "payload_bytes": a["payload_bytes"] + b["payload_bytes"],
        "digest": rotate_left(a["digest"], 7 * b["batches"]) ^ b["digest"],
        "first_sequence": a["first_sequence"], "last_sequence": b["last_sequence"],
    }


def generated_checksum(shard, sequence, records, record_bytes):
    length = records * record_bytes
    if not 0 < length <= 8 * 1024 * 1024:
        raise ValueError("invalid payload length")
    state = (sequence + 0x9e3779b97f4a7c15 + shard * 0xd1b54a32d192ed03) & MASK64
    payload = bytearray()
    while len(payload) < length:
        state ^= state >> 12
        state ^= (state << 25) & MASK64
        state ^= state >> 27
        word = ((state * 0x2545f4914f6cdd1d) & MASK64).to_bytes(8, "little")
        payload.extend(word[:min(8, length - len(payload))])
    return zlib.crc32(payload)


def check_acknowledged_prefix(actual, acknowledged, shard, batch_records, record_bytes):
    if acknowledged["batches"] == 0:
        raise AssertionError("failure experiment must first establish an acknowledged prefix")
    if (actual["first_sequence"] != acknowledged["first_sequence"]
            or actual["batches"] < acknowledged["batches"]
            or actual["last_sequence"] != actual["first_sequence"] + actual["batches"] - 1):
        raise AssertionError("acknowledged sequence prefix is missing after crash")
    expected = dict(acknowledged)
    for sequence in range(acknowledged["last_sequence"] + 1, actual["last_sequence"] + 1):
        expected["digest"] = (
            rotate_left(expected["digest"], 7)
            ^ generated_checksum(shard, sequence, batch_records, record_bytes)
            ^ ((sequence * 0x9e3779b97f4a7c15) & MASK64)
            ^ shard
        )
        expected["batches"] += 1
        expected["records"] += batch_records
        expected["payload_bytes"] += batch_records * record_bytes
        expected["last_sequence"] = sequence
    if actual != expected:
        raise AssertionError("persisted prefix digest or acknowledged record contents differ")


def check_quorum_rejection(returncode, stdout):
    if returncode != 1:
        raise AssertionError(f"expected explicit write rejection, received exit {returncode}")
    rejected = json.loads(stdout)
    if rejected["errors"] < 1 or "quorum" not in rejected["error"].lower():
        raise AssertionError("failure did not identify an unavailable quorum")
    acknowledgements = rejected["acknowledged_per_shard"]
    if len(acknowledgements) != 1 or any(item["batches"] != 0 for item in acknowledgements):
        raise AssertionError("a write was acknowledged without a majority")


def failures(binary, engine, args, output, scratch):
    directory = output / f"failure-{engine}"
    directory.mkdir()
    data = scratch / f"failure-{engine}"
    data.mkdir()
    nodes = []
    failed = False
    try:
        for i in range(2):
            nodes.append(Node(binary, engine, data / f"follower-{i}", directory, f"follower-{i}", 1))
        leader = Node(binary, engine, data / "leader", directory, "leader", 1,
                      [node.address for node in nodes])
        nodes.append(leader)
        write_json(directory / "nodes.json", [node.description() for node in nodes])

        def bench(name, start, batches):
            return command_json([
                str(binary), "bench", "--address", leader.address, "--shards", "1",
                "--batch-records", "128", "--record-bytes", "1024",
                "--seconds", "10", "--max-batches", str(batches),
                "--start-sequence", str(start),
            ], directory / f"{name}.json", timeout=120)

        first = bench("healthy", 0, 16)["per_shard"][0]
        wait_for_audits(binary, nodes, [first], directory / "healthy-drained.json")
        nodes[0].stop(crash=True)
        second = bench("one-follower-lost", first["batches"], 16)["per_shard"][0]
        expected = combine(first, second)
        wait_for_audits(binary, nodes[1:], [expected], directory / "degraded-drained.json")
        nodes[1].stop(crash=True)
        command = [
            str(binary), "bench", "--address", leader.address, "--shards", "1",
            "--batch-records", "128", "--record-bytes", "1024",
            "--seconds", "1", "--max-batches", "1", "--start-sequence", str(expected["batches"]),
        ]
        rejected = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, timeout=30)
        write_json(directory / "majority-lost.json", {
            "command": command, "returncode": rejected.returncode,
            "stdout": rejected.stdout, "stderr": rejected.stderr,
        })
        check_quorum_rejection(rejected.returncode, rejected.stdout)
        leader.stop(crash=True)
        recovered = {}
        for index, node in enumerate(nodes):
            inspection = command_json([
                str(binary), "inspect", "--engine", engine,
                "--dir", str(node.directory), "--shards", "1",
            ], directory / f"{node.label}.crash-inspect.json")
            audit = inspection["per_shard"][0]
            minimum = first if index == 0 else expected
            check_acknowledged_prefix(audit, minimum, 0, 128, 1024)
            recovered[node.label] = audit
        result = {
            "verdict": "VERIFIED", "engine": engine,
            "one_follower_loss": "writes continued with durable local and surviving follower",
            "majority_loss": "write returned nonzero",
            "process_kill_recovery": recovered,
            "acknowledged_prefix": expected,
            "unacknowledged_leader_tail_batches": recovered["leader"]["batches"] - expected["batches"],
            "limitations": ["no leader election", "no fencing", "no replica catch-up",
                            "shared physical host", "not a hardware power-loss experiment"],
        }
        write_json(directory / "result.json", result)
        return result
    except BaseException as error:
        failed = True
        write_json(directory / "failure.json", {
            "verdict": "NOT VERIFIED", "error": str(error),
            "retained_data_directory": str(data),
        })
        raise
    finally:
        for node in reversed(nodes):
            node.stop()
        if not args.keep_data and not failed:
            if data.parent != scratch or data.is_symlink():
                raise RuntimeError(f"Refusing cleanup of unexpected path {data}")
            shutil.rmtree(data)


def slow_peer(binary, engine, policy, repeat, args, output, scratch):
    name = f"slow-{engine}-{policy}-r{repeat}"
    directory = output / "trials" / name
    directory.mkdir(parents=True)
    data = scratch / name
    data.mkdir()
    nodes = []
    failed = False
    try:
        slow = Node(binary, engine, data / "slow", directory, "slow", 1,
                    replication_delay_ms=250)
        nodes.append(slow)
        fast = Node(binary, engine, data / "fast", directory, "fast", 1)
        nodes.append(fast)
        leader = Node(binary, engine, data / "leader", directory, "leader", 1,
                      [slow.address, fast.address], peer_admission=policy)
        nodes.append(leader)
        write_json(directory / "nodes.json", [node.description() for node in nodes])
        command = [
            str(binary), "bench", "--address", leader.address, "--shards", "1",
            "--batch-records", "128", "--record-bytes", "1024",
            "--seconds", "20", "--max-batches", "24",
        ]
        bench = command_json(command, directory / "bench.json", 60)
        if bench["batches"] != 24 or bench["errors"] != 0:
            raise AssertionError("controlled slow-peer workload did not complete its 24 batches")
        expected = bench["per_shard"]
        wait_for_audits(binary, [leader, fast], expected, directory / "majority-status.json")
        if policy == "block":
            wait_for_audits(binary, nodes, expected, directory / "all-status.json")
        else:
            fast.stop(crash=True)
            rejected = subprocess.run(
                command[:-1] + ["1", "--start-sequence", "24"],
                cwd=ROOT, capture_output=True, text=True, timeout=30,
            )
            write_json(directory / "remaining-majority-lost.json", {
                "returncode": rejected.returncode, "stdout": rejected.stdout, "stderr": rejected.stderr,
            })
            check_quorum_rejection(rejected.returncode, rejected.stdout)
        for node in reversed(nodes):
            node.stop()
        recovered = {}
        for node in nodes:
            inspection = command_json([
                str(binary), "inspect", "--engine", engine,
                "--dir", str(node.directory), "--shards", "1",
            ], directory / f"{node.label}.inspect.json")
            recovered[node.label] = inspection["per_shard"][0]
        check_acknowledged_prefix(recovered["leader"], expected[0], 0, 128, 1024)
        if recovered["fast"] != expected[0]:
            raise AssertionError("fast durable follower does not contain all acknowledged input")
        if policy == "block" and recovered["slow"] != expected[0]:
            raise AssertionError("blocking control did not drain its slow follower")
        if policy == "isolate" and recovered["slow"]["batches"] >= expected[0]["batches"]:
            raise AssertionError("experiment did not exercise slow-follower isolation")
        result = {
            "verdict": "VERIFIED", "trial": name, "engine": engine,
            "peer_admission": policy, "repeat": repeat, "injected_ack_delay_ms": 250,
            "bench": {key: value for key, value in bench.items() if key != "latencies_us"},
            "recovered": recovered,
            "limits": "isolation permanently excludes the lagging POC peer; production requires catch-up",
        }
        write_json(directory / "result.json", result)
        return result
    except BaseException as error:
        failed = True
        write_json(directory / "failure.json", {
            "verdict": "NOT VERIFIED", "error": str(error), "retained_data_directory": str(data),
        })
        raise
    finally:
        for node in reversed(nodes):
            node.stop()
        if not args.keep_data and not failed:
            if data.parent != scratch or data.is_symlink():
                raise RuntimeError(f"Refusing cleanup of unexpected path {data}")
            shutil.rmtree(data)


def cases(matrix):
    if matrix == "smoke":
        return [
            {"name": f"smoke-r{r}", "replicas": r, "shards": 2,
             "batch_records": 16, "record_bytes": 1024}
            for r in (1, 3)
        ]
    if matrix == "main":
        return [
            {"name": f"r{r}-s{s}-b{b}-z1024", "replicas": r, "shards": s,
             "batch_records": b, "record_bytes": 1024}
            for r in (1, 3) for s, b in ((1, 1), (1, 128), (4, 128))
        ]
    if matrix == "scale":
        return [
            {"name": f"r3-s{s}-b{b}-z1024", "replicas": 3, "shards": s,
             "batch_records": b, "record_bytes": 1024}
            for s in (1, 2, 4, 8) for b in (128, 512)
        ]
    if matrix == "sizes":
        return [
            {"name": f"r3-s4-b128-z{size}", "replicas": 3, "shards": 4,
             "batch_records": 128, "record_bytes": size}
            for size in (64, 1024, 16384)
        ]
    if matrix == "density":
        return [
            {"name": "r1-s1-b1-z64-bookmarks100k", "replicas": 1, "shards": 1,
             "batch_records": 1, "record_bytes": 64, "max_batches": 100000}
        ]
    if matrix == "sustained":
        return [
            {"name": "r3-s8-b512-z1024-sustained", "replicas": 3, "shards": 8,
             "batch_records": 512, "record_bytes": 1024}
        ]
    raise ValueError(f"unknown matrix {matrix}")


def summarize(results, output):
    grouped = {}
    for result in results:
        grouped.setdefault((result["case"]["name"], result["engine"]), []).append(result)
    rows = []
    for (case, engine), trials in sorted(grouped.items()):
        rates = [item["bench"]["records_per_second"] for item in trials]
        rss = [item["peak_summed_node_rss_bytes"] for item in trials
               if item["peak_summed_node_rss_bytes"] is not None]
        rows.append({
            "case": case, "engine": engine, "trials": len(trials),
            "records_per_second_median": statistics.median(rates),
            "records_per_second_min": min(rates), "records_per_second_max": max(rates),
            "mib_per_second_median": statistics.median(item["bench"]["mib_per_second"] for item in trials),
            "trial_p99_us_median": statistics.median(item["bench"]["latency_us"]["p99"] for item in trials),
            "elapsed_seconds_min": min(item["bench"]["elapsed_seconds"] for item in trials),
            "batches_min": min(item["bench"]["batches"] for item in trials),
            "node_rss_mib_peak": max(rss) / 1048576 if rss else None,
            "verdict": "VERIFIED",
        })
    if not rows:
        return
    with (output / "summary.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    lines = [
        "# Measured results", "",
        "Medians across repeated trials. Latency is the median of each trial's p99.",
        "These are same-host fixed-leader POCs, not independent-host Raft results.", "",
        "| Case | Engine | Trials | Records/s median | MiB/s median | p99 us | RPS min | RPS max |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in rows:
        lines.append(
            f"| {row['case']} | {row['engine']} | {row['trials']} | "
            f"{row['records_per_second_median']:.0f} | {row['mib_per_second_median']:.2f} | "
            f"{row['trial_p99_us_median']:.0f} | {row['records_per_second_min']:.0f} | "
            f"{row['records_per_second_max']:.0f} |"
        )
    (output / "summary.md").write_text("\n".join(lines) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--matrix", choices=["smoke", "main", "scale", "sizes", "density",
                                           "sustained", "failure", "slow-peer"],
                        default="main")
    parser.add_argument("--engines", default="segment,redb,fjall,rocksdb")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--seconds", type=float, default=2)
    parser.add_argument("--max-mib", type=int, default=128)
    parser.add_argument("--seed", type=int, default=73191)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--binary", type=Path, default=ROOT / "target/release/stream-poc")
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--keep-data", action="store_true")
    args = parser.parse_args()
    if args.repeats < 1 or not math.isfinite(args.seconds) or args.seconds <= 0 or args.max_mib < 1:
        parser.error("repeats, seconds, and max-mib must be positive")
    engines = args.engines.split(",")
    if not engines or any(engine not in {"segment", "redb", "fjall", "rocksdb"} for engine in engines):
        parser.error("engines must be a comma-separated subset of segment,redb,fjall,rocksdb")
    if len(set(engines)) != len(engines):
        parser.error("duplicate engines would overwrite trial evidence")
    binary = args.binary.resolve()
    if not args.skip_build and binary != (ROOT / "target/release/stream-poc").resolve():
        parser.error("a custom binary requires --skip-build; its source correspondence is unverified")
    output = args.output.resolve()
    if output.exists():
        parser.error(f"refusing to overwrite evidence directory {output}")
    output.mkdir(parents=True)
    if not args.skip_build:
        command = ["cargo", "build", "--release", "-p", "stream-poc", "--locked", "--jobs", "4"]
        built = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, timeout=900)
        write_json(output / "build.json", {
            "command": command, "returncode": built.returncode,
            "stdout": built.stdout, "stderr": built.stderr,
        })
        if built.returncode != 0:
            raise RuntimeError(f"release build failed; see {output / 'build.json'}")
    if not binary.is_file():
        parser.error(f"missing binary {binary}")
    scratch = ROOT / ".poc-data" / uuid.uuid4().hex
    scratch.mkdir(parents=True)
    host = environment(binary)
    host["binary_source_relation"] = (
        "unverified: build skipped" if args.skip_build else "Cargo release build completed before capture"
    )
    write_json(output / "environment.json", host)
    snapshot_sources(host, output)
    write_json(output / "arguments.json", {key: str(value) if isinstance(value, Path) else value
                                         for key, value in vars(args).items()})
    results = []
    try:
        if args.matrix == "failure":
            for engine in engines:
                results.append(failures(binary, engine, args, output, scratch))
        elif args.matrix == "slow-peer":
            schedule = [(engine, policy, repeat) for repeat in range(1, args.repeats + 1)
                        for engine in engines for policy in ("block", "isolate")]
            random.Random(args.seed).shuffle(schedule)
            write_json(output / "schedule.json", schedule)
            for engine, policy, repeat in schedule:
                print(f"slow follower: {engine} {policy} repeat={repeat}", flush=True)
                result = slow_peer(binary, engine, policy, repeat, args, output, scratch)
                results.append(result)
                print(f"  {result['bench']['elapsed_seconds']:.3f}s for 24 durable batches", flush=True)
        else:
            schedule = [(case, engine, repeat) for repeat in range(1, args.repeats + 1)
                        for case in cases(args.matrix) for engine in engines]
            random.Random(args.seed).shuffle(schedule)
            write_json(output / "schedule.json", schedule)
            for index, (case, engine, repeat) in enumerate(schedule, 1):
                print(f"[{index}/{len(schedule)}] {case['name']} {engine} repeat={repeat}", flush=True)
                result = trial(binary, case, engine, repeat, args, output, scratch)
                results.append(result)
                summarize(results, output)
                print(f"  {result['bench']['records_per_second']:.0f} records/s; persisted audit matched",
                      flush=True)
        write_json(output / "run.json", {"verdict": "VERIFIED", "completed": len(results),
                                        "finished_at": datetime.now(timezone.utc).isoformat()})
    except BaseException as error:
        write_json(output / "run.json", {"verdict": "NOT VERIFIED", "completed": len(results),
                                        "error": str(error)})
        raise
    finally:
        if not args.keep_data and not any(scratch.iterdir()):
            scratch.rmdir()


if __name__ == "__main__":
    main()
