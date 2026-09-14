#!/usr/bin/env python3

import argparse
import json
import shutil
import statistics
import subprocess
import time
import uuid
from pathlib import Path

from verify import OwnedServer, VerificationError, write_json


ROOT = Path(__file__).resolve().parents[1]


def run(command, timeout=30):
    result = subprocess.run(
        [str(value) for value in command],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if result.returncode != 0:
        raise VerificationError(
            f"command exited {result.returncode}: {command}\n{result.stdout}\n{result.stderr}"
        )
    return json.loads(result.stdout)


def percentile(values, percentile_value):
    ordered = sorted(values)
    index = ((percentile_value * len(ordered) + 99) // 100) - 1
    return ordered[index]


def trial(label, server_binary, cli_binary, artifacts, index, operations):
    data_dir = artifacts / "scratch" / f"{label}-{index}"
    server = OwnedServer(
        server_binary,
        data_dir,
        artifacts / "node-logs",
        f"{label}-{index}",
    )
    cluster = str(uuid.uuid4())
    stream = str(uuid.uuid4())
    session = str(uuid.uuid4())
    endpoint = f"http://{server.ready['public_address']}"
    try:
        run(
            [
                cli_binary,
                "--endpoint",
                endpoint,
                "cluster",
                "bootstrap",
                "--cluster-id",
                cluster,
                "--stream-id",
                stream,
                "--stream-name",
                "perf",
            ],
            timeout=60,
        )
        publish_seconds = []
        for sequence in range(1, operations + 1):
            started = time.monotonic()
            run(
                [
                    cli_binary,
                    "--endpoint",
                    endpoint,
                    "publish",
                    "--cluster-id",
                    cluster,
                    "--stream-id",
                    stream,
                    "--partition",
                    "0",
                    "--principal",
                    "perf",
                    "--session",
                    session,
                    "--sequence",
                    str(sequence),
                    "--payload",
                    "x" * 1024,
                ]
            )
            publish_seconds.append(time.monotonic() - started)
        fetch_seconds = []
        for _ in range(20):
            started = time.monotonic()
            value = run(
                [
                    cli_binary,
                    "--endpoint",
                    endpoint,
                    "fetch",
                    "--cluster-id",
                    cluster,
                    "--stream-id",
                    stream,
                    "--partition",
                    "0",
                    "--offset",
                    "0",
                    "--limit",
                    str(operations),
                ]
            )
            fetch_seconds.append(time.monotonic() - started)
            if len(value["page"]["records"]) != operations:
                raise VerificationError(f"{label} fetch returned the wrong record count")
        return {
            "publish_throughput": operations / sum(publish_seconds),
            "publish_p99_seconds": percentile(publish_seconds, 99),
            "fetch_p99_seconds": percentile(fetch_seconds, 99),
            "publish_samples_seconds": publish_seconds,
            "fetch_samples_seconds": fetch_seconds,
        }
    finally:
        server.stop()


def summarize(trials):
    return {
        "publish_throughput_median": statistics.median(
            trial_value["publish_throughput"] for trial_value in trials
        ),
        "publish_p99_median_seconds": statistics.median(
            trial_value["publish_p99_seconds"] for trial_value in trials
        ),
        "fetch_p99_median_seconds": statistics.median(
            trial_value["fetch_p99_seconds"] for trial_value in trials
        ),
        "trials": trials,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline-server", required=True, type=Path)
    parser.add_argument("--baseline-cli", required=True, type=Path)
    parser.add_argument("--head-server", required=True, type=Path)
    parser.add_argument("--head-cli", required=True, type=Path)
    parser.add_argument("--artifacts", required=True, type=Path)
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--operations", type=int, default=50)
    args = parser.parse_args()
    args.artifacts.mkdir(parents=True, exist_ok=False)
    baseline_trials = []
    head_trials = []
    for index in range(args.trials):
        baseline_trials.append(
            trial(
                "baseline",
                args.baseline_server,
                args.baseline_cli,
                args.artifacts,
                index,
                args.operations,
            )
        )
        head_trials.append(
            trial(
                "head",
                args.head_server,
                args.head_cli,
                args.artifacts,
                index,
                args.operations,
            )
        )
    baseline = summarize(baseline_trials)
    head = summarize(head_trials)
    throughput_ratio = (
        head["publish_throughput_median"] / baseline["publish_throughput_median"]
    )
    publish_p99_ratio = (
        head["publish_p99_median_seconds"] / baseline["publish_p99_median_seconds"]
    )
    fetch_p99_ratio = (
        head["fetch_p99_median_seconds"] / baseline["fetch_p99_median_seconds"]
    )
    verdict = (
        "PASS"
        if throughput_ratio >= 0.90
        and publish_p99_ratio <= 1.20
        and fetch_p99_ratio <= 1.20
        else "FAIL"
    )
    result = {
        "verdict": verdict,
        "baseline": baseline,
        "head": head,
        "ratios": {
            "publish_throughput": throughput_ratio,
            "publish_p99": publish_p99_ratio,
            "fetch_p99": fetch_p99_ratio,
        },
        "limits": {
            "minimum_throughput_ratio": 0.90,
            "maximum_p99_ratio": 1.20,
        },
        "operations_per_trial": args.operations,
    }
    write_json(args.artifacts / "result.json", result)
    if verdict != "PASS":
        raise VerificationError(f"LS05 B4 comparison failed: {result['ratios']}")
    shutil.rmtree(args.artifacts / "scratch")


if __name__ == "__main__":
    main()
