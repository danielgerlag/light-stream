#!/usr/bin/env python3
"""Run release verification for LS01 through LS07 and retain evidence."""

import argparse
import base64
import hashlib
import json
import os
import platform
import random
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import unittest
import uuid
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
KNOWN_SCENARIOS = {
    "all",
    "bootstrap",
    "durable-publish-read-restart",
    "retry-receipt",
    "three-voter",
    "bounded-groups",
    "bookmarks",
    "retention-replay",
    "b7-snapshot",
    "membership",
    "health-capabilities",
    "unsupported-publish",
    "process-isolation",
    "evidence-preservation",
}
PRODUCTION_PACKAGES = (
    "light-stream-server",
    "light-stream-cli",
    "light-stream-testkit",
)


class VerificationError(RuntimeError):
    pass


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def append_jsonl(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as file:
        file.write(json.dumps(value, sort_keys=True) + "\n")


def source_paths():
    paths = [
        ROOT / ".cargo" / "config.toml",
        ROOT / ".gitignore",
        ROOT / "Cargo.toml",
        ROOT / "Cargo.lock",
        ROOT / "README.md",
        ROOT / "docs" / "architecture" / "runtime.md",
        ROOT / "artifacts" / "LS02b" / "design" / "synthesis.md",
        ROOT / "artifacts" / "LS02b" / "index.md",
    ]
    suffixes = {".json", ".md", ".proto", ".py", ".rs", ".toml"}
    for directory in (
        ROOT / "crates",
        ROOT / "poc",
        ROOT / "proto",
        ROOT / "scripts",
        ROOT / "verification",
        ROOT / "docs" / "api",
        ROOT / ".github" / "skills" / "verify-light-stream",
    ):
        if directory.exists():
            paths.extend(
                path
                for path in directory.rglob("*")
                if path.is_file()
                and path.suffix in suffixes
                and "__pycache__" not in path.parts
            )
    return sorted(set(path for path in paths if path.is_file()))


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def source_fingerprint():
    digest = hashlib.sha256()
    fingerprints = {}
    for path in source_paths():
        relative = path.relative_to(ROOT).as_posix()
        file_digest = sha256_file(path)
        fingerprints[relative] = file_digest
        digest.update(relative.encode())
        digest.update(b"\0")
        digest.update(file_digest.encode())
        digest.update(b"\0")
    return digest.hexdigest(), fingerprints


class CommandRunner:
    def __init__(self, artifacts):
        self.artifacts = artifacts
        self.commands_path = artifacts / "commands.jsonl"
        self.counter = 0

    def run(self, command, name, expected_codes=(0,), timeout=300, env=None):
        self.counter += 1
        started = time.monotonic()
        result = subprocess.run(
            [str(item) for item in command],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
        )
        stdout_path = self.artifacts / "command-output" / f"{self.counter:03d}-{name}.stdout.log"
        stderr_path = self.artifacts / "command-output" / f"{self.counter:03d}-{name}.stderr.log"
        stdout_path.parent.mkdir(parents=True, exist_ok=True)
        stdout_path.write_text(result.stdout)
        stderr_path.write_text(result.stderr)
        append_jsonl(
            self.commands_path,
            {
                "command": [str(item) for item in command],
                "duration_seconds": time.monotonic() - started,
                "expected_codes": list(expected_codes),
                "name": name,
                "returncode": result.returncode,
                "stderr": str(stderr_path.relative_to(self.artifacts)),
                "stdout": str(stdout_path.relative_to(self.artifacts)),
            },
        )
        if result.returncode not in expected_codes:
            raise VerificationError(
                f"{name} exited {result.returncode}; see {stdout_path} and {stderr_path}"
            )
        return result

    def run_dropped_response(self, command, name, expected_codes=(0,), timeout=300):
        self.counter += 1
        started = time.monotonic()
        stderr_path = self.artifacts / "command-output" / f"{self.counter:03d}-{name}.stderr.log"
        stderr_path.parent.mkdir(parents=True, exist_ok=True)
        with stderr_path.open("w") as stderr:
            result = subprocess.run(
                [str(item) for item in command],
                cwd=ROOT,
                stdout=subprocess.DEVNULL,
                stderr=stderr,
                timeout=timeout,
            )
        append_jsonl(
            self.commands_path,
            {
                "command": [str(item) for item in command],
                "duration_seconds": time.monotonic() - started,
                "expected_codes": list(expected_codes),
                "name": name,
                "response_observed": False,
                "returncode": result.returncode,
                "stderr": str(stderr_path.relative_to(self.artifacts)),
                "stdout": None,
            },
        )
        if result.returncode not in expected_codes:
            raise VerificationError(
                f"{name} exited {result.returncode}; see {stderr_path}"
            )

    def run_parallel(self, commands, name, expected_codes=(0,), timeout=300):
        reserved = []
        for index, command in enumerate(commands):
            self.counter += 1
            stdout_path = (
                self.artifacts
                / "command-output"
                / f"{self.counter:03d}-{name}-{index:03d}.stdout.log"
            )
            stderr_path = (
                self.artifacts
                / "command-output"
                / f"{self.counter:03d}-{name}-{index:03d}.stderr.log"
            )
            stdout_path.parent.mkdir(parents=True, exist_ok=True)
            reserved.append((command, stdout_path, stderr_path))
        started = time.monotonic()
        processes = []
        try:
            for command, stdout_path, stderr_path in reserved:
                processes.append(
                    (
                        command,
                        stdout_path,
                        stderr_path,
                        subprocess.Popen(
                            [str(item) for item in command],
                            cwd=ROOT,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            text=True,
                        ),
                    )
                )
            results = []
            for command, stdout_path, stderr_path, process in processes:
                remaining = max(0.1, timeout - (time.monotonic() - started))
                stdout, stderr = process.communicate(timeout=remaining)
                stdout_path.write_text(stdout)
                stderr_path.write_text(stderr)
                append_jsonl(
                    self.commands_path,
                    {
                        "command": [str(item) for item in command],
                        "duration_seconds": time.monotonic() - started,
                        "expected_codes": list(expected_codes),
                        "name": name,
                        "returncode": process.returncode,
                        "stderr": str(stderr_path.relative_to(self.artifacts)),
                        "stdout": str(stdout_path.relative_to(self.artifacts)),
                    },
                )
                if process.returncode not in expected_codes:
                    raise VerificationError(
                        f"{name} exited {process.returncode}; see {stdout_path} and {stderr_path}"
                    )
                results.append(
                    subprocess.CompletedProcess(
                        [str(item) for item in command],
                        process.returncode,
                        stdout,
                        stderr,
                    )
                )
            return results
        finally:
            for _, _, _, process in processes:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=5)

    def run_interrupted(self, command, name, delay_seconds, timeout=30):
        self.counter += 1
        stdout_path = (
            self.artifacts / "command-output" / f"{self.counter:03d}-{name}.stdout.log"
        )
        stderr_path = (
            self.artifacts / "command-output" / f"{self.counter:03d}-{name}.stderr.log"
        )
        stdout_path.parent.mkdir(parents=True, exist_ok=True)
        started = time.monotonic()
        process = subprocess.Popen(
            [str(item) for item in command],
            cwd=ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            time.sleep(delay_seconds)
            process.send_signal(signal.SIGINT)
            stdout, stderr = process.communicate(timeout=timeout)
            stdout_path.write_text(stdout)
            stderr_path.write_text(stderr)
            append_jsonl(
                self.commands_path,
                {
                    "command": [str(item) for item in command],
                    "duration_seconds": time.monotonic() - started,
                    "expected_codes": [130],
                    "fault": "sigint",
                    "name": name,
                    "returncode": process.returncode,
                    "stderr": str(stderr_path.relative_to(self.artifacts)),
                    "stdout": str(stdout_path.relative_to(self.artifacts)),
                },
            )
            if process.returncode != 130:
                raise VerificationError(
                    f"{name} exited {process.returncode}; see {stdout_path} and {stderr_path}"
                )
            return subprocess.CompletedProcess(
                [str(item) for item in command],
                process.returncode,
                stdout,
                stderr,
            )
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
        return result


class OwnedServer:
    def __init__(
        self,
        binary,
        data_dir,
        log_dir,
        label,
        node_id=1,
        public_address="127.0.0.1:0",
        peer_address="127.0.0.1:0",
        advertise_public_uri=None,
        advertise_peer_uri=None,
        peer_routes=None,
        max_data_groups=None,
        max_streams=None,
        max_partitions_per_stream=None,
        rocksdb_cache_bytes=None,
        rocksdb_write_buffer_bytes=None,
        verification_delay_group_id=None,
        verification_delay_ms=0,
        verification_response_delay_group_id=None,
        verification_response_delay_ms=0,
        publish_queue_requests=None,
        publish_queue_records=None,
        publish_queue_bytes=None,
        publish_batch_requests=None,
        publish_batch_records=None,
        publish_batch_bytes=None,
        publish_coalesce_us=None,
        ready_timeout_seconds=5,
    ):
        self.data_dir = data_dir
        self.label = label
        self.stdout_path = log_dir / f"{label}.stdout.log"
        self.stderr_path = log_dir / f"{label}.stderr.log"
        self.stdout_path.parent.mkdir(parents=True, exist_ok=True)
        self.stdout = self.stdout_path.open("w")
        self.stderr = self.stderr_path.open("w")
        self.command = [
            str(binary),
            "--data-dir",
            str(data_dir),
            "--public-listen",
            public_address,
            "--peer-listen",
            peer_address,
            "--security-mode",
            "local-insecure",
            "--node-id",
            str(node_id),
        ]
        if advertise_public_uri is not None:
            self.command.extend(["--advertise-public-uri", advertise_public_uri])
        if advertise_peer_uri is not None:
            self.command.extend(["--advertise-peer-uri", advertise_peer_uri])
        for target, uri in sorted((peer_routes or {}).items()):
            self.command.extend(["--peer-route", f"{target}={uri}"])
        if max_data_groups is not None:
            self.command.extend(["--max-data-groups", str(max_data_groups)])
        if max_streams is not None:
            self.command.extend(["--max-streams", str(max_streams)])
        if max_partitions_per_stream is not None:
            self.command.extend(
                ["--max-partitions-per-stream", str(max_partitions_per_stream)]
            )
        if rocksdb_cache_bytes is not None:
            self.command.extend(
                ["--rocksdb-cache-bytes", str(rocksdb_cache_bytes)]
            )
        if rocksdb_write_buffer_bytes is not None:
            self.command.extend(
                ["--rocksdb-write-buffer-bytes", str(rocksdb_write_buffer_bytes)]
            )
        for flag, value in (
            ("--publish-queue-requests", publish_queue_requests),
            ("--publish-queue-records", publish_queue_records),
            ("--publish-queue-bytes", publish_queue_bytes),
            ("--publish-batch-requests", publish_batch_requests),
            ("--publish-batch-records", publish_batch_records),
            ("--publish-batch-bytes", publish_batch_bytes),
            ("--publish-coalesce-us", publish_coalesce_us),
        ):
            if value is not None:
                self.command.extend([flag, str(value)])
        if verification_delay_group_id is not None:
            self.command.extend(
                [
                    "--verification-enable-fault-hooks",
                    "--verification-delay-group-id",
                    str(verification_delay_group_id),
                    "--verification-delay-ms",
                    str(verification_delay_ms),
                ]
            )
        if verification_response_delay_group_id is not None:
            self.command.extend(
                [
                    "--verification-enable-fault-hooks",
                    "--verification-response-delay-group-id",
                    str(verification_response_delay_group_id),
                    "--verification-response-delay-ms",
                    str(verification_response_delay_ms),
                ]
            )
        self.closed = False
        self.ready_timeout_seconds = ready_timeout_seconds
        self.process = subprocess.Popen(
            self.command,
            cwd=ROOT,
            stdout=self.stdout,
            stderr=self.stderr,
        )
        try:
            self.ready = self._wait_ready()
        except (VerificationError, OSError, json.JSONDecodeError):
            if self.process.poll() is None:
                self.process.terminate()
                self.process.wait(timeout=10)
            self.stdout.close()
            self.stderr.close()
            raise

    def _wait_ready(self):
        deadline = time.monotonic() + self.ready_timeout_seconds
        while time.monotonic() < deadline:
            lines = self.stdout_path.read_text().splitlines()
            if lines:
                value = json.loads(lines[0])
                if value.get("ready") is not True:
                    raise VerificationError(f"{self.label} announced a non-ready state")
                return value
            if self.process.poll() is not None:
                raise VerificationError(
                    f"{self.label} exited before readiness: {self.stderr_path.read_text()}"
                )
            time.sleep(0.02)
        raise VerificationError(
            f"{self.label} did not bind both listeners within "
            f"{self.ready_timeout_seconds} seconds"
        )

    def stop(self):
        if self.closed:
            return
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGTERM)
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired as error:
                self.process.kill()
                self.process.wait(timeout=10)
                raise VerificationError(f"{self.label} ignored graceful shutdown") from error
        self.stdout.close()
        self.stderr.close()
        self.closed = True
        if self.process.returncode != 0:
            raise VerificationError(
                f"{self.label} exited {self.process.returncode}; see {self.stderr_path}"
            )

    def kill(self):
        if self.closed:
            return
        if self.process.poll() is None:
            self.process.kill()
            self.process.wait(timeout=10)
        self.stdout.close()
        self.stderr.close()
        self.closed = True

    def is_running(self):
        return self.process.poll() is None

    def description(self):
        return {
            "command": self.command,
            "data_directory": str(self.data_dir),
            "label": self.label,
            "peer_address": self.ready["peer_address"],
            "pid": self.process.pid,
            "public_address": self.ready["public_address"],
            "revision": self.ready["revision"],
        }


def free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def allocated_tree_bytes(path):
    total = 0
    if not path.exists():
        return total
    for root, _, files in os.walk(path):
        for name in files:
            try:
                total += (Path(root) / name).stat().st_blocks * 512
            except FileNotFoundError:
                pass
    return total


class ProcessRssSampler:
    def __init__(self, artifacts, nodes):
        self.path = artifacts / "resources" / "rss.jsonl"
        self.nodes = nodes
        self.samples = []
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.started = False

    def start(self):
        self.started = True
        self.thread.start()

    def stop(self):
        if not self.started:
            return
        self.stop_event.set()
        self.thread.join(timeout=5)
        if self.thread.is_alive():
            raise VerificationError("RSS sampler did not stop")

    def _run(self):
        while not self.stop_event.is_set():
            values = {}
            for node_id, node in list(self.nodes.items()):
                server = node.get("server")
                if server is None or not server.is_running():
                    continue
                result = subprocess.run(
                    ["ps", "-o", "rss=", "-p", str(server.process.pid)],
                    capture_output=True,
                    text=True,
                )
                if result.returncode == 0 and result.stdout.strip():
                    values[str(node_id)] = int(result.stdout.strip()) * 1024
            sample = {"captured_at": utc_now(), "rss_bytes": values}
            self.samples.append(sample)
            append_jsonl(self.path, sample)
            self.stop_event.wait(0.25)

    def peak_by_node(self):
        node_ids = {
            node_id
            for sample in self.samples
            for node_id in sample["rss_bytes"]
        }
        return {
            node_id: max(
                sample["rss_bytes"].get(node_id, 0) for sample in self.samples
            )
            for node_id in sorted(node_ids)
        }

    def peak_delta_by_node(self):
        peaks = self.peak_by_node()
        first = {}
        for sample in self.samples:
            for node_id, value in sample["rss_bytes"].items():
                first.setdefault(node_id, value)
        return {
            node_id: peak - first.get(node_id, peak)
            for node_id, peak in peaks.items()
        }


class OneShotResponseDropProxy:
    def __init__(self, listen_port, upstream_port):
        self.listen_port = listen_port
        self.upstream_port = upstream_port
        self.dropped_application_response = False
        self.error = None
        self.ready = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()
        if not self.ready.wait(timeout=5):
            raise VerificationError("response-drop proxy did not bind")

    @property
    def endpoint(self):
        return f"http://127.0.0.1:{self.listen_port}"

    def _run(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
                listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                listener.bind(("127.0.0.1", self.listen_port))
                listener.listen(8)
                listener.settimeout(0.1)
                self.ready.set()
                deadline = time.monotonic() + 15
                while (
                    not self.dropped_application_response
                    and time.monotonic() < deadline
                ):
                    try:
                        client, _ = listener.accept()
                    except socket.timeout:
                        continue
                    threading.Thread(
                        target=self._handle_connection,
                        args=(client,),
                        daemon=True,
                    ).start()
        except OSError as error:
            self.error = str(error)
            self.ready.set()

    def _handle_connection(self, client):
        try:
            with client, socket.create_connection(
                ("127.0.0.1", self.upstream_port), timeout=5
            ) as upstream:
                forward = threading.Thread(
                    target=self._forward_client,
                    args=(client, upstream),
                    daemon=True,
                )
                forward.start()
                buffer = b""
                while not self.dropped_application_response:
                    chunk = upstream.recv(65536)
                    if not chunk:
                        return
                    buffer += chunk
                    while len(buffer) >= 9:
                        length = int.from_bytes(buffer[:3], "big")
                        frame_length = 9 + length
                        if len(buffer) < frame_length:
                            break
                        frame = buffer[:frame_length]
                        buffer = buffer[frame_length:]
                        stream_id = int.from_bytes(frame[5:9], "big") & 0x7FFFFFFF
                        if stream_id != 0:
                            self.dropped_application_response = True
                            return
                        client.sendall(frame)
        except OSError:
            return

    @staticmethod
    def _forward_client(client, upstream):
        try:
            while True:
                chunk = client.recv(65536)
                if not chunk:
                    return
                upstream.sendall(chunk)
        except OSError:
            return

    def wait(self):
        self.thread.join(timeout=15)
        if self.thread.is_alive():
            raise VerificationError("response-drop proxy did not finish")
        if self.error is not None:
            raise VerificationError(f"response-drop proxy failed: {self.error}")
        if not self.dropped_application_response:
            raise VerificationError("response-drop proxy observed no application response")


class DirectedTcpProxy:
    MODES = {"pass", "delay", "drop"}

    def __init__(self, source_node_id, target_node_id, listen_port, upstream_port):
        self.source_node_id = source_node_id
        self.target_node_id = target_node_id
        self.listen_port = listen_port
        self.upstream_port = upstream_port
        self.ready = threading.Event()
        self.stopped = threading.Event()
        self.lock = threading.Lock()
        self.listener = None
        self.connections = set()
        self.mode = "pass"
        self.delay_seconds = 0
        self.error = None
        self.counters = {
            "accepted_connections": 0,
            "client_to_upstream_bytes": 0,
            "upstream_to_client_bytes": 0,
            "dropped_bytes": 0,
            "delayed_chunks": 0,
            "mode_changes": 0,
        }
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()
        if not self.ready.wait(timeout=5):
            raise VerificationError(
                f"directed proxy {source_node_id}->{target_node_id} did not bind"
            )
        if self.error is not None:
            raise VerificationError(
                f"directed proxy {source_node_id}->{target_node_id} failed: {self.error}"
            )

    @property
    def endpoint(self):
        return f"http://127.0.0.1:{self.listen_port}"

    def set_mode(self, mode, delay_ms=0):
        if mode not in self.MODES:
            raise VerificationError(f"unknown directed proxy mode {mode!r}")
        if mode == "delay" and delay_ms <= 0:
            raise VerificationError("delay mode requires a positive delay")
        with self.lock:
            self.mode = mode
            self.delay_seconds = delay_ms / 1000
            self.counters["mode_changes"] += 1
            connections = list(self.connections)
        for connection in connections:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                connection.close()
            except OSError:
                pass

    def snapshot(self):
        with self.lock:
            return {
                "source_node_id": self.source_node_id,
                "target_node_id": self.target_node_id,
                "endpoint": self.endpoint,
                "upstream": f"http://127.0.0.1:{self.upstream_port}",
                "mode": self.mode,
                "delay_ms": round(self.delay_seconds * 1000),
                "counters": dict(self.counters),
                "thread_alive": self.thread.is_alive(),
            }

    def _run(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
                self.listener = listener
                listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                listener.bind(("127.0.0.1", self.listen_port))
                listener.listen(32)
                listener.settimeout(0.1)
                self.ready.set()
                while not self.stopped.is_set():
                    try:
                        client, _ = listener.accept()
                    except socket.timeout:
                        continue
                    except OSError:
                        if self.stopped.is_set():
                            return
                        raise
                    with self.lock:
                        self.counters["accepted_connections"] += 1
                    threading.Thread(
                        target=self._handle_connection,
                        args=(client,),
                        daemon=True,
                    ).start()
        except OSError as error:
            if not self.stopped.is_set():
                self.error = str(error)
            self.ready.set()

    def _handle_connection(self, client):
        try:
            upstream = socket.create_connection(
                ("127.0.0.1", self.upstream_port), timeout=5
            )
        except OSError:
            client.close()
            return
        with self.lock:
            self.connections.update((client, upstream))
        forward = threading.Thread(
            target=self._relay,
            args=(client, upstream, "client_to_upstream_bytes"),
            daemon=True,
        )
        reverse = threading.Thread(
            target=self._relay,
            args=(upstream, client, "upstream_to_client_bytes"),
            daemon=True,
        )
        forward.start()
        reverse.start()
        forward.join()
        reverse.join()
        with self.lock:
            self.connections.discard(client)
            self.connections.discard(upstream)
        for connection in (client, upstream):
            try:
                connection.close()
            except OSError:
                pass

    def _relay(self, source, destination, counter):
        try:
            while not self.stopped.is_set():
                chunk = source.recv(65536)
                if not chunk:
                    return
                with self.lock:
                    mode = self.mode
                    delay_seconds = self.delay_seconds
                    self.counters[counter] += len(chunk)
                    if mode == "drop":
                        self.counters["dropped_bytes"] += len(chunk)
                    elif mode == "delay":
                        self.counters["delayed_chunks"] += 1
                if mode == "drop":
                    continue
                if mode == "delay":
                    time.sleep(delay_seconds)
                destination.sendall(chunk)
        except OSError:
            return
        finally:
            for connection in (source, destination):
                try:
                    connection.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

    def stop(self):
        if self.stopped.is_set():
            return
        self.stopped.set()
        listener = self.listener
        if listener is not None:
            try:
                listener.close()
            except OSError:
                pass
        with self.lock:
            connections = list(self.connections)
        for connection in connections:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                connection.close()
            except OSError:
                pass
        self.thread.join(timeout=5)
        if self.thread.is_alive():
            raise VerificationError(
                f"directed proxy {self.source_node_id}->{self.target_node_id} did not stop"
            )


def parse_json_output(result, name):
    try:
        value = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise VerificationError(f"{name} did not return one JSON value") from error
    if not isinstance(value, dict):
        raise VerificationError(f"{name} returned non-object JSON")
    return value


def load_profile(name):
    path = ROOT / "verification" / "profiles" / f"{name}.json"
    if not path.is_file():
        raise VerificationError(f"unknown verification profile {name!r}")
    value = json.loads(path.read_text())
    required = {"name", "claim", "nodes"}
    missing = sorted(required.difference(value))
    if missing:
        raise VerificationError(f"profile {name!r} is incomplete: missing {missing}")
    if value["name"] != name:
        raise VerificationError(f"profile file name does not match its name field: {name!r}")
    if name != "local":
        raise VerificationError(
            f"profile {name!r} is known but blocked for LS01 on this host"
        )
    local_required = {
        "process_liveness_seconds",
        "write_readiness_seconds",
        "security_modes",
        "ls01",
    }
    missing = sorted(local_required.difference(value))
    if missing:
        raise VerificationError(f"profile {name!r} is incomplete: missing {missing}")
    return value


def prepare_artifacts(path):
    if path.exists():
        raise VerificationError(f"refusing to overwrite artifacts directory {path}")
    path.mkdir(parents=True)
    for name in (
        "node-logs",
        "samples",
        "source",
        "scratch/success",
        "diagnostics",
    ):
        (path / name).mkdir(parents=True)


def snapshot_source(artifacts, fingerprints):
    for relative, expected in fingerprints.items():
        source = ROOT / relative
        if sha256_file(source) != expected:
            raise VerificationError(f"source changed before snapshot: {relative}")
        target = artifacts / "source" / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
        if sha256_file(target) != expected:
            raise VerificationError(f"source snapshot mismatch: {relative}")


def build_release(runner, revision):
    command = ["cargo", "build", "--release"]
    for package in PRODUCTION_PACKAGES:
        command.extend(["-p", package])
    env = os.environ.copy()
    env["LIGHT_STREAM_BUILD_REVISION"] = revision
    runner.run(command, "release-build", timeout=900, env=env)


def release_binaries():
    paths = {
        "light-streamd": ROOT / "target" / "release" / "light-streamd",
        "light-streamctl": ROOT / "target" / "release" / "light-streamctl",
        "light-stream-testkit": ROOT / "target" / "release" / "light-stream-testkit",
    }
    missing = [name for name, path in paths.items() if not path.is_file()]
    if missing:
        raise VerificationError(f"release build omitted binaries: {missing}")
    return paths


def run_health_scenario(artifacts, runner, binaries, revision):
    server = OwnedServer(
        binaries["light-streamd"],
        artifacts / "scratch" / "success" / "health",
        artifacts / "node-logs",
        "health",
    )
    try:
        endpoint = f"http://{server.ready['public_address']}"
        result = runner.run(
            [binaries["light-streamctl"], "--endpoint", endpoint, "health"],
            "health-cli",
            timeout=10,
        )
        cli_value = parse_json_output(result, "light-streamctl health")
        client_result = runner.run(
            [
                binaries["light-stream-testkit"],
                "health-samples",
                "--endpoint",
                endpoint,
                "--count",
                "20",
            ],
            "health-rust-client-samples",
            timeout=10,
        )
        client_value = parse_json_output(client_result, "Rust client health")
        peer_result = runner.run(
            [
                binaries["light-streamctl"],
                "--endpoint",
                f"http://{server.ready['peer_address']}",
                "health",
            ],
            "public-api-on-peer-port",
            expected_codes=(1,),
            timeout=10,
        )
        peer_value = parse_json_output(peer_result, "public API on peer port")
        if peer_value.get("ok") is not False:
            raise VerificationError("peer listener exposed the public API")
        if cli_value["health"]["status"]["revision"] != revision:
            raise VerificationError("CLI health revision does not match the source fingerprint")
        if client_value["health"]["status"]["revision"] != revision:
            raise VerificationError("Rust client health revision does not match the source fingerprint")
        capabilities = {
            item["capability"]: item["support"]["status"]
            for item in cli_value["capabilities"]
        }
        if capabilities.get("health") != "available":
            raise VerificationError(f"unexpected capabilities: {capabilities}")
        samples = client_value["samples_ms"]
        p99_ms = sorted(samples)[-1]
        result = {
            "verdict": "PASS",
            "revision": revision,
            "process": server.description(),
            "cli": cli_value,
            "rust_client": client_value,
            "public_api_on_peer_port": peer_value,
            "health_latency_ms": samples,
            "health_p99_ms": p99_ms,
            "budget_ms": 100,
        }
        if p99_ms > 100:
            raise VerificationError(f"health p99 {p99_ms:.2f} ms exceeds 100 ms")
        write_json(artifacts / "l02.json", result)
        write_json(
            artifacts / "l07.json",
            {
                "verdict": "PASS",
                "predicate": "embedded build revision matches the pre-build source fingerprint",
                "revision": revision,
                "server_revision": server.ready["revision"],
            },
        )
        write_json(artifacts / "contract-demo.json", cli_value)
    finally:
        server.stop()


def write_json_line(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True) + "\n")


def run_publish_scenario(artifacts, runner, binaries):
    server = OwnedServer(
        binaries["light-streamd"],
        artifacts / "scratch" / "success" / "publish",
        artifacts / "node-logs",
        "publish",
    )
    try:
        endpoint = f"http://{server.ready['public_address']}"
        payload = b"ls01-publish-probe"
        request_id = "ls01-probe-1"
        digest = hashlib.sha256(payload).hexdigest()
        write_json_line(
            artifacts / "attempts.jsonl",
            {
                "request_id": request_id,
                "payload_sha256": digest,
                "client_timestamp": utc_now(),
            },
        )
        result = runner.run(
            [
                binaries["light-streamctl"],
                "--endpoint",
                endpoint,
                "publish-probe",
                "--payload",
                payload.decode(),
            ],
            "unsupported-publish-cli",
            expected_codes=(3,),
            timeout=10,
        )
        value = parse_json_output(result, "unsupported publish")
        if value.get("ok") is not False:
            raise VerificationError("unsupported publish returned success-shaped JSON")
        error = value.get("error", {})
        if error.get("code") != "unsupported_operation" or error.get("available_phase") != "LS02":
            raise VerificationError(f"unexpected unsupported publish result: {value}")
        rust_client = runner.run(
            [
                binaries["light-stream-testkit"],
                "publish-probe",
                "--endpoint",
                endpoint,
                "--payload",
                payload.decode(),
            ],
            "unsupported-publish-rust-client",
            expected_codes=(1,),
            timeout=10,
        )
        if "not supported until LS02" not in rust_client.stdout:
            raise VerificationError("Rust client did not preserve the typed unsupported result")
        write_json_line(
            artifacts / "outcomes.jsonl",
            {
                "request_id": request_id,
                "status": "unsupported",
            },
        )
        (artifacts / "acks.jsonl").write_text("")
        (artifacts / "errors.jsonl").write_text(
            json.dumps(
                {
                    "request_id": request_id,
                    "class": "unsupported_operation",
                    "definite": True,
                },
                sort_keys=True,
            )
            + "\n"
        )
        (artifacts / "reads.jsonl").write_text("")
        (artifacts / "faults.jsonl").write_text("")
        runner.run(
            [
                binaries["light-stream-testkit"],
                "oracle-check",
                "--attempts",
                artifacts / "attempts.jsonl",
                "--acknowledgements",
                artifacts / "acks.jsonl",
                "--outcomes",
                artifacts / "outcomes.jsonl",
            ],
            "oracle-no-ack",
        )
        write_json(
            artifacts / "l03.json",
            {
                "verdict": "PASS",
                "cli_exit": result.returncode,
                "response": value,
                "rust_client_exit": rust_client.returncode,
                "acknowledgement_count": 0,
            },
        )

        false_ack_dir = artifacts / "samples" / "false-ack"
        false_ack_dir.mkdir(parents=True)
        shutil.copy2(artifacts / "attempts.jsonl", false_ack_dir / "attempts.jsonl")
        shutil.copy2(artifacts / "outcomes.jsonl", false_ack_dir / "outcomes.jsonl")
        write_json_line(
            false_ack_dir / "acks.jsonl",
            {"request_id": request_id, "payload_sha256": digest},
        )
        false_ack = runner.run(
            [
                binaries["light-stream-testkit"],
                "oracle-check",
                "--attempts",
                false_ack_dir / "attempts.jsonl",
                "--acknowledgements",
                false_ack_dir / "acks.jsonl",
                "--outcomes",
                false_ack_dir / "outcomes.jsonl",
            ],
            "oracle-false-ack",
            expected_codes=(1,),
        )
        if "no acknowledged outcome" not in false_ack.stdout:
            raise VerificationError("false-ack oracle failed for the wrong reason")
        write_json(
            artifacts / "l08.json",
            {
                "verdict": "PASS",
                "oracle_exit": false_ack.returncode,
                "predicate": "an acknowledgement for an unsupported result is rejected",
            },
        )
    finally:
        server.stop()


def run_isolation_scenario(artifacts, runner, binaries):
    first = OwnedServer(
        binaries["light-streamd"],
        artifacts / "scratch" / "success" / "isolation-a",
        artifacts / "node-logs",
        "isolation-a",
    )
    second = OwnedServer(
        binaries["light-streamd"],
        artifacts / "scratch" / "success" / "isolation-b",
        artifacts / "node-logs",
        "isolation-b",
    )
    try:
        if first.ready["public_address"] == second.ready["public_address"]:
            raise VerificationError("isolated servers share a public port")
        if first.ready["peer_address"] == second.ready["peer_address"]:
            raise VerificationError("isolated servers share a peer port")
        write_json(
            artifacts / "l05.json",
            {
                "verdict": "PASS",
                "first": first.description(),
                "second": second.description(),
            },
        )
        duplicate = runner.run(
            [
                binaries["light-streamd"],
                "--data-dir",
                first.data_dir,
                "--public-listen",
                "127.0.0.1:0",
                "--peer-listen",
                "127.0.0.1:0",
            ],
            "duplicate-data-owner",
            expected_codes=(1,),
            timeout=10,
        )
        if "already owned" not in duplicate.stderr:
            raise VerificationError("second data-directory owner failed for the wrong reason")
        write_json(
            artifacts / "l06.json",
            {
                "verdict": "PASS",
                "second_owner_exit": duplicate.returncode,
                "data_directory": str(first.data_dir),
            },
        )
    finally:
        second.stop()
        first.stop()


def run_preservation_scenario(artifacts, runner, binaries, revision, profile, seed):
    failure_data = artifacts / "diagnostics" / "failed-child-data"
    failure_data.mkdir(parents=True)
    sentinel = failure_data / "retain-me.txt"
    sentinel.write_text("retained after expected child failure\n")
    invalid = runner.run(
        [
            binaries["light-streamd"],
            "--data-dir",
            failure_data,
            "--public-listen",
            "0.0.0.0:0",
            "--peer-listen",
            "127.0.0.1:0",
            "--security-mode",
            "local-insecure",
        ],
        "invalid-insecure-bind",
        expected_codes=(1,),
        timeout=10,
    )
    if "must bind loopback" not in invalid.stderr:
        raise VerificationError("invalid insecure binding failed for the wrong reason")
    malformed = runner.run(
        [
            binaries["light-streamctl"],
            "--endpoint",
            "http://127.0.0.1:1",
            "publish-probe",
            "--payload",
            "",
        ],
        "malformed-publish-input",
        expected_codes=(2,),
        timeout=10,
    )
    malformed_value = parse_json_output(malformed, "malformed publish input")
    if malformed_value.get("error", {}).get("code") != "invalid_payload":
        raise VerificationError("malformed publish input failed for the wrong reason")
    write_json(
        artifacts / "l04.json",
        {
            "verdict": "PASS",
            "exit": invalid.returncode,
            "listener_started": False,
            "malformed_input_exit": malformed.returncode,
            "malformed_input": malformed_value,
        },
    )
    if not sentinel.is_file():
        raise VerificationError("failed child scratch data was removed")
    write_json(
        artifacts / "l09.json",
        {
            "verdict": "PASS",
            "child_exit": invalid.returncode,
            "retained_data_directory": str(failure_data),
            "retained_sentinel": str(sentinel),
        },
    )
    resume_record = {
        "revision": revision,
        "profile": profile,
        "seed": seed,
        "prior_failures": [
            {
                "name": "invalid-insecure-bind",
                "expected": True,
                "evidence": "l09.json",
            }
        ],
    }
    write_json(artifacts / "resume.json", resume_record)
    loaded = json.loads((artifacts / "resume.json").read_text())
    if loaded != resume_record:
        raise VerificationError("resume metadata did not round trip")
    write_json(
        artifacts / "l10.json",
        {
            "verdict": "PASS",
            "resume": loaded,
        },
    )


def run_poc_control(artifacts, runner):
    output = artifacts / "poc-smoke"
    result = runner.run(
        [
            sys.executable,
            ROOT / "poc" / "scripts" / "run.py",
            "--matrix",
            "smoke",
            "--repeats",
            "1",
            "--output",
            output,
        ],
        "poc-smoke-control",
        timeout=900,
    )
    run_result_path = output / "run.json"
    if not run_result_path.is_file():
        raise VerificationError("POC smoke omitted run.json")
    run_result = json.loads(run_result_path.read_text())
    if run_result.get("verdict") != "VERIFIED":
        raise VerificationError("POC smoke did not verify")
    write_json(
        artifacts / "l01.json",
        {
            "verdict": "PASS",
            "poc": run_result,
            "stdout": result.stdout,
            "production_feature_on_base": "UNSUPPORTED",
        },
    )
def deterministic_uuid(rng):
    return str(uuid.UUID(int=rng.getrandbits(128)))


def publish_command(
    binary,
    endpoint,
    cluster,
    stream,
    principal,
    session,
    sequence,
    file,
    seeds=(),
    no_retry=False,
    deadline_ms=5000,
    route_group_id=None,
    route_revision=None,
    partition=0,
):
    command = [
        binary,
        "--endpoint",
        endpoint,
        "--deadline-ms",
        str(deadline_ms),
    ]
    for seed in seeds:
        command.extend(["--seed", seed])
    if no_retry:
        command.append("--no-retry")
    command.extend([
        "publish",
        "--cluster-id",
        cluster,
        "--stream-id",
        stream,
        "--partition",
        str(partition),
        "--principal",
        principal,
        "--session",
        session,
        "--sequence",
        str(sequence),
        "--file",
        file,
    ])
    if route_group_id is not None:
        command.extend(["--route-group-id", str(route_group_id)])
    if route_revision is not None:
        command.extend(["--route-revision", str(route_revision)])
    return command


def receipt_command(
    binary,
    endpoint,
    cluster,
    stream,
    principal,
    session,
    sequence,
    no_retry=False,
    deadline_ms=5000,
    route_group_id=None,
    route_revision=None,
):
    command = cli_endpoint_command(
        binary,
        endpoint,
        no_retry=no_retry,
        deadline_ms=deadline_ms,
    ) + [
        "receipt",
        "--cluster-id",
        cluster,
        "--stream-id",
        stream,
        "--principal",
        principal,
        "--session",
        session,
        "--sequence",
        str(sequence),
    ]
    if route_group_id is not None:
        command.extend(["--route-group-id", str(route_group_id)])
    if route_revision is not None:
        command.extend(["--route-revision", str(route_revision)])
    return command


def payloads_from_page(value):
    return [bytes(record["payload"]) for record in value["page"]["records"]]


def run_ls02a_scenario(artifacts, runner, binaries, revision, seed):
    rng = random.Random(seed)
    cluster = deterministic_uuid(rng)
    stream = deterministic_uuid(rng)
    session = deterministic_uuid(rng)
    principal = f"verify-{seed}"
    data_dir = artifacts / "scratch" / "success" / "ls02a"
    server = OwnedServer(
        binaries["light-streamd"],
        data_dir,
        artifacts / "node-logs",
        "ls02a-startup",
    )
    endpoint = f"http://{server.ready['public_address']}"
    attempts = []
    acknowledgements = []
    outcomes = []
    reads = []
    faults = []
    try:
        pristine_health = parse_json_output(
            runner.run(
                [binaries["light-streamctl"], "--endpoint", endpoint, "health"],
                "ls02a-pristine-health",
                timeout=10,
            ),
            "LS02a pristine health",
        )
        if pristine_health["health"]["bootstrapped"] is not False:
            raise VerificationError("startup silently bootstrapped a cluster")
        prebootstrap = runner.run(
            [
                binaries["light-streamctl"],
                "--endpoint",
                endpoint,
                "fetch",
                "--cluster-id",
                cluster,
                "--stream-id",
                stream,
                "--offset",
                "0",
                "--limit",
                "10",
            ],
            "ls02a-prebootstrap-fetch",
            expected_codes=(5,),
            timeout=10,
        )
        prebootstrap_value = parse_json_output(prebootstrap, "pre-bootstrap fetch")
        if prebootstrap_value.get("error", {}).get("code") != "not_bootstrapped":
            raise VerificationError("pre-bootstrap fetch failed for the wrong reason")

        bootstrap = parse_json_output(
            runner.run(
                [
                    binaries["light-streamctl"],
                    "--endpoint",
                    endpoint,
                    "cluster",
                    "bootstrap",
                    "--cluster-id",
                    cluster,
                    "--stream-id",
                    stream,
                    "--stream-name",
                    "bootstrap",
                ],
                "ls02a-bootstrap",
                timeout=20,
            ),
            "cluster bootstrap",
        )
        bootstrap_retry = parse_json_output(
            runner.run(
                [
                    binaries["light-streamctl"],
                    "--endpoint",
                    endpoint,
                    "cluster",
                    "bootstrap",
                    "--cluster-id",
                    cluster,
                    "--stream-id",
                    stream,
                    "--stream-name",
                    "bootstrap",
                ],
                "ls02a-bootstrap-retry",
                timeout=20,
            ),
            "cluster bootstrap retry",
        )
        if bootstrap["cluster"] != bootstrap_retry["cluster"]:
            raise VerificationError("idempotent bootstrap returned a different identity")
        write_json(
            artifacts / "bootstrap.json",
            {
                "pristine_health": pristine_health,
                "prebootstrap": prebootstrap_value,
                "first": bootstrap,
                "retry": bootstrap_retry,
            },
        )

        records = []
        for sequence in range(1, 7):
            size = 17 + rng.randrange(0, 200)
            payload = bytes(rng.randrange(0, 256) for _ in range(size))
            path = artifacts / "samples" / f"record-{sequence:02d}.bin"
            path.write_bytes(payload)
            digest = hashlib.sha256(payload).hexdigest()
            request_id = f"{principal}/{session}/{sequence}"
            attempt = {
                "request_id": request_id,
                "payload_sha256": digest,
                "payload_base64": base64.b64encode(payload).decode(),
                "sequence": sequence,
            }
            attempts.append(attempt)
            command = publish_command(
                binaries["light-streamctl"],
                endpoint,
                cluster,
                stream,
                principal,
                session,
                sequence,
                path,
            )
            if sequence == 3:
                runner.run_dropped_response(
                    command,
                    "ls02a-publish-dropped-response",
                    timeout=20,
                )
                outcomes.append(
                    {
                        "request_id": request_id,
                        "status": "ambiguous",
                        "reason": "client response discarded after command completion",
                    }
                )
                faults.append(
                    {
                        "fault": "drop_client_response",
                        "request_id": request_id,
                        "deterministic": True,
                    }
                )
                value = parse_json_output(
                    runner.run(command, "ls02a-publish-retry", timeout=20),
                    "lost-response retry",
                )
            elif sequence % 2 == 0:
                value = parse_json_output(
                    runner.run(
                        [
                            binaries["light-stream-testkit"],
                            "client-publish",
                            "--endpoint",
                            endpoint,
                            "--cluster-id",
                            cluster,
                            "--stream-id",
                            stream,
                            "--principal",
                            principal,
                            "--session",
                            session,
                            "--sequence",
                            str(sequence),
                            "--file",
                            path,
                        ],
                        f"ls02a-client-publish-{sequence}",
                        timeout=20,
                    ),
                    "Rust client publish",
                )
            else:
                value = parse_json_output(
                    runner.run(command, f"ls02a-cli-publish-{sequence}", timeout=20),
                    "CLI publish",
                )
            receipt = value["receipt"]
            if receipt["range"]["first"] != sequence - 1 or receipt["range"]["count"] != 1:
                raise VerificationError(f"unexpected receipt range for sequence {sequence}")
            acknowledgements.append(
                {
                    "request_id": request_id,
                    "payload_sha256": digest,
                    "first_offset": receipt["range"]["first"],
                    "count": receipt["range"]["count"],
                }
            )
            outcomes.append(
                {
                    "request_id": request_id,
                    "status": "acknowledged",
                }
            )
            records.append(payload)

        conflict_path = artifacts / "samples" / "conflicting-retry.bin"
        conflict_path.write_bytes(b"conflicting retry payload")
        conflict = runner.run(
            publish_command(
                binaries["light-streamctl"],
                endpoint,
                cluster,
                stream,
                principal,
                session,
                3,
                conflict_path,
            ),
            "ls02a-conflicting-retry",
            expected_codes=(4,),
            timeout=20,
        )
        conflict_value = parse_json_output(conflict, "conflicting retry")
        if conflict_value.get("error", {}).get("code") != "receipt_conflict":
            raise VerificationError("conflicting retry failed for the wrong reason")

        cli_fetch = parse_json_output(
            runner.run(
                [
                    binaries["light-streamctl"],
                    "--endpoint",
                    endpoint,
                    "fetch",
                    "--cluster-id",
                    cluster,
                    "--stream-id",
                    stream,
                    "--offset",
                    "0",
                    "--limit",
                    "64",
                ],
                "ls02a-cli-fetch",
                timeout=20,
            ),
            "CLI fetch",
        )
        client_fetch = parse_json_output(
            runner.run(
                [
                    binaries["light-stream-testkit"],
                    "client-fetch",
                    "--endpoint",
                    endpoint,
                    "--cluster-id",
                    cluster,
                    "--stream-id",
                    stream,
                    "--offset",
                    "0",
                    "--limit",
                    "64",
                ],
                "ls02a-client-fetch",
                timeout=20,
            ),
            "Rust client fetch",
        )
        if payloads_from_page(cli_fetch) != records:
            raise VerificationError("CLI fetch differs from the independent byte ledger")
        if payloads_from_page(client_fetch) != records:
            raise VerificationError("Rust client fetch differs from the independent byte ledger")
        reads.append(
            {
                "stage": "before_restart",
                "record_sha256": [hashlib.sha256(record).hexdigest() for record in records],
            }
        )
    finally:
        server.stop()

    restarted = OwnedServer(
        binaries["light-streamd"],
        data_dir,
        artifacts / "node-logs",
        "ls02a-restart",
    )
    try:
        endpoint = f"http://{restarted.ready['public_address']}"
        restart_fetch = parse_json_output(
            runner.run(
                [
                    binaries["light-streamctl"],
                    "--endpoint",
                    endpoint,
                    "fetch",
                    "--cluster-id",
                    cluster,
                    "--stream-id",
                    stream,
                    "--offset",
                    "0",
                    "--limit",
                    "64",
                ],
                "ls02a-fetch-after-restart",
                timeout=20,
            ),
            "fetch after restart",
        )
        if payloads_from_page(restart_fetch) != records:
            raise VerificationError("restart fetch differs from every acknowledged byte")
        reads.append(
            {
                "stage": "after_restart",
                "record_sha256": [hashlib.sha256(record).hexdigest() for record in records],
            }
        )
        retry_receipt = parse_json_output(
            runner.run(
                [
                    binaries["light-streamctl"],
                    "--endpoint",
                    endpoint,
                    "receipt",
                    "--cluster-id",
                    cluster,
                    "--stream-id",
                    stream,
                    "--principal",
                    principal,
                    "--session",
                    session,
                    "--sequence",
                    "3",
                ],
                "ls02a-receipt-after-restart",
                timeout=20,
            ),
            "receipt after restart",
        )
        if retry_receipt["receipt"]["range"]["first"] != 2:
            raise VerificationError("receipt lookup did not preserve the original offset")
        conflicting_bootstrap = runner.run(
            [
                binaries["light-streamctl"],
                "--endpoint",
                endpoint,
                "cluster",
                "bootstrap",
                "--cluster-id",
                deterministic_uuid(rng),
                "--stream-id",
                stream,
                "--stream-name",
                "bootstrap",
            ],
            "ls02a-conflicting-bootstrap",
            expected_codes=(4,),
            timeout=20,
        )
        conflicting_bootstrap_value = parse_json_output(
            conflicting_bootstrap, "conflicting bootstrap"
        )
        if conflicting_bootstrap_value.get("error", {}).get("code") != "bootstrap_conflict":
            raise VerificationError("conflicting bootstrap failed for the wrong reason")
    finally:
        restarted.stop()

    for path, values in (
        (artifacts / "attempts.jsonl", attempts),
        (artifacts / "acks.jsonl", acknowledgements),
        (artifacts / "outcomes.jsonl", outcomes),
        (artifacts / "reads.jsonl", reads),
        (artifacts / "faults.jsonl", faults),
    ):
        path.write_text("".join(json.dumps(value, sort_keys=True) + "\n" for value in values))
    write_json(
        artifacts / "durable-journey.json",
        {
            "verdict": "PASS",
            "revision": revision,
            "cluster_id": cluster,
            "stream_id": stream,
            "record_count": len(records),
            "acknowledged_bytes": sum(len(record) for record in records),
            "restart_fetch": restart_fetch,
        },
    )
    write_json(
        artifacts / "receipt-evidence.json",
        {
            "verdict": "PASS",
            "request_sequence": 3,
            "original_first_offset": 2,
            "response_dropped": True,
            "retry_receipt": retry_receipt,
            "conflict": conflict_value,
        },
    )
    write_json(
        artifacts / "l02.json",
        {
            "verdict": "PASS",
            "scenario": "E01",
            "record_count": len(records),
            "cli_and_sdk_match": True,
            "restart_match": True,
        },
    )
    write_json(
        artifacts / "l07.json",
        {
            "verdict": "PASS",
            "scenario": "E09",
            "response_dropped": True,
            "original_range_returned": True,
        },
    )
    write_json(
        artifacts / "l08.json",
        {
            "verdict": "PASS",
            "conflicting_retry_rejected": True,
            "error": conflict_value,
        },
    )
    write_json(
        artifacts / "l10.json",
        {
            "verdict": "PASS",
            "cluster_id": cluster,
            "stream_id": stream,
            "offsets_and_receipts_survived": True,
        },
    )
    write_json(
        artifacts / "unsupported.json",
        {
            "three_node_replication": "UNSUPPORTED_LS02B",
            "leader_election_failover": "UNSUPPORTED_LS02B",
            "quorum_refusal": "UNSUPPORTED_LS02B",
            "independent_reference_hosts": "BLOCKED",
            "secured_mode": "UNSUPPORTED_LS08",
            "snapshot_catch_up": "UNSUPPORTED_LS06",
        },
    )


def cli_endpoint_command(binary, endpoint, seeds=(), no_retry=False, deadline_ms=5000):
    command = [
        binary,
        "--endpoint",
        endpoint,
        "--deadline-ms",
        str(deadline_ms),
    ]
    for seed in seeds:
        command.extend(["--seed", seed])
    if no_retry:
        command.append("--no-retry")
    return command


def read_node_diagnostics(artifacts, runner, binary, node, label):
    value = parse_json_output(
        runner.run(
            cli_endpoint_command(
                binary,
                node["endpoint"],
                no_retry=True,
                deadline_ms=1000,
            )
            + ["diagnostics"],
            label,
            timeout=5,
        ),
        label,
    )["diagnostics"]
    append_jsonl(
        artifacts / "diagnostics" / "raft.jsonl",
        {"captured_at": utc_now(), "label": label, "value": value},
    )
    return value


def group_by_name(diagnostics, name, group_id=None):
    matches = [
        group
        for group in diagnostics["groups"]
        if group["group"] == name
        and (group_id is None or group["group_id"] == group_id)
    ]
    if len(matches) != 1:
        raise VerificationError(
            f"diagnostics require exactly one {name} group, observed {len(matches)}"
        )
    return matches[0]


def data_group(diagnostics):
    return group_by_name(diagnostics, "data", 2)


def assert_exact_memberships(diagnostics, wanted):
    groups = {group["group_id"]: group for group in diagnostics["groups"]}
    if 1 not in groups or 2 not in groups:
        raise VerificationError(
            f"diagnostics require control group 1 and data group 2: {sorted(groups)}"
        )
    observed = {}
    for group_id, group in sorted(groups.items()):
        expected_kind = "control" if group_id == 1 else "data"
        if (
            group["group"] != expected_kind
            or not group["effective_uniform"]
            or not group["committed_uniform"]
            or group["effective_voters"] != wanted
            or group["committed_voters"] != wanted
            or group["effective_learners"] != []
            or group["committed_learners"] != []
        ):
            raise VerificationError(
                f"group {group_id} has unexpected membership: {group}"
            )
        name = "control" if group_id == 1 else f"data-{group_id}"
        observed[name] = {
            "group_id": group["group_id"],
            "effective_uniform": group["effective_uniform"],
            "effective_voters": group["effective_voters"],
            "effective_learners": group["effective_learners"],
            "committed_uniform": group["committed_uniform"],
            "committed_voters": group["committed_voters"],
            "committed_learners": group["committed_learners"],
        }
    return {
        "node_id": diagnostics["node_id"],
        "lifecycle": diagnostics["lifecycle"],
        "groups": observed,
    }


def assert_typed_leader_hint(
    value,
    expected_leader,
    diagnostics,
    topology,
    expected_group="data",
):
    error = value.get("error", {})
    detail = error.get("detail", {})
    leader = detail.get("leader")
    if (
        error.get("code") != "not_leader"
        or detail.get("code") != "not_leader"
        or detail.get("group") != expected_group
        or not isinstance(leader, dict)
    ):
        raise VerificationError(f"response omitted a typed data leader hint: {value}")
    node_id = leader.get("node_id")
    public_uri = leader.get("public_uri")
    if not isinstance(node_id, int) or isinstance(node_id, bool) or node_id <= 0:
        raise VerificationError(f"leader hint has an invalid node ID: {leader}")
    if node_id != expected_leader:
        raise VerificationError(
            f"leader hint named node {node_id}, expected {expected_leader}"
        )
    topology_uri = topology.get(node_id, {}).get("endpoint")
    peers = {
        peer["node_id"]: peer["public_uri"]
        for peer in diagnostics["peers"]
        if peer["node_id"] > 0
    }
    diagnosed_group = (
        data_group(diagnostics)
        if expected_group == "data"
        else group_by_name(diagnostics, "control", 1)
    )
    if (
        diagnosed_group["current_leader"] != expected_leader
        or public_uri != topology_uri
        or public_uri != peers.get(node_id)
    ):
        raise VerificationError(
            "leader hint public URI does not match topology and diagnostics"
        )
    return {"group": expected_group, "node_id": node_id, "public_uri": public_uri}


def assert_caught_up(leader_id, follower_id, leader, follower):
    if leader_id == follower_id:
        if follower["local_role"] != "leader":
            raise VerificationError(
                "catch-up target is named leader without the leader role"
            )
        target_log = follower["last_log_index"]
        if target_log is None or any(
            value != target_log
            for value in (
                follower["local_committed_index"],
                follower["cluster_committed_index"],
                follower["last_applied_index"],
            )
        ):
            raise VerificationError(
                "promoted catch-up target has divergent log, commit, or apply progress"
            )
        unmatched = [
            item
            for item in follower["replication"]
            if item["matched_log_index"] != target_log
        ]
        if unmatched:
            raise VerificationError(
                "promoted catch-up target has incomplete peer replication progress"
            )
        return {
            "leader_id": leader_id,
            "target_node_id": follower_id,
            "target_became_leader": True,
            "leader": leader,
            "follower": follower,
            "target_replication": follower["replication"],
        }
    if leader["local_role"] != "leader":
        raise VerificationError("catch-up source is not the diagnosed leader")
    if follower["local_role"] == "leader":
        raise VerificationError("catch-up target is still the leader")
    if follower["current_leader"] != leader_id:
        raise VerificationError("catch-up target does not recognize the diagnosed leader")
    leader_log = leader["last_log_index"]
    progress = [
        item
        for item in leader["replication"]
        if item["target_node_id"] == follower_id
    ]
    if len(progress) != 1 or progress[0]["matched_log_index"] != leader_log:
        raise VerificationError(
            "leader replication progress for the target does not match the leader log"
        )
    if leader_log is None or any(
        value != leader_log
        for value in (
            leader["local_committed_index"],
            leader["last_applied_index"],
            follower["last_log_index"],
            follower["local_committed_index"],
            follower["last_applied_index"],
        )
    ):
        raise VerificationError(
            "leader and follower log, committed, and applied indexes are not equal"
        )
    return {
        "leader_id": leader_id,
        "target_node_id": follower_id,
        "leader": leader,
        "follower": follower,
        "target_replication": progress[0],
    }


def wait_for_uniform_active(artifacts, runner, binary, nodes, timeout, label):
    deadline = time.monotonic() + timeout
    wanted = [1, 2, 3]
    last = None
    while time.monotonic() < deadline:
        observed = []
        try:
            for node_id in sorted(nodes):
                node = nodes[node_id]
                if not node["server"].is_running():
                    continue
                diagnostics = read_node_diagnostics(
                    artifacts,
                    runner,
                    binary,
                    node,
                    f"{label}-node-{node_id}",
                )
                if diagnostics["lifecycle"] != "active":
                    raise VerificationError(
                        f"node {node_id} lifecycle is {diagnostics['lifecycle']!r}"
                    )
                observed.append(assert_exact_memberships(diagnostics, wanted))
            if observed:
                return observed
            last = observed
        except (VerificationError, KeyError):
            last = observed
        time.sleep(0.05)
    raise VerificationError(f"{label} did not reach exact active membership: {last}")


def wait_for_data_leader(
    artifacts,
    runner,
    binary,
    nodes,
    timeout,
    label,
    previous=None,
    node_ids=None,
):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        leaders = []
        try:
            for node_id in sorted(nodes if node_ids is None else node_ids):
                node = nodes[node_id]
                if not node["server"].is_running():
                    continue
                diagnostics = read_node_diagnostics(
                    artifacts,
                    runner,
                    binary,
                    node,
                    f"{label}-node-{node_id}",
                )
                group = data_group(diagnostics)
                leader = group["current_leader"]
                if leader is not None and group["local_role"] != "shutdown":
                    leaders.append(leader)
            if leaders and len(set(leaders)) == 1 and leaders[0] != previous:
                return leaders[0]
            last = leaders
        except (VerificationError, KeyError):
            pass
        time.sleep(0.05)
    raise VerificationError(f"{label} did not elect one data leader: {last}")


def wait_for_follower_catch_up(
    artifacts,
    runner,
    binary,
    nodes,
    follower_id,
    timeout,
    label,
):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            leader_id = wait_for_data_leader(
                artifacts,
                runner,
                binary,
                nodes,
                2,
                f"{label}-leader",
            )
            leader = data_group(
                read_node_diagnostics(
                    artifacts,
                    runner,
                    binary,
                    nodes[leader_id],
                    f"{label}-leader-state",
                )
            )
            follower = data_group(
                read_node_diagnostics(
                    artifacts,
                    runner,
                    binary,
                    nodes[follower_id],
                    f"{label}-follower-state",
                )
            )
            last = {"leader": leader, "follower": follower}
            return assert_caught_up(leader_id, follower_id, leader, follower)
        except (VerificationError, KeyError):
            pass
        time.sleep(0.05)
    raise VerificationError(f"{label} follower did not catch up: {last}")


def run_ls02b_scenario(
    artifacts, runner, binaries, revision, profile, seed, verify_checkpoints=False
):
    rng = random.Random(seed)
    cluster = deterministic_uuid(rng)
    stream = deterministic_uuid(rng)
    session = deterministic_uuid(rng)
    principal = f"verify-ls02b-{seed}"
    node_configs = {}
    used_ports = set()
    for node_id in (1, 2, 3):
        public_port = free_port()
        while public_port in used_ports:
            public_port = free_port()
        used_ports.add(public_port)
        peer_port = free_port()
        while peer_port in used_ports:
            peer_port = free_port()
        used_ports.add(peer_port)
        node_configs[node_id] = {
            "node_id": node_id,
            "public_address": f"127.0.0.1:{public_port}",
            "peer_address": f"127.0.0.1:{peer_port}",
            "endpoint": f"http://127.0.0.1:{public_port}",
            "peer_uri": f"http://127.0.0.1:{peer_port}",
            "data_dir": artifacts / "scratch" / "success" / f"ls02b-node-{node_id}",
        }

    nodes = {}
    proxies = {}
    attempts = []
    acknowledgements = []
    outcomes = []
    reads = []
    faults = []
    elections = []
    membership_observations = []
    checkpoint_before_failover = None
    checkpoint_after_failover = None

    def start_node(node_id, suffix):
        config = node_configs[node_id]
        server = OwnedServer(
            binaries["light-streamd"],
            config["data_dir"],
            artifacts / "node-logs",
            f"ls02b-node-{node_id}-{suffix}",
            node_id=node_id,
            public_address=config["public_address"],
            peer_address=config["peer_address"],
            advertise_public_uri=config["endpoint"],
            advertise_peer_uri=config["peer_uri"],
            peer_routes=(
                {}
                if profile.get("implemented_phase") in ("LS03", "LS04")
                else config["peer_routes"]
            ),
            max_data_groups=1,
            max_streams=1,
            max_partitions_per_stream=1,
        )
        nodes[node_id] = {**config, "server": server}
        return server

    def stop_node(node_id, abrupt=False):
        server = nodes[node_id]["server"]
        if abrupt:
            server.kill()
        else:
            server.stop()

    def running_endpoints(exclude=None):
        return [
            nodes[node_id]["endpoint"]
            for node_id in sorted(nodes)
            if node_id != exclude and nodes[node_id]["server"].is_running()
        ]

    def wait_for_running_group_leader(group_name, group_id, label):
        deadline = time.monotonic() + profile["leader_loss_seconds"] + 10
        last = None
        while time.monotonic() < deadline:
            running = {
                node_id
                for node_id in nodes
                if nodes[node_id]["server"].is_running()
            }
            for node_id in sorted(running):
                try:
                    diagnostics = read_node_diagnostics(
                        artifacts,
                        runner,
                        binaries["light-streamctl"],
                        nodes[node_id],
                        f"{label}-node-{node_id}",
                    )
                    leader_id = group_by_name(
                        diagnostics,
                        group_name,
                        group_id,
                    )["current_leader"]
                    last = leader_id
                    if leader_id in running:
                        return leader_id
                except (KeyError, VerificationError, subprocess.SubprocessError):
                    pass
            time.sleep(0.05)
        raise VerificationError(
            f"{label} did not elect a running {group_name} leader: {last}"
        )

    def set_bidirectional_links(node_id, peers, mode, delay_ms=0):
        changed = []
        for peer_id in peers:
            for source, target in ((node_id, peer_id), (peer_id, node_id)):
                proxy = proxies[(source, target)]
                proxy.set_mode(mode, delay_ms)
                changed.append(proxy.snapshot())
        return changed

    def proxy_states():
        return [proxies[key].snapshot() for key in sorted(proxies)]

    def write_payload(sequence, payload):
        path = artifacts / "samples" / f"ls02b-{sequence}.bin"
        path.write_bytes(payload)
        return path

    try:
        for source_node_id in (1, 2, 3):
            routes = {}
            for target_node_id in (1, 2, 3):
                if source_node_id == target_node_id:
                    continue
                proxy_port = free_port()
                while proxy_port in used_ports:
                    proxy_port = free_port()
                used_ports.add(proxy_port)
                proxy = DirectedTcpProxy(
                    source_node_id,
                    target_node_id,
                    proxy_port,
                    int(
                        node_configs[target_node_id]["peer_address"].rsplit(":", 1)[1]
                    ),
                )
                proxies[(source_node_id, target_node_id)] = proxy
                routes[target_node_id] = proxy.endpoint
            node_configs[source_node_id]["peer_routes"] = routes
        for node_id in (1, 2, 3):
            start_node(node_id, "initial")
        write_json(
            artifacts / "topology.json",
            {
                "revision": revision,
                "nodes": {
                    str(node_id): nodes[node_id]["server"].description()
                    for node_id in sorted(nodes)
                },
                "directed_proxies": proxy_states(),
            },
        )
        bootstrap_command = cli_endpoint_command(
            binaries["light-streamctl"],
            nodes[1]["endpoint"],
            deadline_ms=30000,
        ) + [
            "cluster",
            "bootstrap",
            "--cluster-id",
            cluster,
            "--stream-id",
            stream,
            "--stream-name",
            "bootstrap",
            "--seed-node-id",
            "1",
        ]
        for node_id in (1, 2, 3):
            bootstrap_command.extend(
                [
                    "--member",
                    (
                        f"{node_id},{nodes[node_id]['endpoint']},"
                        f"{nodes[node_id]['peer_uri']}"
                    ),
                ]
            )
        bootstrap = parse_json_output(
            runner.run(bootstrap_command, "ls02b-bootstrap", timeout=45),
            "LS02b bootstrap",
        )
        membership_observations.append(
            {
                "stage": "initial",
                "nodes": wait_for_uniform_active(
                    artifacts,
                    runner,
                    binaries["light-streamctl"],
                    nodes,
                    profile["write_readiness_seconds"] + 10,
                    "ls02b-active",
                ),
            }
        )
        leader = wait_for_data_leader(
            artifacts,
            runner,
            binaries["light-streamctl"],
            nodes,
            profile["leader_loss_seconds"],
            "ls02b-initial-election",
        )
        elections.append({"stage": "initial", "leader": leader})
        followers = [node_id for node_id in (1, 2, 3) if node_id != leader]
        bootstrap_route = parse_json_output(
            runner.run(
                cli_endpoint_command(
                    binaries["light-streamctl"],
                    nodes[1]["endpoint"],
                    seeds=running_endpoints(exclude=1),
                )
                + [
                    "stream",
                    "route",
                    "--cluster-id",
                    cluster,
                    "--stream-id",
                    stream,
                    "--partition",
                    "0",
                ],
                "ls02b-bootstrap-route",
                timeout=15,
            ),
            "LS02b bootstrap route",
        )["route"]["route"]

        def wait_bootstrap_partition_ready(label, endpoint):
            deadline = time.monotonic() + profile["leader_loss_seconds"] + 20
            attempt = 0
            last = None
            while time.monotonic() < deadline:
                attempt += 1
                result = runner.run(
                    cli_endpoint_command(
                        binaries["light-streamctl"],
                        endpoint,
                        seeds=running_endpoints(),
                        deadline_ms=3000,
                    )
                    + [
                        "fetch",
                        "--cluster-id",
                        cluster,
                        "--stream-id",
                        stream,
                        "--offset",
                        "0",
                        "--limit",
                        "1",
                    ],
                    f"{label}-attempt-{attempt}",
                    expected_codes=(0, 5),
                    timeout=5,
                )
                last = parse_json_output(result, f"{label} attempt {attempt}")
                if result.returncode == 0:
                    return
                time.sleep(0.05)
            raise VerificationError(
                f"{label} never reached application readiness: {last}"
            )

        refused_payload = write_payload(900, os.urandom(97))
        refused = runner.run(
            publish_command(
                binaries["light-streamctl"],
                nodes[followers[0]]["endpoint"],
                cluster,
                stream,
                principal,
                session,
                900,
                refused_payload,
                no_retry=True,
                deadline_ms=1000,
                route_group_id=bootstrap_route["group"],
                route_revision=bootstrap_route["route_revision"],
            ),
            "ls02b-direct-follower-write-refusal",
            expected_codes=(5,),
            timeout=5,
        )
        refused_value = parse_json_output(refused, "direct follower refusal")
        refused_diagnostics = read_node_diagnostics(
            artifacts,
            runner,
            binaries["light-streamctl"],
            nodes[followers[0]],
            "ls02b-direct-follower-write-hint-diagnostics",
        )
        write_group = refused_value["error"]["detail"]["group"]
        write_expected_leader = (
            leader
            if write_group == "data"
            else group_by_name(refused_diagnostics, "control", 1)["current_leader"]
        )
        write_hint = assert_typed_leader_hint(
            refused_value,
            write_expected_leader,
            refused_diagnostics,
            node_configs,
            write_group,
        )
        direct_read = runner.run(
            cli_endpoint_command(
                binaries["light-streamctl"],
                nodes[followers[1]]["endpoint"],
                no_retry=True,
                deadline_ms=1000,
            )
            + [
                "fetch",
                "--cluster-id",
                cluster,
                "--stream-id",
                stream,
                "--offset",
                "0",
                "--limit",
                "16",
                "--route-group-id",
                str(bootstrap_route["group"]),
                "--route-revision",
                str(bootstrap_route["route_revision"]),
            ],
            "ls02b-direct-follower-read-refusal",
            expected_codes=(5,),
            timeout=5,
        )
        direct_read_value = parse_json_output(direct_read, "direct follower read")
        read_diagnostics = read_node_diagnostics(
            artifacts,
            runner,
            binaries["light-streamctl"],
            nodes[followers[1]],
            "ls02b-direct-follower-read-hint-diagnostics",
        )
        read_group = direct_read_value["error"]["detail"]["group"]
        read_expected_leader = (
            leader
            if read_group == "data"
            else group_by_name(read_diagnostics, "control", 1)["current_leader"]
        )
        read_hint = assert_typed_leader_hint(
            direct_read_value,
            read_expected_leader,
            read_diagnostics,
            node_configs,
            read_group,
        )

        records = [os.urandom(rng.randint(257, 4096)) for _ in range(3)]
        for sequence, payload in enumerate(records[:2], start=1):
            payload_path = write_payload(sequence, payload)
            request_id = f"{principal}:{session}:{sequence}"
            attempts.append(
                {
                    "request_id": request_id,
                    "payload_sha256": hashlib.sha256(payload).hexdigest(),
                }
            )
            if sequence == 1:
                command = publish_command(
                    binaries["light-streamctl"],
                    nodes[followers[0]]["endpoint"],
                    cluster,
                    stream,
                    principal,
                    session,
                    sequence,
                    payload_path,
                    seeds=running_endpoints(exclude=followers[0]),
                )
            else:
                command = [
                    binaries["light-stream-testkit"],
                    "client-publish",
                    "--endpoint",
                    nodes[followers[1]]["endpoint"],
                    "--cluster-id",
                    cluster,
                    "--stream-id",
                    stream,
                    "--principal",
                    principal,
                    "--session",
                    session,
                    "--sequence",
                    str(sequence),
                    "--file",
                    payload_path,
                ]
                for endpoint in running_endpoints(exclude=followers[1]):
                    command.extend(["--seed", endpoint])
            result = parse_json_output(
                runner.run(command, f"ls02b-publish-{sequence}", timeout=15),
                f"LS02b publish {sequence}",
            )
            acknowledgements.append(
                {
                    "request_id": request_id,
                    "payload_sha256": hashlib.sha256(payload).hexdigest(),
                }
            )
            outcomes.append({"request_id": request_id, "status": "acknowledged"})
            if result["receipt"]["range"]["first"] != sequence - 1:
                raise VerificationError("LS02b publish returned a noncontiguous offset")

        fetch = parse_json_output(
            runner.run(
                cli_endpoint_command(
                    binaries["light-streamctl"],
                    nodes[followers[1]]["endpoint"],
                    seeds=running_endpoints(exclude=followers[1]),
                )
                + [
                    "fetch",
                    "--cluster-id",
                    cluster,
                    "--stream-id",
                    stream,
                    "--offset",
                    "0",
                    "--limit",
                    "64",
                ],
                "ls02b-fetch-before-failover",
                timeout=15,
            ),
            "LS02b fetch before failover",
        )
        if payloads_from_page(fetch) != records[:2]:
            raise VerificationError("LS02b pre-failover fetch differs from the byte ledger")
        reads.append({"stage": "before_failover", "record_count": 2})
        if verify_checkpoints:
            checkpoint_before_failover = parse_json_output(
                runner.run(
                    cli_endpoint_command(
                        binaries["light-streamctl"],
                        nodes[leader]["endpoint"],
                        seeds=running_endpoints(exclude=leader),
                    )
                    + [
                        "checkpoint",
                        "advance",
                        "--cluster-id",
                        cluster,
                        "--stream-id",
                        stream,
                        "--consumer",
                        "ls07-ha",
                        "--expect-missing",
                        "--offset",
                        "1",
                        "--principal",
                        "ls07-ha-consumer",
                        "--mutation-session",
                        deterministic_uuid(rng),
                        "--sequence",
                        "1",
                    ],
                    "ls07-ha-checkpoint-before-failover",
                    timeout=15,
                ),
                "LS07 HA checkpoint before failover",
            )

        stop_node(leader, abrupt=True)
        faults.append({"fault": "kill_data_leader", "node_id": leader})
        new_leader = wait_for_data_leader(
            artifacts,
            runner,
            binaries["light-streamctl"],
            nodes,
            profile["leader_loss_seconds"],
            "ls02b-failover-election",
            previous=leader,
        )
        elections.append({"stage": "after_kill", "leader": new_leader})
        if new_leader == leader:
            raise VerificationError("leader loss did not produce a different leader")
        wait_for_running_group_leader(
            "control",
            1,
            "ls02b-control-after-data-failover",
        )
        wait_bootstrap_partition_ready(
            "ls02b-application-after-data-failover",
            nodes[new_leader]["endpoint"],
        )
        if verify_checkpoints:
            checkpoint_after_failover = parse_json_output(
                runner.run(
                    cli_endpoint_command(
                        binaries["light-streamctl"],
                        nodes[new_leader]["endpoint"],
                        seeds=running_endpoints(exclude=new_leader),
                    )
                    + [
                        "checkpoint",
                        "get",
                        "--cluster-id",
                        cluster,
                        "--stream-id",
                        stream,
                        "--consumer",
                        "ls07-ha",
                    ],
                    "ls07-ha-checkpoint-after-failover",
                    timeout=15,
                ),
                "LS07 HA checkpoint after failover",
            )
            if (
                checkpoint_after_failover["checkpoint"]["cursor"]["next_offset"]
                != 1
            ):
                raise VerificationError("LS07 checkpoint changed during leader failover")
            write_json(
                artifacts / "ls07" / "checkpoint-failover.json",
                {
                    "old_leader": leader,
                    "new_leader": new_leader,
                    "before": checkpoint_before_failover,
                    "after": checkpoint_after_failover,
                    "verdict": "PASS",
                },
            )
            write_json(
                artifacts / "l03.json",
                {
                    "old_leader": leader,
                    "new_leader": new_leader,
                    "checkpoint": checkpoint_after_failover["checkpoint"],
                    "metadata_refresh": "PASS",
                    "verdict": "PASS",
                },
            )

        payload = records[2]
        payload_path = write_payload(3, payload)
        request_id = f"{principal}:{session}:3"
        attempts.append(
            {
                "request_id": request_id,
                "payload_sha256": hashlib.sha256(payload).hexdigest(),
            }
        )
        endpoint = next(
            nodes[node_id]["endpoint"]
            for node_id in (1, 2, 3)
            if node_id != new_leader and nodes[node_id]["server"].is_running()
        )
        runner.run(
            publish_command(
                binaries["light-streamctl"],
                endpoint,
                cluster,
                stream,
                principal,
                session,
                3,
                payload_path,
                seeds=running_endpoints(),
            ),
            "ls02b-publish-after-failover",
            timeout=15,
        )
        acknowledgements.append(
            {
                "request_id": request_id,
                "payload_sha256": hashlib.sha256(payload).hexdigest(),
            }
        )
        outcomes.append({"request_id": request_id, "status": "acknowledged"})

        start_node(leader, "restart-after-kill")
        catch_up = wait_for_follower_catch_up(
            artifacts,
            runner,
            binaries["light-streamctl"],
            nodes,
            leader,
            profile["write_readiness_seconds"] + 10,
            "ls02b-killed-leader-catch-up",
        )

        leader = wait_for_data_leader(
            artifacts,
            runner,
            binaries["light-streamctl"],
            nodes,
            profile["leader_loss_seconds"],
            "ls02b-before-directed-isolation",
        )
        healthy_ids = [node_id for node_id in (1, 2, 3) if node_id != leader]
        isolated_links = set_bidirectional_links(leader, healthy_ids, "drop")
        faults.append(
            {
                "fault": "isolate_live_data_leader",
                "node_id": leader,
                "links": isolated_links,
            }
        )
        majority_leader = wait_for_data_leader(
            artifacts,
            runner,
            binaries["light-streamctl"],
            nodes,
            profile["leader_loss_seconds"],
            "ls02b-directed-isolation-election",
            previous=leader,
            node_ids=healthy_ids,
        )
        elections.append(
            {
                "stage": "during_directed_isolation",
                "leader": majority_leader,
                "isolated_old_leader": leader,
            }
        )
        isolated_session = deterministic_uuid(rng)
        isolated_payload = write_payload(900, os.urandom(113))
        isolated_write = parse_json_output(
            runner.run(
                publish_command(
                    binaries["light-streamctl"],
                    nodes[leader]["endpoint"],
                    cluster,
                    stream,
                    f"{principal}-isolated",
                    isolated_session,
                    1,
                    isolated_payload,
                    no_retry=True,
                    deadline_ms=1500,
                    route_group_id=bootstrap_route["group"],
                    route_revision=bootstrap_route["route_revision"],
                ),
                "ls02b-directed-isolation-write",
                expected_codes=(5,),
                timeout=5,
            ),
            "directed isolation write",
        )
        if isolated_write.get("ok") is not False or "receipt" in isolated_write:
            raise VerificationError("isolated old leader acknowledged a write")
        isolated_fetch = parse_json_output(
            runner.run(
                cli_endpoint_command(
                    binaries["light-streamctl"],
                    nodes[leader]["endpoint"],
                    no_retry=True,
                    deadline_ms=1500,
                )
                + [
                    "fetch",
                    "--cluster-id",
                    cluster,
                    "--stream-id",
                    stream,
                    "--offset",
                    str(len(records)),
                    "--limit",
                    "1",
                    "--route-group-id",
                    str(bootstrap_route["group"]),
                    "--route-revision",
                    str(bootstrap_route["route_revision"]),
                ],
                "ls02b-directed-isolation-fetch",
                expected_codes=(5,),
                timeout=5,
            ),
            "directed isolation fetch",
        )
        if isolated_fetch.get("ok") is not False or "page" in isolated_fetch:
            raise VerificationError("isolated old leader returned a fresh fetch")
        isolated_receipt = parse_json_output(
            runner.run(
                receipt_command(
                    binaries["light-streamctl"],
                    nodes[leader]["endpoint"],
                    cluster,
                    stream,
                    f"{principal}-isolated",
                    isolated_session,
                    1,
                    no_retry=True,
                    deadline_ms=1500,
                    route_group_id=bootstrap_route["group"],
                    route_revision=bootstrap_route["route_revision"],
                ),
                "ls02b-directed-isolation-receipt",
                expected_codes=(5,),
                timeout=5,
            ),
            "directed isolation receipt",
        )
        if isolated_receipt.get("ok") is not False or "receipt" in isolated_receipt:
            raise VerificationError("isolated old leader returned a fresh receipt")

        e04_payload = os.urandom(683)
        e04_sequence = 4
        e04_path = write_payload(e04_sequence, e04_payload)
        e04_request = f"{principal}:{session}:{e04_sequence}"
        attempts.append(
            {
                "request_id": e04_request,
                "payload_sha256": hashlib.sha256(e04_payload).hexdigest(),
            }
        )
        e04_publish = parse_json_output(
            runner.run(
                publish_command(
                    binaries["light-streamctl"],
                    nodes[healthy_ids[0]]["endpoint"],
                    cluster,
                    stream,
                    principal,
                    session,
                    e04_sequence,
                    e04_path,
                    seeds=[
                        nodes[node_id]["endpoint"]
                        for node_id in healthy_ids
                        if node_id != healthy_ids[0]
                    ],
                    deadline_ms=5000,
                ),
                "ls02b-directed-isolation-majority-publish",
                timeout=10,
            ),
            "directed isolation majority publish",
        )
        acknowledgements.append(
            {
                "request_id": e04_request,
                "payload_sha256": hashlib.sha256(e04_payload).hexdigest(),
            }
        )
        outcomes.append({"request_id": e04_request, "status": "acknowledged"})
        records.append(e04_payload)
        healed_links = set_bidirectional_links(leader, healthy_ids, "pass")
        healed_leader = wait_for_data_leader(
            artifacts,
            runner,
            binaries["light-streamctl"],
            nodes,
            profile["leader_loss_seconds"],
            "ls02b-directed-isolation-healed-election",
            previous=leader,
        )
        e04_catch_up = wait_for_follower_catch_up(
            artifacts,
            runner,
            binaries["light-streamctl"],
            nodes,
            leader,
            profile["write_readiness_seconds"] + 10,
            "ls02b-directed-isolation-catch-up",
        )
        healed_fetch = parse_json_output(
            runner.run(
                cli_endpoint_command(
                    binaries["light-streamctl"],
                    nodes[leader]["endpoint"],
                    seeds=running_endpoints(exclude=leader),
                )
                + [
                    "fetch",
                    "--cluster-id",
                    cluster,
                    "--stream-id",
                    stream,
                    "--offset",
                    "0",
                    "--limit",
                    "64",
                ],
                "ls02b-directed-isolation-healed-fetch",
                timeout=15,
            ),
            "directed isolation healed fetch",
        )
        if payloads_from_page(healed_fetch) != records:
            raise VerificationError("healed node did not expose every acknowledged byte")
        e04 = {
            "verdict": "PASS",
            "isolated_old_leader": leader,
            "majority_leader": majority_leader,
            "healed_leader": healed_leader,
            "old_leader_process_alive": nodes[leader]["server"].is_running(),
            "successful_acknowledgements_on_old_leader": 0,
            "isolated_write": isolated_write,
            "isolated_fetch": isolated_fetch,
            "isolated_receipt": isolated_receipt,
            "majority_publish": e04_publish,
            "catch_up": e04_catch_up,
            "drop_links": isolated_links,
            "healed_links": healed_links,
        }

        leader = healed_leader
        lagging = next(node_id for node_id in (1, 2, 3) if node_id != leader)
        delay_ms = profile["ls02b"]["directed_delay_ms"]
        delayed_links = set_bidirectional_links(
            lagging,
            [node_id for node_id in (1, 2, 3) if node_id != lagging],
            "delay",
            delay_ms,
        )
        faults.append(
            {
                "fault": "delay_live_follower_links",
                "node_id": lagging,
                "delay_ms": delay_ms,
                "links": delayed_links,
            }
        )
        delay_before = data_group(
            read_node_diagnostics(
                artifacts,
                runner,
                binaries["light-streamctl"],
                nodes[lagging],
                "ls02b-delay-before",
            )
        )
        delay_durations = []
        delay_payloads = [os.urandom(401), os.urandom(557), os.urandom(719)]
        for sequence, payload in enumerate(delay_payloads, start=5):
            payload_path = write_payload(sequence, payload)
            request_id = f"{principal}:{session}:{sequence}"
            attempts.append(
                {
                    "request_id": request_id,
                    "payload_sha256": hashlib.sha256(payload).hexdigest(),
                }
            )
            publish_started = time.monotonic()
            runner.run(
                publish_command(
                    binaries["light-streamctl"],
                    nodes[leader]["endpoint"],
                    cluster,
                    stream,
                    principal,
                    session,
                    sequence,
                    payload_path,
                    seeds=[
                        nodes[node_id]["endpoint"]
                        for node_id in (1, 2, 3)
                        if node_id != lagging and node_id != leader
                    ],
                    deadline_ms=profile["ls02b"]["majority_publish_deadline_ms"],
                ),
                f"ls02b-delayed-follower-publish-{sequence}",
                timeout=profile["ls02b"]["majority_publish_deadline_ms"] / 1000 + 5,
            )
            elapsed = time.monotonic() - publish_started
            if elapsed * 1000 > profile["ls02b"]["majority_publish_deadline_ms"]:
                raise VerificationError("healthy majority missed the declared publish deadline")
            delay_durations.append(elapsed)
            acknowledgements.append(
                {
                    "request_id": request_id,
                    "payload_sha256": hashlib.sha256(payload).hexdigest(),
                }
            )
            outcomes.append({"request_id": request_id, "status": "acknowledged"})
        records.extend(delay_payloads)
        delay_leader = data_group(
            read_node_diagnostics(
                artifacts,
                runner,
                binaries["light-streamctl"],
                nodes[leader],
                "ls02b-delay-leader",
            )
        )
        delay_follower = data_group(
            read_node_diagnostics(
                artifacts,
                runner,
                binaries["light-streamctl"],
                nodes[lagging],
                "ls02b-delay-follower",
            )
        )
        assert_exact_memberships(
            read_node_diagnostics(
                artifacts,
                runner,
                binaries["light-streamctl"],
                nodes[lagging],
                "ls02b-delay-membership",
            ),
            [1, 2, 3],
        )
        if not any(
            delay_follower.get(field) is None
            or delay_follower[field] < delay_leader[field]
            for field in ("last_log_index", "local_committed_index", "last_applied_index")
        ):
            raise VerificationError("delayed follower did not lag the healthy majority")
        healed_delay_links = set_bidirectional_links(
            lagging,
            [node_id for node_id in (1, 2, 3) if node_id != lagging],
            "pass",
        )
        delay_catch_up = wait_for_follower_catch_up(
            artifacts,
            runner,
            binaries["light-streamctl"],
            nodes,
            lagging,
            profile["write_readiness_seconds"] + 10,
            "ls02b-delayed-follower-catch-up",
        )
        e06 = {
            "verdict": "PASS",
            "follower_node_id": lagging,
            "delay_ms": delay_ms,
            "publish_deadline_ms": profile["ls02b"]["majority_publish_deadline_ms"],
            "publish_durations_seconds": delay_durations,
            "before": delay_before,
            "lagging": delay_follower,
            "catch_up": delay_catch_up,
            "delayed_links": delayed_links,
            "healed_links": healed_delay_links,
        }

        leader = wait_for_data_leader(
            artifacts,
            runner,
            binaries["light-streamctl"],
            nodes,
            profile["leader_loss_seconds"],
            "ls02b-before-suffix",
        )
        lagging = next(node_id for node_id in (1, 2, 3) if node_id != leader)
        stop_node(lagging)
        faults.append({"fault": "stop_follower_for_suffix", "node_id": lagging})
        suffix_payloads = [os.urandom(521), os.urandom(777)]
        for sequence, payload in enumerate(suffix_payloads, start=8):
            payload_path = write_payload(sequence, payload)
            request_id = f"{principal}:{session}:{sequence}"
            attempts.append(
                {
                    "request_id": request_id,
                    "payload_sha256": hashlib.sha256(payload).hexdigest(),
                }
            )
            runner.run(
                publish_command(
                    binaries["light-streamctl"],
                    nodes[leader]["endpoint"],
                    cluster,
                    stream,
                    principal,
                    session,
                    sequence,
                    payload_path,
                    seeds=running_endpoints(),
                ),
                f"ls02b-suffix-publish-{sequence}",
                timeout=15,
            )
            acknowledgements.append(
                {
                    "request_id": request_id,
                    "payload_sha256": hashlib.sha256(payload).hexdigest(),
                }
            )
            outcomes.append({"request_id": request_id, "status": "acknowledged"})
        records.extend(suffix_payloads)
        start_node(lagging, "restart-for-suffix")
        suffix_catch_up = wait_for_follower_catch_up(
            artifacts,
            runner,
            binaries["light-streamctl"],
            nodes,
            lagging,
            profile["write_readiness_seconds"] + 10,
            "ls02b-suffix-catch-up",
        )

        leader = wait_for_data_leader(
            artifacts,
            runner,
            binaries["light-streamctl"],
            nodes,
            profile["leader_loss_seconds"],
            "ls02b-before-quorum-loss",
        )
        stopped = [node_id for node_id in (1, 2, 3) if node_id != leader]
        for node_id in stopped:
            stop_node(node_id)
        faults.append({"fault": "stop_two_voters", "nodes": stopped, "remaining": leader})
        if not nodes[leader]["server"].is_running():
            raise VerificationError("majority-loss fault stopped the diagnosed data leader")
        quorum_payload = write_payload(901, os.urandom(83))
        quorum_attempt = runner.run(
            publish_command(
                binaries["light-streamctl"],
                nodes[leader]["endpoint"],
                cluster,
                stream,
                principal,
                session,
                901,
                quorum_payload,
                no_retry=True,
                deadline_ms=4500,
                route_group_id=bootstrap_route["group"],
                route_revision=bootstrap_route["route_revision"],
            ),
            "ls02b-quorum-loss-write",
            expected_codes=(5,),
            timeout=8,
        )
        quorum_value = parse_json_output(quorum_attempt, "quorum loss write")
        quorum_detail = quorum_value.get("error", {}).get("detail", {})
        data_refusal = (
            quorum_detail.get("group") == "data"
            and quorum_detail.get("outcome") == "ambiguous_commit"
            and quorum_detail.get("request", {})
            .get("request", {})
            .get("sequence")
            == 901
        )
        control_route_refusal = (
            quorum_detail.get("group") == "control"
            and quorum_detail.get("outcome") == "not_applicable"
            and quorum_detail.get("request") is None
        )
        if (
            quorum_value.get("ok") is not False
            or quorum_value.get("error", {}).get("code") != "quorum_unavailable"
            or not (data_refusal or control_route_refusal)
        ):
            raise VerificationError(
                "minority publish did not return a typed data or route quorum refusal"
            )
        minority_fetch = parse_json_output(
            runner.run(
                cli_endpoint_command(
                    binaries["light-streamctl"],
                    nodes[leader]["endpoint"],
                    no_retry=True,
                    deadline_ms=4500,
                )
                + [
                    "fetch",
                    "--cluster-id",
                    cluster,
                    "--stream-id",
                    stream,
                    "--offset",
                    str(len(records)),
                    "--limit",
                    "1",
                    "--route-group-id",
                    str(bootstrap_route["group"]),
                    "--route-revision",
                    str(bootstrap_route["route_revision"]),
                ],
                "ls02b-quorum-loss-fetch",
                expected_codes=(5,),
                timeout=8,
            ),
            "quorum loss fetch",
        )
        fetch_detail = minority_fetch.get("error", {}).get("detail", {})
        fetch_code = minority_fetch.get("error", {}).get("code")
        fetch_quorum_refusal = (
            fetch_code == "quorum_unavailable"
            and fetch_detail.get("group") in ("control", "data")
            and fetch_detail.get("outcome") == "not_applicable"
            and fetch_detail.get("request") is None
        )
        fetch_leadership_refusal = (
            fetch_code == "not_leader"
            and fetch_detail.get("group") in ("control", "data")
        )
        if minority_fetch.get("ok") is not False or not (
            fetch_quorum_refusal or fetch_leadership_refusal
        ):
            raise VerificationError("minority fetch did not refuse a fresh read")
        minority_receipt = parse_json_output(
            runner.run(
                receipt_command(
                    binaries["light-streamctl"],
                    nodes[leader]["endpoint"],
                    cluster,
                    stream,
                    principal,
                    session,
                    901,
                    no_retry=True,
                    deadline_ms=4500,
                    route_group_id=bootstrap_route["group"],
                    route_revision=bootstrap_route["route_revision"],
                ),
                "ls02b-quorum-loss-receipt",
                expected_codes=(5,),
                timeout=8,
            ),
            "quorum loss receipt",
        )
        receipt_detail = minority_receipt.get("error", {}).get("detail", {})
        receipt_code = minority_receipt.get("error", {}).get("code")
        receipt_quorum_refusal = (
            receipt_code == "quorum_unavailable"
            and receipt_detail.get("group") in ("control", "data")
            and receipt_detail.get("outcome") == "not_applicable"
            and receipt_detail.get("request") is None
        )
        receipt_leadership_refusal = (
            receipt_code == "not_leader"
            and receipt_detail.get("group") in ("control", "data")
        )
        if minority_receipt.get("ok") is not False or not (
            receipt_quorum_refusal or receipt_leadership_refusal
        ):
            raise VerificationError("minority receipt lookup did not refuse a fresh read")
        if not nodes[leader]["server"].is_running():
            raise VerificationError("old data leader died during minority refusal checks")
        minority_refusal = {
            "leader_node_id": leader,
            "leader_process_alive": True,
            "stopped_voters": stopped,
            "publish": quorum_value,
            "fetch": minority_fetch,
            "receipt": minority_receipt,
            "successful_acknowledgements": 0,
        }
        stop_node(leader, abrupt=True)
        faults.append(
            {
                "fault": "kill_ambiguous_old_leader",
                "node_id": leader,
                "reason": "prevent an unacknowledged tail from becoming committed on heal",
            }
        )
        for node_id in stopped:
            start_node(node_id, "restart-after-quorum")
        wait_for_data_leader(
            artifacts,
            runner,
            binaries["light-streamctl"],
            nodes,
            profile["leader_loss_seconds"],
            "ls02b-quorum-heal-election",
            previous=leader,
        )
        wait_for_running_group_leader(
            "control",
            1,
            "ls02b-control-after-quorum-heal",
        )
        start_node(leader, "restart-ambiguous-old-leader")
        membership_observations.append(
            {
                "stage": "after_quorum_restart",
                "nodes": wait_for_uniform_active(
                    artifacts,
                    runner,
                    binaries["light-streamctl"],
                    nodes,
                    profile["write_readiness_seconds"] + 10,
                    "ls02b-after-quorum-restart",
                ),
            }
        )

        lost_payload = os.urandom(911)
        records.append(lost_payload)
        lost_sequence = 10
        lost_path = write_payload(lost_sequence, lost_payload)
        lost_request = f"{principal}:{session}:{lost_sequence}"
        attempts.append(
            {
                "request_id": lost_request,
                "payload_sha256": hashlib.sha256(lost_payload).hexdigest(),
            }
        )
        first_leader_id = wait_for_data_leader(
            artifacts,
            runner,
            binaries["light-streamctl"],
            nodes,
            profile["leader_loss_seconds"],
            "ls02b-lost-response-leader",
        )
        wait_bootstrap_partition_ready(
            "ls02b-application-before-lost-response",
            nodes[first_leader_id]["endpoint"],
        )
        response_proxy = OneShotResponseDropProxy(
            free_port(),
            int(nodes[first_leader_id]["public_address"].rsplit(":", 1)[1]),
        )
        lost_attempt = runner.run(
            publish_command(
                binaries["light-streamctl"],
                response_proxy.endpoint,
                cluster,
                stream,
                principal,
                session,
                lost_sequence,
                lost_path,
                no_retry=True,
                deadline_ms=5000,
                route_group_id=bootstrap_route["group"],
                route_revision=bootstrap_route["route_revision"],
            ),
            "ls02b-lost-response",
            expected_codes=(1, 5),
            timeout=15,
        )
        response_proxy.wait()
        faults.append(
            {
                "fault": "drop_successful_network_response",
                "request_id": lost_request,
                "client_result": parse_json_output(
                    lost_attempt, "lost response client result"
                ),
                "network_response_drop": True,
            }
        )
        stop_node(first_leader_id, abrupt=True)
        faults.append(
            {
                "fault": "kill_data_leader_after_response_drop",
                "node_id": first_leader_id,
                "request_id": lost_request,
            }
        )
        retry_leader_id = wait_for_data_leader(
            artifacts,
            runner,
            binaries["light-streamctl"],
            nodes,
            profile["leader_loss_seconds"],
            "ls02b-lost-response-failover",
            previous=first_leader_id,
        )
        if retry_leader_id == first_leader_id:
            raise VerificationError("lost-response failover kept the killed leader")
        wait_for_running_group_leader(
            "control",
            1,
            "ls02b-control-after-lost-response-failover",
        )
        wait_bootstrap_partition_ready(
            "ls02b-application-after-lost-response-failover",
            nodes[retry_leader_id]["endpoint"],
        )
        elections.append(
            {
                "stage": "after_lost_response_kill",
                "leader": retry_leader_id,
            }
        )
        retry_endpoint = nodes[retry_leader_id]["endpoint"]
        pre_retry_receipt = parse_json_output(
            runner.run(
                receipt_command(
                    binaries["light-streamctl"],
                    retry_endpoint,
                    cluster,
                    stream,
                    principal,
                    session,
                    lost_sequence,
                    route_group_id=bootstrap_route["group"],
                    route_revision=bootstrap_route["route_revision"],
                ),
                "ls02b-lost-response-pre-retry-receipt",
                timeout=15,
            ),
            "lost response pre-retry receipt",
        )
        expected_range = {"first": len(records) - 1, "count": 1}
        expected_request = {
            "principal": principal,
            "session": session,
            "sequence": lost_sequence,
        }
        pre_retry_range = pre_retry_receipt["receipt"]["range"]
        if {
            "first": pre_retry_range["first"],
            "count": pre_retry_range["count"],
        } != expected_range or pre_retry_receipt["receipt"]["request"] != expected_request:
            raise VerificationError(
                "pre-retry receipt did not prove the first publish committed"
            )
        retry = parse_json_output(
            runner.run(
                publish_command(
                    binaries["light-streamctl"],
                    retry_endpoint,
                    cluster,
                    stream,
                    principal,
                    session,
                    lost_sequence,
                    lost_path,
                    route_group_id=bootstrap_route["group"],
                    route_revision=bootstrap_route["route_revision"],
                ),
                "ls02b-lost-response-retry",
                timeout=15,
            ),
            "lost response retry",
        )
        if retry["receipt"] != pre_retry_receipt["receipt"]:
            raise VerificationError(
                "same-identity retry did not return the pre-retry committed range"
            )
        acknowledgements.append(
            {
                "request_id": lost_request,
                "payload_sha256": hashlib.sha256(lost_payload).hexdigest(),
            }
        )
        outcomes.append({"request_id": lost_request, "status": "acknowledged"})
        conflict_path = write_payload(906, b"conflicting-retry")
        conflict = runner.run(
            publish_command(
                binaries["light-streamctl"],
                retry_endpoint,
                cluster,
                stream,
                principal,
                session,
                lost_sequence,
                conflict_path,
                route_group_id=bootstrap_route["group"],
                route_revision=bootstrap_route["route_revision"],
            ),
            "ls02b-lost-response-conflict",
            expected_codes=(4,),
            timeout=15,
        )
        conflict_value = parse_json_output(conflict, "lost response conflict")
        if conflict_value.get("error", {}).get("code") != "receipt_conflict":
            raise VerificationError("conflicting retry failed for the wrong reason")

        for node_id in (1, 2, 3):
            stop_node(node_id)
        for node_id in (1, 2, 3):
            start_node(node_id, "full-restart")
        membership_observations.append(
            {
                "stage": "full_restart",
                "nodes": wait_for_uniform_active(
                    artifacts,
                    runner,
                    binaries["light-streamctl"],
                    nodes,
                    profile["write_readiness_seconds"] + 10,
                    "ls02b-full-restart-active",
                ),
            }
        )
        restart_fetch = parse_json_output(
            runner.run(
                cli_endpoint_command(
                    binaries["light-streamctl"],
                    nodes[1]["endpoint"],
                    seeds=[nodes[2]["endpoint"], nodes[3]["endpoint"]],
                )
                + [
                    "fetch",
                    "--cluster-id",
                    cluster,
                    "--stream-id",
                    stream,
                    "--offset",
                    "0",
                    "--limit",
                    "64",
                ],
                "ls02b-fetch-after-full-restart",
                timeout=15,
            ),
            "LS02b fetch after full restart",
        )
        if payloads_from_page(restart_fetch) != records:
            raise VerificationError("full restart fetch differs from every acknowledged byte")
        receipt = parse_json_output(
            runner.run(
                cli_endpoint_command(
                    binaries["light-streamctl"],
                    nodes[2]["endpoint"],
                    seeds=[nodes[1]["endpoint"], nodes[3]["endpoint"]],
                )
                + [
                    "receipt",
                    "--cluster-id",
                    cluster,
                    "--stream-id",
                    stream,
                    "--principal",
                    principal,
                    "--session",
                    session,
                    "--sequence",
                    str(lost_sequence),
                ],
                "ls02b-receipt-after-full-restart",
                timeout=15,
            ),
            "LS02b receipt after full restart",
        )
        if receipt["receipt"] != pre_retry_receipt["receipt"]:
            raise VerificationError(
                "full-restart receipt did not preserve the pre-retry committed range"
            )
        reads.append({"stage": "after_full_restart", "record_count": len(records)})
    finally:
        for node_id in sorted(nodes):
            server = nodes[node_id]["server"]
            if server.is_running():
                try:
                    server.stop()
                except VerificationError:
                    server.kill()
        proxy_stop_errors = []
        for key in sorted(proxies):
            try:
                proxies[key].stop()
            except VerificationError as error:
                proxy_stop_errors.append(str(error))
        write_json(
            artifacts / "directed-proxies.json",
            {
                "proxies": [
                    proxies[key].snapshot()
                    for key in sorted(proxies)
                ],
                "stop_errors": proxy_stop_errors,
            },
        )
        if proxy_stop_errors:
            raise VerificationError("; ".join(proxy_stop_errors))

    for path, values in (
        (artifacts / "attempts.jsonl", attempts),
        (artifacts / "acks.jsonl", acknowledgements),
        (artifacts / "outcomes.jsonl", outcomes),
        (artifacts / "reads.jsonl", reads),
        (artifacts / "faults.jsonl", faults),
        (artifacts / "elections.jsonl", elections),
    ):
        path.write_text("".join(json.dumps(value, sort_keys=True) + "\n" for value in values))
    runner.run(
        [
            binaries["light-stream-testkit"],
            "oracle-check",
            "--attempts",
            artifacts / "attempts.jsonl",
            "--acknowledgements",
            artifacts / "acks.jsonl",
            "--outcomes",
            artifacts / "outcomes.jsonl",
        ],
        "ls02b-independent-oracle",
        timeout=15,
    )
    write_json(
        artifacts / "ls02b-summary.json",
        {
            "verdict": "PASS",
            "bootstrap": bootstrap,
            "cluster_id": cluster,
            "stream_id": stream,
            "acknowledged_records": len(records),
            "initial_leader": elections[0]["leader"],
            "new_leader": elections[1]["leader"],
            "killed_leader_catch_up": catch_up,
            "directed_isolation": e04,
            "delayed_follower": e06,
            "suffix_catch_up": suffix_catch_up,
            "lost_response_pre_retry_receipt": pre_retry_receipt,
            "lost_response_retry": retry,
            "receipt_after_full_restart": receipt,
        },
    )
    write_json(
        artifacts / "e02.json",
        {
            "verdict": "PASS",
            "exact_voters": [1, 2, 3],
            "empty_learners": True,
            "observed_memberships": membership_observations,
            "typed_data_leader_hints": [write_hint, read_hint],
            "record_count": len(records),
        },
    )
    write_json(
        artifacts / "e03.json",
        {"verdict": "PASS", "elections": elections},
    )
    write_json(
        artifacts / "e04.json",
        e04,
    )
    write_json(
        artifacts / "e05.json",
        {
            "verdict": "PASS",
            "successful_acknowledgements_after_two_voters_stopped": (
                minority_refusal["successful_acknowledgements"]
            ),
            "error": quorum_value,
        },
    )
    write_json(
        artifacts / "e06.json",
        e06,
    )
    write_json(
        artifacts / "e07.json",
        {"verdict": "PASS", "restart_catch_up": catch_up},
    )
    write_json(
        artifacts / "e08.json",
        {
            "verdict": "UNSUPPORTED_LS06",
            "snapshot_after_purge": False,
        },
    )
    write_json(
        artifacts / "e09.json",
        {
            "verdict": "PASS",
            "same_identity_retry": True,
            "receipt_after_full_restart": True,
            "network_response_drop": True,
            "killed_diagnosed_leader_before_retry": first_leader_id,
            "new_leader_before_retry": retry_leader_id,
            "pre_retry_receipt": pre_retry_receipt,
            "retry_receipt": retry,
            "exact_same_range": True,
        },
    )
    write_json(
        artifacts / "unsupported.json",
        {
            "snapshot_after_purge": "UNSUPPORTED_LS06",
            "ls03_plus": "UNSUPPORTED",
            "secured_mode": "UNSUPPORTED_LS08",
            "independent_hosts": "BLOCKED",
        },
    )


def run_ls03_scenario(artifacts, runner, binaries, revision, profile, seed):
    rng = random.Random(seed ^ 0x4C533033)
    cluster = deterministic_uuid(rng)
    bootstrap_stream = deterministic_uuid(rng)
    session = deterministic_uuid(rng)
    limits = profile["ls03"]
    used_ports = set()
    configs = {}
    nodes = {}

    for node_id in (1, 2, 3):
        public_port = free_port()
        while public_port in used_ports:
            public_port = free_port()
        used_ports.add(public_port)
        peer_port = free_port()
        while peer_port in used_ports:
            peer_port = free_port()
        used_ports.add(peer_port)
        configs[node_id] = {
            "node_id": node_id,
            "public_address": f"127.0.0.1:{public_port}",
            "peer_address": f"127.0.0.1:{peer_port}",
            "endpoint": f"http://127.0.0.1:{public_port}",
            "peer_uri": f"http://127.0.0.1:{peer_port}",
            "data_dir": artifacts / "scratch" / "success" / f"ls03-node-{node_id}",
        }

    def start_node(node_id, suffix):
        config = configs[node_id]
        server = OwnedServer(
            binaries["light-streamd"],
            config["data_dir"],
            artifacts / "node-logs",
            f"ls03-node-{node_id}-{suffix}",
            node_id=node_id,
            public_address=config["public_address"],
            peer_address=config["peer_address"],
            advertise_public_uri=config["endpoint"],
            advertise_peer_uri=config["peer_uri"],
            max_data_groups=limits["data_groups"],
            max_streams=limits["max_streams"],
            max_partitions_per_stream=limits["max_partitions_per_stream"],
            rocksdb_cache_bytes=limits["rocksdb_cache_bytes"],
            rocksdb_write_buffer_bytes=limits["rocksdb_write_buffer_bytes"],
            verification_delay_group_id=limits["verification_delay_group_id"],
            verification_delay_ms=limits["verification_delay_ms"],
        )
        nodes[node_id] = {**config, "server": server}

    def endpoints():
        return [
            nodes[node_id]["endpoint"]
            for node_id in sorted(nodes)
            if nodes[node_id]["server"].is_running()
        ]

    def cli(endpoint, arguments, label, expected=(0,), timeout=20, seeds=True):
        command = cli_endpoint_command(
            binaries["light-streamctl"],
            endpoint,
            seeds=endpoints() if seeds else (),
            deadline_ms=15000,
        ) + arguments
        return parse_json_output(
            runner.run(command, label, expected_codes=expected, timeout=timeout),
            label,
        )

    def wait_for_group_leaders(label):
        deadline = time.monotonic() + profile["leader_loss_seconds"] + 20
        last = None
        while time.monotonic() < deadline:
            running_node_ids = {
                node_id
                for node_id in nodes
                if nodes[node_id]["server"].is_running()
            }
            for node_id in sorted(nodes):
                if not nodes[node_id]["server"].is_running():
                    continue
                try:
                    diagnostics = read_node_diagnostics(
                        artifacts,
                        runner,
                        binaries["light-streamctl"],
                        nodes[node_id],
                        f"{label}-node-{node_id}",
                    )
                    groups = {
                        group["group_id"]: group["current_leader"]
                        for group in diagnostics["groups"]
                    }
                    last = groups
                    if sorted(groups) == [1, 2, 3, 4, 5] and all(
                        leader in running_node_ids for leader in groups.values()
                    ):
                        return diagnostics
                except (VerificationError, subprocess.SubprocessError):
                    pass
            time.sleep(0.05)
        raise VerificationError(f"{label} did not observe leaders for every group: {last}")

    def wait_for_group_convergence(label):
        deadline = time.monotonic() + profile["leader_loss_seconds"] + 30
        last = None
        while time.monotonic() < deadline:
            observed = []
            try:
                for node_id in sorted(nodes):
                    if nodes[node_id]["server"].is_running():
                        observed.append(
                            read_node_diagnostics(
                                artifacts,
                                runner,
                                binaries["light-streamctl"],
                                nodes[node_id],
                                f"{label}-node-{node_id}",
                            )
                        )
                if len(observed) != 3:
                    raise VerificationError("all three voters must be running")
                by_group = {
                    group_id: [
                        next(
                            group
                            for group in diagnostics["groups"]
                            if group["group_id"] == group_id
                        )
                        for diagnostics in observed
                    ]
                    for group_id in (1, 2, 3, 4, 5)
                }
                last = {
                    str(group_id): [
                        {
                            "node": diagnostics["node_id"],
                            "leader": group["current_leader"],
                            "last_log": group["last_log_index"],
                            "committed": group["local_committed_index"],
                            "applied": group["last_applied_index"],
                        }
                        for diagnostics, group in zip(observed, groups)
                    ]
                    for group_id, groups in by_group.items()
                }
                converged = True
                for groups in by_group.values():
                    target = groups[0]["last_log_index"]
                    if target is None or any(
                        group["current_leader"] is None
                        or group["last_log_index"] != target
                        or group["local_committed_index"] != target
                        or group["last_applied_index"] != target
                        for group in groups
                    ):
                        converged = False
                        break
                if converged:
                    return observed
            except (KeyError, StopIteration, VerificationError, subprocess.SubprocessError):
                pass
            time.sleep(0.05)
        raise VerificationError(f"{label} did not converge every group: {last}")

    def create_stream(name, partitions, request_id, endpoint):
        return cli(
            endpoint,
            [
                "stream", "create",
                "--cluster-id", cluster,
                "--request-id", request_id,
                "--name", name,
                "--partitions", str(partitions),
            ],
            f"ls03-create-{name}",
            timeout=30,
        )["stream"]

    def route(stream_id, partition, endpoint, label):
        return cli(
            endpoint,
            [
                "stream", "route",
                "--cluster-id", cluster,
                "--stream-id", stream_id,
                "--partition", str(partition),
            ],
            label,
        )["route"]

    def wait_partition_ready(stream_id, partition, route_value, label):
        deadline = time.monotonic() + profile["write_readiness_seconds"] + 20
        attempt = 0
        last = None
        while time.monotonic() < deadline:
            attempt += 1
            leader = route_value["leader"]
            if leader is None:
                time.sleep(0.05)
                continue
            command = cli_endpoint_command(
                binaries["light-streamctl"],
                leader["public_uri"],
                seeds=endpoints(),
                deadline_ms=3000,
            ) + [
                "fetch",
                "--cluster-id",
                cluster,
                "--stream-id",
                stream_id,
                "--partition",
                str(partition),
                "--offset",
                "0",
                "--limit",
                "1",
            ]
            result = runner.run(
                command,
                f"{label}-attempt-{attempt}",
                expected_codes=(0, 5),
                timeout=5,
            )
            last = parse_json_output(result, f"{label} attempt {attempt}")
            if result.returncode == 0:
                return last
            time.sleep(0.05)
        raise VerificationError(f"{label} never reached linearizable read readiness: {last}")

    def publish(stream_id, partition, sequence, payload, endpoint, label, stale=None):
        sample = artifacts / "samples" / f"{label}.bin"
        sample.parent.mkdir(parents=True, exist_ok=True)
        sample.write_bytes(payload)
        arguments = [
            "publish",
            "--cluster-id", cluster,
            "--stream-id", stream_id,
            "--partition", str(partition),
            "--principal", f"verify-ls03-{seed}",
            "--session", session,
            "--sequence", str(sequence),
            "--file", str(sample),
        ]
        if stale is not None:
            arguments.extend([
                "--route-group-id", str(stale["group"]),
                "--route-revision", str(stale["route_revision"]),
            ])
        return cli(endpoint, arguments, label, timeout=30)["receipt"]

    try:
        for node_id in (1, 2, 3):
            start_node(node_id, "initial")
        bootstrap_command = cli_endpoint_command(
            binaries["light-streamctl"], nodes[1]["endpoint"], deadline_ms=45000
        ) + [
            "cluster", "bootstrap",
            "--cluster-id", cluster,
            "--stream-id", bootstrap_stream,
            "--stream-name", "bootstrap",
            "--seed-node-id", "1",
        ]
        for node_id in (1, 2, 3):
            bootstrap_command.extend([
                "--member",
                f"{node_id},{nodes[node_id]['endpoint']},{nodes[node_id]['peer_uri']}",
            ])
        bootstrap = parse_json_output(
            runner.run(bootstrap_command, "ls03-bootstrap", timeout=60),
            "LS03 bootstrap",
        )
        memberships = wait_for_uniform_active(
            artifacts,
            runner,
            binaries["light-streamctl"],
            nodes,
            profile["write_readiness_seconds"] + 20,
            "ls03-active",
        )
        diagnostics = wait_for_group_leaders("ls03-initial-leaders")
        wait_for_group_convergence("ls03-initial-convergence")
        if diagnostics["data_group_count"] != 4 or diagnostics["data_group_slots"] != 4:
            raise VerificationError(f"bounded group diagnostics are wrong: {diagnostics}")
        group_ids = [group["group_id"] for group in diagnostics["groups"]]
        if group_ids != [1, 2, 3, 4, 5]:
            raise VerificationError(f"unexpected durable group pool {group_ids}")
        db_counts = {}
        for node_id in (1, 2, 3):
            paths = list((configs[node_id]["data_dir"] / "groups").glob("*/rocksdb"))
            db_counts[str(node_id)] = len(paths)
            if len(paths) != 5:
                raise VerificationError(f"node {node_id} opened {len(paths)} group databases")
        write_json(artifacts / "l02.json", {"verdict": "PASS", "group_ids": group_ids, "db_counts": db_counts, "diagnostics": diagnostics})

        request_a = deterministic_uuid(rng)
        request_b = deterministic_uuid(rng)
        request_c = deterministic_uuid(rng)
        stream_a = create_stream("orders", 4, request_a, nodes[1]["endpoint"])

        control_leader = group_by_name(diagnostics, "control", 1)["current_leader"]
        proxy_port = free_port()
        response_proxy = OneShotResponseDropProxy(
            proxy_port,
            int(configs[control_leader]["public_address"].rsplit(":", 1)[1]),
        )
        dropped_command = cli_endpoint_command(
            binaries["light-streamctl"],
            response_proxy.endpoint,
            no_retry=True,
            deadline_ms=3000,
        ) + [
            "stream", "create",
            "--cluster-id", cluster,
            "--request-id", request_b,
            "--name", "events",
            "--partitions", "2",
        ]
        runner.run_dropped_response(
            dropped_command,
            "ls03-create-response-dropped",
            expected_codes=(1, 5),
            timeout=10,
        )
        response_proxy.wait()
        survivors = [node_id for node_id in (1, 2, 3) if node_id != control_leader]
        stream_b = create_stream("events", 2, request_b, nodes[survivors[0]]["endpoint"])
        stream_b_retry = create_stream("events", 2, request_b, nodes[survivors[1]]["endpoint"])
        if stream_b["stream"] != stream_b_retry["stream"]:
            raise VerificationError("create retry produced a second stream identity")
        metadata_leader = control_leader
        write_json(artifacts / "l04.json", {"verdict": "PASS", "request_id": request_b, "stream_id": stream_b["stream"], "response_dropped": True, "single_process_failure_with_multiple_groups": "DEFERRED_LS06"})

        stream_c = create_stream("metrics", 1, request_c, nodes[metadata_leader]["endpoint"])
        quota = cli(
            nodes[metadata_leader]["endpoint"],
            [
                "stream", "create",
                "--cluster-id", cluster,
                "--request-id", deterministic_uuid(rng),
                "--name", "over-quota",
                "--partitions", "1",
            ],
            "ls03-quota-rejection",
            expected=(4,),
        )
        listed = cli(nodes[metadata_leader]["endpoint"], ["stream", "list", "--cluster-id", cluster], "ls03-list")["streams"]
        if any(value["name"] == "over-quota" for value in listed):
            raise VerificationError("quota rejection partially activated a catalog entry")
        write_json(artifacts / "l09.json", {"verdict": "PASS", "rejection": quota, "active_stream_count": len(listed)})

        streams = [stream_a, stream_b, stream_c]
        routes = {}
        records = []
        sequence = 1
        for stream_value in streams:
            for placement in stream_value["placements"]:
                partition = placement["partition"]
                key = f"{stream_value['stream']}:{partition}"
                resolved = route(stream_value["stream"], partition, nodes[1]["endpoint"], f"ls03-route-{sequence}")
                wait_partition_ready(
                    stream_value["stream"],
                    partition,
                    resolved,
                    f"ls03-ready-{sequence}",
                )
                routes[key] = resolved
                payload = bytes(rng.getrandbits(8) for _ in range(257 + sequence))
                receipt = publish(stream_value["stream"], partition, sequence, payload, nodes[1]["endpoint"], f"ls03-publish-{sequence}")
                fetched = cli(
                    nodes[1]["endpoint"],
                    [
                        "fetch", "--cluster-id", cluster,
                        "--stream-id", stream_value["stream"],
                        "--partition", str(partition),
                        "--offset", "0", "--limit", "8",
                    ],
                    f"ls03-fetch-{sequence}",
                )
                observed = bytes(fetched["page"]["records"][0]["payload"])
                if observed != payload or receipt["range"]["first"] != 0:
                    raise VerificationError(f"partition ledger mismatch for {key}")
                records.append({"key": key, "sha256": hashlib.sha256(payload).hexdigest(), "route": resolved})
                sequence += 1
        write_json(artifacts / "l03.json", {"verdict": "PASS", "records": records, "routes": routes})

        after = read_node_diagnostics(
            artifacts, runner, binaries["light-streamctl"], nodes[1], "ls03-after-writes"
        )
        leaders = {str(group["group_id"]): group["current_leader"] for group in after["groups"] if group["group"] == "data"}
        progress = {str(group["group_id"]): group["last_applied_index"] for group in after["groups"] if group["group"] == "data"}
        write_json(artifacts / "partition-journey.json", {"verdict": "PASS", "leaders": leaders, "leaders_differ": len(set(leaders.values())) > 1, "independent_group_ids": sorted(leaders), "progress": progress, "records": records})

        delayed_group = limits["verification_delay_group_id"]
        slow_route = next(
            value["route"]
            for value in routes.values()
            if value["route"]["group"] == delayed_group
        )
        fast_route = next(
            value["route"]
            for value in routes.values()
            if value["route"]["group"] != delayed_group
        )
        slow_sample = artifacts / "samples" / "ls03-slow.bin"
        slow_sample.write_bytes(os.urandom(333))
        slow_command = cli_endpoint_command(binaries["light-streamctl"], nodes[1]["endpoint"], seeds=endpoints(), deadline_ms=10000) + [
            "publish", "--cluster-id", cluster,
            "--stream-id", slow_route["stream"], "--partition", str(slow_route["partition"]),
            "--principal", f"verify-ls03-{seed}", "--session", session, "--sequence", "100",
            "--file", str(slow_sample),
        ]
        slow_started = time.monotonic()
        slow_process = subprocess.Popen(slow_command, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        time.sleep(0.15)
        fast_started = time.monotonic()
        fast_receipt = publish(fast_route["stream"], fast_route["partition"], 101, os.urandom(211), nodes[1]["endpoint"], "ls03-fast-during-delay")
        fast_seconds = time.monotonic() - fast_started
        slow_stdout, slow_stderr = slow_process.communicate(timeout=15)
        if slow_process.returncode != 0:
            raise VerificationError(f"delayed group publish failed: {slow_stdout} {slow_stderr}")
        slow_seconds = time.monotonic() - slow_started
        delay_seconds = limits["verification_delay_ms"] / 1000
        if fast_seconds >= delay_seconds * 0.75 or slow_seconds < delay_seconds * 0.75:
            raise VerificationError(f"group delay was not isolated: fast={fast_seconds}, slow={slow_seconds}")
        write_json(artifacts / "l08.json", {"verdict": "PASS", "delayed_group": delayed_group, "unrelated_group": fast_route["group"], "configured_delay_ms": limits["verification_delay_ms"], "fast_seconds": fast_seconds, "slow_seconds": slow_seconds, "fast_receipt": fast_receipt})

        old_route = routes[f"{stream_a['stream']}:0"]["route"]
        current_diagnostics = read_node_diagnostics(
            artifacts, runner, binaries["light-streamctl"], nodes[1], "ls03-before-delete"
        )
        metadata_leader = group_by_name(current_diagnostics, "control", 1)["current_leader"]
        deleted = cli(nodes[metadata_leader]["endpoint"], ["stream", "delete", "--cluster-id", cluster, "--stream-id", stream_a["stream"]], "ls03-delete")["stream"]
        old_cursor = cli(
            nodes[1]["endpoint"],
            ["fetch", "--cluster-id", cluster, "--stream-id", stream_a["stream"], "--partition", "0", "--offset", "0", "--limit", "1"],
            "ls03-old-cursor-rejected",
            expected=(4,),
        )
        replacement = create_stream("orders", 4, deterministic_uuid(rng), nodes[1]["endpoint"])
        if replacement["stream"] == stream_a["stream"]:
            raise VerificationError("name reuse retained the deleted stream identity")
        stale_receipt = publish(
            replacement["stream"], 0, 200, os.urandom(199), nodes[1]["endpoint"],
            "ls03-stale-route-refresh", stale=old_route,
        )
        write_json(artifacts / "l05.json", {"verdict": "PASS", "deleted": deleted, "replacement": replacement})
        write_json(artifacts / "l06.json", {"verdict": "PASS", "old_cursor_rejection": old_cursor, "old_stream_id": stream_a["stream"], "new_stream_id": replacement["stream"]})
        write_json(artifacts / "l07.json", {"verdict": "PASS", "stale_route": old_route, "refreshed_receipt": stale_receipt})

        for node_id in (1, 2, 3):
            nodes[node_id]["server"].stop()
        for node_id in (1, 2, 3):
            start_node(node_id, "full-restart")
        restarted = wait_for_uniform_active(
            artifacts, runner, binaries["light-streamctl"], nodes,
            profile["write_readiness_seconds"] + 20, "ls03-full-restart",
        )
        restart_diagnostics = read_node_diagnostics(
            artifacts, runner, binaries["light-streamctl"], nodes[1], "ls03-restart-diagnostics"
        )
        metadata_leader = group_by_name(restart_diagnostics, "control", 1)["current_leader"]
        restarted_list = cli(nodes[metadata_leader]["endpoint"], ["stream", "list", "--cluster-id", cluster], "ls03-list-after-restart")["streams"]
        if not any(value["stream"] == replacement["stream"] for value in restarted_list):
            raise VerificationError("replacement stream disappeared after restart")
        write_json(artifacts / "l10.json", {"verdict": "PASS", "memberships": restarted, "streams": restarted_list})
        write_json(artifacts / "l01.json", {"verdict": "PASS", "bootstrap": bootstrap, "memberships": memberships})
        write_json(artifacts / "ls03-summary.json", {"verdict": "PASS", "streams": [stream_a, stream_b, stream_c, replacement], "groups": group_ids, "leaders": leaders})
        write_json(artifacts / "unsupported.json", {"bookmarks": "UNSUPPORTED_LS04", "retention": "UNSUPPORTED_LS05", "snapshots_after_purge": "UNSUPPORTED_LS06", "secured_mode": "UNSUPPORTED_LS08", "independent_hosts": "BLOCKED"})
    finally:
        for node in nodes.values():
            if node["server"].is_running():
                node["server"].stop()


def run_ls04_scenario(artifacts, runner, binaries, revision, profile, seed):
    rng = random.Random(seed ^ 0x4C533034)
    cluster = deterministic_uuid(rng)
    bootstrap_stream = deterministic_uuid(rng)
    stream_request = deterministic_uuid(rng)
    session = deterministic_uuid(rng)
    nodes = {}
    configs = {}
    used_ports = set()

    for node_id in (1, 2, 3):
        public_port = free_port()
        while public_port in used_ports:
            public_port = free_port()
        used_ports.add(public_port)
        peer_port = free_port()
        while peer_port in used_ports:
            peer_port = free_port()
        used_ports.add(peer_port)
        configs[node_id] = {
            "node_id": node_id,
            "public_address": f"127.0.0.1:{public_port}",
            "peer_address": f"127.0.0.1:{peer_port}",
            "endpoint": f"http://127.0.0.1:{public_port}",
            "peer_uri": f"http://127.0.0.1:{peer_port}",
            "data_dir": artifacts / "scratch" / "success" / f"ls04-node-{node_id}",
        }

    def start_node(node_id, suffix):
        config = configs[node_id]
        server = OwnedServer(
            binaries["light-streamd"],
            config["data_dir"],
            artifacts / "node-logs",
            f"ls04-node-{node_id}-{suffix}",
            node_id=node_id,
            public_address=config["public_address"],
            peer_address=config["peer_address"],
            advertise_public_uri=config["endpoint"],
            advertise_peer_uri=config["peer_uri"],
            max_data_groups=1,
            max_streams=8,
            max_partitions_per_stream=4,
        )
        nodes[node_id] = {**config, "server": server}

    def endpoints():
        return [
            nodes[node_id]["endpoint"]
            for node_id in sorted(nodes)
            if nodes[node_id]["server"].is_running()
        ]

    def cli(endpoint, arguments, label, expected=(0,), timeout=30, no_retry=False):
        command = cli_endpoint_command(
            binaries["light-streamctl"],
            endpoint,
            seeds=endpoints(),
            no_retry=no_retry,
            deadline_ms=20000,
        ) + arguments
        return parse_json_output(
            runner.run(command, label, expected_codes=expected, timeout=timeout),
            label,
        )

    def bookmark_arguments(stream_id, partition=0):
        return [
            "--cluster-id",
            cluster,
            "--stream-id",
            stream_id,
            "--partition",
            str(partition),
        ]

    def publish(stream_id, sequence, payload, label, bookmark=None, endpoint=None):
        sample = artifacts / "samples" / f"{label}.bin"
        sample.parent.mkdir(parents=True, exist_ok=True)
        sample.write_bytes(payload)
        arguments = [
            "publish",
            *bookmark_arguments(stream_id),
            "--principal",
            f"verify-ls04-{seed}",
            "--session",
            session,
            "--sequence",
            str(sequence),
            "--file",
            str(sample),
        ]
        if bookmark is not None:
            arguments.extend(["--bookmark", bookmark])
        return cli(
            endpoint or nodes[1]["endpoint"],
            arguments,
            label,
            timeout=45,
        )["receipt"]

    def create_bookmark(stream_id, bookmark_id, name, offset, label):
        return cli(
            nodes[1]["endpoint"],
            [
                "bookmark",
                "create",
                *bookmark_arguments(stream_id),
                "--bookmark-id",
                bookmark_id,
                "--name",
                name,
                "--offset",
                str(offset),
            ],
            label,
        )["bookmark"]

    def resolve_bookmark(stream_id, name, label):
        return cli(
            nodes[2]["endpoint"],
            [
                "bookmark",
                "resolve",
                *bookmark_arguments(stream_id),
                "--name",
                name,
            ],
            label,
        )["bookmark"]

    def list_bookmarks(
        stream_id,
        limit,
        label,
        publication_ceiling=None,
        before=None,
    ):
        arguments = [
            "bookmark",
            "list",
            *bookmark_arguments(stream_id),
            "--limit",
            str(limit),
        ]
        if publication_ceiling is not None:
            arguments.extend(["--publication-ceiling", str(publication_ceiling)])
        if before is not None:
            arguments.extend(["--before", str(before)])
        started = time.monotonic()
        page = cli(nodes[3]["endpoint"], arguments, label)["page"]
        return page, time.monotonic() - started

    try:
        for node_id in (1, 2, 3):
            start_node(node_id, "initial")
        bootstrap_command = cli_endpoint_command(
            binaries["light-streamctl"],
            nodes[1]["endpoint"],
            deadline_ms=45000,
        ) + [
            "cluster",
            "bootstrap",
            "--cluster-id",
            cluster,
            "--stream-id",
            bootstrap_stream,
            "--stream-name",
            "bootstrap",
            "--seed-node-id",
            "1",
        ]
        for node_id in (1, 2, 3):
            bootstrap_command.extend(
                [
                    "--member",
                    f"{node_id},{nodes[node_id]['endpoint']},{nodes[node_id]['peer_uri']}",
                ]
            )
        bootstrap = parse_json_output(
            runner.run(bootstrap_command, "ls04-bootstrap", timeout=60),
            "LS04 bootstrap",
        )
        memberships = wait_for_uniform_active(
            artifacts,
            runner,
            binaries["light-streamctl"],
            nodes,
            profile["write_readiness_seconds"] + 20,
            "ls04-active",
        )
        stream = cli(
            nodes[1]["endpoint"],
            [
                "stream",
                "create",
                "--cluster-id",
                cluster,
                "--request-id",
                stream_request,
                "--name",
                "imports",
                "--partitions",
                "2",
            ],
            "ls04-create-stream",
        )["stream"]
        stream_id = stream["stream"]
        readiness_deadline = time.monotonic() + profile["write_readiness_seconds"] + 20
        while True:
            ready = cli(
                nodes[1]["endpoint"],
                [
                    "fetch",
                    *bookmark_arguments(stream_id),
                    "--offset",
                    "0",
                    "--limit",
                    "1",
                ],
                "ls04-readiness",
                expected=(0, 5),
                timeout=10,
            )
            if ready.get("ok"):
                break
            if time.monotonic() >= readiness_deadline:
                raise VerificationError(f"LS04 partition did not become readable: {ready}")
            time.sleep(0.05)

        first_payload = b"before-boundary"
        first = publish(
            stream_id,
            1,
            first_payload,
            "ls04-publish-boundary",
            bookmark="import-boundary",
        )
        boundary = first["bookmark"]
        if boundary is None or boundary["cursor"]["next_offset"] != 1:
            raise VerificationError(f"atomic bookmark cursor is wrong: {first}")
        second_payload = b"after-boundary"
        publish(stream_id, 2, second_payload, "ls04-publish-after-boundary")
        resolved = resolve_bookmark(stream_id, "import-boundary", "ls04-resolve-boundary")
        resumed = cli(
            nodes[3]["endpoint"],
            [
                "fetch",
                *bookmark_arguments(stream_id),
                "--offset",
                str(resolved["cursor"]["next_offset"]),
                "--limit",
                "1",
            ],
            "ls04-resume",
        )["page"]
        observed = bytes(resumed["records"][0]["payload"])
        if observed != second_payload or resumed["records"][0]["offset"] != 1:
            raise VerificationError("bookmark resume duplicated or skipped the boundary record")
        write_json(
            artifacts / "l02.json",
            {
                "verdict": "PASS",
                "receipt": first,
                "resolved": resolved,
                "resumed_sha256": hashlib.sha256(observed).hexdigest(),
            },
        )

        route = cli(
            nodes[1]["endpoint"],
            [
                "stream",
                "route",
                "--cluster-id",
                cluster,
                "--stream-id",
                stream_id,
                "--partition",
                "0",
            ],
            "ls04-route",
        )["route"]
        data_leader_endpoint = route["leader"]["public_uri"]
        upstream_port = int(data_leader_endpoint.rsplit(":", 1)[1])
        proxy = OneShotResponseDropProxy(free_port(), upstream_port)
        lost_sample = artifacts / "samples" / "ls04-lost-response.bin"
        lost_sample.write_bytes(b"lost-response")
        lost_arguments = [
            "publish",
            *bookmark_arguments(stream_id),
            "--principal",
            f"verify-ls04-{seed}",
            "--session",
            session,
            "--sequence",
            "3",
            "--file",
            str(lost_sample),
            "--bookmark",
            "response-lost",
        ]
        dropped_command = cli_endpoint_command(
            binaries["light-streamctl"],
            proxy.endpoint,
            no_retry=True,
            deadline_ms=3000,
        ) + lost_arguments
        runner.run_dropped_response(
            dropped_command,
            "ls04-publish-response-dropped",
            expected_codes=(1, 5),
            timeout=10,
        )
        proxy.wait()
        retry = cli(
            nodes[2]["endpoint"],
            lost_arguments,
            "ls04-publish-response-retry",
        )["receipt"]
        receipt = cli(
            nodes[3]["endpoint"],
            [
                "receipt",
                *bookmark_arguments(stream_id),
                "--principal",
                f"verify-ls04-{seed}",
                "--session",
                session,
                "--sequence",
                "3",
            ],
            "ls04-receipt-after-lost-response",
        )["receipt"]
        if retry != receipt or retry["bookmark"] is None:
            raise VerificationError("lost-response retry changed its offsets or bookmark")
        write_json(
            artifacts / "l04.json",
            {
                "verdict": "PASS",
                "response_dropped": True,
                "retry": retry,
                "receipt": receipt,
            },
        )

        conflict_sample = artifacts / "samples" / "ls04-bookmark-conflict.bin"
        conflict_sample.write_bytes(b"must-not-commit")
        conflict = cli(
            nodes[1]["endpoint"],
            [
                "publish",
                *bookmark_arguments(stream_id),
                "--principal",
                f"verify-ls04-{seed}",
                "--session",
                session,
                "--sequence",
                "4",
                "--file",
                str(conflict_sample),
                "--bookmark",
                "import-boundary",
            ],
            "ls04-reject-atomic-name-conflict",
            expected=(4,),
        )
        unchanged_tail = cli(
            nodes[2]["endpoint"],
            [
                "fetch",
                *bookmark_arguments(stream_id),
                "--offset",
                "3",
                "--limit",
                "1",
            ],
            "ls04-tail-after-name-conflict",
        )["page"]
        if unchanged_tail["records"]:
            raise VerificationError("bookmark name conflict partially appended its record")
        for node_id in (1, 2, 3):
            nodes[node_id]["server"].kill()
        for node_id in (1, 2, 3):
            start_node(node_id, "atomic-crash-restart")
        crash_memberships = wait_for_uniform_active(
            artifacts,
            runner,
            binaries["light-streamctl"],
            nodes,
            profile["write_readiness_seconds"] + 20,
            "ls04-atomic-crash-restart",
        )
        after_crash = resolve_bookmark(
            stream_id,
            "response-lost",
            "ls04-resolve-after-atomic-crash",
        )
        after_crash_tail = cli(
            nodes[2]["endpoint"],
            [
                "fetch",
                *bookmark_arguments(stream_id),
                "--offset",
                "0",
                "--limit",
                "8",
            ],
            "ls04-fetch-after-atomic-crash",
        )["page"]
        if after_crash != retry["bookmark"] or len(after_crash_tail["records"]) != 3:
            raise VerificationError("crash exposed a partial publish-plus-bookmark result")

        old_id = deterministic_uuid(rng)
        old = create_bookmark(
            stream_id,
            old_id,
            "backdated",
            0,
            "ls04-create-backdated",
        )
        current_id = deterministic_uuid(rng)
        current = create_bookmark(
            stream_id,
            current_id,
            "current",
            3,
            "ls04-create-current",
        )
        recent, _ = list_bookmarks(stream_id, 10, "ls04-list-publication-order")
        publications = [
            bookmark["publication"] for bookmark in recent["items"]
        ]
        if publications != sorted(publications, reverse=True):
            raise VerificationError("recent bookmarks are not in publication order")
        if recent["items"][0]["id"] != current_id or old["cursor"]["next_offset"] != 0:
            raise VerificationError("backdated bookmark changed recent-list ordering")
        write_json(
            artifacts / "l05.json",
            {"verdict": "PASS", "backdated": old, "current": current, "recent": recent},
        )

        page_one, _ = list_bookmarks(stream_id, 2, "ls04-page-one")
        concurrent = create_bookmark(
            stream_id,
            deterministic_uuid(rng),
            "created-during-pagination",
            3,
            "ls04-create-during-pagination",
        )
        page_two, _ = list_bookmarks(
            stream_id,
            2,
            "ls04-page-two",
            publication_ceiling=page_one["publication_ceiling"],
            before=page_one["next_before"],
        )
        first_ids = {bookmark["id"] for bookmark in page_one["items"]}
        second_ids = {bookmark["id"] for bookmark in page_two["items"]}
        if first_ids.intersection(second_ids) or concurrent["id"] in second_ids:
            raise VerificationError("stable pagination duplicated or admitted a newer bookmark")
        write_json(
            artifacts / "l06.json",
            {
                "verdict": "PASS",
                "page_one": page_one,
                "concurrent": concurrent,
                "page_two": page_two,
            },
        )

        deleted = cli(
            nodes[1]["endpoint"],
            [
                "bookmark",
                "delete",
                *bookmark_arguments(stream_id),
                "--bookmark-id",
                old_id,
            ],
            "ls04-delete-backdated",
        )["bookmark"]
        replacement_id = deterministic_uuid(rng)
        replacement = create_bookmark(
            stream_id,
            replacement_id,
            "backdated",
            1,
            "ls04-reuse-bookmark-name",
        )
        old_retry = create_bookmark(
            stream_id,
            old_id,
            "backdated",
            0,
            "ls04-retry-old-bookmark-request",
        )
        if (
            deleted["lifecycle"] != "deleted"
            or replacement["id"] == old_id
            or old_retry["id"] != old_id
            or old_retry["lifecycle"] != "deleted"
        ):
            raise VerificationError("bookmark deletion or name reuse resurrected an old identity")
        write_json(
            artifacts / "l07.json",
            {
                "verdict": "PASS",
                "deleted": deleted,
                "replacement": replacement,
                "old_retry": old_retry,
            },
        )

        vector_id = deterministic_uuid(rng)
        vector = cli(
            nodes[1]["endpoint"],
            [
                "bookmark",
                "stream-create",
                "--cluster-id",
                cluster,
                "--stream-id",
                stream_id,
                "--bookmark-id",
                vector_id,
                "--name",
                "two-partition-boundary",
                "--position",
                "0:3",
                "--position",
                "1:0",
            ],
            "ls04-create-stream-vector",
        )["bookmark"]
        resolved_vector = cli(
            nodes[2]["endpoint"],
            [
                "bookmark",
                "stream-resolve",
                "--cluster-id",
                cluster,
                "--stream-id",
                stream_id,
                "--name",
                "two-partition-boundary",
            ],
            "ls04-resolve-stream-vector",
        )["bookmark"]
        incomplete_vector = cli(
            nodes[3]["endpoint"],
            [
                "bookmark",
                "stream-create",
                "--cluster-id",
                cluster,
                "--stream-id",
                stream_id,
                "--bookmark-id",
                deterministic_uuid(rng),
                "--name",
                "incomplete-vector",
                "--position",
                "0:3",
            ],
            "ls04-reject-incomplete-vector",
            expected=(2,),
        )
        future_vector = cli(
            nodes[3]["endpoint"],
            [
                "bookmark",
                "stream-create",
                "--cluster-id",
                cluster,
                "--stream-id",
                stream_id,
                "--bookmark-id",
                deterministic_uuid(rng),
                "--name",
                "future-vector",
                "--position",
                "0:3",
                "--position",
                "1:1",
            ],
            "ls04-reject-future-vector",
            expected=(2,),
        )
        if vector != resolved_vector:
            raise VerificationError("stream bookmark vector changed during resolution")
        write_json(
            artifacts / "l08.json",
            {
                "verdict": "PASS",
                "bookmark": vector,
                "positions_are_independent": True,
                "consistent_cut_claim": False,
                "incomplete_vector_rejection": incomplete_vector,
                "future_offset_rejection": future_vector,
            },
        )

        tiny = artifacts / "samples" / "ls04-tiny.bin"
        tiny.write_bytes(b"x")
        sequence = 10
        remaining = 10000 - 3
        while remaining:
            count = min(128, remaining)
            arguments = [
                "publish",
                *bookmark_arguments(stream_id),
                "--principal",
                f"verify-ls04-{seed}",
                "--session",
                session,
                "--sequence",
                str(sequence),
            ]
            for _ in range(count):
                arguments.extend(["--file", str(tiny)])
            cli(
                nodes[1]["endpoint"],
                arguments,
                f"ls04-fill-{sequence}",
                timeout=60,
            )
            remaining -= count
            sequence += 1
        for index in range(96):
            create_bookmark(
                stream_id,
                deterministic_uuid(rng),
                f"perf-{index:03d}",
                10000 - index,
                f"ls04-perf-bookmark-{index:03d}",
            )
        list_durations = []
        perf_page = None
        for attempt in range(100):
            perf_page, duration = list_bookmarks(
                stream_id,
                100,
                f"ls04-list-100-{attempt:02d}",
            )
            list_durations.append(duration)
        list_durations.sort()
        p99_index = ((99 * len(list_durations) + 99) // 100) - 1
        p99 = list_durations[p99_index]
        if len(perf_page["items"]) != 100 or p99 > 0.100:
            raise VerificationError(
                f"last-100 bookmark lookup failed its bound: count={len(perf_page['items'])}, p99={p99}"
            )
        write_json(
            artifacts / "l09.json",
            {
                "verdict": "PASS",
                "record_count": 10000,
                "bookmark_count": 102,
                "lookup_seconds": list_durations,
                "p99_seconds": p99,
                "payload_scan": False,
            },
        )

        before_restart = resolve_bookmark(
            stream_id,
            "import-boundary",
            "ls04-resolve-before-restart",
        )
        for node_id in (1, 2, 3):
            nodes[node_id]["server"].stop()
        for node_id in (1, 2, 3):
            start_node(node_id, "full-restart")
        restarted = wait_for_uniform_active(
            artifacts,
            runner,
            binaries["light-streamctl"],
            nodes,
            profile["write_readiness_seconds"] + 20,
            "ls04-full-restart",
        )
        after_restart = resolve_bookmark(
            stream_id,
            "import-boundary",
            "ls04-resolve-after-restart",
        )
        vector_after_restart = cli(
            nodes[2]["endpoint"],
            [
                "bookmark",
                "stream-resolve",
                "--cluster-id",
                cluster,
                "--stream-id",
                stream_id,
                "--name",
                "two-partition-boundary",
            ],
            "ls04-resolve-vector-after-restart",
        )["bookmark"]
        if before_restart != after_restart or vector_after_restart != vector:
            raise VerificationError("bookmark identity or cursor changed after full restart")
        write_json(
            artifacts / "l10.json",
            {
                "verdict": "PASS",
                "before_restart": before_restart,
                "after_restart": after_restart,
                "vector_after_restart": vector_after_restart,
                "memberships": restarted,
                "single_process_failover": "DEFERRED_LS06",
            },
        )
        write_json(
            artifacts / "l01.json",
            {
                "verdict": "PASS",
                "bootstrap": bootstrap,
                "memberships": memberships,
                "ordinary_publish_fetch_preserved": True,
            },
        )
        write_json(
            artifacts / "l03.json",
            {
                "verdict": "PASS",
                "combined_publish_and_bookmark": "ATOMIC_STATE_MACHINE_WRITE",
                "name_conflict_rejection": conflict,
                "tail_after_conflict": unchanged_tail,
                "crash_memberships": crash_memberships,
                "bookmark_after_crash": after_crash,
                "records_after_crash": after_crash_tail["records"],
            },
        )
        write_json(
            artifacts / "bookmark-journey.json",
            {
                "verdict": "PASS",
                "revision": revision,
                "stream": stream,
                "boundary": boundary,
                "resume_offset": resolved["cursor"]["next_offset"],
                "lost_response_bookmark": retry["bookmark"],
                "full_restart_preserved": True,
                "stream_cursor_vector": vector,
                "single_process_failover": "DEFERRED_LS06",
            },
        )
        write_json(
            artifacts / "unsupported.json",
            {
                "retention_expiration": "UNSUPPORTED_LS05",
                "single_process_failover": "DEFERRED_LS06",
                "secured_mode": "UNSUPPORTED_LS08",
                "independent_hosts": "BLOCKED",
            },
        )
    finally:
        for node in nodes.values():
            if node["server"].is_running():
                node["server"].stop()


def run_ls05_scenario(artifacts, runner, binaries, revision, profile, seed):
    rng = random.Random(seed ^ 0x4C533035)
    cluster = deterministic_uuid(rng)
    bootstrap_stream = deterministic_uuid(rng)
    stream_request = deterministic_uuid(rng)
    producer_session = deterministic_uuid(rng)
    replay_session = deterministic_uuid(rng)
    retention_session = deterministic_uuid(rng)
    floor_first_session = deterministic_uuid(rng)
    limits = profile["ls05"]
    nodes = {}
    configs = {}
    used_ports = set()

    for node_id in (1, 2, 3):
        public_port = free_port()
        while public_port in used_ports:
            public_port = free_port()
        used_ports.add(public_port)
        peer_port = free_port()
        while peer_port in used_ports:
            peer_port = free_port()
        used_ports.add(peer_port)
        configs[node_id] = {
            "node_id": node_id,
            "public_address": f"127.0.0.1:{public_port}",
            "peer_address": f"127.0.0.1:{peer_port}",
            "endpoint": f"http://127.0.0.1:{public_port}",
            "peer_uri": f"http://127.0.0.1:{peer_port}",
            "data_dir": artifacts / "scratch" / "success" / f"ls05-node-{node_id}",
        }

    def start_node(node_id, suffix):
        config = configs[node_id]
        server = OwnedServer(
            binaries["light-streamd"],
            config["data_dir"],
            artifacts / "node-logs",
            f"ls05-node-{node_id}-{suffix}",
            node_id=node_id,
            public_address=config["public_address"],
            peer_address=config["peer_address"],
            advertise_public_uri=config["endpoint"],
            advertise_peer_uri=config["peer_uri"],
            max_data_groups=1,
            max_streams=8,
            max_partitions_per_stream=4,
        )
        nodes[node_id] = {**config, "server": server}

    def endpoints():
        return [
            nodes[node_id]["endpoint"]
            for node_id in sorted(nodes)
            if nodes[node_id]["server"].is_running()
        ]

    def cli(endpoint, arguments, label, expected=(0,), timeout=30):
        command = cli_endpoint_command(
            binaries["light-streamctl"],
            endpoint,
            seeds=endpoints(),
            deadline_ms=20000,
        ) + arguments
        return parse_json_output(
            runner.run(command, label, expected_codes=expected, timeout=timeout),
            label,
        )

    def target(stream_id, partition):
        return [
            "--cluster-id",
            cluster,
            "--stream-id",
            stream_id,
            "--partition",
            str(partition),
        ]

    def mutation(principal, session, sequence):
        return [
            "--principal",
            principal,
            "--mutation-session",
            session,
            "--sequence",
            str(sequence),
        ]

    def publish(stream_id, partition, sequence, payload, label):
        return cli(
            nodes[1]["endpoint"],
            [
                "publish",
                *target(stream_id, partition),
                "--principal",
                f"verify-ls05-{seed}",
                "--session",
                producer_session,
                "--sequence",
                str(sequence),
                "--payload",
                payload,
            ],
            label,
        )["receipt"]

    def fetch(stream_id, partition, offset, limit, label, expected=(0,)):
        return cli(
            nodes[2]["endpoint"],
            [
                "fetch",
                *target(stream_id, partition),
                "--offset",
                str(offset),
                "--limit",
                str(limit),
            ],
            label,
            expected=expected,
        )

    def advance(stream_id, partition, session, sequence, floor, label):
        started = time.monotonic()
        value = cli(
            nodes[1]["endpoint"],
            [
                "retention",
                "advance",
                *target(stream_id, partition),
                *mutation("retention-operator", session, sequence),
                "--floor",
                str(floor),
            ],
            label,
        )
        return value["retention"], time.monotonic() - started

    def admit(
        stream_id,
        partition,
        session,
        sequence,
        start,
        end,
        duration_ms,
        max_bytes,
        label,
        expected=(0,),
    ):
        started = time.monotonic()
        value = cli(
            nodes[1]["endpoint"],
            [
                "replay",
                "admit",
                *target(stream_id, partition),
                *mutation("replay-job", session, sequence),
                "--start",
                str(start),
                "--end",
                str(end),
                "--duration-ms",
                str(duration_ms),
                "--max-bytes",
                str(max_bytes),
            ],
            label,
            expected=expected,
        )
        return value, time.monotonic() - started

    def renew(stream_id, partition, session, sequence, lease_id, duration_ms, label):
        started = time.monotonic()
        value = cli(
            nodes[2]["endpoint"],
            [
                "replay",
                "renew",
                *target(stream_id, partition),
                *mutation("replay-job", session, sequence),
                "--lease-id",
                lease_id,
                "--duration-ms",
                str(duration_ms),
            ],
            label,
        )
        return value["lease"], time.monotonic() - started

    def release(stream_id, partition, session, sequence, lease_id, label):
        started = time.monotonic()
        value = cli(
            nodes[3]["endpoint"],
            [
                "replay",
                "release",
                *target(stream_id, partition),
                *mutation("replay-job", session, sequence),
                "--lease-id",
                lease_id,
            ],
            label,
        )
        return value["lease"], time.monotonic() - started

    def protected_fetch(stream_id, partition, lease_id, offset, limit, label, expected=(0,)):
        return cli(
            nodes[3]["endpoint"],
            [
                "replay",
                "fetch",
                *target(stream_id, partition),
                "--lease-id",
                lease_id,
                "--offset",
                str(offset),
                "--limit",
                str(limit),
            ],
            label,
            expected=expected,
        )

    def retention_status(stream_id, partition, label):
        return cli(
            nodes[2]["endpoint"],
            ["retention", "status", *target(stream_id, partition)],
            label,
        )["retention"]

    try:
        for node_id in (1, 2, 3):
            start_node(node_id, "initial")
        bootstrap_command = cli_endpoint_command(
            binaries["light-streamctl"],
            nodes[1]["endpoint"],
            deadline_ms=45000,
        ) + [
            "cluster",
            "bootstrap",
            "--cluster-id",
            cluster,
            "--stream-id",
            bootstrap_stream,
            "--stream-name",
            "bootstrap",
            "--seed-node-id",
            "1",
        ]
        for node_id in (1, 2, 3):
            bootstrap_command.extend(
                [
                    "--member",
                    f"{node_id},{nodes[node_id]['endpoint']},{nodes[node_id]['peer_uri']}",
                ]
            )
        bootstrap = parse_json_output(
            runner.run(bootstrap_command, "ls05-bootstrap", timeout=60),
            "LS05 bootstrap",
        )
        memberships = wait_for_uniform_active(
            artifacts,
            runner,
            binaries["light-streamctl"],
            nodes,
            profile["write_readiness_seconds"] + 20,
            "ls05-active",
        )
        stream = cli(
            nodes[1]["endpoint"],
            [
                "stream",
                "create",
                "--cluster-id",
                cluster,
                "--request-id",
                stream_request,
                "--name",
                "retained-orders",
                "--partitions",
                "2",
            ],
            "ls05-create-stream",
        )["stream"]
        stream_id = stream["stream"]
        for partition in (0, 1):
            deadline = time.monotonic() + profile["write_readiness_seconds"] + 20
            while True:
                ready = fetch(
                    stream_id,
                    partition,
                    0,
                    1,
                    f"ls05-ready-{partition}",
                    expected=(0, 5),
                )
                if ready.get("ok"):
                    break
                if time.monotonic() >= deadline:
                    raise VerificationError(f"LS05 partition {partition} did not become ready")
                time.sleep(0.05)

        p0_payloads = [f"p0-{index}" for index in range(6)]
        p1_payloads = [f"p1-{index}" for index in range(3)]
        sequence = 1
        for partition, payloads in ((0, p0_payloads), (1, p1_payloads)):
            for payload in payloads:
                publish(
                    stream_id,
                    partition,
                    sequence,
                    payload,
                    f"ls05-publish-{partition}-{sequence}",
                )
                sequence += 1
        initial = fetch(stream_id, 0, 0, 16, "ls05-initial-fetch")["page"]
        if [bytes(record["payload"]).decode() for record in initial["records"]] != p0_payloads:
            raise VerificationError("retention-disabled record ledger changed")
        write_json(
            artifacts / "l01.json",
            {
                "verdict": "PASS",
                "bootstrap": bootstrap,
                "memberships": memberships,
                "records": p0_payloads,
                "retention_disabled_compatible": True,
            },
        )

        bookmark_id = deterministic_uuid(rng)
        bookmark = cli(
            nodes[1]["endpoint"],
            [
                "bookmark",
                "create",
                *target(stream_id, 0),
                "--bookmark-id",
                bookmark_id,
                "--name",
                "old-replay",
                "--offset",
                "1",
            ],
            "ls05-create-old-bookmark",
        )["bookmark"]
        admission, admit_seconds = admit(
            stream_id,
            0,
            replay_session,
            1,
            1,
            4,
            30000,
            1024,
            "ls05-admit-protected-range",
        )
        lease = admission["lease"]
        floor, floor_seconds = advance(
            stream_id,
            0,
            retention_session,
            1,
            6,
            "ls05-advance-floor",
        )
        expired_fetch = fetch(
            stream_id,
            0,
            1,
            16,
            "ls05-expired-bookmark-fetch",
            expected=(4,),
        )
        resolved = cli(
            nodes[2]["endpoint"],
            [
                "bookmark",
                "resolve",
                *target(stream_id, 0),
                "--name",
                "old-replay",
            ],
            "ls05-resolve-expired-bookmark",
        )["bookmark"]
        if resolved["id"] != bookmark_id or expired_fetch["error"]["code"] != "cursor_expired":
            raise VerificationError("expired bookmark metadata or error contract is wrong")
        write_json(
            artifacts / "l02.json",
            {
                "verdict": "PASS",
                "bookmark": bookmark,
                "resolved_after_expiry": resolved,
                "fetch_error": expired_fetch,
                "floor": floor,
            },
        )

        protected = protected_fetch(
            stream_id,
            0,
            lease["id"],
            1,
            16,
            "ls05-protected-fetch",
        )["page"]
        protected_payloads = [
            bytes(record["payload"]).decode() for record in protected["records"]
        ]
        status_after_floor = retention_status(stream_id, 0, "ls05-status-after-floor")
        if protected_payloads != p0_payloads[1:4] or status_after_floor["logical_floor"] != 6:
            raise VerificationError("protected replay did not preserve its exact island")
        write_json(
            artifacts / "l03.json",
            {
                "verdict": "PASS",
                "lease": lease,
                "protected_payloads": protected_payloads,
                "retention": status_after_floor,
            },
        )

        first_page = fetch(stream_id, 1, 0, 1, "ls05-unleased-first-page")["page"]
        advance(
            stream_id,
            1,
            floor_first_session,
            1,
            3,
            "ls05-advance-unleased-floor",
        )
        next_page = fetch(
            stream_id,
            1,
            1,
            8,
            "ls05-unleased-next-page",
            expected=(4,),
        )
        if len(first_page["records"]) != 1 or next_page["error"]["code"] != "cursor_expired":
            raise VerificationError("unleased replay returned a shortened success")
        write_json(
            artifacts / "l04.json",
            {
                "verdict": "PASS",
                "first_page": first_page,
                "terminal_next_page": next_page,
            },
        )

        floor_first, _ = admit(
            stream_id,
            1,
            deterministic_uuid(rng),
            1,
            0,
            2,
            30000,
            1024,
            "ls05-floor-first-admission",
            expected=(4,),
        )
        if floor_first["error"]["code"] != "cursor_expired":
            raise VerificationError("floor-first ordering did not reject admission")
        write_json(
            artifacts / "l05.json",
            {
                "verdict": "PASS",
                "admission_first": {
                    "lease": lease,
                    "floor": floor,
                    "protected_payloads": protected_payloads,
                },
                "floor_first": floor_first,
            },
        )

        publish(stream_id, 0, sequence, "quota-record", "ls05-publish-quota-record")
        sequence += 1
        quota, _ = admit(
            stream_id,
            0,
            deterministic_uuid(rng),
            1,
            6,
            7,
            limits["max_lease_duration_ms"] + 1,
            1024,
            "ls05-duration-quota",
            expected=(4,),
        )
        if quota["error"]["code"] != "resource_limit":
            raise VerificationError("lease duration quota failed for the wrong reason")
        write_json(
            artifacts / "l06.json",
            {
                "verdict": "PASS",
                "duration_rejection": quota,
                "configured_limits": limits,
                "aggregate_and_active_quota_unit_coverage": True,
                "recovery_headroom": "LOGICAL_GUARD_ONLY_LS05",
            },
        )

        admission_retry, _ = admit(
            stream_id,
            0,
            replay_session,
            1,
            1,
            4,
            30000,
            1024,
            "ls05-admission-retry",
        )
        if admission_retry["lease"]["id"] != lease["id"]:
            raise VerificationError("admission retry returned a new lease identity")
        mutation_route = cli(
            nodes[1]["endpoint"],
            [
                "stream",
                "route",
                "--cluster-id",
                cluster,
                "--stream-id",
                stream_id,
                "--partition",
                "0",
            ],
            "ls05-mutation-route",
        )["route"]
        renew_proxy = OneShotResponseDropProxy(
            free_port(),
            int(mutation_route["leader"]["public_uri"].rsplit(":", 1)[1]),
        )
        renew_command = cli_endpoint_command(
            binaries["light-streamctl"],
            renew_proxy.endpoint,
            no_retry=True,
            deadline_ms=3000,
        ) + [
            "replay",
            "renew",
            *target(stream_id, 0),
            *mutation("replay-job", replay_session, 2),
            "--lease-id",
            lease["id"],
            "--duration-ms",
            "30000",
            "--route-group-id",
            str(mutation_route["route"]["group"]),
            "--route-revision",
            str(mutation_route["route"]["route_revision"]),
        ]
        runner.run_dropped_response(
            renew_command,
            "ls05-renew-response-dropped",
            expected_codes=(1, 5),
            timeout=10,
        )
        renew_proxy.wait()
        renewed, renew_seconds = renew(
            stream_id,
            0,
            replay_session,
            2,
            lease["id"],
            30000,
            "ls05-renew",
        )
        renewed_retry, _ = renew(
            stream_id,
            0,
            replay_session,
            2,
            lease["id"],
            30000,
            "ls05-renew-retry",
        )
        if renewed_retry != renewed:
            raise VerificationError("renewal retry did not resolve the dropped response")
        release_route = cli(
            nodes[1]["endpoint"],
            [
                "stream",
                "route",
                "--cluster-id",
                cluster,
                "--stream-id",
                stream_id,
                "--partition",
                "0",
            ],
            "ls05-release-route",
        )["route"]
        release_proxy = OneShotResponseDropProxy(
            free_port(),
            int(release_route["leader"]["public_uri"].rsplit(":", 1)[1]),
        )
        release_command = cli_endpoint_command(
            binaries["light-streamctl"],
            release_proxy.endpoint,
            no_retry=True,
            deadline_ms=3000,
        ) + [
            "replay",
            "release",
            *target(stream_id, 0),
            *mutation("replay-job", replay_session, 3),
            "--lease-id",
            lease["id"],
            "--route-group-id",
            str(release_route["route"]["group"]),
            "--route-revision",
            str(release_route["route"]["route_revision"]),
        ]
        runner.run_dropped_response(
            release_command,
            "ls05-release-response-dropped",
            expected_codes=(1, 5),
            timeout=10,
        )
        release_proxy.wait()
        released, release_seconds = release(
            stream_id,
            0,
            replay_session,
            3,
            lease["id"],
            "ls05-release",
        )
        released_retry, _ = release(
            stream_id,
            0,
            replay_session,
            3,
            lease["id"],
            "ls05-release-retry",
        )
        released_fetch = protected_fetch(
            stream_id,
            0,
            lease["id"],
            1,
            16,
            "ls05-released-fetch",
            expected=(4,),
        )
        if (
            released_retry != released
            or released_fetch["error"]["code"] != "replay_lease_inactive"
        ):
            raise VerificationError("release retry or terminal state is wrong")
        write_json(
            artifacts / "l07.json",
            {
                "verdict": "PASS",
                "admission_retry": admission_retry["lease"],
                "renewed": renewed,
                "renewed_retry": renewed_retry,
                "released": released,
                "released_retry": released_retry,
                "released_fetch": released_fetch,
                "renew_response_dropped": True,
                "release_response_dropped": True,
            },
        )

        publish(stream_id, 0, sequence, "short", "ls05-publish-short")
        sequence += 1
        short_admission, _ = admit(
            stream_id,
            0,
            deterministic_uuid(rng),
            1,
            7,
            8,
            1,
            1024,
            "ls05-admit-short",
        )
        short_lease = short_admission["lease"]
        immediate = protected_fetch(
            stream_id,
            0,
            short_lease["id"],
            7,
            1,
            "ls05-short-immediate",
        )
        before_idle_expiry = cli(
            nodes[2]["endpoint"],
            ["diagnostics"],
            "ls05-before-idle-expiry",
        )["diagnostics"]
        wait_seconds = limits["max_clock_skew_ms"] * 2 / 1000 + 0.5
        started = time.monotonic()
        time.sleep(wait_seconds)
        after_idle_expiry = cli(
            nodes[2]["endpoint"],
            ["diagnostics"],
            "ls05-after-idle-expiry",
        )["diagnostics"]
        expired = cli(
            nodes[2]["endpoint"],
            [
                "replay",
                "status",
                *target(stream_id, 0),
                "--lease-id",
                short_lease["id"],
            ],
            "ls05-short-expired",
        )["lease"]
        elapsed = time.monotonic() - started
        before_data = next(
            group
            for group in before_idle_expiry["groups"]
            if group["group"] == "data" and group["group_id"] == 2
        )
        after_data = next(
            group
            for group in after_idle_expiry["groups"]
            if group["group"] == "data" and group["group_id"] == 2
        )
        if (
            not immediate["ok"]
            or expired["lifecycle"] != "expired"
            or after_data["last_applied_index"] <= before_data["last_applied_index"]
        ):
            raise VerificationError("safe-time expiry contract failed")
        write_json(
            artifacts / "l08.json",
            {
                "verdict": "PASS",
                "declared_max_clock_skew_ms": limits["max_clock_skew_ms"],
                "lease": short_lease,
                "immediate_read": immediate,
                "expired": expired,
                "wait_seconds": elapsed,
                "clock_source": "system_time_with_declared_bound",
                "background_maintenance_progress": {
                    "before_applied": before_data["last_applied_index"],
                    "after_applied": after_data["last_applied_index"],
                },
            },
        )

        publish(stream_id, 0, sequence, "stall", "ls05-publish-stall")
        stall_session = deterministic_uuid(rng)
        stall_admission, _ = admit(
            stream_id,
            0,
            stall_session,
            1,
            8,
            9,
            limits["max_lease_duration_ms"],
            1024,
            "ls05-admit-stalled-reader",
        )
        stall_lease = stall_admission["lease"]
        advance(
            stream_id,
            0,
            retention_session,
            2,
            9,
            "ls05-advance-around-stalled-reader",
        )
        stalled_first = protected_fetch(
            stream_id,
            0,
            stall_lease["id"],
            8,
            1,
            "ls05-stalled-first-page",
        )
        time.sleep(0.25)
        stalled_second = protected_fetch(
            stream_id,
            0,
            stall_lease["id"],
            9,
            1,
            "ls05-stalled-second-page",
        )
        status_stalled = retention_status(stream_id, 0, "ls05-status-stalled-reader")
        if (
            len(stalled_first["page"]["records"]) != 1
            or stalled_second["page"]["records"]
            or status_stalled["reclaim_cursor"] != 9
        ):
            raise VerificationError("stalled reader violated bounded page or reclaim behavior")
        write_json(
            artifacts / "l09.json",
            {
                "verdict": "PASS",
                "first_page": stalled_first,
                "second_page": stalled_second,
                "stall_seconds": 0.25,
                "rocksdb_snapshot_across_stall": False,
                "retention": status_stalled,
            },
        )

        before_restart = {
            "retention": retention_status(stream_id, 0, "ls05-status-before-restart"),
            "bookmark": resolved,
            "active_lease": stall_lease,
            "released_lease": released,
            "expired_lease": expired,
        }
        for node_id in (1, 2, 3):
            nodes[node_id]["server"].stop()
        for node_id in (1, 2, 3):
            start_node(node_id, "full-restart")
        restarted = wait_for_uniform_active(
            artifacts,
            runner,
            binaries["light-streamctl"],
            nodes,
            profile["write_readiness_seconds"] + 20,
            "ls05-full-restart",
        )
        after_restart = {
            "retention": retention_status(stream_id, 0, "ls05-status-after-restart"),
            "bookmark": cli(
                nodes[2]["endpoint"],
                [
                    "bookmark",
                    "resolve",
                    *target(stream_id, 0),
                    "--name",
                    "old-replay",
                ],
                "ls05-bookmark-after-restart",
            )["bookmark"],
            "active_read": protected_fetch(
                stream_id,
                0,
                stall_lease["id"],
                8,
                1,
                "ls05-active-lease-after-restart",
            ),
            "released": cli(
                nodes[2]["endpoint"],
                [
                    "replay",
                    "status",
                    *target(stream_id, 0),
                    "--lease-id",
                    released["id"],
                ],
                "ls05-released-after-restart",
            )["lease"],
            "expired": cli(
                nodes[2]["endpoint"],
                [
                    "replay",
                    "status",
                    *target(stream_id, 0),
                    "--lease-id",
                    expired["id"],
                ],
                "ls05-expired-after-restart",
            )["lease"],
        }
        if (
            after_restart["retention"] != before_restart["retention"]
            or after_restart["bookmark"] != before_restart["bookmark"]
            or after_restart["released"]["lifecycle"] != "released"
            or after_restart["expired"]["lifecycle"] != "expired"
        ):
            raise VerificationError("LS05 state changed after full restart")
        write_json(
            artifacts / "l10.json",
            {
                "verdict": "PASS",
                "before_restart": before_restart,
                "after_restart": after_restart,
                "memberships": restarted,
                "partial_node_recovery": "DEFERRED_LS06",
            },
        )

        mutation_durations = sorted([admit_seconds, floor_seconds, renew_seconds, release_seconds])
        p99_index = ((99 * len(mutation_durations) + 99) // 100) - 1
        p99_seconds = mutation_durations[p99_index]
        if p99_seconds * 1000 > limits["lease_operation_p99_ms"]:
            raise VerificationError(
                f"lease mutation p99 exceeded {limits['lease_operation_p99_ms']} ms: {p99_seconds}"
            )
        write_json(
            artifacts / "retention-journey.json",
            {
                "verdict": "PASS",
                "revision": revision,
                "stream": stream,
                "logical_floor": after_restart["retention"]["logical_floor"],
                "bookmark_survived_expiry": True,
                "protected_replay_complete": True,
                "unleased_replay_explicitly_expired": True,
                "lease_mutation_seconds": mutation_durations,
                "lease_mutation_p99_seconds": p99_seconds,
                "logical_reclaimed_bytes": after_restart["retention"][
                    "logically_expired_bytes"
                ],
                "raft_only_bytes": after_restart["retention"]["raft_only_bytes"],
                "filesystem_payload_reclaim": "DEFERRED_LS06",
            },
        )
        write_json(
            artifacts / "unsupported.json",
            {
                "age_based_retention": "NOT_IMPLEMENTED",
                "response_loss_proxy_for_lease_mutations": "UNIT_COVERAGE_ONLY",
                "raft_owned_payload_reclaim": "DEFERRED_LS06",
                "partial_node_recovery": "DEFERRED_LS06",
                "secured_mode": "UNSUPPORTED_LS08",
                "independent_hosts": "BLOCKED",
            },
        )
    finally:
        for node in nodes.values():
            if node["server"].is_running():
                node["server"].stop()


def record_security(artifacts, runner, binaries, security):
    statuses = []
    if security in ("local-insecure", "all"):
        statuses.append({"mode": "local-insecure", "verdict": "PASS"})
    if security in ("secured", "all"):
        result = runner.run(
            [
                binaries["light-streamd"],
                "--data-dir",
                artifacts / "diagnostics" / "secured-not-supported",
                "--public-listen",
                "127.0.0.1:0",
                "--peer-listen",
                "127.0.0.1:0",
                "--security-mode",
                "secured",
            ],
            "secured-mode-not-supported",
            expected_codes=(1,),
            timeout=10,
        )
        if "not implemented until LS08" not in result.stderr:
            raise VerificationError("secured mode failed without the LS08 unsupported result")
        statuses.append(
            {
                "mode": "secured",
                "verdict": "UNSUPPORTED",
                "available_phase": "LS08",
            }
        )
    write_json(artifacts / "security.json", {"modes": statuses})


def cleanup_success_data(artifacts, succeeded):
    success = artifacts / "scratch" / "success"
    if succeeded and success.exists():
        shutil.rmtree(success)
    retained = sorted(
        str(path.relative_to(artifacts))
        for path in artifacts.rglob("*")
        if path.is_file()
    )
    write_json(
        artifacts / "cleanup.json",
        {
            "completed_at": utc_now(),
            "success_data_removed": succeeded and not success.exists(),
            "failed_data_retained": (not succeeded and success.exists())
            or (
                artifacts / "diagnostics" / "failed-child-data" / "retain-me.txt"
            ).is_file(),
            "retained_evidence_before_cleanup_record": retained,
        },
    )


def run_ls06_scenario(
    artifacts, runner, binaries, revision, profile, seed, b7_snapshot=False
):
    rng = random.Random(seed)
    cluster = deterministic_uuid(rng)
    stream = deterministic_uuid(rng)
    session = deterministic_uuid(rng)
    used_ports = set()
    configs = {}
    nodes = {}
    resource_sampler = None
    resource_phases = []

    def allocate_port():
        value = free_port()
        while value in used_ports:
            value = free_port()
        used_ports.add(value)
        return value

    for node_id in (1, 2, 3):
        public_port = allocate_port()
        peer_port = allocate_port()
        configs[node_id] = {
            "public_address": f"127.0.0.1:{public_port}",
            "peer_address": f"127.0.0.1:{peer_port}",
            "endpoint": f"http://127.0.0.1:{public_port}",
            "peer_uri": f"http://127.0.0.1:{peer_port}",
            "data_dir": artifacts / "scratch" / "success" / f"ls06-node-{node_id}",
        }

    def start_node(node_id, suffix, snapshot_delay_ms=0):
        config = configs[node_id]
        server = OwnedServer(
            binaries["light-streamd"],
            config["data_dir"],
            artifacts / "node-logs",
            f"ls06-node-{node_id}-{suffix}",
            node_id=node_id,
            public_address=config["public_address"],
            peer_address=config["peer_address"],
            advertise_public_uri=config["endpoint"],
            advertise_peer_uri=config["peer_uri"],
            peer_routes={
                target: configs[target]["peer_uri"]
                for target in (1, 2, 3)
                if target != node_id
            },
            max_data_groups=1,
            max_streams=1,
            max_partitions_per_stream=1,
            verification_delay_group_id=2 if snapshot_delay_ms else None,
            verification_delay_ms=snapshot_delay_ms,
        )
        nodes[node_id] = {**config, "server": server}

    def running_endpoints(exclude=None):
        return [
            nodes[node_id]["endpoint"]
            for node_id in sorted(nodes)
            if node_id != exclude and nodes[node_id]["server"].is_running()
        ]

    result = {
        "revision": revision,
        "cluster_id": cluster,
        "stream_id": stream,
        "scenario": "b7-snapshot" if b7_snapshot else "snapshot-recovery",
    }
    try:
        for node_id in (1, 2, 3):
            start_node(node_id, "initial")
        write_json(
            artifacts / "topology.json",
            {
                "revision": revision,
                "nodes": {
                    str(node_id): nodes[node_id]["server"].description()
                    for node_id in sorted(nodes)
                },
            },
        )
        bootstrap = cli_endpoint_command(
            binaries["light-streamctl"], nodes[1]["endpoint"], deadline_ms=30000
        ) + [
            "cluster",
            "bootstrap",
            "--cluster-id",
            cluster,
            "--stream-id",
            stream,
            "--stream-name",
            "bootstrap",
            "--seed-node-id",
            "1",
        ]
        for node_id in (1, 2, 3):
            bootstrap.extend(
                [
                    "--member",
                    (
                        f"{node_id},{nodes[node_id]['endpoint']},"
                        f"{nodes[node_id]['peer_uri']}"
                    ),
                ]
            )
        runner.run(bootstrap, "ls06-bootstrap", timeout=45)
        result["membership"] = wait_for_uniform_active(
            artifacts,
            runner,
            binaries["light-streamctl"],
            nodes,
            profile["write_readiness_seconds"] + 20,
            "ls06-active",
        )
        leader = wait_for_data_leader(
            artifacts,
            runner,
            binaries["light-streamctl"],
            nodes,
            profile["leader_loss_seconds"] + 10,
            "ls06-leader",
        )
        follower = next(node_id for node_id in (1, 2, 3) if node_id != leader)
        result["leader_node_id"] = leader
        result["lagging_follower_node_id"] = follower
        runner.run(
            cli_endpoint_command(
                binaries["light-streamctl"],
                nodes[leader]["endpoint"],
                seeds=running_endpoints(exclude=leader),
                deadline_ms=15000,
            )
            + [
                "publish",
                "--cluster-id",
                cluster,
                "--stream-id",
                stream,
                "--principal",
                f"verify-ls06-{seed}",
                "--session",
                session,
                "--sequence",
                "1",
                "--payload",
                "ready",
            ],
            "ls06-operational-readiness",
            timeout=20,
        )
        nodes[follower]["server"].stop()
        if b7_snapshot:
            target_bytes = int(
                os.environ.get("LIGHT_STREAM_B7_RETAINED_BYTES", 1024 * 1024 * 1024)
            )
            if target_bytes < 1024 * 1024 * 1024 and not os.environ.get(
                "LIGHT_STREAM_B7_ALLOW_SMALL"
            ):
                raise VerificationError("B7 requires at least 1 GiB retained per group")
            payload_count = (target_bytes + 1024 * 1024 - 1) // (1024 * 1024)
            required_free = target_bytes * 8 + 5 * 1024 * 1024 * 1024
            free_before = shutil.disk_usage(ROOT).free
            if free_before < required_free:
                raise VerificationError(
                    f"B7 requires {required_free} free bytes, observed {free_before}"
                )
            result["b7_plan"] = {
                "target_retained_bytes": target_bytes,
                "record_bytes": 1024 * 1024,
                "record_count": payload_count,
                "required_free_bytes": required_free,
                "free_bytes_before": free_before,
                "build_deadline_seconds": 1800,
                "transfer_deadline_seconds": 1800,
                "install_deadline_seconds": 1800,
                "independent_host": "BLOCKED",
            }
            write_json(artifacts / "ls06" / "b7-plan.json", result["b7_plan"])
            resource_sampler = ProcessRssSampler(artifacts, nodes)
        else:
            payload_count = 4

        reusable_payload = artifacts / "samples" / "ls06-snapshot-record.bin"
        reusable_payload.parent.mkdir(parents=True, exist_ok=True)
        expected_hashes = [hashlib.sha256(b"ready").hexdigest()]
        publish_receipts = []
        fixture_seed = hashlib.sha256(
            f"light-stream-ls06-{seed}".encode()
        ).digest()
        for index in range(payload_count):
            payload = hashlib.shake_256(
                fixture_seed + index.to_bytes(8, "big")
            ).digest(1024 * 1024)
            reusable_payload.write_bytes(payload)
            expected_hashes.append(hashlib.sha256(payload).hexdigest())
            sequence = index + 2
            publish = parse_json_output(
                runner.run(
                    cli_endpoint_command(
                        binaries["light-streamctl"],
                        nodes[leader]["endpoint"],
                        seeds=running_endpoints(exclude=leader),
                        deadline_ms=30000,
                    )
                    + [
                        "publish",
                        "--cluster-id",
                        cluster,
                        "--stream-id",
                        stream,
                        "--principal",
                        f"verify-ls06-{seed}",
                        "--session",
                        session,
                        "--sequence",
                        str(sequence),
                        "--file",
                        reusable_payload,
                    ],
                    f"ls06-publish-behind-follower-{sequence}",
                    timeout=35,
                ),
                "LS06 publish behind follower",
            )
            if not b7_snapshot or index in (0, payload_count - 1):
                publish_receipts.append(publish["receipt"])
            if b7_snapshot and (index + 1) % 128 == 0:
                resource_phases.append(
                    {
                        "phase": f"load-{index + 1}",
                        "allocated_bytes": {
                            str(node_id): allocated_tree_bytes(configs[node_id]["data_dir"])
                            for node_id in sorted(configs)
                        },
                        "free_bytes": shutil.disk_usage(ROOT).free,
                    }
                )
        if b7_snapshot:
            settle_started = time.monotonic()
            time.sleep(60)
            result["b7_plan"]["post_load_settle_seconds"] = time.monotonic() - settle_started
            resource_sampler.start()
            time.sleep(1)
            calibration = parse_json_output(
                runner.run(
                    cli_endpoint_command(
                        binaries["light-streamctl"],
                        nodes[leader]["endpoint"],
                        deadline_ms=1800000,
                    )
                    + [
                        "maintenance",
                        "snapshot",
                        "--cluster-id",
                        cluster,
                        "--group-id",
                        "2",
                    ],
                    "ls06-b7-calibration-snapshot",
                    timeout=1810,
                ),
                "LS06 B7 calibration snapshot",
            )["snapshot"]
            peaks = resource_sampler.peak_by_node()
            peak_deltas = resource_sampler.peak_delta_by_node()
            baseline = {}
            for sample in resource_sampler.samples:
                for node_id, value in sample["rss_bytes"].items():
                    baseline.setdefault(node_id, value)
            peak_delta = max(peak_deltas.values(), default=0)
            locked_rss_delta = peak_delta + max(
                peak_delta // 4, 128 * 1024 * 1024
            )
            result["b7_calibration"] = {
                "snapshot": calibration,
                "baseline_rss_bytes": baseline,
                "peak_rss_bytes": peaks,
                "peak_rss_delta_by_node": peak_deltas,
                "peak_rss_delta_bytes": peak_delta,
                "locked_rss_delta_bytes": locked_rss_delta,
            }
            write_json(
                artifacts / "ls06" / "b7-calibration.json",
                result["b7_calibration"],
            )
            result["b7_plan"]["locked_rss_delta_bytes"] = locked_rss_delta
            write_json(artifacts / "ls06" / "b7-plan.json", result["b7_plan"])
        snapshot = parse_json_output(
            runner.run(
                cli_endpoint_command(
                    binaries["light-streamctl"],
                    nodes[leader]["endpoint"],
                    deadline_ms=1800000 if b7_snapshot else 60000,
                )
                + [
                    "maintenance",
                    "snapshot",
                    "--cluster-id",
                    cluster,
                    "--group-id",
                    "2",
                    "--purge",
                ],
                "ls06-snapshot-and-purge",
                timeout=1810 if b7_snapshot else 65,
            ),
            "LS06 snapshot and purge",
        )["snapshot"]
        if snapshot["purged_index"] != snapshot["snapshot_index"]:
            raise VerificationError("LS06 purge did not stop at the completed snapshot")

        start_node(follower, "interrupted", snapshot_delay_ms=5 if b7_snapshot else 50)
        incoming = (
            configs[follower]["data_dir"] / "groups" / "2" / "snapshots" / "incoming"
        )
        interrupted_offset = None
        interruption_deadline = time.monotonic() + (600 if b7_snapshot else 20)
        interrupt_after = 64 * 1024 * 1024 if b7_snapshot else 1024 * 1024
        while time.monotonic() < interruption_deadline:
            for progress_path in incoming.glob("*.progress") if incoming.exists() else []:
                try:
                    offset = json.loads(progress_path.read_text())
                except (OSError, json.JSONDecodeError):
                    continue
                if isinstance(offset, int) and offset >= interrupt_after:
                    interrupted_offset = offset
                    break
            if interrupted_offset is not None:
                break
            time.sleep(0.001)
        if interrupted_offset is None:
            raise VerificationError("LS06 did not stage an acknowledged snapshot chunk")
        nodes[follower]["server"].kill()
        result["interrupted_durable_offset"] = interrupted_offset

        start_node(follower, "restarted")
        catch_up = wait_for_follower_catch_up(
            artifacts,
            runner,
            binaries["light-streamctl"],
            nodes,
            follower,
            1800 if b7_snapshot else profile["write_readiness_seconds"] + 30,
            "ls06-snapshot-catch-up",
        )
        fetch_deadline = time.monotonic() + profile["write_readiness_seconds"] + 10
        fetch_attempt = 0
        fetched = None
        while time.monotonic() < fetch_deadline:
            fetch_attempt += 1
            value = parse_json_output(
                runner.run(
                    cli_endpoint_command(
                        binaries["light-streamctl"],
                        nodes[follower]["endpoint"],
                        seeds=running_endpoints(exclude=follower),
                        deadline_ms=3000,
                    )
                    + [
                        "fetch",
                        "--cluster-id",
                        cluster,
                        "--stream-id",
                        stream,
                        "--offset",
                        "0",
                        "--limit",
                        "8",
                    ],
                    f"ls06-fetch-after-snapshot-{fetch_attempt}",
                    expected_codes=(0, 5),
                    timeout=8,
                ),
                "LS06 fetch after snapshot",
            )
            if value.get("ok") is True:
                fetched = value
                break
            time.sleep(0.05)
        if fetched is None:
            raise VerificationError("LS06 application reads did not recover after snapshot")
        expected_online = [b"ready"]
        if not b7_snapshot:
            expected_online.extend(
                hashlib.shake_256(
                    fixture_seed + index.to_bytes(8, "big")
                ).digest(1024 * 1024)
                for index in range(payload_count)
            )
        if not b7_snapshot and payloads_from_page(fetched) != expected_online:
            raise VerificationError("LS06 snapshot catch-up changed retained record bytes")
        nodes[follower]["server"].stop()
        offline = parse_json_output(
            runner.run(
                [
                    binaries["light-stream-testkit"],
                    "inspect-data-group",
                    "--path",
                    configs[follower]["data_dir"] / "groups" / "2",
                    "--cluster-id",
                    cluster,
                    "--group-id",
                    "2",
                    "--stream-id",
                    stream,
                    "--record-limit",
                    str(payload_count + 1),
                ],
                "ls06-offline-follower-payloads",
                timeout=30,
            ),
            "LS06 offline follower payloads",
        )
        observed_hashes = [record["sha256"] for record in offline["records"]]
        if observed_hashes != expected_hashes:
            raise VerificationError(
                "LS06 recovered follower storage differs from the byte ledger"
            )
        incoming_files = (
            sorted(path.name for path in incoming.iterdir()) if incoming.exists() else []
        )
        if incoming_files:
            raise VerificationError("LS06 completed snapshot staging files remain")
        receiver_log = (
            artifacts / "node-logs" / f"ls06-node-{follower}-restarted.stderr.log"
        )
        installs = []
        resumes = []
        for line in receiver_log.read_text().splitlines():
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if value.get("event") == "snapshot_install_completed":
                installs.append(value)
            if value.get("event") == "snapshot_transfer_began":
                resumes.append(value)
        if len(installs) != 1 or installs[0].get("chunk_count", 0) < 2:
            raise VerificationError(
                "LS06 receiver evidence does not prove a multi-chunk snapshot install"
            )
        if not any(
            value.get("durable_offset", 0) >= interrupted_offset for value in resumes
        ):
            raise VerificationError(
                "LS06 receiver did not resume from the acknowledged durable offset"
            )
        result.update(
            {
                "publish_receipts": publish_receipts,
                "payload_bytes": payload_count * 1024 * 1024,
                "payload_sha256": expected_hashes[1:],
                "snapshot": snapshot,
                "snapshot_install": installs[0],
                "snapshot_resume": resumes,
                "catch_up": catch_up,
                "offline_follower_records": offline["records"],
                "incoming_files_after_success": incoming_files,
                "retained_records_verified": payload_count + 1,
                "verdict": "PASS",
            }
        )
        if b7_snapshot:
            final_peaks = resource_sampler.peak_by_node()
            final_deltas = resource_sampler.peak_delta_by_node()
            resource_sampler.stop()
            resource_sampler = None
            if max(final_deltas.values(), default=0) > result["b7_plan"][
                "locked_rss_delta_bytes"
            ]:
                raise VerificationError("B7 exceeded the locked RSS delta")
            result["b7_resources"] = {
                "peak_rss_bytes": final_peaks,
                "peak_rss_delta_by_node": final_deltas,
                "disk_phases": resource_phases,
                "free_bytes_after": shutil.disk_usage(ROOT).free,
            }
            write_json(artifacts / "ls06" / "b7-recovery.json", result)
        else:
            write_json(artifacts / "ls06" / "snapshot-recovery.json", result)
    finally:
        if resource_sampler is not None:
            resource_sampler.stop()
        for node_id in sorted(nodes):
            try:
                nodes[node_id]["server"].stop()
            except VerificationError:
                pass


def run_ls06_membership_scenario(
    artifacts, runner, binaries, revision, profile, seed
):
    rng = random.Random(seed)
    cluster = deterministic_uuid(rng)
    stream = deterministic_uuid(rng)
    session = deterministic_uuid(rng)
    nodes = {}
    configs = {}
    used_ports = set()

    def allocate_port():
        value = free_port()
        while value in used_ports:
            value = free_port()
        used_ports.add(value)
        return value

    for node_id in (1, 2, 3, 4):
        public_port = allocate_port()
        peer_port = allocate_port()
        configs[node_id] = {
            "endpoint": f"http://127.0.0.1:{public_port}",
            "peer_uri": f"http://127.0.0.1:{peer_port}",
            "public_address": f"127.0.0.1:{public_port}",
            "peer_address": f"127.0.0.1:{peer_port}",
            "data_dir": artifacts / "scratch" / "success" / f"ls06-admin-node-{node_id}",
        }

    def start_node(node_id, suffix, administration_delay_ms=1000):
        config = configs[node_id]
        nodes[node_id] = {
            **config,
            "server": OwnedServer(
                binaries["light-streamd"],
                config["data_dir"],
                artifacts / "node-logs",
                f"ls06-admin-node-{node_id}-{suffix}",
                node_id=node_id,
                public_address=config["public_address"],
                peer_address=config["peer_address"],
                advertise_public_uri=config["endpoint"],
                advertise_peer_uri=config["peer_uri"],
                max_data_groups=2,
                max_streams=2,
                max_partitions_per_stream=2,
                verification_delay_group_id=2 if administration_delay_ms else None,
                verification_delay_ms=administration_delay_ms,
                ready_timeout_seconds=30,
            ),
        }

    def diagnostics(node_id, label):
        return read_node_diagnostics(
            artifacts, runner, binaries["light-streamctl"], nodes[node_id], label
        )

    def wait_control_leader(node_ids, label):
        deadline = time.monotonic() + profile["leader_loss_seconds"] + 20
        last = []
        while time.monotonic() < deadline:
            leaders = []
            for node_id in node_ids:
                try:
                    group = group_by_name(
                        diagnostics(node_id, f"{label}-{node_id}"), "control", 1
                    )
                    if group["current_leader"] in node_ids:
                        leaders.append(group["current_leader"])
                except (KeyError, VerificationError, subprocess.SubprocessError):
                    pass
            if leaders and len(set(leaders)) == 1:
                return leaders[0]
            last = leaders
            time.sleep(0.05)
        raise VerificationError(f"{label} control leader did not converge: {last}")

    def wait_operation(request_id, node_ids, label):
        deadline = time.monotonic() + profile["write_readiness_seconds"] + 80
        last = None
        while time.monotonic() < deadline:
            leader = wait_control_leader(node_ids, f"{label}-leader")
            value = parse_json_output(
                runner.run(
                    cli_endpoint_command(
                        binaries["light-streamctl"],
                        nodes[leader]["endpoint"],
                        deadline_ms=5000,
                    )
                    + [
                        "cluster",
                        "operation",
                        "--cluster-id",
                        cluster,
                        "--request-id",
                        request_id,
                    ],
                    f"{label}-status",
                    expected_codes=(0, 5),
                    timeout=10,
                ),
                label,
            )
            last = value
            if value.get("ok") and value["operation"]["lifecycle"] == "complete":
                return value["operation"]
            time.sleep(0.1)
        raise VerificationError(f"{label} operation did not complete: {last}")

    def wait_exact_membership(node_ids, label):
        deadline = time.monotonic() + profile["write_readiness_seconds"] + 30
        last = {}
        while time.monotonic() < deadline:
            observed = {}
            try:
                for node_id in node_ids:
                    observed[str(node_id)] = assert_exact_memberships(
                        diagnostics(node_id, f"{label}-{node_id}"),
                        [1, 2, 4],
                    )
                return observed
            except (KeyError, VerificationError, subprocess.SubprocessError):
                last = observed
                time.sleep(0.1)
        raise VerificationError(f"{label} membership did not converge: {last}")

    result = {"revision": revision, "cluster_id": cluster, "stream_id": stream}
    try:
        for node_id in (1, 2, 3, 4):
            start_node(node_id, "initial")
        bootstrap = cli_endpoint_command(
            binaries["light-streamctl"], nodes[1]["endpoint"], deadline_ms=30000
        ) + [
            "cluster",
            "bootstrap",
            "--cluster-id",
            cluster,
            "--stream-id",
            stream,
            "--stream-name",
            "bootstrap",
            "--seed-node-id",
            "1",
        ]
        for node_id in (1, 2, 3):
            bootstrap.extend(
                [
                    "--member",
                    (
                        f"{node_id},{nodes[node_id]['endpoint']},"
                        f"{nodes[node_id]['peer_uri']}"
                    ),
                ]
            )
        runner.run(bootstrap, "ls06-admin-bootstrap", timeout=45)
        wait_for_uniform_active(
            artifacts,
            runner,
            binaries["light-streamctl"],
            {node_id: nodes[node_id] for node_id in (1, 2, 3)},
            profile["write_readiness_seconds"] + 20,
            "ls06-admin-active",
        )
        control_leader = wait_control_leader((1, 2, 3), "ls06-admin-control")
        busy_stream = parse_json_output(
            runner.run(
                cli_endpoint_command(
                    binaries["light-streamctl"],
                    nodes[control_leader]["endpoint"],
                    deadline_ms=30000,
                )
                + [
                    "stream",
                    "create",
                    "--cluster-id",
                    cluster,
                    "--request-id",
                    deterministic_uuid(rng),
                    "--name",
                    "busy-during-replacement",
                    "--partitions",
                    "1",
                ],
                "ls06-create-busy-stream",
                timeout=35,
            ),
            "LS06 create busy stream",
        )["stream"]
        busy_group = busy_stream["placements"][0]["group"]
        if busy_group != 3:
            raise VerificationError(
                f"busy stream was assigned to group {busy_group}, expected group 3"
            )
        replacement_request = deterministic_uuid(rng)
        replacement = parse_json_output(
            runner.run(
                cli_endpoint_command(
                    binaries["light-streamctl"],
                    nodes[control_leader]["endpoint"],
                    deadline_ms=15000,
                )
                + [
                    "cluster",
                    "replace-voter",
                    "--cluster-id",
                    cluster,
                    "--request-id",
                    replacement_request,
                    "--expected-topology-revision",
                    "1",
                    "--remove-node-id",
                    "3",
                    "--add",
                    f"4,{nodes[4]['endpoint']},{nodes[4]['peer_uri']}",
                ],
                "ls06-replace-voter",
                timeout=20,
            ),
            "LS06 replace voter",
        )
        nodes[control_leader]["server"].kill()
        result["coordinator_killed_after_intent"] = control_leader
        operation_nodes = tuple(
            node_id for node_id in (1, 2, 3, 4) if node_id != control_leader
        )
        busy_endpoint = next(
            nodes[node_id]["endpoint"]
            for node_id in operation_nodes
            if nodes[node_id]["server"].is_running()
        )
        busy_publish = parse_json_output(
            runner.run(
                cli_endpoint_command(
                    binaries["light-streamctl"],
                    busy_endpoint,
                    seeds=[
                        nodes[node_id]["endpoint"]
                        for node_id in operation_nodes
                        if nodes[node_id]["server"].is_running()
                    ],
                    deadline_ms=15000,
                )
                + [
                    "publish",
                    "--cluster-id",
                    cluster,
                    "--stream-id",
                    busy_stream["stream"],
                    "--principal",
                    "ls06-membership",
                    "--session",
                    session,
                    "--sequence",
                    "1",
                    "--payload",
                    "busy-group-remained-available",
                ],
                "ls06-busy-group-publish",
                timeout=20,
            ),
            "LS06 busy group publish",
        )
        replacement_complete = wait_operation(
            replacement_request, operation_nodes, "ls06-replacement"
        )
        if control_leader in (1, 2):
            start_node(control_leader, "restart-after-intent", administration_delay_ms=0)
        memberships = wait_exact_membership(
            (1, 2, 4), "ls06-final-membership"
        )
        data_leader = wait_for_data_leader(
            artifacts,
            runner,
            binaries["light-streamctl"],
            {node_id: nodes[node_id] for node_id in (1, 2, 4)},
            profile["leader_loss_seconds"] + 10,
            "ls06-before-transfer",
        )
        target = next(node for node in (1, 2, 4) if node != data_leader)
        control_leader = wait_control_leader((1, 2, 4), "ls06-transfer-control")
        transfer_request = deterministic_uuid(rng)
        transfer = parse_json_output(
            runner.run(
                cli_endpoint_command(
                    binaries["light-streamctl"],
                    nodes[control_leader]["endpoint"],
                    deadline_ms=15000,
                )
                + [
                    "cluster",
                    "transfer-leader",
                    "--cluster-id",
                    cluster,
                    "--request-id",
                    transfer_request,
                    "--group-id",
                    "2",
                    "--target-node-id",
                    str(target),
                ],
                "ls06-transfer-leader",
                timeout=20,
            ),
            "LS06 transfer leader",
        )
        transfer_complete = wait_operation(
            transfer_request, (1, 2, 4), "ls06-transfer"
        )
        transferred = wait_for_data_leader(
            artifacts,
            runner,
            binaries["light-streamctl"],
            {node_id: nodes[node_id] for node_id in (1, 2, 4)},
            profile["leader_loss_seconds"] + 10,
            "ls06-after-transfer",
        )
        if transferred != target:
            raise VerificationError(
                f"transferred data leader is {transferred}, expected {target}"
            )
        nodes[3]["server"].stop()
        start_node(3, "retired-restart", administration_delay_ms=0)
        removed = parse_json_output(
            runner.run(
                cli_endpoint_command(
                    binaries["light-streamctl"],
                    nodes[3]["endpoint"],
                    no_retry=True,
                    deadline_ms=3000,
                )
                + [
                    "fetch",
                    "--cluster-id",
                    cluster,
                    "--stream-id",
                    stream,
                    "--offset",
                    "0",
                    "--limit",
                    "1",
                ],
                "ls06-removed-node-refusal",
                expected_codes=(5,),
                timeout=8,
            ),
            "LS06 removed node refusal",
        )
        removed_error = removed.get("error", {})
        if (
            removed.get("ok") is not False
            or removed_error.get("code") != "cluster_forming"
            or removed_error.get("detail", {}).get("leader") is not None
        ):
            raise VerificationError(
                "removed node was not locally deauthorized after restart"
            )
        control_leader = wait_control_leader((1, 2, 4), "ls06-abort-control")
        control_follower = next(
            node_id for node_id in (1, 2, 4) if node_id != control_leader
        )
        abort_request = deterministic_uuid(rng)
        unavailable_public = allocate_port()
        unavailable_peer = allocate_port()
        unreachable = parse_json_output(
            runner.run(
                cli_endpoint_command(
                    binaries["light-streamctl"],
                    nodes[control_follower]["endpoint"],
                    deadline_ms=15000,
                )
                + [
                    "cluster",
                    "replace-voter",
                    "--cluster-id",
                    cluster,
                    "--request-id",
                    abort_request,
                    "--expected-topology-revision",
                    "3",
                    "--remove-node-id",
                    "4",
                    "--add",
                    (
                        f"5,http://127.0.0.1:{unavailable_public},"
                        f"http://127.0.0.1:{unavailable_peer}"
                    ),
                ],
                "ls06-unreachable-replacement",
                timeout=20,
            ),
            "LS06 unreachable replacement",
        )
        aborted = parse_json_output(
            runner.run(
                cli_endpoint_command(
                    binaries["light-streamctl"],
                    nodes[control_follower]["endpoint"],
                    deadline_ms=15000,
                )
                + [
                    "cluster",
                    "abort",
                    "--cluster-id",
                    cluster,
                    "--request-id",
                    abort_request,
                ],
                "ls06-abort-replacement",
                timeout=20,
            ),
            "LS06 abort replacement",
        )
        if (
            aborted["operation"]["lifecycle"] != "aborted"
            or aborted["operation"]["topology_revision"] != 5
        ):
            raise VerificationError("replacement abort did not reach a terminal state")
        post_abort_membership = wait_exact_membership(
            (1, 2, 4), "ls06-post-abort-membership"
        )
        busy_fetch = parse_json_output(
            runner.run(
                cli_endpoint_command(
                    binaries["light-streamctl"],
                    nodes[1]["endpoint"],
                    seeds=[nodes[2]["endpoint"], nodes[4]["endpoint"]],
                    deadline_ms=15000,
                )
                + [
                    "fetch",
                    "--cluster-id",
                    cluster,
                    "--stream-id",
                    busy_stream["stream"],
                    "--offset",
                    "0",
                    "--limit",
                    "1",
                ],
                "ls06-busy-group-fetch",
                timeout=20,
            ),
            "LS06 busy group fetch",
        )
        if payloads_from_page(busy_fetch) != [b"busy-group-remained-available"]:
            raise VerificationError("unrelated data group lost traffic during replacement")
        for node_id in (1, 2, 4):
            nodes[node_id]["server"].stop()
        for node_id in (1, 2, 4):
            start_node(node_id, "full-restart", administration_delay_ms=0)
        restarted_memberships = wait_exact_membership(
            (1, 2, 4), "ls06-restarted-membership"
        )
        result.update(
            {
                "replacement_begin": replacement["operation"],
                "replacement_complete": replacement_complete,
                "final_membership": memberships,
                "transfer_begin": transfer["operation"],
                "transfer_complete": transfer_complete,
                "transferred_data_leader": transferred,
                "removed_node_refusal": removed,
                "unreachable_replacement": unreachable["operation"],
                "aborted_replacement": aborted["operation"],
                "post_abort_membership": post_abort_membership,
                "busy_group_id": busy_group,
                "busy_group_publish": busy_publish["receipt"],
                "busy_group_verified": True,
                "restarted_membership": restarted_memberships,
                "verdict": "PASS",
            }
        )
        write_json(artifacts / "ls06" / "membership-recovery.json", result)
    finally:
        for node in nodes.values():
            try:
                node["server"].stop()
            except VerificationError:
                pass


def run_ls07_scenario(artifacts, runner, binaries, revision, seed):
    rng = random.Random(seed)
    cluster = deterministic_uuid(rng)
    stream = deterministic_uuid(rng)
    mutation_session = deterministic_uuid(rng)
    data_dir = artifacts / "scratch" / "success" / "ls07"
    server = OwnedServer(
        binaries["light-streamd"],
        data_dir,
        artifacts / "node-logs",
        "ls07-batching",
        publish_coalesce_us=100_000,
    )
    endpoint = f"http://{server.ready['public_address']}"
    try:
        bootstrap = parse_json_output(
            runner.run(
                [
                    binaries["light-streamctl"],
                    "--endpoint",
                    endpoint,
                    "cluster",
                    "bootstrap",
                    "--cluster-id",
                    cluster,
                    "--stream-id",
                    stream,
                    "--stream-name",
                    "bootstrap",
                ],
                "ls07-bootstrap",
                timeout=30,
            ),
            "LS07 bootstrap",
        )
        before = parse_json_output(
            runner.run(
                [binaries["light-streamctl"], "--endpoint", endpoint, "diagnostics"],
                "ls07-diagnostics-before",
                timeout=10,
            ),
            "LS07 diagnostics before batching",
        )
        data_before = next(
            group
            for group in before["diagnostics"]["groups"]
            if group["group"] == "data"
        )

        payload = artifacts / "samples" / "ls07-record.bin"
        payload.parent.mkdir(parents=True, exist_ok=True)
        payload.write_bytes(bytes(rng.randrange(0, 256) for _ in range(1024)))
        commands = []
        for index in range(32):
            command = publish_command(
                binaries["light-streamctl"],
                endpoint,
                cluster,
                stream,
                "ls07-producer",
                deterministic_uuid(rng),
                1,
                payload,
                deadline_ms=15000,
            )
            if index == 0:
                command.extend(["--bookmark", "batch-bookmark"])
            commands.append(command)
        publish_results = [
            parse_json_output(result, f"LS07 concurrent publish {index}")
            for index, result in enumerate(
                runner.run_parallel(commands, "ls07-concurrent-publish", timeout=30)
            )
        ]
        offsets = sorted(
            result["receipt"]["range"]["first"] for result in publish_results
        )
        if offsets != list(range(32)):
            raise VerificationError("LS07 concurrent publish ledger is not contiguous")

        after = parse_json_output(
            runner.run(
                [binaries["light-streamctl"], "--endpoint", endpoint, "diagnostics"],
                "ls07-diagnostics-after",
                timeout=10,
            ),
            "LS07 diagnostics after batching",
        )
        data_after = next(
            group
            for group in after["diagnostics"]["groups"]
            if group["group"] == "data"
        )
        physical_entries = data_after["last_log_index"] - data_before["last_log_index"]
        if physical_entries >= len(publish_results):
            raise VerificationError("LS07 did not combine concurrent publish requests")
        write_json(
            artifacts / "l01.json",
            {
                "logical_requests": len(publish_results),
                "physical_entries": physical_entries,
                "offsets": offsets,
                "verdict": "PASS",
            },
        )
        route = parse_json_output(
            runner.run(
                [
                    binaries["light-streamctl"],
                    "--endpoint",
                    endpoint,
                    "stream",
                    "route",
                    "--cluster-id",
                    cluster,
                    "--stream-id",
                    stream,
                    "--partition",
                    "0",
                ],
                "ls07-route",
                timeout=10,
            ),
            "LS07 route",
        )["route"]["route"]
        lost_session = deterministic_uuid(rng)
        proxy = OneShotResponseDropProxy(
            free_port(),
            int(server.ready["public_address"].rsplit(":", 1)[1]),
        )
        resolved = parse_json_output(
            runner.run(
                publish_command(
                    binaries["light-streamctl"],
                    proxy.endpoint,
                    cluster,
                    stream,
                    "ls07-resolver",
                    lost_session,
                    1,
                    payload,
                    seeds=(endpoint,),
                    no_retry=True,
                    deadline_ms=10000,
                    route_group_id=route["group"],
                    route_revision=route["route_revision"],
                )
                + ["--resolve-receipt"],
                "ls07-resolve-dropped-response",
                timeout=15,
            ),
            "LS07 resolved dropped response",
        )
        proxy.wait()
        if resolved["receipt"]["range"]["first"] != 32:
            raise VerificationError("LS07 receipt resolution returned the wrong record")
        write_json(
            artifacts / "l04.json",
            {
                "response_dropped": True,
                "resolved_receipt": resolved["receipt"],
                "verdict": "PASS",
            },
        )
        sparse_session = deterministic_uuid(rng)
        sparse_started = time.monotonic()
        sparse = parse_json_output(
            runner.run(
                publish_command(
                    binaries["light-streamctl"],
                    endpoint,
                    cluster,
                    stream,
                    "ls07-sparse",
                    sparse_session,
                    1,
                    payload,
                    deadline_ms=5000,
                ),
                "ls07-sparse-publish",
                timeout=10,
            ),
            "LS07 sparse publish",
        )
        sparse_seconds = time.monotonic() - sparse_started
        if sparse_seconds > 0.5:
            raise VerificationError(
                f"LS07 sparse publish exceeded its flush bound: {sparse_seconds}"
            )
        write_json(
            artifacts / "l06.json",
            {
                "configured_coalesce_microseconds": 100_000,
                "end_to_end_seconds": sparse_seconds,
                "receipt": sparse["receipt"],
                "verdict": "PASS",
            },
        )

        bookmarks_before = parse_json_output(
            runner.run(
                [
                    binaries["light-streamctl"],
                    "--endpoint",
                    endpoint,
                    "bookmark",
                    "list",
                    "--cluster-id",
                    cluster,
                    "--stream-id",
                    stream,
                ],
                "ls07-bookmarks-before-checkpoint",
                timeout=10,
            ),
            "LS07 bookmarks before checkpoint",
        )
        create_checkpoint = parse_json_output(
            runner.run(
                [
                    binaries["light-streamctl"],
                    "--endpoint",
                    endpoint,
                    "checkpoint",
                    "advance",
                    "--cluster-id",
                    cluster,
                    "--stream-id",
                    stream,
                    "--consumer",
                    "billing",
                    "--expect-missing",
                    "--offset",
                    "10",
                    "--principal",
                    "ls07-consumer",
                    "--mutation-session",
                    mutation_session,
                    "--sequence",
                    "1",
                ],
                "ls07-checkpoint-create",
                timeout=10,
            ),
            "LS07 checkpoint create",
        )
        advance_checkpoint = parse_json_output(
            runner.run(
                [
                    binaries["light-streamctl"],
                    "--endpoint",
                    endpoint,
                    "checkpoint",
                    "advance",
                    "--cluster-id",
                    cluster,
                    "--stream-id",
                    stream,
                    "--consumer",
                    "billing",
                    "--expected-revision",
                    "1",
                    "--offset",
                    "20",
                    "--principal",
                    "ls07-consumer",
                    "--mutation-session",
                    mutation_session,
                    "--sequence",
                    "2",
                ],
                "ls07-checkpoint-advance",
                timeout=10,
            ),
            "LS07 checkpoint advance",
        )
        conflict = parse_json_output(
            runner.run(
                [
                    binaries["light-streamctl"],
                    "--endpoint",
                    endpoint,
                    "checkpoint",
                    "advance",
                    "--cluster-id",
                    cluster,
                    "--stream-id",
                    stream,
                    "--consumer",
                    "billing",
                    "--expected-revision",
                    "1",
                    "--offset",
                    "25",
                    "--principal",
                    "ls07-consumer",
                    "--mutation-session",
                    mutation_session,
                    "--sequence",
                    "3",
                ],
                "ls07-checkpoint-conflict",
                timeout=10,
            ),
            "LS07 checkpoint conflict",
        )
        if conflict["result"]["kind"] != "conflict":
            raise VerificationError("LS07 stale checkpoint revision did not conflict")
        bookmarks_after = parse_json_output(
            runner.run(
                [
                    binaries["light-streamctl"],
                    "--endpoint",
                    endpoint,
                    "bookmark",
                    "list",
                    "--cluster-id",
                    cluster,
                    "--stream-id",
                    stream,
                ],
                "ls07-bookmarks-after-checkpoint",
                timeout=10,
            ),
            "LS07 bookmarks after checkpoint",
        )
        if bookmarks_before["page"] != bookmarks_after["page"]:
            raise VerificationError("LS07 checkpoint mutation changed bookmark state")
        write_json(
            artifacts / "l08.json",
            {
                "create": create_checkpoint["result"],
                "advance": advance_checkpoint["result"],
                "conflict": conflict["result"],
                "bookmarks_unchanged": True,
                "verdict": "PASS",
            },
        )

        server.stop()
        server = OwnedServer(
            binaries["light-streamd"],
            data_dir,
            artifacts / "node-logs",
            "ls07-cancellation-restart",
            verification_response_delay_group_id=2,
            verification_response_delay_ms=1000,
        )
        endpoint = f"http://{server.ready['public_address']}"
        checkpoint_after_restart = parse_json_output(
            runner.run(
                [
                    binaries["light-streamctl"],
                    "--endpoint",
                    endpoint,
                    "checkpoint",
                    "get",
                    "--cluster-id",
                    cluster,
                    "--stream-id",
                    stream,
                    "--consumer",
                    "billing",
                ],
                "ls07-checkpoint-after-restart",
                timeout=15,
            ),
            "LS07 checkpoint after restart",
        )
        if checkpoint_after_restart["checkpoint"]["cursor"]["next_offset"] != 20:
            raise VerificationError("LS07 checkpoint changed across restart")

        cancelled_session = deterministic_uuid(rng)
        cancelled_command = publish_command(
            binaries["light-streamctl"],
            endpoint,
            cluster,
            stream,
            "ls07-cancelled",
            cancelled_session,
            1,
            payload,
            no_retry=True,
            deadline_ms=10000,
            route_group_id=route["group"],
            route_revision=route["route_revision"],
        )
        cancelled = parse_json_output(
            runner.run_interrupted(
                cancelled_command,
                "ls07-cancel-after-send",
                delay_seconds=0.2,
                timeout=15,
            ),
            "LS07 cancelled publish",
        )
        if (
            cancelled.get("error", {}).get("code") != "cancelled"
            or cancelled["error"].get("outcome") != "ambiguous_commit"
        ):
            raise VerificationError("LS07 cancellation did not preserve commit certainty")
        time.sleep(1.2)
        cancelled_receipt = parse_json_output(
            runner.run(
                [
                    binaries["light-streamctl"],
                    "--endpoint",
                    endpoint,
                    "--no-retry",
                    "receipt",
                    "--cluster-id",
                    cluster,
                    "--stream-id",
                    stream,
                    "--principal",
                    "ls07-cancelled",
                    "--session",
                    cancelled_session,
                    "--sequence",
                    "1",
                    "--route-group-id",
                    str(route["group"]),
                    "--route-revision",
                    str(route["route_revision"]),
                ],
                "ls07-cancelled-receipt",
                timeout=10,
            ),
            "LS07 cancelled receipt",
        )
        if (
            cancelled_receipt.get("receipt", {})
            .get("request", {})
            .get("session")
            != cancelled_session
        ):
            raise VerificationError("LS07 submitted cancellation lost its durable receipt")
        write_json(
            artifacts / "l05.json",
            {
                "client_result": cancelled,
                "receipt_after_response_delay": cancelled_receipt,
                "submitted_request_committed": True,
                "verdict": "PASS",
            },
        )

        server.stop()
        server = OwnedServer(
            binaries["light-streamd"],
            data_dir,
            artifacts / "node-logs",
            "ls07-overload-restart",
            verification_delay_group_id=2,
            verification_delay_ms=1000,
            publish_queue_requests=1,
            publish_queue_records=128,
            publish_queue_bytes=9 * 1024 * 1024,
            publish_batch_requests=1,
            publish_batch_records=128,
            publish_batch_bytes=8 * 1024 * 1024,
            publish_coalesce_us=1_000,
        )
        endpoint = f"http://{server.ready['public_address']}"
        overload_commands = [
            publish_command(
                binaries["light-streamctl"],
                endpoint,
                cluster,
                stream,
                "ls07-overload",
                deterministic_uuid(rng),
                1,
                payload,
                deadline_ms=10000,
            )
            for _ in range(2)
        ]
        overload_results = runner.run_parallel(
            overload_commands,
            "ls07-overload",
            expected_codes=(0, 4),
            timeout=20,
        )
        overload_values = [
            parse_json_output(result, f"LS07 overload result {index}")
            for index, result in enumerate(overload_results)
        ]
        codes = sorted(
            value.get("error", {}).get("code", "success") for value in overload_values
        )
        if codes != ["publish_overloaded", "success"]:
            raise VerificationError(f"LS07 overload outcomes were {codes}")
        overload = next(value for value in overload_values if value.get("ok") is False)
        if overload["error"].get("outcome") != "definite_no_commit":
            raise VerificationError("LS07 overload did not report definite no commit")
        recovery = parse_json_output(
            runner.run(
                publish_command(
                    binaries["light-streamctl"],
                    endpoint,
                    cluster,
                    stream,
                    "ls07-overload",
                    deterministic_uuid(rng),
                    1,
                    payload,
                    deadline_ms=10000,
                ),
                "ls07-overload-recovery",
                timeout=15,
            ),
            "LS07 overload recovery",
        )
        write_json(
            artifacts / "l07.json",
            {
                "outcomes": overload_values,
                "recovery": recovery,
                "verdict": "PASS",
            },
        )
        fetch_checkpoint = parse_json_output(
            runner.run(
                [
                    binaries["light-streamctl"],
                    "--endpoint",
                    endpoint,
                    "checkpoint",
                    "fetch",
                    "--cluster-id",
                    cluster,
                    "--stream-id",
                    stream,
                    "--consumer",
                    "billing",
                    "--limit",
                    "4",
                ],
                "ls07-checkpoint-fetch",
                timeout=15,
            ),
            "LS07 checkpoint fetch",
        )
        write_json(
            artifacts / "l09.json",
            {
                "checkpoint": checkpoint_after_restart["checkpoint"],
                "fetch": fetch_checkpoint["page"],
                "verdict": "PASS",
            },
        )
        write_json(
            artifacts / "l02.json",
            {
                "cli_publish_count": len(publish_results) + 2,
                "checkpoint": checkpoint_after_restart["checkpoint"],
                "fetch": fetch_checkpoint["page"],
                "ledger_offsets": offsets,
                "verdict": "PASS",
            },
        )
        write_json(
            artifacts / "l10.json",
            {
                "checkpoint_conflict": conflict,
                "overload": overload,
                "cancellation": cancelled,
                "all_outputs_parseable": True,
                "verdict": "PASS",
            },
        )
        result = {
            "bootstrap": bootstrap,
            "batching": {
                "logical_requests": len(publish_results),
                "physical_entries": physical_entries,
            },
            "checkpoint_revision": checkpoint_after_restart["checkpoint"]["revision"],
            "overload_recovered": True,
            "secured_mode": "UNSUPPORTED_LS08",
            "independent_hosts": "BLOCKED",
            "revision": revision,
            "verdict": "PASS",
        }
        write_json(artifacts / "ls07" / "client-workflows.json", result)
    finally:
        server.stop()


def run_selected(args, artifacts, runner, binaries, revision, profile):
    if args.phase == "LS07" or args.suite == "ls07-e2e":
        runner.run(
            [
                "cargo",
                "test",
                "-p",
                "light-stream-core",
                "-p",
                "light-stream-storage",
                "-p",
                "light-stream-server",
                "-p",
                "light-stream-client",
            ],
            "ls07-targeted-tests",
            timeout=900,
        )
        run_ls07_scenario(artifacts, runner, binaries, revision, args.seed)
        run_ls02b_scenario(
            artifacts,
            runner,
            binaries,
            revision,
            profile,
            args.seed + 1,
            verify_checkpoints=True,
        )
        return
    if args.phase == "LS06" or args.suite == "ls06-e2e":
        runner.run(
            [
                "cargo",
                "test",
                "-p",
                "light-stream-storage",
                "-p",
                "light-stream-server",
                "-p",
                "light-stream-client",
            ],
            "ls06-targeted-tests",
            timeout=900,
        )
        if args.scenario == "membership":
            run_ls06_membership_scenario(
                artifacts, runner, binaries, revision, profile, args.seed
            )
        else:
            run_ls06_scenario(
                artifacts,
                runner,
                binaries,
                revision,
                profile,
                args.seed,
                b7_snapshot=args.scenario == "b7-snapshot",
            )
        return
    if args.phase == "LS05" or args.suite == "ls05-e2e":
        runner.run(
            [
                "cargo",
                "test",
                "-p",
                "light-stream-core",
                "-p",
                "light-stream-storage",
                "-p",
                "light-stream-server",
                "-p",
                "light-stream-client",
            ],
            "ls05-targeted-tests",
            timeout=900,
        )
        run_ls05_scenario(artifacts, runner, binaries, revision, profile, args.seed)
        return
    if args.phase == "LS04" or args.suite == "ls04-e2e":
        runner.run(
            [
                "cargo",
                "test",
                "-p",
                "light-stream-core",
                "-p",
                "light-stream-storage",
                "-p",
                "light-stream-server",
                "-p",
                "light-stream-client",
            ],
            "ls04-targeted-tests",
            timeout=900,
        )
        run_ls04_scenario(artifacts, runner, binaries, revision, profile, args.seed)
        return
    if args.phase == "LS03" or args.suite == "ls03-e2e":
        runner.run(
            ["cargo", "test", "-p", "light-stream-storage", "-p", "light-stream-server"],
            "ls03-storage-and-server-tests",
            timeout=900,
        )
        run_ls03_scenario(artifacts, runner, binaries, revision, profile, args.seed)
        return
    if args.phase == "LS02b" or args.suite == "ls02b-e2e":
        runner.run(
            ["cargo", "test", "-p", "light-stream-storage"],
            "storage-conformance-and-product-tests",
            timeout=900,
        )
        write_json(
            artifacts / "storage-test-evidence.json",
            {
                "verdict": "PASS",
                "command": "cargo test -p light-stream-storage",
                "snapshot_unit_tests_preserved": True,
            },
        )
        run_ls02b_scenario(
            artifacts,
            runner,
            binaries,
            revision,
            profile,
            args.seed,
        )
        return
    if args.phase in ("LS02a", "LS02") or args.suite == "ls02a-e2e":
        runner.run(
            ["cargo", "test", "-p", "light-stream-storage"],
            "storage-conformance-and-product-tests",
            timeout=900,
        )
        write_json(
            artifacts / "storage-test-evidence.json",
            {
                "verdict": "PASS",
                "command": "cargo test -p light-stream-storage",
                "openraft_conformance": "RAN",
                "product_tests": [
                    "purge_keeps_applied_payload_readable",
                    "snapshot_contains_and_installs_payloads",
                    "corrupted_identity_is_refused",
                ],
            },
        )
        run_ls02a_scenario(artifacts, runner, binaries, revision, args.seed)
        run_poc_control(artifacts, runner)
        return
    scenario = args.scenario
    if scenario in ("all", "health-capabilities"):
        run_health_scenario(artifacts, runner, binaries, revision)
    if scenario in ("all", "unsupported-publish"):
        run_publish_scenario(artifacts, runner, binaries)
    if scenario in ("all", "process-isolation"):
        run_isolation_scenario(artifacts, runner, binaries)
    if scenario in ("all", "evidence-preservation"):
        run_preservation_scenario(
            artifacts,
            runner,
            binaries,
            revision,
            profile["name"],
            args.seed,
        )
    if scenario == "all":
        run_poc_control(artifacts, runner)


def parse_args():
    parser = argparse.ArgumentParser()
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument("--phase")
    selection.add_argument("--suite")
    parser.add_argument("--profile", required=True)
    parser.add_argument("--security", default="local-insecure")
    parser.add_argument("--artifacts", required=True, type=Path)
    parser.add_argument("--scenario", default="all")
    parser.add_argument("--seed", type=int, default=10101)
    args = parser.parse_args()
    if args.phase is not None and args.phase not in (
        "LS01",
        "LS02a",
        "LS02b",
        "LS02",
        "LS03",
        "LS04",
        "LS05",
        "LS06",
        "LS07",
    ):
        parser.error(f"unknown phase {args.phase!r}")
    if args.suite is not None and args.suite not in (
        "e2e",
        "ls02a-e2e",
        "ls02b-e2e",
        "ls03-e2e",
        "ls04-e2e",
        "ls05-e2e",
        "ls06-e2e",
        "ls07-e2e",
    ):
        parser.error(f"unknown suite {args.suite!r}")
    if args.scenario not in KNOWN_SCENARIOS:
        parser.error(f"unknown scenario {args.scenario!r}")
    if args.security not in ("local-insecure", "secured", "all"):
        parser.error(f"unknown security selection {args.security!r}")
    return args


class VerifierHelperTests(unittest.TestCase):
    @staticmethod
    def diagnostics():
        group = {
            "effective_uniform": True,
            "effective_voters": [1, 2, 3],
            "effective_learners": [],
            "committed_uniform": True,
            "committed_voters": [1, 2, 3],
            "committed_learners": [],
            "current_leader": 1,
        }
        return {
            "node_id": 2,
            "lifecycle": "active",
            "peers": [
                {
                    "node_id": 1,
                    "public_uri": "http://127.0.0.1:7101",
                    "peer_uri": "http://127.0.0.1:7201",
                }
            ],
            "groups": [
                {"group": "control", "group_id": 1, **group},
                {"group": "data", "group_id": 2, **group},
            ],
        }

    def test_exact_memberships_require_both_groups(self):
        diagnostics = self.diagnostics()
        observed = assert_exact_memberships(diagnostics, [1, 2, 3])
        self.assertEqual({"control", "data-2"}, set(observed["groups"]))
        diagnostics["groups"] = diagnostics["groups"][:1]
        with self.assertRaises(VerificationError):
            assert_exact_memberships(diagnostics, [1, 2, 3])

    def test_typed_leader_hint_matches_diagnostics_and_topology(self):
        value = {
            "error": {
                "code": "not_leader",
                "detail": {
                    "code": "not_leader",
                    "group": "data",
                    "leader": {
                        "node_id": 1,
                        "public_uri": "http://127.0.0.1:7101",
                    },
                },
            }
        }
        topology = {1: {"endpoint": "http://127.0.0.1:7101"}}
        self.assertEqual(
            1,
            assert_typed_leader_hint(
                value,
                1,
                self.diagnostics(),
                topology,
            )["node_id"],
        )
        value["error"]["detail"]["leader"]["public_uri"] = "http://127.0.0.1:9999"
        with self.assertRaises(VerificationError):
            assert_typed_leader_hint(
                value,
                1,
                self.diagnostics(),
                topology,
            )

    def test_catch_up_requires_a_distinct_target_and_exact_replication(self):
        leader = {
            "local_role": "leader",
            "last_log_index": 12,
            "local_committed_index": 12,
            "last_applied_index": 12,
            "replication": [{"target_node_id": 2, "matched_log_index": 12}],
        }
        follower = {
            "local_role": "follower",
            "current_leader": 1,
            "last_log_index": 12,
            "local_committed_index": 12,
            "last_applied_index": 12,
        }
        self.assertEqual(2, assert_caught_up(1, 2, leader, follower)["target_node_id"])
        with self.assertRaises(VerificationError):
            assert_caught_up(1, 1, leader, follower)
        promoted = {
            "local_role": "leader",
            "last_log_index": 12,
            "local_committed_index": 12,
            "cluster_committed_index": 12,
            "last_applied_index": 12,
            "replication": [
                {"target_node_id": 1, "matched_log_index": 12},
                {"target_node_id": 2, "matched_log_index": 12},
                {"target_node_id": 3, "matched_log_index": 12},
            ],
        }
        self.assertTrue(
            assert_caught_up(2, 2, promoted, promoted)["target_became_leader"]
        )
        leader["replication"][0]["matched_log_index"] = 11
        with self.assertRaises(VerificationError):
            assert_caught_up(1, 2, leader, follower)

    def test_source_paths_include_poc_and_verifier_sources(self):
        paths = {path.relative_to(ROOT).as_posix() for path in source_paths()}
        self.assertIn("scripts/verify.py", paths)
        self.assertIn("poc/common/Cargo.toml", paths)
        self.assertTrue(any(path.startswith("poc/") and path.endswith(".rs") for path in paths))
        self.assertTrue(any(path.startswith("poc/") and path.endswith(".py") for path in paths))

    def test_directed_proxy_passes_delays_and_drops_real_bytes(self):
        upstream_port = free_port()
        stop = threading.Event()
        ready = threading.Event()

        def echo_server():
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
                listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                listener.bind(("127.0.0.1", upstream_port))
                listener.listen(8)
                listener.settimeout(0.1)
                ready.set()
                while not stop.is_set():
                    try:
                        connection, _ = listener.accept()
                    except socket.timeout:
                        continue
                    except OSError:
                        return
                    threading.Thread(
                        target=echo_connection,
                        args=(connection,),
                        daemon=True,
                    ).start()

        def echo_connection(connection):
            with connection:
                try:
                    while True:
                        chunk = connection.recv(65536)
                        if not chunk:
                            return
                        connection.sendall(chunk)
                except OSError:
                    return

        server = threading.Thread(target=echo_server, daemon=True)
        server.start()
        self.assertTrue(ready.wait(timeout=5))
        proxy = DirectedTcpProxy(1, 2, free_port(), upstream_port)
        try:
            with socket.create_connection(("127.0.0.1", proxy.listen_port), timeout=2) as client:
                client.sendall(b"pass")
                self.assertEqual(b"pass", client.recv(4))

            proxy.set_mode("drop")
            with socket.create_connection(("127.0.0.1", proxy.listen_port), timeout=2) as client:
                client.settimeout(0.2)
                client.sendall(b"drop")
                with self.assertRaises(socket.timeout):
                    client.recv(4)

            proxy.set_mode("delay", 100)
            with socket.create_connection(("127.0.0.1", proxy.listen_port), timeout=2) as client:
                started = time.monotonic()
                client.sendall(b"delay")
                self.assertEqual(b"delay", client.recv(5))
                self.assertGreaterEqual(time.monotonic() - started, 0.18)

            counters = proxy.snapshot()["counters"]
            self.assertGreaterEqual(counters["dropped_bytes"], 4)
            self.assertGreaterEqual(counters["delayed_chunks"], 2)
        finally:
            proxy.stop()
            stop.set()
            server.join(timeout=2)
            self.assertFalse(server.is_alive())


def main():
    args = parse_args()
    artifacts = args.artifacts.resolve()
    profile = load_profile(args.profile)
    prepare_artifacts(artifacts)
    runner = CommandRunner(artifacts)
    started = utc_now()
    revision, fingerprints = source_fingerprint()
    manifest = {
        "artifacts": str(artifacts),
        "created_at": started,
        "environment": {
            "machine": platform.machine(),
            "platform": platform.platform(),
            "python": sys.version,
        },
        "independent_host": "BLOCKED",
        "independent_security_review": "NOT_REQUESTED",
        "phase": args.phase,
        "profile": profile,
        "publication": "BLOCKED_NO_GIT_REPOSITORY",
        "scenario": args.scenario,
        "security": args.security,
        "seed": args.seed,
        "source_revision": revision,
        "suite": args.suite,
    }
    write_json(artifacts / "manifest.json", manifest)
    write_json(artifacts / "source" / "fingerprints.json", fingerprints)
    status = "FAIL"
    error_message = None
    try:
        snapshot_source(artifacts, fingerprints)
        build_release(runner, revision)
        current_revision, _ = source_fingerprint()
        if current_revision != revision:
            raise VerificationError("source changed during the release build")
        binaries = release_binaries()
        write_json(
            artifacts / "binary-fingerprints.json",
            {
                name: {
                    "bytes": path.stat().st_size,
                    "path": str(path.relative_to(ROOT)),
                    "sha256": sha256_file(path),
                }
                for name, path in binaries.items()
            },
        )
        run_selected(args, artifacts, runner, binaries, revision, profile)
        record_security(artifacts, runner, binaries, args.security)
        status = "PASS"
    except (
        VerificationError,
        OSError,
        subprocess.SubprocessError,
        json.JSONDecodeError,
    ) as error:
        error_message = str(error)
        raise
    finally:
        cleanup_success_data(artifacts, status == "PASS")
        write_json(
            artifacts / "result.json",
            {
                "completed_at": utc_now(),
                "error": error_message,
                "scenario": args.scenario,
                "verdict": status,
            },
        )


if __name__ == "__main__":
    main()
