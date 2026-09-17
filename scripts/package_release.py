#!/usr/bin/env python3

import argparse
import base64
import gzip
import hashlib
import io
import json
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path, PurePosixPath


ROOT = Path(__file__).resolve().parents[1]
BINARIES = ("light-streamd", "light-streamctl")
FILES = ("bin/light-streamctl", "bin/light-streamd", "release.json", "SHA256SUMS")
SECRET_MARKERS = (
    b"-----BEGIN PRIVATE KEY-----",
    b"-----BEGIN RSA PRIVATE KEY-----",
    b"-----BEGIN EC PRIVATE KEY-----",
    b"-----BEGIN OPENSSH PRIVATE KEY-----",
)


class PackageError(RuntimeError):
    pass


@dataclass(frozen=True)
class BinaryManifest:
    path: str
    bytes: int
    sha256: str
    dynamic_libraries: tuple[str, ...]


@dataclass(frozen=True)
class ReleaseManifest:
    package_format: int
    package_version: str
    source_revision: str
    source_date_epoch: int
    target: str
    cargo_lock_sha256: str
    public_api: str
    peer_protocol: int
    peer_codec: int
    storage_format: int
    record_schema: int
    standalone_manifest: int
    replicated_manifest_read: tuple[int, ...]
    replicated_manifest_write: int
    security_config: int
    binaries: tuple[BinaryManifest, ...]

    def to_bytes(self):
        return canonical_json(asdict(self))

    @classmethod
    def from_bytes(cls, payload):
        try:
            value = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise PackageError("release.json is invalid JSON") from error
        if canonical_json(value) != payload:
            raise PackageError("release.json is not canonical")
        if not isinstance(value, dict) or set(value) != set(cls.__dataclass_fields__):
            raise PackageError("release.json fields are invalid")
        try:
            if not isinstance(value["binaries"], list):
                raise PackageError("release binary list is invalid")
            for item in value["binaries"]:
                if not isinstance(item, dict) or set(item) != {
                    "path",
                    "bytes",
                    "sha256",
                    "dynamic_libraries",
                }:
                    raise PackageError("release binary fields are invalid")
                if not isinstance(item["dynamic_libraries"], list):
                    raise PackageError("dynamic library list is invalid")
            value["binaries"] = tuple(
                BinaryManifest(
                    path=item["path"],
                    bytes=item["bytes"],
                    sha256=item["sha256"],
                    dynamic_libraries=tuple(item["dynamic_libraries"]),
                )
                for item in value["binaries"]
            )
            value["replicated_manifest_read"] = tuple(
                value["replicated_manifest_read"]
            )
            manifest = cls(**value)
        except (KeyError, TypeError) as error:
            raise PackageError("release.json values are invalid") from error
        manifest.validate()
        return manifest

    def validate(self):
        integer_fields = (
            self.package_format,
            self.source_date_epoch,
            self.peer_protocol,
            self.peer_codec,
            self.storage_format,
            self.record_schema,
            self.standalone_manifest,
            self.replicated_manifest_write,
            self.security_config,
        )
        if any(type(value) is not int for value in integer_fields):
            raise PackageError("release integer fields are invalid")
        if any(
            type(value) is not str
            for value in (
                self.package_version,
                self.source_revision,
                self.target,
                self.cargo_lock_sha256,
                self.public_api,
            )
        ):
            raise PackageError("release string fields are invalid")
        if self.package_format != 1:
            raise PackageError("unsupported package format")
        if len(self.source_revision) not in (40, 64) or any(
            value not in "0123456789abcdef" for value in self.source_revision
        ):
            raise PackageError("source revision must be lowercase hexadecimal")
        if self.source_date_epoch < 0:
            raise PackageError("SOURCE_DATE_EPOCH must be non-negative")
        if len(self.cargo_lock_sha256) != 64 or any(
            value not in "0123456789abcdef" for value in self.cargo_lock_sha256
        ):
            raise PackageError("Cargo.lock digest is invalid")
        if any(type(value) is not int for value in self.replicated_manifest_read):
            raise PackageError("replicated manifest versions are invalid")
        versions = (
            self.public_api,
            self.peer_protocol,
            self.peer_codec,
            self.storage_format,
            self.record_schema,
            self.standalone_manifest,
            self.replicated_manifest_read,
            self.replicated_manifest_write,
            self.security_config,
        )
        if versions != ("lightstream.v1", 1, 1, 1, 2, 1, (3, 4, 5), 5, 1):
            raise PackageError("release compatibility versions are invalid")
        expected = tuple(f"bin/{name}" for name in BINARIES)
        if tuple(item.path for item in self.binaries) != expected:
            raise PackageError("release binary inventory is invalid")
        for item in self.binaries:
            if (
                type(item.path) is not str
                or type(item.bytes) is not int
                or item.bytes <= 0
                or type(item.sha256) is not str
                or len(item.sha256) != 64
                or any(value not in "0123456789abcdef" for value in item.sha256)
                or any(type(value) is not str for value in item.dynamic_libraries)
            ):
                raise PackageError("release binary metadata is invalid")
            if tuple(sorted(set(item.dynamic_libraries))) != item.dynamic_libraries:
                raise PackageError("dynamic library inventory is not canonical")


def canonical_json(value):
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
        + b"\n"
    )


def sha256_bytes(payload):
    return hashlib.sha256(payload).hexdigest()


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def scan_secrets(payload, forbidden=()):
    raw = tuple(marker for marker in (*SECRET_MARKERS, *forbidden) if marker)
    encoded = tuple(base64.b64encode(marker) for marker in raw)
    url_encoded = tuple(base64.urlsafe_b64encode(marker) for marker in raw)
    if any(marker in payload for marker in (*raw, *encoded, *url_encoded)):
        raise PackageError("package contains secret or build-path material")


def native_target(root=ROOT):
    result = subprocess.run(
        ["rustc", "-vV"], cwd=root, capture_output=True, text=True, check=True
    )
    for line in result.stdout.splitlines():
        if line.startswith("host: "):
            return line[6:]
    raise PackageError("rustc did not report a host target")


def build_environment(root, work, revision, epoch):
    environment = {
        key: value
        for key, value in os.environ.items()
        if key
        in {
            "PATH",
            "HOME",
            "CARGO_HOME",
            "RUSTUP_HOME",
            "TMPDIR",
            "SSL_CERT_FILE",
            "SSL_CERT_DIR",
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "ALL_PROXY",
            "NO_PROXY",
            "http_proxy",
            "https_proxy",
            "all_proxy",
            "no_proxy",
        }
    }
    remaps = (
        f"--remap-path-prefix={root}=/usr/src/light-stream "
        f"--remap-path-prefix={work}=/usr/src/build"
    )
    environment.update(
        {
            "CARGO_INCREMENTAL": "0",
            "CARGO_TARGET_DIR": str(work / "target"),
            "LIGHT_STREAM_BUILD_REVISION": revision,
            "SOURCE_DATE_EPOCH": str(epoch),
            "LC_ALL": "C",
            "TZ": "UTC",
            "RUSTFLAGS": f"-C debuginfo=0 -C strip=symbols {remaps}",
        }
    )
    return environment


def dynamic_libraries(binary, target, environment):
    if target.endswith("-apple-darwin"):
        result = subprocess.run(
            ["otool", "-L", str(binary)],
            cwd=binary.parent,
            env=environment,
            capture_output=True,
            text=True,
            check=True,
        )
        libraries = tuple(
            sorted(
                {
                    line.strip().split(" (compatibility version", 1)[0]
                    for line in result.stdout.splitlines()[1:]
                    if line.strip()
                }
            )
        )
        allowed = ("/usr/lib/", "/System/Library/")
    elif target.endswith("-unknown-linux-gnu"):
        result = subprocess.run(
            ["ldd", str(binary)],
            cwd=binary.parent,
            env=environment,
            capture_output=True,
            text=True,
            check=True,
        )
        paths = []
        for line in result.stdout.splitlines():
            fields = line.strip().split()
            if "=>" in fields:
                paths.append(fields[fields.index("=>") + 1])
            elif fields and fields[0].startswith("/"):
                paths.append(fields[0])
        if "not" in paths:
            raise PackageError("ldd reported an unresolved library")
        libraries = tuple(sorted(set(paths)))
        allowed = ("/lib/", "/lib64/", "/usr/lib/", "/usr/lib64/")
    else:
        raise PackageError(f"unsupported package target {target}")
    if any(not path.startswith(allowed) for path in libraries):
        raise PackageError(f"{binary.name} uses a library outside system roots")
    return libraries


def normalized_archive(root_name, epoch, payloads):
    tar_payload = io.BytesIO()
    with tarfile.open(
        fileobj=tar_payload, mode="w", format=tarfile.USTAR_FORMAT
    ) as archive:
        for name in (root_name, f"{root_name}/bin"):
            info = tarfile.TarInfo(name)
            info.type = tarfile.DIRTYPE
            info.mode = 0o755
            info.mtime = epoch
            archive.addfile(info)
        for relative in FILES:
            payload = payloads[relative]
            info = tarfile.TarInfo(f"{root_name}/{relative}")
            info.size = len(payload)
            info.mode = 0o755 if relative.startswith("bin/") else 0o644
            info.mtime = epoch
            archive.addfile(info, io.BytesIO(payload))
    result = io.BytesIO()
    with gzip.GzipFile(fileobj=result, mode="wb", filename="", mtime=epoch) as output:
        output.write(tar_payload.getvalue())
    return result.getvalue()


def workspace_version(root):
    active = False
    for line in (root / "Cargo.toml").read_text().splitlines():
        stripped = line.strip()
        if stripped.startswith("["):
            active = stripped == "[workspace.package]"
        elif active and stripped.startswith("version = "):
            return stripped.split('"')[1]
    raise PackageError("workspace package version is missing")


def create_release(output, target, revision, epoch, root=ROOT, forbidden=()):
    revision = revision.lower()
    if output.exists():
        raise PackageError("output directory already exists")
    if target != native_target(root):
        raise PackageError("archive packaging supports only the native target")
    if len(revision) not in (40, 64) or any(
        value not in "0123456789abcdef" for value in revision
    ):
        raise PackageError("source revision must be hexadecimal")
    output.parent.mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix=".ls-package-", dir=output.parent))
    try:
        environment = build_environment(root, work, revision, epoch)
        command = [
            "cargo",
            "build",
            "--locked",
            "--release",
            "--target",
            target,
            "-p",
            "light-stream-server",
            "-p",
            "light-stream-cli",
        ]
        if subprocess.run(command, cwd=root, env=environment).returncode != 0:
            raise PackageError("release build failed")
        payloads = {}
        binaries = []
        release = work / "target" / target / "release"
        for name in BINARIES:
            path = release / name
            if not path.is_file():
                raise PackageError(f"release build omitted {name}")
            payload = path.read_bytes()
            for label, value in (
                ("source", str(root).encode()),
                ("temporary build", str(work).encode()),
            ):
                if value in payload:
                    raise PackageError(f"{name} contains an absolute {label} path")
            scan_secrets(payload, forbidden)
            relative = f"bin/{name}"
            payloads[relative] = payload
            binaries.append(
                BinaryManifest(
                    path=relative,
                    bytes=len(payload),
                    sha256=sha256_bytes(payload),
                    dynamic_libraries=dynamic_libraries(path, target, environment),
                )
            )
        version = workspace_version(root)
        manifest = ReleaseManifest(
            package_format=1,
            package_version=version,
            source_revision=revision,
            source_date_epoch=epoch,
            target=target,
            cargo_lock_sha256=sha256_file(root / "Cargo.lock"),
            public_api="lightstream.v1",
            peer_protocol=1,
            peer_codec=1,
            storage_format=1,
            record_schema=2,
            standalone_manifest=1,
            replicated_manifest_read=(3, 4, 5),
            replicated_manifest_write=5,
            security_config=1,
            binaries=tuple(binaries),
        )
        manifest.validate()
        payloads["release.json"] = manifest.to_bytes()
        payloads["SHA256SUMS"] = "".join(
            f"{sha256_bytes(payloads[name])}  {name}\n"
            for name in sorted(payloads)
        ).encode()
        for payload in payloads.values():
            scan_secrets(
                payload,
                (*forbidden, str(root).encode(), str(work).encode()),
            )
        root_name = f"light-stream-{version}-{target}"
        output.mkdir(mode=0o755)
        archive = output / f"{root_name}.tar.gz"
        sidecar = output / f"{root_name}.release.json"
        archive.write_bytes(normalized_archive(root_name, epoch, payloads))
        sidecar.write_bytes(payloads["release.json"])
        return archive, sidecar, manifest
    finally:
        shutil.rmtree(work, ignore_errors=True)


def parse_sums(payload):
    result = {}
    for line in payload.decode().splitlines():
        digest, separator, name = line.partition("  ")
        if not separator or len(digest) != 64 or name not in FILES:
            raise PackageError("SHA256SUMS is invalid")
        result[name] = digest
    if set(result) != set(FILES) - {"SHA256SUMS"}:
        raise PackageError("SHA256SUMS inventory is invalid")
    return result


def extract_release(archive, destination, expected_sha256=None):
    if destination.exists():
        raise PackageError("extraction destination already exists")
    payload = archive.read_bytes()
    if expected_sha256 and sha256_bytes(payload) != expected_sha256:
        raise PackageError("archive digest does not match")
    with tarfile.open(fileobj=io.BytesIO(payload), mode="r:gz") as source:
        members = source.getmembers()
        roots = {PurePosixPath(member.name).parts[0] for member in members}
        if len(roots) != 1 or any(
            member.issym()
            or member.islnk()
            or PurePosixPath(member.name).is_absolute()
            or ".." in PurePosixPath(member.name).parts
            for member in members
        ):
            raise PackageError("archive contains an unsafe member")
        root_name = roots.pop()
        expected = {
            root_name,
            f"{root_name}/bin",
            *(f"{root_name}/{name}" for name in FILES),
        }
        if {member.name for member in members} != expected:
            raise PackageError("archive inventory is invalid")
        staging = Path(tempfile.mkdtemp(prefix=".ls-extract-", dir=destination.parent))
        try:
            root = staging / root_name
            (root / "bin").mkdir(parents=True)
            for relative in FILES:
                extracted = source.extractfile(f"{root_name}/{relative}")
                if extracted is None:
                    raise PackageError(f"archive omitted {relative}")
                path = root / relative
                path.write_bytes(extracted.read())
                path.chmod(0o755 if relative.startswith("bin/") else 0o644)
            manifest = ReleaseManifest.from_bytes((root / "release.json").read_bytes())
            if root_name != f"light-stream-{manifest.package_version}-{manifest.target}":
                raise PackageError("archive root does not match release.json")
            sums = parse_sums((root / "SHA256SUMS").read_bytes())
            for item in manifest.binaries:
                path = root / item.path
                if path.stat().st_size != item.bytes or sha256_file(path) != item.sha256:
                    raise PackageError(f"{item.path} does not match release.json")
            for name, digest in sums.items():
                if sha256_file(root / name) != digest:
                    raise PackageError(f"{name} does not match SHA256SUMS")
            staging.rename(destination)
            return destination / root_name, manifest
        except BaseException:
            shutil.rmtree(staging, ignore_errors=True)
            raise


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--target", default=native_target())
    parser.add_argument("--revision", required=True)
    parser.add_argument("--source-date-epoch", required=True, type=int)
    args = parser.parse_args()
    try:
        archive, sidecar, manifest = create_release(
            args.output,
            args.target,
            args.revision,
            args.source_date_epoch,
        )
    except (OSError, subprocess.SubprocessError, PackageError) as error:
        print(json.dumps({"error": str(error), "ok": False}, sort_keys=True))
        return 1
    print(
        json.dumps(
            {
                "archive": str(archive),
                "archive_bytes": archive.stat().st_size,
                "archive_sha256": sha256_file(archive),
                "manifest": str(sidecar),
                "ok": True,
                "release": asdict(manifest),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
