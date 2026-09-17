import io
import tempfile
import tarfile
import unittest
from dataclasses import asdict
from pathlib import Path

from scripts import package_release


class PackageReleaseTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self):
        self.temporary.cleanup()

    def manifest(self):
        target = package_release.native_target()
        library = (
            "/usr/lib/libSystem.B.dylib"
            if target.endswith("-apple-darwin")
            else "/lib/libc.so.6"
        )
        return package_release.ReleaseManifest(
            package_format=1,
            package_version="0.1.0",
            source_revision="a" * 40,
            source_date_epoch=123,
            target=target,
            cargo_lock_sha256="b" * 64,
            public_api="lightstream.v1",
            peer_protocol=1,
            peer_codec=1,
            storage_format=1,
            record_schema=2,
            standalone_manifest=1,
            replicated_manifest_read=(3, 4, 5),
            replicated_manifest_write=5,
            security_config=1,
            binaries=(
                package_release.BinaryManifest(
                    "bin/light-streamd", 6, "c" * 64, (library,)
                ),
                package_release.BinaryManifest(
                    "bin/light-streamctl", 6, "d" * 64, (library,)
                ),
            ),
        )

    def payloads(self):
        manifest = self.manifest()
        payloads = {
            "bin/light-streamd": b"server",
            "bin/light-streamctl": b"client",
            "release.json": manifest.to_bytes(),
        }
        payloads["SHA256SUMS"] = "".join(
            f"{package_release.sha256_bytes(payloads[name])}  {name}\n"
            for name in sorted(payloads)
        ).encode()
        return manifest, payloads

    def test_manifest_round_trips_and_rejects_unknown_fields(self):
        manifest = self.manifest()
        self.assertEqual(
            package_release.ReleaseManifest.from_bytes(manifest.to_bytes()), manifest
        )
        value = asdict(manifest)
        value["extra"] = True
        with self.assertRaises(package_release.PackageError):
            package_release.ReleaseManifest.from_bytes(
                package_release.canonical_json(value)
            )

    def test_manifest_rejects_boolean_integers_and_bad_digests(self):
        value = asdict(self.manifest())
        value["package_format"] = True
        with self.assertRaises(package_release.PackageError):
            package_release.ReleaseManifest.from_bytes(
                package_release.canonical_json(value)
            )
        value = asdict(self.manifest())
        value["binaries"][0]["dynamic_libraries"] = "not-an-array"
        with self.assertRaises(package_release.PackageError):
            package_release.ReleaseManifest.from_bytes(
                package_release.canonical_json(value)
            )
        value = asdict(self.manifest())
        value["binaries"][0]["extra"] = True
        with self.assertRaises(package_release.PackageError):
            package_release.ReleaseManifest.from_bytes(
                package_release.canonical_json(value)
            )
        value = asdict(self.manifest())
        value["cargo_lock_sha256"] = "not-a-digest"
        with self.assertRaises(package_release.PackageError):
            package_release.ReleaseManifest.from_bytes(
                package_release.canonical_json(value)
            )

    def test_archive_is_deterministic_and_normalized(self):
        _, payloads = self.payloads()
        first = package_release.normalized_archive("release", 123, payloads)
        second = package_release.normalized_archive("release", 123, payloads)
        self.assertEqual(first, second)
        with tarfile.open(fileobj=io.BytesIO(first), mode="r:gz") as archive:
            members = archive.getmembers()
        self.assertTrue(all(member.uid == 0 and member.gid == 0 for member in members))
        self.assertTrue(all(member.mtime == 123 for member in members))

    def test_extract_rejects_path_traversal(self):
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
            info = tarfile.TarInfo("../escape")
            info.size = 1
            archive.addfile(info, io.BytesIO(b"x"))
        path = self.root / "bad.tar.gz"
        path.write_bytes(buffer.getvalue())
        with self.assertRaises(package_release.PackageError):
            package_release.extract_release(path, self.root / "out")

    def test_extract_checks_release_digest(self):
        manifest, payloads = self.payloads()
        payloads["bin/light-streamd"] = b"changed"
        path = self.root / "bad.tar.gz"
        path.write_bytes(
            package_release.normalized_archive(
                f"light-stream-{manifest.package_version}-{manifest.target}",
                manifest.source_date_epoch,
                payloads,
            )
        )
        with self.assertRaises(package_release.PackageError):
            package_release.extract_release(path, self.root / "out")

    def test_secret_scanner_rejects_private_keys_and_supplied_values(self):
        with self.assertRaises(package_release.PackageError):
            package_release.scan_secrets(b"-----BEGIN PRIVATE KEY-----")
        with self.assertRaises(package_release.PackageError):
            package_release.scan_secrets(b"prefix-secret", (b"secret",))

    def test_existing_output_is_refused_before_build(self):
        output = self.root / "dist"
        output.mkdir()
        with self.assertRaises(package_release.PackageError):
            package_release.create_release(
                output,
                package_release.native_target(),
                "a" * 40,
                123,
            )


if __name__ == "__main__":
    unittest.main()
