import csv
import json
import tempfile
import unittest
from pathlib import Path

from run import (
    ROOT, check_acknowledged_prefix, check_bookmarks, check_quorum_rejection,
    combine, generated_checksum, rotate_left, summarize,
)


class EvidenceTests(unittest.TestCase):
    def test_workspace_root(self):
        self.assertTrue((ROOT / "poc" / "CONTRACT.md").is_file())
        self.assertTrue((ROOT / "Cargo.toml").is_file())

    def test_combined_digest_matches_sequential_observation(self):
        def observe(values, first):
            digest = 0
            for value in values:
                digest = rotate_left(digest, 7) ^ value
            return {
                "batches": len(values), "records": len(values) * 128,
                "payload_bytes": len(values) * 128 * 1024, "digest": digest,
                "first_sequence": first, "last_sequence": first + len(values) - 1,
            }

        values = [17, 1234567, (1 << 63) + 3, 31, 97, 133]
        self.assertEqual(combine(observe(values[:2], 0), observe(values[2:], 2)),
                         observe(values, 0))
        with self.assertRaises(AssertionError):
            combine(observe(values[:2], 0), observe(values[2:], 3))

    def test_bookmark_assertions_reject_missing_and_shifted_positions(self):
        audit = {"batches": 2, "last_sequence": 4}
        correct = [
            {"sequence": 4, "next_record": 640},
            {"sequence": 3, "next_record": 512},
        ]
        check_bookmarks(correct, audit, 128)
        with self.assertRaises(AssertionError):
            check_bookmarks(correct[:1], audit, 128)
        with self.assertRaises(AssertionError):
            check_bookmarks([{"sequence": 4, "next_record": 639}, correct[1]], audit, 128)

    def test_summary_keeps_range_and_trial_p99_definition(self):
        results = []
        for rate, latency in [(10, 100), (100, 200), (20, 900)]:
            results.append({
                "case": {"name": "r3-s1-b128"}, "engine": "segment",
                "peak_summed_node_rss_bytes": 1048576,
                "bench": {"records_per_second": rate, "mib_per_second": rate / 1024,
                          "latency_us": {"p99": latency}, "elapsed_seconds": 2,
                          "batches": 1024},
            })
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            summarize(results, path)
            with (path / "summary.csv").open() as handle:
                row = list(csv.DictReader(handle))[0]
            self.assertEqual(float(row["records_per_second_median"]), 20)
            self.assertEqual(float(row["records_per_second_min"]), 10)
            self.assertEqual(float(row["records_per_second_max"]), 100)
            self.assertEqual(float(row["trial_p99_us_median"]), 200)
            self.assertEqual(int(row["trials"]), 3)

    def test_quorum_gate_rejects_unrelated_errors_and_acknowledgements(self):
        valid = {
            "errors": 1, "error": "quorum unavailable",
            "acknowledged_per_shard": [{"batches": 0}],
        }
        check_quorum_rejection(1, json.dumps(valid))
        for code in (0, 2, 101):
            with self.assertRaises(AssertionError):
                check_quorum_rejection(code, json.dumps(valid))
        with self.assertRaises(AssertionError):
            check_quorum_rejection(1, json.dumps({**valid, "error": "invalid CLI argument"}))
        with self.assertRaises(AssertionError):
            check_quorum_rejection(1, json.dumps({
                **valid, "acknowledged_per_shard": [{"batches": 1}],
            }))

    def test_extra_tail_does_not_bypass_prefix_digest(self):
        prefix = {
            "batches": 1, "records": 1, "payload_bytes": 17,
            "digest": generated_checksum(0, 0, 1, 17),
            "first_sequence": 0, "last_sequence": 0,
        }
        actual = {
            "batches": 2, "records": 2, "payload_bytes": 34,
            "digest": (rotate_left(prefix["digest"], 7)
                       ^ generated_checksum(0, 1, 1, 17) ^ 0x9e3779b97f4a7c15),
            "first_sequence": 0, "last_sequence": 1,
        }
        check_acknowledged_prefix(actual, prefix, 0, 1, 17)
        with self.assertRaises(AssertionError):
            check_acknowledged_prefix(actual, {**prefix, "digest": prefix["digest"] ^ 1},
                                      0, 1, 17)


if __name__ == "__main__":
    unittest.main()
