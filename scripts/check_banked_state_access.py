#!/usr/bin/env python3

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "crates/light-stream-storage/src/lib.rs"
ALLOWED_RAW_MARKERS = {
    "fn raw_cf(",
    "fn migrate_state_banks(",
    "fn clear_legacy_state(",
    "fn clear_state_bank(",
    "fn install_snapshot(",
    "fn legacy_state_bank_migration_converts_payload_ownership_before_publication(",
}


def main() -> int:
    text = SOURCE.read_text()
    lines = text.splitlines()
    violations = []
    current_function = ""
    for number, line in enumerate(lines, start=1):
        stripped = line.strip()
        if stripped.startswith("fn ") or stripped.startswith("async fn "):
            current_function = stripped
        if "cf_handle(CF_STATE" in line or "raw_cf(CF_STATE" in line:
            if not any(marker in current_function for marker in ALLOWED_RAW_MARKERS):
                violations.append(f"{SOURCE.relative_to(ROOT)}:{number}: {stripped}")
    if violations:
        print("raw applied-state access bypasses the bank router:")
        print("\n".join(violations))
        return 1
    print("banked state access check passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
