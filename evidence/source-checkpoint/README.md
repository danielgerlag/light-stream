# Source checkpoint

`source/` preserves the workspace source at the checkpoint.
`main-source-comparison.json` compares it with the fingerprints recorded by `main-v2`.

All recorded Rust files, Cargo manifests, the lockfile, and the synchronization configuration match the main experiment.
The Python orchestrator and its tests had already evolved, so their historical versions are not recovered by this checkpoint.
The raw per-trial command files and Rust benchmark sources remain available for reproduction.

Later runs save their own source snapshots before execution.
