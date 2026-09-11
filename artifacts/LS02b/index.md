# LS02b artifacts

## Design and decisions

- `design/synthesis.md` records the selected three-voter design.
- `decisions.tsv` records implementation and verification decisions.

## Verification

- `run-20260910-1` is a retained failed run that exposed an ambiguous no-quorum tail interacting with the main producer session.
- `run-20260910-2` is the first passing three-voter run.
- `run-20260910-3` is a retained failed run that exposed the client's separate routed connection to the response-drop proxy.
- `run-20260910-4` is the passing run with a real TCP response drop.

Each run preserves its source snapshot, binary fingerprints, commands, logs, diagnostics, faults, ledgers, and result. Successful runs remove only disposable node stores. Failed runs retain their node stores.

LS02b does not claim snapshot catch-up after purge, secured mode, LS03 features, or independent-host behavior.
