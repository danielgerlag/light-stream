# LS01 evidence

LS01 evidence is local functional evidence. It does not prove consensus, writes, secured transport, independent-host availability, or production capacity.

## Primary runs

- `full/` preserves the first failed full run.
- `full-verified/` contains the complete passing LS01 verifier run and all ten lane records.
- `skill-health/` contains the repository-skill health and capability run.
- `e2e-suite/` contains the passing `--suite e2e --security all` run.

## Program decisions

- `decisions.tsv` records implementation and verification decisions.
- `../program/authorization.json` records the waived LS01 operator review.
- `publication.json` records `BLOCKED_NO_GIT_REPOSITORY`.
- `independent-host.json` records `BLOCKED`.
- `security-review.json` records `NOT_REQUESTED`.
- `validation.json` records the final command results and the out-of-scope POC clippy blocker.

## Required checks

Inspect these files after the final run:

- `full/result.json`
- `full-verified/result.json`
- `full-verified/manifest.json`
- `full-verified/binary-fingerprints.json`
- `full-verified/cleanup.json`
- `full-verified/l01.json` through `full-verified/l10.json`
- `full-verified/security.json`
- `skill-health/result.json`
- `skill-health/cleanup.json`
- `e2e-suite/result.json`
- `validation.json`
