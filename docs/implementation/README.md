# Implementation handoff

This is a plan for a coding agent. It does not start implementation.

| Document | Use |
| --- | --- |
| [Agent task](agent-task.md) | Copy this into the coding agent's task. |
| [Implementation plan](plan.md) | Execute the ten dependent work packages and their completion gates. |
| [End-to-end verification](verification.md) | Build the black-box runner, evidence oracle, fault scenarios, and capacity gates. |
| [Security contract](security.md) | Implement configurable security and optionally run the independent security review. |

The implementation baseline is Rust, RocksDB, an established Raft library, independently replicated shard groups, and a native versioned API.
The POCs remain experimental controls, not production consensus code.

Security has two independent choices.
The planned broker supports insecure local development and secured deployments.
Functional verification of both modes is mandatory.
An independent security review is optional and must be recorded as not requested when disabled.

The operator reviews and merges changes.
The agent must not deploy, provision paid infrastructure, or weaken verification to produce a green result.

[Plan structure output](plan-check.txt) records the checklist check only.
It is not implementation or runtime verification.
