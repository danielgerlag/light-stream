# Light Stream design discussion

Updated on 2026-09-10 after executable POC experiments. A production broker is not implemented.

The target is now high-volume usage. The current direction is a bookmark-first Rust broker with batched, independently replicated writer groups. RocksDB and Fjall are the storage finalists. The original installation-wide redb recommendation is historical.

| Document | Contents |
| --- | --- |
| [Proposal](proposal.md) | Product direction, bookmarks, retention, HA, wire compatibility, differentiators, and first-release scope. |
| [Measured POC evidence](../experiments/high-volume.md) | Throughput, latency, memory, multi-node configurations, the sustained failure, and raw evidence paths. |
| [Coding-agent implementation plan](../implementation/README.md) | Ordered work packages, end-to-end verification, configurable security, and optional independent review. |
| [Initial proposal](initial-proposal.md) | The earlier small-installation design, preserved for comparison. |
| [Storage arena](storage-arena.md) | Competing proposals, scores, disagreement, selection rationale, and rejected alternatives. |
| [Sources](sources.md) | Kafka and competitor baselines, storage documentation, image-size observations, and evidence limits. |
| [Evaluation](evaluation.md) | Correctness obligations and future performance comparisons. |

## Decisions still open

- A native bookmark workflow versus unchanged Kafka clients in the first release.
- RocksDB versus Fjall after independent-host and sustained-load evidence.
- Protection leases in the first release versus explicitly abortable replay when retention catches up.
- Concrete workload, hardware, recovery, and resource budgets.

The [Rust POCs](../../poc/README.md) and raw results are available. Their same-host fixed-leader measurements do not prove production HA or independent-host scaling.
