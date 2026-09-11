# Independent evidence review

Reviewed by GPT-6 Astra in a fresh context, using the same model family as the implementation lead.
The reviewer ran no workloads and changed no files.

## Baseline verdict

The reviewer independently reconciled all 72 `main-v2` raw benchmark files with their result files.
It recomputed throughput, nearest-rank percentiles, and all 24 summary rows.
No discrepancy invalidated the limited, normalized, same-host fixed-leader experiment.

The reviewer also inspected persisted leader and follower audits for a replicated Fjall case and a standalone RocksDB case.
Their expected client digests, actual reopened data, retention boundaries, and absolute marker positions agreed.

This verdict does not certify sustained reliability, independent-host scaling, or production HA.

## Findings and disposition

| Finding | Disposition |
| --- | --- |
| Blocking enqueue to every follower can throttle a healthy majority when a minority queue fills. | Accepted. Six controlled block-versus-isolate trials preserve the baseline control and verify the alternative. Production still requires replication catch-up. |
| A failure gate accepted any nonzero command exit. | Fixed. It now requires the expected error exit, an explicit quorum error, and zero acknowledged batches. |
| An extra recovered tail bypassed exact prefix-digest equality. | Fixed. The gate extends the acknowledged digest with independently generated known tail bytes and compares the complete recovered audit. |
| Failed failure-matrix data was deleted. | Fixed for future runs. Both normal and failure matrices retain failed scratch data; normal failed trials also save memory samples. |
| Older orchestrator source was represented only by hashes. | Historical limitation retained. Rust sources, Cargo inputs, and full-sync configuration were recovered exactly in `source-checkpoint`; newer runs snapshot their sources before execution. |
| Test-count claims linked to smoke outcomes rather than saved test output. | Test output is now captured under `evidence/test-*.log`; the earlier console-only execution is not rewritten as a historical file. |
| Sustained Fjall failure could be hidden by successful-only aggregation. | Kept visible. The report counts two successful attempts and one failure, separately from successful-run rates. |

The [stronger failure-gate recheck](failure-gate-recheck.json) confirms that all four historical failure scenarios satisfy the corrected assertions.
The [repeated failure scenarios](failures-v2/run.json) also pass the stronger gates against freshly reopened stores.
The [final smoke run](smoke-final/run.json) covers all four engines in standalone and three-process configurations after the isolation change.

## Unresolved evidence limits

The original sustained failure has no retained stores or memory samples.
Its acknowledged-prefix survival was not established by an offline audit after that failure.
The logs prove timeout-driven loss of the acknowledgement quorum, not a specific Fjall or host root cause.

The experiment is closed-loop, uses shared hardware and warm caches, and measures retention after ingest.
The baseline replication protocol backpressures all peer queues.
The new isolation POC does not replace that protocol with Raft or implement catch-up.

## Comment review

A separate read-only comment reviewer identified one redundant segment-repair comment.
That comment was removed after preserving the baseline source.
No suppressions, restored comments, or application behavior changes were required by that review.

## Final controlled-experiment review

The same independent reviewer reconciled all six slow-peer result files with their raw benchmark outputs.
Each acknowledged the same 24 batches.
Median completion was 4.92004275 seconds for blocking admission and 0.244844334 seconds for isolation, a 20.09x difference in this injected-delay scenario.

It also inspected an isolate case's explicit zero-ack quorum rejection after the fast follower stopped.
The fast follower retained the acknowledged 24 batches.
The leader retained those batches plus one unacknowledged tail batch.
The slow follower retained only five batches and is not represented as caught up.

The final verdict is VERIFIED for the controlled experiment and the tested fixed-leader safety boundary.
It is not a general throughput multiplier, production HA proof, or an explanation of the original sustained Fjall failure.
All final trail paths resolve, including four freshly repeated failure scenarios, eight final smoke cases, 34 Rust tests, and six Python tests.

## Attention

Reviewed by GPT-6 Astra, using the same model family as the implementation lead.

- Permanent peer exclusion is not replica catch-up or continued tolerance of another replica loss.
- The same-host, closed-loop experiments do not establish independent-host capacity or Raft behavior.
- The intermittent sustained Fjall failure remains unresolved, and its original stores and memory samples are unavailable.
- Historical Rust and Cargo inputs were recovered exactly, but the earlier Python orchestrator versions were not.
