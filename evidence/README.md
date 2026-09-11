# High-volume POC evidence

The [experiment contract](../poc/CONTRACT.md) defines the workload and correctness obligations.
The [POC guide](../poc/README.md) contains rerun commands and limits.
The [decision trail](decisions.tsv) records actual choices and results as the work proceeds.

The [measured report](../docs/experiments/high-volume.md) is the main entry point.
It links the main matrix, shard scaling, message-size comparison, sustained attempts, and controlled slow-peer experiment.
The [independent review](review.md) records accepted findings and remaining limits.

Runtime evidence directories contain the exact environment, binary and source fingerprints, randomized schedule, raw latencies, process logs, reopened-data audits, and summaries.
An experiment is not successful merely because its process exits or a live counter advances.
The orchestrator requires acknowledged data to match bytes reopened from every healthy replica.

Three-process tests use one physical host and filesystem.
Their fixed-leader protocol measures durable replication cost, not Raft elections or production HA.
Performance statements must identify the evidence directory and workload that support them.
