# Aborted run

The first trial failed the recent-bookmark count assertion.
The executable still returned a 1,024-marker limit, while the completed runner source and contract specified 100.
The release build had preceded that final worker change.

The orchestrator correctly stopped. No trial from this directory enters the final ranking.
The release executable was rebuilt after handoff.
The orchestrator now builds before each run by default and records the build command.
