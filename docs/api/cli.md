# `light-streamctl` reference

Status: implemented through LS04.

`light-streamctl` writes one JSON object to standard output for each command. Diagnostics go to standard error. Every command requires `--endpoint`.

Use `--seed` more than once to add fallback public endpoints. `--deadline-ms` sets one absolute deadline for each public operation. The initial connection, fallback connections, retry delays, and RPCs all consume that deadline. `--no-retry` sends one request to the initial endpoint. The default deadline is 5000 milliseconds.

## Exit codes

| Code | Meaning |
| --- | --- |
| `0` | The operation completed. |
| `1` | The client could not connect, the RPC failed, or the response violated the protocol. |
| `2` | The endpoint or local input was invalid. |
| `3` | The LS01 `publish-probe` call returned `unsupported_operation`. |
| `4` | The server rejected an identity, bootstrap, receipt, stream, or bookmark request. |
| `5` | The cluster is forming, lacks a quorum, is not bootstrapped, is not leader, or durable storage failed. |

## `health`

```sh
light-streamctl --endpoint http://127.0.0.1:7101 health
```

The response includes `bootstrapped` and `cluster_id`. A pristine process is ready for health and bootstrap but rejects publish, fetch, and receipt calls.

## `cluster bootstrap`

```sh
light-streamctl \
  --endpoint http://127.0.0.1:7101 \
  cluster bootstrap \
  --cluster-id 018f3f7e-5b3b-7c11-98f7-b65ac15f6501 \
  --stream-id 018f3f7e-5b3b-7c11-98f7-b65ac15f6502 \
  --stream-name bootstrap
```

The command creates control group `1`, data group `2`, and partition `0` of the named stream. Repeating the same command returns the same result. Different identities return `bootstrap_conflict`.

Add `--seed-node-id` and exactly three `--member NODE_ID,PUBLIC_URI,PEER_URI` values to form an LS02b cluster. The contacted node must be the declared seed.

```sh
light-streamctl \
  --endpoint http://127.0.0.1:7101 \
  --deadline-ms 30000 \
  cluster bootstrap \
  --cluster-id 018f3f7e-5b3b-7c11-98f7-b65ac15f6501 \
  --stream-id 018f3f7e-5b3b-7c11-98f7-b65ac15f6502 \
  --stream-name bootstrap \
  --seed-node-id 1 \
  --member 1,http://127.0.0.1:7101,http://127.0.0.1:7201 \
  --member 2,http://127.0.0.1:7102,http://127.0.0.1:7202 \
  --member 3,http://127.0.0.1:7103,http://127.0.0.1:7203
```

## `publish`

```sh
light-streamctl \
  --endpoint http://127.0.0.1:7101 \
  publish \
  --cluster-id 018f3f7e-5b3b-7c11-98f7-b65ac15f6501 \
  --stream-id 018f3f7e-5b3b-7c11-98f7-b65ac15f6502 \
  --partition 0 \
  --principal local \
  --session 018f3f7e-5b3b-7c11-98f7-b65ac15f6503 \
  --sequence 1 \
  --file record.bin
```

Use one or more `--file` values for arbitrary byte records, or use `--payload` for one UTF-8 record. The success JSON contains the first offset and count. A matching retry returns the same range. A different payload with the same producer identity returns `receipt_conflict`.

Add `--bookmark NAME` to create an immutable bookmark after the committed batch. The receipt contains the bookmark ID and the exact next offset. The records, receipt, and bookmark commit in one state-machine write.

Use `--route-group-id` and `--route-revision` together to supply a cached route. A stale route is refreshed through the catalog before the same producer identity is retried.

## `stream`

Create a stream with a caller-supplied idempotency request ID:

```sh
light-streamctl --endpoint http://127.0.0.1:7101 stream create \
  --cluster-id 018f3f7e-5b3b-7c11-98f7-b65ac15f6501 \
  --request-id 018f3f7e-5b3b-7c11-98f7-b65ac15f6504 \
  --name orders \
  --partitions 4
```

Use `stream list`, `stream describe`, `stream route`, and `stream delete` for catalog and route operations. Stream names become visible only after activation. Reusing a deleted name creates a new immutable stream identity.

LS03 creates a bounded pool of data groups during cluster formation and maps partitions across that pool. It does not create one RocksDB database per stream.

## `fetch`

```sh
light-streamctl \
  --endpoint http://127.0.0.1:7101 \
  fetch \
  --cluster-id 018f3f7e-5b3b-7c11-98f7-b65ac15f6501 \
  --stream-id 018f3f7e-5b3b-7c11-98f7-b65ac15f6502 \
  --partition 0 \
  --offset 0 \
  --limit 128
```

The limit must be between `1` and `1024`. A page is also capped at 8 MiB. JSON encodes each payload as an array of byte values.

## `receipt`

```sh
light-streamctl \
  --endpoint http://127.0.0.1:7101 \
  receipt \
  --cluster-id 018f3f7e-5b3b-7c11-98f7-b65ac15f6501 \
  --stream-id 018f3f7e-5b3b-7c11-98f7-b65ac15f6502 \
  --partition 0 \
  --principal local \
  --session 018f3f7e-5b3b-7c11-98f7-b65ac15f6503 \
  --sequence 1
```

The command performs a linearizable read and returns the durable publish result.

## `bookmark`

Create a partition bookmark at any committed next offset:

```sh
light-streamctl --endpoint http://127.0.0.1:7101 bookmark create \
  --cluster-id 018f3f7e-5b3b-7c11-98f7-b65ac15f6501 \
  --stream-id 018f3f7e-5b3b-7c11-98f7-b65ac15f6502 \
  --partition 0 \
  --bookmark-id 018f3f7e-5b3b-7c11-98f7-b65ac15f6505 \
  --name import-boundary \
  --offset 42
```

Use `bookmark resolve`, `bookmark list`, and `bookmark delete` with the same cluster, stream, and partition. Listing is newest first by publication sequence. Pass the returned `publication_ceiling` and `next_before` as `--publication-ceiling` and `--before` to page through a stable snapshot while newer bookmarks are created.

Deletion is terminal for the old bookmark ID. Reusing its name requires a new ID.

Create a stream bookmark with one independent position for every partition:

```sh
light-streamctl --endpoint http://127.0.0.1:7101 bookmark stream-create \
  --cluster-id 018f3f7e-5b3b-7c11-98f7-b65ac15f6501 \
  --stream-id 018f3f7e-5b3b-7c11-98f7-b65ac15f6502 \
  --bookmark-id 018f3f7e-5b3b-7c11-98f7-b65ac15f6506 \
  --name batch-boundary \
  --position 0:42 \
  --position 1:17
```

Use `bookmark stream-resolve`, `bookmark stream-list`, and `bookmark stream-delete` for stream bookmark lifecycle. The server validates every position against its partition tail before committing the vector in the control group. The positions are independent. The API does not claim that they represent one cross-group consistent cut.

## `diagnostics`

```sh
light-streamctl --endpoint http://127.0.0.1:7101 diagnostics
```

The response is read-only. It reports the node lifecycle, durable peer descriptors, and control and data group state. Each group includes the local role, current leader, effective and committed memberships, log and commit positions, application position, leader replication progress, and snapshot and purge positions.

Followers return `not_leader` with a typed data-group leader hint. The normal client follows the hint within its remaining deadline. `--no-retry` returns the follower response unchanged.

## `publish-probe`

`publish-probe` is the LS01 compatibility call. It validates its request and returns `unsupported_operation`. It does not publish to the LS02a cluster.

## Security

`local-insecure` binds loopback by default. `secured` remains unavailable until LS08.
