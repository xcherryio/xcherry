# server-zd

server-zd is a Dex Server written in Rust whose only runtime dependency is object storage (S3 or
an S3-compatible store). The local disk is only a cache: every acknowledged write is durable in
object storage, and a node rebuilds its local state from snapshots and the command log.

It serves the same `protos/dex.proto` FlowService and pull-mode WorkerTaskService as the Go Server,
so the Dex SDKs work without changes. It differs from the Go Server in a few documented ways (for
example, it supports only pull Workers and has no Continue-As-New).

## Layout

| Crate | Role |
| --- | --- |
| `dexzd-protocol` | Generated protobuf and gRPC code for the public protocol and internal formats |
| `dexzd-keyspace` | Identifiers, partition hashing, key encodings, value references |
| `dexzd-clock` | Wall-clock abstraction for real and simulated time |
| `dexzd-object-store` | Object storage interface, S3 implementation, fault-injecting test implementation |
| `dexzd-visibility-query` | Visibility query parser and type checker |
| `dexzd-log` | Command log batches and the conditional-write commit protocol |
| `dexzd-cluster-metadata` | Cluster object, partition manifest, node leases |
| `dexzd-apply` | Deterministic state machine that applies commands |
| `dexzd-stream-store` | In-memory Stream store |
| `dexzd-storage` | Local RocksDB state store and snapshots |
| `dexzd-worker-dispatch` | Pull Worker protocol server |
| `dexzd-search` | SearchFlows backends |
| `dexzd-engine` | Commit pipeline, Flow runtime, effects, recovery |
| `dexzd-api` | FlowService gRPC handlers |
| `dexzd-server` | The `dex-server-zd` executable |
| `dexzd-simulation` | Deterministic simulation tests |
| `dexzd-e2e-tests` | End-to-end tests over the real gRPC protocol |

Crate layers are registered in `script/rust-crate-layers.json`; a crate may depend only on crates
in the same or a lower layer.

## Development

```bash
cd server-zd
cargo test -p <crate>        # iterate on one crate
make fmt-check clippy test   # what CI runs
make build-server            # builds target/release/dex-server-zd
```

Local tests use only the in-process s3mock (`../s3mock`) or the in-memory object store. They need
no docker, MinIO, or Temporal.

## Rules specific to server-zd

- `dexzd-apply` must be deterministic. It never reads clocks, random numbers, configuration, the
  environment, files, or the network, and it never iterates a hash map. Its `clippy.toml` bans the
  common offenders.
- A change to the result of applying an already committed command needs a new behavior version
  with a named predicate.
- Persistent formats (`proto/dexzd/`, key layout, object storage layout, snapshots) change only
  additively, following the same rules as `protos/dex.proto`.
