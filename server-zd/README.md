# server-zd

server-zd is a Dex Server written in Rust whose only runtime dependency is object storage (S3 or
an S3-compatible store). The local disk is only a cache: every acknowledged write is durable in
object storage, and a node rebuilds its local state from snapshots and the command log.

It serves the same `protos/dex.proto` FlowService and pull-mode WorkerTaskService as the Go Server,
so the Dex SDKs work without changes. It differs from the Go Server in a few documented ways (for
example, it supports only pull Workers).

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

## Running

`dex-server-zd` has two commands: `serve` for a Server node on external object storage and `dev`
for local development.

### serve

```bash
dex-server-zd serve --config /etc/dex/dex-server-zd.yaml
```

`serve` reads every setting from one YAML file. `config/dex-server-zd.example.yaml` lists every key
with its default. Set at least `cluster.nodeId`, `objectStore.bucket`, and
`localStorage.directory`. Unknown keys and a `dev` section with non-default values fail startup.
An invalid value fails startup with the YAML path of the rejected field.

`localStorage.directory` needs room for the live state plus `localStorage.maxWalBytes` of RocksDB
write-ahead log and up to four uncompacted 64 MiB level-0 files per column family. A compaction
after a bulk replay returns the directory to about the live state size.

Object storage uses `objectStore.endpoint` (empty for AWS endpoint resolution), `region`, `bucket`,
`prefix`, and `forcePathStyle`. `objectStore.hardDeleteStore` optionally sends hard-delete values
to a separate destination with the same keys; without it they use the primary bucket and prefix.
`credentialsSource` selects the credentials of each destination:

| Value | Credentials |
| --- | --- |
| `defaultChain` (default) | The AWS SDK default credential provider chain |
| `environment` | `DEX_ZD_OBJECT_STORE_ACCESS_KEY_ID`, `DEX_ZD_OBJECT_STORE_SECRET_ACCESS_KEY`, and the optional `DEX_ZD_OBJECT_STORE_SESSION_TOKEN`; the hard-delete store reads the `DEX_ZD_HARD_DELETE_STORE_*` variables with the same suffixes |
| `anonymous` | Unsigned requests, for a local emulator or a bucket that allows anonymous access |

Startup proceeds in this order:

1. The node binds `api.listen`. gRPC health Check and Watch report NOT_SERVING for `""`,
   `dex.FlowService`, and `dex.WorkerTaskService`; FlowService and WorkerTaskService calls return
   Unavailable while startup continues.
2. Every destination must pass a conditional-write self-check.
3. The node acquires the lease of `cluster.nodeId` for `lease.duration`. A lease that another
   process still holds is awaited until it expires, or fails startup when `lease.failIfHeld` is
   true.
4. The node restores `localStorage.directory` from the latest snapshot and the command log. The
   directory is a cache; an empty one is rebuilt from object storage. Replay keeps up to
   `recovery.readAheadBatches` log reads in flight, holding at most `recovery.readAheadBytes` of
   log objects or one larger batch, and applies batches in log order as they arrive. After a
   replay of at least `localStorage.compactAfterReplayBytes`, the local store is compacted in the
   background while the node serves.
5. After recovery and rebuild finish, health switches to SERVING and the API serves requests.
   Startup failure closes the listener.

### Snapshot uploads after ownership changes

The first snapshot after every takeover or restart uploads every file in the local checkpoint to
a fresh lineage. It cannot reuse objects from the previous lineage, even when the local RocksDB
directory is reused. Expect uploads approximately equal to the full checkpoint size, plus manifest
data. Transfer time is at least checkpoint bytes divided by effective upload throughput, plus
checkpoint and per-file request time. For example, 10 GiB at 100 MiB/s takes at least about 102
seconds to transfer. Later snapshots from the same owner can reuse unchanged files.

`search.backend: builtin` serves SearchFlows from committed local search documents.

`search.backend: paradedb` serves SearchFlows from ParadeDB:

- Before the log is replayed, the node opens `search.paradeDbConnectionPoolSize` connections to
  `search.paradeDbConnectionString` and creates or upgrades the search schema. It tries three
  times, then fails startup. The first startup needs a role that may create the `pg_search`
  extension, unless the extension is already installed.
- A `search.paradeDbSchema` other than `public` is applied as the `search_path` connection option.
  A transaction pooler must pass that startup option through; otherwise keep the tables in `public`.
- A background task copies each committed partition frame's search documents to ParadeDB. Commits
  never wait for it. When ParadeDB is unavailable, the task retries. When it falls behind its queue
  or its stored position diverges, it rebuilds the partition from committed local state.
- On startup, every partition whose ParadeDB position differs from the local store is rebuilt.
- SearchFlows waits up to `search.readYourWritesWait` until frames committed before the request are
  searchable, then returns Unavailable.
- One ParadeDB schema belongs to one Dex cluster.

On SIGTERM or SIGINT the node stops within 14.5 seconds. Health reports NOT_SERVING, Workers are
told to reconnect, long polls close, and open calls get 4 seconds to finish. Then effects and the
engine stop, and the node releases its lease. The process exits 0, and a replacement process with
the same `cluster.nodeId` acquires the lease at once. When object storage rejects the release, or
the release does not finish within 2 seconds, the process exits 1 and the lease stays held until it
expires.

A lost lease, a stopped commit pipeline, or a halted Flow runtime stops the node, and the process
exits 1. Restart it. A signal before the node acquires its lease cancels startup and exits 0. If
the node already holds its lease, startup finishes before graceful shutdown begins. That shutdown
has the same 14.5-second total and exit status as a signal received while serving. A failed startup
releases its lease and exits 1.

Logs go to standard error. `RUST_LOG` filters them; the default level is `info`.

### dev

```bash
dex-server-zd dev --data-dir ./zd-data [--listen 127.0.0.1:8801] [--log-dir <dir>] [--config <overrides.yaml>]
```

`dev` runs one node with an embedded s3mock whose data lives under `<data-dir>/s3`, and keeps local
state under `<data-dir>/store`. `dexcli dev -engine zd` starts it with these flags. It fixes the
node ID to `dev`, anonymous credentials, the embedded endpoint, and the `--listen` address, and it
rejects `objectStore.hardDeleteStore`. With `--log-dir`, logs also go to
`<log-dir>/dex-server-zd.log`. A `search.backend: paradedb` override uses ParadeDB as `serve` does.

## Development

```bash
cd server-zd
cargo test -p <crate>        # iterate on one crate
make fmt-check clippy test   # what CI runs
make build-server            # builds target/release/dex-server-zd
```

Local tests use only the in-process s3mock (`../s3mock`) or the in-memory object store. They need
no docker, MinIO, or Temporal.

### Persistent format testdata

`testdata/v<N>` holds a recorded log, snapshot, cluster metadata, and committed state.
`cargo test -p dexzd-engine --test format_compat` checks that the current code decodes every
version and replays it to the recorded state. Recorded versions are immutable; a pinned digest
of every file fails the test when a byte changes. The scripted workload must also reproduce the
newest version byte for byte, so a change to the bytes the engine writes for it needs a new
version.

Add the next version in the change that does any of the following:

- changes `proto/dexzd/`, the key layout, the object storage layout, or the snapshot format;
- adds a command kind, a state table, an index kind, or an object kind;
- adds a behavior version;
- prepares a release whose persisted data differs from the newest version.

`testdata/README.md` describes the generator and the steps.

## Rules specific to server-zd

- `dexzd-apply` must be deterministic. It never reads clocks, random numbers, configuration, the
  environment, files, or the network, and it never iterates a hash map. Its `clippy.toml` bans the
  common offenders.
- A change to the result of applying an already committed command needs a new behavior version
  with a named predicate.
- Persistent formats (`proto/dexzd/`, key layout, object storage layout, snapshots) change only
  additively, following the same rules as `protos/dex.proto`.
