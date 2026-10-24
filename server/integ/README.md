### Integration tests

This directory contains integration tests for the Dex service.

* [How to run](../CONTRIBUTING.md#how-to-run-server-or-integration-test)
* The integration tests are written without Dex SDKs. The workflows are implemented in REST API routes. e.g. [this basic workflow](./workflow/basic/routers.go)

Run existing scenarios with pull Workers against the in-process Go Server:

```shell
cd server
DEX_INTEG_WORKER_MODE=pull go test ./integ -cadence=false -run 'TestBasicFlowTemporal'
```

To launch a Dex Server binary for each test, pass `-dex-server-binary` and
`-dex-server-engine=zd`. Binary mode selects pull Workers and gives each process
a fresh data directory and loopback port. Each test waits for gRPC health on the
empty service name before making FlowService calls. Configuration supported by
the binary is written to a temporary YAML file. Tests requiring Go Server
settings or direct Temporal state are skipped with a specific reason.

The adapter opens four Flow type streams and four sticky streams per registered
Flow type. Scenarios with more blocking Worker tasks than those streams can
exhaust this test capacity. Pull Worker errors are reported as Worker errors;
push mode can classify the same transport error as a Server error. The
Worker-address switching scenario remains push-only.

Binary mode applies the harness's 12-second WaitForFlow limit in its client and
adds SYNC durability to StartFlow requests. The Go Server's 2-second minimum
Step heartbeat timeout has no binary equivalent; tests that rely on it start
the in-process Go Server and are skipped in binary mode. The Step cancellation
scenario uses a 10-second heartbeat timeout in binary mode instead.

Each launch registers the custom search attributes that the Temporal dev server
predefines, such as **CustomKeywordField** and **CustomDatetimeField**, through
**SyncAttributeIndexes**. The YAML file always sets the Stream Store backend.
It is disabled unless a test selects the memory backend, as in the Go Server.

Tests for features that server-zd rejects or changes skip in binary mode or
adjust their expectations. Each guard has a comment that names the
server-zd behavior difference. Stream Store tests that use Redis still need
Redis on `127.0.0.1:6379`.

```shell
cd server
go test ./integ -dex-server-binary=/path/to/dex-server-zd -dex-server-engine=zd -run 'TestBasicFlowTemporal'
```

Attribute Sync integration uses isolated MySQL and PostgreSQL services:

```shell
docker compose -f docker-compose/attribute-store-dependencies.yml up -d --wait
make attributeSyncIntegTests
```

The suite covers startup schema contracts, both relational upsert dialects,
filtering, schema refresh recovery, additive SQL columns, and Attribute Sync
happy paths on both stores with Temporal and Cadence. In pull requests, the
**Attribute Sync CI** workflow runs only when the **ci:attribute-sync** label is
present. Regular **Server CI** does not start MySQL or PostgreSQL.

MongoDB Attribute Sync runs in a separate label-gated job because its container
and workflow coverage are comparatively expensive:

```shell
docker compose -f docker-compose/mongodb-attribute-store-dependency.yml up -d --wait
make mongodbAttributeStoreIntegTests
```

The MongoDB suite covers startup collection contracts, upserts, partial updates,
filtering, and Attribute Sync happy paths on Temporal and Cadence. In pull
requests, the **Attribute Sync CI** workflow runs this suite with the relational
suite when the **ci:attribute-sync** label is present.

Databricks and Snowflake use local SQL contract tests because the suite does not
connect to cloud warehouses.

In-process tests use an isolated local Blob Store with the default 1 KiB
offload threshold. S3-specific tests replace it with MinIO.

Use **-temporalNamespace** to select a fresh local namespace when other workers
share the test backend. Search run coverage verifies actual Continue-as-New,
default exclusion, explicit inclusion, other statuses, OR conditions, and
pagination on Temporal and Cadence. Cadence coverage creates its own domain.

Step cancellation coverage runs against both Temporal and Cadence. It verifies
Flow-wide and sibling selectors; queued and active executions; local and
regular activities; local-timeout fallback with cumulative attempts;
heartbeat-driven handler cancellation;
fire-and-continue behavior; late-result suppression;
continue-as-new; Step and RPC producers; signal and synchronous-update RPC
delivery; RPC sibling-selector rejection; snapshot exclusion of RPC next Steps;
and clean active state.

Channel queue coverage verifies server-generated UUIDv7 identities, FIFO pending
lists, consumption and deletion, stable NotFound mapping, Continue-as-New and
reset preservation, ChannelMap state, and large-Value hydration. Temporal tests
cover transactional RPC deletion and implicit transactions from Attribute
locking. Temporal and Cadence tests cover best-effort signal deletion and the
Cadence query-plus-signal boundary.

RPC selective-state coverage verifies that ordinary Attributes and all Channel
size metadata are always available. AttributeMap entries and pending Channel or
ChannelMap messages require explicit selectors. The suite covers loaded-empty
collections, all-instance and exact-instance selectors, FIFO envelopes, message IDs, eager and lazy
blob loading, and the independence of loading, transactions, and Attribute
locks.

RPC Blob Store coverage rejects writes while pure reads exchange inputs and
outputs larger than the durable threshold. It covers string and encoded-object
values and eager and lazy loading. Transport tests include Worker state and
metadata in the message-size limit and reject
oversized requests with Blob Store enabled or disabled without calling the
Worker or creating objects. Server and Worker size-limit failures expose the same
client error code, substatus, and original Worker status. Pure-read tests check that semantic history contains
no RPC completion event and Temporal history contains no Signal or Update event.
External-storage tests verify persisted transactional payloads and RPC state
changes, with direct nontransactional input/output transport in both loading
modes. Nontransactional effect history excludes RPC inputs and outputs.
Transactional size-limit tests cover explicit transactions, Attribute
locks, and synchronous-update configuration in both loading modes. They verify
the client error code and Worker status, a single non-retryable local activity
attempt in Temporal history, and successful subsequent calls after failure.
Wire-compatibility tests decode Signal payloads with the published field numbers
and removed RPC input/output fields, then verify Attribute writes on both backends.

Resumable Stream integration covers per-message size limits, Flow-type scope
isolation, global FIFO trim, resume, repeated sources, and multi-server trim
coordination. It requires Redis 7 on `127.0.0.1:6379`. The standard dependency
Compose files provide it with the `noeviction` policy. Run the focused server
and Redis coverage with:

```shell
docker compose -f docker-compose/integ-dependencies.yml up -d redis
make streamIntegTests
```

The focused suite covers cross-Flow global FIFO, independent trim-trigger and
trim-target watermarks, hard-capacity rejection and retry, repeated and
concurrent source values, resume behavior, long polling, concurrent writers,
concurrent trigger lease contention and recovery, disabled configuration, and
Redis failure isolation. Worker coverage includes multiple heartbeat and Stream
frames, heartbeat detail recovery, protocol termination, and pre-frame
headless failover.
