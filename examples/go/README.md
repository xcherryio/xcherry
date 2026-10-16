# Dex Go examples

These examples target `github.com/superdurable/dex/sdk-go v1.10.1`.

`dex.None` marks a nil-only Step, RPC, or Channel payload. Calls pass `nil`.

The sample process hosts one gRPC Worker on `127.0.0.1:8803` and an HTTP controller
on `127.0.0.1:8080`. One Registry and disk BlobCache are shared by its Worker and
Client.

## Layout

```
products/            # real-world business scenarios
patterns/            # design patterns
primitives/          # one minimal example per Dex primitive
production/          # production Worker configurations
registry/            # shared Flow registry
server/              # HTTP helpers
shared/              # mock services
cmd/server/          # default Worker and HTTP entrypoint (`dex-samples`)
cmd/deal-dsl/    # Deal DSL Worker and HTTP entrypoint (`dex-deal-dsl`)
```

HTTP routes use category prefixes:

- `/products/<kebab>/...` — e.g. `/products/job-post/create`
- `/patterns/<kebab>/...` — e.g. `/patterns/polling/start`
- `/primitives/<kebab>/...` — e.g. `/primitives/channel/approve`

## Run locally

Start Dex, then build and run the default examples. This path does not use
PostgreSQL:

```bash
dexcli dev
make bins
./dex-samples
```

The Worker synchronizes all registered Indexed Attributes with Dex before it
opens its listener; no backend CLI registration is required.

The examples share namespace-level slots by index type: `keyword1`,
`text1`, `int1`, and numbered later slots such as `keyword2`.
Application searches must constrain `FlowType` and parenthesize caller filters.
The sample search endpoints enforce their Flow type.

Because that sync happens first, changing a Flow type's Indexed Attributes while
a store already holds runs of that Flow type can stop the Worker before it binds.
The symptom is silence: the process stays alive, logs nothing after Gin's startup
banner, and never opens `DEX_EXAMPLES_HTTP_ADDRESS`, so every request fails to
connect rather than returning an error. Adding, removing or retyping a
`dex.Indexed` Attribute counts as such a change.

Point Dex at an empty store to confirm it, since a fresh store has nothing to
reconcile:

```bash
dexcli dev -dex-port 8901 -web-port 8902   # a new port means a new store directory
```

If the Worker binds there and not against the original store, the schema change
is the cause and not the code. Existing runs stay in the old store, so keep the
old port if you need them.

The defaults connect to Dex at `localhost:8801`. These environment variables override the local addresses:

- `DEX_FLOW_SERVICE_ADDRESS`: Dex gRPC target.
- `DEX_WORKER_BIND_ADDRESS`: WorkerService bind address.
- `DEX_WORKER_TARGET`: address advertised to Dex when it differs from the bind address.
- `DEX_EXAMPLES_HTTP_ADDRESS`: HTTP controller bind address.
- `DEX_BLOB_CACHE_DIR`: shared Client/Worker blob-cache directory.

When Dex runs in Docker, set `DEX_WORKER_TARGET=host.docker.internal:8803`.

`make bins` also builds `./dex-deal-dsl`. That binary is not required for
the default samples server.

## Deal DSL

[Deal DSL](./products/deal-dsl) is a separate process. It uses the same
default HTTP (`127.0.0.1:8080`) and Worker (`127.0.0.1:8803`) ports as
`dex-samples`, and it requires PostgreSQL:

```bash
docker compose -f deal-dsl/docker-compose.yml up -d --wait
dexcli dev
make bins
./dex-deal-dsl
```

Override `DEAL_DSL_POSTGRES_URL` when Postgres is not on
`127.0.0.1:15432`. The demo script uses different HTTP/Worker ports; see
[products/deal-dsl/README.md](./products/deal-dsl/README.md).

## Error handling

Examples match expected SDK failures with `errors.As`. Lifecycle and history reads use
`FlowNotFoundError`; RPC, publish, and mutation paths use `FlowNotActiveOrNotFoundError`.
A query-only RPC can read a retained closed Flow when it has no locks, transactionality,
durable effects, or Server-forced Update routing. At that confirmed read boundary, the
error means no readable target was found. Return the missing-target result directly
without lifecycle probes, retries, or history decoding; preserve other failures.
Duplicate starts use `FlowAlreadyStartedError`, and server long-poll expiry uses
`LongPollTimeoutError`. A Flow that closes without completing returns
`FlowUncompletedError`. `ServiceError.SubStatus` is retained for diagnostics
and is not used for control flow.

## Verify

The E2E suite starts Dex through `dexcli dev` and runs every start, channel
publish, and RPC path covered by the existing product integ tests:

```bash
make e2eTests
```

Pass `--keep-running` to leave Dex, Temporal, entity-store PostgreSQL, and Dex
Web running after tests for manual exploration:

```bash
./run-e2e-tests.sh --keep-running
```

`make e2eTests` also runs Deal DSL against its own binary-equivalent Worker
and PostgreSQL. That Postgres is started only for those tests.

Run only the interactive Deal DSL scenario and its full API verification:

```bash
make dealDSLDemo
```

When PostgreSQL, Dex, and `dex-deal-dsl` are already running, trigger the three
demo executions without starting or stopping services:

```bash
DEAL_DSL_API_URL=http://127.0.0.1:20804 make triggerDealDSLDemo
```

`DEAL_DSL_PROCESS_ID` optionally changes the created process ID. Repeated
runs update that process definition and create new full, refund, and pending
executions.

Set `KEEP_DEAL_DSL_DEMO=1` to leave PostgreSQL, Dex, Temporal, the worker,
and the REST/UI server running. The script prints all UI URLs and shutdown
details.

## Products

- [Money transfer saga](./products/money-transfer)
- [Order processing](./products/order-processing)
- [Microservice orchestration](./products/microservices)
- [Employer/job-seeker engagement](./products/engagement)
- [Subscription](./products/subscription)
- [User onboarding process](./products/signup)
- [Job posting](./products/job-post)
- [Deal DSL](./products/deal-dsl) (separate UI and `dex-deal-dsl` binary)
- [Customer refund](./products/customer-refund) (deterministic and agentic FDG 2.0 Flows)
- [Connector factory](./products/connector-factory) (OpenAI operation-specific factories, recovery, Result Attributes, and Streams)

## Patterns

Under [`patterns/`](./patterns):

- [Cron schedule](./patterns/cron) (auto-started; no HTTP)
- [Drain internal / externally published channels](./patterns/drain-channels)
- [Interruptible execution](./patterns/interruptible)
- [Manual recovery](./patterns/intervention)
- [Parallel Steps: static, dynamic, await, and first win](./patterns/parallel)
- [Parallel SubFlows: basic, long-lived parent, short-lived parent, partitioning, and back pressure](./patterns/parallel-subflows)
- [Polling an external job in one long-running Step](./patterns/polling)
- [Failure recovery](./patterns/recovery)
- [Reminders](./patterns/reminders)
- [Inactiveness Tracker Timer](./patterns/inactiveness-tracker-timer)
- [Entity Store user profiles](./patterns/entity-store) ([PostgreSQL setup](../entity-store))
- [Timeout handling](./patterns/timeout)
- [Wait for Step completion](./patterns/wait-for-step-completion)

## Primitives

Seven minimal examples under [`primitives/`](./primitives/): step, attribute,
channel, timer, rpc, subflow, and client-apis.
