# Dex Java examples

These examples target `io.superdurable:dex-sdk:1.2.1` (`io.superdurable.dex`).

The sample process hosts one gRPC Worker on `127.0.0.1:8803` and an HTTP
controller on port `8080`. One Registry and disk BlobCache are shared by its
Worker and Client.

## Layout

```
src/main/java/io/superdurable/dex/
├── products/       # real-world business scenarios
├── patterns/       # design patterns
├── primitives/     # one minimal example per Dex primitive
└── shared/         # mock services and HTTP helpers
```

HTTP routes use category prefixes:

- `/products/<kebab>/...` — e.g. `/products/job-post/create`
- `/patterns/<kebab>/...` — e.g. `/patterns/polling/start`
- `/primitives/<kebab>/...` — e.g. `/primitives/step/start`

## Run locally

```bash
dexcli dev
./gradlew bootRun
```

The Worker synchronizes all registered Indexed Attributes with Dex before it
opens its listener; no backend CLI registration is required.

The examples share namespace-level slots by index type: `keyword1`,
`text1`, `int1`, and numbered later slots such as `keyword2`.
Use a fresh local store and a new port after changing an example's index schema.

Application searches must constrain `FlowType` and parenthesize caller filters.
The sample search endpoints enforce their Flow type.

Use JDK 17+ to run Gradle; the examples build and run on the newest Java
release by default (the toolchain downloads automatically), and
`-PjavaVersion=17` (or 21, 25, ...) selects another JDK.
Defaults connect to Dex at `localhost:8801`. Override with
`DEX_FLOW_SERVICE_ADDRESS`, `DEX_WORKER_BIND_ADDRESS`, `DEX_WORKER_TARGET`,
`DEX_EXAMPLES_HTTP_ADDRESS`, `DEX_BLOB_CACHE_DIR`.

- `DEX_EXAMPLES_WORKER_MODE`: `push` (default) listens on the Worker bind
  address. `pull` connects to Dex as a pull Worker, ignores the bind address and
  target, and starts Flows with the pull Worker target.

## Verify

Run the integration suite against an isolated `dexcli dev` environment:

```bash
./run-integration-tests.sh
```

Set `DEX_EXAMPLES_WORKER_MODE=pull` to run the same tests with pull Workers.
Set `DEX_INTEG_ENGINE=zd` to run them against server-zd. That mode defaults to
pull Workers, does not start the entity store, and skips samples that use
failure policies, start delay, Flow retry, or Attribute Store sync. Set
`DEX_SERVER_ZD_BINARY` to reuse a built `dex-server-zd`.

Examples catch concrete types from `io.superdurable.dex.exceptions`.
`FlowNotFoundException` is for read operations with no matching execution;
`FlowNotActiveOrNotFoundException` covers missing RPC targets and mutations that cannot
use a closed Flow. A query-only RPC can read a retained closed Flow when it has no
locks, transactionality, durable effects, or Server-forced Update routing. At that
confirmed read boundary, return the missing-target result directly without lifecycle
probes, retries, or history decoding. Preserve other service and Worker failures.
`ErrorSubStatus` remains diagnostic metadata and is not used for control flow.

The Go examples support `./run-e2e-tests.sh --keep-running` to leave Dex running
after E2E tests for manual HTTP exploration.

## Products

- [Money transfer](./src/main/java/io/superdurable/dex/products/moneytransfer)
- [Order processing](./src/main/java/io/superdurable/dex/products/orderprocessing)
- [Microservice orchestration](./src/main/java/io/superdurable/dex/products/microservices)
- [Engagement](./src/main/java/io/superdurable/dex/products/engagement)
- [Subscription](./src/main/java/io/superdurable/dex/products/subscription)
- [User onboarding process](./src/main/java/io/superdurable/dex/products/signup)
- [Job posting](./src/main/java/io/superdurable/dex/products/jobpost)
- [Deal DSL](./src/main/java/io/superdurable/dex/products/dealdsl)

## Patterns

Under [`patterns/`](./src/main/java/io/superdurable/dex/patterns):

- Cron schedule
- Drain internal / externally published channels
- Interruptible execution
- Manual recovery
- Parallel Steps (static / dynamic / await / first win)
- Parallel SubFlows (basic / long-lived parent / short-lived parent / partitioning / back pressure)
- [Polling an external job in one long-running Step](./src/main/java/io/superdurable/dex/patterns/polling)
- Failure recovery (saga)
- Reminders
- Inactiveness Tracker Timer
- [Entity Store user profiles](./src/main/java/io/superdurable/dex/patterns/entitystore) ([PostgreSQL setup](../entity-store))
- Timeout handling
- Wait for Step completion

## Primitives

Minimal examples under [`primitives/`](./src/main/java/io/superdurable/dex/primitives):

- [Step](./src/main/java/io/superdurable/dex/primitives/step)
- [Step custom retry](./src/main/java/io/superdurable/dex/primitives/customretry)
- [Step durability](./src/main/java/io/superdurable/dex/primitives/durability)
- [Step heartbeat](./src/main/java/io/superdurable/dex/primitives/stepheartbeat)
- [Step options override](./src/main/java/io/superdurable/dex/primitives/optionsoverride)
- [Step decision](./src/main/java/io/superdurable/dex/primitives/stepdecision)
- [Step wait types](./src/main/java/io/superdurable/dex/primitives/waittypes)
- [Attribute](./src/main/java/io/superdurable/dex/primitives/attribute)
- [Timer](./src/main/java/io/superdurable/dex/primitives/timer)
- [Channel](./src/main/java/io/superdurable/dex/primitives/channel)
- [RPC](./src/main/java/io/superdurable/dex/primitives/rpc)
- [SubFlow](./src/main/java/io/superdurable/dex/primitives/subflow)
- [Client APIs](./src/main/java/io/superdurable/dex/primitives/clientapis)
