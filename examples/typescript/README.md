# Dex TypeScript examples

These examples target [`@superdurable/dex@1.2.1`](https://www.npmjs.com/package/@superdurable/dex).

The sample process hosts one gRPC Worker (default `127.0.0.1:8803`) and an HTTP
controller on port `8080`. Step `execute` / `waitFor` / RPC handlers may
`await` the shared `Client` directly (async await on the Worker). Disk
BlobCaches are under `DEX_BLOB_CACHE_DIR`.

Controllers handle expected duplicate and missing-Flow failures through
concrete SDK error classes; no example compares Dex sub-status metadata.

RPC and mutation calls can return `FlowNotActiveOrNotFoundError` when the Flow is missing
or the operation requires an active execution that has ended. A query-only RPC
can still read a retained terminal Flow when it has no locks, transactionality,
durable effects, or Server-forced Update routing. At that confirmed read boundary,
handle the error as missing or unavailable without a lifecycle probe, a short
`WaitForFlow` timeout, or historical Step-output decoding. RPC success alone does
not prove that the Flow is active.

## Layout

```
src/
├── products/       # real-world business scenarios
├── patterns/       # design patterns
├── primitives/     # one minimal example per Dex primitive
├── config/         # env and Cron schedule bootstrap
└── main.ts         # Worker and HTTP entrypoint
```

HTTP routes use category prefixes:

- `/products/<kebab>/...` — e.g. `/products/job-post/create`
- `/patterns/<kebab>/...` — e.g. `/patterns/polling/start`
- `/primitives/<kebab>/...` — e.g. `/primitives/channel/approve`

## Run locally

```bash
dexcli dev
cd examples/typescript
npm install
npm start
```

The Worker synchronizes all registered Indexed Attributes with Dex before it
opens its listener; no backend CLI registration is required.

The examples share namespace-level slots by index type: `keyword1`,
`text1`, `int1`, and numbered later slots such as `keyword2`.
Use a fresh local store and a new port after changing an example's index schema.

Application searches must constrain `FlowType` and parenthesize caller filters.
The sample search endpoints enforce their Flow type.

Use Node.js 22 or 24. Defaults connect to Dex at `localhost:8801`. Override with
`DEX_FLOW_SERVICE_ADDRESS`, `DEX_WORKER_BIND_ADDRESS`, `DEX_WORKER_TARGET`,
`DEX_EXAMPLES_HTTP_ADDRESS`, `DEX_BLOB_CACHE_DIR`.

Set `DEX_EXAMPLES_WORKER_MODE` to `push` (default) to listen on the Worker bind
address. Set it to `pull` to connect to Dex as a pull Worker. Pull mode ignores
the bind address and target, and starts Flows with the pull Worker target.

## Verify

```bash
npm test                 # SubscriptionBilling unit tests
npm run test:integ       # product integ tests (requires Dex)
npm run smoke            # every product + pattern HTTP route
./run-integration-tests.sh # start dexcli dev and run both integration suites
```

The integration suite starts and verifies Money Transfer, Order Processing,
Engagement, Microservice, Subscription, and Failure Recovery Flows.

Set `DEX_EXAMPLES_WORKER_MODE=pull` to run the same tests with pull Workers.
Set `DEX_INTEG_ENGINE=zd` to run them against server-zd. That mode defaults to
pull Workers, does not start the entity store, and skips samples that use
failure policies, start delay, Flow retry, or Attribute Store sync. Set
`DEX_SERVER_ZD_BINARY` to reuse a built `dex-server-zd`.

The Go examples support `./run-e2e-tests.sh --keep-running` to leave Dex running
after E2E tests for manual HTTP exploration.

## Products

- [Money transfer](./src/products/money-transfer)
- [Order processing](./src/products/order-processing)
- [Microservice orchestration](./src/products/microservices)
- [Engagement](./src/products/engagement)
- [Subscription](./src/products/subscription)
- [User onboarding process](./src/products/signup)
- [Job posting](./src/products/job-post)
- [Deal DSL](./src/products/deal-dsl)

## Patterns

Under [`src/patterns/`](./src/patterns):

- Cron schedule
- Drain internal / externally published channels
- Interruptible execution
- Manual recovery
- Parallel Steps (static / dynamic / await / first win)
- Parallel SubFlows (basic / long-lived parent / short-lived parent / partitioning / back pressure)
- [Polling an external job in one long-running Step](./src/patterns/polling)
- Failure recovery (saga)
- Reminders
- Inactiveness Tracker Timer
- [Entity Store user profiles](./src/patterns/entity-store) ([PostgreSQL setup](../entity-store))
- Timeout handling
- Wait for Step completion

## Primitives

Seven minimal examples under [`src/primitives/`](./src/primitives/): step,
attribute, channel, timer, rpc, subflow, and client-apis.
