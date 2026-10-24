# Rust examples

## Purpose

Runnable Rust examples for Dex Flows, Steps, and application patterns.

This application mirrors the examples shared by the Java, Python, and TypeScript
applications. It intentionally depends on the published `dex-sdk = "=1.2.1"`
crate without a repository path override.

RPC and mutation calls can return `SdkError::FlowNotActiveOrNotFound` when the Flow is missing
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
└── server/         # Axum HTTP router
```

HTTP routes use category prefixes:

- `/products/<kebab>/...` — e.g. `/products/job-post/create`
- `/patterns/<kebab>/...` — e.g. `/patterns/polling/start`
- `/primitives/<kebab>/...` — e.g. `/primitives/channel/approve`

## Run

Rust 1.97 or newer is required. Start Dex, then run the Worker and HTTP server:

```bash
cd examples/rust
dexcli dev
cargo run --locked
```

The examples share namespace-level slots by index type: `keyword1`,
`text1`, `int1`, and numbered later slots such as `keyword2`.
Use a fresh local store and a new port after changing an example's index schema.

Application searches must constrain `FlowType` and parenthesize caller filters.
The sample search endpoints enforce their Flow type.

The Worker connects to `127.0.0.1:8801`, listens on `127.0.0.1:8803`, serves HTTP
on `127.0.0.1:8080`, and stores large payload blobs under
`/tmp/dex-rust-examples-blobs`. Override these with `DEX_SERVER_ADDRESS`,
`DEX_WORKER_BIND_ADDRESS`, `DEX_EXAMPLES_HTTP_ADDRESS`, and `DEX_BLOB_CACHE_DIR`.

Set `DEX_EXAMPLES_WORKER_MODE` to `push` (default) to listen on the Worker bind
address. Set it to `pull` to connect to Dex as a pull Worker. Pull mode ignores
the bind address and starts Flows with the pull Worker target.

## Products

Every shared product example has a distinct Rust Flow and implementation file.

| Java/Python/TypeScript example | Rust Flow | Demonstrated SDK features |
|---|---|---|
| Money transfer | [`MoneyTransferFlow`](src/products/money_transfer/flow.rs) | Debit/credit saga, Execute retry, and compensation |
| Order processing | [`OrderProcessingFlow`](src/products/order_processing) | Charge, seller Channel + reminder Timer, ship retry, refund |
| Microservice orchestration | [`OrchestrationFlow`](src/products/microservices.rs) | Parallel Steps, Attribute swap RPC, Channel-or-timer wait |
| Engagement | [`EngagementFlow`](src/products/engagement.rs) | Indexed status, decision RPCs, reminders, external notification |
| Subscription | [`SubscriptionFlow`](src/products/subscription.rs) | Billing timers, concurrent control Step, update/cancel RPCs |
| User onboarding process | [`UserOnboardingFlow`](src/products/signup/flow.rs) | Email verification and two ordered tasks with durable reminders |
| Job posting | [`JobPostingFlow`](src/products/job_post/flow.rs) | Locked, versioned RPC updates feeding independent FIFO LinkedIn and Indeed Channel consumers |
| Deal DSL | [`DealDSLFlow`](src/products/deal_dsl/flow.rs) | Durable state-machine interpreter with keyed condition waits and one-action checkpoints |

## Patterns

Every shared design-pattern Flow is independently registered; multi-Flow patterns
remain split so their orchestration boundaries are visible.

| Java/Python/TypeScript pattern | Rust Flow | Demonstrated SDK features |
|---|---|---|
| Cron schedule | [`CronScheduleFlow`](src/patterns/cron) | Fixed-interval durable timer loop |
| Drain internal Channel | [`DrainInternalChannelFlow`](src/patterns/drain_channels.rs) | Internal publication, one-at-a-time drain, sentinel completion |
| Draining External Channel Publishing | [`DrainingExternalChannelFlow`](src/patterns/drain_channels/flow.rs) | RPC publication, one-at-a-time drain, conditional completion |
| Interruptible execution | [`InterruptibleFlow`](src/patterns/interruptible/flow.rs) | Graceful interrupt handling |
| Manual recovery | [`ManualRecoveryFlow`](src/patterns/intervention/flow.rs) | Exhausted retry recovery and manual Channel decision |
| Static parallel Steps | [`StaticParallelStepsFlow`](src/patterns/parallel/parallel_step_flows.rs) | A fixed set of concurrent Step types |
| Dynamic parallel Steps | [`DynamicParallelStepsFlow`](src/patterns/parallel/parallel_step_flows.rs) | A runtime-sized set of one Step type |
| Await parallel Steps | [`AwaitParallelStepsFlow`](src/patterns/parallel/parallel_step_flows.rs) | Wait for every worker's Channel message |
| First-win parallel Steps | [`FirstWinParallelStepsFlow`](src/patterns/parallel/parallel_step_flows.rs) | Keep the first result and cancel siblings |
| Basic SubFlow fan-out | [`BasicParentFlow`](src/patterns/parallel_subflows/flow.rs) | Wait for half of the SubFlows and cancel the remaining IDs |
| Long-lived SubFlow parent | [`AdvancedLongLiveParentFlow`](src/patterns/parallel_subflows/flow.rs) | Bounded workers, request Channel, stop Attribute, and SubFlow loop |
| Short-lived SubFlow parent | [`AdvancedShortLiveParentFlow`](src/patterns/parallel_subflows/flow.rs) | Locked active count and atomic completion when the Channel is empty |
| Partitioning and back pressure | [`SubmitRequestFlow`](src/patterns/parallel_subflows/flow.rs) | Stable parent partitioning and durable retries after admission rejection |
| Polling | [`PollingFlow`](src/patterns/polling/flow.rs) | One long-running Execute loop with heartbeats, a progress Stream, and an engine-enforced maximum wait |
| Failure recovery | [`FailureRecoveryFlow`](src/patterns/recovery.rs) | Retry exhaustion and compensation Step |
| Reminders | [`ReminderFlow`](src/patterns/reminders.rs) | Reminder loop, accept/opt-out Channels, global timeout |
| Inactiveness Tracker Timer | [`InactivenessTrackerFlow`](src/patterns/inactiveness_tracker/flow.rs) | Activity resets a timer; expiry processes inactiveness |
| Entity Store | [`UserProfileFlow`](src/patterns/entity_store.rs) | Attribute Store projection and profile RPCs |
| Timeout handling | [`FlowGracefulTimeout`](src/patterns/timeout.rs) | Task-versus-timeout race and forced completion/failure |
| Wait for Step completion | [`WaitForStepCompletionFlow`](src/patterns/wait_for_step_completion.rs) | `Client::wait_for_step_completion` target followed by background work |

The Java retrying-Worker example and Python basic/resource-control/AI-agent
examples are language-specific. The Go Deal DSL browser UI and PostgreSQL
catalog remain in its separate `dex-deal-dsl` binary.

## Verify

```bash
make fmt-check
make clippy
make test
./run-integration-tests.sh
```

The catalog integration test constructs every Flow, checks the exact 8 + 23
mapping, rejects duplicate names, validates all definitions in one Registry, and
ensures the manifest uses the published crate rather than a local path.

The integration script starts the current checkout's `dexcli dev`, then starts
and verifies Money Transfer, Engagement, Microservice, Subscription,
Deal DSL, and Failure Recovery Flows through the published Rust SDK.

Set `DEX_EXAMPLES_WORKER_MODE=pull` to run the same tests with pull Workers.
Set `DEX_INTEG_ENGINE=zd` to run them against server-zd. That mode defaults to
pull Workers, does not start the entity store, and skips samples that use
failure policies or Attribute Store sync. `make integTests` and
`make flowSmokeTests` filter those tests out by exact name, so `cargo test`
reports them as filtered out, and print each one with the reason
`server-zd does not support <feature>`. Flow smoke entries that server-zd
rejects run as their own tests so they can be filtered. `make test` runs
`check-server-zd-exclusions.sh`, which verifies the filter. Set
`DEX_SERVER_ZD_BINARY` to reuse a built `dex-server-zd`.

The Go examples support `./run-e2e-tests.sh --keep-running` to leave Dex running
after E2E tests for manual HTTP exploration.
