# Rust SDK guide

The SDK API reference uses the [released v1.15.0 baseline](../core/bundle-baselines.md); the source examples' older dependency pins below remain factual and do not define the current error API.

Use this page first for every Rust application task. Then load only the topic pages needed by the request. The examples on this page are pinned to the Dex source baseline recorded by this skill.

## Install and run

The baseline examples use `dex-sdk = "=1.14.0"` and require Rust 1.97 or newer. Do not blindly copy that dependency into an existing application: inspect its `Cargo.toml` and `Cargo.lock`, then consult [versioning](versioning.md). The published examples start a local Dex server, Worker, and HTTP controller with:

```bash
cd examples/rust
dexcli dev
cargo run --locked
```

The example defaults are Dex at `127.0.0.1:8801`, Worker bind at `127.0.0.1:8803`, and the HTTP controller at `127.0.0.1:8080`. Use the environment variables documented in the [pinned Rust example README](https://github.com/superdurable/dex/blob/sdk-rust/v1.15.0/examples/rust/README.md) when those addresses or the BlobCache directory differ.

## Pull Workers and Dex credentials

Use `WorkerOptions::new().pull(PullWorkerOptions::new().max_concurrent_tasks(8))` for outbound polling, without a bind address or listening target. Advertise `worker.worker_target().clone()` or `WorkerTarget::pull()` through `ClientOptions::worker_target` or a StartFlow or SubFlow FlowConfig override. Pull options also have `idle_streams_per_queue`, `app_worker_id`, and `stop_drain_timeout`; defaults are one idle stream per queue, a generated process ID, and 30 seconds. Change an explicit app Worker ID after restart. With Server and Rust SDK v1.12.0, a Worker that lacks a requested Step type or RPC name declines it before the handler runs, so Dex can reassign the task without consuming the Step attempt. Follow the [pull Worker guarantees](../core/pull-workers.md) for sticky routing, retries, heartbeats, stream capacity, and Stop. [Released Rust options](https://github.com/superdurable/dex/blob/sdk-rust/v1.15.0/sdk-rust/crates/dex-sdk/src/worker_options.rs) [Runnable pull Worker example](https://github.com/superdurable/dex/blob/sdk-rust/v1.15.0/examples/rust/src/production/pull_workers.rs)

Rust SDK v1.14.0 adds **worker.wait_pull_registered().await**. Run blocking **start()** on a dedicated thread, handle its result, and wrap the registration future with **tokio::time::timeout** before activating the Worker. Dropping the future cancels only that wait. Probe **worker.pull_registration_state().is_ready**; queue entries expose **queue**, **has_registered_stream**, and **last_permanent_error**. A rejection returns an **SdkError** whose **pull_registration_error()** accessor yields **PullRegistrationError**, preserving the queue and original gRPC status. **PullWorkerOptions::stop_on_permanent_stream_rejection(true)** opts into draining and returning that error from **start()** when a permanent rejection leaves a queue without another registered stream. Always call **stop()** afterward. Follow the shared [readiness contract](../core/pull-workers.md#startup-registration-and-readiness), including stable readiness across reconnects and Stop. [Released Rust readiness API](https://github.com/superdurable/dex/blob/sdk-rust/v1.15.0/sdk-rust/crates/dex-sdk/src/worker.rs) and [typed rejection](https://github.com/superdurable/dex/blob/sdk-rust/v1.15.0/sdk-rust/crates/dex-sdk/src/sdk_error.rs)

With SDK v1.15.0, **PullWorkerOptions::reserved_rpc_tasks** reserves RPC capacity. Omit the builder setting for the default; pass **0** to opt out. Unset reserves one slot when resolved maximum concurrent tasks **M ≥ 2**; otherwise it reserves zero and logs one startup warning. Require **0 ≤ R < M**; invalid reserves fail construction. This intentionally gives Steps one fewer slot by default. Both API-direct and transactional RPC tasks may use all **M** slots; WaitFor and Execute, including local attempts, are declined at **M − R** total busy handlers. Export **Worker::pull_reserved_rpc_declines()** to measure reserve declines. Follow the shared [reserve semantics and sizing advice](../core/pull-workers.md#reserved-rpc-capacity) for dispatch exhaustion, counter scope, and long-running Steps. [Released capacity guidance](https://github.com/superdurable/dex/blob/sdk-rust/v1.15.0/sdk-rust/README.md)

With SDK v1.14.1, the Worker gracefully closes surplus idle streams after load drops while preserving readiness and the configured registered idle streams per queue. Size steady-state streams as **M + (F + 1) × I**, where **M** is maximum concurrent tasks, **F** is registered Flow types, and **I** is idle streams per queue. Burst peaks remain bounded by **matching.maxConcurrentTasksCap** and **matching.maxStreams**; follow the shared [capacity guidance](../core/pull-workers.md#heartbeats-capacity-and-shutdown).

Set `credential(Arc<dyn Credential>)` separately on ClientOptions and WorkerOptions when public API authentication is enabled. `FileCredential::new(path)` reads a rotating bearer token. A custom `Credential` returns the current token for each call and pull stream opening; a credential error fails the call locally. Protect the plaintext connection. [Released Rust credentials](https://github.com/superdurable/dex/blob/sdk-rust/v1.15.0/sdk-rust/crates/dex-sdk/src/credential.rs)

## Connector runtime support

The Connector SDK's `projectconfig.LoadFromEnvironment` loader and the generated `NewProjectConnection` API are Go-only, and no equivalent Rust SDK exists today. Do not invent a Rust loader or deserialize Dex Web's credential file into Flow state. For a Rust application, keep provider credentials behind an application-owned secret/runtime adapter until an official Rust Connector SDK exposes the same contract.

## Raw Temporal payload inspection

Use **dexcli codec-server** with CLI v1.4.2 or later to inspect Dex internal payloads
in Temporal Cloud or a local Temporal Web UI. Follow the shared
[codec-server guidance](../core/operations.md#inspect-raw-temporal-payloads);
local UI ports need no additional server arguments.

## Deployed configuration and recovery boundary

Dex provides no deployed Connector configuration or credential store. A deployed Go
application reads its connections with the Connector SDK's `sdkgo/projectconfig`, but
no equivalent Rust SDK exists today; see
[deployed Connector configuration](../core/operations.md#deployed-connector-configuration).
Keep credentials behind the deployment's secret management; a browser, Flow input,
or ordinary Attribute cannot select secret storage.
When the Flow itself must hold a short-lived secret, such as an API key passed
at start, use a [hard-delete Attribute](data-handling.md#hard-delete-attributes)
instead of an ordinary Attribute.
For recovery after a Worker fix, preserve the original resource and Flow ID,
reconcile external effects, and use an explicitly authorized, recorded Step
boundary as described in [safe recovery](../core/operations.md#safe-recovery).

## Dex Web v2 management metadata

The current FDG 2.0 management-interface analyzer reads Go source only. A Rust
Flow cannot currently publish the Summary, Display, Action, Indexed Attribute,
group, explanation, or Action input capture metadata that drives the full Dex
Web v2 management interface. Rust applications can still operate Flows and use
their supported Client and CLI surfaces; do not invent Rust attributes for the
Go-only analyzer contract.

## Minimal Flow and Step

A Rust Flow owns its Step values and returns a `StepList` that borrows them. Each Step declares its input type and returns a `StepDecision`. `wait_for` is optional; omitting it makes Execute eligible immediately.

[Runnable source](https://github.com/superdurable/dex/blob/sdk-rust/v1.15.0/examples/rust/src/primitives/timer/flow.rs)
<!-- dex-source: examples/rust/src/primitives/timer/flow.rs -->
```rust
#[derive(Default)]
pub struct TimerFlow {
    start: TimerStep,
}

impl Flow for TimerFlow {
    type StartInput = i32;

    fn steps(&self) -> StepList<'_, Self::StartInput> {
        StepList::start(&self.start)
    }
}

#[derive(Default)]
struct TimerStep;

impl Step for TimerStep {
    type Input = i32;

    fn wait_for(&self, _context: &mut Context, input: Self::Input) -> HandlerResult<Wait> {
        Ok(Wait::until(Timer::by_duration(Duration::from_secs(
            input.max(0) as u64,
        ))))
    }

    fn execute(&self, _context: &mut Context, _input: Self::Input) -> HandlerResult<StepDecision> {
        Ok(StepDecision::graceful_complete("timer-fired".to_string()))
    }
}
```

Keep every `Attribute`, `AttributeMap`, `Channel`, `ChannelMap`, and `Stream` schema definition stable. Module-level `static LazyLock<T>` is the repository convention for definitions that do not need runtime construction. Do not hide schema creation behind a function. A Flow may own a map only when its instance is needed to build child Steps; clone the initialized map value only where an owned field is required.

## Registry, Worker, and Client

The Registry must contain every Flow that a Client starts or a Worker executes. Registration validates names, Step graphs, persistence declarations, loads, and RPC definitions. Build the registry with chained `register` calls and propagate `SdkResult`.

[Runnable source](https://github.com/superdurable/dex/blob/sdk-rust/v1.15.0/examples/rust/src/primitives/mod.rs)
<!-- dex-source: examples/rust/src/primitives/mod.rs -->
```rust
pub fn register(registry: Registry) -> SdkResult<Registry> {
    registry
        .register(step::flow::StepFlow::default())?
        .register(flow::flow::ExampleFlow::default())?
        .register(step::retry_flow::RetryFlow::default())?
        .register(custom_retry::flow::CustomRetryFlow::default())?
        .register(durability::flow::DurabilityFlow::default())?
        .register(heartbeat::flow::HeartbeatFlow::default())?
        .register(options_override::flow::OptionsOverrideFlow::default())?
        .register(proceed_on_wait_failure::flow::ProceedOnWaitFailureFlow::default())?
        .register(step_execution_local::flow::StepExecutionLocalFlow::default())?
        .register(step_decision::flow::StepDecisionFlow::default())?
        .register(wait_types::flow::WaitTypesFlow::default())?
        .register(attribute::flow::AttributeFlow::default())?
        .register(channel::flow::ChannelFlow::default())?
        .register(stream::flow::StreamFlow::default())?
        .register(timer::flow::TimerFlow::default())?
        .register(rpc::flow::RpcFlow::default())?
        .register(subflow::flow::SubFlowChildFlow::default())?
        .register(subflow::flow::SubFlowParentFlow::new())?
        .register(client_apis::flow::ClientApisFlow::default())
}
```

Create one shared `Arc<BlobCache>` for the Client and Worker. Start the blocking Worker on a dedicated OS thread; application HTTP work may remain on Tokio. If a Flow constructor needs a Client, construct the Client registry and Worker registry separately so dependency injection remains explicit. See the complete [bootstrap](https://github.com/superdurable/dex/blob/sdk-rust/v1.15.0/examples/rust/src/main.rs).

## Suggested project layout

```text
src/
  flows/<feature>/flow.rs       # schema, Flow, and Steps
  flows/<feature>/controller.rs # Client calls at the process boundary
  flows/mod.rs                  # registry composition
  main.rs                       # cache, Client, Worker, HTTP bootstrap
tests/
  integration.rs               # real Dex Server behavior
```

Keep controller concerns out of Step handlers. The controller supplies idempotent request IDs and unique Flow IDs; the Flow models durable state and decisions; Steps perform bounded external work.

## Route to the right page

- [Primitives](primitives.md): exact Rust shapes for Flow, Wait, persistence, RPC, Stream, Timer, SubFlow, and Client.
- [Patterns](patterns.md): choose and implement an official design pattern.
- [Testing](testing.md): real-server integration and terminal assertions.
- [Error handling](error-handling.md): read before Client boundary work for `HandlerError`, `SdkError`, start identity, retries, and recovery.
- [Data handling](data-handling.md): serde values, selective loads, locks, transactions, and BlobCache.
- [Observability](observability.md): IDs, status, heartbeat progress, Stream output, and diagnostics.
- [Versioning](versioning.md): pinned API evidence and open-Flow compatibility.
- [Gotchas](gotchas.md): ownership, blocking execution, schemas, and unsafe assumptions.
- [Advanced features](advanced-features.md): timeout handlers, cancellation, streaming, and asynchronous durability.
