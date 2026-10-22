# Python handbook

The SDK API reference uses the [released v1.15.0 baseline](../core/bundle-baselines.md); the source examples' older dependency pins below remain factual and do not define the current error API.

Use this page first for Python. The [baseline project](https://github.com/superdurable/dex/blob/sdk-python/v1.15.0/examples/python/pyproject.toml) selects `dex-python-sdk==1.14.0`; check `pyproject.toml`/lockfile and installed metadata before relying on baseline syntax.

## Runtime choice

Python has async (`AsyncClient`, `AsyncWorker`, `AsyncContext`) and sync (`Client`, `Worker`, `Context`) surfaces. Match the whole call chain. An async Step is an `async def` coroutine and awaits heartbeat/Client I/O. A synchronous streaming Step is a generator: it yields every `StepOutput` from heartbeat or Stream writes and returns `StepDecision` as the generator return value. A plain synchronous Step directly returns `StepDecision`. Do not mix these contracts.

## Setup and local run

Requires Python 3.11+ at the pinned example. Put Flows in application modules, compose instances in an app/registry module, share one Registry and BlobCache between Client and Worker, and keep HTTP handlers outside Flow classes.

```bash
dexcli dev
cd examples/python
uv sync --locked
uv run --frozen python main.py
```

Defaults use Dex `localhost:8801`, Worker `127.0.0.1:8803`, and HTTP `127.0.0.1:8080`.

## Pull Workers and Dex credentials

Pass `WorkerOptions(pull=PullWorkerOptions(max_concurrent_tasks=8))` to `Worker` or `AsyncWorker` for outbound polling, with no bind address or listening target. Advertise `worker.worker_target` or `WorkerTarget(pull=True)` through `ClientOptions.worker_target` or a StartFlow or SubFlow FlowConfig override. `PullWorkerOptions` also has `idle_streams_per_queue`, `app_worker_id`, and `stop_drain_timeout`; defaults are one idle stream per queue, a generated process ID, and 30 seconds. Change an explicitly assigned app Worker ID after restart. With Server and Python SDK v1.12.0, a Worker that lacks a requested Step type or RPC name declines it before the handler runs, so Dex can reassign the task without consuming the Step attempt. The synchronous and async Workers share the [pull Worker guarantees](../core/pull-workers.md), including Step retries, heartbeats, stream capacity, and Stop. [Released Python options](https://github.com/superdurable/dex/blob/sdk-python/v1.15.0/sdk-python/dex/worker_options.py) [Runnable pull Worker example](https://github.com/superdurable/dex/blob/sdk-python/v1.15.0/examples/python/dex_examples/production/pull_workers.py)

Python SDK v1.14.0 adds **worker.wait_pull_registered(timeout=30)** on Worker and AsyncWorker; the timeout is in seconds and **None** waits indefinitely. Start a synchronous Worker on another thread, or start AsyncWorker in another task and **await worker.wait_pull_registered(timeout=30)** before activating it. Timeout raises **TimeoutError**; async cancellation ends only that wait. Probe **worker.pull_registration_state().is_ready**; queue entries expose **queue**, **has_registered_stream**, and **last_permanent_error**. **PullRegistrationError.queue** and **.error** retain the rejected queue and original transport error. **PullWorkerOptions(stop_on_permanent_stream_rejection=True)** opts into draining and raising that error from **start()** when a permanent rejection leaves a queue without another registered stream. Handle startup and later run failures, and always call **stop()** or **close()** afterward, awaiting async cleanup. Follow the shared [readiness contract](../core/pull-workers.md#startup-registration-and-readiness), including stable readiness across reconnects and Stop. [Released Worker API](https://github.com/superdurable/dex/blob/sdk-python/v1.15.0/sdk-python/dex/worker.py) and [AsyncWorker API](https://github.com/superdurable/dex/blob/sdk-python/v1.15.0/sdk-python/dex/async_worker.py)

With SDK v1.15.0, **PullWorkerOptions.reserved_rpc_tasks** reserves RPC capacity. Leave it as **None** for the default; pass **0** to opt out. The counter property is available on both **Worker** and **AsyncWorker**. Unset reserves one slot when resolved maximum concurrent tasks **M ≥ 2**; otherwise it reserves zero and logs one startup warning. Require **0 ≤ R < M**; invalid reserves fail construction. This intentionally gives Steps one fewer slot by default. Both API-direct and transactional RPC tasks may use all **M** slots; WaitFor and Execute, including local attempts, are declined at **M − R** total busy handlers. Export **Worker.pull_reserved_rpc_declines** to measure reserve declines. Follow the shared [reserve semantics and sizing advice](../core/pull-workers.md#reserved-rpc-capacity) for dispatch exhaustion, counter scope, and long-running Steps. [Released capacity guidance](https://github.com/superdurable/dex/blob/sdk-python/v1.15.0/sdk-python/README.md)

With SDK v1.14.1, the Worker gracefully closes surplus idle streams after load drops while preserving readiness and the configured registered idle streams per queue. Size steady-state streams as **M + (F + 1) × I**, where **M** is maximum concurrent tasks, **F** is registered Flow types, and **I** is idle streams per queue. Burst peaks remain bounded by **matching.maxConcurrentTasksCap** and **matching.maxStreams**; follow the shared [capacity guidance](../core/pull-workers.md#heartbeats-capacity-and-shutdown).

Set `credential` separately in ClientOptions and WorkerOptions when Dex authentication is enabled. `FileCredential(path)` reads a rotating bearer token. A custom `Credential.token()` may return an awaitable for async callers; synchronous callers require a string. Since SDK v1.11.0, an `async def` token runs on the event loop. A synchronous token called from the async API runs once in a lazy, SDK-owned, four-thread executor; closing the Client or Worker does not wait for a blocked call, so make lookups return promptly. A token error fails the call before it reaches Dex. Client calls raise the credential's exception unchanged, while Worker startup may wrap it. Pull streams read the token when opened or reconnected, and stopping a Worker cancels a pending lookup. Protect the plaintext connection. [Released Python credentials](https://github.com/superdurable/dex/blob/sdk-python/v1.15.0/sdk-python/dex/credential.py)

## Connector runtime support

The Connector SDK's `projectconfig.LoadFromEnvironment` loader and the generated `NewProjectConnection` API are Go-only, and no equivalent Python SDK exists today. Do not invent a Python loader or deserialize Dex Web's credential file into Flow state. For a Python application, keep provider credentials behind an application-owned secret/runtime adapter until an official Python Connector SDK exposes the same contract.

## Raw Temporal payload inspection

Use **dexcli codec-server** with CLI v1.4.2 or later to inspect Dex internal payloads
in Temporal Cloud or a local Temporal Web UI. Follow the shared
[codec-server guidance](../core/operations.md#inspect-raw-temporal-payloads);
local UI ports need no additional server arguments.

## Deployed configuration and recovery boundary

Dex provides no deployed Connector configuration or credential store. A deployed Go
application reads its connections with the Connector SDK's `sdkgo/projectconfig`, but
no equivalent Python SDK exists today; see
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

The current FDG 2.0 management-interface analyzer reads Go source only. A
Python Flow cannot currently publish the Summary, Display, Action, Indexed
Attribute, group, explanation, or Action input capture metadata that drives the
full Dex Web v2 management interface. Python applications can still operate
Flows and use their supported Client and CLI surfaces; do not invent Python
decorators for the Go-only analyzer contract.

## Minimal Flow

[Pinned runnable source](https://github.com/superdurable/dex/blob/sdk-python/v1.15.0/examples/python/dex_examples/primitives/flow/example_flow.py)
<!-- dex-source: examples/python/dex_examples/primitives/flow/example_flow.py -->
```python
status = Attribute("status", str)
notify = Channel("notify", None)


class ExampleFlow(Flow[int]):
    def __init__(self) -> None:
        self.finish = Finish()
        self.example = Example(self.finish)

    def get_steps(self) -> StepList[int]:
        return StepList.start_step(self.example).other_steps(self.finish)

    def get_persistence_schema(self) -> PersistenceSchema:
        return PersistenceSchema.of(status, notify)
```

Instantiate Steps once per Flow object and return those same instances in `StepList`. Define persistent schema objects once at module scope.

A synchronous Step directly returns a decision:

[Pinned runnable source](https://github.com/superdurable/dex/blob/sdk-python/v1.15.0/examples/python/dex_examples/primitives/flow/example_flow.py)
<!-- dex-source: examples/python/dex_examples/primitives/flow/example_flow.py -->
```python
class Finish(Step[int]):
    def execute(self, context: Context, input: int) -> StepDecision:
        status.set(context, "done")
        return graceful_complete(input + 1)
```

## Registry, Worker, Client

The async [application composition](https://github.com/superdurable/dex/blob/sdk-python/v1.15.0/examples/python/dex_examples/app.py) shows Flow construction, Registry, BlobCache, AsyncClient, and AsyncWorker. The [sync example](https://github.com/superdurable/dex/blob/sdk-python/v1.15.0/examples/python/sync-python/sync_examples/app.py) shows the synchronous counterpart. Inject services in constructors; use a provider for a Client needed after circular bootstrap.

## Route by task

- API/state choices: [primitives](primitives.md)
- Application shape: [patterns](patterns.md)
- Real-server tests: [testing](testing.md)
- Client boundary design and exception/retry policy, before implementation: [error handling](error-handling.md)
- codecs/maps/Streams: [data handling](data-handling.md)
- histories/progress: [observability](observability.md)
- open-Flow upgrades: [versioning](versioning.md)
- Python traps: [gotchas](gotchas.md)
- locks/loading/heartbeat/cancellation: [advanced features](advanced-features.md)

## Completion checklist

Verify all Steps and persistent definitions are registered, async/sync contracts are consistent, Context outputs/errors are propagated, Client awaits have deadlines, tests use unique IDs and real Dex, and no application code depends on Server internals.
