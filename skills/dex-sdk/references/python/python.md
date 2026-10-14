# Python handbook

The SDK API reference uses the [released v1.4.0 baseline](../core/bundle-baselines.md); the source examples' older dependency pins below remain factual and do not define the current error API.

Use this page first for Python. The [baseline project](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/python/pyproject.toml) selects `dex-python-sdk==1.2.1`; check `pyproject.toml`/lockfile and installed metadata before relying on baseline syntax.

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

[Pinned runnable source](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/python/dex_examples/primitives/flow/example_flow.py)
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

[Pinned runnable source](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/python/dex_examples/primitives/flow/example_flow.py)
<!-- dex-source: examples/python/dex_examples/primitives/flow/example_flow.py -->
```python
class Finish(Step[int]):
    def execute(self, context: Context, input: int) -> StepDecision:
        status.set(context, "done")
        return graceful_complete(input + 1)
```

## Registry, Worker, Client

The async [application composition](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/python/dex_examples/app.py) shows Flow construction, Registry, BlobCache, AsyncClient, and AsyncWorker. The [sync example](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/python/sync-python/sync_examples/app.py) shows the synchronous counterpart. Inject services in constructors; use a provider for a Client needed after circular bootstrap.

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
