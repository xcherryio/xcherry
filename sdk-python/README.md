
# Dex SDK for Python

## Authentication

When Dex authentication is enabled, pass a bearer credential to each Client and
Worker that connects to the server. FileCredential reads the token at construction,
checks its mtime and size at most once per second, and reloads it after rotation.
Whitespace around the token is removed. An unreadable or empty file fails calls
until it is restored; the SDK does not send an old token.

```python
from dex import Client, ClientOptions, FileCredential, Worker, WorkerOptions

credential = FileCredential("/var/run/secrets/dex/token")
client = Client(registry, cache, ClientOptions(credential=credential))
worker = Worker(registry, cache, WorkerOptions(credential=credential))
```

The same option works with AsyncClient and AsyncWorker. Worker credentials cover
startup synchronization, blob hydration, and pull WorkerTaskService streams. A
pull stream reads the token when it opens and again on every reconnect. For a
custom credential, implement Credential.token() to return the current token;
the method may be called concurrently, and an exception fails the call before
it contacts Dex. AsyncClient and AsyncWorker also accept an awaitable token.
Synchronous tokens run in an executor for async calls and on the calling thread
for sync calls. Async calls bound token lookup by the RPC deadline when set;
pull stream opens use a 30-second lookup budget. Sync token lookup must return
promptly. By default, no authorization metadata is sent. Dex uses plaintext
gRPC, so protect the network path that carries the token.

## Pull mode

Set `WorkerOptions.pull` to run a `Worker` or `AsyncWorker` with outbound task
streams. The Worker connects to the Dex server through `server_address` and
does not open a listener. Advertise `worker.worker_target` through
`ClientOptions.worker_target`. A single Flow can override the Client default
with `StartFlowOptions(config_override=FlowConfig(worker_target=...))`; a
SubFlow can use `SubFlowOptions` with the same `FlowConfig` override.

```python
import threading
from dex import Client, ClientOptions, PullWorkerOptions, Worker, WorkerOptions

worker = Worker(
    registry,
    cache,
    WorkerOptions(
        server_address="dex.example:8801",
        pull=PullWorkerOptions(max_concurrent_tasks=8, idle_streams_per_queue=1),
    ),
)
worker_thread = threading.Thread(target=worker.start)
worker_thread.start()
client = Client(
    registry,
    cache,
    ClientOptions(server_address="dex.example:8801", worker_target=worker.worker_target),
)
try:
    client.start_flow(flow, flow_id, input_value)
finally:
    client.close()
    worker.stop()
    worker_thread.join()
```

The Worker polls its sticky queue and every registered Flow type queue. Flow
type names must be unique within a Dex server. `max_concurrent_tasks` defaults
to the CPU count, and `idle_streams_per_queue` defaults to one. At capacity,
the Worker rejects an assignment so Dex can reassign it. Each stream runs one
task at a time; Step heartbeat and stream writes use the same stream. A Worker
may hold up to `(1 + number of Flow types) * (idle_streams_per_queue +
max_concurrent_tasks)` streams because busy streams are replaced with idle
streams and retained after tasks finish. Size these options for the server's
per-Worker stream cap (256 by default).

The generated `app_worker_id` uses the first nonempty `DEX_WORKER_INSTANCE`,
`POD_NAME`, `HOSTNAME`, or system hostname, plus eight random base32 characters.
An override must uniquely identify one Worker process and change after restart.
It must contain 1-63 ASCII letters, digits, dots, underscores, or hyphens and
must not equal a registered Flow type. Pull mode cannot configure a bind address
or an advertised address or headless target. Use `WorkerTarget(pull=True)` for
Client defaults or Flow overrides when the Worker target is not directly in
scope.

Pull streams reconnect after failures. A server-directed reconnect waits for
an active task to finish, then waits up to `stop_drain_timeout` for the server
to close the stream after the final frame. An idle stream reconnects
immediately. An older server without pull support produces one explicit error
log and is retried after one minute. `start()` reports startup
synchronization errors, while later stream errors are logged and retried.
`stop()` drains for at most
`stop_drain_timeout`, which defaults to 30 seconds. Afterward it requests
cancellation and waits for handlers to return. Synchronous handlers should
check `Context.is_cancellation_requested()` during long operations. Streams
that sent a final frame wait for server EOF until the remaining Stop drain
deadline before the channel closes.

## Flow state I/O

Applications read and write Flow state through typed RPCs. Client and AsyncClient
do not expose direct Attribute reads or writes, Channel publication, or
pending-message mutation. This keeps each external state transition behind a
Flow-owned method.

RPC handlers can stage `channel.delete(context, message_id)`. Declare the RPC as
`@rpc(is_transactional=True)` when a missing message must abort its other writes.
Attribute locks already select transactional execution, but Channel deletion
itself does not.

## RPC state loading

RPCs receive ordinary Attributes and all Channel size metadata by default.
AttributeMap entries and pending Channel messages are opt-in:

```python
@dex.rpc(
    load_attribute_map_instances=(items.load("tenant-a"),),
    load_channels=(queued,),
    load_channel_maps=(by_tenant,),
)
def snapshot(self, context: dex.Context) -> dex.RPCResult[Snapshot]:
    messages = queued.pending_messages(context)
    return dex.RPCResult(Snapshot(messages=messages))
```

Put an AttributeMap or ChannelMap directly in its plural load option when every
current instance is needed. Use the singular instance options for the less common
exact-instance case. A selected empty queue returns an empty tuple; reading an
unselected map entry raises **AttributeMapNotLoadedError**. An unselected pending-message
snapshot raises **ChannelMessagesNotLoadedError**.
Pending messages preserve FIFO order and include the server-assigned message ID.
The snapshot does not change after the handler stages a publish or deletion.

Loading controls which data reaches the Worker. **is_transactional** controls
atomic commit and Channel deletion validation. Attribute locks add isolation
only among cooperating Steps and RPCs using the same lock. Write-only publish
and delete operations do not require loading.

### Invocation-time map instances

The `@rpc` decorator remains the registration-time contract for timeout,
transactionality, locks, and fixed state loads. Pass frozen `RPCInvokeOptions`
through the keyword-only `options` argument when the caller chooses an exact map
instance at runtime:

```python
options = dex.RPCInvokeOptions(
    lock_attribute_map_instances=(profiles.lock(partition_name),),
    load_attribute_map_instances=(profiles.load(partition_name),),
)
result = client.invoke_rpc(
    directory.upsert_customer_profile,
    flow_id,
    profile,
    options=options,
)
```

`Client` and `AsyncClient` union, sort, and deduplicate these selections with the
registered ones. Locking and loading are independent; read-modify-write handlers
need both for the same instance.

See the runnable [sequential chunking](../examples/python/dex_examples/patterns/sequentially-chunked-attribute-map)
and [hash partitioning](../examples/python/dex_examples/patterns/hash-partitioned-attribute-map)
patterns for complete async call sites.

## Step and timeout-handler state loading

`StepOptions` provides the same five selections independently for `wait_for` and
`execute`: all AttributeMap instances, exact AttributeMap instances, Channels,
all ChannelMap instances, and exact ChannelMap instances. The methods receive
independent snapshots. Execute reads after the winning Wait consumes messages;
retries of one logical method call reuse its first snapshot.

`FlowTimeoutHandlerOptions` provides Execute-style timeouts, heartbeat detection,
retry, durability, Attribute locks, and the same state selections. Set it on
`StartFlowOptions` or `SubFlowOptions` only for a positive timeout using `HANDLER`.
Exhausted retries may proceed to a registered `Step[None]`; read the final
failure from `Context.recovery_error`.

```python
class TimeoutRecoveryStep(Step[None]):
    def execute(self, context: Context, input: None) -> StepDecision:
        failure = context.recovery_error
        if failure is None:
            return force_fail("timeout recovery has no failure")
        return force_fail(failure.detail)
```

Python SDK for [Dex workflow engine](https://github.com/superdurable/dex)

## New user contracts

The rewrite targets Python 3.11+ and exposes strongly typed workflow contracts
from `dex`. This phase includes definitions, attributes, channels, streams, waits,
decisions, codecs, registry validation, synchronous client calls, and synchronous
worker handlers. Python owns its gRPC Client and Worker transport;
the shared Rust Core is used only for BlobCache.

```python
from datetime import timedelta
from typing import Generator

import dex

counter = dex.Attribute("counter", int)
counters_by_region = dex.AttributeMap("counters-by-region", int)
progress = dex.Stream("progress", str, 10 * 1024 * 1024)

class Run(dex.Step[str]):
    def wait_for(
        self, context: dex.Context, input: str
    ) -> dex.Wait:
        return dex.Wait.until(
            dex.Timer.by_duration(timedelta(seconds=1))
        )

    def execute(
        self, context: dex.Context, input: str
    ) -> Generator[dex.StepOutput, None, dex.StepDecision]:
        yield progress.write(context, "running")
        yield dex.heartbeat({"phase": "running"})
        return dex.graceful_complete(input)

class CounterFlow(dex.Flow[str]):
    run = Run()

    def get_flow_type(self) -> str:
        return "Counter"

    def get_steps(self) -> dex.StepList[str]:
        return dex.StepList.start_step(self.run)

    def get_persistence_schema(self) -> dex.PersistenceSchema:
        return dex.PersistenceSchema.of(counter, counters_by_region, progress)

    @dex.rpc(name="Increment")
    def increment(
        self, context: dex.Context, input: int
    ) -> dex.RPCResult[int]:
        return dex.RPCResult(input + 1)

flow = CounterFlow()
registry = dex.Registry((flow,))
```

Registry derives codecs from declared Python types and handler annotations.
Built-in primitive types and dataclasses need no codec arguments. Register an
explicit codec only for a custom encoding or a type Registry cannot derive.
`PersistenceSchema.of(...)` accepts attributes, channels, and streams together and
partitions them by definition type.

`Worker` and `AsyncWorker` synchronize all registered Indexed Attributes with
Dex Server before opening their listener. Existing indexes return immediately;
failure or the default two-minute deadline aborts startup. An indexed
`AttributeMap` must provide one fixed `index_key`.

Initial attributes retain their value types without a public wrapper class:

```python
options = (
    dex.StartFlowOptions()
    .with_attribute(counter, 1)
    .with_attribute(counters_by_region, "us-west", 1)
)
```

Opt in when declaring an Attribute or AttributeMap, and select the Store in
Flow configuration:

```python
email = dex.Attribute("customer-email", str, sync_to_attribute_store=True)
config = dex.FlowConfig(attribute_store_names=["profiles", "audit"])
```

Stores are asynchronous latest-state projections. Every enabled Attribute write
is sent to every selected Store. Deletion writes SQL `NULL`, and projection
failures do not roll back Flow Attributes. `None` preserves current targets;
an explicit empty list disables future synchronization while retaining protocol
presence.

```
pip install dex-python-sdk==0.1.0
```

See [samples](../examples/python) for use case examples.

## Requirements

- Python 3.11+
- [Dex server](https://github.com/superdurable/dex#how-to-use)

## Concepts

Applications implement two generic interfaces from [`dex`](dex/):

- `Flow[START_INPUT]` returns `StepList.start_step(...)`, followed by optional
  `.other_steps(...)`, from one `get_steps()` method. The `StepList` generic
  binds the Flow input to the starting Step input. Use `StepList.empty()` when
  a Flow has no Steps.
- `Step[INPUT]` implements `execute` and optionally `wait_for`. The default
  Worker accepts ordinary synchronous handlers and generator handlers. A generator
  yields `StepOutput` progress frames and returns its final `Wait` or `StepDecision`.
  With `AsyncWorker` and `Registry(..., allow_async_handlers=True)`, Step
  coroutines use `AsyncContext`; async RPCs keep `Context`.

`StepOptions.wait_for_method_timeout` and `execute_method_timeout` bound the
two handler calls. Timer and channel conditions determine how long a Step waits.

`wait_for_retry` and `execute_retry` limit one logical handler execution. With
`StepDurability.ASYNC`, local and fallback regular activities share maximum
attempts, total duration, and 1-based attempt numbers. Fallback starts
immediately; later regular retries continue the backoff sequence at the
cumulative attempt.

The default Step durability is synchronous. A Flow configuration can select
asynchronous durability, and a Step method override has highest precedence. The
default retry total duration is four hours. Regular attempts default to a two-hour
method timeout and one-minute heartbeat timeout; an explicit heartbeat timeout must
meet the server minimum, which defaults to ten seconds. Asynchronous durability
first allows at most three local attempts in seven seconds. The local phase ignores
method timeouts and heartbeat frames before falling back to a regular activity.

### Step progress and heartbeat recovery

A synchronous handler yields every heartbeat and Stream write. The generator return
value is the only final result:

```python
def execute(
    self, context: dex.Context, input: str
) -> Generator[dex.StepOutput, None, dex.StepDecision]:
    yield dex.heartbeat({"offset": 10})
    yield progress.write(context, "processed 10 items")
    return dex.graceful_complete(input)
```

An asynchronous handler keeps its normal coroutine return. Stream writes enqueue
without waiting for Stream Store acknowledgement; heartbeat waits only for the
Worker output queue:

```python
async def execute(
    self, context: dex.AsyncContext, input: str
) -> dex.StepDecision:
    writer = progress.buffered_text(context)
    writer.write("started")
    await context.heartbeat({"offset": 10})
    return dex.graceful_complete(input)
```

The async buffered writer has a synchronous `write` method, so `writer.write`
can be passed directly as an LLM delta callback. It flushes after one second, at
a soft 16 KiB UTF-8 threshold, or before the final result or error. It
concatenates chunks exactly and ignores empty chunks.

A synchronous generator uses the cooperative form because only yielded
`StepOutput` values can reach gRPC:

```python
writer = progress.buffered_text(context)
yield from writer.write(delta)
yield from writer.flush()
```

Its interval is checked by the next write, and the handler must explicitly
flush the tail. Retry does not restore either writer's unsent buffer or
deduplicate sent batches.

Call `heartbeat()` or `await context.heartbeat()` without a value to clear previous
details. Passing Python `None` persists a present null Value. On a later regular
attempt, use `context.has_last_heartbeat_value()` before decoding with
`context.get_last_heartbeat_value(ExpectedType)`. A Stream frame is also an implicit
heartbeat, but it preserves the latest explicit heartbeat state.

A Step may write the same Stream any number of times. Dex assigns
`#<stepExecutionID>` as each Step message's `StreamMessage.source`. Client writes
provide their own non-empty source; duplicate sources and `#` are allowed and every
write appends:

```python
client.write_stream(flow_id, progress, "frontend#preview", "rendering")
message = client.read_stream(flow_id, progress)
print(message.source)
```

`Client.read_stream` and `AsyncClient.read_stream` move forward one message at
a time and can long-poll. Their `list_stream_messages` methods return one
non-blocking newest-first retained page. Pass the registered `Stream`, a
positive page size, and an empty token for the first page. Pass
`StreamMessagesPage.next_page_token` unchanged to read older messages until it
is empty. The server caps page size at 1000 by default. A trimmed anchor returns
an empty page, and newer concurrent writes do not enter an existing older-page
chain.

### Canceling Step executions

A successful Step can cancel queued or active executions while continuing with
its normal decision:

```python
return (
    dex.go_to(RecordQuote, quote)
    .with_canceling_sibling_steps(QuoteCarrierA, QuoteCarrierB)
    .with_canceling_steps(GlobalQuoteTimeout)
)
```

`with_canceling_steps` selects every current execution of each registered Step
type. `with_canceling_sibling_steps` selects only executions with the same
`Context.from_step_execution_id` as the current execution. Decisions are
immutable; repeated calls form a union, and Flow-wide selection wins for the
same Step type. Unregistered selectors produce an invalid Step result.

Dex resolves one snapshot after the current execution succeeds. Completed,
already-canceled, and absent targets are no-ops. Next Steps created by the same
decision are outside the snapshot. Dex immediately applies the next or close
action; late decisions, writes, retries, and recovery Steps are discarded.

`RPCResult.with_canceling_steps` provides the Flow-wide selector for RPCs.
RPCs do not support sibling selection because they have no Step execution
lineage.

### Soft Flow timeout

Override `Flow.handle_timeout` to make a positive timeout use handler policy
by default. Both synchronous and async Workers support the hook:

```python
class Orders(dex.Flow[str]):
    async def handle_timeout(self, context: dex.Context) -> dex.StepDecision:
        await notify_expiration(context)
        return dex.force_complete("expired")

options = dex.StartFlowOptions(
    timeout=timedelta(minutes=30),
    timeout_policy=dex.FlowTimeoutPolicy.HANDLER,
)
```

Register async hooks with `allow_async_handlers=True` and run them with
`AsyncWorker`. `FAIL` produces `FlowErrorType.FLOW_TIMEOUT` and permits Flow
retry; `CANCEL` cancels without retry. Continue-as-new preserves the deadline,
while retry runs receive a fresh budget. A zero or absent timeout
disables the feature.

`Registry` validates every Flow, Step, RPC signature, durable name, lock, and
codec before Client or Worker startup. `Client` methods use these typed objects
instead of raw Flow, Step, or RPC strings.

### Waiting and map inspection

`Wait.until`, `Wait.all_of`, and `Wait.any_of` use unnamed Conditions by
default. Do not add condition IDs merely because a Condition is nested in one
of these waits. Every Condition in `Wait.any_combination_of` must have a
non-empty user ID; the same Condition instance may appear in multiple
combinations.

Both `Client` and `AsyncClient` provide singleton and AttributeMap-instance
overloads of `wait_for_attribute_match`. Build matches with the six
`AttributeMatch` factories. The method targets the current run and returns the
decoded matched value. String and bool support equality operators; int and
float support every operator. JSON objects, bytes, null, non-finite floats, and
invalid ordering fail before transport. Every AttributeMap and ChannelMap instance must be non-empty
and must not contain `/`. `AttributeMap.get_map_size/get_all_instance_keys` include
buffered sets and deletes. The matching `ChannelMap` methods are RPC-only,
include buffered publishes, and omit empty instances. Keys are decoded and
sorted. Use `force_complete_if_channels_empty(...)` for conditional completion.

Request IDs are optional for both durable waits. When omitted, the server
derives a stable ID from the Step execution or Attribute condition. Reuse an
override only for the same wait. `request_timeout` is the total SDK call budget
across transparent transport reattachments. Zero waits indefinitely.
Temporal permits 10 in-flight Updates and 2,000 total Updates in History per
Workflow Execution. A caller timeout or cancellation does not finish an
accepted durable wait, so an abandoned handler can retain one in-flight slot.
Set `internal_handler_timeout` only when abandoned waits can approach that
limit. An active call transparently starts another generation, which counts
toward the 2,000-Update history limit. Prefer a value comfortably longer than
normal request timeouts and reconnect gaps. Zero disables timed rollover.
Request expiry raises `RequestTimeoutError`.

`Client.wait_for_flow` and `AsyncClient.wait_for_flow` return a
`FlowResult` after hydrating every output-bearing completion. Use
`single_output` only when the Flow contract produces exactly one output:

```python
output = client.wait_for_flow(flow_id).single_output(OrderResult)

result = client.wait_for_flow(flow_id)
for completion in result.completions:
    if completion.step_execution_id == expected_execution_id:
        output = completion.decode(OrderResult)
```

`completions` is an immutable tuple in server collection order. Parallel branch
order is not deterministic, so select by `step_type` or `step_execution_id`.
No-output Flows return an empty tuple; `single_output` raises `ValueError` for
zero or multiple completions. Every terminal status returns a `FlowResult`; inspect
`status`, `error_type`, and `error_message` for unsuccessful completion.

SubFlows are normal, independently addressable Flows used as durable Conditions:

```python
def wait_for(self, context: Context, input: ChargeInput) -> Wait:
    return Wait.until(SubFlow.run(self.charge_flow, input))

def execute(self, context: Context, input: ChargeInput) -> StepDecision:
    del input
    receipt = SubFlow.get_condition_results(context).single_output(Receipt)
    return graceful_complete(receipt)
```

`SubFlow.get_flow_id(context, index=0)` remains available for a running `any_of`
loser. `SubFlowOptions` configures timing, timeout policy, retry, initial target
Attributes, Flow config, Condition ID, and reuse. Parent completion does not cancel an unfinished
SubFlow.

### Errors

Client calls raise concrete `DexServiceError` subclasses. Existing-Flow reads
(`describe_flow`, `wait_for_flow`, and `time_travel`) raise
`FlowNotFoundError` when the Flow does not exist. Mutations, RPCs, timer/Step
waits, config updates, and continue-as-new triggers raise
`FlowNotActiveOrNotFoundError` when no running Flow can accept the operation.

RPCs also use this error when the target Flow cannot be found, including query-only
reads. A query-only RPC without locks, transactional execution, durable effects, or
Server-forced Update routing can read a retained terminal execution. At that confirmed
read boundary, this error means no readable target was found; return the application's
missing or unavailable result directly, without a lifecycle probe, a short timeout, a
retry, or historical Step-output decoding. Preserve other service and Worker failures.
For mutations and active-only RPCs, the error can mean either missing or closed and does
not prove that the requested action succeeded. RPC success does not prove that the Flow
is active.

```python
try:
    client.invoke_rpc(orders.update_order, order_id, update)
except dex.FlowNotActiveOrNotFoundError:
    # The Flow is missing or already closed.
    pass
```

Duplicate starts, worker failures, RPC lock contention, and long-poll timeouts
raise `FlowAlreadyStartedError`, `WorkerInvocationError`,
`RpcLockConflictError`, and `LongPollTimeoutError`. All service errors retain
`code`, `sub_status`, `detail`, `operation`, `flow_id`, and the original gRPC
exception through Python exception chaining. Worker failures also expose
`worker_code`, `worker_error_type`, and `worker_error_detail`. Registration,
serialization, and invalid handler returns use `FlowDefinitionError`,
`ValueMappingError`, and `InvalidStepResultError`.

### Sync vs asyncio

- **Sync (default):** `Client` and `Worker` use blocking gRPC and a thread-pool
  Worker. A progress generator cooperatively hands each yielded frame to gRPC;
  `Stream.write` must therefore be yielded. Blocking Client calls inside
  `Step.execute` are safe while other pool threads remain available.
- **Asyncio:** `AsyncClient` and `AsyncWorker` use `grpc.aio`. Use
  `Registry(..., allow_async_handlers=True)` when Steps/RPCs are coroutines.
  Step coroutines annotate `AsyncContext`, call `Stream.write` without `await`,
  and await `context.heartbeat`. Inside async `execute`, inject `AsyncClient` —
  do not call sync `Client` on the Worker event loop. Async generators are rejected.

Integration scenarios live under
[`tests/integ`](tests/integ/README.md). They exercise the same workflows,
client operations, and assertions as the Java suite against an isolated
`dexcli dev` environment.

## Implementation status

The strongly typed contracts, registry, synchronous Client/Worker, optional
`AsyncClient`/`AsyncWorker` (`grpc.aio`), and Rust-backed BlobCache are
implemented. Python owns its gRPC transport; the native bridge is limited to
the shared BlobCache. Serialized object values use wire encoding `json` for JSON
and `raw` for raw bytes. Internal Blob references are opaque. Hydration sends the
owning Flow ID with each reference, and the cache is isolated by
`(flow_id, blob_ref)`. Design notes:
[`python-sdk-async-apis.md`](../docs/design/plan/python-sdk-async-apis.md) and
[`python-sdk-step-streaming.md`](../docs/design/plan/python-sdk-step-streaming.md).

## Running Dex locally

Install and start the complete local environment with `dexcli`:

```bash
brew install superdurable/tap/dexcli
dexcli dev
```

Dex Server listens on `127.0.0.1:8801`. See the
[CLI README](../cli/README.md) for endpoints and persistence options.

## How To Contribute

This project uses [uv](https://docs.astral.sh/uv/) for Python versions,
dependencies, virtual environments, locking, building, and publishing.

To install requirements:

```bash
uv sync --locked
```

Run the complete Python SDK integration suite with an isolated Dex development
environment:

```bash
./run-integration-tests.sh
```

### Measure integration coverage

Run the same integration suite with Python source coverage:

```bash
./run-integration-tests.sh --coverage
```

Only the integration scenarios contribute execution data, and only production
Python modules under `dex` are measured. Generated protobuf modules under
`dex/dexpb` are excluded. The
terminal report lists uncovered line ranges. The browser report starts at
`coverage/html/index.html`; `coverage/coverage.xml` and `coverage/lcov.info`
are also generated.

CI uploads LCOV to Codecov with GitHub OIDC under the
`sdk-python-integration` flag and retains the full report as the
`sdk-python-integration-coverage` Actions artifact.

#### Update IDL

Edit [`protos/dex.proto`](../protos/dex.proto). Rename catalog: [`docs/design/idl-renames.md`](../docs/design/idl-renames.md).

#### Generate stubs from IDL

```bash
make -C ../protos proto-python
```

Checked-in Python stubs land in `dex/dexpb/`.
#### Linting

Validate that every `dex.__all__` class, function, constant, public method,
argument, return value, dataclass field, enum value, and public instance
attribute has a Google-style docstring:

```bash
uv run --frozen python scripts/check_public_docs.py
```

The checker resolves definitions from the public package export table, so
private helpers and generated protobuf modules are excluded. Use `help(dex.Client)`
or IDE hover information to read the same documentation. To run all other
linting for this project:

```bash
uv run --frozen pre-commit run --show-diff-on-failure --color=always --all-files
```

## Code of Conduct
This project is governed by the [Contributor Covenant v 1.4.1](CODE_OF_CONDUCT.md). (Review the Code of Conduct and remove this sentence before publishing your project.)

## Publishing to PyPI

1. Optionally run **Publish Python SDK to PyPI** via workflow_dispatch with a version and
   `publish=false` to validate all distributions without uploading.
2. Create a GitHub Release with tag `sdk-python/vX.Y.Z` (for example `sdk-python/v0.1.0`).
   CI stamps that version into `pyproject.toml` for the build (same idea as the TypeScript
   SDK release), then builds and smoke-tests Linux x86_64/ARM64, macOS x86_64/ARM64, and
   Windows x86_64 wheels, verifies the source distribution, and publishes with `PYPI_TOKEN`.
3. After publishing, bump the committed `pyproject.toml` / docs install line when you want
   the repo tip to reflect the released version.

A manual run publishes only from `main`, and only when `publish` is explicitly selected.
The dispatch `version` input is stamped the same way as a release tag.

See [CONTRIBUTING.md](../CONTRIBUTING.md#releases-monorepo-tags) for monorepo tag conventions.

## Server protocol compatibility

Both `Worker` and `AsyncWorker` call `GetServerInfo`, negotiate the highest
common protocol, synchronize Attribute indexes, and only then bind
WorkerService. The initial Python SDK interval is `[1,1]`. Missing information,
invalid or disjoint intervals, and RPC failures stop startup before binding.

The diagnostic SDK version comes from Python distribution metadata for
`dex-python-sdk` and falls back to `dev` when the distribution is unavailable.
It does not affect protocol compatibility.

## License

[Sustainable Use License 1.0](LICENSE), with legacy portions under their
original terms as described in [LEGACY_NOTICES.md](LEGACY_NOTICES.md).

## Search run inclusion

Dex Server excludes ContinuedAsNew runs by default. Application queries still
need `FlowType` when using shared index slots. An explicit inclusion option
removes only the default exclusion; it does not override query filters. Keep
the query, inclusion option, and page size consistent across pages.

Pass keyword-only `include_continued_as_new=True` to synchronous or asynchronous `search_flows` for execution-chain inspection. The default is `False`.
