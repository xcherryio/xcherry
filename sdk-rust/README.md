# Dex Rust SDK

## Authentication

Set a credential on each Client and Worker when Dex authentication is enabled.
`FileCredential::new` reads a bearer token file at construction and checks for
rotation at most once per second. It trims surrounding whitespace. Missing,
unreadable, or empty files fail requests instead of sending a stale token.
The same credential can be shared across threads and across a Client and Worker.

```rust
use std::sync::Arc;
use dex_sdk::{ClientOptions, FileCredential, WorkerOptions};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let credential = Arc::new(FileCredential::new("/var/run/secrets/dex/token")?);
    let client_options = ClientOptions::new().credential(credential.clone());
    let worker_options = WorkerOptions::new().credential(credential);
    Ok(())
}
```

The Client sends the token with every FlowService call. The Worker sends it with
FlowService calls, including startup synchronization and blob hydration, and
when each pull task stream opens. Reconnected streams use the current token.
Without a credential, the SDK sends no authorization metadata. Dex uses
plaintext gRPC, so protect the network path carrying the token.

## Flow state I/O

Applications read and write Flow state through typed RPCs. The Client does not
expose direct Attribute reads or writes, Channel publication, or pending-message
mutation. This keeps each external state transition behind a Flow-owned method.

An RPC can stage `channel.delete(context, message_id)`. Define it with
`Rpc::is_transactional()` when a missing ID must abort all other RPC writes.
Attribute locks already select transactional execution, while Channel deletion
requires the explicit option.

## RPC state loading

RPCs receive ordinary Attributes and all Channel size metadata by default.
AttributeMap entries and pending Channel messages are opt-in:

```rust
RpcList::new().procedure(
    Self::SNAPSHOT
        .load_attribute_map_instance(items.load("tenant-a"))
        .load_channel(&queued)
        .load_channel_map(&by_tenant),
    Self::snapshot,
)
```

Pass an AttributeMap or ChannelMap definition to its plural load method when
every current instance is needed. Use the singular instance methods for the less
common exact-instance case. A selected empty queue returns an empty vector;
reading an unselected map entry or pending-message snapshot returns a handler
error with type **dex_sdk::AttributeMapNotLoadedError**. An unselected pending-message
snapshot returns **dex_sdk::ChannelMessagesNotLoadedError**. Pending messages preserve FIFO
order and include the server-assigned message ID. The snapshot does not change
after the handler stages a publish or deletion.

Loading controls which data reaches the Worker. **is_transactional** controls
atomic commit and Channel deletion validation. Attribute locks add isolation
only among cooperating Steps and RPCs using the same lock. Write-only publish
and delete operations do not require loading.

### Invocation-time map instances

`RpcDefinition` remains the registration-time contract for timeout,
transactionality, locks, and fixed state loads. Use `RpcInvokeOptions` when the
caller chooses an exact map instance at runtime:

```rust
let options = RpcInvokeOptions::new()
    .lock_attribute_map_instance(profiles.lock(&partition_name))
    .load_attribute_map_instance(profiles.load(&partition_name));
let result = client.invoke_rpc_with_options(
    flow_id,
    CustomerDirectoryFlow::UPSERT_CUSTOMER_PROFILE,
    profile,
    options,
)?;
```

The Client unions, sorts, and deduplicates these selections with the registered
ones. Locking and loading are independent; read-modify-write handlers need both
for the same instance.

See the runnable [sequential chunking](../examples/rust/src/patterns/sequentially_chunked_attribute_map)
and [hash partitioning](../examples/rust/src/patterns/hash_partitioned_attribute_map)
patterns for complete call sites.

## Step and timeout-handler state loading

`StepOptions` provides the same five selections independently for `wait_for` and
`execute`: all AttributeMap instances, exact AttributeMap instances, Channels,
all ChannelMap instances, and exact ChannelMap instances. The methods receive
independent snapshots. Execute reads after the winning Wait consumes messages;
retries of one logical method call reuse its first snapshot.

`FlowTimeoutHandlerOptions` provides Execute-style timeouts, heartbeat detection,
retry, durability, Attribute locks, and the same state selections. Set it on
`StartFlowOptions` or `SubFlowOptions` only for a positive timeout using
`FlowTimeoutPolicy::Handler`. Exhausted retries may proceed to a registered
`Step<Input = ()>`; read the final failure with `Context::recovery_error`.

[![Rust SDK CI](https://github.com/superdurable/dex/actions/workflows/sdk-rust-ci.yml/badge.svg?branch=main)](https://github.com/superdurable/dex/actions/workflows/sdk-rust-ci.yml)

This workspace contains the synchronous Rust SDK, the shared DXBC BlobCache,
and the generated Rust protocol used by the native bindings.

The crates are:

- `dex-sdk`: strongly typed synchronous Client, Worker, Flow, Step, RPC, and persistence APIs.
- `dex-blob-cache`: transport-neutral, Go-compatible disk cache.
- `dex-blob-cache-jni`: Java 8-compatible binding containing only cache APIs.
- `dex-blob-cache-python`: PyO3 binding for the Python SDK.
- `dex-blob-cache-node`: Node-API binding for the TypeScript SDK.
- `dex-protocol`: generated Rust protobuf and gRPC protocol.

The architecture is defined in
[Multi-language Rust SDK Core](../docs/design/multi-language-rust-sdk-core.md).
The public Rust API is defined in
[Rust SDK User Interface](../docs/design/rust-sdk-user-interface.md).

The `dex-sdk` source layout follows the application developer's mental model:
Flows, Steps, Attributes, Channels, Streams, RPCs, timers, and waits each have dedicated
modules. Client, Worker, Registry, and each options family are separated as
their own entry points instead of being collected into infrastructure-oriented
files. Handler failures and SDK/service failures are also separate modules.

Single-condition waits read as `Wait::until(condition)`. `Wait::all_of` and
`Wait::any_of` remain available for aggregate conditions. Client failures use
domain-specific `SdkError` variants such as `FlowNotFound`, `FlowNotActiveOrNotFound`,
`FlowAlreadyStarted`, `RpcLockConflict`, and `WorkerInvocation` instead of
requiring callers to inspect transport metadata.

`Wait::until`, `Wait::all_of`, and `Wait::any_of` use unnamed Conditions by
default. Do not add Condition IDs merely because a Condition is nested in one
of these waits. `Wait::any_combination_of` requires a non-empty user ID on every
Condition; a cloned Condition retains its identity and may be reused across
combinations.

`Client::wait_for_attribute_match` and
`Client::wait_for_attribute_map_instance_match` target the current run and
return the decoded matched value. Build a match with one of the six
`AttributeMatch` factories. String and bool support equality operators; integer
and double support every operator. JSON, bytes, null, non-finite doubles, and
invalid ordering return a local `InvalidArgument`. Every AttributeMap and ChannelMap instance must
be non-empty and must not contain `/`. `AttributeMap::map_size/all_instance_keys` include
buffered sets and deletes. The matching `ChannelMap` methods are RPC-only,
include buffered publishes, and omit empty instances. Keys are decoded and
sorted. Conditional completion is
`StepDecision::force_complete_if_channels_empty`.

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
Request expiry returns `SdkError::RequestTimeout`.

`Client::wait_for_flow` and `wait_for_flow_with_timeout` return a
`FlowResult` after hydrating every output-bearing completion. Use the
strict helper for a single-output Flow:

```rust
let output: OrderResult = client.wait_for_flow(flow_id)?.single_output()?;

let result = client.wait_for_flow(flow_id)?;
for completion in result.completions() {
    if completion.step_type == "ChargeCard" {
        let receipt: Receipt = completion.decode()?;
    }
}
```

The completion slice preserves server collection order, but parallel branch
order is not deterministic. No-output Flows return an empty slice;
`single_output` returns `SdkError::InvalidArgument` for zero or multiple
completions. Every terminal status returns a `FlowResult`; inspect `status`,
`error_type`, and `error_message` for unsuccessful completion.

SubFlows are normal, independently addressable Flows used as durable Conditions:

```rust
fn wait_for(&self, _context: &mut Context, input: ChargeInput) -> HandlerResult<Wait> {
    let condition = SubFlow::run(&ChargeFlow::new(), input)
        .map_err(|error| HandlerError::new(error.to_string()))?;
    Ok(Wait::until(condition))
}

fn execute(&self, context: &mut Context, _input: ChargeInput) -> HandlerResult<StepDecision> {
    let receipt: Receipt = SubFlow::condition_result(context)?
        .single_output()
        .map_err(|error| HandlerError::new(error.to_string()))?;
    Ok(StepDecision::graceful_complete(receipt))
}
```

`SubFlow::flow_id_at(context, index)` remains available for a running `any_of` loser.
`SubFlowOptions` configures timing, timeout policy, retry, initial target Attributes,
Flow config, Condition ID, and reuse. Parent completion does not cancel an unfinished SubFlow.

Existing-Flow reads (`describe_flow`, `wait_for_flow`, and
`time_travel`) use `FlowNotFound`; operations requiring a running Flow use
`FlowNotActiveOrNotFound`. Each remote variant owns a `ServiceError`, available through
`SdkError::service_error()`, with gRPC code, Dex sub-status, detail, operation,
Flow ID, and the original `tonic::Status` source. `WorkerInvocation` also owns
a `WorkerError` with the original worker code, type, and detail.

RPCs also use this error when the target Flow cannot be found, including query-only
reads. A query-only RPC without locks, transactional execution, durable effects, or
Server-forced Update routing can read a retained terminal execution. At that confirmed
read boundary, this error means no readable target was found; return the application's
missing or unavailable result directly, without a lifecycle probe, a short timeout, a
retry, or historical Step-output decoding. Preserve other service and Worker failures.
For mutations and active-only RPCs, the error can mean either missing or closed and does
not prove that the requested action succeeded. RPC success does not prove that the Flow
is active.

```rust
match client.invoke_rpc(flow_id, Orders::UPDATE, update) {
    Err(SdkError::FlowNotActiveOrNotFound { service }) => {
        eprintln!("{} failed for {:?}", service.operation(), service.flow_id());
    }
    result => result?,
}
```

`Registry::register` reports invalid definitions as `SdkError::FlowDefinition`.
Value conversion and invalid Step results use `ValueMapping` and
`InvalidStepResult`.

### Soft Flow timeout

A Flow registers its optional handler through `timeout_handler`. Its presence
makes a positive timeout use handler policy by default:

```rust
fn handle_timeout(&self, context: &mut Context) -> HandlerResult<StepDecision> {
    notify_expiration(context)?;
    Ok(StepDecision::force_complete("expired".to_string()))
}

fn timeout_handler(&self) -> Option<FlowTimeoutHandler<Self>> {
    Some(Self::handle_timeout)
}

let options = StartFlowOptions::new()
    .timeout(Duration::from_secs(30 * 60))
    .timeout_policy(FlowTimeoutPolicy::Handler);
```

`Fail` produces `FlowErrorType::FlowTimeout` and permits Flow retry; `Cancel`
cancels without retry. Continue-as-new preserves the deadline and handler
execution, while retry runs receive a fresh budget. An absent or zero
timeout disables the feature.

## Rust SDK runtime

Flows bind their start input and every Step binds its own input at compile time:

```rust
use dex_sdk::{Context, Flow, HandlerResult, Step, StepDecision, StepList};

struct GreetingFlow {
    greet: Greet,
}

impl Flow for GreetingFlow {
    type StartInput = String;

    fn steps(&self) -> StepList<'_, Self::StartInput> {
        StepList::start(&self.greet)
    }
}

struct Greet;

impl Step for Greet {
    type Input = String;

    fn execute(
        &self,
        _context: &mut Context,
        name: String,
    ) -> HandlerResult<StepDecision> {
        Ok(StepDecision::graceful_complete(format!("Hello, {name}!")))
    }
}
```

`Client` calls block and return their final result. `Worker::start` serves until
another thread calls `Worker::stop`. Synchronous user handlers run on Tokio's
blocking executor, so they do not occupy gRPC I/O tasks. Long-running Step
handlers should emit heartbeats or Stream messages while polling
`Context::is_cancelled`. Cancellation becomes observable when the Worker
response stream closes.

### Pull mode

Set `WorkerOptions::pull` to open outbound task streams instead of a WorkerService
listener. Advertise `worker.worker_target().clone()` through
`ClientOptions::worker_target`, or use `FlowConfig::worker_target` in a
`StartFlowOptions` or `SubFlowOptions` override for one Flow. Pull mode cannot
be combined with a bind address or an advertised listening target.

```rust
let worker = Worker::try_new(
    registry.clone(),
    cache.clone(),
    WorkerOptions::new()
        .server_address("dex.example:8801")
        .pull(PullWorkerOptions::new()
            .max_concurrent_tasks(8)
            .idle_streams_per_queue(1)),
)?;
let client = Client::try_new(
    registry,
    cache,
    ClientOptions::new()
        .server_address("dex.example:8801")
        .worker_target(worker.worker_target().clone()),
)?;
```

The Worker polls its sticky queue and every registered Flow type queue. Flow
type names must be unique within a Dex server. The task limit defaults to the
CPU count, and each queue keeps one idle stream by default. A stream runs one
task at a time. At capacity, the Worker rejects an assignment for reassignment.
The generated app Worker ID uses the first nonempty `DEX_WORKER_INSTANCE`,
`POD_NAME`, or `HOSTNAME` value, or the system hostname (falling back to
`worker`), plus eight random base32 characters. An override must identify one
Worker process, change after restart, contain 1–63 ASCII letters, digits, dots,
underscores, or hyphens, and differ from every registered Flow type.

The Worker reconnects after stream failures and drains an active task before a
server-directed reconnect. Registration failures back off; an older server
without pull support logs one explicit error and retries. `Worker::start`
returns startup errors, while later polling failures are logged and retried.
A permanent registration error waits one minute. If the next attempt fails
transiently before registration, that retry also waits one minute; subsequent
transient retries resume at the five-second cap. A registered stream or a
server-directed reconnect resets backoff to 100 ms, with jitter on every retry.
The Worker may hold up to
`(1 + number of Flow types) * (idleStreamsPerQueue + maxConcurrentTasks)`
streams because busy streams are kept alongside each queue's idle streams.
Size both options within the server's per-Worker stream cap (256 by default)
and per-queue stream limits.

`Worker::stop` begins a graceful drain. After 30 seconds by default, the Worker
cancels active handlers; `Worker::start` waits for them to return before it
exits. Set `PullWorkerOptions::stop_drain_timeout` to change that deadline.
Handlers should check `Context::is_cancelled` during interruptible work.

### Step progress and Streams

WaitFor and Execute handlers can send progress before returning:

```rust
if let Some(checkpoint) = context.last_heartbeat_value::<Checkpoint>()? {
    resume_from(checkpoint)?;
}
context.record_heartbeat_value(Checkpoint::Saved { offset })?;
events.write(context, Event::Imported { offset })?;
context.record_heartbeat()?;
```

`record_heartbeat_value` persists a typed checkpoint for the next regular activity attempt.
`record_heartbeat` sends a heartbeat without a Value and clears the persisted details. A missing
Value and a null Value remain distinguishable by decoding `Option<T>`: the latter returns
`Some(None)`. Local activities transmit heartbeat frames but Dex ignores their values.

Each call blocks only when the Worker's single-frame output buffer is full. This bounded
backpressure preserves handler order without unbounded memory. `Stream::write` uses the same Step
response stream and may append repeatedly to the same Stream. It does not wait for a Stream Store
acknowledgment; storage rejection or failure is observable on Dex, not by the handler.
Flow timeout handlers and RPCs cannot send heartbeat or Stream progress.

Use an invocation-managed writer for text deltas:

```rust
let progress = THINKING.buffered_text(context)?;
progress.write(delta)?;
```

`buffered_text_with_options` accepts a `BufferedTextStreamOptions` interval and soft UTF-8 byte
threshold. Defaults are one second and 16 KiB. Invocation finalization sends the tail before the
final result or error. Empty buffers do not emit a message or heartbeat. Retry does not restore
unsent text or deduplicate emitted batches.

External writes remain unary. `Client::write_stream` requires a non-empty `source`, accepts `#`,
and appends every call even when a source repeats. Read messages return that metadata in
`StreamMessage::source`. Step writes use `#<stepExecutionID>`.

`Client::read_stream_with_timeout` moves forward one message at a time and can long-poll.
`Client::list_stream_messages` returns a non-blocking newest-first retained page. Pass the
registered typed `Stream`, a positive page size, and an empty token for the first page. Pass
`StreamMessagesPage::next_page_token` unchanged to read older messages until it is empty. The
server caps page size at 1000 by default. A trimmed anchor returns an empty page, and newer
concurrent writes do not enter an existing older-page chain.

### Canceling Step executions

A successful Step can cancel queued or active executions while continuing with
its normal decision:

```rust
Ok(StepDecision::go_to(&self.record_quote, quote)
    .cancel_sibling_step(&self.carrier_a)
    .cancel_sibling_step(&self.carrier_b)
    .cancel_step(&self.global_quote_timeout))
```

`cancel_step` selects every current execution of one registered Step type.
`cancel_sibling_step` selects only executions whose
`Context::from_step_execution_id()` matches the current execution. Chain the
builders to select multiple types; Flow-wide selection wins for the same type.
The Worker rejects unregistered Step types as invalid results.

Dex resolves one snapshot after the current execution succeeds. Completed,
already-canceled, and absent targets are no-ops. Next Steps created by that
decision are outside the snapshot. Dex immediately applies the next or close
action; late decisions, writes, retries, and recovery Steps are discarded.

`RpcResult::cancel_step` provides Flow-wide selection for RPCs. RPCs do not
support sibling selection because they have no Step execution lineage.

Opt an Attribute or AttributeMap into Attribute Store synchronization and
select Server-configured Stores for the Flow:

```rust
use dex_sdk::{Attribute, FlowConfig};

let email = Attribute::<String>::new("customer-email").sync_to_attribute_store();
let config = FlowConfig::new().attribute_store_names(vec!["profiles".into(), "audit".into()]);
```

Stores are asynchronous latest-state projections. Every enabled Attribute write
is sent to every selected Store. Deletion writes SQL `NULL`, and projection
failures do not roll back Flow Attributes. Omitting the builder preserves
current targets; passing an empty vector disables future synchronization while
retaining protocol presence.

## Blob cache

The shared cache keeps opaque payload bytes on disk and uses Stretto only for
metadata admission and eviction:

Serialized object values use wire encoding `json` for JSON and `raw` for raw bytes.
Internal Blob references are opaque. Hydration sends the owning Flow ID with
each reference, and SDK cache keys are isolated by `(flow_id, blob_ref)`.

```rust
use dex_blob_cache::{BlobCache, BlobCacheConfig};

let config = BlobCacheConfig::new("./blob-cache", 256 * 1024 * 1024, 10_000)?;
let cache = BlobCache::open(config)?;
let retained = cache.put("server-minted-blob-id", b"opaque bytes")?;
let cached_payload = cache.get("server-minted-blob-id")?;
if !retained || cached_payload.is_none() {
    // Keep using the freshly loaded bytes.
}
cache.close()?;
```

The calls are synchronous. Event-loop bridges must dispatch them through a
bounded blocking executor. Concurrent reads are supported; mutations and close
coordinate inside the cache. An orderly `close` does not delete committed
files, so they can be reused after restart; use `delete_all` before `close` for
ephemeral storage. The cache is not authoritative: policy rejection, a miss,
or a cache error must fall back to fresh server data.

Build the Java cache binding as an optimized native library:

```bash
cargo build --release -p dex-blob-cache-jni --locked
```

Build the Node cache binding and stage it into the TypeScript package:

```bash
cargo build --release -p dex-blob-cache-node --locked
# or from sdk-typescript:
npm run build:native
```

## Development

Rust 1.97 or newer is required.

Format and test the workspace:

```bash
make fmt
make lint
make test
make docs-check
```

`docs-check` denies missing rustdoc and rustdoc warnings for the hand-written
`dex-sdk` and `dex-blob-cache` public APIs, then compiles every documentation
example as a doctest. Generated protocol and language-binding crates are not
part of that public SDK surface. Generated HTML starts at
`target/doc/dex_sdk/index.html` and `target/doc/dex_blob_cache/index.html`.

Run the SDK integration suite against a fresh local `dexcli dev` stack:

```bash
./run-integration-tests.sh
```

The script creates its own Dex environment, ports, and BlobCache directory,
then removes the temporary state. Each Worker synchronizes its registered
Indexed Attributes before listening; failure or the default two-minute
deadline aborts startup.

### Measure integration coverage

Install `cargo-llvm-cov`, then run the same integration suite with Rust source
coverage:

```bash
cargo install cargo-llvm-cov --locked
./run-integration-tests.sh --coverage
```

Only the `integ` and `cross_sdk` integration test targets contribute execution
data. Test sources and dependencies are excluded from the report by
`cargo-llvm-cov`; coverage is reported for the production `dex-sdk` crate.

The LCOV report is written to `coverage/lcov.info`, and the browser report
starts at `coverage/html/index.html`. CI uploads the LCOV report with the
`sdk-rust-integration` flag and retains the full report as the
`sdk-rust-integration-coverage` Actions artifact.

## Releases

Publish `dex-sdk`, `dex-blob-cache`, and `dex-protocol` by creating a GitHub
Release whose tag is `sdk-rust/vX.Y.Z`. The release workflow stamps all crate
versions and internal registry requirements from the tag in its temporary
checkout, then publishes the crates in dependency order. No version-bump commit
is required before publishing.

The workflow also supports a manual validation run. Manual publishing is
restricted to `main` and requires the `CRATES_IO_TOKEN` repository secret.

## Server protocol compatibility

Worker startup calls `GetServerInfo`, negotiates the highest common protocol,
synchronizes Attribute indexes, and then binds WorkerService or starts pull polling. The initial Rust
SDK interval is `[1,1]`. Missing information, invalid or disjoint intervals, and
RPC failures stop startup before binding. The negotiated protocol is retained
for future protocol-specific paths.

The diagnostic SDK version is compiled from Cargo package metadata. Release
stamping therefore updates both the crate artifact and its runtime value. The
artifact version does not affect compatibility.

## License

[Sustainable Use License 1.0](LICENSE), with legacy portions under their
original terms as described in [LEGACY_NOTICES.md](LEGACY_NOTICES.md).

## Search run inclusion

Dex Server excludes ContinuedAsNew runs by default. Application queries still
need `FlowType` when using shared index slots. An explicit inclusion option
removes only the default exclusion; it does not override query filters. Keep
the query, inclusion option, and page size consistent across pages.

Use `search_flows_with_options(query, page_size, next_page_token, SearchFlowsOptions::new().include_continued_as_new(true))` for execution-chain inspection. `search_flows` and `search_flows_page` use the default exclusion.
