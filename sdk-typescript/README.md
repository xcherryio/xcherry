# Dex SDK for TypeScript

## Flow state I/O

Applications read and write Flow state through typed RPCs. The Client does not
expose direct Attribute reads or writes, Channel publication, or pending-message
mutation. This keeps each external state transition behind a Flow-owned method.

An RPC can stage `channel.delete(context, messageId)`. Set the decorator option
`isTransactional: true` when a missing ID must abort all other RPC writes.
Attribute locks already select transactional execution; Channel deletion requires
the explicit option.

## RPC state loading

RPCs receive ordinary Attributes and all Channel size metadata by default.
AttributeMap entries and pending Channel messages are opt-in:

```typescript
@rpc({
  loadAttributeMapInstances: [items.load("tenant-a")],
  loadChannels: [queued],
  loadChannelMaps: [byTenant],
})
snapshot(context: Context): RPCResult<Snapshot> {
  return { output: { messages: queued.pendingMessages(context) } };
}
```

Put an AttributeMap or ChannelMap directly in its plural load option when every
current instance is needed. Use the singular instance options for the less common
exact-instance case. A selected empty queue returns an empty array; reading an
unselected map entry throws **AttributeMapNotLoadedError**. An unselected pending-message
snapshot throws **ChannelMessagesNotLoadedError**.
Pending messages preserve FIFO order and include the server-assigned message ID.
The snapshot does not change after the handler stages a publish or deletion.

Loading controls which data reaches the Worker. **isTransactional** controls
atomic commit and Channel deletion validation. Attribute locks add isolation
only among cooperating Steps and RPCs using the same lock. Write-only publish
and delete operations do not require loading.

### Invocation-time map instances

Decorator `RPCOptions` remains the registration-time contract for timeout,
transactionality, locks, and fixed state loads. Use `invokeRPCWithOptions` when
the caller chooses an exact map instance at runtime:

```typescript
const result = await client.invokeRPCWithOptions(
  directory.upsertCustomerProfile,
  flowId,
  profile,
  {
    lockAttributeMapInstances: [profiles.lock(partitionName)],
    loadAttributeMapInstances: [profiles.load(partitionName)],
  },
);
```

The separate method avoids ambiguity with the positional `runId`. Selections are
additive, sorted, and deduplicated. Locking and loading are independent;
read-modify-write handlers need both for the same instance.

See the runnable [sequential chunking](../examples/typescript/src/patterns/sequentially-chunked-attribute-map)
and [hash partitioning](../examples/typescript/src/patterns/hash-partitioned-attribute-map)
patterns for complete call sites.

## Step and timeout-handler state loading

`StepOptions` provides the same five selections independently for `waitFor` and
`execute`: all AttributeMap instances, exact AttributeMap instances, Channels,
all ChannelMap instances, and exact ChannelMap instances. The methods receive
independent snapshots. Execute reads after the winning Wait consumes messages;
retries of one logical method call reuse its first snapshot.

`FlowTimeoutHandlerOptions` provides Execute-style timeouts, heartbeat detection,
retry, durability, Attribute locks, and the same state selections. Set it on
`StartFlowOptions` or `SubFlowOptions` only for a positive timeout using `HANDLER`.
Exhausted retries may proceed to a registered `Step<void>` using `voidCodec`;
read the final failure from `Context.recoveryError`.

```typescript
class TimeoutRecoveryStep implements Step<void> {
  public readonly inputCodec = voidCodec;

  public getStepType(): string {
    return "TimeoutRecoveryStep";
  }

  public execute(context: Context, _input: void): StepDecision {
    const failure = context.recoveryError;
    return forceFail(failure?.detail ?? "timeout recovery has no failure");
  }
}
```

This package targets Node.js 22 and 24. It provides strongly typed workflow
contracts and a Promise-based gRPC Client. The Client and Worker runtime use
`@grpc/grpc-js`. Blob caching uses the shared Rust DXBC implementation through
a Node-API addon.

Application values use `Codec<T>`. Flow, Step, RPC, Attribute, Channel, and Stream
definitions retain their input and output types. Client methods return Promise
because Node network I/O is asynchronous.

The Client uses `@grpc/grpc-js` directly. Rust is only the implementation
boundary for the shared BlobCache; TypeScript callbacks and network transport
stay in Node.

Omitted Step input codecs and RPC input/output codecs use JSON
(`JSON.stringify` / `JSON.parse`, no structural validation). Scalar wire kinds
still need `stringCodec`, `booleanCodec`, `int64Codec`, `doubleCodec`, or
`bytesCodec`. A `@rpc()` method with only `Context` and a `void` return stays a
procedure.

```typescript
class ApproveOrder implements Step<{ orderId: string }> {
  getStepType(): string {
    return "ApproveOrder";
  }

  waitFor(_context: Context, _order: { orderId: string }): Wait {
    return Wait.until(Timer.byDuration(1_000));
  }

  execute(_context: Context, order: { orderId: string }): StepDecision {
    return gracefulComplete(order);
  }
}

class Orders implements Flow<{ orderId: string }> {
  readonly approve = new ApproveOrder();

  getFlowType(): string {
    return "Orders";
  }

  getSteps() {
    return StepList.startStep(this.approve);
  }
}

const orders = new Orders();
const registry = new Registry([orders]);
```

Flows return all Steps once. Start with `StepList.startStep(step)` and append
heterogeneous Steps with `.otherSteps(...)`. Use
`StepList.withoutStartStep<void>(...)` for RPC-triggered Steps, or
`StepList.empty()` when the Flow has no Steps.
`Flow<StartInput>` only types the starting Step and `Client.startFlow()` input;
`StepList<StartInput>` enforces that relationship during type checking.
Non-starting Steps may use unrelated input types. `Flow` defaults to `void` for
Flows without a start input.

`Worker.start()` synchronizes every registered Indexed Attribute with Dex
Server before binding its listener or opening pull streams. Existing indexes
return immediately; failure or the default 120-second deadline aborts startup.
An indexed `AttributeMap` must provide one fixed `indexKey`.

### Pull mode

Set `WorkerOptions.pull` to open outbound task streams without a WorkerService
listener. Advertise `worker.workerTarget` through `ClientOptions.workerTarget`.
`StartFlowOptions.configOverride.workerTarget` can select pull routing for one
Flow, and `SubFlowOptions.configOverride.workerTarget` can select it for a SubFlow.
The pull target is `{ pull: true }`; it cannot include a nonempty address or
`headless`.
A pull Worker cannot set `bindAddress` or an advertised `workerTarget`.

```typescript
const worker = new Worker(registry, cache, {
  serverAddress: "dex.example:8801",
  pull: {
    maxConcurrentTasks: 8,
    idleStreamsPerQueue: 1,
    stopDrainTimeoutMs: 30_000,
  },
});
await worker.start();
const client = new Client(registry, cache, {
  serverAddress: "dex.example:8801",
  workerTarget: worker.workerTarget,
});
try {
  await client.startFlow(flow, flowId, input);
} finally {
  await client.close();
  await worker.close();
}
```

After `await worker.start()`, call `await worker.waitPullRegistered({ timeoutMs: 10_000 })`
to wait until every Flow type queue and the app Worker ID queue has registered at least once.
The wait also accepts an `AbortSignal` as `signal`. It is safe before startup and for concurrent
callers. It rejects with `PullRegistrationError` (queue and original gRPC `cause`) if a permanent
rejection leaves a queue without another registered stream before readiness. Startup failures
and shutdown before readiness also reject the wait. Once registration succeeds, later waits
resolve even after shutdown.

Use `worker.pullRegistrationState().isReady` for ongoing probes. Snapshots include `queues`
with `queue`, `hasRegisteredStream`, and `lastPermanentError`. Readiness does not flap during
reconnects or transient errors. A permanent rejection without another registered stream makes
it false; re-registration clears that queue's error. Closing does not reset readiness.

Set `pull.stopOnPermanentStreamRejection: true` to drain and stop on such a permanent rejection
before or after readiness. It defaults to false, retaining indefinite retries. `worker.run()`
rejects with `PullRegistrationError` after draining; `start()` keeps its non-blocking startup
behavior. Always call `close()`, which remains safe after `run()` returns.

Pull mode polls the Worker's sticky queue and every registered Flow type queue.
Flow type names must be unique within one Dex server. `maxConcurrentTasks`
defaults to the logical CPU count, and `idleStreamsPerQueue` defaults to one.
Each stream runs one task at a time. When all slots are busy, the Worker rejects
additional assignments for reassignment. The Worker keeps idle streams on each
queue even when all slots are busy. After tasks finish, it gracefully closes surplus
idle streams while preserving readiness. With `F` registered Flow types, the
steady-state bound after load drops is
`maxConcurrentTasks + (1 + F) * idleStreamsPerQueue` streams. The transient peak
during a burst remains bounded by the matching caps. Size both options for
`matching.maxConcurrentTasksCap` per app Worker ID per matching owner and
`matching.maxStreams` per Matching instance.

The Worker rejects an unregistered Step or RPC by name. Dex retries that task
on another pull Worker without consuming a Step attempt, including when the
Flow was sticky to the rejecting Worker. If no capable Worker accepts it within
the server's dispatch wait (10 seconds by default, or the local Step attempt),
the Step follows its failure and retry policy; an RPC caller receives the named
error. This reassignment requires a Dex Server that supports it. Against older
Servers, the Worker returns the not-registered error as before.

The default `appWorkerID` uses the first nonempty value from
`DEX_WORKER_INSTANCE`, `POD_NAME`, `HOSTNAME`, or the system hostname, followed by
eight random base32 characters. An override must identify one Worker process
uniquely and change after each restart. It must contain 1-63 ASCII letters,
digits, dots, underscores, or hyphens, and cannot equal a registered Flow type.

The Worker reconnects after transport failures. A server-directed reconnect
waits for the current task to finish and gives the final frame up to
`stopDrainTimeoutMs` to reach the server. An older server without pull support
produces one explicit error log and is retried after one minute. Startup reports
protocol and index synchronization failures; later poll failures are retried.
`Worker.close()` drains for at most `stopDrainTimeoutMs`, which defaults to
30 seconds. It then cancels active handlers and waits for them to return. Streams
that sent a final frame wait for the server to close them until the remaining
drain deadline before the channel closes. Idle streams also wait for the
remaining drain deadline. A handler that ignores cancellation can delay shutdown.

`StepOptions.waitForMethodTimeoutMs` and `executeMethodTimeoutMs` bound the two
handler calls. Timer and channel conditions determine how long a Step waits.

#### Reserved RPC capacity

Set `reservedRpcTasks` when synchronous RPC latency must stay responsive behind
long-running Steps. When unset, it reserves one slot if the resolved task limit
is at least two, otherwise zero. With the CPU-count default, Steps get one fewer
slot than before. Set it explicitly to zero to opt out. With one slot and no
explicit reserve, Worker start logs one warning: RPCs may wait behind
long-running Steps; use a limit of at least two or set the reserve explicitly.
Flows must stay responsive; Dex cannot prioritize synchronous RPCs like workflow
engines prioritize reads. The value must be nonnegative and less than
`maxConcurrentTasks` after its CPU-count default is resolved; invalid values
fail construction. RPC and Step handlers count together: WaitFor and Execute
tasks, including local attempts, are declined with `NO_CAPACITY` when the busy
count reaches the task limit minus the reserve. API-direct and transactional
InvokeWorkerRPC tasks can use the full limit.

```ts
const pull: PullWorkerOptions = { maxConcurrentTasks: 8, reservedRpcTasks: 2 };
```

A decline lets Dex try another Worker without consuming an attempt. If all
Workers remain at their Step limit, a Step can wait until the dispatch wait
expires, then receive `NO_WORKER_AVAILABLE`, consume an attempt, and follow its
retry policy, as when all Workers are full. Local attempts wait the local
dispatch wait, then fall back according to their policy. Each queue still keeps
its configured idle streams; a reserve decline does not close a stream or change
registration readiness.

Each reserve decline records the Flow type through node:util debuglog under
dex:pull (enable with NODE_DEBUG=dex:pull). Export the cumulative
`pullReservedRpcDeclines` Worker value to distinguish reserve pressure from
full-capacity rejections. The count excludes full-capacity rejections and
unknown names, never resets, and remains available after shutdown. Listening
Workers return zero.

### Canceling Step executions

A successful Step can cancel queued or active executions while continuing with
its normal decision:

```typescript
return withCancelingSteps(
  withCancelingSiblingSteps(
    goTo(RecordQuote, quote),
    QuoteCarrierA,
    QuoteCarrierB,
  ),
  GlobalQuoteTimeout,
);
```

`withCancelingSteps` selects every current execution of each registered Step
type. `withCancelingSiblingSteps` selects only executions whose
`Context.fromStepExecutionId` matches the current execution. Both helpers return
a new decision; repeated calls form a union, and Flow-wide selection wins for
the same Step class. Unregistered selectors produce an invalid Step result.

Dex resolves one snapshot after the current execution succeeds. Completed,
already-canceled, and absent targets are no-ops. Next Steps created by that
decision are outside the snapshot. Dex immediately applies the next or close
action; late decisions, writes, retries, and recovery Steps are discarded.

An RPC result may set `cancelingSteps` for Flow-wide selection. RPCs do not
support sibling selection because they have no Step execution lineage.

### Soft Flow timeout

An optional `Flow.handleTimeout` makes a positive timeout use handler policy by
default. It may be synchronous or async and returns a normal `StepDecision`:

```typescript
async handleTimeout(context: Context): Promise<StepDecision> {
  await notifyExpiration(context);
  return forceComplete("expired");
}

await client.startFlow(orders, "order-42", input, {
  timeoutMs: 30 * 60_000,
  timeoutPolicy: FlowTimeoutPolicy.HANDLER,
});
```

`FAIL` produces `FlowErrorType.FLOW_TIMEOUT` and permits Flow retry; `CANCEL`
cancels without retry. Continue-as-new preserves the durable timer's deadline;
retry runs get a fresh budget. A zero or omitted timeout disables the
feature.

Opt an Attribute or AttributeMap into Attribute Store synchronization, then
select Server-configured Stores for the Flow:

```typescript
const email = new Attribute("customer-email", stringCodec)
  .syncToAttributeStore();
const config: FlowConfig = { attributeStoreNames: ["profiles", "audit"] };
```

Stores are asynchronous latest-state projections. Every enabled Attribute write
is sent to every selected Store. Deletion writes SQL `NULL`, and projection
failures do not roll back Flow Attributes. Omitting `attributeStoreNames`
preserves current targets; `attributeStoreNames: []` disables future
synchronization while retaining protocol presence.

Use `hardDeleteOnNull()` for short-lived secrets, such as API keys, that Dex
must be able to delete permanently:

```typescript
const apiKey = new Attribute("api-key", stringCodec).hardDeleteOnNull();
const vendorTokens = new AttributeMap("vendor-tokens", jsonCodec<VendorToken>())
  .hardDeleteOnNull();
```

Dex stores these values only in the Dex blob store, never in Flow history.
Deleting the Attribute or setting it to `null` or `undefined` permanently
deletes the stored value, and a deleted value reads as absent. Handlers receive
values inline, and the SDK never stores them in the local blob cache.

- Only string and object values are supported, including JSON values and byte
  arrays. Built-in numeric and Boolean codecs fail at definition time. Custom
  codecs that encode a number or Boolean fail when the value is written.
- A hard-delete definition cannot have an index or use
  `syncToAttributeStore()`. `Client.waitForAttributeMatch` rejects it before
  contacting Dex. Use the registered definition for initial Attributes.
- Writes require a Server with an enabled blob store and Server protocol
  version 3 or later. The Client checks the protocol once before its first
  hard-delete write. The Worker fails a handler result that writes a
  hard-delete Attribute to an older Server.
- Each Flow keeps one stored copy per Attribute key and overwrites it in
  place. A Step or RPC attempt writes or deletes that copy before Dex commits
  its result. Retries, other Steps and RPCs, and Client reads can observe a
  write or deletion from an attempt that later failed. Concurrent writers to the
  same key may leave a value other than the last committed write.
- A failed `startFlow` or SubFlow start for an already-running Flow ID still
  overwrites the running Flow's value; a null initial value deletes it.
  Executions that reuse a Flow ID share the stored value.
- `timeTravel` does not restore a deleted value. Values remain until deleted
  or until the Flow is removed by retention and blob cleanup. Blob store
  backups or bucket versioning can retain copies.

### Waiting and map inspection

`Wait.until`, `Wait.allOf`, and `Wait.anyOf` use unnamed Conditions by default.
Do not add Condition IDs merely because a Condition is nested in one of these
waits. Every Condition in `Wait.anyCombinationOf` needs a non-empty user ID;
the same Condition object may be reused across combinations.

`Client.waitForAttributeMatch` overloads target the current run and return the
decoded matched value. Build a match with one of the six `AttributeMatch`
factories. String and boolean support equality operators; integer and double
support every operator. JSON, bytes, null, non-finite doubles, and invalid
ordering reject before transport. Every AttributeMap and ChannelMap
instance must be non-empty and must not contain `/`. `AttributeMap.getMapSize`
and `getAllInstanceKeys` include buffered sets and deletes. The matching
`ChannelMap` methods are RPC-only, include buffered publishes, and omit empty
instances. Keys are decoded and sorted. Use
`forceCompleteIfChannelsEmpty(...)` for conditional completion.

Request IDs are optional for both durable waits. When omitted, the server
derives a stable ID from the Step execution or Attribute condition. Reuse an
override only for the same wait. `requestTimeoutMs` is the total SDK call budget
across transparent transport reattachments. Omit it or use zero to wait
indefinitely. Temporal permits 10 in-flight Updates and 2,000 total Updates in
History per Workflow Execution. A caller timeout or cancellation does not
finish an accepted durable wait, so an abandoned handler can retain one
in-flight slot. Set `internalHandlerTimeoutMs` only when abandoned waits can
approach that limit. An active call transparently starts another generation,
which counts toward the 2,000-Update history limit. Prefer a value comfortably
longer than normal request timeouts and reconnect gaps. Zero disables timed
rollover. Request expiry throws `RequestTimeoutError`.

### Async handlers

`Step.execute`, `Step.waitFor`, and RPC methods may be `async` and return a
`Promise` of their result. Because the Worker runs on Node's single event loop,
an async handler that `await`s `Client` calls (`startFlow`, `waitForFlow`,
`invokeRPC`, ...) yields the loop, so the **same** Worker keeps serving other
WorkerService calls — including the child Flow or RPC you just started. One
Worker is enough; no sidecar Worker is needed.

```ts
async execute(context: Context, childId: string): Promise<StepDecision> {
  await client.startFlow(childFlow, childId, input);
  const result = await client.waitForFlow(childId, 5_000);
  const output = result.singleOutput(stringCodec);
  return gracefulComplete(output);
}
```

Synchronous handlers stay valid — returning a plain `StepDecision` / `Wait` /
`RPCResult` needs no change. Two rules keep the loop healthy:

- Never block the event loop from a handler (`Atomics.wait`,
  `child_process.spawnSync`). That freezes the Worker and defeats the point.
- A long `await client.waitForFlow(...)` holds the current WorkerService call
  until it resolves or times out. Prefer a short timeout plus Step retry (or a
  timer-backed re-check) over one unbounded poll. `executeMethodTimeoutMs` /
`waitForMethodTimeoutMs` clock the full async handler duration.

### Step progress and Streams

Annotate a Promise-returning Step handler with `AsyncContext` when it needs to
record progress. The heartbeat value argument is required. Passing `undefined`
explicitly sends a heartbeat without a Value and clears previously persisted
heartbeat details. Passing `null` sends a present JSON-null Value.

```typescript
async execute(context: AsyncContext, input: ImportInput): Promise<StepDecision> {
  const restored = context.hasLastHeartbeatValue()
    ? context.getLastHeartbeatValue(importCheckpointCodec)
    : undefined;

  for await (const page of remainingPages(restored)) {
    importedRows.write(context, page.rows);
    await context.recordHeartbeat(page.checkpoint, importCheckpointCodec);
  }
  return gracefulComplete();
}
```

Omitting the heartbeat codec uses JSON. Read a restored value with the same
codec used to record it. `hasLastHeartbeatValue()` distinguishes an absent Value
from a present JSON-null Value; both decode to `undefined`.

`Stream.write(context, value)` is synchronous and fire-and-forget. It validates
and encodes locally, then writes a frame to the current WaitFor or Execute gRPC
response stream. A handler may write any number of messages to the same or
different Streams. Dex attempts each append in call order, but Stream Store
failures are not returned to the handler. RPC and Flow timeout Contexts reject
Step Stream writes.

For text deltas, create one writer and pass its bound `write` method directly
to the producer:

```typescript
const progress = thinking.bufferedText(context, { flushIntervalMs: 500 });
await generateText(input, progress.write);
```

The defaults are one second and a soft 16 KiB UTF-8 threshold. Invocation
completion sends the tail before the final result or error. Chunks are
concatenated exactly. Retry does not restore unsent text or deduplicate emitted
batches.

External writes use `client.writeStream(flowId, stream, source, value)`. Source
must be non-empty, may contain `#`, and may repeat; every write appends a new
message. `Client.readStream` returns it as `StreamMessage.source`. Step writes
use `#<stepExecutionID>` as source metadata.

`Client.readStream` moves forward one message at a time and can long-poll.
`Client.listStreamMessages` returns a non-blocking newest-first retained page.
Pass the registered typed `Stream`, a positive page size, and an empty token for
the first page. Pass `StreamMessagesPage.nextPageToken` unchanged to read older
messages until it is empty. The server caps page size at 1000 by default. A
trimmed anchor returns an empty page, and newer concurrent writes do not enter
an existing older-page chain.

Step durability defaults to the Flow configuration and then sync. Regular
attempts default to two hours, heartbeat timeout defaults to one minute, and
retry total duration defaults to four hours. The SDK accepts non-negative
whole-second heartbeat timeout values, including two seconds, and leaves the
deployment-configured minimum to Dex Server. Async durability first uses a
seven-second local-activity window with at most three attempts; that phase
ignores method timeouts and heartbeats but still writes Stream messages.

Every TypeScript Flow and Step must return an explicit durable name from
`getFlowType()` or `getStepType()`. Class names are never used as fallbacks
because bundlers and minifiers may rename them.

`waitForFlow` hydrates every output-bearing completion before resolving. For a
multi-output Flow, select by Step identity and decode each value with its codec:

```typescript
const result = await client.waitForFlow(flowId);
const receipt = result.completions
  .find((completion) => completion.stepType === "ChargeCard")
  ?.decode(receiptCodec);
```

The completion array is read-only and keeps server collection order. Parallel
branch order is not deterministic. A no-output Flow returns an empty array;
`singleOutput` throws for zero or multiple completions. Every terminal status returns
a `FlowResult`; inspect `status`, `errorType`, and `errorMessage` for unsuccessful
completion.

SubFlows are normal, independently addressable Flows used as durable Conditions:

```typescript
public waitFor(_context: Context, input: ChargeInput): Wait {
  return Wait.until(SubFlow.run(this.chargeFlow, input));
}

public execute(context: Context, _input: ChargeInput): StepDecision {
  const receipt = SubFlow.getConditionResults(context).singleOutput(receiptCodec);
  return gracefulComplete(receipt);
}
```

`SubFlow.getFlowId(context, index)` remains available for a running `anyOf` loser.
`SubFlowOptions` configures timing, timeout policy, retry, initial target Attributes,
Flow config, Condition ID, and reuse. Parent completion does not cancel an unfinished SubFlow.

## Authentication

Set `ClientOptions.credential` and `WorkerOptions.credential` when Dex Server
authentication is enabled. Each FlowService call requests the current token.
Workers also send it during startup index synchronization, blob hydration, and
when opening pull task streams. A reconnect requests the token again. Omitted
credentials send no authorization metadata.

```typescript
import { Client, FileCredential, Worker } from "@superdurable/dex";

const credential = new FileCredential("/var/run/secrets/dex/token");
const worker = new Worker(registry, cache, { credential, pull: {} });
const client = new Client(registry, cache, {
  credential,
  workerTarget: worker.workerTarget,
});
```

`FileCredential` reads the file at construction and checks its modification
time and size at most once per second. It trims surrounding whitespace and
rereads a changed file. Construction fails if the file is unreadable or empty;
later calls fail if it becomes unreadable or empty, instead of sending a stale
token. A custom credential may return a string or `Promise<string>` from
`token()`. Calls can ask for tokens concurrently. A thrown or rejected error
fails the call before contacting Dex and reports gRPC `UNAUTHENTICATED` with
the original error as its cause. Failed pull streams retry after 60–90 seconds.
File checks use small synchronous stat and read operations.

Use `ApiKeyCredential` when the application holds a long-lived API key and the
token endpoint issues short-lived access tokens for a supplied Dex server ID:

```typescript
import { ApiKeyCredential, Client, Worker } from "@superdurable/dex";

const credential = new ApiKeyCredential({
  tokenUrl: "https://auth.example.com/token",
  serverId: "dex-server-id",
  apiKey: process.env.DEX_API_KEY!,
});
const worker = new Worker(registry, cache, { credential, pull: {} });
const client = new Client(registry, cache, { credential, workerTarget: worker.workerTarget });
try {
  await worker.start();
  await client.searchFlows("", 10);
} finally {
  await client.close();
  await worker.close();
}
```

The token URL must use HTTPS; HTTP is accepted only on localhost, 127.0.0.0/8,
or ::1. The constructor validates settings without making a request. The first
call exchanges the key. After 80% of the token lifetime, the next call starts
one shared refresh and immediately returns the still-valid token. Failed
refreshes wait at least five seconds before another attempt. Calls after expiry
wait for the shared exchange, which defaults to a 10-second timeout covering
connection and response. Set `exchangeTimeoutMs` to change it, or supply `fetch`
for a proxy or a test endpoint. The application owns an injected fetch client.

If Dex rejects a token with `UNAUTHENTICATED`, the credential discards it only
when it is still current. Unary Client and Worker calls then retry once with a
fresh token. Rejected pull streams reconnect immediately once with a fresh token;
a second rejection uses the Worker's 60–90 second backoff. The API key and access
token are absent from credential inspection and exchange error messages. A single
credential is safe to share among concurrent Clients and Workers. The application
owns it; closing a Client or Worker does not close the credential. It holds no
persistent HTTP client or idle background task; an active exchange ends within
its timeout.

The bearer token travels over the SDK's plaintext gRPC connection. Protect
the network path between the application and Dex.

## Errors

Client calls reject with concrete `DexServiceError` subclasses. Existing-Flow
reads (`describeFlow`, `waitForFlow`, and `timeTravel`) use
`FlowNotFoundError`; operations that require a running Flow use
`FlowNotActiveOrNotFoundError`. Start conflicts, worker failures, RPC lock contention,
and long-poll timeouts use `FlowAlreadyStartedError`,
`WorkerInvocationError`, `RpcLockConflictError`, and `LongPollTimeoutError`.

RPCs also use this error when the target Flow cannot be found, including query-only
reads. A query-only RPC without locks, transactional execution, durable effects, or
Server-forced Update routing can read a retained terminal execution. At that confirmed
read boundary, this error means no readable target was found; return the application's
missing or unavailable result directly, without a lifecycle probe, a short timeout, a
retry, or historical Step-output decoding. Preserve other service and Worker failures.
For mutations and active-only RPCs, the error can mean either missing or closed and does
not prove that the requested action succeeded. RPC success does not prove that the Flow
is active.
Durable Step and Attribute waits reattach transparently instead of exposing
`LongPollTimeoutError`.

```typescript
try {
  await client.invokeRPC(orders.updateOrder, flowId, update);
} catch (error) {
  if (error instanceof FlowNotActiveOrNotFoundError) {
    // The Flow is missing or already closed.
  } else {
    throw error;
  }
}
```

Every service error retains `code`, `subStatus`, `detail`, `operation`,
`flowId`, and the original gRPC error as `cause`. `WorkerInvocationError` also
retains `workerCode`, `workerErrorType`, and `workerErrorDetail`. Registration,
serialization, and invalid handler returns use `FlowDefinitionError`,
`ValueMappingError`, and `InvalidStepResultError` instead of transport errors.

## Source layout

Public contracts are grouped by domain under `src/`. The root `src/index.ts`
is a barrel that re-exports the supported package API; applications should
continue importing only from `@superdurable/dex`.

- `codec.ts`: wire values and codecs
- `persistence.ts`: attributes, indexes, locks, and schemas
- `wait.ts`: channels, timers, conditions, and waits
- `step.ts`: Steps, movements, options, and decisions
- `rpc.ts`: typed RPC contracts and decorators
- `flow.ts`: Flows, registration, and validation
- `client.ts`: Promise-based FlowService Client
- `worker.ts`: Worker gRPC service and lifecycle
- `worker-dispatcher.ts`: typed callback dispatch and response mapping
- `invocation-context.ts`: invocation-scoped persistence and condition state
- `blob-cache.ts`: DXBC BlobCache contract and Node-API binding loader
- `gen/`: checked-in protobuf and grpc-js bindings

Serialized object values use wire encoding `json` for JSON and `raw` for raw bytes.
Internal Blob references are opaque. Hydration sends the owning Flow ID with
each reference, and the cache is isolated by `(flowId, blobRef)`.

Run `npm run build:native` once to stage the DXBC Node addon for the current
platform, then `npm test` for runtime contracts, `npm run typecheck` for strict
static contracts, and `npm run docs:check` for public API documentation. The
documentation check follows the actual exports of `src/index.ts` and requires
JSDoc for every public class, interface, type, overload, member, object-style
enum value, type parameter, input, and output; generated sources are excluded.
The comments appear in TypeScript language-service and IDE hovers.

Run `./run-integration-tests.sh` for all 58 IWF
compatibility scenarios against an isolated `dexcli dev` environment. After
changing `protos/dex.proto`, run `make generated-code` from the repository root
and commit every server and SDK output.

## Integration coverage

Run the complete integration suite with TypeScript source coverage:

```shell
npm run coverage:integration
```

The terminal report lists coverage per SDK source file and every uncovered
line. Open `coverage/index.html` for annotated source, or inspect
`coverage/coverage-summary.json` programmatically. `coverage/lcov.info` is the
report uploaded by CI. Generated protobuf code under `src/gen/` is excluded.

CI uploads the LCOV report to Codecov with GitHub OIDC, so no upload secret is
stored in this repository. The report uses the `sdk-typescript-integration`
flag and contributes to the TypeScript SDK component defined in the root
`codecov.yml`. After the first successful `main` upload, Codecov displays
project and patch coverage in its dashboard, GitHub checks, and PR comments.
The Actions run also publishes the complete HTML report as
`sdk-typescript-integration-coverage`.

The complete legacy IWF integration inventory lives under
[`test/integ`](test/integ/README.md). Its Flow fixtures retain the Java
suite's workflow behavior and its 58 assertions run against a real Dex server.

## Releases

The npm package is published as [`@superdurable/dex`](https://www.npmjs.com/package/@superdurable/dex).
After the initial bootstrap, publish a GitHub Release tagged
`sdk-typescript/vX.Y.Z`. The tag is the release version source of truth; CI
temporarily writes it to `package.json` and `package-lock.json` without
committing either file. The release workflow then runs type checks and tests,
inspects the tarball, and publishes through npm Trusted Publishing. Prerelease
versions use the `next` npm dist-tag; stable versions use `latest`.

Trusted Publishing can only be configured after the package exists. Bootstrap
the first version from a maintainer workstation with 2FA:

```shell
cd sdk-typescript
npm ci
npm run typecheck
npm test
npm pack --dry-run
npm login
npm publish --access public
```

Then open the package settings on npmjs.com and add a GitHub Actions trusted
publisher with organization `superdurable`, repository `dex`, workflow
`sdk-typescript-publish.yml`, no environment, and `npm publish` permission.
Future releases use short-lived OIDC credentials and require no `NPM_TOKEN`.
After verifying the first OIDC release, configure npm publishing access to
require 2FA and disallow token-based publication.

## Server protocol compatibility

Worker startup calls `GetServerInfo`, negotiates the highest common protocol,
synchronizes Attribute indexes, and then binds WorkerService. The TypeScript
SDK interval is `[1,3]`. Missing information, invalid or disjoint intervals,
and RPC failures stop startup before binding. The Client negotiates lazily, once,
before its first hard-delete Attribute write. Hard-delete Attribute writes
require negotiated protocol version 3 or later.

The build generates the diagnostic SDK version from `package.json`, so the
runtime value follows the version stamped by the release workflow. The artifact
version does not participate in compatibility decisions.

## Search run inclusion

Dex Server excludes ContinuedAsNew runs by default. Application queries still
need `FlowType` when using shared index slots. An explicit inclusion option
removes only the default exclusion; it does not override query filters. Keep
the query, inclusion option, and page size consistent across pages.

Pass `{ includeContinuedAsNew: true }` as the fourth argument of `searchFlows(query, pageSize, nextPageToken, options)` for execution-chain inspection. The default is `false`.
