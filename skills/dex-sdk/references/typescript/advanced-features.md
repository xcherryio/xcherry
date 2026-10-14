# Advanced TypeScript features

## Async handlers

Step, WaitFor, RPC, and timeout handlers may return Promises. Awaiting Client work yields the Node event loop, so the same Worker can serve the child Flow or RPC it calls. Avoid circular waits and synchronous CPU loops. Propagate `AbortSignal` to libraries that support cancellation.

## Selective loading and transactional RPCs

Use Step/RPC load options for AttributeMap instances, Channels, and ChannelMaps. Reading outside the selected scope throws. Use `isTransactional: true` when a failed Channel deletion must roll back other staged RPC mutations. Locks provide isolation only among cooperating handlers.

Registration `RPCOptions` fixes timeout, locks, transactionality, and loads. `RPCInvokeOptions` adds only `lockAttributeMapInstances`, `loadAttributeMapInstances`, and `loadChannelMapInstances` for one call. Use `Client.invokeRPCWithOptions` instead of overloading the positional `runId` call shape. A dynamic read-modify-write supplies both `attributeMap.lock(instance)` and `attributeMap.load(instance)` for the same bucket. The Client unions, sorts, and deduplicates invocation selections with registration settings and validates the registered Flow definition and slash-free nonempty instance before network I/O. See the pinned [hash-partitioned controller](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/typescript/src/patterns/hash-partitioned-attribute-map/controller.ts).

## Buffered Streams

[Pinned buffered Stream source](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/typescript/src/primitives/stream/stream-flow.ts)
<!-- dex-source: examples/typescript/src/primitives/stream/stream-flow.ts -->
```typescript
public async execute(context: Context, input: string): Promise<StepDecision> {
  const writer = progress.bufferedText(context, { flushIntervalMs: 500 });
  writer.write(`Rendering preview for ${input}`);
  writer.write(`Preview ready for ${input}`);
  return gracefulComplete(`Rendered ${input}`);
}
```

Create the writer within one invocation. Buffered text flushes before the final result, but unsent data is not restored or deduplicated on retry. Keep durable status elsewhere.

## Heartbeat checkpoints

Use `AsyncContext.recordHeartbeat` with the same codec used by `getLastHeartbeatValue`. A Stream write acts as liveness but does not replace a useful checkpoint. Cancellation can arrive through the signal while an awaited operation is running.

## StepOptions and Flow durability

Set `StartFlowOptions.configOverride.stepDurability` to `"async"` when most handlers are short. Leave ordinary `StepOptions.waitForDurability` and `executeDurability` unset, then set the applicable method to `"sync"` for known long work. WaitFor and Execute are independent.

StepOptions also owns method timeouts, heartbeat timeout, retries, selected loads, locks, and failure routes. Do not infer durability from a large method timeout. Read [core StepOptions guidance](../core/step-options.md) for the five-second heuristic, regular fallback, shared budget, and child cancellation deadline.

## Timeout handlers

Implement `handleTimeout` when a soft deadline needs a business decision or compensation. Configure handler loads/retry separately. An exhausted handler can route to a registered `Step<void>` and inspect `context.recoveryError`.

## Cancellation selectors

Wrap a successful decision with `withCancelingSiblingSteps` for same-parent branches or `withCancelingSteps` for all executions of a Step type. Targets are resolved from the current snapshot; Steps created by that decision are excluded. RPCs may request Flow-wide cancellation but have no sibling lineage.

## Step-execution local data

Execution-local values pass data between `waitFor` and `execute` in one Worker process. They are not durable across replacement. Treat absence as normal and reconstruct from durable input/state.

## BlobCache and projections

Share a process-level BlobCache. Attribute Store synchronization is asynchronous latest-state projection, not a transactional read model. Model correctness inside the Flow.
