# TypeScript primitives

Choose primitives from the business behavior first, then encode them with the SDK's TypeScript shapes.

| Primitive | TypeScript shape | Use it for | Key constraint |
| --- | --- | --- | --- |
| Flow | `Flow<I>` | Durable application boundary | Stable `getFlowType`, complete Step/schema registry |
| Step | `Step<I>` | Retryable work and transitions | Stable Step type and matching `Codec<I>` |
| Wait | `Wait.until`, `anyOf`, `allOf`, `anyCombinationOf` | Durable readiness | Wait is returned, never awaited as a Promise |
| Attribute / AttributeMap | `new Attribute`, `new AttributeMap` | Durable latest state | Durable name and codec are compatibility surface |
| Channel / ChannelMap | `new Channel`, `new ChannelMap` | Durable FIFO messages | Selected waits consume messages |
| RPC | `@rpc({...})` and `RPCResult<T>` | Synchronous command or query | Query-only handlers can read retained terminal runs; locks, transactions, and returned effects require active execution |
| Stream | `new Stream` | Best-effort progress | Not a source of truth |
| Timer | `Timer.byDuration`, `Timer.byTimestamp` | Durable deadlines | Milliseconds are numbers; no event-loop timer survives Worker loss |
| SubFlow | `SubFlow.run` | Durable child work | Parent/child lifetime is an application decision |
| Client | Promise-returning `Client` methods | Lifecycle, RPCs, Attribute-match waits, Streams, search | Await network operations and catch typed errors |

## Wait composition

[Pinned wait example](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/typescript/src/primitives/wait-types/wait-types-flow.ts)
<!-- dex-source: examples/typescript/src/primitives/wait-types/wait-types-flow.ts -->
```typescript
if (input.mode === "any") {
  return Wait.anyOf(
    channelA.forOne("signal"),
    Timer.byDuration(timeoutMs, "timeout"),
  );
}
if (input.mode === "all") {
  return Wait.allOf(
    channelA.forOne("signal-a"),
    channelB.forOne("signal-b"),
  );
}
```

Among ready `Wait.anyOf` candidates, Dex uses canonical Timer, Channel, then SubFlow order and preserves argument order within each kind. An earlier unready Condition does not block a later ready one. Only the winning Channel consumes messages. For strict priority, return only the current higher-priority Condition until it resolves.

Use condition IDs when execution must distinguish winners and for every condition inside `anyCombinationOf`. Inspect results on the current `Context`.

## Codecs and schema

Use `stringCodec`, `booleanCodec`, `int64Codec`, `doubleCodec`, `bytesCodec`, or a deliberate `jsonCodec<T>` with validation for application models. Default JSON encoding does not validate TypeScript shapes at runtime. Register every Attribute, Channel, and Stream in exactly one Flow schema.

## Stream reads

Use `Client.readStream` for forward, one-at-a-time, optionally long-polling consumption. Use `Client.listStreamMessages` for non-blocking newest-first pages. Pass the typed Stream directly, and pass `nextPageToken` unchanged until it is empty.

[Pinned runnable listing](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/typescript/src/primitives/stream/controller.ts)
<!-- dex-source: examples/typescript/src/primitives/stream/controller.ts -->
```typescript
const page = await client.listStreamMessages(
  String(request.query.workflowId ?? ""),
  progress,
  Number(request.query.pageSize),
  String(request.query.beforePageToken ?? ""),
);
```

The before-page token is exclusive and scope-bound. The first page uses an empty token. Listing is a best-effort retained snapshot: concurrent newer writes stay outside the older-page chain, while trimming may remove messages. A trimmed anchor returns an empty page. The server requires a positive page size and caps it at 1000 by default.

## State, locks, and transaction

[Pinned Channel transaction example](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/typescript/src/primitives/channel/channel-flow.ts)
<!-- dex-source: examples/typescript/src/primitives/channel/channel-flow.ts -->
```typescript
@rpc({
  isTransactional: true,
  loadChannels: [queuedMessages],
  inputCodec: queuedMessageReferenceCodec,
})
public moveQueuedMessageToPrioritizedMessages(
  context: Context,
  queuedMessage: QueuedMessageReference,
): void {
  const messageToPrioritize = queuedMessages.findPendingMessage(
    context,
    queuedMessage.messageId,
  );
  queuedMessages.delete(context, queuedMessage.messageId);
  if (messageToPrioritize !== undefined) {
    prioritizedMessages.publish(context, messageToPrioritize.value);
  }
}
```

Staged Attribute and Channel mutations commit with a successful handler result. An exception discards that attempt's mutations. Locks coordinate only handlers requesting the same lock; external effects still need idempotency.

Pending-message reads inside a Step or RPC are invocation snapshots. Other handlers may consume, delete, or publish concurrently. Transactional execution validates selected deletions and commits writes atomically, but does not lock the whole snapshot. Read and write pending messages directly only when the operation explicitly tolerates that race. When a decision requires the queue to remain unchanged, every cooperating Step and RPC writer must use the same Attribute lock.

Use typed RPCs for application reads and writes of Flow-owned Attribute and Channel state. Direct Client state methods are not part of the application API. Attribute match remains the blocking observation surface.

An RPC without Attribute locks or `isTransactional` can query a retained terminal run when it returns no durable effects. Locks, transactions, returned effects, or Server policy require an active execution. RPC success alone does not prove the Flow is active.

## Decisions

Use `goTo`, `goToMany`, `gracefulComplete`, `forceComplete`, `forceFail`, `deadEnd`, and conditional completion helpers to express lifecycle. Do not encode control flow in mutable module globals. A Flow can have `StepList.empty()` or `withoutStartStep` when started work is driven by RPCs.
