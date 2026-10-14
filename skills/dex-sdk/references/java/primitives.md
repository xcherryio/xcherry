# Java primitives

Choose the primitive from the behavior the application needs, not from a preferred API.

| Primitive | Java shape | Use it for | Key constraint |
| --- | --- | --- | --- |
| Flow | `Flow<I>` | Durable application boundary | `getSteps()` and schema define one stable Flow type |
| Step | `Step<I>` | Retryable side effects or durable decisions | Concrete, non-parameterized `Class<I>` input |
| Wait | `Wait.until`, `anyOf`, `allOf`, `anyCombinationOf` | Durable readiness | `waitFor` observes; `execute` acts after readiness |
| Attribute / AttributeMap | `Attribute.define`, `AttributeMap.define` | Durable latest state | Define once and register in the schema |
| Channel / ChannelMap | `Channel.define`, `ChannelMap.define` | Durable FIFO commands/events | A satisfied condition consumes selected messages |
| RPC | `@RPC` and `RPCResult<T>` | Synchronous command or query | Query-only handlers can read retained terminal runs; locks, transactions, and returned effects require active execution |
| Stream | `Stream.define` | Best-effort progress | Not authoritative state; clients resume with tokens |
| Timer | `Timer.byDuration`, `Timer.byTimestamp` | Durable deadlines | Timer readiness is not a Java sleep |
| SubFlow | `SubFlow.run` | Independently managed durable child work | Decide parent lifetime and cancellation explicitly |
| Client | `Client` | Lifecycle, RPCs, Attribute-match waits, Streams, search | Catch concrete SDK exceptions |

## Wait composition

[Pinned wait example](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/java/src/main/java/io/superdurable/dex/primitives/waittypes/WaitTypesFlow.java)
<!-- dex-source: examples/java/src/main/java/io/superdurable/dex/primitives/waittypes/WaitTypesFlow.java -->
```java
case "any":
    return Wait.anyOf(
            channelA.forOne("signal"),
            Timer.byDuration(timeout, "timeout"));
case "all":
    return Wait.allOf(
            channelA.forOne("signal-a"),
            channelB.forOne("signal-b"));
case "combo":
    return Wait.anyCombinationOf(
            ConditionCombination.of(
                    channelA.forOne("signal-a"),
                    Timer.byDuration(timeout, "timeout")),
            ConditionCombination.of(channelB.forOne("signal-b")));
```

Among ready `Wait.anyOf` candidates, Dex uses canonical Timer, Channel, then SubFlow order and preserves argument order within each kind. An earlier unready Condition does not block a later ready one. Only the winning Channel consumes messages. For strict priority, return only the current higher-priority Condition until it resolves.

Use condition IDs when code must distinguish winners, and for every condition inside `anyCombinationOf`. Read Channel results only from the current execution's `Context`.

## Durable state and locking

[Pinned Attribute example](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/java/src/main/java/io/superdurable/dex/primitives/attribute/AttributeFlow.java)
<!-- dex-source: examples/java/src/main/java/io/superdurable/dex/primitives/attribute/AttributeFlow.java -->
```java
@Override
public StepOptions getStepOptions() {
    return StepOptions.newBuilder()
            .addWaitForLock(AttributeLock.of(status))
            .addWaitForLock(AttributeLock.of(progress, "payment"))
            .addExecuteLock(AttributeLock.of(status))
            .addExecuteLock(AttributeLock.of(progress, "payment"))
            .build();
}
```

Locks coordinate only Steps and RPCs that request the same lock. They do not make external calls transactional. AttributeMap and ChannelMap instance names must be stable business keys.

Pending-message reads inside a Step or RPC are invocation snapshots. Other handlers may consume, delete, or publish concurrently. Transactional execution validates selected deletions and commits writes atomically, but does not lock the whole snapshot. Read and write pending messages directly only when the operation explicitly tolerates that race. When a decision requires the queue to remain unchanged, every cooperating Step and RPC writer must use the same Attribute lock.

An RPC without Attribute locks or `isTransactional` can query a retained terminal run when it returns no durable effects. Locks, transactions, returned effects, or Server policy require an active execution. Use `describeFlow` when lifecycle status matters; RPC success alone does not prove the Flow is active.

## Stream reads

Use `Client.readStream` for forward, one-at-a-time, optionally long-polling consumption. Use `Client.listStreamMessages` for non-blocking newest-first pages. Pass the typed Stream directly, and pass `getNextPageToken()` unchanged until it is empty.

[Pinned runnable listing](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/java/src/main/java/io/superdurable/dex/primitives/stream/StreamController.java)
<!-- dex-source: examples/java/src/main/java/io/superdurable/dex/primitives/stream/StreamController.java -->
```java
final StreamMessagesPage<String> page = client.listStreamMessages(
        workflowId,
        flow.progress,
        pageSize,
        beforePageToken);
```

The before-page token is exclusive and scope-bound. The first page uses an empty token. Listing is a best-effort retained snapshot: concurrent newer writes stay outside the older-page chain, while trimming may remove messages. A trimmed anchor returns an empty page. The server requires a positive page size and caps it at 1000 by default.

## Decisions and commit boundary

Return `StepDecision.goTo`, `goToMany`, `gracefulComplete`, `forceComplete`, `forceFail`, or `deadEnd` according to the intended lifecycle. Attribute writes and Channel publications are staged with the successful handler result. An exception discards that attempt's staged mutations. External side effects require idempotency because their success cannot be rolled back.

## Client interaction

Use `startFlow` for a new execution, `waitForFlow` for terminal status, a typed RPC stub for Flow-owned Attribute and Channel state, and Attribute match for blocking observation. Direct Client state methods are not part of the application API. Use bounded waits at service boundaries and continue polling after a long-poll timeout. `searchFlows` is for indexed discovery, not coordination.
