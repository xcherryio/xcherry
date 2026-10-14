# Advanced Java features

## Selective loading and transactions

Use `StepOptions` and `@RPC` load declarations to request only the AttributeMap instances and pending Channels a handler reads. Selective loading is a correctness boundary: an unselected read throws. Combine Attribute locks with transactional RPC execution when multiple durable mutations must commit atomically.

`@RPC` and its registration options define fixed timeout, locks, transactionality, and loads. `RPCInvokeOptions` adds only exact AttributeMap instance locks, exact AttributeMap instance loads, and exact ChannelMap instance loads. Build it with `addLockAttributeMapInstance`, `addLoadAttributeMapInstance`, or `addLoadChannelMapInstance`, then pass it to the matching `Client.invokeRPC` overload. Include the same AttributeMap instance in both lock and load for read-modify-write. The Client unions, sorts, and deduplicates registration and invocation selections, and validates the Flow definition and slash-free nonblank instance before the request. See the pinned [hash-partitioned controller](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/java/src/main/java/io/superdurable/dex/patterns/hashpartitionedattributemap/HashPartitionedAttributeMapController.java).

## Heartbeat and buffered Stream progress

[Pinned buffered Stream source](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/java/src/main/java/io/superdurable/dex/primitives/stream/StreamFlow.java)
<!-- dex-source: examples/java/src/main/java/io/superdurable/dex/primitives/stream/StreamFlow.java -->
```java
public StepDecision execute(final Context context, final String input) {
    final BufferedTextStream writer = BufferedTextStream.create(context, progress);
    writer.write("Rendering preview for " + input);
    writer.write("Preview ready for " + input);
    return StepDecision.gracefulComplete("Rendered " + input);
}
```

Create one buffered writer per invocation. Flush/finalization occurs with handler completion; retries do not reconstruct unsent text. Keep a separate durable Attribute for authoritative progress.

## Step-execution locals

`setStepExecutionLocal` passes process-local data from `waitFor` to `execute` for the same Step execution. It is not durable across Worker replacement. Use it only as an optimization; reconstruct from durable input/state when absent.

## Timeout handlers

Override `Flow.handleTimeout` and select handler timeout policy only when the business needs a final notification, compensation, or explicit outcome. Give the handler its own retries and loads. Its recovery target must be a registered `Step<Void>` and can inspect `Context.getRecoveryError()`.

[Pinned timeout start source](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/java/src/main/java/io/superdurable/dex/patterns/timeout/TimeoutController.java)
<!-- dex-source: examples/java/src/main/java/io/superdurable/dex/patterns/timeout/TimeoutController.java -->
```java
StartFlowOptions.newBuilder()
        .timeout(Duration.ofMinutes(1))
        .timeoutPolicy(FlowTimeoutPolicy.HANDLER)
        .timeoutHandlerOptions(FlowTimeoutHandlerOptions.newBuilder()
                .methodTimeout(Duration.ofSeconds(30))
                .retry(RetryPolicy.newBuilder().maximumAttempts(3).build())
                .build())
        .build());
```

Use `AttributeLock.of(attribute)` for handler locks. Do not pass an Attribute directly to `addLock`.

[Pinned timeout option source](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/sdk-java/src/test/java/io/superdurable/dex/WorkerServiceIntegrationTest.java)
<!-- dex-source: sdk-java/src/test/java/io/superdurable/dex/WorkerServiceIntegrationTest.java -->
```java
final FlowTimeoutHandlerOptions timeoutOptions =
        FlowTimeoutHandlerOptions.newBuilder()
                .methodTimeout(Duration.ofSeconds(10))
                .heartbeatTimeout(Duration.ofSeconds(5))
                .retry(RetryPolicy.newBuilder().maximumAttempts(3).build())
                .durability(StepDurability.ASYNC)
                .addLock(AttributeLock.of(status))
                .addLoadAttributeMap(attributes)
                .addLoadAttributeMapInstance(attributes, "tenant-a")
                .addLoadChannel(commands)
                .addLoadChannelMap(channels)
                .addLoadChannelMapInstance(channels, "tenant-a")
                .onFailureProceedTo(TimeoutRecoveryStep.class)
                .build();
```

## Cancellation selectors

Use `withCancelingSiblingSteps` for branches created by the same predecessor and `withCancelingSteps` for all executions of a Step type. Cancellation applies after the successful decision and excludes Steps created by that same decision. Make target handlers interruption-aware and their effects idempotent.

## StepOptions and Flow durability

Set a short-operation default through `StartFlowOptions.newBuilder().configOverride(FlowConfig.newBuilder().stepDurability(StepDurability.ASYNC).build())`. Leave ordinary methods at `StepDurability.DEFAULT`. Override known long methods with `StepOptions.Builder.waitForDurability(StepDurability.SYNC)` or `executeDurability(StepDurability.SYNC)`; the phases are independent.

`StepOptions` also owns method timeouts, heartbeat timeout, retries, selected loads, locks, and failure routes. Do not infer durability from a long method timeout. Read [core StepOptions guidance](../core/step-options.md) for the five-second heuristic, fallback, and child-deadline requirement.

Choose Step durability from workload semantics. Async execution can optimize short handlers before falling back, but shares the logical retry budget. Method timeout and heartbeat behavior can differ between phases; verify the installed SDK docs and test Worker replacement.

## BlobCache and Attribute Store

Reuse the process-level BlobCache. Opt only queryable latest-state Attributes into configured Stores. The projection is not a transactional database view and must not control a Flow decision.
