# Java observability

## Correlation

Include `context.getFlowId()`, `getRunId()`, and `getStepExecutionId()` in structured logs from handlers. Add `getAttempt()` and method name for retries. A Flow ID follows the logical application instance; a Run ID identifies one execution attempt. Keep both.

Record business milestones with `context.recordEvent(name, value, ValueClass.class)` when they belong in Flow history. Use logs for diagnostics and events for durable application milestones. Never log secrets or whole large payloads.

## Heartbeats

Long-running handlers must call `recordHeartbeat` before the configured heartbeat timeout. Store a compact checkpoint that lets a later attempt resume safely. A heartbeat is liveness plus progress, not an exactly-once commit.

Server v1.10.1 enforces the Step HeartbeatTimeout for pull Workers; Server v1.10.0 does not. Cancellation reaches a Step handler through heartbeats, so record one within the timeout when a long-running handler must observe cancellation. For a pull target, later attempts after the one-attempt ASYNC local phase receive the last recorded heartbeat. See [pull Worker routing](../core/pull-workers.md).

[Pinned Java heartbeat source](https://github.com/superdurable/dex/blob/sdk-java/v1.14.0/examples/java/src/main/java/io/superdurable/dex/primitives/stepheartbeat/StepHeartbeatFlow.java)
<!-- dex-source: examples/java/src/main/java/io/superdurable/dex/primitives/stepheartbeat/StepHeartbeatFlow.java -->
```java
final int completedBatches = context.hasLastHeartbeatValue()
        ? context.getLastHeartbeatValue(Integer.class)
        : 0;
for (int batch = completedBatches; batch < batches; batch++) {
    if (context.isCancellationRequested()) {
        return StepDecision.deadEnd();
    }
```

## Streams

Streams carry best-effort progress. Use `BufferedTextStream` for token/text deltas and direct `Stream.write` for discrete events. Stream writes can fail local validation or transport even though Store rejection does not fail the Step. Keep final status and resumable state in Attributes or Flow output.

Track operationally: active Flow age, Step method attempt, retry delay, heartbeat age, Worker version/target, queue depth, Stream consumer lag, and terminal failure type. Alert on user-visible stuckness, not on a single retry.

At incident time collect Flow ID, Run ID, Flow type, StepExecution ID, Worker version/target, last successful transition, current attempt, and recent application logs before changing state.
