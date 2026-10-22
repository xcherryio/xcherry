# TypeScript observability

## Correlation

Log `context.flowId`, `runId`, `stepExecutionId`, `attempt`, and handler name as structured fields. Keep Flow ID and Run ID distinct. Use `context.recordEvent` for durable business milestones, logs for diagnostic detail, and redact application payloads.

## Heartbeats and cancellation

Async long-running handlers should accept `AsyncContext`, recover a compact checkpoint, call `await context.recordHeartbeat(...)` before the heartbeat timeout, and pass `context.cancellationSignal` to abort-aware APIs.

Server v1.10.1 enforces the Step HeartbeatTimeout for pull Workers; Server v1.10.0 does not. Cancellation reaches a Step handler through heartbeats, so record one within the timeout when a long-running handler must observe cancellation. For a pull target, later attempts after the one-attempt ASYNC local phase receive the last recorded heartbeat. See [pull Worker routing](../core/pull-workers.md).

[Pinned heartbeat source](https://github.com/superdurable/dex/blob/sdk-typescript/v1.14.1/examples/typescript/src/primitives/heartbeat/heartbeat-flow.ts)
<!-- dex-source: examples/typescript/src/primitives/heartbeat/heartbeat-flow.ts -->
```typescript
public async execute(context: AsyncContext, batches: number): Promise<StepDecision> {
  const completedBatches = context.getLastHeartbeatValue(doubleCodec) ?? 0;
  for (let batch = completedBatches; batch < batches; batch++) {
    if (context.cancellationSignal.aborted) {
      return deadEnd();
    }
    await new Promise((resolve) => setTimeout(resolve, 2_000));
    await context.recordHeartbeat(batch + 1, doubleCodec);
  }
  return gracefulComplete("processed");
}
```

## Streams

Streams are best-effort progress. Save and reuse each returned resume token. The source identifies the producing Step execution but does not deduplicate. Store authoritative progress in Attributes or terminal output.

Monitor Flow age/status, active Step age, attempt, heartbeat age, Worker version/target, Channel backlog, RPC latency/error class, Stream consumer lag, and terminal failure category. Alert on sustained stuckness or exhausted recovery rather than any individual retry.

For incidents collect Flow ID, Run ID, Flow type, StepExecution ID, Worker version, last transition, current attempt, and application logs before issuing a mutation.
