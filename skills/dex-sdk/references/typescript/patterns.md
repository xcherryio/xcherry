# TypeScript design patterns

Select a pattern by its invariant. The TypeScript implementations below are pinned to the Dex baseline.

## Catalog

| Pattern | TypeScript-native shape | Critical invariant | Runnable implementation |
| --- | --- | --- | --- |
| Static parallel Steps | `goToMany(StepMovement.of(...))` | Branch set is fixed | [parallel-step-flows.ts](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/typescript/src/patterns/parallel/parallel-step-flows.ts) |
| Dynamic parallel Steps | map input to `StepMovement` | Branch identity/input is deterministic | [parallel-step-flows.ts](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/typescript/src/patterns/parallel/parallel-step-flows.ts) |
| Await all Steps | completion Channel and `forN(count)` | One completion message per branch | [parallel-step-flows.ts](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/typescript/src/patterns/parallel/parallel-step-flows.ts) |
| First win | `withCancelingSiblingSteps` | Losers are cancellation-aware and idempotent | [parallel-step-flows.ts](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/typescript/src/patterns/parallel/parallel-step-flows.ts) |
| Basic parallel SubFlows | `Wait.allOf(...SubFlow.run(...))` | Parent waits for every child | [parallel-subflows.ts](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/typescript/src/patterns/parallel-subflows/parallel-subflows.ts) |
| Wait for half | coordinator waits for quorum Channel | Loser stop behavior is explicit | [parallel-subflows.ts](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/typescript/src/patterns/parallel-subflows/parallel-subflows.ts) |
| Long-lived parent | fixed handlers loop over request Channel | Queue is bounded and stop is explicit | [parallel-subflows.ts](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/typescript/src/patterns/parallel-subflows/parallel-subflows.ts) |
| Short-lived parent | active-child Attribute plus conditional close | Counter/queue changes are serialized | [parallel-subflows.ts](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/typescript/src/patterns/parallel-subflows/parallel-subflows.ts) |
| Partition and back pressure | stable hash + acceptance RPC + start-if-missing | Same key chooses same parent; rejection is handled | [parallel-subflows.ts](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/typescript/src/patterns/parallel-subflows/parallel-subflows.ts) |
| Polling | one long-running async `execute` loop; see [Polling](#polling) | “Not ready” is a loop branch, never an error | [Design pattern](https://docs.superdurable.io/design-patterns/polling); no pinned runnable example yet |
| Cron | scheduler calls `startFlow` | ID/reuse policy defines overlap | [cron-schedule-flow.ts](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/typescript/src/patterns/cron/cron-schedule-flow.ts) |
| Reminder | Timer races opt-out Channel | Opt-out does not cancel unrelated work | [reminder-flow.ts](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/typescript/src/patterns/reminders/reminder-flow.ts) |
| Inactivity tracking | durable activity time plus Timer loop | Stale timer cannot close a newer activity window | [inactiveness-tracker-flow.ts](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/typescript/src/patterns/inactiveness-tracker-timer/inactiveness-tracker-flow.ts) |
| Execute recovery | `ExecuteFailure.proceedTo` compensation | Persist compensation facts before risky work | [failure-recovery-flow.ts](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/typescript/src/patterns/recovery/failure-recovery-flow.ts) |
| WaitFor recovery | `WaitForFailure` / proceed-on-failure | Execute distinguishes readiness from failed wait | [proceed-on-wait-failure-flow.ts](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/typescript/src/primitives/proceed-on-wait-failure/proceed-on-wait-failure-flow.ts) |
| Manual recovery | failed Step proceeds to Channel wait | Operator retry/skip is auditable and safe | [manual-recovery-flow.ts](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/typescript/src/patterns/intervention/manual-recovery-flow.ts) |
| Graceful timeout | `handleTimeout` returns business decision | Deadline outcome is explicit | [flow-graceful-timeout.ts](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/typescript/src/patterns/timeout/flow-graceful-timeout.ts) |
| Drain internal Channel | side Step loops until final marker | Main path cannot strand queued work | [drain-internal-channels-flow.ts](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/typescript/src/patterns/drain-channels/internal/drain-internal-channels-flow.ts) |
| Drain external publishing | `forceCompleteIfChannelsEmpty` | Publish and close cannot race | [draining-channel-flow.ts](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/typescript/src/patterns/drain-channels/external-publishing/draining-channel-flow.ts) |
| Interruptible execution | cancellation selector + `AbortSignal` | Handler cooperates; remote effects compensate | [interruptible-execution-flow.ts](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/typescript/src/patterns/interruptible/interruptible-execution-flow.ts) |
| Responsive Step update | `waitForStepCompletion` with wait options | Server derives a stable ID from the Step execution | [Runnable SDK test](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/sdk-typescript/test/integ/basic.integration.test.ts) |
| Responsive Attribute update | `waitForAttributeMatch` returns a revision watermark | Server derives an ID from the exact predicate | [Runnable SDK test](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/sdk-typescript/test/integ/rpc.integration.test.ts) |
| Responsive Stream update | `readStream` with resume token | Best effort; reconstruct from durable state | [stream-flow.ts](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/typescript/src/primitives/stream/stream-flow.ts) |
| Entity store | Flow per entity, RPC mutation, Attribute Store projection | Flow ID is entity key and writes serialize | [user-profile-flow.ts](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/typescript/src/patterns/entity-store/user-profile-flow.ts) |
| Sequentially chunked AttributeMap | bounded `current` array plus immutable sequence-named archives | Append order remains stable and one page loads one chunk | [chunked-subscriber-flow.ts](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/typescript/src/patterns/sequentially-chunked-attribute-map/chunked-subscriber-flow.ts) |
| Hash-partitioned AttributeMap | canonical key, wrapping FNV-1a 32-bit, 1000 buckets | Full canonical keys distinguish bucket collisions | [customer-directory-flow.ts](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/typescript/src/patterns/hash-partitioned-attribute-map/customer-directory-flow.ts) |

Use sequential chunks for append-only messages, events, and subscriber history. All appenders share the `current` instance lock and retry only after `RpcLockConflictError`. Use hash partitions for direct lookup and updates by a stable business key. An upsert supplies the same bucket through both `lockAttributeMapInstances` and `loadAttributeMapInstances`; a read supplies only the load. Changing the partition count requires a full rehash or a new Flow version.

## Polling

Apply the Core [Polling pattern](../core/patterns.md#polling) to every wait on an external system. One `execute(context: AsyncContext, ...)` loops: call the provider with its own timeout, such as `AbortSignal.timeout(10_000)`; throw transient errors to Step retry; keep “not ready” in the loop; return the next decision on a terminal status, including a business-failure decision when the provider reports a definite failure. Write a Stream frame on status change, otherwise `await context.recordHeartbeat(checkpoint, codec)`, then `await setTimeout(intervalMs)` from `node:timers/promises`. Resume with `context.getLastHeartbeatValue(codec)`; the checkpoint holds resume state such as the last reported status, never a deadline. Bound the wait only with StepOptions: `executeMethodTimeoutMs` is the maximum wait, `executeRetry` allows a few attempts with `totalDurationMs` equal to the maximum wait (all attempts, measured from the first), and `heartbeatTimeoutMs` keeps one minute with `interval + call timeout <= heartbeat timeout - 10s`. On expiry the engine fails the Step; `executeFailure: ExecuteFailure.proceedTo(RecordFailureStep)` routes it to a Step that reads `context.recoveryError` (`detail`, `errorType`) and records the business failure, and without that route the Flow fails. Never loop with a WaitFor Timer and `goTo`, `retryAfter` or retry policy, a hand-written deadline that duplicates `executeMethodTimeoutMs` or `totalDurationMs`, or a page token passed to the next execution.

## Responsive Attribute revision

Initialize the integer revision Attribute to zero. Every state-changing Step or RPC must declare the same Attribute lock and increment the revision inside the locked invocation. The [Job Posting Flow](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/typescript/src/products/job-post/job-post-flow.ts) demonstrates the `lockAttributes` boundary.

Use `AttributeMatch.equalTo`, `notEqualTo`, `greaterThan`, `greaterThanOrEqual`, `lessThan`, or `lessThanOrEqual`. `waitForAttributeMatch` is overloaded for singleton Attributes and AttributeMap instances and returns the actual decoded matched value, including for non-equal operators. The map form also takes the instance. `requestId` is an optional override; the server otherwise derives it from the exact Attribute predicate. `requestTimeoutMs` bounds the complete SDK call across transparent reattachments. Omit it or use zero to wait indefinitely; a positive expiry throws `RequestTimeoutError` without terminating the accepted durable wait. Leave `internalHandlerTimeoutMs` unset or zero unless abandoned waits could approach the per-Flow limit on concurrently accepted waits. A positive value transparently rolls an active call to another handler generation, releasing the old slot but counting toward the per-Flow total of accepted waits. Prefer it comfortably longer than normal request timeouts and reconnect gaps. See the core responsive-update guidance for the full tradeoff. After the wait, call the application's read RPC. A revision is a coalescing watermark, not an event stream.

[Runnable inequality wait](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/sdk-typescript/test/integ/rpc.integration.test.ts)
<!-- dex-source: sdk-typescript/test/integ/rpc.integration.test.ts -->
```typescript
assert.equal(
  await client.waitForAttributeMatch(
    id,
    flow.counter,
    AttributeMatch.greaterThan(0),
    { requestId: `${id}-wait-counter`, requestTimeoutMs: 30_000 },
  ),
  succeeded,
);
```

## Representative shapes

[Pinned parallel source](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/typescript/src/patterns/parallel/parallel-step-flows.ts)
<!-- dex-source: examples/typescript/src/patterns/parallel/parallel-step-flows.ts -->
```typescript
public execute(_context: Context, input: string): StepDecision {
  return goToMany(
    StepMovement.of(WorkA, input),
    StepMovement.of(WorkB, input),
  );
}
```

[Pinned quorum source](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/typescript/src/patterns/parallel-subflows/parallel-subflows.ts)
<!-- dex-source: examples/typescript/src/patterns/parallel-subflows/parallel-subflows.ts -->
```typescript
public waitFor(_context: Context, total: number): Wait {
  return Wait.until(subFlowCompletedCh.forN(Math.ceil(total / 2)));
}
```

[Pinned recovery source](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/typescript/src/patterns/recovery/failure-recovery-flow.ts)
<!-- dex-source: examples/typescript/src/patterns/recovery/failure-recovery-flow.ts -->
```typescript
public getStepOptions(): StepOptions {
  return {
    executeFailure: ExecuteFailure.proceedTo(UpdateQuantityRecovery, {
      executeRetry: { maximumAttempts: 5 },
    }),
    executeRetry: { maximumAttempts: 5 },
  };
}
```

Before coding, write down terminal states, queue bounds, cancellation target, idempotency key, and recovery behavior. Combine patterns only when each invariant is required.
