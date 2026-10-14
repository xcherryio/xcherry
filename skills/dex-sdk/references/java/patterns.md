# Java design patterns

Select the smallest shape that preserves the business invariant. Every link below is pinned to the Dex baseline.

## Catalog

| Pattern | Java-native shape | Critical invariant | Runnable implementation |
| --- | --- | --- | --- |
| Static parallel Steps | `StepDecision.goToMany(StepMovement.of(...))` | All branches are known in code | [StaticParallelStepsFlow](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/java/src/main/java/io/superdurable/dex/patterns/parallel/StaticParallelStepsFlow.java) |
| Dynamic parallel Steps | build `List<StepMovement<?>>` from input | Branch identity and input remain deterministic | [DynamicParallelStepsFlow](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/java/src/main/java/io/superdurable/dex/patterns/parallel/DynamicParallelStepsFlow.java) |
| Await all Steps | completion Channel plus `Wait.until(channel.forN(n))` | Workers publish exactly one completion per branch | [AwaitParallelStepsFlow](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/java/src/main/java/io/superdurable/dex/patterns/parallel/AwaitParallelStepsFlow.java) |
| First win | successful decision cancels sibling Step type | Losing side effects remain idempotent and cancellation-aware | [FirstWinParallelStepsFlow](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/java/src/main/java/io/superdurable/dex/patterns/parallel/FirstWinParallelStepsFlow.java) |
| Basic parallel SubFlows | `Wait.allOf(SubFlow.run(...))` | Parent waits for every child terminal result | [BasicParentFlow](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/java/src/main/java/io/superdurable/dex/patterns/parallelsubflows/BasicParentFlow.java) |
| Wait for half | child branches publish completion; coordinator waits for quorum | Define loser stop behavior before starting children | [WaitForHalfParentFlow](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/java/src/main/java/io/superdurable/dex/patterns/parallelsubflows/WaitForHalfParentFlow.java) |
| Long-lived parent | fixed handlers loop over a request Channel | Bound the queue and expose stop semantics | [AdvancedLongLiveParentFlow](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/java/src/main/java/io/superdurable/dex/patterns/parallelsubflows/AdvancedLongLiveParentFlow.java) |
| Short-lived parent | count active children and conditionally complete | Counter update and queue drain share a lock | [AdvancedShortLiveParentFlow](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/java/src/main/java/io/superdurable/dex/patterns/parallelsubflows/AdvancedShortLiveParentFlow.java) |
| Partition and back pressure | stable hash chooses parent; RPC returns acceptance | Start-if-missing is race-safe and rejection is retried intentionally | [SubmitRequestFlow](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/java/src/main/java/io/superdurable/dex/patterns/parallelsubflows/SubmitRequestFlow.java) |
| Polling | one long-running `execute` loop; see [Polling](#polling) | “Not ready” is a loop branch, never an error | [Design pattern](https://docs.superdurable.io/design-patterns/polling); no pinned runnable example yet |
| Cron | external scheduler starts a Flow on a schedule | Flow ID/reuse policy defines overlap semantics | [CronScheduleFlow](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/java/src/main/java/io/superdurable/dex/patterns/cron/CronScheduleFlow.java) |
| Reminder | Timer and opt-out Channel race | Opt-out affects future reminders, not unrelated work | [ReminderFlow](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/java/src/main/java/io/superdurable/dex/patterns/reminders/ReminderFlow.java) |
| Inactivity tracking | resettable timestamp plus Timer loop | Late activity must not close a newer active window | [InactivenessTrackerFlow](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/java/src/main/java/io/superdurable/dex/patterns/inactivenesstracker/InactivenessTrackerFlow.java) |
| Execute recovery | `onExecuteFailureProceedTo` compensation Step | Compensation is idempotent and uses persisted facts | [FailureRecoveryFlow](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/java/src/main/java/io/superdurable/dex/patterns/recovery/FailureRecoveryFlow.java) |
| WaitFor recovery | `waitForFailure(WaitForFailurePolicy.PROCEED)` | Recovery distinguishes wait failure from readiness | [ProceedOnWaitFailureFlow](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/java/src/main/java/io/superdurable/dex/primitives/proceedonwaitfailure/ProceedOnWaitFailureFlow.java) |
| Manual recovery | exhausted work proceeds to a Step waiting on retry/skip Channels | Operators receive a safe, auditable choice | [ManualRecoveryFlow](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/java/src/main/java/io/superdurable/dex/patterns/intervention/ManualRecoveryFlow.java) |
| Graceful timeout | `Flow.handleTimeout` returns a business decision | Timeout outcome is distinct from infrastructure failure | [FlowGracefulTimeout](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/java/src/main/java/io/superdurable/dex/patterns/timeout/FlowGracefulTimeout.java) |
| Drain internal Channel | side Step loops until final marker | Main completion does not strand durable messages | [DrainInternalChannelFlow](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/java/src/main/java/io/superdurable/dex/patterns/drainchannels/internal/DrainInternalChannelFlow.java) |
| Drain external publishing | conditional completion checks queue emptiness | Concurrent publishers cannot race with close | [DrainingExternalChannelFlow](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/java/src/main/java/io/superdurable/dex/patterns/drainchannels/externalpublishing/DrainingExternalChannelFlow.java) |
| Interruptible execution | cancellation Channel/RPC cancels the active Step | Handler cooperates; remote effects still need compensation | [InterruptibleFlow](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/java/src/main/java/io/superdurable/dex/patterns/interruptible/InterruptibleFlow.java) |
| Responsive Step update | client waits for Step completion with `WaitForStepCompletionOptions` | Server derives a stable ID from the Step execution | [Runnable SDK test](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/sdk-java/src/test/java/io/superdurable/dex/integ/BasicTest.java) |
| Responsive Attribute update | `waitForAttributeMatch` returns a revision watermark | Server derives an ID from the exact predicate | [Runnable SDK test](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/sdk-java/src/test/java/io/superdurable/dex/integ/PersistenceTest.java) |
| Responsive Stream update | client reads best-effort messages with resume token | Reconnect tolerates retention gaps; state comes from Attributes | [StreamFlow](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/java/src/main/java/io/superdurable/dex/primitives/stream/StreamFlow.java) |
| Entity store | long-lived Flow per entity, RPC mutations, Attribute Store projection | Flow ID is entity key and commands serialize mutations | [UserProfileFlow](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/java/src/main/java/io/superdurable/dex/patterns/entitystore/UserProfileFlow.java) |
| Sequentially chunked AttributeMap | bounded `current` list plus immutable sequence-named archives | Append order is stable and one page loads one chunk | [ChunkedSubscriberFlow](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/java/src/main/java/io/superdurable/dex/patterns/sequentiallychunkedattributemap/ChunkedSubscriberFlow.java) |
| Hash-partitioned AttributeMap | canonical key, wrapping FNV-1a 32-bit, 1000 buckets | Full canonical keys distinguish collisions; partition mapping stays stable | [CustomerDirectoryFlow](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/java/src/main/java/io/superdurable/dex/patterns/hashpartitionedattributemap/CustomerDirectoryFlow.java) |

Use sequential chunks for append-only messages, events, and subscriber history. All appenders share the `current` map-instance lock and retry only after `RpcLockConflictException`. Use hash partitions for customer profiles or metadata that needs direct keyed lookup and updates. An upsert adds the same bucket as both an invocation-time lock and load; a read adds only the load. Changing the partition count requires a full rehash or a new Flow version.

## Polling

Apply the Core [Polling pattern](../core/patterns.md#polling) to every wait on an external system. One `execute` loops: call the provider with its own timeout, such as 10 seconds; throw transient errors to Step retry; keep “not ready” in the loop; return the next decision on a terminal status, including a business-failure decision when the provider reports a definite failure. Write a Stream frame on status change, otherwise `context.recordHeartbeat(checkpoint)`, then plain `Thread.sleep(interval)`; on `InterruptedException`, restore the interrupt flag and return promptly. Resume with `context.hasLastHeartbeatValue()` and `getLastHeartbeatValue(type)`; the checkpoint holds resume state such as the last reported status, never a deadline. Bound the wait only with `StepOptions.newBuilder()`: `executeMethodTimeout` is the maximum wait, `executeRetry` allows a few attempts with `RetryPolicy.newBuilder().totalDuration(...)` equal to the maximum wait (all attempts, measured from the first), and `heartbeatTimeout` keeps one minute with `interval + call timeout <= heartbeat timeout - 10s`. On expiry the engine fails the Step; `onExecuteFailureProceedTo(RecordFailureStep.class)` routes it to a Step that reads `context.getRecoveryError()` (`getDetail()`, `getErrorType()`) and records the business failure, and without that route the Flow fails. Never loop with a WaitFor Timer and `goTo`, `RetryAfterException` or retry policy, a hand-written deadline that duplicates `executeMethodTimeout` or `totalDuration`, or a page token passed to the next execution.

## Responsive Attribute revision

Initialize the integer revision Attribute to zero. Every state-changing Step or RPC must use the same Attribute lock and increment the revision inside the locked invocation. The [Job Posting Flow](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/java/src/main/java/io/superdurable/dex/products/jobpost/JobPostingFlow.java) demonstrates the `@RPC(lockAttributes = {"UpdatePostingLock"})` boundary.

Use `AttributeMatch.equalTo`, `notEqualTo`, `greaterThan`, `greaterThanOrEqual`, `lessThan`, or `lessThanOrEqual`. Both singleton and AttributeMap overloads of `waitForAttributeMatch` return the actual decoded matched value, including for non-equal operators. The AttributeMap overload also takes the instance. The Request ID is an optional override; the server otherwise derives it from the exact Attribute predicate. `requestTimeout(Duration)` bounds the complete SDK call across transparent reattachments. Zero waits indefinitely; a positive expiry throws `RequestTimeoutException` without terminating the accepted durable wait. Leave `internalHandlerTimeout(Duration)` at zero unless abandoned waits could approach the per-Flow limit on concurrently accepted waits. A positive value transparently rolls an active call to another handler generation, releasing the old slot but counting toward the per-Flow total of accepted waits. Prefer it comfortably longer than normal request timeouts and reconnect gaps. See the core responsive-update guidance for the full tradeoff. After the wait, call the application's read RPC. A revision is a coalescing watermark, so callers must not expect every intermediate value.

[Runnable inequality wait](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/sdk-java/src/test/java/io/superdurable/dex/integ/PersistenceTest.java)
<!-- dex-source: sdk-java/src/test/java/io/superdurable/dex/integ/PersistenceTest.java -->
```java
assertEquals(
        3,
        environment.client().waitForAttributeMatch(
                flowId,
                SET_ATTRIBUTES_WORKFLOW.integer,
                AttributeMatch.greaterThan(0),
                waitOptions(flowId, "integer", Duration.ofSeconds(30))));
```

## Representative shapes

[Pinned static-parallel source](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/java/src/main/java/io/superdurable/dex/patterns/parallel/StaticParallelStepsFlow.java)
<!-- dex-source: examples/java/src/main/java/io/superdurable/dex/patterns/parallel/StaticParallelStepsFlow.java -->
```java
@Override
public StepDecision execute(final Context context, final String input) {
    return StepDecision.goToMany(
            StepMovement.of(WorkA.class, input),
            StepMovement.of(WorkB.class, input));
}
```

[Pinned recovery source](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/java/src/main/java/io/superdurable/dex/patterns/recovery/FailureRecoveryFlow.java)
<!-- dex-source: examples/java/src/main/java/io/superdurable/dex/patterns/recovery/FailureRecoveryFlow.java -->
```java
@Override
public StepOptions getStepOptions() {
    return StepOptions.newBuilder()
            .onExecuteFailureProceedTo(UpdateQuantityRecovery.class)
            .executeRetry(RetryPolicy.newBuilder().maximumAttempts(5).build())
            .build();
}
```

Before implementing, write the terminal states, cancellation target, queue bound, deduplication key, and recovery behavior. Prefer one pattern over combining several unless their invariants are independently necessary.
