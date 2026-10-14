# Java error handling

Before designing sequential writes and reads, use the [shared read-after-write matrix](../core/read-after-write.md). A typed read-only RPC issued after a successful direct-state RPC write observes that write when it loads the written state; search indexes, Attribute Store projections and triggered business completion are separate. Follow this page for the language-specific error and timeout model.

Separate three categories: application rejection, retryable Worker failure, and Client/service failure.

## Handler failures

Throw an application exception only when the current method attempt should fail. Configure `waitForRetry` and `executeRetry` separately. Use `waitForFailure(WaitForFailurePolicy.PROCEED)` to continue to Execute after WaitFor exhaustion, or `onExecuteFailureProceedTo` to select a registered recovery Step. A recovery Step reads `context.getRecoveryError()` and must itself be idempotent.

Do not catch an exception only to return success. That commits staged state and hides retry. Conversely, do not throw after an irreversible external success without an idempotency key or persisted receipt.

## Business outcomes

Use `StepDecision.forceFail(detail)` for a deliberate terminal failed outcome, `gracefulComplete(output)` when siblings may finish, and `forceComplete(output)` when the Flow must close now. Expected timeouts should be modeled through a Timer winner or `handleTimeout`, not as random exceptions.

## Client exceptions

Catch concrete classes in `io.superdurable.dex.exceptions`. `FlowNotFoundException` means a read found no execution. `FlowNotActiveOrNotFoundException` reports a missing target or an operation that cannot use a closed Flow, and is the InvokeRPC mapping for missing targets; interpret it using the registered RPC path, not its class name alone. A query-only RPC can instead succeed against a retained terminal execution. Durable Step and Attribute waits never expose transport long-poll expiry; they reattach with the effective Request ID and preserve the total request budget. `RequestTimeoutException` means that caller-visible budget expired, not that the Flow or accepted durable wait failed. Internal handler rollover is transparent. An unclassified remote request failure remains `DexServiceException`. Its named gRPC code and Dex sub-status are diagnostic metadata; never branch on message text or raw numeric values.

Normal domain logic catches only the concrete exceptions whose outcomes it can decide. Every remote Client exception extends the public `DexServiceException`; local validation, definition, serialization, value-mapping, and programming failures do not. Catch the base only at a narrow boundary whose policy intentionally treats every remote Dex failure the same, such as service availability translation or explicitly best-effort output. Do not repeat that translation around every invocation. Catching `RuntimeException` is not equivalent because it also hides local SDK and application defects.

When the API promises a critical operation has completed, follow the shared [required business milestone rule](../core/error-handling.md#wait-for-a-required-business-milestone): after start success or matching-request deduplication, call `Client.waitForAttributeMatch` or `Client.waitForStepCompletion` for the committed condition or the specific persistence Step execution. Publish that condition only after the business effect commits. Bound the wait; timeout does not undo acceptance or prove the operation failed. An acceptance-only response needs no wait.

For an idempotent start, set one stable `StartFlowOptions.Builder.requestId(...)` and use `ignoreAlreadyStarted(true)` only when a retry of that same logical request may attach to the existing run. A remaining `FlowAlreadyStartedException` means the existing Flow carries a different Request ID. Treat it as a domain conflict unless the resource-scoped coordinator contract deliberately redirects the command to that existing Flow. A different `DexServiceException` can leave start acceptance unknown; retry with the same identities or read owning domain state, never a dedicated start-deduplication table.

## Query-only Get failures

Follow the shared [missing query target rule](../core/error-handling.md#missing-query-targets). For a business Get confirmed to have no registration or invocation locks, no transaction, no returned durable effects, and no Server policy that runs every RPC transactionally, catch `FlowNotActiveOrNotFoundException` or `FlowNotFoundException` and return the contract's not-found result directly. Retained closed executions remain readable, so do not call WaitForFlow, describe/search/history APIs, or add a timeout probe or retry to distinguish missing from closed. Preserve other errors and any explicit retention/unavailable contract. Do not apply this translation to mutations or active-only RPC paths.

## Closed-Flow races

`FlowNotActiveOrNotFoundException` says the mutation found no active target; it does not say the requested action succeeded. Catch it only where the operation contract is known. First reconcile from authoritative domain state and operation invariants. Call `describeFlow` and inspect `FlowStatus` only when an otherwise unknown terminal distinction changes the outcome:

- Treat `COMPLETED` as idempotent success only when successful completion guarantees the requested condition.
- For any other terminal status or a subsequent `FlowNotFoundException`, record or return an explicit domain failure or unknown outcome instead of retrying a terminal fact indefinitely.
- If the Flow still appears running, query its domain state before a bounded, idempotent retry.

For parent-child cleanup, a successfully completed child may let the parent continue. A missing or unsuccessfully terminal child must follow the parent's explicit cleanup-failure or cleanup-unknown route.

For an explicitly best-effort external `Client.writeStream`, catch `DexServiceException` because that boundary deliberately treats every remote write failure as lossy. Log sanitized Flow and phase identifiers without payloads or credentials and let the business Flow continue. Local failures remain outside that hierarchy and must surface. Never reconstruct authoritative completion state from retained Stream messages.

## Recovery checklist

- Persist the facts compensation needs before the risky side effect.
- Limit retries by attempts or duration when recovery must eventually run.
- Make recovery safe to retry and safe after partial external success.
- Preserve the original failure in logs with Flow ID, Run ID, StepExecution ID, method, and attempt.
- Test both `waitFor` and `execute` exhaustion when both are configured.
- Never use a recovery Step as a generic exception sink.

[Pinned heartbeat/cancellation source](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/java/src/main/java/io/superdurable/dex/primitives/stepheartbeat/StepHeartbeatFlow.java)
<!-- dex-source: examples/java/src/main/java/io/superdurable/dex/primitives/stepheartbeat/StepHeartbeatFlow.java -->
```java
if (context.isCancellationRequested()) {
    return StepDecision.deadEnd();
}
try {
    Thread.sleep(Duration.ofSeconds(2).toMillis());
} catch (InterruptedException interrupted) {
    Thread.currentThread().interrupt();
    return StepDecision.deadEnd();
}
```

Cancellation is cooperative and cannot undo remote side effects. Restore the thread interrupt flag and return promptly from blocking Java handlers.
