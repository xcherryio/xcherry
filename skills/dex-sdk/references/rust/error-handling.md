# Rust error handling

Before designing sequential writes and reads, use the [shared read-after-write matrix](../core/read-after-write.md). A typed read-only RPC issued after a successful direct-state RPC write observes that write when it loads the written state; search indexes, Attribute Store projections and triggered business completion are separate. Follow this page for the language-specific error and timeout model.

Separate handler failures from controller/client failures. A Step or RPC returns `HandlerResult<T>` and uses `HandlerError` to control retryable Worker behavior. A Client operation returns `SdkResult<T>` and exposes typed `SdkError` variants. Do not collapse either into strings before policy or HTTP mapping has examined it.

## Handler failures and retries

Return errors with `?` when a durable read or write fails. For an application failure, construct a stable error type and useful message. `HandlerError::retry_after` overrides the next retry delay for that failure; `StepOptions::execute_retry` still bounds attempts. Use it only as explicit backoff for a transient failure. It is not a polling loop: an external “not ready yet” status is a normal branch of the [Polling pattern](patterns.md#polling), never an error. The excerpt below only demonstrates the delay override.

[Runnable source](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/rust/src/primitives/custom_retry/flow.rs)
<!-- dex-source: examples/rust/src/primitives/custom_retry/flow.rs -->
```rust
fn options(&self) -> StepOptions<Self::Input> {
    StepOptions::new().execute_retry(RetryPolicy::new().maximum_attempts(5))
}

fn execute(
    &self,
    context: &mut Context,
    ready_after_attempt: Self::Input,
) -> HandlerResult<StepDecision> {
    if i32::try_from(context.attempt()).unwrap_or(i32::MAX) < ready_after_attempt {
        return Err(HandlerError::retry_after(
            7,
            "CustomRetry",
            format!("not ready on attempt {}", context.attempt()),
        ));
    }
    Ok(StepDecision::graceful_complete(String::from("ready")))
}
```

Make Execute side effects idempotent across attempts. Dex retries a logical method execution; a remote service may have accepted the previous call even when the Worker did not observe the response.

## WaitFor versus Execute failure

Configure retry policies for the phase that can fail. WaitFor should normally be pure durable preparation. If WaitFor exhaustion is explicitly recoverable, set `WaitForFailurePolicy::Proceed`; Execute must then check `context.wait_for_method_failed()` before selecting a recovery transition.

[Runnable source](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/rust/src/primitives/proceed_on_wait_failure/flow.rs)
<!-- dex-source: examples/rust/src/primitives/proceed_on_wait_failure/flow.rs -->
```rust
fn options(&self) -> StepOptions<Self::Input> {
    StepOptions::new()
        .wait_for_failure(WaitForFailurePolicy::Proceed)
        .wait_for_retry(RetryPolicy::new().maximum_attempts(2))
}

fn wait_for(&self, _context: &mut Context, _input: Self::Input) -> HandlerResult<Wait> {
    Err(HandlerError::new(
        "ProceedOnWaitFailure",
        "planned WaitFor failure",
    ))
}
```

Never enable Proceed without a deliberate Execute branch; otherwise failed preparation is mistaken for successful readiness.

## Recovery choices

- Compensation Step: use when completed effects can be reversed deterministically.
- Manual recovery: wait on operator Channels after automatic retries exhaust.
- Graceful timeout handler: make a final bounded decision when the Flow deadline wins.
- Force fail: use when no safe continuation exists.
- Dead end: terminate only one branch; do not confuse it with failing the whole Flow.

Record recovery progress durably before triggering another non-idempotent action.

## Client errors

Match `SdkError` variants that affect external behavior, such as `FlowAlreadyStarted`, `RequestTimeout`, `RpcLockConflict`, Channel message absence, or Worker invocation failure. Durable Step and Attribute waits never expose transport long-poll expiry; they reattach with the effective Request ID and preserve the total request budget. `RequestTimeout` means that caller-visible budget expired, not that the Flow or accepted durable wait failed. Internal handler rollover is transparent. Controllers should map known conflicts and invalid requests distinctly from infrastructure failures. Preserve `source()` chains in logs. Do not retry every `SdkError`; only retry operations whose semantics and request IDs make repetition safe.

Unlike the class-based SDKs, `SdkError` is one enum containing both service-backed variants and local `FlowDefinition`, `InvalidArgument`, `ValueMapping`, and `InvalidStepResult` defects. Never discard every `SdkError` to implement a remote-failure policy. Match the documented service variant for the operation, or use `service_error().is_some()` only at a narrow boundary that intentionally handles every remote variant identically.

When the API promises a critical operation has completed, follow the shared [required business milestone rule](../core/error-handling.md#wait-for-a-required-business-milestone): after start success or matching-request deduplication, call `Client::wait_for_attribute_match` or `Client::wait_for_step_completion` for the committed condition or the specific persistence Step execution. Publish that condition only after the business effect commits. Bound the wait; timeout does not undo acceptance or prove the operation failed. An acceptance-only response needs no wait.

For an idempotent start, set one stable `StartFlowOptions::request_id(...)` and `ignore_already_started(true)` only when a retry of that same logical request may attach to the existing run. A remaining `SdkError::FlowAlreadyStarted` means a different Request ID owns the Flow ID. Treat it as a domain conflict unless the coordinator contract deliberately redirects the command to that existing Flow. Another service-backed variant can leave acceptance unknown; retry with the same identities or read owning domain state, never a dedicated start-deduplication table.

For an explicitly best-effort external `Client::write_stream`, suppress only `SdkError::Service { .. }`, the remote fallback produced by that operation. Propagate local argument and value-mapping variants. Context Stream writes inside a handler remain `HandlerResult` work and must participate in the Step's retry or recovery policy.

## Query-only Get failures

Follow the shared [missing query target rule](../core/error-handling.md#missing-query-targets). For a business Get confirmed to have no registration or invocation locks, no transaction, no returned durable effects, and no Server policy that runs every RPC transactionally, catch `SdkError::FlowNotActiveOrNotFound` or `SdkError::FlowNotFound` and return the contract's not-found result directly. Retained closed executions remain readable, so do not call WaitForFlow, describe/search/history APIs, or add a timeout probe or retry to distinguish missing from closed. Preserve other errors and any explicit retention/unavailable contract. Do not apply this translation to mutations or active-only RPC paths.

## Terminal decisions

`graceful_complete` waits for the Flow's branch semantics; `force_complete` terminates immediately; `force_fail` fails immediately. Assert the intended Flow status in integration tests. A successful method return is not evidence that buffered Stream output or unrelated sibling work has finished.
