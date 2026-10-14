# Rust observability

Observe a Dex application through durable identities and public Client APIs. Logs and metrics explain Worker execution; Flow status, typed snapshot RPC responses, heartbeats, and Streams explain durable application state.

## Correlation fields

Carry the Flow ID through controller logs and responses. Add Run ID, Step execution ID, Step type, RPC name, Channel name, and retry attempt when available. Do not log only a Rust thread ID or in-memory object address; those disappear on Worker replacement.

Use stable application error types from `HandlerError`, and preserve the underlying `SdkError` when mapping errors at the HTTP boundary. Avoid payload logging by default when values may contain credentials, personal data, or large blobs.

## Health and lifecycle

Call `Client::health_check` after bootstrap before declaring the application ready. Monitor both Dex connectivity and the Worker serving endpoint. A healthy controller with a dead Worker cannot advance Steps.

At shutdown, stop the Worker, join its thread, and close BlobCache. A panic in the Worker thread must not be silently discarded.

## Heartbeat progress

Heartbeat long-running Execute work at bounded intervals. On retry, resume from the last heartbeat value. Check cancellation during the same loop.

[Runnable source](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/rust/src/primitives/heartbeat/flow.rs)
<!-- dex-source: examples/rust/src/primitives/heartbeat/flow.rs -->
```rust
let completed_batches = context.last_heartbeat_value::<i32>()?.unwrap_or_default();
for batch in completed_batches..batches {
    if context.is_cancelled() {
        return Ok(StepDecision::dead_end());
    }
    thread::sleep(Duration::from_secs(2));
    context.record_heartbeat_value(batch + 1)?;
}
```

The heartbeat is recovery progress, not the final business commit. Write it only after the corresponding batch is safely repeatable or complete.

## Stream output

Use `Stream<T>` for durable progressive output and the Client Stream APIs for consumers. Buffered text reduces frame overhead and is suitable for tokens, logs intended for users, and render progress. Buffer flush is asynchronous; a handler acknowledgment does not mean storage has accepted every emitted frame. Do not use Stream as the sole audit record for money movement or access decisions.

## Metrics and server diagnostics

Consult the Dex production metrics documentation for current metric names rather than inventing SDK counters. Correlate Worker invocation failures and retry attempts with Flow/Step identity. When a Flow appears stuck, inspect:

1. current Flow status and active Step executions;
2. expected pending Channel messages or timers;
3. Worker registration and reachability;
4. last handler error and retry policy;
5. locks or transactions that can conflict;
6. loaded versus declared persistence state.

The Rust SDK does not expose an application tracing facade at this baseline. Use the application's logging/tracing stack around controller and handler boundaries, and do not claim automatic span propagation unless the installed SDK documents it.
