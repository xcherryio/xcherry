# Python error handling

Before designing sequential writes and reads, use the [shared read-after-write matrix](../core/read-after-write.md). A typed read-only RPC issued after a successful direct-state RPC write observes that write when it loads the written state; search indexes, Attribute Store projections and triggered business completion are separate. Follow this page for the language-specific error and timeout model.

Application exceptions from `wait_for`, `execute`, RPC, and timeout handlers drive configured retry/recovery. Typed Dex exceptions describe service outcomes; `asyncio` cancellation/timeouts and transport failures describe caller/runtime conditions.

Catch precise exceptions such as `FlowNotFoundError`, `FlowNotActiveOrNotFoundError`, `FlowAlreadyStartedError`, `LongPollTimeoutError`, `RequestTimeoutError`, `FlowUncompletedError`, `RpcLockConflictError`, and not-loaded errors. Durable Step and Attribute waits never expose transport long-poll expiry; they reattach with the effective Request ID and preserve the total `request_timeout` budget. `RequestTimeoutError` means that caller-visible budget expired, not that the Flow or accepted durable wait failed. `internal_handler_timeout` rollover is transparent. Do not parse messages. Let unexpected defects retain tracebacks.

Concrete remote Client errors inherit `DexServiceError`. Local definition, not-loaded, value-mapping, argument, and programming failures do not. Catch `DexServiceError` only at a narrow boundary whose policy intentionally treats every remote Dex outcome the same; do not use it instead of concrete business outcomes. Catching `RuntimeError` is not equivalent because it also captures local SDK and application defects.

When the API promises a critical operation has completed, follow the shared [required business milestone rule](../core/error-handling.md#wait-for-a-required-business-milestone): after start success or matching-request deduplication, call `AsyncClient.wait_for_attribute_match` or `AsyncClient.wait_for_step_completion` for the committed condition or the specific persistence Step execution. Publish that condition only after the business effect commits. Bound the wait; timeout does not undo acceptance or prove the operation failed. An acceptance-only response needs no wait.

For an idempotent start, set one stable `StartFlowOptions.request_id` and `ignore_already_started=True` only when a retry of that same logical request may attach to the existing run. A remaining `FlowAlreadyStartedError` means a different Request ID owns the Flow ID. Treat it as a domain conflict unless the coordinator contract deliberately redirects the command to that existing Flow. Another `DexServiceError` can leave acceptance unknown; retry with the same identities or read owning domain state, never a dedicated start-deduplication table.

For an explicitly best-effort external `Client.write_stream` or `AsyncClient.write_stream`, suppressing `DexServiceError` is appropriate after logging sanitized identity and phase metadata. Do not suppress arbitrary exceptions. Context Stream writes inside a Step remain part of handler execution: yield or propagate their outputs and failures according to the sync or async handler contract.

Return/raise so Step retry owns retries. Use the SDK retry-after mechanism only with a meaningful delay. Never wrap durable Step work in an in-memory retry loop.

External side effects require idempotency keys derived from durable identity. Persist success or transition to recovery before acknowledging. Compensation is idempotent and carries the original commit identity.

Async APIs must be awaited. In a sync generator, yield every heartbeat/Stream `StepOutput`; swallowing one means the runtime never receives it. Do not catch `CancelledError` just to continue unsafe work.

Use automatic failure policies for deterministic recovery, operator Channels for manual choice, and the Flow timeout handler for Flow-deadline semantics. Neither an internal transport reattachment nor a caller-visible request timeout means the Flow failed.

## Query-only Get failures

Follow the shared [missing query target rule](../core/error-handling.md#missing-query-targets). For a business Get confirmed to have no registration or invocation locks, no transaction, no returned durable effects, and no Server policy that runs every RPC transactionally, catch `FlowNotActiveOrNotFoundError` or `FlowNotFoundError` and return the contract's not-found result directly. Retained closed executions remain readable, so do not call WaitForFlow, describe/search/history APIs, or add a timeout probe or retry to distinguish missing from closed. Preserve other errors and any explicit retention/unavailable contract. Do not apply this translation to mutations or active-only RPC paths.
