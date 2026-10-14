# Go error handling

Before designing sequential writes and reads, use the [shared read-after-write matrix](../core/read-after-write.md). A typed read-only RPC issued after a successful direct-state RPC write observes that write when it loads the written state; search indexes, Attribute Store projections and triggered business completion are separate. Follow this page for the language-specific error and timeout model.

## Error layers

Application errors from WaitFor, Execute, RPC, and timeout handlers drive configured retry/recovery. Typed Client errors describe Dex outcomes. Context/transport errors describe caller cancellation or connectivity. Keep these layers distinct.

Use `errors.As` for `*dex.FlowNotFoundError`, `*dex.FlowNotActiveOrNotFoundError`, `*dex.FlowAlreadyStartedError`, `*dex.LongPollTimeoutError`, `*dex.RequestTimeoutError`, and `*dex.FlowUncompletedError`. Durable Step and Attribute waits never expose transport long-poll expiry; they reattach with the effective Request ID and preserve the total `RequestTimeout` budget. `RequestTimeoutError` means that caller-visible budget expired, not that the Flow or accepted durable wait failed. `InternalHandlerTimeout` rollover is transparent. Keep `ServiceError.SubStatus` for diagnostics; never parse strings.

Every concrete remote Client error unwraps to `*dex.ServiceError`; local definition, value-mapping, argument, and programming errors do not. Use `errors.As(err, &serviceError)` only at a narrow boundary whose policy intentionally treats every remote Dex outcome the same. Ordinary domain logic should continue matching the concrete error type it can decide.

For an explicitly best-effort external `Client.WriteStream`, an `errors.As` match on `*dex.ServiceError` may be logged with sanitized identity and discarded. Otherwise return the error. Context operations inside a handler, including Stream writes, must still return or wrap their error so Dex owns retry and recovery.

## StartFlow error handling

Follow the shared [start-first rule and result matrix](../core/error-handling.md#start-first-reconcile-only-after-an-error). Call `Client.StartFlow` directly; do not preflight with `InvokeRPC`, `GetFlowSummary`, or search merely to avoid AlreadyStarted or retry uncertainty.

For a retry that may attach to the same logical start, pass a pointer to the stable request identity in `StartFlowOptions.RequestID` and set `AlreadyStarted: &dex.AlreadyStartedOptions{IgnoreError: true}`. Preserve both across retries; use `IDReusePolicy: dex.IDReuseDisallow` when this logical start must not create another execution after closure. An intentional new lifecycle needs its own explicit identity/reuse contract. Setting RequestID alone still permits `*dex.FlowAlreadyStartedError`; setting IgnoreError alone does not suppress a conflict with a different Request ID. A nil RequestID makes the SDK generate a fresh UUID for each separate StartFlow call, and a pointer to an empty string is invalid.

When `Client.StartFlow` returns nil error and the contract only acknowledges acceptance, return the accepted DTO using validated/normalized input and known initial fields; do not unconditionally call `InvokeRPC`/`Read` to compare fingerprints, owners, or initial Attributes. Bind RequestID to the complete immutable request before the call: the Server's matching-ID success does not compare payloads. Keep any explicit admission/completion-result wait separate from an accepted-start acknowledgement.

When the API promises a critical operation has completed, follow the shared [required business milestone rule](../core/error-handling.md#wait-for-a-required-business-milestone): after start success or matching-request deduplication, call `Client.WaitForAttributeMatch` or `Client.WaitForStepCompletion` for the committed condition or the specific persistence Step execution. Publish that condition only after the business effect commits. Bound the wait; timeout does not undo acceptance or prove the operation failed. An acceptance-only response needs no wait.

The API handler must not call StartFlow again to launch dependent work after the first start or a snapshot/status check. Follow [durable downstream-start ownership](../core/modeling.md#own-downstream-starts-durably): use Steps in one lifecycle, or start a separately owned top-level Flow from Execute through an injected Client with stable downstream identities/options. Let the Step's retry/recovery resume a lost downstream start response or a crash before its commit; do not build an HTTP-handler retry loop to orchestrate the sequence.

Handle `*dex.FlowAlreadyStartedError` with `errors.As`. Inspect the start options before interpreting it: with IgnoreError disabled, even the same request can produce the error. With IgnoreError enabled, the Server could not confirm a matching start request. Return a domain conflict unless the contract explicitly allows the existing Flow to serve this operation. If that decision needs existing state, call the typed read-only `Get*` RPC only in this error branch, and verify the relevant request identity or domain condition before treating the operation as successful. Do not add a lifecycle/history fallback to the read.

For a failure that leaves acceptance unknown, a bounded retry can call StartFlow again with the same stable identities and options, without an intervening read. Alternatively read owning domain state after the error if it can establish acceptance. Preserve local/validation errors; do not turn every `*dex.ServiceError` into a replay. See the shared [start verification scenarios](../core/testing.md#startflow-ordering-and-retry-identity).

## Terminal business reads

Apply the shared [terminal read RPC rule](../core/error-handling.md#terminal-read-rpc-rule) before catching `*dex.FlowNotActiveOrNotFoundError` from a snapshot RPC. Inspect its `dex.DefineRPC` registration in `GetRPCs`: `dex.RPCOptions.LockAttributes` must be empty and `IsTransactional` false for a terminal query. Include any `RPCInvokeOptions.LockAttributeMapInstances` in that check; collection loads alone do not require a transaction. The handler must return only typed output, with no durable effects, and Server policy must permit the query path.

Call `Client.InvokeRPC` (or `InvokeRPCWithOptions` for selective instance loads) directly on the typed `Get*` method. Do not catch not-active and decode historical Step outputs into the snapshot. Reserve `WaitForFlow` for its explicit lifecycle/completion or mutation-reconciliation contract. Prove the read through the real-server [terminal entity test](../core/testing.md#terminal-entity-reads).

## Query-only Get failures

Follow the shared [missing query target rule](../core/error-handling.md#missing-query-targets). For a business Get confirmed to have no registration or invocation locks, no transaction, no returned durable effects, and no Server policy that runs every RPC transactionally, catch `*dex.FlowNotActiveOrNotFoundError` or `*dex.FlowNotFoundError` with `errors.As` and return the contract's not-found result directly. Retained closed executions remain readable, so do not call WaitForFlow, describe/search/history APIs, or add a timeout probe or retry to distinguish missing from closed. Preserve other errors and any explicit retention/unavailable contract. Do not apply this translation to mutations or active-only RPC paths.

## Retry ownership

Return an error when Step options should decide retry. Use `dex.RetryAfter` only as explicit backoff for a transient error whose meaningful delay the application knows, such as a provider's rate-limit hint. It is not a polling loop: an external “not ready yet” status is a normal branch of the [Polling pattern](patterns.md#polling), never an error. Never add an in-memory retry loop around failed Step work; it disappears with the Worker and hides attempts.

## Commit and recovery

Before non-idempotent external work, use an idempotency key derived from durable identity. After a successful side effect, persist the fact or move to a recovery-capable Step. A returned error permits retry from the previous boundary.

Use Execute/WaitFor failure policies for automatic recovery, operator Channels for manual recovery, and a timeout handler for Flow-deadline semantics. Compensation must be idempotent and identify the original commit.

Every Context operation can fail. Return or wrap its error; do not continue after a failed Attribute write, Channel delete, heartbeat, or Stream write as though it committed.
