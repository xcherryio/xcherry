# Error handling design

Use the [read-after-write matrix](read-after-write.md) to distinguish accepted mutations, direct-state readback and later business completion. Do not classify every RPC write as eventual. A successful direct-state RPC write followed by a matching typed read-only RPC needs no convergence polling; a failed or ambiguous write still needs reconciliation.

Use this guide before implementing a Client boundary, not only after a failure. Design the outcome of Flow starts, RPCs, cleanup, admission, waits, and external Stream writes alongside the happy path. Then read the selected language's **error-handling.md** for its actual public error model.

## Classify at the decision boundary

Classify a failure at the narrowest boundary with enough context to decide its meaning:

- **Business outcome**: an expected rejection, conflict, or terminal domain decision.
- **Dex or provider failure**: a typed service, transport, authentication, or availability error that policy may retry or translate.
- **Local defect**: invalid input handling, incompatible definitions, serialization, or programming errors that must remain visible.

Normal application logic catches only concrete SDK errors whose outcomes it can decide. Do not enumerate every Client failure merely to turn them all into the same retryable response; leave an unclassified failure to ordinary server-error handling unless the boundary can prove it is retryable. A narrow reconciliation boundary after a failed mutation may handle documented remote failures only when each one leaves the same mutation outcome uncertain. Use a named status or sub-status only when the SDK intentionally has no more specific error; never branch on human-readable detail or raw numeric codes. Never catch a language's broad runtime or exception base for service-error translation.

The public error shape is language-specific. Python, Java, and TypeScript expose a remote-only service-error base; Go concrete remote errors unwrap to `*ServiceError`; Rust uses one `SdkError` enum for both service-backed and local failures. Never copy a catch pattern between SDKs without checking the selected language page and installed version.

## Missing or inactive target errors

At the released SDK API baseline, use the exact public names:

| SDK | Combined missing/inactive target error | Source |
| --- | --- | --- |
| Go | `*dex.FlowNotActiveOrNotFoundError` | [Definition and mapping](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/sdk-go/dex/errors.go) |
| Python | `FlowNotActiveOrNotFoundError` | [Definition](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/sdk-python/dex/runtime_errors.py) |
| TypeScript | `FlowNotActiveOrNotFoundError` | [Definition](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/sdk-typescript/src/errors.ts) |
| Java | `FlowNotActiveOrNotFoundException` | [Definition](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/sdk-java/src/main/java/io/superdurable/dex/exceptions/FlowNotActiveOrNotFoundException.java) |
| Rust | `SdkError::FlowNotActiveOrNotFound` | [Definition and mapping](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/sdk-rust/crates/dex-sdk/src/sdk_error.rs) |

These names replace the old not-active-only names without compatibility aliases. The rename preserves Server routing, protocol sub-statuses, service metadata, and cause chains. The separate Flow-not-found error remains available for operations mapped to it. Inspect an existing application's installed SDK before changing a catch or import; do not assume older packages export the new names.

The combined type alone cannot distinguish a missing Flow from an unusable closed target. Interpret it using the actual operation: at a confirmed query-only Get boundary it means missing/unreadable target, while an active-required mutation can also encounter a closed target. Neither outcome proves the requested mutation succeeded. Preserve unrelated Worker, transport, and service failures.

## Start identity and duplicate starts

### Start first; reconcile only after an error

For an operation that starts a Flow, call `StartFlow` first with a stable Flow ID and the intended ID reuse policy. Do not call a read-only RPC, search, or lifecycle/status API first merely to check existence or avoid a possible retry/AlreadyStarted edge case. That adds a round trip to every normal start and still races with another caller. A read required independently by the business contract is different from a start-deduplication preflight; do not introduce one just for defensive retry handling.

Use Dex itself as the start deduplication boundary. Treat the Flow ID and start Request ID as separate identities: derive the Flow ID from the logical operation or resource, and preserve one stable Request ID across retries of the same complete logical start request. Choose an explicit ID reuse policy consistent with that lifecycle. Do not add an application-owned table, row, outbox, lease, lock, cache, or generic admission projection solely to deduplicate or serialize starts.

A stable Request ID alone does not suppress AlreadyStarted. For a retry that may attach to the existing execution, also enable the SDK's ignore-already-started option. That option suppresses the error only when the retained execution's start Request ID matches; it does not accept any existing Flow indiscriminately. The Server compares Request IDs, not business-input equality, so never reuse one ID for different logical requests. For replay of one logical request, use an ID reuse policy that prevents another execution even after the original closes. A policy permitting a new execution can bypass the AlreadyStarted/matching-Request-ID path; the Request ID alone does not prevent that new start.

When the Flow ID already exists and the reuse policy rejects creating another execution:

| Start Request ID | Ignore-already-started option | Result |
| --- | --- | --- |
| Same as the existing start | Disabled or omitted | Typed AlreadyStarted error |
| Same as the existing start | Enabled | Success with the existing execution |
| Different from the existing start | Enabled or disabled | Typed AlreadyStarted error |
| Omitted from the SDK call | Enabled | SDK-generated IDs do not provide stable identity across separate calls; a retry can still return AlreadyStarted |

Handle the result at the application boundary:

- **Success:** when the contract only acknowledges acceptance, return the accepted-start response from validated/normalized input and the known initial-state fields that the response contract guarantees. Do not immediately read a snapshot to recheck identity, fingerprint, ownership, or Attributes supplied to StartFlow, or to decide whether to launch the next Flow. With correctly bound request identity, a replay attaches to the same logical start; success does not mean asynchronous work has completed or its current state is still the initial state. Only an explicitly required admission/result contract justifies waiting for its named durable boundary or obtaining its result; do not invent a current-snapshot requirement for a start acknowledgement.
- **Typed AlreadyStarted:** without the ignore option, this can still be the same Request ID. With the option enabled, the existing start was not confirmed as the same request. Preserve a domain conflict unless the resource/coordinator contract explicitly permits reuse. When the outcome requires existing business state, invoke its typed read-only RPC in this error branch and verify the relevant request identity or business invariant before declaring success. A successful read alone does not prove the attempted request ran; do not retry a proven conflict indefinitely or report it as service unavailability.
- **Failure leaving acceptance unknown:** retry within a bounded policy using the same Flow ID, stable Request ID, and start options, or reconcile authoritative domain state after that failure when it can establish the outcome. A replay-safe start does not need a mandatory read before retrying. If no stable Request ID is available, do not claim cross-call request deduplication; use the explicit AlreadyStarted/domain reconciliation path or report an unknown outcome when the contract cannot prove acceptance. Do not shadow Dex start identity in a dedicated database record.

```text
Wrong: read snapshot/check existence → choose whether to StartFlow
Wrong: successful StartFlow → reread identity/status → launch another Flow in the API
Right: StartFlow with stable identity → accepted response from known request fields
       → only on a relevant error, reconcile through the typed snapshot RPC
         or return a domain conflict/unknown outcome
```

### Wait for a required business milestone

When the response contract requires an important operation to finish, call the selected SDK's Attribute-match wait or Step-completion wait immediately after StartFlow succeeds, including matching-request deduplication. A deduplicated start does not bypass that completion requirement. Do not add a preflight read or an identity-verification RPC between the start and the wait.

For example, an API may promise that a business row is committed to a database table used as the source of truth before it returns success. The owning Flow performs that write in Execute. Publish a committed Attribute only after the database commit, or complete the selected persistence Step only after the commit succeeds. Wait for that committed value or the specific Step execution, then return the promised milestone outcome. Step completion does not return Step output or prove the whole Flow finished; an Attribute set before the database commit cannot prove persistence.

Choose the smallest durable condition that establishes the promised outcome. If the contract only acknowledges accepted background work, return after StartFlow without a wait. Bound required waits by the caller's deadline/request budget. A wait timeout or caller cancellation does not undo the accepted start or prove the write failed; preserve its identity and explicit pending/unknown completion outcome rather than starting a replacement. For a Flow that can close before an Attribute wait attaches, handle the actual wait error through authoritative business state or the supported terminal-read contract; do not assume Attribute waits read closed executions.

```text
Acceptance-only API: StartFlow succeeds or deduplicates → return accepted
Commit-confirming API: StartFlow succeeds or deduplicates
                      → wait for committed Attribute or persistence Step completion
                      → return confirmed database commit
```

## Closed-Flow races

For an operation that requires an active target, a typed terminal or not-active error proves that the attempted interaction had no active target. It does not prove that the requested work succeeded. First reconcile from already loaded authoritative domain state and operation invariants. Re-inspect the Flow only when the outcome depends on distinguishing running, successfully completed, other terminal, and missing states and that distinction is not otherwise available. Do not spend a status call when every possible state has the same idempotent outcome. Interpret query-only RPC errors under the [missing query target rule](#missing-query-targets) instead.

Do not assume every RPC targets only an active Flow. A query-only RPC without locks or transactional execution can read a retained terminal execution. It may succeed after closure, so use a lifecycle API when active versus terminal changes the result. If a non-transactional handler returns durable effects, it may run before Dex rejects those effects with a not-active error. Transactional and locked RPCs, and every RPC under a Server policy that runs RPCs transactionally, require an active execution before the Worker handler runs.

A completed child may satisfy an idempotent cleanup only when successful completion guarantees the requested condition. An unsuccessful terminal or missing child should become an explicit domain failure or unknown outcome; do not retry a terminal fact indefinitely. A bounded wait that returns a running snapshot is still nonterminal.

### Terminal read RPC rule

Before handling a typed not-active error from an RPC, inspect its registered options, any invocation-time AttributeMap locks, the handler result, and Server routing policy. A business snapshot RPC with no Attribute locks, no transactional execution, no durable effects, and no Server policy requiring an active execution must read the retained terminal execution directly. Closure alone is not a reason for this query to fail. Handle typed missing-target errors under the [query-only rule](#missing-query-targets). Preserve other routing, handler, retention, or service failures; do not mask them with a history fallback.

Do not add a `FlowNotActiveOrNotFoundError → WaitForFlow → Step-output decoding` fallback to a business read. Read the snapshot from its typed read-only RPC and retained Attributes/AttributeMaps. Do not reconstruct it by matching historical Step names: execution history is not the entity's read contract, and an intermediate Step output may omit later committed state.

Use `WaitForFlow` when the caller explicitly needs engine terminal status or completion output, or to reconcile an inactive mutation/idempotent cleanup whose outcome cannot be established through authoritative domain state or a typed read-only RPC. Keep its completion-output contract distinct from an entity snapshot. History remains appropriate for explicit diagnosis or execution recovery, not routine business reads. Retention expiry or a missing execution must retain an explicit unavailable/missing outcome.

Anti-pattern and replacement:

```text
Wrong: typed snapshot RPC fails as not-active
       → WaitForFlow → find an earlier Step output in history → return snapshot
Right: register a typed snapshot RPC as query-only over retained Attributes
       → invoke that typed RPC directly before and after closure
       → surface genuine read failures; request engine status separately only if needed
```

### Missing query targets

At a business Get boundary whose RPC is confirmed query-only by the checks above, translate the SDK's typed combined missing/inactive error or separate Flow-not-found error directly to the contract's not-found result. A retained closed execution is readable on this path; the error means no readable target execution exists, not that closure needs reconciliation. InvokeRPC maps the Server's Flow-not-found sub-status to the combined missing/inactive type even for a query; its name does not make that query active-only.

Do not follow that missing-target error with WaitForFlow, DescribeFlow, search, history lookup, a short timeout probe, or a retry just to distinguish missing from closed. Return the missing result at the Get boundary. No readable retained execution does not prove the business entity never existed; preserve the domain contract's retention/unavailable distinction when it requires one. Keep other service and Worker failures visible rather than converting every read error to not-found.

This translation belongs only to that confirmed query-only boundary. Mutating, transactional, and locked RPCs, and RPCs under a Server policy that runs every RPC transactionally, can fail because a retained execution is closed; their not-active error still needs the operation-specific interpretation above.

```text
Wrong: pure Get → FlowNotActiveOrNotFoundError → WaitForFlow with a short timeout → decide missing
Right: pure Get → typed missing/not-active error → return the Get contract's not-found result
Closed retained Flow → the same pure Get → return its retained snapshot
```

## Ambiguous mutations

After an ambiguous provider or Client mutation, reconcile authoritative remote or domain state before repeating an effect whose safe replay is not established. A StartFlow retry with stable Flow ID/Request ID and matching start options follows the [start-first rule](#start-first-reconcile-only-after-an-error) and can replay without a preflight read. Bound retries and keep the repeated mutation idempotent. Preserve the stable request identity across every retry of the same logical mutation.

## Best-effort output and fast closure

When a Stream or progress write is explicitly best effort, select only service-backed failures using the language SDK's public model because the side channel deliberately treats those failures as lossy. This means the remote-only base in Python, Java, or TypeScript, `errors.As` to Go's `*ServiceError`, or Rust's service-backed variant for that operation. Log sanitized identity and phase metadata and continue the business Flow. Let local definition, validation, serialization, and programming failures surface. Retained Stream data is never the authoritative record of business completion.

If an API must return the first admission decision after a Flow can close quickly, wait for the named admission Step or Flow result and read any accepted business state from its owning domain record. A typed read-only RPC over retained Flow state can serve that business read; an active-only admission or mutation RPC cannot be the sole source of correctness after closure. Do not create a generic admission projection solely for start deduplication.

## Design review

- Does every caught error change a domain decision, retry policy, or boundary translation?
- Are typed conflicts handled before a remote-error fallback?
- Can an accepted mutation lose its response, and if so, what authoritative fact reconciles it?
- Does Flow start idempotency rely only on Dex identity rather than an extra database mechanism?
- Does the normal start path call StartFlow first and return its accepted response without preflight or post-success identity/Attribute verification reads?
- If success promises a critical business operation, does the success/dedup path wait for a condition published after that operation commits, with a bounded wait and no acceptance-only waits?
- Does the owning Flow durably coordinate downstream work instead of chaining dependent StartFlow calls in the API handler?
- Do start retries preserve Request ID and the ignore-already-started option together, while conflicts and unknown acceptance reconcile only in their error branches?
- Are retries bounded, idempotent, and tied to one stable Request ID?
- Can a closed or missing Flow converge without an unnecessary status call?
- Do terminal snapshot reads use typed query-only RPCs with the required collection loads, without locks, transactions, durable effects, or active-only Server routing?
- Does a confirmed query-only Get map typed missing/not-active directly to its missing result, without a lifecycle probe, retry, or added timeout?
- Is every terminal-readable entity covered by a real-server post-closure RPC test, including an assertion that the business read uses no lifecycle/history fallback?
- Does best-effort observation remain separate from authoritative business state?
- Do local definition, mapping, serialization, and programming defects remain visible?
