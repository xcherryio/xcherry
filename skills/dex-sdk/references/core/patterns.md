# Pattern selection

Choose the smallest tested Flow shape that matches the business requirement. Then read the selected language's **patterns.md** for exact APIs and runnable sources.

## Root Flow admission

Start one domain-named root Flow directly from the application boundary. Use a stable Flow ID for the logical operation, a Request ID derived from the complete logical start request, and the explicit ID reuse policy that matches the lifecycle. A retry of the same request reuses both identities; the same Flow ID with a different Request ID is a conflict unless the domain intentionally routes commands to one coordinator.

Do not create a separate database table, row, outbox, lease, lock, cache, or admission projection to deduplicate or serialize Flow starts. Store request fingerprints or accepted state only when they are genuine fields of the owning domain record. When the caller needs a durable acceptance response before the Flow completes, wait for the named admission Step and then read that domain record.

## Parallel Steps

- **Static parallelism**: the branches are known when the Flow is defined. Emit explicit independent Steps; add a join only when the business outcome requires one.
- **Dynamic parallelism**: the input determines branch count. Bound concurrency and retain a durable correlation between each input item and result.
- **Await all**: every branch matters. Decide whether one exhausted branch fails the whole operation or routes to recovery while the join waits for the remaining branches.
- **First win**: one acceptable result is enough. Make the winner durable and define cancellation or harmless completion for losers.

Parallel Steps are the default when branches share one Flow identity and
lifecycle. Do not turn a fixed graph into runtime fan-out solely to reduce
source code.

## Parallel SubFlows

Do not introduce SubFlows in an initial design. These patterns are evolution
tools for an existing Flow whose graph is already impractical to review,
evolve, or operate, or whose fan-out genuinely exceeds the 200-concurrent-Step
architecture-review threshold. First reject parallel Steps, batching, Channel
coordination, RPCs, and ordinary code abstraction with concrete reasons, define
the complete parent-child lifecycle, and obtain explicit user confirmation.
The threshold is not a Dex Server limit and does not automatically select a
SubFlow.

- **Basic**: child work needs its own identity or lifecycle; the parent waits for every child.
- **Short-lived parent**: drain the admitted batch and complete only when the active-child count is zero and the request Channel is atomically empty.
- **Long-lived parent**: the parent remains available to admit, observe, or coordinate children over time.
- **Wait for half**: quorum is sufficient. Define what happens to children outside the quorum.
- **Partitioning**: route work by a stable partition key so each parent owns a bounded subset.
- **Back pressure**: cap buffered requests, reject admission when full, and let a durable submitter retry with backoff.

The examples below apply only after that gate. Independent retry, scaling,
ownership, or identity alone is insufficient. State parent completion, child
reuse, cancellation, duplicate submission, and concurrency semantics
explicitly.

## Polling

Use this one pattern whenever a Flow waits for an external system to become ready or finish, such as a cloud build, Kubernetes rollout, provisioning job, or third-party API status. Business signals inside Dex (Channels, RPCs) and business timers (TTL, reminders, inactivity) still use WaitFor; they are not polling.

One long-running Step's Execute owns the whole wait. Each round:

1. Call the external API with its own short timeout, such as 10 seconds.
2. On a transient error, return the error so the Execute retry policy re-runs the Step.
3. On a terminal status, return the next decision: success, or a business-failure decision for a definite business failure such as a job the provider reports as failed.
4. Report progress: write a Stream frame when the observed status changes (each write is an implicit heartbeat); otherwise record a heartbeat with the resume checkpoint.
5. Sleep for the interval with the language's plain sleep, then repeat.

“Not ready yet” is a normal loop branch, never an error. Keep `interval + call timeout <= HeartbeatTimeout - 10s`; with the one-minute default and a 10-second call timeout, the interval is at most 40 seconds. Use a few seconds when a user is watching something they just started and tens of seconds for long jobs such as builds. Do not add cancellation-select machinery around the sleep: the call has its own timeout, and after a cancelled attempt the next call fails fast.

Every wait limit comes from the polling Step's Execute options, and the engine enforces each one. The Step code never computes a deadline:

- **Method timeout**: the maximum wait. It bounds one attempt; an omitted value uses the two-hour server default.
- **Retry**: a few attempts with backoff for Worker crashes and transient errors, with a total duration equal to the maximum wait. The total duration is measured from the first attempt's schedule, includes every retry, and also cuts the in-flight attempt. An omitted total duration uses the four-hour server default, so always set it.
- **Heartbeat timeout**: keep the one-minute default, so a stalled Worker is detected within a minute rather than after the whole attempt timeout.
- **Durability**: override Execute to SYNC when the Flow default is ASYNC; the ASYNC local phase ignores heartbeat and method timeouts.

When the method timeout or the total duration expires, the engine fails the Step with a timeout. Route Execute failure to a failure-record Step that reads the recovery error (detail and error type) and records the business failure; without an Execute-failure route the Flow fails. The last observed status is visible in the progress Stream and logs, not in the recovery error.

The heartbeat checkpoint is only resume state, such as the last reported status so a new attempt does not repeat a progress frame; it never carries a deadline. A business condition that is not a fixed duration, such as stopping when a lease the Flow does not own expires, is still checked inside the loop; it is a business rule, not a polling timeout. Stream progress is best effort; consumers recover from durable Attributes or a snapshot RPC. Write frames only on status changes so the shared Stream budget is not flooded. Do not hold Execute lock Attributes for the whole wait; commit results that need a lock in a short following Step.

Never:

- WaitFor a Timer and `GoTo` the same Step to poll again.
- Use retry policy, backoff, or RetryAfter as the polling loop by returning “not ready” as an error.
- Run a long loop that never heartbeats or writes progress.
- Call an external system without its own timeout.
- Hand-write a deadline that duplicates the method timeout or the retry total duration, whether in the loop, the heartbeat checkpoint, or the Step input.
- Pass a page token to the Step's next execution to iterate; the Iteration pattern is removed.

## Durable timers

- **Cron**: calculate and persist the next schedule occurrence; handle missed or duplicated external effects idempotently.
- **Reminder**: self-loop after delivery while the durable completion condition remains false.
- **Inactivity tracking**: race a reset signal against a Timer and create a new deadline after each accepted activity.

A Timer is a durable condition, not an in-process timer. Define skip, cancellation, and late-delivery behavior.

## Failure handling

- **WaitFor recovery**: route a failure establishing the wait to a registered recovery Step.
- **Execute recovery**: route exhausted execution retries to explicit compensating or operator-visible work.
- **Manual recovery**: persist the failure and wait on a durable operator decision Channel.
- **Graceful timeout**: use a terminal timeout policy or a timeout handler that records a business outcome and performs bounded cleanup.

Compensation runs in reverse business order, is idempotent, and has its own failure route. Never catch an error only to report success.

## Channel draining

- **Internal publishing**: the Flow knows when every internal producer is finished; drain until producer completion and queue exhaustion are both durable.
- **External publishing**: use the atomic complete-if-Channels-empty decision so an external publication cannot race between the final empty check and Flow completion.

Keep accepted messages durable and define the point after which publication is rejected or redirected.

## Interruptible execution

Write interruption intent to an Attribute through an RPC and check it at bounded Step boundaries. Persist the outcome and make cleanup explicit. Do not assume cancelling a local future reverses an external side effect.

## Responsive update

- **Step completion**: wait for a selected named Step to commit when the caller needs durable acceptance before full Flow completion.
- **Attribute match**: wait for an application-owned revision Attribute to advance, then reload canonical state through a Describe or read RPC.
- **Stream**: publish low-latency progress when loss or duplication is acceptable; reload durable Attributes for canonical state.

Use Channels for durable commands, Attributes for durable state, and Streams for best-effort progress.

Initialize the revision to zero. Every Step and RPC that advances the represented state must declare the same Attribute lock, then read, increment, and write the revision inside that invocation. Do not advance it through concurrent Client Attribute writes.

Call **WaitForAttributeMatch** with greater-than and the last observed revision. The wait returns the current matched revision; use that value as the next watermark, then call the application's Describe or read RPC. A rapid `0 → 1 → 2 → 3` transition may return `3` from one wait for greater than `0`. This is coalescing, not an event stream, and it does not expose Flow history or run IDs. WaitForAttributeMatch rejects [hard-delete Attributes](data-handling.md#hard-delete-attributes-for-short-lived-secrets); keep the revision in a regular Attribute and read the hard-delete value through a typed RPC when a caller needs it.

Request IDs are optional for both waits. Step completion derives `wait-for-step-completion:<StepExecutionID>`. Attribute matching derives `wait-for-attribute:<AttributeName><Condition>` from the exact predicate. The SDK automatically reattaches transport long polls with the effective ID, so one logical wait remains one accepted durable wait rather than a new wait for every transport window. An explicit Request ID overrides the derived value and must identify only that logical wait.

Request timeout is the caller-visible budget for the entire SDK call, including every transparent transport reattachment. Zero waits indefinitely until caller cancellation or another terminal result. A positive expiry returns the language's typed Request Timeout error. It does not terminate the accepted durable wait, so the same logical wait can reattach later. The server's transport cap is internal: its expiry neither ends the SDK call nor creates another handler generation.

Internal handler timeout bounds one accepted handler generation of the wait. Leave it at zero for ordinary waits. Set it only when abandoned callers, dynamic predicates, many consumers, or conditions that may never match could approach the per-Flow limit on concurrently accepted waits. Expiry releases that slot without surfacing a handler-timeout error; an active SDK call transparently advances the derived or explicit Request ID through `-1`, `-2`, and later generations. Each generation also counts toward the per-Flow total of accepted waits. On a Temporal-backed deployment the default limits are 10 concurrent and 2,000 total accepted waits per Flow; they are deployment limits, not Dex guarantees, so verify them for the target environment. Prefer a value comfortably longer than normal request timeouts and reconnect gaps. For an intentional infinite revision loop, use the last returned revision as the next greater-than operand; the changed predicate produces a new derived Request ID.

String and Boolean Attributes support equal and not-equal. Integer and floating-point Attributes support all six comparison operators. Missing Attributes never match. Cross-type comparisons do not match. Object, bytes, null, blob-backed, non-finite floating-point, invalid operator, and invalid ordering operands fail. Attribute match waits target the current active Flow and are available on Temporal-backed deployments; a Cadence-backed deployment returns Unimplemented.

## Entity store

Represent one entity lifecycle as a Flow, keep its current state in Attributes, mutate it through RPCs or Channels, and synchronize selected Attributes to an application-owned relational schema when external queries need them. Define optimistic concurrency, deletion, retention, and open-Flow compatibility. A store with a `rowKey` template (Dex Server v1.6.0 or later) can key the projected row by the entity's business ID taken from the Flow ID, and an AttributeMap table can project the entity's keyed records as rows of their own. Choose the Flow ID format and the template together; see [row keys, column names, and AttributeMap tables](data-handling.md#row-keys-column-names-and-attributemap-tables).

## Partitioned AttributeMap state

- **Sequentially chunked:** append to one bounded `current` instance. When it reaches the fixed chunk size, copy it to an immutable sequence-named instance and replace `current` in the same commit. This preserves append order and supports newest-first page tokens without rewriting all history. All appenders contend on the active-instance lock; retry only the rejected invocation after a typed RPC lock conflict.
- **Hash-partitioned:** canonicalize a stable lookup key, hash its bytes with a versioned algorithm, and choose one of a fixed number of instances. Store a dictionary keyed by the complete canonical value inside each bucket so hash collisions cannot overwrite records. Lock and load the same exact instance for an upsert; a read loads only that instance. Changing the hash algorithm or partition count requires migration because the mapping is persisted schema.

Use sequential chunks for append-only messages, audit events, or subscriber history. Use hash partitions for customer profiles, account metadata, or configuration records that need direct keyed lookup and updates. Neither pattern creates server-side map pagination or a secondary index.

For a cross-instance write invariant, use a [singleton coordination Attribute](data-handling.md#whole-map-coordination) shared by every writer of the protected map. This is an application lock convention, not an automatic lock over all map instances. Independent partition updates can retain per-instance locks when no map-wide invariant applies.

## Selection checklist

- Does the Flow graph expose every success, wait, retry exhaustion, cancellation, timeout, and terminal path?
- Is every runtime-sized fan-out bounded?
- Are losing or abandoned branches safe?
- Are side effects idempotent or compensated?
- Is progress correctly classified as durable or best effort?
- Can a replacement Worker resume from durable state alone?

Sources:

- Pattern catalog: https://docs.superdurable.io/design-patterns
- Polling: https://docs.superdurable.io/design-patterns/polling
- Sequential chunking: https://docs.superdurable.io/design-patterns/sequentially-chunked-attribute-map
- Hash partitioning: https://docs.superdurable.io/design-patterns/hash-partitioned-attribute-map
- Baseline runnable implementations: https://github.com/superdurable/dex/tree/sdk-go/v1.14.1/examples
