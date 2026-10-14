# Flow modeling

Use this guide before implementing a non-trivial Flow or when a Flow has become difficult to reason about.

## Find the Flow boundary

A Flow should represent one durable business execution with a stable identity
and lifecycle, such as an order, subscription, transfer, approval, or
processing job. Do not conflate parallel Steps, independent top-level Flows,
and parent-child SubFlows.

For each candidate data set or process, ask:

1. Does it have a different authoritative owner and retention or cleanup
   lifecycle?
2. Does it have independent waits, Timers, and terminal outcomes?
3. Would putting it in an existing Flow force that Flow to retain, clean up, or
   coordinate state unrelated to its primary lifecycle?
4. Would the split make the Flows exchange RPCs or Channel messages on frequent
   events, such as every user operation, every external callback or every poll?

A yes to question 4 rules the split out, whatever the other answers. Every call
between Flows adds entries to both Flows' history, and every RPC, including a
read-only one, is a separate engine operation with its own latency and, on
metered backends, its own cost. A separate Flow that tracks another Flow's
inactivity, for example, needs a message from every operation; keep that
deadline in the Flow where the operations happen. Calls between Flows are fine
for rare events: a start, a completion, a cancellation or an explicit user
command.

When the answers to questions 1 to 3 are collectively yes and the answer to
question 4 is no, use an independent top-level Flow.
Start it independently and coordinate through typed RPCs or Channels; it does
not become a SubFlow merely because another Flow consumes its result. Otherwise
prefer Steps, Attributes, or AttributeMaps in the existing Flow. Field count,
source size, number of phases, retry policy, code reuse, provider neutrality,
or ordinary parallelism does not establish a new Flow boundary.

Keep long-lived authoritative facts in their stable owner. A temporary Flow
stores only the validation or coordination state its own wait requires and
cleans it up according to its own retention policy. Failed, expired, or
otherwise temporary state must not pollute the authoritative store.

## Own downstream starts durably

For one business operation, the API starts one owning Flow and returns its accepted response. Do not sequence dependent top-level starts in the API handler, including `StartFlow → read status → StartFlow`. The process can exit or the request can be cancelled after the first start succeeds, leaving the next start unissued; retrying the HTTP handler does not establish a durable recovery owner.

Put the follow-on work and its typed input/launch intent in the first Flow's durable graph and state. If the work shares its identity and lifecycle, implement it as Steps. If a genuinely separate owner/lifecycle requires another top-level Flow, have an Execute Step start it through an injected Client and coordinate via typed RPCs or Channels. Keep WaitFor for the durable conditions; applications never start or mutate another Flow there. Starting another top-level Flow from a Step does not make it a SubFlow; apply the existing evolution gate only when a real parent-child SubFlow is needed.

A downstream start is a separate acceptance boundary, not an atomic transaction with the caller's Step completion. Preserve its stable Flow ID, Request ID bound to that complete downstream request, and appropriate start/reuse options in durable input or state. If the Worker dies after the downstream start is accepted but before the Step commits, Execute must retry the same start safely. Return or reconcile typed errors so Step retry/recovery owns progress. Never depend on an API-side snapshot/status branch to fill that gap, and never mark downstream work complete merely because its start succeeded.

## Start with parallel Steps

New designs default to no SubFlows. Emit static or dynamic parallel Steps when
branches share one Flow identity and lifecycle. Workers can execute those Step
executions concurrently; use Channels for joins or quorum and batching when the
runtime-sized fan-out needs a bound. Use ordinary language helpers or
interfaces for code and provider abstraction.

Provider-neutral adapters, research phases, model calls, an independent retry
policy, or ordinary bounded fan-out do not justify a SubFlow. Prefer another
Step when work is the next state or a parallel branch of the same business
execution.

## Gate SubFlows as an evolution

Propose a SubFlow only after all of these are true:

1. an existing single-Flow design or running system provides evidence, rather
   than a concern that it may become complex later;
2. the main graph is already impractical to review, evolve, or operate, or one
   fan-out genuinely requires more than 200 concurrent Step executions;
3. parallel Steps, batching, Channel coordination, RPCs, and ordinary code
   abstraction have been evaluated and rejected with reasons;
4. child identity, input/output, parent completion, cancellation, failure,
   retry, duplicate submission, and concurrency semantics are defined; and
5. the user explicitly confirms the SubFlow design.

The 200-Step value is an architecture-review threshold, not a Dex Server
limit. Crossing it permits a SubFlow proposal; it does not select one
automatically.

## Turn the business process into a Step graph

For each Step, record:

- typed input
- Conditions returned by **WaitFor**
- side effects performed by **Execute**
- Attribute and Channel reads or writes
- Flow-default and method-specific durability
- WaitFor and Execute method timeouts
- Execute heartbeat timeout
- retry, selective-load, lock, and failure-route policy
- success transition
- exhausted-retry or business-failure transition

Use stable domain names. A Step type is part of the durable contract of open executions, not merely a function name.

Treat StepOptions as part of the graph design, not tuning added after implementation. Read [step-options.md](step-options.md) before choosing durability or execution policy.

## Separate waiting from work

**WaitFor** declares when a Step may execute. It may prepare durable state needed to establish that wait. Do not call third-party services or perform irreversible side effects there.

**Execute** performs application work and returns the next movement or terminal decision. Make external operations idempotent when Dex may retry the Step.

## Persist only durable coordination state

Use Attributes for state that later Steps, RPCs, application RPC responses, search, or recovery need. Large size alone does not require a separate application store: Dex can keep large values as blobs and hydrate them through the SDK BlobCache. Read [data-handling.md](data-handling.md) before designing another blob or cache layer.

Keep authoritative long-lived business records in a stable owner Flow's typed
Attributes or AttributeMaps when Dex can satisfy their access and retention
requirements. Consider an external store only for a confirmed query,
contention, transaction, analytics, or retention requirement that Dex cannot
reasonably satisfy.

Use Indexed Attributes for bounded lookup and operational search. Use Attribute Store sync when an application-owned database needs a durable projection. When the store splits Flow IDs into key columns with `rowKey`, the Flow ID format is part of that projection contract; see [row keys, column names, and AttributeMap tables](data-handling.md#row-keys-column-names-and-attributemap-tables).

Do not use Channels as generic storage. A Channel message is consumed by a matching wait. Use Attributes for current state and Channels for ordered durable messages.

## Make failure behavior part of the graph

For every external side effect, decide:

- which failures are retryable
- total retry budget and backoff
- whether the operation needs heartbeat/progress reporting
- what happens after retry exhaustion
- whether compensation is required
- whether an operator can recover the Flow
- what deadline ends the business process

Represent compensation and operator recovery as explicit Steps. Do not catch every error and silently mark the Flow successful.

## Design for open executions

Running Flows can outlive a deployment. Prefer additive changes:

- add new Step types instead of changing incompatible behavior in an existing type
- add optional Attributes and RPCs
- retain old Step implementations while open executions can reference them
- version request schemas or RPC names when compatibility cannot be preserved

Use a new routing flag or Attribute so only new executions enter an incompatible path.

## Review checklist

- Can the graph explain every success, wait, failure, cancellation, and timeout path?
- Are Step inputs and transitions typed?
- Are side effects idempotent under retry?
- Does one owning Flow durably sequence dependent starts, including recovery before downstream acceptance and after acceptance but before Step commit?
- Does each Channel have one clear producer/consumer contract?
- Are shared state changes protected when concurrent Steps or RPCs can race?
- Is fan-out bounded?
- Is every top-level Flow justified by owner, retention/cleanup, wait, and
  terminal-lifecycle differences?
- Does any pair of Flows exchange RPCs or messages on frequent events? Then the
  boundary is wrong: merge them, or move the frequent decision into the Flow
  where the events happen.
- Did every SubFlow pass the evolution gate and receive explicit confirmation?
- Can an operator identify the current business state from Dex Web or dexcli?
