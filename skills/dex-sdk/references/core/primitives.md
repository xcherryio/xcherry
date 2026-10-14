# Primitive selection

Read only the sections relevant to the task.

## Flow

Use a Flow as the top-level durable business execution. It owns the Step list, persistence schema, and RPC handlers. Give the Flow type and execution ID stable business meanings.

Name each Flow type after its domain, such as `ApprovalFlow`, and never just `Flow`. Go, Java, Python, and Rust derive the default Flow type from the type or class name without its package or module, so Flow types must be unique within one Worker registry. Dex Web, run history, and Flow type searches show that name as written.

Docs: https://docs.superdurable.io/primitives/flow

## Step and Wait

Use a Step for retried background work and explicit state transitions. **WaitFor** returns Conditions; **Execute** performs work and returns a Step decision.

Leave Conditions unnamed in **Until**, **AnyOf**, and **AllOf**. Do not add condition IDs merely to distinguish branches: read Channel results from the Channel definition and inspect Timer outcomes through the Context. Every Condition in **AnyCombinationOf** needs a unique ID. A Timer also needs an ID when another operation selects it by ID, such as **SkipTimer**.

When multiple **AnyOf** alternatives are ready in one evaluation, Dex selects the first feasible candidate in canonical order: Timer Conditions in declaration order, then Channel Conditions in declaration order, then SubFlow Conditions in declaration order. An earlier unready Condition does not block a later ready Condition. Mixed Condition kinds do not preserve one global call argument order. Only the winning Channel Condition consumes messages; other Channel candidates remain unchanged.

**AnyOf** models a race among the currently available alternatives. When business logic requires strict priority, return only the active high-priority Condition until it is resolved. Do not put lower-priority alternatives in that Wait.

Use multiple next Steps for parallel work. Use cancellation deliberately when a first-winner branch makes siblings unnecessary.

Step durability resolves in this order: a method override, FlowConfig, then **SYNC**. WaitFor and Execute can override durability independently. Applications dominated by short idempotent operations can set a Flow-level **ASYNC** default and override known long-running methods to **SYNC**.

Every **WaitFor** and **Execute** call receives ordinary Attributes and Channel size metadata. AttributeMap values and pending Channel messages require method-specific selections in StepOptions. Select the whole map only when the method must enumerate instances; otherwise select exact instances. WaitFor and Execute use independent snapshots. Execute loads after Wait consumption, and retries reuse the first snapshot for that logical method call. Attribute locks do not load state.

A long-running regular attempt must emit an explicit heartbeat or Stream message before its heartbeat timeout. A heartbeat value is a retry checkpoint. An explicit valueless heartbeat clears the checkpoint; a Stream message preserves its current state. The local phase of **ASYNC** durability ignores heartbeats but still emits Stream messages.

For an LLM call that may remain healthy without output for more than one minute, tell the application developer to raise **HeartbeatTimeout** above its one-minute default. Size it to the longest acceptable silent interval, and use the method timeout to cap the whole attempt. Do not add periodic heartbeats solely to mask provider silence; they prove only that application code is running, not that the upstream request is progressing.

Read [StepOptions](step-options.md) for the ASYNC local phase, fallback, classification heuristic, shared retry budget, timeout boundary, and full policy checklist.

Docs: https://docs.superdurable.io/primitives/step

## Attribute

Use an Attribute for durable state inside one Flow execution. Register every Attribute in the persistence schema before reading or writing it.

Use one Attribute when the value is cohesive and should be replaced as a unit. Use an AttributeMap when runtime-keyed instances change independently; each instance is stored separately, avoiding a rewrite of the whole collection. Use stable domain keys and delete instances that are no longer needed.

Steps, timeout handlers, and RPCs receive regular Attribute values automatically. They must explicitly load AttributeMap entries. Load the whole map for broad snapshots or exact instances for known keys. AttributeMap size does not make its entries available. An explicit load controls data transfer only; it does not enable transactional execution or isolation.

Lock the exact AttributeMap instance when Steps or RPCs can race on it. For an invariant spanning the whole map, register a separate non-indexed singleton bool Attribute as a coordination lock and require every cooperating writer to acquire it; see [whole-map coordination](data-handling.md#whole-map-coordination). Do not treat an AttributeMap index as an index over its instances: all instances share one Flow search field, later writes replace that field, and instance keys are not searchable. AttributeMap enumeration is not server-side pagination.

Assign every application Indexed Attribute an explicit generic index key from a namespace-wide typed pool: **keyword1**, **keyword2**, **keywordList1**, **keywordList2**, **text1**, **text2**, **int1**, **int2**, **bool1**, **bool2**, **double1**, **double2**, **datetime1**, **datetime2**, and later numbers as capacity permits. Keep the Attribute name meaningful to the business; only its physical search key uses a generic slot. Reuse each slot across Flow types with the same index type, even when its business meaning differs. Two logical indexed fields in one Flow need distinct slots. Do not use business names or the default Attribute-derived key for application indexes. Dex system indexes keep their reserved names.

Generic slots can mean different things for different Flow types. Application run searches using a generic slot must constrain **FlowType**. Combine that scope predicate with parenthesized caller filters so an OR cannot bypass it. Do not index sensitive data or PII. Before assigning many distinct index keys, read [Indexed Attribute capacity](operations.md#indexed-attribute-capacity). The default local **dexcli dev** stack has a smaller SQLite index pool than an Elasticsearch-backed production deployment.

Read [data-handling.md](data-handling.md) for large values, map chunking, BlobCache locality, and external projections.

Docs: https://docs.superdurable.io/primitives/attribute

## Channel

Use a Channel for ordered, durable, typed messages scoped to one Flow execution. One matching wait consumes a message once.

Use a ChannelMap when the same message contract is partitioned by a dynamic key. Plan how externally published messages are drained before Flow completion.

Every pending Channel message has a server-assigned message ID. Expose a typed Flow RPC when an application needs a durable queue UI. The RPC explicitly loads and lists pending messages in FIFO order without consuming them. Only a pending message can be deleted; deletion after consumption returns the Channel-message-not-found error.

Steps, timeout handlers, and RPCs always receive ChannelInfo sizes, so Channel size and ChannelMap keys and sizes do not require a load. Reading pending message envelopes requires an explicit Channel or ChannelMap load. Load the whole ChannelMap or exact instances according to what the handler reads. A loaded empty queue is empty; reading an unloaded queue is a usage error.

Steps and timeout handlers may delete a pending message in the same response as Attribute writes, Channel publications, and a decision. A known message ID can be deleted without loading its envelope. Deletion is a response side effect, not part of the Step decision. If the message is already absent, deletion is best effort and the remaining effects still apply.

A Channel queue is not conversation history. Keep consumed user and assistant messages in Attributes when the application must display or reconstruct them.

Use a transactional RPC to move or edit a pending message atomically. The caller should send only the message ID. Explicitly load the source Channel, find the original Value in the RPC snapshot, then stage its deletion and destination publication. A missing message rejects the entire transaction, including all Attribute writes and Channel publications.

Attribute locking already selects transactional execution. Channel deletion without an Attribute lock must explicitly select the SDK's transactional RPC option. Transactional validation protects an ID-only move from concurrent consumption, but it does not isolate decisions based on the whole snapshot. For those decisions, every cooperating Step and RPC writer must use the same Attribute lock. The lock does not implicitly load map entries or Channel messages.

Treat pending-message reads inside Steps and RPCs as potentially stale snapshots. Other handlers can consume, delete, or publish while the current handler runs. Read and write pending messages directly only when the operation explicitly tolerates that race. If the result depends on the queue remaining unchanged, use one shared Attribute lock across every cooperating Step and RPC writer; transactional execution alone is insufficient.

Without transactional execution, a missing deletion may be a no-op while other RPC effects commit. When a deployment cannot provide the required atomic guarantee, reconcile from a fresh pending-message list.

Docs: https://docs.superdurable.io/primitives/channel

## RPC

Use an RPC for every application read or write of Flow-owned Attribute, AttributeMap, Channel, or ChannelMap state. RPC handlers may return snapshots, update or delete Attributes, and publish, list, move, or delete pending Channel messages. Protect shared mutations with Attribute locks when they can race with Steps or other RPCs.

Use a Channel instead when the caller should enqueue work without synchronous application-level handling.

An RPC without Attribute locks or transactional execution runs its handler against a snapshot of the Flow's state. If the handler returns only output, Dex writes nothing to the Flow, and a retained terminal execution can serve the read. Success therefore does not prove that the Flow is active. Locks, explicit transactions, returned durable effects, or a Server policy that runs every RPC transactionally require an active Flow. A non-transactional handler that returns durable effects runs before Dex applies them; if the Flow closes in between, applying the effects fails with a not-active error after the handler ran. Keep external mutations idempotent and do not treat `FlowNotActiveOrNotFoundError` as proof that the handler never ran.

Use a lifecycle API when a response needs current execution status. A read-only RPC returns its application-state snapshot; it does not add terminal status that the application did not persist.

Do not search for a Run ID before an ordinary read-only RPC. Omit an optional Run ID so the Server resolves and pins the current execution for that invocation. Supply one only when the application must target an exact execution across Flow ID reuse.

Keep application read models cohesive. When one page needs conversation Attributes, a description, and pending queues, prefer one read-only snapshot RPC that explicitly loads those collections over several independently timed requests.

When one response requires multiple Attributes or AttributeMap instances, assemble it in one dedicated read-only RPC rather than issuing sequential Client reads. Select only the required map instances so the invocation has one coherent read boundary and avoids repeated round trips. Keep separate views as separate RPCs; do not combine unrelated read models merely to reduce calls.

Select transactional execution when an RPC must atomically validate a pending Channel message ID and commit its deletion with other Flow-state writes. Handle the Channel-message-not-found error as a stale queue view and refresh before retrying.

Keep registration-time and invocation-time options separate. Registration options own fixed timeout, locks, transactionality, whole-map loads, singleton Channel loads, and other state requirements. Invocation options are additive and intentionally narrow: they can add exact AttributeMap instance locks, exact AttributeMap instance loads, and exact ChannelMap instance loads selected from request data. They never replace or clear registration options.

The Client validates invocation selections before network I/O. Every definition must belong to the RPC's Flow, every dynamic lock must target an AttributeMap instance, and every instance name must be nonempty and slash-free. A dynamic read-modify-write must include the same instance as both a lock and a load. Reads need only the load. All cooperating writers must use the same instance lock, and lock conflicts are explicit contention rather than queued execution.

Docs: https://docs.superdurable.io/primitives/rpc

## Stream

Use a Stream for low-latency, best-effort, resumable updates such as progress displayed in a UI. Do not use a Stream when delivery must be durable; use a Channel or Attribute instead.

A Step may append any number of messages to the same or different Streams before its final result. A Step Stream write is fire-and-forget: local encoding or registration can fail immediately, but Dex Server does not acknowledge Stream Store persistence and a Store failure does not fail the Step.

When a Step calls a streaming LLM API, create one invocation-managed buffered text writer before starting the request and pass its bound write method as the token or delta callback. Do not call direct Stream write for each LLM token or delta. The default one-second timer and 16 KiB soft UTF-8 threshold reduce message volume, and the invocation flushes the tail before its result or error. In Go, Java, Python async, TypeScript, and Rust, call only write; the SDK owns finalization. Python sync generators are cooperative and must yield from an explicit final flush. An empty buffer does not heartbeat. Retry does not restore unsent text and may repeat batches already sent.

Use the text-specific API for the installed SDK version: **NewBufferedTextStream** in Go, **BufferedTextStream.create** in Java, **buffered_text** in Python, **bufferedText** in TypeScript, or **buffered_text** in Rust. For example, a Python async Step should create **progress = thinking.buffered_text(context)** and pass **progress.write** to the LLM helper.

Continue using direct Stream writes for semantically complete, independent messages. Do not buffer events merely because they arrive quickly; batching changes the message boundaries observed by readers.

Step messages use **#StepExecutionID** as source metadata. The source is not an idempotency key: attempts and messages may share it, and every write appends. Client Stream writes require a nonempty source, which may repeat or contain **#**.

Stream clients have two distinct read paths. **ReadStream** consumes forward one message at a time, resumes from a message token, and can long-poll for the next write. **ListStreamMessages** immediately returns a retained-message page in newest-first order. Its empty before-page token starts at the retained tail; each nonempty next-page token is an exclusive, scope-bound anchor for older messages. An empty next-page token marks the end.

Treat reverse listing as a best-effort retained-message snapshot. Writes after the first page do not enter the older-page chain, but trimming can create gaps. If trimming moves the retained head past the anchor, Dex returns an empty page instead of restarting from the tail. Page size must be positive and cannot exceed the Server **streamStore.maxReadMessages** limit, which defaults to 1000.

Docs: https://docs.superdurable.io/primitives/stream

## Timer

Use a Timer Condition for a durable delay, reminder, deadline branch, or scheduling loop. Do not use a Timer loop to poll an external system; use the [Polling pattern](patterns.md#polling). Decide what happens if a timer is skipped and whether the business deadline should complete, cancel, fail, or route to a handler.

A Flow timeout handler has Execute semantics. Configure its per-attempt timeout, heartbeat timeout, retry, failure route, durability, locks, and selective state loads through FlowTimeoutHandlerOptions on StartFlowOptions or SubFlowOptions. These options require a positive Flow timeout and the Handler policy. Handler timing starts after the soft timeout fires and may extend beyond the original deadline.

Route exhausted handler retries only to a registered no-input Step. The SDK supplies null or unit input, and the recovery Step reads the final failure from Context. Without a failure target, exhausted retries fail the Flow. Flow retry preserves timeout-handler options and starts a new soft-timeout budget.

Docs: https://docs.superdurable.io/primitives/timer

## SubFlow

Use a SubFlow for child work that benefits from a separate Flow identity and lifecycle. Bound parallel SubFlows and define their reuse, cancellation, and parent-completion behavior.

Docs: https://docs.superdurable.io/primitives/subflow

## Client

Use the Client at application boundaries to start, stop, inspect, search, invoke typed Flow RPCs, wait for Attribute matches, consume Streams, and manage Flow lifecycle. Do not model application state access with removed direct Attribute or Channel Client methods. Handle typed SDK failures for duplicate starts, missing Flows, closed Flows, lock conflicts, missing messages, long-poll expiry, and uncompleted closure.

Docs: https://docs.superdurable.io/primitives/client
