# Read-after-write consistency and asynchronous completion

Use this reference before implementing or reviewing sequential Client mutations and reads. Classify the **write API → read API pair**. Language-level `await`, a completed Future, or a nil error proves only that the selected API's response boundary was reached.

## Three different guarantees

1. **Acceptance:** Dex accepted the request. Acceptance alone does not mean the Interpreter has processed it.
2. **Read-after-write:** after the write returns successfully, a subsequent read of the authoritative Flow state reflects its direct effects or a later state. The read may block until the Interpreter catches up. Strong does not mean zero latency.
3. **Business completion:** Steps, provider operations, cleanup, and other work triggered by the write have finished. A fresh state read does not automatically establish this.

A successful RPC that writes Attributes, AttributeMap instances, or Channel messages has its direct effects applied before any later read of the same Flow is served. A typed read-only RPC issued after the write returns observes the write or a later state on its first successful read. Do not add callback waits or convergence polling to that pair.

This guarantee requires a successful write, a read initiated afterward, the same logical Flow and appropriate state selections, and no application failure preventing the effect from being applied. An AttributeMap instance must be loaded by the read RPC. Channel pending-message reads must load the queue; another consumer can remove a just-published message. Concurrent writes can supersede the value. Strong read-after-write does not add transaction isolation, make a multi-RPC sequence atomic, or guarantee a particular value survives competing mutations.

## Write/read matrix

| Write | Subsequent read or confirmation | Guarantee and what to do |
| --- | --- | --- |
| RPC that writes Attributes or AttributeMap instances | Typed read-only RPC over the same Attributes, loading the written map instances | **Strong.** The first successful read includes the write or a later state. Read once; do not poll. |
| RPC that publishes or deletes Channel or ChannelMap messages | Typed queue snapshot RPC that loads the Channel or ChannelMap | **Strong** for the pending queue. A consumed or deleted message is not expected to remain pending. A non-transactional deletion of a missing ID can be a no-op while the RPC's other effects apply. |
| Transactional or locked RPC | Typed read-only RPC over the written state | **Strong.** The RPC returns after its handler and effects commit together. It does not wait for triggered Steps or external projections. Requires an active Flow. |
| `UpdateFlowConfig` | Server or CLI Flow configuration read; a typed read RPC served by the new Worker for Worker-target changes | **Strong** once the Flow can process the change. The write returns on acceptance; a delayed-start Flow can make the read wait until startup. Applies only supplied fields and does not recall already-dispatched work. The public SDK has no generic configuration-read API. |
| `SkipTimer` | Current Timer and Step state; named Step completion or a business snapshot for the result | The Timer change is readable directly. **Execute and business completion are asynchronous.** The Timer may fire on its own before the skip applies; acceptance is not proof of execution. |
| `StopFlow` CANCEL/default or FAIL | `WaitForFlow` and explicit terminal status | **Asynchronous terminal completion.** The stop requests cooperative cleanup; terminalization and Attribute Store draining can finish later or fail. CANCEL, FAIL and TERMINATE are different outcomes. |
| `StopFlow` TERMINATE | Terminal status | Ends the Flow without cooperative cleanup. It does not prove provider compensation or projection completion. |
| Step or RPC write or delete of a hard-delete Attribute | Typed read RPC, another Step or RPC, or a retried attempt | **Visible before commit.** The stored value can reflect a write or deletion from an attempt that later failed, and concurrent writers may not leave the last committed write. Make handlers safe to repeat and avoid read-modify-write. See [hard-delete Attributes](data-handling.md#hard-delete-attributes-for-short-lived-secrets). |
| Any indexed Attribute write | `SearchFlows`, CLI search, or Dex Web work discovery | **Eventual.** A search miss is not proof the write failed. Confirm through the typed read RPC, or bound a search retry when the caller needs the index itself. |
| Any Attribute Store-enabled write | External Attribute Store query | **Eventual latest-state projection.** Valid direct Flow state can be readable before the projection finishes. Projection failures do not roll back Flow state; retry exhaustion can prevent convergence, and so does a Flow ID that does not match the store's `rowKey` template, whose batches are skipped. |
| RPC Step movements, cancellation or close decisions | Named Step wait, Attribute-match or business condition, or terminal result | **Asynchronous business completion.** Cancellation can yield before later Step movements are queued; a direct Attribute snapshot is not a scheduling or completion barrier. |
| Server `SetAttributes`, `PublishToChannel`, or `DeleteChannelMessage` | Server Attribute or Channel read, or a typed application snapshot | Same direct-state guarantee as an RPC write. These Server APIs are not public SDK state APIs; applications use typed RPCs. |
| SubFlow completion delivered to its parent | Parent's SubFlow condition, parent Step completion, or business state | Delivery is not proof that the parent consumed the result or completed subsequent work. It is internal, not a Client completion API. |

`StartFlow` and TimeTravel/Reset are not state writes. Their returned run identifies an accepted or new execution; use a named admission Step or Flow result when the API must establish business acceptance or completion. A Stream message is best-effort observation, not an authoritative receipt for any of the operations above.

## Backend and lifecycle boundaries

The strong rows above hold on Temporal-backed Dex deployments. On a Cadence-backed deployment, a read after a write is not guaranteed to observe the write on the first read: confirm effects with a bounded authoritative read loop. Cadence-backed deployments also return Unimplemented for locked RPCs, Step-completion waits, and Attribute-match waits.

A delayed-start Flow can accept a write before it starts processing. A subsequent read waits for that processing instead of returning stale state. Set a read deadline that allows the configured start delay; a timeout proves only that confirmation was not obtained. Interpreter unavailability can similarly block or fail the read. Do not weaken a strong path into polling to hide that failure.

A Worker-target update changes future routing; dispatched calls may still use the previous Worker. Validate configuration readback separately from the new Worker's registration, reachability and typed RPC response. A stale Pod IP or unavailable Worker is a routing or availability failure, not evidence of eventual Attribute consistency. Prefer a stable reachable Worker target when the deployment supports one.

Address a Flow by its FlowID; the RunID is diagnostic and must not become a business revision or fence. A query-only RPC can read retained terminal state; locks, transactions, returned effects, and a Server policy that runs every RPC transactionally require an active Flow. Retention expiry must remain an explicit missing or unavailable result.

## Confirmation and retries

Use a single typed authoritative read after a successful direct-state write. For subsequent business work, select the existing named Step completion, Attribute match, business RPC state, or terminal-result API that proves the requested condition. Bound the wait and inspect terminal status; a completed Flow proves success only if its completion contract guarantees that condition.

Never use a fixed sleep as confirmation or blindly resend accepted requests. Do not attach a callback to every write. Add an application acknowledgement only when its business contract needs a stronger completion fact that existing public observation cannot express. Do not invent an operation receipt or SDK config-wait API.

If transport failure or timeout makes a mutation's acceptance unknown, reconcile authoritative state before retrying an effect whose safe replay is not established. StartFlow follows the [start-first replay rule](error-handling.md#start-first-reconcile-only-after-an-error); a stable start identity and matching start options can permit bounded replay without a mandatory read. Preserve the request identity where the installed API supports it, and distinguish start deduplication from RPC idempotency. Config updates and timer skips do not acquire exactly-once semantics merely because the client retries. An accepted command can still encounter closure or a processing failure; do not promise unconditional eventual success.

## Verification

For strong RPC read-after-write, test a real Server, Interpreter, and Worker with repeated sequential writes, then invoke the typed read RPC immediately. Assert ordinary Attributes, loaded AttributeMap instances and non-consumed pending queues on the **first** successful read. No sleeps or convergence polling are allowed between that write and read. Cover ordinary, transactional, and locked RPC writes.

Establish lifecycle transitions separately: close the Flow, then test retained query-only reads. For delayed config updates, submit while startup is delayed, immediately read configuration, and independently read through the new Worker. The read may block; do not assert that every immediate read must return an old value.

Use a controlled gate on a subsequent business Step or projection writer to prove direct state is readable while later work remains unfinished. Release the gate and confirm completion with a bounded condition. For a paused Interpreter, distinguish accepted write submission from a read deadline, then resume and confirm state. On a Cadence-backed deployment, assert bounded convergence rather than the first-read result.

## Source authority

The following links explain the existing semantics at the bundle's immutable baselines. Preserve installed project versions and inspect version-matched source before writing exact API calls. This reference introduces no new API.

- [Server RPC routing](https://github.com/superdurable/dex/blob/server/v1.13.0/server/service/api/service.go)
- [Attribute writes](https://github.com/superdurable/dex/blob/server/v1.13.0/server/service/interpreter/persistence.go)
- [Attribute Store scheduling](https://github.com/superdurable/dex/blob/server/v1.13.0/server/service/interpreter/attributeSynchronizer.go)
- [Go Client](https://github.com/superdurable/dex/blob/sdk-go/v1.13.0/sdk-go/dex/client.go), [Python Client](https://github.com/superdurable/dex/blob/sdk-python/v1.12.0/sdk-python/dex/client.py), [Python AsyncClient](https://github.com/superdurable/dex/blob/sdk-python/v1.12.0/sdk-python/dex/async_client.py), [Java Client](https://github.com/superdurable/dex/blob/sdk-java/v1.12.0/sdk-java/src/main/java/io/superdurable/dex/Client.java), [TypeScript Client](https://github.com/superdurable/dex/blob/sdk-typescript/v1.12.0/sdk-typescript/src/client.ts), [Rust Client](https://github.com/superdurable/dex/blob/sdk-rust/v1.12.0/sdk-rust/crates/dex-sdk/src/client.rs)
- [CLI operations and success fields](https://github.com/superdurable/dex/blob/cli-v1.13.0/cli/internal/command/flow_client_operations.go)
- [Web action RPC routing](https://github.com/superdurable/dex/blob/server/v1.13.0/web/api/v2.go)
