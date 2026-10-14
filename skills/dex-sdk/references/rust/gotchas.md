# Rust gotchas

## Stable schema objects

Define each `Attribute`, `Channel`, and `Stream` at module scope with `static LazyLock<T>`. Do the same for map definitions when ownership permits. Do not return a newly constructed definition from a helper on each call. Stable logical names matter more than pointer identity, but one canonical Rust value prevents drift.

When a Flow owns an `AttributeMap` or `ChannelMap`, clone that initialized schema only to give Steps owned access. Never clone Context state to avoid reasoning about ownership.

## Flow type names

The default Flow and Step types are the last path segment of the Rust type name. Name each Flow type after its domain, such as `ApprovalFlow`, never just `Flow`; the module path does not separate two types with the same name.

## Borrowed Step graph

`StepList` borrows Step fields from the Flow. Build the Flow with all Steps it may register, including Steps reached only by RPC movement or failure recovery. Constructing a fresh Step in `go_to` can be valid where the API accepts it, but the Step type still must be present in the Flow's registered graph.

## Blocking versus async

Rust Step handlers are synchronous at this baseline. Use blocking clients or services deliberately and configure method timeouts. Do not create a Tokio future and drop it from Execute. In an Axum controller, move blocking Client calls off the async executor; the examples route them through `run_blocking`.

## Unit is a real payload

Use `()` for no input or output. Choose the corresponding RPC registration and Client invocation methods. Do not invent `Option<()>` or JSON null wrappers unless the public API requires them.

## Context does not survive

Never store `&mut Context`, a Condition result reference, or a buffered Stream writer in the Flow or Step. Handler-local state disappears after the method. Persist recovery state through declared primitives.

## Loads are explicit

Declaring a map or Channel in `PersistenceSchema` does not hydrate its data into every invocation. Add the correct WaitFor, Execute, RPC, or timeout-handler load. A missing load should be fixed in options, not bypassed with Server internals.

## Locks and transactions differ

An Attribute lock serializes a protected invariant and implies transactional RPC execution. `is_transactional()` is required for an all-or-nothing RPC that only mutates Channels. Loading state alone provides neither.

## Retry attempts repeat side effects

`context.attempt()` counts attempts for the current method. A prior external call may have succeeded even when the handler retries. Use idempotency keys, durable checkpoints, or a query-before-write protocol. Do not branch on process-local counters.

## Completion semantics

`dead_end` ends a branch, `graceful_complete` participates in normal Flow completion, and force decisions end the Flow. First-win patterns must explicitly cancel siblings. A Step completing does not necessarily mean the Flow is terminal.

## Timers are durable, not precise clocks

Assert that a timer eventually fires within a generous deadline. Do not assert exact elapsed milliseconds. Use condition IDs or Context timer results when a Wait combines several timers.

## No unverified API invention

If a desired Rust feature is absent from the pinned examples and matching SDK source, say so and link the closest supported primitive. In particular, do not translate a Python coroutine shape or Java annotation literally into Rust.

## Temporal Cloud indexes

Temporal Cloud API-key deployments require externally provisioned indexes and `attributeIndexesManagedExternally: true`.
