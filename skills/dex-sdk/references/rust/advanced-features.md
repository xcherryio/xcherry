# Advanced Rust features

Load this page only after the Flow model and basic primitives are settled.

## Timeout handlers

A Flow may return a `FlowTimeoutHandler<Self>`. The handler is one retryable logical execution and returns a normal `StepDecision`. Start/SubFlow options select handler policy and can configure handler retries, locks, and selective loads. Keep the handler bounded; it runs because the original deadline has already expired.

[Runnable source](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/rust/src/primitives/flow/flow.rs)
<!-- dex-source: examples/rust/src/primitives/flow/flow.rs -->
```rust
fn timeout_handler(&self) -> Option<FlowTimeoutHandler<Self>> {
    Some(Self::handle_timeout)
}
```

Use `force_complete` only if the timed-out outcome is a valid result; use `force_fail` when the deadline invalidates the operation. Persist any status needed by operators before returning.

## Cancellation-aware execution

Dex cancellation becomes observable through `Context::is_cancelled`. Long-running Execute loops should check it between bounded units of work and heartbeat progress. Cancellation is cooperative at the application boundary: blocking a single call indefinitely prevents timely observation.

For interruptible orchestration, store the interrupt request in an Attribute through an RPC, have concurrent Steps read it at safe points, and choose a deliberate graceful or forced terminal decision. See the runnable [Interruptible Flow](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/rust/src/patterns/interruptible/flow.rs).

## Buffered Stream durability

`buffered_text` and `buffered_text_with_options` emit progressive text without blocking for each frame's storage acknowledgment. The buffer interval and soft byte threshold trade latency for overhead. Check handler cancellation and always propagate writer errors. Use durable Attributes or a terminal result for state that must be committed before the Step succeeds.

## Selective loading for large state

`StepOptions` offers separate WaitFor and Execute loads. `Rpc` and timeout handler options have their own loads. Prefer one map instance or ChannelMap instance when possible. This keeps Worker payloads bounded and makes data dependencies visible in the definition.

`Rpc` registration defines fixed timeout, locks, transactionality, and loads. `RpcInvokeOptions` adds only exact AttributeMap instance locks, exact AttributeMap instance loads, and exact ChannelMap instance loads. Pass it to `Client::invoke_rpc_with_options` or `invoke_rpc_without_input_with_options`. A read-modify-write uses both `lock_attribute_map_instance(attribute_map.lock(instance))` and `load_attribute_map_instance(attribute_map.load(instance))` for the same bucket. The Client unions, sorts, and deduplicates both layers and validates the registered Flow definition and slash-free nonempty instance before network I/O. See the pinned [hash-partitioned controller](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/rust/src/patterns/hash_partitioned_attribute_map/controller.rs).

## Atomic updates

Use Attribute locks around read-modify-write invariants. Combine `.lock(...)` calls when several values must change together. Use `Rpc::is_transactional()` when Channel operations need an atomic boundary without an Attribute lock. Test conflict behavior as a typed Client error.

## Step-specific option overrides

A transition may use `StepMovement::to_with_options` to override the target execution without changing the Step's global defaults. Reserve this for call-site-specific timeout or retry semantics; if every transition needs the override, put it in `Step::options`.

## StepOptions and Flow durability

Set a short-operation default with `StartFlowOptions::new().config_override(FlowConfig::new().step_durability(StepDurability::Async))`. Leave ordinary methods at `StepDurability::Default`, then use `StepOptions::wait_for_durability(StepDurability::Sync)` or `execute_durability(StepDurability::Sync)` for known long work. The phases are independent.

StepOptions also owns method timeouts, heartbeat timeout, retries, selected loads, locks, and failure routes. Do not infer durability from a large method timeout. Read [core StepOptions guidance](../core/step-options.md) for the five-second heuristic, fallback, shared retry budget, and child cancellation deadline.

## Waiting for durable acceptance

`Client::wait_for_step_completion` lets a controller wait until a selected Step reaches its defined completion, then return while background branches continue. The server derives a stable namespaced Request ID from the Step execution when options omit one. `request_timeout` is the caller-visible budget across transparent transport reattachments; zero waits indefinitely. `internal_handler_timeout` is an advanced capacity safeguard for abandoned accepted waits and is normally zero. A positive value rolls an active call to the next `-N` generation without surfacing an error. This is useful when an API needs a durable acceptance point rather than full Flow completion. The runnable [Wait for Step completion pattern](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/rust/src/patterns/wait_for_step_completion/flow.rs) persists the request before launching background work.

## Attribute Store entity projection

Attributes marked for synchronization can project a long-lived entity Flow into an Attribute Store. Configure the store on the Flow and use RPCs as the mutation boundary. The runnable [Entity Store pattern](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/rust/src/patterns/entity_store/flow.rs) covers typed profile fields and read/update/clear RPCs.

## Feature gaps

At the pinned baseline, the Rust examples do not provide a language-specific AI-agent framework integration or resource-control example. Use core AI-agent guidance plus Rust primitives, and mark framework-specific code as application-owned. Do not manufacture SDK helpers based on another language.
