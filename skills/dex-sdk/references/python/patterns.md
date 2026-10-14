# Python design patterns

Preserve each invariant, not merely the shape. Pinned files are runnable implementations.

## Parallel Steps

- **Static:** return `go_to_many` for a fixed set of Step movements. Branches are independent. [Runnable](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/python/dex_examples/patterns/parallel/static_parallel_steps_flow.py).
- **Dynamic:** construct movements from runtime input; each branch receives retry-safe data. [Runnable](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/python/dex_examples/patterns/parallel/dynamic_parallel_steps_flow.py).
- **Await all:** workers publish completion once and dead-end; coordinator waits for N Channel messages. [Runnable](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/python/dex_examples/patterns/parallel/await_parallel_steps_flow.py).
- **First win:** winner cancels sibling executions; losers must tolerate cooperative cancellation and avoid uncompensated commits. [Runnable](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/python/dex_examples/patterns/parallel/first_win_parallel_steps_flow.py).

[Pinned runnable source](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/python/dex_examples/patterns/parallel/static_parallel_steps_flow.py)
<!-- dex-source: examples/python/dex_examples/patterns/parallel/static_parallel_steps_flow.py -->
```python
def execute(self, context: Context, input: str) -> StepDecision:
    return go_to_many(
        StepMovement.of(WorkA, input),
        StepMovement.of(WorkB, input),
    )
```

## Parallel SubFlows

- **Basic:** make one `SubFlow` condition per request and wait for all; use when one parent can hold the batch. [Runnable](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/python/dex_examples/patterns/parallel-subflows/basic_parent_flow.py).
- **Long-lived parent:** RPC bounds/publishes queue; a loop drains children; stop plus empty is the completion invariant. [Runnable](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/python/dex_examples/patterns/parallel-subflows/advanced_long_live_parent_flow.py).
- **Short-lived parent:** bounded batch, atomic channel-empty close, explicit reuse for replacement run. [Runnable](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/python/dex_examples/patterns/parallel-subflows/advanced_short_live_parent_flow.py).
- **Wait for half:** race child with all-done, count once, wait for `(total+1)//2`; quorum is not universal success. [Runnable](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/python/dex_examples/patterns/parallel-subflows/wait_for_half_parent_flow.py).
- **Partitioning:** hash a stable affinity key into a fixed parent set. Version changes to partition topology. [Runnable](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/python/dex_examples/patterns/parallel-subflows/submit_request_flow.py).
- **Back pressure:** durable submit Flow turns rejection into Step retry/backoff; bound parent queue before acceptance. [Runnable](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/python/dex_examples/patterns/parallel-subflows/submit_request_flow.py).

## Polling and durable timers

- **Polling:** apply the Core [Polling pattern](../core/patterns.md#polling) in one long-running async `execute`: call the provider with its own timeout, raise transient errors to Step retry, keep “not ready” in the loop, and return the next decision on a terminal status, including a business-failure decision when the provider reports a definite failure. Write a Stream frame on status change, otherwise `await context.heartbeat(checkpoint)`, then `await asyncio.sleep(interval)`; a sync generator yields `heartbeat(...)` and uses `time.sleep`. Resume from `context.get_last_heartbeat_value(...)`; the checkpoint holds resume state such as the last reported status, never a deadline. Bound the wait only with `StepOptions`: `execute_method_timeout` is the maximum wait, `execute_retry` is a `RetryPolicy` with a few attempts and `total_duration` equal to the maximum wait (all attempts, measured from the first), and `heartbeat_timeout` keeps one minute with `interval + call timeout <= heartbeat timeout - 10s`. On expiry the engine fails the Step; `.on_execute_failure_proceed_to(RecordFailureStep)` routes it to a Step that reads `context.recovery_error` (`detail`, `error_type`) and records the business failure, and without that route the Flow fails. Never loop with a `wait_for` Timer, `retry_after` or retry policy, a hand-written deadline that duplicates `execute_method_timeout` or `total_duration`, or a page token passed to the next execution. Follow the [design pattern](https://docs.superdurable.io/design-patterns/polling); the pinned baseline has no runnable example of it yet.
- **Cron:** race Timer with trigger/skip, scheduling current run and next wait durably. [Runnable](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/python/dex_examples/patterns/cron/cron_schedule_flow.py).
- **Reminder:** race Timer with opt-out and send once per iteration. [Runnable](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/python/dex_examples/patterns/reminders/reminder_flow.py).
- **Inactivity:** activity restarts wait; Timer moves to processing; coalesce bursts. [Runnable](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/python/dex_examples/patterns/inactiveness-tracker-timer/inactiveness_tracker_flow.py).

## Failure handling

- **Execute recovery:** proceed to idempotent compensation with enough durable input for the original commit. [Runnable](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/python/dex_examples/patterns/recovery/failure_recovery_flow.py).
- **WaitFor recovery:** proceed only when recovery can distinguish the failed wait and rebuild state. [Runnable](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/python/dex_examples/primitives/proceed_on_wait_failure/proceed_on_wait_failure_flow.py).
- **Manual recovery:** after exhaustion, wait on durable operator decision Channels exposed through action-verb RPCs. The pinned example exposes retry only; add a skip RPC and durable command-ID deduplication when the application needs those semantics. [Runnable](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/python/dex_examples/patterns/intervention/manual_recovery_flow.py).
- **Graceful timeout:** Handler policy plus explicit `handle_timeout` terminal decision; request needed map/channel loads. [Runnable](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/python/dex_examples/patterns/timeout/flow_graceful_timeout.py).

## Draining, interruption, responsive update, entity store

- **Internal drain:** coordinate parallel main/side Steps and finalize after accepted messages drain. [Runnable](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/python/dex_examples/patterns/drain-channels/internal/drain_internal_channels_flow.py).
- **External drain:** accept via RPC and atomically close only with Channels empty. [Runnable](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/python/dex_examples/patterns/drain-channels/external_publishing/draining_channel_flow.py).
- **Interruptible:** RPC records interrupt; short work slices check it between operations. [Runnable](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/python/dex_examples/patterns/interruptible/interruptible_execution_flow.py).
- **Responsive Step:** await one named Step completion, not whole Flow. The server derives a stable namespaced Request ID from the Step execution when options omit one. [Runnable SDK test](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/sdk-python/tests/integ/test_basic_runtime_async.py).
- **Responsive Attribute:** wait for a revision match, keep the returned watermark, then reload durable state. The server derives a namespaced Request ID from the exact Attribute predicate. [Runnable SDK test](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/sdk-python/tests/integ/test_persistence_runtime.py).
- **Responsive Stream:** long-poll and resume from token; tolerate duplicates at retries. [Runnable](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/python/dex_examples/primitives/stream/controller.py).
- **Entity store:** AttributeMap instances plus Attribute Store; lock exactly the entity mutated by RPC. [Runnable](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/python/dex_examples/patterns/entity-store/user_profile_flow.py).

## Partitioned AttributeMap state

- **Sequentially chunked:** keep at most 100 ordered records in `current`; archive full chunks under zero-padded starting sequences. A page RPC loads one token-selected chunk with frozen `RPCInvokeOptions`. All appenders cooperate on the `current` lock and retry a rejected call after `RpcLockConflictError`. [Runnable](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/python/dex_examples/patterns/sequentially-chunked-attribute-map/chunked_subscriber_flow.py).
- **Hash-partitioned:** canonicalize ASCII email, calculate wrapping FNV-1a 32-bit, and place the full canonical key in one of 1000 buckets. An upsert adds the same bucket to `lock_attribute_map_instances` and `load_attribute_map_instances`; a lookup loads only the bucket. [Runnable](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/python/dex_examples/patterns/hash-partitioned-attribute-map/customer_directory_flow.py).

Choose sequential chunks for append-only history and hash partitions for direct lookup or updates by a stable business key. Hash collisions share a bucket but do not share the dictionary key. Changing the partition count requires rehashing stored records or introducing a new Flow version.

Python also has a runnable resource-control pattern, but it is not part of the current cross-language official catalog: [source](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/python/dex_examples/patterns/resource-control/controller_flow.py).

### Revision Attribute match

Initialize the integer revision Attribute to zero. Every state-changing Step or RPC must declare the same Attribute lock and increment the revision inside the locked invocation. The [Job Posting Flow](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/python/dex_examples/products/job-post/job_post_flow.py) demonstrates that mutation boundary.

Both `Client` and `AsyncClient` expose `wait_for_attribute_match`. Use `AttributeMatch.equal_to`, `not_equal_to`, `greater_than`, `greater_than_or_equal`, `less_than`, or `less_than_or_equal`. The singleton form accepts Flow ID, Attribute, match, and `WaitForAttributeOptions`. The AttributeMap form inserts the instance before the match. Both return the actual decoded matched value, including for non-equal operators. `request_id` is an optional override; the server otherwise derives it from the exact Attribute predicate. `request_timeout` bounds the complete SDK call across transparent reattachments. Zero waits indefinitely; a positive expiry raises `RequestTimeoutError` without terminating the accepted durable wait. Leave `internal_handler_timeout` at zero unless abandoned waits could approach the per-Flow limit on concurrently accepted waits. A positive value transparently rolls an active call to another handler generation, releasing the old slot but counting toward the per-Flow total of accepted waits. Prefer it comfortably longer than normal request timeouts and reconnect gaps. See the core responsive-update guidance for the full tradeoff. Call the application's read RPC afterward. The revision is a coalescing watermark, not an event stream.

[Runnable inequality wait](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/sdk-python/tests/integ/test_persistence_runtime.py)
<!-- dex-source: sdk-python/tests/integ/test_persistence_runtime.py -->
```python
assert (
    environment.client.wait_for_attribute_match(
        flow_id,
        flow.integer,
        AttributeMatch.greater_than(0),
        WaitForAttributeOptions(f"{flow_id}-integer", WAIT_TIMEOUT),
    )
    == 3
)
```
