# Advanced Go features

Use these only after Flow boundaries and recovery are explicit.

## Selective loading, locks, and transactions

Ordinary Attributes and Channel size metadata load automatically; large AttributeMaps and pending Channel payloads are opt-in per handler. Load exact instances for entity work. A not-loaded error is modeling feedback, not absence.

Declare Attribute or map-instance locks when RPC/Step mutations conflict. Lock the smallest stable key. Keep remote I/O outside transaction/lock windows. Treat `RPCLockConflictError` as contention.

`RPCOptions` fixes registration-time timeout, locks, transactionality, and loads. `RPCInvokeOptions` is additive and accepts only `LockAttributeMapInstances`, `LoadAttributeMapInstances`, and `LoadChannelMapInstances`. Call `Client.InvokeRPCWithOptions` when request data chooses an exact instance. Use `dex.LockAttributeMap(definition, instance)` for the lock and `definition.Load(instance)` for the load; include both for read-modify-write. The Client unions, sorts, and deduplicates these selections with registration options and validates the Flow definition and instance name before network I/O. The pinned [hash-partitioned controller](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/go/patterns/hash-partitioned-attribute-map/controller.go) shows write and read calls.

## Buffered Stream and heartbeat

`dex.NewBufferedTextStream` batches by interval/bytes. Flush at semantic boundaries and return errors. Stream writes are liveness frames but preserve the explicit heartbeat checkpoint.

[Pinned SDK contract](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/sdk-go/dex/contracts_test.go)
<!-- dex-source: sdk-go/dex/contracts_test.go -->
```go
progress, err := dex.NewBufferedTextStream(
	context,
	progressStream,
	dex.BufferedTextStreamFlushInterval(500*time.Millisecond),
	dex.BufferedTextStreamMaxBytes(8<<10),
)
```

Set heartbeat timeout for long Execute work. On retry, test whether a prior value exists before decoding; resume from an externally committed checkpoint. Heartbeat is progress, not authoritative business state.

## StepOptions and Flow durability

Set an application-wide default through `StartFlowOptions.ConfigOverride` and `FlowConfig.StepDurability`. For a short-operation Flow, use `StepDurabilityAsync`. Leave ordinary methods at `StepDurabilityDefault`, then set `WaitForDurability` or `ExecuteDurability` to `StepDurabilitySync` for a known long method. These two method overrides are independent.

`StepOptions` also owns `WaitForMethodTimeout`, `ExecuteMethodTimeout`, `HeartbeatTimeout`, per-method retry, selected loads, locks, and failure routes. Do not infer durability from `ExecuteMethodTimeout`. Read [core StepOptions guidance](../core/step-options.md) for the five-second heuristic and local fallback semantics.

## Cancellation, timeout, BlobCache

A decision/RPC result can cancel selected Step types or siblings. Cancellation is cooperative; external calls need context cancellation and idempotency.

Sync durability waits for persistence acknowledgement. Async improves latency but can replay a recently acknowledged attempt after failure; restrict it to idempotent work and test replay. See [durability runnable](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/go/primitives/durability/workflow.go).

A timeout handler has its own timeout, retry, durability, locks, and state loads. It follows normal commit rules.

Open one BlobCache at bootstrap and share it with Client/Worker. Align path/capacity across replacements needing large values. It provides payload locality, not arbitrary durable files.
