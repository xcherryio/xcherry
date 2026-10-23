# dexzd-stream-store

## Purpose

In-memory Stream store that serves WriteStream, ReadStream, and ListStreamMessages for server-zd.

The store is process-local and independent of Flow existence. It keys capacity by
tenant ID, Flow type, and Stream name, and retains messages per Flow ID within
that shared scope. Each append charges the serialized Value size, Flow ID bytes,
source bytes, and 512 bytes of estimated overhead. When usage reaches the high
water mark, one owned background worker trims the oldest entries until usage
reaches the low water mark. A write that cannot fit returns
`StreamStoreError::CapacityExceeded` and schedules trimming.

`StreamStoreConfig` is disabled by default. Its default capacity is 10 MiB per
scope when a write does not supply one. Explicit capacities match the
`WriteStreamRequest` field. The default Value limit is 100 KiB, the maximum
reverse page size is 1000, and the trim water marks are 90% and 80%.

`read` returns one retained message after an opaque resume token or `None` when
the caller's deadline expires. `list` returns newest first with an exclusive
before-page token. Tokens are bound to tenant ID, Flow type, Flow ID, and Stream
name; callers should pass them unchanged. `write_worker_best_effort` sets the
source to `#<StepExecutionID>` and logs a rejected write without failing the
caller. Call `close` to wake readers and join the trim worker.

The API layer maps invalid arguments and tokens to `InvalidArgument`, oversized
Values and exhausted capacity to `ResourceExhausted`, disabled storage to
`FailedPrecondition`, and closed storage to `Unavailable`. A missing Flow is
valid for Stream operations and does not return `NotFound`. A `None` long-poll
result maps to `DeadlineExceeded` with the long-poll timeout substatus.
