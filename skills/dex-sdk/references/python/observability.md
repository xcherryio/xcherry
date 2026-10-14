# Observability for Python applications

Log Flow/run IDs, Step type/execution ID, attempt, and operation as structured fields. Keep IDs out of metric labels. Persist external request IDs if retries need them.

Heartbeat is liveness/checkpoint data for one Execute attempt. It is not business state. Async handlers await heartbeat; synchronous generator handlers yield the heartbeat output. Resume only from a present, correctly typed prior value.

[Pinned runnable source](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/python/dex_examples/primitives/heartbeat/heartbeat_flow.py)
<!-- dex-source: examples/python/dex_examples/primitives/heartbeat/heartbeat_flow.py -->
```python
completed_batches = context.get_last_heartbeat_value(int) or 0
for batch in range(completed_batches, batches):
    if context.is_cancellation_requested():
        return dead_end()
    await asyncio.sleep(2)
    await context.heartbeat(batch + 1)
return graceful_complete("processed")
```

Use summary/status for execution state, history for causality, a typed snapshot RPC for business state, Streams for progress, and metrics for rates/saturation. Track attempt duration/count, exhaustion, timeout handlers, RPC conflicts, Channel backlog, Stream lag, Worker reachability, and BlobCache errors.

For a stall: identify Flow/run; inspect latest history and active attempt; verify registered types; verify Dex reaches advertised Worker target; inspect exception chain. The pinned [Client API example](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/python/dex_examples/primitives/client-apis/controller.py) shows supported reads.
