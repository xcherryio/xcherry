# Python primitives

Read core semantics first; this page gives Python shapes at the pinned baseline.

## Flow, Step, Wait, decisions

Subclass `Flow[InputT]` and `Step[InputT]`. `get_steps` returns `StepList.start_step(instance).other_steps(...)`. A Step without `wait_for` executes immediately. `Wait.until`, all/any condition APIs, and `Wait.skip_immediately` control waiting. Return `go_to`, `go_to_many`, `dead_end`, graceful, or force decisions.

[Pinned runnable source](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/python/dex_examples/primitives/flow/example_flow.py)
<!-- dex-source: examples/python/dex_examples/primitives/flow/example_flow.py -->
```python
class Example(Step[int]):
    def __init__(self, finish: Finish) -> None:
        self.finish = finish

    def wait_for(self, context: Context, input: int) -> Wait:
        status.set(context, "running")
        return Wait.skip_immediately()

    def execute(self, context: Context, input: int) -> StepDecision:
        return go_to(Finish, input + 1)
```

Among ready `Wait.any_of` candidates, Dex uses canonical Timer, Channel, then SubFlow order and preserves argument order within each kind. An earlier unready Condition does not block a later ready one. Only the winning Channel consumes messages. For strict priority, return only the current higher-priority Condition until it resolves.

## Attribute/AttributeMap and Channel/ChannelMap

Create `Attribute(name, value_type)` and `AttributeMap(name, value_type)` at module scope and include them in `PersistenceSchema`. Access via invocation Context. Maps partition by validated instance. Indexed definitions support search; Attribute Store sync supports external entity access.

Channels are durable queues. Conditions wait for one/N messages; after firing, inspect condition messages and delete/move them deliberately. ChannelMap gives one logical definition with independent instance queues. External callers invoke a typed Flow RPC; its handler publishes through Context.

Pending-message reads inside a Step or RPC are invocation snapshots. Other handlers may consume, delete, or publish concurrently. Transactional execution validates selected deletions and commits writes atomically, but does not lock the whole snapshot. Read and write pending messages directly only when the operation explicitly tolerates that race. When a decision requires the queue to remain unchanged, every cooperating Step and RPC writer must use the same Attribute lock.

## Timer, RPC, Stream, SubFlow

Timer belongs in `wait_for` as a durable condition, never `asyncio.sleep` for durable scheduling. Decorate Flow methods with `@rpc`; type the input/output with `RPCResult[T]`, keep handlers short, and lock conflicting state. An RPC without locks or `is_transactional` can query a retained terminal run when it returns no durable effects. Locks, transactions, returned effects, or Server policy require an active execution.

Streams are typed feeds registered in persistence schema. In sync generator handlers, `yield` every Stream output. In async handlers, Stream writes are synchronous API calls at the pinned surface while heartbeat is awaited. Consumers resume from tokens.

[Pinned runnable source](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/python/dex_examples/primitives/stream/stream_flow.py)
<!-- dex-source: examples/python/dex_examples/primitives/stream/stream_flow.py -->
```python
class RenderPreview(Step[str]):
    def __init__(self, progress: Stream[str]) -> None:
        self.progress = progress

    async def execute(self, context: AsyncContext, input: str) -> StepDecision:
        progress = self.progress.buffered_text(context)
        progress.write(f"Rendering preview for {input}")
        progress.write(f"Preview ready for {input}")
        return graceful_complete(f"Rendered {input}")
```

Use `read_stream` for forward, one-at-a-time, optionally long-polling consumption. Use `list_stream_messages` on `Client` or `AsyncClient` for non-blocking newest-first pages. Pass the typed Stream directly, and pass `next_page_token` unchanged until it is empty.

[Pinned runnable listing](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/python/dex_examples/primitives/stream/controller.py)
<!-- dex-source: examples/python/dex_examples/primitives/stream/controller.py -->
```python
page = await app_state.client.list_stream_messages(
    required_query("workflowId"),
    app_state.stream.progress,
    required_int_query("pageSize"),
    optional_query("beforePageToken", ""),
)
```

The before-page token is exclusive and scope-bound. The first page uses an empty token. Listing is a best-effort retained snapshot: concurrent newer writes stay outside the older-page chain, while trimming may remove messages. A trimmed anchor returns an empty page. The server requires a positive page size and caps it at 1000 by default.

`SubFlow(child, input)` is a parent Wait condition. Register both definitions and model unfinished-child behavior explicitly.

## Client

`Client` and `AsyncClient` own Flow lifecycle, typed RPC invocation, Attribute-match waits, Stream reads/listing, history, search, config, timers, and reset. Read and write Flow-owned Attribute and Channel state through typed RPCs, not removed direct Client methods. Async calls must be awaited; sync calls must not run on an event loop thread. Use typed exceptions and explicit deadlines.

## Selection

Attribute = current state; Channel = queued intent; RPC = synchronous mutation/snapshot; Stream = incremental output; Timer = durable time; SubFlow = independently identified durable child.
