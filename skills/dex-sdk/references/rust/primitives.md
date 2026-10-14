# Rust primitives

Read the core primitives guide first for product semantics. This page records the Rust SDK shapes at the pinned baseline.

## Flow, Step, and Wait

`Flow::StartInput` is the input accepted by `Client::start_flow`. `steps` returns the closed Step graph. A Step's WaitFor phase decides durable readiness; Execute performs side effects and returns the next graph movement. `Wait::until`, `any_of`, `all_of`, and `any_combination_of` compose Conditions. `Wait::skip_immediately` bypasses waiting.

[Runnable source](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/rust/src/primitives/wait_types/flow.rs)
<!-- dex-source: examples/rust/src/primitives/wait_types/flow.rs -->
```rust
fn wait_for(&self, _context: &mut Context, input: Self::Input) -> HandlerResult<Wait> {
    let timeout = Duration::from_secs(input.timeout_seconds.max(0) as u64);
    match input.mode.as_str() {
        "any" => Ok(Wait::any_of([
            SIGNAL_A_CHANNEL.for_one().with_id("signal"),
            Timer::by_duration(timeout).with_id("timeout"),
        ])),
        "all" => Ok(Wait::all_of([
            SIGNAL_A_CHANNEL.for_one().with_id("signal-a"),
            SIGNAL_B_CHANNEL.for_one().with_id("signal-b"),
        ])),
        "combo" => Ok(Wait::any_combination_of([
            ConditionCombination::all_of([
                SIGNAL_A_CHANNEL.for_one().with_id("signal-a"),
                Timer::by_duration(timeout).with_id("timeout"),
            ]),
            ConditionCombination::all_of([SIGNAL_B_CHANNEL.for_one().with_id("signal-b")]),
        ])),
        _ => Err(HandlerError::new(
            "WaitTypes",
            format!("unknown wait mode {}", input.mode),
        )),
    }
}
```

Among ready `Wait::any_of` candidates, Dex uses canonical Timer, Channel, then SubFlow order and preserves iterator order within each kind. An earlier unready Condition does not block a later ready one. Only the winning Channel consumes messages. For strict priority, return only the current higher-priority Condition until it resolves.

Use condition IDs when Execute must distinguish multiple timers or conditions. Read typed condition results from `Context`; do not infer a winner from elapsed wall time.

## Step decisions

- `StepDecision::go_to` schedules one next Step.
- `go_to_many` fans out movements.
- `graceful_complete` contributes a terminal output after siblings settle.
- `force_complete` ends the Flow immediately.
- `force_fail` terminally fails it.
- `dead_end` ends only that branch.
- cancellation modifiers cancel a sibling Step type or a selected execution.

Use `StepMovement::to_with_options` when one transition needs different options without changing the Step's defaults. Never choose force completion merely to make a test finish; it changes sibling and outstanding-work semantics.

## Attributes and maps

An `Attribute<T>` stores one typed durable value. `AttributeMap<T>` stores independently addressable instances. Both belong in `PersistenceSchema`. Indexed attributes support Flow search; `sync_to_attribute_store` projects values into an Attribute Store configured by `FlowConfig`.

[Runnable source](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/rust/src/primitives/attribute/flow.rs)
<!-- dex-source: examples/rust/src/primitives/attribute/flow.rs -->
```rust
static STATUS: LazyLock<Attribute<String>> = LazyLock::new(|| {
    Attribute::new("primitive-attribute-status")
        .indexed(AttributeIndex::keyword().with_key("keyword1"))
});
static EMAIL: LazyLock<Attribute<String>> =
    LazyLock::new(|| Attribute::new("primitive-attribute-email").sync_to_attribute_store());

pub fn attribute_store_config() -> FlowConfig {
    FlowConfig::new().attribute_store_names(vec!["profiles".to_owned()])
}
```

Call `get`, `set`, or `clear` through `&mut Context`. Load only the map instances needed by a handler, and lock every attribute whose read-modify-write invariant must be serialized.

## Channels and maps

Channels are durable message queues. `for_one` and `for_n` create wait Conditions; `publish` appends; `pending_messages`, `find_pending_message`, and `delete` support explicit queue management. A `ChannelMap<T>` partitions queues by instance key. Declare the definition before attaching instance loads.

[Runnable source](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/rust/src/primitives/channel/flow.rs)
<!-- dex-source: examples/rust/src/primitives/channel/flow.rs -->
```rust
fn rpcs(&self) -> RpcList<Self> {
    RpcList::new()
        .procedure_without_input(PUBLISH_APPROVAL_MESSAGE, Self::publish_approval_message)
        .procedure(ENQUEUE_CHANNEL_MESSAGE, Self::enqueue_channel_message)
        .function_without_input(
            GET_QUEUED_MESSAGES.load_channel(&QUEUED_MESSAGES),
            Self::get_queued_messages,
        )
        .procedure(
            DELETE_QUEUED_MESSAGE
                .is_transactional()
                .load_channel(&QUEUED_MESSAGES),
            Self::delete_queued_message,
        )
        .function_without_input(
            GET_PRIORITIZED_MESSAGES.load_channel(&PRIORITIZED_MESSAGES),
            Self::get_prioritized_messages,
        )
        .procedure(
            MOVE_QUEUED_MESSAGE_TO_PRIORITIZED_MESSAGES
                .is_transactional()
                .load_channel(&QUEUED_MESSAGES),
            Self::move_queued_message_to_prioritized_messages,
        )
}
```

Deleting and republishing is a transaction only when the RPC requests transactional execution. A Channel wait does not itself guarantee that a later Execute mutation is atomic with publication.

Pending-message reads inside a Step or RPC are invocation snapshots. Other handlers may consume, delete, or publish concurrently. Transactional execution validates selected deletions and commits writes atomically, but does not lock the whole snapshot. Read and write pending messages directly only when the operation explicitly tolerates that race. When a decision requires the queue to remain unchanged, every cooperating Step and RPC writer must use the same Attribute lock.

## RPC

Define an `Rpc<Input, Output>` with a stable logical name, register it in `Flow::rpcs`, and expose a handler taking `&mut Context`. Use `procedure_without_input` for unit input and a function registration when there is a typed response. An `RpcResult` may return a value and a `StepMovement`.

RPCs can declare locks, timeout, transactions, and selective loads. An RPC without locks or transactional execution can query a retained terminal run when it returns no durable effects. Locks, transactions, returned effects, or Server policy require an active execution. Treat an RPC as a durable command/query boundary, not an arbitrary escape hatch into Worker memory.

## Stream

`Stream<T>` is append-oriented output. Give it a maximum payload size and add it to `PersistenceSchema`. Text output can be buffered to avoid one remote write per fragment.

[Runnable source](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/rust/src/primitives/stream/flow.rs)
<!-- dex-source: examples/rust/src/primitives/stream/flow.rs -->
```rust
let progress = PROGRESS.buffered_text_with_options(
    context,
    BufferedTextStreamOptions::new(Duration::from_millis(500), 16 * 1024),
)?;
progress.write(format!("Rendering preview for {input}"))?;
progress.write(format!("Preview ready for {input}"))?;
```

Buffered writes are asynchronous; use them for progress-like output, not as the only proof that a business mutation committed.

Use `Client::read_stream_with_timeout` for forward, one-at-a-time, long-polling consumption. Use `Client::list_stream_messages` for non-blocking newest-first pages. Pass the typed Stream directly, and pass `next_page_token` unchanged until it is empty.

[Runnable listing source](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/rust/src/primitives/stream/controller.rs)
<!-- dex-source: examples/rust/src/primitives/stream/controller.rs -->
```rust
client
    .list_stream_messages(
        &query.workflow_id,
        &PROGRESS,
        query.page_size,
        &query.before_page_token,
    )
```

The before-page token is exclusive and scope-bound. The first page uses an empty token. Listing is a best-effort retained snapshot: concurrent newer writes stay outside the older-page chain, while trimming may remove messages. A trimmed anchor returns an empty page. The server requires a positive page size and caps it at 1000 by default.

## Timer and SubFlow

`Timer::by_duration` creates a durable relative timer. Compose it with Channels for reminders, inactivity races, and deadlines. `SubFlow::run` starts a child and returns a Condition. `SubFlowOptions` controls timeout and timeout policy. After the wait, read the typed `SubFlow::condition_result`, outputs, and Flow IDs from Context.

## Client

`Client` starts, stops, searches, and inspects Flows; invokes typed RPCs; waits for Attribute matches and Flow or Step completion; and retrieves Stream output. Read and write Flow-owned Attribute and Channel state through typed RPCs, not removed direct Client methods. Controllers should map `SdkError` explicitly and move blocking client calls off async executor threads. Use a unique Flow ID for each test or logical entity, and a stable request ID when retrying the same command.
