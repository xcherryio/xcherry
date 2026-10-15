# Go primitives

Read core primitive semantics first. This page supplies Go API shapes at the pinned baseline.

## Flow, Step, and decisions

A Flow implements `dex.Flow`; embedding `dex.FlowDefaults` supplies optional behavior. Register the start Step with `dex.DefineStartStep` and every reachable Step with `dex.DefineStep`. Embed `dex.StepDefaultsNoWaitFor[T]` when there is no WaitFor; otherwise implement both methods. Use the inherited type-name defaults and a domain Flow type name such as `OrderFlow`, never just `Flow`; follow the [production-rename exception](versioning.md#default-flow-and-step-type-names) before adding `GetFlowType` or `GetStepType`.

[Pinned runnable source](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/go/primitives/flow/workflow.go)
<!-- dex-source: examples/go/primitives/flow/workflow.go -->
```go
func (ExampleStep) WaitFor(ctx dex.Context, _ int) (*dex.Wait, error) {
	if err := Status.Set(ctx, "running"); err != nil {
		return nil, err
	}
	return dex.SkipWaitImmediately(), nil
}

func (ExampleStep) Execute(_ dex.Context, input int) (*dex.StepDecision, error) {
	return dex.GoTo(Finish{}, input+1), nil
}
```

Use `GoTo`, `GoToMany`, or `DeadEnd` to keep work open. Graceful completion waits for compatible active branches; force completion/failure is an explicit terminal override.

## Wait and Timer

`dex.Until(condition)` waits for one condition; `AllOf` and `AnyOf` combine conditions. Among ready `AnyOf` candidates, Dex uses canonical Timer, Channel, then SubFlow order and preserves argument order within each kind. An earlier unready Condition does not block a later ready one. Only the winning Channel consumes messages. For strict priority, return only the current higher-priority Condition until it resolves. Timers are durable conditions, not sleeps inside Execute.

[Pinned runnable source](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/go/primitives/timer/workflow.go)
<!-- dex-source: examples/go/primitives/timer/workflow.go -->
```go
func (timerStep) WaitFor(_ dex.Context, input int) (*dex.Wait, error) {
	return dex.Until(dex.Timer(time.Duration(input) * time.Second)), nil
}

func (timerStep) Execute(_ dex.Context, _ int) (*dex.StepDecision, error) {
	return dex.GracefulComplete("timer-fired"), nil
}
```

## Attribute and AttributeMap

Define typed state at package scope with `DefineAttribute[T]` or `DefineAttributeMap[T]` and register it in `PersistenceSchema`. Reads/writes use `dex.Context` and return errors. AttributeMap instances partition one schema definition; selectively load exact instances when handlers do not receive the whole map. Index only query fields. Use an Attribute Store only when state must be queryable outside an invocation.

## Channel and ChannelMap

Channels are durable queues. `ForOne` and `ForN` create conditions; after firing, read condition results and delete/move messages deliberately. ChannelMap separates queues by validated instance name. External callers invoke a typed Flow RPC; its handler publishes through Context.

Pending-message reads inside a Step or RPC are invocation snapshots. Other handlers may consume, delete, or publish concurrently. Transactional execution validates selected deletions and commits writes atomically, but does not lock the whole snapshot. Read and write pending messages directly only when the operation explicitly tolerates that race. When a decision requires the queue to remain unchanged, every cooperating Step and RPC writer must use the same Attribute lock.

[Pinned runnable source](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/go/primitives/channel/workflow.go)
<!-- dex-source: examples/go/primitives/channel/workflow.go -->
```go
func (channelWait) WaitFor(_ dex.Context, input int) (*dex.Wait, error) {
	return dex.AnyOf(
		ApprovalMessages.ForOne(),
		dex.Timer(time.Duration(input)*time.Second),
	), nil
}
```

## RPC

Only methods returned from `GetRPCs` are Worker RPCs. Register direct bound Flow methods with `dex.DefineRPC`. Put timeout, locks, transactions, and selective loads in its `dex.RPCOptions`; `Client.InvokeRPC` takes no options argument. A definition without locks or transactional execution can query a retained terminal run when the handler returns no durable effects. Locks, transactions, returned effects, or Server policy require an active execution. Keep handlers short and lock conflicting Attributes. RPC results may request supported movements, publishing, or cancellation; never emulate transactions with process-local locks.

## Stream

Define a Stream with a byte limit and register it. A Step writes ordered progress; consumers resume from the Client token. A Stream is a feed, not authoritative state.

[Pinned runnable source](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/go/primitives/stream/workflow.go)
<!-- dex-source: examples/go/primitives/stream/workflow.go -->
```go
var Progress = dex.DefineStream[string]("Progress", 10<<20)

type StreamFlow struct {
	dex.FlowDefaults
}
```

Use `Client.ReadStream` for forward, one-at-a-time, optionally long-polling consumption. Use `Client.ListStreamMessages` for non-blocking newest-first pages. Pass the typed Stream directly, and pass `NextPageToken` unchanged to the next call until it is empty.

[Pinned runnable listing](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/go/primitives/stream/controller.go)
<!-- dex-source: examples/go/primitives/stream/controller.go -->
```go
var page sdk.StreamMessagesPage[string]
err = controller.client.ListStreamMessages(
	request.Request.Context(),
	flowID,
	Progress,
	int32(pageSize),
	request.Query("beforePageToken"),
	&page,
)
```

The before-page token is exclusive and scope-bound. The first page uses an empty token. Listing is a best-effort retained snapshot: concurrent newer writes stay outside the older-page chain, while trimming may remove messages. A trimmed anchor returns an empty page. The server requires a positive page size and caps it at 1000 by default.

## SubFlow and Client

`dex.SubFlow(child, input)` is a parent-owned Wait condition. Register parent and child, and explicitly model what parent completion means for unfinished children. Client owns Flow lifecycle, typed RPC invocation, Attribute-match waits, Stream operations, history, search, config, timers, and reset. Read and write Flow-owned Attribute and Channel state through typed RPCs, not removed direct Client methods. Always pass a bounded context and match typed errors with `errors.As`.

## Selection rule

Use Attribute for current state, Channel for queued intent, RPC for synchronous mutation/snapshot, Stream for incremental output, Timer for durable time, and SubFlow for an independently identified durable child.
