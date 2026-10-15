# Dex Go SDK

## Flow state I/O

Applications read and write Flow state through typed RPCs. The Client does not
expose direct Attribute reads or writes, Channel publication, or pending-message
mutation. This keeps each external state transition behind a Flow-owned method.

An RPC can stage `channel.Delete(ctx, messageID)`. Register that RPC with
`dex.RPCOptions{IsTransactional: true}` when a missing ID must abort every
other RPC write. Attribute locking already selects transactional execution, while
Channel deletion requires this explicit option.

## RPC state loading

RPCs receive ordinary Attributes and all Channel size metadata by default.
AttributeMap entries and pending Channel messages are opt-in:

```go
func (flow *OrderFlow) GetRPCs() []dex.RPCDef {
	return []dex.RPCDef{
		dex.DefineRPC(flow.GetOrder, &dex.RPCOptions{
			LoadAttributeMapInstances: []dex.AttributeMapLoad{Items.Load("tenant-a")},
			LoadChannels:              []dex.ChannelDef{Queued},
			LoadChannelMaps:           []dex.ChannelDef{ByTenant},
		}),
	}
}
```

Put an AttributeMap or ChannelMap directly in its plural load option when every
current instance is needed. Use the singular instance options for the less common
exact-instance case. A selected empty queue returns an empty slice; reading an
unselected map entry returns **AttributeMapNotLoadedError**. An unselected pending-message
snapshot returns **ChannelMessagesNotLoadedError**.
Pending messages preserve FIFO order and include the server-assigned message ID.
The snapshot does not change after the handler stages a publish or deletion.

Loading controls which data reaches the Worker. **IsTransactional** controls
atomic commit and Channel deletion validation. Attribute locks add isolation
only among cooperating Steps and RPCs using the same lock. Write-only publish
and delete operations do not require loading.

### Invocation-time map instances

`RPCOptions` remains the registration-time contract for timeout, transactionality,
locks, and fixed state loads. Use `RPCInvokeOptions` when the caller chooses an
exact map instance at runtime:

```go
partition := "partition-007"
err := client.InvokeRPCWithOptions(
	ctx,
	flowID,
	flow.UpsertCustomerProfile,
	profile,
	&result,
	dex.RPCInvokeOptions{
		LockAttributeMapInstances: []dex.AttributeLock{
			dex.LockAttributeMap(CustomerProfiles, partition),
		},
		LoadAttributeMapInstances: []dex.AttributeMapLoad{
			CustomerProfiles.Load(partition),
		},
	},
)
```

Invocation selections are additive. The Client unions, sorts, and deduplicates
them with the registered selections. Locking and loading are independent; a
read-modify-write RPC must request both for the same AttributeMap instance.

Runnable applications use this API in the
[sequential chunking](../examples/go/patterns/sequentially-chunked-attribute-map)
and [hash partitioning](../examples/go/patterns/hash-partitioned-attribute-map)
patterns. The product guides explain the
[ordered append layout](../docs/content/design-patterns/sequentially-chunked-attribute-map.mdx)
and [stable-key lookup layout](../docs/content/design-patterns/hash-partitioned-attribute-map.mdx).

## Action permission projection

An RPC can declare a state-dependent Action in `GetRPCs`. Dex maintains the
Temporal KeywordList Search Attribute `DexWorkQueuePermissions`:

```go
func (flow RefundFlow) GetRPCs() []dex.RPCDef {
	return []dex.RPCDef{
		dex.DefineRPC(flow.ApproveRefund, &dex.RPCOptions{
			Action: dex.DefineAction(
				"Approve",
				dex.WhenAttributeMatches(
					caseStatus,
					dex.AttributeMatchEqual("awaiting-manager"),
					dex.AttributeMatchEqual("awaiting-agent"),
				),
				dex.ActionRequiresPermission("refund.approve"),
			),
		}),
	}
}
```

An Action has a non-empty display label, one Attribute condition, and exactly
one required permission. A condition accepts one or more
`AttributeMatchEqual` values and matches when any value equals the current
Attribute value. The source must be a registered ordinary scalar Attribute.
String, boolean, integer, finite floating-point, and named forms of those types
are supported. A missing or deleted Attribute does not match. Permissions use
lowercase alphanumeric segments separated by dots or hyphens, such as
`refund.approve`.

The initial projection is the sorted union of every matching Action permission.
Duplicate permissions are removed. StartFlow and SubFlow evaluate the initial
Attributes directly. When a successful WaitFor, Execute, timeout handler, or
RPC writes an Action source, the Worker sends the complete Action mapping with
the business writes. The Server overlays those writes on its authoritative
Attribute state and atomically adds newly matching permissions to the stored
set. Unrelated writes and read-only RPCs do not send the mapping.
Projection-only Attribute locks are not required.

The Server skips an upsert when no permission is added. Once a permission has
matched, later state changes and empty mappings do not remove it. The set is
carried across Continue-as-New and remains searchable after completion. An
upgrade preserves permissions that are still stored but does not reconstruct
permissions removed before the upgrade. A new, independent Flow execution
starts from its own initial Attributes. A changed Action definition is first
evaluated when a later source write sends its mapping or a new Flow starts.

`DexWorkQueuePermissions` is reserved for Server-managed runtime projection
and is available as `dex.WorkQueuePermissionsIndexKey` for Work Queue searches. An
application cannot register that name as an Attribute or custom IndexKey. The
projection identifies Runs where an Action is or has been available. An Action
gateway must authorize the selected RPC against its statically registered
permission and current state. Search Attribute contents are never proof that a
caller is authorized or that the Action remains available.

This feature requires the Temporal backend because it uses a KeywordList Search
Attribute. Cadence rejects runtime Action mappings. Go SDK v0.10.2 requires
Server protocol 2.

## Step and timeout-handler state loading

`StepOptions` provides the same five selections independently for `WaitFor` and
`Execute`: all AttributeMap instances, exact AttributeMap instances, Channels,
all ChannelMap instances, and exact ChannelMap instances. The two methods receive
independent snapshots. Execute reads after the winning Wait consumes messages;
retries of one logical method call reuse its first snapshot.

`FlowTimeoutHandlerOptions` provides Execute-style timeouts, heartbeat detection,
retry, durability, Attribute locks, and the same state selections. Set it on
`StartFlowOptions` or `SubFlowOptions` only for a positive timeout using the
handler policy. Exhausted retries may proceed to a registered `Step[None]`; read
the final failure with `Context.RecoveryError`.

```go
type timeoutRecoveryStep struct {
	dex.StepDefaultsNoWaitFor[dex.None]
}

func (timeoutRecoveryStep) Execute(
	ctx dex.Context,
	_ dex.None,
) (*dex.StepDecision, error) {
	failure := ctx.RecoveryError()
	if failure == nil {
		return dex.ForceFail("timeout recovery has no failure"), nil
	}
	return dex.ForceFail(failure.Detail), nil
}
```

The Go SDK is being rewritten around the current Dex `Flow`, `Step`,
`Attribute`, `Channel`, `Stream`, `WaitFor`, and `Execute` contracts.

The rewrite provides the public model, value/protobuf mapping, immutable
registration, application-hosted WorkerService, and typed FlowService Client.

## License

[Sustainable Use License 1.0](LICENSE), with legacy portions under their
original terms as described in [LEGACY_NOTICES.md](LEGACY_NOTICES.md).

## Authoring a flow

Application packages import `dex`, never `gen/dexpb`.

```go
var (
	OrderStatus = dex.DefineAttribute[string](
		"order-status",
		dex.Indexed(dex.AttributeIndex{Type: dex.IndexKeyword}),
	)
	Commands = dex.DefineChannel[Command]("commands")
	Progress = dex.DefineStream[string]("progress", 10<<20)
)

type WaitForCommandStep struct {
	dex.StepDefaults
}

func (WaitForCommandStep) WaitFor(
	ctx dex.Context,
	input OrderInput,
) (*dex.Wait, error) {
	var checkpoint string
	found, err := ctx.GetLastHeartbeatValue(&checkpoint)
	if err != nil {
		return nil, err
	}
	if !found {
		checkpoint = "starting"
	}
	if err := ctx.RecordHeartbeat(checkpoint); err != nil {
		return nil, err
	}
	if err := Progress.Write(ctx, "waiting for a command"); err != nil {
		return nil, err
	}
	if err := Progress.Write(ctx, "inventory check started"); err != nil {
		return nil, err
	}
	if err := OrderStatus.Set(ctx, "waiting"); err != nil {
		return nil, err
	}
	return dex.AnyOf(
		Commands.ForOne(dex.WithConditionID("command")),
		dex.Timer(
			30*time.Minute,
			dex.WithConditionID("timeout"),
		),
	), nil
}

func (WaitForCommandStep) Execute(
	ctx dex.Context,
	input OrderInput,
) (*dex.StepDecision, error) {
	if ctx.HasTimerFired() {
		return dex.ForceFail("command timed out"), nil
	}
	commands, err := Commands.GetConditionResults(ctx)
	if err != nil {
		return nil, err
	}
	return dex.GracefulComplete(commands), nil
}

var WaitForCommand = WaitForCommandStep{}

type OrderFlow struct {
	dex.FlowDefaults
}

func (OrderFlow) GetSteps() []dex.StepDef {
	return []dex.StepDef{
		dex.DefineStartStep(WaitForCommand),
	}
}

func (OrderFlow) GetPersistenceSchema() dex.PersistenceSchema {
	return dex.PersistenceSchema{
		Attributes: []dex.AttributeDef{OrderStatus},
		Channels:   []dex.ChannelDef{Commands},
		Streams:    []dex.StreamDef{Progress},
	}
}
```

Flows use `dex.DefineStartStep` for at most one starting step and
`dex.DefineStep` for every non-starting step.

Execute-only steps embed `dex.StepDefaultsNoWaitFor[IN]`. Step transitions use
`dex.GoTo`, or `dex.MovementOf` with `dex.GoToMulti`.

`StepOptions.WaitForMethodTimeout` and `ExecuteMethodTimeout` bound the two
handler calls. Timer and channel conditions determine how long a Step waits.
Use `dex.Until(condition)` for one condition, and `dex.AllOf` or `dex.AnyOf`
for multiple conditions.

`WaitForRetry` and `ExecuteRetry` limit one logical handler execution. With
`StepDurabilityAsync`, local and fallback regular activities share maximum
attempts, total duration, and 1-based attempt numbers. Fallback starts
immediately; later regular retries continue the backoff sequence at the
cumulative attempt.

Step durability resolves from the method override, then FlowConfig, then
`StepDurabilitySync`. Retry total duration defaults to four hours. Regular
attempts default to two hours with a one-minute heartbeat timeout. The server's
minimum explicit heartbeat timeout defaults to ten seconds and can be changed
by operators; the Go SDK validates only non-negative whole seconds in the
signed int32 range. Async execution first uses at most seven local-activity
seconds and three attempts. The local phase ignores method and heartbeat
timeouts before regular fallback.

### Heartbeats and Stream progress

`Context.RecordHeartbeat` emits a checkpoint from WaitFor or Execute. A retry
restores the most recent regular-activity checkpoint through
`Context.GetLastHeartbeatValue`. Passing nil, including a typed nil, explicitly
clears the checkpoint. Local-activity heartbeats are ignored. Flow timeout
handlers and RPCs cannot send heartbeat or Stream progress.

`Stream.Write` emits a fire-and-forget frame on the same Worker response stream.
A handler may write any number of messages to the same or different Streams.
The call reports local validation, encoding, and gRPC send failures, but it does
not wait for a Stream Store acknowledgment. Server-side store rejection or
unavailability is visible through server logs and metrics rather than the
handler return value. Heartbeats and Stream writes may run concurrently; their
frames are serialized by the Worker.

Use `NewBufferedTextStream` for token-sized text deltas. It preserves text
exactly, flushes after one second or a soft 16 KiB UTF-8 threshold, and emits
the tail before the handler result or error:

```go
progress, err := dex.NewBufferedTextStream(ctx, Thinking)
if err != nil {
	return nil, err
}
if err := progress.Write(delta); err != nil {
	return nil, err
}
```

`BufferedTextStreamFlushInterval` and `BufferedTextStreamMaxBytes` override the
defaults. Empty buffers emit no message or heartbeat. Retry does not restore an
unsent buffer or deduplicate batches already sent.

Messages written by a Step have `#<stepExecutionID>` in
`StreamMessage.Source`. Client writes accept any non-empty source, including
values containing `#`. Reusing a source appends another message; source is
metadata, not an idempotency key.

`Client.ReadStream` reads forward one message at a time and can long-poll.
`Client.ListStreamMessages` returns a non-blocking newest-first retained page.
Pass the registered typed Stream directly, a positive page size, and an empty
token for the first page. Pass `StreamMessagesPage.NextPageToken` unchanged to
read older messages until it is empty. The server caps page size at 1000 by
default. A trimmed anchor returns an empty page, and newer concurrent writes do
not enter an existing older-page chain.

### Canceling Step executions

A successful Step can cancel queued or active executions while continuing with
its normal decision:

```go
return dex.GoTo(RecordQuote, quote).
	CancelSiblingSteps(CarrierA, CarrierB).
	CancelSteps(GlobalQuoteTimeout), nil
```

`CancelSteps` selects every current execution of each registered Step type.
`CancelSiblingSteps` selects only executions whose
`Context.FromStepExecutionID()` matches the current execution. Repeated calls
form a union, and a Flow-wide selector wins for the same Step type. The Worker
rejects nil, mismatched, or unregistered selectors as invalid Step results.

Dex resolves one snapshot after the current execution succeeds. Completed,
already-canceled, and absent targets are no-ops. Next Steps created by the same
decision are not in that snapshot. Dex immediately applies the next or close
action without waiting for target handlers; late decisions, writes, retries,
and recovery Steps are discarded.

An RPC may call `RPCResult.CancelSteps` for Flow-wide selection while also
returning output and scheduling Next Steps. RPCs do not support sibling
selection because an RPC invocation has no Step execution lineage.

### Soft Flow timeout

`StartFlowOptions.Timeout` starts a durable Dex timer rather than a backend
execution timeout. A Flow implementing `FlowTimeoutHandler` defaults to
`TimeoutHandler`; other Flows default to `TimeoutFail`. Override either default
with `TimeoutFail` or `TimeoutCancel`:

```go
func (OrderFlow) HandleTimeout(ctx dex.Context) (*dex.StepDecision, error) {
	return dex.ForceComplete("expired"), nil
}

timeout := 30 * time.Minute
options := dex.StartFlowOptions{
	Timeout:       &timeout,
	TimeoutPolicy: dex.TimeoutHandler,
}
```

The hook is Execute-only, receives no input, and runs at most once after its
durable timer fires or is skipped. It may use `Context` normally and return any
`StepDecision`. Continue-as-new preserves its deadline; retry runs get
a fresh timeout budget. Zero or nil timeout disables the feature.

Use `dex.None` when a Step, RPC, or Channel has no application payload, and pass
`nil` at every call site. It rejects accidental values unlike `any` and makes
the absence of a payload explicit.

Flow RPCs are methods matching `dex.RPC[IN, OUT]`. Attributes and channels stay
strongly typed inside handlers. Step-execution locals and recorded events accept
arbitrary values through `dex.Context`.

Invocation attribute reads return `(value, error)`. A missing static or map
attribute returns `*dex.AttributeNotFoundError`, which callers can inspect with
`errors.As` when absence is expected.

## Waiting and map inspection

`Until`, `AllOf`, and `AnyOf` use unnamed Conditions by default. Do not add
Condition IDs merely because a Condition is nested in one of these waits.
Every Condition in `AnyComboOf` must use `WithConditionID`. Reusing the same
Condition value across combinations is supported, while duplicate IDs on
distinct Conditions are rejected.

Clients wait on typed scalar comparisons in the current run with
`WaitForAttributeMatch` or `WaitForAttributeMapInstanceMatch`. Build matches
with `AttributeMatchEqual`, `AttributeMatchNotEqual`,
`AttributeMatchGreaterThan`, `AttributeMatchGreaterThanOrEqual`,
`AttributeMatchLessThan`, or `AttributeMatchLessThanOrEqual`. The matched value
is decoded into the output pointer. String and bool support equality operators;
integers and doubles support every operator. Every AttributeMap and ChannelMap
instance must be non-empty and must not contain `/`. Objects,
bytes, null, non-finite doubles, and invalid ordering fail before the RPC.

Request IDs are optional for both durable waits. When omitted, the server
derives a stable ID from the Step execution or Attribute condition. Reuse an
override only for the same wait. `RequestTimeout` is the total SDK call budget
across transparent transport reattachments. Zero waits indefinitely until the
caller context is canceled. Temporal permits 10 in-flight Updates and 2,000
total Updates in History per Workflow Execution. A caller timeout or
cancellation does not finish an accepted durable wait, so an abandoned handler
can retain one in-flight slot. Set `InternalHandlerTimeout` only when abandoned
waits can approach that limit. An active call transparently starts another
generation, which counts toward the 2,000-Update history limit. Prefer a value
comfortably longer than normal request timeouts and reconnect gaps. Zero
disables timed rollover. Request expiry returns `*dex.RequestTimeoutError`.

Inside a handler, `AttributeMap.MapSize` and `AllInstanceKeys` include buffered
sets and deletes. `ChannelMap.MapSize` and `AllInstanceKeys` are RPC-only and
include buffered publishes, but omit empty instances. Keys are decoded and
sorted. Use `ForceCompleteIfChannelsEmpty` for atomic conditional completion.

SubFlows are normal, independently addressable Flows used as durable Conditions:

```go
func (ParentStep) WaitFor(_ dex.Context, input ChargeInput) (*dex.Wait, error) {
	return dex.Until(dex.SubFlow(ChargeFlow{}, input)), nil
}

func (ParentStep) Execute(ctx dex.Context, _ ChargeInput) (*dex.StepDecision, error) {
	result, err := dex.SubFlowResult(ctx)
	if err != nil {
		return nil, err
	}
	var receipt Receipt
	if err := result.DecodeSingleOutput(&receipt); err != nil {
		return nil, err
	}
	return dex.GracefulComplete(receipt), nil
}
```

`SubFlowID(ctx, index...)` returns the generated identity, including for a running
`AnyOf` loser. `SubFlowOptions` configures timing, timeout policy, retry, initial
target Attributes, Flow config, Condition ID, and reuse. Parent completion does not cancel an
unfinished SubFlow.

## Registration

Registration uses Go type names without package or pointer by default:
`*orders.OrderFlow` becomes `OrderFlow`. Name each Flow type after its domain,
such as `ApprovalFlow`, and never just `Flow`: the package does not
disambiguate Flow types in one Registry, and Dex Web shows the name as is.
Embed `FlowDefaults` in a Flow. Waiting steps embed `StepDefaults`;
execute-only steps embed `StepDefaultsNoWaitFor[IN]`. Both include the default
step type and options. Override `GetFlowType` or `GetStepType` only when an
explicit durable identity is required. A generic Flow or Step type has no
default name and must override it. `dexcli visualize` and Dex Web v2 use these
same registered names.

Registration is assembled once from each Flow's final durable type, steps,
persistence schema, and exported RPC methods. It rejects empty or duplicate
names, multiple starting steps, invalid indexes, undeclared locks, and
incompatible execute-failure targets before WorkerService starts.

`Worker.Start` aggregates every registered Indexed Attribute and synchronizes
the physical index names with Dex Server before opening its listener. Existing
indexes return immediately; a synchronization failure or the default two-minute
deadline fails startup. Indexed AttributeMaps must declare a fixed index key.

## Attribute Store synchronization

Opt an Attribute or AttributeMap into the Flow's external latest-state
projection, then select Server-configured Attribute Stores on the Flow:

```go
var CustomerEmail = dex.DefineAttribute[string](
	"customer-email",
	dex.SyncToAttributeStore(),
)

config := &dex.FlowConfig{
	AttributeStoreNames: []string{"profiles", "audit"},
}
```

Store updates are asynchronous. Every enabled Attribute write is sent to every
selected Store. Deleting an opted-in Attribute writes SQL `NULL`, and a Store
failure does not roll back the Flow Attribute. A `nil` `AttributeStoreNames`
preserves current targets; an empty non-nil slice disables future synchronization
while preserving protocol presence.

`DefineStep` and `DefineStartStep` retain the step input type behind a private
`typedStepDef` that implements the sealed `StepDef` interface. Runtime
movements resolve through the current Flow's registered step definitions, so a
same-name Step value cannot replace the registered handler or defaults.

RPCs require no communication schema. A Flow lists its RPCs and immutable
execution options explicitly through `GetRPCs`:

```go
func (flow *OrderFlow) GetRPCs() []dex.RPCDef {
	return []dex.RPCDef{
		dex.DefineRPC(flow.GetOrder, nil),
		dex.DefineRPC(flow.UpdateOrder, &dex.RPCOptions{
			Timeout:        30 * time.Second,
			LockAttributes: []dex.AttributeLock{dex.LockAttribute(OrderStatus)},
		}),
	}
}
```

Each registered method uses this exact shape and keeps its Go method name as the
durable RPC name:

```go
func (
	ctx dex.Context,
	input IN,
) (*dex.RPCResult[OUT], error)
```

Only methods returned from `GetRPCs` are RPCs. Exported helpers and unregistered
RPC-shaped methods are ignored. Each definition must contain a direct bound
method on that Flow value; package functions, methods from another Flow, method
expressions, closures, wrappers, duplicates, and invalid options fail Registry
construction. Client calls pass the same direct bound method, such as
`Orders.Update`, but do not pass RPC options. Return `nil, err` on failure;
returning `nil, nil` is an invalid Worker result.

Registered Flow and Step values are retained and may be invoked concurrently.
They must be immutable or concurrency-safe and must keep
invocation state in `dex.Context`.

## Running a Worker

Applications share one Registry and BlobCache between Worker and Client:

The cache comes from
[`github.com/superdurable/dex/blob-cache-go/blobcache`](https://pkg.go.dev/github.com/superdurable/dex/blob-cache-go/blobcache).

```go
logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
registry, err := dex.NewRegistry([]dex.Flow{Orders})
if err != nil {
	return err
}
cache, err := blobcache.New(&blobcache.Config{
	Dir:      "/var/tmp/order-dex-blobs",
	MaxBytes: 1 << 30,
	Logger:   logger,
})
if err != nil {
	return err
}

worker, err := dex.NewWorker(registry, cache, dex.WorkerOptions{
	BindAddress: ":8803",
	Logger:      logger,
})
if err != nil {
	return err
}

client, err := dex.NewClient(registry, cache, dex.ClientOptions{
	WorkerTarget: worker.WorkerTarget(),
	Logger:       logger,
})
if err != nil {
	return err
}
defer client.Close()

if err := worker.Start(); err != nil {
	return err
}
```

`BindAddress` is the local plaintext listener. When
`WorkerOptions.WorkerTarget.Address` is empty, the advertised target derives
from it; wildcard hosts become `localhost`. Set an explicit target when Dex
reaches the Worker through different DNS, a load balancer, or a headless
address. `WorkerTarget()` returns the resolved value for
`FlowConfig.WorkerTarget`.

`Start` blocks and is one-shot. `Stop(ctx)` is idempotent, drains in-flight
calls, and force-stops them if the context expires. The SDK does not install
signal handlers; the [Order example](examples/order/main.go) shows signal-driven
shutdown.

Each invocation receives independent buffered attributes, channel messages,
locals, and events. Successful handlers commit the whole response. Returned
errors, mapping failures, and recovered panics commit nothing. Flow and Step
values may be called concurrently and must be concurrency-safe.

Blob-backed values hydrate through private FlowService calls.
`FlowServiceAddress` defaults to `localhost:8801`. Registry and BlobCache are
required; the caller closes the cache after Client and Worker stop using it.
When `LoadBlobs` omits requested blobs, the error lists every missing blob ID,
its Flow ID, and each affected Worker value: Step or RPC input, Attribute key,
Channel name and message ID, Step-execution local key, heartbeat, or condition
result. It also shows the resolved `FlowServiceAddress`. Check that the Worker's
Flow service matches the Server dispatching the call. Worker error details are
bounded to 1 KiB; when the complete diagnostic exceeds that limit, the SDK also
logs every missing blob and value through its logger.

`ClientOptions.GrpcMaxMessageBytes` and `WorkerOptions.GrpcMaxMessageBytes`
limit the gRPC messages the Client or Worker receives. Zero uses
`dex.DefaultGrpcMaxMessageBytes` (32 MiB), which matches the Dex server's
default `api.grpcMaxMessageBytes`, and keeps gRPC's unlimited send limit, so
the server bounds what the SDK sends. A positive value limits both directions;
negative values are invalid. The Worker applies the limit to its WorkerService
listener, which receives Step and RPC invocations, and to its FlowService
connection. Every hop applies the smaller limit of its two ends, so raise the
server and SDK limits together. Keep the limit above the largest application
value by an explicit margin for the message envelope; the default leaves 16 MiB
of headroom for values up to 16 MiB.

Hydration first requests every uncached blob in one `LoadBlobs` call. Blob
references do not carry sizes, so when a response exceeds a gRPC message limit,
the SDK splits the request in half and retries each half. N uncached blobs take
at most 2N-1 calls. A single blob that still does not fit returns
`*dex.BlobMessageTooLargeError` with its Flow ID and blob ID.

`dex.Logger` supports structured debug, info, warning, and error messages.
`blobcache.Config.Logger` defaults to `slog.Default` and is inherited by Client
and Worker. Their option-level loggers override it for that component.

## Calling FlowService

Client methods target the current run by sending an empty run ID. This follows
Continue-as-New automatically. `StartFlow` and `TimeTravel` return run IDs for
diagnostics, while normal calls need only the flow ID.

`ClientOptions.Credential` and `WorkerOptions.Credential` send a bearer
credential with every FlowService call. A Dex server with authentication enabled
requires one on every public API call. `dex.FileCredential(path)` reads a
token file, such as a projected service account token, and rereads it when it
changes; an emptied or missing file fails calls instead of sending a stale
token. The Worker sends its credential with startup index verification and
blob hydration.

`ClientOptions.WorkerTarget` is the default advertised target for StartFlow.
A per-call `ConfigOverride.WorkerTarget` takes precedence. UpdateFlowConfig does
not inherit the Client default.

StartFlow uses the starting step retained by Registry. Its input must match
that step's input type, and its step options come from the registered Step.
Flows without a starting step require nil input.

The SDK generates request IDs for StartFlow and InvokeRPC. A
`StartFlowOptions.RequestID` may provide a stable business identifier spanning
separate calls. Durable wait request IDs are optional overrides of server-derived
stable IDs.

Client is safe for concurrent calls. `Close` is idempotent and closes only its
owned gRPC connection. Calls after Close return a local error.

`WaitForFlow` hydrates every requested completion before returning. Decode one
known output with `result.DecodeSingleOutput(&output)`; it returns a local error
unless exactly one completion exists. For multiple outputs, match
`result.Completions` by `StepType` or `StepExecutionID`, then call
`completion.Output.Decode(&target)`. The slice preserves server collection
order, but parallel branch order is not deterministic. No-output Flows return
an empty slice.

Remote FlowService failures use concrete Go error types. Match expected
conditions with `errors.As` instead of comparing gRPC codes or sub-statuses:

```go
var inactive *dex.FlowNotActiveOrNotFoundError
if errors.As(err, &inactive) {
	// The Flow is missing or already closed for this mutation.
}
```

`FlowNotFoundError` is returned by reads requiring an existing Flow, including
WaitForFlow and TimeTravel. RPCs, timer operations, and step or attribute waits return
`FlowNotActiveOrNotFoundError` when no active Flow is available.

RPCs also use this error when the target Flow cannot be found, including query-only
reads. A query-only RPC without locks, transactional execution, durable effects, or
Server-forced Update routing can read a retained terminal execution. At that confirmed
read boundary, this error means no readable target was found; return the application's
missing or unavailable result directly, without a lifecycle probe, a short timeout, a
retry, or historical Step-output decoding. Preserve other service and Worker failures.
For mutations and active-only RPCs, the error can mean either missing or closed and does
not prove that the requested action succeeded. RPC success does not prove that the Flow
is active.

Duplicate starts return `FlowAlreadyStartedError`. Server long polls return
`LongPollTimeoutError`, including the operation and Flow ID. Durable Step and
Attribute waits reattach transparently instead of exposing that error. Worker handler
failures return `WorkerInvocationError`; its `Worker` field preserves the
original Worker code, type, and detail. Concurrent locking RPCs return
`RPCLockConflictError` separately.

`WaitForFlow` returns `FlowResult` for every terminal status. Inspect `Status`,
`ErrorType`, `ErrorMessage`, and any requested `Completions`; transport and
long-poll failures remain errors.

Every specific service error unwraps to `*dex.ServiceError` and then to the
original gRPC error. `ServiceError` exposes `Op`, `FlowID`, `Code`, `SubStatus`,
and `Detail`; sub-status is diagnostic metadata, not application control flow.
Unknown or malformed server details fall back to `ServiceError`.

Registry failures use `FlowDefinitionError`, invalid Worker handler results use
`InvalidStepResultError`, and codec failures use `ValueMappingError`. Ordinary
argument and context errors remain ordinary Go errors.

## Value encoding

Strings, booleans, signed integers, representable unsigned integers, and
floating-point values use native Dex value arms. Strings must contain valid
UTF-8; use `[]byte` for arbitrary binary data. Byte slices use an object arm
with wire encoding `"raw"` and store the bytes directly without base64.
Structs, maps, other slices, arrays, and JSON-compatible values use an object
arm with wire encoding `"json"`.

Internal Blob references are opaque. Hydration sends the owning Flow ID with
each reference, and the local cache is isolated by `(flowID, blobRef)`.

Returned dynamic values remain opaque until decoded:

```go
var result OrderResult
if err := value.Decode(&result); err != nil {
	return err
}
```

Decode requires a non-nil pointer. Invalid UTF-8 strings, integer overflow,
incompatible targets, unknown encodings, unhydrated blob references, and
malformed JSON return errors.

Ordinary nil encodes with the Dex null arm. In an Attribute write, that arm
deletes the Attribute. In every other Value context, it represents null.

### Indexed attributes

Keyword and text indexes accept valid UTF-8 strings. Keyword-array indexes
accept slices containing valid UTF-8 strings. Int, double, and bool indexes
accept their matching Go primitive types. Datetime indexes accept `time.Time`
or RFC3339Nano strings, including UTC `Z` and numeric offsets. Fractional
seconds are preserved. Numeric strings are not treated as Unix nanoseconds.
Initial indexed values are validated by `dex.InitialAttribute` and
`dex.InitialAttributeMapValue`.

The SDK generates UUIDs for StartFlow and InvokeRPC.
`StartFlowOptions.RequestID` may override the generated start ID. Durable waits
use server-derived IDs unless the caller supplies an override.

Large string and object values may be returned as blob references. Worker
inputs and Client results hydrate before handler or application decode. Decode
never performs network I/O.

Compilable examples:

- [Order flow](examples/order/main.go)
- [Every Client API](examples/order/client.go)
- [Step transitions](examples/transitions/main.go)
- [Flow method RPC](examples/rpc/main.go)

## Verification

```text
make unitTests
make clientIntegTests
make workerIntegTests
make e2eTests
make docsCheck
make copyright-check
```

`docsCheck` parses the hand-written `dex`, `logging`, and shared `blobcache`
packages and requires a Go doc comment on every exported type, function,
method, field, interface method, constant, and variable. Comments must begin
with the declared name. Generated protobufs and tests are excluded. Use
`go doc dex.<Name>` (or an IDE hover) to inspect the same application-facing
API documentation locally.

`e2eTests` uses the current checkout's `dexcli dev` environment. It
runs the migrated iWF Go SDK scenarios through the public Dex SDK. It also
visualizes `integ/webv2approval` and `integ/webv2reply` into a Flow rendering
directory and checks that Dex Web v2 lists, searches, displays, edits, acts
on, and starts both Flows under the names the Worker registers.
`DEX_INTEG_DEX_PORT`, `DEX_INTEG_WEB_PORT`, and `DEX_INTEG_WORKER_PORT` pin
the Dex gRPC, Dex Web, and integration Worker ports; each defaults to a free
port.

`integTests` runs `./integ` against a Dex you start yourself. Start it with
`dexcli dev -flow-rendering-dir` pointing at the two visualized fixtures, and
set `DEX_FLOW_SERVICE_ADDRESS` and `DEX_WEB_ADDRESS` (default
`127.0.0.1:8801` and `127.0.0.1:8802`).

### Measure integration coverage

Run the local Client and Worker integration suites plus all `dexcli dev`
E2E scenarios with Go coverage:

```shell
make integrationCoverage
```

The report measures only production packages under `./dex/...`. Unit
tests, standalone BlobCache module tests, examples, generated protobuf stubs under
`gen/`, and `*_test.go` files are excluded. Open `coverage/index.html` for
annotated source, or inspect `coverage/coverage.txt` for per-function
totals. `coverage/coverage.out` is the profile uploaded by CI.

CI uploads the profile to Codecov with GitHub OIDC, so no upload secret is
stored in this repository. The report uses the `sdk-go-integration` flag
and contributes to the Go SDK component defined in the root `codecov.yml`.
The Actions run also publishes the report directory as
`sdk-go-integration-coverage`.

The detailed design and later phase boundaries are in the
[Go SDK rewrite plan](../docs/design/plan/go-sdk-rewrite.md).

## Server protocol compatibility

Worker startup calls `GetServerInfo`, negotiates the highest version in the
intersection of the Server and Go SDK protocol intervals, synchronizes Attribute
indexes, and then binds WorkerService. The initial SDK interval is `[1,1]`.
Missing server information, invalid or disjoint intervals, and RPC failures stop
startup before binding. The negotiated protocol is retained for future
protocol-specific paths.

The diagnostic SDK version comes from Go build information for the
`github.com/superdurable/dex/sdk-go` module. Source builds without module version
metadata report `dev`; neither value changes compatibility decisions.

## Search run inclusion

Dex Server excludes ContinuedAsNew runs by default. Application queries still
need `FlowType` when using shared index slots. An explicit inclusion option
removes only the default exclusion; it does not override query filters. Keep
the query, inclusion option, and page size consistent across pages.

Use `SearchFlowsWithOptions(ctx, query, pageSize, nextPageToken, SearchFlowsOptions{IncludeContinuedAsNew: true})` for execution-chain inspection. `SearchFlows` uses the default exclusion.
