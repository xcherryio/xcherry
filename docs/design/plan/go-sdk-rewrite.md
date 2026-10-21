# Go SDK rewrite plan

Pending Channel queues use `ChannelMessage[T]` envelopes. Client list methods
decode into a caller-owned slice pointer, and deletion uses the server-assigned
message ID. RPC handlers stage deletion through the Channel definition and set
`RPCOptions.IsTransactional` in `DefineRPC` when existence validation must be
atomic with all other RPC writes.

`RPCOptions` also carries typed AttributeMap, Channel, and ChannelMap loads.
Ordinary Attributes and Channel size metadata remain automatic.
Map entries and pending message envelopes are available only when loaded.
Exact map loads use a slash-free logical instance and encode its physical name;
all-instance loads use a trailing slash. The RPC Context returns a stable
state-not-loaded error when an unloaded collection is read.

`RPCInvokeOptions` is the narrow invocation-time companion to `RPCOptions`. It
adds exact AttributeMap locks, exact AttributeMap loads, and exact ChannelMap
loads selected from application input. `Client.InvokeRPCWithOptions` validates
the selections against the RPC's Flow before network I/O, then unions, sorts,
and deduplicates them with registered state requirements. It cannot replace or
clear registration-time options. Dynamic locks and loads remain independent.

Status: Phases 1 through 5 are implemented.

## Current source of truth

This plan is based on the current:

- [`protos/dex.proto`](../../../protos/dex.proto)
- [`protos/README.md`](../../../protos/README.md)
- [`server/service/interpreter/activityImpl.go`](../../../server/service/interpreter/activityImpl.go)
- [`server/service/interpreter/workflowImpl.go`](../../../server/service/interpreter/workflowImpl.go)
- [`server/service/interpreter/channel/plan.go`](../../../server/service/interpreter/channel/plan.go)
- [`server/service/common/rpc/invoke.go`](../../../server/service/common/rpc/invoke.go)
- [`server/service/api/service.go`](../../../server/service/api/service.go)
- [`sdk-go/dex/registration.go`](../../../sdk-go/dex/registration.go)
- [`sdk-go/dex/rpc_registration.go`](../../../sdk-go/dex/rpc_registration.go)
- [`sdk-go/dex/proto_mapper.go`](../../../sdk-go/dex/proto_mapper.go)
- [`sdk-go/dex/hydration.go`](../../../sdk-go/dex/hydration.go)
- [`sdk-go/dex/client.go`](../../../sdk-go/dex/client.go)
- [`sdk-go/dex/options.go`](../../../sdk-go/dex/options.go)
- [`sdk-go/dex/errors.go`](../../../sdk-go/dex/errors.go)
- [`blob-cache-go/blobcache/cache.go`](../../../blob-cache-go/blobcache/cache.go)

The old `sdk-go/dex` API is not a compatibility constraint. The product has not
launched, so the rewrite removes obsolete Workflow/State/WaitUntil,
DataAttribute/SearchAttribute, and Signal/InternalChannel concepts instead of
adding aliases.

### Server contracts the SDK must preserve

- `Flow`, `Step`, `Attribute`, `Channel`, `Stream`, `WaitFor`, and `Execute` are the
  canonical concepts.
- Persistence is the declared set of attributes, channels, and Streams.
- Attributes are unified. Indexing is an optional `IndexConfig` attached to an
  attribute write.
- Channels are unified and carry multiple FIFO values.
- Streams are best-effort resumable messages with an approximate Flow-type-wide budget.
- Both channel bounds are optional:
  - neither bound: exactly one;
  - only `at_least`: at least N, with no upper bound;
  - only `at_most`: zero through N;
  - both: the inclusive range.
- `StepDecision` has normal next steps and a separate `CloseDecision`.
- A normal Execute must return a non-nil, non-empty decision. Only conditional
  force-complete may combine a close decision with fallback next steps.
- `Context.from_step_execution_id` exposes step-execution lineage.
- WaitFor, Execute, and RPC may write attributes, record events, and publish
  channel messages. Step-execution locals pass from WaitFor to Execute.
- RPC may trigger next-step movements, but the current server rejects a close
  decision returned by RPC.
- Channel sizes are supplied only to Worker RPC invocations.
- StartFlow and InvokeRPC require a request ID. The SDK generates it, except
  that StartFlow may use a caller-supplied business identifier. Durable Step
  and Attribute waits accept an optional override; otherwise the server derives
  a stable logical ID. Locking RPC and durable waits use these IDs as Temporal
  synchronous-update IDs.
- `WorkerTarget` belongs to `FlowConfig` and may describe a normal or headless
  plaintext gRPC target. `ClientOptions` may provide its StartFlow default.
- Large string/object `Value` arms may contain blob IDs. Phase 4 wires the
  existing cache into hydration; cache storage policy remains independently
  designed.

## Phase boundaries

### Phase 1 — public API and type model

Design and add the public Go contracts used by application code:

- flow and step interfaces plus the RPC function signature;
- `PersistenceSchema`;
- typed attributes and channels;
- untyped step-execution locals and recorded events;
- invocation `Context`;
- waiting conditions and typed result accessors;
- step movements, close decisions, and step options;
- public client request/result structs and non-generic client methods;
- public SDK errors.

Phase 1 does not implement registration, WorkerService, gRPC calls, protobuf
mapping, value encoding, blob hydration, or blob caching.

### Phase 2 — value codec and protobuf mapping

Implement concrete-value encoding, pure proto conversion, error conversion,
and the internal hydration seam. The separately implemented blob-cache
component may later be injected behind that seam; this SDK plan does not define
its storage, eviction, size, recovery, or test strategy.

### Phase 3 — registration assembly

Build the immutable registry used later by WorkerService and Client. Validate
flow, step, persistence, and fallback definitions; erase generic step and RPC
handlers; discover Flow method RPCs; and assemble scoped lookups. Phase 3 adds
no WorkerService, invocation context, protobuf request handling, or transport.

### Phase 4 — WorkerService runtime

Implement the application-hosted plaintext gRPC worker around the Phase 3
registry. Add invocation contexts, request hydration and decoding, buffered
commit mapping, method errors, panic recovery, and worker lifecycle. Keep the
generated WorkerService and hydration machinery private.

### Phase 5 — FlowService client and integration migration

Implement the public FlowService client, connect response hydration and error
mapping, migrate the Go integration suite, and run it against the default
Temporal-backed Dex server.

## Phase 2 detailed design

### Value encoding

Application code continues to use Go values and opaque `dex.Value`; `dexpb`
remains internal.

| Go value | Proto arm |
|---|---|
| valid UTF-8 string and named strings | `string_value` |
| signed integers | `int_value` |
| unsigned integers up to `math.MaxInt64` | `int_value` |
| float32 and float64 | `double_value` |
| bool and named bools | `bool_value` |
| ordinary nil and typed nil | `null_value` |
| `[]byte` and named byte slices | `obj_value`, encoding `"raw"` |
| all other JSON-compatible values | `obj_value`, encoding `"json"` |

Strings with invalid UTF-8 return an encoding error; arbitrary binary data uses
`[]byte`. Raw-byte object payloads contain the bytes directly without JSON or
base64 encoding. Structs, maps, other slices, arrays, and non-indexed
`time.Time` use JSON. The proto null arm represents top-level null. Within an
Attribute write, it deletes the Attribute.

`Value.Decode` requires a non-nil pointer. It rejects overflow, incompatible
targets, malformed JSON, unknown object encodings, and blob arms that have not
passed through hydration. Dynamic failures return errors and never panic.

### Indexed attributes

| Index type | Accepted Go values |
|---|---|
| keyword and text | valid UTF-8 string and named strings |
| keyword array | valid UTF-8 string slices and equivalent named slices |
| int | signed integers and unsigned integers up to `math.MaxInt64` |
| double | float32 and float64 |
| bool | bool and named bools |
| datetime | `time.Time` or an RFC3339Nano string |

An indexed delete carries both the null arm and the definition's index config.
An attribute without indexing omits `IndexConfig`.
Datetime strings accept UTC `Z` and numeric offsets, preserve fractional
seconds, and do not interpret numeric strings as Unix nanoseconds.

### Pure proto mapping

Phase 2 maps waits, conditions, decisions, movements, retries, step and flow
options, initial attributes, result values, statuses, search entries, and health
results without assembling a WorkerService or FlowService invocation.

Positive durations round up to whole seconds; negative durations fail.
Pointer durations retain absence. Unknown SDK or proto enum values fail.
Unnamed conditions receive deterministic reserved IDs in declaration order.
Explicit IDs must be non-empty, unique, and outside the reserved prefix.

### Errors and hydration

The internal error mapper preserves the gRPC code, Dex substatus and detail,
plus original worker code, type, and detail. A gRPC error without Dex details
uses `ErrorSubStatusUncategorized`; local non-gRPC errors remain unchanged.

The private hydration seam accepts blob arms and returns concrete values in the
same order. It deduplicates repeated references and validates count and arm
kind. String cache payloads are raw UTF-8 bytes. Object cache payloads are
deterministic protobuf encodings of the complete `EncodedObject`.

Phase 2 does not call `LoadBlobs`, wire the disk cache, register flows or RPCs,
run WorkerService, or execute public client methods.

### Phase 2 exit gate

1. Concrete and indexed values round-trip without exposing `dexpb`.
2. All pure mappers and invariant failures have package-internal tests.
3. Error details and blob hydration contracts are independently testable.
4. Registration, WorkerService, and FlowService transport remain absent.

## Phase 3 detailed design

### Registration surface

The SDK builds one opaque immutable registry from `[]Flow`:

```go
type Registry struct {
	// unexported
}

func NewRegistry(flows []Flow) (*Registry, error)
```

The Phase 3 implementation may initially keep this wrapper private. Phase 5
exports it so one validated instance can be shared by Client and Worker.
Application code declares its contents through `Flow.GetSteps`,
`Flow.GetRPCs`, and `Flow.GetPersistenceSchema`.

There is no mutable `AddFlow`, `AddStep`, or `AddRPC` API. Construction is
atomic: validation and lookup assembly happen in temporary state, and any error
returns no registry. Registration errors are ordinary descriptive errors; no
new public error hierarchy is needed.

### Registry structure and lookup scope

The registry owns:

- a flow lookup keyed by durable flow type;
- per-flow step lookups keyed by durable step type;
- per-flow RPC lookups keyed by explicitly registered Go method name;
- per-flow attribute and channel definition lookups.

Step and RPC names are scoped to one flow. The same durable step or RPC name may
appear in different flows. Flow types are registry-wide and must be unique.
Attribute names are unique within a flow's attribute namespace, and channel
names are unique within its channel namespace. An attribute and channel may
share a name because the server stores them separately.

Flow and step names use a non-empty `GetFlowType` or `GetStepType` override.
Otherwise the SDK uses the Go type name without its package or pointer
markers, producing names such as `OrderFlow` and `initializeStep`. A generic
or unnamed type has no default name and must override it. Registration,
movements, failure targets, Worker dispatch, and StartFlow share this resolver.
Because the package is not part of the name, a Flow type is named after its
domain, such as `ApprovalFlow`, never just `Flow`.

All lookup APIs remain private. Phase 4 receives immutable descriptors rather
than raw maps and is responsible for converting a missing lookup into the
appropriate WorkerService error.

### Flow and persistence validation

`NewRegistry` evaluates each supplied Flow once. It rejects:

- nil and typed-nil Flow values;
- duplicate final durable flow types;
- nil persistence definitions;
- empty or duplicate attribute names;
- empty or duplicate channel names;
- invalid attribute index types;
- conflicting index types for one non-empty shared index key.

A definition records whether it is static or a map. Registration uses that
metadata later to validate locks, channel references, and worker writes. A
static attribute's empty `IndexKey` resolves to its attribute name. An
attribute map with an empty `IndexKey` continues to index each concrete physical
key independently.

Index keys are server visibility names, so each effective key must use one index
type throughout the registry. Definitions in different flows may share that key
only when their index types agree.

### Generic step erasure

`StepDef` keeps its public opaque shape, but `DefineStep` and
`DefineStartStep` create a private generic adapter instead of storing an
unclassified `any`:

```go
// typedStepDef erases Step[IN] so differently typed steps can share []StepDef.
type typedStepDef[IN any] struct {
	step     Step[IN]
	starting bool
}

type StepDef interface {
	stepType() string
	stepInputType() reflect.Type
	stepOptions() *StepOptions
	stepValue() any
	isStarting() bool
	skipWaitFor() bool
	waitFor(Context, any) (*Wait, error)
	execute(Context, any) (*StepDecision, error)
}
```

`StepDef` is the sealed, non-generic form of `Step` for heterogeneous lists.
`typedStepDef` is the only implementation: it captures `IN` at `DefineStep` time
so `GetSteps`, movements, and execute-failure targets can share one slice type.

The adapter validates the concrete input type before invoking the typed
handler. A mismatch returns an error and never reaches application code. It
does not encode or decode protobuf values; Phase 4 performs that work before
and after calling the adapter.

Registration rejects:

- a duplicate final durable step type within one flow;
- a zero `StepDef`, nil handler, or typed-nil handler;
- more than one `DefineStartStep` entry;
- invalid step defaults or attribute locks, reporting the first illegal step
  in `GetSteps` order;
- an execute-failure target outside the same flow;
- an execute-failure target with a different input Go type;
- recursive execute-failure fallback configuration.

Zero steps and zero starting steps remain valid. A starting step is only the
default selected by `StartFlow`; it is not a separate handler kind.

The private `NoWaitFor` marker is authoritative. Registration caches
`skipWaitFor` and Phase 4 never invokes the marker's panic implementation. A
step that embeds `NoWaitFor` and also shadows `WaitFor` is still Execute-only.

Step defaults are obtained once during registration. Registered definitions
and returned option values transfer ownership to the SDK and must not be
mutated afterward.

### Registry-aware step references

Movements and execute-failure fallbacks continue to preserve their generic input
type through private adapters. Registration and Phase 4 must resolve each
reference through the current flow's step lookup.

The durable name alone is not enough. Resolution verifies that:

- the target step is registered in the current flow;
- the supplied generic input type exactly matches the registered input type;
- the movement input value is assignable to that type;
- registered target defaults, not an unregistered lookalike value, are used.

This prevents an application from constructing another Step value with the same
durable name and changing the registered handler or defaults. Phase 3 provides
the private resolver; Phase 4 applies it to movements returned by Execute and
RPC.

Conditional close decisions likewise resolve every channel through the current
flow schema. Duplicate, empty, undeclared, or wrong static/map references fail
before a WorkerService response is committed.

### Explicit Flow RPC registration

`Flow.GetRPCs` returns sealed `RPCDef` values created by `DefineRPC`. The typed
wrapper stores the bound handler, input/output types, and immutable
`RPCOptions`, then invokes the handler directly. A method is an RPC only when it
is present in this list and its signature is exactly:

```go
func(
	ctx dex.Context,
	input IN,
) (*dex.RPCResult[OUT], error)
```

`Context` must be the SDK interface, the second result must be `error`, and the
first result must be a pointer to a concrete `RPCResult[OUT]`. Value results and
defined lookalike result types are not accepted. Exported helper methods and
unregistered RPC-shaped methods are ignored.

The exported Go method name is the durable RPC name. The registry retains:

- the typed wrapper bound to the exact registered Flow receiver;
- its input and output Go types;
- its durable method name; and
- its static RPC options.

Value-receiver and pointer-receiver RPCs are supported when the supplied Flow
value exposes them. The registry retains that exact receiver so constructor
dependencies stored on the Flow remain available. A definition using a method
from another Flow or a pointer-only method unavailable on the registered value
fails Registry construction.

`*RPCResult[OUT]` implements a private erasure contract so the typed wrapper can
expose its output and next movements without exporting a non-generic result.
Handlers return `nil, err` on failure; `nil, nil` is rejected as an invalid
Worker result. RPC invocation still receives and returns concrete Go values in
Phase 3 tests; protobuf conversion and invocation state belong to Phase 4.

Package-level functions, closures, method expressions, anonymous wrappers,
duplicate methods, and nil definitions are not registrable RPCs. Registration
validates locks and selective loads against the Flow schema and validates
timeouts once. The later non-generic client accepts the same direct bound Flow
method value and resolves the registered descriptor by canonical method
identity. It does not receive or override RPC options.

### RPC Action permission projection

`RPCOptions.Action` attaches one state-dependent Action contract to a
registered RPC. `DefineAction` accepts a non-empty label, one condition, and
exactly one `ActionRequiresPermission` option. Permission keys match
`^[a-z][a-z0-9]*(?:[.-][a-z0-9]+)*$`. Roles are intentionally outside this
contract.

`WhenAttributeMatches` reuses the typed `AttributeMatch` representation and
encoding validation. It accepts one or more `AttributeMatchEqual` operands with
OR semantics. Registration rejects ordering and NotEqual operators, missing or
foreign Attributes, AttributeMaps, and non-scalar values. Supported source
types are strings, booleans, signed and unsigned integers representable by the
value codec, finite floating-point values, and named forms of those types.

When a Registry contains an Action, `SyncAttributeIndexes` includes the
internal KeywordList index `DexWorkQueuePermissions`. The name is exposed as
`WorkQueuePermissionsIndexKey` for search queries and reserved against
application Attribute names and custom IndexKeys. This first implementation is
Temporal-only and does not add an IDL capability flag.

StartFlow and SubFlow creation evaluate Actions from initial Attributes. When a
successful WaitFor, Execute, timeout-handler Execute, or RPC writes any Action
source, the Worker attaches the complete Action mapping to the top-level
response. Unrelated Attribute writes and query-only RPCs omit it.

The Server overlays business writes on its authoritative Attribute state. It
then computes the deduplicated, byte-sorted union of stored permissions and
newly matching permissions and appends it in the same Workflow Task. Missing
and deleted values do not match, but they do not remove stored permissions. An
unchanged logical set emits no Search Attribute upsert. The Server does not
persist the mapping.

Runtime Worker responses and SetAttributes requests cannot write
`DexWorkQueuePermissions` directly. SetAttributes carries the complete mapping
only when it modifies an Action source. Projection-only locks are unnecessary;
business locks still provide application isolation where required.

Once a permission matches, later state changes, empty mappings, and Action
definition changes do not remove it from that Flow execution. Continue-as-New
carries the permission history with the ordinary Attribute state. A new,
independent Flow execution starts from its initial matching permissions. A
changed Action definition is first evaluated when a later source write sends
its mapping or a new Flow starts. A Server upgrade does not reconstruct
permissions removed before the upgrade.

The Search Attribute records where an Action is or has been available. It is
Work Queue discovery data, not authorization evidence or proof of current
availability. The Action gateway must enforce the required permission and
current state from the registered RPC contract before invocation. The
projection remains eventually consistent candidate data.

### Handler lifecycle and concurrency

Registration retains the exact Flow and Step values supplied by the
application. It does not construct a handler per request and never calls
`WaitFor`, `Execute`, or an RPC while building the registry.

Phase 4 may invoke registered handlers concurrently. Flow and Step values must
therefore be immutable or concurrency-safe and must keep invocation-specific
state in `Context`, attributes, channels, or step-execution locals.

### Phase exclusions

Phase 3 does not implement:

- a gRPC server or `WorkerServiceServer`;
- protobuf request decoding or response mapping;
- concrete invocation `Context` values;
- buffered attribute, event, local, or channel commits;
- method panic recovery or worker error conversion;
- worker startup, readiness, draining, or shutdown;
- FlowService client calls or RPC request-ID retries.

Those runtime concerns remain in Phase 4 or Phase 5. Phase 3 only proves that a
valid application definition can become an immutable, type-safe opaque
Registry.

### Phase 3 exit gate

1. Heterogeneous generic steps assemble behind private typed adapters.
2. Flow RPCs and their static options are explicitly registered with `GetRPCs`.
3. Invalid names, duplicates, schema references, options, and fallbacks fail
   atomically during registration.
4. Runtime step references resolve only to registered definitions in their
   current flow.
5. No application package imports `dexpb`, and no WorkerService or client
   transport is added.

### Tests

Phase 3 uses package-internal unit tests because no WorkerService transport
exists yet. Integration coverage cannot reach the registry until Phase 4.

Add focused tests for:

1. heterogeneous steps, zero or one starting step, and flow-scoped lookups;
2. nil, empty, duplicate, and typed-nil flow and step definitions;
3. duplicate persistence names, static/map metadata, and index-key conflicts;
4. `NoWaitFor` detection without calling its panic implementation;
5. typed adapter input validation and successful WaitFor/Execute dispatch;
6. undeclared locks, invalid fallback targets, mismatched input types, and
   fallback cycles;
7. value- and pointer-receiver RPC registration, input/output type retention,
   durable method names, and typed dispatch;
8. exported helper methods are ignored; nil, duplicate, package-function,
   foreign-Flow, method-expression, closure, wrapper, and invalid-option
   definitions are rejected;
9. lookalike Step references using registered defaults, plus undeclared or
   wrong-kind channel references;
10. atomic failure without a partially usable registry;
11. stable first-error reporting when multiple steps have invalid options.

Run Phase 3 verification through the Makefile:

```bash
make -C sdk-go unitTests 2>&1 | tee /tmp/test-go-sdk-phase3.log
make copyright-check 2>&1 | tee /tmp/test-go-sdk-phase3-copyright.log
```

Phase 4 integration tests must invoke registered WaitFor, Execute, and RPC
handlers through an in-process WorkerService and verify protobuf boundaries,
buffer commit/rollback, errors, and concurrency.

### Documentation

- Keep this plan linked from [`docs/README.md`](../../README.md).
- Update [`sdk-go/README.md`](../../../sdk-go/README.md) when Phase 3 lands with
  Flow registration, starting-step, explicit RPC definitions, and concurrency rules.
- Update [`sdk-go/CONTRIBUTION.md`](../../../sdk-go/CONTRIBUTION.md) with the
  Phase 3 verification commands.
- Add `NewRegistry` construction to SDK examples with the Phase 4 Worker entry
  point.

### UI/UX

N/A: no in-repo web UI.

## Phase 4 detailed design

### Public worker surface

Phase 4 adds one application-hosted worker. It references the shared Phase 3
Registry and owns the generated WorkerService gRPC server:

```go
type Logger interface {
	Debug(msg string, keyvals ...interface{})
	Info(msg string, keyvals ...interface{})
	Warn(msg string, keyvals ...interface{})
	Error(msg string, keyvals ...interface{})
}

type WorkerOptions struct {
	BindAddress        string
	WorkerTarget       WorkerTarget
	FlowServiceAddress string
	AttributeIndexSyncTimeout time.Duration
	Logger             Logger
	Credential         Credential
	GrpcMaxMessageBytes int
}

// Credential supplies the bearer credential sent with every FlowService call.
type Credential interface {
	Token(ctx context.Context) (string, error)
}

// FileCredential reads a token file and rereads it when it changes.
func FileCredential(path string) (Credential, error)

type Worker struct {
	// unexported
}

func NewWorker(
	registry *Registry,
	cache *blobcache.Cache,
	options WorkerOptions,
) (*Worker, error)

func (worker *Worker) WorkerTarget() *WorkerTarget
func (worker *Worker) Start() error
func (worker *Worker) Stop(ctx context.Context) error
```

`NewWorker` requires the already validated Registry and BlobCache. Passing nil
for either required dependency panics. It returns option errors before opening
a listener and retains no duplicate flow, step, RPC, or schema lookup.

An empty `BindAddress` uses `:8803`. It controls only the local plaintext gRPC
listener. `WorkerTarget` is the server-reachable target normally supplied once
through `ClientOptions.WorkerTarget`. A StartFlow config may override it, and
`UpdateFlowConfig` may change it for an existing run.

An empty `WorkerTarget.Address` derives from `BindAddress`. A concrete bind host
and port are copied. An unspecified bind host such as `:8803`, `0.0.0.0:8803`,
or `[::]:8803` becomes `localhost:8803`, because a wildcard is not a dialable
advertised host. `WorkerTarget.Headless` is preserved. This default is intended
for a Worker and Dex server with matching local network reachability;
deployments with containers, pods, load balancers, or DNS must set the
advertised address explicitly.

`ClientOptions.Credential` and `WorkerOptions.Credential` attach a bearer
credential to every FlowService call. A Dex server with authentication enabled
requires it on every public API call.

Pull Workers set `WorkerOptions.Pull` with `MaxConcurrentTasks`,
`IdleStreamsPerQueue`, optional `AppWorkerID`, and `StopDrainTimeout`. They connect outbound through
`FlowServiceAddress`, using the same `Credential`, without opening the local
listener. `Worker.WorkerTarget()` returns `{Pull: true}`. A Client or per-Flow
`FlowConfig.WorkerTarget` can advertise that value; `StartFlowOptions` and
`SubFlowOptions` both carry the per-Flow override through `ConfigOverride`.
`WorkerTarget.Pull` and `WorkerTarget.Address` are mutually exclusive. The SDK
maps pull mode to `WorkerTarget.is_pull_mode`, keeps one task per stream, and
polls the sticky Worker ID queue plus each registered Flow type queue. A Flow
type must be unique within one Dex server for pull routing.
Go SDK 1.13.0 adds `Worker.WaitPullRegistered(context.Context) error` for
startup gating and `Worker.PullRegistrationState() PullRegistrationState` for
probes. Concurrent waiters use notifications. Every queue must register once
before startup readiness latches. Startup failures retain their error chain;
permanent rejections return `*PullRegistrationError{Queue, Err}`. Cancellation
and stopping before readiness return errors. After initial readiness, the wait
always succeeds. Snapshot `IsReady` changes to false only when a queue has no
registered stream and a permanent error. Successful registration clears that
queue's error. Transient disconnects and ownership changes preserve readiness.
Snapshot `Queues` entries expose `Queue`, `HasRegisteredStream`, and
`LastPermanentError`; non-pull Workers return the zero snapshot. Shutdown alone
preserves the latched snapshot, so applications withdraw stopping Workers.
`StopOnPermanentStreamRejection` defaults to false. Enabling it drains every
queue on the first permanent rejection at any time, bounds the drain with
`StopDrainTimeout`, cancels streams, waits for handlers, and returns the rejection
from `Start`. Handlers must respond to cancellation. `Stop` remains safe and
should be called before exit. The production pull Worker example gates startup
and exposes an HTTP readiness handler. The other SDKs will follow.

`FileCredential` rereads its file when the file changes, so a rotated token,
such as a projected service account token, takes effect without a restart.

`Worker.WorkerTarget()` returns a fresh copy of the resolved target for
`ClientOptions`. It does not mutate a caller-owned option value.

The bind address and WorkerTarget are not required to match. Given the shared
`registry` and `cache`, a Worker may bind every local interface while Dex dials
a headless Kubernetes service:

```go
worker, err := dex.NewWorker(
	registry,
	cache,
	dex.WorkerOptions{
		BindAddress: "0.0.0.0:8803",
		WorkerTarget: dex.WorkerTarget{
			Address:  "orders-worker.default.svc.cluster.local:8803",
			Headless: true,
		},
	},
)
if err != nil {
	return err
}

client, err := dex.NewClient(
	registry,
	cache,
	dex.ClientOptions{
		WorkerTarget: worker.WorkerTarget(),
	},
)
if err != nil {
	return err
}

runID, err := client.StartFlow(
	ctx,
	Orders,
	"order-1",
	OrderInput{},
	dex.StartFlowOptions{},
)
```

`WorkerTarget.Headless` describes how the server resolves the advertised
target; it does not change the local listener. The Worker resolves the empty
address default but never automatically registers or updates the target for a
flow. Client applies its configured default only when StartFlow assembles the
new run's FlowConfig.

An empty `FlowServiceAddress` uses `localhost:8801`. Before binding, the Worker
uses this plaintext target to synchronize the Registry's Indexed Attributes.
It also uses the target for private `LoadBlobs` calls. The synchronization
deadline defaults to two minutes when `AttributeIndexSyncTimeout` is zero.

`GrpcMaxMessageBytes` limits received messages on the WorkerService listener and
on the private FlowService connection. Zero uses `DefaultGrpcMaxMessageBytes`
(32 MiB, the Dex server default) and keeps gRPC's unlimited send limit; a
positive value also limits sent messages. Negative values are invalid. When a
`LoadBlobs` response exceeds a gRPC message limit, hydration splits the batch in
half and retries, so N uncached blobs take at most 2N-1 calls. A single blob
that does not fit returns `BlobMessageTooLargeError`.

The caller owns the shared BlobCache. Stopping the Worker does not purge or
close it. The Worker owns and closes only its private FlowService connection.

`Logger` accepts structured debug, info, warning, and error messages. Nil uses
the shared BlobCache logger, which defaults to `slog.Default`. A Worker override
applies to Worker lifecycle, hydration, cache, and recovered-panic logs.

`Start` synchronizes Indexed Attributes, binds the configured address, serves WorkerService, and blocks. It may
be called once. A normal `Stop` makes `Start` return nil; bind and serve failures
are returned. The Worker does not install signal handlers or start itself in a
goroutine.

`Stop` is idempotent. It stops accepting calls and waits for in-flight handlers.
If `ctx` expires first, it force-stops the gRPC server, cancels in-flight RPC
contexts, and returns `ctx.Err()`. Calling `Stop` before `Start` succeeds and
prevents a later start. A Worker is one-shot and cannot restart after stopping.

The public `Worker` does not implement generated request methods. An unexported
server value implements `dexpb.WorkerServiceServer`, so application code never
uses `dexpb` to host or invoke the worker.

### Runtime structure

The Worker references shared definitions and cache while owning its transport:

```text
Worker
  registry                 shared immutable Phase 3 descriptors
  blob cache               shared caller-owned storage
  workerService            generated gRPC adapter
  valueHydrator            shared cache + private FlowService LoadBlobs client
  grpcServer / listener    plaintext WorkerService transport
  lifecycle state          start, drain, force-stop, terminal error
```

Each gRPC call creates one independent invocation object. No attribute values,
condition results, locals, events, or buffered writes are retained on the
registered Flow or Step values. Calls for different flows and runs may execute
concurrently.

The private WorkerService handlers use a common pipeline:

1. validate the request envelope and resolve its flow, step, or RPC;
2. collect and hydrate every input value required by that method;
3. decode the typed handler input;
4. build the method-specific invocation Context;
5. invoke the registered handler with panic recovery;
6. validate registry-aware Wait, decision, and movement references;
7. encode the handler result and buffered mutations; and
8. return one response only after every conversion succeeds.

Any failure before step 8 discards the complete invocation buffer. Worker calls
never partially commit attributes, locals, events, or channel messages.

### Request validation and lookup

All three handlers reject a nil request or nil proto Context. WaitFor and
Execute require non-empty flow type, step type, step input, flow ID, run ID,
step-execution ID, `Attempt >= 1`, and a non-zero first-attempt timestamp. RPC
requires non-empty flow type, RPC name, input, flow ID, and run ID; its proto
does not carry step-attempt metadata.

Lookup is always scoped through the Phase 3 registry:

- an unknown flow, step, or RPC is `NotFound`;
- a WaitFor call for a registered `NoWaitFor` step is `FailedPrecondition` and
  never calls the marker's panic method;
- a malformed request or duplicate input key is `InvalidArgument`;
- a registered handler is selected only from the current flow descriptor.

The runtime never uses an application-supplied durable name to bypass the
registered descriptor. Runtime movements and conditional channel references
are resolved again before response mapping.

### Hydration and typed input decoding

Before decoding, the Worker flattens request values in deterministic request
order:

- WaitFor: step input, then attributes;
- Execute: step input, attributes, step-execution locals, channel result values;
- RPC: input, then attributes.

It calls the Phase 2 hydration seam once for that ordered list. Repeated blob
references are deduplicated without changing reconstructed request order.

The value hydrator resolves a blob as follows:

1. try `BlobCache.Get` from the required shared cache;
2. decode and validate the cached payload against the blob arm;
3. delete a missing or corrupt cache entry and treat it as a miss;
4. batch all misses into one private `FlowService.LoadBlobs` request;
5. require one concrete response of the correct arm for every requested ID;
6. use the fresh response even when cache admission rejects it; and
7. log cache read, deletion, or write errors while continuing with a valid
   fresh response.

An unavailable FlowService, missing result, wrong result kind, or response that
still contains a blob arm fails the Worker call. Cache payloads retain the Phase
2 string/object format. The runtime does not change cache capacity, eviction,
recovery, or directory-ownership semantics.

Typed step and RPC inputs are allocated from the registered `reflect.Type`,
decoded through the Phase 2 codec, and passed through the Phase 3 erased
adapter. Decode failures return errors; reflection and codec failures never
panic across the gRPC boundary.

### Invocation Context

The private invocation type implements `Context`, `attributeInvocation`, and
`channelInvocation`. It embeds the incoming gRPC context, so deadlines,
cancellation, and forced Worker shutdown propagate to application handlers.

Metadata maps directly from proto Context. Unix-second timestamps use
`time.Unix(seconds, 0)`. Step-execution IDs, lineage, first-attempt time, and
attempt are populated for WaitFor and Execute. RPC returns empty
step-execution IDs, zero `FirstAttemptAt`, and zero `Attempt` because the server
supplies no step attempt metadata for RPC.

Method-specific operations are enforced at runtime:

| Operation | WaitFor | Execute | RPC |
|---|:---:|:---:|:---:|
| attribute Get/Set/Delete | yes | yes | yes |
| channel Publish | yes | yes | yes |
| RecordEvent | yes | yes | yes |
| SetStepExecutionLocal | yes | no | no |
| GetStepExecutionLocal | no | yes | no |
| channel condition results | no | yes | no |
| timer/wait-failure helpers | no | yes | no |
| channel Size | no | no | yes |

Operations returning an error use `errInvalidInvocationContext` outside their
allowed method. `Channel.Size` has no error result, so invalid use panics; the
Worker recovers it and returns a structured method failure. Invocation values
must not be retained or used after the handler returns.

`HasTimerFired` is true when any timer result is completed.
`HasTimerFiredByIndex` uses the timer-result order supplied by the server and
returns false outside Execute or for an invalid index. Natural completion and
`SkipTimer` are intentionally indistinguishable. `WaitForMethodFailed` reads
`ConditionResults.wait_for_failed`.

### Attributes and buffered reads

Incoming attributes are validated as unique, non-empty physical keys and stored
as hydrated concrete values. Attribute operations resolve the registered
definition by logical name and verify static/map kind. The runtime uses the
registered index configuration rather than trusting a same-name lookalike
handle.

Map operations derive the physical key from the definition name and instance.
Get first observes the invocation's latest buffered write:

- a buffered Set decodes that value;
- a buffered Delete returns `*AttributeNotFoundError`;
- otherwise Get reads the hydrated request snapshot; and
- a missing key returns the typed zero value plus `*AttributeNotFoundError`.

Set and Delete encode immediately. A failed encode leaves the previous buffer
unchanged. Multiple writes to one physical key use last-write-wins and emit one
`AttributeWrite`; distinct keys retain first-write order for deterministic
responses. Deletes keep the registered index config.

### Step-execution locals and events

Execute validates incoming locals as unique non-empty keys. Get requires a
non-nil pointer and decodes the hydrated value. Missing locals return
`found=false`.

WaitFor may Set a local more than once; the last value wins and the response
uses first-write key order. Execute and RPC reject Set.

`RecordEvent` requires a non-empty name. One name may be recorded once per
method invocation; a duplicate returns an error without replacing the first
value. Events preserve call order. They remain outside `PersistenceSchema`.

### Channels and condition results

Publish resolves a registered static or map channel, encodes the value
immediately, and appends one `ChannelMessage`. Multiple publishes preserve call
order and are never coalesced.

Wait mapping validates every channel condition against the current flow schema.
A same-name static/map mismatch, empty map instance, invalid bounds, undeclared
channel, condition-ID error, or encoding error rejects the complete response.
`SkipWaitImmediately` maps to an omitted waiting condition.

Execute stores hydrated condition results in server order. A typed
`GetConditionResults` call:

- resolves the requested registered channel and physical map instance;
- selects completed channel results with that physical name;
- concatenates values in the request's channel-result order; and
- decodes each value into the channel's `T`.

Waiting results contribute no values. Duplicate results for the same concrete
channel are intentionally concatenated. A malformed status, empty channel
name, or undecodable value fails the invocation.

RPC channel sizes start from `InvokeWorkerRPCRequest.channel_infos`. Missing
entries read as zero. Each successful Publish earlier in the same RPC
increments the matching concrete size, so `Size` observes invocation-local
publishes. Empty names and negative incoming sizes are invalid. WaitFor and
Execute never receive or synthesize channel sizes.

### Streams

`DefineStream[T]` creates an immutable definition with a name and positive
`streamCapacityBytes`. The Registry requires exact definition identity, unique
persistence names within a Flow, and ownership by exactly one Flow. This lets
different Flow types use the same Stream name with separate capacity scopes.

`Stream.Write` is available only in WaitFor and Execute. It emits a
fire-and-forget frame on the current Worker response stream, so it does not make
a Worker-to-server FlowService call. One invocation may emit any number of
messages to the same or different Streams. The server attempts them in order;
write failures are observable only through server logs and metrics. Step frames
use `#stepExecutionID` as source metadata. Flow timeout and RPC writes are
rejected.

`Context.RecordHeartbeat(value)` sends a heartbeat frame on that response
stream. A nil value, including a typed nil pointer, map, slice, or interface,
sends no Value and explicitly clears backend heartbeat details. A non-nil value
uses normal Value encoding. `Context.GetLastHeartbeatValue(valuePtr)` requires a
non-nil pointer, returns false without changing it when details are absent, and
decodes the latest regular-activity checkpoint otherwise. Local-activity
heartbeats are ignored and do not reach regular fallback.

Heartbeat and Stream writes share an invocation-scoped emitter and send lock.
Sequential calls preserve order; concurrent calls enter the gRPC stream in lock
acquisition order. The first send failure is retained, cancels the handler
Context, rejects later progress, and prevents a final result even if application
code ignored the original error. The Worker closes progress before mapping and
sending the unique result frame. Calls from late goroutines return an invalid
context error, so no frame can follow the result. Applications must still stop
and join Context-using goroutines before their handler returns; other Context


`Client.WriteStream` accepts the exact registered Stream, Flow ID, non-empty
source, and typed value. Sources may repeat and may contain `#`.
`Client.ReadStream` accepts an opaque resume token, decodes the next value into
a caller pointer, and returns the next token, creation time, and source. Empty
or trimmed-away tokens resume from the retained head according to
the server contract.

`Client.ListStreamMessages` accepts the exact registered Stream, a positive
page size, an exclusive before-page token, and a pointer to
`StreamMessagesPage[T]`. It returns retained messages newest first without
waiting. An empty token starts at the retained tail. The returned next-page
token anchors the next older page and is empty at the end. A trimmed anchor
returns an empty page instead of restarting at the tail. Messages appended
after the first page do not enter that older-page chain. The server-configured
page-size limit defaults to 1000.

### WaitFor response

WaitFor invokes the registered typed handler unless `skipWaitFor` is true. A
successful response contains:

- the registry-validated waiting condition;
- buffered attribute writes;
- buffered step-execution locals;
- recorded events; and
- published channel messages.

The Worker never sets `local_activity_metadata`; that field is server-owned.

### Execute response

Execute receives condition results and source-step locals, calls the registered
typed handler, and requires a non-nil, non-empty `StepDecision`.

Every movement is resolved through `registeredFlow.resolveMovement`. Mapping
uses the registered target's immutable defaults plus the explicit movement
overrides. It rejects unregistered lookalikes, input-type mismatches, invalid
options, and worker-owned lineage fields.

Conditional close channels are resolved through the registered flow before
mapping. All other close decisions remain mutually exclusive with next steps.
The response contains the decision plus buffered attributes, events, and
channel messages. The Worker leaves `local_activity_metadata` empty and does not
emit Execute local writes.

### RPC response

RPC resolves the reflected method from the current flow, decodes its registered
input type, and invokes the exact bound receiver retained by Phase 3.

The erased `RPCResult` output is encoded directly. Optional movements use the
same registry-aware resolution as Execute. RPC cannot return a close decision
or step-execution locals. The response contains output, optional next steps,
and buffered attributes, events, and channel messages.

### Method failures and panic recovery

Every Worker failure crosses gRPC as a status with one `WorkerErrorResponse`.
The server can therefore preserve the original worker code, concrete error
type, and detail in the Client's public `WorkerInvocationError` conversion.

| Failure | gRPC code |
|---|---|
| malformed request or handler result | `InvalidArgument` |
| unknown flow, step, or RPC | `NotFound` |
| WaitFor called for `NoWaitFor` | `FailedPrecondition` |
| handler-returned gRPC status | preserve its code |
| ordinary handler error | `Unknown` |
| request cancellation/deadline | `Canceled` / `DeadlineExceeded` |
| panic or SDK invariant failure | `Internal` |

For a returned error, `WorkerErrorResponse.error_type` is its concrete Go type.
For a panic it records the recovered value type. Panic stacks are logged by the
Worker but are not returned over gRPC. A panic in application code, reflection,
or an invocation helper is recovered at the outer handler boundary after all
buffers have become unreachable.

### Concurrency and ownership

The registry and registered definitions are immutable after `NewRegistry`.
Separate invocation objects make concurrent calls independent, and the worker
implementation must pass the race detector.

Application Flow and Step values may be called concurrently and remain the
application's responsibility to make concurrency-safe. Invocation Context
mutation is synchronous handler state: applications must finish any goroutines
using it before returning.

Request protobuf values are freshly deserialized and transferred to the
invocation. The runtime does not defensively clone them. Encoded response
values and mutation buffers become owned by the response only after successful
mapping.

### Phase exclusions

Phase 4 does not implement:

- public FlowService Client methods or request-ID retries;
- automatic WorkerTarget registration or service discovery;
- TLS WorkerService or FlowService transport;
- public gRPC request/response types or a custom service registrar;
- custom value codecs or codec registries; or
- blob-cache eviction, recovery, or capacity changes.

### Phase 4 exit gate

1. Applications can construct one Registry and run a Worker from it without
   importing `dexpb`.
2. WaitFor, Execute, and RPC dispatch through the Phase 3 registry over gRPC.
3. Typed values, attributes, channels, locals, results, and decisions cross the
   proto boundary through Phase 2 codecs and registry-aware validation.
4. Successful handlers commit their complete buffer; errors and panics commit
   nothing.
5. Blob-backed request values hydrate through private LoadBlobs and the shared
   caller-owned cache before application decode.
6. Graceful and forced shutdown have deterministic, race-free behavior.
7. The Worker exposes its resolved target for flow options without making
   Client calls or updating flows automatically.
8. No public Client transport or mutable registration API is added.

### Tests

Phase 4 adds in-process WorkerService integration tests using a real gRPC
server and client. Package-internal tests may use generated stubs; application
examples and external contract tests still do not import `dexpb`.

Cover these scenarios:

1. Registry construction rejects invalid registration; Worker construction
   rejects nil dependencies and invalid target configuration. Empty target
   addresses derive from concrete and wildcard bind addresses, and Stop leaves
   the caller-owned cache usable.
2. `Start`/`Stop` lifecycle is one-shot, idempotent, and deadline-bounded.
3. WaitFor dispatch decodes typed input and maps attributes, locals, events,
   channel publishes, timers, combinations, and immediate execution.
4. Execute dispatch decodes locals and channel results, exposes timer and
   WaitFor-failure helpers, and maps every next/close decision.
5. Multiple completed conditions on one channel concatenate values in server
   order, including static and map channels.
6. Typed RPC dispatch preserves input/output, movement validation,
   attributes, events, publishes, and channel sizes including local publishes.
7. Set-then-Get observes buffered state; Delete-then-Get returns
   `*AttributeNotFoundError`; duplicate events and malformed keys fail.
8. Nil Wait/StepDecision results, returned errors, gRPC status errors, panics,
   cancellation, and mapping failures return WorkerError details and commit no
   buffered mutations.
9. Unknown flow/step/RPC, a WaitFor request for `NoWaitFor`, undeclared schema
   handles, and lookalike movement targets return the planned status codes.
10. A fake FlowService plus real disk cache covers cache hit, miss, corrupt
   payload reload, batching, deduplication, wrong kind, missing result, and
   cache failure while using a fresh result.
11. Concurrent WaitFor, Execute, and RPC calls isolate invocation state under
    the race detector; graceful stop drains an in-flight handler and deadline
    expiry cancels it.

Run Phase 4 verification through the Makefile:

```bash
make -C sdk-go unitTests 2>&1 | tee /tmp/test-go-sdk-phase4.log
make -C sdk-go workerIntegTests 2>&1 | tee /tmp/test-go-sdk-phase4-worker.log
make -C blob-cache-go blobCacheTests 2>&1 | tee /tmp/test-go-sdk-phase4-blobcache.log
make copyright-check 2>&1 | tee /tmp/test-go-sdk-phase4-copyright.log
```

`workerIntegTests` runs the WorkerService integration package with `-race`.

The default Temporal-backed end-to-end suite remains Phase 5 because Phase 4
does not yet provide the public Client calls needed to start and drive runs.

### Documentation

- Keep this plan linked from [`docs/README.md`](../../README.md).
- When Phase 4 lands, update [`sdk-go/README.md`](../../../sdk-go/README.md)
  with Worker construction, bind-versus-advertise addresses, concurrency,
  hydration, error, and shutdown semantics.
- Update [`sdk-go/CONTRIBUTION.md`](../../../sdk-go/CONTRIBUTION.md) with the
  worker integration and race verification commands.
- Update the Go examples to construct one Registry and BlobCache, pass both to
  the Worker, and show signal-driven `Stop(ctx)` without generated protobufs.
- Update [`blob-cache-go/blobcache/README.md`](../../../blob-cache-go/blobcache/README.md)
  with cache ownership and Worker hydration wiring; do not change cache policy
  documentation.

### UI/UX

N/A: no in-repo web UI.

## Phase 5 detailed design

### Contract corrections before transport

Phase 5 implements the current public Client surface, but five existing
contracts must be corrected in the same change so the transport does not encode
known inconsistencies:

1. `StartFlowOptions.Timeout == nil` maps to zero seconds. FlowService accepts
   zero as no Dex soft Flow timeout and rejects only negative values. Positive
   values continue to round up to whole seconds.
2. The current server requires `request_id` for StartFlow and every InvokeRPC.
   Only StartFlow exposes an override because its request ID may be a business
   identifier. IDs for InvokeRPC are generated and remain internal. Durable
   Step and Attribute waits accept optional overrides and otherwise derive IDs
   on the server.
3. `StepExecutionID.ExecutionNumber` remains optional. Nil means execution one.
   SkipTimer keeps the existing proto and server contract; the Client formats
   the effective step execution ID before sending it.
4. SearchFlows already returns flow type, status, start time, and close time.
   `SearchFlowEntry` must retain those fields instead of discarding them.
5. Phase 4 currently constructs a private registry per Worker and accepts an
   optional cache in WorkerOptions. Phase 5 exposes one immutable Registry and
   requires the same caller-owned BlobCache in both Worker and Client.

The existing StepExecutionID shape remains unchanged. SearchFlowEntry adds the
server fields directly:

```go
type StepExecutionID struct {
	StepType        string
	ExecutionNumber *int32
}

type SearchFlowEntry struct {
	FlowID           string
	RunID            string
	FlowType         string
	Status           FlowStatus
	StartedAt        time.Time
	ClosedAt         time.Time
	IndexedAttributes map[string]Value
}
```

`ClosedAt` is zero while a run is open. Unknown status enums and invalid proto
timestamps are response-mapping errors. `StopOptions{}` maps to cancel, matching
the existing server default.

### Shared Registry and BlobCache

Phase 5 exposes the Phase 3 registry as an opaque immutable dependency:

```go
type Registry struct {
	// unexported
}

func NewRegistry(flows []Flow) (*Registry, error)
```

`NewRegistry` performs the existing atomic Phase 3 assembly. It retains the
supplied Flow and Step values once, discovers RPC methods once, and validates
the complete cross-flow index schema once. It returns no Registry on failure.
Registry exposes no Add, Remove, lookup, or mutable map APIs.

Applications construct one Registry and one disk blob cache, then pass the same
pointers to Client and Worker:

```go
logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
registry, err := dex.NewRegistry([]dex.Flow{Orders})
if err != nil {
	return err
}
cache, err := blobcache.New(&blobcache.Config{
	Dir:      "/var/tmp/orders-dex-blobs",
	MaxBytes: 1 << 30,
	Logger:   logger,
})
if err != nil {
	return err
}

worker, err := dex.NewWorker(registry, cache, dex.WorkerOptions{Logger: logger})
if err != nil {
	return err
}
client, err := dex.NewClient(
	registry,
	cache,
	dex.ClientOptions{
		WorkerTarget: worker.WorkerTarget(),
		Logger:       logger,
	},
)
if err != nil {
	return err
}
```

There is no package-global Registry or BlobCache. Separate application groups
may construct separate pairs. A Client-only process still constructs a Registry
from the Flow definitions it uses, even though it does not invoke handlers.

Registry is safe for concurrent reads and has no close operation. The caller
owns BlobCache and closes it only after every Client and Worker using it has
closed or stopped. Client.Close and Worker.Stop never close or purge these
shared dependencies. Passing a nil Registry or BlobCache to either constructor
panics because both are required; Phase 5 has no uncached runtime mode.

### Public Client construction

Phase 5 replaces the placeholder `Client.runtime` field with references to the
shared Registry and BlobCache, one owned FlowService connection, and the
internal response hydrator:

```go
type ClientOptions struct {
	FlowServiceAddress  string
	WorkerTarget        *WorkerTarget
	Logger              Logger
	Credential          Credential
	GrpcMaxMessageBytes int
}

func NewClient(
	registry *Registry,
	cache *blobcache.Cache,
	options ClientOptions,
) (*Client, error)
func (client *Client) Close() error
```

An empty `FlowServiceAddress` uses `localhost:8801`. It is a plaintext gRPC dial
target. Phase 5 does not expose generated clients, `grpc.ClientConn`, dial
options, TLS credentials, or an HTTP URL.

`GrpcMaxMessageBytes` limits received messages on the Client connection. Zero
uses `DefaultGrpcMaxMessageBytes` (32 MiB) and keeps gRPC's unlimited send
limit; a positive value also limits sent messages. Negative values are invalid.

`WorkerTarget` is the default advertised target for every StartFlow call made
by the Client. Nil omits the default. A non-nil value requires a non-empty
Address and is copied during Client construction, including Headless, so later
caller mutation cannot change Client behavior.

`Logger` overrides logging for this Client and its hydrator. Nil inherits the
shared BlobCache logger; a nil cache logger defaults to `slog.Default`.

This follows the iWF Go SDK's client-level
[`WorkerUrl`](https://github.com/indeedeng/iwf-golang-sdk/blob/main/iwf/client_options.go)
pattern while retaining Dex's typed Headless setting.

`grpc.NewClient` is lazy, so `NewClient` validates the target and constructs the
connection but does not assert server readiness. `HealthCheck` is the explicit
readiness call.

`Close` closes only the Client's gRPC connection. It does not mutate Registry
or close BlobCache. `Close` is idempotent. Calls after Close return a stable
local error.

A Client is safe for concurrent use. Each call owns its request, request ID,
encoded values, response values, and hydration pointer list. Closing a Client
concurrently with a call may terminate that call through gRPC cancellation.

Client construction does not construct a Worker or make a FlowService call.
Applications normally pass `worker.WorkerTarget()` once through ClientOptions.
The target is advertised only when StartFlow sends a new run request.

### Common call pipeline and current-run targeting

Every public method follows the same transport boundary:

1. reject a nil context, closed Client, empty required ID, or invalid public
   definition before issuing an RPC;
2. validate and encode all request values without mutating caller-owned
   options;
3. select or generate any client-owned request ID once;
4. assemble one generated request with an empty `run_id`;
5. call the generated FlowService stub with the caller's context;
6. translate a gRPC failure using the endpoint's Flow requirement;
7. hydrate all response blob arms in one ordered batch; and
8. map and decode only after the complete response is valid.

All methods other than StartFlow target the current run for `flowID`. An empty
wire `run_id` lets the server follow Continue-as-New. TimeTravel returns the new
run ID, while StartFlow returns the created or matched run ID. The public Client
does not add a run-specific variant in Phase 5.

Local validation errors remain ordinary Go errors. Encoding and decoding errors
use `*dex.ValueMappingError`. Only errors received from FlowService become
`*dex.ServiceError` or a concrete service error wrapping it.

The Client builds each non-wait protobuf request once. gRPC's pre-commit
transparent retry therefore reuses the same request and request ID. Durable
Step and Attribute waits automatically reattach after a transport long-poll
timeout. Every reattachment sends the same logical wait and remaining caller-visible
budget, allowing the server to reuse its selected Update ID. A positive budget does
not guarantee immediate accepted-handler completion or slot release.

### Request ID ownership

Request ID generation moves out of the Phase 2 pure mappers and into the Client
entry methods. The Client generates one random UUID for:

- StartFlow, unless `StartFlowOptions.RequestID` is non-nil;
- every InvokeRPC, whether it is locking or non-locking.

A non-nil StartFlow override must be non-empty. It may be a stable business
identifier, supports a logical retry spanning separate Client calls, and is
the StartFlow request-ID override. Normal application code may instead leave it
nil for an SDK-generated UUID.

Request IDs are optional for Step completion and Attribute waits. When omitted,
the server derives a namespaced stable ID from the Step execution or encoded
Attribute condition. Reuse an override only for the same logical Step execution
or Attribute predicate. The Client preserves the logical ID across automatic
long-poll reattachments. When a previous Update using that ID completed with a
handler timeout, the server appends an increasing `-N` suffix until it can start
a new Update.

InvokeRPC uses the ID for external-value ownership and as the `IRPC`
Temporal Update ID. Durable waits use their server-selected generation ID. The
ID is not exposed in results.

### StartFlow assembly

`Client.StartFlow` uses the supplied Flow's durable type to resolve the shared
Registry descriptor. The Flow type must have been included in `NewRegistry`;
the Client reuses its schema, starting-step selection, input type, and immutable
step defaults. It does not reevaluate steps, persistence, or RPC methods.

StartFlow clones `StartFlowOptions.ConfigOverride` before mapping it. A non-nil
per-call `ConfigOverride.WorkerTarget` wins. Otherwise the Client's default
WorkerTarget is inserted, creating a FlowConfig when necessary. If neither is
set, the request omits WorkerTarget. Other per-call FlowConfig fields are
preserved, and UpdateFlowConfig never inherits the Client default.

When a starting step is declared:

- `input` must be assignable to that step's registered input type;
- the Client encodes the input and uses the registered durable step name; and
- the Client maps the starting step's `GetStepOptions` defaults.

When no starting step is declared, `input` must be nil and the Client omits the
step type, input, and step options. This preserves flows that begin with RPC
only and later move into a step.

`StartFlowOptions.Timeout == nil` sends zero, meaning no Dex soft Flow timeout.
A non-nil duration must be non-negative; positive sub-second values round up.
`StartDelay == nil` sends zero. A non-nil start delay must also be
non-negative and rounds up when positive.

Initial attributes are already constructed through `InitialAttribute` and
`InitialAttributeMapValue`. Request assembly revalidates their physical names,
encoded concrete values, index configuration, and uniqueness. Flow config,
retry, reuse, and already-started options use the Phase 2 mappers.

The response must contain a non-empty run ID. `AlreadyStarted.IgnoreError`
retains server behavior: the same flow ID returns the existing run only when
the server accepts that option. The request ID override does not by itself
change already-started behavior.

Typical local Worker wiring remains explicit:

```go
worker, err := dex.NewWorker(registry, cache, dex.WorkerOptions{})
if err != nil {
	return err
}
client, err := dex.NewClient(
	registry,
	cache,
	dex.ClientOptions{
		WorkerTarget: worker.WorkerTarget(),
	},
)
if err != nil {
	return err
}
defer client.Close()

runID, err := client.StartFlow(
	ctx,
	Orders,
	"order-1",
	OrderInput{OrderID: "order-1"},
	dex.StartFlowOptions{},
)
```

### Flow state I/O

The Client does not expose direct Attribute, AttributeMap, Channel, or ChannelMap
reads and writes. Applications define typed Flow RPCs for state snapshots,
mutations, message publication, pending-message reads, and deletion. RPC handlers
use the registered persistence definitions and can request transactional locks.

WaitForAttributeMatch and WaitForAttributeMapInstanceMatch remain blocking
observation APIs. They do not provide general snapshot reads or mutations.

### RPC invocation

InvokeRPC accepts a direct bound Flow method. The shared Registry matches its
identity and signature to the descriptor created from `GetRPCs`, which supplies
the durable method name, IN/OUT types, and static `RPCOptions`. The Client
neither invokes the method nor accepts invocation-specific RPC options.

Before transport, the Client:

- rejects package functions, wrappers, unbound method expressions, and invalid
  RPC signatures;
- verifies that `input` is assignable to the method's IN type;
- requires `outputPtr` to be a non-nil pointer compatible with OUT;
- uses the descriptor's validated locks and selective loads; and
- encodes the input and generates one request ID.

The response output is hydrated before it is decoded into `outputPtr`. A nil or
empty output Value is a response error. The Client can validate method shape
and value types, but it cannot prove that the remote Worker registered that
bound method for the target flow ID; that failure returns the server's Worker
error.

At Registry construction, RPC timeout zero keeps the server default, positive
values round up, and negative values fail. Lock ordering is preserved and
duplicate physical locks are rejected. The caller's context still controls its
own wait and cancellation. RPC next-step movements remain entirely worker-side
and do not appear in the public invocation response.

### Wait, lifecycle, and administrative operations

WaitForAttributeMatch and WaitForAttributeMapInstanceMatch resolve the definition
and encode the typed match operand. The map form requires an instance. The
matched current value is decoded into the output
pointer. String and bool support equality operators. Integer and double support
all six operators. Object, bytes, null, non-finite double, and invalid ordering
fail locally before transport.

WaitForStepCompletion requires a non-empty step type. A nil execution number
defaults to one; a non-nil value must be positive. Its wire execution number
remains decimal text because that is the server contract. Both wait option
types accept an optional Request ID override. Otherwise the server derives a
stable logical ID from the Step execution or Attribute condition.
`RequestTimeout` is the total caller-visible SDK budget. Zero waits indefinitely;
positive values must be whole seconds within int32 range. Each transport attempt
uses the remaining budget as its local deadline. The Client retries internal
long-poll timeouts with the same logical Request ID without resetting the budget.
Expiry returns `*dex.RequestTimeoutError`. `context.Context` remains an
independent local cancellation mechanism, and the earlier boundary wins.

`InternalHandlerTimeout` controls one accepted Temporal Update handler generation.
Temporal permits 10 in-flight Updates and 2,000 total Updates in History per
Workflow Execution. Request expiry or context cancellation can leave an accepted
handler in flight. Use a positive value only when abandoned waits can approach
the concurrent limit. When it expires, the service completes the old handler and
continues an active call with a new `-N` generation without returning a handler
timeout. This does not expand the concurrent limit. Each generation counts toward
the history limit, so prefer a value longer than normal request deadlines and
reconnect gaps. Transport reattachment alone does not create another generation.

WaitForFlow uses the same server-capped duration. A successful response maps
status and error metadata, then hydrates every requested completion output
before returning. `NeedsResults=false` never requires result decoding. A long-poll timeout returns
`*dex.LongPollTimeoutError` with `DeadlineExceeded` and the Flow ID. Every closed
Flow returns `dex.FlowResult` with its status, error metadata, and requested
completions.
`DecodeSingleOutput` decodes only when exactly one completion exists; zero or
multiple completions return a local contract error. Callers handling parallel
branches select by `StepType` or `StepExecutionID`, not slice position.

ReadStream follows the same rule: the server caps the protocol duration, while
`context.WithTimeout` or `context.WithDeadline` can shorten the call. The SDK
does not duplicate a Go context deadline in an options field.

SkipTimer requires a non-empty step type and exactly one TimerID selector: a
non-empty condition ID or a non-negative index. A nil execution number defaults
to one; a non-nil value must be positive. The Client formats the existing wire
`step_execution_id` as `<stepType>-<effectiveExecutionNumber>` without changing
the proto or server. Natural completion and a successful skip remain
indistinguishable inside Execute.

The remaining calls map as follows:

| Client method | Phase 5 behavior |
|---|---|
| `StopFlow` | zero type cancels; terminate/fail preserve the optional reason |
| `SearchFlows` | validates non-negative page size and maps full flow metadata |
| `TimeTravel` | validates the field required by the selected time travel type and returns a non-empty new run ID |
| `UpdateFlowConfig` | maps a partial config and preserves pointer presence |
| `TriggerContinueAsNew` | signals the current run |
| `HealthCheck` | maps the returned health record; it is also the explicit readiness check |

Time travel validation is mutually exclusive: history ID must be positive, history
time must be non-zero, and step type or step execution ID must be non-empty for
their respective modes. Fields unrelated to the selected time travel mode must be
zero so impossible proto combinations fail before transport.

SearchFlows passes query text and page token through unchanged. SearchFlowsWithOptions
also forwards IncludeContinuedAsNew; the Server excludes earlier Continue-as-New
runs by default. Page size zero
keeps the server default. Search attributes are hydrated before being wrapped
as opaque Values.

### Response hydration

The Client reuses the internal `valueHydrator` and `valueHydratorImpl` behavior
from Phase 4 with its own FlowService stub and the shared required cache.
Hydration remains private; applications never call LoadBlobs or see a blob arm.

Each public response collects all Value pointers in deterministic response
order and calls `HydrateValuesInPlace` once:

- InvokeRPC: output;
- WaitForFlow: step completion outputs; and
- SearchFlows: every entry's Indexed Attributes.

Repeated blob IDs are deduplicated inside that call. A response without blob
arms makes no LoadBlobs RPC. Cached payload validation, corrupt-entry deletion,
miss batching, response-kind validation, and admission behavior remain the
Phase 4 contract.

Hydration errors are classified at the boundary that owns the call. A remote
LoadBlobs status becomes `*dex.ServiceError` for a public Client call. A malformed,
missing, wrong-kind, or still-blob response remains a local SDK response error.
The Worker retains its WorkerError classification. The shared hydrator does not
expose Worker-specific errors to Client applications.

No partially hydrated public result is returned. Opaque `Value` instances hold
only validated concrete representations, and `Value.Decode` never performs
network I/O.

### Error conversion and cancellation

Every generated FlowService error passes through the service error translator
exactly once. It preserves operation, Flow ID, gRPC code, Dex substatus,
detail, original Worker error, and the gRPC cause. Unknown, absent, or malformed
Dex details map to `ServiceError` with `ErrorSubStatusUncategorized`.

Caller cancellation and deadlines propagate through gRPC. If gRPC returns
their status, the Client exposes `*dex.ServiceError` with `Canceled` or
`DeadlineExceeded`. A context already done before request assembly returns its
context error locally and sends no RPC.

The Client never converts definition, codec, reflection, option, response-shape,
or closed Client failures into fake gRPC statuses. Matching
`*dex.ServiceError` therefore distinguishes remote service failures from local
SDK errors.

### Integration suite migration

The existing `sdk-go/integ` suite targets the removed Workflow/State HTTP
worker and generated proto types. Phase 5 replaces it with an external-package
suite built entirely on Flow, Step, Worker, Client, Attribute, and Channel.
Legacy tests are not adapted through compatibility aliases.

The migrated suite creates one Registry and BlobCache, passes both to a real Go
Worker and Client, and closes the cache after both consumers. The Worker uses a
dynamically allocated local port and advertises its reachable WorkerTarget.
Every test uses a unique flow ID. Polling and long-poll APIs replace sleeps used
for convergence.

The test target builds `dexcli` from the current checkout and uses `dexcli dev`
to run Dex plus a local Temporal server. Proto, server validation, SDK, and
integration tests therefore remain one atomic change. The suite waits on
Client.HealthCheck and Temporal search attribute setup before running.

The obsolete `sdk-go/dextest` mocks and Gin-based worker harness are deleted.
Application projects that need mocks define a narrow interface around the
Client methods they consume. Dependencies used only by those obsolete trees
are removed from `sdk-go/go.mod`.

### Phase exclusions

Phase 5 does not add:

- public generated protobuf or gRPC client access;
- run-specific overloads for current-run methods;
- GetFlowSummary, history, state-dump, or other web/debug FlowService APIs;
- public LoadBlobs or cache mutation methods;
- semantic retries that may replay signal-backed operations;
- TLS, authentication, custom dial options, or service discovery;
- custom value codecs or codec registries;
- package-global Registry or BlobCache state;
- mutable registration APIs; or
- a compatibility Client, WorkerService, Workflow, or State API.

Those surfaces require separate product and security design.

### Phase 5 exit gate

1. Application code can construct one Registry and BlobCache, share them with
   Client and Worker, and drive a typed Flow without importing `dexpb`.
2. Every approved Phase 1 Client method performs a real FlowService RPC or a
   documented local no-op.
3. Starting-step input/options, definitions, values, enums, durations, and
   results cross the boundary through the Phase 2 codec and Phase 3 metadata.
4. Required request IDs are selected once per logical call and reused by any
   transparent retry. StartFlow and durable waits accept caller-supplied
   overrides.
5. Blob-backed Client and Worker values hydrate privately through LoadBlobs and
   their shared cache before application decode.
6. Remote failures preserve Dex and Worker details; local misuse remains a
   local Go error.
7. The migrated Go end-to-end suite passes against the current
   Temporal-backed Dex server without old SDK vocabulary or generated imports.

### Tests

Phase 5 adds real-gRPC Client integration tests with an in-process fake
FlowService. These exercise transport branches that do not need Temporal:

1. NewRegistry atomic validation; NewClient required dependencies, address and
   WorkerTarget validation, defensive option copying, concurrent calls,
   idempotent Close, calls after Close, and a cache usable after Close.
2. StartFlow registry reuse without reevaluating steps, persistence, or RPC
   methods; starting/no-start flows, nil/incompatible input, step defaults,
   optional durations, initial attributes, Client WorkerTarget injection,
   per-call precedence and omission, and request-ID override/generation.
3. Direct bound RPC identity, IN/OUT validation, locking and non-locking request
   IDs, lock mapping, output hydration, and Worker error conversion.
4. Wait time rounding, immediate checks, default execution number, existing
   SkipTimer mapping, time-travel-mode validation, cancel default, config updates,
   SearchFlows metadata, and HealthCheck.
5. gRPC status with and without Dex details, caller cancellation, long-poll
   timeout, malformed responses, and local errors that remain unwrapped.
6. Client response hydration with cache hit, miss, corrupt entry, duplicate blob
   ID, missing result, wrong kind, LoadBlobs status, and no-blob fast path.

The rewritten `sdk-go/integ` suite migrates the former iWF Go SDK scenarios:

1. basic transitions, execute-only steps, retry defaults, duplicate IDs, and
   reuse after an abnormal exit;
2. WaitFor and Execute failure policies, method timeouts, force-fail, flow
   timeout, cancel, and explicit fail;
3. channel publication from clients, RPCs, and parallel steps, including typed
   results, combinations, natural timers, and SkipTimer;
4. flows with no starting step or no steps, plus reflected RPC movements and
   Worker error preservation;
5. initial static/map attributes, buffered persistence, indexed types, batch
   reads, and eventual SearchFlows visibility; and
6. one Registry and BlobCache shared by the public Client and Worker without
   generated protobuf imports.

Run Phase 5 verification through the Makefile:

```bash
make -C sdk-go unitTests 2>&1 | tee /tmp/test-go-sdk-phase5.log
make -C sdk-go clientIntegTests 2>&1 | tee /tmp/test-go-sdk-phase5-client.log
make -C sdk-go workerIntegTests 2>&1 | tee /tmp/test-go-sdk-phase5-worker.log
make -C blob-cache-go blobCacheTests 2>&1 | tee /tmp/test-go-sdk-phase5-blobcache.log
make -C sdk-go e2eTests 2>&1 | tee /tmp/test-go-sdk-phase5-e2e.log
make copyright-check 2>&1 | tee /tmp/test-go-sdk-phase5-copyright.log
```

`clientIntegTests` and `workerIntegTests` run with the race detector.
`e2eTests` owns the current-checkout `dexcli` build, startup,
readiness, test execution, failure logs, and cleanup. GitHub Actions runs all
five SDK targets and uploads Temporal/Dex logs on an E2E failure.

### Documentation

- Keep this plan linked from [`docs/README.md`](../../README.md) and mark all
  five phases implemented only after the exit gate passes.
- Update [`sdk-go/README.md`](../../../sdk-go/README.md) with Registry and cache
  construction, Client-level WorkerTarget defaults, current-run targeting,
  request-ID ownership, starting-step behavior, hydration, errors, concurrency,
  and Close.
- Update [`sdk-go/CONTRIBUTION.md`](../../../sdk-go/CONTRIBUTION.md) with Client
  integration and Temporal end-to-end commands and environment prerequisites.
- Update the Go examples to construct one Registry and BlobCache, pass both to
  Client and Worker, configure WorkerTarget once on Client, close them in
  ownership order, and demonstrate every Client API including initial
  attributes.
- Update [`blob-cache-go/blobcache/README.md`](../../../blob-cache-go/blobcache/README.md)
  to state that Client and Worker receive the same caller-owned cache.
- Remove documentation for the legacy HTTP worker, generated application
  imports, Workflow/State, and the obsolete `dextest` package.

### UI/UX

N/A: no in-repo web UI.

## Phase 1 detailed design

### Design rules

1. Application code imports `dex`, not `gen/dexpb`.
2. Flow and step names default to Go type names without package or pointer.
   Attributes and channels use explicit strings; RPCs use Flow method names.
3. Typed attributes and channels are immutable and safe as package variables.
4. Methods that use invocation state take `Context` explicitly. Flow, step, and
   RPC implementations must not retain a current invocation.
5. Dynamic encoding failures return errors; they do not panic.
6. Constructors and unexported fields protect server invariants. Do not expose
   structs that permit impossible proto combinations.
7. Durations use `time.Duration`. A later mapper converts them to whole seconds,
   rounding positive sub-second remainders up so the SDK never shortens a
   requested timeout.
8. No public compatibility types retain the old SDK vocabulary.

### Package surface

Phase 1 owns these conceptual files under `sdk-go/dex/`:

```text
flow.go             Flow and persistence declaration
step.go             Step, StepDef, None, defaults, and NoWaitFor
context.go          Context, invocation metadata, locals, and events
attribute.go        Attribute, AttributeMap, indexing, invocation operations
channel.go          Channel, ChannelMap, publish, size, bounded conditions
condition.go        Wait, timers, and combinations
decision.go         StepMovement, StepDecision, CloseDecision helpers
options.go          retry, durability, step, RPC, start, and flow options
rpc.go              RPC, RPCDef, DefineRPC, and RPCResult
client.go           Client façade declarations and public result types
errors.go           public error model
```

This is a logical split, not permission to add runtime implementations during
Phase 1.

### Flow and durable names

Application code implements a minimal flow interface:

```go
type Flow interface {
	GetFlowType() string
	GetSteps() []StepDef
	GetRPCs() []RPCDef
	GetPersistenceSchema() PersistenceSchema
}
```

Embedding `FlowDefaults` makes `GetFlowType` return empty, selecting the
Go type name without its package or pointer, such as `OrderFlow`. An
explicit non-empty result overrides that default. Registration of a flow
together with its heterogeneous steps and RPCs belongs to Phase 3.
`GetSteps` supplies every step through an opaque `StepDef`. `GetRPCs` supplies
every synchronous RPC and its static execution options through an opaque
`RPCDef`. Embedding `FlowDefaults` returns nil for both lists unless the Flow
overrides them. Generic handler adapters remain internal implementation details.

A flow declares at most one starting step with `DefineStartStep`. Other
steps use `DefineStep`. A flow without a starting step starts with no step,
matching dex-base and the current server contract.

### Step handlers

Every application step implements the same `Step[IN]` interface:

```go
type Step[IN any] interface {
	GetStepType() string
	GetStepOptions() *StepOptions
	WaitFor(ctx Context, input IN) (*Wait, error)
	Execute(ctx Context, input IN) (*StepDecision, error)
}

type none struct{}
type None = *none

type StepDef interface {
	// unexported
}

func DefineStep[IN any](step Step[IN]) StepDef
func DefineStartStep[IN any](step Step[IN]) StepDef

type NoWaitFor[IN any] struct{}

type DefaultStepType struct{}

type DefaultStepOptions struct {
	DefaultStepType
}

type StepDefaults struct {
	DefaultStepOptions
}

type StepDefaultsNoWaitFor[IN any] struct {
	StepDefaults
	NoWaitFor[IN]
}

func (NoWaitFor[IN]) WaitFor(Context, IN) (*Wait, error) {
	panic("NoWaitFor: framework must skip WaitFor")
}

func (NoWaitFor[IN]) noWaitFor() {}

func (DefaultStepOptions) GetStepOptions() *StepOptions {
	return nil
}

func (DefaultStepType) GetStepType() string {
	return ""
}
```

`None` is a pointer alias for a Step, RPC, or Channel with no application
payload. Applications pass `nil`; the unexported pointed-to type prevents
constructing a non-nil payload and preserves compile-time rejection unlike
`any`.

An Execute-only step embeds `StepDefaultsNoWaitFor[IN]`, or embeds
`NoWaitFor[IN]` and implements its type and options methods. `NoWaitFor`
supplies the interface method and carries an unexported marker. Phase 3
registration detects that marker and sets `skip_wait_for`; it never calls the
supplied `WaitFor` method. There is no public `SkipWaitFor` field.

A waiting step embeds `StepDefaults` and implements `WaitFor` and `Execute`.
`StepDefaults` contains only `DefaultStepOptions`, so it never supplies or
skips `WaitFor`. A custom-options waiting step embeds `DefaultStepType` and
implements `GetStepOptions` directly. An explicit non-empty `GetStepType`
result overrides the default step type, the Go type name without its package.

`DefineStep` and `DefineStartStep` retain the handler's input type while
building the heterogeneous `GetSteps` result. Phase 3 validates duplicate step
types and rejects flows with multiple starting steps.

`GetStepOptions() == nil` uses server defaults. A non-nil value supplies
immutable defaults whenever the step is scheduled. The starting step uses those
options directly; movement options may override them field by field.

Example:

```go
type ApproveOrderStep struct {
	dex.StepDefaults
}

func (ApproveOrderStep) WaitFor(
	ctx dex.Context,
	input ApproveOrderInput,
) (*dex.Wait, error) {
	return dex.AnyOf(
		ApprovalChannel.ForOne(),
		dex.Timer(
			input.Timeout,
			dex.WithConditionID("approval-timeout"),
		),
	), nil
}

func (ApproveOrderStep) Execute(
	ctx dex.Context,
	input ApproveOrderInput,
) (*dex.StepDecision, error) {
	if ctx.HasTimerFired() {
		return dex.ForceFail("approval timed out"), nil
	}
	approvals, err := ApprovalChannel.GetConditionResults(ctx)
	if err != nil {
		return nil, err
	}
	_ = approvals
	return dex.GoTo(ShipOrder, ShipOrderInput{
		OrderID: input.OrderID,
	}), nil
}

var ApproveOrder = ApproveOrderStep{}
var _ dex.Step[ApproveOrderInput] = ApproveOrder

type ShipOrderStep struct {
	dex.StepDefaultsNoWaitFor[ShipOrderInput]
}

func (ShipOrderStep) Execute(
	ctx dex.Context,
	input ShipOrderInput,
) (*dex.StepDecision, error) {
	return dex.DeadEnd(), nil
}

var ShipOrder = ShipOrderStep{}
var _ dex.Step[ShipOrderInput] = ShipOrder
```

### Invocation Context

`Context` embeds the request context and exposes current proto context using
Go-idiomatic names and time values:

```go
type Context interface {
	context.Context

	FlowID() string
	RunID() string
	FlowStartedAt() time.Time
	StepExecutionID() string
	FromStepExecutionID() string
	RecoveryError() *RecoveryErrorInfo
	FirstAttemptAt() time.Time
	Attempt() int32

	HasTimerFired() bool
	HasTimerFiredByIndex(index int) bool
	WaitForMethodFailed() bool
	RecordHeartbeat(value any) error
	GetLastHeartbeatValue(valuePtr any) (found bool, err error)

	SetStepExecutionLocal(key string, value any) error
	GetStepExecutionLocal(key string, valuePtr any) (found bool, err error)
	RecordEvent(name string, value any) error
}
```

Semantics:

- `StepExecutionID` and `FromStepExecutionID` are empty for RPC invocations.
- `HasTimerFired` is true when any timer completed. A timer completed through
  `SkipTimer` counts as fired.
- `HasTimerFiredByIndex` uses the timer declaration order in WaitFor. It returns
  false when the index is out of range.
- Both timer helpers return false outside Execute or when Execute skipped
  WaitFor.
- `WaitForMethodFailed` is true only when Execute follows a failed WaitFor under
  `ProceedOnFailure`.
- `RecordHeartbeat` is available in WaitFor and Execute. Flow timeout and RPC
  invocation contexts reject it. `GetLastHeartbeatValue` is also available to
  Flow timeout handlers; RPC invocation contexts reject it.
- Step-method `Attempt` starts at one. RPC returns zero `Attempt` and a zero
  `FirstAttemptAt` because its request carries no attempt metadata.
- writes, events, channel deletions, and channel publishes are buffered until the method returns
  successfully;
- attribute reads observe earlier writes in the same invocation.

Step-execution locals deliberately do not carry a Go type:

```go
if err := ctx.SetStepExecutionLocal("snapshot", snapshot); err != nil {
	return nil, err
}

var snapshot OrderSnapshot
found, err := ctx.GetStepExecutionLocal("snapshot", &snapshot)
if err != nil {
	return nil, err
}
```

`valuePtr` must be a non-nil pointer. `SetStepExecutionLocal` is valid in
WaitFor; `GetStepExecutionLocal` is valid in Execute for the same step
execution. Locals are unavailable to RPC.

`RecordEvent` accepts arbitrary data because events are not read through the
SDK. An event name may be recorded once per method invocation. Events do not
belong to `PersistenceSchema`.

### PersistenceSchema

Persistence remains the combination of attributes and channels:

```go
type AttributeDef interface {
	attributeName() string
	attributeIndex() *AttributeIndex
	attributeIsMap() bool
}

type ChannelDef interface {
	channelName() string
	channelIsMap() bool
}

type PersistenceSchema struct {
	Attributes []AttributeDef
	Channels   []ChannelDef
}
```

`AttributeDef` and `ChannelDef` are sealed, erased interfaces
implemented by the generic definitions below. Unexported methods keep
third-party types from satisfying them. A flow declares both static and
map definitions in this schema. Concrete `Attribute` / `Channel` values still
expose public `AttributeName` / `ChannelName` for application use.

Step-execution locals and record events are not flow persistence definitions:
locals have one step-execution scope, while events are history annotations.

### Typed attributes

Definitions are immutable:

```go
type Attribute[T any] struct {
	// unexported
}

type AttributeMap[T any] struct {
	// unexported
}

func DefineAttribute[T any](
	key string,
	options ...AttributeOption,
) Attribute[T]

func DefineAttributeMap[T any](
	name string,
	options ...AttributeOption,
) AttributeMap[T]
```

`AttributeMap` maps an application instance key to one flat server attribute
key. Slash is prohibited in instance keys because it is a reserved character.
Physical keys are internal SDK details.

Invocation operations take `Context` explicitly:

```go
func (a Attribute[T]) Get(ctx Context) (value T, err error)
func (a Attribute[T]) Set(ctx Context, value T) error
func (a Attribute[T]) Delete(ctx Context) error

func (a AttributeMap[T]) Get(
	ctx Context,
	instance string,
) (value T, err error)
func (a AttributeMap[T]) Set(
	ctx Context,
	instance string,
	value T,
) error
func (a AttributeMap[T]) Delete(ctx Context, instance string) error
```

A missing invocation attribute returns its typed zero value plus
`*AttributeNotFoundError`; callers use `errors.As` when absence is expected.
`Delete` maps to the proto null arm. A delete retains the definition's index
configuration so an indexed value is also removed from visibility.

Indexing is opt-in:

```go
type IndexType uint8

const (
	IndexKeyword IndexType = iota + 1
	IndexFullText
	IndexKeywordArray
	IndexInt
	IndexDouble
	IndexBool
	IndexDatetime
)

type AttributeIndex struct {
	Type     IndexType
	IndexKey string
}

func Indexed(index AttributeIndex) AttributeOption
func SyncToAttributeStore() AttributeOption
func HardDeleteOnNull() AttributeOption
```

An empty `IndexKey` uses the concrete attribute key. A non-empty key supports
dynamic attributes that write to a shared visibility key. Phase 2 validates
that encoded values are compatible with the selected index type.

`SyncToAttributeStore` marks every write from the definition for asynchronous
latest-state projection to the Flow's selected Attribute Store. Deletes project
SQL `NULL`; Store failures do not roll back Flow Attribute writes.

`HardDeleteOnNull` marks every write from the definition with
`is_hard_delete_on_null`. The Server stores string and object values only in its
blob store, at one object per Flow and Attribute key, and deletes that object on
a null write. Registration rejects numeric and Boolean value types and the
combination with `Indexed` or `SyncToAttributeStore`; Action conditions and
`WaitForAttributeMatch` reject the Attribute. Hard-delete writes require Server
protocol 3: the Worker checks its negotiated protocol before returning a result,
and the Client negotiates lazily before a `StartFlow` with hard-delete initial
values, returning `HardDeleteAttributeUnsupportedError` otherwise.

### Typed channels

Static and map channel definitions are immutable:

```go
type Channel[T any] struct {
	// unexported
}

type ChannelMap[T any] struct {
	// unexported
}

func DefineChannel[T any](name string) Channel[T]
func DefineChannelMap[T any](name string) ChannelMap[T]
```

Publishing uses invocation state, while wait construction does not:

```go
func (c Channel[T]) Publish(ctx Context, value T) error
func (c ChannelMap[T]) Publish(ctx Context, instance string, value T) error

func (c Channel[T]) ForOne(options ...ConditionOption) Condition
func (c Channel[T]) ForN(count int, options ...ConditionOption) Condition
func (c Channel[T]) AtLeast(count int, options ...ConditionOption) Condition
func (c Channel[T]) AtMost(count int, options ...ConditionOption) Condition
func (c Channel[T]) AtLeastAtMost(
	atLeast int,
	atMost int,
	options ...ConditionOption,
) Condition

func (c ChannelMap[T]) ForOne(
	instance string,
	options ...ConditionOption,
) Condition
func (c ChannelMap[T]) ForN(
	instance string,
	count int,
	options ...ConditionOption,
) Condition
func (c ChannelMap[T]) AtLeast(
	instance string,
	count int,
	options ...ConditionOption,
) Condition
func (c ChannelMap[T]) AtMost(
	instance string,
	count int,
	options ...ConditionOption,
) Condition
func (c ChannelMap[T]) AtLeastAtMost(
	instance string,
	atLeast int,
	atMost int,
	options ...ConditionOption,
) Condition
```

Mapping:

| SDK condition | `at_least` | `at_most` |
|---|---:|---:|
| `ForOne()` | omit | omit |
| `ForN(n)` | n | n |
| `AtLeast(n)` | n | omit |
| `AtMost(n)` | omit | n |
| `AtLeastAtMost(lo, hi)` | lo | hi |

Counts must be non-negative and an upper bound cannot be below the lower bound.
`AtMost(0)` is valid and completes without consuming a message.

Channel size and condition results:

```go
func (c Channel[T]) Size(ctx Context) int
func (c ChannelMap[T]) Size(ctx Context, instance string) int

func (c Channel[T]) GetConditionResults(ctx Context) ([]T, error)
func (c ChannelMap[T]) GetConditionResults(
	ctx Context,
	instance string,
) ([]T, error)
```

The intended usage is invocation-specific:

```go
size := OrderCommands.Size(ctx)
orderSize := CommandsByOrder.Size(ctx, "order-1")

commands, err := OrderCommands.GetConditionResults(ctx)
orderCommands, err := CommandsByOrder.GetConditionResults(ctx, "order-1")
```

`Size` starts from `InvokeWorkerRPCRequest.channel_infos` and includes messages
published earlier in the current RPC invocation. Calling it from WaitFor or
Execute is a programming error detected from `ctx`. There is no
`Context.ChannelSize` or raw channel-name API.

`GetConditionResults` is Execute-only and decodes consumed values directly to
`T`. When multiple completed conditions reference the same concrete channel,
their values are concatenated in channel-condition declaration order. The raw
proto-shaped condition result types remain internal.

Conditional close decisions take `Channel[T]` values directly through the
erased `[]ChannelDef` slice. Callers do not construct physical channel
references.

### Waiting conditions

```go
type Wait struct {
	// unexported
}

type Condition interface {
	condition()
}

type ConditionOption interface {
	applyCondition(*conditionValue)
}

func SkipWaitImmediately() *Wait
func Until(condition Condition) *Wait
func AllOf(conditions ...Condition) *Wait
func AnyOf(conditions ...Condition) *Wait
func Combo(conditions ...Condition) ConditionCombination
func AnyComboOf(combinations ...ConditionCombination) *Wait
func Timer(
	duration time.Duration,
	options ...ConditionOption,
) Condition
```

Channel condition IDs are optional for `Until`, `AllOf`, and `AnyOf`:

```go
func WithConditionID(conditionID string) ConditionOption
```

`Condition` is an actual wait condition. `ConditionOption` only configures a
timer or channel condition, so an option cannot be passed directly to `AllOf`,
`AnyOf`, or `Combo`.

`AllOf` and `AnyOf` serialize unnamed conditions with an empty ID.
`AnyComboOf` requires every Condition to have a non-empty user-provided ID.
The same Condition value may be reused across combinations; distinct Conditions
must not share an ID. Explicit IDs also remain useful for timer skipping.

Channel values are read through the strongly typed channel handles.
`Context.HasTimerFired` and `HasTimerFiredByIndex` expose timer completion
without exporting proto-shaped condition result types.

### Step movements and close decisions

`StepMovement`, `StepDecision`, and `CloseDecision` have unexported fields.
Helpers construct only combinations accepted by the current server:

```go
type StepMovement struct {
	// unexported
}

func GoTo[IN any](
	step Step[IN],
	input IN,
	options ...StepMoveOption,
) *StepDecision

func MovementOf[IN any](
	step Step[IN],
	input IN,
	options ...StepMoveOption,
) StepMovement

func GoToMany(movements ...StepMovement) *StepDecision
func GracefulComplete(output any) *StepDecision
func ForceComplete(output any) *StepDecision
func ForceFail(reason string) *StepDecision
func DeadEnd() *StepDecision

func ForceCompleteIfChannelsEmpty(
	output any,
	channels []ChannelDef,
	otherwise ...StepMovement,
) *StepDecision

type StepSelector interface {
	GetStepType() string
	GetStepOptions() *StepOptions
}

func (decision *StepDecision) CancelSteps(
	steps ...StepSelector,
) *StepDecision

func (decision *StepDecision) CancelSiblingSteps(
	steps ...StepSelector,
) *StepDecision
```

Rules enforced by construction or Phase 3 validation:

- `GoTo` constructs one movement and `GoToMany` requires at least one valid
  movement.
- graceful complete, force complete, force fail, and dead end cannot have next
  steps;
- force fail accepts only a string reason;
- dead end has no output;
- conditional force-complete requires unique non-empty channels and at least one
  fallback movement;
- normal movements never expose the server-owned lineage field.

An RPC may return optional next-step movements but cannot close the flow.
Execute must return a valid, non-nil, non-empty `StepDecision`.

Cancellation selectors are resolved after the successful execution commits and
before its next Steps are queued. `CancelSteps` selects the current Flow;
`CancelSiblingSteps` additionally requires matching
`Context.FromStepExecutionID()`. Repeated calls form a stable-order union, and a
Flow-wide selector supersedes the same sibling selector. Registered identity is
validated when the Worker maps the result.

### Step options

Public option types mirror server semantics without exposing proto enum names:

```go
type RetryPolicy struct {
	InitialInterval    time.Duration
	BackoffCoefficient float64
	MaximumInterval    time.Duration
	MaximumAttempts    int32
	TotalDuration      time.Duration
}

type StepDurability uint8

const (
	StepDurabilityDefault StepDurability = iota
	StepDurabilitySync
	StepDurabilityAsync
)

type WaitForFailurePolicy uint8

const (
	FailFlowOnWaitForFailure WaitForFailurePolicy = iota
	ProceedOnWaitForFailure
)

type ExecuteFailure struct {
	// unexported
}

type AttributeLock interface {
	attributeLock()
}

func LockAttribute[T any](attribute Attribute[T]) AttributeLock
func LockAttributeMap[T any](
	attribute AttributeMap[T],
	instance string,
) AttributeLock

func ProceedToOnExecuteFailure[IN any](
	step Step[IN],
	options *StepOptions,
) *ExecuteFailure

type StepOptions struct {
	WaitForMethodTimeout  time.Duration
	ExecuteMethodTimeout  time.Duration
	HeartbeatTimeout      time.Duration
	WaitForRetry    *RetryPolicy
	ExecuteRetry    *RetryPolicy
	WaitForFailure  WaitForFailurePolicy
	ExecuteFailure  *ExecuteFailure
	WaitForDurability  StepDurability
	ExecuteDurability  StepDurability
	WaitForLockAttributes []AttributeLock
	ExecuteLockAttributes []AttributeLock
	WaitForLoadAttributeMaps []AttributeDef
	WaitForLoadAttributeMapInstances []AttributeMapLoad
	WaitForLoadChannels []ChannelDef
	WaitForLoadChannelMaps []ChannelDef
	WaitForLoadChannelMapInstances []ChannelMapLoad
	ExecuteLoadAttributeMaps []AttributeDef
	ExecuteLoadAttributeMapInstances []AttributeMapLoad
	ExecuteLoadChannels []ChannelDef
	ExecuteLoadChannelMaps []ChannelDef
	ExecuteLoadChannelMapInstances []ChannelMapLoad
}
```

The typed constructor prevents callers from placing an erased step inside
`ExecuteFailure`. Phase 3 validates that its target consumes the failed step's
unchanged input, because the server reuses that input. `StepOptions` does not
expose physical attribute keys, server-owned fields, or a generic skip flag.
`HeartbeatTimeout` applies to regular WaitFor and Execute activities. Zero uses
the one-minute default. The Go SDK accepts non-negative whole seconds in the
signed int32 range. The server enforces a configurable minimum that defaults to
ten seconds. Local activities ignore it, and an asynchronous regular fallback
uses it. Worker heartbeat output may include a typed checkpoint Value. The next
regular attempt exposes the last checkpoint through its Context.

Ordinary Attributes and Channel size metadata load for every Step method.
AttributeMap contents and pending Channel messages require method-specific
selections. WaitFor and Execute use independent snapshots. Execute loads after
the winning Wait consumes messages, and retries reuse the initial snapshot for
one logical method execution. Attribute locks never imply state loading.

The server defaults Step durability to sync and retry total duration to four
hours. Regular attempts default to two hours. Async durability first uses at
most seven local-activity seconds and three attempts, then applies the remaining
retry budget to regular fallback.

### RPC

RPC output and its optional next-step movements are explicit:

```go
type RPC[IN, OUT any] func(
	ctx Context,
	input IN,
) (*RPCResult[OUT], error)

type RPCDef interface {
	// unexported
}

func DefineRPC[IN, OUT any](
	rpc RPC[IN, OUT],
	options *RPCOptions,
) RPCDef

type RPCResult[OUT any] struct {
	Output         OUT
	NextSteps      []StepMovement
	CancelingSteps []StepSelector
}

func (result *RPCResult[OUT]) CancelSteps(
	steps ...StepSelector,
) *RPCResult[OUT]
```

Application code defines a Flow method with that signature:

```go
type BillingFlow struct{ dex.FlowDefaults }

func (flow *BillingFlow) GetRPCs() []dex.RPCDef {
	return []dex.RPCDef{
		dex.DefineRPC(flow.Refund, &dex.RPCOptions{Timeout: 30 * time.Second}),
	}
}

func (*BillingFlow) Refund(
	ctx dex.Context,
	input RefundInput,
) (*dex.RPCResult[RefundOutput], error) {
	return &dex.RPCResult[RefundOutput]{Output: RefundOutput{}}, nil
}
```

A Flow exposes RPCs by returning direct bound methods from `GetRPCs`. Phase 3
registration validates the definition and uses the Go method name as the durable
RPC name. Package-level functions and methods from another Flow are not
registrable RPCs. Methods omitted from `GetRPCs` are ordinary methods.

RPC methods use typed attributes/channels and `Context.RecordEvent`. They do not
receive a legacy `Persistence` or `Communication` argument. RPC cannot use
step-execution locals or return a close decision. It cannot use the sibling
selector because an RPC has no Step execution lineage. Its Flow-wide selector
is resolved before the same result's Next Steps are queued.

### Public client types and façade

Phase 1 fixes signatures but does not implement gRPC.

As in dex-base, every FlowService operation is a method on `Client`. Client
methods are intentionally non-generic; application handler APIs retain their
strong typing, while transport calls use `any` and caller-provided output
pointers.

```go
type Client struct {
	// unexported
}

func (client *Client) StartFlow(
	ctx context.Context,
	flow Flow,
	flowID string,
	input any,
	options StartFlowOptions,
) (runID string, err error)

func (client *Client) InvokeRPC(
	ctx context.Context,
	flowID string,
	rpc any,
	input any,
	outputPtr any,
) error

func (client *Client) WaitForAttributeMatch(
	ctx context.Context,
	flowID string,
	attribute AttributeDef,
	match AttributeMatchDef,
	valuePtr any,
	options WaitForAttributeOptions,
) error

func (client *Client) WaitForAttributeMapInstanceMatch(
	ctx context.Context,
	flowID string,
	attribute AttributeDef,
	instance string,
	match AttributeMatchDef,
	valuePtr any,
	options WaitForAttributeOptions,
) error
```

`StartFlow` sends `input` to the Flow's starting step. Phase 3 registration
resolves that step and validates its handler signature. `InvokeRPC` accepts an
application RPC value as `any`. `valuePtr` and `outputPtr` must be non-nil
pointers. Client methods target
the current run for a flow ID; they do not take a `runID` argument.
`StartFlow` still returns the created run ID.

The remaining FlowService operations use non-generic public types:

| Server RPC | Phase 1 façade |
|---|---|
| `StopFlow` | `Client.StopFlow(ctx, flowID, StopOptions)` |
| `WaitForFlow` | `Client.WaitForFlow(ctx, flowID, WaitForFlowOptions)` |
| `SearchFlows` | `Client.SearchFlows(ctx, query, pageSize, nextPageToken)` |
| `TimeTravel` | `Client.TimeTravel(ctx, flowID, TimeTravelOptions)` |
| `SkipTimer` | `Client.SkipTimer(ctx, flowID, StepExecutionID, TimerID)` |
| `UpdateFlowConfig` | `Client.UpdateFlowConfig(ctx, flowID, FlowConfig)` |
| `WaitForStepCompletion` | `Client.WaitForStepCompletion(ctx, flowID, StepExecutionID, WaitForStepCompletionOptions)` |
| `TriggerContinueAsNew` | `Client.TriggerContinueAsNew(ctx, flowID)` |
| `HealthCheck` | `Client.HealthCheck(ctx)` |

`WaitForAttributeMatch` compares the encoded server value and returns the value
that satisfied the match. Waiting on a blob-backed stored value may return
`FailedPrecondition`; SDK hydration does not change server-side wait semantics.

Request IDs:

- the SDK generates one UUID per logical `InvokeRPC` call, and for StartFlow
  when no override is supplied;
- `StartFlowOptions.RequestID` may provide a non-empty business identifier;
- Step completion and Attribute wait Request IDs are optional overrides;
- the server derives a namespaced ID from the Step execution or Attribute
  condition when none is supplied;
- automatic wait reattachments reuse the logical ID, and handler-timeout reuse
  advances an `-N` suffix; and
- every Temporal RPC Update and durable wait uses its request ID as a Temporal Update ID.

### Client result structs

Dynamic values returned by WaitForFlow and SearchFlows use an opaque SDK value,
never `dexpb.Value`:

```go
type Value struct {
	// unexported
}

func (value Value) Decode(valuePtr any) error

type FlowStatus uint8

const (
	FlowRunning FlowStatus = iota + 1
	FlowCompleted
	FlowFailed
	FlowServerSideTimeoutInternalOnly
	FlowTerminated
	FlowCanceled
	FlowContinuedAsNew
)

type FlowErrorType uint8

const (
	FlowErrorStepDecision FlowErrorType = iota + 1
	FlowErrorClientAPI
	FlowErrorWorkerMethod
	FlowErrorInvalidUserCode
	FlowErrorInternal
)

type StepCompletion struct {
	StepType        string
	StepExecutionID string
	Output          Value
}

type FlowResult struct {
	Status       FlowStatus
	Completions  []StepCompletion
	ErrorType    FlowErrorType
	ErrorMessage string
}

func (result FlowResult) IsTerminal() bool
func (result FlowResult) DecodeSingleOutput(target any) error

type SearchFlowEntry struct {
	FlowID           string
	RunID            string
	FlowType         string
	Status           FlowStatus
	StartedAt        time.Time
	ClosedAt         time.Time
	IndexedAttributes map[string]Value
}

type SearchFlowsPage struct {
	Flows         []SearchFlowEntry
	NextPageToken string
}

type HealthInfo struct {
	Condition string
	Hostname  string
	Duration  int32
}
```

`StepCompletion.Output` and Indexed Attributes decode into a caller-provided
non-nil pointer. The SDK does not guess an application type when the response
does not carry a typed definition.

`LoadBlobs` is not a public client operation. Phase 5 connects it to the
internal Phase 2 hydration seam.

### Client option structs

```go
type WorkerTarget struct {
	Address  string
	Headless bool
}

type ActiveStepSearchMode uint8

const (
	SearchActiveStepsDefault ActiveStepSearchMode = iota
	SearchAllActiveSteps
	SearchActiveStepsWithWaitFor
	DisableActiveStepSearch
)

type FlowConfig struct {
	ActiveStepSearchMode       *ActiveStepSearchMode
	AttributeStoreNames        []string
	ContinueAsNewThreshold     *int32
	ContinueAsNewPageSizeBytes *int32
	StepDurability             *StepDurability
	WorkerTarget               *WorkerTarget
}

type StartFlowOptions struct {
	Timeout        *time.Duration
	TimeoutPolicy  FlowTimeoutPolicy
	IDReusePolicy  IDReusePolicy
	StartDelay     *time.Duration
	RetryPolicy    *FlowRetryPolicy
	Attributes     []InitialAttributeDef
	ConfigOverride *FlowConfig
	AlreadyStarted *AlreadyStartedOptions
	RequestID      *string
}

type FlowTimeoutPolicy uint8

const (
	TimeoutDefault FlowTimeoutPolicy = iota
	TimeoutFail
	TimeoutCancel
	TimeoutHandler
)

type SubFlowReusePolicy uint8

const (
	RestartSubFlowIfPreviousExitedAbnormally SubFlowReusePolicy = iota
	AttachSubFlow
	AlwaysRestartSubFlow
)

type SubFlowOptions struct {
	Timeout        *time.Duration
	TimeoutPolicy  FlowTimeoutPolicy
	StartDelay     *time.Duration
	RetryPolicy    *FlowRetryPolicy
	Attributes     []InitialAttributeDef
	ConfigOverride *FlowConfig
	ReusePolicy    SubFlowReusePolicy
	ConditionID    string
}

type IDReusePolicy uint8

const (
	IDReuseDefault IDReusePolicy = iota
	IDReuseAllowIfPreviousFailed
	IDReuseAllowIfNotRunning
	IDReuseDisallow
	IDReuseTerminateIfRunning
)

type FlowRetryPolicy struct {
	InitialInterval    time.Duration
	BackoffCoefficient float64
	MaximumInterval    time.Duration
	MaximumAttempts    int32
}

type AlreadyStartedOptions struct {
	IgnoreError bool
}

type RPCOptions struct {
	Timeout                   time.Duration
	LockAttributes            []AttributeLock
	IsTransactional           bool
	LoadAttributeMaps         []AttributeDef
	LoadAttributeMapInstances []AttributeMapLoad
	LoadChannels              []ChannelDef
	LoadChannelMaps           []ChannelDef
	LoadChannelMapInstances   []ChannelMapLoad
}

type WaitForFlowOptions struct {
	NeedsResults bool
}

type StopType uint8

const (
	CancelFlow StopType = iota + 1
	TerminateFlow
	FailFlow
)

type StopOptions struct {
	Type   StopType
	Reason string
}

type StepExecutionID struct {
	StepType        string
	ExecutionNumber *int32
}

type TimerID struct {
	ConditionID string
	Index       *int32
}

type TimeTravelType uint8

const (
	TimeTravelToBeginning TimeTravelType = iota + 1
	TimeTravelByHistoryEventTime
	TimeTravelByStepType
	TimeTravelByStepExecutionID
)

type TimeTravelStepMethod uint8

const (
	TimeTravelStepWaitFor TimeTravelStepMethod = iota + 1
	TimeTravelStepExecute
)

type TimeTravelOptions struct {
	Type                       TimeTravelType
	Reason                     string
	HistoryEventTime           time.Time
	StepType                   string
	StepExecutionID            string
	StepMethod                 TimeTravelStepMethod
	SkipWritesReapply          bool
}
```

Pointer fields in `FlowConfig` preserve proto presence for partial overrides.
`AttributeStoreNames == nil` omits the target override, while an empty non-nil
slice disables synchronization for future enabled writes. Every enabled Attribute
write is projected to every selected Store.
`WorkerTarget` is configured through `FlowConfig`, not as a separate StartFlow
argument. Phase 5 adds `ClientOptions.WorkerTarget` as the default inserted into
that FlowConfig.

`StartFlowOptions.Timeout == nil` means no Dex soft Flow timeout. A positive
timeout uses `TimeoutHandler` when the registered Flow implements
`FlowTimeoutHandler`; otherwise it uses `TimeoutFail`. Explicit
`TimeoutHandler` without that capability fails locally. `TimeoutFail` produces
`FlowErrorTypeFlowTimeout` and permits Flow retry, while `TimeoutCancel` does
not retry. Continue-as-new preserves the absolute deadline; retry runs receive
a new budget.
`StartFlowOptions.TimeoutHandlerOptions` and the matching SubFlow field are
valid only for a positive timeout whose resolved policy is `TimeoutHandler`.
They configure Execute-style method and heartbeat timeouts, retry, durability,
Attribute locks, selective state loading, and exhausted-retry routing to a
registered `Step[None]`. The recovery Step receives normalized `None` input and
reads the final failure through `Context.RecoveryError`. Handler timing starts
when the soft timeout fires and may extend beyond that deadline. Continue-as-new
and Flow retry preserve the options.
`StartFlowOptions.StartDelay == nil` omits the start delay. Starting-step
options come from the step wrapped by `DefineStartStep`; StartFlow has no
separate step-options override.

Go wait methods use the caller's context deadline and always send a zero
server-side duration, selecting the server-configured maximum long poll.
`WaitForFlowOptions` exists only to request completion results.
`StepExecutionID.ExecutionNumber == nil` selects execution one.

`InitialAttributeDef` is sealed and constructed with typed helpers so initial
values carry the definition's index and Attribute Store sync configuration:

```go
type InitialAttributeDef interface {
	initialAttribute()
}

func InitialAttribute[T any](
	attribute Attribute[T],
	value T,
) (InitialAttributeDef, error)

func InitialAttributeMapValue[T any](
	attribute AttributeMap[T],
	instance string,
	value T,
) (InitialAttributeDef, error)
```

No legacy time-travel, memo, loading-policy, or worker-URL fields are retained.

### Public errors

Invocation attribute misses return a typed local error:

```go
type AttributeNotFoundError struct {
	AttributeName string
	Instance      string
}
```

`Instance` is populated for `AttributeMap` reads. The error supports
`errors.As` and is never converted to a gRPC status.

All FlowService failures preserve gRPC status, Dex details, operation, Flow ID,
and the original gRPC cause:

```go
type ServiceError struct {
	Op        string
	FlowID    string
	Code      codes.Code
	SubStatus ErrorSubStatus
	Detail    string
}

type WorkerError struct {
	Code   codes.Code
	Type   string
	Detail string
}

type ErrorSubStatus uint8

const (
	ErrorSubStatusUncategorized ErrorSubStatus = iota + 1
	ErrorSubStatusFlowAlreadyStarted
	ErrorSubStatusFlowNotFound
	ErrorSubStatusWorkerAPI
	ErrorSubStatusLongPollTimeout
)
```

Applications use `errors.As` with `FlowAlreadyStartedError`,
`FlowNotFoundError`, `FlowNotActiveOrNotFoundError`, `WorkerInvocationError`,
`RPCLockConflictError`, or `LongPollTimeoutError`. Each unwraps through
`ServiceError` to the original gRPC status. `ErrorSubStatus` remains diagnostic
metadata.

`WaitForFlow` returns `FlowResult` for every terminal status. Transport,
long-poll, hydration, and invalid-input failures remain errors.

WaitForFlow and TimeTravel require an existing Flow. Stop, timer, config, and
Step or Attribute wait operations require an active Flow. RPCs use
`FlowNotActiveOrNotFoundError` for a missing target or a closed target that the
operation cannot use. The error alone cannot distinguish those cases. A retained
terminal execution remains readable through an unlocked, non-transactional RPC
with no durable effects when the Server permits the query path. At that confirmed
read boundary, handle the error directly as missing or unavailable without
`WaitForFlow`, fixed short timeouts, retries, or historical output decoding.
Handlers returning durable effects and Server-forced Update routing require an
active execution. RPC success alone does not prove the Flow remains active. The
shared server `FLOW_NOT_EXISTS` sub-status retains its endpoint-specific mapping.

Registry and unregistered-definition failures return `FlowDefinitionError`.
Invalid Wait, StepDecision, and RPCResult values return
`InvalidStepResultError` from WorkerService. Value encoding and decoding
failures return `ValueMappingError`; invalid call arguments remain ordinary Go
errors.

### End-to-end authoring example

```go
var (
	OrderStatus = dex.DefineAttribute[string](
		"order-status",
		dex.Indexed(dex.AttributeIndex{Type: dex.IndexKeyword}),
	)
	Commands = dex.DefineChannel[Command]("commands")
)

type WaitForCommandStep struct {
	dex.StepDefaults
}

func (WaitForCommandStep) WaitFor(
	ctx dex.Context,
	input OrderInput,
) (*dex.Wait, error) {
	if err := OrderStatus.Set(ctx, "waiting"); err != nil {
		return nil, err
	}

	if err := ctx.SetStepExecutionLocal(
		"snapshot",
		OrderSnapshot{OrderID: input.OrderID},
	); err != nil {
		return nil, err
	}
	if err := ctx.RecordEvent("waiting-for-command", input); err != nil {
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
	_ = commands

	var snapshot OrderSnapshot
	found, err := ctx.GetStepExecutionLocal("snapshot", &snapshot)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("snapshot is missing")
	}
	return dex.GracefulComplete(snapshot), nil
}

var WaitForCommand = WaitForCommandStep{}
var _ dex.Step[OrderInput] = WaitForCommand

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
	}
}

var Orders = OrderFlow{}
var _ dex.Flow = Orders
```

Step handlers return nil with an error. Other exact zero values only satisfy
Go's return rules.

## Phase 1 deliverables

1. Approve this public API shape.
2. Add the public declarations with unexported runtime fields.
3. Add compile-time example coverage for generic interfaces and signatures.
4. Keep registration-only generic handler adapters unexported.
5. Keep raw condition results unexported.
6. Delete old public API files only when their replacements compile in the same
   change; do not add compatibility aliases.
7. Stop at the Phase 1 exit gate. Do not add registry or WorkerService code.

## Tests

Phase 2 cannot use server integration tests because it intentionally has no
registration, worker, or transport. External contract tests cover the public
surface; package-internal tests cover codec and mapper branches.

Add SDK external-package tests (`package dex_test`) for these scenarios:

1. A package can declare a flow, heterogeneous typed steps, attributes,
   channels, and RPCs without importing `dexpb`.
2. Waiting and Execute-only handlers satisfy the same `Step[IN]`; the former
   embeds `StepDefaults` and the latter embeds `StepDefaultsNoWaitFor[IN]`.
3. `None` preserves typed Step, RPC, and Channel declarations without accepting
   arbitrary payloads.
4. `DefineStep`, `DefineStartStep`, `GoTo`, `MovementOf`, and `GoToMany`
   preserve target step input types.
5. A Flow method matching `RPC[IN, OUT]` preserves typed worker handlers, while
   `Client.InvokeRPC` accepts `any` and a caller-provided output pointer.
6. Static and map attributes expose typed Get/Set/Delete methods that take
   `Context`; missing reads return `*AttributeNotFoundError`, while physical
   keys remain internal behind sealed lock values.
7. Static and map channels expose Publish, all five count forms, and the
   `myCh.Size(ctx)` / `myChMap.Size(ctx, key)` RPC shape.
8. A Wait combines timers and channel conditions through All, Any, and
   AnyCombo.
9. Static and map channel result methods return `[]T` without exposing raw
   condition-result types.
10. The `Context.HasTimerFired` and `HasTimerFiredByIndex` signatures compile.
11. Execute can return `GoTo`, `GoToMany`, all close decisions, and conditional
    force-complete fallback using channel definitions directly.
12. Step-execution local Set accepts `any`, Get accepts a caller-provided
    pointer, and `RecordEvent(name, any)` compiles.
13. `Context.WaitForMethodFailed` compiles for Execute implementations.
14. Non-generic client methods use server identifiers directly and return only
    run ID from starts.

Package-internal tests cover native and JSON values, indexed attributes,
duration rounding, waits, decisions, errors, hydration validation, and cache
payload round trips.

Run Phase 2 verification through the Makefile:

```bash
make -C sdk-go unitTests 2>&1 | tee /tmp/test-go-sdk-phase2.log
make -C blob-cache-go blobCacheTests 2>&1 | tee /tmp/test-go-sdk-phase2-blobcache.log
make copyright-check 2>&1 | tee /tmp/test-go-sdk-phase2-copyright.log
```

Later phases must add Temporal integration coverage for:

1. static and map channel results decode to their declared Go types;
2. multiple completed conditions on one concrete channel concatenate values in
   declaration order;
3. natural timer completion and `SkipTimer` both make `HasTimerFired` true;
4. `HasTimerFiredByIndex` identifies the completed timer in `AnyComboOf`;
5. untyped step-execution locals round-trip from WaitFor to Execute through a
   caller-provided pointer;
6. arbitrary event values are encoded and recorded once per invocation name;
7. Flow method RPC registration, handler invocation, and optional next steps
   map to the current server contract.

Cadence execution is not required because the default Dex server image is
Temporal-backed.

## Documentation

- Keep this plan linked from [`docs/README.md`](../../README.md).
- Keep [`sdk-go/README.md`](../../../sdk-go/README.md) aligned with the
  authoring and value-codec contracts. Cover the single `Step[IN]`
  interface, step defaults, Flow method RPCs, close decisions, typed
  attributes/channels, untyped step-execution locals and events, strongly typed
  channel results, timer-fired helpers, `WaitForMethodFailed`, and RPC-only
  channel size.
- Update [`sdk-go/CONTRIBUTION.md`](../../../sdk-go/CONTRIBUTION.md) with the
  Phase 2 verification commands and the rule that application packages do not
  import `dexpb`.
- Blob-cache documentation belongs with its independent component. The Go SDK
  documentation will later describe only how that component is configured or
  injected.

## UI/UX

N/A: no in-repo web UI.
