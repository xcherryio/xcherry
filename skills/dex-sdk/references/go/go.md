# Go handbook

The SDK API reference uses the [released Go SDK v1.14.1 baseline](../core/bundle-baselines.md); the source examples' older dependency pins below remain factual and do not define the current error API.

Use this page first for a Go application. It describes the application boundary and points to focused pages; always check the version selected by `go.mod` and `go.sum`.

## Version and source authority

The [baseline module](https://github.com/superdurable/dex/blob/sdk-go/v1.15.0/examples/go/go.mod) uses `github.com/superdurable/dex/sdk-go v1.13.0`. Before editing an existing project, run `go list -m github.com/superdurable/dex/sdk-go` and inspect that SDK version. If it differs from the baseline, installed source wins. Identify the inspected module-cache path, vendored path, or immutable tag/commit before presenting exact syntax. If no version-matched source is available, give only the version-independent Flow model and request that source; do not adapt baseline snippets speculatively. Pinned sources show known-good shapes, not a promise that every version has the same surface.

## Project shape and local run

Keep Flow and Step definitions in application packages, compose them in one registry package, and create one shared Registry and BlobCache for Client and Worker. Inject business services into Flow constructors. The official example layout is `products/`, `patterns/`, `primitives/`, `registry/`, `shared/`, and `cmd/server/`.

```bash
dexcli dev
cd examples/go
make bins
./dex-samples
```

Defaults use Dex at `localhost:8801`, a Worker listener at `127.0.0.1:8803`, and HTTP at `127.0.0.1:8080`. `DEX_WORKER_TARGET` is the address advertised to Dex and can differ from the bind address.

## Pull Workers and Dex credentials

Set `WorkerOptions.Pull` to `&dex.PullWorkerOptions{...}` for outbound polling; leave `BindAddress` and the listening `WorkerTarget` unset. `worker.WorkerTarget()` returns the pull target for `ClientOptions.WorkerTarget`. A Client can also use `&dex.WorkerTarget{Pull: true}` as its default, or a StartFlow or SubFlow `FlowConfig.WorkerTarget` override. `PullWorkerOptions` exposes `MaxConcurrentTasks` (CPU count by default), `IdleStreamsPerQueue` (one), `AppWorkerID` (generated), and `StopDrainTimeout` (30 seconds). Set a fresh `AppWorkerID` after restart if overriding it. With Server and Go SDK v1.12.0, a Worker that lacks a requested Step type or RPC name declines it before the handler runs, so Dex can reassign the task without consuming the Step attempt. Follow the [pull Worker guarantees](../core/pull-workers.md) for sticky routing, retries, heartbeats, stream sizing, and shutdown. [Released Go API](https://github.com/superdurable/dex/blob/sdk-go/v1.15.0/sdk-go/dex/worker.go) [Runnable pull Worker example](https://github.com/superdurable/dex/blob/sdk-go/v1.15.0/examples/go/production/pull-workers/worker.go)

With Go SDK v1.13.0, launch **Start** on a goroutine and gate startup with **WaitPullRegistered(ctx)** using a deadline. For rolling deployments, wait for replacement Workers before stopping old Workers. Use **PullRegistrationState().IsReady** plus application lifecycle health for subsequent probes; reconnects do not flap it, and Stop does not clear it. Inspect **Queues**, **HasRegisteredStream**, and **LastPermanentError** to diagnose readiness. A permanent rejection matters only when its queue has no other registered stream. Before readiness it returns **PullRegistrationError** with **Queue** and the underlying status in **Err**; use **errors.As** to inspect it. **PullWorkerOptions.StopOnPermanentStreamRejection** is opt-in, defaults to false, and drains all queues before **Start** returns the rejection. Keep handling **Start** errors, call **Stop** on exit, and follow the [complete registration and shutdown contract](../core/pull-workers.md#startup-registration-and-readiness). The [released API documentation](https://github.com/superdurable/dex/blob/sdk-go/v1.15.0/sdk-go/dex/worker.go) includes a complete startup and HTTP probe example; the pinned runnable application example also includes a bounded registration wait, startup failure cleanup, and an HTTP readiness handler.

With SDK v1.15.0, **PullWorkerOptions.ReservedRPCTasks** reserves RPC capacity. Use **ptr.Any(0)** to opt out; the field is a pointer to **int** and nil selects the default. Unset reserves one slot when resolved maximum concurrent tasks **M ≥ 2**; otherwise it reserves zero and logs one startup warning. Require **0 ≤ R < M**; invalid reserves fail construction. This intentionally gives Steps one fewer slot by default. Both API-direct and transactional RPC tasks may use all **M** slots; WaitFor and Execute, including local attempts, are declined at **M − R** total busy handlers. Export **Worker.PullReservedRPCDeclines()** to measure reserve declines. Follow the shared [reserve semantics and sizing advice](../core/pull-workers.md#reserved-rpc-capacity) for dispatch exhaustion, counter scope, and long-running Steps. [Released capacity guidance](https://github.com/superdurable/dex/blob/sdk-go/v1.15.0/sdk-go/README.md)

With SDK v1.14.1, the Worker gracefully closes surplus idle streams after load drops while preserving readiness and the configured registered idle streams per queue. Size steady-state streams as **M + (F + 1) × I**, where **M** is maximum concurrent tasks, **F** is registered Flow types, and **I** is idle streams per queue. Burst peaks remain bounded by **matching.maxConcurrentTasksCap** and **matching.maxStreams**; follow the shared [capacity guidance](../core/pull-workers.md#heartbeats-capacity-and-shutdown).

For an authenticated Dex Server, set `ClientOptions.Credential` and `WorkerOptions.Credential` separately. `dex.FileCredential(path)` reads a rotating bearer-token file and fails calls if the file becomes empty or unreadable. A custom `dex.Credential` returns the current token before each FlowService call; pull streams obtain it when they open or reconnect. Keep tokens out of Flow state and protect the plaintext connection. [Released Go credential API](https://github.com/superdurable/dex/blob/sdk-go/v1.15.0/sdk-go/dex/credential.go)

## Connector configuration

Official Go connectors from v0.21.0 open each connection with the generated `NewProjectConnection(project, "connection-name")`, where `project` comes from `projectconfig.LoadFromEnvironment(ctx)` in the Connector SDK's `sdkgo/projectconfig`; with `sdkgo` v0.28.0 or later it reads the Dex Web development store when **DEX_CONNECTOR_CONFIG_FILE** names it. Set the same static `ConnectionName` on every operation-specific factory config. Earlier connector releases generate `NewLocalConnection(store, "connection-name")` and read the Dex Web development store with `localconfig.LoadFromEnvironment`, started with **DEX_CONNECTOR_CONFIG_FILE**. [Local Connector credentials](../core/operations.md#local-connector-credentials) says which release line reads what, and [Deployed Connector configuration](../core/operations.md#deployed-connector-configuration) describes the project configuration.

Settings are captured when the application starts. Credentials are resolved for each provider call, so reauthorization does not require a restart; settings changes do. Keep generated secret values outside Flow state and logs.

Connector Step branch targets receive only the current generated result alias. Query aliases resolve to `sdkgo.QueryResult[OUT]`; Mutation aliases resolve to `sdkgo.MutationResult[OUT]`. `MapToOperationInput` is a pure mapping from the current application Step input into provider input; it has no error result, and the application input is not present in the operation result. Persist business context with an application Step and Attribute before the Connector call, then reload it in success, failure, RPC, and recovery Steps. Use `Annotations` for graph group and explanation metadata; do not use it for visual styling.

`ResultAttribute` is optional for every Query and Mutation. Omit it when only the branch target needs the operation result. Configure and register the precisely typed Attribute when an RPC, display, audit, recovery operator, or another path must read the raw result outside the transition chain.

Provider-neutral Triggers run outside Steps. Give each generated Trigger factory a static binding name, then pass an application-owned target. `NewDexFlowTriggerTarget` takes the typed Flow, a `TriggerFilter`, a stable `FlowIDResolver`, and a `FlowInputMapper`. `NewDexRPCTriggerTarget` takes the application's typed RPC, a filter, the same identity resolver, and an `RPCInputMapper`; no string RPC name is configured.

The filter runs before identity resolution, input mapping, or any Dex call. Return false to consume an event that does not satisfy the application's channel, sender, message, tenant, authorization, routing-field, or other domain rule. Filter, resolver, and mapper callbacks are deterministic, side-effect-free pure functions without error results. A blank or invalid resolved Flow ID is an application defect. Provider binding matchers reduce traffic but do not replace this application admission boundary.

For RPC delivery, register the application's bound method with application-owned `dex.RPCOptions`, then pass that method directly to `NewDexRPCTriggerTarget`. The application decides whether stable provider event IDs need deduplication, stores only bounded domain state, and locks only the business state or effect that must commit atomically.

Use the released [Slack example](https://github.com/superdurable/dex-connectors-library/tree/connectors/slack/v0.21.0/connectors/slack/examples/thread-approval) and [Gmail example](https://github.com/superdurable/dex-connectors-library/tree/connectors/google/gmail/v0.21.0/connectors/google/gmail/examples/thread-reply) as exact integration references. Their modules pin the exact Connector SDK releases they support.

## Raw Temporal payload inspection

Use **dexcli codec-server** with CLI v1.4.2 or later to inspect Dex internal payloads
in Temporal Cloud or a local Temporal Web UI. Follow the shared
[codec-server guidance](../core/operations.md#inspect-raw-temporal-payloads);
local UI ports need no additional server arguments.

## Deployed configuration and recovery boundary

Dex provides no deployed Connector configuration or credential store. A deployed
Go application reads its connections with the Connector SDK's `sdkgo/projectconfig`,
as described in [deployed Connector configuration](../core/operations.md#deployed-connector-configuration).
Keep production credentials behind that storage and the deployment's secret
management; a browser, Flow input, or ordinary Attribute cannot select secret storage.
When the Flow itself must hold a short-lived secret, such as an API key passed
at start, use a [hard-delete Attribute](data-handling.md#hard-delete-attributes)
instead of an ordinary Attribute.
For recovery after a Worker fix, preserve the original resource and Flow ID,
reconcile external effects, and use an explicitly authorized, recorded Step
boundary as described in [safe recovery](../core/operations.md#safe-recovery).

## Dex Web v2 management metadata

The current FDG 2.0 management-interface analyzer reads Go source. Go Flow
definitions can publish Indexed Attribute presentation, Summary and Display RPC
fields, Action metadata, typed Action forms, conditions, permissions, groups,
explanations, and user-string `capture:qr-code` hints. Validate that metadata
with the v2 analyzer. This source-analysis capability is not currently available
for Java, Python, TypeScript, or Rust Flow definitions.

## Minimal Flow

Declare schema at package scope, embed defaults, register Step types, and return a decision from every Execute method. Keep SDK-derived Flow and Step type names; do not implement `GetFlowType` or `GetStepType` except for the [required production-rename case](versioning.md#default-flow-and-step-type-names).

[Pinned runnable source](https://github.com/superdurable/dex/blob/sdk-go/v1.15.0/examples/go/primitives/flow/workflow.go)
<!-- dex-source: examples/go/primitives/flow/workflow.go -->
```go
var (
	Status = dex.DefineAttribute[string]("status")
	Notify = dex.DefineChannel[dex.None]("notify")
)

type ExampleFlow struct {
	dex.FlowDefaults
}

func NewExampleFlow() *ExampleFlow {
	return &ExampleFlow{}
}

func (*ExampleFlow) GetSteps() []dex.StepDef {
	return []dex.StepDef{
		dex.DefineStartStep(ExampleStep{}),
		dex.DefineStep(Finish{}),
	}
}
```

Use `dex.None` for nil-only input/output and concrete structs for durable payloads.

A no-wait Step embeds the input-typed default and returns a durable decision:

[Pinned runnable source](https://github.com/superdurable/dex/blob/sdk-go/v1.15.0/examples/go/primitives/flow/workflow.go)
<!-- dex-source: examples/go/primitives/flow/workflow.go -->
```go
type Finish struct {
	dex.StepDefaultsNoWaitFor[int]
}

func (Finish) Execute(ctx dex.Context, input int) (*dex.StepDecision, error) {
	if err := Status.Set(ctx, "done"); err != nil {
		return nil, err
	}
	return dex.GracefulComplete(input + 1), nil
}
```

## Registry, Worker, and Client

Construct every Flow once, then pass the same definitions into the Registry used by Worker and Client. See the [registry](https://github.com/superdurable/dex/blob/sdk-go/v1.15.0/examples/go/registry/registry.go) and [bootstrap](https://github.com/superdurable/dex/blob/sdk-go/v1.15.0/examples/go/cmd/server/dex/dex.go). Controllers start with `client.StartFlow(ctx, flow, flowID, input, options)` and retain the run ID for diagnostics.

## Route by task

- Syntax/state semantics: [primitives](primitives.md)
- Application shape: [patterns](patterns.md)
- Real-server verification: [testing](testing.md)
- Client boundary design and retry ownership, before implementation: [error handling](error-handling.md)
- Persistence/maps/Streams: [data handling](data-handling.md)
- Histories/progress: [observability](observability.md)
- Open-Flow compatibility: [versioning](versioning.md)
- Go traps: [gotchas](gotchas.md)
- Locks/loading/heartbeat/cancellation: [advanced features](advanced-features.md)

## Completion checklist

Confirm every Step type and persistent definition is registered, errors are returned, Client calls have deadlines, tests use unique IDs and a real Dex Server, and application code does not depend on Dex Server internals.
