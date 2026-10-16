# TypeScript handbook

The SDK API reference uses the [released v1.11.0 baseline](../core/bundle-baselines.md); the source examples' older dependency pins below remain factual and do not define the current error API.

Read this page first for TypeScript application work, then open only the topic reference needed. The [baseline package](https://github.com/superdurable/dex/blob/sdk-go/v1.11.0/examples/typescript/package.json) uses `@superdurable/dex@1.10.1` on Node.js 22 or 24. Always inspect the application's lockfile and installed declarations before using a precise API.

## Project shape

Keep Flow/Step definitions separate from HTTP handlers, construct all Flow instances in one registry module, and create one `Registry`, `BlobCache`, `Worker`, and `Client` per process. Pass the Client into adapters or a controlled holder when async Steps start or interact with other Flows. Values cross the boundary through `Codec<T>`.

## Pull Workers and Dex credentials

Construct a Worker with `{ pull: { maxConcurrentTasks: 8 } }` and no `bindAddress` or listening `workerTarget`. Advertise `worker.workerTarget` or `{ pull: true }` through `ClientOptions.workerTarget` or a StartFlow or SubFlow FlowConfig override. Pull options also include `idleStreamsPerQueue`, `appWorkerID`, and `stopDrainTimeoutMs`; defaults are one idle stream per queue, a generated process ID, and 30,000 milliseconds. Change an explicit app Worker ID after restart. `worker.close()` drains active handlers within that timeout. Follow the [pull Worker guarantees](../core/pull-workers.md) for sticky routing, retries, heartbeats, and stream capacity. [Released TypeScript options](https://github.com/superdurable/dex/blob/sdk-go/v1.11.0/sdk-typescript/src/options.ts) [Runnable pull Worker example](https://github.com/superdurable/dex/blob/sdk-go/v1.11.0/examples/typescript/src/production/pull-workers.ts)

Set `credential` separately in ClientOptions and WorkerOptions for an authenticated Dex Server. `FileCredential` reads a rotating file, while a custom `Credential.token()` can return a string or Promise for each call or pull stream opening. A thrown or rejected token fails the call locally. Keep the token out of Flow state and protect the plaintext connection. [Released TypeScript credentials](https://github.com/superdurable/dex/blob/sdk-go/v1.11.0/sdk-typescript/src/credential.ts)

## Connector runtime support

The Connector SDK's `projectconfig.LoadFromEnvironment` loader and the generated `NewProjectConnection` API are Go-only, and no equivalent TypeScript SDK exists today. Do not invent a TypeScript loader or deserialize Dex Web's credential file into Flow state. For a TypeScript application, keep provider credentials behind an application-owned secret/runtime adapter until an official TypeScript Connector SDK exposes the same contract.

## Raw Temporal payload inspection

Use **dexcli codec-server** with CLI v1.4.2 or later to inspect Dex internal payloads
in Temporal Cloud or a local Temporal Web UI. Follow the shared
[codec-server guidance](../core/operations.md#inspect-raw-temporal-payloads);
local UI ports need no additional server arguments.

## Deployed configuration and recovery boundary

Dex provides no deployed Connector configuration or credential store. A deployed Go
application reads its connections with the Connector SDK's `sdkgo/projectconfig`, but
no equivalent TypeScript SDK exists today; see
[deployed Connector configuration](../core/operations.md#deployed-connector-configuration).
Keep credentials behind the deployment's secret management; a browser, Flow input,
or ordinary Attribute cannot select secret storage.
When the Flow itself must hold a short-lived secret, such as an API key passed
at start, use a [hard-delete Attribute](data-handling.md#hard-delete-attributes)
instead of an ordinary Attribute.
For recovery after a Worker fix, preserve the original resource and Flow ID,
reconcile external effects, and use an explicitly authorized, recorded Step
boundary as described in [safe recovery](../core/operations.md#safe-recovery).

## Dex Web v2 management metadata

The current FDG 2.0 management-interface analyzer reads Go source only. A
TypeScript Flow cannot currently publish the Summary, Display, Action, Indexed
Attribute, group, explanation, or Action input capture metadata that drives the
full Dex Web v2 management interface. TypeScript applications can still operate
Flows and use their supported Client and CLI surfaces; do not invent decorators
for the Go-only analyzer contract.

## Minimal Flow

[Pinned runnable source](https://github.com/superdurable/dex/blob/sdk-go/v1.11.0/examples/typescript/src/primitives/flow/example-flow.ts)
<!-- dex-source: examples/typescript/src/primitives/flow/example-flow.ts -->
```typescript
export class ExampleFlow implements Flow<number> {
  private readonly finish = new Finish();
  private readonly example = new ExampleStep(this.finish);

  public getFlowType(): string {
    return "ExampleFlow";
  }

  public getSteps(): StepList<number> {
    return StepList.startStep<number>(this.example).otherSteps(this.finish);
  }

  public getPersistenceSchema(): PersistenceSchema {
    return { attributes: [status], channels: [notify] };
  }
```

Each Step supplies a stable `getStepType()` and an input codec when a non-default wire form is required. A Flow returns all registered Step instances once. The starting Step input must match `Flow<I>`.

[Pinned Step source](https://github.com/superdurable/dex/blob/sdk-go/v1.11.0/examples/typescript/src/primitives/flow/example-flow.ts)
<!-- dex-source: examples/typescript/src/primitives/flow/example-flow.ts -->
```typescript
class ExampleStep implements Step<number> {
  public readonly inputCodec = doubleCodec;

  public constructor(private readonly finish: Finish) {}

  public getStepType(): string {
    return "ExampleStep";
  }

  public waitFor(context: Context, _input: number): Wait {
    status.set(context, "running");
    return Wait.skipImmediately();
  }

  public execute(_context: Context, input: number): StepDecision {
    return goTo(Finish, input + 1);
  }
}
```

## Registry, Worker, and Client

[Pinned process bootstrap](https://github.com/superdurable/dex/blob/sdk-go/v1.11.0/examples/typescript/src/main.ts)
<!-- dex-source: examples/typescript/src/main.ts -->
```typescript
const registry = createExampleRegistry();
const blobCache = openBlobCache({
  directory: env.blobCacheDir,
  maxBytes: 1 << 30,
});
const worker = new Worker(registry, blobCache, {
  bindAddress: env.workerBindAddress,
  serverAddress: env.serverAddress,
  ...(env.workerTarget !== undefined
    ? { workerTarget: { address: env.workerTarget } }
    : {}),
});
await worker.start();
const client = new Client(registry, blobCache, {
  serverAddress: env.serverAddress,
  workerTarget: worker.workerTarget,
});
```

Await Worker startup before accepting application traffic. Close Client and Worker and then close BlobCache on shutdown.

Install the application's resolved `@superdurable/dex` version with its package manager. Adapters call the shared Client and `await` starts, typed RPCs for Flow-owned state, Attribute-match waits, searches, Streams, and stops.

## Run locally

```bash
dexcli dev
cd examples/typescript
npm install
npm start
```

Use `npm run test:integ` with Dex already running, or `./run-integration-tests.sh` for the managed suite.

## Continue reading

- [Primitives](primitives.md) and [patterns](patterns.md) for modeling.
- [Error handling](error-handling.md) before writing Client, admission, RPC, cleanup, wait, or external Stream boundaries.
- [Testing](testing.md) before delivery.
- [Data](data-handling.md), [observability](observability.md), and [versioning](versioning.md) for production.
- [Gotchas](gotchas.md) and [advanced features](advanced-features.md) for async, loading, heartbeat, and cancellation details.
