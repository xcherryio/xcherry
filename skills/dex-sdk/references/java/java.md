# Java handbook

The SDK API reference uses the [released v1.12.0 baseline](../core/bundle-baselines.md); the source examples' older dependency pins below remain factual and do not define the current error API.

Use this page first for Java application work. Then load the topic page that matches the task. The [baseline build](https://github.com/superdurable/dex/blob/sdk-java/v1.12.0/examples/java/build.gradle) uses `io.superdurable:dex-sdk:1.11.0`, Spring Boot, and JDK 17 or newer. The application's Gradle or Maven lockfile remains authoritative.

## Project shape

Keep Flow and Step types in application packages, expose them as Spring beans, build one `Registry` from every Flow bean, and share one disk `BlobCache` between the `Worker` and `Client`. Inject business dependencies through constructors. Parameterized Step inputs are not supported directly; use a concrete holder class or an array.

The official example layout separates `products/`, `patterns/`, `primitives/`, and `shared/`. A smaller service normally needs `flows/`, `steps/`, `model/`, and a bootstrap/configuration class.

## Pull Workers and Dex credentials

Use `WorkerOptions.newBuilder().pull(PullWorkerOptions.newBuilder().maxConcurrentTasks(8).build()).build()` for outbound polling. Leave the bind address and listening Worker target unset. Advertise `worker.getWorkerTarget()` or `WorkerTarget.pull()` through `ClientOptions.Builder.workerTarget(...)` or a StartFlow or SubFlow FlowConfig override. `PullWorkerOptions.Builder` also offers `idleStreamsPerQueue`, `appWorkerID`, and `stopDrainTimeout`; defaults are one idle stream per queue, a generated process ID, and 30 seconds. A supplied app Worker ID must change after restart. With Server and Java SDK v1.12.0, a Worker that lacks a requested Step type or RPC name declines it before the handler runs, so Dex can reassign the task without consuming the Step attempt. Read the [pull Worker guarantees](../core/pull-workers.md) for sticky routing, retry consumption, heartbeats, capacity, and Stop. [Released Java options](https://github.com/superdurable/dex/blob/sdk-java/v1.12.0/sdk-java/src/main/java/io/superdurable/dex/PullWorkerOptions.java) [Runnable pull Worker example](https://github.com/superdurable/dex/blob/sdk-java/v1.12.0/examples/java/src/main/java/io/superdurable/dex/production/pullworkers/PullGreetingExample.java)

The published Java SDK remains at v1.12.0. Go SDK v1.13.0's **WaitPullRegistered**, **PullRegistrationState**, **PullRegistrationError**, and opt-in **StopOnPermanentStreamRejection** are not available in this SDK yet. Rely on Worker process health and watch logs for "Worker task stream rejected"; process health does not prove every queue registered. Keep deployment overlap while validating replacement Workers. See [the readiness contract](../core/pull-workers.md#startup-registration-and-readiness) and [SDK parity tracking](https://github.com/superdurable/dex/issues/728).

Set `credential(...)` separately on ClientOptions and WorkerOptions when public API authentication is enabled. `FileCredential` reads a rotating token file; a custom `Credential.token()` supplies the current bearer token for each call and pull stream opening. A credential error prevents the call from reaching Dex. Protect the plaintext connection. [Released Java credential API](https://github.com/superdurable/dex/blob/sdk-java/v1.12.0/sdk-java/src/main/java/io/superdurable/dex/FileCredential.java)

## Connector runtime support

The Connector SDK's `projectconfig.LoadFromEnvironment` loader and the generated `NewProjectConnection` API are Go-only, and no equivalent Java SDK exists today. Do not invent a Java loader or deserialize Dex Web's credential file into Flow state. For a Java application, keep provider credentials behind an application-owned secret/runtime adapter until an official Java Connector SDK exposes the same contract.

## Raw Temporal payload inspection

Use **dexcli codec-server** with CLI v1.4.2 or later to inspect Dex internal payloads
in Temporal Cloud or a local Temporal Web UI. Follow the shared
[codec-server guidance](../core/operations.md#inspect-raw-temporal-payloads);
local UI ports need no additional server arguments.

## Deployed configuration and recovery boundary

Dex provides no deployed Connector configuration or credential store. A deployed Go
application reads its connections with the Connector SDK's `sdkgo/projectconfig`, but
no equivalent Java SDK exists today; see
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

The current FDG 2.0 management-interface analyzer reads Go source only. A Java
Flow cannot currently publish the Summary, Display, Action, Indexed Attribute,
group, explanation, or Action input capture metadata that drives the full Dex
Web v2 management interface. Java applications can still operate Flows and use
their supported Client and CLI surfaces; do not invent Java annotations for the
Go-only analyzer contract.

## Minimal Flow

[Pinned runnable source](https://github.com/superdurable/dex/blob/sdk-java/v1.12.0/examples/java/src/main/java/io/superdurable/dex/primitives/flow/ExampleFlow.java)
<!-- dex-source: examples/java/src/main/java/io/superdurable/dex/primitives/flow/ExampleFlow.java -->
```java
@Component
public class ExampleFlow implements Flow<Integer> {
    public static final Attribute<String> status = Attribute.define("status", String.class);
    public static final Channel<Void> notify = Channel.define("notify", Void.class);

    private final ExampleStep exampleStep = new ExampleStep();
    private final Finish finishStep = new Finish();

    @Override
    public StepList<Integer> getSteps() {
        return StepList.startStep(exampleStep).otherSteps(finishStep);
    }

    @Override
    public PersistenceSchema getPersistenceSchema() {
        return PersistenceSchema.of(status, notify);
    }
```

A Flow returns its complete Step registry once. The first Step input type must match `Flow<I>`. All persisted definitions belong in `getPersistenceSchema()`.

[Pinned Step source](https://github.com/superdurable/dex/blob/sdk-java/v1.12.0/examples/java/src/main/java/io/superdurable/dex/primitives/flow/ExampleFlow.java)
<!-- dex-source: examples/java/src/main/java/io/superdurable/dex/primitives/flow/ExampleFlow.java -->
```java
final class ExampleStep implements Step<Integer> {
    @Override
    public Class<Integer> getInputType() {
        return Integer.class;
    }

    @Override
    public Wait waitFor(final Context context, final Integer input) {
        status.set(context, "running");
        return Wait.skipImmediately();
    }

    @Override
    public StepDecision execute(final Context context, final Integer input) {
        return StepDecision.goTo(Finish.class, input + 1);
    }
}
```

## Registry, Worker, and Client

[Pinned bootstrap source](https://github.com/superdurable/dex/blob/sdk-java/v1.12.0/examples/java/src/main/java/io/superdurable/dex/config/DexConfig.java)
<!-- dex-source: examples/java/src/main/java/io/superdurable/dex/config/DexConfig.java -->
```java
@Bean
public Registry registry(final List<Flow<?>> flows) {
    return new Registry(new ArrayList<Flow<?>>(flows));
}

@Bean(destroyMethod = "close")
public BlobCache blobCache(
        @Value("${dex.blob-cache-dir}") final String blobCacheDir) {
    return BlobCache.open(new BlobCacheConfig(blobCacheDir, 1L << 30));
}
```

Create the Worker before the Client so the Client can use `worker.getWorkerTarget()`. Start the blocking Worker on a managed thread and close Worker, Client, and BlobCache during application shutdown. Do not create a cache per request.

Add `io.superdurable:dex-sdk:<resolved-version>` to Gradle or Maven. Application adapters call the injected Client to start a registered Flow, invoke typed RPC stubs for Flow-owned state, wait for Attribute matches, or wait for terminal status.

## Run locally

```bash
dexcli dev
cd examples/java
./gradlew bootRun
```

Use `./run-integration-tests.sh` for the official application suite. Defaults connect to `localhost:8801`; configure the server address, Worker bind/target, and cache directory with the example environment variables.

## Continue reading

- [Primitives](primitives.md) for application building blocks.
- [Patterns](patterns.md) before selecting a Flow shape.
- [Errors](error-handling.md) before writing Client, admission, RPC, cleanup, wait, or external Stream boundaries.
- [Testing](testing.md) before declaring durable behavior complete.
- [Data](data-handling.md), [observability](observability.md), and [versioning](versioning.md) for production behavior.
- [Gotchas](gotchas.md) and [advanced features](advanced-features.md) for constraints that are easy to miss.
