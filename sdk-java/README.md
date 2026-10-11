# dex-sdk (Java)

## Flow state I/O

Applications read and write Flow state through typed RPCs. The Client does not
expose direct Attribute reads or writes, Channel publication, or pending-message
mutation. This keeps each external state transition behind a Flow-owned method.

An RPC can stage `channel.delete(context, messageId)`. Annotate it with
`@RPC(isTransactional = true)` when a missing ID must abort every other RPC
write. Attribute locks already select transactional execution, but Channel
deletion does not do so implicitly.

## RPC state loading

RPCs receive ordinary Attributes and all Channel size metadata by default.
AttributeMap entries and pending Channel messages are opt-in:

```java
@RPC(
    loadAttributeMapInstances = {"Items/tenant-a"},
    loadChannels = {"Queued"},
    loadChannelMaps = {"ByTenant"}
)
public RPCResult<Snapshot> snapshot(Context context) {
    return RPCResult.of(new Snapshot(queued.pendingMessages(context)));
}
```

Put a map name in **loadAttributeMaps** or **loadChannelMaps** to load every
current instance. Use the singular instance options for the less common exact
instance case. Text after the first slash is the logical instance and the SDK
escapes it. A selected empty queue returns an empty list; reading an unselected
map entry throws **AttributeMapNotLoadedException**. An unselected pending-message
snapshot throws **ChannelMessagesNotLoadedException**. Pending messages preserve FIFO order and
include the server-assigned message ID. The
snapshot does not change after the handler stages a publish or deletion.

Loading controls which data reaches the Worker. **isTransactional** controls
atomic commit and Channel deletion validation. Attribute locks add isolation
only among cooperating Steps and RPCs using the same lock. Write-only publish
and delete operations do not require loading.

### Invocation-time map instances

`@RPC` remains the registration-time contract for timeout, transactionality,
locks, and fixed state loads. Use `RPCInvokeOptions` when the caller chooses an
exact map instance at runtime:

```java
RPCInvokeOptions options = RPCInvokeOptions.newBuilder()
        .addLockAttributeMapInstance(customerProfiles, partitionName)
        .addLoadAttributeMapInstance(customerProfiles, partitionName)
        .build();
CustomerProfile result = client.invokeRPC(
        directoryRpc::upsertCustomerProfile,
        profile,
        options);
```

The four function/procedure and input/no-input `invokeRPC` families accept these
options. Selections are additive, sorted, and deduplicated. Locking and loading
are independent; read-modify-write handlers need both for the same instance.

See the runnable [sequential chunking](../examples/java/src/main/java/io/superdurable/dex/patterns/sequentiallychunkedattributemap)
and [hash partitioning](../examples/java/src/main/java/io/superdurable/dex/patterns/hashpartitionedattributemap)
patterns for complete call sites.

## Step and timeout-handler state loading

`StepOptions` provides the same five selections independently for `waitFor` and
`execute`: all AttributeMap instances, exact AttributeMap instances, Channels,
all ChannelMap instances, and exact ChannelMap instances. The methods receive
independent snapshots. Execute reads after the winning Wait consumes messages;
retries of one logical method call reuse its first snapshot.

`FlowTimeoutHandlerOptions` provides Execute-style timeouts, heartbeat detection,
retry, durability, Attribute locks, and the same state selections. Set it on
`StartFlowOptions` or `SubFlowOptions` only for a positive timeout using `HANDLER`.
Exhausted retries may proceed to a registered `Step<Void>`; read the final
failure with `Context.getRecoveryError()`.

Java SDK for [Dex workflow engine](https://github.com/superdurable/dex)

See [samples](../examples/java) for how to use this SDK to build your workflow.

Maven coordinates: `io.superdurable:dex-sdk` (namespace for domain [superdurable.io](https://superdurable.io)).

## New user contracts

The canonical rewrite API is under `io.superdurable.dex`. This phase includes
strongly typed workflow definitions, persistence handles, registry
validation, synchronous gRPC Client operations, and a Java gRPC Worker.
Rust is used only by the DXBC BlobCache JNI binding.

Java workflows and steps are interfaces, while RPCs keep the annotation and
typed-stub model:

```java
public final class IncrementStep implements Step<Long> {
    @Override
    public Class<Long> getInputType() {
        return Long.class;
    }

    @Override
    public StepDecision execute(Context context, Long input) {
        return StepDecision.gracefulComplete(input + 1L);
    }
}

public class CounterFlow implements Flow<Long> {
    private final IncrementStep start = new IncrementStep();

    @Override
    public StepList<Long> getSteps() {
        return StepList.startStep(start);
    }

    @RPC(name = "increment", timeoutSeconds = 10)
    public RPCResult<Long> increment(Context context, Long amount) {
        return RPCResult.of(amount + 1L);
    }
}

CounterFlow stub = client.newRpcStub(CounterFlow.class, flowId, runId);
long value = client.invokeRPC(stub::increment, 1L);
```

Flows exposing RPC methods and their annotated methods must not be `final`.
RPC stubs intercept those methods without invoking Flow constructors.
In Kotlin, declare the Flow class and its RPC methods with `open`.

`Step<I>` declares `Class<I> getInputType()`; parameterized Step inputs are not
supported. Steps, attributes, and channels declare Java classes, not codecs:

```java
Attribute<String> status = Attribute.define("status", String.class);
Channel<Void> wakeup = Channel.define("wakeup", Void.class);
```

Wait factories read from the domain nouns they create:

```java
return Wait.until(
        Timer.byDuration(Duration.ofSeconds(1)));
```

`StepOptions.waitForMethodTimeout(...)` and `executeMethodTimeout(...)` bound
the two handler calls. Timer and channel conditions determine how long a Step
waits.

`waitForRetry(...)` and `executeRetry(...)` limit one logical handler
execution. With asynchronous Step durability, local and fallback regular
activities share maximum attempts, total duration, and 1-based attempt numbers.
Fallback starts immediately; later regular retries continue the backoff
sequence at the cumulative attempt.

Dex defaults Step methods to SYNC durability, a four-hour retry duration, a
two-hour regular attempt timeout, and a one-minute heartbeat timeout. A Step
method durability override wins over `FlowConfig.stepDurability(...)`, which
wins over SYNC. Failure policies do not change that order. ASYNC first uses a
local activity for at most seven seconds and three attempts; the local phase
ignores method and heartbeat timeouts before regular-activity fallback.

### Step progress and Streams

Dex does not heartbeat a Step automatically. Long-running Step handlers should
emit progress before the configured heartbeat timeout:

```java
@Override
public StepDecision execute(Context context, Batch batch) {
    int start = context.hasLastHeartbeatValue()
            ? context.getLastHeartbeatValue(Integer.class)
            : 0;
    for (int index = start; index < batch.items().size(); index++) {
        process(batch.items().get(index));
        context.recordHeartbeat(index + 1);
        progress.write(context, "processed " + (index + 1));
    }
    return StepDecision.gracefulComplete();
}
```

`recordHeartbeat(value)` persists a nonnull checkpoint for a later regular
activity attempt. `recordHeartbeat(null)` sends a heartbeat without details and
clears the current checkpoint. ASYNC local activities ignore heartbeat frames.
A Stream write is an implicit heartbeat that preserves the latest explicit
heartbeat details.

Step Stream writes travel on the Worker response stream and are fire-and-forget.
One handler may write the same Stream any number of times. Dex assigns
`#<stepExecutionID>` as source metadata. Storage rejection does not fail the
handler, although local validation, serialization, and Worker transport errors
can still fail it.

For text deltas, create one invocation-managed writer and pass its `write`
method to the producer:

```java
BufferedTextStream progress = BufferedTextStream.create(context, thinking);
progress.write(delta);
```

It flushes after one second, at a soft 16 KiB UTF-8 threshold, or before the
final result or error. The configuration overload accepts a `Duration` and byte
threshold. It preserves chunks exactly, and an empty buffer does not emit a
message or heartbeat. Retry does not restore unsent text or deduplicate batches.

Flow timeout handlers and RPCs cannot send heartbeat or Stream progress.

`Client.writeStream(flowId, stream, source, value)` appends on every call.
Sources are required metadata, may repeat, and may contain `#`. Read them with
`StreamMessage.getSource()`; they do not provide deduplication.

`Client.readStream` moves forward one message at a time and can long-poll.
`Client.listStreamMessages` returns a non-blocking newest-first retained page.
Pass the registered typed `Stream`, a positive page size, and an empty token for
the first page. Pass `StreamMessagesPage.getNextPageToken()` unchanged to read
older messages until it is empty. The server caps page size at 1000 by default.
A trimmed anchor returns an empty page, and newer concurrent writes do not enter
an existing older-page chain.

### Soft Flow timeout

Override `Flow.handleTimeout` to make a positive timeout use handler policy by
default. Dex invokes it as an Execute-only system Step after its durable timer
fires or is skipped:

```java
@Override
public StepDecision handleTimeout(Context context) {
    return StepDecision.forceComplete("expired");
}

StartFlowOptions options = StartFlowOptions.newBuilder()
        .timeout(Duration.ofMinutes(30))
        .timeoutPolicy(FlowTimeoutPolicy.HANDLER)
        .build();
```

`FAIL` produces `FlowErrorType.FLOW_TIMEOUT` and permits Flow retry; `CANCEL`
cancels without retry. `SubFlowOptions` exposes the same `timeoutPolicy(...)`
override. Continue-as-new preserves the deadline and handler execution, while
retry runs receive a fresh budget. A zero or absent timeout disables
the feature.

### Canceling Step executions

A successful Step can cancel queued or active executions by registered Step
type while still choosing its normal next or close action:

```java
@Override
public StepDecision execute(Context context, Quote quote) {
    return StepDecision.goTo(RecordQuote.class, quote)
            .withCancelingSiblingSteps(QuoteCarrierA.class, QuoteCarrierB.class);
}
```

`withCancelingSteps(...)` selects every queued or active execution of each Step
type in the current Flow. `withCancelingSiblingSteps(...)` selects only
executions whose `Context.getFromStepExecutionId()` is the same as the current
execution's source. Both methods are immutable: repeated calls form a union,
duplicate registered Step classes are removed, and a Flow-wide selector supersedes a
sibling selector for the same Step type. Passing an unregistered or null Step class
causes an invalid Step result.

Dex resolves selectors from a snapshot after the current execution succeeds.
Executions already completed, already canceled, or absent are no-ops. Next
Steps created by the same decision are outside that snapshot. Dex cancels the
selected executions in workflow state and immediately applies the decision's
next or close action; it does not wait for selected Java handlers to return.
Their late decisions, Attribute and Channel writes, outputs, retry paths, and configured
failure-proceed Steps are discarded.

An RPC can apply a Flow-wide Step type selector while returning its output and
scheduling next Steps atomically:

```java
@RPC(name = "acceptQuote")
public RPCResult<QuoteStatus> acceptQuote(Context context, Quote quote) {
    return RPCResult.of(
                    QuoteStatus.ACCEPTED,
                    StepMovement.of(recordQuote, quote))
            .withCancelingSteps(carrierA, carrierB, globalQuoteTimeout);
}
```

`RPCResult` does not support sibling selection because separate invocations of
the same RPC name do not share an invocation-scoped Step execution lineage.
Attribute and Channel writes from the RPC commit before the cancellation
snapshot, while next Steps in the same result remain outside it.

An ASYNC local activity keeps the current backend Workflow Task open until it
returns or reaches Dex's seven-second local optimization timeout. This can delay
another Step from starting its durable timer, or delay processing a timer that
already elapsed. An already-started timer keeps its deadline. Use SYNC durability
when cancellation must reach a running handler close to the intended deadline.
After an ASYNC local timeout, Dex falls back to a regular activity; a backend
error without local attempt metadata conservatively consumes one attempt and the
elapsed local duration before fallback.

`ClientOptions` and `WorkerOptions` contain a default Jackson `ObjectMapper` and
accept a configured mapper when needed. Java does not expose a public Codec API.
Workers use a builder so optional transport settings remain readable:

```java
WorkerOptions options = WorkerOptions.newBuilder()
        .bindAddress(":8803")
        .serverAddress("localhost:8801")
        .objectMapper(objectMapper)
        .grpcErrorStatusMapping(GrpcErrorStatusMapping.newBuilder()
                .map(PaymentDeclinedException.class, Status.Code.FAILED_PRECONDITION)
                .build())
        .build();
```

`bindAddress` is the Java WorkerService listener. `serverAddress` is used only
to hydrate blob-backed values through Dex FlowService. `workerTarget` is the
address advertised in Flow configuration; when omitted, Worker derives it from
the bind address and exposes it through `worker.getWorkerTarget()`.

Serialized object values use wire encoding `json` for JSON and `raw` for raw bytes.
Internal Blob references are opaque. Hydration sends the owning Flow ID with
each reference, and the local cache is isolated by `(flowId, blobRef)`.

Every `Throwable` escaping application Step `waitFor`, Step `execute`, or RPC
code becomes a structured Worker error. The default gRPC status is `INTERNAL`,
including when application code throws `StatusException` or
`StatusRuntimeException`. Configure `GrpcErrorStatusMapping` only for exception
classes with a stable meaning to the owner of the Worker application. The most
specific mapped superclass wins; an unmapped class remains `INTERNAL`. These
diagnostic statuses do not change Step retry or failure policies. Capacity and
other Worker transport failures retain their own SDK-selected statuses.

Java Worker errors include the original exception type, detail, and stack trace,
including causes and suppressed exceptions. To keep the encoded worker failure
within transport limits, Dex persists at most 256 bytes of type, 1 KiB of detail,
and 4 KiB of UTF-8 stack trace. Each truncated field receives a marker without
splitting a character. The Worker log keeps the complete local stack.

Failure-policy recovery exposes the previous method error through
`context.getRecoveryError()`. Wait-for recovery passes the error directly to
execute in the same run. Execute recovery passes it to both methods of the
configured recovery Step, including when that Step starts after
continue-as-new:

```java
RecoveryErrorInfo error = context.getRecoveryError();
if (error != null) {
    logger.warn("Recovering from " + error.getErrorType() + ": " + error.getDetail());
}
```

To override the next Temporal retry interval, throw a retry request with the
current attempt failure as its cause:

```java
throw RetryAfterException.after(Duration.ofSeconds(30), currentFailure);
```

The delay must be a positive whole number of seconds in the signed 32-bit
range. Retry limits and method timeouts remain unchanged. Cadence rejects a
dynamic retry interval with an `INVALID_USER_FLOW_CODE` validation error.
Recovery exposes only the final failure type and detail; stack traces remain in
failure history, and retry delays are scheduling metadata.

`Flow<StartInput>.getSteps()` returns `StepList<StartInput>`. Start with
`StepList.startStep(step)` and append heterogeneous Steps with `otherSteps(...)`.
Use `StepList.empty()` when a Flow has no Steps, or
`StepList.withoutStartStep(...)` when every Step is RPC-triggered. The generic
binding catches start-input mismatches during compilation; Registry also
validates the runtime classes with
`inputType.isAssignableFrom(registeredType)`.
`waitForFlow` returns every output-bearing Step completion after hydrating all
blob-backed values. Decode a single-output Flow with the strict helper:

```java
String output = client.waitForFlow("flow-123").getSingleOutput(String.class);
```

For multiple outputs, select by Step type or Step execution ID and decode each
completion independently:

```java
FlowResult result = client.waitForFlow("flow-123");
for (StepCompletion completion : result.getCompletions()) {
    if (completion.getStepType().equals("ChargeCard")) {
        Receipt receipt = completion.getOutput(Receipt.class);
    }
}
```

The completion list is immutable and preserves server collection order, but
parallel branch order is not deterministic. `getSingleOutput` throws when the
Flow has zero or multiple outputs. A no-output Flow returns an empty list.

## Current API conventions

Step executions use `StepExecutionId.of(stepType)` or
`StepExecutionId.of(stepType, executionNumber)`. Constructors are intentionally
not part of the public API.

`Wait.until`, `Wait.allOf`, and `Wait.anyOf` use unnamed Conditions by default.
Do not add Condition IDs merely because a Condition is nested in one of these
waits. Every Condition in `Wait.anyCombinationOf` must have a non-empty user ID;
the same Condition object may be reused across combinations.

`SubFlow.run(ChildFlow.class, input)` declares a normal Flow execution as a
durable condition. Read it during `execute` with
`SubFlow.getConditionResults(context, index)`. `allOf` results are terminal;
an unfinished `anyOf` loser is a `RUNNING` snapshot. Obtain its deterministic
Flow ID with `SubFlow.getFlowId(context, index)` before passing it to
`Client.stopFlow`. Continue-as-new retains only each SubFlow Condition ID and
stable list position, not its starting input or options. SubFlows continue when
their parent closes.

If the server cannot start a SubFlow within its retry budget, the waiting Step's
`waitForFailure` policy decides the outcome. The default fails the parent Flow.
With `WaitForFailurePolicy.PROCEED`, `execute` runs and the SubFlow result
reports `FlowStatus.FAILED` with `FlowErrorType.SUB_FLOW_START_FAILED`.

`client.waitForAttributeMatch(...)` overloads cover singleton Attributes and
AttributeMap instances in the current run. Build a match with one of the six
`AttributeMatch` factories. The method returns the decoded matched value.
String and boolean support equality operators; integers and floating-point
values support every operator. Every AttributeMap and ChannelMap instance must
be nonblank and must not contain `/`. Objects, bytes, null, non-finite doubles,
and invalid ordering fail locally. `AttributeMap.getMapSize/getAllInstanceKeys` reflect
buffered sets and deletes. The corresponding `ChannelMap` methods are RPC-only,
include buffered publishes, and omit empty instances. Returned keys are decoded
and sorted. Conditional completion is
`StepDecision.forceCompleteIfChannelsEmpty(...)`.

Request IDs are optional for both durable waits. When omitted, the server
derives a stable ID from the Step execution or Attribute condition. Reuse an
override only for the same wait. `requestTimeout` is the total SDK call budget
across transparent transport reattachments. Zero waits indefinitely.
Temporal permits 10 in-flight Updates and 2,000 total Updates in History per
Workflow Execution. A caller timeout or cancellation does not finish an
accepted durable wait, so an abandoned handler can retain one in-flight slot.
Set `internalHandlerTimeout` only when abandoned waits can approach that limit.
An active call transparently starts another generation, which counts toward the
2,000-Update history limit. Prefer a value comfortably longer than normal
request timeouts and reconnect gaps. Zero disables timed rollover. Request
expiry throws `RequestTimeoutException`.

## Exceptions

Public exceptions live in `io.superdurable.dex.exceptions`. Catch concrete
types for expected outcomes instead of comparing `ErrorSubStatus`:

```java
try {
    client.invokeRPC(stub::updateOrder, input);
} catch (FlowNotActiveOrNotFoundException inactive) {
    // The Flow never existed or is already closed.
} catch (RpcLockConflictException conflict) {
    // Retry the RPC after lock contention.
} catch (WorkerInvocationException workerFailure) {
    log.error(
            "Worker {} failed: {}",
            workerFailure.getWorkerErrorType(),
            workerFailure.getWorkerErrorDetail());
}
```

`FlowNotFoundException` is returned by read and history operations such as
`describeFlow`, `waitForFlow`, and `timeTravel`. These operations
can read a closed Flow, so failure means no matching execution was found.
`FlowNotActiveOrNotFoundException` is returned by RPC, stop, timer,
configuration, and step-wait operations that require an open Flow.

RPCs also use this error when the target Flow cannot be found, including query-only
reads. A query-only RPC without locks, transactional execution, durable effects, or
Server-forced Update routing can read a retained terminal execution. At that confirmed
read boundary, this error means no readable target was found; return the application's
missing or unavailable result directly, without a lifecycle probe, a short timeout, a
retry, or historical Step-output decoding. Preserve other service and Worker failures.
For mutations and active-only RPCs, the error can mean either missing or closed and does
not prove that the requested action succeeded. RPC success does not prove that the Flow
is active.

`FlowAlreadyStartedException` identifies duplicate starts.
`LongPollTimeoutException` identifies an expected long-poll timeout.
`RequestTimeoutException` identifies an exhausted caller-defined request
budget.
`Client.waitForFlow` returns `FlowResult` for successful and unsuccessful
terminal statuses. Inspect `getStatus`, `getErrorType`, and `getErrorMessage`
before decoding outputs from an unsuccessful result.
`WorkerInvocationException` preserves the original WorkerService error type,
detail, gRPC code, and Java stack trace. Read the persisted trace with
`getWorkerStackTrace()`; it may be empty for Workers implemented by another SDK.
`DexServiceException` remains the generic service failure and exposes status
metadata for diagnostics.

Local definition and value failures use `FlowDefinitionException`,
`InvalidStepResultException`, and `ValueMappingException`.

Persistence schemas use factory methods instead of builders. Definitions may
be passed directly, as one mixed list, or as separate attribute and channel
lists:

```java
PersistenceSchema.of(counter, commands);
PersistenceSchema.of(definitions);
PersistenceSchema.of(attributes, channels);
```

Initial values bind each persistence definition to its Java value type directly
in `StartFlowOptions.Builder`:

```java
StartFlowOptions options = StartFlowOptions.newBuilder()
        .addAttribute(status, "created")
        .addAttribute(items, "order-1", 1)
        .build();
```

Attribute Store synchronization is definition-level and immutable:

```java
Attribute<String> email = Attribute.define("customer-email", String.class)
        .syncToAttributeStore();
FlowConfig config = FlowConfig.newBuilder()
        .attributeStoreNames("profiles", "audit")
        .build();
```

Stores are asynchronous latest-state projections. Every enabled Attribute write
is sent to every selected Store. Deletion writes SQL `NULL`, and projection
failures do not roll back Flow Attributes. Omitting `attributeStoreNames`
preserves current targets; `attributeStoreNames()` disables future
synchronization with protocol presence.

The IWF integration inventory is implemented as real Dex E2E tests under
[`src/test/java/io/superdurable/dex/integ`](src/test/java/io/superdurable/dex/integ/README.md).
They run Java Client and Java Worker against `dexcli dev`, with Rust used only
for BlobCache. They cover flows, steps, RPCs, persistence, time travel, timers,
failure modes, and options.

## Runtime architecture

Java owns Registry reflection, FlowService client transport, WorkerService gRPC
transport, callback dispatch, and exception mapping. Worker requests never
cross JNI. Synchronous Step and RPC handlers run on a bounded JVM executor so
they do not block gRPC event-loop progress. Java failures are logged with their
complete stack and returned with their Java type, message, mapped status, and
bounded persisted stack.

Client and Worker accept the Java `BlobCache` interface. The default
`BlobCache.open(...)` implementation is the shared Rust DXBC cache. Blob-backed
values are hydrated in Java: cache hits are decoded locally, while misses use
FlowService `LoadBlobs` and populate the cache.

The Maven artifact includes BlobCache native libraries for Linux x86-64 and
ARM64, macOS x86-64 and ARM64, and Windows x86-64. The SDK extracts the matching
library from `META-INF/native` automatically. Use the absolute path in
`-Ddex.blobCache.nativeLibrary=...` only to override the packaged library.
Application callbacks, Registry, Client, and Worker remain usable with another
`BlobCache` implementation and do not load JNI.

Measure warm cache hits and identical-value puts across JNI with an optimized
native library:

```bash
./gradlew blobCacheBenchmark -PblobCacheBenchmarkThreads=8
```

## License

[Sustainable Use License 1.0](LICENSE), with legacy portions under their
original terms as described in [LEGACY_NOTICES.md](LEGACY_NOTICES.md).

## Requirements

- Java 1.8+

## How to use

After publish, artifacts appear on
[Maven Central](https://repo1.maven.org/maven2/io/superdurable/dex-sdk/)
(and on [MVN Repository](https://mvnrepository.com/artifact/io.superdurable/dex-sdk) with some delay).
Javadoc: [javadoc.io](https://www.javadoc.io/doc/io.superdurable/dex-sdk/latest/index.html).

### Gradle

```gradle
implementation 'io.superdurable:dex-sdk:0.0.3'
```

### Maven

```xml
<dependency>
    <groupId>io.superdurable</groupId>
    <artifactId>dex-sdk</artifactId>
    <version>0.0.3</version>
</dependency>
```


## Concepts

Applications implement [`Flow<I>`](src/main/java/io/superdurable/dex/Flow.java)
and [`Step<I>`](src/main/java/io/superdurable/dex/Step.java). A Flow declares
all registered Steps and the optional start marker in one `getSteps()` list,
plus its persistence schema and annotated RPC methods. Registry validates these
definitions before Client or Worker startup.

## How to build & run

### Using IntelliJ

1. Protobuf IDL lives in monorepo [`protos/dex.proto`](../protos/dex.proto) (no submodule checkout needed).
2. In "Build, Execution, Deployment" -> "Gradle", choose "wrapper task in Gradle build script" for "Use gradle from".
3. Open Gradle tab, click "build" under "build" to build the project

## Development Guide

### Update IDL

Edit [`protos/dex.proto`](../protos/dex.proto), then run `make -C ../protos proto` to refresh checked-in stubs under `src/main/java/io/superdurable/gen/`.

### Local testing

Run the JVM/native integration tests and all E2E scenarios against a fresh
`dexcli dev` environment:

```shell
./run-integration-tests.sh
```

The script builds `dexcli` from the current checkout and starts an isolated
Dex environment. Each Worker automatically synchronizes its registered Indexed
Attributes before listening. Gradle builds
the Rust BlobCache JNI library and starts a fresh Java Worker for each E2E case
with a unique worker port and flow ID. A clean checkout also requires Go 1.24+,
Node.js 22+, Rust 1.97+, and Temporal CLI.

### Measure integration coverage

Run the same integration suite with JaCoCo coverage:

```shell
./run-integration-tests.sh --coverage
```

The report combines the local JNI and WorkerService integration tests with all
`dexcli dev` E2E scenarios. It measures only production classes under
`io.superdurable.dex`; contract tests, other unit tests, test fixtures, and
generated protobuf classes are excluded. The XML report is written to
`build/reports/jacoco/integration/jacoco.xml`, and the browser report starts at
`build/reports/jacoco/integration/html/index.html`.

### Validate the publication

The validation command builds a Maven repository, verifies its JAR and POM,
then compiles and runs independent Gradle and Maven consumers. Both consumers
open the packaged Rust BlobCache library and perform a cache round trip.

```shell
./validate-publication.sh 0.0.3-local
```

The command requires JDK 17+ (to run Gradle 9), Rust 1.97+, and Maven. Build
and test JDKs are Gradle toolchains downloaded automatically. Published
classes still target Java 8.

### Validate public API documentation

Every hand-written public API under `io.superdurable.dex` must have detailed
Javadoc. The documentation check ignores package-private runtime code and
generated protobuf classes, validates parameter and return tags, and renders
the same documentation shipped in the Maven Javadoc JAR:

```shell
./gradlew checkstyleMain javadoc --no-daemon
```

Open `build/docs/javadoc/index.html` to review the generated documentation.

### Publish a release

Maven Central publishing requires Portal user-token credentials for the
verified `io.superdurable` namespace and an ASCII-armored GPG private key. Set
the repository secrets `MAVEN_CENTRAL_USERNAME`, `MAVEN_CENTRAL_PASSWORD`, and
`GPG_SECRET_KEY`.

Publish a GitHub release whose tag is `sdk-java/vX.Y.Z`. The workflow derives
the immutable Maven version from that tag, builds all supported native
libraries, validates Gradle and Maven consumers, signs every artifact, and
closes and releases the Central staging repository. Manual dispatch requires
the same `X.Y.Z` version as an explicit input.

After the workflow succeeds, wait for the version and its artifacts to appear
in the canonical [Maven Central directory](https://repo1.maven.org/maven2/io/superdurable/dex-sdk/)
before updating examples or downstream dependencies. Secondary indexes such as
MVN Repository and Javadoc may become available later.

If you'd like to test your changes to the SDK with the workflows in the
[samples](https://github.com/superdurable/dex/tree/main/examples/java) repo,
use the local publishing command:

1. Run:
  ```
  ./gradlew publishToMavenLocal -PreleaseVersion=0.0.3-local
  ```

2. In the [samples](https://github.com/superdurable/dex/tree/main/examples/java)
   repo, depend on the same version passed with `-PreleaseVersion`. Then run:
  ```
   ./gradlew --refresh-dependencies build
  ```

3. Once you're done, to remove the locally published version, run:
  ```
  ./gradlew unpublishFromMavenLocal -PreleaseVersion=0.0.3-local
  ```

### Repo structure
* `.github/workflows/`: the GithubActions workflows
* IDL source lives in monorepo [`protos/dex.proto`](../protos/dex.proto) (see [`docs/design/idl-renames.md`](../docs/design/idl-renames.md))
* Generated stubs: `src/main/java/io/superdurable/gen/`
* `script/`: some scripts for GithubActions and testing
* `src/`: Java source code
  * `main/java/io/superdurable/dex/`: public SDK and Java runtime
  * `main/java/io/superdurable/gen/`: checked-in protobuf and gRPC stubs
  * `test/java/io/superdurable/dex/`: contract, transport, and integration tests

## Server protocol compatibility

Worker startup calls `GetServerInfo`, negotiates the highest common protocol,
synchronizes Attribute indexes, and then binds WorkerService. The initial Java
SDK interval is `[1,1]`. An absent RPC, invalid or nonoverlapping intervals, and
request failures stop startup before binding.

Published JARs store the diagnostic SDK version in the
`Implementation-Version` manifest attribute. Direct class-directory builds use
`dev`. The artifact version never participates in the compatibility decision.

## Search run inclusion

Dex Server excludes ContinuedAsNew runs by default. Application queries still
need `FlowType` when using shared index slots. An explicit inclusion option
removes only the default exclusion; it does not override query filters. Keep
the query, inclusion option, and page size consistent across pages.

Pass `SearchFlowsOptions.newBuilder().setIncludeContinuedAsNew(true).build()` to `searchFlows(query, pageSize, nextPageToken, options)` for execution-chain inspection. Existing overloads use the default.
