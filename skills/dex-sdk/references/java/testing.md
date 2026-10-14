# Testing Java Dex applications

Durability claims require a real Dex Server. Unit tests are useful only for pure business helpers and codecs.

## Integration harness

For Dex repository work, use the repository-only `DexDevTestEnvironment` or `examples/java/run-integration-tests.sh`. Do not import `DexDevTestEnvironment` from an application; it is not part of the published SDK. In an application repository, start a real Dex Server, reuse the application's public Registry, BlobCache, Client, and Worker bootstrap in the test fixture, and run the application's Gradle or Maven integration-test task. Register only the Flows under test and generate a unique Flow ID for every test so histories cannot collide.

[Pinned integration example](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/sdk-java/src/test/java/io/superdurable/dex/integ/TimerTest.java)
<!-- dex-source: sdk-java/src/test/java/io/superdurable/dex/integ/TimerTest.java -->
```java
try (DexDevTestEnvironment environment = DexDevTestEnvironment.start(
        cacheDirectory,
        WORKFLOW)) {
    final String flowId = "basic-timer-" + UUID.randomUUID();
    final long startedAt = System.nanoTime();
    environment.client().startFlow(WORKFLOW, flowId, 5);
    environment.client().waitForStepCompletion(
            flowId,
            StepExecutionId.of("TimerStep"),
            waitOptions(flowId, Duration.ofSeconds(10)));
    environment.client().waitForFlow(flowId);
```

The snippet demonstrates lifecycle and assertions inside the Dex repository. Copy the application's public bootstrap shape instead of this private fixture type.

## Required scenarios

1. Happy path: start, interact, wait for terminal status, and assert typed output.
2. Worker replacement: stop the Worker while the Flow is waiting or retrying, start a replacement with the same Registry, and prove progress resumes.
3. Retry exhaustion: make `waitFor` and `execute` fail deterministically, assert the configured recovery or terminal failure, and verify attempt-sensitive behavior.
4. Channel and RPC: publish before and after a wait registers; invoke concurrent RPCs when locks matter; verify FIFO and typed results.
5. Timer: assert it does not finish early with a tolerant upper bound. Use server waits or deadline polling, not `Thread.sleep` for convergence.
6. Terminal behavior: RPC/publish after completion must produce the expected concrete exception; a bounded `waitForFlow` timeout is not a terminal result.
7. SubFlow and cancellation: prove parent completion policy, child outcome propagation, and loser cleanup.
8. Stream: assert source and resume behavior while keeping authoritative assertions on Attributes or Flow output.

## Deadline polling

[Pinned polling helper](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/sdk-java/src/test/java/io/superdurable/dex/integ/IntegrationTestWaits.java)
<!-- dex-source: sdk-java/src/test/java/io/superdurable/dex/integ/IntegrationTestWaits.java -->
```java
final long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
DexServiceException lastFailure = null;
while (System.nanoTime() < deadline) {
    try {
        client.skipTimer(flowId, stepExecutionId, timerId);
        return;
    } catch (DexServiceException failure) {
        if (!failure.getDetail().contains(
                "timer condition does not exist or is not pending")) {
            throw failure;
        }
        lastFailure = failure;
        Thread.yield();
    }
}
throw new AssertionError("timer condition was not registered", lastFailure);
```

Keep deadlines short but non-flaky. Preserve the last failure to make timeouts diagnosable. Do not skip failures by backend or retry indefinitely.

## Commands

```bash
cd examples/java
./run-integration-tests.sh
```

For SDK work, use the repository's documented Gradle integration tasks and a real `dexcli dev`; do not substitute mocks for Worker replacement or durable waiting.
