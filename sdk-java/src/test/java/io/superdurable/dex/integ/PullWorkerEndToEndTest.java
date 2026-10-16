/*
 * Portions of this file are derived from indeedeng/iwf-java-sdk.
 * Those portions are licensed under the Apache License, Version 2.0.
 * See LICENSES/Apache-2.0.txt and LEGACY_NOTICES.md.
 *
 * Modifications Copyright (c) 2026 Super Durable, Inc.
 *
 * Modifications are licensed under the Sustainable Use License 1.0.
 * Third-Party Materials remain under the Apache License, Version 2.0.
 * See LICENSE and LEGACY_NOTICES.md.
 */

package io.superdurable.dex.integ;

import io.superdurable.dex.BlobCache;
import io.superdurable.dex.BlobCacheConfig;
import io.superdurable.dex.Client;
import io.superdurable.dex.ClientOptions;
import io.superdurable.dex.Context;
import io.superdurable.dex.Flow;
import io.superdurable.dex.FlowConfig;
import io.superdurable.dex.PullWorkerOptions;
import io.superdurable.dex.Registry;
import io.superdurable.dex.RPC;
import io.superdurable.dex.RPCResult;
import io.superdurable.dex.Step;
import io.superdurable.dex.StepDecision;
import io.superdurable.dex.StepList;
import io.superdurable.dex.StartFlowOptions;
import io.superdurable.dex.Wait;
import io.superdurable.dex.Worker;
import io.superdurable.dex.WorkerOptions;
import io.superdurable.dex.WorkerTarget;
import io.superdurable.dex.testing.DexDevTestEnvironment;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("dex-dev")
public final class PullWorkerEndToEndTest {
    @TempDir
    Path cacheDirectory;

    @Test
    void routesStickyStepsRpcAndDrainWhilePushStillWorks() throws Exception {
        final String address = System.getProperty("dex.test.serverAddress", "127.0.0.1:8801");
        final BlockingQueue<String> observations = new LinkedBlockingQueue<String>();
        final CountDownLatch firstRelease = new CountDownLatch(1);
        final CountDownLatch secondRelease = new CountDownLatch(1);
        final AtomicInteger firstExecuteCalls = new AtomicInteger();
        final AtomicInteger secondExecuteCalls = new AtomicInteger();
        final PullFlow firstWorkerFlow = new PullFlow(
                "first", observations, firstRelease, secondRelease,
                firstExecuteCalls, secondExecuteCalls);
        final PullFlow secondWorkerFlow = new PullFlow(
                "second", observations, new CountDownLatch(0), new CountDownLatch(0),
                firstExecuteCalls, secondExecuteCalls);
        final ProbeFlow probeFlow = new ProbeFlow();
        try (PullRuntime first = new PullRuntime(cacheDirectory.resolve("first"), address,
                "java-pull-first-" + UUID.randomUUID(), firstWorkerFlow);
                BlobCache clientCache = BlobCache.open(new BlobCacheConfig(
                        cacheDirectory.resolve("client").toString(), 64L << 20));
                Client client = new Client(
                        new Registry(Arrays.<Flow<?>>asList(firstWorkerFlow, probeFlow)),
                        clientCache, new ClientOptions(address, WorkerTarget.pull()))) {
            final String flowID = "java-pull-" + UUID.randomUUID();
            client.startFlow(firstWorkerFlow, flowID, "done");
            assertEquals("first:wait-1", observations.poll(30, TimeUnit.SECONDS));
            assertEquals("first:execute-1", observations.poll(30, TimeUnit.SECONDS));
            try (PullRuntime second = new PullRuntime(cacheDirectory.resolve("second"), address,
                    "java-pull-second-" + UUID.randomUUID(), secondWorkerFlow, probeFlow)) {
                final String probeID = "java-pull-probe-" + UUID.randomUUID();
                client.startFlow(probeFlow, probeID, "probe");
                assertEquals("second", client.waitForFlow(probeID, Duration.ofSeconds(30))
                        .getSingleOutput(String.class));
                try (Client overrideClient = new Client(
                        new Registry(Arrays.<Flow<?>>asList(probeFlow)), clientCache,
                        new ClientOptions(address))) {
                    final String overrideFlowID = "java-pull-override-" + UUID.randomUUID();
                    overrideClient.startFlow(probeFlow, overrideFlowID, "probe",
                            StartFlowOptions.newBuilder()
                                    .configOverride(FlowConfig.newBuilder()
                                            .workerTarget(WorkerTarget.pull()).build())
                                    .build());
                    assertEquals("second", overrideClient.waitForFlow(
                            overrideFlowID, Duration.ofSeconds(30))
                            .getSingleOutput(String.class));
                }

                firstRelease.countDown();
                assertEquals("first:wait-2", observations.poll(30, TimeUnit.SECONDS));
                assertEquals("first:execute-2", observations.poll(30, TimeUnit.SECONDS));
                final PullFlow rpc = client.newRpcStub(PullFlow.class, flowID);
                assertEquals("first", client.invokeRPC(rpc::getWorker));
                assertEquals("first:rpc", observations.poll(30, TimeUnit.SECONDS));

                final CountDownLatch stopStarted = new CountDownLatch(1);
                final AtomicReference<Throwable> stopFailure = new AtomicReference<Throwable>();
                final Thread stopping = new Thread(() -> {
                    stopStarted.countDown();
                    try {
                        first.stop();
                    } catch (Throwable failure) {
                        stopFailure.set(failure);
                    }
                }, "dex-java-pull-e2e-stop");
                stopping.start();
                assertTrue(stopStarted.await(5, TimeUnit.SECONDS));
                assertTrue(stopping.isAlive());
                secondRelease.countDown();
                final String output = client.waitForFlow(flowID, Duration.ofSeconds(30))
                        .getSingleOutput(String.class);
                assertEquals("done", output);
                stopping.join(10_000L);
                assertFalse(stopping.isAlive());
                assertEquals(null, stopFailure.get());
                assertEquals(1, secondExecuteCalls.get());
                assertEquals(1, firstExecuteCalls.get());
            }
        }

        final PushFlow registered = new PushFlow();
        try (DexDevTestEnvironment push = DexDevTestEnvironment.startPush(
                cacheDirectory.resolve("push"), registered)) {
            final String flowID = "java-push-" + UUID.randomUUID();
            push.client().startFlow(registered, flowID, "push");
            assertEquals("push", push.client().waitForFlow(flowID, Duration.ofSeconds(30))
                    .getSingleOutput(String.class));
        }
    }

    @Test
    void reassignsUnregisteredRpcFromStickyWorker() throws Exception {
        final String address = System.getProperty("dex.test.serverAddress", "127.0.0.1:8801");
        final BlockingQueue<String> observations = new LinkedBlockingQueue<String>();
        final CountDownLatch firstRelease = new CountDownLatch(1);
        final AtomicInteger firstExecuteCalls = new AtomicInteger();
        final AtomicInteger secondExecuteCalls = new AtomicInteger();
        final OldPullFlow oldFlow = new OldPullFlow(observations, firstRelease,
                firstExecuteCalls, secondExecuteCalls);
        final PullFlow newFlow = new PullFlow("new", observations, new CountDownLatch(0),
                new CountDownLatch(0), firstExecuteCalls, secondExecuteCalls);
        try (PullRuntime oldWorker = new PullRuntime(cacheDirectory.resolve("old"), address,
                "java-pull-old-" + UUID.randomUUID(), oldFlow);
                BlobCache clientCache = BlobCache.open(new BlobCacheConfig(
                        cacheDirectory.resolve("client-unknown").toString(), 64L << 20));
                Client client = new Client(new Registry(Arrays.<Flow<?>>asList(newFlow)),
                        clientCache, new ClientOptions(address, oldWorker.worker.getWorkerTarget()))) {
            final String flowID = "java-pull-unknown-" + UUID.randomUUID();
            client.startFlow(newFlow, flowID, "complete");
            assertEquals("old:wait-1", observations.poll(30, TimeUnit.SECONDS));
            assertEquals("old:execute-1", observations.poll(30, TimeUnit.SECONDS));
            try (PullRuntime newWorker = new PullRuntime(cacheDirectory.resolve("new"), address,
                    "java-pull-new-" + UUID.randomUUID(), newFlow)) {
                assertEquals("new", client.invokeRPC(
                        client.newRpcStub(PullFlow.class, flowID)::getWorker));
                client.stopFlow(flowID);
                assertEquals(1, firstExecuteCalls.get());
                assertEquals(0, secondExecuteCalls.get());
            } finally {
                firstRelease.countDown();
            }
        }
    }

    @Test
    void reassignsUnregisteredStartingStep() throws Exception {
        final String address = System.getProperty("dex.test.serverAddress", "127.0.0.1:8801");
        final BlockingQueue<String> observations = new LinkedBlockingQueue<String>();
        final CountDownLatch released = new CountDownLatch(0);
        final AtomicInteger firstExecuteCalls = new AtomicInteger();
        final AtomicInteger secondExecuteCalls = new AtomicInteger();
        final OldPullFlow oldFlow = new OldPullFlow(observations, released,
                firstExecuteCalls, secondExecuteCalls);
        final NewStartPullFlow newFlow = new NewStartPullFlow(observations,
                firstExecuteCalls, secondExecuteCalls);
        try (PullRuntime oldWorker = new PullRuntime(cacheDirectory.resolve("old-step"), address,
                "java-pull-old-step-" + UUID.randomUUID(), oldFlow);
                BlobCache clientCache = BlobCache.open(new BlobCacheConfig(
                        cacheDirectory.resolve("client-step").toString(), 64L << 20));
                Client client = new Client(new Registry(Arrays.<Flow<?>>asList(newFlow)),
                        clientCache, new ClientOptions(address, oldWorker.worker.getWorkerTarget()))) {
            final String flowID = "java-pull-unknown-step-" + UUID.randomUUID();
            client.startFlow(newFlow, flowID, "complete");
            try (PullRuntime newWorker = new PullRuntime(cacheDirectory.resolve("new-step"),
                    address, "java-pull-new-step-" + UUID.randomUUID(), newFlow)) {
                assertEquals("complete", client.waitForFlow(flowID, Duration.ofSeconds(30))
                        .getSingleOutput(String.class));
                assertEquals("new:wait-2", observations.poll(30, TimeUnit.SECONDS));
                assertEquals("new:execute-2", observations.poll(30, TimeUnit.SECONDS));
                assertEquals(0, firstExecuteCalls.get());
                assertEquals(1, secondExecuteCalls.get());
            }
        }
    }

    public static class PullFlow implements Flow<String> {
        private final String workerID;
        private final BlockingQueue<String> observations;
        private final CountDownLatch firstRelease;
        private final CountDownLatch secondRelease;
        private final AtomicInteger firstExecuteCalls;
        private final AtomicInteger secondExecuteCalls;

        public PullFlow() {
            this("stub", new LinkedBlockingQueue<String>(), new CountDownLatch(0),
                    new CountDownLatch(0), new AtomicInteger(), new AtomicInteger());
        }

        PullFlow(final String workerID, final BlockingQueue<String> observations,
                final CountDownLatch firstRelease, final CountDownLatch secondRelease,
                final AtomicInteger firstExecuteCalls, final AtomicInteger secondExecuteCalls) {
            this.workerID = workerID;
            this.observations = observations;
            this.firstRelease = firstRelease;
            this.secondRelease = secondRelease;
            this.firstExecuteCalls = firstExecuteCalls;
            this.secondExecuteCalls = secondExecuteCalls;
        }

        @Override
        public StepList<String> getSteps() {
            return StepList.startStep(new FirstStep(this)).otherSteps(new SecondStep(this));
        }

        @RPC
        public RPCResult<String> getWorker(final Context context) {
            observations.add(workerID + ":rpc");
            return RPCResult.of(workerID);
        }
    }

    private static final class OldPullFlow extends PullFlow {
        OldPullFlow(final BlockingQueue<String> observations, final CountDownLatch firstRelease,
                final AtomicInteger firstExecuteCalls, final AtomicInteger secondExecuteCalls) {
            super("old", observations, firstRelease, new CountDownLatch(0),
                    firstExecuteCalls, secondExecuteCalls);
        }

        @Override
        public String getFlowType() {
            return "PullFlow";
        }

        @Override
        public StepList<String> getSteps() {
            return StepList.startStep(new FirstStep(this));
        }

        @Override
        public RPCResult<String> getWorker(final Context context) {
            return RPCResult.of("old");
        }
    }

    private static class NewStartPullFlow extends PullFlow {
        NewStartPullFlow(final BlockingQueue<String> observations,
                final AtomicInteger firstExecuteCalls, final AtomicInteger secondExecuteCalls) {
            super("new", observations, new CountDownLatch(0), new CountDownLatch(0),
                    firstExecuteCalls, secondExecuteCalls);
        }

        @Override
        public String getFlowType() {
            return "PullFlow";
        }

        @Override
        public StepList<String> getSteps() {
            return StepList.startStep(new SecondStep(this));
        }

        @Override
        public RPCResult<String> getWorker(final Context context) {
            return RPCResult.of("new");
        }
    }

    private static final class FirstStep implements Step<String> {
        private final PullFlow flow;

        private FirstStep(final PullFlow flow) {
            this.flow = flow;
        }

        @Override
        public Class<String> getInputType() {
            return String.class;
        }

        @Override
        public Wait waitFor(final Context context, final String input) {
            flow.observations.add(flow.workerID + ":wait-1");
            return Wait.skipImmediately();
        }

        @Override
        public StepDecision execute(final Context context, final String input) {
            flow.firstExecuteCalls.incrementAndGet();
            flow.observations.add(flow.workerID + ":execute-1");
            await(flow.firstRelease);
            return StepDecision.goTo(SecondStep.class, input);
        }
    }

    private static final class SecondStep implements Step<String> {
        private final PullFlow flow;

        private SecondStep(final PullFlow flow) {
            this.flow = flow;
        }

        @Override
        public Class<String> getInputType() {
            return String.class;
        }

        @Override
        public Wait waitFor(final Context context, final String input) {
            flow.observations.add(flow.workerID + ":wait-2");
            return Wait.skipImmediately();
        }

        @Override
        public StepDecision execute(final Context context, final String input) {
            flow.secondExecuteCalls.incrementAndGet();
            flow.observations.add(flow.workerID + ":execute-2");
            await(flow.secondRelease);
            return StepDecision.gracefulComplete(input);
        }
    }

    private static final class ProbeFlow implements Flow<String> {
        @Override
        public StepList<String> getSteps() {
            return StepList.startStep(new ProbeStep());
        }
    }

    private static final class ProbeStep implements Step<String> {
        @Override
        public Class<String> getInputType() {
            return String.class;
        }

        @Override
        public Wait waitFor(final Context context, final String input) {
            return Wait.skipImmediately();
        }

        @Override
        public StepDecision execute(final Context context, final String input) {
            return StepDecision.gracefulComplete("second");
        }
    }

    private static final class PushFlow implements Flow<String> {
        @Override
        public StepList<String> getSteps() {
            return StepList.startStep(new PushStep());
        }
    }

    private static final class PushStep implements Step<String> {
        @Override
        public Class<String> getInputType() {
            return String.class;
        }

        @Override
        public Wait waitFor(final Context context, final String input) {
            return Wait.skipImmediately();
        }

        @Override
        public StepDecision execute(final Context context, final String input) {
            return StepDecision.gracefulComplete(input);
        }
    }

    private static void await(final CountDownLatch gate) {
        try {
            assertTrue(gate.await(30, TimeUnit.SECONDS));
        } catch (InterruptedException canceled) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("handler canceled", canceled);
        }
    }

    private static final class PullRuntime implements AutoCloseable {
        private final BlobCache cache;
        private final Worker worker;
        private final Thread thread;
        private final AtomicReference<Throwable> failure = new AtomicReference<Throwable>();

        private PullRuntime(final Path directory, final String address, final String workerID,
                final Flow<?>... flows) {
            cache = BlobCache.open(new BlobCacheConfig(directory.toString(), 64L << 20));
            worker = new Worker(new Registry(Arrays.asList(flows)), cache,
                    WorkerOptions.newBuilder().serverAddress(address)
                            .pull(PullWorkerOptions.newBuilder().appWorkerID(workerID)
                                    .maxConcurrentTasks(4).build())
                            .build());
            thread = new Thread(() -> {
                try {
                    worker.start();
                } catch (Throwable raised) {
                    failure.set(raised);
                }
            }, "dex-java-pull-e2e-worker");
            thread.start();
        }

        private void stop() throws InterruptedException {
            worker.stop();
            thread.join(10_000L);
            assertFalse(thread.isAlive());
            assertEquals(null, failure.get());
        }

        @Override
        public void close() throws InterruptedException {
            stop();
            cache.close();
        }
    }
}
