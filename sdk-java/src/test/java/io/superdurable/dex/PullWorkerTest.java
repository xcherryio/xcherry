/*
 * Copyright (c) 2026 Super Durable, Inc.
 *
 * Licensed under the Sustainable Use License 1.0.
 * You may not use this file except in compliance with the License.
 * See the LICENSE file in the repository root.
 *
 * SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0
 */

package io.superdurable.dex;

import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.superdurable.dex.exceptions.RetryAfterException;
import io.superdurable.gen.FlowServiceGrpc;
import io.superdurable.gen.InvokeExecuteMethodRequest;
import io.superdurable.gen.InvokeWaitForMethodRequest;
import io.superdurable.gen.PollWorkerTask;
import io.superdurable.gen.PollWorkerTaskCancelTask;
import io.superdurable.gen.PollWorkerTaskReconnect;
import io.superdurable.gen.PollWorkerTaskRequest;
import io.superdurable.gen.PollWorkerTaskResponse;
import io.superdurable.gen.ServerInfo;
import io.superdurable.gen.SyncAttributeIndexRequest;
import io.superdurable.gen.SyncAttributeIndexResponse;
import io.superdurable.gen.WorkerTaskServiceGrpc;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class PullWorkerTest {
    @Test
    void waitsForTheLastQueueAndSupportsTimeoutInterruptionAndEarlyStop() throws Exception {
        final PollServer pollServer = new PollServer(null, false, Duration.ZERO,
                Collections.<Boolean>emptyList(), true);
        try (RunningWorker running = new RunningWorker(pollServer, new TestFlow(), 1)) {
            final PollStream pending = pollServer.awaitQueue("PullTestFlow");
            assertThrows(java.util.concurrent.TimeoutException.class,
                    () -> running.worker.waitPullRegistered(Duration.ofMillis(20)));
            assertFalse(running.worker.getPullRegistrationState().isReady());
            final AtomicReference<Throwable> waitingFailure = new AtomicReference<Throwable>();
            final Thread waiting = new Thread(() -> {
                try {
                    running.worker.waitPullRegistered(Duration.ofSeconds(5));
                } catch (Throwable failure) {
                    waitingFailure.set(failure);
                }
            });
            waiting.start();
            waiting.interrupt();
            waiting.join(5000L);
            assertTrue(waitingFailure.get() instanceof InterruptedException);
            pending.respond(PollWorkerTaskResponse.newBuilder().setRegistered(
                    io.superdurable.gen.PollWorkerTaskRegistered.getDefaultInstance()).build());
            running.worker.waitPullRegistered(Duration.ofSeconds(5));
            assertTrue(running.worker.getPullRegistrationState().isReady());
        }
        try (Worker worker = new Worker(new Registry(Collections.<Flow<?>>emptyList()), new EmptyBlobCache(),
                WorkerOptions.newBuilder().pull(PullWorkerOptions.newBuilder().build()).build())) {
            worker.stop();
            assertThrows(IllegalStateException.class, () -> worker.waitPullRegistered(Duration.ofSeconds(1)));
        }
    }

    @Test
    void waitsForEveryQueueAndKeepsReadinessAfterStop() throws Exception {
        final PollServer pollServer = new PollServer();
        try (RunningWorker running = new RunningWorker(pollServer, new TestFlow(), 1)) {
            running.worker.waitPullRegistered(Duration.ofSeconds(5));
            final PullRegistrationState snapshot = running.worker.getPullRegistrationState();
            assertTrue(snapshot.isReady());
            assertEquals(2, snapshot.getQueues().size());
            assertTrue(snapshot.getQueues().get(0).hasRegisteredStream());
            running.stop();
            running.worker.waitPullRegistered(Duration.ZERO);
            assertTrue(running.worker.getPullRegistrationState().isReady());
            assertFalse(running.worker.getPullRegistrationState().getQueues().get(0).hasRegisteredStream());
            assertTrue(snapshot.getQueues().get(0).hasRegisteredStream());
            assertThrows(UnsupportedOperationException.class, () -> snapshot.getQueues().clear());
        }
    }

    @Test
    void reportsEveryPermanentStatusBeforeReadinessWithoutStoppingByDefault() throws Exception {
        for (Status rejection : Arrays.asList(Status.INVALID_ARGUMENT, Status.RESOURCE_EXHAUSTED,
                Status.UNIMPLEMENTED, Status.PERMISSION_DENIED, Status.UNAUTHENTICATED)) {
            try (RunningWorker running = new RunningWorker(new PollServer(rejection), new TestFlow(), 1)) {
                final PullRegistrationError failure = assertThrows(PullRegistrationError.class,
                        () -> running.worker.waitPullRegistered(Duration.ofSeconds(5)));
                assertFalse(failure.getQueue().isEmpty());
                assertEquals(rejection.getCode(), Status.fromThrowable(failure.getCause()).getCode());
                assertFalse(running.worker.getPullRegistrationState().isReady());
                assertTrue(running.workerThread.isAlive());
            }
        }
    }

    @Test
    void readinessDoesNotFlapOnTransientDisconnectAndRegistrationClearsPermanentError() throws Exception {
        final PollServer pollServer = new PollServer();
        try (RunningWorker running = new RunningWorker(pollServer, new TestFlow(), 1)) {
            running.worker.waitPullRegistered(Duration.ofSeconds(5));
            final PollStream first = pollServer.awaitQueue("PullTestFlow");
            first.responses.onError(Status.UNAVAILABLE.asRuntimeException());
            awaitRegistrationState(running, "PullTestFlow", false, false);
            assertTrue(running.worker.getPullRegistrationState().isReady());
            final PollStream replacement = pollServer.awaitQueue("PullTestFlow");
            running.worker.waitPullRegistered(Duration.ZERO);
            replacement.responses.onError(Status.INVALID_ARGUMENT.asRuntimeException());
            awaitRegistrationState(running, "PullTestFlow", false, true);
            assertFalse(running.worker.getPullRegistrationState().isReady());
            running.worker.waitPullRegistered(Duration.ZERO);
            final PullWorker pull = running.pullWorker();
            final Field streamsField = PullWorker.class.getDeclaredField("streams");
            streamsField.setAccessible(true);
            synchronized (pull) {
                for (Object stream : (java.util.List<?>) streamsField.get(pull)) {
                    final Field queueField = stream.getClass().getDeclaredField("queue");
                    queueField.setAccessible(true);
                    if ("PullTestFlow".equals(queueField.get(stream))) {
                        final java.lang.reflect.Method open = stream.getClass().getDeclaredMethod("open");
                        open.setAccessible(true);
                        open.invoke(stream);
                    }
                }
            }
            awaitRegistrationState(running, "PullTestFlow", true, false);
            assertTrue(running.worker.getPullRegistrationState().isReady());
        }
    }

    @Test
    void permanentRejectionOptionStopsBeforeAndAfterReadiness() throws Exception {
        for (boolean hasRegistered : new boolean[] {false, true}) {
            final PollServer pollServer = hasRegistered ? new PollServer() : new PollServer(Status.INVALID_ARGUMENT);
            try (RunningWorker running = new RunningWorker(pollServer, new TestFlow(), 1,
                    Duration.ofMillis(100), true, 1)) {
                if (hasRegistered) {
                    running.worker.waitPullRegistered(Duration.ofSeconds(5));
                    pollServer.awaitQueue("PullTestFlow").responses.onError(Status.PERMISSION_DENIED.asRuntimeException());
                } else {
                    assertThrows(PullRegistrationError.class,
                            () -> running.worker.waitPullRegistered(Duration.ofSeconds(5)));
                }
                running.workerThread.join(5000L);
                assertFalse(running.workerThread.isAlive());
                assertTrue(running.failure.get() instanceof PullRegistrationError);
                running.worker.stop();
            }
        }
    }

    @Test
    void rejectedSurplusStreamDoesNotStopOrClearReadiness() throws Exception {
        final PollServer pollServer = new PollServer();
        try (RunningWorker running = new RunningWorker(pollServer, new TestFlow(), 1,
                Duration.ofMillis(100), true, 2)) {
            running.worker.waitPullRegistered(Duration.ofSeconds(5));
            final PollStream first = pollServer.awaitQueue("PullTestFlow");
            final PollStream second = pollServer.awaitQueue("PullTestFlow");
            second.respond(PollWorkerTaskResponse.newBuilder()
                    .setPing(io.superdurable.gen.PollWorkerTaskPing.getDefaultInstance()).build());
            assertTrue(second.awaitFrame().hasPong());
            first.responses.onError(Status.PERMISSION_DENIED.asRuntimeException());
            awaitRegistrationState(running, "PullTestFlow", true, true);
            assertTrue(running.worker.getPullRegistrationState().isReady());
            assertTrue(running.workerThread.isAlive());
            second.responses.onError(Status.PERMISSION_DENIED.asRuntimeException());
            running.workerThread.join(5000L);
            assertFalse(running.workerThread.isAlive());
            assertTrue(running.failure.get() instanceof PullRegistrationError);
        }
    }

    @Test
    void startupFailureReachesRegistrationWaiters() throws Exception {
        try (Worker worker = new Worker(new Registry(Collections.<Flow<?>>emptyList()), new EmptyBlobCache(),
                WorkerOptions.newBuilder().serverAddress("127.0.0.1:1")
                        .attributeIndexSyncTimeout(Duration.ofMillis(100))
                        .pull(PullWorkerOptions.newBuilder().build()).build())) {
            final java.util.concurrent.CompletableFuture<Throwable> waiting =
                    java.util.concurrent.CompletableFuture.supplyAsync(() -> {
                        try {
                            worker.waitPullRegistered(Duration.ofSeconds(5));
                            return null;
                        } catch (Throwable failure) {
                            return failure;
                        }
                    });
            final IllegalStateException failure = assertThrows(IllegalStateException.class, worker::start);
            assertTrue(waiting.get(5, TimeUnit.SECONDS) == failure);
        }
        try (Worker worker = new Worker(new Registry(Collections.<Flow<?>>emptyList()), new EmptyBlobCache())) {
            assertFalse(worker.getPullRegistrationState().isReady());
            assertTrue(worker.getPullRegistrationState().getQueues().isEmpty());
            assertThrows(IllegalStateException.class, () -> worker.waitPullRegistered(Duration.ZERO));
        }
    }

    private static void awaitRegistrationState(final RunningWorker running, final String queue,
            final boolean hasRegisteredStream, final boolean hasPermanentError) throws Exception {
        final PullWorker pull = running.pullWorker();
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        synchronized (pull) {
            while (System.nanoTime() < deadline) {
                for (PullQueueRegistrationState registration : running.worker.getPullRegistrationState().getQueues()) {
                    if (queue.equals(registration.getQueue())
                            && registration.hasRegisteredStream() == hasRegisteredStream
                            && (registration.getLastPermanentError() != null) == hasPermanentError) {
                        return;
                    }
                }
                pull.wait(10L);
            }
        }
        throw new AssertionError("registration state did not converge for " + queue);
    }

    @Test
    void validatesOptionsAndGeneratesHostnameBasedIdentity() {
        assertThrows(IllegalArgumentException.class,
                () -> PullWorkerOptions.newBuilder().maxConcurrentTasks(-1));
        assertThrows(IllegalArgumentException.class,
                () -> PullWorkerOptions.newBuilder().idleStreamsPerQueue(-1));
        assertThrows(IllegalArgumentException.class,
                () -> PullWorkerOptions.newBuilder().appWorkerID("bad id"));
        assertThrows(IllegalArgumentException.class,
                () -> PullWorkerOptions.newBuilder().appWorkerID(repeat("a", 64)));
        assertThrows(IllegalArgumentException.class,
                () -> PullWorkerOptions.newBuilder().stopDrainTimeout(Duration.ofSeconds(-1)));
        assertThrows(IllegalArgumentException.class, () -> WorkerOptions.newBuilder()
                .pull(PullWorkerOptions.newBuilder().build()).bindAddress(":8803").build());
        assertThrows(IllegalArgumentException.class, () -> WorkerOptions.newBuilder()
                .pull(PullWorkerOptions.newBuilder().build())
                .workerTarget(new WorkerTarget("localhost:8803", false)).build());
        assertThrows(IllegalArgumentException.class,
                () -> WorkerOptions.newBuilder().workerTarget(WorkerTarget.pull()).build());
        assertNotNull(WorkerOptions.newBuilder().bindAddress("").build());

        final PullWorkerOptions defaults = PullWorkerOptions.newBuilder()
                .maxConcurrentTasks(0).idleStreamsPerQueue(0)
                .stopDrainTimeout(Duration.ZERO).build();
        assertEquals(Runtime.getRuntime().availableProcessors(), defaults.getMaxConcurrentTasks());
        assertEquals(1, defaults.getIdleStreamsPerQueue());
        assertEquals(Duration.ofSeconds(30), defaults.getStopDrainTimeout());
        final String workerID = PullWorker.generateAppWorkerID("My.Host_Name");
        assertTrue(workerID.matches("my-host-name-[a-z2-7]{8}"));
        assertNotEquals(workerID, PullWorker.generateAppWorkerID("My.Host_Name"));
        final java.util.Map<String, String> environment =
                new java.util.HashMap<String, String>();
        final AtomicInteger hostnameCalls = new AtomicInteger();
        assertEquals("host-one", PullWorker.defaultWorkerIdentity(environment, () -> {
            hostnameCalls.incrementAndGet();
            return "host-one";
        }));
        assertEquals(1, hostnameCalls.get());
        environment.put("HOSTNAME", "env-host");
        environment.put("POD_NAME", "pod-host");
        environment.put("DEX_WORKER_INSTANCE", "instance-host");
        assertEquals("instance-host", PullWorker.defaultWorkerIdentity(environment, () -> {
            hostnameCalls.incrementAndGet();
            return "host-two";
        }));
        assertEquals(1, hostnameCalls.get());
        assertEquals("worker", PullWorker.defaultWorkerIdentity(
                new java.util.HashMap<String, String>(), () -> ""));
        assertTrue(PullWorker.generateAppWorkerID("###").startsWith("worker-"));
    }

    @Test
    void shrinksSurplusStreamsAfterBurstWithoutClosingBusyStreams() throws Exception {
        for (int idleStreams : new int[] {1, 2}) {
            final TestFlow flow = new TestFlow();
            final PollServer pollServer = new PollServer();
            try (RunningWorker running = new RunningWorker(pollServer, flow, 3,
                    Duration.ofSeconds(5), false, idleStreams)) {
                running.worker.waitPullRegistered(Duration.ofSeconds(5));
                final java.util.List<PollStream> busyStreams = new java.util.ArrayList<PollStream>();
                for (int taskNumber = 0; taskNumber < 3; taskNumber++) {
                    final PollStream stream = pollServer.awaitQueue("PullTestFlow");
                    busyStreams.add(stream);
                    stream.assign(executeTask("burst-" + taskNumber, "block"));
                }
                assertEquals(3 + 2 * idleStreams, running.awaitStableStreamCount());
                assertEquals(3, flow.executeCalls.get());
                for (PollStream stream : busyStreams) {
                    assertEquals(1L, stream.closed.getCount());
                }
                assertTrue(running.worker.getPullRegistrationState().isReady());
                flow.release.countDown();
                for (PollStream stream : busyStreams) {
                    assertTrue(stream.awaitFrame().hasTaskFrame());
                }
                assertEquals(2 * idleStreams, running.awaitStableStreamCount());
                assertEquals(idleStreams, running.idleStreamCount("PullTestFlow"));
                assertTrue(running.worker.getPullRegistrationState().isReady());
                running.worker.waitPullRegistered(Duration.ZERO);
                assertEquals(2 * idleStreams, running.awaitStableStreamCount());
            }
        }
    }

    @Test
    void rejectsAtCapacityAndDrainsLargeFinalFrameOnReconnectAndStop() throws Exception {
        final TestFlow flow = new TestFlow();
        final PollServer pollServer = new PollServer();
        try (RunningWorker running = new RunningWorker(pollServer, flow, 1)) {
            final PollStream first = pollServer.awaitQueue("PullTestFlow");
            first.assign(executeTask("first", "large"));
            assertTrue(flow.entered.await(5, TimeUnit.SECONDS));

            final PollStream second = pollServer.awaitQueue("PullTestFlow");
            second.assign(executeTask("second", "small"));
            final PollWorkerTaskRequest rejection = second.awaitFrame();
            assertTrue(rejection.hasReject());
            assertEquals("second", rejection.getReject().getTaskId());

            first.respond(PollWorkerTaskResponse.newBuilder()
                    .setReconnect(PollWorkerTaskReconnect.getDefaultInstance()).build());
            flow.release.countDown();
            final PollWorkerTaskRequest result = first.awaitFrame();
            assertTrue(result.hasTaskFrame());
            assertTrue(result.getTaskFrame().getExecute().hasResult());
            assertTrue(result.getSerializedSize() > 3_000_000);
            assertTrue(first.closed.await(5, TimeUnit.SECONDS));
            assertEquals(1, flow.executeCalls.get());
            second.assign(executeTask("third", "small"));
            assertEquals("third", second.awaitFrame().getTaskFrame().getTaskId());
            running.stop();
            assertEquals(2, flow.executeCalls.get());
        }
    }

    @Test
    void cancellationAndMappedFailureUseWorkerErrorStatus() throws Exception {
        final TestFlow flow = new TestFlow();
        final PollServer pollServer = new PollServer();
        try (RunningWorker running = new RunningWorker(pollServer, flow, 2)) {
            final PollStream first = pollServer.awaitQueue("PullTestFlow");
            first.assign(executeTask("cancel", "block"));
            assertTrue(flow.entered.await(5, TimeUnit.SECONDS));
            first.respond(PollWorkerTaskResponse.newBuilder()
                    .setCancelTask(PollWorkerTaskCancelTask.newBuilder().setTaskId("cancel"))
                    .build());
            final PollWorkerTaskRequest canceled = first.awaitFrame();
            assertEquals(Status.Code.CANCELLED.value(),
                    canceled.getTaskFrame().getWorkerError().getGrpcStatus());

            final PollStream second = pollServer.awaitQueue("PullTestFlow");
            second.assign(executeTask("failure", "failure"));
            final PollWorkerTaskRequest failed = second.awaitFrame();
            assertEquals(Status.Code.INVALID_ARGUMENT.value(),
                    failed.getTaskFrame().getWorkerError().getGrpcStatus());
            assertTrue(failed.getTaskFrame().getWorkerError().getErrorType()
                    .contains("IllegalArgumentException"));
            assertTrue(failed.getTaskFrame().getWorkerError().getStackTrace()
                    .contains("application failure"));
            running.stop();
        }
    }

    @Test
    void reusesStreamAfterFinalResult() throws Exception {
        final TestFlow flow = new TestFlow();
        final PollServer pollServer = new PollServer(null, false, Duration.ZERO,
                Collections.<Boolean>emptyList(), true);
        try (RunningWorker running = new RunningWorker(pollServer, flow, 1)) {
            final PollStream stream = pollServer.awaitQueue("PullTestFlow");
            stream.respond(PollWorkerTaskResponse.newBuilder().setRegistered(
                    io.superdurable.gen.PollWorkerTaskRegistered.getDefaultInstance()).build());
            stream.assign(executeTask("one", "one"));
            assertEquals("one", stream.awaitFrame().getTaskFrame().getTaskId());
            stream.assign(executeTask("two", "two"));
            final PollWorkerTaskRequest second = stream.awaitFrame();
            assertTrue(second.hasTaskFrame());
            assertEquals("two", second.getTaskFrame().getTaskId());
            assertEquals(2, flow.executeCalls.get());
        }
    }

    @Test
    void unknownTaskRejectionFollowsRegisteredCapabilityOnReconnect() throws Exception {
        final TestFlow flow = new TestFlow();
        final PollServer pollServer = new PollServer(Boolean.FALSE, Boolean.TRUE, Boolean.FALSE);
        try (RunningWorker running = new RunningWorker(pollServer, flow, 1)) {
            final PollStream legacy = pollServer.awaitQueue("PullTestFlow");
            legacy.respond(PollWorkerTaskResponse.newBuilder()
                    .setPing(io.superdurable.gen.PollWorkerTaskPing.getDefaultInstance()).build());
            assertTrue(legacy.awaitFrame().hasPong());

            assertLegacyUnknownTask(legacy, unknownStepTask("legacy-step"));
            assertTrue(legacy.closed.await(5, TimeUnit.SECONDS));
            final PollStream capable = pollServer.awaitQueue("PullTestFlow");
            assertUnknownTaskRejected(capable, unknownStepTask("capable-step"),
                    io.superdurable.gen.PollWorkerTaskRejectReason
                            .POLL_WORKER_TASK_REJECT_REASON_UNKNOWN_STEP);
            assertUnknownTaskRejected(capable, unknownRPCTask("capable-rpc"),
                    io.superdurable.gen.PollWorkerTaskRejectReason
                            .POLL_WORKER_TASK_REJECT_REASON_UNKNOWN_RPC);

            capable.respond(PollWorkerTaskResponse.newBuilder()
                    .setReconnect(PollWorkerTaskReconnect.getDefaultInstance()).build());
            final PollStream reconnected = pollServer.awaitQueue("PullTestFlow");
            assertLegacyUnknownTask(reconnected, unknownRPCTask("reconnected-rpc"));
        }
    }

    @Test
    void streamsHeartbeatAndWriteBeforeFinalResult() throws Exception {
        final PollServer pollServer = new PollServer();
        try (RunningWorker running = new RunningWorker(pollServer, new TestFlow(), 1)) {
            final PollStream stream = pollServer.awaitQueue("PullTestFlow");
            stream.assign(executeTask("progress", "progress"));
            assertTrue(stream.awaitFrame().getTaskFrame().getExecute().hasHeartbeat());
            assertTrue(stream.awaitFrame().getTaskFrame().getExecute().hasStreamWrite());
            assertTrue(stream.awaitFrame().getTaskFrame().getExecute().hasResult());
        }
    }

    @Test
    void preservesRetryAfterAndFailureDetails() throws Exception {
        final PollServer pollServer = new PollServer();
        try (RunningWorker running = new RunningWorker(pollServer, new TestFlow(), 1)) {
            final PollStream stream = pollServer.awaitQueue("PullTestFlow");
            stream.assign(executeTask("retry", "retry"));
            final io.superdurable.gen.WorkerErrorResponse error =
                    stream.awaitFrame().getTaskFrame().getWorkerError();
            assertEquals(Status.Code.INVALID_ARGUMENT.value(), error.getGrpcStatus());
            assertEquals("retry failure", error.getDetail());
            assertEquals(7, error.getRetryAfterSeconds());
            assertTrue(error.getErrorType().contains("IllegalArgumentException"));
            assertTrue(error.getStackTrace().contains("retry failure"));
        }
    }

    @Test
    void stopDrainsLargeResultWhenCloseFollowsCompletion() throws Exception {
        final TestFlow flow = new TestFlow();
        final PollServer pollServer = new PollServer(true);
        try (RunningWorker running = new RunningWorker(
                pollServer, flow, 1, Duration.ofSeconds(8))) {
            final PollStream stream = pollServer.awaitQueue("PullTestFlow");
            stream.assign(executeTask("drain", "large"));
            assertTrue(flow.entered.await(5, TimeUnit.SECONDS));
            final AtomicReference<Throwable> stopFailure = new AtomicReference<Throwable>();
            final Thread stopping = new Thread(() -> {
                try {
                    running.stop();
                } catch (Throwable failure) {
                    stopFailure.set(failure);
                }
            }, "dex-java-pull-unit-stop");
            stopping.start();
            assertTrue(stopping.isAlive());
            pollServer.releasePendingReadsAfter(Duration.ofSeconds(3));
            flow.release.countDown();
            final PollWorkerTaskRequest result = stream.awaitFrame();
            assertTrue(result.getTaskFrame().getExecute().hasResult());
            assertTrue(result.getSerializedSize() > 3_000_000);
            assertTrue(stream.closed.await(5, TimeUnit.SECONDS));
            stopping.join(10_000L);
            assertFalse(stopping.isAlive());
            assertEquals(null, stopFailure.get());
            assertEquals(1, flow.executeCalls.get());
        }
    }

    @Test
    void busyReconnectPreservesLargeResultWithSlowReader() throws Exception {
        final TestFlow flow = new TestFlow();
        final PollServer pollServer = new PollServer(true);
        try (RunningWorker running = new RunningWorker(pollServer, flow, 1)) {
            final PollStream stream = pollServer.awaitQueue("PullTestFlow");
            stream.assign(executeTask("reconnect-slow", "large"));
            assertTrue(flow.entered.await(5, TimeUnit.SECONDS));
            stream.respond(PollWorkerTaskResponse.newBuilder()
                    .setReconnect(PollWorkerTaskReconnect.getDefaultInstance()).build());
            assertNotNull(pollServer.awaitQueue("PullTestFlow"));
            pollServer.releasePendingReadsAfter(Duration.ofSeconds(3));
            flow.release.countDown();
            final PollWorkerTaskRequest result = stream.awaitFrame();
            assertTrue(result.getTaskFrame().getExecute().hasResult());
            assertTrue(result.getSerializedSize() > 3_000_000);
            assertTrue(stream.closed.await(8, TimeUnit.SECONDS));
            assertEquals(1, flow.executeCalls.get());
        }
    }

    @Test
    void reconnectRetiresSurplusStreamsAfterDelayedEof() throws Exception {
        final TestFlow flow = new TestFlow();
        final PollServer pollServer = new PollServer(Duration.ofSeconds(1));
        try (RunningWorker running = new RunningWorker(pollServer, flow, 1)) {
            final java.util.List<Integer> streamCounts = new java.util.ArrayList<Integer>();
            for (int round = 0; round < 6; round++) {
                final Cycle cycle = new Cycle();
                flow.cycles.add(cycle);
                final PollStream busy = pollServer.awaitQueue("PullTestFlow");
                busy.assign(executeTask("reconnect-" + round, "cycle"));
                assertTrue(cycle.entered.await(5, TimeUnit.SECONDS));
                final java.util.List<PollStream> draining = pollServer.reconnectAll(busy);
                cycle.release.countDown();
                assertTrue(busy.awaitFrame().hasTaskFrame());
                for (PollStream stream : draining) {
                    assertTrue(stream.serverEof.await(5, TimeUnit.SECONDS));
                }
                streamCounts.add(running.awaitStableStreamCount());
                assertEquals(1, running.idleStreamCount("PullTestFlow"));
                assertEquals(1, running.idleStreamCount("pull-java-unit"));
            }
            assertEquals(6, flow.executeCalls.get());
            assertTrue(java.util.Collections.max(streamCounts) <= 4, "stream counts: " + streamCounts);
            assertEquals(2, streamCounts.get(streamCounts.size() - 1));
        }
    }

    @Test
    void deadlineCancelsHandlerWithDeadlineExceeded() throws Exception {
        final TestFlow flow = new TestFlow();
        final PollServer pollServer = new PollServer();
        try (RunningWorker running = new RunningWorker(pollServer, flow, 1)) {
            final PollStream stream = pollServer.awaitQueue("PullTestFlow");
            final Instant deadline = Instant.now().plusMillis(100);
            stream.assign(executeTask("deadline", "block").toBuilder()
                    .setDeadline(Timestamp.newBuilder().setSeconds(deadline.getEpochSecond())
                            .setNanos(deadline.getNano())).build());
            final PollWorkerTaskRequest result = stream.awaitFrame();
            assertEquals(Status.Code.DEADLINE_EXCEEDED.value(),
                    result.getTaskFrame().getWorkerError().getGrpcStatus());
        }
    }

    @Test
    void expiredDeadlineNeverInvokesHandler() throws Exception {
        final TestFlow flow = new TestFlow();
        final PollServer pollServer = new PollServer();
        try (RunningWorker running = new RunningWorker(pollServer, flow, 1)) {
            final PollStream stream = pollServer.awaitQueue("PullTestFlow");
            stream.assign(executeTask("expired", "block").toBuilder()
                    .setDeadline(Timestamp.newBuilder().setSeconds(1)).build());
            final PollWorkerTaskRequest result = stream.awaitFrame();
            assertEquals(Status.Code.DEADLINE_EXCEEDED.value(),
                    result.getTaskFrame().getWorkerError().getGrpcStatus());
            assertEquals(0, flow.executeCalls.get());
        }
    }

    @Test
    void forcedStopInterruptsAndWaitsForHandler() throws Exception {
        final TestFlow flow = new TestFlow();
        final PollServer pollServer = new PollServer();
        try (RunningWorker running = new RunningWorker(
                pollServer, flow, 1, Duration.ofMillis(100))) {
            final PollStream stream = pollServer.awaitQueue("PullTestFlow");
            stream.assign(executeTask("forced", "block"));
            assertTrue(flow.entered.await(5, TimeUnit.SECONDS));
            running.stop();
            assertEquals(1, flow.executeCalls.get());
            assertTrue(stream.closed.await(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void unsupportedServerLogsOnceAndKeepsWorkerRunning() throws Exception {
        final PollServer pollServer = new PollServer(Status.UNIMPLEMENTED);
        final AtomicInteger unsupportedLogs = new AtomicInteger();
        final CountDownLatch unsupportedLogged = new CountDownLatch(1);
        final Logger logger = Logger.getLogger(PullWorker.class.getName());
        final Handler capture = new Handler() {
            @Override
            public void publish(final LogRecord record) {
                if (record.getMessage().contains("unsupported by this Dex server")) {
                    unsupportedLogs.incrementAndGet();
                    unsupportedLogged.countDown();
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        logger.addHandler(capture);
        try (RunningWorker running = new RunningWorker(pollServer, new TestFlow(), 1)) {
            assertTrue(pollServer.registrationAttempts.await(5, TimeUnit.SECONDS));
            assertTrue(unsupportedLogged.await(5, TimeUnit.SECONDS));
            assertEquals(1, unsupportedLogs.get());
            running.stop();
        } finally {
            logger.removeHandler(capture);
        }
    }

    @Test
    void preservesPullTargetInSubFlowOverride() throws Throwable {
        final Registry registry = new Registry(Arrays.<Flow<?>>asList(
                new ParentFlow(), new ChildFlow()));
        final ManagedChannel channel = ManagedChannelBuilder.forTarget("127.0.0.1:1")
                .usePlaintext().build();
        try {
            final ValueMapper mapper = new ValueMapper(new com.fasterxml.jackson.databind.ObjectMapper());
            final WorkerDispatcher dispatcher = new WorkerDispatcher(registry, mapper,
                    new ValueHydrator(FlowServiceGrpc.newBlockingStub(channel),
                            new EmptyBlobCache()),
                    new NegotiatedServerProtocol());
            final io.superdurable.gen.InvokeWaitForMethodResponse response =
                    dispatcher.invokeWaitFor(InvokeWaitForMethodRequest.newBuilder()
                            .setFlowType("ParentFlow")
                            .setStepType("ParentStep")
                            .setContext(io.superdurable.gen.Context.newBuilder().setFlowId("parent"))
                            .setStepInput(mapper.encode("input"))
                            .build(), new StepOutputEmitter() {
                                @Override
                                public void emitHeartbeat(final io.superdurable.gen.StepMethodHeartbeat heartbeat) {
                                }

                                @Override
                                public void emitStreamWrite(final io.superdurable.gen.StepStreamWrite streamWrite) {
                                }
                            });
            assertTrue(response.getWaitingCondition().getSubFlowConditions(0)
                    .getOptions().getFlowConfigOverride().getWorkerTarget().getIsPullMode());
        } finally {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private static final class ParentFlow implements Flow<String> {
        @Override
        public StepList<String> getSteps() {
            return StepList.startStep(new ParentStep());
        }
    }

    private static final class ParentStep implements Step<String> {
        @Override
        public Class<String> getInputType() {
            return String.class;
        }

        @Override
        public Wait waitFor(final Context context, final String input) {
            return Wait.until(SubFlow.run(ChildFlow.class, input,
                    SubFlowOptions.newBuilder()
                            .configOverride(FlowConfig.newBuilder()
                                    .workerTarget(WorkerTarget.pull()).build())
                            .build()));
        }

        @Override
        public StepDecision execute(final Context context, final String input) {
            return StepDecision.gracefulComplete(input);
        }
    }

    private static final class ChildFlow implements Flow<String> {
        @Override
        public StepList<String> getSteps() {
            return StepList.startStep(new ChildStep());
        }
    }

    private static final class ChildStep implements Step<String> {
        @Override
        public Class<String> getInputType() {
            return String.class;
        }

        @Override
        public StepDecision execute(final Context context, final String input) {
            return StepDecision.gracefulComplete(input);
        }
    }

    private static PollWorkerTask executeTask(final String taskID, final String input) {
        final ValueMapper mapper = new ValueMapper(new com.fasterxml.jackson.databind.ObjectMapper());
        return PollWorkerTask.newBuilder().setTaskId(taskID)
                .setExecute(InvokeExecuteMethodRequest.newBuilder()
                        .setFlowType("PullTestFlow")
                        .setStepType("TestStep")
                        .setContext(io.superdurable.gen.Context.newBuilder().setFlowId("test-flow"))
                        .setStepInput(mapper.encode(input)))
                .build();
    }

    private static PollWorkerTask unknownStepTask(final String taskID) {
        final ValueMapper mapper = new ValueMapper(new com.fasterxml.jackson.databind.ObjectMapper());
        return PollWorkerTask.newBuilder().setTaskId(taskID)
                .setWaitFor(InvokeWaitForMethodRequest.newBuilder()
                        .setFlowType("PullTestFlow").setStepType("MissingStep")
                        .setContext(io.superdurable.gen.Context.newBuilder().setFlowId("test-flow"))
                        .setStepInput(mapper.encode("unknown")))
                .build();
    }

    private static PollWorkerTask unknownRPCTask(final String taskID) {
        final ValueMapper mapper = new ValueMapper(new com.fasterxml.jackson.databind.ObjectMapper());
        return PollWorkerTask.newBuilder().setTaskId(taskID)
                .setRpc(io.superdurable.gen.InvokeWorkerRPCRequest.newBuilder()
                        .setFlowType("PullTestFlow").setRpcName("MissingRPC")
                        .setContext(io.superdurable.gen.Context.newBuilder().setFlowId("test-flow"))
                        .setInput(mapper.encode("unknown")))
                .build();
    }

    private static void assertLegacyUnknownTask(final PollStream stream, final PollWorkerTask task)
            throws InterruptedException {
        stream.assign(task);
        final PollWorkerTaskRequest frame = stream.awaitFrame();
        assertFalse(frame.hasReject());
        assertEquals(task.getTaskId(), frame.getTaskFrame().getTaskId());
        assertTrue(frame.getTaskFrame().getWorkerError().getDetail().contains("not registered"),
                frame.getTaskFrame().getWorkerError().getDetail());
    }

    private static void assertUnknownTaskRejected(final PollStream stream, final PollWorkerTask task,
            final io.superdurable.gen.PollWorkerTaskRejectReason reason)
            throws InterruptedException {
        stream.assign(task);
        final PollWorkerTaskRequest frame = stream.awaitFrame();
        assertTrue(frame.hasReject());
        assertEquals(task.getTaskId(), frame.getReject().getTaskId());
        assertEquals(reason, frame.getReject().getReason());
    }

    private static String repeat(final String value, final int count) {
        final StringBuilder result = new StringBuilder();
        for (int index = 0; index < count; index++) {
            result.append(value);
        }
        return result.toString();
    }

    private static final class TestFlow implements Flow<String> {
        private final Stream<String> progress = Stream.define("progress", String.class, 1_048_576);
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final AtomicInteger executeCalls = new AtomicInteger();
        private final BlockingQueue<Cycle> cycles = new LinkedBlockingQueue<Cycle>();

        @Override
        public StepList<String> getSteps() {
            return StepList.startStep(new TestStep(this));
        }

        @Override
        public PersistenceSchema getPersistenceSchema() {
            return PersistenceSchema.of(progress);
        }

        @Override
        public String getFlowType() {
            return "PullTestFlow";
        }
    }

    private static final class Cycle {
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
    }

    private static final class TestStep implements Step<String> {
        private final TestFlow flow;

        private TestStep(final TestFlow flow) {
            this.flow = flow;
        }

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
            flow.executeCalls.incrementAndGet();
            if ("failure".equals(input)) {
                throw new IllegalArgumentException("application failure");
            }
            if ("retry".equals(input)) {
                throw RetryAfterException.after(Duration.ofSeconds(7),
                        new IllegalArgumentException("retry failure"));
            }
            if ("progress".equals(input)) {
                context.recordHeartbeat("checkpoint");
                flow.progress.write(context, "delta");
            }
            if ("large".equals(input) || "block".equals(input)) {
                flow.entered.countDown();
                try {
                    flow.release.await();
                } catch (InterruptedException canceled) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("handler canceled", canceled);
                }
            }
            if ("cycle".equals(input)) {
                try {
                    final Cycle cycle = flow.cycles.poll(5, TimeUnit.SECONDS);
                    if (cycle == null) {
                        throw new IllegalStateException("missing reconnect cycle");
                    }
                    cycle.entered.countDown();
                    cycle.release.await();
                } catch (InterruptedException canceled) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("handler canceled", canceled);
                }
            }
            return StepDecision.gracefulComplete("large".equals(input)
                    ? repeat("x", 3_100_000) : input);
        }
    }

    private static final class PollServer extends WorkerTaskServiceGrpc.WorkerTaskServiceImplBase {
        private final BlockingQueue<PollStream> registrations = new LinkedBlockingQueue<PollStream>();
        private final CountDownLatch registrationAttempts = new CountDownLatch(2);
        private final AtomicReference<Throwable> workerFailure = new AtomicReference<Throwable>();
        private final Status rejection;
        private final boolean shouldDelayFlowRegistration;
        private final boolean isReaderPaused;
        private final Duration closeResponseDelay;
        private final java.util.List<Boolean> unknownNameRejectionCapabilities;
        private final AtomicInteger registrationCount = new AtomicInteger();
        private final java.util.List<PollStream> allStreams =
                new java.util.concurrent.CopyOnWriteArrayList<PollStream>();
        private final ScheduledExecutorService readScheduler =
                Executors.newSingleThreadScheduledExecutor(runnable -> {
                    final Thread thread = new Thread(runnable, "dex-java-pull-test-reader");
                    thread.setDaemon(true);
                    return thread;
                });

        private PollServer() {
            this(null, false, Duration.ZERO, Collections.<Boolean>emptyList());
        }

        private PollServer(final Status rejection) {
            this(rejection, false, Duration.ZERO, Collections.<Boolean>emptyList());
        }

        private PollServer(final boolean isReaderPaused) {
            this(null, isReaderPaused, Duration.ZERO, Collections.<Boolean>emptyList());
        }

        private PollServer(final Duration closeResponseDelay) {
            this(null, false, closeResponseDelay, Collections.<Boolean>emptyList());
        }

        private PollServer(final Boolean firstCapability, final Boolean... remainingCapabilities) {
            this(null, false, Duration.ZERO,
                    unknownNameRejectionCapabilities(firstCapability, remainingCapabilities));
        }

        private PollServer(final Status rejection, final boolean isReaderPaused,
                final Duration closeResponseDelay,
                final java.util.List<Boolean> unknownNameRejectionCapabilities) {
            this(rejection, isReaderPaused, closeResponseDelay, unknownNameRejectionCapabilities, false);
        }

        private PollServer(final Status rejection, final boolean isReaderPaused,
                final Duration closeResponseDelay,
                final java.util.List<Boolean> unknownNameRejectionCapabilities,
                final boolean shouldDelayFlowRegistration) {
            this.rejection = rejection;
            this.shouldDelayFlowRegistration = shouldDelayFlowRegistration;
            this.isReaderPaused = isReaderPaused;
            this.closeResponseDelay = closeResponseDelay;
            this.unknownNameRejectionCapabilities = unknownNameRejectionCapabilities;
        }

        private static java.util.List<Boolean> unknownNameRejectionCapabilities(
                final Boolean firstCapability, final Boolean... remainingCapabilities) {
            final java.util.List<Boolean> capabilities = new java.util.ArrayList<Boolean>();
            capabilities.add(firstCapability);
            capabilities.addAll(Arrays.asList(remainingCapabilities));
            return capabilities;
        }

        @Override
        public StreamObserver<PollWorkerTaskRequest> pollWorkerTask(
                final StreamObserver<PollWorkerTaskResponse> responses) {
            final PollStream stream = new PollStream(responses);
            final ServerCallStreamObserver<PollWorkerTaskResponse> readControl =
                    (ServerCallStreamObserver<PollWorkerTaskResponse>) responses;
            stream.readControl = readControl;
            allStreams.add(stream);
            if (isReaderPaused) {
                readControl.disableAutoRequest();
                readControl.request(1);
            }
            return new StreamObserver<PollWorkerTaskRequest>() {
                @Override
                public void onNext(final PollWorkerTaskRequest request) {
                    if (request.hasRegister()) {
                        if (rejection != null) {
                            registrationAttempts.countDown();
                            responses.onError(rejection.asRuntimeException());
                            return;
                        }
                        stream.queue = request.getRegister().getQueue();
                        final boolean supportsUnknownNameRejection;
                        if ("PullTestFlow".equals(stream.queue)) {
                            final int registrationIndex = registrationCount.getAndIncrement();
                            supportsUnknownNameRejection = registrationIndex
                                    < unknownNameRejectionCapabilities.size()
                                    && unknownNameRejectionCapabilities.get(registrationIndex);
                        } else {
                            supportsUnknownNameRejection = false;
                        }
                        if (!shouldDelayFlowRegistration || !"PullTestFlow".equals(stream.queue)) {
                            stream.respond(PollWorkerTaskResponse.newBuilder()
                                .setRegistered(io.superdurable.gen.PollWorkerTaskRegistered
                                        .newBuilder().setSupportsUnknownNameRejection(
                                                supportsUnknownNameRejection)).build());
                        }
                        registrations.add(stream);
                    } else {
                        stream.frames.add(request);
                    }
                }

                @Override
                public void onError(final Throwable failure) {
                    stream.closed.countDown();
                }

                @Override
                public void onCompleted() {
                    stream.closed.countDown();
                    if (closeResponseDelay.isZero()) {
                        responses.onCompleted();
                        stream.serverEof.countDown();
                    } else {
                        readScheduler.schedule(() -> {
                            responses.onCompleted();
                            stream.serverEof.countDown();
                        }, closeResponseDelay.toMillis(), TimeUnit.MILLISECONDS);
                    }
                }
            };
        }

        private PollStream awaitQueue(final String queue) throws InterruptedException {
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            final java.util.List<PollStream> deferred = new java.util.ArrayList<PollStream>();
            try {
                while (System.nanoTime() < deadline) {
                    if (workerFailure.get() != null) {
                        throw new AssertionError("Worker failed before registration", workerFailure.get());
                    }
                    final PollStream stream = registrations.poll(1, TimeUnit.SECONDS);
                    if (stream != null && stream.closed.getCount() == 0) {
                        continue;
                    }
                    if (stream != null && queue.equals(stream.queue)) {
                        return stream;
                    }
                    if (stream != null) {
                        deferred.add(stream);
                    }
                }
                throw new AssertionError("No registration for queue " + queue);
            } finally {
                registrations.addAll(deferred);
            }
        }

        private void shutdown() {
            readScheduler.shutdownNow();
        }

        private java.util.List<PollStream> reconnectAll(final PollStream busy)
                throws InterruptedException {
            final java.util.List<PollStream> draining = new java.util.ArrayList<PollStream>();
            for (PollStream stream : allStreams) {
                if (stream != busy && stream.queue != null && stream.closed.getCount() > 0) {
                    stream.respond(PollWorkerTaskResponse.newBuilder()
                            .setReconnect(PollWorkerTaskReconnect.getDefaultInstance()).build());
                    draining.add(stream);
                }
            }
            for (PollStream stream : draining) {
                assertTrue(stream.closed.await(5, TimeUnit.SECONDS));
            }
            busy.respond(PollWorkerTaskResponse.newBuilder()
                    .setReconnect(PollWorkerTaskReconnect.getDefaultInstance()).build());
            draining.add(busy);
            return draining;
        }

        private void releasePendingReadsAfter(final Duration delay) {
            for (PollStream stream : allStreams) {
                readScheduler.schedule(() -> {
                    if (stream.closed.getCount() > 0) {
                        stream.readControl.request(1);
                    }
                }, delay.toMillis(), TimeUnit.MILLISECONDS);
            }
        }
    }

    private static final class PollStream {
        private final StreamObserver<PollWorkerTaskResponse> responses;
        private final BlockingQueue<PollWorkerTaskRequest> frames =
                new LinkedBlockingQueue<PollWorkerTaskRequest>();
        private final CountDownLatch closed = new CountDownLatch(1);
        private final CountDownLatch serverEof = new CountDownLatch(1);
        private ServerCallStreamObserver<PollWorkerTaskResponse> readControl;
        private String queue;

        private PollStream(final StreamObserver<PollWorkerTaskResponse> responses) {
            this.responses = responses;
        }

        private void assign(final PollWorkerTask task) {
            respond(PollWorkerTaskResponse.newBuilder().setTask(task).build());
        }

        private synchronized void respond(final PollWorkerTaskResponse response) {
            responses.onNext(response);
        }

        private PollWorkerTaskRequest awaitFrame() throws InterruptedException {
            final PollWorkerTaskRequest frame = frames.poll(15, TimeUnit.SECONDS);
            assertNotNull(frame);
            return frame;
        }
    }

    private static final class RunningWorker implements AutoCloseable {
        private final Server server;
        private final PollServer pollServer;
        private final Worker worker;
        private final Thread workerThread;
        private final AtomicReference<Throwable> failure = new AtomicReference<Throwable>();
        private final boolean shouldStopOnRejection;

        private int awaitStableStreamCount() throws Exception {
            final PullWorker pullWorker = pullWorker();
            final Field streamsField = PullWorker.class.getDeclaredField("streams");
            streamsField.setAccessible(true);
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            long stableSince = System.nanoTime();
            int previous = -1;
            synchronized (pullWorker) {
                while (System.nanoTime() < deadline) {
                    final int current = ((java.util.List<?>) streamsField.get(pullWorker)).size();
                    if (current != previous) {
                        previous = current;
                        stableSince = System.nanoTime();
                    }
                    if (System.nanoTime() - stableSince >= TimeUnit.MILLISECONDS.toNanos(300)) {
                        return current;
                    }
                    pullWorker.wait(25L);
                }
            }
            throw new AssertionError("Worker stream count did not stabilize");
        }

        private int idleStreamCount(final String queue) throws Exception {
            final PullWorker pullWorker = pullWorker();
            final Field streamsField = PullWorker.class.getDeclaredField("streams");
            streamsField.setAccessible(true);
            int idle = 0;
            synchronized (pullWorker) {
                for (Object stream : (java.util.List<?>) streamsField.get(pullWorker)) {
                    final Class<?> streamClass = stream.getClass();
                    final Field queueField = streamClass.getDeclaredField("queue");
                    final Field busyField = streamClass.getDeclaredField("isBusy");
                    final Field drainingField = streamClass.getDeclaredField("isDraining");
                    final Field closedField = streamClass.getDeclaredField("isClosed");
                    queueField.setAccessible(true);
                    busyField.setAccessible(true);
                    drainingField.setAccessible(true);
                    closedField.setAccessible(true);
                    if (queue.equals(queueField.get(stream)) && !busyField.getBoolean(stream)
                            && !drainingField.getBoolean(stream) && !closedField.getBoolean(stream)) {
                        idle++;
                    }
                }
            }
            return idle;
        }

        private PullWorker pullWorker() throws ReflectiveOperationException {
            final Field pullWorkerField = Worker.class.getDeclaredField("pullWorker");
            pullWorkerField.setAccessible(true);
            return (PullWorker) pullWorkerField.get(worker);
        }

        private RunningWorker(final PollServer pollServer, final TestFlow flow, final int capacity)
                throws IOException {
            this(pollServer, flow, capacity, Duration.ofSeconds(5));
        }

        private RunningWorker(final PollServer pollServer, final TestFlow flow, final int capacity,
                final Duration stopTimeout)
                throws IOException {
            this(pollServer, flow, capacity, stopTimeout, false, 1);
        }

        private RunningWorker(final PollServer pollServer, final TestFlow flow, final int capacity,
                final Duration stopTimeout, final boolean shouldStopOnRejection, final int idleStreams)
                throws IOException {
            this.pollServer = pollServer;
            this.shouldStopOnRejection = shouldStopOnRejection;
            server = NettyServerBuilder.forPort(0).flowControlWindow(64 * 1024)
                    .addService(new FlowServiceGrpc.FlowServiceImplBase() {
                        @Override
                        public void getServerInfo(
                                final Empty request,
                                final StreamObserver<ServerInfo> observer) {
                            observer.onNext(ServerInfo.newBuilder().setServerVersion("test")
                                    .setMinimumSupportedProtocolVersion(1)
                                    .setCurrentProtocolVersion(2).build());
                            observer.onCompleted();
                        }

                        @Override
                        public void syncAttributeIndexes(
                                final SyncAttributeIndexRequest request,
                                final StreamObserver<SyncAttributeIndexResponse> observer) {
                            observer.onNext(SyncAttributeIndexResponse.getDefaultInstance());
                            observer.onCompleted();
                        }
                    })
                    .addService(pollServer).build().start();
            worker = new Worker(new Registry(Arrays.<Flow<?>>asList(flow)), new EmptyBlobCache(),
                    WorkerOptions.newBuilder().serverAddress("127.0.0.1:" + server.getPort())
                            .pull(PullWorkerOptions.newBuilder()
                                    .appWorkerID("pull-java-unit")
                                    .maxConcurrentTasks(capacity)
                                    .idleStreamsPerQueue(idleStreams)
                                    .stopOnPermanentStreamRejection(shouldStopOnRejection)
                                    .stopDrainTimeout(stopTimeout).build())
                            .grpcErrorStatusMapping(GrpcErrorStatusMapping.newBuilder()
                                    .map(IllegalArgumentException.class, Status.Code.INVALID_ARGUMENT)
                                    .build())
                            .build());
            assertTrue(worker.getWorkerTarget().isPull());
            workerThread = new Thread(() -> {
                try {
                    worker.start();
                } catch (Throwable raised) {
                    failure.set(raised);
                    pollServer.workerFailure.set(raised);
                }
            }, "dex-java-pull-unit-worker");
            workerThread.start();
        }

        private void stop() throws InterruptedException {
            worker.close();
            workerThread.join(10_000L);
            assertFalse(workerThread.isAlive());
            if (shouldStopOnRejection && failure.get() != null) {
                assertTrue(failure.get() instanceof PullRegistrationError);
            } else {
                assertEquals(null, failure.get());
            }
        }

        @Override
        public void close() throws Exception {
            stop();
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            pollServer.shutdown();
        }
    }

    private static final class EmptyBlobCache implements BlobCache {
        @Override
        public Optional<byte[]> get(final String blobID) {
            return Optional.empty();
        }

        @Override
        public boolean put(final String blobID, final byte[] payload) {
            return false;
        }

        @Override
        public void delete(final String blobID) {
        }

        @Override
        public void deleteAll() {
        }

        @Override
        public void close() {
        }
    }
}
