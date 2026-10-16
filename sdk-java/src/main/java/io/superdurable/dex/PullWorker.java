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

import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.superdurable.gen.InvokeExecuteMethodOutput;
import io.superdurable.gen.InvokeWaitForMethodOutput;
import io.superdurable.gen.PollWorkerTask;
import io.superdurable.gen.PollWorkerTaskFrame;
import io.superdurable.gen.PollWorkerTaskPong;
import io.superdurable.gen.PollWorkerTaskRegister;
import io.superdurable.gen.PollWorkerTaskReject;
import io.superdurable.gen.PollWorkerTaskRejectReason;
import io.superdurable.gen.PollWorkerTaskRequest;
import io.superdurable.gen.PollWorkerTaskResponse;
import io.superdurable.gen.StepMethodHeartbeat;
import io.superdurable.gen.StepStreamWrite;
import io.superdurable.gen.WorkerTaskServiceGrpc;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.function.Supplier;

final class PullWorker {
    private static final Logger LOGGER = Logger.getLogger(PullWorker.class.getName());
    private static final char[] BASE32 = "abcdefghijklmnopqrstuvwxyz234567".toCharArray();
    private final WorkerTaskServiceGrpc.WorkerTaskServiceStub taskService;
    private final JavaWorkerService workerService;
    private final WorkerDispatcher dispatcher;
    private final ExecutorService handlers;
    private final ScheduledExecutorService scheduler;
    private final PullWorkerOptions options;
    private final List<String> flowTypes;
    private final Map<String, Registry.RegisteredFlow> registeredFlows;
    private final List<String> queues;
    private final List<PullStream> streams = new ArrayList<PullStream>();
    private final String appWorkerID;
    private final String sdkVersion;
    private final Random jitter = new SecureRandom();
    private int busyTasks;
    private boolean isStopping;
    private boolean hasLoggedUnsupported;
    private long stopDeadlineNanos;

    PullWorker(
            final Registry registry,
            final JavaWorkerService workerService,
            final WorkerDispatcher dispatcher,
            final ExecutorService handlers,
            final ManagedChannel channel,
            final PullWorkerOptions options,
            final Credential credential,
            final String sdkVersion) {
        this.workerService = workerService;
        this.dispatcher = dispatcher;
        this.handlers = handlers;
        final WorkerTaskServiceGrpc.WorkerTaskServiceStub serviceStub =
                WorkerTaskServiceGrpc.newStub(channel);
        this.taskService = credential == null ? serviceStub
                : serviceStub.withCallCredentials(new GrpcCredential(credential));
        this.options = options;
        this.sdkVersion = sdkVersion;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            final Thread thread = new Thread(runnable, "dex-java-pull-timer");
            thread.setDaemon(true);
            return thread;
        });
        final List<String> registered = new ArrayList<String>();
        final Map<String, Registry.RegisteredFlow> definitions =
                new HashMap<String, Registry.RegisteredFlow>();
        for (Flow<?> flow : registry.getFlows()) {
            registered.add(flow.getFlowType());
            definitions.put(flow.getFlowType(), registry.getFlow(flow.getFlowType()));
        }
        this.registeredFlows = Collections.unmodifiableMap(definitions);
        Collections.sort(registered);
        this.flowTypes = Collections.unmodifiableList(registered);
        this.appWorkerID = options.getAppWorkerID().isEmpty()
                ? generateAppWorkerID(defaultWorkerIdentity()) : options.getAppWorkerID();
        if (registered.contains(appWorkerID)) {
            scheduler.shutdownNow();
            throw new IllegalArgumentException("appWorkerID conflicts with Flow type " + appWorkerID);
        }
        this.queues = new ArrayList<String>();
        queues.add(appWorkerID);
        queues.addAll(registered);
    }

    private PollWorkerTaskRejectReason unknownTaskReason(final PollWorkerTask task) {
        switch (task.getRequestCase()) {
            case WAIT_FOR:
                final Registry.RegisteredFlow waitFlow =
                        registeredFlows.get(task.getWaitFor().getFlowType());
                return waitFlow != null && waitFlow.getSteps().containsKey(task.getWaitFor().getStepType())
                        ? PollWorkerTaskRejectReason.POLL_WORKER_TASK_REJECT_REASON_UNSPECIFIED
                        : PollWorkerTaskRejectReason.POLL_WORKER_TASK_REJECT_REASON_UNKNOWN_STEP;
            case EXECUTE:
                final Registry.RegisteredFlow executeFlow =
                        registeredFlows.get(task.getExecute().getFlowType());
                if ("sys:timeout_handler".equals(task.getExecute().getStepType())) {
                    return executeFlow != null && executeFlow.hasTimeoutHandler()
                            ? PollWorkerTaskRejectReason.POLL_WORKER_TASK_REJECT_REASON_UNSPECIFIED
                            : PollWorkerTaskRejectReason.POLL_WORKER_TASK_REJECT_REASON_UNKNOWN_STEP;
                }
                return executeFlow != null && executeFlow.getSteps().containsKey(task.getExecute().getStepType())
                        ? PollWorkerTaskRejectReason.POLL_WORKER_TASK_REJECT_REASON_UNSPECIFIED
                        : PollWorkerTaskRejectReason.POLL_WORKER_TASK_REJECT_REASON_UNKNOWN_STEP;
            case RPC:
                final Registry.RegisteredFlow rpcFlow =
                        registeredFlows.get(task.getRpc().getFlowType());
                return rpcFlow != null && rpcFlow.getRpcs().containsKey(task.getRpc().getRpcName())
                        ? PollWorkerTaskRejectReason.POLL_WORKER_TASK_REJECT_REASON_UNSPECIFIED
                        : PollWorkerTaskRejectReason.POLL_WORKER_TASK_REJECT_REASON_UNKNOWN_RPC;
            default:
                return PollWorkerTaskRejectReason.POLL_WORKER_TASK_REJECT_REASON_UNSPECIFIED;
        }
    }

    String getAppWorkerID() {
        return appWorkerID;
    }

    static String defaultWorkerIdentity() {
        return defaultWorkerIdentity(System.getenv(), () -> {
            try {
                return InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException unavailable) {
                return "worker";
            }
        });
    }

    static String defaultWorkerIdentity(
            final Map<String, String> environment,
            final Supplier<String> hostname) {
        for (String environmentName : new String[] {"DEX_WORKER_INSTANCE", "POD_NAME", "HOSTNAME"}) {
            final String candidate = environment.get(environmentName);
            if (candidate != null && !candidate.trim().isEmpty()) {
                return candidate;
            }
        }
        final String resolved = hostname.get();
        return resolved == null || resolved.trim().isEmpty() ? "worker" : resolved;
    }

    static String generateAppWorkerID(final String identity) {
        final StringBuilder sanitized = new StringBuilder();
        for (char character : identity.toLowerCase(java.util.Locale.ROOT).toCharArray()) {
            if ((character >= 'a' && character <= 'z')
                    || (character >= '0' && character <= '9') || character == '-') {
                sanitized.append(character);
            } else if (sanitized.length() > 0) {
                sanitized.append('-');
            }
            if (sanitized.length() >= 54) {
                break;
            }
        }
        String prefix = sanitized.toString().replaceAll("^-+|-+$", "");
        if (prefix.isEmpty()) {
            prefix = "worker";
        }
        final byte[] randomBytes = new byte[5];
        new SecureRandom().nextBytes(randomBytes);
        long bits = 0L;
        for (byte value : randomBytes) {
            bits = (bits << 8) | (value & 0xffL);
        }
        final StringBuilder generated = new StringBuilder(prefix).append('-');
        for (int index = 7; index >= 0; index--) {
            generated.append(BASE32[(int) (bits >>> (index * 5)) & 31]);
        }
        return generated.toString();
    }

    void runBlocking() {
        boolean interrupted = false;
        synchronized (this) {
            reconcile();
            while (!isStopping || !streams.isEmpty() || busyTasks > 0) {
                try {
                    wait();
                } catch (InterruptedException interruption) {
                    interrupted = true;
                    break;
                }
            }
        }
        if (interrupted) {
            stop();
            Thread.currentThread().interrupt();
        }
    }

    void stop() {
        boolean interrupted = false;
        synchronized (this) {
            if (!isStopping) {
                isStopping = true;
                stopDeadlineNanos = System.nanoTime() + options.getStopDrainTimeout().toNanos();
            }
            final long deadline = stopDeadlineNanos;
            notifyAll();
            for (PullStream stream : new ArrayList<PullStream>(streams)) {
                stream.isDraining = true;
                if (!stream.isBusy) {
                    stream.halfClose();
                }
            }
            while ((!streams.isEmpty() || busyTasks > 0) && System.nanoTime() < deadline) {
                interrupted |= awaitUntil(deadline);
            }
            if (!streams.isEmpty() || busyTasks > 0) {
                for (PullStream stream : new ArrayList<PullStream>(streams)) {
                    stream.cancelTask(Status.Code.CANCELLED);
                    if (!stream.isHalfClosed && !stream.isClosed && stream.requests != null) {
                        final RuntimeException cancellation = Status.CANCELLED.asRuntimeException();
                        try {
                            stream.requests.onError(cancellation);
                        } catch (RuntimeException failure) {
                            LOGGER.log(Level.WARNING, "cancel Worker task stream", failure);
                        } finally {
                            stream.close(cancellation);
                        }
                    }
                }
                while (busyTasks > 0 || !streams.isEmpty()) {
                    try {
                        wait();
                    } catch (InterruptedException interruption) {
                        interrupted = true;
                    }
                }
            }
        }
        scheduler.shutdownNow();
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private boolean awaitUntil(final long deadline) {
        final long remaining = Math.max(1L, deadline - System.nanoTime());
        try {
            TimeUnit.NANOSECONDS.timedWait(this, remaining);
            return false;
        } catch (InterruptedException interruption) {
            return true;
        }
    }

    private void reconcile() {
        if (isStopping) {
            return;
        }
        for (String queue : queues) {
            int idle = 0;
            for (PullStream stream : streams) {
                if (stream.queue.equals(queue) && !stream.isBusy && !stream.isDraining) {
                    idle++;
                }
            }
            while (idle < options.getIdleStreamsPerQueue()) {
                final PullStream stream = new PullStream(queue);
                streams.add(stream);
                stream.open();
                idle++;
            }
        }
    }

    private long retryDelay(final long backoff) {
        return backoff + (long) (jitter.nextDouble() * backoff / 2.0);
    }

    private final class PullStream implements StreamObserver<PollWorkerTaskResponse> {
        private final String queue;
        private StreamObserver<PollWorkerTaskRequest> requests;
        private Context.CancellableContext taskContext;
        private ScheduledFuture<?> deadlineTimer;
        private ScheduledFuture<?> closeTimer;
        private Thread handlerThread;
        private Throwable closeFailure;
        private String taskID;
        private volatile Status.Code cancellationStatus;
        private long backoffMillis = 100L;
        private boolean isBusy;
        private boolean isDraining;
        private boolean isHalfClosed;
        private boolean isClosed;
        private boolean isRegistered;

        private PullStream(final String queue) {
            this.queue = queue;
        }

        private void open() {
            isClosed = false;
            isHalfClosed = false;
            isRegistered = false;
            closeFailure = null;
            try {
                requests = taskService.pollWorkerTask(this);
                send(PollWorkerTaskRequest.newBuilder()
                        .setRegister(PollWorkerTaskRegister.newBuilder()
                                .setQueue(queue)
                                .setAppWorkerId(appWorkerID)
                                .addAllFlowTypes(flowTypes)
                                .setSdkName("java")
                                .setSdkVersion(sdkVersion)
                                .build())
                        .build());
            } catch (RuntimeException failure) {
                close(failure);
            }
        }

        @Override
        public void onNext(final PollWorkerTaskResponse response) {
            synchronized (PullWorker.this) {
                if (isClosed) {
                    return;
                }
                if (!isRegistered) {
                    if (response.hasReconnect()) {
                        isDraining = true;
                        halfClose();
                    } else if (response.hasRegistered()) {
                        isRegistered = true;
                        backoffMillis = 100L;
                    } else {
                        requests.onError(Status.INTERNAL
                                .withDescription("Worker task stream did not register")
                                .asRuntimeException());
                    }
                    return;
                }
                if (response.hasTask()) {
                    startTask(response.getTask());
                } else if (response.hasCancelTask()) {
                    if (response.getCancelTask().getTaskId().equals(taskID)) {
                        cancelTask(Status.Code.CANCELLED);
                    }
                } else if (response.hasPing()) {
                    send(PollWorkerTaskRequest.newBuilder()
                            .setPong(PollWorkerTaskPong.getDefaultInstance()).build());
                } else if (response.hasReconnect()) {
                    isDraining = true;
                    if (!isBusy) {
                        halfClose();
                    } else {
                        reconcile();
                    }
                }
            }
        }

        private void startTask(final PollWorkerTask task) {
            final PollWorkerTaskRejectReason unknownReason = unknownTaskReason(task);
            if (unknownReason != PollWorkerTaskRejectReason.POLL_WORKER_TASK_REJECT_REASON_UNSPECIFIED) {
                reject(task.getTaskId(), unknownReason);
                return;
            }
            if (isBusy || isDraining || isStopping
                    || busyTasks >= options.getMaxConcurrentTasks()) {
                reject(task.getTaskId(), PollWorkerTaskRejectReason.POLL_WORKER_TASK_REJECT_REASON_NO_CAPACITY);
                return;
            }
            isBusy = true;
            busyTasks++;
            taskID = task.getTaskId();
            taskContext = Context.current().withCancellation();
            cancellationStatus = null;
            if (task.hasDeadline()) {
                final Instant deadline = Instant.ofEpochSecond(
                        task.getDeadline().getSeconds(), task.getDeadline().getNanos());
                final long delay = Duration.between(Instant.now(), deadline).toMillis();
                if (delay <= 0L) {
                    cancelTask(Status.Code.DEADLINE_EXCEEDED);
                } else {
                    final String deadlineTaskID = taskID;
                    deadlineTimer = scheduler.schedule(() -> {
                        synchronized (PullWorker.this) {
                            if (deadlineTaskID.equals(taskID)) {
                                cancelTask(Status.Code.DEADLINE_EXCEEDED);
                            }
                        }
                    },
                            delay, TimeUnit.MILLISECONDS);
                }
            }
            try {
                handlers.execute(() -> runTask(task));
            } catch (RejectedExecutionException rejected) {
                finishTask();
                reject(task.getTaskId(), PollWorkerTaskRejectReason.POLL_WORKER_TASK_REJECT_REASON_NO_CAPACITY);
                return;
            }
            reconcile();
        }

        private void reject(final String rejectedTaskID, final PollWorkerTaskRejectReason reason) {
            send(PollWorkerTaskRequest.newBuilder()
                    .setReject(PollWorkerTaskReject.newBuilder()
                            .setTaskId(rejectedTaskID)
                            .setReason(reason)
                            .build())
                    .build());
        }

        private void runTask(final PollWorkerTask task) {
            synchronized (PullWorker.this) {
                handlerThread = Thread.currentThread();
            }
            final Context previous = taskContext.attach();
            try {
                if (taskContext.isCancelled()) {
                    throw Status.CANCELLED.asRuntimeException();
                }
                switch (task.getRequestCase()) {
                    case WAIT_FOR:
                        sendWaitFor(task);
                        break;
                    case EXECUTE:
                        sendExecute(task);
                        break;
                    case RPC:
                        sendFrame(PollWorkerTaskFrame.newBuilder()
                                .setRpc(dispatcher.invokeRpc(task.getRpc())).build());
                        break;
                    default:
                        throw new IllegalArgumentException("unsupported Worker task request");
                }
            } catch (Throwable failure) {
                LOGGER.log(Level.SEVERE, "Java pull Worker invocation failed", failure);
                sendFrame(PollWorkerTaskFrame.newBuilder()
                        .setWorkerError(workerService.pullWorkerError(failure, cancellationStatus))
                        .build());
            } finally {
                taskContext.detach(previous);
                synchronized (PullWorker.this) {
                    finishTask();
                }
            }
        }

        private void sendWaitFor(final PollWorkerTask task) throws Throwable {
            final StepOutputEmitter emitter = new StepOutputEmitter() {
                @Override
                public void emitHeartbeat(final StepMethodHeartbeat heartbeat) {
                    sendFrame(PollWorkerTaskFrame.newBuilder()
                            .setWaitFor(InvokeWaitForMethodOutput.newBuilder()
                                    .setHeartbeat(heartbeat).build()).build());
                }

                @Override
                public void emitStreamWrite(final StepStreamWrite streamWrite) {
                    sendFrame(PollWorkerTaskFrame.newBuilder()
                            .setWaitFor(InvokeWaitForMethodOutput.newBuilder()
                                    .setStreamWrite(streamWrite).build()).build());
                }
            };
            sendFrame(PollWorkerTaskFrame.newBuilder()
                    .setWaitFor(InvokeWaitForMethodOutput.newBuilder()
                            .setResult(dispatcher.invokeWaitFor(task.getWaitFor(), emitter)).build())
                    .build());
        }

        private void sendExecute(final PollWorkerTask task) throws Throwable {
            final StepOutputEmitter emitter = new StepOutputEmitter() {
                @Override
                public void emitHeartbeat(final StepMethodHeartbeat heartbeat) {
                    sendFrame(PollWorkerTaskFrame.newBuilder()
                            .setExecute(InvokeExecuteMethodOutput.newBuilder()
                                    .setHeartbeat(heartbeat).build()).build());
                }

                @Override
                public void emitStreamWrite(final StepStreamWrite streamWrite) {
                    sendFrame(PollWorkerTaskFrame.newBuilder()
                            .setExecute(InvokeExecuteMethodOutput.newBuilder()
                                    .setStreamWrite(streamWrite).build()).build());
                }
            };
            sendFrame(PollWorkerTaskFrame.newBuilder()
                    .setExecute(InvokeExecuteMethodOutput.newBuilder()
                            .setResult(dispatcher.invokeExecute(task.getExecute(), emitter)).build())
                    .build());
        }

        private void sendFrame(final PollWorkerTaskFrame frame) {
            if (!frame.hasWorkerError() && cancellationStatus != null) {
                throw Status.fromCode(cancellationStatus).asRuntimeException();
            }
            send(PollWorkerTaskRequest.newBuilder()
                    .setTaskFrame(frame.toBuilder().setTaskId(taskID)).build());
        }

        private void send(final PollWorkerTaskRequest request) {
            synchronized (PullWorker.this) {
                if (!isHalfClosed && !isClosed && requests != null) {
                    requests.onNext(request);
                }
            }
        }

        private void cancelTask(final Status.Code code) {
            synchronized (PullWorker.this) {
                if (taskContext != null && cancellationStatus == null) {
                    cancellationStatus = code;
                    taskContext.cancel(Status.fromCode(code).asRuntimeException());
                    if (handlerThread != null) {
                        handlerThread.interrupt();
                    }
                }
            }
        }

        private void finishTask() {
            if (deadlineTimer != null) {
                deadlineTimer.cancel(false);
                deadlineTimer = null;
            }
            if (taskContext != null) {
                taskContext.cancel(null);
                taskContext = null;
            }
            handlerThread = null;
            taskID = null;
            isBusy = false;
            busyTasks--;
            if (isClosed) {
                retry(closeFailure);
            } else if (isDraining || isStopping) {
                halfClose();
            } else {
                reconcile();
            }
            PullWorker.this.notifyAll();
        }

        private void halfClose() {
            if (isClosed) {
                if (!isBusy && isStopping) {
                    streams.remove(this);
                    PullWorker.this.notifyAll();
                }
                return;
            }
            if (isHalfClosed) {
                if (isStopping) {
                    scheduleCloseFallback();
                }
                return;
            }
            isHalfClosed = true;
            try {
                requests.onCompleted();
            } catch (RuntimeException failure) {
                close(failure);
                return;
            }
            scheduleCloseFallback();
        }

        private void scheduleCloseFallback() {
            if (closeTimer != null) {
                closeTimer.cancel(false);
            }
            final long waitNanos = isStopping
                    ? Math.max(1L, stopDeadlineNanos - System.nanoTime())
                    : options.getStopDrainTimeout().toNanos();
            closeTimer = scheduler.schedule(() -> {
                synchronized (PullWorker.this) {
                    if (!isClosed) {
                        final RuntimeException timeout = Status.DEADLINE_EXCEEDED
                                .withDescription("Worker stream close timed out")
                                .asRuntimeException();
                        try {
                            requests.onError(timeout);
                        } catch (RuntimeException failure) {
                            LOGGER.log(Level.WARNING, "cancel timed-out Worker task stream", failure);
                        } finally {
                            close(timeout);
                        }
                    }
                }
            }, waitNanos, TimeUnit.NANOSECONDS);
        }

        @Override
        public void onError(final Throwable failure) {
            close(failure);
        }

        @Override
        public void onCompleted() {
            close(null);
        }

        private void close(final Throwable failure) {
            synchronized (PullWorker.this) {
                if (isClosed) {
                    return;
                }
                isClosed = true;
                closeFailure = failure;
                if (closeTimer != null) {
                    closeTimer.cancel(false);
                }
                if (isBusy) {
                    cancelTask(Status.Code.CANCELLED);
                    return;
                }
                retry(failure);
            }
        }

        private void retry(final Throwable failure) {
            if (isStopping) {
                streams.remove(this);
                PullWorker.this.notifyAll();
                return;
            }
            if (isDraining && hasIdleReplacement()) {
                streams.remove(this);
                PullWorker.this.notifyAll();
                return;
            }
            final Status.Code code = failure == null ? Status.Code.OK
                    : Status.fromThrowable(failure).getCode();
            final boolean isPermanent = code == Status.Code.UNAUTHENTICATED
                    || code == Status.Code.PERMISSION_DENIED
                    || code == Status.Code.INVALID_ARGUMENT
                    || code == Status.Code.UNIMPLEMENTED
                    || code == Status.Code.RESOURCE_EXHAUSTED;
            if (code == Status.Code.UNIMPLEMENTED && !hasLoggedUnsupported) {
                hasLoggedUnsupported = true;
                LOGGER.log(Level.SEVERE, "pull Worker polling is unsupported by this Dex server", failure);
            } else if (isPermanent && code != Status.Code.UNIMPLEMENTED) {
                LOGGER.log(Level.SEVERE, "Worker task stream rejected for queue " + queue, failure);
            } else if (failure != null && code != Status.Code.CANCELLED) {
                LOGGER.log(Level.WARNING, "Worker task stream disconnected for queue " + queue, failure);
            }
            if (isDraining) {
                backoffMillis = 100L;
            }
            final long delay = retryDelay(isPermanent ? 60_000L : backoffMillis);
            if (isDraining) {
                isDraining = false;
            } else if (!isPermanent) {
                backoffMillis = Math.min(backoffMillis * 2, 5000L);
            }
            scheduler.schedule(() -> {
                synchronized (PullWorker.this) {
                    if (!isStopping) {
                        open();
                    }
                }
            }, delay, TimeUnit.MILLISECONDS);
            PullWorker.this.notifyAll();
        }

        private boolean hasIdleReplacement() {
            int idleReplacements = 0;
            for (PullStream stream : streams) {
                if (stream != this && stream.queue.equals(queue)
                        && !stream.isBusy && !stream.isDraining) {
                    idleReplacements++;
                }
            }
            return idleReplacements >= options.getIdleStreamsPerQueue();
        }
    }
}
