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

import com.google.common.net.HostAndPort;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.superdurable.gen.FlowServiceGrpc;
import io.superdurable.gen.ServerInfo;
import io.superdurable.gen.SyncAttributeIndexRequest;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Hosts synchronous Java Step and RPC implementations for registered Flows.
 *
 * <p>The worker exposes a gRPC listener or opens outbound pull streams to Dex. It dispatches
 * invocations on a bounded JVM executor.
 * User methods are ordinary blocking Java methods; slow external I/O occupies a handler thread and
 * should respect the configured Step or RPC timeout. Call {@link #start} on a dedicated application
 * thread because it blocks until the worker stops. A worker can be started only once. The supplied
 * {@link BlobCache} is borrowed and remains the caller's responsibility.
 *
 * <pre>{@code
 * Worker worker = new Worker(registry, blobCache, WorkerOptions.newBuilder()
 *         .bindAddress("0.0.0.0:8803")
 *         .workerTarget(new WorkerTarget("orders-worker:8803", false))
 *         .build());
 * Runtime.getRuntime().addShutdownHook(new Thread(worker::stop));
 * worker.start();
 * }</pre>
 */
public final class Worker implements AutoCloseable {
    private enum State {
        CREATED,
        RUNNING,
        STOPPING,
        STOPPED,
        CLOSED
    }

    private final Registry registry;
    private final WorkerOptions options;
    private final WorkerTarget workerTarget;
    private final ExecutorService handlers;
    private final ScheduledExecutorService bufferedStreamScheduler;
    private final ManagedChannel flowChannel;
    private final FlowServiceGrpc.FlowServiceBlockingStub flowService;
    private final JavaWorkerService workerService;
    private final PullWorker pullWorker;
    private final String sdkVersion;
    private final NegotiatedServerProtocol serverProtocol = new NegotiatedServerProtocol();
    private Server server;
    private State state = State.CREATED;
    private boolean hasStartCalled;

    /**
     * Creates a worker using local development defaults.
     *
     * @param registry the nonnull registry of Flow implementations
     * @param blobCache the nonnull cache used to hydrate blob-backed invocation values
     * @throws IllegalArgumentException if either argument is {@code null}
     */
    public Worker(final Registry registry, final BlobCache blobCache) {
        this(registry, blobCache, WorkerOptions.newBuilder().build());
    }

    /**
     * Creates a worker with explicit listener or pull, routing, connection, and serialization options.
     *
     * @param registry the nonnull registry of Flow implementations
     * @param blobCache the nonnull cache used to hydrate blob-backed invocation values
     * @param options the nonnull worker options
     * @throws IllegalArgumentException if any argument is {@code null} or the pull Worker ID
     *     conflicts with a registered Flow type
     */
    public Worker(
            final Registry registry,
            final BlobCache blobCache,
            final WorkerOptions options) {
        if (registry == null || blobCache == null || options == null) {
            throw new IllegalArgumentException("registry, blobCache, and options are required");
        }
        this.registry = registry;
        this.options = options;
        this.workerTarget = options.getPull() != null
                ? WorkerTarget.pull()
                : options.getWorkerTarget() == null
                        ? targetFromBindAddress(options.getBindAddress())
                        : options.getWorkerTarget();
        this.handlers = newHandlerExecutor(options.getPull() == null
                ? Math.max(2, Math.min(32, Runtime.getRuntime().availableProcessors()))
                : options.getPull().getMaxConcurrentTasks());
        this.bufferedStreamScheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            final Thread thread = new Thread(runnable, "dex-java-buffered-stream");
            thread.setDaemon(true);
            return thread;
        });
        this.flowChannel = createOutboundChannel(options.getServerAddress());
        final FlowServiceGrpc.FlowServiceBlockingStub serviceStub =
                FlowServiceGrpc.newBlockingStub(flowChannel);
        this.flowService = options.getCredential() == null ? serviceStub
                : serviceStub.withCallCredentials(new GrpcCredential(options.getCredential()));
        this.sdkVersion = ServerProtocolCompatibility.sdkVersion();
        final ValueMapper values = new ValueMapper(options.getObjectMapper());
        final WorkerDispatcher dispatcher = new WorkerDispatcher(
                registry,
                values,
                new ValueHydrator(flowService, blobCache),
                bufferedStreamScheduler,
                serverProtocol);
        this.workerService = new JavaWorkerService(
                dispatcher,
                handlers,
                options.getGrpcErrorStatusMapping());
        try {
            this.pullWorker = options.getPull() == null ? null : new PullWorker(
                    registry, workerService, dispatcher, handlers, flowChannel,
                    options.getPull(), options.getCredential(), sdkVersion);
        } catch (RuntimeException failure) {
            shutdownResources();
            throw failure;
        }
    }

    Registry getRegistry() {
        return registry;
    }

    /**
     * Returns the target this worker advertises for Dex routing.
     *
     * <p>A pull Worker returns a pull target. Otherwise, when options do not specify a target,
     * the worker derives one from its bind address.
     *
     * @return the effective worker target
     */
    public WorkerTarget getWorkerTarget() {
        return workerTarget;
    }

    /**
     * Starts the worker listener or pull streams and blocks until termination.
     *
     * <p>Only a newly created worker may start. It negotiates the Server protocol before syncing
     * Attribute indexes and opening the listener or pull streams. If the waiting thread is interrupted, the worker
     * stops and restores the thread's interruption status before returning.
     *
     * @throws PullRegistrationError if an uncovered permanent stream rejection stops the Worker
     *     with stopOnPermanentStreamRejection enabled, before or after readiness
     * @throws IllegalStateException if the worker was already started, its address cannot bind,
     *     or authentication fails during startup
     */
    public void start() {
        synchronized (this) {
            if (hasStartCalled || state != State.CREATED) {
                throw new IllegalStateException("Worker cannot start from state " + state);
            }
            hasStartCalled = true;
        }
        Throwable startFailure = null;
        try {
            startInternal();
        } catch (RuntimeException | Error failure) {
            startFailure = failure;
            if (pullWorker != null) {
                pullWorker.finishRegistration(failure);
            }
            shutdownResources();
            synchronized (this) {
                state = State.STOPPED;
            }
            throw failure;
        } finally {
            if (pullWorker != null) {
                pullWorker.finishRegistration(startFailure);
            }
        }
    }

    /**
     * Waits until every pull queue has received a Registered acknowledgement at least once.
     *
     * <p>Start the Worker on another thread. Multiple callers may wait independently, before or
     * after start. Success latches even after stop; use getPullRegistrationState for later probes.
     * Before readiness, a permanent rejection fails this wait only if that queue has no other
     * registered stream. Startup failures are propagated to waiters. Interrupting this wait leaves
     * the Worker running.
     *
     * <pre>{@code
     * Thread workerThread = new Thread(worker::start);
     * workerThread.start();
     * worker.waitPullRegistered(Duration.ofSeconds(30));
     * boolean isReady = worker.getPullRegistrationState().isReady();
     * }</pre>
     *
     * @param timeout the nonnull, nonnegative maximum wait duration
     * @throws InterruptedException if the waiting thread is interrupted
     * @throws TimeoutException if the timeout expires before readiness
     * @throws PullRegistrationError if a queue is permanently rejected before readiness
     * @throws IllegalArgumentException if timeout is null or negative
     * @throws IllegalStateException if this is a listening Worker, startup fails, or the Worker
     *     stops before readiness
     */
    public void waitPullRegistered(final Duration timeout) throws InterruptedException, TimeoutException {
        if (timeout == null || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be nonnull and nonnegative");
        }
        if (pullWorker == null) {
            throw new IllegalStateException("waitPullRegistered requires a pull Worker");
        }
        pullWorker.waitRegistered(timeout);
    }

    /**
     * Returns an independent snapshot for pull Worker readiness probes.
     *
     * <p>After initial readiness, transient failures, reconnects, and stop keep readiness true.
     * A permanent rejection makes it false only while that queue has no registered stream.
     * Re-registration clears the error. Safe concurrently with start, stop, and registration waits.
     * See waitPullRegistered for a startup example.
     *
     * @return immutable queue state, or an empty nonready snapshot for a listening Worker
     */
    public PullRegistrationState getPullRegistrationState() {
        return pullWorker == null
                ? new PullRegistrationState(false, Collections.<PullQueueRegistrationState>emptyList())
                : pullWorker.registrationState();
    }

    private void startInternal() {
        final Server runningServer;
        synchronized (this) {
            if (state != State.CREATED) {
                throw new IllegalStateException("Worker cannot start from state " + state);
            }
            final ServerInfo serverInfo;
            try {
                serverInfo = flowService
                        .withDeadlineAfter(
                                options.getAttributeIndexSyncTimeout().toMillis(),
                                TimeUnit.MILLISECONDS)
                        .getServerInfo(Empty.getDefaultInstance());
            } catch (RuntimeException failure) {
                if (Status.fromThrowable(failure).getCode() == Status.Code.UNAUTHENTICATED) {
                    throw authenticationFailure(failure);
                }
                throw ServerProtocolCompatibility.requestFailure(sdkVersion, failure);
            }
            serverProtocol.recordNegotiatedVersion(
                    ServerProtocolCompatibility.negotiate(serverInfo, sdkVersion));
            try {
                flowService
                        .withDeadlineAfter(
                                options.getAttributeIndexSyncTimeout().toMillis(),
                                TimeUnit.MILLISECONDS)
                        .syncAttributeIndexes(SyncAttributeIndexRequest.newBuilder()
                                .putAllAttributeIndexes(registry.getAttributeIndexes())
                                .build());
            } catch (RuntimeException failure) {
                if (Status.fromThrowable(failure).getCode() == Status.Code.UNAUTHENTICATED) {
                    throw authenticationFailure(failure);
                }
                throw new IllegalStateException("cannot synchronize Attribute indexes", failure);
            }
            if (pullWorker == null) {
                try {
                    server = NettyServerBuilder.forAddress(bindAddress(options.getBindAddress()))
                            .addService(workerService)
                            .build()
                            .start();
                } catch (IOException failure) {
                    throw new IllegalStateException(
                            "cannot bind Java Worker to " + options.getBindAddress(),
                            failure);
                }
            }
            state = State.RUNNING;
            runningServer = server;
        }

        boolean interrupted = false;
        try {
            if (pullWorker != null) {
                pullWorker.runBlocking();
            } else {
                runningServer.awaitTermination();
            }
        } catch (InterruptedException interruption) {
            interrupted = true;
        } finally {
            if (pullWorker == null || interrupted || Thread.currentThread().isInterrupted()) {
                stop();
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Gracefully stops the listener or pull streams, handler executor, and server channel.
     *
     * <p>The method is idempotent. Pull mode drains for the configured stop timeout, then cancels
     * remaining handlers and waits for them to return. A handler ignoring cancellation can block
     * Stop. Listener mode waits up to 30 seconds before forcing shutdown. Interruption status is
     * preserved.
     */
    public void stop() {
        final Server runningServer;
        synchronized (this) {
            if (state == State.STOPPED || state == State.CLOSED) {
                return;
            }
            state = State.STOPPING;
            runningServer = server;
        }
        if (runningServer != null) {
            runningServer.shutdown();
            try {
                if (!runningServer.awaitTermination(30, TimeUnit.SECONDS)) {
                    runningServer.shutdownNow();
                    runningServer.awaitTermination(5, TimeUnit.SECONDS);
                }
            } catch (InterruptedException interruption) {
                runningServer.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        shutdownResources();
        synchronized (this) {
            if (state != State.CLOSED) {
                state = State.STOPPED;
            }
        }
    }

    /**
     * Stops the worker and permanently closes its lifecycle.
     *
     * <p>The borrowed {@link BlobCache} is not closed.
     */
    @Override
    public void close() {
        stop();
        synchronized (this) {
            state = State.CLOSED;
        }
    }

    private void shutdownResources() {
        if (pullWorker != null) {
            pullWorker.stop();
        }
        handlers.shutdown();
        try {
            if (!handlers.awaitTermination(30, TimeUnit.SECONDS)) {
                handlers.shutdownNow();
            }
        } catch (InterruptedException interruption) {
            handlers.shutdownNow();
            Thread.currentThread().interrupt();
        }
        bufferedStreamScheduler.shutdownNow();
        flowChannel.shutdown();
        try {
            if (!flowChannel.awaitTermination(5, TimeUnit.SECONDS)) {
                flowChannel.shutdownNow();
            }
        } catch (InterruptedException interruption) {
            flowChannel.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private static IllegalStateException authenticationFailure(final RuntimeException cause) {
        final String description = Status.fromThrowable(cause).getDescription();
        return new IllegalStateException("cannot authenticate Worker with Dex"
                + (description == null ? "" : ": " + description), cause);
    }

    private static ExecutorService newHandlerExecutor(final int concurrency) {
        final int queueCapacity = concurrency * 2;
        final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        final AtomicInteger nextThread = new AtomicInteger();
        return new ThreadPoolExecutor(
                concurrency,
                concurrency,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(queueCapacity),
                runnable -> {
                    final Thread thread = new Thread(
                            runnable,
                            "dex-java-handler-" + nextThread.incrementAndGet());
                    thread.setContextClassLoader(contextClassLoader);
                    thread.setDaemon(true);
                    return thread;
                },
                new ThreadPoolExecutor.AbortPolicy());
    }

    private static ManagedChannel createOutboundChannel(final String address) {
        return ManagedChannelBuilder.forTarget(address).usePlaintext().build();
    }

    private static InetSocketAddress bindAddress(final String address) {
        final HostAndPort parsed = parseAddress(address, "Worker bind address");
        return parsed.getHost().isEmpty()
                ? new InetSocketAddress(parsed.getPort())
                : new InetSocketAddress(parsed.getHost(), parsed.getPort());
    }

    private static WorkerTarget targetFromBindAddress(final String address) {
        final HostAndPort parsed = parseAddress(address, "Worker bind address");
        final String host = parsed.getHost().isEmpty()
                || "0.0.0.0".equals(parsed.getHost())
                || "::".equals(parsed.getHost())
                ? "localhost"
                : parsed.getHost();
        return new WorkerTarget(HostAndPort.fromParts(host, parsed.getPort()).toString(), false);
    }

    private static HostAndPort parseAddress(final String address, final String description) {
        if (address == null || !address.equals(address.trim())) {
            throw new IllegalArgumentException(description + " is required without whitespace");
        }
        final HostAndPort parsed;
        try {
            parsed = HostAndPort.fromString(address);
        } catch (IllegalArgumentException failure) {
            throw new IllegalArgumentException(description + " is invalid: " + address, failure);
        }
        if (!parsed.hasPort() || parsed.getPort() < 1 || parsed.getPort() > 65535) {
            throw new IllegalArgumentException(description + " requires port 1-65535");
        }
        return parsed;
    }
}
