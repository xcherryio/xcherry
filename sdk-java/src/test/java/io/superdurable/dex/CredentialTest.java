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
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.superdurable.gen.FlowServiceGrpc;
import io.superdurable.gen.PollWorkerTaskRequest;
import io.superdurable.gen.PollWorkerTaskResponse;
import io.superdurable.gen.SearchFlowsRequest;
import io.superdurable.gen.SearchFlowsResponse;
import io.superdurable.gen.ServerInfo;
import io.superdurable.gen.SyncAttributeIndexRequest;
import io.superdurable.gen.SyncAttributeIndexResponse;
import io.superdurable.gen.WorkerTaskServiceGrpc;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class CredentialTest {
    private static final Metadata.Key<String> AUTHORIZATION =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    @TempDir
    Path tempDirectory;

    @Test
    void fileCredentialRefreshesAndFailsClosed() throws Exception {
        final Path path = tempDirectory.resolve("token");
        Files.write(path, " initial-token \n".getBytes(StandardCharsets.UTF_8));
        final AtomicLong now = new AtomicLong(10_000_000_000L);
        final FileCredential credential = new FileCredential(path, now::get);
        assertEquals("initial-token", credential.token());
        assertFalse(credential.toString().contains("initial-token"));
        assertFalse(ClientOptions.newBuilder().credential(credential).build()
                .toString().contains("initial-token"));
        assertFalse(WorkerOptions.newBuilder().credential(credential).build()
                .toString().contains("initial-token"));

        Files.write(path, "rotated-token-longer".getBytes(StandardCharsets.UTF_8));
        assertEquals("initial-token", credential.token());
        now.addAndGet(1_000_000_000L);
        assertEquals("rotated-token-longer", credential.token());

        Files.write(path, "  \n".getBytes(StandardCharsets.UTF_8));
        now.addAndGet(1_000_000_000L);
        assertThrows(IOException.class, credential::token);
        Files.delete(path);
        now.addAndGet(1_000_000_000L);
        assertThrows(IOException.class, credential::token);
        Files.write(path, "restored".getBytes(StandardCharsets.UTF_8));
        now.addAndGet(1_000_000_000L);
        assertEquals("restored", credential.token());
        assertThrows(IOException.class, () -> new FileCredential(tempDirectory.resolve("missing")));
        Files.write(path, " \n".getBytes(StandardCharsets.UTF_8));
        assertThrows(IOException.class, () -> new FileCredential(path));
    }

    @Test
    void clientAddsMetadataPerCallAndDefaultAddsNone() throws Exception {
        try (FakeServer fakeServer = new FakeServer()) {
            final Path path = tempDirectory.resolve("client-token");
            Files.write(path, "first-token".getBytes(StandardCharsets.UTF_8));
            final AtomicLong now = new AtomicLong(10_000_000_000L);
            final FileCredential credential = new FileCredential(path, now::get);
            final Registry registry = new Registry(Collections.<Flow<?>>emptyList());
            try (Client client = new Client(registry, new EmptyBlobCache(),
                    ClientOptions.newBuilder().serverAddress(fakeServer.address())
                            .credential(credential).build())) {
                client.searchFlows("", 1);
                assertEquals("Bearer first-token", fakeServer.takeHeader("SearchFlows"));
                Files.write(path, "second-token-longer".getBytes(StandardCharsets.UTF_8));
                now.addAndGet(1_000_000_000L);
                client.searchFlows("", 1);
                assertEquals("Bearer second-token-longer", fakeServer.takeHeader("SearchFlows"));
                Files.write(path, " \n".getBytes(StandardCharsets.UTF_8));
                now.addAndGet(1_000_000_000L);
                final long requestCount = fakeServer.searchRequests.get();
                assertThrows(RuntimeException.class, () -> client.searchFlows("", 1));
                assertEquals(requestCount, fakeServer.searchRequests.get());
            }
            try (Client client = new Client(registry, new EmptyBlobCache(),
                    new ClientOptions(fakeServer.address()))) {
                client.searchFlows("", 1);
                assertEquals("<none>", fakeServer.takeHeader("SearchFlows"));
            }
            try (Client client = new Client(registry, new EmptyBlobCache(),
                    ClientOptions.newBuilder().serverAddress(fakeServer.address())
                            .credential(() -> {
                                throw new IOException("token unavailable");
                            }).build())) {
                final long requestCount = fakeServer.searchRequests.get();
                assertThrows(RuntimeException.class, () -> client.searchFlows("", 1));
                assertEquals(requestCount, fakeServer.searchRequests.get());
            }
        }
    }

    @Test
    void workerAuthenticatesStartupAndEachPullStreamOpen() throws Exception {
        try (FakeServer fakeServer = new FakeServer()) {
            final AtomicReference<String> token = new AtomicReference<String>("worker-one");
            final Worker worker = new Worker(
                    new Registry(Collections.<Flow<?>>emptyList()), new EmptyBlobCache(),
                    WorkerOptions.newBuilder().serverAddress(fakeServer.address())
                            .credential(token::get)
                            .pull(PullWorkerOptions.newBuilder().appWorkerID("auth-worker").build())
                            .build());
            final AtomicReference<Throwable> failure = new AtomicReference<Throwable>();
            final Thread workerThread = new Thread(() -> {
                try {
                    worker.start();
                } catch (Throwable raised) {
                    failure.set(raised);
                }
            });
            workerThread.setDaemon(true);
            workerThread.start();
            try {
                assertEquals("Bearer worker-one", fakeServer.takeHeader("GetServerInfo"));
                assertEquals("Bearer worker-one", fakeServer.takeHeader("SyncAttributeIndexes"));
                assertEquals("Bearer worker-one", fakeServer.takeHeader("PollWorkerTask"));
                token.set("worker-two");
                final StreamObserver<PollWorkerTaskResponse> stream = fakeServer.takePullStream();
                stream.onError(Status.UNAVAILABLE.asRuntimeException());
                assertEquals("Bearer worker-two", fakeServer.takeHeader("PollWorkerTask"));
            } finally {
                worker.close();
                workerThread.join(10_000L);
            }
            assertFalse(workerThread.isAlive());
            assertEquals(null, failure.get());
        }
    }

    @Test
    void workerDefaultSendsNoAuthorizationMetadata() throws Exception {
        try (FakeServer fakeServer = new FakeServer()) {
            final Worker worker = new Worker(
                    new Registry(Collections.<Flow<?>>emptyList()), new EmptyBlobCache(),
                    WorkerOptions.newBuilder().serverAddress(fakeServer.address())
                            .pull(PullWorkerOptions.newBuilder().appWorkerID("no-auth-worker").build())
                            .build());
            final AtomicReference<Throwable> failure = new AtomicReference<Throwable>();
            final Thread workerThread = new Thread(() -> {
                try {
                    worker.start();
                } catch (Throwable raised) {
                    failure.set(raised);
                }
            });
            workerThread.setDaemon(true);
            workerThread.start();
            try {
                assertEquals("<none>", fakeServer.takeHeader("GetServerInfo"));
                assertEquals("<none>", fakeServer.takeHeader("SyncAttributeIndexes"));
                assertEquals("<none>", fakeServer.takeHeader("PollWorkerTask"));
            } finally {
                worker.close();
                workerThread.join(10_000L);
            }
            assertFalse(workerThread.isAlive());
            assertEquals(null, failure.get());
        }
    }

    @Test
    void deadlineAndCancellationEndCallsWhileTokenSourceBlocks() throws Exception {
        try (FakeServer fakeServer = new FakeServer()) {
            final ManagedChannel channel = ManagedChannelBuilder.forTarget(fakeServer.address())
                    .usePlaintext().build();
            final ExecutorService calls = Executors.newSingleThreadExecutor(runnable -> {
                final Thread thread = new Thread(runnable, "dex-credential-deadline-test");
                thread.setDaemon(true);
                return thread;
            });
            final CountDownLatch tokenStarted = new CountDownLatch(1);
            final CountDownLatch secondTokenStarted = new CountDownLatch(1);
            final CountDownLatch releaseToken = new CountDownLatch(1);
            final AtomicInteger tokenCalls = new AtomicInteger();
            final Credential blocked = () -> {
                tokenStarted.countDown();
                if (tokenCalls.incrementAndGet() == 2) {
                    secondTokenStarted.countDown();
                }
                releaseToken.await();
                return "late-token";
            };
            try {
                final FlowServiceGrpc.FlowServiceBlockingStub blocking =
                        FlowServiceGrpc.newBlockingStub(channel)
                                .withCallCredentials(new GrpcCredential(blocked))
                                .withDeadlineAfter(1, TimeUnit.SECONDS);
                final Future<Status.Code> deadlineCall = calls.submit(() -> {
                    try {
                        blocking.searchFlows(SearchFlowsRequest.getDefaultInstance());
                        throw new AssertionError("blocked credential unexpectedly reached Dex");
                    } catch (RuntimeException failure) {
                        return Status.fromThrowable(failure).getCode();
                    }
                });
                assertTrue(tokenStarted.await(5, TimeUnit.SECONDS));
                assertEquals(Status.Code.DEADLINE_EXCEEDED,
                        deadlineCall.get(3, TimeUnit.SECONDS));

                final CountDownLatch cancelled = new CountDownLatch(1);
                final AtomicReference<Status.Code> cancellationCode =
                        new AtomicReference<Status.Code>();
                final Context.CancellableContext context = Context.current().withCancellation();
                context.run(() -> FlowServiceGrpc.newStub(channel)
                        .withCallCredentials(new GrpcCredential(blocked))
                        .searchFlows(SearchFlowsRequest.getDefaultInstance(),
                                new StreamObserver<SearchFlowsResponse>() {
                                    @Override
                                    public void onNext(final SearchFlowsResponse response) {
                                    }

                                    @Override
                                    public void onError(final Throwable error) {
                                        cancellationCode.set(Status.fromThrowable(error).getCode());
                                        cancelled.countDown();
                                    }

                                    @Override
                                    public void onCompleted() {
                                    }
                                }));
                assertTrue(secondTokenStarted.await(2, TimeUnit.SECONDS));
                context.cancel(null);
                assertTrue(cancelled.await(2, TimeUnit.SECONDS));
                assertEquals(Status.Code.CANCELLED, cancellationCode.get());
                assertEquals(0L, fakeServer.searchRequests.get());
            } finally {
                releaseToken.countDown();
                calls.shutdownNow();
                assertTrue(calls.awaitTermination(5, TimeUnit.SECONDS));
                channel.shutdownNow();
                assertTrue(channel.awaitTermination(5, TimeUnit.SECONDS));
            }
        }
    }

    private static final class FakeServer implements AutoCloseable {
        private final BlockingQueue<String> headers = new LinkedBlockingQueue<String>();
        private final BlockingQueue<StreamObserver<PollWorkerTaskResponse>> pullStreams =
                new LinkedBlockingQueue<StreamObserver<PollWorkerTaskResponse>>();
        private final AtomicLong searchRequests = new AtomicLong();
        private final Server server;

        private FakeServer() throws IOException {
            final ServerInterceptor capture = new ServerInterceptor() {
                @Override
                public <RequestT, ResponseT> ServerCall.Listener<RequestT> interceptCall(
                        final ServerCall<RequestT, ResponseT> call,
                        final Metadata metadata,
                        final ServerCallHandler<RequestT, ResponseT> next) {
                    final String authorization = metadata.get(AUTHORIZATION);
                    headers.add(call.getMethodDescriptor().getBareMethodName() + ":"
                            + (authorization == null ? "<none>" : authorization));
                    return next.startCall(call, metadata);
                }
            };
            server = ServerBuilder.forPort(0)
                    .addService(ServerInterceptors.intercept(
                            new FlowServiceGrpc.FlowServiceImplBase() {
                                @Override
                                public void searchFlows(
                                        final SearchFlowsRequest request,
                                        final StreamObserver<SearchFlowsResponse> observer) {
                                    searchRequests.incrementAndGet();
                                    observer.onNext(SearchFlowsResponse.getDefaultInstance());
                                    observer.onCompleted();
                                }

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
                            }, capture))
                    .addService(ServerInterceptors.intercept(
                            new WorkerTaskServiceGrpc.WorkerTaskServiceImplBase() {
                                @Override
                                public StreamObserver<PollWorkerTaskRequest> pollWorkerTask(
                                        final StreamObserver<PollWorkerTaskResponse> observer) {
                                    pullStreams.add(observer);
                                    return new StreamObserver<PollWorkerTaskRequest>() {
                                        @Override
                                        public void onNext(final PollWorkerTaskRequest request) {
                                        }

                                        @Override
                                        public void onError(final Throwable error) {
                                        }

                                        @Override
                                        public void onCompleted() {
                                            observer.onCompleted();
                                        }
                                    };
                                }
                            }, capture))
                    .build().start();
        }

        private String address() {
            return "127.0.0.1:" + server.getPort();
        }

        private String takeHeader(final String method) throws InterruptedException {
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (System.nanoTime() < deadline) {
                final String received = headers.poll(100, TimeUnit.MILLISECONDS);
                if (received == null) {
                    continue;
                }
                if (received.startsWith(method + ":")) {
                    return received.substring(method.length() + 1);
                }
            }
            throw new AssertionError("no authorization header captured for " + method);
        }

        private StreamObserver<PollWorkerTaskResponse> takePullStream() throws InterruptedException {
            final StreamObserver<PollWorkerTaskResponse> stream =
                    pullStreams.poll(5, TimeUnit.SECONDS);
            assertNotNull(stream);
            return stream;
        }

        @Override
        public void close() throws InterruptedException {
            server.shutdownNow();
            assertTrue(server.awaitTermination(5, TimeUnit.SECONDS));
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
