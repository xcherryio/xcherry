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
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.superdurable.dex.exceptions.DexServiceException;
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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ApiKeyCredentialTest {
    private static final String API_KEY = "private-api-key";
    private static final String TOKEN_A = "private-token-A";
    private static final String TOKEN_B = "private-token-B";

    @Test
    void validatesConstructorAndEndpointScheme() {
        assertThrows(IllegalArgumentException.class,
                () -> ApiKeyCredential.builder(null, "server", API_KEY));
        assertThrows(IllegalArgumentException.class,
                () -> ApiKeyCredential.builder(URI.create("http://example.com/token"), "server", API_KEY));
        assertThrows(IllegalArgumentException.class,
                () -> ApiKeyCredential.builder(URI.create("file:///token"), "server", API_KEY));
        assertThrows(IllegalArgumentException.class,
                () -> ApiKeyCredential.builder(URI.create("https://example.com/token"), "", API_KEY));
        assertThrows(IllegalArgumentException.class,
                () -> ApiKeyCredential.builder(URI.create("https://example.com/token"), "server", ""));
        assertThrows(IllegalArgumentException.class,
                () -> ApiKeyCredential.builder(URI.create("http://128.0.0.1/token"), "server", API_KEY));
        assertThrows(IllegalArgumentException.class,
                () -> ApiKeyCredential.builder(URI.create("http://localhost.evil/token"), "server", API_KEY));
        assertThrows(IllegalArgumentException.class,
                () -> ApiKeyCredential.builder(URI.create("https://user@example.com/token"), "server", API_KEY));
        for (String host : new String[] {"localhost", "127.1.2.3", "[::1]"}) {
            try (ApiKeyCredential credential = ApiKeyCredential.builder(
                    URI.create("http://" + host + "/token"), "server", API_KEY).build()) {
                assertFalse(credential.toString().contains(API_KEY));
            }
        }
        try (ApiKeyCredential credential = ApiKeyCredential.builder(
                URI.create("https://example.com/token"), "server", API_KEY).build()) {
            assertThrows(IllegalArgumentException.class,
                    () -> ApiKeyCredential.builder(URI.create("https://example.com/token"),
                            "server", API_KEY).exchangeTimeout(Duration.ZERO));
        }
    }

    @Test
    void sendsExactJsonAndHeaders() throws Exception {
        try (TokenEndpoint endpoint = new TokenEndpoint();
                ApiKeyCredential credential = ApiKeyCredential.builder(endpoint.url(), "server-17", API_KEY)
                        .build()) {
            endpoint.reply(200, success(TOKEN_A, 60));
            assertEquals(TOKEN_A, credential.token());
            assertFalse(credential.toString().contains(API_KEY));
            assertFalse(credential.toString().contains(TOKEN_A));
            final ExchangeRequest request = endpoint.takeRequest();
            assertEquals("POST", request.method);
            assertEquals("application/json", request.contentType);
            assertEquals("application/json", request.accept);
            assertEquals("{\"server_id\":\"server-17\",\"api_key\":\"private-api-key\"}", request.body);
            assertEquals(1, endpoint.requestCount());
        }
    }

    @Test
    void rejectsEveryInvalidResponseAndRedactsErrors() throws Exception {
        try (TokenEndpoint endpoint = new TokenEndpoint()) {
            final List<Reply> invalid = new ArrayList<Reply>();
            invalid.add(new Reply(401, "{\"error\":\"denied\",\"error_description\":\"bad key\"}"));
            invalid.add(new Reply(200, "not json"));
            invalid.add(new Reply(200, "[]"));
            invalid.add(new Reply(200, "{}"));
            invalid.add(new Reply(200, "{\"access_token\":\"\",\"token_type\":\"Bearer\",\"expires_in\":1}"));
            invalid.add(new Reply(200, "{\"access_token\":1,\"token_type\":\"Bearer\",\"expires_in\":1}"));
            invalid.add(new Reply(200, "{\"access_token\":\"x\",\"token_type\":\"Basic\",\"expires_in\":1}"));
            invalid.add(new Reply(200, "{\"access_token\":\"x\",\"token_type\":\"Bearer\",\"expires_in\":0}"));
            invalid.add(new Reply(200, "{\"access_token\":\"x\",\"token_type\":\"Bearer\",\"expires_in\":1.5}"));
            invalid.add(new Reply(200, "{\"access_token\":\"x\",\"token_type\":\"Bearer\",\"expires_in\":\"1\"}"));
            for (Reply reply : invalid) {
                endpoint.reply(reply.status, reply.body);
                try (ApiKeyCredential credential = ApiKeyCredential.builder(endpoint.url(), "server", API_KEY)
                        .build()) {
                    final ApiKeyCredential.ExchangeException error = assertThrows(
                            ApiKeyCredential.ExchangeException.class, credential::token);
                    assertTrue(error.getMessage().contains(endpoint.url().toString()));
                    assertTrue(error.getMessage().contains("HTTP " + reply.status));
                    assertFalse(error.toString().contains(API_KEY));
                    assertFalse(error.toString().contains(TOKEN_A));
                }
            }
            endpoint.reply(200, success(TOKEN_A, 60));
            try (ApiKeyCredential credential = ApiKeyCredential.builder(endpoint.url(), "server", API_KEY)
                    .build()) {
                assertEquals(TOKEN_A, credential.token());
                credential.rejectToken(TOKEN_A);
                endpoint.reply(503, "{\"error\":\"" + API_KEY + "\",\"error_description\":\""
                        + TOKEN_A + "\"}");
                final ApiKeyCredential.ExchangeException error = assertThrows(
                        ApiKeyCredential.ExchangeException.class, credential::token);
                assertFalse(error.toString().contains(API_KEY));
                assertFalse(error.toString().contains(TOKEN_A));
            }
            endpoint.reply(403, "{\"error\":\"" + repeat("x", 300) + "\"}");
            try (ApiKeyCredential credential = ApiKeyCredential.builder(endpoint.url(), "server", API_KEY)
                    .build()) {
                final ApiKeyCredential.ExchangeException error = assertThrows(
                        ApiKeyCredential.ExchangeException.class, credential::token);
                assertFalse(error.getMessage().contains(repeat("x", 201)));
            }
        }
    }

    @Test
    void refreshesAtEightyPercentAndKeepsValidTokenAfterFailure() throws Exception {
        final AtomicLong clock = new AtomicLong(TimeUnit.SECONDS.toNanos(10));
        try (TokenEndpoint endpoint = new TokenEndpoint();
                ApiKeyCredential credential = ApiKeyCredential.builder(endpoint.url(), "server", API_KEY)
                        .clock(clock::get).build()) {
            endpoint.reply(200, success(TOKEN_A, 30));
            assertEquals(TOKEN_A, credential.token());
            endpoint.takeRequest();
            clock.addAndGet(TimeUnit.SECONDS.toNanos(23));
            assertEquals(TOKEN_A, credential.token());
            assertEquals(1, endpoint.requestCount());
            endpoint.reply(503, "{\"error\":\"unavailable\"}");
            clock.addAndGet(TimeUnit.SECONDS.toNanos(1));
            assertEquals(TOKEN_A, credential.token());
            endpoint.takeRequest();
            endpoint.awaitCompleted(2);
            awaitExchangeCompletion(credential);
            assertEquals(TOKEN_A, credential.token());
            assertEquals(2, endpoint.requestCount());
            endpoint.reply(200, success(TOKEN_B, 20));
            clock.addAndGet(TimeUnit.SECONDS.toNanos(5));
            awaitRequestCount(credential, endpoint, 3);
            endpoint.takeRequest();
            awaitToken(credential, TOKEN_B);
        }
    }

    @Test
    void refreshesTwoSecondTokenWithoutBlockingCallers() throws Exception {
        final AtomicLong clock = new AtomicLong(TimeUnit.SECONDS.toNanos(10));
        try (TokenEndpoint endpoint = new TokenEndpoint();
                ApiKeyCredential credential = ApiKeyCredential.builder(endpoint.url(), "server", API_KEY)
                        .clock(clock::get).build()) {
            endpoint.reply(200, success(TOKEN_A, 2));
            assertEquals(TOKEN_A, credential.token());
            endpoint.takeRequest();
            clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(1599));
            assertEquals(TOKEN_A, credential.token());
            assertEquals(1, endpoint.requestCount());
            final CountDownLatch release = new CountDownLatch(1);
            endpoint.reply(new Reply(200, success(TOKEN_B, 2), release));
            clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(1));
            assertEquals(TOKEN_A, credential.token());
            endpoint.takeRequest();
            assertEquals(TOKEN_A, credential.token());
            release.countDown();
            awaitToken(credential, TOKEN_B);
            assertEquals(2, endpoint.requestCount());
        }
    }

    @Test
    void timesOutWithoutExposingSecrets() throws Exception {
        try (TokenEndpoint endpoint = new TokenEndpoint();
                ApiKeyCredential credential = ApiKeyCredential.builder(endpoint.url(), "server", API_KEY)
                        .exchangeTimeout(Duration.ofMillis(100)).build()) {
            final CountDownLatch release = new CountDownLatch(1);
            endpoint.reply(new Reply(200, success(TOKEN_A, 60), release));
            try {
                final ApiKeyCredential.ExchangeException error = assertThrows(
                        ApiKeyCredential.ExchangeException.class, credential::token);
                assertEquals(-1, error.getHttpStatus());
                assertTrue(error.toString().contains(endpoint.url().toString()));
                assertTrue(error.toString().contains("HTTP status unavailable"));
                assertFalse(error.toString().contains(API_KEY));
                assertFalse(error.toString().contains(TOKEN_A));
            } finally {
                release.countDown();
            }
        }
    }

    @Test
    void sharesFirstExchangeAndFailsAfterExpiry() throws Exception {
        final AtomicLong clock = new AtomicLong(TimeUnit.SECONDS.toNanos(10));
        try (TokenEndpoint endpoint = new TokenEndpoint();
                ApiKeyCredential credential = ApiKeyCredential.builder(endpoint.url(), "server", API_KEY)
                        .clock(clock::get).exchangeTimeout(Duration.ofSeconds(3)).build()) {
            final CountDownLatch release = new CountDownLatch(1);
            endpoint.reply(new Reply(200, success(TOKEN_A, 2), release));
            final ExecutorService callers = Executors.newFixedThreadPool(12);
            try {
                final List<Future<String>> results = new ArrayList<Future<String>>();
                for (int index = 0; index < 12; index++) {
                    results.add(callers.submit(credential::token));
                }
                endpoint.takeRequest();
                assertEquals(1, endpoint.requestCount());
                release.countDown();
                for (Future<String> result : results) {
                    assertEquals(TOKEN_A, result.get(3, TimeUnit.SECONDS));
                }
                clock.addAndGet(TimeUnit.SECONDS.toNanos(2));
                endpoint.reply(503, "{\"error\":\"offline\"}");
                final ApiKeyCredential.ExchangeException failure = assertThrows(
                        ApiKeyCredential.ExchangeException.class, credential::token);
                assertEquals(503, failure.getHttpStatus());
                assertEquals(2, endpoint.requestCount());
                assertFalse(failure.toString().contains(API_KEY));
                assertFalse(failure.toString().contains(TOKEN_A));
            } finally {
                callers.shutdownNow();
            }
        }
    }

    @Test
    void rejectionDiscardsOnlyTheCurrentToken() throws Exception {
        final AtomicLong clock = new AtomicLong(TimeUnit.SECONDS.toNanos(10));
        try (TokenEndpoint endpoint = new TokenEndpoint();
                ApiKeyCredential credential = ApiKeyCredential.builder(endpoint.url(), "server", API_KEY)
                        .clock(clock::get).build()) {
            endpoint.reply(200, success(TOKEN_A, 2));
            assertEquals(TOKEN_A, credential.token());
            credential.rejectToken(TOKEN_A);
            endpoint.reply(200, success(TOKEN_B, 2));
            assertEquals(TOKEN_B, credential.token());
            credential.rejectToken(TOKEN_A);
            assertEquals(TOKEN_B, credential.token());
            assertEquals(2, endpoint.requestCount());
            credential.close();
            final IllegalStateException closed = assertThrows(
                    IllegalStateException.class, credential::token);
            assertFalse(closed.toString().contains(API_KEY));
            assertFalse(closed.toString().contains(TOKEN_A));
            assertFalse(closed.toString().contains(TOKEN_B));
        }
    }

    @Test
    void clientRetriesExactlyOnceAfterRejectedToken() throws Exception {
        for (boolean shouldRejectBoth : new boolean[] {false, true}) {
            try (TokenEndpoint endpoint = new TokenEndpoint();
                    AuthenticatedGrpcServer grpc = new AuthenticatedGrpcServer(shouldRejectBoth, false);
                    ApiKeyCredential credential = ApiKeyCredential.builder(endpoint.url(), "server", API_KEY)
                            .build();
                    Client client = new Client(new Registry(Collections.<Flow<?>>emptyList()),
                            new EmptyBlobCache(), ClientOptions.newBuilder()
                                    .serverAddress(grpc.address()).credential(credential).build())) {
                endpoint.reply(200, success(TOKEN_A, 60));
                endpoint.reply(200, success(TOKEN_B, 60));
                if (shouldRejectBoth) {
                    final DexServiceException rejection = assertThrows(DexServiceException.class,
                            () -> client.searchFlows("", 1));
                    assertEquals(Status.Code.UNAUTHENTICATED, rejection.getCode());
                } else {
                    client.searchFlows("", 1);
                }
                assertEquals("Bearer " + TOKEN_A, grpc.takeHeader("SearchFlows"));
                assertEquals("Bearer " + TOKEN_B, grpc.takeHeader("SearchFlows"));
                assertEquals(2, grpc.searchAttempts.get());
                assertEquals(2, endpoint.requestCount());
            }
        }
    }

    @Test
    void workerPollReconnectsWithFreshTokenAfterRejection() throws Exception {
        try (TokenEndpoint endpoint = new TokenEndpoint();
                AuthenticatedGrpcServer grpc = new AuthenticatedGrpcServer(false, true);
                ApiKeyCredential credential = ApiKeyCredential.builder(endpoint.url(), "server", API_KEY)
                        .build()) {
            endpoint.reply(200, success(TOKEN_A, 60));
            endpoint.reply(200, success(TOKEN_B, 60));
            final Worker worker = new Worker(new Registry(Collections.<Flow<?>>emptyList()),
                    new EmptyBlobCache(), WorkerOptions.newBuilder()
                            .serverAddress(grpc.address()).credential(credential)
                            .pull(PullWorkerOptions.newBuilder().appWorkerID("api-key-worker")
                                    .stopDrainTimeout(Duration.ofSeconds(1)).build())
                            .build());
            final AtomicReference<Throwable> workerFailure = new AtomicReference<Throwable>();
            final Thread workerThread = new Thread(() -> {
                try {
                    worker.start();
                } catch (Throwable failure) {
                    workerFailure.set(failure);
                }
            });
            workerThread.setDaemon(true);
            workerThread.start();
            try {
                assertEquals("Bearer " + TOKEN_A, grpc.takeHeader("GetServerInfo"));
                assertEquals("Bearer " + TOKEN_A, grpc.takeHeader("SyncAttributeIndexes"));
                assertEquals("Bearer " + TOKEN_A, grpc.takeHeader("PollWorkerTask"));
                assertEquals("Bearer " + TOKEN_B, grpc.takeHeader("PollWorkerTask"));
                assertEquals(2, grpc.pollAttempts.get());
                assertEquals(2, endpoint.requestCount());
            } finally {
                worker.close();
                workerThread.join(5_000L);
            }
            assertFalse(workerThread.isAlive());
            assertEquals(null, workerFailure.get());
        }
    }

    private static String success(final String token, final int lifetimeSeconds) {
        return "{\"access_token\":\"" + token + "\",\"token_type\":\"bEaReR\",\"expires_in\":"
                + lifetimeSeconds + ",\"extra\":true}";
    }

    private static String repeat(final String value, final int count) {
        final StringBuilder result = new StringBuilder();
        for (int index = 0; index < count; index++) {
            result.append(value);
        }
        return result.toString();
    }

    private static void awaitToken(final ApiKeyCredential credential, final String expected)
            throws Exception {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (System.nanoTime() < deadline) {
            if (expected.equals(credential.token())) {
                return;
            }
            Thread.yield();
        }
        assertEquals(expected, credential.token());
    }

    private static void awaitRequestCount(final ApiKeyCredential credential,
            final TokenEndpoint endpoint, final int expected)
            throws Exception {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (System.nanoTime() < deadline) {
            credential.token();
            if (endpoint.requestCount() >= expected) {
                return;
            }
            Thread.yield();
        }
        assertEquals(expected, endpoint.requestCount());
    }

    private static void awaitExchangeCompletion(final ApiKeyCredential credential) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (System.nanoTime() < deadline) {
            if (!credential.isExchangeInFlight()) {
                return;
            }
            Thread.yield();
        }
        assertFalse(credential.isExchangeInFlight());
    }

    private static final class Reply {
        private final int status;
        private final String body;
        private final CountDownLatch release;

        private Reply(final int status, final String body) {
            this(status, body, new CountDownLatch(0));
        }

        private Reply(final int status, final String body, final CountDownLatch release) {
            this.status = status;
            this.body = body;
            this.release = release;
        }
    }

    private static final class ExchangeRequest {
        private final String method;
        private final String contentType;
        private final String accept;
        private final String body;

        private ExchangeRequest(final HttpExchange exchange, final String body) {
            this.method = exchange.getRequestMethod();
            this.contentType = exchange.getRequestHeaders().getFirst("Content-Type");
            this.accept = exchange.getRequestHeaders().getFirst("Accept");
            this.body = body;
        }
    }

    private static final class TokenEndpoint implements AutoCloseable {
        private final HttpServer server;
        private final BlockingQueue<Reply> replies = new LinkedBlockingQueue<Reply>();
        private final BlockingQueue<ExchangeRequest> requests =
                new LinkedBlockingQueue<ExchangeRequest>();
        private final AtomicInteger requestCount = new AtomicInteger();
        private final AtomicInteger completedCount = new AtomicInteger();
        private final ExecutorService handlers = Executors.newCachedThreadPool(runnable -> {
            final Thread thread = new Thread(runnable, "dex-api-key-test-endpoint");
            thread.setDaemon(true);
            return thread;
        });

        private TokenEndpoint() throws IOException {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/token", this::handle);
            server.setExecutor(handlers);
            server.start();
        }

        private URI url() {
            return URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/token");
        }

        private void reply(final int status, final String body) {
            reply(new Reply(status, body));
        }

        private void reply(final Reply reply) {
            replies.add(reply);
        }

        private ExchangeRequest takeRequest() throws InterruptedException {
            final ExchangeRequest request = requests.poll(3, TimeUnit.SECONDS);
            assertTrue(request != null, "no token exchange request arrived");
            return request;
        }

        private int requestCount() {
            return requestCount.get();
        }

        private void awaitCompleted(final int expected) {
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            while (System.nanoTime() < deadline) {
                if (completedCount.get() >= expected) {
                    return;
                }
                Thread.yield();
            }
            assertEquals(expected, completedCount.get());
        }

        private void handle(final HttpExchange exchange) throws IOException {
            try {
                final byte[] requestBody = readBody(exchange);
                requests.add(new ExchangeRequest(exchange,
                        new String(requestBody, StandardCharsets.UTF_8)));
                requestCount.incrementAndGet();
                final Reply reply = replies.poll(3, TimeUnit.SECONDS);
                if (reply == null || !reply.release.await(3, TimeUnit.SECONDS)) {
                    exchange.sendResponseHeaders(500, -1);
                    return;
                }
                final byte[] responseBody = reply.body.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(reply.status, responseBody.length);
                exchange.getResponseBody().write(responseBody);
                completedCount.incrementAndGet();
            } catch (InterruptedException interruption) {
                Thread.currentThread().interrupt();
                exchange.sendResponseHeaders(500, -1);
            } finally {
                exchange.close();
            }
        }

        private static byte[] readBody(final HttpExchange exchange) throws IOException {
            final java.io.ByteArrayOutputStream result = new java.io.ByteArrayOutputStream();
            final byte[] buffer = new byte[4096];
            int count;
            while ((count = exchange.getRequestBody().read(buffer)) != -1) {
                result.write(buffer, 0, count);
            }
            return result.toByteArray();
        }

        @Override
        public void close() {
            server.stop(0);
            handlers.shutdownNow();
        }
    }

    private static final class AuthenticatedGrpcServer implements AutoCloseable {
        private static final Metadata.Key<String> AUTHORIZATION =
                Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
        private final BlockingQueue<String> headers = new LinkedBlockingQueue<String>();
        private final AtomicInteger searchAttempts = new AtomicInteger();
        private final AtomicInteger pollAttempts = new AtomicInteger();
        private final Server server;

        private AuthenticatedGrpcServer(final boolean shouldRejectBoth,
                final boolean shouldRejectFirstPoll)
                throws IOException {
            final ServerInterceptor authentication = new ServerInterceptor() {
                @Override
                public <RequestT, ResponseT> ServerCall.Listener<RequestT> interceptCall(
                        final ServerCall<RequestT, ResponseT> call, final Metadata metadata,
                        final ServerCallHandler<RequestT, ResponseT> next) {
                    final String method = call.getMethodDescriptor().getBareMethodName();
                    final String authorization = metadata.get(AUTHORIZATION);
                    headers.add(method + ":" + authorization);
                    if ("SearchFlows".equals(method)) {
                        searchAttempts.incrementAndGet();
                    } else if ("PollWorkerTask".equals(method)) {
                        pollAttempts.incrementAndGet();
                    }
                    if (("SearchFlows".equals(method)
                            && (shouldRejectBoth || !"Bearer ".concat(TOKEN_B).equals(authorization)))
                            || ("PollWorkerTask".equals(method) && shouldRejectFirstPoll
                                    && !"Bearer ".concat(TOKEN_B).equals(authorization))) {
                        call.close(Status.UNAUTHENTICATED, new Metadata());
                        return new ServerCall.Listener<RequestT>() { };
                    }
                    return next.startCall(call, metadata);
                }
            };
            server = ServerBuilder.forPort(0)
                    .addService(ServerInterceptors.intercept(
                            new FlowServiceGrpc.FlowServiceImplBase() {
                                @Override
                                public void searchFlows(final SearchFlowsRequest request,
                                        final StreamObserver<SearchFlowsResponse> observer) {
                                    observer.onNext(SearchFlowsResponse.getDefaultInstance());
                                    observer.onCompleted();
                                }

                                @Override
                                public void getServerInfo(final Empty request,
                                        final StreamObserver<ServerInfo> observer) {
                                    observer.onNext(ServerInfo.newBuilder().setServerVersion("test")
                                            .setMinimumSupportedProtocolVersion(1)
                                            .setCurrentProtocolVersion(2).build());
                                    observer.onCompleted();
                                }

                                @Override
                                public void syncAttributeIndexes(final SyncAttributeIndexRequest request,
                                        final StreamObserver<SyncAttributeIndexResponse> observer) {
                                    observer.onNext(SyncAttributeIndexResponse.getDefaultInstance());
                                    observer.onCompleted();
                                }
                            }, authentication))
                    .addService(ServerInterceptors.intercept(
                            new WorkerTaskServiceGrpc.WorkerTaskServiceImplBase() {
                                @Override
                                public StreamObserver<PollWorkerTaskRequest> pollWorkerTask(
                                        final StreamObserver<PollWorkerTaskResponse> observer) {
                                    return new StreamObserver<PollWorkerTaskRequest>() {
                                        @Override
                                        public void onNext(final PollWorkerTaskRequest request) { }

                                        @Override
                                        public void onError(final Throwable failure) { }

                                        @Override
                                        public void onCompleted() {
                                            observer.onCompleted();
                                        }
                                    };
                                }
                            }, authentication))
                    .build().start();
        }

        private String address() {
            return "127.0.0.1:" + server.getPort();
        }

        private String takeHeader(final String method) throws InterruptedException {
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (System.nanoTime() < deadline) {
                final String received = headers.poll(100, TimeUnit.MILLISECONDS);
                if (received != null && received.startsWith(method + ":")) {
                    return received.substring(method.length() + 1);
                }
            }
            throw new AssertionError("no gRPC request arrived for " + method);
        }

        @Override
        public void close() throws InterruptedException {
            server.shutdownNow();
            assertTrue(server.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    private static final class EmptyBlobCache implements BlobCache {
        @Override
        public Optional<byte[]> get(final String blobId) {
            return Optional.empty();
        }

        @Override
        public boolean put(final String blobId, final byte[] payload) {
            return false;
        }

        @Override
        public void delete(final String blobId) { }

        @Override
        public void deleteAll() { }

        @Override
        public void close() { }
    }
}
