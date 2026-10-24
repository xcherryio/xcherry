/*
 * Copyright (c) 2022-2026 Super Durable, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.superdurable.dex.integ;

import io.superdurable.dex.BlobCache;
import io.superdurable.dex.BlobCacheConfig;
import io.superdurable.dex.Client;
import io.superdurable.dex.ClientOptions;
import io.superdurable.dex.Flow;
import io.superdurable.dex.Registry;
import io.superdurable.dex.StartFlowOptions;
import io.superdurable.dex.Worker;
import io.superdurable.dex.WorkerOptions;
import io.superdurable.dex.WaitForAttributeOptions;
import io.superdurable.dex.shared.ExampleWorkerMode;
import io.superdurable.dex.shared.MyDependencyService;
import io.superdurable.dex.products.engagement.EngagementFlow;
import io.superdurable.dex.products.dealdsl.DealDSLFlow;
import io.superdurable.dex.products.jobpost.JobPostingFlow;
import io.superdurable.dex.products.microservices.OrchestrationFlow;
import io.superdurable.dex.products.moneytransfer.MoneyTransferFlow;
import io.superdurable.dex.products.orderprocessing.OrderProcessingFlow;
import io.superdurable.dex.patterns.polling.FakeJobService;
import io.superdurable.dex.patterns.polling.PollingFlow;
import io.superdurable.dex.primitives.channel.ChannelFlow;
import io.superdurable.dex.primitives.step.RetryingFailureFlow;
import io.superdurable.dex.primitives.stream.StreamFlow;
import io.superdurable.dex.products.subscription.SubscriptionFlow;
import io.superdurable.dex.products.signup.UserOnboardingFlow;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.function.Supplier;

final class IntegEnvironment implements AutoCloseable {
    private static final long CACHE_SIZE_BYTES = 64L * 1024L * 1024L;
    private static final AtomicLong FLOW_COUNTER = new AtomicLong();

    private final Path cacheDirectory;
    private final BlobCache blobCache;
    private final Worker worker;
    private final Thread workerThread;
    private final AtomicReference<Throwable> workerFailure;
    private final Client client;
    private final MoneyTransferFlow moneyTransferFlow;
    private final OrderProcessingFlow orderProcessingFlow;
    private final EngagementFlow engagementFlow;
    private final DealDSLFlow dealDSLFlow;
    private final JobPostingFlow jobPostingFlow;
    private final OrchestrationFlow orchestrationFlow;
    private final RetryingFailureFlow retryingFailureFlow;
    private final ChannelFlow channelFlow;
    private final StreamFlow streamFlow;
    private final SubscriptionFlow subscriptionFlow;
    private final UserOnboardingFlow userOnboardingFlow;
    private final FakeJobService fakeJobService;
    private final PollingFlow pollingFlow;

    private IntegEnvironment(
            final Path cacheDirectory,
            final BlobCache blobCache,
            final Worker worker,
            final Thread workerThread,
            final AtomicReference<Throwable> workerFailure,
            final Client client,
            final MoneyTransferFlow moneyTransferFlow,
            final OrderProcessingFlow orderProcessingFlow,
            final EngagementFlow engagementFlow,
            final DealDSLFlow dealDSLFlow,
            final JobPostingFlow jobPostingFlow,
            final OrchestrationFlow orchestrationFlow,
            final RetryingFailureFlow retryingFailureFlow,
            final ChannelFlow channelFlow,
            final StreamFlow streamFlow,
            final SubscriptionFlow subscriptionFlow,
            final UserOnboardingFlow userOnboardingFlow,
            final FakeJobService fakeJobService,
            final PollingFlow pollingFlow) {
        this.cacheDirectory = cacheDirectory;
        this.blobCache = blobCache;
        this.worker = worker;
        this.workerThread = workerThread;
        this.workerFailure = workerFailure;
        this.client = client;
        this.moneyTransferFlow = moneyTransferFlow;
        this.orderProcessingFlow = orderProcessingFlow;
        this.engagementFlow = engagementFlow;
        this.dealDSLFlow = dealDSLFlow;
        this.jobPostingFlow = jobPostingFlow;
        this.orchestrationFlow = orchestrationFlow;
        this.retryingFailureFlow = retryingFailureFlow;
        this.channelFlow = channelFlow;
        this.streamFlow = streamFlow;
        this.subscriptionFlow = subscriptionFlow;
        this.userOnboardingFlow = userOnboardingFlow;
        this.fakeJobService = fakeJobService;
        this.pollingFlow = pollingFlow;
    }

    static IntegEnvironment start() throws IOException {
        final String serverAddress = System.getenv().getOrDefault(
                "DEX_FLOW_SERVICE_ADDRESS",
                "127.0.0.1:8801");
        final MyDependencyService service = new MyDependencyService();
        final MoneyTransferFlow moneyTransferFlow = new MoneyTransferFlow(service);
        final OrderProcessingFlow orderProcessingFlow = new OrderProcessingFlow(service);
        final EngagementFlow engagementFlow = new EngagementFlow(service);
        final DealDSLFlow dealDSLFlow = new DealDSLFlow();
        final JobPostingFlow jobPostingFlow = new JobPostingFlow(service);
        final OrchestrationFlow orchestrationFlow = new OrchestrationFlow(service);
        final RetryingFailureFlow retryingFailureFlow = new RetryingFailureFlow();
        final ChannelFlow channelFlow = new ChannelFlow();
        final StreamFlow streamFlow = new StreamFlow();
        final SubscriptionFlow subscriptionFlow = new SubscriptionFlow(service);
        final UserOnboardingFlow userOnboardingFlow = new UserOnboardingFlow(service);
        final FakeJobService fakeJobService = new FakeJobService();
        final PollingFlow pollingFlow = new PollingFlow(fakeJobService);
        final List<Flow<?>> flows = Arrays.<Flow<?>>asList(
                moneyTransferFlow,
                orderProcessingFlow,
                engagementFlow,
                dealDSLFlow,
                jobPostingFlow,
                orchestrationFlow,
                retryingFailureFlow,
                channelFlow,
                streamFlow,
                subscriptionFlow,
                userOnboardingFlow,
                pollingFlow);

        final ExampleWorkerMode workerMode = ExampleWorkerMode.readFromEnvironment();
        final Path cacheDirectory = Files.createTempDirectory("dex-java-examples-integ-");
        final int workerPort = availablePort();
        final Registry registry = new Registry(flows);
        final BlobCache blobCache = BlobCache.open(new BlobCacheConfig(
                cacheDirectory.toString(),
                CACHE_SIZE_BYTES));
        final Worker worker = new Worker(
                registry,
                blobCache,
                integWorkerOptions(workerMode, serverAddress, workerPort));
        final AtomicReference<Throwable> workerFailure = new AtomicReference<Throwable>();
        final Thread workerThread = new Thread(() -> {
            try {
                worker.start();
            } catch (final Throwable failure) {
                workerFailure.set(failure);
            }
        }, "dex-java-examples-integ-worker");
        workerThread.setDaemon(true);
        workerThread.start();
        if (workerMode.isPull()) {
            awaitPullWorkerRegistration(worker, workerFailure);
        } else {
            awaitWorker(workerPort, workerFailure);
        }
        final Client client = new Client(
                registry,
                blobCache,
                new ClientOptions(serverAddress, worker.getWorkerTarget()));
        return new IntegEnvironment(
                cacheDirectory,
                blobCache,
                worker,
                workerThread,
                workerFailure,
                client,
                moneyTransferFlow,
                orderProcessingFlow,
                engagementFlow,
                dealDSLFlow,
                jobPostingFlow,
                orchestrationFlow,
                retryingFailureFlow,
                channelFlow,
                streamFlow,
                subscriptionFlow,
                userOnboardingFlow,
                fakeJobService,
                pollingFlow);
    }

    Client client() {
        return client;
    }

    MoneyTransferFlow moneyTransferFlow() {
        return moneyTransferFlow;
    }

    OrderProcessingFlow orderProcessingFlow() {
        return orderProcessingFlow;
    }

    EngagementFlow engagementFlow() {
        return engagementFlow;
    }

    DealDSLFlow dealDSLFlow() {
        return dealDSLFlow;
    }

    JobPostingFlow jobPostingFlow() {
        return jobPostingFlow;
    }

    OrchestrationFlow orchestrationFlow() {
        return orchestrationFlow;
    }

    RetryingFailureFlow retryingFailureFlow() {
        return retryingFailureFlow;
    }

    ChannelFlow channelFlow() {
        return channelFlow;
    }

    StreamFlow streamFlow() {
        return streamFlow;
    }

    SubscriptionFlow subscriptionFlow() {
        return subscriptionFlow;
    }

    UserOnboardingFlow userOnboardingFlow() {
        return userOnboardingFlow;
    }

    FakeJobService fakeJobService() {
        return fakeJobService;
    }

    PollingFlow pollingFlow() {
        return pollingFlow;
    }

    StartFlowOptions startOptions() {
        return StartFlowOptions.newBuilder().timeout(Duration.ofHours(1)).build();
    }

    String newFlowId(final String prefix) {
        return prefix + "-" + System.nanoTime() + "-" + FLOW_COUNTER.incrementAndGet();
    }

    <T> T awaitAttribute(
            final String flowId,
            final io.superdurable.dex.Attribute<T> attribute,
            final T expected,
            final Duration timeout) throws InterruptedException {
        return client.waitForAttributeMatch(
                flowId,
                attribute,
                io.superdurable.dex.AttributeMatch.equalTo(expected),
                WaitForAttributeOptions.newBuilder().requestTimeout(timeout).build());
    }

    <T> T awaitCondition(
            final Supplier<T> supplier,
            final Predicate<T> predicate,
            final Duration timeout,
            final String message) throws InterruptedException {
        final long deadline = System.nanoTime() + timeout.toNanos();
        T value = null;
        while (System.nanoTime() < deadline) {
            value = supplier.get();
            if (value != null && predicate.test(value)) {
                return value;
            }
            Thread.sleep(200L);
        }
        throw new AssertionError(message + " last=" + value);
    }

    @Override
    public void close() throws InterruptedException, IOException {
        client.close();
        worker.close();
        workerThread.join(10_000L);
        blobCache.close();
        deleteRecursively(cacheDirectory);
        final Throwable failure = workerFailure.get();
        if (failure != null) {
            throw new IllegalStateException("Java examples integ Worker failed", failure);
        }
    }

    private static WorkerOptions integWorkerOptions(
            final ExampleWorkerMode workerMode,
            final String serverAddress,
            final int workerPort) {
        if (workerMode.isPull()) {
            return WorkerOptions.newBuilder()
                    .serverAddress(serverAddress)
                    .pull(ExampleWorkerMode.newExamplePullWorkerOptions())
                    .build();
        }
        return WorkerOptions.newBuilder()
                .bindAddress("127.0.0.1:" + workerPort)
                .serverAddress(serverAddress)
                .build();
    }

    private static int availablePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void awaitWorker(
            final int workerPort,
            final AtomicReference<Throwable> workerFailure) throws IOException {
        final long deadline = System.nanoTime() + 10_000_000_000L;
        while (System.nanoTime() < deadline) {
            final Throwable failure = workerFailure.get();
            if (failure != null) {
                throw new IllegalStateException("Java examples integ Worker failed", failure);
            }
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress("127.0.0.1", workerPort), 100);
                return;
            } catch (final IOException unavailable) {
                Thread.yield();
            }
        }
        throw new IOException("Java examples integ Worker did not become ready");
    }

    private static void awaitPullWorkerRegistration(
            final Worker worker,
            final AtomicReference<Throwable> workerFailure) throws IOException {
        try {
            worker.waitPullRegistered(Duration.ofSeconds(30));
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted waiting for Java examples integ pull Worker", interrupted);
        } catch (final TimeoutException timeout) {
            final IOException notReady =
                    new IOException("Java examples integ pull Worker did not register", timeout);
            final Throwable failure = workerFailure.get();
            if (failure != null) {
                notReady.addSuppressed(failure);
            }
            throw notReady;
        }
    }

    private static void deleteRecursively(final Path path) throws IOException {
        if (path == null || !Files.exists(path)) {
            return;
        }
        Files.walk(path)
                .sorted(Comparator.reverseOrder())
                .forEach(entry -> {
                    try {
                        Files.deleteIfExists(entry);
                    } catch (final IOException ignored) {
                        // best-effort cleanup of temp cache
                    }
                });
    }
}
