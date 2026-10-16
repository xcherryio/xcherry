/*
 * Copyright (c) 2026 Super Durable, Inc.
 *
 * Licensed under the Sustainable Use License 1.0.
 * You may not use this file except in compliance with the License.
 * See the LICENSE file in the repository root.
 *
 * SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0
 */

package io.superdurable.dex.testing;

import io.superdurable.dex.BlobCache;
import io.superdurable.dex.BlobCacheConfig;
import io.superdurable.dex.Client;
import io.superdurable.dex.ClientOptions;
import io.superdurable.dex.Flow;
import io.superdurable.dex.PullWorkerOptions;
import io.superdurable.dex.Registry;
import io.superdurable.dex.Worker;
import io.superdurable.dex.WorkerOptions;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

public final class DexDevTestEnvironment implements AutoCloseable {
    private static final long CACHE_SIZE_BYTES = 64L * 1024L * 1024L;

    private final BlobCache blobCache;
    private final Worker worker;
    private final Thread workerThread;
    private final AtomicReference<Throwable> workerFailure;
    private final Client client;

    private DexDevTestEnvironment(
            final BlobCache blobCache,
            final Worker worker,
            final Thread workerThread,
            final AtomicReference<Throwable> workerFailure,
            final Client client) {
        this.blobCache = blobCache;
        this.worker = worker;
        this.workerThread = workerThread;
        this.workerFailure = workerFailure;
        this.client = client;
    }

    public static DexDevTestEnvironment start(
            final Path cacheDirectory,
            final Flow<?>... flows) throws IOException {
        return start(cacheDirectory, isPullMode(), flows);
    }

    public static DexDevTestEnvironment startPush(
            final Path cacheDirectory,
            final Flow<?>... flows) throws IOException {
        return start(cacheDirectory, false, flows);
    }

    private static DexDevTestEnvironment start(
            final Path cacheDirectory,
            final boolean isPullMode,
            final Flow<?>... flows) throws IOException {
        final String serverAddress = System.getProperty(
                "dex.test.serverAddress",
                "127.0.0.1:8801");
        final Registry registry = new Registry(Arrays.asList(flows));
        final BlobCache blobCache = BlobCache.open(new BlobCacheConfig(
                cacheDirectory.toString(),
                CACHE_SIZE_BYTES));
        final WorkerOptions.Builder workerOptions = WorkerOptions.newBuilder()
                .serverAddress(serverAddress);
        final int workerPort;
        if (isPullMode) {
            workerPort = 0;
            workerOptions.pull(PullWorkerOptions.newBuilder().build());
        } else {
            workerPort = availablePort();
            workerOptions.bindAddress("127.0.0.1:" + workerPort);
        }
        final Worker worker = new Worker(
                registry,
                blobCache,
                workerOptions.build());
        final AtomicReference<Throwable> workerFailure = new AtomicReference<Throwable>();
        final Thread workerThread = new Thread(() -> {
            try {
                worker.start();
            } catch (Throwable failure) {
                workerFailure.set(failure);
            }
        }, "dex-java-integration-worker");
        workerThread.start();
        if (!isPullMode) {
            awaitWorker(workerPort, workerFailure);
        }
        final Client client = new Client(
                registry,
                blobCache,
                new ClientOptions(serverAddress, worker.getWorkerTarget()));
        return new DexDevTestEnvironment(
                blobCache,
                worker,
                workerThread,
                workerFailure,
                client);
    }

    public static boolean isPullMode() {
        final String workerMode = System.getenv("DEX_INTEG_WORKER_MODE");
        if (workerMode == null || "push".equals(workerMode)) {
            return false;
        }
        if ("pull".equals(workerMode)) {
            return true;
        }
        throw new IllegalArgumentException("DEX_INTEG_WORKER_MODE must be push or pull");
    }

    public Client client() {
        return client;
    }

    @Override
    public void close() throws InterruptedException {
        client.close();
        worker.close();
        workerThread.join(10_000L);
        blobCache.close();
        final Throwable failure = workerFailure.get();
        if (failure != null) {
            throw new IllegalStateException("Java integration Worker failed", failure);
        }
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
                throw new IllegalStateException("Java integration Worker failed", failure);
            }
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress("127.0.0.1", workerPort), 100);
                return;
            } catch (IOException unavailable) {
                Thread.yield();
            }
        }
        throw new IOException("Java integration Worker did not become ready");
    }
}
