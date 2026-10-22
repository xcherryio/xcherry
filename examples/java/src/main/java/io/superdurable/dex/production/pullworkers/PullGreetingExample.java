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

package io.superdurable.dex.production.pullworkers;

import io.superdurable.dex.BlobCache;
import io.superdurable.dex.Client;
import io.superdurable.dex.ClientOptions;
import io.superdurable.dex.Flow;
import io.superdurable.dex.PullWorkerOptions;
import io.superdurable.dex.Registry;
import io.superdurable.dex.Worker;
import io.superdurable.dex.WorkerOptions;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

public final class PullGreetingExample {
    private PullGreetingExample() {
    }

    public static RunningPullGreeting start(
            final BlobCache cache, final String serverAddress) throws InterruptedException, TimeoutException {
        final GreetingFlow flow = new GreetingFlow();
        final Registry registry = new Registry(List.<Flow<?>>of(flow));
        final Worker worker = new Worker(registry, cache, WorkerOptions.newBuilder()
                .serverAddress(serverAddress)
                .pull(PullWorkerOptions.newBuilder().maxConcurrentTasks(8).build())
                .build());
        final Client client = new Client(registry, cache,
                new ClientOptions(serverAddress, worker.getWorkerTarget()));
        final CompletableFuture<Void> workerResult = new CompletableFuture<>();
        final Thread workerThread = new Thread(() -> {
            try {
                worker.start();
                workerResult.complete(null);
            } catch (final Throwable failure) {
                workerResult.completeExceptionally(failure);
            }
        }, "pull-greeting-worker");
        workerThread.start();
        final RunningPullGreeting running = new RunningPullGreeting(
                flow, worker, client, workerThread, workerResult);
        try {
            worker.waitPullRegistered(Duration.ofSeconds(30));
        } catch (final InterruptedException | TimeoutException | RuntimeException | Error failure) {
            try {
                running.close();
            } catch (final InterruptedException | RuntimeException | Error cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
                if (cleanupFailure instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
            }
            throw failure;
        }
        return running;
    }

    public static boolean isPullGreetingReady(final Worker worker) {
        return worker.getPullRegistrationState().isReady();
    }

    public record RunningPullGreeting(
            GreetingFlow flow, Worker worker, Client client, Thread workerThread,
            CompletableFuture<Void> workerResult) implements AutoCloseable {
        @Override
        public void close() throws InterruptedException {
            client.close();
            worker.close();
            workerThread.join();
            workerResult.join();
        }
    }
}
