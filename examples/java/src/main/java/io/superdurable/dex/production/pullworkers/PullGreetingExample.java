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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, EXPRESS OR IMPLIED.
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

import java.util.List;

public final class PullGreetingExample {
    private PullGreetingExample() {
    }

    public static RunningPullGreeting start(
            final BlobCache cache, final String serverAddress) {
        final GreetingFlow flow = new GreetingFlow();
        final Registry registry = new Registry(List.<Flow<?>>of(flow));
        final Worker worker = new Worker(registry, cache, WorkerOptions.newBuilder()
                .serverAddress(serverAddress)
                .pull(PullWorkerOptions.newBuilder().maxConcurrentTasks(8).build())
                .build());
        final Client client = new Client(registry, cache,
                new ClientOptions(serverAddress, worker.getWorkerTarget()));
        final Thread workerThread = new Thread(worker::start, "pull-greeting-worker");
        workerThread.start();
        return new RunningPullGreeting(flow, worker, client, workerThread);
    }

    public record RunningPullGreeting(
            GreetingFlow flow, Worker worker, Client client, Thread workerThread) {
    }
}
