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
import io.superdurable.dex.StartFlowOptions;
import io.superdurable.dex.Worker;
import io.superdurable.dex.production.pullworkers.GreetingFlow;
import io.superdurable.dex.production.pullworkers.PullGreetingExample;
import io.superdurable.dex.production.pullworkers.PullGreetingExample.RunningPullGreeting;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@ExtendWith(SharedIntegExtension.class)
public class PullWorkersIntegTest {
    @Test
    void pullWorkerServesGreetingFlow(@TempDir final Path cacheDirectory)
            throws InterruptedException {
        final IntegEnvironment environment = SharedIntegExtension.environment();
        final String serverAddress = System.getenv().getOrDefault(
                "DEX_FLOW_SERVICE_ADDRESS", "127.0.0.1:8801");
        try (BlobCache cache = BlobCache.open(new BlobCacheConfig(
                cacheDirectory.resolve("pull-cache").toString(), 64L << 20))) {
            final RunningPullGreeting running = PullGreetingExample.start(cache, serverAddress);
            try (Client client = running.client(); Worker worker = running.worker()) {
                final GreetingFlow flow = running.flow();
                final String flowId = environment.newFlowId("pull-greeting");
                client.startFlow(flow, flowId, "Dex", StartFlowOptions.newBuilder()
                        .timeout(Duration.ofMinutes(1)).build());
                assertEquals("Hello, Dex!", client.waitForFlow(flowId, Duration.ofSeconds(45))
                        .getSingleOutput(String.class));
                final GreetingFlow stub = client.newRpcStub(GreetingFlow.class, flowId);
                assertEquals("Hello, Dex!", client.invokeRPC(stub::getGreeting));
            } finally {
                running.workerThread().join(10_000L);
                assertFalse(running.workerThread().isAlive());
                running.workerResult().join();
            }
        }
    }
}
