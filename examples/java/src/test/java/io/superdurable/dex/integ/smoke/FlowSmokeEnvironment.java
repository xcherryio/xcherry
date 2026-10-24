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

package io.superdurable.dex.integ.smoke;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.superdurable.dex.SpringMainApplication;
import io.superdurable.dex.Worker;
import io.superdurable.dex.shared.ExampleWorkerMode;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.web.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

final class FlowSmokeEnvironment implements AutoCloseable {
    private static final AtomicLong FLOW_COUNTER = new AtomicLong();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final HttpClient HTTP_CLIENT =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

    private final ConfigurableApplicationContext context;
    private final Path cacheDirectory;
    private final String baseUrl;

    private FlowSmokeEnvironment(
            final ConfigurableApplicationContext context,
            final Path cacheDirectory,
            final String baseUrl) {
        this.context = context;
        this.cacheDirectory = cacheDirectory;
        this.baseUrl = baseUrl;
    }

    static FlowSmokeEnvironment start() throws Exception {
        final String serverAddress =
                System.getenv().getOrDefault("DEX_FLOW_SERVICE_ADDRESS", "127.0.0.1:8801");
        final ExampleWorkerMode workerMode = ExampleWorkerMode.readFromEnvironment();
        final Path cacheDirectory = Files.createTempDirectory("dex-java-flow-smoke-");
        final int workerPort = availablePort();
        final String workerAddress = "127.0.0.1:" + workerPort;

        final SpringApplication application = new SpringApplication(SpringMainApplication.class);
        final ConfigurableApplicationContext context = application.run(
                "--server.port=0",
                "--dex.server-address=" + serverAddress,
                "--dex.worker-bind-address=" + workerAddress,
                "--dex.blob-cache-dir=" + cacheDirectory.toString());
        final WebServerApplicationContext webContext = (WebServerApplicationContext) context;
        final int httpPort = webContext.getWebServer().getPort();
        if (workerMode.isPull()) {
            context.getBean(Worker.class).waitPullRegistered(Duration.ofSeconds(30));
        } else {
            awaitWorker(workerPort);
        }
        return new FlowSmokeEnvironment(
                context,
                cacheDirectory,
                "http://127.0.0.1:" + httpPort);
    }

    String newFlowId(final String prefix) {
        return prefix + "-" + System.nanoTime() + "-" + FLOW_COUNTER.incrementAndGet();
    }

    FlowSmokeTriggerResult triggerHttp(
            final String method,
            final String path,
            final Map<String, String> query,
            final Object body) throws Exception {
        final String requestUrl = baseUrl + path + encodeQuery(query);
        final HttpRequest.Builder builder =
                HttpRequest.newBuilder(URI.create(requestUrl)).timeout(Duration.ofSeconds(30));
        if ("POST".equals(method)) {
            builder.header("Content-Type", "application/json");
            builder.POST(
                    HttpRequest.BodyPublishers.ofString(
                            body == null ? "{}" : OBJECT_MAPPER.writeValueAsString(body)));
        } else {
            builder.GET();
        }
        final HttpResponse<String> response =
                HTTP_CLIENT.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IOException(
                    method
                            + " "
                            + path
                            + " returned "
                            + response.statusCode()
                            + ": "
                            + response.body());
        }
        final String workflowId = firstNonEmpty(query, "workflowId", "username");
        return FlowSmokeHelper.parseFlowTriggerResponse(response.body(), workflowId);
    }

    @Override
    public void close() throws Exception {
        context.close();
        deleteRecursively(cacheDirectory);
    }

    private static String encodeQuery(final Map<String, String> query) {
        if (query.isEmpty()) {
            return "";
        }
        return query.entrySet().stream()
                .map(
                        entry ->
                                URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8)
                                        + "="
                                        + URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8))
                .collect(Collectors.joining("&", "?", ""));
    }

    private static String firstNonEmpty(final Map<String, String> query, final String... keys) {
        for (final String key : keys) {
            final String value = query.get(key);
            if (value != null && !value.isEmpty()) {
                return value;
            }
        }
        return "";
    }

    private static int availablePort() throws IOException {
        try (var socket = new java.net.ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void awaitWorker(final int workerPort) throws InterruptedException, IOException {
        final long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < deadline) {
            try (var socket = new java.net.Socket()) {
                socket.connect(new java.net.InetSocketAddress("127.0.0.1", workerPort), 100);
                return;
            } catch (final IOException unavailable) {
                Thread.sleep(50L);
            }
        }
        throw new IOException("Java flow smoke Worker did not become ready");
    }

    private static void deleteRecursively(final Path path) throws IOException {
        if (path == null || !Files.exists(path)) {
            return;
        }
        try (var paths = Files.walk(path)) {
            paths.sorted(java.util.Comparator.reverseOrder()).forEach(entry -> {
                try {
                    Files.deleteIfExists(entry);
                } catch (final IOException ignored) {
                }
            });
        }
    }
}
