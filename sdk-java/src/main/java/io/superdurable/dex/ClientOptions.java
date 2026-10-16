/*
 * Legacy Materials in this file remain under their original licenses.
 * See LEGACY_NOTICES.md.
 */

/*
 * Modifications Copyright (c) 2026 Super Durable, Inc.
 *
 * Modifications after the Legacy Cutoff are licensed under the
 * Sustainable Use License 1.0.
 * Legacy Materials remain under their original licenses.
 * See LICENSE and LEGACY_NOTICES.md.
 */

package io.superdurable.dex;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Configures a synchronous {@link Client} connection and value mapper.
 *
 * <p>The client connects to the Dex server over plaintext gRPC. Its default server address is
 * {@code localhost:8801}. A worker target is copied into start-time Flow configuration unless a
 * request supplies a more specific target. Use {@link WorkerTarget#pull()} when the application
 * Worker polls Dex instead of accepting inbound calls. The default Jackson mapper discovers installed modules;
 * pass a configured mapper when application values require custom serialization.
 *
 * <pre>{@code
 * ClientOptions options = new ClientOptions(
 *         "dex.example.com:8801",
 *         new WorkerTarget("orders-worker:8803", false),
 *         applicationObjectMapper);
 * }</pre>
 */
public final class ClientOptions {
    private final String serverAddress;
    private final WorkerTarget workerTarget;
    private final ObjectMapper objectMapper;

    /** Creates options for {@code localhost:8801} with the default Jackson mapper. */
    public ClientOptions() {
        this("localhost:8801", null, new ObjectMapper().findAndRegisterModules());
    }

    /**
     * Creates options for a Dex server with default worker routing and serialization.
     *
     * @param serverAddress the gRPC target understood by {@code ManagedChannelBuilder}
     */
    public ClientOptions(final String serverAddress) {
        this(serverAddress, null, new ObjectMapper().findAndRegisterModules());
    }

    /**
     * Creates options with explicit server and default worker routing.
     *
     * @param serverAddress the Dex gRPC target
     * @param workerTarget the default Flow worker target, or {@code null}
     */
    public ClientOptions(
            final String serverAddress,
            final WorkerTarget workerTarget) {
        this(serverAddress, workerTarget, new ObjectMapper().findAndRegisterModules());
    }

    /**
     * Creates options with explicit transport, routing, and serialization settings.
     *
     * @param serverAddress the Dex gRPC target
     * @param workerTarget the default Flow worker target, or {@code null}
     * @param objectMapper the nonnull Jackson mapper used for application values
     * @throws IllegalArgumentException if {@code objectMapper} is {@code null}
     */
    public ClientOptions(
            final String serverAddress,
            final WorkerTarget workerTarget,
            final ObjectMapper objectMapper) {
        if (objectMapper == null) {
            throw new IllegalArgumentException("objectMapper is required");
        }
        this.serverAddress = serverAddress;
        this.workerTarget = workerTarget;
        this.objectMapper = objectMapper;
    }

    String getServerAddress() {
        return serverAddress;
    }

    WorkerTarget getWorkerTarget() {
        return workerTarget;
    }

    ObjectMapper getObjectMapper() {
        return objectMapper;
    }
}
