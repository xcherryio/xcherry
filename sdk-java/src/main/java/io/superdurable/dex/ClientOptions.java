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
 * A credential sends a bearer token with each FlowService call. By default no token is sent.
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
    private final Credential credential;

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
        this(serverAddress, workerTarget, objectMapper, null);
    }

    private ClientOptions(
            final String serverAddress,
            final WorkerTarget workerTarget,
            final ObjectMapper objectMapper,
            final Credential credential) {
        if (objectMapper == null) {
            throw new IllegalArgumentException("objectMapper is required");
        }
        this.serverAddress = serverAddress;
        this.workerTarget = workerTarget;
        this.objectMapper = objectMapper;
        this.credential = credential;
    }

    /**
     * Creates a builder with local development defaults.
     *
     * <p>Use the builder to attach a {@link Credential} without changing existing constructor
     * call sites. No credential is sent unless one is configured.
     *
     * <pre>{@code
     * ClientOptions options = ClientOptions.newBuilder()
     *         .serverAddress("dex:8801")
     *         .credential(new FileCredential("/var/run/secrets/dex/token"))
     *         .build();
     * }</pre>
     *
     * @return a new mutable builder
     */
    public static Builder newBuilder() {
        return new Builder();
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

    Credential getCredential() {
        return credential;
    }

    /** Builds immutable Client options. */
    public static final class Builder {
        private String serverAddress = "localhost:8801";
        private WorkerTarget workerTarget;
        private ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();
        private Credential credential;

        private Builder() {
        }

        /**
         * Sets the Dex gRPC server target.
         *
         * @param value the server target; the default is {@code localhost:8801}
         * @return this builder
         */
        public Builder serverAddress(final String value) {
            this.serverAddress = value;
            return this;
        }

        /**
         * Sets the default target for started Flows.
         *
         * @param value the target, or {@code null} for no default
         * @return this builder
         */
        public Builder workerTarget(final WorkerTarget value) {
            this.workerTarget = value;
            return this;
        }

        /**
         * Sets the Jackson mapper for application values.
         *
         * @param value a nonnull mapper
         * @return this builder
         * @throws IllegalArgumentException if value is null
         */
        public Builder objectMapper(final ObjectMapper value) {
            if (value == null) {
                throw new IllegalArgumentException("objectMapper is required");
            }
            this.objectMapper = value;
            return this;
        }

        /**
         * Sets the token source for every Client FlowService call.
         *
         * <p>The source can be called concurrently and must not block. A failure prevents Dex
         * contact and throws {@link io.superdurable.dex.exceptions.DexServiceException} with gRPC
         * {@code UNAUTHENTICATED}. The default {@code null} sends no authorization metadata. See
         * the builder example above.
         *
         * @param value the credential, or {@code null} to send no token
         * @return this builder
         */
        public Builder credential(final Credential value) {
            this.credential = value;
            return this;
        }

        /**
         * Builds immutable options for a Client.
         *
         * @return the configured options
         * @throws IllegalArgumentException if the mapper is null
         */
        public ClientOptions build() {
            return new ClientOptions(serverAddress, workerTarget, objectMapper, credential);
        }
    }
}
