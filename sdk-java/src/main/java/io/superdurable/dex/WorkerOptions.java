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

import java.time.Duration;

/**
 * Configures a Java Worker's listener or outbound polling, routing, connection, and serialization.
 *
 * <p>The worker listens on {@code :8803} and connects to {@code localhost:8801} by default. The
 * bind address controls the local gRPC listener; the worker target is the address advertised for
 * server routing and may differ behind a proxy or service discovery layer. The default Jackson
 * mapper discovers installed modules. Set {@link Builder#pull(PullWorkerOptions)} for outbound
 * polling and advertise {@link WorkerTarget#pull()} to Clients and Flow configurations.
 *
 * <pre>{@code
 * WorkerOptions options = WorkerOptions.newBuilder()
 *         .bindAddress("0.0.0.0:8803")
 *         .workerTarget(new WorkerTarget("orders-worker:8803", false))
 *         .serverAddress("dex:8801")
 *         .objectMapper(applicationObjectMapper)
 *         .grpcErrorStatusMapping(GrpcErrorStatusMapping.newBuilder()
 *                 .map(PaymentDeclinedException.class, Status.Code.FAILED_PRECONDITION)
 *                 .build())
 *         .build();
 * }</pre>
 */
public final class WorkerOptions {
    private final String bindAddress;
    private final WorkerTarget workerTarget;
    private final String serverAddress;
    private final ObjectMapper objectMapper;
    private final GrpcErrorStatusMapping grpcErrorStatusMapping;
    private final Duration attributeIndexSyncTimeout;
    private final PullWorkerOptions pull;
    private final boolean isBindAddressConfigured;

    private WorkerOptions(final Builder builder) {
        this.bindAddress = builder.bindAddress;
        this.workerTarget = builder.workerTarget;
        this.serverAddress = builder.serverAddress;
        this.objectMapper = builder.objectMapper;
        this.grpcErrorStatusMapping = builder.grpcErrorStatusMapping;
        this.attributeIndexSyncTimeout = builder.attributeIndexSyncTimeout;
        this.pull = builder.pull;
        this.isBindAddressConfigured = builder.isBindAddressConfigured;
    }

    /**
     * Creates a builder initialized with local development defaults.
     *
     * @return a new mutable builder
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    String getBindAddress() {
        return bindAddress;
    }

    WorkerTarget getWorkerTarget() {
        return workerTarget;
    }

    String getServerAddress() {
        return serverAddress;
    }

    ObjectMapper getObjectMapper() {
        return objectMapper;
    }

    GrpcErrorStatusMapping getGrpcErrorStatusMapping() {
        return grpcErrorStatusMapping;
    }

    Duration getAttributeIndexSyncTimeout() {
        return attributeIndexSyncTimeout;
    }

    PullWorkerOptions getPull() {
        return pull;
    }

    boolean isBindAddressConfigured() {
        return isBindAddressConfigured;
    }

    /** Builds immutable {@link WorkerOptions} values. */
    public static final class Builder {
        private String bindAddress = ":8803";
        private WorkerTarget workerTarget;
        private String serverAddress = "localhost:8801";
        private ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();
        private GrpcErrorStatusMapping grpcErrorStatusMapping =
                GrpcErrorStatusMapping.newBuilder().build();
        private Duration attributeIndexSyncTimeout = Duration.ofMinutes(2);
        private PullWorkerOptions pull;
        private boolean isBindAddressConfigured;

        private Builder() {
        }

        /**
         * Sets the address on which the worker gRPC service listens.
         *
         * <p>Leave this unset when configuring {@link #pull(PullWorkerOptions)}.
         *
         * @param value the bind address; the default is {@code :8803}
         * @return this builder
         */
        public Builder bindAddress(final String value) {
            this.bindAddress = value;
            this.isBindAddressConfigured = true;
            return this;
        }

        /**
         * Enables outbound task polling without a Worker listener.
         *
         * <p>Do not set a bind address or listening Worker target with pull options. Advertise
         * {@link WorkerTarget#pull()} to a Client or Flow configuration.
         *
         * @param value the nonnull pull settings
         * @return this builder
         * @throws IllegalArgumentException if value is null
         */
        public Builder pull(final PullWorkerOptions value) {
            if (value == null) {
                throw new IllegalArgumentException("pull options are required");
            }
            this.pull = value;
            return this;
        }

        /**
         * Sets the Worker target advertised for Flow routing.
         *
         * <p>Use {@link WorkerTarget#pull()} with pull options, or a listening target with a bind
         * address. The Worker derives a listening target from its bind address when this is unset.
         *
         * @param value the advertised target, or {@code null} for no explicit target
         * @return this builder
         */
        public Builder workerTarget(final WorkerTarget value) {
            this.workerTarget = value;
            return this;
        }

        /**
         * Sets the Dex server gRPC target used for worker callbacks.
         *
         * @param value the server target; the default is {@code localhost:8801}
         * @return this builder
         */
        public Builder serverAddress(final String value) {
            this.serverAddress = value;
            return this;
        }

        /**
         * Sets the Jackson mapper used for application values.
         *
         * @param value the nonnull application mapper
         * @return this builder
         * @throws IllegalArgumentException if {@code value} is {@code null}
         */
        public Builder objectMapper(final ObjectMapper value) {
            if (value == null) {
                throw new IllegalArgumentException("objectMapper is required");
            }
            this.objectMapper = value;
            return this;
        }

        /**
         * Sets how Worker application exceptions map to gRPC status codes.
         *
         * <p>The default mapping reports every Step-method and RPC exception as
         * {@link io.grpc.Status.Code#INTERNAL}. Use a custom mapping only when an exception class has
         * a stable operational meaning for the owner of the Worker application. The selected status
         * is stored for diagnostics and does not replace Step retry or failure-policy configuration.
         *
         * @param value the nonnull immutable exception status mapping
         * @return this builder
         * @throws IllegalArgumentException if {@code value} is {@code null}
         */
        public Builder grpcErrorStatusMapping(final GrpcErrorStatusMapping value) {
            if (value == null) {
                throw new IllegalArgumentException("grpcErrorStatusMapping is required");
            }
            this.grpcErrorStatusMapping = value;
            return this;
        }

        /**
         * Sets the maximum duration for startup index synchronization.
         *
         * @param value a positive duration; the default is two minutes
         * @return this builder
         * @throws IllegalArgumentException if the duration is null, zero, or negative
         */
        public Builder attributeIndexSyncTimeout(final Duration value) {
            if (value == null || value.isZero() || value.isNegative()) {
                throw new IllegalArgumentException("attributeIndexSyncTimeout must be positive");
            }
            this.attributeIndexSyncTimeout = value;
            return this;
        }

        /**
         * Builds immutable worker options from the current values.
         *
         * @return the configured worker options
         * @throws IllegalArgumentException if pull is combined with a bind address or listening
         *     target, or a pull target is configured without pull options
         */
        public WorkerOptions build() {
            if (pull != null && (isBindAddressConfigured || workerTarget != null && !workerTarget.isPull())) {
                throw new IllegalArgumentException("Pull Worker cannot configure bindAddress or listening workerTarget");
            }
            if (pull == null && workerTarget != null && workerTarget.isPull()) {
                throw new IllegalArgumentException("pull WorkerTarget requires pull Worker options");
            }
            return new WorkerOptions(this);
        }
    }
}
