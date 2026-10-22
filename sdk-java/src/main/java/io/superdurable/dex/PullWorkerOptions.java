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

import java.time.Duration;

/**
 * Configures a Worker that opens outbound task streams to Dex.
 *
 * <p>Pass these options to {@link WorkerOptions.Builder#pull(PullWorkerOptions)}. Each Worker polls
 * its sticky queue and every registered Flow type queue. The worker does not open a listener.
 * Zero task and idle counts and a zero timeout select defaults. An empty Worker ID is generated per Worker.
 *
 * <pre>{@code
 * PullWorkerOptions pull = PullWorkerOptions.newBuilder()
 *         .maxConcurrentTasks(8)
 *         .stopDrainTimeout(Duration.ofSeconds(45))
 *         .build();
 * WorkerOptions options = WorkerOptions.newBuilder().pull(pull).build();
 * }</pre>
 */
public final class PullWorkerOptions {
    private final int maxConcurrentTasks;
    private final int reservedRpcTasks;
    private final boolean shouldWarnRpcCapacity;
    private final int idleStreamsPerQueue;
    private final String appWorkerID;
    private final Duration stopDrainTimeout;
    private final boolean stopOnPermanentStreamRejection;

    private PullWorkerOptions(final Builder builder) {
        this.stopOnPermanentStreamRejection = builder.stopOnPermanentStreamRejection;
        this.maxConcurrentTasks = builder.maxConcurrentTasks == 0
                ? Runtime.getRuntime().availableProcessors() : builder.maxConcurrentTasks;
        this.shouldWarnRpcCapacity = builder.reservedRpcTasks == null && maxConcurrentTasks == 1;
        this.reservedRpcTasks = builder.reservedRpcTasks == null
                ? (maxConcurrentTasks >= 2 ? 1 : 0) : builder.reservedRpcTasks;
        if (reservedRpcTasks < 0 || reservedRpcTasks >= maxConcurrentTasks) {
            throw new IllegalArgumentException("reservedRpcTasks must be nonnegative and less than maxConcurrentTasks");
        }
        this.idleStreamsPerQueue = builder.idleStreamsPerQueue == 0
                ? 1 : builder.idleStreamsPerQueue;
        this.appWorkerID = builder.appWorkerID;
        this.stopDrainTimeout = builder.stopDrainTimeout.isZero()
                ? Duration.ofSeconds(30) : builder.stopDrainTimeout;
    }

    /**
     * Creates a builder with CPU-count task capacity, one idle stream per queue, and a 30-second drain.
     *
     * @return a mutable pull options builder
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    int getMaxConcurrentTasks() {
        return maxConcurrentTasks;
    }

    boolean shouldWarnRpcCapacity() {
        return shouldWarnRpcCapacity;
    }

    int getReservedRpcTasks() {
        return reservedRpcTasks;
    }

    int getIdleStreamsPerQueue() {
        return idleStreamsPerQueue;
    }

    String getAppWorkerID() {
        return appWorkerID;
    }

    boolean isStopOnPermanentStreamRejection() {
        return stopOnPermanentStreamRejection;
    }

    Duration getStopDrainTimeout() {
        return stopDrainTimeout;
    }

    /**
     * Builds immutable outbound polling options.
     *
     * <p>Obtain a builder from {@link PullWorkerOptions#newBuilder()}. Zero values use the
     * defaults documented by each setter. The enclosing type shows a complete Worker example.
     */
    public static final class Builder {
        private int maxConcurrentTasks;
        private Integer reservedRpcTasks;
        private int idleStreamsPerQueue;
        private String appWorkerID = "";
        private Duration stopDrainTimeout = Duration.ZERO;
        private boolean stopOnPermanentStreamRejection;

        private Builder() {
        }

        /**
         * Sets the maximum number of concurrent Step and RPC tasks across all queues.
         *
         * <p>This also sizes the Worker's handler pool. Assigned tasks above this capacity
         * are rejected so Dex can assign them elsewhere.
         *
         * @param value a nonnegative count; zero uses the available CPU count
         * @return this builder
         * @throws IllegalArgumentException if value is negative
         */
        public Builder maxConcurrentTasks(final int value) {
            if (value < 0) {
                throw new IllegalArgumentException("maxConcurrentTasks must not be negative");
            }
            this.maxConcurrentTasks = value;
            return this;
        }

        /**
         * Reserves handler capacity to protect synchronous RPC latency from long-running Steps.
         *
         * <p>Unset reserves one slot when the resolved task limit is at least two, otherwise zero.
         * With the CPU-count default, Steps get one fewer slot than before. Set zero to opt out.
         * With one slot and no explicit reserve, Worker start logs one warning about RPC starvation.
         * Flows must stay responsive; Dex cannot prioritize synchronous RPCs like workflow engines prioritize reads.
         * WaitFor and Execute tasks, including local attempts, are declined when
         * total busy handlers reach maxConcurrentTasks minus this reserve. RPCs can use the full
         * task limit, including API-direct and transactional RPCs. Declines reassign without consuming an attempt.
         * If all Workers remain at the Step limit until the dispatch wait expires, NO_WORKER_AVAILABLE
         * consumes an attempt and follows the Step retry policy, as when all Workers are full.
         * Local attempts wait the local dispatch wait, then fall back according to their policy.
         * Declines emit a FINE debug log, increment getPullReservedRpcDeclines, and preserve readiness.
         *
         * <pre>{@code
         * PullWorkerOptions pull = PullWorkerOptions.newBuilder()
         *         .maxConcurrentTasks(8).reservedRpcTasks(2).build();
         * }</pre>
         *
         * @param value nonnegative reserved capacity below the resolved limit; null selects the default
         * @return this builder
         * @throws IllegalArgumentException from build if value is negative or reaches the task limit
         */
        public Builder reservedRpcTasks(final Integer value) {
            this.reservedRpcTasks = value;
            return this;
        }

        /**
         * Sets the number of waiting streams maintained for each task queue.
         *
         * @param value a nonnegative count; zero uses one idle stream per queue
         * @return this builder
         * @throws IllegalArgumentException if value is negative
         */
        public Builder idleStreamsPerQueue(final int value) {
            if (value < 0) {
                throw new IllegalArgumentException("idleStreamsPerQueue must not be negative");
            }
            this.idleStreamsPerQueue = value;
            return this;
        }

        /**
         * Sets the process identity used for sticky task routing.
         *
         * <p>The ID has 1-63 ASCII letters, digits, periods, underscores, or hyphens. It must
         * differ from every registered Flow type. Empty selects a hostname-derived random ID.
         *
         * @param value the Worker ID, or empty to generate one
         * @return this builder
         * @throws IllegalArgumentException if value is null or malformed
         */
        public Builder appWorkerID(final String value) {
            if (value == null || (!value.isEmpty() && !value.matches("[A-Za-z0-9._-]{1,63}"))) {
                throw new IllegalArgumentException("appWorkerID must contain 1-63 ASCII letters, digits, periods, underscores, or hyphens");
            }
            this.appWorkerID = value;
            return this;
        }

        /**
         * Sets how long Stop waits for active pull tasks before canceling them.
         *
         * <p>A canceled handler must return before Stop completes.
         *
         * @param value a nonnegative duration; zero uses 30 seconds
         * @return this builder
         * @throws IllegalArgumentException if value is null or negative
         */
        public Builder stopDrainTimeout(final Duration value) {
            if (value == null || value.isNegative()) {
                throw new IllegalArgumentException("stopDrainTimeout must not be negative");
            }
            this.stopDrainTimeout = value;
            return this;
        }

        /**
         * Stops the Worker on a permanent rejection leaving a queue without a registered stream.
         *
         * <p>Defaults to false, which retries indefinitely. When enabled, the first such rejection
         * before or after readiness drains within stopDrainTimeout, then makes Worker.start throw
         * PullRegistrationError. Other registered streams for that queue prevent this shutdown.
         * See the enclosing type for a complete options example.
         *
         * @param value whether to stop on an uncovered permanent stream rejection
         * @return this builder
         */
        public Builder stopOnPermanentStreamRejection(final boolean value) {
            this.stopOnPermanentStreamRejection = value;
            return this;
        }

        /**
         * Builds pull options for a Worker.
         *
         * @return immutable pull options
         */
        public PullWorkerOptions build() {
            return new PullWorkerOptions(this);
        }
    }
}
