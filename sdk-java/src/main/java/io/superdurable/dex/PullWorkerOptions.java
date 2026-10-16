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
 * Zero counts and a zero timeout select defaults. An empty Worker ID is generated per Worker.
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
    private final int idleStreamsPerQueue;
    private final String appWorkerID;
    private final Duration stopDrainTimeout;

    private PullWorkerOptions(final Builder builder) {
        this.maxConcurrentTasks = builder.maxConcurrentTasks == 0
                ? Runtime.getRuntime().availableProcessors() : builder.maxConcurrentTasks;
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

    int getIdleStreamsPerQueue() {
        return idleStreamsPerQueue;
    }

    String getAppWorkerID() {
        return appWorkerID;
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
        private int idleStreamsPerQueue;
        private String appWorkerID = "";
        private Duration stopDrainTimeout = Duration.ZERO;

        private Builder() {
        }

        /**
         * Sets the maximum number of concurrent Step and RPC tasks across all queues.
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
         * Builds pull options for a Worker.
         *
         * @return immutable pull options
         */
        public PullWorkerOptions build() {
            return new PullWorkerOptions(this);
        }
    }
}
