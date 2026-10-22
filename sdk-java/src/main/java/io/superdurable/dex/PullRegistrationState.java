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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Provides an independent, immutable snapshot of pull queue readiness.
 *
 * <p>Use {@link Worker#getPullRegistrationState()} for readiness probes after initial startup.
 * Transient failures, reconnects, and stop preserve readiness after every queue has registered
 * once. A permanent rejection makes readiness false while its queue has no registered stream.
 * See {@link Worker#waitPullRegistered(java.time.Duration)} for a startup example.
 */
public final class PullRegistrationState {
    private final boolean isReady;
    private final List<PullQueueRegistrationState> queues;

    PullRegistrationState(final boolean isReady, final List<PullQueueRegistrationState> queues) {
        this.isReady = isReady;
        this.queues = Collections.unmodifiableList(new ArrayList<PullQueueRegistrationState>(queues));
    }

    /**
     * Reports whether initial registration completed without an uncovered permanent rejection.
     *
     * @return true after all queues registered once unless a queue has a permanent error
     *     and no registered stream; false for a listening Worker
     */
    public boolean isReady() {
        return isReady;
    }

    /**
     * Returns queue snapshots ordered by app Worker ID and then sorted Flow types.
     *
     * @return an immutable list, empty for a listening Worker
     */
    public List<PullQueueRegistrationState> getQueues() {
        return queues;
    }
}
