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

/**
 * Describes one queue in an immutable pull readiness snapshot.
 *
 * <p>Obtain entries from {@link PullRegistrationState#getQueues()}. A registered stream
 * clears the queue's last permanent error. Treat the error object as immutable.
 */
public final class PullQueueRegistrationState {
    private final String queue;
    private final boolean hasRegisteredStream;
    private final Throwable lastPermanentError;

    PullQueueRegistrationState(final String queue, final boolean hasRegisteredStream,
            final Throwable lastPermanentError) {
        this.queue = queue;
        this.hasRegisteredStream = hasRegisteredStream;
        this.lastPermanentError = lastPermanentError;
    }

    /**
     * Returns the queue name.
     *
     * @return the registered Flow type or app Worker ID queue
     */
    public String getQueue() {
        return queue;
    }

    /**
     * Reports whether a stream is currently registered for this queue.
     *
     * @return true while at least one stream has a Registered acknowledgement
     */
    public boolean hasRegisteredStream() {
        return hasRegisteredStream;
    }

    /**
     * Returns the most recent permanent rejection, cleared by re-registration.
     *
     * @return the original transport error, or null if no permanent error remains
     */
    public Throwable getLastPermanentError() {
        return lastPermanentError;
    }
}
