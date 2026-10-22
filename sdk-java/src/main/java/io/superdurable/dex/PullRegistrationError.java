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
 * Identifies a permanent pull rejection leaving a queue without a registered stream.
 *
 * <p>Thrown by {@link Worker#waitPullRegistered(java.time.Duration)} before initial readiness,
 * or by {@link Worker#start()} when stopping on permanent rejection is enabled.
 * Permanent codes are InvalidArgument, ResourceExhausted, Unimplemented, PermissionDenied,
 * and Unauthenticated. The cause is the original transport error. See the wait method for a startup example.
 */
public final class PullRegistrationError extends IllegalStateException {
    private static final long serialVersionUID = 1L;
    private final String queue;

    /**
     * Creates a rejection retaining its original transport cause.
     *
     * @param queue the rejected Flow type or app Worker ID queue
     * @param cause the original permanent transport error
     */
    public PullRegistrationError(final String queue, final Throwable cause) {
        super("pull Worker queue " + queue + " registration rejected", cause);
        this.queue = queue;
    }

    /**
     * Returns the rejected queue name.
     *
     * @return the Flow type or app Worker ID queue
     */
    public String getQueue() {
        return queue;
    }
}
