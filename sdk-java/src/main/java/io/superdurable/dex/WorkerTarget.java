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
 * Identifies the Worker endpoint or pull queue to which Dex routes Flow invocations.
 *
 * <p>Use the same target in {@link WorkerOptions} and in the client-side Flow configuration. A
 * headless target is resolved by Dex as a headless service address; a non-headless target is used
 * as a directly reachable worker address. Use {@link #pull()} for a Worker that polls Dex;
 * a pull target has no address or headless service.
 *
 * <pre>{@code
 * WorkerTarget target = new WorkerTarget("orders-worker:8803", false);
 * WorkerOptions workerOptions = WorkerOptions.newBuilder()
 *         .workerTarget(target)
 *         .build();
 * }</pre>
 */
public final class WorkerTarget {
    private final String address;
    private final boolean headless;
    private final boolean pull;

    /**
     * Creates a worker routing target.
     *
     * @param address the worker address advertised to Dex; must not be blank
     * @param headless whether the address represents a headless service
     * @throws IllegalArgumentException if {@code address} is {@code null} or blank
     */
    public WorkerTarget(final String address, final boolean headless) {
        if (address == null || address.trim().isEmpty()) {
            throw new IllegalArgumentException("Worker target address is required");
        }
        this.address = address;
        this.headless = headless;
        this.pull = false;
    }

    private WorkerTarget() {
        this.address = "";
        this.headless = false;
        this.pull = true;
    }

    /**
     * Selects a Worker that polls Dex for tasks.
     *
     * <p>Use this target in Client defaults or a Flow configuration when the Worker uses
     * {@link PullWorkerOptions}. Pull routing has no address or headless service.
     *
     * <pre>{@code
     * WorkerTarget target = WorkerTarget.pull();
     * ClientOptions options = new ClientOptions("dex:8801", target);
     * }</pre>
     *
     * @return a pull routing target
     */
    public static WorkerTarget pull() {
        return new WorkerTarget();
    }

    String getAddress() {
        return address;
    }

    boolean isHeadless() {
        return headless;
    }

    boolean isPull() {
        return pull;
    }
}
