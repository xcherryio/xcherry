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

package io.superdurable.dex.shared;

import io.superdurable.dex.PullWorkerOptions;

/** How an example Worker receives tasks from Dex Server. */
public enum ExampleWorkerMode {
    /** Listens on a Worker address that Dex Server calls. */
    PUSH,
    /** Opens outbound task streams to Dex Server. */
    PULL;

    /** Names the environment variable that selects the Worker mode. */
    public static final String ENVIRONMENT_VARIABLE = "DEX_EXAMPLES_WORKER_MODE";

    /**
     * Returns the Worker mode selected by DEX_EXAMPLES_WORKER_MODE.
     *
     * @return push when the variable is unset or empty, otherwise the selected mode
     * @throws IllegalArgumentException when the value is not push or pull
     */
    public static ExampleWorkerMode readFromEnvironment() {
        return parse(System.getenv(ENVIRONMENT_VARIABLE));
    }

    /**
     * Parses a DEX_EXAMPLES_WORKER_MODE value.
     *
     * @param value the configured value; null or empty selects push
     * @return the selected Worker mode
     * @throws IllegalArgumentException when the value is not push or pull
     */
    public static ExampleWorkerMode parse(final String value) {
        if (value == null || value.isEmpty() || "push".equals(value)) {
            return PUSH;
        }
        if ("pull".equals(value)) {
            return PULL;
        }
        throw new IllegalArgumentException(
                ENVIRONMENT_VARIABLE + " must be push or pull, got \"" + value + "\"");
    }

    /**
     * Reports whether the Worker connects to Dex Server as a pull Worker.
     *
     * @return true for pull mode
     */
    public boolean isPull() {
        return this == PULL;
    }

    /**
     * Returns pull options sized for every sample Flow sharing one Worker process.
     *
     * @return pull Worker options with 64 concurrent tasks
     */
    public static PullWorkerOptions newExamplePullWorkerOptions() {
        return PullWorkerOptions.newBuilder().maxConcurrentTasks(64).build();
    }
}
