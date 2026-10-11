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
 * Classifies why a Flow reached a terminal status other than {@link FlowStatus#COMPLETED}.
 *
 * <p>{@link FlowResult#getErrorType()} exposes this value after Dex records a Flow failure.
 */
public enum FlowErrorType {
    /** A Step returned a decision that failed the Flow. */
    STEP_DECISION_FAILED,

    /** A client API operation caused the Flow to fail. */
    CLIENT_API_FAILED,

    /** A worker API invocation caused the Flow to fail. */
    WORKER_API_FAILED,

    /** Dex rejected invalid user Flow code or an invalid definition. */
    INVALID_USER_FLOW_CODE,

    /** Dex encountered an internal failure. */
    INTERNAL,

    /** A Dex soft Flow timeout expired under {@link FlowTimeoutPolicy#FAIL}. */
    FLOW_TIMEOUT,

    /**
     * The server could not start a SubFlow within its configured retry budget.
     *
     * <p>Only {@link SubFlow#getConditionResults(Context, int)} reports this value. The SubFlow never
     * ran, so the result has no outputs and {@link FlowResult#getErrorMessage()} holds the last start
     * error. The waiting Step sees this result only when it uses {@link WaitForFailurePolicy#PROCEED}.
     */
    SUB_FLOW_START_FAILED
}
