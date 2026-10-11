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
 * Creates durable SubFlow conditions and reads their results during Step execution.
 *
 * <p>A SubFlow is a normal, independently addressable Flow. Returning a condition created by
 * {@link #run} from {@link Step#waitFor} starts or reuses that Flow and makes its completion part
 * of the surrounding {@link Wait}. Read the corresponding result only from the same Step's
 * {@link Step#execute} method.
 *
 * <p>The server starts the SubFlow after the wait-for method returns and retries the start within
 * its configured retry budget. When the start still fails, the waiting Step's
 * {@link WaitForFailurePolicy} decides the outcome. {@link WaitForFailurePolicy#FAIL_FLOW}, the
 * default, fails the parent Flow. {@link WaitForFailurePolicy#PROCEED} runs the execute method, and
 * the SubFlow result reports {@link FlowErrorType#SUB_FLOW_START_FAILED}.
 *
 * <pre>{@code
 * public Wait waitFor(Context context, Order input) {
 *     return Wait.until(SubFlow.run(ChargeOrderFlow.class, input));
 * }
 *
 * public StepDecision execute(Context context, Order input) {
 *     FlowResult charge = SubFlow.getConditionResults(context);
 *     return StepDecision.gracefulComplete(charge.getSingleOutput(Receipt.class));
 * }
 * }</pre>
 */
public final class SubFlow {
    private SubFlow() {
    }

    /**
     * Creates a SubFlow condition using default options.
     *
     * @param flowClass the registered SubFlow class
     * @param input the typed input for its starting Step
     * @param <I> the starting input type
     * @param <F> the Flow implementation type
     * @return a durable SubFlow condition
     * @throws IllegalArgumentException if {@code flowClass} is {@code null}
     */
    @SuppressWarnings("unchecked")
    public static <I, F extends Flow<I>> Condition run(
            final Class<F> flowClass,
            final I input) {
        return Condition.subFlow((Class<? extends Flow<?>>) (Class<?>) flowClass, input, null);
    }

    /**
     * Creates a SubFlow condition with explicit options.
     *
     * @param flowClass the registered SubFlow class
     * @param input the typed input for its starting Step
     * @param options start, reuse, and condition options
     * @param <I> the starting input type
     * @param <F> the Flow implementation type
     * @return a durable SubFlow condition
     * @throws IllegalArgumentException if {@code flowClass} is {@code null}
     */
    @SuppressWarnings("unchecked")
    public static <I, F extends Flow<I>> Condition run(
            final Class<F> flowClass,
            final I input,
            final SubFlowOptions options) {
        return Condition.subFlow(
                (Class<? extends Flow<?>>) (Class<?>) flowClass, input, options);
    }

    /**
     * Returns the first SubFlow condition result from this Step invocation.
     *
     * @param context the current execute invocation context
     * @return the running snapshot or terminal result at SubFlow index zero
     * @throws IllegalStateException if called outside the Step's execute invocation
     * @throws IllegalArgumentException if the context is not managed by Dex or no SubFlow exists
     */
    public static FlowResult getConditionResults(final Context context) {
        return getConditionResults(context, 0);
    }

    /**
     * Returns one SubFlow condition result from this Step invocation.
     *
     * <p>A SubFlow that the server could not start returns {@link FlowStatus#FAILED} with
     * {@link FlowErrorType#SUB_FLOW_START_FAILED} and the start error as its error message. The
     * execute method sees this result only when the Step uses {@link WaitForFailurePolicy#PROCEED}.
     * Check the error type to tell a start failure apart from a SubFlow that started and then failed.
     *
     * <pre>{@code
     * FlowResult charge = SubFlow.getConditionResults(context, 0);
     * if (charge.getErrorType() == FlowErrorType.SUB_FLOW_START_FAILED) {
     *     return StepDecision.forceFail("SubFlow did not start: " + charge.getErrorMessage());
     * }
     * }</pre>
     *
     * @param context the current execute invocation context
     * @param index the zero-based SubFlow condition index
     * @return the running snapshot or terminal result
     * @throws IllegalStateException if called outside the Step's execute invocation
     * @throws IllegalArgumentException if the context is not managed by Dex or the index is invalid
     */
    public static FlowResult getConditionResults(final Context context, final int index) {
        if (!(context instanceof InvocationContext)) {
            throw new IllegalArgumentException("SubFlow results require a Dex invocation Context");
        }
        return ((InvocationContext) context).subFlowResult(index);
    }

    /**
     * Returns the generated Flow ID for the first SubFlow condition in this Step invocation.
     *
     * <p>The ID starts with {@code SubFlow:} and is available for running and terminal results.
     * It can be passed to
     * {@link Client#stopFlow(String)} when another condition wins a {@link Wait#anyOf}.
     *
     * @param context the current execute invocation context
     * @return the generated SubFlow Flow ID at index zero
     * @throws IllegalStateException if called outside the Step's execute invocation
     * @throws IllegalArgumentException if the context is not managed by Dex or no SubFlow exists
     */
    public static String getFlowId(final Context context) {
        return getFlowId(context, 0);
    }

    /**
     * Returns the generated Flow ID for one SubFlow condition in this Step invocation.
     *
     * <p>The zero-based index uses the stable order of SubFlow conditions in the surrounding
     * {@link Wait}. The ID remains valid after the parent Step completes.
     *
     * @param context the current execute invocation context
     * @param index the zero-based SubFlow condition index
     * @return the generated SubFlow Flow ID
     * @throws IllegalStateException if called outside the Step's execute invocation
     * @throws IllegalArgumentException if the context is not managed by Dex or the index is invalid
     */
    public static String getFlowId(final Context context, final int index) {
        if (!(context instanceof InvocationContext)) {
            throw new IllegalArgumentException("SubFlow IDs require a Dex invocation Context");
        }
        return ((InvocationContext) context).subFlowId(index);
    }
}
