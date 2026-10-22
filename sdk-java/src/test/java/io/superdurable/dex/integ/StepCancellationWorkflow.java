/*
 * Portions of this file are derived from indeedeng/iwf-java-sdk.
 * Those portions are licensed under the Apache License, Version 2.0.
 * See LICENSES/Apache-2.0.txt and LEGACY_NOTICES.md.
 *
 * Modifications Copyright (c) 2026 Super Durable, Inc.
 *
 * Modifications are licensed under the Sustainable Use License 1.0.
 * Third-Party Materials remain under the Apache License, Version 2.0.
 * See LICENSE and LEGACY_NOTICES.md.
 */

package io.superdurable.dex.integ;

import io.superdurable.dex.Attribute;
import io.superdurable.dex.Channel;
import io.superdurable.dex.Context;
import io.superdurable.dex.Flow;
import io.superdurable.dex.PersistenceSchema;
import io.superdurable.dex.RPC;
import io.superdurable.dex.Step;
import io.superdurable.dex.StepDecision;
import io.superdurable.dex.StepDurability;
import io.superdurable.dex.StepList;
import io.superdurable.dex.StepMovement;
import io.superdurable.dex.StepOptions;
import io.superdurable.dex.Timer;
import io.superdurable.dex.Wait;
import io.superdurable.dex.WaitForFailurePolicy;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

class StepCancellationWorkflow implements Flow<Void> {
    enum Scenario {
        HEARTBEAT_EXECUTE,
        HEARTBEAT_WAIT_FOR,
        LOCAL_EXECUTE,
        LOCAL_TIMEOUT_FALLBACK,
        NO_HEARTBEAT,
        GLOBAL_SELECTOR,
        SIBLING_SELECTOR
    }

    private static final Duration HANDLER_TIMEOUT = Duration.ofSeconds(15);
    private static final Duration HEARTBEAT_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration CANCELLATION_INTERRUPT_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration WINNER_DELAY = Duration.ofSeconds(3);
    private static final long LOCAL_WINNER_DELAY_MILLIS = 1_000L;
    private static final long BLOCKING_HANDLER_MILLIS = 10_000L;
    private static final long LATE_HANDLER_MILLIS = 7_000L;
    private static final long HEARTBEAT_INTERVAL_MILLIS = 250L;

    final Attribute<String> lateWrite = Attribute.define("cancellation-late-write", String.class);
    final Channel<Void> selectorWinnerRelease =
            Channel.define("selector-winner-release", Void.class);
    final Channel<Void> selectorWaitingRelease =
            Channel.define("selector-waiting-release", Void.class);
    final Channel<Void> selectorFinalRelease =
            Channel.define("selector-final-release", Void.class);

    private final Scenario scenario;
    private final CountDownLatch blockingHandlerStarted = new CountDownLatch(1);
    private final CountDownLatch cancellationObserved = new CountDownLatch(1);
    private final CountDownLatch lateHandlerReturned = new CountDownLatch(1);
    private final CountDownLatch selectorWaitsRegistered = new CountDownLatch(2);
    private final CountDownLatch secondSelectorExecution = new CountDownLatch(1);
    private final AtomicBoolean handlerInterrupted = new AtomicBoolean();
    private final AtomicBoolean contextReportedCancellation = new AtomicBoolean();
    private final AtomicBoolean recoveryRan = new AtomicBoolean();
    private final AtomicBoolean firstSelectorExecuted = new AtomicBoolean();
    private final AtomicBoolean secondSelectorExecuted = new AtomicBoolean();
    private final AtomicInteger blockingExecuteInvocations = new AtomicInteger();

    private final StepCancellationStartStep start = new StepCancellationStartStep();
    private final StepCancellationBlockingExecuteStep blockingExecute =
            new StepCancellationBlockingExecuteStep();
    private final StepCancellationBlockingWaitForStep blockingWaitFor =
            new StepCancellationBlockingWaitForStep();
    private final StepCancellationWinnerStep winner = new StepCancellationWinnerStep();
    private final StepCancellationRecoveryStep recovery = new StepCancellationRecoveryStep();
    private final StepCancellationFinalStep finalStep = new StepCancellationFinalStep();
    private final StepCancellationFirstParentStep firstParent =
            new StepCancellationFirstParentStep();
    private final StepCancellationSecondParentStep secondParent =
            new StepCancellationSecondParentStep();
    private final StepCancellationSelectorWinnerStep selectorWinner =
            new StepCancellationSelectorWinnerStep();
    private final StepCancellationSelectorWaitingStep selectorWaiting =
            new StepCancellationSelectorWaitingStep();

    StepCancellationWorkflow(final Scenario scenario) {
        this.scenario = scenario;
    }

    @Override
    public StepList<Void> getSteps() {
        return StepList.startStep(start).otherSteps(
                blockingExecute,
                blockingWaitFor,
                winner,
                recovery,
                finalStep,
                firstParent,
                secondParent,
                selectorWinner,
                selectorWaiting);
    }

    @Override
    public PersistenceSchema getPersistenceSchema() {
        return PersistenceSchema.of(
                lateWrite,
                selectorWinnerRelease,
                selectorWaitingRelease,
                selectorFinalRelease);
    }

    @RPC
    public void releaseSelectorWinner(final Context context) {
        selectorWinnerRelease.publish(context, null);
    }

    @RPC
    public void releaseSelectorWaiting(final Context context) {
        selectorWaitingRelease.publish(context, null);
    }

    @RPC
    public void releaseSelectorFinal(final Context context) {
        selectorFinalRelease.publish(context, null);
    }

    String canceledStepType() {
        return scenario == Scenario.HEARTBEAT_WAIT_FOR
                ? blockingWaitFor.getStepType()
                : blockingExecute.getStepType();
    }

    boolean awaitBlockingHandlerStarted(final Duration timeout) throws InterruptedException {
        return blockingHandlerStarted.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    boolean awaitCancellation(final Duration timeout) throws InterruptedException {
        return cancellationObserved.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    boolean awaitLateHandlerReturn(final Duration timeout) throws InterruptedException {
        return lateHandlerReturned.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    boolean awaitSelectorWaits(final Duration timeout) throws InterruptedException {
        return selectorWaitsRegistered.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    boolean awaitSecondSelectorExecution(final Duration timeout) throws InterruptedException {
        return secondSelectorExecution.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    boolean hasLateHandlerReturned() {
        return lateHandlerReturned.getCount() == 0L;
    }

    boolean wasHandlerInterrupted() {
        return handlerInterrupted.get();
    }

    boolean didContextReportCancellation() {
        return contextReportedCancellation.get();
    }

    boolean wasRecoveryRun() {
        return recoveryRan.get();
    }

    boolean wasFirstSelectorExecuted() {
        return firstSelectorExecuted.get();
    }

    boolean wasSecondSelectorExecuted() {
        return secondSelectorExecuted.get();
    }

    String selectorWinnerStepType() {
        return selectorWinner.getStepType();
    }

    int blockingExecuteInvocations() {
        return blockingExecuteInvocations.get();
    }

    private void blockUntilCanceled(final Context context) {
        blockingHandlerStarted.countDown();
        try {
            final long deadline = System.currentTimeMillis() + BLOCKING_HANDLER_MILLIS;
            while (System.currentTimeMillis() < deadline) {
                Thread.sleep(HEARTBEAT_INTERVAL_MILLIS);
                context.recordHeartbeat(null);
            }
        } catch (InterruptedException interruption) {
            handlerInterrupted.set(true);
            observeCancellation(context);
        } catch (RuntimeException failure) {
            if (!context.isCancellationRequested()) {
                throw failure;
            }
            observeCancellation(context);
        }
    }

    private void observeCancellation(final Context context) {
        // Context cancellation can become visible before the Worker interrupts this handler.
        if (!handlerInterrupted.get() && !Thread.currentThread().isInterrupted()) {
            try {
                new CountDownLatch(1).await(
                        CANCELLATION_INTERRUPT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException interruption) {
                handlerInterrupted.set(true);
            }
        }
        if (Thread.currentThread().isInterrupted()) {
            handlerInterrupted.set(true);
        }
        contextReportedCancellation.set(context.isCancellationRequested());
        cancellationObserved.countDown();
    }

    final class StepCancellationStartStep implements Step<Void> {
        @Override
        public Class<Void> getInputType() {
            return Void.class;
        }

        @Override
        public StepDecision execute(final Context context, final Void input) {
            switch (scenario) {
                case HEARTBEAT_WAIT_FOR:
                    return StepDecision.goToMany(
                            StepMovement.of(StepCancellationBlockingWaitForStep.class, null),
                            StepMovement.of(StepCancellationWinnerStep.class, null));
                case GLOBAL_SELECTOR:
                case SIBLING_SELECTOR:
                    return StepDecision.goToMany(
                            StepMovement.of(StepCancellationFirstParentStep.class, null),
                            StepMovement.of(StepCancellationSecondParentStep.class, null));
                default:
                    return StepDecision.goToMany(
                            StepMovement.of(StepCancellationBlockingExecuteStep.class, null),
                            StepMovement.of(StepCancellationWinnerStep.class, null));
            }
        }
    }

    final class StepCancellationBlockingExecuteStep implements Step<Void> {
        @Override
        public Class<Void> getInputType() {
            return Void.class;
        }

        @Override
        public StepDecision execute(final Context context, final Void input) {
            blockingExecuteInvocations.incrementAndGet();
            blockingHandlerStarted.countDown();
            try {
                if (scenario == Scenario.HEARTBEAT_EXECUTE
                        || scenario == Scenario.LOCAL_EXECUTE) {
                    final long deadline = System.currentTimeMillis() + BLOCKING_HANDLER_MILLIS;
                    while (System.currentTimeMillis() < deadline) {
                        Thread.sleep(HEARTBEAT_INTERVAL_MILLIS);
                        context.recordHeartbeat(null);
                    }
                } else {
                    Thread.sleep(scenario == Scenario.NO_HEARTBEAT
                            ? LATE_HANDLER_MILLIS
                            : BLOCKING_HANDLER_MILLIS);
                }
            } catch (InterruptedException interruption) {
                handlerInterrupted.set(true);
                observeCancellation(context);
            } catch (RuntimeException failure) {
                if (!context.isCancellationRequested()) {
                    throw failure;
                }
                observeCancellation(context);
            }
            lateWrite.set(context, "late");
            lateHandlerReturned.countDown();
            return StepDecision.goTo(StepCancellationRecoveryStep.class, null);
        }

        @Override
        public StepOptions getStepOptions() {
            final StepOptions.Builder options = StepOptions.newBuilder()
                    .executeMethodTimeout(HANDLER_TIMEOUT)
                    .onExecuteFailureProceedTo(StepCancellationRecoveryStep.class);
            if (scenario == Scenario.LOCAL_EXECUTE) {
                return options.executeDurability(StepDurability.ASYNC).build();
            }
            if (scenario == Scenario.LOCAL_TIMEOUT_FALLBACK) {
                return options
                        .heartbeatTimeout(HEARTBEAT_TIMEOUT)
                        .executeDurability(StepDurability.ASYNC)
                        .build();
            }
            if (scenario == Scenario.HEARTBEAT_EXECUTE) {
                options.heartbeatTimeout(HEARTBEAT_TIMEOUT);
            }
            return options.executeDurability(StepDurability.SYNC).build();
        }
    }

    final class StepCancellationBlockingWaitForStep implements Step<Void> {
        @Override
        public Class<Void> getInputType() {
            return Void.class;
        }

        @Override
        public Wait waitFor(final Context context, final Void input) {
            blockUntilCanceled(context);
            return Wait.skipImmediately();
        }

        @Override
        public StepDecision execute(final Context context, final Void input) {
            recoveryRan.set(true);
            return StepDecision.forceFail("canceled waitFor execution continued");
        }

        @Override
        public StepOptions getStepOptions() {
            return StepOptions.newBuilder()
                    .waitForMethodTimeout(HANDLER_TIMEOUT)
                    .heartbeatTimeout(HEARTBEAT_TIMEOUT)
                    .waitForDurability(StepDurability.SYNC)
                    .waitForFailure(WaitForFailurePolicy.PROCEED)
                    .build();
        }
    }

    final class StepCancellationWinnerStep implements Step<Void> {
        @Override
        public Class<Void> getInputType() {
            return Void.class;
        }

        @Override
        public Wait waitFor(final Context context, final Void input) {
            if (scenario == Scenario.LOCAL_EXECUTE) {
                return Wait.skipImmediately();
            }
            return Wait.until(Timer.byDuration(WINNER_DELAY));
        }

        @Override
        public StepDecision execute(final Context context, final Void input) {
            if (scenario == Scenario.LOCAL_EXECUTE) {
                awaitLocalWinnerDelay();
            }
            final Class<? extends Step<?>> canceled = scenario == Scenario.HEARTBEAT_WAIT_FOR
                    ? StepCancellationBlockingWaitForStep.class
                    : StepCancellationBlockingExecuteStep.class;
            return StepDecision.goTo(StepCancellationFinalStep.class, scenario.name())
                    .withCancelingSteps(canceled);
        }

        private void awaitLocalWinnerDelay() {
            try {
                if (!blockingHandlerStarted.await(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("local loser did not start");
                }
                Thread.sleep(LOCAL_WINNER_DELAY_MILLIS);
            } catch (InterruptedException interruption) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("local winner interrupted", interruption);
            }
        }
    }

    final class StepCancellationRecoveryStep implements Step<Void> {
        @Override
        public Class<Void> getInputType() {
            return Void.class;
        }

        @Override
        public StepDecision execute(final Context context, final Void input) {
            recoveryRan.set(true);
            return StepDecision.forceFail("canceled execution reached recovery");
        }
    }

    final class StepCancellationFinalStep implements Step<String> {
        @Override
        public Class<String> getInputType() {
            return String.class;
        }

        @Override
        public Wait waitFor(final Context context, final String input) {
            if (Scenario.GLOBAL_SELECTOR.name().equals(input)) {
                return Wait.skipImmediately();
            }
            if (Scenario.SIBLING_SELECTOR.name().equals(input)) {
                return Wait.until(selectorFinalRelease.forOne());
            }
            return Wait.until(Timer.byDuration(Duration.ofSeconds(1)));
        }

        @Override
        public StepDecision execute(final Context context, final String input) {
            return StepDecision.gracefulComplete(input);
        }
    }

    final class StepCancellationFirstParentStep implements Step<Void> {
        @Override
        public Class<Void> getInputType() {
            return Void.class;
        }

        @Override
        public StepDecision execute(final Context context, final Void input) {
            return StepDecision.goToMany(
                    StepMovement.of(StepCancellationSelectorWinnerStep.class, null),
                    StepMovement.of(StepCancellationSelectorWaitingStep.class, "first"));
        }
    }

    final class StepCancellationSecondParentStep implements Step<Void> {
        @Override
        public Class<Void> getInputType() {
            return Void.class;
        }

        @Override
        public StepDecision execute(final Context context, final Void input) {
            return StepDecision.goTo(StepCancellationSelectorWaitingStep.class, "second");
        }
    }

    final class StepCancellationSelectorWinnerStep implements Step<Void> {
        @Override
        public Class<Void> getInputType() {
            return Void.class;
        }

        @Override
        public Wait waitFor(final Context context, final Void input) {
            return Wait.until(selectorWinnerRelease.forOne());
        }

        @Override
        public StepDecision execute(final Context context, final Void input) {
            final StepDecision decision = StepDecision.goTo(
                    StepCancellationFinalStep.class, scenario.name());
            return scenario == Scenario.GLOBAL_SELECTOR
                    ? decision.withCancelingSteps(StepCancellationSelectorWaitingStep.class)
                    : decision.withCancelingSiblingSteps(StepCancellationSelectorWaitingStep.class);
        }
    }

    final class StepCancellationSelectorWaitingStep implements Step<String> {
        @Override
        public Class<String> getInputType() {
            return String.class;
        }

        @Override
        public Wait waitFor(final Context context, final String input) {
            selectorWaitsRegistered.countDown();
            return Wait.until(selectorWaitingRelease.forOne());
        }

        @Override
        public StepDecision execute(final Context context, final String input) {
            if ("first".equals(input)) {
                firstSelectorExecuted.set(true);
            } else {
                secondSelectorExecuted.set(true);
                secondSelectorExecution.countDown();
            }
            return StepDecision.deadEnd();
        }
    }
}
