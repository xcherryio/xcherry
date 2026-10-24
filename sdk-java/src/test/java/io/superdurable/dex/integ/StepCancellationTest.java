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

import io.superdurable.dex.FlowResult;
import io.superdurable.dex.StepExecutionId;
import io.superdurable.dex.WaitForStepCompletionOptions;
import io.superdurable.dex.testing.DexDevTestEnvironment;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("dex-dev")
public final class StepCancellationTest {
    private static final Duration START_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration CANCELLATION_TIMEOUT = Duration.ofSeconds(8);
    private static final Duration FLOW_TIMEOUT = Duration.ofSeconds(30);

    @TempDir
    Path cacheDirectory;

    @Test
    void testHeartbeatCancelsRegularExecuteAndInterruptsHandler() throws Exception {
        IntegrationTestWaits.skipIfServerZD("Step failure policies");
        assertCooperativeCancellation(
                StepCancellationWorkflow.Scenario.HEARTBEAT_EXECUTE,
                CANCELLATION_TIMEOUT);
    }

    @Test
    void testHeartbeatCancelsRegularWaitForAndInterruptsHandler() throws Exception {
        IntegrationTestWaits.skipIfServerZD("Step failure policies");
        assertCooperativeCancellation(
                StepCancellationWorkflow.Scenario.HEARTBEAT_WAIT_FOR,
                CANCELLATION_TIMEOUT);
    }

    @Test
    void testLocalHeartbeatIsIgnoredUntilRegularFallback() throws Exception {
        IntegrationTestWaits.skipIfServerZD("Step failure policies");
        final StepCancellationWorkflow workflow = new StepCancellationWorkflow(
                StepCancellationWorkflow.Scenario.LOCAL_EXECUTE);
        try (DexDevTestEnvironment environment = DexDevTestEnvironment.start(
                cacheDirectory,
                workflow)) {
            final String flowId = start(environment, workflow, "cancel-local");
            assertTrue(workflow.awaitBlockingHandlerStarted(START_TIMEOUT));
            environment.client().waitForStepCompletion(
                    flowId,
                    StepExecutionId.of(workflow.canceledStepType()),
                    waitOptions(flowId, "local", FLOW_TIMEOUT));
            assertTrue(workflow.awaitCancellation(FLOW_TIMEOUT));
            assertCompleted(environment, flowId, StepCancellationWorkflow.Scenario.LOCAL_EXECUTE);
            assertTrue(workflow.wasHandlerInterrupted());
            assertTrue(workflow.didContextReportCancellation());
            assertEquals(2, workflow.blockingExecuteInvocations());
            assertFalse(workflow.wasRecoveryRun());
        }
    }

    @Test
    void testTimerCancelsAfterLocalTimeoutFallbackWithoutWorkerProgress() throws Exception {
        IntegrationTestWaits.skipIfServerZD("Step failure policies");
        final StepCancellationWorkflow workflow = new StepCancellationWorkflow(
                StepCancellationWorkflow.Scenario.LOCAL_TIMEOUT_FALLBACK);
        try (DexDevTestEnvironment environment = DexDevTestEnvironment.start(
                cacheDirectory,
                workflow)) {
            final String flowId = start(environment, workflow, "cancel-local-fallback");
            assertTrue(workflow.awaitBlockingHandlerStarted(START_TIMEOUT));
            environment.client().waitForStepCompletion(
                    flowId,
                    StepExecutionId.of(workflow.canceledStepType()),
                    waitOptions(flowId, "local-fallback", FLOW_TIMEOUT));
            assertCompleted(
                    environment,
                    flowId,
                    StepCancellationWorkflow.Scenario.LOCAL_TIMEOUT_FALLBACK);
            assertEquals(2, workflow.blockingExecuteInvocations());
            assertTrue(workflow.wasHandlerInterrupted());
            assertTrue(workflow.didContextReportCancellation());
            assertFalse(workflow.wasRecoveryRun());
        }
    }

    @Test
    void testCancellationWithoutHeartbeatContinuesBeforeLateHandlerReturn() throws Exception {
        IntegrationTestWaits.skipIfServerZD("Step failure policies");
        final StepCancellationWorkflow workflow = new StepCancellationWorkflow(
                StepCancellationWorkflow.Scenario.NO_HEARTBEAT);
        try (DexDevTestEnvironment environment = DexDevTestEnvironment.start(
                cacheDirectory,
                workflow)) {
            final String flowId = start(environment, workflow, "cancel-no-heartbeat");
            assertTrue(workflow.awaitBlockingHandlerStarted(START_TIMEOUT));
            environment.client().waitForStepCompletion(
                    flowId,
                    StepExecutionId.of(workflow.canceledStepType()),
                    waitOptions(flowId, "no-heartbeat", CANCELLATION_TIMEOUT));
            assertCompleted(environment, flowId, StepCancellationWorkflow.Scenario.NO_HEARTBEAT);
            assertFalse(workflow.hasLateHandlerReturned());
            assertFalse(workflow.wasHandlerInterrupted());
            assertTrue(workflow.awaitLateHandlerReturn(CANCELLATION_TIMEOUT));
            assertEquals(1, workflow.blockingExecuteInvocations());
            assertFalse(workflow.wasRecoveryRun());
        }
    }

    @Test
    void testGlobalSelectorCancelsMatchingExecutionsFromBothParents() throws Exception {
        final StepCancellationWorkflow workflow = new StepCancellationWorkflow(
                StepCancellationWorkflow.Scenario.GLOBAL_SELECTOR);
        try (DexDevTestEnvironment environment = DexDevTestEnvironment.start(
                cacheDirectory,
                workflow)) {
            final String flowId = start(environment, workflow, "cancel-global");
            assertTrue(workflow.awaitSelectorWaits(START_TIMEOUT));
            final StepCancellationWorkflow stub = environment.client().newRpcStub(
                    StepCancellationWorkflow.class,
                    flowId);
            environment.client().invokeRPC(stub::releaseSelectorWinner);
            assertCompleted(environment, flowId, StepCancellationWorkflow.Scenario.GLOBAL_SELECTOR);
            assertFalse(workflow.wasFirstSelectorExecuted());
            assertFalse(workflow.wasSecondSelectorExecuted());
        }
    }

    @Test
    void testSiblingSelectorKeepsMatchingExecutionFromOtherParent() throws Exception {
        final StepCancellationWorkflow workflow = new StepCancellationWorkflow(
                StepCancellationWorkflow.Scenario.SIBLING_SELECTOR);
        try (DexDevTestEnvironment environment = DexDevTestEnvironment.start(
                cacheDirectory,
                workflow)) {
            final String flowId = start(environment, workflow, "cancel-sibling");
            assertTrue(workflow.awaitSelectorWaits(START_TIMEOUT));
            final StepCancellationWorkflow stub = environment.client().newRpcStub(
                    StepCancellationWorkflow.class,
                    flowId);
            environment.client().invokeRPC(stub::releaseSelectorWinner);
            environment.client().waitForStepCompletion(
                    flowId,
                    StepExecutionId.of(workflow.selectorWinnerStepType()),
                    waitOptions(flowId, "selector", CANCELLATION_TIMEOUT));
            environment.client().invokeRPC(stub::releaseSelectorWaiting);
            assertTrue(workflow.awaitSecondSelectorExecution(CANCELLATION_TIMEOUT));
            environment.client().invokeRPC(stub::releaseSelectorFinal);
            assertCompleted(environment, flowId, StepCancellationWorkflow.Scenario.SIBLING_SELECTOR);
            assertFalse(workflow.wasFirstSelectorExecuted());
            assertTrue(workflow.wasSecondSelectorExecuted());
        }
    }

    private void assertCooperativeCancellation(
            final StepCancellationWorkflow.Scenario scenario,
            final Duration cancellationTimeout) throws Exception {
        final StepCancellationWorkflow workflow = new StepCancellationWorkflow(scenario);
        try (DexDevTestEnvironment environment = DexDevTestEnvironment.start(
                cacheDirectory,
                workflow)) {
            final String flowId = start(environment, workflow, "cancel-heartbeat");
            assertTrue(workflow.awaitBlockingHandlerStarted(START_TIMEOUT));
            environment.client().waitForStepCompletion(
                    flowId,
                    StepExecutionId.of(workflow.canceledStepType()),
                    waitOptions(flowId, scenario.name(), cancellationTimeout));
            assertTrue(workflow.awaitCancellation(cancellationTimeout));
            assertCompleted(environment, flowId, scenario);
            assertTrue(workflow.wasHandlerInterrupted());
            assertTrue(workflow.didContextReportCancellation());
            assertFalse(workflow.wasRecoveryRun());
        }
    }

    private static String start(
            final DexDevTestEnvironment environment,
            final StepCancellationWorkflow workflow,
            final String prefix) {
        final String flowId = prefix + "-" + UUID.randomUUID();
        environment.client().startFlow(workflow, flowId, null);
        return flowId;
    }

    private static WaitForStepCompletionOptions waitOptions(
            final String flowId,
            final String suffix,
            final Duration requestTimeout) {
        return WaitForStepCompletionOptions.newBuilder()
                .requestId(flowId + "-wait-" + suffix)
                .requestTimeout(requestTimeout)
                .build();
    }

    private static void assertCompleted(
            final DexDevTestEnvironment environment,
            final String flowId,
            final StepCancellationWorkflow.Scenario scenario) {
        final FlowResult result = environment.client().waitForFlow(flowId, FLOW_TIMEOUT);
        assertEquals(scenario.name(), result.getSingleOutput(String.class));
    }
}
