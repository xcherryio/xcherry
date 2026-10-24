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

import io.superdurable.dex.ActiveStepSearchMode;
import io.superdurable.dex.Client;
import io.superdurable.dex.FlowErrorType;
import io.superdurable.dex.FlowConfig;
import io.superdurable.dex.FlowStatus;
import io.superdurable.dex.FlowInfo;
import io.superdurable.dex.IdReusePolicy;
import io.superdurable.dex.TimeTravelOptions;
import io.superdurable.dex.TimeTravelType;
import io.superdurable.dex.StartFlowOptions;
import io.superdurable.dex.StepExecutionId;
import io.superdurable.dex.StepCompletion;
import io.superdurable.dex.TimerId;
import io.superdurable.dex.FlowResult;
import io.superdurable.dex.WorkerTarget;
import io.superdurable.dex.WaitForStepCompletionOptions;
import io.superdurable.dex.exceptions.FlowAlreadyStartedException;
import io.superdurable.dex.exceptions.FlowDefinitionException;
import io.superdurable.dex.exceptions.FlowNotActiveOrNotFoundException;
import io.superdurable.dex.exceptions.FlowNotFoundException;
import io.superdurable.dex.testing.DexDevTestEnvironment;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("dex-dev")
public final class BasicTest {
    private static final BasicWorkflow WORKFLOW = new BasicWorkflow();
    private static final BasicAbnormalExitWorkflow ABNORMAL_EXIT_WORKFLOW =
            new BasicAbnormalExitWorkflow();
    private static final BasicEmptyInputWorkflow EMPTY_INPUT_WORKFLOW =
            new BasicEmptyInputWorkflow();
    private static final BasicModelInputWorkflow MODEL_INPUT_WORKFLOW =
            new BasicModelInputWorkflow();
    private static final BasicProceedOnWaitFailureWorkflow WAIT_FAILURE_WORKFLOW =
            new BasicProceedOnWaitFailureWorkflow();
    private static final SkipWaitUntilMixedWaitWorkflow MIXED_WAIT_WORKFLOW =
            new SkipWaitUntilMixedWaitWorkflow();
    private static final BasicImmutableStepOptionsWorkflow IMMUTABLE_OPTIONS_WORKFLOW =
            new BasicImmutableStepOptionsWorkflow();
    private static final SubFlowParentWorkflow SUB_FLOW_PARENT_WORKFLOW =
            new SubFlowParentWorkflow();
    private static final SubFlowAllParentWorkflow SUB_FLOW_ALL_PARENT_WORKFLOW =
            new SubFlowAllParentWorkflow();
    private static final SubFlowAnyParentWorkflow SUB_FLOW_ANY_PARENT_WORKFLOW =
            new SubFlowAnyParentWorkflow();
    private static final SubFlowAttachParentWorkflow SUB_FLOW_ATTACH_PARENT_WORKFLOW =
            new SubFlowAttachParentWorkflow();
    private static final SubFlowAlwaysRestartParentWorkflow SUB_FLOW_ALWAYS_RESTART_PARENT_WORKFLOW =
            new SubFlowAlwaysRestartParentWorkflow();
    private static final SubFlowAbnormalRestartParentWorkflow SUB_FLOW_ABNORMAL_PARENT_WORKFLOW =
            new SubFlowAbnormalRestartParentWorkflow();
    private static final SubFlowContinueAsNewParentWorkflow SUB_FLOW_CAN_PARENT_WORKFLOW =
            new SubFlowContinueAsNewParentWorkflow();

    @TempDir
    Path cacheDirectory;

    @Test
    void testBasicWorkflow() throws Exception {
        try (DexDevTestEnvironment environment = DexDevTestEnvironment.start(
                cacheDirectory,
                WORKFLOW)) {
            final String flowId = "basic-" + UUID.randomUUID();
            final Integer input = 0;
            final StartFlowOptions options = StartFlowOptions.newBuilder()
                    .idReusePolicy(IdReusePolicy.DISALLOW)
                    .build();
            environment.client().startFlow(WORKFLOW, flowId, input, options);
            final Integer output = environment.client().waitForFlow(flowId, Duration.ofSeconds(30)).getSingleOutput(Integer.class);
            assertEquals(input + 2, output);
            assertThrows(
                    FlowAlreadyStartedException.class,
                    () -> environment.client().startFlow(WORKFLOW, flowId, input, options));
        }
    }

    @Test
    void testSubFlowConditionReturnsIdentityAndOutput() throws Exception {
        try (DexDevTestEnvironment environment = DexDevTestEnvironment.start(
                cacheDirectory,
                SUB_FLOW_PARENT_WORKFLOW,
                WORKFLOW)) {
            final String flowId = flowId("sub-flow-parent");
            environment.client().startFlow(SUB_FLOW_PARENT_WORKFLOW, flowId, 4);

            final String output = environment.client()
                    .waitForFlow(flowId, Duration.ofSeconds(30))
                    .getSingleOutput(String.class);
            final String[] parts = output.split("\\|", -1);
            assertEquals(2, parts.length);
            assertEquals("SubFlow:" + flowId + "-ParentStep-1-0", parts[0]);
            assertEquals("6", parts[1]);
        }
    }

    @Test
    void testSubFlowAllOfReturnsStableTerminalResults() throws Exception {
        try (DexDevTestEnvironment environment = DexDevTestEnvironment.start(
                cacheDirectory,
                SUB_FLOW_ALL_PARENT_WORKFLOW,
                WORKFLOW)) {
            final String flowId = flowId("sub-flow-all");
            environment.client().startFlow(SUB_FLOW_ALL_PARENT_WORKFLOW, flowId, 4);

            final String output = environment.client()
                    .waitForFlow(flowId, Duration.ofSeconds(30))
                    .getSingleOutput(String.class);
            final String[] results = output.split(";", -1);
            assertEquals(2, results.length);
            assertSubFlowResult(results[0], flowId, 0, "COMPLETED", "6");
            assertSubFlowResult(results[1], flowId, 1, "COMPLETED", "16");
        }
    }

    @Test
    void testSubFlowAnyOfReturnsRunningIdentityThatCanBeStopped() throws Exception {
        try (DexDevTestEnvironment environment = DexDevTestEnvironment.start(
                cacheDirectory,
                SUB_FLOW_ANY_PARENT_WORKFLOW,
                new TimerWorkflow())) {
            final String flowId = flowId("sub-flow-any");
            environment.client().startFlow(SUB_FLOW_ANY_PARENT_WORKFLOW, flowId, 300);

            final String output = environment.client()
                    .waitForFlow(flowId, Duration.ofSeconds(30))
                    .getSingleOutput(String.class);
            final String[] result = output.split("\\|", -1);
            assertEquals(4, result.length);
            assertEquals("SubFlow:" + flowId + "-ParentStep-1-0", result[0]);
            assertEquals("RUNNING", result[1]);
            assertEquals("false", result[2]);
            assertEquals("true", result[3]);

            environment.client().stopFlow(result[0]);
            assertEquals(
                    FlowStatus.CANCELED,
                    environment.client()
                            .waitForFlow(result[0], Duration.ofSeconds(30))
                            .getStatus());
        }
    }

    @Test
    void testSubFlowAttachKeepsRunningExecutionAcrossParentReset() throws Exception {
        assertRunningSubFlowReuseAcrossReset(SUB_FLOW_ATTACH_PARENT_WORKFLOW, false);
    }

    @Test
    void testSubFlowAlwaysRestartReplacesRunningExecutionAcrossParentReset() throws Exception {
        assertRunningSubFlowReuseAcrossReset(SUB_FLOW_ALWAYS_RESTART_PARENT_WORKFLOW, true);
    }

    @Test
    void testSubFlowDefaultReuseRestartsFailedExecutionAcrossParentReset() throws Exception {
        try (DexDevTestEnvironment environment = DexDevTestEnvironment.start(
                cacheDirectory,
                SUB_FLOW_ABNORMAL_PARENT_WORKFLOW,
                ABNORMAL_EXIT_WORKFLOW)) {
            final String flowId = flowId("sub-flow-abnormal");
            final String childFlowId = "SubFlow:" + flowId + "-ParentStep-1-0";
            environment.client().startFlow(SUB_FLOW_ABNORMAL_PARENT_WORKFLOW, flowId, 1);
            final String[] first = environment.client()
                    .waitForFlow(flowId, Duration.ofSeconds(30))
                    .getSingleOutput(String.class)
                    .split("\\|", -1);
            assertEquals("FAILED", first[1]);
            final String firstChildRunId = environment.client().describeFlow(childFlowId).getRunId();

            environment.client().timeTravel(
                    flowId,
                    TimeTravelOptions.newBuilder(TimeTravelType.BEGINNING)
                            .reason("verify SubFlow abnormal reuse")
                            .build());
            final String[] second = environment.client()
                    .waitForFlow(flowId, Duration.ofSeconds(30))
                    .getSingleOutput(String.class)
                    .split("\\|", -1);
            assertEquals("FAILED", second[1]);
            assertTrue(!firstChildRunId.equals(
                    environment.client().describeFlow(childFlowId).getRunId()));
        }
    }

    @Test
    void testSubFlowPartialResultsSurviveContinueAsNewWithoutRestart() throws Exception {
        // C3: server-zd never continues a run as new.
        IntegrationTestWaits.skipIfServerZD("Continue-As-New");
        try (DexDevTestEnvironment environment = DexDevTestEnvironment.start(
                cacheDirectory,
                SUB_FLOW_CAN_PARENT_WORKFLOW,
                WORKFLOW,
                new TimerWorkflow())) {
            final String flowId = flowId("sub-flow-can");
            final String completedChildId = "SubFlow:" + flowId + "-ParentStep-1-0";
            final String delayedChildId = "SubFlow:" + flowId + "-ParentStep-1-1";
            final String firstParentRunId = environment.client().startFlow(
                    SUB_FLOW_CAN_PARENT_WORKFLOW,
                    flowId,
                    4,
                    StartFlowOptions.newBuilder()
                            .configOverride(FlowConfig.newBuilder()
                                    .continueAsNewThreshold(1)
                                    .build())
                            .build());

            awaitFlowRun(environment.client(), flowId, firstParentRunId);
            final String completedChildRunId =
                    environment.client().describeFlow(completedChildId).getRunId();
            environment.client().skipTimer(
                    delayedChildId,
                    StepExecutionId.of("TimerStep"),
                    TimerId.byConditionId("test-timer-id"));

            final String[] output = environment.client()
                    .waitForFlow(flowId, Duration.ofSeconds(30))
                    .getSingleOutput(String.class)
                    .split("\\|", -1);
            assertEquals(4, output.length);
            assertEquals(completedChildId, output[0]);
            assertEquals("6", output[1]);
            assertEquals(delayedChildId, output[2]);
            assertEquals("COMPLETED", output[3]);
            assertEquals(
                    completedChildRunId,
                    environment.client().describeFlow(completedChildId).getRunId());
        }
    }

    @Test
    void testParallelBranchesReturnHeterogeneousStepCompletions() throws Exception {
        final MultiOutputWorkflow workflow = new MultiOutputWorkflow();
        try (DexDevTestEnvironment environment = DexDevTestEnvironment.start(
                cacheDirectory,
                workflow)) {
            final String flowId = flowId("multi-output");
            environment.client().startFlow(workflow, flowId, null);
            final FlowResult result =
                    environment.client().waitForFlow(flowId, Duration.ofSeconds(30));
            final Map<String, StepCompletion> completions =
                    new HashMap<String, StepCompletion>();
            for (final StepCompletion completion : result.getCompletions()) {
                completions.put(completion.getStepType(), completion);
                assertTrue(!completion.getStepExecutionId().isEmpty());
            }
            assertEquals(2, completions.size());
            assertEquals(
                    "branch-one",
                    completions.get(workflow.stringStep.getStepType()).getOutput(String.class));
            assertEquals(
                    Integer.valueOf(42),
                    completions.get(workflow.integerStep.getStepType()).getOutput(Integer.class));
        }
    }

    @Test
    void testBasicWorkflowAbnormalExitReuse() throws Exception {
        try (DexDevTestEnvironment environment = DexDevTestEnvironment.start(
                cacheDirectory,
                ABNORMAL_EXIT_WORKFLOW,
                WORKFLOW)) {
            final String flowId = flowId("abnormal-exit-reuse");
            final StartFlowOptions options = StartFlowOptions.newBuilder()
                    .idReusePolicy(IdReusePolicy.ALLOW_IF_PREVIOUS_FAILED)
                    .build();
            environment.client().startFlow(
                    ABNORMAL_EXIT_WORKFLOW,
                    flowId,
                    0,
                    options);
            final FlowResult failure =
                    environment.client().waitForFlow(flowId, Duration.ofSeconds(30));
            assertEquals(FlowStatus.FAILED, failure.getStatus());
            environment.client().startFlow(WORKFLOW, flowId, 0, options);
            assertEquals(2, environment.client().waitForFlow(flowId, Duration.ofSeconds(30)).getSingleOutput(Integer.class));
        }
    }

    @Test
    void testEmptyInputWorkflow() throws Exception {
        try (DexDevTestEnvironment environment = DexDevTestEnvironment.start(
                cacheDirectory,
                EMPTY_INPUT_WORKFLOW)) {
            final String flowId = flowId("empty-input");
            environment.client().startFlow(EMPTY_INPUT_WORKFLOW, flowId, null);
            assertTrue(environment.client()
                    .waitForFlow(flowId, Duration.ofSeconds(30))
                    .getCompletions()
                    .isEmpty());
            assertThrows(
                    FlowNotFoundException.class,
                    () -> environment.client().waitForFlow(flowId("missing"), Duration.ofSeconds(1)).getSingleOutput(Integer.class));
        }
    }

    @Test
    void testTypeSpecifiedWorkflow() throws Exception {
        try (DexDevTestEnvironment environment = DexDevTestEnvironment.start(
                cacheDirectory,
                EMPTY_INPUT_WORKFLOW)) {
            final String flowId = flowId("type-specified");
            assertEquals("test-customized-flow-type", EMPTY_INPUT_WORKFLOW.getFlowType());
            environment.client().startFlow(EMPTY_INPUT_WORKFLOW, flowId, null);
            assertTrue(environment.client()
                    .waitForFlow(flowId, Duration.ofSeconds(30))
                    .getCompletions()
                    .isEmpty());
            final BasicEmptyInputWorkflow unregistered = new BasicEmptyInputWorkflow();
            assertThrows(
                    FlowDefinitionException.class,
                    () -> environment.client().startFlow(
                            unregistered,
                            flowId("unregistered"),
                            null));
        }
    }

    @Test
    void testModelInputWorkflow() throws Exception {
        try (DexDevTestEnvironment environment = DexDevTestEnvironment.start(
                cacheDirectory,
                MODEL_INPUT_WORKFLOW)) {
            final String flowId = flowId("model-input");
            final BasicModelInputWorkflow.Input input = new BasicModelInputWorkflow.Input();
            input.value = 10;
            environment.client().startFlow(MODEL_INPUT_WORKFLOW, flowId, input);
            assertEquals(10, environment.client().waitForFlow(flowId, Duration.ofSeconds(30)).getSingleOutput(Integer.class));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> startWithWrongInput(environment.client(), flowId("wrong-input")));
        }
    }

    @Test
    void testWorkflowConfigOverride() throws Exception {
        try (DexDevTestEnvironment environment = DexDevTestEnvironment.start(
                cacheDirectory,
                WORKFLOW)) {
            final String flowId = flowId("config-override");
            final StartFlowOptions options = StartFlowOptions.newBuilder()
                    .configOverride(FlowConfig.newBuilder()
                            .continueAsNewThreshold(1)
                            .build())
                    .build();
            environment.client().startFlow(WORKFLOW, flowId, 0, options);
            assertEquals(2, environment.client().waitForFlow(flowId, Duration.ofSeconds(30)).getSingleOutput(Integer.class));
        }
    }

    @Test
    void testGetWorkflowStatusWhenNoExistingWorkflow() throws Exception {
        try (DexDevTestEnvironment environment = DexDevTestEnvironment.start(
                cacheDirectory,
                WORKFLOW)) {
            assertThrows(
                    FlowNotFoundException.class,
                    () -> environment.client().describeFlow(flowId("missing")));
        }
    }

    @Test
    void testGetWorkflowStatusWhenWorkflowIsRunning() throws Exception {
        final SignalWorkflow waitingWorkflow = new SignalWorkflow();
        try (DexDevTestEnvironment environment = DexDevTestEnvironment.start(
                cacheDirectory,
                waitingWorkflow)) {
            final String flowId = flowId("running");
            environment.client().startFlow(waitingWorkflow, flowId, 0);
            assertEquals(FlowStatus.RUNNING, environment.client()
                    .describeFlow(flowId)
                    .getStatus());
            environment.client().stopFlow(flowId);
        }
    }

    @Test
    void testWorkflowWaitForStepCompletion() throws Exception {
        try (DexDevTestEnvironment environment = DexDevTestEnvironment.start(
                cacheDirectory,
                WORKFLOW)) {
            final String flowId = flowId("wait-step");
            environment.client().startFlow(WORKFLOW, flowId, 5);
            environment.client().waitForStepCompletion(
                    flowId,
                    StepExecutionId.of("BasicSecondStep"),
                    stepWaitOptions(flowId, "first", Duration.ofSeconds(30)));
            assertEquals(7, environment.client().waitForFlow(flowId, Duration.ofSeconds(30)).getSingleOutput(Integer.class));
            assertThrows(
                    FlowNotActiveOrNotFoundException.class,
                    () -> environment.client().waitForStepCompletion(
                            flowId,
                            StepExecutionId.of("BasicSecondStep", 2),
                            stepWaitOptions(flowId, "second", Duration.ofSeconds(1))));
        }
    }

    @Test
    void testProceedOnWaitFailureWorkflow() throws Exception {
        // C21: server-zd rejects Step failure policies.
        IntegrationTestWaits.skipIfServerZD("Step failure policies");
        try (DexDevTestEnvironment environment = DexDevTestEnvironment.start(
                cacheDirectory,
                WAIT_FAILURE_WORKFLOW)) {
            final String flowId = flowId("proceed-on-wait-failure");
            environment.client().startFlow(WAIT_FAILURE_WORKFLOW, flowId, "input");
            assertEquals("input-recovered", environment.client().waitForFlow(flowId, Duration.ofSeconds(30)).getSingleOutput(String.class));
        }
    }

    @Test
    void testMixedWaitStyles() throws Exception {
        try (DexDevTestEnvironment environment = DexDevTestEnvironment.start(
                cacheDirectory,
                MIXED_WAIT_WORKFLOW)) {
            final String flowId = flowId("mixed-wait");
            environment.client().startFlow(MIXED_WAIT_WORKFLOW, flowId, 0);
            assertEquals(2, environment.client().waitForFlow(flowId, Duration.ofSeconds(30)).getSingleOutput(Integer.class));
        }
    }

    @Test
    void testMovementOptionsDoNotMutateStepDefaults() throws Exception {
        // C21: server-zd rejects Step failure policies.
        IntegrationTestWaits.skipIfServerZD("Step failure policies");
        try (DexDevTestEnvironment environment = DexDevTestEnvironment.start(
                cacheDirectory,
                IMMUTABLE_OPTIONS_WORKFLOW)) {
            final String flowId = flowId("immutable-options");
            environment.client().startFlow(IMMUTABLE_OPTIONS_WORKFLOW, flowId, 0);

            final FlowResult failure =
                    environment.client().waitForFlow(flowId, Duration.ofSeconds(30));
            assertEquals(FlowStatus.FAILED, failure.getStatus());
            assertEquals(FlowErrorType.WORKER_API_FAILED, failure.getErrorType());
            assertEquals("expected wait failure 2", failure.getErrorMessage());
        }
    }

    void compileBasicAndReuse(final Client client) {
        final StartFlowOptions options = StartFlowOptions.newBuilder()
                .timeout(Duration.ofSeconds(10))
                .idReusePolicy(IdReusePolicy.ALLOW_IF_NOT_RUNNING)
                .build();
        client.startFlow(WORKFLOW, "basic", 10, options);
        final Integer output = client.waitForFlow("basic").getSingleOutput(Integer.class);
        client.startFlow(ABNORMAL_EXIT_WORKFLOW, "abnormal", 10, options);
        client.startFlow(WORKFLOW, "abnormal", output, options);
    }

    void compileEmptyAndModelInputs(final Client client) {
        client.startFlow(EMPTY_INPUT_WORKFLOW, "empty", null);
        final BasicModelInputWorkflow.Input input = new BasicModelInputWorkflow.Input();
        input.value = 10;
        client.startFlow(MODEL_INPUT_WORKFLOW, "model", input);
    }

    void compileFailurePolicyAndConfigOverride(final Client client) {
        final FlowConfig config = FlowConfig.newBuilder()
                .activeStepSearchMode(ActiveStepSearchMode.ALL)
                .workerTarget(new WorkerTarget("worker:8803", false))
                .build();
        final StartFlowOptions options = StartFlowOptions.newBuilder()
                .configOverride(config)
                .build();
        client.startFlow(WAIT_FAILURE_WORKFLOW, "recover", "input", options);
        client.startFlow(MIXED_WAIT_WORKFLOW, "mixed", 0, options);
        client.updateFlowConfig("mixed", config);
    }

    void compileDescribeAndStepWait(final Client client) {
        final FlowInfo info = client.describeFlow("basic");
        client.waitForStepCompletion(
                "basic",
                StepExecutionId.of("BasicSecondStep"),
                stepWaitOptions("basic", "second", Duration.ofSeconds(5)));
        consume(info);
    }

    private static void consume(final Object value) {
    }

    private static WaitForStepCompletionOptions stepWaitOptions(
            final String flowId,
            final String suffix,
            final Duration requestTimeout) {
        return WaitForStepCompletionOptions.newBuilder()
                .requestId(flowId + "-wait-" + suffix)
                .requestTimeout(requestTimeout)
                .build();
    }

    private void assertRunningSubFlowReuseAcrossReset(
            final TimerSubFlowParentWorkflow parent,
            final boolean expectsRestart) throws Exception {
        try (DexDevTestEnvironment environment = DexDevTestEnvironment.start(
                cacheDirectory,
                parent,
                new TimerWorkflow())) {
            final String flowId = flowId("sub-flow-reuse");
            final String childFlowId = "SubFlow:" + flowId + "-ParentStep-1-0";
            environment.client().startFlow(parent, flowId, 300);
            final String firstChildRunId = awaitFlowRun(
                    environment.client(), childFlowId, null);

            environment.client().timeTravel(
                    flowId,
                    TimeTravelOptions.newBuilder(TimeTravelType.BEGINNING)
                            .reason("verify SubFlow running reuse")
                            .build());
            final String activeChildRunId = expectsRestart
                    ? awaitFlowRun(environment.client(), childFlowId, firstChildRunId)
                    : awaitFlowRun(environment.client(), childFlowId, null);
            if (expectsRestart) {
                assertTrue(!firstChildRunId.equals(activeChildRunId));
            } else {
                assertEquals(firstChildRunId, activeChildRunId);
            }

            IntegrationTestWaits.skipTimerWhenRegistered(
                    environment.client(),
                    childFlowId,
                    StepExecutionId.of("TimerStep"),
                    TimerId.byConditionId("test-timer-id"));
            final String[] output = environment.client()
                    .waitForFlow(flowId, Duration.ofSeconds(30))
                    .getSingleOutput(String.class)
                    .split("\\|", -1);
            assertEquals(childFlowId, output[0]);
            assertEquals("COMPLETED", output[1]);
        }
    }

    private static String awaitFlowRun(
            final Client client,
            final String flowId,
            final String excludedRunId) {
        final long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline) {
            try {
                final FlowInfo info = client.describeFlow(flowId);
                if (info.getStatus() == FlowStatus.RUNNING
                        && (excludedRunId == null || !excludedRunId.equals(info.getRunId()))) {
                    return info.getRunId();
                }
            } catch (FlowNotFoundException notStartedYet) {
                Thread.yield();
                continue;
            }
            Thread.yield();
        }
        throw new AssertionError("SubFlow did not reach the expected running execution: " + flowId);
    }

    private static void assertSubFlowResult(
            final String encoded,
            final String parentFlowId,
            final int index,
            final String status,
            final String output) {
        final String[] fields = encoded.split("\\|", -1);
        assertEquals(3, fields.length);
        assertEquals("SubFlow:" + parentFlowId + "-ParentStep-1-" + index, fields[0]);
        assertEquals(status, fields[1]);
        assertEquals(output, fields[2]);
    }

    private static String flowId(final String prefix) {
        return prefix + "-" + UUID.randomUUID();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static void startWithWrongInput(final Client client, final String flowId) {
        client.startFlow((io.superdurable.dex.Flow) MODEL_INPUT_WORKFLOW, flowId, "wrong");
    }
}
