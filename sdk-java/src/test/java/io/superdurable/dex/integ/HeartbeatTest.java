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
import io.superdurable.dex.StreamMessage;
import io.superdurable.dex.testing.DexDevTestEnvironment;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("dex-dev")
public final class HeartbeatTest {
    @TempDir
    Path cacheDirectory;

    @Test
    void testRetryRestoresHeartbeatValue() throws Exception {
        assertHeartbeatRetry(HeartbeatTestWorkflow.Scenario.RESTORE_VALUE, "restored");
    }

    @Test
    void testNullHeartbeatAndStreamPreserveClearedDetails() throws Exception {
        assertHeartbeatRetry(HeartbeatTestWorkflow.Scenario.CLEAR_VALUE, "cleared");
    }

    @Test
    void testLocalHeartbeatIsIgnoredWhileStreamIsWritten() throws Exception {
        assertHeartbeatRetry(
                HeartbeatTestWorkflow.Scenario.LOCAL_IGNORES_VALUE,
                "local-ignored");
    }

    private void assertHeartbeatRetry(
            final HeartbeatTestWorkflow.Scenario scenario,
            final String expectedOutput) throws Exception {
        final HeartbeatTestWorkflow workflow = new HeartbeatTestWorkflow(
                scenario,
                DexDevTestEnvironment.isPullMode(),
                IntegrationTestWaits.isServerZD());
        try (DexDevTestEnvironment environment = DexDevTestEnvironment.start(
                cacheDirectory,
                workflow)) {
            final String flowId = "heartbeat-" + UUID.randomUUID();
            environment.client().startFlow(workflow, flowId, null);
            final FlowResult result = environment.client()
                    .waitForFlow(flowId, Duration.ofSeconds(30));
            assertEquals(expectedOutput, result.getSingleOutput(String.class));

            final StreamMessage<String> progress = environment.client().readStream(
                    flowId,
                    workflow.progress,
                    "",
                    Duration.ofSeconds(30));
            final String expectedProgress = scenario
                    == HeartbeatTestWorkflow.Scenario.LOCAL_IGNORES_VALUE
                    ? "local-attempt-1"
                    : "attempt-one";
            assertEquals(expectedProgress, progress.getValue());
            assertEquals("#HeartbeatStep-1", progress.getSource());
        }
    }
}
