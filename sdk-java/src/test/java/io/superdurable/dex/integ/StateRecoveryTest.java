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

import io.superdurable.dex.Client;
import io.superdurable.dex.testing.DexDevTestEnvironment;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("dex-dev")
public final class StateRecoveryTest {
    private static final StateRecoveryWorkflow WORKFLOW = new StateRecoveryWorkflow();
    private static final StateRecoveryNoWaitWorkflow NO_WAIT_WORKFLOW =
            new StateRecoveryNoWaitWorkflow();

    @TempDir
    Path cacheDirectory;

    @Test
    void testStateApiFailAndRecoveryWorkflow() throws Exception {
        // C21: server-zd rejects Step failure policies.
        IntegrationTestWaits.skipIfServerZD("Step failure policies");
        try (DexDevTestEnvironment environment = DexDevTestEnvironment.start(
                cacheDirectory,
                WORKFLOW)) {
            final String flowId = "state-recovery-" + UUID.randomUUID();
            environment.client().startFlow(WORKFLOW, flowId, 5);
            assertEquals(10, environment.client().waitForFlow(flowId, Duration.ofSeconds(30)).getSingleOutput(Integer.class));
        }
    }

    @Test
    void testStateApiFailAndRecoveryNoWaitUntilWorkflow() throws Exception {
        // C21: server-zd rejects Step failure policies.
        IntegrationTestWaits.skipIfServerZD("Step failure policies");
        try (DexDevTestEnvironment environment = DexDevTestEnvironment.start(
                cacheDirectory,
                NO_WAIT_WORKFLOW)) {
            final String flowId = "state-recovery-no-wait-" + UUID.randomUUID();
            environment.client().startFlow(NO_WAIT_WORKFLOW, flowId, 5);
            assertEquals(10, environment.client().waitForFlow(flowId, Duration.ofSeconds(30)).getSingleOutput(Integer.class));
        }
    }

    void compileWaitAndExecuteRecovery(final Client client) {
        client.startFlow(WORKFLOW, "state-recovery", 1);
        final Integer output = client.waitForFlow("state-recovery").getSingleOutput(Integer.class);
        consume(output);
    }

    void compileExecuteOnlyRecovery(final Client client) {
        client.startFlow(NO_WAIT_WORKFLOW, "state-recovery-no-wait", 1);
        final Integer output = client.waitForFlow("state-recovery-no-wait").getSingleOutput(Integer.class);
        consume(output);
    }

    private static void consume(final Object value) {
    }
}
