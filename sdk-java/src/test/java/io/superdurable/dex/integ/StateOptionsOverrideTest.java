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
public final class StateOptionsOverrideTest {
    private static final StateOptionsOverrideWorkflow WORKFLOW =
            new StateOptionsOverrideWorkflow();

    @TempDir
    Path cacheDirectory;

    @Test
    void testStateOptionsOverrideWorkflow() throws Exception {
        IntegrationTestWaits.skipIfServerZD("Step failure policies");
        try (DexDevTestEnvironment environment = DexDevTestEnvironment.start(
                cacheDirectory,
                WORKFLOW)) {
            final String flowId = "state-options-override-" + UUID.randomUUID();
            environment.client().startFlow(WORKFLOW, flowId, "input");
            assertEquals(
                    "input_state1_start_state1_decide_state2_start_state2_decide",
                    environment.client().waitForFlow(flowId, Duration.ofSeconds(30)).getSingleOutput(String.class));
        }
    }

    void compileMovementOptionsOverride(final Client client) {
        client.startFlow(WORKFLOW, "options-override", "input");
        final String output = client.waitForFlow("options-override").getSingleOutput(String.class);
        consume(output);
    }

    private static void consume(final Object value) {
    }
}
