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
import io.superdurable.dex.FlowConfig;
import io.superdurable.dex.StartFlowOptions;
import io.superdurable.dex.testing.DexDevTestEnvironment;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("dex-dev")
public final class SkipWaitUntilTest {
    private static final SkipWaitUntilWorkflow WORKFLOW = new SkipWaitUntilWorkflow();
    private static final SkipWaitUntilMixedWaitWorkflow MIXED_WAIT_WORKFLOW =
            new SkipWaitUntilMixedWaitWorkflow();

    @TempDir
    Path cacheDirectory;

    @Test
    void testSkipWaitUntil() throws Exception {
        try (DexDevTestEnvironment environment = DexDevTestEnvironment.start(
                cacheDirectory,
                WORKFLOW)) {
            final String flowId = "skip-wait-until-" + UUID.randomUUID();
            final int input = 0;
            final StartFlowOptions options = StartFlowOptions.newBuilder()
                    .configOverride(FlowConfig.newBuilder()
                            .continueAsNewThreshold(1)
                            .build())
                    .build();
            environment.client().startFlow(WORKFLOW, flowId, input, options);
            assertEquals(input + 2, environment.client().waitForFlow(flowId, Duration.ofSeconds(30)).getSingleOutput(Integer.class));
        }
    }

    void compileExecuteOnlySteps(final Client client) {
        client.startFlow(WORKFLOW, "execute-only", 0);
        final Integer output = client.waitForFlow("execute-only").getSingleOutput(Integer.class);
        consume(output);
    }

    void compileMixedWaitStyles(final Client client) {
        client.startFlow(MIXED_WAIT_WORKFLOW, "mixed-wait", 0);
        final Integer output = client.waitForFlow("mixed-wait").getSingleOutput(Integer.class);
        consume(output);
    }

    private static void consume(final Object value) {
    }
}
