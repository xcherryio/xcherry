/*
 * Copyright (c) 2026 Super Durable, Inc.
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

package io.superdurable.dex.integ;

import io.superdurable.dex.FlowResult;
import io.superdurable.dex.FlowStatus;
import io.superdurable.dex.StreamMessage;
import io.superdurable.dex.patterns.polling.JobState;
import io.superdurable.dex.patterns.polling.JobStatus;
import io.superdurable.dex.patterns.polling.PollingFlow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(SharedIntegExtension.class)
public class PollingIntegTest {
    @Test
    void pollingStreamsJobProgressAndCompletesWithJobResult() {
        ServerZDSkips.skipWhenServerZD("failure policies");
        final IntegEnvironment environment = SharedIntegExtension.environment();
        final String flowId = environment.newFlowId("pattern-polling");
        environment.client().startFlow(
                environment.pollingFlow(),
                flowId,
                null,
                environment.startOptions());

        final StreamMessage<JobStatus> queued = environment.client().readStream(
                flowId,
                PollingFlow.jobProgress,
                "",
                Duration.ofSeconds(20));
        assertEquals(JobState.QUEUED, queued.getValue().state());

        final StreamMessage<JobStatus> running = environment.client().readStream(
                flowId,
                PollingFlow.jobProgress,
                queued.getResumeToken(),
                Duration.ofSeconds(20));
        assertEquals(JobState.RUNNING, running.getValue().state());

        environment.fakeJobService().completeJob(flowId);

        final FlowResult result = environment.client().waitForFlow(flowId, Duration.ofSeconds(45));
        assertEquals(FlowStatus.COMPLETED, result.getStatus());
        assertEquals("artifact for " + flowId, result.getSingleOutput(String.class));
    }
}
