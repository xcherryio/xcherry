/*
 * Copyright (c) 2022-2026 Super Durable, Inc.
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

import io.superdurable.dex.products.moneytransfer.TransferRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(SharedIntegExtension.class)
public class MoneyTransferIntegTest {
    @Test
    void moneyTransferStart() {
        ServerZDSkips.skipWhenServerZD("failure policies");
        final IntegEnvironment environment = SharedIntegExtension.environment();
        final String flowId = environment.newFlowId("money-transfer");
        final TransferRequest input = new TransferRequest(
                "from-ci",
                "to-ci",
                42,
                "examples/java integration");

        final String runId = environment.client().startFlow(
                environment.moneyTransferFlow(),
                flowId,
                input,
                environment.startOptions());
        assertNotNull(runId);
        assertFalse(runId.isEmpty());

        final String output = environment.client().waitForFlow(flowId, Duration.ofSeconds(45)).getSingleOutput(String.class);
        assertTrue(output.contains("transfer is done"));
        assertTrue(output.contains("from-ci"));
        assertTrue(output.contains("to-ci"));
    }
}
