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

import io.superdurable.dex.StepExecutionId;
import io.superdurable.dex.TimerId;
import io.superdurable.dex.WaitForStepCompletionOptions;
import io.superdurable.dex.products.orderprocessing.OrderProcessingFlow;
import io.superdurable.dex.products.orderprocessing.OrderRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(SharedIntegExtension.class)
public class OrderProcessingIntegTest {
    @Test
    void orderProcessingHappyPath() {
        ServerZDSkips.skipWhenServerZD("failure policies");
        final IntegEnvironment environment = SharedIntegExtension.environment();
        final String flowId = environment.newFlowId("order-processing");
        final String runId = environment.client().startFlow(
                environment.orderProcessingFlow(),
                flowId,
                new OrderRequest(flowId, "buyer@example.com", "customer-1", 42, false),
                environment.startOptions());
        assertNotNull(runId);
        assertFalse(runId.isEmpty());
        environment.client().waitForStepCompletion(
                flowId,
                StepExecutionId.of("Charge"),
                waitOptions());
        assertEquals("ok", environment.client().invokeRPC(
                environment.client().newRpcStub(OrderProcessingFlow.class, flowId)::approve,
                ""));
        final String output = environment.client()
                .waitForFlow(flowId, Duration.ofSeconds(45))
                .getSingleOutput(String.class);
        assertEquals("shipped:" + flowId, output);
    }

    @Test
    void orderProcessingReminderThenShip() throws InterruptedException {
        ServerZDSkips.skipWhenServerZD("failure policies");
        final IntegEnvironment environment = SharedIntegExtension.environment();
        final String flowId = environment.newFlowId("order-processing-reminder");
        environment.client().startFlow(
                environment.orderProcessingFlow(),
                flowId,
                new OrderRequest(flowId, "buyer@example.com", "customer-1", 42, false),
                environment.startOptions());
        environment.client().waitForStepCompletion(
                flowId,
                StepExecutionId.of("Charge"),
                waitOptions());
        environment.awaitCondition(
                () -> {
                    try {
                        environment.client().skipTimer(
                                flowId,
                                StepExecutionId.of("Ship"),
                                TimerId.byConditionIndex(0));
                        return Boolean.TRUE;
                    } catch (final RuntimeException ignored) {
                        return Boolean.FALSE;
                    }
                },
                Boolean.TRUE::equals,
                Duration.ofSeconds(15),
                "skip timer did not succeed");
        environment.client().waitForStepCompletion(
                flowId,
                StepExecutionId.of("Ship"),
                waitOptions());
        assertEquals("ok", environment.client().invokeRPC(
                environment.client().newRpcStub(OrderProcessingFlow.class, flowId)::approve,
                ""));
        final String output = environment.client()
                .waitForFlow(flowId, Duration.ofSeconds(45))
                .getSingleOutput(String.class);
        assertEquals("shipped:" + flowId, output);
    }

    @Test
    void orderProcessingShipFailureRefunds() {
        ServerZDSkips.skipWhenServerZD("failure policies");
        final IntegEnvironment environment = SharedIntegExtension.environment();
        final String flowId = environment.newFlowId("order-processing-refund");
        environment.client().startFlow(
                environment.orderProcessingFlow(),
                flowId,
                new OrderRequest(flowId, "buyer@example.com", "customer-1", 42, true),
                environment.startOptions());
        environment.client().waitForStepCompletion(
                flowId,
                StepExecutionId.of("Charge"),
                waitOptions());
        assertEquals("ok", environment.client().invokeRPC(
                environment.client().newRpcStub(OrderProcessingFlow.class, flowId)::approve,
                ""));
        final String output = environment.client()
                .waitForFlow(flowId, Duration.ofSeconds(45))
                .getSingleOutput(String.class);
        assertEquals("refunded:" + flowId, output);
    }

    private static WaitForStepCompletionOptions waitOptions() {
        return WaitForStepCompletionOptions.newBuilder()
                .requestTimeout(Duration.ofSeconds(30))
                .build();
    }
}
