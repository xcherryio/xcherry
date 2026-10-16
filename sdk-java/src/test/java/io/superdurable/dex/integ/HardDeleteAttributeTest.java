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

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.superdurable.dex.AttributeMatch;
import io.superdurable.dex.StartFlowOptions;
import io.superdurable.dex.StepExecutionId;
import io.superdurable.dex.WaitForAttributeOptions;
import io.superdurable.dex.WaitForStepCompletionOptions;
import io.superdurable.dex.testing.DexDevTestEnvironment;
import io.superdurable.gen.FlowServiceGrpc;
import io.superdurable.gen.GetAttributesRequest;
import io.superdurable.gen.KV;
import io.superdurable.gen.Value;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("dex-dev")
public final class HardDeleteAttributeTest {
    private static final HardDeleteAttributeWorkflow WORKFLOW = new HardDeleteAttributeWorkflow();
    private static final String API_KEY = "hard-delete-api-key";
    private static final String TENANT_A_KEY =
            HardDeleteAttributeWorkflow.TENANT_CREDENTIALS_NAME + "/tenant-a";
    private static final String TENANT_B_KEY =
            HardDeleteAttributeWorkflow.TENANT_CREDENTIALS_NAME + "/tenant-b";

    @TempDir
    Path cacheDirectory;

    @Test
    void testHardDeleteAttributeLifecycle() throws Exception {
        final ManagedChannel channel = ManagedChannelBuilder
                .forTarget(System.getProperty("dex.test.serverAddress", "127.0.0.1:8801"))
                .usePlaintext()
                .build();
        try (DexDevTestEnvironment environment = DexDevTestEnvironment.start(
                cacheDirectory,
                WORKFLOW)) {
            final FlowServiceGrpc.FlowServiceBlockingStub flowService =
                    FlowServiceGrpc.newBlockingStub(channel);
            final String flowId = "hard-delete-attributes-" + UUID.randomUUID();
            environment.client().startFlow(
                    WORKFLOW,
                    flowId,
                    "start",
                    StartFlowOptions.newBuilder()
                            .addAttribute(WORKFLOW.apiKey, "initial-key")
                            .addAttribute(
                                    WORKFLOW.tenantCredentials,
                                    "tenant-a",
                                    new HardDeleteAttributeWorkflow.TenantCredential("tenant-a-token"))
                            .build());
            environment.client().waitForStepCompletion(
                    flowId,
                    StepExecutionId.of("HardDeleteAttributeRotateStep"),
                    WaitForStepCompletionOptions.newBuilder()
                            .requestId(flowId + "-wait-rotate")
                            .requestTimeout(Duration.ofSeconds(30))
                            .build());
            final HardDeleteAttributeWorkflow stub = environment.client().newRpcStub(
                    HardDeleteAttributeWorkflow.class,
                    flowId);

            HardDeleteAttributeWorkflow.HardDeleteAttributeSnapshot snapshot =
                    environment.client().invokeRPC(stub::getHardDeleteAttributes);
            assertEquals("start:initial-key|tenant-a-token", snapshot.stepObservation);
            assertEquals("start-rotated-key", snapshot.apiKey);
            assertEquals("tenant-a-token", snapshot.tenantA.token);
            assertNull(snapshot.tenantB);
            assertEquals(Collections.singletonList("tenant-a"), snapshot.tenantKeys);
            final Map<String, Value> stored = readAttributes(
                    flowService, flowId, API_KEY, TENANT_A_KEY, TENANT_B_KEY);
            assertEquals(Value.KindCase.STRING_VALUE, stored.get(API_KEY).getKindCase());
            assertEquals("start-rotated-key", stored.get(API_KEY).getStringValue());
            assertEquals(Value.KindCase.OBJ_VALUE, stored.get(TENANT_A_KEY).getKindCase());
            assertFalse(stored.containsKey(TENANT_B_KEY));

            assertThrows(
                    IllegalArgumentException.class,
                    () -> environment.client().waitForAttributeMatch(
                            flowId,
                            WORKFLOW.apiKey,
                            AttributeMatch.equalTo("start-rotated-key"),
                            WaitForAttributeOptions.newBuilder()
                                    .requestTimeout(Duration.ofSeconds(1))
                                    .build()));

            environment.client().invokeRPC(stub::updateHardDeleteAttributes, "rpc-key");
            snapshot = environment.client().invokeRPC(stub::getHardDeleteAttributes);
            assertEquals("rpc-key", snapshot.apiKey);
            assertEquals("rpc-key-tenant-b", snapshot.tenantB.token);
            assertEquals(Arrays.asList("tenant-a", "tenant-b"), snapshot.tenantKeys);

            environment.client().invokeRPC(stub::clearApiKeyWithNull);
            environment.client().invokeRPC(stub::deleteTenantB);
            snapshot = environment.client().invokeRPC(stub::getHardDeleteAttributes);
            assertNull(snapshot.apiKey);
            assertNull(snapshot.tenantB);
            assertEquals(Collections.singletonList("tenant-a"), snapshot.tenantKeys);
            final Map<String, Value> afterRpcDeletion = readAttributes(
                    flowService, flowId, API_KEY, TENANT_B_KEY);
            assertFalse(afterRpcDeletion.containsKey(API_KEY));
            assertFalse(afterRpcDeletion.containsKey(TENANT_B_KEY));

            environment.client().invokeRPC(stub::updateHardDeleteAttributes, "restored-key");
            environment.client().invokeRPC(stub::requestDeletion);
            environment.client().waitForStepCompletion(
                    flowId,
                    StepExecutionId.of("HardDeleteAttributeDeleteStep"),
                    WaitForStepCompletionOptions.newBuilder()
                            .requestId(flowId + "-wait-delete")
                            .requestTimeout(Duration.ofSeconds(30))
                            .build());
            snapshot = environment.client().invokeRPC(stub::getHardDeleteAttributes);
            assertEquals(
                    "delete:restored-key|restored-key-tenant-b|after:null|[]",
                    snapshot.stepObservation);
            assertNull(snapshot.apiKey);
            assertNull(snapshot.tenantA);
            assertNull(snapshot.tenantB);
            assertTrue(snapshot.tenantKeys.isEmpty());
            final Map<String, Value> afterStepDeletion = readAttributes(
                    flowService, flowId, API_KEY, TENANT_A_KEY, TENANT_B_KEY);
            assertTrue(afterStepDeletion.isEmpty(), afterStepDeletion.toString());

            environment.client().invokeRPC(stub::requestFinish);
            assertEquals(
                    "delete:restored-key|restored-key-tenant-b|after:null|[]",
                    environment.client()
                            .waitForFlow(flowId, Duration.ofSeconds(30))
                            .getSingleOutput(String.class));
        } finally {
            channel.shutdownNow();
            channel.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    private static Map<String, Value> readAttributes(
            final FlowServiceGrpc.FlowServiceBlockingStub flowService,
            final String flowId,
            final String... keys) {
        final Map<String, Value> attributes = new HashMap<String, Value>();
        for (KV attribute : flowService
                .withDeadlineAfter(30, TimeUnit.SECONDS)
                .getAttributes(GetAttributesRequest.newBuilder()
                        .setFlowId(flowId)
                        .addAllKeys(Arrays.asList(keys))
                        .build())
                .getAttributesList()) {
            attributes.put(attribute.getKey(), attribute.getValue());
        }
        return attributes;
    }
}
