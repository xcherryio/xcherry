/*
 * Copyright (c) 2026 Super Durable, Inc.
 *
 * Licensed under the Sustainable Use License 1.0.
 * You may not use this file except in compliance with the License.
 * See the LICENSE file in the repository root.
 *
 * SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0
 */

package io.superdurable.dex;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import io.superdurable.gen.AttributeWrite;
import io.superdurable.gen.FlowServiceGrpc;
import io.superdurable.gen.FlowStartOptions;
import io.superdurable.gen.InvokeExecuteMethodOutput;
import io.superdurable.gen.InvokeExecuteMethodRequest;
import io.superdurable.gen.InvokeExecuteMethodResponse;
import io.superdurable.gen.InvokeWaitForMethodRequest;
import io.superdurable.gen.InvokeWaitForMethodResponse;
import io.superdurable.gen.InvokeWorkerRPCRequest;
import io.superdurable.gen.InvokeWorkerRPCResponse;
import io.superdurable.gen.ServerInfo;
import io.superdurable.gen.StartFlowRequest;
import io.superdurable.gen.StartFlowResponse;
import io.superdurable.gen.SyncAttributeIndexRequest;
import io.superdurable.gen.SyncAttributeIndexResponse;
import io.superdurable.gen.Value;
import io.superdurable.gen.WaitForAttributeRequest;
import io.superdurable.gen.WaitForAttributeResponse;
import io.superdurable.gen.WorkerErrorResponse;
import io.superdurable.gen.WorkerServiceGrpc;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class HardDeleteAttributeMappingTest {
    private static final ManagedChannel UNUSED_CHANNEL =
            ManagedChannelBuilder.forTarget("127.0.0.1:1").usePlaintext().build();
    private static final Attribute<String> SECRET =
            Attribute.define("secret", String.class).hardDeleteOnNull();
    private static final AttributeMap<String> TENANT_SECRETS =
            AttributeMap.define("tenant-secrets", String.class).hardDeleteOnNull();
    private static final Attribute<Object> FLEXIBLE_SECRET =
            Attribute.define("flexible-secret", Object.class).hardDeleteOnNull();
    private static final Attribute<String> PLAIN = Attribute.define("plain", String.class);
    private static final Attribute<String> CHILD_SECRET =
            Attribute.define("child-secret", String.class).hardDeleteOnNull();
    private static final Attribute<String> CHILD_PLAIN =
            Attribute.define("child-plain", String.class);

    @Test
    void rejectsUnsupportedDefinitionsAtDefinitionTime() {
        final List<Class<?>> unsupportedTypes = Arrays.<Class<?>>asList(
                Boolean.class, boolean.class, Byte.class, byte.class, Short.class, short.class,
                Integer.class, int.class, Long.class, long.class, Float.class, float.class,
                Double.class, double.class);
        for (Class<?> valueType : unsupportedTypes) {
            assertDefinitionFailure(
                    "supports only string and object values",
                    () -> Attribute.define("numeric", valueType).hardDeleteOnNull());
            assertDefinitionFailure(
                    "supports only string and object values",
                    () -> AttributeMap.define("numeric", valueType).hardDeleteOnNull());
        }
        final AttributeIndex index = new AttributeIndex(AttributeIndex.Type.KEYWORD);
        assertDefinitionFailure(
                "cannot have an index",
                () -> Attribute.define("indexed", String.class, index).hardDeleteOnNull());
        assertDefinitionFailure(
                "cannot have an index",
                () -> AttributeMap.define("indexed", String.class, index).hardDeleteOnNull());
        assertDefinitionFailure(
                "cannot use syncToAttributeStore",
                () -> Attribute.define("synced", String.class).syncToAttributeStore().hardDeleteOnNull());
        assertDefinitionFailure(
                "cannot use syncToAttributeStore",
                () -> Attribute.define("synced", String.class).hardDeleteOnNull().syncToAttributeStore());
        assertDefinitionFailure(
                "cannot use syncToAttributeStore",
                () -> AttributeMap.define("synced", String.class).syncToAttributeStore().hardDeleteOnNull());
        assertDefinitionFailure(
                "cannot use syncToAttributeStore",
                () -> AttributeMap.define("synced", String.class).hardDeleteOnNull().syncToAttributeStore());

        assertTrue(Attribute.define("bytes", byte[].class).hardDeleteOnNull().isHardDeleteOnNull());
        assertTrue(Attribute.define("object", Object.class).hardDeleteOnNull().isHardDeleteOnNull());
        assertTrue(Attribute.define("number", Number.class).hardDeleteOnNull().isHardDeleteOnNull());
        assertTrue(Attribute.define("model", SecretModel.class).hardDeleteOnNull().isHardDeleteOnNull());
        assertTrue(SECRET.isHardDeleteOnNull());
        assertTrue(TENANT_SECRETS.isHardDeleteOnNull());
        assertFalse(SECRET.isSyncToAttributeStore());
        assertFalse(PLAIN.isHardDeleteOnNull());
        assertFalse(AttributeMap.define("plain-map", String.class).isHardDeleteOnNull());
    }

    @Test
    void marksStartFlowInitialAttributesAndRejectsScalarValues() {
        final Client client = new Client(
                new Registry(Collections.<Flow<?>>emptyList()),
                new EmptyBlobCache());
        try {
            final FlowStartOptions start = client.mapStartOptions(StartFlowOptions.newBuilder()
                    .addAttribute(PLAIN, "plain")
                    .addAttribute(SECRET, "secret")
                    .addAttribute(TENANT_SECRETS, "tenant-a", "tenant-secret")
                    .addAttribute(TENANT_SECRETS, "tenant-b", null)
                    .addAttribute(FLEXIBLE_SECRET, Collections.singletonMap("token", "value"))
                    .build());
            assertFalse(start.getAttributes(0).getIsHardDeleteOnNull());
            assertTrue(start.getAttributes(1).getIsHardDeleteOnNull());
            assertEquals("secret", start.getAttributes(1).getValue().getStringValue());
            assertTrue(start.getAttributes(2).getIsHardDeleteOnNull());
            assertEquals("tenant-secrets/tenant-a", start.getAttributes(2).getKey());
            assertTrue(start.getAttributes(3).getIsHardDeleteOnNull());
            assertEquals(Value.KindCase.NULL_VALUE, start.getAttributes(3).getValue().getKindCase());
            assertTrue(start.getAttributes(4).getIsHardDeleteOnNull());
            assertEquals(Value.KindCase.OBJ_VALUE, start.getAttributes(4).getValue().getKindCase());
            for (Object scalar : Arrays.<Object>asList(5, 5L, 1.5d, 1.5f, true)) {
                final IllegalArgumentException failure = assertThrows(
                        IllegalArgumentException.class,
                        () -> client.mapStartOptions(StartFlowOptions.newBuilder()
                                .addAttribute(FLEXIBLE_SECRET, scalar)
                                .build()));
                assertTrue(failure.getMessage().contains("flexible-secret"));
            }
        } finally {
            client.close();
        }
    }

    @Test
    void marksStepRpcAndSubFlowWritesWhenServerSupportsProtocolThree() throws Throwable {
        final Registry registry = new Registry(Arrays.<Flow<?>>asList(
                new HardDeleteMappingFlow(), new HardDeleteMappingChildFlow()));
        final ValueMapper values = new ValueMapper(new ObjectMapper());
        final NegotiatedServerProtocol protocol = new NegotiatedServerProtocol();
        protocol.recordNegotiatedVersion(3);
        final WorkerDispatcher dispatcher = new WorkerDispatcher(
                registry, values, unusedHydrator(), protocol);

        final Map<String, AttributeWrite> stepWrites = writesByKey(dispatcher.invokeExecute(
                executeRequest(values, "write"), null).getUpsertAttributesList());
        assertTrue(stepWrites.get("secret").getIsHardDeleteOnNull());
        assertEquals("step-secret", stepWrites.get("secret").getValue().getStringValue());
        assertTrue(stepWrites.get("tenant-secrets/tenant-a").getIsHardDeleteOnNull());
        assertEquals(
                Value.KindCase.NULL_VALUE,
                stepWrites.get("tenant-secrets/tenant-a").getValue().getKindCase());
        assertTrue(stepWrites.get("tenant-secrets/tenant-b").getIsHardDeleteOnNull());
        assertEquals(
                Value.KindCase.NULL_VALUE,
                stepWrites.get("tenant-secrets/tenant-b").getValue().getKindCase());
        assertFalse(stepWrites.get("plain").getIsHardDeleteOnNull());

        final Map<String, AttributeWrite> deleteWrites = writesByKey(dispatcher.invokeExecute(
                executeRequest(values, "delete"), null).getUpsertAttributesList());
        assertTrue(deleteWrites.get("secret").getIsHardDeleteOnNull());
        assertEquals(
                Value.KindCase.NULL_VALUE,
                deleteWrites.get("secret").getValue().getKindCase());

        final AttributeWrite rpcSet = dispatcher.invokeRpc(rpcRequest(values, "rotateSecret", "rpc-secret"))
                .getUpsertAttributes(0);
        assertTrue(rpcSet.getIsHardDeleteOnNull());
        assertEquals("rpc-secret", rpcSet.getValue().getStringValue());
        final AttributeWrite rpcDelete = dispatcher.invokeRpc(rpcRequest(values, "deleteSecret", null))
                .getUpsertAttributes(0);
        assertTrue(rpcDelete.getIsHardDeleteOnNull());
        assertEquals(Value.KindCase.NULL_VALUE, rpcDelete.getValue().getKindCase());

        final InvokeWaitForMethodResponse subFlow = dispatcher.invokeWaitFor(
                subFlowRequest(values), null);
        final Map<String, AttributeWrite> subFlowWrites = writesByKey(subFlow
                .getWaitingCondition()
                .getSubFlowConditions(0)
                .getOptions()
                .getAttributesList());
        assertTrue(subFlowWrites.get("child-secret").getIsHardDeleteOnNull());
        assertEquals("child-secret-value", subFlowWrites.get("child-secret").getValue().getStringValue());
        assertFalse(subFlowWrites.get("child-plain").getIsHardDeleteOnNull());

        final Map<String, AttributeWrite> objectWrites = writesByKey(dispatcher.invokeExecute(
                executeRequest(values, "object"), null).getUpsertAttributesList());
        assertTrue(objectWrites.get("flexible-secret").getIsHardDeleteOnNull());
        assertEquals(
                Value.KindCase.OBJ_VALUE,
                objectWrites.get("flexible-secret").getValue().getKindCase());
        for (String scalarInput : Arrays.asList("integer", "double", "boolean")) {
            final IllegalArgumentException failure = assertThrows(
                    IllegalArgumentException.class,
                    () -> dispatcher.invokeExecute(executeRequest(values, scalarInput), null));
            assertTrue(failure.getMessage().contains("supports only string and object values"));
        }
    }

    @Test
    void rejectsWorkerHardDeleteWritesBelowProtocolThree() {
        final Registry registry = new Registry(Arrays.<Flow<?>>asList(
                new HardDeleteMappingFlow(), new HardDeleteMappingChildFlow()));
        final ValueMapper values = new ValueMapper(new ObjectMapper());
        final NegotiatedServerProtocol notNegotiated = new NegotiatedServerProtocol();
        final NegotiatedServerProtocol protocolTwo = new NegotiatedServerProtocol();
        protocolTwo.recordNegotiatedVersion(2);
        for (NegotiatedServerProtocol protocol : Arrays.asList(notNegotiated, protocolTwo)) {
            final WorkerDispatcher dispatcher = new WorkerDispatcher(
                    registry, values, unusedHydrator(), protocol);
            assertProtocolFailure(() -> dispatcher.invokeExecute(executeRequest(values, "write"), null));
            assertProtocolFailure(() -> dispatcher.invokeExecute(executeRequest(values, "delete"), null));
            assertProtocolFailure(() -> dispatcher.invokeRpc(rpcRequest(values, "rotateSecret", "value")));
            assertProtocolFailure(() -> dispatcher.invokeRpc(rpcRequest(values, "deleteSecret", null)));
            assertProtocolFailure(() -> dispatcher.invokeWaitFor(subFlowRequest(values), null));
            final InvokeExecuteMethodResponse plain = assertDoesNotFail(
                    () -> dispatcher.invokeExecute(executeRequest(values, "plain"), null));
            assertFalse(plain.getUpsertAttributes(0).getIsHardDeleteOnNull());
        }
    }

    @Test
    void workerUsesTheProtocolNegotiatedAtStartup() throws Exception {
        try (FakeFlowService protocolTwo = new FakeFlowService(2);
                RunningWorker worker = new RunningWorker(protocolTwo)) {
            final StatusRuntimeException failure = assertThrows(
                    StatusRuntimeException.class,
                    () -> worker.invokeExecute("write"));
            assertEquals(Status.Code.INTERNAL, failure.getStatus().getCode());
            final WorkerErrorResponse details = StatusProto.fromThrowable(failure)
                    .getDetails(0)
                    .unpack(WorkerErrorResponse.class);
            assertEquals(IllegalStateException.class.getName(), details.getErrorType());
            assertTrue(details.getDetail().contains("requires Server protocol version 3"),
                    details.getDetail());
            assertFalse(worker.invokeExecute("plain").getUpsertAttributes(0).getIsHardDeleteOnNull());
        }
        try (FakeFlowService protocolThree = new FakeFlowService(3);
                RunningWorker worker = new RunningWorker(protocolThree)) {
            final Map<String, AttributeWrite> writes =
                    writesByKey(worker.invokeExecute("write").getUpsertAttributesList());
            assertTrue(writes.get("secret").getIsHardDeleteOnNull());
        }
    }

    @Test
    void clientNegotiatesLazilyBeforeHardDeleteStartFlow() throws Exception {
        final HardDeleteMappingFlow flow = new HardDeleteMappingFlow();
        final Registry registry = new Registry(Arrays.<Flow<?>>asList(
                flow, new HardDeleteMappingChildFlow()));
        try (FakeFlowService protocolTwo = new FakeFlowService(2)) {
            final Client client = protocolTwo.newClient(registry);
            try {
                client.startFlow(flow, "plain-start", "plain", StartFlowOptions.newBuilder()
                        .addAttribute(PLAIN, "plain")
                        .build());
                assertEquals(0, protocolTwo.serverInfoCalls.get());
                assertEquals(1, protocolTwo.startRequests.size());
                for (int attempt = 0; attempt < 2; attempt++) {
                    final IllegalStateException failure = assertThrows(
                            IllegalStateException.class,
                            () -> client.startFlow(flow, "hard-start", "plain",
                                    StartFlowOptions.newBuilder()
                                            .addAttribute(TENANT_SECRETS, "tenant-a", "secret")
                                            .build()));
                    assertTrue(failure.getMessage().contains("requires Server protocol version 3"),
                            failure.getMessage());
                    assertTrue(failure.getMessage().contains("is 2"), failure.getMessage());
                }
                assertEquals(1, protocolTwo.serverInfoCalls.get());
                assertEquals(1, protocolTwo.startRequests.size());
            } finally {
                client.close();
            }
        }
        try (FakeFlowService protocolThree = new FakeFlowService(3)) {
            final Client client = protocolThree.newClient(registry);
            try {
                for (int attempt = 0; attempt < 2; attempt++) {
                    client.startFlow(flow, "hard-start-" + attempt, "plain",
                            StartFlowOptions.newBuilder()
                                    .addAttribute(SECRET, "secret")
                                    .addAttribute(PLAIN, "plain")
                                    .build());
                }
                assertEquals(1, protocolThree.serverInfoCalls.get());
                assertEquals(2, protocolThree.startRequests.size());
                final Map<String, AttributeWrite> writes = writesByKey(protocolThree.startRequests
                        .get(1)
                        .getFlowStartOptions()
                        .getAttributesList());
                assertTrue(writes.get("secret").getIsHardDeleteOnNull());
                assertFalse(writes.get("plain").getIsHardDeleteOnNull());
            } finally {
                client.close();
            }
        }
    }

    @Test
    void clientRejectsHardDeleteMismatchesAndWaitsBeforeCallingServer() throws Exception {
        final HardDeleteMappingFlow flow = new HardDeleteMappingFlow();
        final Registry registry = new Registry(Arrays.<Flow<?>>asList(
                flow, new HardDeleteMappingChildFlow()));
        try (FakeFlowService protocolThree = new FakeFlowService(3)) {
            final Client client = protocolThree.newClient(registry);
            try {
                final IllegalArgumentException mismatch = assertThrows(
                        IllegalArgumentException.class,
                        () -> client.startFlow(flow, "mismatch", "plain",
                                StartFlowOptions.newBuilder()
                                        .addAttribute(Attribute.define("secret", String.class), "leak")
                                        .build()));
                assertTrue(mismatch.getMessage().contains("hardDeleteOnNull"), mismatch.getMessage());
                final IllegalArgumentException unexpected = assertThrows(
                        IllegalArgumentException.class,
                        () -> client.startFlow(flow, "mismatch", "plain",
                                StartFlowOptions.newBuilder()
                                        .addAttribute(
                                                Attribute.define("plain", String.class).hardDeleteOnNull(),
                                                "value")
                                        .build()));
                assertTrue(unexpected.getMessage().contains("hardDeleteOnNull"), unexpected.getMessage());
                assertEquals(0, protocolThree.startRequests.size());

                final WaitForAttributeOptions options = WaitForAttributeOptions.newBuilder().build();
                final IllegalArgumentException singletonWait = assertThrows(
                        IllegalArgumentException.class,
                        () -> client.waitForAttributeMatch(
                                "flow", SECRET, AttributeMatch.equalTo("secret"), options));
                assertTrue(singletonWait.getMessage().contains("hard-delete Attribute secret"));
                final IllegalArgumentException mapWait = assertThrows(
                        IllegalArgumentException.class,
                        () -> client.waitForAttributeMatch(
                                "flow", TENANT_SECRETS, "tenant-a", AttributeMatch.equalTo("secret"),
                                options));
                assertTrue(mapWait.getMessage().contains("hard-delete Attribute tenant-secrets"));
                assertEquals(0, protocolThree.waitForAttributeCalls.get());
                assertEquals(0, protocolThree.serverInfoCalls.get());
            } finally {
                client.close();
            }
        }
    }

    private static void assertDefinitionFailure(final String expectedMessage, final Executable definition) {
        final IllegalStateException failure = assertThrows(IllegalStateException.class, definition);
        assertTrue(failure.getMessage().contains(expectedMessage), failure.getMessage());
    }

    private static void assertProtocolFailure(final Executable invocation) {
        final IllegalStateException failure = assertThrows(IllegalStateException.class, invocation);
        assertTrue(failure.getMessage().contains("requires Server protocol version 3"),
                failure.getMessage());
    }

    private static <T> T assertDoesNotFail(final ThrowingSupplier<T> supplier) {
        try {
            return supplier.get();
        } catch (Throwable failure) {
            throw new AssertionError("unexpected failure", failure);
        }
    }

    private interface ThrowingSupplier<T> {
        T get() throws Throwable;
    }

    private static Map<String, AttributeWrite> writesByKey(final List<AttributeWrite> writes) {
        final Map<String, AttributeWrite> byKey = new HashMap<String, AttributeWrite>();
        for (AttributeWrite write : writes) {
            byKey.put(write.getKey(), write);
        }
        return byKey;
    }

    private static io.superdurable.gen.Context context() {
        return io.superdurable.gen.Context.newBuilder()
                .setFlowId("flow-1")
                .setRunId("run-1")
                .setFlowStartedTimestamp(1)
                .setStepExecutionId("step-1")
                .setFirstAttemptTimestamp(1)
                .setAttempt(1)
                .build();
    }

    private static InvokeExecuteMethodRequest executeRequest(
            final ValueMapper values,
            final String input) {
        return InvokeExecuteMethodRequest.newBuilder()
                .setContext(context())
                .setFlowType("HardDeleteMappingFlow")
                .setStepType("HardDeleteMappingWriteStep")
                .setStepInput(values.encode(input))
                .build();
    }

    private static InvokeWorkerRPCRequest rpcRequest(
            final ValueMapper values,
            final String rpcName,
            final String input) {
        return InvokeWorkerRPCRequest.newBuilder()
                .setContext(context())
                .setFlowType("HardDeleteMappingFlow")
                .setRpcName(rpcName)
                .setInput(values.encode(input))
                .build();
    }

    private static InvokeWaitForMethodRequest subFlowRequest(final ValueMapper values) {
        return InvokeWaitForMethodRequest.newBuilder()
                .setContext(context())
                .setFlowType("HardDeleteMappingFlow")
                .setStepType("HardDeleteMappingSubFlowStep")
                .setStepInput(values.encode("child-input"))
                .build();
    }

    private static ValueHydrator unusedHydrator() {
        return new ValueHydrator(FlowServiceGrpc.newBlockingStub(UNUSED_CHANNEL), new EmptyBlobCache());
    }

    private static int availablePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    static final class SecretModel {
        public String token;
    }

    @AfterAll
    static void closeUnusedChannel() throws InterruptedException {
        UNUSED_CHANNEL.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }

    static class HardDeleteMappingFlow implements Flow<String> {
        @Override
        public StepList<String> getSteps() {
            return StepList.startStep(new HardDeleteMappingWriteStep())
                    .otherSteps(new HardDeleteMappingSubFlowStep());
        }

        @Override
        public PersistenceSchema getPersistenceSchema() {
            return PersistenceSchema.of(SECRET, TENANT_SECRETS, FLEXIBLE_SECRET, PLAIN);
        }

        @RPC
        public void rotateSecret(final Context context, final String value) {
            SECRET.set(context, value);
        }

        @RPC
        public void deleteSecret(final Context context) {
            SECRET.delete(context);
        }
    }

    private static final class HardDeleteMappingWriteStep implements Step<String> {
        @Override
        public Class<String> getInputType() {
            return String.class;
        }

        @Override
        public StepDecision execute(final Context context, final String input) {
            switch (input) {
                case "write":
                    SECRET.set(context, "step-secret");
                    TENANT_SECRETS.delete(context, "tenant-a");
                    TENANT_SECRETS.set(context, "tenant-b", null);
                    PLAIN.set(context, "plain");
                    break;
                case "delete":
                    SECRET.delete(context);
                    break;
                case "object":
                    FLEXIBLE_SECRET.set(context, Collections.singletonMap("token", "value"));
                    break;
                case "integer":
                    FLEXIBLE_SECRET.set(context, 5);
                    break;
                case "double":
                    FLEXIBLE_SECRET.set(context, 1.5d);
                    break;
                case "boolean":
                    FLEXIBLE_SECRET.set(context, Boolean.TRUE);
                    break;
                default:
                    PLAIN.set(context, input);
                    break;
            }
            return StepDecision.gracefulComplete();
        }
    }

    private static final class HardDeleteMappingSubFlowStep implements Step<String> {
        @Override
        public Class<String> getInputType() {
            return String.class;
        }

        @Override
        public Wait waitFor(final Context context, final String input) {
            return Wait.until(SubFlow.run(
                    HardDeleteMappingChildFlow.class,
                    input,
                    SubFlowOptions.newBuilder()
                            .addAttribute(CHILD_SECRET, "child-secret-value")
                            .addAttribute(CHILD_PLAIN, "child-plain-value")
                            .build()));
        }

        @Override
        public StepDecision execute(final Context context, final String input) {
            return StepDecision.gracefulComplete();
        }
    }

    private static final class HardDeleteMappingChildFlow implements Flow<String> {
        @Override
        public StepList<String> getSteps() {
            return StepList.startStep(new HardDeleteMappingChildStep());
        }

        @Override
        public PersistenceSchema getPersistenceSchema() {
            return PersistenceSchema.of(CHILD_SECRET, CHILD_PLAIN);
        }
    }

    private static final class HardDeleteMappingChildStep implements Step<String> {
        @Override
        public Class<String> getInputType() {
            return String.class;
        }

        @Override
        public StepDecision execute(final Context context, final String input) {
            return StepDecision.gracefulComplete();
        }
    }

    private static final class FakeFlowService extends FlowServiceGrpc.FlowServiceImplBase
            implements AutoCloseable {
        private final int currentProtocolVersion;
        private final Server server;
        private final AtomicInteger serverInfoCalls = new AtomicInteger();
        private final AtomicInteger waitForAttributeCalls = new AtomicInteger();
        private final List<StartFlowRequest> startRequests =
                new CopyOnWriteArrayList<StartFlowRequest>();

        private FakeFlowService(final int currentProtocolVersion) throws IOException {
            this.currentProtocolVersion = currentProtocolVersion;
            this.server = ServerBuilder.forPort(0).addService(this).build().start();
        }

        String getAddress() {
            return "127.0.0.1:" + server.getPort();
        }

        Client newClient(final Registry registry) {
            return new Client(registry, new EmptyBlobCache(), new ClientOptions(getAddress()));
        }

        @Override
        public void getServerInfo(
                final Empty request,
                final StreamObserver<ServerInfo> observer) {
            serverInfoCalls.incrementAndGet();
            observer.onNext(ServerInfo.newBuilder()
                    .setServerVersion("test")
                    .setMinimumSupportedProtocolVersion(1)
                    .setCurrentProtocolVersion(currentProtocolVersion)
                    .build());
            observer.onCompleted();
        }

        @Override
        public void syncAttributeIndexes(
                final SyncAttributeIndexRequest request,
                final StreamObserver<SyncAttributeIndexResponse> observer) {
            observer.onNext(SyncAttributeIndexResponse.getDefaultInstance());
            observer.onCompleted();
        }

        @Override
        public void startFlow(
                final StartFlowRequest request,
                final StreamObserver<StartFlowResponse> observer) {
            startRequests.add(request);
            observer.onNext(StartFlowResponse.newBuilder().setRunId("run-" + startRequests.size()).build());
            observer.onCompleted();
        }

        @Override
        public void waitForAttribute(
                final WaitForAttributeRequest request,
                final StreamObserver<WaitForAttributeResponse> observer) {
            waitForAttributeCalls.incrementAndGet();
            observer.onError(Status.UNIMPLEMENTED.asRuntimeException());
        }

        @Override
        public void close() throws InterruptedException {
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private static final class RunningWorker implements AutoCloseable {
        private final Worker worker;
        private final Thread workerThread;
        private final AtomicReference<Throwable> workerFailure = new AtomicReference<Throwable>();
        private final ManagedChannel channel;
        private final WorkerServiceGrpc.WorkerServiceBlockingStub stub;
        private final ValueMapper values = new ValueMapper(new ObjectMapper());

        private RunningWorker(final FakeFlowService flowService) throws IOException {
            final int port = availablePort();
            this.worker = new Worker(
                    new Registry(Arrays.<Flow<?>>asList(
                            new HardDeleteMappingFlow(), new HardDeleteMappingChildFlow())),
                    new EmptyBlobCache(),
                    WorkerOptions.newBuilder()
                            .bindAddress("127.0.0.1:" + port)
                            .serverAddress(flowService.getAddress())
                            .build());
            this.workerThread = new Thread(() -> {
                try {
                    worker.start();
                } catch (Throwable failure) {
                    workerFailure.set(failure);
                }
            }, "hard-delete-test-worker");
            workerThread.start();
            this.channel = ManagedChannelBuilder.forAddress("127.0.0.1", port)
                    .usePlaintext()
                    .build();
            this.stub = WorkerServiceGrpc.newBlockingStub(channel).withWaitForReady();
        }

        InvokeExecuteMethodResponse invokeExecute(final String input) {
            final List<InvokeExecuteMethodOutput> outputs = new ArrayList<InvokeExecuteMethodOutput>();
            final Iterator<InvokeExecuteMethodOutput> iterator = stub
                    .withDeadlineAfter(10, TimeUnit.SECONDS)
                    .invokeExecuteMethod(executeRequest(values, input));
            while (iterator.hasNext()) {
                outputs.add(iterator.next());
            }
            assertTrue(outputs.get(outputs.size() - 1).hasResult());
            return outputs.get(outputs.size() - 1).getResult();
        }

        @Override
        public void close() throws InterruptedException {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            worker.close();
            workerThread.join(5_000L);
            assertNull(workerFailure.get());
        }
    }

    private static final class EmptyBlobCache implements BlobCache {
        @Override
        public Optional<byte[]> get(final String blobId) {
            return Optional.empty();
        }

        @Override
        public boolean put(final String blobId, final byte[] payload) {
            return false;
        }

        @Override
        public void delete(final String blobId) {
        }

        @Override
        public void deleteAll() {
        }

        @Override
        public void close() {
        }
    }
}
