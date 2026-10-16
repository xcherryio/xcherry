/*
 * Legacy Materials in this file remain under their original licenses.
 * See LEGACY_NOTICES.md.
 */

/*
 * Modifications Copyright (c) 2026 Super Durable, Inc.
 *
 * Modifications after the Legacy Cutoff are licensed under the
 * Sustainable Use License 1.0.
 * Legacy Materials remain under their original licenses.
 * See LICENSE and LEGACY_NOTICES.md.
 */

package io.superdurable.dex.contracts;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.superdurable.dex.Attribute;
import io.superdurable.dex.AttributeLock;
import io.superdurable.dex.AttributeMap;
import io.superdurable.dex.BlobCache;
import io.superdurable.dex.BlobCacheConfig;
import io.superdurable.dex.BufferedTextStream;
import io.superdurable.dex.Channel;
import io.superdurable.dex.ChannelMap;
import io.superdurable.dex.Client;
import io.superdurable.dex.ClientOptions;
import io.superdurable.dex.Context;
import io.superdurable.dex.Flow;
import io.superdurable.dex.PersistenceSchema;
import io.superdurable.dex.RPC;
import io.superdurable.dex.RPCAttributeMapLock;
import io.superdurable.dex.RPCResult;
import io.superdurable.dex.RPCInvokeOptions;
import io.superdurable.dex.Registry;
import io.superdurable.dex.Step;
import io.superdurable.dex.StepList;
import io.superdurable.dex.StepDecision;
import io.superdurable.dex.Stream;
import io.superdurable.dex.Wait;
import io.superdurable.dex.WorkerOptions;
import io.superdurable.dex.WorkerTarget;
import io.superdurable.dex.exceptions.FlowDefinitionException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Method;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

public class UserContractsTest {
    private static final BlobCache BLOB_CACHE = new ContractBlobCache();
    private static final Attribute<String> STATUS = Attribute.define("status", String.class);
    private static final AttributeMap<String> ITEMS = AttributeMap.define("items", String.class);
    private static final Channel<OrderInput> COMMANDS =
            Channel.define("commands", OrderInput.class);
    private static final ChannelMap<OrderInput> QUEUED_COMMANDS =
            ChannelMap.define("queued-commands", OrderInput.class);
    private static final Stream<String> PROGRESS =
            Stream.define("progress", String.class, 10L * 1024L * 1024L);
    private static final Step<OrderInput> APPROVE = new ApproveStep();
    private static final Flow<OrderInput> ORDERS = new OrderFlow();

    @TempDir
    Path cacheDirectory;

    @Test
    public void typedDefinitionsBuildWithoutRuntime() {
        final Registry registry = new Registry(Collections.<Flow<?>>singletonList(ORDERS));
        Assertions.assertNotNull(registry);
        Assertions.assertEquals("OrderFlow", ORDERS.getFlowType());
        Assertions.assertEquals(OrderInput.class, APPROVE.getInputType());
        Assertions.assertEquals("GetOrder", rpcAnnotationName());
        Assertions.assertNotNull(PersistenceSchema.of(Collections.singletonList(STATUS)));
        Assertions.assertNotNull(PersistenceSchema.of(Collections.singletonList(COMMANDS)));
        Assertions.assertNotNull(PersistenceSchema.of(Collections.singletonList(PROGRESS)));
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> PersistenceSchema.of(
                        Collections.singletonList(COMMANDS),
                        Collections.emptyList()));
    }

    @Test
    public void persistenceDefinitionNamesReserveSlash() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> Attribute.define("orders/by-id", String.class));
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> AttributeMap.define("orders/by-id", String.class));
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> Channel.define("orders/by-id", String.class));
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> ChannelMap.define("orders/by-id", String.class));

        final ChannelMap<String> messages = ChannelMap.define("messages", String.class);
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> messages.forOne("orders/by-id"));
        final AttributeMap<String> items = AttributeMap.define("items", String.class);
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> AttributeLock.of(items, "orders/by-id"));
    }

    @Test
    public void rpcAnnotationDefaultsAreAvailableThroughReflection() {
        final RPC annotation = rpcAnnotation("recordOrder");
        Assertions.assertEquals("", annotation.name());
        Assertions.assertEquals(0, annotation.timeoutSeconds());
        Assertions.assertArrayEquals(new String[0], annotation.lockAttributes());
        Assertions.assertEquals(0, annotation.lockAttributeMaps().length);
    }

    @Test
    public void rpcAnnotationSupportsNestedAnnotationArrays() {
        final RPC annotation = rpcAnnotation("getOrder");
        Assertions.assertEquals(1, annotation.lockAttributeMaps().length);
        Assertions.assertEquals("items", annotation.lockAttributeMaps()[0].attribute());
        Assertions.assertEquals("one", annotation.lockAttributeMaps()[0].instance());
    }

    @Test
    public void optionsAcceptDefaultAndCustomObjectMappers() {
        final ObjectMapper mapper = new ObjectMapper();
        final ClientOptions clientOptions = new ClientOptions("localhost:8801", null, mapper);
        final WorkerOptions workerOptions = WorkerOptions.newBuilder()
                .bindAddress(":8803")
                .serverAddress("localhost:8801")
                .objectMapper(mapper)
                .build();
        Assertions.assertNotNull(clientOptions);
        Assertions.assertNotNull(workerOptions);
        Assertions.assertNotNull(new ClientOptions());
        Assertions.assertNotNull(WorkerOptions.newBuilder().build());
        Assertions.assertNotNull(WorkerOptions.newBuilder()
                .pull(io.superdurable.dex.PullWorkerOptions.newBuilder()
                        .maxConcurrentTasks(2)
                        .idleStreamsPerQueue(1)
                        .stopDrainTimeout(java.time.Duration.ofSeconds(30))
                        .build())
                .build());
        Assertions.assertNotNull(WorkerTarget.pull());
    }

    @Test
    public void registryRejectsUnknownAnnotationLocks() {
        Assertions.assertThrows(
                FlowDefinitionException.class,
                () -> new Registry(Collections.<Flow<?>>singletonList(new InvalidOrderFlow())));
    }

    @Test
    public void registryChecksStartInputAssignability() {
        Assertions.assertNotNull(new Registry(Collections.<Flow<?>>singletonList(
                new AssignableStartInputFlow())));
        Assertions.assertThrows(
                FlowDefinitionException.class,
                () -> new Registry(Collections.<Flow<?>>singletonList(
                        new IncompatibleStartInputFlow())));
    }

    @Test
    public void registryRejectsDuplicateStepClasses() {
        final FlowDefinitionException exception = Assertions.assertThrows(
                FlowDefinitionException.class,
                () -> new Registry(Collections.<Flow<?>>singletonList(
                        new DuplicateStepClassFlow())));
        Assertions.assertTrue(exception.getMessage().contains("duplicate Step class"));
    }

    @Test
    public void clientValidatesInputBeforeTransport() {
        final Client client = new Client(
                new Registry(Collections.<Flow<?>>singletonList(ORDERS)),
                BLOB_CACHE);
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> startWithWrongInput(client));
        final FlowDefinitionException unregistered = Assertions.assertThrows(
                FlowDefinitionException.class,
                () -> client.startFlow(new OrderFlow(), "unregistered", new OrderInput()));
        Assertions.assertTrue(unregistered.getMessage().contains("OrderFlow"));
    }

    @Test
    public void rpcStubBypassesFlowConstructors() {
        ConstructorOnlyRpcFlow.constructorCalls = 0;
        final ConstructorOnlyRpcFlow flow = new ConstructorOnlyRpcFlow("registered");
        final Registry registry = new Registry(Collections.<Flow<?>>singletonList(flow));
        try (Client client = new Client(registry, BLOB_CACHE)) {
            final ConstructorOnlyRpcFlow stub =
                    client.newRpcStub(ConstructorOnlyRpcFlow.class, "flow-id");
            Assertions.assertEquals(1, ConstructorOnlyRpcFlow.constructorCalls);
            Assertions.assertNotEquals(ConstructorOnlyRpcFlow.class, stub.getClass());
        }
    }

    @Test
    public void registryRejectsNonInterceptableRpcDefinitions() {
        final FlowDefinitionException finalFlowError = Assertions.assertThrows(
                FlowDefinitionException.class,
                () -> new Registry(Collections.<Flow<?>>singletonList(new FinalRpcFlow())));
        Assertions.assertTrue(finalFlowError.getMessage().contains("Flow class must not be final"));
        Assertions.assertTrue(
                finalFlowError.getMessage().contains("declare the Flow class with 'open'"));

        final FlowDefinitionException finalMethodError = Assertions.assertThrows(
                FlowDefinitionException.class,
                () -> new Registry(Collections.<Flow<?>>singletonList(new FinalRpcMethodFlow())));
        Assertions.assertTrue(finalMethodError.getMessage().contains("method must not be final"));
        Assertions.assertTrue(
                finalMethodError.getMessage().contains("declare the RPC method with 'open'"));
    }

    @Test
    public void blobCacheStoresAndReadsPayload() {
        final BlobCacheConfig config = new BlobCacheConfig(
                cacheDirectory.toString(),
                64L * 1024L * 1024L);
        final byte[] payload = new byte[] {1, 2, 3};
        try (BlobCache cache = BlobCache.open(config)) {
            Assertions.assertTrue(cache.put("contract-blob", payload));
            Assertions.assertArrayEquals(
                    payload,
                    cache.get("contract-blob").orElseThrow(AssertionError::new));
        }
    }

    private static String rpcAnnotationName() {
        return rpcAnnotation("getOrder").name();
    }

    private static RPC rpcAnnotation(final String methodName) {
        try {
            final Method method = OrderFlow.class
                    .getMethod(methodName, Context.class, OrderInput.class);
            return method.getAnnotation(RPC.class);
        } catch (NoSuchMethodException exception) {
            throw new AssertionError(exception);
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static <StartInput> StepList<StartInput> uncheckedStartStep(
            final Step<?> step) {
        return (StepList<StartInput>) (StepList) StepList.startStep((Step) step);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static void startWithWrongInput(final Client client) {
        client.startFlow((Flow) ORDERS, "order-1", "wrong input");
    }

    @SuppressWarnings("unused")
    private static void compileRPCStubContract(
            final Client client,
            final OrderFlow rpcStub,
            final OrderInput input) {
        final OrderOutput output = client.invokeRPC(rpcStub::getOrder, input);
        client.invokeRPC(rpcStub::recordOrder, input);
        final RPCInvokeOptions options = RPCInvokeOptions.newBuilder()
                .addLockAttributeMapInstance(ITEMS, "tenant-a")
                .addLoadAttributeMapInstance(ITEMS, "tenant-a")
                .addLoadChannelMapInstance(QUEUED_COMMANDS, "tenant-a")
                .build();
        final OrderOutput selected = client.invokeRPC(rpcStub::getOrder, input, options);
        client.invokeRPC(rpcStub::recordOrder, input, options);
        final OrderOutput current = client.invokeRPC(rpcStub::getCurrentOrder, options);
        client.invokeRPC(rpcStub::recordHeartbeat, options);
        if (output == null || selected == null || current == null) {
            throw new AssertionError("compile-only contract");
        }
    }

    @SuppressWarnings("unused")
    private static void compileBufferedTextStreamContract(final Context context) {
        final BufferedTextStream progress = BufferedTextStream.create(
                context,
                PROGRESS,
                Duration.ofMillis(500),
                8 * 1024);
        progress.write("thinking ");
    }

    public static class OrderFlow implements Flow<OrderInput> {
        @Override
        public StepList<OrderInput> getSteps() {
            return StepList.startStep(APPROVE);
        }

        @Override
        public PersistenceSchema getPersistenceSchema() {
            return PersistenceSchema.of(
                    Arrays.asList(STATUS, ITEMS),
                    Arrays.asList(COMMANDS, QUEUED_COMMANDS),
                    Collections.singletonList(PROGRESS));
        }

        @RPC(
                name = "GetOrder",
                timeoutSeconds = 10,
                lockAttributes = {"status"},
                lockAttributeMaps = {
                    @RPCAttributeMapLock(attribute = "items", instance = "one")
                })
        public RPCResult<OrderOutput> getOrder(
                final Context context,
                final OrderInput input) {
            return RPCResult.of(new OrderOutput());
        }

        @RPC
        public void recordOrder(final Context context, final OrderInput input) {
        }

        @RPC
        public RPCResult<OrderOutput> getCurrentOrder(final Context context) {
            return RPCResult.of(new OrderOutput());
        }

        @RPC
        public void recordHeartbeat(final Context context) {
        }
    }

    public static class InvalidOrderFlow implements Flow<OrderInput> {
        @RPC(lockAttributes = {"missing"})
        public RPCResult<OrderOutput> getOrder(
                final Context context,
                final OrderInput input) {
            return RPCResult.of(new OrderOutput());
        }
    }

    public static class AssignableStartInputFlow implements Flow<Number> {
        @Override
        public StepList<Number> getSteps() {
            return uncheckedStartStep(new IntegerStartStep());
        }
    }

    public static class IncompatibleStartInputFlow implements Flow<String> {
        @Override
        public StepList<String> getSteps() {
            return uncheckedStartStep(new IntegerStartStep());
        }
    }

    public static class DuplicateStepClassFlow implements Flow<Integer> {
        @Override
        public StepList<Integer> getSteps() {
            return StepList.startStep(new NamedIntegerStep("first"))
                    .otherSteps(new NamedIntegerStep("second"));
        }
    }

    public static class NamedIntegerStep implements Step<Integer> {
        private final String stepType;

        public NamedIntegerStep(final String stepType) {
            this.stepType = stepType;
        }

        @Override
        public String getStepType() {
            return stepType;
        }

        @Override
        public Class<Integer> getInputType() {
            return Integer.class;
        }

        @Override
        public StepDecision execute(final Context context, final Integer input) {
            return StepDecision.gracefulComplete(input);
        }
    }

    public static class IntegerStartStep implements Step<Integer> {
        @Override
        public Class<Integer> getInputType() {
            return Integer.class;
        }

        @Override
        public StepDecision execute(final Context context, final Integer input) {
            return StepDecision.gracefulComplete(input);
        }
    }

    public static class ApproveStep implements Step<OrderInput> {
        @Override
        public Class<OrderInput> getInputType() {
            return OrderInput.class;
        }

        @Override
        public StepDecision execute(final Context context, final OrderInput input) {
            return StepDecision.gracefulComplete(input.orderId);
        }

        @Override
        public Wait waitFor(final Context context, final OrderInput input) {
            return Wait.until(COMMANDS.forOne());
        }
    }

    public static class OrderInput {
        public String orderId;
    }

    public static class OrderOutput {
        public boolean accepted;
    }

    public static class ConstructorOnlyRpcFlow implements Flow<Void> {
        static int constructorCalls;

        private ConstructorOnlyRpcFlow(final String dependency) {
            constructorCalls++;
            if (dependency == null) {
                throw new IllegalArgumentException("dependency is required");
            }
        }

        @RPC
        public RPCResult<String> read(final Context context) {
            throw new AssertionError("RPC stub must intercept the implementation");
        }
    }

    public static final class FinalRpcFlow implements Flow<Void> {
        @RPC
        public RPCResult<String> read(final Context context) {
            return RPCResult.of("local");
        }
    }

    public static class FinalRpcMethodFlow implements Flow<Void> {
        @RPC
        public final RPCResult<String> read(final Context context) {
            return RPCResult.of("local");
        }
    }

    private static final class ContractBlobCache implements BlobCache {
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
