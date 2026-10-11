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

package io.superdurable.dex;

import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.superdurable.dex.GrpcExceptionTranslator.FlowTargetRequirement;
import io.superdurable.dex.exceptions.DexServiceException;
import io.superdurable.dex.exceptions.FlowAlreadyStartedException;
import io.superdurable.dex.exceptions.FlowDefinitionException;
import io.superdurable.dex.exceptions.FlowNotActiveOrNotFoundException;
import io.superdurable.dex.exceptions.FlowNotFoundException;
import io.superdurable.dex.exceptions.LongPollTimeoutException;
import io.superdurable.dex.exceptions.RpcLockConflictException;
import io.superdurable.dex.exceptions.WorkerInvocationException;
import io.superdurable.dex.exceptions.RequestTimeoutException;
import io.superdurable.gen.AttributeSyncConfig;
import io.superdurable.gen.AttributeWrite;
import io.superdurable.gen.FlowAlreadyStartedOptions;
import io.superdurable.gen.FlowExecutionID;
import io.superdurable.gen.FlowResetStepMethod;
import io.superdurable.gen.FlowResetType;
import io.superdurable.gen.FlowServiceGrpc;
import io.superdurable.gen.FlowStartOptions;
import io.superdurable.gen.GetFlowSummaryRequest;
import io.superdurable.gen.GetFlowSummaryResponse;
import io.superdurable.gen.InvokeRPCRequest;
import io.superdurable.gen.KV;
import io.superdurable.gen.ReadStreamRequest;
import io.superdurable.gen.ReadStreamResponse;
import io.superdurable.gen.ListStreamMessagesRequest;
import io.superdurable.gen.ListStreamMessagesResponse;
import io.superdurable.gen.ResetFlowRequest;
import io.superdurable.gen.SearchFlowsRequest;
import io.superdurable.gen.SearchFlowsResponse;
import io.superdurable.gen.SearchFlowsResponseEntry;
import io.superdurable.gen.SkipTimerRequest;
import io.superdurable.gen.StartFlowRequest;
import io.superdurable.gen.StopFlowRequest;
import io.superdurable.gen.TriggerContinueAsNewRequest;
import io.superdurable.gen.UpdateFlowConfigRequest;
import io.superdurable.gen.Value;
import io.superdurable.gen.WaitForFlowRequest;
import io.superdurable.gen.WaitForStepCompletionRequest;
import io.superdurable.gen.AttributeMatchOperator;
import io.superdurable.gen.WaitForAttributeRequest;
import io.superdurable.gen.WaitForAttributeResponse;
import io.superdurable.gen.WriteStreamRequest;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.dynamic.scaffold.subclass.ConstructorStrategy;
import net.bytebuddy.implementation.InvocationHandlerAdapter;
import net.bytebuddy.matcher.ElementMatchers;
import org.objenesis.ObjenesisStd;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Provides synchronous, strongly typed operations for starting and controlling Dex Flows.
 *
 * <p>All network methods block the calling thread until the gRPC request completes. Create one
 * client for a registered set of Flow definitions and reuse it across calls; the underlying gRPC
 * channel supports concurrent callers. The supplied {@link BlobCache} is borrowed and is not closed
 * by the client. Service failures use typed {@link DexServiceException} subclasses. Long-poll
 * operations can throw {@link LongPollTimeoutException}. {@link #waitForFlow(String)} returns a
 * {@link FlowResult} for every terminal status.
 *
 * <pre>{@code
 * Registry registry = new Registry(Collections.<Flow<?>>singletonList(orderFlow));
 * try (Client client = new Client(registry, blobCache)) {
 *     String runId = client.startFlow(orderFlow, "order-123", input);
 *     OrderFlow rpc = client.newRpcStub(OrderFlow.class, "order-123", runId);
 *     OrderStatus status = client.invokeRPC(rpc::getStatus);
 *     OrderOutput output = client.waitForFlow("order-123").getSingleOutput(OrderOutput.class);
 * }
 * }</pre>
 */
public final class Client implements AutoCloseable {
    private final Registry registry;
    private final BlobCache blobCache;
    private final ClientOptions options;
    private final ManagedChannel channel;
    private final FlowServiceGrpc.FlowServiceBlockingStub service;
    private final ValueMapper values;
    private final ValueHydrator hydrator;
    private final WorkerDispatcher mappings;
    private final ThreadLocal<RPCInvokeOptions> rpcInvokeOptions =
            new ThreadLocal<RPCInvokeOptions>();

    /**
     * Creates a client using local development defaults.
     *
     * @param registry the nonnull registry of Flow definitions used for typed mapping
     * @param blobCache the nonnull cache used to hydrate blob-backed values
     * @throws IllegalArgumentException if either argument is {@code null}
     */
    public Client(final Registry registry, final BlobCache blobCache) {
        this(registry, blobCache, new ClientOptions());
    }

    /**
     * Creates a client with explicit connection, routing, and serialization options.
     *
     * @param registry the nonnull registry of Flow definitions used for typed mapping
     * @param blobCache the nonnull cache used to hydrate blob-backed values
     * @param options the nonnull client options
     * @throws IllegalArgumentException if any argument is {@code null}
     */
    public Client(
            final Registry registry,
            final BlobCache blobCache,
            final ClientOptions options) {
        if (registry == null || blobCache == null || options == null) {
            throw new IllegalArgumentException("registry, blobCache, and options are required");
        }
        this.registry = registry;
        this.blobCache = blobCache;
        this.options = options;
        this.values = new ValueMapper(options.getObjectMapper());
        this.channel = ManagedChannelBuilder.forTarget(options.getServerAddress())
                .usePlaintext()
                .build();
        this.service = FlowServiceGrpc.newBlockingStub(channel);
        this.hydrator = new ValueHydrator(service, blobCache);
        this.mappings = new WorkerDispatcher(registry, values, hydrator);
    }

    Registry getRegistry() {
        return registry;
    }

    BlobCache getBlobCache() {
        return blobCache;
    }

    ClientOptions getOptions() {
        return options;
    }

    /**
     * Starts a registered Flow using default start options.
     *
     * @param flow the exact Flow instance registered with this client
     * @param flowId the nonblank application-assigned Flow ID
     * @param input the typed start-Step input, or {@code null} for a Flow without a start Step
     * @param <I> the Flow start input type
     * @return the server-assigned run ID
     * @throws IllegalArgumentException if the Flow is unregistered, the ID is invalid, or the input
     *     does not match the registered start Step
     * @throws FlowAlreadyStartedException if the Flow ID conflicts with an existing execution
     * @throws DexServiceException if Dex otherwise rejects or cannot complete the request
     */
    public <I> String startFlow(
            final Flow<I> flow,
            final String flowId,
            final I input) {
        return startFlow(flow, flowId, input, new StartFlowOptions());
    }

    /**
     * Starts a registered Flow with explicit start options.
     *
     * @param flow the exact Flow instance registered with this client
     * @param flowId the nonblank application-assigned Flow ID
     * @param input the typed start-Step input, or {@code null} for a Flow without a start Step
     * @param startOptions timeout, scheduling, initial state, routing, and idempotency settings
     * @param <I> the Flow start input type
     * @return the server-assigned run ID
     * @throws IllegalArgumentException if the Flow, ID, input, or duration settings are invalid
     * @throws FlowAlreadyStartedException if the Flow ID conflicts with an existing execution
     * @throws DexServiceException if Dex otherwise rejects or cannot complete the request
     */
    public <I> String startFlow(
            final Flow<I> flow,
            final String flowId,
            final I input,
            final StartFlowOptions startOptions) {
        final Registry.RegisteredFlow registered = registry.getFlow(flow.getFlowType());
        if (registered.getFlow() != flow) {
            throw new FlowDefinitionException(
                    "Flow instance is not registered: " + flow.getFlowType());
        }
        final FlowTimeoutPolicy timeoutPolicy = resolveTimeoutPolicy(
                registered, startOptions.getTimeout(), startOptions.getTimeoutPolicy());
        final FlowStartOptions.Builder flowStartOptions = mapStartOptions(startOptions).toBuilder();
        final io.superdurable.gen.FlowTimeoutHandlerOptions timeoutHandlerOptions =
                mappings.mapFlowTimeoutHandlerOptions(
                        registered,
                        startOptions.getTimeout(),
                        timeoutPolicy,
                        startOptions.getTimeoutHandlerOptions());
        if (timeoutHandlerOptions != null) {
            flowStartOptions.setTimeoutHandlerOptions(timeoutHandlerOptions);
        }
        final StartFlowRequest.Builder request = StartFlowRequest.newBuilder()
                .setFlowId(Attribute.requireName(flowId))
                .setFlowType(registered.getName())
                .setRequestId(startOptions.getRequestId() == null
                        ? UUID.randomUUID().toString()
                        : startOptions.getRequestId())
                .setFlowStartOptions(flowStartOptions);
        if (registered.getStartStep() != null) {
            final Class<?> inputType = registered.getStartStep().getStep().getInputType();
            if (input != null && !inputType.isInstance(input)) {
                throw new IllegalArgumentException(
                        "Flow input must be " + inputType.getName()
                                + ", got " + input.getClass().getName());
            }
            request.setStartStepType(registered.getStartStep().getName())
                    .setStepInput(values.encode(input));
            final io.superdurable.gen.StepOptions stepOptions = mappings.mapStepOptions(
                    registered,
                    registered.getStartStep().getStep().getStepOptions());
            final io.superdurable.gen.StepOptions.Builder mappedStep = stepOptions == null
                    ? io.superdurable.gen.StepOptions.newBuilder()
                    : stepOptions.toBuilder();
            mappedStep.setSkipWaitFor(registered.getStartStep().skipsWaitFor());
            request.setStepOptions(mappedStep);
        } else if (input != null) {
            throw new IllegalArgumentException("Flow without a start Step requires null input");
        }
        if (startOptions.getTimeout() != null) {
            request.setFlowTimeoutSeconds(seconds32(startOptions.getTimeout()));
        }
        if (timeoutPolicy != FlowTimeoutPolicy.DEFAULT) {
            request.setFlowTimeoutPolicy(mapFlowTimeoutPolicy(timeoutPolicy));
        }
        return call(() -> service.startFlow(request.build())).getRunId();
    }

    static FlowTimeoutPolicy resolveTimeoutPolicy(
            final Registry.RegisteredFlow flow,
            final Duration timeout,
            final FlowTimeoutPolicy policy) {
        final boolean hasPositiveTimeout = timeout != null
                && !timeout.isZero()
                && !timeout.isNegative();
        if (!hasPositiveTimeout) {
            if (policy != FlowTimeoutPolicy.DEFAULT) {
                throw new IllegalArgumentException(
                        "Flow timeout policy requires a positive timeout");
            }
            return FlowTimeoutPolicy.DEFAULT;
        }
        final FlowTimeoutPolicy resolved = policy == FlowTimeoutPolicy.DEFAULT
                ? (flow.hasTimeoutHandler() ? FlowTimeoutPolicy.HANDLER : FlowTimeoutPolicy.FAIL)
                : policy;
        if (resolved == FlowTimeoutPolicy.HANDLER && !flow.hasTimeoutHandler()) {
            throw new IllegalArgumentException(
                    "Flow " + flow.getName() + " does not override handleTimeout");
        }
        return resolved;
    }

    static io.superdurable.gen.FlowTimeoutPolicy mapFlowTimeoutPolicy(
            final FlowTimeoutPolicy policy) {
        switch (policy) {
            case FAIL:
                return io.superdurable.gen.FlowTimeoutPolicy.FLOW_TIMEOUT_POLICY_FAIL;
            case CANCEL:
                return io.superdurable.gen.FlowTimeoutPolicy.FLOW_TIMEOUT_POLICY_CANCEL;
            case HANDLER:
                return io.superdurable.gen.FlowTimeoutPolicy.FLOW_TIMEOUT_POLICY_HANDLER;
            default:
                return io.superdurable.gen.FlowTimeoutPolicy.FLOW_TIMEOUT_POLICY_UNSPECIFIED;
        }
    }

    /**
     * Creates a strongly typed RPC stub targeting the current run of a Flow.
     *
     * @param rpcClass the non-final registered Flow class containing {@link RPC} methods
     * @param flowId the target Flow ID
     * @param <T> the concrete Flow class
     * @return a constructor-free stub whose RPC methods perform client requests
     * @throws IllegalArgumentException if the class is unregistered, final, or cannot be subclassed
     */
    public <T> T newRpcStub(final Class<T> rpcClass, final String flowId) {
        return newRpcStub(rpcClass, flowId, "");
    }

    /**
     * Creates a strongly typed RPC stub targeting a specific Flow run.
     *
     * <p>Dex subclasses the class with ByteBuddy and allocates the stub without invoking a
     * constructor. RPC methods and the containing class must therefore be non-final. Kotlin classes
     * and methods are final by default and must be declared {@code open}.
     *
     * @param rpcClass the non-final registered Flow class containing {@link RPC} methods
     * @param flowId the target Flow ID
     * @param runId the target run ID, or an empty string for the current run
     * @param <T> the concrete Flow class
     * @return a constructor-free typed RPC stub
     * @throws IllegalArgumentException if the class is unregistered, final, or cannot be subclassed
     */
    public <T> T newRpcStub(
            final Class<T> rpcClass,
            final String flowId,
            final String runId) {
        final Registry.RegisteredFlow flow = registry.getFlow(rpcClass);
        try {
            final Class<? extends T> stubClass = new ByteBuddy()
                    .subclass(rpcClass, ConstructorStrategy.Default.NO_CONSTRUCTORS)
                    .method(ElementMatchers.isAnnotatedWith(RPC.class))
                    .intercept(InvocationHandlerAdapter.of(
                            new RpcInvocationHandler(new RpcTarget(flow, flowId, runId))))
                    .make()
                    .load(rpcClass.getClassLoader(), ClassLoadingStrategy.Default.INJECTION)
                    .getLoaded();
            return new ObjenesisStd().newInstance(stubClass);
        } catch (RuntimeException exception) {
            throw new FlowDefinitionException(
                    "RPC stub could not subclass " + rpcClass.getName(), exception);
        }
    }

    /**
     * Invokes a function-style RPC with one input and returns its typed output.
     *
     * <p>A non-transactional RPC without Attribute locks starts from a backend query. A
     * retained terminal execution can serve the query when the handler returns no durable effects.
     * Locks, transactional execution, returned effects, or server policy can require an active
     * execution.
     *
     * @param rpcStubMethod a direct method reference from a stub created by this client
     * @param input the typed RPC input
     * @param <I> the RPC input type
     * @param <O> the RPC output type
     * @return the decoded RPC output
     * @throws FlowNotActiveOrNotFoundException if the Flow is missing or the selected path cannot use
     *     its closed execution
     * @throws RpcLockConflictException if the RPC cannot acquire its Attribute locks
     * @throws WorkerInvocationException if worker code fails while executing the RPC
     * @throws DexServiceException if Dex otherwise rejects or cannot complete the RPC
     */
    public <I, O> O invokeRPC(
            final RpcDefinitions.RpcFunc1<I, O> rpcStubMethod,
            final I input) {
        return rpcStubMethod.execute(null, input).getOutput();
    }

    /**
     * Invokes a function-style RPC with one input and runtime-selected map instances.
     *
     * @param rpcStubMethod a direct method reference from a stub created by this client
     * @param input the typed RPC input
     * @param invokeOptions additive Attribute-map locks and exact map-instance loads
     * @param <I> the RPC input type
     * @param <O> the RPC output type
     * @return the decoded RPC output
     * @throws FlowDefinitionException if an option references a definition outside the RPC Flow
     * @throws IllegalArgumentException if {@code invokeOptions} is {@code null}
     * @throws FlowNotActiveOrNotFoundException if the Flow is missing or the selected path cannot use
     *     its closed execution
     * @throws RpcLockConflictException if the RPC cannot acquire its Attribute locks
     * @throws WorkerInvocationException if worker code fails while executing the RPC
     * @throws DexServiceException if Dex otherwise rejects or cannot complete the RPC
     */
    public <I, O> O invokeRPC(
            final RpcDefinitions.RpcFunc1<I, O> rpcStubMethod,
            final I input,
            final RPCInvokeOptions invokeOptions) {
        return withRPCInvokeOptions(
                invokeOptions,
                () -> rpcStubMethod.execute(null, input).getOutput());
    }

    /**
     * Invokes a function-style RPC without application input.
     *
     * <p>A retained terminal execution can serve a query-only RPC. Locks, transactional
     * execution, returned effects, or server policy can require an active execution.
     *
     * @param rpcStubMethod a direct method reference from a stub created by this client
     * @param <O> the RPC output type
     * @return the decoded RPC output
     * @throws FlowNotActiveOrNotFoundException if the Flow is missing or the selected path cannot use
     *     its closed execution
     * @throws RpcLockConflictException if the RPC cannot acquire its Attribute locks
     * @throws WorkerInvocationException if worker code fails while executing the RPC
     * @throws DexServiceException if Dex otherwise rejects or cannot complete the RPC
     */
    public <O> O invokeRPC(final RpcDefinitions.RpcFunc0<O> rpcStubMethod) {
        return rpcStubMethod.execute(null).getOutput();
    }

    /**
     * Invokes a function-style RPC without application input and adds map-instance selections.
     *
     * @param rpcStubMethod a direct method reference from a stub created by this client
     * @param invokeOptions additive Attribute-map locks and exact map-instance loads
     * @param <O> the RPC output type
     * @return the decoded RPC output
     * @throws FlowDefinitionException if an option references a definition outside the RPC Flow
     * @throws IllegalArgumentException if {@code invokeOptions} is {@code null}
     * @throws FlowNotActiveOrNotFoundException if the Flow is missing or the selected path cannot use
     *     its closed execution
     * @throws RpcLockConflictException if the RPC cannot acquire its Attribute locks
     * @throws WorkerInvocationException if worker code fails while executing the RPC
     * @throws DexServiceException if Dex otherwise rejects or cannot complete the RPC
     */
    public <O> O invokeRPC(
            final RpcDefinitions.RpcFunc0<O> rpcStubMethod,
            final RPCInvokeOptions invokeOptions) {
        return withRPCInvokeOptions(
                invokeOptions,
                () -> rpcStubMethod.execute(null).getOutput());
    }

    /**
     * Invokes a procedure-style RPC with one input.
     *
     * <p>A retained terminal execution can serve a query-only RPC. Locks, transactional
     * execution, returned effects, or server policy can require an active execution.
     *
     * @param rpcStubMethod a direct method reference from a stub created by this client
     * @param input the typed RPC input
     * @param <I> the RPC input type
     * @throws FlowNotActiveOrNotFoundException if the Flow is missing or the selected path cannot use
     *     its closed execution
     * @throws RpcLockConflictException if the RPC cannot acquire its Attribute locks
     * @throws WorkerInvocationException if worker code fails while executing the RPC
     * @throws DexServiceException if Dex otherwise rejects or cannot complete the RPC
     */
    public <I> void invokeRPC(
            final RpcDefinitions.RpcProc1<I> rpcStubMethod,
            final I input) {
        rpcStubMethod.execute(null, input);
    }

    /**
     * Invokes a procedure-style RPC with one input and runtime-selected map instances.
     *
     * @param rpcStubMethod a direct method reference from a stub created by this client
     * @param input the typed RPC input
     * @param invokeOptions additive Attribute-map locks and exact map-instance loads
     * @param <I> the RPC input type
     * @throws FlowDefinitionException if an option references a definition outside the RPC Flow
     * @throws IllegalArgumentException if {@code invokeOptions} is {@code null}
     * @throws FlowNotActiveOrNotFoundException if the Flow is missing or the selected path cannot use
     *     its closed execution
     * @throws RpcLockConflictException if the RPC cannot acquire its Attribute locks
     * @throws WorkerInvocationException if worker code fails while executing the RPC
     * @throws DexServiceException if Dex otherwise rejects or cannot complete the RPC
     */
    public <I> void invokeRPC(
            final RpcDefinitions.RpcProc1<I> rpcStubMethod,
            final I input,
            final RPCInvokeOptions invokeOptions) {
        withRPCInvokeOptions(invokeOptions, () -> {
            rpcStubMethod.execute(null, input);
            return null;
        });
    }

    /**
     * Invokes a procedure-style RPC without application input.
     *
     * <p>A retained terminal execution can serve a query-only RPC. Locks, transactional
     * execution, returned effects, or server policy can require an active execution.
     *
     * @param rpcStubMethod a direct method reference from a stub created by this client
     * @throws FlowNotActiveOrNotFoundException if the Flow is missing or the selected path cannot use
     *     its closed execution
     * @throws RpcLockConflictException if the RPC cannot acquire its Attribute locks
     * @throws WorkerInvocationException if worker code fails while executing the RPC
     * @throws DexServiceException if Dex otherwise rejects or cannot complete the RPC
     */
    public void invokeRPC(final RpcDefinitions.RpcProc0 rpcStubMethod) {
        rpcStubMethod.execute(null);
    }

    /**
     * Invokes a procedure-style RPC without input and adds map-instance selections.
     *
     * @param rpcStubMethod a direct method reference from a stub created by this client
     * @param invokeOptions additive Attribute-map locks and exact map-instance loads
     * @throws FlowDefinitionException if an option references a definition outside the RPC Flow
     * @throws IllegalArgumentException if {@code invokeOptions} is {@code null}
     * @throws FlowNotActiveOrNotFoundException if the Flow is missing or the selected path cannot use
     *     its closed execution
     * @throws RpcLockConflictException if the RPC cannot acquire its Attribute locks
     * @throws WorkerInvocationException if worker code fails while executing the RPC
     * @throws DexServiceException if Dex otherwise rejects or cannot complete the RPC
     */
    public void invokeRPC(
            final RpcDefinitions.RpcProc0 rpcStubMethod,
            final RPCInvokeOptions invokeOptions) {
        withRPCInvokeOptions(invokeOptions, () -> {
            rpcStubMethod.execute(null);
            return null;
        });
    }

    /**
     * Appends one typed best-effort Stream message with producer metadata.
     *
     * <p>Each call appends a new message. The source is informational and may be reused, including
     * by concurrent producers. A source may contain {@code #}.
     *
     * @param flowId the logical Flow instance ID; the Flow need not exist or be active
     * @param stream the exact Stream registered in one Flow schema
     * @param source the nonblank producer description returned with the message
     * @param value the typed message to append
     * @param <T> the Stream message type
     * @throws FlowDefinitionException if the Stream is not registered
     * @throws IllegalArgumentException if the Flow ID or source is invalid
     * @throws DexServiceException if Dex cannot append the message
     */
    public <T> void writeStream(
            final String flowId,
            final Stream<T> stream,
            final String source,
            final T value) {
        final String validatedSource = Attribute.requireName(source);
        final Registry.RegisteredFlow flow = registry.getFlow(stream);
        call(() -> service.writeStream(WriteStreamRequest.newBuilder()
                .setFlowId(Attribute.requireName(flowId))
                .setFlowType(flow.getName())
                .setStreamName(stream.getStreamName())
                .setStreamCapacityBytes(stream.getStreamCapacityBytes())
                .setValue(values.encode(value))
                .setSource(validatedSource)
                .build()));
    }

    /**
     * Blocks for the next retained Stream message using the server default wait.
     *
     * @param flowId the logical Flow instance ID
     * @param stream the exact Stream registered in one Flow schema
     * @param resumeToken the prior token, or empty for the retained head
     * @param <T> the Stream message type
     * @return the decoded message and resumable metadata
     */
    public <T> StreamMessage<T> readStream(
            final String flowId,
            final Stream<T> stream,
            final String resumeToken) {
        return readStream(flowId, stream, resumeToken, null);
    }

    /**
     * Blocks for the next retained Stream message with an explicit long-poll duration.
     *
     * @param flowId the logical Flow instance ID
     * @param stream the exact Stream registered in one Flow schema
     * @param resumeToken the prior token, or empty for the retained head
     * @param timeout the nonnegative whole-second server wait, or {@code null} for its default
     * @param <T> the Stream message type
     * @return the decoded message and resumable metadata
     * @throws LongPollTimeoutException if no message arrives before the wait expires
     * @throws FlowDefinitionException if the Stream is not registered
     * @throws DexServiceException if Dex cannot read the Stream
     */
    public <T> StreamMessage<T> readStream(
            final String flowId,
            final Stream<T> stream,
            final String resumeToken,
            final Duration timeout) {
        final Registry.RegisteredFlow flow = registry.getFlow(stream);
        final ReadStreamRequest.Builder request = ReadStreamRequest.newBuilder()
                .setFlowId(Attribute.requireName(flowId))
                .setFlowType(flow.getName())
                .setStreamName(stream.getStreamName())
                .setResumeToken(resumeToken == null ? "" : resumeToken);
        if (timeout != null) {
            request.setWaitTimeSeconds(seconds32(timeout));
        }
        final ReadStreamResponse response = call(() -> service.readStream(request.build()));
        if (!response.hasMessage()
                || !response.getMessage().hasValue()
                || !response.getMessage().hasCreatedTime()
                || response.getMessage().getResumeToken().isEmpty()) {
            throw new IllegalStateException("Dex returned an incomplete Stream message");
        }
        @SuppressWarnings("unchecked")
        final T value = (T) values.decode(
                response.getMessage().getValue(),
                stream.getValueType());
        return new StreamMessage<T>(
                value,
                response.getMessage().getResumeToken(),
                instant(response.getMessage().getCreatedTime()),
                response.getMessage().getSource());
    }

    /**
     * Returns one newest-first page of retained Stream messages without waiting for writes.
     *
     * <p>An empty token starts at the retained tail. Pass the returned next-page token unchanged
     * to read older messages. Stream trimming may remove messages between pages.
     *
     * @param flowId the logical Flow instance ID used as the Stream instance key
     * @param stream the exact Stream registered in one Flow schema
     * @param pageSize the positive maximum count within the server-configured limit
     * @param beforePageToken the preceding page token, or empty for the first page
     * @param <T> the Stream message type
     * @return decoded messages in newest-first order and the next-page token
     * @throws IllegalArgumentException if {@code pageSize} is not positive
     * @throws FlowDefinitionException if the Stream is not registered
     * @throws DexServiceException if Dex cannot list the Stream
     */
    public <T> StreamMessagesPage<T> listStreamMessages(
            final String flowId,
            final Stream<T> stream,
            final int pageSize,
            final String beforePageToken) {
        if (pageSize < 1) {
            throw new IllegalArgumentException("Stream page size must be positive");
        }
        final Registry.RegisteredFlow flow = registry.getFlow(stream);
        final ListStreamMessagesResponse response = call(() -> service.listStreamMessages(
                ListStreamMessagesRequest.newBuilder()
                        .setFlowId(Attribute.requireName(flowId))
                        .setFlowType(flow.getName())
                        .setStreamName(stream.getStreamName())
                        .setPageSize(pageSize)
                        .setBeforePageToken(beforePageToken == null ? "" : beforePageToken)
                        .build()));
        final List<StreamMessage<T>> messages = new ArrayList<StreamMessage<T>>(
                response.getMessagesCount());
        for (io.superdurable.gen.StreamMessage message : response.getMessagesList()) {
            if (!message.hasValue()
                    || !message.hasCreatedTime()
                    || message.getResumeToken().isEmpty()) {
                throw new IllegalStateException("Dex returned an incomplete Stream message");
            }
            @SuppressWarnings("unchecked")
            final T value = (T) values.decode(message.getValue(), stream.getValueType());
            messages.add(new StreamMessage<T>(
                    value,
                    message.getResumeToken(),
                    instant(message.getCreatedTime()),
                    message.getSource()));
        }
        return new StreamMessagesPage<T>(messages, response.getNextPageToken());
    }

    /**
     * Cancels a running Flow without an explicit reason.
     *
     * @param flowId the target Flow ID
     * @throws FlowNotActiveOrNotFoundException if the target Flow has no active execution
     * @throws DexServiceException if Dex otherwise cannot stop the Flow
     */
    public void stopFlow(final String flowId) {
        stopFlow(flowId, new StopFlowOptions());
    }

    /**
     * Stops a running Flow with explicit terminal behavior.
     *
     * @param flowId the target Flow ID
     * @param stopOptions the stop mode and optional reason
     * @throws FlowNotActiveOrNotFoundException if the target Flow has no active execution
     * @throws DexServiceException if Dex otherwise cannot stop the Flow
     */
    public void stopFlow(final String flowId, final StopFlowOptions stopOptions) {
        call(() -> service.stopFlow(StopFlowRequest.newBuilder()
                .setFlowId(flowId)
                .setReason(stopOptions.getReason() == null ? "" : stopOptions.getReason())
                .setStopType(mapStopType(stopOptions.getType()))
                .build()), FlowTargetRequirement.ACTIVE, flowId);
    }

    /**
     * Blocks until a Flow reaches any terminal status.
     *
     * @param flowId the target Flow ID
     * @return the terminal Flow result
     * @throws FlowNotFoundException if no matching Flow execution exists
     * @throws DexServiceException if Dex otherwise cannot complete the wait request
     */
    public FlowResult waitForFlow(final String flowId) {
        return waitForFlow(flowId, null);
    }

    /**
     * Blocks for a bounded duration until a Flow reaches any terminal status.
     *
     * @param flowId the target Flow ID
     * @param timeout the nonnegative whole-second long-poll duration, or {@code null} for no bound
     * @return the terminal Flow result
     * @throws LongPollTimeoutException if {@code timeout} expires while the Flow remains running
     * @throws IllegalArgumentException if {@code timeout} is not a supported whole-second duration
     * @throws FlowNotFoundException if no matching Flow execution exists
     * @throws DexServiceException if Dex otherwise cannot complete the wait request
     */
    public FlowResult waitForFlow(
            final String flowId,
            final Duration timeout) {
        final io.superdurable.gen.FlowResult response = waitForFlowResponse(flowId, timeout);
        return new FlowResult(
                mapFlowStatus(response.getFlowStatus()),
                mapNullableFlowErrorType(response.getErrorType()),
                response.getErrorMessage().isEmpty() ? null : response.getErrorMessage(),
                mapStepCompletions(flowId, response.getResultsList()));
    }

    /**
     * Returns a {@link FlowInfo} summary of the current Flow run.
     *
     * <p>The summary contains the Flow ID, run ID, Flow type, current status, and start time. It
     * reflects the execution when Dex handles this request; a running Flow may change afterward.
     *
     * @param flowId the target Flow ID
     * @return the current Flow run summary
     * @throws FlowNotFoundException if no matching Flow execution exists
     * @throws DexServiceException if Dex otherwise cannot describe the Flow
     */
    public FlowInfo describeFlow(final String flowId) {
        final GetFlowSummaryResponse response = call(() -> service.getFlowSummary(
                GetFlowSummaryRequest.newBuilder().setFlowId(flowId).build()),
                FlowTargetRequirement.EXISTING,
                flowId);
        final FlowExecutionID execution = response.getFlowExecutionId();
        return new FlowInfo(
                execution.getFlowId(),
                execution.getRunId(),
                response.getFlowType(),
                mapFlowStatus(response.getFlowStatus()),
                instant(response.getStartTime()));
    }

    /**
     * Searches Flow executions and returns the first page.
     *
     * @param query the server search expression, or {@code null} for the default search
     * @param pageSize the nonnegative requested page size; zero uses the server default
     * @return the first immutable search-results page
     * @throws IllegalArgumentException if {@code pageSize} is negative
     * @throws DexServiceException if Dex cannot execute the search
     */
    public SearchFlowsPage searchFlows(final String query, final int pageSize) {
        return searchFlows(query, pageSize, "");
    }

    /**
     * Searches Flow executions from a continuation token.
     *
     * @param query the same server search expression used for the previous page
     * @param pageSize the nonnegative requested page size; zero uses the server default
     * @param nextPageToken the prior page's token, or {@code null} for the first page
     * @return an immutable search-results page
     * @throws IllegalArgumentException if {@code pageSize} is negative
     * @throws DexServiceException if Dex cannot execute the search
     */
    public SearchFlowsPage searchFlows(
            final String query,
            final int pageSize,
            final String nextPageToken) {
        return searchFlows(query, pageSize, nextPageToken, SearchFlowsOptions.newBuilder().build());
    }

    /**
     * Searches with inclusion options.
     * @param query filter
     * @param pageSize maximum
     * @param nextPageToken token
     * @param options inclusion
     * @return page
     */
    public SearchFlowsPage searchFlows(
            final String query,
            final int pageSize,
            final String nextPageToken,
            final SearchFlowsOptions options) {
        java.util.Objects.requireNonNull(options, "options");
        if (pageSize < 0) {
            throw new IllegalArgumentException("search page size must not be negative");
        }
        final SearchFlowsResponse response = call(() -> service.searchFlows(
                SearchFlowsRequest.newBuilder()
                        .setQuery(query == null ? "" : query)
                        .setPageSize(pageSize)
                        .setNextPageToken(nextPageToken == null ? "" : nextPageToken)
                        .setIncludeContinuedAsNew(options.getIncludeContinuedAsNew())
                        .build()));
        final List<SearchFlowEntry> flows =
                new ArrayList<SearchFlowEntry>(response.getFlowRunsCount());
        for (final SearchFlowsResponseEntry entry : response.getFlowRunsList()) {
            flows.add(mapSearchEntry(entry));
        }
        return new SearchFlowsPage(flows, response.getNextPageToken());
    }

    private SearchFlowEntry mapSearchEntry(final SearchFlowsResponseEntry entry) {
        final Map<String, Object> attributes = new LinkedHashMap<String, Object>();
        for (final KV attribute : entry.getIndexedAttributesList()) {
            attributes.put(
                    attribute.getKey(),
                    values.decodeToObject(hydrator.hydrate(entry.getFlowId(), attribute.getValue())));
        }
        return new SearchFlowEntry(
                entry.getFlowId(),
                entry.getRunId(),
                entry.getFlowType(),
                mapFlowStatus(entry.getFlowStatus()),
                entry.hasStartTime() ? instant(entry.getStartTime()) : null,
                entry.hasCloseTime() ? instant(entry.getCloseTime()) : null,
                attributes);
    }

    /**
     * Creates a new run from a selected point in an existing Flow's history.
     *
     * @param flowId the target Flow ID
     * @param options the time travel point and replay behavior
     * @return the server-assigned run ID of the new execution
     * @throws FlowNotFoundException if no matching Flow execution exists
     * @throws DexServiceException if Dex otherwise rejects or cannot perform time travel
     */
    public String timeTravel(final String flowId, final TimeTravelOptions options) {
        final ResetFlowRequest.Builder request = ResetFlowRequest.newBuilder()
                .setFlowId(flowId)
                .setResetType(mapTimeTravelType(options.getType()))
                .setReason(options.getReason() == null ? "" : options.getReason())
                .setSkipWritesReapply(options.isSkipWritesReapply());
        if (options.getHistoryEventTime() != null) {
            request.setHistoryEventTime(options.getHistoryEventTime().toString());
        }
        if (options.getStepType() != null) {
            request.setStepType(options.getStepType());
        }
        if (options.getStepExecutionId() != null) {
            request.setStepExecutionId(options.getStepExecutionId());
        }
        if (options.getStepMethod() != null) {
            request.setStepMethod(mapTimeTravelStepMethod(options.getStepMethod()));
        }
        return call(
                () -> service.resetFlow(request.build()),
                FlowTargetRequirement.EXISTING,
                flowId).getRunId();
    }

    /**
     * Makes one durable timer condition fire immediately.
     *
     * @param flowId the target Flow ID
     * @param stepExecutionId the Step execution containing the timer
     * @param timerId the timer selected by condition ID or index
     * @throws FlowNotActiveOrNotFoundException if the target Flow has no active execution
     * @throws DexServiceException if Dex otherwise cannot find or skip the timer
     */
    public void skipTimer(
            final String flowId,
            final StepExecutionId stepExecutionId,
            final TimerId timerId) {
        final SkipTimerRequest.Builder request = SkipTimerRequest.newBuilder()
                .setFlowId(flowId)
                .setStepExecutionId(stepExecutionId.getStepType()
                        + "-" + stepExecutionId.getExecutionNumber());
        if (timerId.getConditionId() != null) {
            request.setTimerConditionId(timerId.getConditionId());
        }
        if (timerId.getIndex() != null) {
            request.setTimerConditionIndex(timerId.getIndex());
        }
        call(
                () -> service.skipTimer(request.build()),
                FlowTargetRequirement.ACTIVE,
                flowId);
    }

    /**
     * Blocks until a specific Step execution completes or its caller-visible request expires.
     * The server derives a stable Request ID from the Step execution when none is supplied.
     * Request timeout covers transparent transport reattachments. Internal handler timeout controls
     * Temporal Update generation rollover and does not end this request.
     *
     * @param flowId the target Flow ID
     * @param stepExecutionId the Step execution to observe
     * @param options the Request ID override and request/handler timeout controls
     * @throws IllegalArgumentException if either timeout is unsupported
     * @throws RequestTimeoutException if a positive request timeout expires first
     * @throws FlowNotActiveOrNotFoundException if the target Flow has no active execution
     * @throws DexServiceException if Dex otherwise cannot complete the wait request
     */
    public void waitForStepCompletion(
            final String flowId,
            final StepExecutionId stepExecutionId,
            final WaitForStepCompletionOptions options) {
        final ClientRequestBudget requestBudget = new ClientRequestBudget(options.getRequestTimeout());
        final int internalHandlerTimeoutSeconds = seconds32(options.getInternalHandlerTimeout());
        while (true) {
            try {
                final ClientRequestAttempt requestAttempt = requestBudget.nextAttempt();
                final FlowServiceGrpc.FlowServiceBlockingStub requestService = requestAttempt.hasDeadline()
                        ? service.withDeadlineAfter(
                                requestAttempt.getRemainingNanos(), TimeUnit.NANOSECONDS)
                        : service;
                call(() -> requestService.waitForStepCompletion(
                        WaitForStepCompletionRequest.newBuilder()
                        .setFlowId(flowId)
                        .setStepType(stepExecutionId.getStepType())
                        .setStepExecutionNumber(
                                Integer.toString(stepExecutionId.getExecutionNumber()))
                        .setRequestTimeoutSeconds(requestAttempt.getRemainingSeconds())
                        .setInternalHandlerTimeoutSeconds(internalHandlerTimeoutSeconds)
                        .setRequestId(options.getRequestId() == null ? "" : options.getRequestId())
                        .build()),
                        FlowTargetRequirement.ACTIVE,
                        flowId);
                return;
            } catch (LongPollTimeoutException timeout) {
                // Reattach to the same logical wait.
            } catch (DexServiceException failure) {
                if (failure.getCode() == io.grpc.Status.Code.DEADLINE_EXCEEDED
                        && requestBudget.hasExpired()) {
                    throw new RequestTimeoutException(
                            io.grpc.Status.Code.DEADLINE_EXCEEDED,
                            "request timed out",
                            failure);
                }
                throw failure;
            }
        }
    }

    /**
     * Blocks until a singleton Attribute satisfies a scalar match or its request timeout expires.
     * The server derives a stable Request ID from the condition when none is supplied.
     * Request timeout covers transparent transport reattachments. Internal handler timeout controls
     * Temporal Update generation rollover and does not end this request.
     *
     * @param flowId the target Flow ID
     * @param attribute the registered Attribute definition
     * @param match the scalar predicate to await
     * @param options the Request ID override and request/handler timeout controls
     * @param <T> the Attribute value type
     * @return the current Attribute value that satisfied the match
     * @throws IllegalArgumentException if the budget, match operand, or operator is invalid
     * @throws RequestTimeoutException if a positive request timeout expires first
     * @throws FlowNotActiveOrNotFoundException if the target Flow has no active execution
     * @throws DexServiceException if Dex otherwise cannot complete the wait
     */
    public <T> T waitForAttributeMatch(
            final String flowId,
            final Attribute<T> attribute,
            final AttributeMatch<T> match,
            final WaitForAttributeOptions options) {
        return waitForAttributeValue(
                flowId, attribute, null, attribute.getValueType(), match, options);
    }

    /**
     * Blocks until an AttributeMap instance satisfies a scalar match.
     *
     * @param flowId the target Flow ID
     * @param attribute the registered Attribute-map definition
     * @param instance the map instance
     * @param match the scalar predicate to await
     * @param options the Request ID override and request/handler timeout controls
     * @param <T> the Attribute value type
     * @return the current AttributeMap value that satisfied the match
     * @throws IllegalArgumentException if the budget, match operand, or operator is invalid
     * @throws RequestTimeoutException if a positive request timeout expires first
     * @throws FlowNotActiveOrNotFoundException if the target Flow has no active execution
     * @throws DexServiceException if Dex otherwise cannot complete the wait
     */
    public <T> T waitForAttributeMatch(
            final String flowId,
            final AttributeMap<T> attribute,
            final String instance,
            final AttributeMatch<T> match,
            final WaitForAttributeOptions options) {
        return waitForAttributeValue(
                flowId, attribute, instance, attribute.getValueType(), match, options);
    }

    private <T> T waitForAttributeValue(
            final String flowId,
            final PersistenceDefinition attribute,
            final String instance,
            final Class<T> valueType,
            final AttributeMatch<T> match,
            final WaitForAttributeOptions options) {
        Objects.requireNonNull(match, "match");
        final Value encoded = values.encode(match.getOperand());
        validateAttributeMatch(match.getOperator(), encoded);
        final String key = instance == null
                ? attribute.getName()
                : Registry.physicalName(attribute.getName(), instance);
        final ClientRequestBudget requestBudget = new ClientRequestBudget(options.getRequestTimeout());
        final int internalHandlerTimeoutSeconds = seconds32(options.getInternalHandlerTimeout());
        while (true) {
            try {
                final ClientRequestAttempt requestAttempt = requestBudget.nextAttempt();
                final FlowServiceGrpc.FlowServiceBlockingStub requestService = requestAttempt.hasDeadline()
                        ? service.withDeadlineAfter(
                                requestAttempt.getRemainingNanos(), TimeUnit.NANOSECONDS)
                        : service;
                final WaitForAttributeResponse response = call(
                        () -> requestService.waitForAttribute(WaitForAttributeRequest.newBuilder()
                        .setFlowId(flowId)
                        .setMatch(io.superdurable.gen.AttributeMatch.newBuilder()
                                .setKey(key)
                                .setOperator(match.getOperator())
                                .setOperand(encoded))
                        .setRequestTimeoutSeconds(requestAttempt.getRemainingSeconds())
                        .setInternalHandlerTimeoutSeconds(internalHandlerTimeoutSeconds)
                        .setRequestId(options.getRequestId() == null ? "" : options.getRequestId())
                        .build()),
                        FlowTargetRequirement.ACTIVE,
                        flowId);
                if (!response.hasMatchedValue()) {
                    throw new IllegalStateException("waitForAttributeMatch response is incomplete");
                }
                return values.decode(response.getMatchedValue(), valueType);
            } catch (LongPollTimeoutException timeout) {
                // Reattach to the same logical wait.
            } catch (DexServiceException failure) {
                if (failure.getCode() == io.grpc.Status.Code.DEADLINE_EXCEEDED
                        && requestBudget.hasExpired()) {
                    throw new RequestTimeoutException(
                            io.grpc.Status.Code.DEADLINE_EXCEEDED,
                            "request timed out",
                            failure);
                }
                throw failure;
            }
        }
    }

    private static void validateAttributeMatch(
            final AttributeMatchOperator operator,
            final Value operand) {
        if (!isPrimitiveValue(operand)) {
            throw new IllegalArgumentException(
                    "AttributeMatch supports only string, Boolean, integer, or floating-point operands");
        }
        final boolean isOrdering =
                operator.getNumber()
                        >= AttributeMatchOperator.ATTRIBUTE_MATCH_OPERATOR_GREATER_THAN.getNumber();
        if (isOrdering
                && operand.getKindCase() != Value.KindCase.INT_VALUE
                && operand.getKindCase() != Value.KindCase.DOUBLE_VALUE) {
            throw new IllegalArgumentException(
                    "AttributeMatch ordering requires an integer or floating-point operand");
        }
        if (operand.getKindCase() == Value.KindCase.DOUBLE_VALUE
                && !Double.isFinite(operand.getDoubleValue())) {
            throw new IllegalArgumentException("AttributeMatch floating-point operand must be finite");
        }
    }

    private static boolean isPrimitiveValue(final Value value) {
        switch (value.getKindCase()) {
            case STRING_VALUE:
            case BOOL_VALUE:
            case INT_VALUE:
            case DOUBLE_VALUE:
                return true;
            default:
                return false;
        }
    }

    /**
     * Replaces the mutable Flow configuration for the current run.
     *
     * @param flowId the target Flow ID
     * @param config the new Flow configuration
     * @throws FlowNotActiveOrNotFoundException if the target Flow has no active execution
     * @throws DexServiceException if Dex otherwise cannot update the Flow
     */
    public void updateFlowConfig(final String flowId, final FlowConfig config) {
        call(
                () -> service.updateFlowConfig(UpdateFlowConfigRequest.newBuilder()
                        .setFlowId(flowId)
                        .setFlowConfig(mapFlowConfig(config))
                        .build()),
                FlowTargetRequirement.ACTIVE,
                flowId);
    }

    /**
     * Requests that the current Flow run continue as new.
     *
     * @param flowId the target Flow ID
     * @throws FlowNotActiveOrNotFoundException if the target Flow has no active execution
     * @throws DexServiceException if Dex otherwise cannot apply the request
     */
    public void triggerContinueAsNew(final String flowId) {
        call(
                () -> service.triggerContinueAsNew(
                        TriggerContinueAsNewRequest.newBuilder().setFlowId(flowId).build()),
                FlowTargetRequirement.ACTIVE,
                flowId);
    }

    boolean healthCheck() {
        call(() -> service.withDeadlineAfter(5, TimeUnit.SECONDS)
                .healthCheck(Empty.getDefaultInstance()));
        return true;
    }

    /**
     * Shuts down the client's gRPC channel.
     *
     * <p>The method waits up to five seconds for channel termination and preserves interruption
     * status. It does not close the borrowed {@link BlobCache}.
     */
    @Override
    public void close() {
        channel.shutdown();
        try {
            if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                channel.shutdownNow();
            }
        } catch (InterruptedException exception) {
            channel.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private Object invokeRpc(
            final RpcTarget target,
            final Method method,
            final Object input,
            final RPCInvokeOptions invokeOptions) {
        final Registry.RegisteredRpc rpc = target.flow.getRpcByMethod(method.getName());
        final List<String> lockAttributeKeys = mergePhysicalNames(
                rpc.getLocks(),
                physicalMapInstanceNames(
                        target.flow,
                        invokeOptions.getLockAttributeMapInstances(),
                        AttributeMap.class));
        final List<String> loadAttributeMapInstances = mergePhysicalNames(
                rpc.getStateLoads().getAttributeMaps(),
                physicalMapInstanceNames(
                        target.flow,
                        invokeOptions.getLoadAttributeMapInstances(),
                        AttributeMap.class));
        final List<String> loadChannelMapInstances = mergePhysicalNames(
                rpc.getStateLoads().getChannelMaps(),
                physicalMapInstanceNames(
                        target.flow,
                        invokeOptions.getLoadChannelMapInstances(),
                        ChannelMap.class));
        final InvokeRPCRequest request = InvokeRPCRequest.newBuilder()
                .setFlowId(target.flowId)
                .setRunId(target.runId)
                .setRpcName(rpc.getName())
                .setInput(values.encode(input))
                .setTimeoutSeconds(rpc.getAnnotation().timeoutSeconds())
                .addAllLockAttributeKeys(lockAttributeKeys)
                .setRequestId(UUID.randomUUID().toString())
                .setIsTransactional(rpc.getAnnotation().isTransactional())
                .addAllLoadAttributeMapInstances(loadAttributeMapInstances)
                .addAllLoadChannelNames(rpc.getStateLoads().getChannels())
                .addAllLoadChannelMapInstances(loadChannelMapInstances)
                .build();
        final io.superdurable.gen.Value output = hydrator.hydrate(
                target.flowId,
                call(
                        () -> service.invokeRPC(request),
                        FlowTargetRequirement.ACTIVE,
                        target.flowId).getOutput());
        if (rpc.getMethod().getReturnType() == Void.TYPE) {
            return null;
        }
        final Type returnType = rpc.getMethod().getGenericReturnType();
        final Type outputType = ((ParameterizedType) returnType).getActualTypeArguments()[0];
        if (!(outputType instanceof Class)) {
            throw new FlowDefinitionException("RPC output must be a concrete Class");
        }
        return values.decode(output, (Class<?>) outputType);
    }

    private <T> T withRPCInvokeOptions(
            final RPCInvokeOptions invokeOptions,
            final RpcInvocation<T> invocation) {
        if (invokeOptions == null) {
            throw new IllegalArgumentException("invokeOptions is required");
        }
        final RPCInvokeOptions previous = rpcInvokeOptions.get();
        rpcInvokeOptions.set(invokeOptions);
        try {
            return invocation.invoke();
        } finally {
            if (previous == null) {
                rpcInvokeOptions.remove();
            } else {
                rpcInvokeOptions.set(previous);
            }
        }
    }

    private static List<String> physicalMapInstanceNames(
            final Registry.RegisteredFlow flow,
            final List<RPCInvokeOptions.MapInstance> selections,
            final Class<? extends PersistenceDefinition> expectedType) {
        final List<String> names = new ArrayList<String>();
        for (RPCInvokeOptions.MapInstance selection : selections) {
            final PersistenceDefinition definition = selection.getDefinition();
            final PersistenceDefinition registered = flow.getPersistence().get(definition.getName());
            if (registered != definition || !expectedType.isInstance(definition)) {
                throw new FlowDefinitionException(
                        "Flow " + flow.getName() + " does not register "
                                + expectedType.getSimpleName() + " " + definition.getName());
            }
            names.add(Registry.physicalName(definition.getName(), selection.getInstance()));
        }
        return names;
    }

    private static List<String> mergePhysicalNames(
            final List<String> registeredNames,
            final List<String> invocationNames) {
        final TreeSet<String> merged = new TreeSet<String>();
        merged.addAll(registeredNames);
        merged.addAll(invocationNames);
        return new ArrayList<String>(merged);
    }

    private io.superdurable.gen.FlowResult waitForFlowResponse(
            final String flowId,
            final Duration timeout) {
        final WaitForFlowRequest.Builder request = WaitForFlowRequest.newBuilder()
                .setFlowId(flowId)
                .setNeedsResults(true);
        if (timeout != null) {
            request.setWaitTimeSeconds(seconds32(timeout));
        }
        return call(
                () -> service.waitForFlow(request.build()),
                FlowTargetRequirement.EXISTING,
                flowId);
    }

    private List<StepCompletion> mapStepCompletions(
            final String flowId,
            final List<io.superdurable.gen.StepCompletionOutput> outputs) {
        final List<StepCompletion> completions = new ArrayList<StepCompletion>();
        for (io.superdurable.gen.StepCompletionOutput completion
                : hydrator.hydrateStepOutputs(flowId, outputs)) {
            completions.add(new StepCompletion(
                    completion,
                    (value, outputType) -> values.decode(value, outputType)));
        }
        return completions;
    }

    FlowStartOptions mapStartOptions(final StartFlowOptions options) {
        final FlowStartOptions.Builder mapped = FlowStartOptions.newBuilder()
                .setIdReusePolicy(mapIdReuse(options.getIdReusePolicy()))
                .setFlowAlreadyStartedOptions(FlowAlreadyStartedOptions.newBuilder()
                        .setIgnoreAlreadyStartedError(options.isIgnoreAlreadyStarted()));
        if (options.getStartDelay() != null) {
            mapped.setFlowStartDelaySeconds(seconds32(options.getStartDelay()));
        }
        if (options.getRetryPolicy() != null) {
            mapped.setRetryPolicy(mapFlowRetry(options.getRetryPolicy()));
        }
        for (StartFlowOptions.AttributeInitialization initialization : options.getAttributes()) {
            final PersistenceDefinition definition = initialization.getDefinition();
            final String key = initialization.getInstance() == null
                    ? definition.getName()
                    : Registry.physicalName(definition.getName(), initialization.getInstance());
            final AttributeWrite.Builder write = AttributeWrite.newBuilder()
                    .setKey(key)
                    .setValue(values.encode(initialization.getValue()));
            applyAttributeSync(write, definition);
            mapped.addAttributes(write);
        }
        final FlowConfig config = options.getConfigOverride();
        if (config != null || this.options.getWorkerTarget() != null) {
            mapped.setFlowConfigOverride(mapFlowConfig(config));
        }
        return mapped.build();
    }

    io.superdurable.gen.FlowConfig mapFlowConfig(final FlowConfig config) {
        final io.superdurable.gen.FlowConfig.Builder mapped =
                io.superdurable.gen.FlowConfig.newBuilder();
        if (config != null) {
            if (config.getActiveStepSearchMode() != null) {
                mapped.setActiveStepSearchMode(mapSearchMode(config.getActiveStepSearchMode()));
            }
            if (config.getContinueAsNewThreshold() != null) {
                mapped.setContinueAsNewThreshold(config.getContinueAsNewThreshold());
            }
            if (config.getContinueAsNewPageSizeBytes() != null) {
                mapped.setContinueAsNewPageSizeInBytes(
                        config.getContinueAsNewPageSizeBytes());
            }
            if (config.getStepDurability() != null) {
                mapped.setStepDurability(mapDurability(config.getStepDurability()));
            }
            if (config.getAttributeStoreNames() != null) {
                mapped.setAttributeStoreNames(io.superdurable.gen.AttributeStoreNames.newBuilder()
                        .addAllNames(config.getAttributeStoreNames()));
            }
        }
        final WorkerTarget target = config != null && config.getWorkerTarget() != null
                ? config.getWorkerTarget()
                : options.getWorkerTarget();
        if (target != null) {
            mapped.setWorkerTarget(io.superdurable.gen.WorkerTarget.newBuilder()
                    .setAddress(target.getAddress())
                    .setIsHeadlessAddress(target.isHeadless()));
        }
        return mapped.build();
    }

    private static void applyAttributeSync(
            final AttributeWrite.Builder write,
            final PersistenceDefinition definition) {
        if (definition.isSyncToAttributeStore()) {
            write.setSyncConfig(AttributeSyncConfig.newBuilder().setEnabled(true));
        }
    }

    private static io.superdurable.gen.FlowRetryPolicy mapFlowRetry(final RetryPolicy retry) {
        final io.superdurable.gen.FlowRetryPolicy.Builder mapped =
                io.superdurable.gen.FlowRetryPolicy.newBuilder()
                        .setBackoffCoefficient((float) retry.getBackoffCoefficient())
                        .setMaximumAttempts(retry.getMaximumAttempts());
        if (retry.getInitialInterval() != null) {
            mapped.setInitialIntervalSeconds(seconds32(retry.getInitialInterval()));
        }
        if (retry.getMaximumInterval() != null) {
            mapped.setMaximumIntervalSeconds(seconds32(retry.getMaximumInterval()));
        }
        return mapped.build();
    }

    private static int seconds32(final Duration duration) {
        if (duration == null || duration.isNegative() || duration.getNano() != 0
                || duration.getSeconds() > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Duration must be whole seconds within int32");
        }
        return (int) duration.getSeconds();
    }

    private static final class ClientRequestBudget {
        private final long deadlineNanos;

        private ClientRequestBudget(final Duration requestTimeout) {
            final int requestTimeoutSeconds = seconds32(requestTimeout);
            deadlineNanos = requestTimeoutSeconds == 0
                    ? 0
                    : System.nanoTime() + requestTimeout.toNanos();
        }

        private ClientRequestAttempt nextAttempt() {
            if (deadlineNanos == 0) {
                return new ClientRequestAttempt(0, 0);
            }
            final long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0) {
                throw new RequestTimeoutException(
                        io.grpc.Status.Code.DEADLINE_EXCEEDED,
                        "request timed out",
                        null);
            }
            final int remainingSeconds = (int) Math.min(
                    Integer.MAX_VALUE,
                    (remainingNanos + TimeUnit.SECONDS.toNanos(1) - 1)
                            / TimeUnit.SECONDS.toNanos(1));
            return new ClientRequestAttempt(remainingSeconds, remainingNanos);
        }

        private boolean hasExpired() {
            return deadlineNanos != 0 && System.nanoTime() >= deadlineNanos;
        }
    }

    private static final class ClientRequestAttempt {
        private final int remainingSeconds;
        private final long remainingNanos;

        private ClientRequestAttempt(final int remainingSeconds, final long remainingNanos) {
            this.remainingSeconds = remainingSeconds;
            this.remainingNanos = remainingNanos;
        }

        private int getRemainingSeconds() {
            return remainingSeconds;
        }

        private long getRemainingNanos() {
            return remainingNanos;
        }

        private boolean hasDeadline() {
            return remainingNanos != 0;
        }
    }

    private static Instant instant(final Timestamp timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }

    private static io.superdurable.gen.IdReusePolicy mapIdReuse(final IdReusePolicy policy) {
        switch (policy) {
            case ALLOW_IF_PREVIOUS_FAILED:
                return io.superdurable.gen.IdReusePolicy
                        .ID_REUSE_POLICY_ALLOW_IF_PREVIOUS_EXISTS_ABNORMALLY;
            case ALLOW_IF_NOT_RUNNING:
                return io.superdurable.gen.IdReusePolicy.ID_REUSE_POLICY_ALLOW_IF_NO_RUNNING;
            case DISALLOW:
                return io.superdurable.gen.IdReusePolicy.ID_REUSE_POLICY_DISALLOW_REUSE;
            case TERMINATE_IF_RUNNING:
                return io.superdurable.gen.IdReusePolicy
                        .ID_REUSE_POLICY_ALLOW_TERMINATE_IF_RUNNING;
            default:
                return io.superdurable.gen.IdReusePolicy.ID_REUSE_POLICY_UNSPECIFIED;
        }
    }

    private static io.superdurable.gen.StopType mapStopType(final StopType type) {
        switch (type) {
            case TERMINATE:
                return io.superdurable.gen.StopType.STOP_TYPE_TERMINATE;
            case FAIL:
                return io.superdurable.gen.StopType.STOP_TYPE_FAIL;
            default:
                return io.superdurable.gen.StopType.STOP_TYPE_CANCEL;
        }
    }

    private static FlowResetType mapTimeTravelType(final TimeTravelType type) {
        switch (type) {
            case BEGINNING:
                return FlowResetType.FLOW_RESET_TYPE_BEGINNING;
            case HISTORY_EVENT_TIME:
                return FlowResetType.FLOW_RESET_TYPE_HISTORY_EVENT_TIME;
            case STEP_TYPE:
                return FlowResetType.FLOW_RESET_TYPE_STEP_TYPE;
            case STEP_EXECUTION_ID:
                return FlowResetType.FLOW_RESET_TYPE_STEP_EXECUTION_ID;
            default:
                return FlowResetType.FLOW_RESET_TYPE_UNSPECIFIED;
        }
    }

    private static FlowResetStepMethod mapTimeTravelStepMethod(final TimeTravelStepMethod method) {
        switch (method) {
            case WAIT_FOR:
                return FlowResetStepMethod.FLOW_RESET_STEP_METHOD_WAIT_FOR;
            case EXECUTE:
                return FlowResetStepMethod.FLOW_RESET_STEP_METHOD_EXECUTE;
            default:
                return FlowResetStepMethod.FLOW_RESET_STEP_METHOD_UNSPECIFIED;
        }
    }

    static FlowStatus mapFlowStatus(final io.superdurable.gen.FlowStatus status) {
        switch (status) {
            case FLOW_STATUS_RUNNING:
                return FlowStatus.RUNNING;
            case FLOW_STATUS_COMPLETED:
                return FlowStatus.COMPLETED;
            case FLOW_STATUS_FAILED:
                return FlowStatus.FAILED;
            case FLOW_STATUS_SERVER_SIDE_TIMEOUT_INTERNAL_ONLY:
                return FlowStatus.SERVER_SIDE_TIMEOUT_INTERNAL_ONLY;
            case FLOW_STATUS_TERMINATED:
                return FlowStatus.TERMINATED;
            case FLOW_STATUS_CANCELED:
                return FlowStatus.CANCELED;
            case FLOW_STATUS_CONTINUED_AS_NEW:
                return FlowStatus.CONTINUED_AS_NEW;
            default:
                throw new IllegalArgumentException("unknown Flow status " + status);
        }
    }

    static FlowErrorType mapNullableFlowErrorType(
            final io.superdurable.gen.FlowErrorType type) {
        switch (type) {
            case FLOW_ERROR_TYPE_STEP_DECISION_FAILING_FLOW:
                return FlowErrorType.STEP_DECISION_FAILED;
            case FLOW_ERROR_TYPE_CLIENT_API_FAILING_FLOW:
                return FlowErrorType.CLIENT_API_FAILED;
            case FLOW_ERROR_TYPE_WORKER_API_FAIL:
                return FlowErrorType.WORKER_API_FAILED;
            case FLOW_ERROR_TYPE_INVALID_USER_FLOW_CODE:
                return FlowErrorType.INVALID_USER_FLOW_CODE;
            case FLOW_ERROR_TYPE_INTERNAL:
                return FlowErrorType.INTERNAL;
            case FLOW_ERROR_TYPE_FLOW_TIMEOUT:
                return FlowErrorType.FLOW_TIMEOUT;
            case FLOW_ERROR_TYPE_SUB_FLOW_START_FAILED:
                return FlowErrorType.SUB_FLOW_START_FAILED;
            default:
                return null;
        }
    }

    private static <T> T call(final RpcCall<T> rpc) {
        return call(rpc, FlowTargetRequirement.NONE, null);
    }

    private static <T> T call(
            final RpcCall<T> rpc,
            final FlowTargetRequirement requirement,
            final String flowId) {
        try {
            return rpc.invoke();
        } catch (StatusRuntimeException exception) {
            throw GrpcExceptionTranslator.translate(exception, requirement, flowId);
        }
    }

    private static io.superdurable.gen.ActiveStepSearchMode mapSearchMode(
            final ActiveStepSearchMode mode) {
        switch (mode) {
            case ALL:
                return io.superdurable.gen.ActiveStepSearchMode
                        .ACTIVE_STEP_SEARCH_MODE_ENABLED_FOR_ALL;
            case WITH_WAIT_FOR:
                return io.superdurable.gen.ActiveStepSearchMode
                        .ACTIVE_STEP_SEARCH_MODE_ENABLED_FOR_STEPS_WITH_WAIT_FOR;
            case DISABLED:
                return io.superdurable.gen.ActiveStepSearchMode
                        .ACTIVE_STEP_SEARCH_MODE_DISABLED;
            default:
                return io.superdurable.gen.ActiveStepSearchMode
                        .ACTIVE_STEP_SEARCH_MODE_UNSPECIFIED;
        }
    }

    private static io.superdurable.gen.StepDurability mapDurability(
            final StepDurability durability) {
        if (durability == StepDurability.SYNC) {
            return io.superdurable.gen.StepDurability.STEP_DURABILITY_SYNC;
        }
        if (durability == StepDurability.ASYNC) {
            return io.superdurable.gen.StepDurability.STEP_DURABILITY_ASYNC;
        }
        return io.superdurable.gen.StepDurability.STEP_DURABILITY_UNSPECIFIED;
    }

    private final class RpcInvocationHandler implements InvocationHandler {
        private final RpcTarget target;

        private RpcInvocationHandler(final RpcTarget target) {
            this.target = target;
        }

        @Override
        public Object invoke(
                final Object stub,
                final Method method,
                final Object[] arguments) {
            final Object input = arguments.length == 2 ? arguments[1] : null;
            final RPCInvokeOptions invokeOptions = rpcInvokeOptions.get() == null
                    ? RPCInvokeOptions.newBuilder().build()
                    : rpcInvokeOptions.get();
            final Object output = invokeRpc(target, method, input, invokeOptions);
            return method.getReturnType() == Void.TYPE ? null : RPCResult.of(output);
        }
    }

    private interface RpcInvocation<T> {
        T invoke();
    }

    private static final class RpcTarget {
        private final Registry.RegisteredFlow flow;
        private final String flowId;
        private final String runId;

        private RpcTarget(
                final Registry.RegisteredFlow flow,
                final String flowId,
                final String runId) {
            this.flow = flow;
            this.flowId = flowId;
            this.runId = runId;
        }
    }

    private interface RpcCall<T> {
        T invoke();
    }
}
