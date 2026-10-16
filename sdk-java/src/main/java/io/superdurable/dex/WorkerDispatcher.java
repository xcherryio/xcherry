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

import io.superdurable.dex.exceptions.FlowDefinitionException;
import io.superdurable.dex.exceptions.InvalidStepResultException;
import io.superdurable.gen.ChannelCondition;
import io.superdurable.gen.AttributeWrite;
import io.superdurable.gen.CloseDecision;
import io.superdurable.gen.CloseDecisionType;
import io.superdurable.gen.ConditionCombination;
import io.superdurable.gen.ExecuteMethodFailurePolicy;
import io.superdurable.gen.InvokeExecuteMethodRequest;
import io.superdurable.gen.InvokeExecuteMethodResponse;
import io.superdurable.gen.InvokeWaitForMethodRequest;
import io.superdurable.gen.InvokeWaitForMethodResponse;
import io.superdurable.gen.InvokeWorkerRPCRequest;
import io.superdurable.gen.InvokeWorkerRPCResponse;
import io.superdurable.gen.StepMovement;
import io.superdurable.gen.SubFlowCondition;
import io.superdurable.gen.TimerCondition;
import io.superdurable.gen.WaitForMethodFailurePolicy;
import io.superdurable.gen.WaitingCondition;
import io.superdurable.gen.WaitingConditionType;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

final class WorkerDispatcher {
    private static final String TIMEOUT_HANDLER_STEP_TYPE = "sys:timeout_handler";

    private final Registry registry;
    private final ValueMapper values;
    private final ValueHydrator hydrator;
    private final ScheduledExecutorService bufferedStreamScheduler;
    private final NegotiatedServerProtocol serverProtocol;

    WorkerDispatcher(
            final Registry registry,
            final ValueMapper values,
            final ValueHydrator hydrator,
            final NegotiatedServerProtocol serverProtocol) {
        this(registry, values, hydrator, null, serverProtocol);
    }

    WorkerDispatcher(
            final Registry registry,
            final ValueMapper values,
            final ValueHydrator hydrator,
            final ScheduledExecutorService bufferedStreamScheduler,
            final NegotiatedServerProtocol serverProtocol) {
        if (serverProtocol == null) {
            throw new IllegalArgumentException("serverProtocol is required");
        }
        this.registry = registry;
        this.values = values;
        this.hydrator = hydrator;
        this.bufferedStreamScheduler = bufferedStreamScheduler;
        this.serverProtocol = serverProtocol;
    }

    InvokeWaitForMethodResponse invokeWaitFor(
            final InvokeWaitForMethodRequest original,
            final StepOutputEmitter stepOutputEmitter) throws Throwable {
        final InvokeWaitForMethodRequest request = hydrator.hydrate(original);
        final Registry.RegisteredFlow flow = registry.getFlow(request.getFlowType());
        final Registry.RegisteredStep step = flow.getStep(request.getStepType());
        final InvocationContext context = new InvocationContext(
                InvocationContext.Method.WAIT_FOR,
                flow,
                request.getContext(),
                values,
                stepOutputEmitter,
                request.getAttributesList(),
                null,
                null,
                request.getChannelInfosMap(),
                bufferedStreamScheduler,
                request.getLoadedChannelMessagesMap(),
                request.getLoadedAttributeMapInstancesList(),
                request.getLoadedChannelNamesList(),
                request.getLoadedChannelMapInstancesList());
        final Object input = values.decode(request.getStepInput(), step.getStep().getInputType());
        try {
            final Wait wait = callWaitFor(step.getStep(), context, input);
            serverProtocol.requireHardDeleteAttributeSupport(context.getAttributeWrites());
            final InvokeWaitForMethodResponse.Builder response =
                    InvokeWaitForMethodResponse.newBuilder()
                            .addAllUpsertAttributes(context.getAttributeWrites())
                            .addAllUpsertStepExeLocals(context.getLocalWrites())
                            .addAllRecordEvents(context.getEvents())
                            .addAllPublishToChannel(context.getPublications())
                            .addAllDeleteFromChannel(context.getChannelDeletions());
            final WaitingCondition waiting = mapWait(flow, step, wait);
            if (waiting != null) {
                response.setWaitingCondition(waiting);
            }
            final Throwable finalizationFailure = context.finalizeStepOutputs(null);
            if (finalizationFailure != null) {
                throw finalizationFailure;
            }
            return response.build();
        } catch (Throwable failure) {
            throw context.finalizeStepOutputs(failure);
        }
    }

    InvokeExecuteMethodResponse invokeExecute(
            final InvokeExecuteMethodRequest original,
            final StepOutputEmitter stepOutputEmitter) throws Throwable {
        final InvokeExecuteMethodRequest request = hydrator.hydrate(original);
        final Registry.RegisteredFlow flow = registry.getFlow(request.getFlowType());
        if (TIMEOUT_HANDLER_STEP_TYPE.equals(request.getStepType())) {
            return invokeTimeoutHandler(request, flow, stepOutputEmitter);
        }
        final Registry.RegisteredStep step = flow.getStep(request.getStepType());
        final InvocationContext context = new InvocationContext(
                InvocationContext.Method.EXECUTE,
                flow,
                request.getContext(),
                values,
                stepOutputEmitter,
                request.getAttributesList(),
                request.getStepExeLocalsList(),
                request.hasConditionResults() ? request.getConditionResults() : null,
                request.getChannelInfosMap(),
                bufferedStreamScheduler,
                request.getLoadedChannelMessagesMap(),
                request.getLoadedAttributeMapInstancesList(),
                request.getLoadedChannelNamesList(),
                request.getLoadedChannelMapInstancesList());
        final Object input = values.decode(request.getStepInput(), step.getStep().getInputType());
        try {
            final StepDecision decision = callExecute(step.getStep(), context, input);
            serverProtocol.requireHardDeleteAttributeSupport(context.getAttributeWrites());
            final InvokeExecuteMethodResponse response = InvokeExecuteMethodResponse.newBuilder()
                    .setStepDecision(mapDecision(flow, step, decision))
                    .addAllUpsertAttributes(context.getAttributeWrites())
                    .addAllRecordEvents(context.getEvents())
                    .addAllUpsertStepExeLocals(context.getLocalWrites())
                    .addAllPublishToChannel(context.getPublications())
                    .addAllDeleteFromChannel(context.getChannelDeletions())
                    .build();
            final Throwable finalizationFailure = context.finalizeStepOutputs(null);
            if (finalizationFailure != null) {
                throw finalizationFailure;
            }
            return response;
        } catch (Throwable failure) {
            throw context.finalizeStepOutputs(failure);
        }
    }

    private InvokeExecuteMethodResponse invokeTimeoutHandler(
            final InvokeExecuteMethodRequest request,
            final Registry.RegisteredFlow flow,
            final StepOutputEmitter stepOutputEmitter) {
        if (request.hasStepInput()) {
            throw new InvalidStepResultException("Flow timeout handler must not receive input");
        }
        if (!flow.hasTimeoutHandler()) {
            throw new FlowDefinitionException(
                    "Flow " + flow.getName() + " does not override handleTimeout");
        }
        final InvocationContext context = new InvocationContext(
                InvocationContext.Method.EXECUTE,
                flow,
                request.getContext(),
                values,
                stepOutputEmitter,
                request.getAttributesList(),
                request.getStepExeLocalsList(),
                request.hasConditionResults() ? request.getConditionResults() : null,
                request.getChannelInfosMap(),
                bufferedStreamScheduler,
                request.getLoadedChannelMessagesMap(),
                request.getLoadedAttributeMapInstancesList(),
                request.getLoadedChannelNamesList(),
                request.getLoadedChannelMapInstancesList());
        final StepDecision decision = flow.getFlow().handleTimeout(context);
        serverProtocol.requireHardDeleteAttributeSupport(context.getAttributeWrites());
        return InvokeExecuteMethodResponse.newBuilder()
                .setStepDecision(mapDecision(
                        flow,
                        "Flow " + flow.getName() + " timeout handler",
                        decision))
                .addAllUpsertAttributes(context.getAttributeWrites())
                .addAllRecordEvents(context.getEvents())
                .addAllUpsertStepExeLocals(context.getLocalWrites())
                .addAllPublishToChannel(context.getPublications())
                .addAllDeleteFromChannel(context.getChannelDeletions())
                .build();
    }

    InvokeWorkerRPCResponse invokeRpc(final InvokeWorkerRPCRequest original) throws Throwable {
        final InvokeWorkerRPCRequest request = hydrator.hydrate(original);
        final Registry.RegisteredFlow flow = registry.getFlow(request.getFlowType());
        final Registry.RegisteredRpc rpc = flow.getRpc(request.getRpcName());
        final InvocationContext context = new InvocationContext(
                InvocationContext.Method.RPC,
                flow,
                request.getContext(),
                values,
                null,
                request.getAttributesList(),
                null,
                null,
                request.getChannelInfosMap(),
                bufferedStreamScheduler,
                request.getLoadedChannelMessagesMap(),
                request.getLoadedAttributeMapInstancesList(),
                request.getLoadedChannelNamesList(),
                request.getLoadedChannelMapInstancesList());
        final Method method = rpc.getMethod();
        final Object[] arguments;
        if (method.getParameterTypes().length == 2) {
            arguments = new Object[] {
                    context,
                    values.decode(request.getInput(), method.getParameterTypes()[1])
            };
        } else {
            arguments = new Object[] {context};
        }
        final Object returned = invoke(flow.getFlow(), method, arguments);
        serverProtocol.requireHardDeleteAttributeSupport(context.getAttributeWrites());
        final InvokeWorkerRPCResponse.Builder response = InvokeWorkerRPCResponse.newBuilder()
                .addAllUpsertAttributes(context.getAttributeWrites())
                .addAllRecordEvents(context.getEvents())
                .addAllPublishToChannel(context.getPublications())
                .addAllDeleteFromChannel(context.getChannelDeletions());
        if (returned instanceof RPCResult) {
            final RPCResult<?> result = (RPCResult<?>) returned;
            response.setOutput(values.encode(result.getOutput()));
            final String source = "Flow " + flow.getName() + " RPC " + rpc.getName();
            final io.superdurable.gen.StepDecision.Builder decision =
                    io.superdurable.gen.StepDecision.newBuilder()
                            .addAllNextSteps(mapMovements(flow, result.getNextSteps()))
                            .addAllCancelStepTypes(mapCancellationSteps(
                                    flow, source, result.getCancelingSteps()));
            if (decision.getNextStepsCount() > 0
                    || decision.getCancelStepTypesCount() > 0) {
                response.setStepDecision(decision);
            }
        } else {
            response.setOutput(values.encode(null));
        }
        return response.build();
    }

    @SuppressWarnings("unchecked")
    private static Wait callWaitFor(
            final Step<?> step,
            final InvocationContext context,
            final Object input) {
        return ((Step<Object>) step).waitFor(context, input);
    }

    @SuppressWarnings("unchecked")
    private static StepDecision callExecute(
            final Step<?> step,
            final InvocationContext context,
            final Object input) {
        return ((Step<Object>) step).execute(context, input);
    }

    private static Object invoke(
            final Object target,
            final Method method,
            final Object[] arguments) throws Throwable {
        try {
            return method.invoke(target, arguments);
        } catch (IllegalAccessException exception) {
            throw new IllegalStateException("cannot invoke RPC " + method.getName(), exception);
        } catch (InvocationTargetException exception) {
            throw exception.getCause();
        }
    }

    private WaitingCondition mapWait(
            final Registry.RegisteredFlow flow,
            final Registry.RegisteredStep step,
            final Wait wait) {
        final String source = "Flow " + flow.getName() + " Step " + step.getName();
        if (wait == null) {
            throw new InvalidStepResultException(source + " waitFor returned null");
        }
        if (wait.getKind() == Wait.Kind.SKIP_IMMEDIATELY) {
            return null;
        }
        final ConditionMapper mapper = new ConditionMapper(flow);
        final WaitingCondition.Builder waiting = WaitingCondition.newBuilder();
        if (wait.getKind() == Wait.Kind.ALL_OF) {
            waiting.setWaitingConditionType(
                    WaitingConditionType.WAITING_CONDITION_TYPE_ALL_COMPLETED);
            addConditions(source, mapper, wait.getConditions());
        } else if (wait.getKind() == Wait.Kind.ANY_OF) {
            waiting.setWaitingConditionType(
                    WaitingConditionType.WAITING_CONDITION_TYPE_ANY_COMPLETED);
            addConditions(source, mapper, wait.getConditions());
        } else if (wait.getKind() == Wait.Kind.ANY_COMBINATION_OF) {
            waiting.setWaitingConditionType(
                    WaitingConditionType.WAITING_CONDITION_TYPE_ANY_COMBINATION_COMPLETED);
            for (io.superdurable.dex.ConditionCombination combination
                    : wait.getCombinations()) {
                final ConditionCombination.Builder mapped = ConditionCombination.newBuilder();
                for (Condition condition : combination.getConditions()) {
                    mapped.addConditionIds(mapper.add(condition, true));
                }
                waiting.addConditionCombinations(mapped);
            }
        } else {
            throw new InvalidStepResultException(source + " returned an unsupported Wait kind");
        }
        waiting.addAllTimerConditions(mapper.timers);
        waiting.addAllChannelConditions(mapper.channels);
        waiting.addAllSubFlowConditions(mapper.subFlows);
        return waiting.build();
    }

    private static void addConditions(
            final String source,
            final ConditionMapper mapper,
            final List<Condition> conditions) {
        if (conditions.isEmpty()) {
            throw new InvalidStepResultException(
                    source + " Wait requires at least one Condition");
        }
        for (Condition condition : conditions) {
            mapper.add(condition, false);
        }
    }

    private io.superdurable.gen.StepDecision mapDecision(
            final Registry.RegisteredFlow flow,
            final Registry.RegisteredStep step,
            final StepDecision decision) {
        final String source = "Flow " + flow.getName() + " Step " + step.getName();
        return mapDecision(flow, source, decision);
    }

    private io.superdurable.gen.StepDecision mapDecision(
            final Registry.RegisteredFlow flow,
            final String source,
            final StepDecision decision) {
        if (decision == null) {
            throw new InvalidStepResultException(source + " execute returned null");
        }
        final io.superdurable.gen.StepDecision.Builder mapped =
                io.superdurable.gen.StepDecision.newBuilder();
        switch (decision.getKind()) {
            case NEXT:
                if (decision.getMovements().isEmpty()) {
                    throw new InvalidStepResultException(
                            source + " goToMany requires a movement");
                }
                mapped.addAllNextSteps(mapMovements(flow, decision.getMovements()));
                break;
            case GRACEFUL_COMPLETE:
                mapped.setCloseDecision(close(
                        CloseDecisionType.CLOSE_DECISION_TYPE_GRACEFUL_COMPLETE,
                        decision.hasOutput(), decision.getOutput()));
                break;
            case FORCE_COMPLETE:
                mapped.setCloseDecision(close(
                        CloseDecisionType.CLOSE_DECISION_TYPE_FORCE_COMPLETE,
                        decision.hasOutput(), decision.getOutput()));
                break;
            case FORCE_FAIL:
                mapped.setCloseDecision(close(
                        CloseDecisionType.CLOSE_DECISION_TYPE_FORCE_FAIL,
                        true, decision.getReason()));
                break;
            case DEAD_END:
                mapped.setCloseDecision(CloseDecision.newBuilder()
                        .setCloseDecisionType(
                                CloseDecisionType.CLOSE_DECISION_TYPE_DEAD_END));
                break;
            case FORCE_COMPLETE_IF_CHANNELS_EMPTY:
                final CloseDecision.Builder conditional = CloseDecision.newBuilder()
                        .setCloseDecisionType(
                                CloseDecisionType
                                        .CLOSE_DECISION_TYPE_FORCE_COMPLETE_ON_CHANNELS_EMPTY)
                        .setCloseInput(values.encode(decision.getOutput()));
                for (Object channel : decision.getEmptyChannels()) {
                    if (!(channel instanceof Channel)) {
                        throw new InvalidStepResultException(
                                source + " conditional close requires static Channels");
                    }
                    conditional.addConditionalChannelNames(((Channel<?>) channel).getName());
                }
                mapped.setCloseDecision(conditional);
                mapped.addNextSteps(mapMovement(flow, decision.getFallback()));
                break;
            default:
                throw new InvalidStepResultException(
                        source + " returned an unsupported StepDecision kind");
        }
        mapped.addAllCancelStepTypes(mapCancellationSteps(
                flow, source, decision.getCancelingSteps()));
        mapped.addAllCancelSiblingStepTypes(mapCancellationSteps(
                flow, source, decision.getCancelingSiblingSteps()));
        return mapped.build();
    }

    private List<String> mapCancellationSteps(
            final Registry.RegisteredFlow flow,
            final String source,
            final List<Class<? extends Step<?>>> stepClasses) {
        final List<String> mapped = new ArrayList<String>(stepClasses.size());
        for (Class<? extends Step<?>> stepClass : stepClasses) {
            if (stepClass == null) {
                throw new InvalidStepResultException(
                        source + " cancellation Step is required");
            }
            final Registry.RegisteredStep registered;
            try {
                registered = flow.getStep(stepClass);
            } catch (FlowDefinitionException exception) {
                throw new InvalidStepResultException(
                        source + " cancellation Step does not belong to Flow");
            }
            mapped.add(registered.getName());
        }
        return mapped;
    }

    private CloseDecision close(
            final CloseDecisionType type,
            final boolean hasOutput,
            final Object output) {
        final CloseDecision.Builder close = CloseDecision.newBuilder()
                .setCloseDecisionType(type);
        if (hasOutput) {
            close.setCloseInput(values.encode(output));
        }
        return close.build();
    }

    private List<StepMovement> mapMovements(
            final Registry.RegisteredFlow flow,
            final List<io.superdurable.dex.StepMovement<?>> movements) {
        final List<StepMovement> mapped = new ArrayList<StepMovement>(movements.size());
        for (io.superdurable.dex.StepMovement<?> movement : movements) {
            mapped.add(mapMovement(flow, movement));
        }
        return mapped;
    }

    private StepMovement mapMovement(
            final Registry.RegisteredFlow flow,
            final io.superdurable.dex.StepMovement<?> movement) {
        if (movement == null) {
            throw new InvalidStepResultException(
                    "Flow " + flow.getName() + " Step movement is required");
        }
        final Registry.RegisteredStep target;
        try {
            target = flow.getStep(movement.getStepClass());
        } catch (FlowDefinitionException exception) {
            throw new InvalidStepResultException(
                    "Flow " + flow.getName() + " Step movement target does not belong to Flow");
        }
        final StepMovement.Builder mapped = StepMovement.newBuilder()
                .setStepType(target.getName())
                .setStepInput(values.encode(movement.getInput()));
        final io.superdurable.gen.StepOptions options = mapStepOptions(
                flow,
                movement.getOptions() == null
                        ? target.getStep().getStepOptions()
                        : movement.getOptions());
        if (options != null || target.skipsWaitFor()) {
            final io.superdurable.gen.StepOptions.Builder mappedOptions = options == null
                    ? io.superdurable.gen.StepOptions.newBuilder()
                    : options.toBuilder();
            mappedOptions.setSkipWaitFor(target.skipsWaitFor());
            mapped.setStepOptions(mappedOptions);
        }
        return mapped.build();
    }

    io.superdurable.gen.StepOptions mapStepOptions(
            final Registry.RegisteredFlow flow,
            final StepOptions options) {
        if (options == null) {
            return null;
        }
        final io.superdurable.gen.StepOptions.Builder mapped =
                io.superdurable.gen.StepOptions.newBuilder();
        if (options.getWaitForMethodTimeout() != null) {
            mapped.setWaitForTimeoutSeconds(seconds32(options.getWaitForMethodTimeout()));
        }
        if (options.getExecuteMethodTimeout() != null) {
            mapped.setExecuteTimeoutSeconds(seconds32(options.getExecuteMethodTimeout()));
        }
        if (options.getHeartbeatTimeout() != null
                && !options.getHeartbeatTimeout().isZero()) {
            mapped.setHeartbeatTimeoutSeconds(
                    positiveSeconds32(options.getHeartbeatTimeout()));
        }
        if (options.getWaitForRetry() != null) {
            mapped.setWaitForRetryPolicy(mapRetry(options.getWaitForRetry()));
        }
        if (options.getExecuteRetry() != null) {
            mapped.setExecuteRetryPolicy(mapRetry(options.getExecuteRetry()));
        }
        mapped.setWaitForFailurePolicy(options.getWaitForFailure()
                == WaitForFailurePolicy.PROCEED
                ? WaitForMethodFailurePolicy
                        .WAIT_FOR_METHOD_FAILURE_POLICY_PROCEED_ON_FAILURE
                : WaitForMethodFailurePolicy
                        .WAIT_FOR_METHOD_FAILURE_POLICY_FAIL_FLOW_ON_FAILURE);
        if (options.getExecuteFailureTarget() != null) {
            final StepOptions.ExecuteFailureTarget target = options.getExecuteFailureTarget();
            final Registry.RegisteredStep registeredTarget;
            try {
                registeredTarget = flow.getStep(target.getStepClass());
            } catch (FlowDefinitionException exception) {
                throw new InvalidStepResultException(
                        "Flow " + flow.getName()
                                + " execute failure Step does not belong to Flow");
            }
            mapped.setExecuteFailurePolicy(ExecuteMethodFailurePolicy
                    .EXECUTE_METHOD_FAILURE_POLICY_PROCEED_TO_CONFIGURED_STEP)
                    .setExecuteFailureProceedStepType(registeredTarget.getName());
            final io.superdurable.gen.StepOptions targetOptions =
                    mapStepOptions(flow, target.getOptions() == null
                            ? registeredTarget.getStep().getStepOptions()
                            : target.getOptions());
            if (targetOptions != null || registeredTarget.skipsWaitFor()) {
                final io.superdurable.gen.StepOptions.Builder mappedTarget =
                        targetOptions == null
                                ? io.superdurable.gen.StepOptions.newBuilder()
                                : targetOptions.toBuilder();
                mappedTarget.setSkipWaitFor(registeredTarget.skipsWaitFor());
                mapped.setExecuteFailureProceedStepOptions(mappedTarget);
            }
        }
        mapped.setWaitForDurabilityOverride(mapDurability(options.getWaitForDurability()));
        mapped.setExecuteDurabilityOverride(mapDurability(options.getExecuteDurability()));
        for (AttributeLock lock : options.getWaitForLocks()) {
            mapped.addWaitForLockAttributeKeys(mapLock(lock));
        }
        for (AttributeLock lock : options.getExecuteLocks()) {
            mapped.addExecuteLockAttributeKeys(mapLock(lock));
        }
        final MappedStateLoads waitForLoads = mapHandlerStateLoads(
                flow, options.getWaitForStateLoads(), "WaitFor");
        mapped.addAllWaitForLoadAttributeMapInstances(waitForLoads.attributeMaps)
                .addAllWaitForLoadChannelNames(waitForLoads.channels)
                .addAllWaitForLoadChannelMapInstances(waitForLoads.channelMaps);
        final MappedStateLoads executeLoads = mapHandlerStateLoads(
                flow, options.getExecuteStateLoads(), "Execute");
        mapped.addAllExecuteLoadAttributeMapInstances(executeLoads.attributeMaps)
                .addAllExecuteLoadChannelNames(executeLoads.channels)
                .addAllExecuteLoadChannelMapInstances(executeLoads.channelMaps);
        return mapped.build();
    }

    io.superdurable.gen.FlowTimeoutHandlerOptions mapFlowTimeoutHandlerOptions(
            final Registry.RegisteredFlow flow,
            final Duration timeout,
            final FlowTimeoutPolicy policy,
            final FlowTimeoutHandlerOptions options) {
        if (options == null) {
            return null;
        }
        if (timeout == null || timeout.isZero() || timeout.isNegative()
                || policy != FlowTimeoutPolicy.HANDLER) {
            throw new IllegalArgumentException(
                    "timeout handler options require a positive timeout with HANDLER policy");
        }
        final io.superdurable.gen.FlowTimeoutHandlerOptions.Builder mapped =
                io.superdurable.gen.FlowTimeoutHandlerOptions.newBuilder();
        if (options.getMethodTimeout() != null) {
            mapped.setMethodTimeoutSeconds(seconds32(options.getMethodTimeout()));
        }
        if (options.getHeartbeatTimeout() != null
                && !options.getHeartbeatTimeout().isZero()) {
            mapped.setHeartbeatTimeoutSeconds(positiveSeconds32(options.getHeartbeatTimeout()));
        }
        if (options.getRetry() != null) {
            mapped.setRetryPolicy(mapRetry(options.getRetry()));
        }
        mapped.setDurabilityOverride(mapDurability(options.getDurability()));
        for (AttributeLock lock : options.getLocks()) {
            mapped.addLockAttributeKeys(mapLock(lock));
        }
        final MappedStateLoads stateLoads = mapHandlerStateLoads(
                flow, options.getStateLoads(), "timeout handler");
        mapped.addAllLoadAttributeMapInstances(stateLoads.attributeMaps)
                .addAllLoadChannelNames(stateLoads.channels)
                .addAllLoadChannelMapInstances(stateLoads.channelMaps);
        if (options.getFailureTarget() != null) {
            final FlowTimeoutHandlerOptions.FailureTarget target = options.getFailureTarget();
            final Registry.RegisteredStep registeredTarget = flow.getStep(target.getStepClass());
            if (registeredTarget.getStep().getInputType() != Void.class) {
                throw new IllegalArgumentException(
                        "timeout handler failure Step must use Void input: "
                                + registeredTarget.getName());
            }
            mapped.setFailurePolicy(ExecuteMethodFailurePolicy
                    .EXECUTE_METHOD_FAILURE_POLICY_PROCEED_TO_CONFIGURED_STEP)
                    .setFailureProceedStepType(registeredTarget.getName());
            final io.superdurable.gen.StepOptions targetOptions = mapStepOptions(
                    flow,
                    target.getOptions() == null
                            ? registeredTarget.getStep().getStepOptions()
                            : target.getOptions());
            if (targetOptions != null || registeredTarget.skipsWaitFor()) {
                final io.superdurable.gen.StepOptions.Builder mappedTarget = targetOptions == null
                        ? io.superdurable.gen.StepOptions.newBuilder()
                        : targetOptions.toBuilder();
                mappedTarget.setSkipWaitFor(registeredTarget.skipsWaitFor());
                mapped.setFailureProceedStepOptions(mappedTarget);
            }
        }
        return mapped.build();
    }

    private static MappedStateLoads mapHandlerStateLoads(
            final Registry.RegisteredFlow flow,
            final HandlerStateLoads loads,
            final String source) {
        final MappedStateLoads mapped = new MappedStateLoads();
        for (AttributeMap<?> definition : loads.getAttributeMaps()) {
            addDefinitionLoad(flow, definition, AttributeMap.class, source,
                    definition == null ? null : definition.getName() + "/",
                    mapped.attributeMapNames, mapped.attributeMaps);
        }
        for (HandlerStateLoads.MapInstance load : loads.getAttributeMapInstances()) {
            addMapInstanceLoad(flow, load, AttributeMap.class, source,
                    mapped.attributeMapNames, mapped.attributeMaps);
        }
        for (Channel<?> definition : loads.getChannels()) {
            addDefinitionLoad(flow, definition, Channel.class, source,
                    definition == null ? null : definition.getName(),
                    mapped.channelNames, mapped.channels);
        }
        for (ChannelMap<?> definition : loads.getChannelMaps()) {
            addDefinitionLoad(flow, definition, ChannelMap.class, source,
                    definition == null ? null : definition.getName() + "/",
                    mapped.channelMapNames, mapped.channelMaps);
        }
        for (HandlerStateLoads.MapInstance load : loads.getChannelMapInstances()) {
            addMapInstanceLoad(flow, load, ChannelMap.class, source,
                    mapped.channelMapNames, mapped.channelMaps);
        }
        Collections.sort(mapped.attributeMaps);
        Collections.sort(mapped.channels);
        Collections.sort(mapped.channelMaps);
        return mapped;
    }

    private static void addMapInstanceLoad(
            final Registry.RegisteredFlow flow,
            final HandlerStateLoads.MapInstance load,
            final Class<?> expectedType,
            final String source,
            final Set<String> seen,
            final List<String> output) {
        final PersistenceDefinition definition = load.getDefinition();
        final String physical = definition == null
                ? null
                : Registry.physicalName(definition.getName(), load.getInstance());
        addDefinitionLoad(flow, definition, expectedType, source, physical, seen, output);
    }

    private static void addDefinitionLoad(
            final Registry.RegisteredFlow flow,
            final PersistenceDefinition definition,
            final Class<?> expectedType,
            final String source,
            final String physical,
            final Set<String> seen,
            final List<String> output) {
        if (definition == null || !expectedType.isInstance(definition)
                || flow.getPersistence().get(definition.getName()) != definition) {
            throw new IllegalArgumentException(
                    source + " state load does not belong to Flow " + flow.getName());
        }
        if (!seen.add(physical)) {
            throw new IllegalArgumentException(source + " has duplicate state load " + physical);
        }
        output.add(physical);
    }

    private static final class MappedStateLoads {
        private final Set<String> attributeMapNames = new HashSet<String>();
        private final Set<String> channelNames = new HashSet<String>();
        private final Set<String> channelMapNames = new HashSet<String>();
        private final List<String> attributeMaps = new ArrayList<String>();
        private final List<String> channels = new ArrayList<String>();
        private final List<String> channelMaps = new ArrayList<String>();
    }

    private static io.superdurable.gen.RetryPolicy mapRetry(final RetryPolicy retry) {
        final io.superdurable.gen.RetryPolicy.Builder mapped =
                io.superdurable.gen.RetryPolicy.newBuilder()
                        .setBackoffCoefficient((float) retry.getBackoffCoefficient())
                        .setMaximumAttempts(retry.getMaximumAttempts());
        if (retry.getInitialInterval() != null) {
            mapped.setInitialIntervalSeconds(seconds32(retry.getInitialInterval()));
        }
        if (retry.getMaximumInterval() != null) {
            mapped.setMaximumIntervalSeconds(seconds32(retry.getMaximumInterval()));
        }
        if (retry.getTotalDuration() != null) {
            mapped.setTotalDurationSeconds(seconds32(retry.getTotalDuration()));
        }
        return mapped.build();
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

    private static String mapLock(final AttributeLock lock) {
        return lock.getInstance() == null
                ? lock.getAttribute()
                : Registry.physicalName(lock.getAttribute(), lock.getInstance());
    }

    private static int seconds32(final Duration duration) {
        if (duration.isNegative() || duration.getNano() != 0
                || duration.getSeconds() > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "Duration must be non-negative whole seconds within int32");
        }
        return (int) duration.getSeconds();
    }

    private static int positiveSeconds32(final Duration duration) {
        final int seconds = seconds32(duration);
        if (seconds == 0) {
            throw new IllegalArgumentException("Duration must be positive");
        }
        return seconds;
    }

    private final class ConditionMapper {
        private final Registry.RegisteredFlow flow;
        private final Map<Condition, String> ids =
                new IdentityHashMap<Condition, String>();
        private final Set<String> used = new HashSet<String>();
        private final List<TimerCondition> timers = new ArrayList<TimerCondition>();
        private final List<ChannelCondition> channels = new ArrayList<ChannelCondition>();
        private final List<SubFlowCondition> subFlows = new ArrayList<SubFlowCondition>();

        ConditionMapper(final Registry.RegisteredFlow flow) {
            this.flow = flow;
        }

        String add(final Condition condition, final boolean idRequired) {
            if (condition == null) {
                throw new InvalidStepResultException(
                        "Flow " + flow.getName() + " Wait Condition is required");
            }
            if (ids.containsKey(condition)) {
                return ids.get(condition);
            }
            final String id = condition.getConditionId() == null
                    ? ""
                    : condition.getConditionId();
            if (idRequired && id.isEmpty()) {
                throw new InvalidStepResultException(
                        "Flow " + flow.getName()
                                + " anyCombinationOf requires every Condition to have an ID");
            }
            if (condition.getConditionId() != null && id.isEmpty()) {
                throw new InvalidStepResultException(
                        "Flow " + flow.getName() + " has an empty Condition ID");
            }
            if (!id.isEmpty() && !used.add(id)) {
                throw new InvalidStepResultException(
                        "Flow " + flow.getName() + " has a duplicate Condition ID");
            }
            if (condition.getKind() == Condition.Kind.TIMER) {
                timers.add(TimerCondition.newBuilder()
                        .setConditionId(id)
                        .setDurationSeconds(condition.getDuration().getSeconds())
                        .build());
            } else if (condition.getKind() == Condition.Kind.CHANNEL) {
                final io.superdurable.dex.PersistenceDefinition definition =
                        flow.getPersistence().get(condition.getChannelName());
                if (!(definition instanceof Channel) && !(definition instanceof ChannelMap)) {
                    throw new InvalidStepResultException(
                            "Flow " + flow.getName() + " Channel is not registered: "
                                    + condition.getChannelName());
                }
                final String channelName = definition instanceof ChannelMap
                        ? Registry.physicalName(
                                condition.getChannelName(), condition.getInstance())
                        : condition.getChannelName();
                final ChannelCondition.Builder channel = ChannelCondition.newBuilder()
                        .setConditionId(id)
                        .setChannelName(channelName);
                if (condition.getAtLeast() != null) {
                    channel.setAtLeast(condition.getAtLeast());
                }
                if (condition.getAtMost() != null) {
                    channel.setAtMost(condition.getAtMost());
                }
                channels.add(channel.build());
            } else {
                subFlows.add(mapSubFlow(condition, id, subFlows.size()));
            }
            ids.put(condition, id);
            return id;
        }

        private SubFlowCondition mapSubFlow(
                final Condition condition,
                final String conditionId,
                final int index) {
            final Registry.RegisteredFlow target = registry.getFlow(condition.getSubFlowClass());
            final Registry.RegisteredStep start = target.getStartStep();
            if (start == null) {
                throw new InvalidStepResultException(
                        "SubFlow " + target.getName() + " requires a starting Step");
            }
            final SubFlowOptions options = condition.getSubFlowOptions();
            final io.superdurable.gen.SubFlowOptions.Builder mappedOptions =
                    io.superdurable.gen.SubFlowOptions.newBuilder()
                            .setReusePolicy(mapSubFlowReuse(options.getReusePolicy()));
            if (options.getTimeout() != null) {
                mappedOptions.setFlowTimeoutSeconds(seconds32(options.getTimeout()));
            }
            final FlowTimeoutPolicy timeoutPolicy = Client.resolveTimeoutPolicy(
                    target, options.getTimeout(), options.getTimeoutPolicy());
            if (timeoutPolicy != FlowTimeoutPolicy.DEFAULT) {
                mappedOptions.setFlowTimeoutPolicy(Client.mapFlowTimeoutPolicy(timeoutPolicy));
            }
            final io.superdurable.gen.FlowTimeoutHandlerOptions timeoutHandlerOptions =
                    mapFlowTimeoutHandlerOptions(
                            target,
                            options.getTimeout(),
                            timeoutPolicy,
                            options.getTimeoutHandlerOptions());
            if (timeoutHandlerOptions != null) {
                mappedOptions.setTimeoutHandlerOptions(timeoutHandlerOptions);
            }
            if (options.getStartDelay() != null) {
                mappedOptions.setFlowStartDelaySeconds(seconds32(options.getStartDelay()));
            }
            if (options.getRetryPolicy() != null) {
                mappedOptions.setRetryPolicy(mapFlowRetry(options.getRetryPolicy()));
            }
            for (SubFlowOptions.AttributeInitialization initialization
                    : options.getAttributes()) {
                final PersistenceDefinition registered = target.getPersistence().get(
                        initialization.getDefinition().getName());
                if (registered != initialization.getDefinition()) {
                    throw new InvalidStepResultException(
                            "SubFlow " + target.getName() + " Attribute does not belong to Flow: "
                                    + initialization.getDefinition().getName());
                }
                final String key = initialization.getInstance() == null
                        ? registered.getName()
                        : Registry.physicalName(registered.getName(), initialization.getInstance());
                final AttributeWrite.Builder write = AttributeWrite.newBuilder()
                        .setKey(key)
                        .setValue(values.encode(initialization.getValue()));
                final AttributeIndex attributeIndex = registered instanceof Attribute
                        ? ((Attribute<?>) registered).getIndex()
                        : ((AttributeMap<?>) registered).getIndex();
                final io.superdurable.gen.IndexConfig indexConfig = values.indexConfig(
                        attributeIndex, initialization.getInstance() != null);
                if (indexConfig != null) {
                    write.setIndexConfig(indexConfig);
                }
                if (registered.isSyncToAttributeStore()) {
                    write.setSyncConfig(io.superdurable.gen.AttributeSyncConfig.newBuilder()
                            .setEnabled(true));
                }
                HardDeleteAttributes.applyHardDeleteOnNull(write, registered);
                if (write.getIsHardDeleteOnNull()) {
                    serverProtocol.requireHardDeleteAttributeSupport(key);
                }
                mappedOptions.addAttributes(write);
            }
            if (options.getConfigOverride() != null) {
                mappedOptions.setFlowConfigOverride(mapFlowConfig(options.getConfigOverride()));
            }
            final SubFlowCondition.Builder mapped = SubFlowCondition.newBuilder()
                    .setConditionId(conditionId)
                    .setSubFlowType(target.getName())
                    .setStartStepType(start.getName())
                    .setStepInput(values.encode(condition.getSubFlowInput()))
                    .setOptions(mappedOptions)
                    .setSubFlowIndex(index);
            final io.superdurable.gen.StepOptions stepOptions =
                    mapStepOptions(target, start.getStep().getStepOptions());
            if (stepOptions != null || start.skipsWaitFor()) {
                final io.superdurable.gen.StepOptions.Builder mappedStepOptions =
                        stepOptions == null
                                ? io.superdurable.gen.StepOptions.newBuilder()
                                : stepOptions.toBuilder();
                mappedStepOptions.setSkipWaitFor(start.skipsWaitFor());
                mapped.setStepOptions(mappedStepOptions);
            }
            return mapped.build();
        }
    }

    private static io.superdurable.gen.SubFlowReusePolicy mapSubFlowReuse(
            final SubFlowReusePolicy policy) {
        switch (policy) {
            case ATTACH:
                return io.superdurable.gen.SubFlowReusePolicy.SUB_FLOW_REUSE_POLICY_ATTACH;
            case ALWAYS_RESTART:
                return io.superdurable.gen.SubFlowReusePolicy
                        .SUB_FLOW_REUSE_POLICY_ALWAYS_RESTART;
            default:
                return io.superdurable.gen.SubFlowReusePolicy
                        .SUB_FLOW_REUSE_POLICY_RESTART_IF_PREVIOUS_EXITS_ABNORMALLY;
        }
    }

    private static io.superdurable.gen.FlowRetryPolicy mapFlowRetry(
            final RetryPolicy retry) {
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

    private static io.superdurable.gen.FlowConfig mapFlowConfig(final FlowConfig config) {
        final io.superdurable.gen.FlowConfig.Builder mapped =
                io.superdurable.gen.FlowConfig.newBuilder();
        if (config.getActiveStepSearchMode() != null) {
            switch (config.getActiveStepSearchMode()) {
                case ALL:
                    mapped.setActiveStepSearchMode(io.superdurable.gen.ActiveStepSearchMode
                            .ACTIVE_STEP_SEARCH_MODE_ENABLED_FOR_ALL);
                    break;
                case WITH_WAIT_FOR:
                    mapped.setActiveStepSearchMode(io.superdurable.gen.ActiveStepSearchMode
                            .ACTIVE_STEP_SEARCH_MODE_ENABLED_FOR_STEPS_WITH_WAIT_FOR);
                    break;
                case DISABLED:
                    mapped.setActiveStepSearchMode(io.superdurable.gen.ActiveStepSearchMode
                            .ACTIVE_STEP_SEARCH_MODE_DISABLED);
                    break;
                default:
                    break;
            }
        }
        if (config.getContinueAsNewThreshold() != null) {
            mapped.setContinueAsNewThreshold(config.getContinueAsNewThreshold());
        }
        if (config.getContinueAsNewPageSizeBytes() != null) {
            mapped.setContinueAsNewPageSizeInBytes(config.getContinueAsNewPageSizeBytes());
        }
        if (config.getStepDurability() != null) {
            mapped.setStepDurability(mapDurability(config.getStepDurability()));
        }
        if (config.getWorkerTarget() != null) {
            mapped.setWorkerTarget(io.superdurable.gen.WorkerTarget.newBuilder()
                    .setAddress(config.getWorkerTarget().getAddress())
                    .setIsHeadlessAddress(config.getWorkerTarget().isHeadless())
                    .setIsPullMode(config.getWorkerTarget().isPull()));
        }
        if (config.getAttributeStoreNames() != null) {
            mapped.setAttributeStoreNames(io.superdurable.gen.AttributeStoreNames.newBuilder()
                    .addAllNames(config.getAttributeStoreNames()));
        }
        return mapped.build();
    }
}
