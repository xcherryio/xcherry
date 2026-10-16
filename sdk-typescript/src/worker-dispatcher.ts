// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import {
  ChannelCondition,
  CloseDecision,
  CloseDecisionType,
  ConditionCombination as ProtoConditionCombination,
  ExecuteMethodFailurePolicy,
  InvokeExecuteMethodResponse,
  InvokeWaitForMethodResponse,
  InvokeWorkerRPCResponse,
  StepDecision as ProtoStepDecision,
  StepMovement as ProtoStepMovement,
  StepOptions as ProtoStepOptions,
  TimerCondition,
  SubFlowCondition as ProtoSubFlowCondition,
  SubFlowOptions as ProtoSubFlowOptions,
  SubFlowReusePolicy as ProtoSubFlowReusePolicy,
  FlowRetryPolicy,
  FlowTimeoutHandlerOptions as ProtoFlowTimeoutHandlerOptions,
  FlowTimeoutPolicy as ProtoFlowTimeoutPolicy,
  FlowConfig as ProtoFlowConfig,
  ActiveStepSearchMode as ProtoActiveStepSearchMode,
  StepDurability as ProtoStepDurability,
  IndexType as ProtoIndexType,
  WaitForMethodFailurePolicy,
  WaitingCondition,
  WaitingConditionType,
  type AttributeWrite,
  type ChannelMessage,
  type ChannelValues,
  type ConditionResults,
  type InvokeExecuteMethodRequest,
  type InvokeWaitForMethodRequest,
  type InvokeWorkerRPCRequest,
  type KV,
  type RetryPolicy as ProtoRetryPolicy,
  type Value,
} from "./gen/dex.js";
import { InvalidStepResultError, ValueMappingError } from "./errors.js";
import {
  registeredFlowByName,
  registeredFlow,
  registeredRPCByName,
  registeredStep,
  type RegisteredFlow,
  type RegisteredStep,
  type Registry,
} from "./flow.js";
import { InvocationContext, type StepOutputEmitter } from "./invocation-context.js";
import {
  Attribute,
  AttributeMap,
  IndexType,
  type AttributeLock,
} from "./persistence.js";
import {
  isHardDeleteOnNullDefinition,
  requireHardDeleteAttributeWriteSupport,
  requireHardDeleteOnNullValueKind,
} from "./attribute-hard-delete.js";
import { mapAttributeStoreNames, mapAttributeStoreSync } from "./attribute-store-sync.js";
import {
  ActiveStepSearchMode,
  FlowTimeoutPolicy,
  type FlowConfig,
  type FlowTimeoutHandlerOptions,
} from "./options.js";
import { SubFlowReusePolicy, type SubFlowOptions } from "./subflow.js";
import { mapWorkerTarget } from "./worker-target.js";
import type { RegisteredRPC } from "./rpc.js";
import type {
  RetryPolicy,
  Step,
  StepClass,
  StepDecision,
  StepMovement,
  StepOptions,
} from "./step.js";
import { voidCodec } from "./codec.js";
import {
  codecOrJson,
  decodeValue,
  encodeUnknown,
  encodeValue,
  type ValueHydrator,
} from "./value-mapper.js";
import { Channel, ChannelMap, type Condition, type Wait } from "./wait.js";
import { requireMapInstance } from "./validation.js";

const timeoutHandlerStepType = "sys:timeout_handler";

export class WorkerDispatcher {
  public constructor(
    private readonly registry: Registry,
    private readonly hydrator: ValueHydrator,
    private readonly negotiatedServerProtocolVersion: () => number,
  ) {}

  public async invokeWaitFor(
    original: InvokeWaitForMethodRequest,
    cancellationSignal: AbortSignal = new AbortController().signal,
    stepOutput?: StepOutputEmitter,
  ): Promise<InvokeWaitForMethodResponse> {
    const request = await this.hydrateWaitFor(original);
    cancellationSignal.throwIfAborted();
    const flow = registeredFlowByName(this.registry, request.flowType);
    const step = registeredStep(flow, request.stepType);
    const context = new InvocationContext(
      "waitFor",
      flow,
      request.context,
      request.attributes,
      [],
      undefined,
      request.channelInfos,
      cancellationSignal,
      stepOutput,
      request.loadedChannelMessages,
      request.loadedAttributeMapInstances,
      request.loadedChannelNames,
      request.loadedChannelMapInstances,
    );
    const input = decodeValue(
      codecOrJson(step.step.inputCodec),
      requireValue(request.stepInput, "Step input"),
    );
    if (step.step.waitFor === undefined) {
      throw new TypeError(`Step ${step.name} does not implement waitFor`);
    }
    try {
      const wait = await step.step.waitFor(context, input);
      let response: InvokeWaitForMethodResponse;
      try {
        response = InvokeWaitForMethodResponse.create({
          upsertAttributes: [...context.getAttributeWrites()],
          waitingCondition: mapWait(this.registry, flow, wait),
          upsertStepExeLocals: [...context.getLocalWrites()],
          recordEvents: [...context.getEvents()],
          publishToChannel: [...context.getPublications()],
          deleteFromChannel: [...context.getChannelDeletions()],
        });
      } catch (failure) {
        throw invalidStepResult(flow.name, step.name, "waitFor", failure);
      }
      this.requireHardDeleteAttributeWriteSupport([
        ...response.upsertAttributes,
        ...(response.waitingCondition?.subFlowConditions ?? [])
          .flatMap((condition) => condition.options?.attributes ?? []),
      ]);
      const finalizationFailure = context.finalizeStepOutputs();
      if (finalizationFailure !== undefined) {
        throw finalizationFailure;
      }
      return response;
    } catch (failure) {
      throw context.finalizeStepOutputs(failure);
    }
  }

  public async invokeExecute(
    original: InvokeExecuteMethodRequest,
    cancellationSignal: AbortSignal = new AbortController().signal,
    stepOutput?: StepOutputEmitter,
  ): Promise<InvokeExecuteMethodResponse> {
    const request = await this.hydrateExecute(original);
    cancellationSignal.throwIfAborted();
    const flow = registeredFlowByName(this.registry, request.flowType);
    if (request.stepType === timeoutHandlerStepType) {
      return this.invokeTimeoutHandler(request, flow, cancellationSignal, stepOutput);
    }
    const step = registeredStep(flow, request.stepType);
    const context = new InvocationContext(
      "execute",
      flow,
      request.context,
      request.attributes,
      request.stepExeLocals,
      request.conditionResults,
      request.channelInfos,
      cancellationSignal,
      stepOutput,
      request.loadedChannelMessages,
      request.loadedAttributeMapInstances,
      request.loadedChannelNames,
      request.loadedChannelMapInstances,
    );
    const input = decodeValue(
      codecOrJson(step.step.inputCodec),
      requireValue(request.stepInput, "Step input"),
    );
    try {
      const decision = await step.step.execute(context, input);
      let response: InvokeExecuteMethodResponse;
      try {
        response = InvokeExecuteMethodResponse.create({
          stepDecision: mapDecision(flow, decision),
          upsertAttributes: [...context.getAttributeWrites()],
          recordEvents: [...context.getEvents()],
          upsertStepExeLocals: [...context.getLocalWrites()],
          publishToChannel: [...context.getPublications()],
          deleteFromChannel: [...context.getChannelDeletions()],
        });
      } catch (failure) {
        throw invalidStepResult(flow.name, step.name, "execute", failure);
      }
      this.requireHardDeleteAttributeWriteSupport(response.upsertAttributes);
      const finalizationFailure = context.finalizeStepOutputs();
      if (finalizationFailure !== undefined) {
        throw finalizationFailure;
      }
      return response;
    } catch (failure) {
      throw context.finalizeStepOutputs(failure);
    }
  }

  private async invokeTimeoutHandler(
    request: InvokeExecuteMethodRequest,
    flow: RegisteredFlow,
    cancellationSignal: AbortSignal,
    stepOutput?: StepOutputEmitter,
  ): Promise<InvokeExecuteMethodResponse> {
    try {
      if (request.stepInput !== undefined) {
        throw new TypeError("timeout handler input must be absent");
      }
      if (flow.flow.handleTimeout === undefined) {
        throw new TypeError(`Flow ${flow.name} does not implement handleTimeout`);
      }
      const context = new InvocationContext(
        "execute",
        flow,
        request.context,
        request.attributes,
        request.stepExeLocals,
        request.conditionResults,
        request.channelInfos,
        cancellationSignal,
        stepOutput,
        request.loadedChannelMessages,
        request.loadedAttributeMapInstances,
        request.loadedChannelNames,
        request.loadedChannelMapInstances,
      );
      try {
        const decision = await flow.flow.handleTimeout(context);
        const response = InvokeExecuteMethodResponse.create({
          stepDecision: mapDecision(flow, decision),
          upsertAttributes: [...context.getAttributeWrites()],
          recordEvents: [...context.getEvents()],
          upsertStepExeLocals: [...context.getLocalWrites()],
          publishToChannel: [...context.getPublications()],
          deleteFromChannel: [...context.getChannelDeletions()],
        });
        this.requireHardDeleteAttributeWriteSupport(response.upsertAttributes);
        const finalizationFailure = context.finalizeStepOutputs();
        if (finalizationFailure !== undefined) {
          throw finalizationFailure;
        }
        return response;
      } catch (failure) {
        throw context.finalizeStepOutputs(failure);
      }
    } catch (failure) {
      throw invalidStepResult(flow.name, timeoutHandlerStepType, "execute", failure);
    }
  }

  public async invokeRPC(
    original: InvokeWorkerRPCRequest,
    cancellationSignal: AbortSignal = new AbortController().signal,
  ): Promise<InvokeWorkerRPCResponse> {
    const request = await this.hydrateRPC(original);
    cancellationSignal.throwIfAborted();
    const flow = registeredFlowByName(this.registry, request.flowType);
    const rpc = registeredRPCByName(flow, request.rpcName);
    const context = new InvocationContext(
      "rpc",
      flow,
      request.context,
      request.attributes,
      [],
      undefined,
      request.channelInfos,
      cancellationSignal,
      undefined,
      request.loadedChannelMessages,
      request.loadedAttributeMapInstances,
      request.loadedChannelNames,
      request.loadedChannelMapInstances,
    );
    const returned = await invokeRPC(flow, rpc, context, request.input);
    let response: InvokeWorkerRPCResponse;
    try {
      const result = rpcResult(rpc, returned);
      response = InvokeWorkerRPCResponse.create({
        output:
          result === undefined
            ? encodeUnknown(undefined)
            : encodeValue(codecOrJson(rpc.options.outputCodec), result.output),
        stepDecision: rpcDecision(flow, result),
        upsertAttributes: [...context.getAttributeWrites()],
        recordEvents: [...context.getEvents()],
        publishToChannel: [...context.getPublications()],
        deleteFromChannel: [...context.getChannelDeletions()],
      });
    } catch (failure) {
      throw invalidStepResult(flow.name, undefined, "rpc", failure);
    }
    this.requireHardDeleteAttributeWriteSupport(response.upsertAttributes);
    return response;
  }

  private requireHardDeleteAttributeWriteSupport(writes: readonly AttributeWrite[]): void {
    requireHardDeleteAttributeWriteSupport(this.negotiatedServerProtocolVersion(), writes);
  }

  private async hydrateWaitFor(
    request: InvokeWaitForMethodRequest,
  ): Promise<InvokeWaitForMethodRequest> {
    const lastHeartbeatValue = request.context?.lastHeartbeatValue;
    const hasLastHeartbeatValue = lastHeartbeatValue !== undefined;
    const values = await this.hydrator.hydrateAll(request.context?.flowId ?? "", [
      ...(hasLastHeartbeatValue ? [lastHeartbeatValue] : []),
      request.stepInput,
      ...request.attributes.map((entry) => entry.value),
      ...loadedMessageValues(request.loadedChannelMessages),
    ]);
    const offset = hasLastHeartbeatValue ? 1 : 0;
    const loadedChannelMessages = replaceLoadedChannelMessageValues(
      request.loadedChannelMessages,
      values.slice(offset + 1 + request.attributes.length),
    );
    return {
      ...request,
      context: hasLastHeartbeatValue
        ? { ...request.context!, lastHeartbeatValue: values[0] }
        : request.context,
      stepInput: values[offset],
      attributes: replaceEntryValues(
        request.attributes,
        values.slice(offset + 1, offset + 1 + request.attributes.length),
      ),
      loadedChannelMessages,
    };
  }

  private async hydrateExecute(
    request: InvokeExecuteMethodRequest,
  ): Promise<InvokeExecuteMethodRequest> {
    const lastHeartbeatValue = request.context?.lastHeartbeatValue;
    const hasLastHeartbeatValue = lastHeartbeatValue !== undefined;
    const channelValues = request.conditionResults?.channelResults.flatMap(
      (result) => result.values,
    ) ?? [];
    const subFlowValues = request.conditionResults?.subFlowResults.flatMap(
      (result) => result.results.map((completion) => completion.completedStepOutput),
    ) ?? [];
    const hasInput = request.stepInput !== undefined;
    const values = await this.hydrator.hydrateAll(request.context?.flowId ?? "", [
      ...(hasLastHeartbeatValue ? [lastHeartbeatValue] : []),
      ...(hasInput ? [request.stepInput] : []),
      ...request.attributes.map((entry) => entry.value),
      ...request.stepExeLocals.map((entry) => entry.value),
      ...channelValues,
      ...subFlowValues,
      ...loadedMessageValues(request.loadedChannelMessages),
    ]);
    let offset = hasLastHeartbeatValue ? 1 : 0;
    const stepInput = hasInput ? values[offset++] : undefined;
    const attributes = replaceEntryValues(
      request.attributes,
      values.slice(offset, (offset += request.attributes.length)),
    );
    const stepExeLocals = replaceEntryValues(
      request.stepExeLocals,
      values.slice(offset, (offset += request.stepExeLocals.length)),
    );
    const conditionResults = replaceConditionValues(
      request.conditionResults,
      values.slice(offset, offset + channelValues.length + subFlowValues.length),
    );
    offset += channelValues.length + subFlowValues.length;
    const loadedChannelMessages = replaceLoadedChannelMessageValues(
      request.loadedChannelMessages,
      values.slice(offset),
    );
    return {
      ...request,
      context: hasLastHeartbeatValue
        ? { ...request.context!, lastHeartbeatValue: values[0] }
        : request.context,
      stepInput,
      attributes,
      stepExeLocals,
      conditionResults,
      loadedChannelMessages,
    };
  }

  private async hydrateRPC(request: InvokeWorkerRPCRequest): Promise<InvokeWorkerRPCRequest> {
    const hasInput = request.input !== undefined;
    const channelMessages = loadedMessageValues(request.loadedChannelMessages);
    const values = await this.hydrator.hydrateAll(request.context?.flowId ?? "", [
      ...(hasInput ? [request.input] : []),
      ...request.attributes.map((entry) => entry.value),
      ...channelMessages,
    ]);
    let offset = hasInput ? 1 : 0;
    const attributes = replaceEntryValues(
      request.attributes,
      values.slice(offset, (offset += request.attributes.length)),
    );
    const loadedChannelMessages = replaceLoadedChannelMessageValues(
      request.loadedChannelMessages,
      values.slice(offset),
    );
    return {
      ...request,
      input: hasInput ? values[0] : undefined,
      attributes,
      loadedChannelMessages,
    };
  }
}

function invokeRPC(
  flow: RegisteredFlow,
  rpc: RegisteredRPC,
  context: InvocationContext,
  input: Value | undefined,
): unknown {
  if (!rpc.hasInput) {
    return rpc.method.call(flow.flow, context);
  }
  return rpc.method.call(
    flow.flow,
    context,
    decodeValue(codecOrJson(rpc.options.inputCodec), requireValue(input, "RPC input")),
  );
}

function rpcResult(
  rpc: RegisteredRPC,
  returned: unknown,
): { output: unknown; nextSteps?: readonly StepMovement<any>[] } | undefined {
  if (returned === undefined) {
    if (rpc.options.outputCodec !== undefined) {
      throw new TypeError(`function RPC ${rpc.name} must return RPCResult`);
    }
    return undefined;
  }
  if (typeof returned !== "object" || returned === null || !("output" in returned)) {
    throw new TypeError(
      rpc.options.outputCodec === undefined && !rpc.hasInput
        ? `procedure RPC ${rpc.name} must not return a value`
        : `function RPC ${rpc.name} must return RPCResult`,
    );
  }
  return returned as { output: unknown; nextSteps?: readonly StepMovement<any>[] };
}

function mapWait(
  registry: Registry,
  flow: RegisteredFlow,
  wait: Wait | undefined,
): WaitingCondition | undefined {
  if (wait === undefined) {
    throw new TypeError("waitFor returned undefined");
  }
  if (wait.kind === "skipImmediately") {
    return undefined;
  }
  const mapper = new ConditionMapper(registry, flow);
  let waitingConditionType: WaitingConditionType;
  const combinations: ProtoConditionCombination[] = [];
  if (wait.kind === "allOf" || wait.kind === "anyOf") {
    if (wait.conditions.length === 0) {
      throw new TypeError("Wait requires at least one Condition");
    }
    waitingConditionType =
      wait.kind === "allOf"
        ? WaitingConditionType.WAITING_CONDITION_TYPE_ALL_COMPLETED
        : WaitingConditionType.WAITING_CONDITION_TYPE_ANY_COMPLETED;
    for (const condition of wait.conditions) {
      mapper.add(condition, false);
    }
  } else {
    waitingConditionType =
      WaitingConditionType.WAITING_CONDITION_TYPE_ANY_COMBINATION_COMPLETED;
    for (const combination of wait.combinations) {
      combinations.push(
        ProtoConditionCombination.create({
          conditionIds: combination.conditions.map((condition) => mapper.add(condition, true)),
        }),
      );
    }
  }
  return WaitingCondition.create({
    waitingConditionType,
    timerConditions: mapper.timers,
    channelConditions: mapper.channels,
    subFlowConditions: mapper.subFlows,
    conditionCombinations: combinations,
  });
}

function mapDecision(flow: RegisteredFlow, decision: StepDecision | undefined): ProtoStepDecision {
  if (decision === undefined) {
    throw new TypeError("execute returned undefined");
  }
  if (decision.kind === "next") {
    if (decision.movements.length === 0) {
      throw new TypeError("goToMany requires a movement");
    }
    return withCancellationSelection(
      flow,
      decision,
      ProtoStepDecision.create({ nextSteps: mapMovements(flow, decision.movements) }),
    );
  }
  if (decision.kind === "deadEnd") {
    return withCancellationSelection(flow, decision, ProtoStepDecision.create({
      closeDecision: CloseDecision.create({
        closeDecisionType: CloseDecisionType.CLOSE_DECISION_TYPE_DEAD_END,
      }),
    }));
  }
  if (decision.kind === "forceCompleteIfChannelsEmpty") {
    const channels = decision.channels.map((channel) => {
      if (!(channel instanceof Channel) || channel instanceof ChannelMap) {
        throw new TypeError("conditional close requires static Channels");
      }
      return channel.name;
    });
    return withCancellationSelection(flow, decision, ProtoStepDecision.create({
      nextSteps: [mapMovement(flow, decision.fallback)],
      closeDecision: CloseDecision.create({
        closeDecisionType:
          CloseDecisionType.CLOSE_DECISION_TYPE_FORCE_COMPLETE_ON_CHANNELS_EMPTY,
        conditionalChannelNames: channels,
        closeInput: encodeUnknown(decision.output),
      }),
    }));
  }
  const closeTypes = {
    gracefulComplete: CloseDecisionType.CLOSE_DECISION_TYPE_GRACEFUL_COMPLETE,
    forceComplete: CloseDecisionType.CLOSE_DECISION_TYPE_FORCE_COMPLETE,
    forceFail: CloseDecisionType.CLOSE_DECISION_TYPE_FORCE_FAIL,
  } as const;
  const closeInput = decision.kind === "forceFail"
    ? encodeUnknown(decision.reason)
    : decision.output === undefined
      ? undefined
      : encodeUnknown(decision.output);
  return withCancellationSelection(flow, decision, ProtoStepDecision.create({
    closeDecision: CloseDecision.create({
      closeDecisionType: closeTypes[decision.kind],
      closeInput,
    }),
  }));
}

function withCancellationSelection(
  flow: RegisteredFlow,
  decision: StepDecision,
  mapped: ProtoStepDecision,
): ProtoStepDecision {
  mapped.cancelStepTypes = mapCancellationSteps(flow, decision.cancelingSteps);
  const globalTypes = new Set(mapped.cancelStepTypes);
  mapped.cancelSiblingStepTypes = mapCancellationSteps(
    flow,
    decision.cancelingSiblingSteps,
  ).filter((stepType) => !globalTypes.has(stepType));
  return mapped;
}

function mapCancellationSteps(
  flow: RegisteredFlow,
  steps: readonly StepClass<any>[] | undefined,
): string[] {
  const stepTypes: string[] = [];
  const seen = new Set<string>();
  for (const step of steps ?? []) {
    const target = flow.stepsByClass.get(step);
    if (target === undefined) {
      throw new TypeError("cancellation Step must belong to the Flow");
    }
    if (seen.has(target.name)) {
      continue;
    }
    seen.add(target.name);
    stepTypes.push(target.name);
  }
  return stepTypes;
}

function rpcDecision(
  flow: RegisteredFlow,
  result: {
    nextSteps?: readonly StepMovement<any>[];
    cancelingSteps?: readonly StepClass<any>[];
  }
    | undefined,
): ProtoStepDecision | undefined {
  if (result === undefined) {
    return undefined;
  }
  const nextSteps = mapMovements(flow, result.nextSteps ?? []);
  const cancelStepTypes = mapCancellationSteps(flow, result.cancelingSteps);
  if (nextSteps.length === 0 && cancelStepTypes.length === 0) {
    return undefined;
  }
  return ProtoStepDecision.create({ nextSteps, cancelStepTypes });
}

function mapMovements(
  flow: RegisteredFlow,
  movements: readonly StepMovement<any>[],
): ProtoStepMovement[] {
  return movements.map((movement) => mapMovement(flow, movement));
}

function mapMovement(flow: RegisteredFlow, movement: StepMovement<any>): ProtoStepMovement {
  const target = flow.stepsByClass.get(movement.step);
  if (target === undefined) {
    throw new TypeError("Step movement target does not belong to Flow");
  }
  return ProtoStepMovement.create({
    stepType: target.name,
    stepInput: encodeValue(codecOrJson(target.step.inputCodec), movement.input),
    stepOptions: mapStepOptions(
      flow,
      movement.options ?? target.step.getStepOptions?.(),
      target.step.waitFor === undefined,
    ),
  });
}

function mapStepOptions(
  flow: RegisteredFlow,
  options: StepOptions | undefined,
  skipWaitFor: boolean,
): ProtoStepOptions {
  const failureTarget = options?.executeFailure?.step;
  const failureDefinition = failureTarget === undefined
    ? undefined
    : flow.stepsByClass.get(failureTarget);
  if (failureTarget !== undefined && failureDefinition === undefined) {
    throw new TypeError("execute failure Step must belong to the Flow");
  }
  const failureOptions = options?.executeFailure?.options ?? failureDefinition?.step.getStepOptions?.();
  const waitForLoads = mapHandlerStateLoads(flow, {
    attributeMaps: options?.waitForLoadAttributeMaps,
    attributeMapInstances: options?.waitForLoadAttributeMapInstances,
    channels: options?.waitForLoadChannels,
    channelMaps: options?.waitForLoadChannelMaps,
    channelMapInstances: options?.waitForLoadChannelMapInstances,
  }, "WaitFor");
  const executeLoads = mapHandlerStateLoads(flow, {
    attributeMaps: options?.executeLoadAttributeMaps,
    attributeMapInstances: options?.executeLoadAttributeMapInstances,
    channels: options?.executeLoadChannels,
    channelMaps: options?.executeLoadChannelMaps,
    channelMapInstances: options?.executeLoadChannelMapInstances,
  }, "Execute");
  return ProtoStepOptions.create({
    waitForTimeoutSeconds: seconds(options?.waitForMethodTimeoutMs),
    executeTimeoutSeconds: seconds(options?.executeMethodTimeoutMs),
    heartbeatTimeoutSeconds: heartbeatSeconds(options?.heartbeatTimeoutMs),
    waitForRetryPolicy: mapRetry(options?.waitForRetry),
    executeRetryPolicy: mapRetry(options?.executeRetry),
    waitForFailurePolicy:
      options?.waitForFailure === "proceed"
        ? WaitForMethodFailurePolicy.WAIT_FOR_METHOD_FAILURE_POLICY_PROCEED_ON_FAILURE
        : options?.waitForFailure === "failFlow"
          ? WaitForMethodFailurePolicy.WAIT_FOR_METHOD_FAILURE_POLICY_FAIL_FLOW_ON_FAILURE
          : WaitForMethodFailurePolicy.WAIT_FOR_METHOD_FAILURE_POLICY_UNSPECIFIED,
    executeFailurePolicy:
      failureDefinition === undefined
        ? ExecuteMethodFailurePolicy.EXECUTE_METHOD_FAILURE_POLICY_UNSPECIFIED
        : ExecuteMethodFailurePolicy.EXECUTE_METHOD_FAILURE_POLICY_PROCEED_TO_CONFIGURED_STEP,
    executeFailureProceedStepType: failureDefinition?.name ?? "",
    executeFailureProceedStepOptions:
      failureDefinition === undefined
        ? undefined
        : mapStepOptions(
            flow,
            failureOptions,
            failureDefinition.step.waitFor === undefined,
          ),
    skipWaitFor,
    waitForDurabilityOverride:
      options?.waitForDurability === "sync"
        ? 1
        : options?.waitForDurability === "async"
          ? 2
          : 0,
    executeDurabilityOverride:
      options?.executeDurability === "sync"
        ? 1
        : options?.executeDurability === "async"
          ? 2
          : 0,
    waitForLockAttributeKeys: (options?.waitForLockAttributes ?? []).map((lock) =>
      lock.instance === undefined
        ? lock.attribute.name
        : physicalName(lock.attribute.name, lock.instance),
    ),
    executeLockAttributeKeys: (options?.executeLockAttributes ?? []).map((lock) =>
      lock.instance === undefined
        ? lock.attribute.name
        : physicalName(lock.attribute.name, lock.instance),
    ),
    waitForLoadAttributeMapInstances: waitForLoads.attributeMaps,
    waitForLoadChannelNames: waitForLoads.channels,
    waitForLoadChannelMapInstances: waitForLoads.channelMaps,
    executeLoadAttributeMapInstances: executeLoads.attributeMaps,
    executeLoadChannelNames: executeLoads.channels,
    executeLoadChannelMapInstances: executeLoads.channelMaps,
  });
}

function mapRetry(retry: RetryPolicy | undefined): ProtoRetryPolicy | undefined {
  if (retry === undefined) {
    return undefined;
  }
  return {
    initialIntervalSeconds: seconds(retry.initialIntervalMs),
    backoffCoefficient: retry.backoffCoefficient ?? 0,
    maximumIntervalSeconds: seconds(retry.maximumIntervalMs),
    maximumAttempts: retry.maximumAttempts ?? 0,
    totalDurationSeconds: seconds(retry.totalDurationMs),
  };
}

class ConditionMapper {
  public readonly timers: TimerCondition[] = [];
  public readonly channels: ChannelCondition[] = [];
  public readonly subFlows: ProtoSubFlowCondition[] = [];
  private readonly ids = new Map<Condition, string>();
  private readonly used = new Set<string>();

  public constructor(
    private readonly registry: Registry,
    private readonly flow: RegisteredFlow,
  ) {}

  public add(condition: Condition, idRequired: boolean): string {
    const existing = this.ids.get(condition);
    if (existing !== undefined) {
      return existing;
    }
    const id = condition.conditionId ?? "";
    if (idRequired && id === "") {
      throw new TypeError("anyCombinationOf requires every Condition to have an ID");
    }
    if (condition.conditionId !== undefined && id === "") {
      throw new TypeError("empty Condition ID");
    }
    if (id !== "" && this.used.has(id)) {
      throw new TypeError("duplicate Condition ID");
    }
    if (id !== "") {
      this.used.add(id);
    }
    if (condition.kind === "timer") {
      this.timers.push(
        TimerCondition.create({
          conditionId: id,
          durationSeconds: BigInt(seconds(condition.durationMs)),
        }),
      );
    } else if (condition.kind === "channel") {
      const registered = this.flow.persistence.get(condition.channelName ?? "");
      if (!(registered instanceof Channel) && !(registered instanceof ChannelMap)) {
        throw new TypeError(`Channel is not registered: ${condition.channelName ?? ""}`);
      }
      const channelName =
        registered instanceof ChannelMap
          ? physicalName(registered.name, condition.instance)
          : registered.name;
      this.channels.push(
        ChannelCondition.create({
          conditionId: id,
          channelName,
          atLeast: condition.atLeast,
          atMost: condition.atMost,
        }),
      );
    } else {
      const targetFlow = condition.subFlow;
      if (targetFlow === undefined) {
        throw new TypeError("SubFlow requires a target Flow");
      }
      const target = registeredFlow(this.registry, targetFlow);
      const start = target.startStep;
      if (start === undefined) {
        throw new TypeError(`SubFlow ${target.name} has no starting Step`);
      }
      const options = condition.subFlowOptions ?? {};
      this.subFlows.push(
        ProtoSubFlowCondition.create({
          conditionId: id,
          subFlowType: target.name,
          startStepType: start.name,
          stepInput: encodeValue(codecOrJson(start.step.inputCodec), condition.subFlowInput),
          stepOptions: mapStepOptions(
            target,
            start.step.getStepOptions?.(),
            start.step.waitFor === undefined,
          ),
          options: mapSubFlowOptions(target, options),
          subFlowIndex: this.subFlows.length,
        }),
      );
    }
    this.ids.set(condition, id);
    return id;
  }
}

function mapSubFlowOptions(
  target: RegisteredFlow,
  options: SubFlowOptions,
): ProtoSubFlowOptions {
  const timeoutSeconds = seconds(options.timeoutMs);
  const attributes = (options.attributes ?? []).map((initial) => {
    if (target.persistence.get(initial.attribute.name) !== initial.attribute) {
      throw new TypeError(`SubFlow Attribute does not belong to ${target.name}`);
    }
    let key: string;
    if (initial.attribute instanceof AttributeMap) {
      key = physicalName(initial.attribute.name, initial.instance);
    } else {
      if (initial.instance !== undefined) {
        throw new TypeError("static Attribute cannot use an instance");
      }
      key = initial.attribute.name;
    }
    return {
      key,
      value: requireHardDeleteOnNullValueKind(
        initial.attribute,
        key,
        encodeValue(initial.attribute.codec, initial.value),
      ),
      indexConfig: mapIndex(initial.attribute.index),
      syncConfig: mapAttributeStoreSync(initial.attribute),
      isHardDeleteOnNull: isHardDeleteOnNullDefinition(initial.attribute),
    };
  });
  const reusePolicy =
    options.reusePolicy === SubFlowReusePolicy.ATTACH
      ? ProtoSubFlowReusePolicy.SUB_FLOW_REUSE_POLICY_ATTACH
      : options.reusePolicy === SubFlowReusePolicy.ALWAYS_RESTART
        ? ProtoSubFlowReusePolicy.SUB_FLOW_REUSE_POLICY_ALWAYS_RESTART
        : ProtoSubFlowReusePolicy.SUB_FLOW_REUSE_POLICY_RESTART_IF_PREVIOUS_EXITS_ABNORMALLY;
  const flowTimeoutPolicy = resolveFlowTimeoutPolicy(
    target,
    timeoutSeconds,
    options.timeoutPolicy,
  );
  return ProtoSubFlowOptions.create({
    reusePolicy,
    flowTimeoutSeconds: timeoutSeconds,
    flowTimeoutPolicy,
    timeoutHandlerOptions: mapFlowTimeoutHandlerOptions(
      target,
      timeoutSeconds,
      flowTimeoutPolicy,
      options.timeoutHandlerOptions,
    ),
    flowStartDelaySeconds: seconds(options.startDelayMs),
    retryPolicy: mapFlowRetry(options.retryPolicy),
    attributes,
    flowConfigOverride: mapSubFlowConfig(options.configOverride),
  });
}

function mapFlowTimeoutHandlerOptions(
  flow: RegisteredFlow,
  timeoutSeconds: number,
  timeoutPolicy: ProtoFlowTimeoutPolicy,
  options: FlowTimeoutHandlerOptions | undefined,
): ProtoFlowTimeoutHandlerOptions | undefined {
  if (options === undefined) {
    return undefined;
  }
  if (
    timeoutSeconds <= 0 ||
    timeoutPolicy !== ProtoFlowTimeoutPolicy.FLOW_TIMEOUT_POLICY_HANDLER
  ) {
    throw new TypeError(
      "timeout handler options require a positive timeout with handler policy",
    );
  }
  const failureStep = options.failure?.step;
  const failureDefinition = failureStep === undefined
    ? undefined
    : flow.stepsByClass.get(failureStep);
  if (failureStep !== undefined && failureDefinition === undefined) {
    throw new TypeError("timeout handler failure Step must belong to the Flow");
  }
  if (failureDefinition !== undefined && failureDefinition.step.inputCodec !== voidCodec) {
    throw new TypeError("timeout handler failure Step must use voidCodec");
  }
  const loads = mapHandlerStateLoads(flow, {
    attributeMaps: options.loadAttributeMaps,
    attributeMapInstances: options.loadAttributeMapInstances,
    channels: options.loadChannels,
    channelMaps: options.loadChannelMaps,
    channelMapInstances: options.loadChannelMapInstances,
  }, "timeout handler");
  return ProtoFlowTimeoutHandlerOptions.create({
    methodTimeoutSeconds: seconds(options.methodTimeoutMs),
    heartbeatTimeoutSeconds: heartbeatSeconds(options.heartbeatTimeoutMs),
    retryPolicy: mapRetry(options.retry),
    failurePolicy: failureDefinition === undefined
      ? ExecuteMethodFailurePolicy.EXECUTE_METHOD_FAILURE_POLICY_UNSPECIFIED
      : ExecuteMethodFailurePolicy.EXECUTE_METHOD_FAILURE_POLICY_PROCEED_TO_CONFIGURED_STEP,
    failureProceedStepType: failureDefinition?.name ?? "",
    failureProceedStepOptions: failureDefinition === undefined
      ? undefined
      : mapStepOptions(
          flow,
          options.failure?.options ?? failureDefinition.step.getStepOptions?.(),
          failureDefinition.step.waitFor === undefined,
        ),
    durabilityOverride: mapDurability(options.durability),
    lockAttributeKeys: mapAttributeLocks(flow, options.lockAttributes ?? [], "timeout handler"),
    loadAttributeMapInstances: loads.attributeMaps,
    loadChannelNames: loads.channels,
    loadChannelMapInstances: loads.channelMaps,
  });
}

interface HandlerStateLoads {
  readonly attributeMaps: readonly AttributeMap<unknown>[] | undefined;
  readonly attributeMapInstances: readonly {
    readonly attributeMap: AttributeMap<unknown>;
    readonly instance: string;
  }[] | undefined;
  readonly channels: readonly Channel<unknown>[] | undefined;
  readonly channelMaps: readonly ChannelMap<unknown>[] | undefined;
  readonly channelMapInstances: readonly {
    readonly channelMap: ChannelMap<unknown>;
    readonly instance: string;
  }[] | undefined;
}

function mapHandlerStateLoads(
  flow: RegisteredFlow,
  loads: HandlerStateLoads,
  source: string,
): { attributeMaps: string[]; channels: string[]; channelMaps: string[] } {
  const attributeMaps = mapDefinitionLoads(
    flow,
    loads.attributeMaps ?? [],
    AttributeMap,
    source,
    (definition) => `${definition.name}/`,
  );
  attributeMaps.push(...mapDefinitionLoads(
    flow,
    loads.attributeMapInstances ?? [],
    AttributeMap,
    source,
    (load) => physicalName(load.attributeMap.name, load.instance),
    (load) => load.attributeMap,
    new Set(attributeMaps),
  ));
  const channels = mapDefinitionLoads(
    flow,
    loads.channels ?? [],
    Channel,
    source,
    (definition) => definition.name,
  );
  const channelMaps = mapDefinitionLoads(
    flow,
    loads.channelMaps ?? [],
    ChannelMap,
    source,
    (definition) => `${definition.name}/`,
  );
  channelMaps.push(...mapDefinitionLoads(
    flow,
    loads.channelMapInstances ?? [],
    ChannelMap,
    source,
    (load) => physicalName(load.channelMap.name, load.instance),
    (load) => load.channelMap,
    new Set(channelMaps),
  ));
  return {
    attributeMaps: attributeMaps.sort(),
    channels: channels.sort(),
    channelMaps: channelMaps.sort(),
  };
}

function mapDefinitionLoads<Load, Definition extends { readonly name: string }>(
  flow: RegisteredFlow,
  loads: readonly Load[],
  expected: abstract new (...arguments_: any[]) => Definition,
  source: string,
  physicalNameFor: (load: Load) => string,
  definitionFor: (load: Load) => Definition = (load) => load as unknown as Definition,
  seen = new Set<string>(),
): string[] {
  const mapped: string[] = [];
  for (const load of loads) {
    const definition = definitionFor(load);
    if (
      !(definition instanceof expected) ||
      flow.persistence.get(definition.name) !== (definition as unknown)
    ) {
      throw new TypeError(`${source} state load does not belong to Flow ${flow.name}`);
    }
    const name = physicalNameFor(load);
    if (seen.has(name)) {
      throw new TypeError(`${source} has duplicate state load ${name}`);
    }
    seen.add(name);
    mapped.push(name);
  }
  return mapped;
}

function mapAttributeLocks(
  flow: RegisteredFlow,
  locks: readonly AttributeLock[],
  source: string,
): string[] {
  return locks.map((lock) => {
    if (flow.persistence.get(lock.attribute.name) !== lock.attribute) {
      throw new TypeError(`${source} lock does not belong to Flow ${flow.name}`);
    }
    return physicalName(lock.attribute.name, lock.instance);
  });
}

function resolveFlowTimeoutPolicy(
  flow: RegisteredFlow,
  timeoutSeconds: number,
  policy: FlowTimeoutPolicy | undefined,
): ProtoFlowTimeoutPolicy {
  const requested = policy ?? FlowTimeoutPolicy.DEFAULT;
  if (timeoutSeconds === 0) {
    if (requested !== FlowTimeoutPolicy.DEFAULT) {
      throw new RangeError("Flow timeout policy requires a positive timeout");
    }
    return ProtoFlowTimeoutPolicy.FLOW_TIMEOUT_POLICY_UNSPECIFIED;
  }
  const resolved = requested === FlowTimeoutPolicy.DEFAULT
    ? flow.hasTimeoutHandler
      ? FlowTimeoutPolicy.HANDLER
      : FlowTimeoutPolicy.FAIL
    : requested;
  if (resolved === FlowTimeoutPolicy.HANDLER && !flow.hasTimeoutHandler) {
    throw new TypeError(`Flow ${flow.name} does not implement handleTimeout`);
  }
  return {
    [FlowTimeoutPolicy.DEFAULT]: ProtoFlowTimeoutPolicy.FLOW_TIMEOUT_POLICY_UNSPECIFIED,
    [FlowTimeoutPolicy.FAIL]: ProtoFlowTimeoutPolicy.FLOW_TIMEOUT_POLICY_FAIL,
    [FlowTimeoutPolicy.CANCEL]: ProtoFlowTimeoutPolicy.FLOW_TIMEOUT_POLICY_CANCEL,
    [FlowTimeoutPolicy.HANDLER]: ProtoFlowTimeoutPolicy.FLOW_TIMEOUT_POLICY_HANDLER,
  }[resolved];
}

function mapFlowRetry(retry: RetryPolicy | undefined): FlowRetryPolicy | undefined {
  if (retry === undefined) return undefined;
  return FlowRetryPolicy.create({
    initialIntervalSeconds: seconds(retry.initialIntervalMs),
    backoffCoefficient: retry.backoffCoefficient ?? 0,
    maximumIntervalSeconds: seconds(retry.maximumIntervalMs),
    maximumAttempts: retry.maximumAttempts ?? 0,
  });
}

function mapDurability(value: "sync" | "async" | undefined): ProtoStepDurability {
  if (value === "sync") {
    return ProtoStepDurability.STEP_DURABILITY_SYNC;
  }
  if (value === "async") {
    return ProtoStepDurability.STEP_DURABILITY_ASYNC;
  }
  return ProtoStepDurability.STEP_DURABILITY_UNSPECIFIED;
}

function mapSubFlowConfig(config: FlowConfig | undefined): ProtoFlowConfig | undefined {
  if (config === undefined) return undefined;
  return ProtoFlowConfig.create({
    activeStepSearchMode:
      config.activeStepSearchMode === ActiveStepSearchMode.ALL
        ? ProtoActiveStepSearchMode.ACTIVE_STEP_SEARCH_MODE_ENABLED_FOR_ALL
        : config.activeStepSearchMode === undefined
          ? undefined
          : ProtoActiveStepSearchMode.ACTIVE_STEP_SEARCH_MODE_UNSPECIFIED,
    attributeStoreNames: mapAttributeStoreNames(config),
    continueAsNewThreshold: config.continueAsNewThreshold,
    continueAsNewPageSizeInBytes: config.continueAsNewPageSizeBytes,
    stepDurability:
      config.stepDurability === "sync"
        ? ProtoStepDurability.STEP_DURABILITY_SYNC
        : config.stepDurability === "async"
          ? ProtoStepDurability.STEP_DURABILITY_ASYNC
          : undefined,
    workerTarget:
      config.workerTarget === undefined
        ? undefined
        : mapWorkerTarget(config.workerTarget),
  });
}

function mapIndex(index: Attribute<unknown>["index"] | undefined) {
  if (index === undefined) return undefined;
  const types: Record<IndexType, ProtoIndexType> = {
    [IndexType.KEYWORD]: ProtoIndexType.INDEX_TYPE_KEYWORD,
    [IndexType.FULL_TEXT]: ProtoIndexType.INDEX_TYPE_TEXT,
    [IndexType.KEYWORD_ARRAY]: ProtoIndexType.INDEX_TYPE_KEYWORD_ARRAY,
    [IndexType.INT]: ProtoIndexType.INDEX_TYPE_INT,
    [IndexType.DOUBLE]: ProtoIndexType.INDEX_TYPE_DOUBLE,
    [IndexType.BOOL]: ProtoIndexType.INDEX_TYPE_BOOL,
    [IndexType.DATETIME]: ProtoIndexType.INDEX_TYPE_DATETIME,
  };
  return { enable: true, type: types[index.type], indexKey: index.indexKey ?? "" };
}

function replaceEntryValues(entries: readonly KV[], values: readonly Value[]): KV[] {
  if (entries.length !== values.length) {
    throw new TypeError("hydrated entry count does not match request");
  }
  return entries.map((entry, index) => ({ ...entry, value: values[index] }));
}

function loadedMessageValues(
  loaded: Readonly<Record<string, ChannelValues>>,
): Array<Value | undefined> {
  return Object.values(loaded).flatMap((channelValues) =>
    channelValues.messages.map((message) => message.value),
  );
}

function replaceLoadedChannelMessageValues(
  loaded: Readonly<Record<string, ChannelValues>>,
  values: readonly Value[],
): Record<string, ChannelValues> {
  let offset = 0;
  const replaced = Object.fromEntries(
    Object.entries(loaded).map(([name, channelValues]) => [
      name,
      {
        messages: channelValues.messages.map((message) => ({
          ...message,
          value: values[offset++],
        })),
      },
    ]),
  );
  if (offset !== values.length) {
    throw new TypeError("hydrated Channel message count does not match request");
  }
  return replaced;
}

function replaceConditionValues(
  results: ConditionResults | undefined,
  values: readonly Value[],
): ConditionResults | undefined {
  if (results === undefined) {
    if (values.length !== 0) {
      throw new TypeError("unexpected hydrated Condition values");
    }
    return undefined;
  }
  let offset = 0;
  const channelResults = results.channelResults.map((result) => {
    const next = offset + result.values.length;
    const replaced = { ...result, values: values.slice(offset, next) };
    offset = next;
    return replaced;
  });
  const subFlowResults = results.subFlowResults.map((result) => ({
    ...result,
    results: result.results.map((completion) => ({
      ...completion,
      completedStepOutput: values[offset++],
    })),
  }));
  if (offset !== values.length) {
    throw new TypeError("hydrated Condition value count does not match request");
  }
  return { ...results, channelResults, subFlowResults };
}

function requireValue(value: Value | undefined, kind: string): Value {
  if (value?.kind === undefined) {
    throw new TypeError(`${kind} is required`);
  }
  return value;
}

function physicalName(name: string, instance: string | undefined): string {
  if (instance === undefined) {
    throw new TypeError(`map definition ${name} requires an instance`);
  }
  requireMapInstance(instance);
  return `${name}/${encodeURIComponent(instance).replace(/[!'()*]/g, (character) =>
    `%${character.charCodeAt(0).toString(16).toUpperCase()}`,
  )}`;
}

function seconds(milliseconds: number | undefined): number {
  if (milliseconds === undefined) {
    return 0;
  }
  if (!Number.isSafeInteger(milliseconds) || milliseconds < 0 || milliseconds % 1_000 !== 0) {
    throw new RangeError("duration must be a non-negative whole number of seconds");
  }
  return milliseconds / 1_000;
}

function heartbeatSeconds(milliseconds: number | undefined): number {
  const value = seconds(milliseconds);
  if (value > 2_147_483_647) {
    throw new RangeError("heartbeat timeout exceeds int32 seconds");
  }
  return value;
}

function invalidStepResult(
  flowType: string,
  stepType: string | undefined,
  method: "waitFor" | "execute" | "rpc",
  failure: unknown,
): Error {
  if (failure instanceof ValueMappingError) {
    return failure;
  }
  if (failure instanceof InvalidStepResultError) {
    return failure;
  }
  const detail = failure instanceof Error ? failure.message : String(failure);
  return new InvalidStepResultError(flowType, stepType, method, detail, { cause: failure });
}
