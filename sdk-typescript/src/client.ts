// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import { credentials, Metadata, status, type ServiceError } from "@grpc/grpc-js";

import type { BlobCache } from "./blob-cache.js";
import { AttributeMatch } from "./attribute-match.js";
import { mapAttributeStoreNames, mapAttributeStoreSync } from "./attribute-store-sync.js";
import type { Codec } from "./codec.js";
import type { Context } from "./context.js";
import {
  ActiveStepSearchMode as ProtoActiveStepSearchMode,
  FlowErrorType as ProtoFlowErrorType,
  FlowTimeoutPolicy as ProtoFlowTimeoutPolicy,
  FlowResetStepMethod as ProtoFlowResetStepMethod,
  FlowResetType as ProtoFlowResetType,
  FlowServiceClient,
  FlowStatus as ProtoFlowStatus,
  IdReusePolicy as ProtoIdReusePolicy,
  IndexType as ProtoIndexType,
  ExecuteMethodFailurePolicy,
  type GetFlowSummaryResponse,
  type InvokeRPCResponse,
  type ListStreamMessagesResponse,
  type ReadStreamResponse,
  type ResetFlowResponse,
  type SearchFlowsResponse,
  type SearchFlowsResponseEntry,
  type StartFlowResponse,
  StartFlowRequest,
  StepDurability as ProtoStepDurability,
  StepOptions as ProtoStepOptions,
  FlowTimeoutHandlerOptions as ProtoFlowTimeoutHandlerOptions,
  StopType as ProtoStopType,
  WaitForMethodFailurePolicy,
  type FlowServiceClient as FlowServiceClientType,
  type FlowConfig as ProtoFlowConfig,
  type FlowResult as ProtoFlowResult,
  type IndexConfig,
  type WaitForStepCompletionResponse,
  type WaitForAttributeResponse,
} from "./gen/dex.js";
import type { Empty } from "./gen/google/protobuf/empty.js";
import {
  ErrorSubStatus,
  DexServiceError,
  FlowDefinitionError,
  FlowErrorType,
  LongPollTimeoutError,
  RequestTimeoutError,
  ValueMappingError,
  type FlowErrorType as FlowErrorTypeValue,
} from "./errors.js";
import {
  registeredFlow,
  registeredRPC,
  registeredRPCFlow,
  registeredStream,
  type Flow,
  type RegisteredFlow,
  type Registry,
} from "./flow.js";
import {
  createFlowResultFromProto,
  type FlowResult,
} from "./flow-result.js";
import { translateServiceError } from "./grpc-status.js";
import { mapWorkerTarget } from "./worker-target.js";
import {
  ActiveStepSearchMode,
  FlowTimeoutPolicy,
  IdReusePolicy,
  TimeTravelStepMethod,
  TimeTravelType,
  StopType,
  type ClientOptions,
  type FlowConfig,
  type FlowInfo,
  type FlowTimeoutHandlerOptions,
  type FlowStatus,
  type TimeTravelOptions,
  type SearchFlowEntry,
  type SearchFlowsPage,
  type SearchFlowsOptions,
  type StartFlowOptions,
  type StepExecutionId,
  type StopFlowOptions,
  type TimerId,
  type WaitForAttributeOptions,
  type WaitForStepCompletionOptions,
} from "./options.js";
import { Attribute, AttributeMap, IndexType, type AttributeLock } from "./persistence.js";
import type { RPCInvokeOptions, RPCResult } from "./rpc.js";
import type { RetryPolicy, StepOptions } from "./step.js";
import { physicalMapName, requireName } from "./validation.js";
import { voidCodec } from "./codec.js";
import { codecOrJson, decodeUnknown, decodeValue, encodeValue, ValueHydrator } from "./value-mapper.js";
import { Channel, ChannelMap } from "./wait.js";
import type { Stream, StreamMessage, StreamMessagesPage } from "./stream.js";

const defaultServerAddress = "localhost:8801";

/**
 * Calls Dex FlowService through registered, typed Flow definitions.
 *
 * Methods perform asynchronous gRPC I/O. A Client owns its service connection but
 * not its Registry or BlobCache; call `close` during application shutdown.
 *
 * @example
 * ```ts
 * const client = new Client(registry, cache);
 * try {
 *   const runId = await client.startFlow(orders, "order-42", input);
 *   const result = await client.waitForFlow("order-42");
 *   const output = result.singleOutput(orderResultCodec);
 * } finally { await client.close(); }
 * ```
 */
export class Client {
  private readonly service: FlowServiceClientType;
  private readonly hydrator: ValueHydrator;

  /**
   * Constructs a Client and lazy plaintext gRPC connection.
   * @param registry - Flow definitions used for validation and routing.
   * @param blobCache - Open cache used to hydrate large response values.
   * @param options - Service address and default Worker target.
   */
  public constructor(
    public readonly registry: Registry,
    public readonly blobCache: BlobCache,
    public readonly options: ClientOptions = {},
  ) {
    if (options.workerTarget !== undefined && "pull" in options.workerTarget) {
      mapWorkerTarget(options.workerTarget);
    }
    this.service = new FlowServiceClient(
      options.serverAddress ?? defaultServerAddress,
      credentials.createInsecure(),
    );
    this.hydrator = new ValueHydrator(this.service, blobCache);
  }

  /**
   * Starts a Flow and returns after Dex accepts it.
   * @typeParam StartInput - Starting Step input type.
   * @param flow - Exact Flow instance registered with this Client.
   * @param flowId - Non-empty application ID stable across runs.
   * @param input - Starting Step input, or `undefined` when no start Step exists.
   * @param options - Timeout, reuse, retry, configuration, and initial state.
   * @returns The server-assigned run ID.
   * @throws {@link FlowAlreadyStartedError} when reuse policy rejects the Flow ID.
   * @throws {@link FlowDefinitionError} when `flow` is not registered.
   */
  public async startFlow<StartInput>(
    flow: Flow<StartInput>,
    flowId: string,
    input: StartInput,
    options: StartFlowOptions = {},
  ): Promise<string> {
    const registered = registeredFlow(this.registry, flow);
    const flowTimeoutSeconds = seconds(options.timeoutMs);
    const flowTimeoutPolicy = resolveFlowTimeoutPolicy(
      registered,
      flowTimeoutSeconds,
      options.timeoutPolicy,
    );
    const request = StartFlowRequest.create({
      flowId: requireName(flowId),
      flowType: registered.name,
      flowTimeoutSeconds,
      flowTimeoutPolicy,
      requestId: options.requestId ?? crypto.randomUUID(),
      flowStartOptions: {
        idReusePolicy: mapIdReusePolicy(options.idReusePolicy),
        flowStartDelaySeconds: seconds(options.startDelayMs),
        retryPolicy: mapFlowRetryPolicy(options.retryPolicy),
        attributes: (options.attributes ?? []).map((initial) => ({
          key: physicalName(initial.attribute.name, initial.instance),
          value: encodeValue(initial.attribute.codec, initial.value),
          indexConfig: mapIndex(initial.attribute.index),
          syncConfig: mapAttributeStoreSync(initial.attribute),
        })),
        flowConfigOverride: mapFlowConfig(options.configOverride, this.options),
        flowAlreadyStartedOptions: {
          ignoreAlreadyStartedError: options.ignoreAlreadyStarted ?? false,
        },
        timeoutHandlerOptions: mapFlowTimeoutHandlerOptions(
          registered,
          flowTimeoutSeconds,
          flowTimeoutPolicy,
          options.timeoutHandlerOptions,
        ),
      },
    });
    if (registered.startStep === undefined) {
      if (input !== undefined) {
        throw new TypeError("Flow without a start Step requires undefined input");
      }
    } else {
      request.startStepType = registered.startStep.name;
      request.stepInput = encodeValue(codecOrJson(registered.startStep.step.inputCodec), input);
      request.stepOptions = mapStepOptions(
        registered.startStep.step.getStepOptions?.(),
        registered.startStep.step.waitFor === undefined,
        registered,
      );
    }
    return (await unary<StartFlowResponse>(
      { operation: "startFlow", flowId, requirement: "none" },
      (callback) => this.service.startFlow(request, callback),
    ))
      .runId;
  }

  /**
   * Invokes an RPC with typed input and output.
   * A non-transactional RPC without Attribute locks starts from a backend query. A retained
   * terminal execution can serve it when the handler returns no durable effects. Locks,
   * transactional execution, returned effects, or server policy can require an active execution.
   * @typeParam Input - RPC input type.
   * @typeParam Output - RPC output type.
   * @param rpcMethod - Bound method decorated with `rpc` on the registered Flow.
   * @param flowId - Non-empty target Flow ID.
   * @param input - Typed handler input.
   * @param runId - Optional exact run; the server resolves the current execution when omitted.
   * @returns The decoded RPCResult output.
   */
  public invokeRPC<Input, Output>(
    rpcMethod: (
      context: Context,
      input: Input,
    ) => RPCResult<Output> | Promise<RPCResult<Output>>,
    flowId: string,
    input: Input,
    runId?: string,
  ): Promise<Output>;

  /**
   * Invokes an input-free RPC with typed output.
   * A retained terminal execution can serve a query-only RPC. Other execution paths can require
   * an active execution.
   * @typeParam Output - RPC output type.
   * @param rpcMethod - Bound method decorated with `rpc` on the registered Flow.
   * @param flowId - Non-empty target Flow ID.
   * @param runId - Optional exact run; the server resolves the current execution when omitted.
   * @returns The decoded RPCResult output.
   */
  public invokeRPC<Output>(
    rpcMethod: (context: Context) => RPCResult<Output> | Promise<RPCResult<Output>>,
    flowId: string,
    runId?: string,
  ): Promise<Output>;

  /**
   * Invokes a typed-input RPC that returns no output.
   * A retained terminal execution can serve a query-only RPC. Other execution paths can require
   * an active execution.
   * @typeParam Input - RPC input type.
   * @param rpcMethod - Bound method decorated with `rpc` on the registered Flow.
   * @param flowId - Non-empty target Flow ID.
   * @param input - Typed handler input.
   * @param runId - Optional exact run; the server resolves the current execution when omitted.
   * @returns A promise resolved after successful handler completion.
   */
  public invokeRPC<Input>(
    rpcMethod: (context: Context, input: Input) => void | Promise<void>,
    flowId: string,
    input: Input,
    runId?: string,
  ): Promise<void>;

  /**
   * Invokes an input-free, output-free RPC.
   * A retained terminal execution can serve a query-only RPC. Other execution paths can require
   * an active execution.
   * @param rpcMethod - Bound method decorated with `rpc` on the registered Flow.
   * @param flowId - Non-empty target Flow ID.
   * @param runId - Optional exact run; the server resolves the current execution when omitted.
   * @returns A promise resolved after successful handler completion.
   */
  public invokeRPC(
    rpcMethod: (context: Context) => void | Promise<void>,
    flowId: string,
    runId?: string,
  ): Promise<void>;

  /**
   * Dispatches a registered RPC using its decorator codecs and locks.
   * @param rpcMethod - Bound registered RPC method.
   * @param flowId - Non-empty target Flow ID.
   * @param inputOrRunId - Typed input, or run ID for an input-free RPC.
   * @param runId - Exact run for an input-bearing RPC; the server resolves the current execution
   * when omitted.
   * @returns Decoded output, or `undefined` for an output-free RPC.
   * @throws {@link RpcLockConflictError} when locks cannot be acquired.
   * @throws {@link WorkerInvocationError} when the application handler fails.
   * @throws {@link FlowNotActiveOrNotFoundError} when the Flow is missing or the selected path
   * cannot use its closed execution.
   */
  public async invokeRPC(
    rpcMethod: Function,
    flowId: string,
    inputOrRunId?: unknown,
    runId = "",
  ): Promise<unknown> {
    const rpc = registeredRPC(this.registry, rpcMethod);
    return this.doInvokeRPC(
      rpcMethod,
      flowId,
      rpc.hasInput ? inputOrRunId : undefined,
      {},
      rpc.hasInput ? runId : (inputOrRunId as string | undefined) ?? "",
    );
  }

  /**
   * Invokes an input-bearing RPC with additive runtime map-instance selections.
   * @typeParam Input - RPC input type.
   * @typeParam Output - RPC output type.
   * @param rpcMethod - Bound method decorated with `rpc` on the registered Flow.
   * @param flowId - Non-empty target Flow ID.
   * @param input - Typed handler input.
   * @param options - Runtime AttributeMap locks and exact map-instance loads.
   * @param runId - Optional exact run; the server resolves the current execution when omitted.
   * @returns The decoded RPCResult output.
   */
  public invokeRPCWithOptions<Input, Output>(
    rpcMethod: (
      context: Context,
      input: Input,
    ) => RPCResult<Output> | Promise<RPCResult<Output>>,
    flowId: string,
    input: Input,
    options: RPCInvokeOptions,
    runId?: string,
  ): Promise<Output>;

  /**
   * Invokes an input-free RPC with additive runtime map-instance selections.
   * @typeParam Output - RPC output type.
   * @param rpcMethod - Bound method decorated with `rpc` on the registered Flow.
   * @param flowId - Non-empty target Flow ID.
   * @param options - Runtime AttributeMap locks and exact map-instance loads.
   * @param runId - Optional exact run; the server resolves the current execution when omitted.
   * @returns The decoded RPCResult output.
   */
  public invokeRPCWithOptions<Output>(
    rpcMethod: (context: Context) => RPCResult<Output> | Promise<RPCResult<Output>>,
    flowId: string,
    options: RPCInvokeOptions,
    runId?: string,
  ): Promise<Output>;

  /**
   * Invokes an input-bearing, output-free RPC with runtime map-instance selections.
   * @typeParam Input - RPC input type.
   * @param rpcMethod - Bound method decorated with `rpc` on the registered Flow.
   * @param flowId - Non-empty target Flow ID.
   * @param input - Typed handler input.
   * @param options - Runtime AttributeMap locks and exact map-instance loads.
   * @param runId - Optional exact run; the server resolves the current execution when omitted.
   * @returns A promise resolved after successful handler completion.
   */
  public invokeRPCWithOptions<Input>(
    rpcMethod: (context: Context, input: Input) => void | Promise<void>,
    flowId: string,
    input: Input,
    options: RPCInvokeOptions,
    runId?: string,
  ): Promise<void>;

  /**
   * Invokes an input-free, output-free RPC with runtime map-instance selections.
   * @param rpcMethod - Bound method decorated with `rpc` on the registered Flow.
   * @param flowId - Non-empty target Flow ID.
   * @param options - Runtime AttributeMap locks and exact map-instance loads.
   * @param runId - Optional exact run; the server resolves the current execution when omitted.
   * @returns A promise resolved after successful handler completion.
   */
  public invokeRPCWithOptions(
    rpcMethod: (context: Context) => void | Promise<void>,
    flowId: string,
    options: RPCInvokeOptions,
    runId?: string,
  ): Promise<void>;

  /**
   * Implements RPC invocation with additive runtime map-instance selections.
   * @param rpcMethod - Bound method decorated with `rpc` on the registered Flow.
   * @param flowId - Non-empty target Flow ID.
   * @param inputOrOptions - Typed input for an input-bearing RPC, or options for an input-free RPC.
   * @param optionsOrRunId - Runtime options for an input-bearing RPC, or an optional run ID for an input-free RPC.
   * @param runId - Optional exact run for an input-bearing RPC.
   * @returns The decoded RPC output, or `undefined` after an output-free RPC completes.
   */
  public async invokeRPCWithOptions(
    rpcMethod: Function,
    flowId: string,
    inputOrOptions: unknown,
    optionsOrRunId?: RPCInvokeOptions | string,
    runId = "",
  ): Promise<unknown> {
    const rpc = registeredRPC(this.registry, rpcMethod);
    if (rpc.hasInput) {
      return this.doInvokeRPC(
        rpcMethod,
        flowId,
        inputOrOptions,
        optionsOrRunId as RPCInvokeOptions,
        runId,
      );
    }
    return this.doInvokeRPC(
      rpcMethod,
      flowId,
      undefined,
      inputOrOptions as RPCInvokeOptions,
      (optionsOrRunId as string | undefined) ?? "",
    );
  }

  private async doInvokeRPC(
    rpcMethod: Function,
    flowId: string,
    input: unknown,
    invokeOptions: RPCInvokeOptions,
    runId: string,
  ): Promise<unknown> {
    const rpc = registeredRPC(this.registry, rpcMethod);
    const flow = registeredRPCFlow(this.registry, rpcMethod);
    const invocationLocks = (invokeOptions.lockAttributeMapInstances ?? []).map((lock) => {
      if (!(lock.attribute instanceof AttributeMap) || lock.instance === undefined) {
        throw new FlowDefinitionError(
          "RPCInvokeOptions lock must target an AttributeMap instance",
        );
      }
      requireRPCInvokeDefinition(flow, lock.attribute, "AttributeMap");
      return physicalName(lock.attribute.name, lock.instance);
    });
    const invocationAttributeMapLoads = (invokeOptions.loadAttributeMapInstances ?? []).map(
      (load) => {
        requireRPCInvokeDefinition(flow, load.attributeMap, "AttributeMap");
        return physicalName(load.attributeMap.name, load.instance);
      },
    );
    const invocationChannelMapLoads = (invokeOptions.loadChannelMapInstances ?? []).map(
      (load) => {
        requireRPCInvokeDefinition(flow, load.channelMap, "ChannelMap");
        return physicalName(load.channelMap.name, load.instance);
      },
    );
    const response = await unary<InvokeRPCResponse>(
      { operation: "invokeRPC", flowId, requirement: "active" },
      (callback) =>
      this.service.invokeRpc(
        {
          flowId: requireName(flowId),
          runId,
          rpcName: rpc.name,
          input: rpc.hasInput
            ? encodeValue(codecOrJson(rpc.options.inputCodec), input)
            : undefined,
          timeoutSeconds: seconds(rpc.options.timeoutMs),
          lockAttributeKeys: uniqueSorted([
            ...(rpc.options.lockAttributes ?? []).map((lock) =>
              physicalName(lock.attribute.name, lock.instance),
            ),
            ...invocationLocks,
          ]),
          requestId: crypto.randomUUID(),
          isTransactional: rpc.options.isTransactional ?? false,
          loadAttributeMapInstances: uniqueSorted([
            ...(rpc.options.loadAttributeMaps ?? []).map((attributeMap) =>
              `${attributeMap.name}/`),
            ...(rpc.options.loadAttributeMapInstances ?? []).map((load) =>
              physicalName(load.attributeMap.name, load.instance)),
            ...invocationAttributeMapLoads,
          ]),
          loadChannelNames: (rpc.options.loadChannels ?? [])
            .map((channel) => channel.name)
            .sort(),
          loadChannelMapInstances: uniqueSorted([
            ...(rpc.options.loadChannelMaps ?? []).map((channelMap) =>
              `${channelMap.name}/`),
            ...(rpc.options.loadChannelMapInstances ?? []).map((load) =>
              physicalName(load.channelMap.name, load.instance)),
            ...invocationChannelMapLoads,
          ]),
        },
        callback,
      ),
    );
    if (rpc.options.outputCodec === undefined && response.output?.kind === undefined) {
      return undefined;
    }
    return decodeValue(
      codecOrJson(rpc.options.outputCodec),
      await this.hydrator.hydrate(flowId, response.output),
    );
  }

  /**
   * Appends one typed best-effort Stream message with source metadata.
   * @typeParam T - Stream message type.
   * @param flowId - Logical Flow instance ID; the Flow need not exist or be active.
   * @param stream - Stream registered in exactly one Flow schema.
   * @param source - Non-empty source metadata. Repeated values and `#` are allowed.
   * @param value - Typed message to append.
   */
  public async writeStream<T>(
    flowId: string,
    stream: Stream<T>,
    source: string,
    value: T,
  ): Promise<void> {
    if (source.length === 0) {
      throw new TypeError("Stream source is required");
    }
    const flow = registeredStream(this.registry, stream as Stream<unknown>);
    await unary<Empty>(
      { operation: "writeStream", flowId, requirement: "none" },
      (callback) => this.service.writeStream({
        flowId: requireName(flowId),
        flowType: flow.name,
        streamName: stream.name,
        streamCapacityBytes: BigInt(stream.streamCapacityBytes),
        value: encodeValue(stream.codec, value),
        source,
      }, callback),
    );
  }

  /**
   * Returns the next retained Stream message after an opaque resume token.
   * @typeParam T - Stream message type.
   * @param flowId - Logical Flow instance ID used as the Stream instance key.
   * @param stream - Stream registered in exactly one Flow schema.
   * @param resumeToken - Previous message token, or empty to start at the retained head.
   * @param timeoutMs - Optional server-side long-poll duration in milliseconds.
   * @returns Decoded value and resumable metadata for one retained message.
   */
  public async readStream<T>(
    flowId: string,
    stream: Stream<T>,
    resumeToken = "",
    timeoutMs?: number,
  ): Promise<StreamMessage<T>> {
    const flow = registeredStream(this.registry, stream as Stream<unknown>);
    const response = await unary<ReadStreamResponse>(
      { operation: "readStream", flowId, requirement: "none" },
      (callback) => this.service.readStream({
        flowId: requireName(flowId),
        flowType: flow.name,
        streamName: stream.name,
        resumeToken,
        waitTimeSeconds: seconds(timeoutMs),
      }, callback),
    );
    const message = response.message;
    if (message?.value === undefined || message.createdTime === undefined || message.resumeToken === "") {
      throw new TypeError("Dex returned an incomplete Stream message");
    }
    return {
      value: decodeValue(stream.codec, message.value),
      resumeToken: message.resumeToken,
      createdTime: message.createdTime,
      source: message.source,
    };
  }

  /**
   * Returns one newest-first page of retained Stream messages without waiting for writes.
   *
   * An empty token starts at the retained tail. Pass `nextPageToken` unchanged to read the next,
   * older page. Stream trimming may remove messages between pages.
   *
   * @typeParam T - Stream message type.
   * @param flowId - Logical Flow instance ID used as the Stream instance key.
   * @param stream - Stream registered in exactly one Flow schema.
   * @param pageSize - Positive maximum count within the server-configured limit.
   * @param beforePageToken - Opaque token from the preceding page, or empty for the first page.
   * @returns Decoded messages in newest-first order and the next-page token.
   */
  public async listStreamMessages<T>(
    flowId: string,
    stream: Stream<T>,
    pageSize: number,
    beforePageToken = "",
  ): Promise<StreamMessagesPage<T>> {
    if (!Number.isSafeInteger(pageSize) || pageSize < 1 || pageSize > 2_147_483_647) {
      throw new RangeError("Stream page size must be a positive 32-bit integer");
    }
    const flow = registeredStream(this.registry, stream as Stream<unknown>);
    const response = await unary<ListStreamMessagesResponse>(
      { operation: "listStreamMessages", flowId, requirement: "none" },
      (callback) => this.service.listStreamMessages({
        flowId: requireName(flowId),
        flowType: flow.name,
        streamName: stream.name,
        pageSize,
        beforePageToken,
      }, callback),
    );
    return {
      messages: response.messages.map((message) => {
        if (message.value === undefined || message.createdTime === undefined || message.resumeToken === "") {
          throw new TypeError("Dex returned an incomplete Stream message");
        }
        return {
          value: decodeValue(stream.codec, message.value),
          resumeToken: message.resumeToken,
          createdTime: message.createdTime,
          source: message.source,
        };
      }),
      nextPageToken: response.nextPageToken,
    };
  }

  /**
   * Long-polls until a Flow closes and returns every output-bearing completion.
   * @param flowId - Non-empty existing Flow ID.
   * @param timeoutMs - Optional server-side long-poll duration in milliseconds.
   * @returns Immutable Step completions in server collection order.
   * @throws {@link LongPollTimeoutError} when the Flow remains open at timeout.
   * @throws {@link ValueMappingError} when Dex returns malformed blob-backed output.
   */
  public async waitForFlow(
    flowId: string,
    timeoutMs?: number,
  ): Promise<FlowResult> {
    const response = await unary<ProtoFlowResult>(
      { operation: "waitForFlow", flowId, requirement: "existing" },
      (callback) =>
        this.service.waitForFlow(
          {
            flowId: requireName(flowId),
            runId: "",
            needsResults: true,
            waitTimeSeconds: seconds(timeoutMs),
          },
          callback,
        ),
    );
    const values = await this.hydrator.hydrateAll(
      flowId,
      response.results.map((result) => result.completedStepOutput),
    );
    return createFlowResultFromProto(response, values);
  }

  /**
   * Requests cancellation, termination, or failure of an active Flow.
   * The promise resolves after acceptance and does not await terminal status.
   * @param flowId - Non-empty active Flow ID.
   * @param options - Stop mode and optional recorded reason.
   */
  public async stopFlow(flowId: string, options: StopFlowOptions = {}): Promise<void> {
    await unary<Empty>(
      { operation: "stopFlow", flowId, requirement: "active" },
      (callback) =>
      this.service.stopFlow(
        {
          flowId: requireName(flowId),
          runId: "",
          reason: options.reason ?? "",
          stopType: mapStopType(options.type),
        },
        callback,
      ),
    );
  }

  /**
   * Returns summary metadata for the current or latest Flow run.
   * @param flowId - Non-empty existing Flow ID.
   * @returns Flow ID, run ID, type, status, and UTC start time.
   */
  public async describeFlow(flowId: string): Promise<FlowInfo> {
    const response = await unary<GetFlowSummaryResponse>(
      { operation: "describeFlow", flowId, requirement: "existing" },
      (callback) => this.service.getFlowSummary({ flowId: requireName(flowId), runId: "" }, callback),
    );
    if (response.flowExecutionId === undefined || response.startTime === undefined) {
      throw new TypeError(`Dex returned an incomplete summary for Flow ${flowId}`);
    }
    return {
      flowId: response.flowExecutionId.flowId,
      runId: response.flowExecutionId.runId,
      flowType: response.flowType,
      status: mapFlowStatus(response.flowStatus),
      startedAt: response.startTime,
    };
  }

  /**
   * Returns one page of Flow runs matching a visibility query.
   * @param query - Dex visibility query; empty uses server defaults.
   * @param pageSize - Non-negative requested maximum result count.
   * @param nextPageToken - Opaque token from the preceding page, or empty first.
   * @param options - Whether to include earlier Continue-as-New runs; defaults to exclusion.
   * @returns Server-ordered entries and the next-page token.
   */
  public async searchFlows(
    query: string,
    pageSize: number,
    nextPageToken = "",
    options: SearchFlowsOptions = {},
  ): Promise<SearchFlowsPage> {
    if (pageSize < 0) {
      throw new RangeError("search page size must not be negative");
    }
    const response = await unary<SearchFlowsResponse>(
      { operation: "searchFlows", requirement: "none" },
      (callback) => this.service.searchFlows({
        query, pageSize, nextPageToken,
        includeContinuedAsNew: options.includeContinuedAsNew ?? false,
      }, callback),
    );
    const flows = await Promise.all(
      response.flowRuns.map((entry) => this.mapSearchEntry(entry)),
    );
    return { flows, nextPageToken: response.nextPageToken };
  }

  private async mapSearchEntry(entry: SearchFlowsResponseEntry): Promise<SearchFlowEntry> {
    if (entry.startTime === undefined) {
      throw new TypeError(`Dex returned a search entry without a start time for Flow ${entry.flowId}`);
    }
    const indexedAttributes = new Map<string, unknown>();
    for (const attribute of entry.indexedAttributes) {
      indexedAttributes.set(
        attribute.key,
        decodeUnknown(await this.hydrator.hydrate(entry.flowId, attribute.value)),
      );
    }
    return {
      flowId: entry.flowId,
      runId: entry.runId,
      flowType: entry.flowType,
      status: mapFlowStatus(entry.flowStatus),
      startedAt: entry.startTime,
      closedAt: entry.closeTime,
      indexedAttributes,
    };
  }

  /**
   * Creates a new run from a selected point in existing Flow history.
   * @param flowId - Non-empty Flow ID whose history supplies the new run.
   * @param options - Time travel selector, reason, and replay controls.
   * @returns The new server-assigned run ID; the Flow ID remains unchanged.
   */
  public async timeTravel(flowId: string, options: TimeTravelOptions): Promise<string> {
    const response = await unary<ResetFlowResponse>(
      { operation: "timeTravel", flowId, requirement: "existing" },
      (callback) =>
      this.service.resetFlow(
        {
          flowId: requireName(flowId),
          runId: "",
          resetType: mapTimeTravelType(options.type),
          reason: options.reason ?? "",
          historyEventTime: options.historyEventTime?.toISOString() ?? "",
          stepType: options.stepType ?? "",
          stepExecutionId: options.stepExecutionId ?? "",
          skipWritesReapply: options.skipWritesReapply ?? false,
          stepMethod: mapTimeTravelStepMethod(options.stepMethod),
        },
        callback,
      ),
    );
    return response.runId;
  }

  /**
   * Makes one waiting Timer condition immediately ready.
   * @param flowId - Non-empty active Flow ID.
   * @param stepExecutionId - Step type and positive execution number.
   * @param timerId - Exactly one Timer condition ID or zero-based index.
   */
  public async skipTimer(
    flowId: string,
    stepExecutionId: StepExecutionId,
    timerId: TimerId,
  ): Promise<void> {
    await unary<Empty>(
      { operation: "skipTimer", flowId, requirement: "active" },
      (callback) =>
      this.service.skipTimer(
        {
          flowId: requireName(flowId),
          runId: "",
          stepExecutionId: `${stepExecutionId.stepType}-${stepExecutionId.number ?? 1}`,
          timerConditionId: timerId.conditionId ?? "",
          timerConditionIndex: timerId.conditionIndex,
        },
        callback,
      ),
    );
  }

  /**
   * Waits until one Step execution completes or its request budget expires.
   * The server derives a stable Request ID from the Step execution when none is supplied.
   * @param flowId - Non-empty active Flow ID.
   * @param stepExecutionId - Step type and positive execution number.
   * @param options - Optional Request ID, request timeout, and internal handler timeout.
   * @throws {@link RequestTimeoutError} when a positive request timeout expires first.
   */
  public async waitForStepCompletion(
    flowId: string,
    stepExecutionId: StepExecutionId,
    options: WaitForStepCompletionOptions,
  ): Promise<void> {
    const requestBudget = new ClientRequestBudget(options.requestTimeoutMs);
    const internalHandlerTimeoutSeconds = seconds32(options.internalHandlerTimeoutMs);
    while (true) {
      const requestAttempt = requestBudget.nextAttempt("waitForStepCompletion", flowId);
      try {
        await unary<WaitForStepCompletionResponse>(
          { operation: "waitForStepCompletion", flowId, requirement: "active" },
          (callback) => {
            const request = {
              flowId: requireName(flowId),
              stepType: stepExecutionId.stepType,
              stepExecutionNumber: String(stepExecutionId.number ?? 1),
              requestTimeoutSeconds: requestAttempt.requestTimeoutSeconds,
              internalHandlerTimeoutSeconds,
              requestId: options.requestId ?? "",
            };
            if (requestAttempt.deadline === undefined) {
              return this.service.waitForStepCompletion(request, callback);
            }
            return this.service.waitForStepCompletion(
              request,
              new Metadata(),
              { deadline: requestAttempt.deadline },
              callback,
            );
          },
        );
        return;
      } catch (error) {
        if (error instanceof LongPollTimeoutError) {
          continue;
        }
        if (
          error instanceof DexServiceError
          && (error.code === status.DEADLINE_EXCEEDED || error.code === status.CANCELLED)
          && requestBudget.hasDeadline()
        ) {
          throw requestBudget.timeoutError("waitForStepCompletion", flowId);
        }
        throw error;
      }
    }
  }

  /**
   * Waits until a singleton Attribute in the current run satisfies a match.
   * Returns the current value observed by the successful wait operation.
   * The server derives a stable Request ID from the condition when none is supplied.
   * Request timeout covers transport reattachments. Internal handler timeout rolls generations.
   * @typeParam T - Attribute value type.
   * @param flowId - Non-empty active Flow ID.
   * @param attribute - Registered singleton Attribute to observe.
   * @param match - Scalar predicate whose operand has the Attribute value type.
   * @param options - Optional Request ID override and request and internal handler timeout controls.
   * @returns The matched current Attribute value.
   */
  public waitForAttributeMatch<T>(
    flowId: string,
    attribute: Attribute<T>,
    match: AttributeMatch<T>,
    options: WaitForAttributeOptions,
  ): Promise<T>;

  /**
   * Waits until one AttributeMap instance in the current run satisfies a match.
   * Primitive-value restrictions, options, and timeout errors match `waitForAttributeMatch`.
   * @typeParam T - AttributeMap value type.
   * @param flowId - Non-empty active Flow ID.
   * @param attribute - Registered AttributeMap to observe.
   * @param instance - The map instance to observe. Slash is prohibited because it is a reserved character.
   * @param match - Scalar predicate whose operand has the AttributeMap value type.
   * @param options - Optional Request ID override and request and internal handler timeout controls.
   * @returns The matched current AttributeMap value.
   */
  public waitForAttributeMatch<T>(
    flowId: string,
    attribute: AttributeMap<T>,
    instance: string,
    match: AttributeMatch<T>,
    options: WaitForAttributeOptions,
  ): Promise<T>;

  /**
   * Waits until a singleton Attribute or AttributeMap instance satisfies a match.
   * @param flowId - Non-empty active Flow ID.
   * @param attribute - Registered Attribute or AttributeMap to observe.
   * @param args - Match and options, optionally preceded by a map instance.
   * @returns The matched current value.
   */
  public async waitForAttributeMatch(
    flowId: string,
    attribute: Attribute<unknown> | AttributeMap<unknown>,
    ...args: unknown[]
  ): Promise<unknown> {
    if (attribute instanceof Attribute) {
      if (args.length !== 2 || !isWaitForAttributeOptions(args[1])) {
        throw new TypeError("waitForAttributeMatch received invalid Attribute arguments");
      }
      return this.waitForAttributeValue(
        flowId,
        attribute,
        undefined,
        args[0] as AttributeMatch<unknown>,
        args[1],
      );
    }
    if (attribute instanceof AttributeMap) {
      if (args.length !== 3 || typeof args[0] !== "string" || !isWaitForAttributeOptions(args[2])) {
        throw new TypeError("waitForAttributeMatch received invalid AttributeMap arguments");
      }
      return this.waitForAttributeValue(
        flowId,
        attribute,
        args[0],
        args[1] as AttributeMatch<unknown>,
        args[2],
      );
    }
    throw new TypeError("waitForAttributeMatch received an invalid Attribute definition");
  }

  private async waitForAttributeValue(
    flowId: string,
    attribute: Attribute<unknown> | AttributeMap<unknown>,
    instance: string | undefined,
    match: AttributeMatch<unknown>,
    options: WaitForAttributeOptions,
  ): Promise<unknown> {
    if (!(match instanceof AttributeMatch)) {
      throw new TypeError("waitForAttributeMatch requires an AttributeMatch");
    }
    const encoded = match.encode(attribute.codec);
    const requestBudget = new ClientRequestBudget(options.requestTimeoutMs);
    const internalHandlerTimeoutSeconds = seconds32(options.internalHandlerTimeoutMs);
    while (true) {
      const requestAttempt = requestBudget.nextAttempt("waitForAttributeMatch", flowId);
      try {
        const response = await unary<WaitForAttributeResponse>(
          { operation: "waitForAttributeMatch", flowId, requirement: "active" },
          (callback) => {
            const request = {
              flowId: requireName(flowId),
              match: {
                key: physicalName(attribute.name, instance),
                operator: encoded.operator,
                operand: encoded.operand,
              },
              requestTimeoutSeconds: requestAttempt.requestTimeoutSeconds,
              internalHandlerTimeoutSeconds,
              requestId: options.requestId ?? "",
            };
            if (requestAttempt.deadline === undefined) {
              return this.service.waitForAttribute(request, callback);
            }
            return this.service.waitForAttribute(
              request,
              new Metadata(),
              { deadline: requestAttempt.deadline },
              callback,
            );
          },
        );
        if (response.matchedValue === undefined) {
          throw new Error("waitForAttributeMatch response is incomplete");
        }
        return decodeValue(attribute.codec, response.matchedValue);
      } catch (error) {
        if (error instanceof LongPollTimeoutError) {
          continue;
        }
        if (
          error instanceof DexServiceError
          && (error.code === status.DEADLINE_EXCEEDED || error.code === status.CANCELLED)
          && requestBudget.hasDeadline()
        ) {
          throw requestBudget.timeoutError("waitForAttributeMatch", flowId);
        }
        throw error;
      }
    }
  }

  /**
   * Replaces mutable configuration for an active Flow.
   * The update affects later decisions and does not recall dispatched work.
   * @param flowId - Non-empty active Flow ID.
   * @param config - New optional Flow configuration fields.
   */
  public async updateFlowConfig(flowId: string, config: FlowConfig): Promise<void> {
    await unary<Empty>(
      { operation: "updateFlowConfig", flowId, requirement: "active" },
      (callback) =>
      this.service.updateFlowConfig(
        {
          flowId: requireName(flowId),
          runId: "",
          flowConfig: mapFlowConfig(config, this.options),
        },
        callback,
      ),
    );
  }

  /**
   * Requests that an active Flow roll its history into a new run.
   * The Flow ID stays the same; the request returns after Dex accepts it.
   * @param flowId - Non-empty active Flow ID.
   */
  public async triggerContinueAsNew(flowId: string): Promise<void> {
    await unary<Empty>(
      { operation: "triggerContinueAsNew", flowId, requirement: "active" },
      (callback) =>
        this.service.triggerContinueAsNew(
          { flowId: requireName(flowId), runId: "" },
          callback,
        ),
    );
  }

  /**
   * Closes the owned FlowService connection.
   * Registry and BlobCache ownership remains with the caller.
   */
  public async close(): Promise<void> {
    this.service.close();
  }
}

function mapStepOptions(
  options: StepOptions | undefined,
  skipWaitFor: boolean,
  flow: RegisteredFlow,
): ProtoStepOptions {
  const executeFailureStep = options?.executeFailure?.step;
  const executeFailureDefinition = executeFailureStep === undefined
    ? undefined
    : flow.stepsByClass.get(executeFailureStep);
  if (executeFailureStep !== undefined && executeFailureDefinition === undefined) {
    throw new TypeError("execute failure Step must belong to the Flow");
  }
  const executeFailureOptions =
    options?.executeFailure?.options ?? executeFailureDefinition?.step.getStepOptions?.();
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
    waitForRetryPolicy: mapRetryPolicy(options?.waitForRetry),
    executeRetryPolicy: mapRetryPolicy(options?.executeRetry),
    waitForFailurePolicy:
      options?.waitForFailure === "proceed"
        ? WaitForMethodFailurePolicy.WAIT_FOR_METHOD_FAILURE_POLICY_PROCEED_ON_FAILURE
        : options?.waitForFailure === "failFlow"
          ? WaitForMethodFailurePolicy.WAIT_FOR_METHOD_FAILURE_POLICY_FAIL_FLOW_ON_FAILURE
          : WaitForMethodFailurePolicy.WAIT_FOR_METHOD_FAILURE_POLICY_UNSPECIFIED,
    executeFailurePolicy:
      executeFailureDefinition === undefined
        ? ExecuteMethodFailurePolicy.EXECUTE_METHOD_FAILURE_POLICY_UNSPECIFIED
        : ExecuteMethodFailurePolicy.EXECUTE_METHOD_FAILURE_POLICY_PROCEED_TO_CONFIGURED_STEP,
    executeFailureProceedStepType: executeFailureDefinition?.name ?? "",
    executeFailureProceedStepOptions:
      executeFailureDefinition === undefined
        ? undefined
        : mapStepOptions(
            executeFailureOptions,
            executeFailureDefinition.step.waitFor === undefined,
            flow,
          ),
    skipWaitFor,
    waitForDurabilityOverride: mapDurability(options?.waitForDurability),
    executeDurabilityOverride: mapDurability(options?.executeDurability),
    waitForLockAttributeKeys: (options?.waitForLockAttributes ?? []).map((lock) =>
      physicalName(lock.attribute.name, lock.instance),
    ),
    executeLockAttributeKeys: (options?.executeLockAttributes ?? []).map((lock) =>
      physicalName(lock.attribute.name, lock.instance),
    ),
    waitForLoadAttributeMapInstances: waitForLoads.attributeMaps,
    waitForLoadChannelNames: waitForLoads.channels,
    waitForLoadChannelMapInstances: waitForLoads.channelMaps,
    executeLoadAttributeMapInstances: executeLoads.attributeMaps,
    executeLoadChannelNames: executeLoads.channels,
    executeLoadChannelMapInstances: executeLoads.channelMaps,
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
    retryPolicy: mapRetryPolicy(options.retry),
    failurePolicy: failureDefinition === undefined
      ? ExecuteMethodFailurePolicy.EXECUTE_METHOD_FAILURE_POLICY_UNSPECIFIED
      : ExecuteMethodFailurePolicy.EXECUTE_METHOD_FAILURE_POLICY_PROCEED_TO_CONFIGURED_STEP,
    failureProceedStepType: failureDefinition?.name ?? "",
    failureProceedStepOptions: failureDefinition === undefined
      ? undefined
      : mapStepOptions(
          options.failure?.options ?? failureDefinition.step.getStepOptions?.(),
          failureDefinition.step.waitFor === undefined,
          flow,
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

function mapRetryPolicy(retry: RetryPolicy | undefined) {
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

function mapFlowRetryPolicy(retry: RetryPolicy | undefined) {
  if (retry === undefined) {
    return undefined;
  }
  return {
    initialIntervalSeconds: seconds(retry.initialIntervalMs),
    backoffCoefficient: retry.backoffCoefficient ?? 0,
    maximumIntervalSeconds: seconds(retry.maximumIntervalMs),
    maximumAttempts: retry.maximumAttempts ?? 0,
  };
}

function mapFlowConfig(
  config: FlowConfig | undefined,
  clientOptions: ClientOptions,
): ProtoFlowConfig | undefined {
  const workerTarget = config?.workerTarget ?? clientOptions.workerTarget;
  if (config === undefined && workerTarget === undefined) {
    return undefined;
  }
  return {
    activeStepSearchMode:
      config?.activeStepSearchMode === undefined
        ? undefined
        : config.activeStepSearchMode === ActiveStepSearchMode.ALL
          ? ProtoActiveStepSearchMode.ACTIVE_STEP_SEARCH_MODE_ENABLED_FOR_ALL
          : ProtoActiveStepSearchMode.ACTIVE_STEP_SEARCH_MODE_UNSPECIFIED,
    attributeStoreNames: mapAttributeStoreNames(config),
    continueAsNewThreshold: config?.continueAsNewThreshold,
    continueAsNewPageSizeInBytes: config?.continueAsNewPageSizeBytes,
    stepDurability:
      config?.stepDurability === undefined ? undefined : mapDurability(config.stepDurability),
    workerTarget:
      workerTarget === undefined
        ? undefined
        : mapWorkerTarget(workerTarget),
  };
}

function mapIndex(index: Attribute<unknown>["index"] | undefined): IndexConfig | undefined {
  if (index === undefined) {
    return undefined;
  }
  const types: Record<IndexType, ProtoIndexType> = {
    [IndexType.KEYWORD]: ProtoIndexType.INDEX_TYPE_KEYWORD,
    [IndexType.FULL_TEXT]: ProtoIndexType.INDEX_TYPE_TEXT,
    [IndexType.KEYWORD_ARRAY]: ProtoIndexType.INDEX_TYPE_KEYWORD_ARRAY,
    [IndexType.INT]: ProtoIndexType.INDEX_TYPE_INT,
    [IndexType.DOUBLE]: ProtoIndexType.INDEX_TYPE_DOUBLE,
    [IndexType.BOOL]: ProtoIndexType.INDEX_TYPE_BOOL,
    [IndexType.DATETIME]: ProtoIndexType.INDEX_TYPE_DATETIME,
  };
  return {
    enable: true,
    type: types[index.type],
    indexKey: index.indexKey ?? "",
  };
}

function mapIdReusePolicy(policy: IdReusePolicy | undefined): ProtoIdReusePolicy {
  switch (policy) {
    case IdReusePolicy.ALLOW_IF_PREVIOUS_FAILED:
      return ProtoIdReusePolicy.ID_REUSE_POLICY_ALLOW_IF_PREVIOUS_EXISTS_ABNORMALLY;
    case IdReusePolicy.ALLOW_IF_NOT_RUNNING:
      return ProtoIdReusePolicy.ID_REUSE_POLICY_ALLOW_IF_NO_RUNNING;
    case IdReusePolicy.ALLOW_TERMINATE_IF_RUNNING:
      return ProtoIdReusePolicy.ID_REUSE_POLICY_ALLOW_TERMINATE_IF_RUNNING;
    case IdReusePolicy.DISALLOW:
      return ProtoIdReusePolicy.ID_REUSE_POLICY_DISALLOW_REUSE;
    default:
      return ProtoIdReusePolicy.ID_REUSE_POLICY_UNSPECIFIED;
  }
}

function mapStopType(type: StopType | undefined): ProtoStopType {
  switch (type) {
    case StopType.TERMINATE:
      return ProtoStopType.STOP_TYPE_TERMINATE;
    case StopType.FAIL:
      return ProtoStopType.STOP_TYPE_FAIL;
    default:
      return ProtoStopType.STOP_TYPE_CANCEL;
  }
}

function mapTimeTravelType(type: TimeTravelType): ProtoFlowResetType {
  const types: Record<TimeTravelType, ProtoFlowResetType> = {
    [TimeTravelType.BEGINNING]: ProtoFlowResetType.FLOW_RESET_TYPE_BEGINNING,
    [TimeTravelType.HISTORY_EVENT_TIME]: ProtoFlowResetType.FLOW_RESET_TYPE_HISTORY_EVENT_TIME,
    [TimeTravelType.STEP_TYPE]: ProtoFlowResetType.FLOW_RESET_TYPE_STEP_TYPE,
    [TimeTravelType.STEP_EXECUTION_ID]: ProtoFlowResetType.FLOW_RESET_TYPE_STEP_EXECUTION_ID,
  };
  return types[type];
}

function mapTimeTravelStepMethod(
  method: TimeTravelStepMethod | undefined,
): ProtoFlowResetStepMethod {
  switch (method) {
    case TimeTravelStepMethod.WAIT_FOR:
      return ProtoFlowResetStepMethod.FLOW_RESET_STEP_METHOD_WAIT_FOR;
    case TimeTravelStepMethod.EXECUTE:
      return ProtoFlowResetStepMethod.FLOW_RESET_STEP_METHOD_EXECUTE;
    default:
      return ProtoFlowResetStepMethod.FLOW_RESET_STEP_METHOD_UNSPECIFIED;
  }
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

function mapFlowStatus(status: ProtoFlowStatus): FlowStatus {
  switch (status) {
    case ProtoFlowStatus.FLOW_STATUS_RUNNING:
      return "running";
    case ProtoFlowStatus.FLOW_STATUS_COMPLETED:
      return "completed";
    case ProtoFlowStatus.FLOW_STATUS_FAILED:
      return "failed";
    case ProtoFlowStatus.FLOW_STATUS_SERVER_SIDE_TIMEOUT_INTERNAL_ONLY:
      return "serverSideTimeoutInternalOnly";
    case ProtoFlowStatus.FLOW_STATUS_TERMINATED:
      return "terminated";
    case ProtoFlowStatus.FLOW_STATUS_CANCELED:
      return "cancelled";
    case ProtoFlowStatus.FLOW_STATUS_CONTINUED_AS_NEW:
      return "continuedAsNew";
    default:
      throw new TypeError(`unsupported Flow status ${status}`);
  }
}

function mapFlowErrorType(type: ProtoFlowErrorType): FlowErrorTypeValue | undefined {
  switch (type) {
    case ProtoFlowErrorType.FLOW_ERROR_TYPE_STEP_DECISION_FAILING_FLOW:
      return FlowErrorType.STEP_DECISION_FAILED;
    case ProtoFlowErrorType.FLOW_ERROR_TYPE_CLIENT_API_FAILING_FLOW:
      return FlowErrorType.CLIENT_API_FAILED;
    case ProtoFlowErrorType.FLOW_ERROR_TYPE_WORKER_API_FAIL:
      return FlowErrorType.WORKER_API_FAILED;
    case ProtoFlowErrorType.FLOW_ERROR_TYPE_INVALID_USER_FLOW_CODE:
      return FlowErrorType.INVALID_USER_FLOW_CODE;
    case ProtoFlowErrorType.FLOW_ERROR_TYPE_INTERNAL:
      return FlowErrorType.INTERNAL;
    case ProtoFlowErrorType.FLOW_ERROR_TYPE_FLOW_TIMEOUT:
      return FlowErrorType.FLOW_TIMEOUT;
    default:
      return undefined;
  }
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

function physicalName(name: string, instance?: string): string {
  if (instance === undefined) {
    return name;
  }
  return physicalMapName(name, instance);
}

function requireRPCInvokeDefinition(
  flow: RegisteredFlow,
  definition: { readonly name: string },
  kind: string,
): void {
  if (flow.persistence.get(definition.name) !== definition) {
    throw new FlowDefinitionError(
      `Flow ${flow.name} does not register ${kind} ${definition.name}`,
    );
  }
}

function uniqueSorted(values: readonly string[]): string[] {
  return [...new Set(values)].sort();
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

function isWaitForAttributeOptions(value: unknown): value is WaitForAttributeOptions {
  return typeof value === "object" && value !== null;
}

interface ClientRequestAttempt {
  readonly requestTimeoutSeconds: number;
  readonly deadline?: Date;
}

class ClientRequestBudget {
  private readonly deadlineMs: number | undefined;

  public constructor(requestTimeoutMs: number | undefined) {
    const requestTimeoutSeconds = seconds32(requestTimeoutMs);
    this.deadlineMs = requestTimeoutSeconds === 0
      ? undefined
      : performance.now() + requestTimeoutSeconds * 1_000;
  }

  public nextAttempt(operation: string, flowId: string): ClientRequestAttempt {
    if (this.deadlineMs === undefined) {
      return { requestTimeoutSeconds: 0 };
    }
    const remainingMs = this.deadlineMs - performance.now();
    if (remainingMs <= 0) {
      throw this.timeoutError(operation, flowId);
    }
    return {
      requestTimeoutSeconds: Math.ceil(remainingMs / 1_000),
      deadline: new Date(Date.now() + remainingMs),
    };
  }

  public hasDeadline(): boolean {
    // Trust the transport's deadline expiry; its timer can precede the monotonic budget.
    return this.deadlineMs !== undefined;
  }

  public timeoutError(operation: string, flowId: string): RequestTimeoutError {
    return new RequestTimeoutError(
      status.DEADLINE_EXCEEDED,
      ErrorSubStatus.REQUEST_TIMEOUT,
      "request timed out",
      operation,
      flowId,
    );
  }
}

function seconds32(milliseconds: number | undefined): number {
  const value = seconds(milliseconds);
  if (value > 2_147_483_647) {
    throw new RangeError("duration exceeds the int32 seconds range");
  }
  return value;
}

function heartbeatSeconds(milliseconds: number | undefined): number {
  const value = seconds(milliseconds);
  if (value > 2_147_483_647) {
    throw new RangeError("heartbeat timeout exceeds int32 seconds");
  }
  return value;
}

function number64(value: bigint | undefined): number {
  if (value === undefined) {
    return 0;
  }
  const number = Number(value);
  if (!Number.isSafeInteger(number)) {
    throw new RangeError("history event ID exceeds JavaScript's safe integer range");
  }
  return number;
}

function unary<Response>(
  target: {
    readonly operation: string;
    readonly flowId?: string;
    readonly requirement: "none" | "existing" | "active";
  },
  invoke: (callback: (error: ServiceError | null, response: Response) => void) => unknown,
): Promise<Response> {
  return new Promise((resolve, reject) => {
    invoke((error, response) => {
      if (error !== null) {
        reject(translateServiceError(error, target.operation, target.flowId, target.requirement));
        return;
      }
      resolve(response);
    });
  });
}
