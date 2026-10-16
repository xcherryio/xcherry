// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import type { Codec } from "./codec.js";
import {
  isHardDeleteOnNullDefinition,
  requireHardDeleteOnNullValueKind,
} from "./attribute-hard-delete.js";
import { mapAttributeStoreSync } from "./attribute-store-sync.js";
import type { AsyncContext, RecoveryErrorInfo } from "./context.js";
import {
  ConditionStatus,
  IndexType as ProtoIndexType,
  type AttributeWrite,
  type ChannelInfo,
  type ChannelMessage as ProtoChannelMessage,
  type ChannelValues,
  type ChannelMessageDeletion,
  type ConditionResults,
  type Context as ProtoContext,
  type IndexConfig,
  type KV,
  type StepStreamWrite,
  type Value,
} from "./gen/dex.js";
import { requireFlowStream, type RegisteredFlow } from "./flow.js";
import { createFlowResultFromProto, type FlowResult } from "./flow-result.js";
import { AttributeMap, IndexType, type Attribute } from "./persistence.js";
import { codecOrJson, decodeValue, deletionValue, encodeValue } from "./value-mapper.js";
import {
  AttributeMapNotLoadedError,
  ChannelMessagesNotLoadedError,
} from "./errors.js";
import { physicalMapName, requireMapInstance, requireName } from "./validation.js";
import { ChannelMap, type Channel, type ChannelMessage } from "./wait.js";
import type { Stream } from "./stream.js";

export type InvocationMethod = "waitFor" | "execute" | "rpc";

export interface StepOutputEmitter {
  recordHeartbeat(value: Value | undefined): Promise<void>;
  writeStream(write: StepStreamWrite): void;
}

export interface StepOutputFinalizer {
  finalizeStepOutput(): void;
  cancelStepOutput(): void;
}

export class InvocationContext implements AsyncContext {
  public readonly flowId: string;
  public readonly runId: string;
  public readonly flowStartedAt: Date;
  public readonly stepExecutionId: string;
  public readonly fromStepExecutionId: string;
  public readonly recoveryError: RecoveryErrorInfo | undefined;
  public readonly firstAttemptAt: Date;
  public readonly attempt: number;
  public readonly cancellationSignal: AbortSignal;

  private readonly attributes: Map<string, Value>;
  private readonly locals: Map<string, Value>;
  private readonly channelInfos: Map<string, ChannelInfo>;
  private readonly loadedChannelMessages: Readonly<Record<string, ChannelValues>>;
  private readonly loadedAttributeMapInstances: ReadonlySet<string>;
  private readonly loadedChannelNames: ReadonlySet<string>;
  private readonly loadedChannelMapInstances: ReadonlySet<string>;
  private readonly attributeWrites = new Map<string, AttributeWrite>();
  private readonly localWrites = new Map<string, KV>();
  private readonly events: KV[] = [];
  private readonly eventNames = new Set<string>();
  private readonly publications: ProtoChannelMessage[] = [];
  private readonly channelDeletions: ChannelMessageDeletion[] = [];
  private readonly lastHeartbeatValue: Value | undefined;
  private readonly stepOutputFinalizers: StepOutputFinalizer[] = [];
  private stepOutputsFinalized = false;

  public constructor(
    private readonly method: InvocationMethod,
    private readonly flow: RegisteredFlow,
    metadata: ProtoContext | undefined,
    attributes: readonly KV[],
    locals: readonly KV[] = [],
    private readonly conditionResults?: ConditionResults,
    channelInfos: Readonly<Record<string, ChannelInfo>> = {},
    cancellationSignal: AbortSignal = new AbortController().signal,
    private readonly stepOutput?: StepOutputEmitter,
    loadedChannelMessages: Readonly<Record<string, ChannelValues>> = {},
    loadedAttributeMapInstances: readonly string[] = [],
    loadedChannelNames: readonly string[] = [],
    loadedChannelMapInstances: readonly string[] = [],
  ) {
    if (metadata === undefined) {
      throw new TypeError("Worker request Context is required");
    }
    this.flowId = metadata.flowId;
    this.runId = metadata.runId;
    this.flowStartedAt = secondsDate(metadata.flowStartedTimestamp);
    this.stepExecutionId = metadata.stepExecutionId;
    this.fromStepExecutionId = metadata.fromStepExecutionId;
    this.recoveryError = metadata.recoveryError;
    this.firstAttemptAt = secondsDate(metadata.firstAttemptTimestamp);
    this.attempt = metadata.attempt;
    this.lastHeartbeatValue = metadata.lastHeartbeatValue;
    this.cancellationSignal = cancellationSignal;
    this.attributes = mapValues("Attribute", attributes);
    this.locals = mapValues("step-execution local", locals);
    this.channelInfos = new Map(Object.entries(channelInfos));
    this.loadedChannelMessages = loadedChannelMessages;
    this.loadedAttributeMapInstances = new Set(loadedAttributeMapInstances);
    this.loadedChannelNames = new Set(loadedChannelNames);
    this.loadedChannelMapInstances = new Set(loadedChannelMapInstances);
    cancellationSignal.addEventListener("abort", () => this.cancelStepOutputs(), { once: true });
  }

  public hasLastHeartbeatValue(): boolean {
    return this.lastHeartbeatValue !== undefined;
  }

  public getLastHeartbeatValue<T = unknown>(codec?: Codec<T>): T | undefined {
    if (this.lastHeartbeatValue === undefined) {
      return undefined;
    }
    return decodeValue(codecOrJson(codec), this.lastHeartbeatValue);
  }

  public recordHeartbeat<T>(value: T | undefined, codec?: Codec<T>): Promise<void> {
    if (this.stepOutput === undefined) {
      throw new TypeError("Heartbeats require an async Step Context");
    }
    return this.stepOutput.recordHeartbeat(
      value === undefined ? undefined : encodeValue(codecOrJson(codec), value),
    );
  }

  public writeStream<T>(stream: Stream<T>, value: T): void {
    if (this.stepOutput === undefined) {
      throw new TypeError("Stream writes require a Step Context");
    }
    requireFlowStream(this.flow, stream as Stream<unknown>);
    this.stepOutput.writeStream({
      streamName: stream.name,
      streamCapacityBytes: BigInt(stream.streamCapacityBytes),
      value: encodeValue(stream.codec, value),
    });
  }

  public prepareBufferedStream(stream: Stream<unknown>): void {
    if (this.stepOutput === undefined || (this.method !== "waitFor" && this.method !== "execute")) {
      throw new TypeError("Buffered Streams require an async Step Context");
    }
    requireFlowStream(this.flow, stream);
  }

  public registerStepOutputFinalizer(finalizer: StepOutputFinalizer): void {
    if (this.stepOutputsFinalized || this.stepOutput === undefined) {
      throw new TypeError("Buffered Stream invocation has finished");
    }
    this.stepOutputFinalizers.push(finalizer);
  }

  public finalizeStepOutputs(failure?: unknown): unknown | undefined {
    if (this.stepOutputsFinalized) {
      return failure;
    }
    this.stepOutputsFinalized = true;
    const failures: unknown[] = failure === undefined ? [] : [failure];
    for (const finalizer of this.stepOutputFinalizers) {
      try {
        finalizer.finalizeStepOutput();
      } catch (finalizerFailure) {
        failures.push(finalizerFailure);
      }
    }
    if (failures.length === 0) {
      return undefined;
    }
    if (failures.length === 1) {
      return failures[0];
    }
    return new AggregateError(failures, "Step handler and buffered Stream finalization failed");
  }

  private cancelStepOutputs(): void {
    if (this.stepOutputsFinalized) {
      return;
    }
    this.stepOutputsFinalized = true;
    for (const finalizer of this.stepOutputFinalizers) {
      finalizer.cancelStepOutput();
    }
  }

  public hasTimerFired(index?: number): boolean {
    const results = this.conditionResults?.timerResults ?? [];
    if (index !== undefined) {
      return results[index]?.conditionStatus === ConditionStatus.CONDITION_STATUS_COMPLETED;
    }
    return results.some(
      (result) => result.conditionStatus === ConditionStatus.CONDITION_STATUS_COMPLETED,
    );
  }

  public waitForMethodFailed(): boolean {
    return this.conditionResults?.waitForFailed ?? false;
  }

  public subFlowResult(index = 0): FlowResult {
    if (this.method !== "execute") {
      throw new TypeError("SubFlow results are available only during execute");
    }
    const result = this.conditionResults?.subFlowResults[index];
    if (index < 0 || result === undefined) {
      throw new RangeError(`SubFlow result index is unavailable: ${index}`);
    }
    return createFlowResultFromProto(
      result,
      result.results.map((completion) => {
        if (completion.completedStepOutput === undefined) {
          throw new TypeError("SubFlow Step completion output is required");
        }
        return completion.completedStepOutput;
      }),
    );
  }

  public subFlowId(index = 0): string {
    this.subFlowResult(index);
    return `SubFlow:${this.flowId}-${this.stepExecutionId}-${index}`;
  }

  public setStepExecutionLocal<T>(key: string, value: T, codec: Codec<T>): void {
    requireName(key);
    this.localWrites.set(key, { key, value: encodeValue(codec, value) });
  }

  public getStepExecutionLocal<T>(key: string, codec: Codec<T>): T | undefined {
    requireName(key);
    const value = this.localWrites.get(key)?.value ?? this.locals.get(key);
    return value === undefined ? undefined : decodeValue(codec, value);
  }

  public recordEvent<T>(name: string, value: T, codec: Codec<T>): void {
    requireName(name);
    if (this.eventNames.has(name)) {
      throw new TypeError(`event was already recorded: ${name}`);
    }
    this.eventNames.add(name);
    this.events.push({ key: name, value: encodeValue(codec, value) });
  }

  public getAttribute<T>(
    attribute: Attribute<T> | AttributeMap<T>,
    instance?: string,
  ): T {
    this.requireRegistered(attribute);
    const key = definitionName(attribute, instance);
    if (
      attribute instanceof AttributeMap &&
      !mapInstanceLoaded(this.loadedAttributeMapInstances, attribute.name, key)
    ) {
      throw new AttributeMapNotLoadedError(
        `AttributeMap instance was not loaded for this invocation: ${key}`,
      );
    }
    const write = this.attributeWrites.get(key)?.value;
    if (write?.kind?.$case === "nullValue") {
      return defaultValue(attribute.codec);
    }
    const value = write ?? this.attributes.get(key);
    return value === undefined ? defaultValue(attribute.codec) : decodeValue(attribute.codec, value);
  }

  public setAttribute<T>(
    attribute: Attribute<T> | AttributeMap<T>,
    value: T,
    instance?: string,
  ): void {
    this.requireRegistered(attribute);
    const key = definitionName(attribute, instance);
    this.attributeWrites.set(key, {
      key,
      value: requireHardDeleteOnNullValueKind(attribute, key, encodeValue(attribute.codec, value)),
      indexConfig: mapIndex(attribute.index),
      syncConfig: mapAttributeStoreSync(attribute),
      isHardDeleteOnNull: isHardDeleteOnNullDefinition(attribute),
    });
  }

  public deleteAttribute(
    attribute: Attribute<unknown> | AttributeMap<unknown>,
    instance?: string,
  ): void {
    this.requireRegistered(attribute);
    const key = definitionName(attribute, instance);
    this.attributeWrites.set(key, {
      key,
      value: deletionValue(),
      indexConfig: mapIndex(attribute.index),
      syncConfig: mapAttributeStoreSync(attribute),
      isHardDeleteOnNull: isHardDeleteOnNullDefinition(attribute),
    });
  }

  public attributeMapKeys(attribute: AttributeMap<unknown>): readonly string[] {
    this.requireRegistered(attribute);
    if (!this.loadedAttributeMapInstances.has(`${attribute.name}/`)) {
      throw new AttributeMapNotLoadedError(
        `all AttributeMap instances were not loaded for this invocation: ${attribute.name}`,
      );
    }
    const prefix = `${attribute.name}/`;
    const physicalKeys = new Set(
      [...this.attributes.keys()].filter((key) => key.startsWith(prefix)),
    );
    for (const [key, write] of this.attributeWrites) {
      if (!key.startsWith(prefix)) {
        continue;
      }
      if (write.value?.kind?.$case === "nullValue") {
        physicalKeys.delete(key);
      } else {
        physicalKeys.add(key);
      }
    }
    return [...physicalKeys]
      .map((key) => requireMapInstance(decodeURIComponent(key.slice(prefix.length))))
      .sort();
  }

  public publish<T>(channel: Channel<T> | ChannelMap<T>, value: T, instance?: string): void {
    this.requireRegistered(channel);
    const name = definitionName(channel, instance);
    this.publications.push({
      channelName: name,
      value: encodeValue(channel.codec, value),
      messageId: "",
    });
    this.channelInfos.set(name, { size: (this.channelInfos.get(name)?.size ?? 0) + 1 });
  }

  public deleteChannelMessage(
    channel: Channel<unknown> | ChannelMap<unknown>,
    messageId: string,
    instance?: string,
  ): void {
    this.requireRegistered(channel);
    const name = definitionName(channel, instance);
    this.channelDeletions.push({ channelName: name, messageId: requireName(messageId) });
    const size = this.channelInfos.get(name)?.size ?? 0;
    if (size > 0) {
      this.channelInfos.set(name, { size: size - 1 });
    }
  }

  public channelSize(
    channel: Channel<unknown> | ChannelMap<unknown>,
    instance?: string,
  ): number {
    this.requireRegistered(channel);
    return this.channelInfos.get(definitionName(channel, instance))?.size ?? 0;
  }

  public pendingChannelMessages<T>(
    channel: Channel<T> | ChannelMap<T>,
    instance?: string,
  ): readonly ChannelMessage<T>[] {
    this.requireRegistered(channel);
    const name = definitionName(channel, instance);
    const isLoaded = channel instanceof ChannelMap
      ? mapInstanceLoaded(this.loadedChannelMapInstances, channel.name, name)
      : this.loadedChannelNames.has(name);
    if (!isLoaded) {
      throw new ChannelMessagesNotLoadedError(
        `Channel messages were not loaded for this invocation: ${name}`,
      );
    }
    return (this.loadedChannelMessages[name]?.messages ?? []).map((message) => {
      if (message.value === undefined) {
        throw new TypeError(`Channel message has no Value: ${message.messageId}`);
      }
      return {
        messageId: message.messageId,
        value: decodeValue(channel.codec, message.value),
      };
    });
  }

  public channelMapKeys(channel: ChannelMap<unknown>): readonly string[] {
    this.requireRegistered(channel);
    const prefix = `${channel.name}/`;
    return [...this.channelInfos]
      .filter(([key, info]) => key.startsWith(prefix) && info.size > 0)
      .map(([key]) => requireMapInstance(decodeURIComponent(key.slice(prefix.length))))
      .sort();
  }

  public channelResults<T>(
    channel: Channel<T> | ChannelMap<T>,
    instance?: string,
  ): readonly T[] {
    this.requireRegistered(channel);
    const name = definitionName(channel, instance);
    return (this.conditionResults?.channelResults ?? [])
      .filter(
        (result) =>
          result.channelName === name &&
          result.conditionStatus === ConditionStatus.CONDITION_STATUS_COMPLETED,
      )
      .flatMap((result) => result.values.map((value) => decodeValue(channel.codec, value)));
  }

  public getAttributeWrites(): readonly AttributeWrite[] {
    return [...this.attributeWrites.values()];
  }

  public getLocalWrites(): readonly KV[] {
    return [...this.localWrites.values()];
  }

  public getEvents(): readonly KV[] {
    return this.events;
  }

  public getPublications(): readonly ProtoChannelMessage[] {
    return this.publications;
  }

  public getChannelDeletions(): readonly ChannelMessageDeletion[] {
    return this.channelDeletions;
  }

  private requireRegistered(
    definition:
      | Attribute<unknown>
      | AttributeMap<unknown>
      | Channel<unknown>
      | ChannelMap<unknown>,
  ): void {
    if (this.flow.persistence.get(definition.name) !== definition) {
      throw new TypeError(`persistence definition does not belong to Flow: ${definition.name}`);
    }
  }
}

function definitionName(
  definition:
    | Attribute<unknown>
    | AttributeMap<unknown>
    | Channel<unknown>
    | ChannelMap<unknown>,
  instance?: string,
): string {
  if (definition instanceof AttributeMap || definition instanceof ChannelMap) {
    if (instance === undefined) {
      throw new TypeError(`map definition ${definition.name} requires an instance`);
    }
    return physicalMapName(definition.name, instance);
  }
  if (instance !== undefined) {
    throw new TypeError(`static definition ${definition.name} cannot use an instance`);
  }
  return definition.name;
}

function mapInstanceLoaded(instances: ReadonlySet<string>, name: string, physical: string): boolean {
  return instances.has(`${name}/`) || instances.has(physical);
}

function mapValues(kind: string, entries: readonly KV[]): Map<string, Value> {
  const values = new Map<string, Value>();
  for (const entry of entries) {
    if (entry.key === "" || entry.value === undefined || values.has(entry.key)) {
      throw new TypeError(`invalid or duplicate ${kind}`);
    }
    values.set(entry.key, entry.value);
  }
  return values;
}

function defaultValue<T>(codec: Codec<T>): T {
  switch (codec.wireKind) {
    case "bool":
      return false as T;
    case "double":
      return 0 as T;
    case "int64":
      return 0n as T;
    default:
      return undefined as T;
  }
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
  return { enable: true, type: types[index.type], indexKey: index.indexKey ?? "" };
}

function secondsDate(seconds: bigint): Date {
  const milliseconds = Number(seconds * 1_000n);
  if (!Number.isSafeInteger(milliseconds)) {
    throw new RangeError("Context timestamp exceeds JavaScript's safe Date range");
  }
  return new Date(milliseconds);
}
