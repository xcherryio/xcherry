// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import {
  Attribute,
  AttributeMatch,
  AttributeMap,
  AttributeMapNotLoadedError,
  ApiKeyCredential,
  Channel,
  ChannelMap,
  ChannelMessagesNotLoadedError,
  Client,
  FileCredential,
  ConditionCombination,
  FlowDefinitionError,
  InvalidStepResultError,
  Registry,
  Stream,
  StepList,
  Timer,
  ValueMappingError,
  Wait,
  bytesCodec,
  gracefulComplete,
  int64Codec,
  jsonCodec,
  openBlobCache,
  rpc,
  stringCodec,
  voidCodec,
  type BlobCache,
  type AsyncContext,
  type Context,
  type Credential,
  type Flow,
  type FlowConfig,
  type RPCResult,
  type RPCInvokeOptions,
  type Step,
  type StepDecision,
  type PullWorkerOptions,
  type PullWorkerTarget,
  type WorkerOptions,
  type SubFlowOptions,
} from "../src/index.js";
import {
  mapAttributeStoreNames,
  mapAttributeStoreSync,
} from "../src/attribute-store-sync.js";
import {
  Context as ProtoContext,
  InvokeExecuteMethodRequest,
  InvokeWaitForMethodRequest,
  InvokeWorkerRPCRequest,
  type StepStreamWrite,
  type Value,
} from "../src/gen/dex.js";
import {
  codecOrJson,
  decodeValue,
  encodeValue,
  type ValueHydrator,
} from "../src/value-mapper.js";
import { WorkerDispatcher } from "../src/worker-dispatcher.js";
import { registeredFlowByName } from "../src/flow.js";
import { InvocationContext } from "../src/invocation-context.js";

interface OrderInput {
  readonly orderId: string;
}

interface OrderOutput {
  readonly accepted: boolean;
}

const orderInput = jsonCodec<OrderInput>({
  typeName: "OrderInput",
  decode: (value) => {
    if (typeof value !== "object" || value === null || !("orderId" in value)) {
      throw new TypeError("invalid OrderInput");
    }
    const orderId = (value as { orderId: unknown }).orderId;
    if (typeof orderId !== "string") {
      throw new TypeError("invalid orderId");
    }
    return { orderId };
  },
});

const orderOutput = jsonCodec<OrderOutput>({
  typeName: "OrderOutput",
  decode: (value) => {
    if (typeof value !== "object" || value === null || !("accepted" in value)) {
      throw new TypeError("invalid OrderOutput");
    }
    return { accepted: Boolean((value as { accepted: unknown }).accepted) };
  },
});

const status = new Attribute("status", stringCodec);
const commands = new Channel("commands", orderInput);
const profiles = new AttributeMap("profiles", stringCodec);
const queuedCommands = new ChannelMap("queued-commands", stringCodec);
const progress = new Stream("progress", stringCodec, 10 * 1024 * 1024);

class ApproveOrder implements Step<OrderInput> {
  public readonly inputCodec = orderInput;

  public getStepType(): string {
    return "ApproveOrder";
  }

  public waitFor(_context: Context, _input: OrderInput): Wait {
    return Wait.anyOf(
      commands.forOne(),
      Timer.byDuration(10_000, "approval-timeout"),
    );
  }

  public execute(_context: Context, input: OrderInput): StepDecision {
    return gracefulComplete(input.orderId);
  }
}

class ArchiveOrder implements Step<string> {
  public readonly inputCodec = stringCodec;

  public getStepType(): string {
    return "ArchiveOrder";
  }

  public execute(_context: Context, input: string): StepDecision {
    return gracefulComplete(input);
  }
}

class Orders implements Flow<OrderInput> {
  public readonly approve = new ApproveOrder();
  public readonly archive = new ArchiveOrder();

  public getFlowType(): string {
    return "Orders";
  }

  public getSteps() {
    return StepList.startStep(this.approve).otherSteps(this.archive);
  }

  public getPersistenceSchema() {
    return {
      attributes: [status, profiles],
      channels: [commands, queuedCommands],
      streams: [progress],
    };
  }

  @rpc({
    name: "GetOrder",
    inputCodec: orderInput,
    outputCodec: orderOutput,
    lockAttributes: [status.lock()],
  })
  public getOrder(_context: Context, _input: OrderInput): RPCResult<OrderOutput> {
    return { output: { accepted: true } };
  }
}

const orders = new Orders();
const blobCache = {} as BlobCache;

test("credential contracts accept synchronous and asynchronous token providers", () => {
  const synchronous: Credential = { token: () => "current-token" };
  const asynchronous: Credential = { token: async () => "rotated-token" };
  const options: WorkerOptions = { credential: asynchronous, pull: {} };
  assert.equal(synchronous.token(), "current-token");
  assert.equal(options.credential, asynchronous);
  assert.equal(typeof FileCredential, "function");
  const exchange = new ApiKeyCredential({ tokenUrl: "http://127.0.0.1/token", serverId: "server", apiKey: "key" });
  assert.equal(typeof exchange.token, "function");
});

test("typed definitions construct without runtime", () => {
  const registry = new Registry([orders]);
  const definitions = [...orders.getSteps()];
  assert.equal(registry.flows[0]?.getFlowType(), "Orders");
  assert.equal(definitions[0]?.step, orders.approve);
  assert.equal(definitions[0]?.isStartStep, true);
  assert.equal(definitions[1]?.step, orders.archive);
  assert.equal(definitions[1]?.isStartStep, false);
});

test("attribute store synchronization is opt-in and immutable", () => {
  const plain = new Attribute("plain", stringCodec);
  const synced = plain.syncToAttributeStore();
  const syncedMap = new AttributeMap("map", stringCodec).syncToAttributeStore();
  const absent: FlowConfig = {};
  const named: FlowConfig = { attributeStoreNames: ["profiles", "audit"] };
  const disabled: FlowConfig = { attributeStoreNames: [] };

  assert.equal(mapAttributeStoreSync(plain), undefined);
  assert.equal(mapAttributeStoreSync(synced)?.enabled, true);
  assert.equal(mapAttributeStoreSync(syncedMap)?.enabled, true);
  assert.equal(mapAttributeStoreNames(absent), undefined);
  assert.deepEqual(mapAttributeStoreNames(named)?.names, ["profiles", "audit"]);
  assert.deepEqual(mapAttributeStoreNames(disabled)?.names, []);
});

test("registry rejects missing durable-name methods at runtime", () => {
  const missingFlowName = { getSteps: () => [] } as unknown as Flow<unknown>;
  assert.throws(() => new Registry([missingFlowName]), FlowDefinitionError);

  const missingStepName = {
    inputCodec: stringCodec,
    execute: () => gracefulComplete(),
  } as unknown as Step<string>;
  const flow: Flow<string> = {
    getFlowType: () => "MissingStepName",
    getSteps: () => StepList.startStep(missingStepName),
  };
  assert.throws(() => new Registry([flow]), FlowDefinitionError);
});

test("canonical codecs enforce wire kinds and int64 range", () => {
  assert.equal(int64Codec.decode(int64Codec.encode(42n)), 42n);
  assert.throws(() => int64Codec.encode(2n ** 63n), RangeError);
  assert.throws(() => int64Codec.decode(stringCodec.encode("42")), TypeError);
});

test("omitted codecs use identity JSON rather than scalar wire kinds", () => {
  const identity = jsonCodec<{ orderId: string }>();
  const order = { orderId: "order-1" };
  assert.deepEqual(identity.decode(identity.encode(order)), order);
  const jsonString = encodeValue(codecOrJson(), "hello");
  assert.equal(jsonString.kind?.$case, "objValue");
  assert.equal(jsonString.kind?.$case === "objValue" ? jsonString.kind.value.encoding : "", "json");
  const scalarString = encodeValue(stringCodec, "hello");
  assert.equal(scalarString.kind?.$case, "stringValue");
  const rawBytes = encodeValue(bytesCodec, new Uint8Array([1, 2, 3]));
  assert.equal(rawBytes.kind?.$case === "objValue" ? rawBytes.kind.value.encoding : "", "raw");
  const nullValue = encodeValue(codecOrJson(), null);
  assert.equal(nullValue.kind?.$case, "nullValue");
  assert.equal(decodeValue(codecOrJson(), nullValue), null);
  assert.equal(decodeValue(voidCodec, nullValue), undefined);
});

test("object Step and RPC omit codecs and still encode JSON", async () => {
  class JsonStep implements Step<OrderInput> {
    public getStepType(): string {
      return "JsonStep";
    }

    public execute(_context: Context, input: OrderInput): StepDecision {
      return gracefulComplete(input);
    }
  }

  class JsonFlow implements Flow<OrderInput> {
    public readonly start = new JsonStep();

    public getFlowType(): string {
      return "JsonFlow";
    }

    public getSteps(): StepList<OrderInput> {
      return StepList.startStep(this.start);
    }

    @rpc()
    public describe(_context: Context, input: OrderInput): RPCResult<OrderInput> {
      return { output: input };
    }

    @rpc()
    public ping(_context: Context): void {}
  }

  const flow = new JsonFlow();
  const hydrator = {
    hydrateAll: async (_flowId: string, values: readonly unknown[]) => values,
  } as unknown as ValueHydrator;
  const dispatcher = new WorkerDispatcher(new Registry([flow]), hydrator, () => 3);
  const executed = await dispatcher.invokeExecute(
    InvokeExecuteMethodRequest.create({
      context: ProtoContext.create(),
      flowType: flow.getFlowType(),
      stepType: flow.start.getStepType(),
      stepInput: encodeValue(codecOrJson(), { orderId: "order-1" }),
      attributes: [],
      stepExeLocals: [],
    }),
  );
  const closeInput = executed.stepDecision?.closeDecision?.closeInput;
  assert.equal(closeInput?.kind?.$case, "objValue");
  const described = await dispatcher.invokeRPC(
    InvokeWorkerRPCRequest.create({
      context: ProtoContext.create(),
      flowType: flow.getFlowType(),
      rpcName: "describe",
      input: encodeValue(codecOrJson(), { orderId: "order-1" }),
      attributes: [],
    }),
  );
  assert.equal(described.output?.kind?.$case, "objValue");
  const pinged = await dispatcher.invokeRPC(
    InvokeWorkerRPCRequest.create({
      context: ProtoContext.create(),
      flowType: flow.getFlowType(),
      rpcName: "ping",
      attributes: [],
    }),
  );
  assert.equal(pinged.output?.kind?.$case, "nullValue");
});

test("fluent wait factories validate channel bounds", () => {
  const wait = Wait.until(Timer.byDuration(1_000));
  assert.equal(wait.conditions.length, 1);
  assert.throws(() => commands.range(), /requires a bound/);
});

test("registry rejects duplicate definitions", () => {
  assert.throws(() => new Registry([orders, orders]), FlowDefinitionError);
});

test("registry rejects duplicate Step classes", () => {
  class DuplicateStep implements Step<number> {
    public constructor(private readonly type: string) {}

    public getStepType(): string {
      return this.type;
    }

    public execute(_context: Context, input: number): StepDecision {
      return gracefulComplete(input);
    }
  }

  class DuplicateStepFlow implements Flow<number> {
    public getFlowType(): string {
      return "DuplicateStepFlow";
    }

    public getSteps(): StepList<number> {
      return StepList.startStep(new DuplicateStep("first")).otherSteps(
        new DuplicateStep("second"),
      );
    }
  }

  assert.throws(() => new Registry([new DuplicateStepFlow()]), /duplicate Step class/);
});

test("value mapping failures have a stable error type", () => {
  const invalid = {
    get orderId(): string {
      throw new TypeError("invalid order input");
    },
  };
  assert.throws(
    () => encodeValue(orderInput, invalid),
    ValueMappingError,
  );
});

test("invalid Step results include Flow and Step context", async () => {
  class InvalidStep implements Step<string> {
    public readonly inputCodec = stringCodec;

    public getStepType(): string {
      return "InvalidStep";
    }

    public execute(_context: Context, _input: string): StepDecision {
      return undefined as unknown as StepDecision;
    }
  }

  class InvalidFlow implements Flow<string> {
    public readonly start = new InvalidStep();

    public getFlowType(): string {
      return "InvalidFlow";
    }

    public getSteps(): StepList<string> {
      return StepList.startStep(this.start);
    }
  }

  const flow = new InvalidFlow();
  const hydrator = {
    hydrateAll: async (_flowId: string, values: readonly unknown[]) => values,
  } as unknown as ValueHydrator;
  const dispatcher = new WorkerDispatcher(new Registry([flow]), hydrator, () => 3);
  const invocation = dispatcher.invokeExecute(
    InvokeExecuteMethodRequest.create({
      context: ProtoContext.create(),
      flowType: flow.getFlowType(),
      stepType: flow.start.getStepType(),
      stepInput: encodeValue(stringCodec, "input"),
      attributes: [],
      stepExeLocals: [],
    }),
  );
  await assert.rejects(invocation, (failure: unknown) => {
    assert.ok(failure instanceof InvalidStepResultError);
    assert.equal(failure.flowType, "InvalidFlow");
    assert.equal(failure.stepType, "InvalidStep");
    assert.equal(failure.method, "execute");
    return true;
  });
});

test("Worker maps only user-provided Condition IDs", async () => {
  const conditionChannel = new Channel("condition-commands", stringCodec);
  class ConditionStep implements Step<string> {
    public readonly inputCodec = stringCodec;

    public getStepType(): string {
      return "ConditionStep";
    }

    public waitFor(_context: Context, input: string): Wait {
      if (input === "unnamed") {
        return Wait.anyOf(Timer.byDuration(1_000), conditionChannel.forOne());
      }
      if (input === "missing") {
        return Wait.anyCombinationOf(ConditionCombination.of(conditionChannel.forOne()));
      }
      if (input === "duplicate") {
        return Wait.anyCombinationOf(
          ConditionCombination.of(
            conditionChannel.forOne("same"),
            Timer.byDuration(1_000, "same"),
          ),
        );
      }
      const reused = conditionChannel.forOne("__dex_internal_condition_0");
      return Wait.anyCombinationOf(
        ConditionCombination.of(reused),
        ConditionCombination.of(reused),
      );
    }

    public execute(_context: Context, input: string): StepDecision {
      return gracefulComplete(input);
    }
  }
  class ConditionFlow implements Flow<string> {
    public readonly start = new ConditionStep();

    public getFlowType(): string {
      return "ConditionFlow";
    }

    public getSteps(): StepList<string> {
      return StepList.startStep(this.start);
    }

    public getPersistenceSchema() {
      return { channels: [conditionChannel] };
    }
  }
  const flow = new ConditionFlow();
  const hydrator = {
    hydrateAll: async (_flowId: string, values: readonly unknown[]) => values,
  } as unknown as ValueHydrator;
  const dispatcher = new WorkerDispatcher(new Registry([flow]), hydrator, () => 3);
  const invoke = (input: string) =>
    dispatcher.invokeWaitFor(
      InvokeWaitForMethodRequest.create({
        context: ProtoContext.create(),
        flowType: flow.getFlowType(),
        stepType: flow.start.getStepType(),
        stepInput: encodeValue(stringCodec, input),
      }),
    );
  const unnamed = await invoke("unnamed");
  assert.equal(unnamed.waitingCondition?.timerConditions[0]?.conditionId, "");
  assert.equal(unnamed.waitingCondition?.channelConditions[0]?.conditionId, "");
  const reused = await invoke("reused");
  assert.equal(
    reused.waitingCondition?.channelConditions[0]?.conditionId,
    "__dex_internal_condition_0",
  );
  assert.deepEqual(
    reused.waitingCondition?.conditionCombinations.map((combination) => combination.conditionIds),
    [["__dex_internal_condition_0"], ["__dex_internal_condition_0"]],
  );
  await assert.rejects(invoke("missing"), /requires every Condition/);
  await assert.rejects(invoke("duplicate"), /duplicate Condition ID/);
  assert.throws(() => conditionChannel.forOne(""), /must not be empty/);
});

test("Step Stream writes emit every message on the active invocation", async () => {
  const thinking = new Stream("thinking", stringCodec, 1_048_576);
  const broken = new Stream("broken", {
    typeName: "broken",
    wireKind: "string",
    encode() {
      throw new Error("cannot encode");
    },
    decode() {
      return "unused";
    },
  }, 1_048_576);
  class StreamStep implements Step<string> {
    public readonly inputCodec = stringCodec;

    public getStepType(): string {
      return "StreamStep";
    }

    public execute(context: Context, input: string): StepDecision {
      thinking.write(context, input);
      thinking.write(context, `${input}-again`);
      return gracefulComplete(input);
    }
  }
  class StreamFlow implements Flow<string> {
    public readonly start = new StreamStep();

    public getFlowType(): string {
      return "StreamFlow";
    }

    public getSteps(): StepList<string> {
      return StepList.startStep(this.start);
    }

    public getPersistenceSchema() {
      return { streams: [thinking, broken] };
    }
  }
  const writes: StepStreamWrite[] = [];
  const output = {
    recordHeartbeat: async () => {},
    writeStream(write: StepStreamWrite) {
      writes.push(write);
    },
  };
  const flow = new StreamFlow();
  const hydrator = {
    hydrateAll: async (_flowId: string, values: readonly unknown[]) => values,
  } as unknown as ValueHydrator;
  const registry = new Registry([flow]);
  const dispatcher = new WorkerDispatcher(registry, hydrator, () => 3);
  await dispatcher.invokeExecute(
    InvokeExecuteMethodRequest.create({
      context: ProtoContext.create({
        flowId: "flow-1",
        runId: "run-1",
        stepExecutionId: "step-1",
      }),
      flowType: flow.getFlowType(),
      stepType: flow.start.getStepType(),
      stepInput: encodeValue(stringCodec, "checking"),
      attributes: [],
      stepExeLocals: [],
    }),
    new AbortController().signal,
    output,
  );
  assert.deepEqual(writes, [
    {
      streamName: "thinking",
      streamCapacityBytes: 1_048_576n,
      value: encodeValue(stringCodec, "checking"),
    },
    {
      streamName: "thinking",
      streamCapacityBytes: 1_048_576n,
      value: encodeValue(stringCodec, "checking-again"),
    },
  ]);

  const direct = new InvocationContext(
    "execute",
    registeredFlowByName(registry, flow.getFlowType()),
    ProtoContext.create(),
    [],
    [],
    undefined,
    {},
    new AbortController().signal,
    output,
  );
  assert.throws(() => broken.write(direct, "value"), ValueMappingError);
  assert.throws(
    () => new Stream("unregistered", stringCodec, 1_024).write(direct, "value"),
    /does not register/,
  );
  const rpc = new InvocationContext(
    "rpc",
    registeredFlowByName(registry, flow.getFlowType()),
    ProtoContext.create(),
    [],
  );
  assert.throws(() => thinking.write(rpc, "value"), /require a Step Context/);
  assert.throws(() => void rpc.recordHeartbeat("value"), /require an async Step Context/);
});

test("buffered text Stream flushes on timer and before the final result", async () => {
  const thinking = new Stream("thinking", stringCodec, 1_048_576);
  let handlerFinished = false;
  let releaseHandler: (() => void) | undefined;
  class BufferedStep implements Step<string> {
    public readonly inputCodec = stringCodec;

    public getStepType(): string {
      return "BufferedStep";
    }

    public async execute(context: Context, input: string): Promise<StepDecision> {
      const progress = thinking.bufferedText(context, {
        flushIntervalMs: 5,
        maxBufferedBytes: 1_024,
      });
      progress.write(input);
      await new Promise<void>((resolve) => {
        releaseHandler = resolve;
      });
      progress.write(" ");
      progress.write("世界");
      handlerFinished = true;
      return gracefulComplete(input);
    }
  }
  class BufferedFlow implements Flow<string> {
    public readonly start = new BufferedStep();

    public getFlowType(): string {
      return "BufferedFlow";
    }

    public getSteps(): StepList<string> {
      return StepList.startStep(this.start);
    }

    public getPersistenceSchema() {
      return { streams: [thinking] };
    }
  }
  const observed: Array<{ value: string; handlerFinished: boolean }> = [];
  const output = {
    recordHeartbeat: async () => {},
    writeStream(write: StepStreamWrite) {
      observed.push({
        value: write.value?.kind?.$case === "stringValue" ? write.value.kind.value : "",
        handlerFinished,
      });
      releaseHandler?.();
    },
  };
  const flow = new BufferedFlow();
  const dispatcher = new WorkerDispatcher(
    new Registry([flow]),
    {
      hydrateAll: async (_flowId: string, values: readonly unknown[]) => values,
    } as unknown as ValueHydrator,
    () => 3,
  );
  await dispatcher.invokeExecute(
    InvokeExecuteMethodRequest.create({
      context: ProtoContext.create({ stepExecutionId: "step-1" }),
      flowType: flow.getFlowType(),
      stepType: flow.start.getStepType(),
      stepInput: encodeValue(stringCodec, "hello"),
    }),
    new AbortController().signal,
    output,
  );
  assert.deepEqual(observed, [
    { value: "hello", handlerFinished: false },
    { value: " 世界", handlerFinished: true },
  ]);
});

test("Async Step Context preserves heartbeat Value presence and codecs", async () => {
  const observed: Array<{ hasValue: boolean; value: unknown }> = [];
  class HeartbeatStep implements Step<string> {
    public readonly inputCodec = stringCodec;

    public getStepType(): string {
      return "HeartbeatStep";
    }

    public async execute(context: AsyncContext, input: string): Promise<StepDecision> {
      observed.push({
        hasValue: context.hasLastHeartbeatValue(),
        value: input === "string"
          ? context.getLastHeartbeatValue(stringCodec)
          : context.getLastHeartbeatValue(),
      });
      await context.recordHeartbeat({ input });
      await context.recordHeartbeat(input, stringCodec);
      await context.recordHeartbeat(null);
      await context.recordHeartbeat(undefined);
      if (input === "unreachable") {
        // @ts-expect-error The heartbeat value is required, including when explicitly undefined.
        await context.recordHeartbeat();
      }
      return gracefulComplete(input);
    }
  }
  class HeartbeatFlow implements Flow<string> {
    public readonly start = new HeartbeatStep();

    public getFlowType(): string {
      return "HeartbeatFlow";
    }

    public getSteps(): StepList<string> {
      return StepList.startStep(this.start);
    }
  }
  const heartbeats: Array<Value | undefined> = [];
  const output = {
    recordHeartbeat(value: Value | undefined): Promise<void> {
      heartbeats.push(value);
      return Promise.resolve();
    },
    writeStream(_write: StepStreamWrite): void {},
  };
  const flow = new HeartbeatFlow();
  const hydrator = {
    hydrateAll: async (_flowId: string, values: readonly unknown[]) => values,
  } as unknown as ValueHydrator;
  const dispatcher = new WorkerDispatcher(new Registry([flow]), hydrator, () => 3);
  const invoke = async (input: string, lastHeartbeatValue?: Value): Promise<void> => {
    await dispatcher.invokeExecute(
      InvokeExecuteMethodRequest.create({
        context: ProtoContext.create({ lastHeartbeatValue }),
        flowType: flow.getFlowType(),
        stepType: flow.start.getStepType(),
        stepInput: encodeValue(stringCodec, input),
        attributes: [],
        stepExeLocals: [],
      }),
      new AbortController().signal,
      output,
    );
  };

  await invoke("absent");
  await invoke("string", encodeValue(stringCodec, "restored"));
  await invoke("null", encodeValue(codecOrJson(), null));
  assert.deepEqual(observed, [
    { hasValue: false, value: undefined },
    { hasValue: true, value: "restored" },
    { hasValue: true, value: null },
  ]);
  assert.equal(heartbeats.length, 12);
  for (let offset = 0; offset < heartbeats.length; offset += 4) {
    assert.equal(heartbeats[offset]?.kind?.$case, "objValue");
    assert.equal(heartbeats[offset + 1]?.kind?.$case, "stringValue");
    assert.equal(heartbeats[offset + 2]?.kind?.$case, "nullValue");
    assert.equal(heartbeats[offset + 3], undefined);
  }
});

test("Context exposes the recovery error", () => {
  class RecoveryFlow implements Flow<void> {
    public getFlowType(): string {
      return "RecoveryFlow";
    }

    public getSteps(): StepList<void> {
      return StepList.empty();
    }
  }
  const flow = new RecoveryFlow();
  const context = new InvocationContext(
    "execute",
    registeredFlowByName(new Registry([flow]), flow.getFlowType()),
    ProtoContext.create({
      recoveryError: {
        detail: "handler exhausted retries",
        errorType: "TimeoutError",
      },
    }),
    [],
  );
  assert.deepEqual(context.recoveryError, {
    detail: "handler exhausted retries",
    errorType: "TimeoutError",
  });
});

class UnsupportedSynchronousHeartbeatStep implements Step<string> {
  public getStepType(): string {
    return "UnsupportedSynchronousHeartbeatStep";
  }

  // @ts-expect-error AsyncContext handlers must return a Promise.
  public execute(_context: AsyncContext, input: string): StepDecision {
    return gracefulComplete(input);
  }
}

void UnsupportedSynchronousHeartbeatStep;

test("map introspection tracks buffered changes", () => {
  const attributes = new AttributeMap("items", stringCodec);
  const channels = new ChannelMap("messages", stringCodec);
  class MapFlow implements Flow<void> {
    public getFlowType(): string {
      return "MapFlow";
    }

    public getSteps(): StepList<void> {
      return StepList.empty();
    }

    public getPersistenceSchema() {
      return { attributes: [attributes], channels: [channels] };
    }
  }
  const registry = new Registry([new MapFlow()]);
  const special = "special % key";
  const physical = (name: string, instance: string) =>
    `${name}/${encodeURIComponent(instance).replace(/[!'()*]/g, (character) =>
      `%${character.charCodeAt(0).toString(16).toUpperCase()}`,
    )}`;
  const context = new InvocationContext(
    "rpc",
    registeredFlowByName(registry, "MapFlow"),
    ProtoContext.create(),
    [
      { key: physical("items", special), value: encodeValue(stringCodec, "initial") },
      { key: physical("items", "z"), value: encodeValue(stringCodec, "remove") },
    ],
    [],
    undefined,
    {
      [physical("messages", special)]: { size: 1 },
      [physical("messages", "empty")]: { size: 0 },
    },
    undefined,
    undefined,
    {},
    ["items/"],
  );
  assert.deepEqual(attributes.getAllInstanceKeys(context), [special, "z"]);
  attributes.set(context, "a", "added");
  attributes.delete(context, "z");
  assert.deepEqual(attributes.getAllInstanceKeys(context), ["a", special]);
  assert.equal(attributes.getMapSize(context), 2);
  assert.deepEqual(channels.getAllInstanceKeys(context), [special]);
  channels.publish(context, "a", "published");
  assert.deepEqual(channels.getAllInstanceKeys(context), ["a", special]);
  assert.equal(channels.getMapSize(context), 2);
});

test("RPC selective state snapshots are typed and distinguish not loaded", () => {
  const attributes = new AttributeMap("selected-items", stringCodec);
  const queued = new Channel("selected-commands", stringCodec);
  const byTenant = new ChannelMap("selected-by-tenant", stringCodec);
  class SelectiveFlow implements Flow<void> {
    public getFlowType(): string {
      return "SelectiveFlow";
    }

    public getSteps(): StepList<void> {
      return StepList.empty();
    }

    public getPersistenceSchema() {
      return { attributes: [attributes], channels: [queued, byTenant] };
    }
  }
  const registry = new Registry([new SelectiveFlow()]);
  const context = new InvocationContext(
    "rpc",
    registeredFlowByName(registry, "SelectiveFlow"),
    ProtoContext.create(),
    [{ key: "selected-items/tenant-a", value: encodeValue(stringCodec, "value") }],
    [],
    undefined,
    {
      "selected-commands": { size: 1 },
      "selected-by-tenant/tenant-a": { size: 1 },
    },
    undefined,
    undefined,
    {
      "selected-commands": {
        messages: [{
          channelName: "selected-commands",
          messageId: "message-1",
          value: encodeValue(stringCodec, "first"),
        }],
      },
      "selected-by-tenant/tenant-a": {
        messages: [{
          channelName: "selected-by-tenant/tenant-a",
          messageId: "message-2",
          value: encodeValue(stringCodec, "second"),
        }],
      },
    },
    ["selected-items/tenant-a"],
    ["selected-commands"],
    ["selected-by-tenant/tenant-a"],
  );

  assert.equal(attributes.get(context, "tenant-a"), "value");
  assert.deepEqual(queued.pendingMessages(context), [
    { messageId: "message-1", value: "first" },
  ]);
  assert.deepEqual(byTenant.pendingMessages(context, "tenant-a"), [
    { messageId: "message-2", value: "second" },
  ]);
  assert.equal(queued.findPendingMessage(context, "message-1")?.value, "first");
  assert.throws(() => attributes.get(context, "other"), AttributeMapNotLoadedError);
  assert.throws(
    () => byTenant.pendingMessages(context, "other"),
    ChannelMessagesNotLoadedError,
  );
});

test("registry rejects invalid and duplicate handler state loads", () => {
  const attributes = new AttributeMap("registry-items", stringCodec);
  const queued = new Channel("registry-commands", stringCodec);
  class DuplicateLoads implements Flow<void> {
    public getFlowType(): string {
      return "DuplicateLoads";
    }

    public getSteps(): StepList<void> {
      return StepList.empty();
    }

    public getPersistenceSchema() {
      return { attributes: [attributes], channels: [queued] };
    }

    @rpc({ loadAttributeMaps: [attributes, attributes] })
    public inspect(_context: Context): void {}
  }

  assert.throws(() => new Registry([new DuplicateLoads()]), /duplicate/);

  class DuplicateStep implements Step<void> {
    public getStepType(): string {
      return "DuplicateStep";
    }

    public getStepOptions() {
      return { executeLoadChannels: [queued, queued] };
    }

    public execute(_context: Context, _input: void): StepDecision {
      return { kind: "deadEnd" };
    }
  }

  class DuplicateStepFlow implements Flow<void> {
    private readonly start = new DuplicateStep();

    public getFlowType(): string {
      return "DuplicateStepFlow";
    }

    public getSteps(): StepList<void> {
      return StepList.startStep(this.start);
    }

    public getPersistenceSchema() {
      return { channels: [queued] };
    }
  }

  assert.throws(() => new Registry([new DuplicateStepFlow()]), /duplicate/);
});

test("persistence definitions and map instances reserve slash", () => {
  assert.throws(() => new Attribute("orders/by-id", stringCodec), TypeError);
  assert.throws(() => new AttributeMap("orders/by-id", stringCodec), TypeError);
  assert.throws(() => new Channel("orders/by-id", stringCodec), TypeError);
  assert.throws(() => new ChannelMap("orders/by-id", stringCodec), TypeError);

  const messages = new ChannelMap("messages", stringCodec);
  assert.throws(() => messages.forOne("orders/by-id"), /map instances must not contain/);
  const items = new AttributeMap("items", stringCodec);
  assert.throws(() => items.lock("orders/by-id"), /map instances must not contain/);
});

test("blob cache contract opens the native DXBC cache", () => {
  const directory = mkdtempSync(join(tmpdir(), "dex-typescript-blob-cache-"));
  const cache = openBlobCache({ directory, maxBytes: 1024, frequencyCounters: 0 });
  try {
    assert.equal(cache.put("blob", Buffer.from("payload")), true);
    assert.deepEqual(cache.get("blob"), new Uint8Array(Buffer.from("payload")));
    cache.delete("blob");
    assert.equal(cache.get("blob"), undefined);
  } finally {
    cache.close();
    rmSync(directory, { recursive: true, force: true });
  }
  assert.throws(() => openBlobCache({ directory: "", maxBytes: 1024 }), TypeError);
});

async function compileStrongTypes(client: Client): Promise<void> {
  const pullOptions: PullWorkerOptions = { maxConcurrentTasks: 8, idleStreamsPerQueue: 1 };
  const workerOptions: WorkerOptions = { pull: pullOptions };
  const pullTarget: PullWorkerTarget = { pull: true };
  const pullConfig: FlowConfig = { workerTarget: pullTarget };
  const subFlowOptions: SubFlowOptions = { configOverride: pullConfig };
  void workerOptions;
  void subFlowOptions;
  const runId: string = await client.startFlow(orders, "order-1", { orderId: "order-1" });
  const output: OrderOutput = await client.invokeRPC(
    orders.getOrder,
    "order-1",
    { orderId: "order-1" },
  );
  const invokeOptions: RPCInvokeOptions = {
    lockAttributeMapInstances: [profiles.lock("partition-007")],
    loadAttributeMapInstances: [profiles.load("partition-007")],
    loadChannelMapInstances: [queuedCommands.loadMessages("partition-007")],
  };
  const selectedOutput: OrderOutput = await client.invokeRPCWithOptions(
    orders.getOrder,
    "order-1",
    { orderId: "order-1" },
    invokeOptions,
  );
  const matchedStatus: string = await client.waitForAttributeMatch(
    "order-1",
    status,
    AttributeMatch.equalTo("ready"),
    { requestId: "wait-order-status", requestTimeoutMs: 30_000 },
  );
  const matchedItem: string = await client.waitForAttributeMatch(
    "order-1",
    new AttributeMap("items", stringCodec),
    "one",
    AttributeMatch.equalTo("ready"),
    { requestId: "wait-order-item", requestTimeoutMs: 30_000 },
  );
  await client.writeStream("order-1", progress, "frontend/1", "starting");
  const progressMessage = await client.readStream("order-1", progress, "", 30_000);
  const progressValue: string = progressMessage.value;
  const progressPage = await client.listStreamMessages("order-1", progress, 100);
  const progressPageValue: string | undefined = progressPage.messages[0]?.value;
  void runId;
  void output;
  void selectedOutput;
  void matchedStatus;
  void matchedItem;
  void progressValue;
  void progressPageValue;

  // @ts-expect-error wrong Flow input
  await client.startFlow(orders, "order-1", { accepted: true });
  // @ts-expect-error wrong RPC input
  await client.invokeRPC(orders.getOrder, "order-1", { accepted: true });
  // @ts-expect-error start Step input must match Flow input
  const mismatchedSteps: StepList<OrderInput> = StepList.startStep(orders.archive);
  void mismatchedSteps;
}

void compileStrongTypes;
