// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import assert from "node:assert/strict";
import test from "node:test";

import { Server, ServerCredentials, status as grpcStatus, type sendUnaryData } from "@grpc/grpc-js";

import {
  Attribute,
  AttributeMap,
  AttributeMatch,
  Client,
  IndexType,
  InitialAttribute,
  Registry,
  StepList,
  SubFlow,
  ValueMappingError,
  Wait,
  booleanCodec,
  bytesCodec,
  doubleCodec,
  gracefulComplete,
  int64Codec,
  jsonCodec,
  optionalCodec,
  rpc,
  stringCodec,
  voidCodec,
  type BlobCache,
  type Codec,
  type Context,
  type Flow,
  type PersistenceSchema,
  type Step,
  type StepDecision,
} from "../src/index.js";
import { mapAttributeStoreSync } from "../src/attribute-store-sync.js";
import {
  Context as ProtoContext,
  FlowServiceService,
  InvokeWaitForMethodRequest,
  InvokeWorkerRPCRequest,
  type AttributeWrite,
  type FlowServiceServer,
  type ServerInfo,
  type StartFlowRequest,
  type StartFlowResponse,
} from "../src/gen/dex.js";
import { maximumSupportedServerProtocolVersion } from "../src/server-protocol.js";
import type { ValueHydrator } from "../src/value-mapper.js";
import { WorkerDispatcher } from "../src/worker-dispatcher.js";

type VendorToken = { readonly token: string };

class HardDeleteChildStep implements Step<string> {
  public readonly inputCodec = stringCodec;

  public getStepType(): string {
    return "HardDeleteChildStep";
  }

  public execute(_context: Context, input: string): StepDecision {
    return gracefulComplete(input);
  }
}

class HardDeleteChildFlow implements Flow<string> {
  public readonly apiKey = new Attribute("child-api-key", stringCodec).hardDeleteOnNull();
  private readonly start = new HardDeleteChildStep();

  public getFlowType(): string {
    return "HardDeleteChildFlow";
  }

  public getSteps() {
    return StepList.startStep(this.start);
  }

  public getPersistenceSchema(): PersistenceSchema {
    return { attributes: [this.apiKey] };
  }
}

class StartHardDeleteChildStep implements Step<string> {
  public readonly inputCodec = stringCodec;

  public constructor(private readonly child: HardDeleteChildFlow) {}

  public getStepType(): string {
    return "StartHardDeleteChildStep";
  }

  public waitFor(_context: Context, input: string): Wait {
    return Wait.until(SubFlow.run(this.child, input, {
      attributes: [InitialAttribute.of(this.child.apiKey, "child-secret")],
    }));
  }

  public execute(_context: Context, input: string): StepDecision {
    return gracefulComplete(input);
  }
}

class HardDeleteUnitFlow implements Flow<string> {
  public readonly apiKey = new Attribute("api-key", stringCodec).hardDeleteOnNull();
  public readonly tokens = new AttributeMap("tokens", jsonCodec<VendorToken>()).hardDeleteOnNull();
  public readonly numeric = new Attribute("numeric", optionalCodec(doubleCodec)).hardDeleteOnNull();
  public readonly plain = new Attribute("plain", stringCodec);
  private readonly start: StartHardDeleteChildStep;

  public constructor(public readonly child: HardDeleteChildFlow) {
    this.start = new StartHardDeleteChildStep(child);
  }

  public getFlowType(): string {
    return "HardDeleteUnitFlow";
  }

  public getSteps() {
    return StepList.startStep(this.start);
  }

  public getPersistenceSchema(): PersistenceSchema {
    return { attributes: [this.apiKey, this.tokens, this.numeric, this.plain] };
  }

  @rpc()
  public writeSecrets(context: Context): void {
    this.apiKey.set(context, "secret-1");
    this.tokens.set(context, "billing", { token: "token-1" });
    this.plain.set(context, "visible");
  }

  @rpc()
  public deleteSecrets(context: Context): void {
    this.apiKey.delete(context);
    this.tokens.delete(context, "billing");
  }

  @rpc()
  public writeNumeric(context: Context): void {
    this.numeric.set(context, 7);
  }

  @rpc()
  public clearNumeric(context: Context): void {
    this.numeric.set(context, undefined);
  }
}

test("hardDeleteOnNull is immutable and maps onto Attribute writes", () => {
  const plain = new Attribute("plain", stringCodec);
  const hardDelete = plain.hardDeleteOnNull();
  const hardDeleteMap = new AttributeMap("map", bytesCodec).hardDeleteOnNull();

  assert.notEqual(hardDelete, plain);
  assert.equal(hardDelete.name, plain.name);
  assert.equal(hardDelete.codec, plain.codec);
  assert.equal(mapAttributeStoreSync(hardDelete), undefined);
  assert.ok(hardDeleteMap instanceof AttributeMap);
  const allowedCodecs: readonly Codec<any>[] = [
    stringCodec,
    bytesCodec,
    jsonCodec(),
    voidCodec,
    optionalCodec(stringCodec),
  ];
  for (const codec of allowedCodecs) {
    assert.doesNotThrow(() => new Attribute("allowed", codec).hardDeleteOnNull());
  }
});

test("hardDeleteOnNull rejects indexes, Attribute Store sync, and scalar codecs", () => {
  assert.throws(
    () => new Attribute("indexed", stringCodec, { type: IndexType.KEYWORD }).hardDeleteOnNull(),
    /hard-delete Attribute indexed cannot be indexed/,
  );
  assert.throws(
    () => new AttributeMap("indexed-map", stringCodec, { type: IndexType.KEYWORD, indexKey: "Key" })
      .hardDeleteOnNull(),
    /hard-delete Attribute indexed-map cannot be indexed/,
  );
  assert.throws(
    () => new Attribute("synced", stringCodec).syncToAttributeStore().hardDeleteOnNull(),
    /cannot sync to an Attribute Store/,
  );
  assert.throws(
    () => new AttributeMap("synced-map", stringCodec).syncToAttributeStore().hardDeleteOnNull(),
    /cannot sync to an Attribute Store/,
  );
  assert.throws(
    () => new Attribute("secret", stringCodec).hardDeleteOnNull().syncToAttributeStore(),
    /syncToAttributeStore does not support hard-delete Attribute secret/,
  );
  assert.throws(
    () => new AttributeMap("secrets", stringCodec).hardDeleteOnNull().syncToAttributeStore(),
    /syncToAttributeStore does not support hard-delete Attribute secrets/,
  );
  for (const [codec, wireKind] of [
    [doubleCodec, "double"],
    [int64Codec, "int64"],
    [booleanCodec, "bool"],
  ] as const) {
    assert.throws(
      () => new Attribute("scalar", codec as never).hardDeleteOnNull(),
      new RegExp(`supports only string and object values; codec \\w+ uses ${wireKind}`),
    );
    assert.throws(
      () => new AttributeMap("scalar-map", codec as never).hardDeleteOnNull(),
      /supports only string and object values/,
    );
  }
});

test("Worker handler writes flag hard-delete Attributes and reject scalar values", async () => {
  const flow = new HardDeleteUnitFlow(new HardDeleteChildFlow());
  const dispatcher = new WorkerDispatcher(new Registry([flow, flow.child]), passthroughHydrator(), () => 3);

  const written = await dispatcher.invokeRPC(rpcRequest(flow, "writeSecrets"));
  const writes = writesByKey(written.upsertAttributes);
  assert.equal(writes.get("api-key")?.isHardDeleteOnNull, true);
  assert.equal(writes.get("api-key")?.value?.kind?.$case, "stringValue");
  assert.equal(writes.get("tokens/billing")?.isHardDeleteOnNull, true);
  assert.equal(writes.get("tokens/billing")?.value?.kind?.$case, "objValue");
  assert.equal(writes.get("plain")?.isHardDeleteOnNull, false);

  const deleted = await dispatcher.invokeRPC(rpcRequest(flow, "deleteSecrets"));
  for (const write of deleted.upsertAttributes) {
    assert.equal(write.isHardDeleteOnNull, true);
    assert.equal(write.value?.kind?.$case, "nullValue");
  }
  assert.deepEqual(deleted.upsertAttributes.map((write) => write.key).sort(), ["api-key", "tokens/billing"]);

  await assert.rejects(
    dispatcher.invokeRPC(rpcRequest(flow, "writeNumeric")),
    (failure: Error) => {
      assert.ok(failure instanceof ValueMappingError);
      assert.match(failure.message, /hard-delete Attribute numeric accepts only string, object, or null values; got doubleValue/);
      return true;
    },
  );
  const cleared = await dispatcher.invokeRPC(rpcRequest(flow, "clearNumeric"));
  assert.equal(cleared.upsertAttributes[0]?.value?.kind?.$case, "nullValue");
  assert.equal(cleared.upsertAttributes[0]?.isHardDeleteOnNull, true);

  const waited = await dispatcher.invokeWaitFor(waitForRequest(flow));
  const childWrite = waited.waitingCondition?.subFlowConditions[0]?.options?.attributes[0];
  assert.equal(childWrite?.key, "child-api-key");
  assert.equal(childWrite?.isHardDeleteOnNull, true);
  assert.equal(childWrite?.value?.kind?.$case, "stringValue");
});

test("Worker rejects hard-delete writes below Server protocol version 3", async () => {
  const flow = new HardDeleteUnitFlow(new HardDeleteChildFlow());
  const dispatcher = new WorkerDispatcher(new Registry([flow, flow.child]), passthroughHydrator(), () => 2);

  await assert.rejects(
    dispatcher.invokeRPC(rpcRequest(flow, "writeSecrets")),
    /hard-delete Attribute api-key requires Server protocol version 3 or later; negotiated protocol version is 2/,
  );
  await assert.rejects(
    dispatcher.invokeRPC(rpcRequest(flow, "deleteSecrets")),
    /requires Server protocol version 3 or later/,
  );
  await assert.rejects(
    dispatcher.invokeWaitFor(waitForRequest(flow)),
    /hard-delete Attribute child-api-key requires Server protocol version 3 or later/,
  );
});

test("TypeScript SDK supports Server protocol version 3", () => {
  assert.equal(maximumSupportedServerProtocolVersion, 3);
});

test("Client negotiates once before hard-delete startFlow writes", async () => {
  const flow = new HardDeleteUnitFlow(new HardDeleteChildFlow());
  const fake = await startFakeFlowService(3);
  const client = new Client(new Registry([flow, flow.child]), new MemoryBlobCache(), {
    serverAddress: fake.address,
  });
  try {
    await client.startFlow(flow, "plain-start", "input", {
      attributes: [InitialAttribute.of(flow.plain, "visible")],
    });
    assert.deepEqual(fake.calls, ["StartFlow"]);

    for (const id of ["hard-delete-1", "hard-delete-2"]) {
      await client.startFlow(flow, id, "input", {
        attributes: [
          InitialAttribute.of(flow.apiKey, "secret-1"),
          InitialAttribute.mapValue(flow.tokens, "billing", { token: "token-1" }),
          InitialAttribute.of(flow.plain, "visible"),
        ],
      });
    }
    assert.deepEqual(fake.calls, ["StartFlow", "GetServerInfo", "StartFlow", "StartFlow"]);
    const writes = writesByKey(fake.startRequests.at(-1)?.flowStartOptions?.attributes ?? []);
    assert.equal(writes.get("api-key")?.isHardDeleteOnNull, true);
    assert.equal(writes.get("tokens/billing")?.isHardDeleteOnNull, true);
    assert.equal(writes.get("plain")?.isHardDeleteOnNull, false);
  } finally {
    await client.close();
    await fake.close();
  }
});

test("Client rejects hard-delete startFlow writes below Server protocol version 3", async () => {
  const flow = new HardDeleteUnitFlow(new HardDeleteChildFlow());
  const fake = await startFakeFlowService(2);
  const client = new Client(new Registry([flow, flow.child]), new MemoryBlobCache(), {
    serverAddress: fake.address,
  });
  try {
    for (let attempt = 0; attempt < 2; attempt += 1) {
      await assert.rejects(
        client.startFlow(flow, "old-server", "input", {
          attributes: [InitialAttribute.of(flow.apiKey, "secret-1")],
        }),
        /hard-delete Attribute api-key requires Server protocol version 3 or later; negotiated protocol version is 2/,
      );
    }
    assert.deepEqual(fake.calls, ["GetServerInfo"]);
  } finally {
    await client.close();
    await fake.close();
  }
});

test("Client retries Server protocol negotiation after GetServerInfo fails", async () => {
  const flow = new HardDeleteUnitFlow(new HardDeleteChildFlow());
  const fake = await startFakeFlowService(3, 1);
  const client = new Client(new Registry([flow, flow.child]), new MemoryBlobCache(), {
    serverAddress: fake.address,
  });
  try {
    const start = () => client.startFlow(flow, "retry-negotiation", "input", {
      attributes: [InitialAttribute.of(flow.apiKey, "secret-1")],
    });
    await assert.rejects(start(), /GetServerInfo failed/);
    await start();
    assert.deepEqual(fake.calls, ["GetServerInfo", "GetServerInfo", "StartFlow"]);
  } finally {
    await client.close();
    await fake.close();
  }
});

test("Client rejects invalid hard-delete Attribute use before contacting Dex", async () => {
  const flow = new HardDeleteUnitFlow(new HardDeleteChildFlow());
  const fake = await startFakeFlowService(3);
  const client = new Client(new Registry([flow, flow.child]), new MemoryBlobCache(), {
    serverAddress: fake.address,
  });
  try {
    await assert.rejects(
      client.waitForAttributeMatch("flow", flow.apiKey, AttributeMatch.equalTo("secret"), {}),
      (failure: Error) => {
        assert.ok(failure instanceof TypeError);
        assert.match(failure.message, /waitForAttributeMatch does not support hard-delete Attribute api-key/);
        return true;
      },
    );
    await assert.rejects(
      client.waitForAttributeMatch("flow", flow.tokens, "billing", AttributeMatch.equalTo({ token: "x" }), {}),
      /waitForAttributeMatch does not support hard-delete Attribute tokens/,
    );
    await assert.rejects(
      client.startFlow(flow, "unregistered", "input", {
        attributes: [InitialAttribute.of(new Attribute("api-key", stringCodec), "secret")],
      }),
      /hard-delete Attribute api-key must use the definition registered with Flow HardDeleteUnitFlow/,
    );
    await assert.rejects(
      client.startFlow(flow, "unregistered", "input", {
        attributes: [InitialAttribute.of(new Attribute("other", stringCodec).hardDeleteOnNull(), "secret")],
      }),
      /hard-delete Attribute other must use the definition registered with Flow HardDeleteUnitFlow/,
    );
    await assert.rejects(
      client.startFlow(flow, "numeric", "input", {
        attributes: [InitialAttribute.of(flow.numeric, 7)],
      }),
      (failure: Error) => {
        assert.ok(failure instanceof ValueMappingError);
        assert.match(failure.message, /hard-delete Attribute numeric accepts only string, object, or null values/);
        return true;
      },
    );
    assert.deepEqual(fake.calls, []);
  } finally {
    await client.close();
    await fake.close();
  }
});

function rpcRequest(flow: HardDeleteUnitFlow, rpcName: string): InvokeWorkerRPCRequest {
  return InvokeWorkerRPCRequest.create({
    context: ProtoContext.create({ flowId: "flow-1" }),
    flowType: flow.getFlowType(),
    rpcName,
    attributes: [],
  });
}

function waitForRequest(flow: HardDeleteUnitFlow): InvokeWaitForMethodRequest {
  return InvokeWaitForMethodRequest.create({
    context: ProtoContext.create({ flowId: "flow-1", stepExecutionId: "step-1" }),
    flowType: flow.getFlowType(),
    stepType: "StartHardDeleteChildStep",
    stepInput: { kind: { $case: "stringValue", value: "child-input" } },
    attributes: [],
  });
}

function writesByKey(writes: readonly AttributeWrite[]): Map<string, AttributeWrite> {
  return new Map(writes.map((write) => [write.key, write]));
}

function passthroughHydrator(): ValueHydrator {
  return {
    hydrateAll: async (_flowId: string, values: readonly unknown[]) => values,
  } as unknown as ValueHydrator;
}

interface FakeFlowService {
  readonly address: string;
  readonly calls: string[];
  readonly startRequests: StartFlowRequest[];
  close(): Promise<void>;
}

async function startFakeFlowService(
  currentProtocolVersion: number,
  failedServerInfoRequests = 0,
): Promise<FakeFlowService> {
  const calls: string[] = [];
  const startRequests: StartFlowRequest[] = [];
  let remainingServerInfoFailures = failedServerInfoRequests;
  const server = new Server();
  server.addService(FlowServiceService, {
    getServerInfo(_call, callback: sendUnaryData<ServerInfo>) {
      calls.push("GetServerInfo");
      if (remainingServerInfoFailures > 0) {
        remainingServerInfoFailures -= 1;
        callback({ code: grpcStatus.UNAVAILABLE, details: "unavailable", name: "Error", message: "unavailable" });
        return;
      }
      callback(null, {
        serverVersion: "test",
        minimumSupportedProtocolVersion: 1,
        currentProtocolVersion,
      });
    },
    startFlow(call, callback: sendUnaryData<StartFlowResponse>) {
      calls.push("StartFlow");
      startRequests.push(call.request as StartFlowRequest);
      callback(null, { runId: "run-1" });
    },
    waitForAttribute(_call, callback) {
      calls.push("WaitForAttribute");
      callback({ code: grpcStatus.INVALID_ARGUMENT, details: "unexpected", name: "Error", message: "unexpected" });
    },
  } as Partial<FlowServiceServer> as FlowServiceServer);
  const port = await new Promise<number>((resolve, reject) => {
    server.bindAsync("127.0.0.1:0", ServerCredentials.createInsecure(), (error, boundPort) => {
      if (error !== null) {
        reject(error);
        return;
      }
      resolve(boundPort);
    });
  });
  return {
    address: `127.0.0.1:${port}`,
    calls,
    startRequests,
    close: () => new Promise((resolve) => server.tryShutdown(() => resolve())),
  };
}

class MemoryBlobCache implements BlobCache {
  public readonly config = { directory: "memory", maxBytes: 1_024 };

  public get(_blobId: string): Uint8Array | undefined {
    return undefined;
  }

  public put(_blobId: string, _payload: Uint8Array): boolean {
    return true;
  }

  public delete(_blobId: string): void {}

  public deleteAll(): void {}

  public close(): void {}
}
