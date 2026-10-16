// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import assert from "node:assert/strict";
import { createConnection, createServer } from "node:net";
import test from "node:test";

import {
  Server,
  ServerCredentials,
  credentials,
  status as grpcStatus,
  type ClientReadableStream,
  type ServiceError,
  type sendUnaryData,
} from "@grpc/grpc-js";

import {
  Attribute,
  IndexType,
  Registry,
  StepList,
  Stream,
  Wait,
  Worker,
  gracefulComplete,
  stringCodec,
  type AsyncContext,
  type BlobCache,
  type Flow,
  type PersistenceSchema,
  type Step,
  type StepDecision,
} from "../src/index.js";
import {
  Context as ProtoContext,
  FlowServiceService,
  IndexType as ProtoIndexType,
  InvokeExecuteMethodRequest,
  InvokeWaitForMethodRequest,
  WorkerServiceClient,
  type ServerInfo,
  type FlowServiceServer,
  type SyncAttributeIndexRequest,
  type SyncAttributeIndexResponse,
} from "../src/gen/dex.js";
import { encodeValue } from "../src/value-mapper.js";

class IndexedFlow implements Flow {
  private readonly status = new Attribute(
    "status",
    stringCodec,
    { type: IndexType.KEYWORD, indexKey: "TypeScriptWorkerStatus" },
  );

  public getFlowType(): string {
    return "IndexedFlow";
  }

  public getSteps(): StepList<void> {
    return StepList.empty();
  }

  public getPersistenceSchema() {
    return { attributes: [this.status] };
  }
}

test("Worker synchronizes indexes before listening", async () => {
  const workerPort = await availablePort();
  let received: SyncAttributeIndexRequest | undefined;
  let listeningDuringSync: boolean | undefined;
  const calls: string[] = [];
  const flowServer = new Server();
  flowServer.addService(FlowServiceService, {
    getServerInfo(_call, callback: sendUnaryData<ServerInfo>) {
      calls.push("GetServerInfo");
      callback(null, compatibleServerInfo());
    },
    syncAttributeIndexes(call, callback: sendUnaryData<SyncAttributeIndexResponse>) {
      calls.push("SyncAttributeIndexes");
      received = call.request as SyncAttributeIndexRequest;
      void canConnect(workerPort).then((listening) => {
        listeningDuringSync = listening;
        callback(null, {});
      }, callback);
    },
  } as Partial<FlowServiceServer> as FlowServiceServer);
  const flowPort = await bind(flowServer);
  const worker = new Worker(new Registry([new IndexedFlow()]), new MemoryBlobCache(), {
    bindAddress: `127.0.0.1:${workerPort}`,
    serverAddress: `127.0.0.1:${flowPort}`,
  });
  try {
    await worker.start();
    assert.equal(
      received?.attributeIndexes.TypeScriptWorkerStatus,
      ProtoIndexType.INDEX_TYPE_KEYWORD,
    );
    assert.equal(listeningDuringSync, false);
    assert.deepEqual(calls, ["GetServerInfo", "SyncAttributeIndexes"]);
    assert.equal(await canConnect(workerPort), true);
  } finally {
    await worker.close();
    await shutdown(flowServer);
  }
});

test("Worker rejects incompatible Server before sync and binding", async () => {
  const workerPort = await availablePort();
  const calls: string[] = [];
  const flowServer = new Server();
  flowServer.addService(FlowServiceService, {
    getServerInfo(_call, callback: sendUnaryData<ServerInfo>) {
      calls.push("GetServerInfo");
      callback(null, {
        serverVersion: "future",
        minimumSupportedProtocolVersion: 4,
        currentProtocolVersion: 4,
      });
    },
    syncAttributeIndexes(_call, callback: sendUnaryData<SyncAttributeIndexResponse>) {
      calls.push("SyncAttributeIndexes");
      callback(null, {});
    },
  } as Partial<FlowServiceServer> as FlowServiceServer);
  const flowPort = await bind(flowServer);
  const worker = new Worker(new Registry([new IndexedFlow()]), new MemoryBlobCache(), {
    bindAddress: `127.0.0.1:${workerPort}`,
    serverAddress: `127.0.0.1:${flowPort}`,
  });
  try {
    await assert.rejects(worker.start(), (failure: Error) => {
      assert.match(String(failure.cause), /protocol intervals do not overlap/);
      return true;
    });
    assert.deepEqual(calls, ["GetServerInfo"]);
    assert.equal(await canConnect(workerPort), false);
  } finally {
    await worker.close();
    await shutdown(flowServer);
  }
});

test("Worker sync failure keeps its port closed", async () => {
  const workerPort = await availablePort();
  const flowServer = new Server();
  flowServer.addService(FlowServiceService, {
    getServerInfo(_call, callback: sendUnaryData<ServerInfo>) {
      callback(null, compatibleServerInfo());
    },
    syncAttributeIndexes(_call, callback: sendUnaryData<SyncAttributeIndexResponse>) {
      callback({ code: grpcStatus.PERMISSION_DENIED, details: "denied", name: "Error", message: "denied" });
    },
  } as Partial<FlowServiceServer> as FlowServiceServer);
  const flowPort = await bind(flowServer);
  const worker = new Worker(new Registry([new IndexedFlow()]), new MemoryBlobCache(), {
    bindAddress: `127.0.0.1:${workerPort}`,
    serverAddress: `127.0.0.1:${flowPort}`,
  });
  try {
    await assert.rejects(worker.start(), /cannot start TypeScript Worker/);
    assert.equal(await canConnect(workerPort), false);
  } finally {
    await worker.close();
    await shutdown(flowServer);
  }
});

test("Worker streams ordered Step progress and exactly one result", async () => {
  const workerPort = await availablePort();
  const flowServer = new Server();
  flowServer.addService(FlowServiceService, {
    getServerInfo(_call, callback: sendUnaryData<ServerInfo>) {
      callback(null, compatibleServerInfo());
    },
    syncAttributeIndexes(_call, callback: sendUnaryData<SyncAttributeIndexResponse>) {
      callback(null, {});
    },
  } as Partial<FlowServiceServer> as FlowServiceServer);
  const flowPort = await bind(flowServer);
  const flow = new StreamingWorkerFlow();
  const worker = new Worker(new Registry([flow]), new MemoryBlobCache(), {
    bindAddress: `127.0.0.1:${workerPort}`,
    serverAddress: `127.0.0.1:${flowPort}`,
  });
  const client = new WorkerServiceClient(
    `127.0.0.1:${workerPort}`,
    credentials.createInsecure(),
  );
  try {
    await worker.start();
    const waitFor = await observeStream(client.invokeWaitForMethod(
      InvokeWaitForMethodRequest.create({
        context: ProtoContext.create({ flowId: "flow-1", stepExecutionId: "step-1" }),
        flowType: flow.getFlowType(),
        stepType: flow.start.getStepType(),
        stepInput: encodeValue(stringCodec, "wait"),
        attributes: [],
      }),
    ));
    assert.equal(waitFor.error, undefined);
    assert.deepEqual(waitFor.outputs.map((output) => output.output?.$case), [
      "heartbeat",
      "streamWrite",
      "streamWrite",
      "result",
    ]);

    const execute = await observeStream(client.invokeExecuteMethod(executeRequest(flow, "success")));
    assert.equal(execute.error, undefined);
    assert.deepEqual(execute.outputs.map((output) => output.output?.$case), [
      "heartbeat",
      "streamWrite",
      "heartbeat",
      "streamWrite",
      "result",
    ]);
    assert.equal(
      execute.outputs.filter((output) => output.output?.$case === "result").length,
      1,
    );
    assert.equal(execute.outputs[2]?.output?.$case, "heartbeat");
    if (execute.outputs[2]?.output?.$case === "heartbeat") {
      assert.equal(execute.outputs[2].output.value.value, undefined);
    }
    assert.throws(() => flow.progress.write(flow.completedContext!, "late"), /no longer active/);
    assert.throws(() => void flow.completedContext!.recordHeartbeat("late"), /no longer active/);

    const failed = await observeStream(client.invokeExecuteMethod(executeRequest(flow, "failure")));
    assert.notEqual(failed.error, undefined);
    assert.deepEqual(failed.outputs.map((output) => output.output?.$case), ["heartbeat"]);

    const cancelledCall = client.invokeExecuteMethod(executeRequest(flow, "cancel"));
    const cancelled = observeStream(cancelledCall, () => cancelledCall.cancel());
    const cancelledResult = await cancelled;
    assert.notEqual(cancelledResult.error, undefined);
    assert.deepEqual(cancelledResult.outputs.map((output) => output.output?.$case), ["heartbeat"]);
  } finally {
    client.close();
    await worker.close();
    await shutdown(flowServer);
  }
});

class StreamingWorkerStep implements Step<string> {
  public readonly inputCodec = stringCodec;

  public constructor(
    private readonly flow: StreamingWorkerFlow,
    private readonly progress: Stream<string>,
  ) {}

  public getStepType(): string {
    return "StreamingWorkerStep";
  }

  public async waitFor(context: AsyncContext, _input: string): Promise<Wait> {
    await context.recordHeartbeat("waiting", stringCodec);
    this.progress.write(context, "wait-1");
    this.progress.write(context, "wait-2");
    return Wait.skipImmediately();
  }

  public async execute(context: AsyncContext, input: string): Promise<StepDecision> {
    await context.recordHeartbeat({ input });
    if (input === "failure") {
      throw new Error("handler failed after progress");
    }
    if (input === "cancel") {
      await aborted(context.cancellationSignal);
      return gracefulComplete(input);
    }
    this.progress.write(context, "execute-1");
    await context.recordHeartbeat(undefined);
    this.progress.write(context, "execute-2");
    this.flow.completedContext = context;
    return gracefulComplete(input);
  }
}

class StreamingWorkerFlow implements Flow<string> {
  public readonly progress = new Stream("worker-progress", stringCodec, 1 << 20);
  public readonly start = new StreamingWorkerStep(this, this.progress);
  public completedContext: AsyncContext | undefined;

  public getFlowType(): string {
    return "StreamingWorkerFlow";
  }

  public getSteps(): StepList<string> {
    return StepList.startStep(this.start);
  }

  public getPersistenceSchema(): PersistenceSchema {
    return { streams: [this.progress] };
  }
}

function executeRequest(flow: StreamingWorkerFlow, input: string): InvokeExecuteMethodRequest {
  return InvokeExecuteMethodRequest.create({
    context: ProtoContext.create({ flowId: "flow-1", stepExecutionId: "step-1" }),
    flowType: flow.getFlowType(),
    stepType: flow.start.getStepType(),
    stepInput: encodeValue(stringCodec, input),
    attributes: [],
    stepExeLocals: [],
  });
}

function observeStream<Output>(
  call: ClientReadableStream<Output>,
  afterFirstOutput?: () => void,
): Promise<{ outputs: Output[]; error: ServiceError | undefined }> {
  return new Promise((resolve) => {
    const outputs: Output[] = [];
    let isSettled = false;
    const settle = (error?: ServiceError): void => {
      if (isSettled) {
        return;
      }
      isSettled = true;
      resolve({ outputs, error });
    };
    call.on("data", (output: Output) => {
      outputs.push(output);
      if (outputs.length === 1) {
        afterFirstOutput?.();
      }
    });
    call.on("error", settle);
    call.on("end", () => settle());
  });
}

function aborted(signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) {
      resolve();
      return;
    }
    signal.addEventListener("abort", () => resolve(), { once: true });
  });
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

function compatibleServerInfo(): ServerInfo {
  return {
    serverVersion: "test",
    minimumSupportedProtocolVersion: 1,
    currentProtocolVersion: 1,
  };
}

function availablePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (address === null || typeof address === "string") {
        server.close();
        reject(new Error("failed to allocate a port"));
        return;
      }
      server.close((error) => error === undefined ? resolve(address.port) : reject(error));
    });
  });
}

function canConnect(port: number): Promise<boolean> {
  return new Promise((resolve) => {
    const socket = createConnection({ host: "127.0.0.1", port });
    socket.once("connect", () => {
      socket.destroy();
      resolve(true);
    });
    socket.once("error", () => {
      socket.destroy();
      resolve(false);
    });
  });
}

function bind(server: Server): Promise<number> {
  return new Promise((resolve, reject) => {
    server.bindAsync("127.0.0.1:0", ServerCredentials.createInsecure(), (error, port) => {
      if (error !== null) {
        reject(error);
        return;
      }
      resolve(port);
    });
  });
}

function shutdown(server: Server): Promise<void> {
  return new Promise((resolve, reject) => {
    server.tryShutdown((error) => error === undefined ? resolve() : reject(error));
  });
}
