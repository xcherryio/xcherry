// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import assert from "node:assert/strict";
import test from "node:test";

import {
  Server,
  ServerCredentials,
  status,
  type ServerDuplexStream,
} from "@grpc/grpc-js";

import { Client, Registry, StepList, Worker, retryAfter, type BlobCache, type Flow, type PullWorkerTarget } from "../src/index.js";
import {
  InvokeWorkerRPCRequest,
  InvokeWorkerRPCResponse,
  InvokeWaitForMethodRequest,
  InvokeWaitForMethodResponse,
  PollWorkerTaskRejectReason,
  Value,
  WorkerTaskServiceService,
  type PollWorkerTaskRequest,
  type PollWorkerTaskResponse,
  type WorkerTaskServiceServer,
} from "../src/gen/dex.js";
import { workerErrorResponse, workerServiceError } from "../src/grpc-status.js";
import { PullWorker } from "../src/pull-worker.js";
import { WorkerDispatcher } from "../src/worker-dispatcher.js";

type TaskStream = ServerDuplexStream<PollWorkerTaskRequest, PollWorkerTaskResponse>;

class TestFlow implements Flow<void> {
  public getFlowType(): string { return "TestFlow"; }
  public getSteps(): StepList<void> { return StepList.empty(); }
}

test("pull options reject listener conflicts, invalid capacity, and Worker IDs", async () => {
  const registry = new Registry([new TestFlow()]);
  const cache = {} as BlobCache;
  assert.throws(() => new Worker(registry, cache, { pull: {}, bindAddress: "127.0.0.1:8000" }), TypeError);
  assert.throws(() => new Worker(registry, cache, { pull: {}, workerTarget: { address: "" } }), TypeError);
  assert.throws(() => new Worker(registry, cache, { pull: { maxConcurrentTasks: -1 } }), RangeError);
  assert.throws(() => new Worker(registry, cache, { pull: { idleStreamsPerQueue: 1.2 } }), RangeError);
  assert.throws(() => new Worker(registry, cache, { pull: { stopDrainTimeoutMs: -1 } }), RangeError);
  assert.throws(() => new Worker(registry, cache, { pull: { appWorkerID: "bad/id" } }), TypeError);
  assert.throws(() => new Worker(registry, cache, { pull: { appWorkerID: "TestFlow" } }), TypeError);
  assert.throws(() => new Client(registry, cache, { workerTarget: { pull: true, address: "127.0.0.1:8000" } as unknown as PullWorkerTarget }), TypeError);
  assert.throws(() => new Client(registry, cache, { workerTarget: { pull: true, headless: true } as unknown as PullWorkerTarget }), TypeError);
  const emptyTargetClient = new Client(registry, cache, { workerTarget: { address: "" } });
  await emptyTargetClient.close();
  const emptyTargetWorker = new Worker(registry, cache, { workerTarget: { address: "" } });
  await emptyTargetWorker.close();
  const worker = new Worker(registry, cache, { pull: {} });
  assert.deepEqual(worker.workerTarget, { address: "", pull: true });
  await worker.close();
});

test("generated appWorkerID uses host identity and eight base32 characters", async () => {
  const registry = new Registry([]);
  const dispatcher = {} as WorkerDispatcher;
  const first = new PullWorker(registry, dispatcher, "127.0.0.1:1", {});
  const second = new PullWorker(registry, dispatcher, "127.0.0.1:1", {});
  try {
    assert.match(first.appWorkerID, /^[A-Za-z0-9._-]{1,54}-[a-z2-7]{8}$/);
    assert.notEqual(first.appWorkerID, second.appWorkerID);
  } finally {
    await first.close();
    await second.close();
  }
});

test("generated appWorkerID uses HOSTNAME when higher-priority identities are absent", async () => {
  const previous = {
    dex: process.env.DEX_WORKER_INSTANCE,
    pod: process.env.POD_NAME,
    host: process.env.HOSTNAME,
  };
  delete process.env.DEX_WORKER_INSTANCE;
  delete process.env.POD_NAME;
  process.env.HOSTNAME = "Pull.Worker Example";
  let worker: PullWorker | undefined;
  try {
    worker = new PullWorker(new Registry([]), {} as WorkerDispatcher, "127.0.0.1:1", {});
    assert.match(worker.appWorkerID, /^pull-worker-example-[a-z2-7]{8}$/);
  } finally {
    await worker?.close();
    restoreEnvironment("DEX_WORKER_INSTANCE", previous.dex);
    restoreEnvironment("POD_NAME", previous.pod);
    restoreEnvironment("HOSTNAME", previous.host);
  }
});

test("poll streams register first, reject at capacity, answer Ping, and reuse after a result", async () => {
  const harness = await startHarness();
  let finishFirst!: () => void;
  const firstGate = new Promise<void>((resolve) => { finishFirst = resolve; });
  let invocationCount = 0;
  const worker = harness.worker(async () => {
    invocationCount++;
    if (invocationCount === 1) await firstGate;
    return InvokeWorkerRPCResponse.create({});
  }, { maxConcurrentTasks: 1 });
  try {
    worker.start();
    const first = await harness.nextStream();
    assert.equal(first.frames[0]?.frame?.$case, "register");
    first.call.write({ frame: { $case: "registered", value: { supportsUnknownNameRejection: false } } });
    first.call.write({ frame: { $case: "ping", value: {} } });
    await first.frame("pong");
    first.call.write(rpcTask("first"));
    const second = await harness.nextStream();
    second.call.write({ frame: { $case: "registered", value: { supportsUnknownNameRejection: false } } });
    second.call.write(rpcTask("second"));
    const rejected = await second.frame("reject");
    assert.equal(rejected.frame?.$case, "reject");
    if (rejected.frame?.$case === "reject") {
      assert.equal(rejected.frame.value.reason, PollWorkerTaskRejectReason.POLL_WORKER_TASK_REJECT_REASON_NO_CAPACITY);
    }
    finishFirst();
    await first.frame("taskFrame");
    second.call.write(rpcTask("third"));
    const result = await second.frame("taskFrame");
    assert.equal(result.frame?.$case, "taskFrame");
    if (result.frame?.$case === "taskFrame") assert.equal(result.frame.value.taskId, "third");
  } finally {
    finishFirst();
    await worker.close();
    await harness.close();
  }
});

test("unknown task rejection follows the Registered capability for each stream", async () => {
  const harness = await startHarness();
  const dispatcher = {
    async invokeWaitFor(): Promise<InvokeWaitForMethodResponse> {
      throw new Error("Step MissingStep is not registered");
    },
    async invokeExecute(): Promise<never> {
      throw new Error("Step MissingStep is not registered");
    },
    async invokeRPC(): Promise<InvokeWorkerRPCResponse> {
      throw new Error("RPC MissingRPC is not registered");
    },
  } as unknown as WorkerDispatcher;
  const worker = harness.workerWithDispatcher(dispatcher);
  try {
    worker.start();
    const legacy = await harness.nextStream();
    legacy.call.write(registered(false));
    legacy.call.write(unknownStepTask("legacy-step"));
    const legacyStep = await legacy.frame("taskFrame");
    legacy.call.write(unknownRPCTask("legacy-rpc"));
    const legacyRPC = await legacy.frame("taskFrame", 2);
    for (const frame of [legacyStep, legacyRPC]) {
      assert.equal(frame.frame?.$case, "taskFrame");
      if (frame.frame?.$case === "taskFrame") {
        assert.equal(frame.frame.value.output?.$case, "workerError");
        if (frame.frame.value.output?.$case === "workerError") {
          assert.match(frame.frame.value.output.value.detail, /not registered/);
        }
      }
    }
    assert.equal(legacy.frames.some((frame) => frame.frame?.$case === "reject"), false);
    legacy.call.write({ frame: { $case: "ping", value: {} } });
    await legacy.frame("pong");

    legacy.call.write({ frame: { $case: "reconnect", value: { reason: "test" } } });
    const capable = await harness.nextStream();
    capable.call.write(registered(true));
    capable.call.write(unknownStepTask("capable-step"));
    capable.call.write(unknownRPCTask("capable-rpc"));
    const stepRejection = await capable.frame("reject");
    const rpcRejection = await capable.frame("reject", 2);
    assert.equal(stepRejection.frame?.$case, "reject");
    assert.equal(rpcRejection.frame?.$case, "reject");
    if (stepRejection.frame?.$case === "reject") {
      assert.equal(stepRejection.frame.value.reason, PollWorkerTaskRejectReason.POLL_WORKER_TASK_REJECT_REASON_UNKNOWN_STEP);
    }
    if (rpcRejection.frame?.$case === "reject") {
      assert.equal(rpcRejection.frame.value.reason, PollWorkerTaskRejectReason.POLL_WORKER_TASK_REJECT_REASON_UNKNOWN_RPC);
    }

    capable.call.write({ frame: { $case: "reconnect", value: { reason: "test" } } });
    const reconnected = await harness.nextStream();
    reconnected.call.write(registered(false));
    reconnected.call.write(unknownStepTask("reconnected-step"));
    const reconnectedError = await reconnected.frame("taskFrame");
    assert.equal(reconnectedError.frame?.$case, "taskFrame");
    if (reconnectedError.frame?.$case === "taskFrame") {
      assert.equal(reconnectedError.frame.value.output?.$case, "workerError");
      if (reconnectedError.frame.value.output?.$case === "workerError") {
        assert.match(reconnectedError.frame.value.output.value.detail, /not registered/);
      }
    }
    assert.equal(reconnected.frames.some((frame) => frame.frame?.$case === "reject"), false);
  } finally {
    await worker.close();
    await harness.close();
  }
});

test("Reconnect backs off idle streams and drains a busy stream", async () => {
  const harness = await startHarness();
  let finishTask!: () => void;
  const gate = new Promise<void>((resolve) => { finishTask = resolve; });
  const worker = harness.worker(async () => {
    await gate;
    return InvokeWorkerRPCResponse.create({});
  });
  try {
    worker.start();
    const idle = await harness.nextStream();
    idle.call.write({ frame: { $case: "registered", value: { supportsUnknownNameRejection: false } } });
    const reconnectRequestedAt = Date.now();
    idle.call.write({ frame: { $case: "reconnect", value: { reason: "move" } } });
    const replacement = await harness.nextStream();
    assert.ok(replacement.openedAt - reconnectRequestedAt >= 95);
    replacement.call.write({ frame: { $case: "registered", value: { supportsUnknownNameRejection: false } } });
    replacement.call.write(rpcTask("busy"));
    const spare = await harness.nextStream();
    spare.call.write({ frame: { $case: "registered", value: { supportsUnknownNameRejection: false } } });
    replacement.call.write({ frame: { $case: "reconnect", value: { reason: "move" } } });
    replacement.call.write({ frame: { $case: "ping", value: {} } });
    await replacement.frame("pong");
    finishTask();
    await replacement.frame("taskFrame");
    await replacement.ended;
    await new Promise<void>((resolve) => setTimeout(resolve, 300));
    assert.equal(harness.openedStreamCount, 3);
    spare.call.write(rpcTask("after-reconnect"));
    const continued = await spare.frame("taskFrame");
    assert.equal(continued.frame?.$case, "taskFrame");
  } finally {
    finishTask();
    await worker.close();
    await harness.close();
  }
});

test("busy Reconnect retains only the replacement idle stream", async () => {
  const harness = await startHarness();
  let finishTask!: () => void;
  const gate = new Promise<void>((resolve) => { finishTask = resolve; });
  const worker = harness.worker(async () => {
    await gate;
    return InvokeWorkerRPCResponse.create({});
  });
  try {
    worker.start();
    const first = await harness.nextStream();
    first.call.write({ frame: { $case: "registered", value: { supportsUnknownNameRejection: false } } });
    first.call.write(rpcTask("busy-reconnect"));
    await until(() => harness.invocationCount === 1);
    const replacement = await harness.nextStream();
    replacement.call.write({ frame: { $case: "registered", value: { supportsUnknownNameRejection: false } } });
    first.call.write({ frame: { $case: "reconnect", value: { reason: "move" } } });
    first.call.write({ frame: { $case: "ping", value: {} } });
    await first.frame("pong");
    finishTask();
    await first.frame("taskFrame");
    await first.ended;
    await new Promise<void>((resolve) => setTimeout(resolve, 300));
    assert.equal(harness.openedStreamCount, 2);
  } finally {
    finishTask();
    await worker.close();
    await harness.close();
  }
});

test("CancelTask and task deadline abort the handler", async () => {
  const harness = await startHarness();
  const cancellations: string[] = [];
  const worker = harness.worker(async (_request, signal) => new Promise<InvokeWorkerRPCResponse>((_resolve, reject) => {
    const onAbort = () => {
      cancellations.push("aborted");
      reject(Object.assign(new Error("cancelled"), { name: "AbortError" }));
    };
    if (signal.aborted) onAbort();
    else signal.addEventListener("abort", onAbort, { once: true });
  }));
  try {
    worker.start();
    const stream = await harness.nextStream();
    stream.call.write({ frame: { $case: "registered", value: { supportsUnknownNameRejection: false } } });
    stream.call.write(rpcTask("cancel"));
    stream.call.write({ frame: { $case: "cancelTask", value: { taskId: "cancel" } } });
    const cancelled = await stream.frame("taskFrame");
    assert.equal(cancelled.frame?.$case, "taskFrame");
    if (cancelled.frame?.$case === "taskFrame") {
      assert.equal(cancelled.frame.value.output?.$case, "workerError");
      if (cancelled.frame.value.output?.$case === "workerError") {
        assert.equal(cancelled.frame.value.output.value.grpcStatus, status.CANCELLED);
      }
    }
    stream.call.write(rpcTask("deadline", new Date(Date.now() + 20)));
    const timedOut = await stream.frame("taskFrame", 2);
    assert.equal(timedOut.frame?.$case, "taskFrame");
    if (timedOut.frame?.$case === "taskFrame") {
      assert.equal(timedOut.frame.value.output?.$case, "workerError");
      if (timedOut.frame.value.output?.$case === "workerError") {
        assert.equal(timedOut.frame.value.output.value.grpcStatus, status.DEADLINE_EXCEEDED);
      }
    }
    stream.call.write(rpcTask("expired", new Date(Date.now() - 1_000)));
    await stream.frame("taskFrame", 3);
    assert.equal(cancellations.length, 3);
  } finally {
    await worker.close();
    await harness.close();
  }
});

test("pull WaitFor sends heartbeat and stream write before its final result", async () => {
  const harness = await startHarness();
  const dispatcher = {
    async invokeWaitFor(_request: InvokeWaitForMethodRequest, _signal: AbortSignal, output: {
      recordHeartbeat(value: undefined): Promise<void>;
      writeStream(write: { streamName: string; streamCapacityBytes: bigint; value: undefined }): void;
    }) {
      await output.recordHeartbeat(undefined);
      output.writeStream({ streamName: "progress", streamCapacityBytes: 1024n, value: undefined });
      return InvokeWaitForMethodResponse.create({});
    },
  } as WorkerDispatcher;
  const worker = harness.workerWithDispatcher(dispatcher);
  try {
    worker.start();
    const stream = await harness.nextStream();
    stream.call.write({ frame: { $case: "registered", value: { supportsUnknownNameRejection: false } } });
    stream.call.write({ frame: { $case: "task", value: {
      taskId: "wait-for", deadline: undefined,
      request: { $case: "waitFor", value: InvokeWaitForMethodRequest.create({ flowType: "TestFlow", stepType: "TestStep" }) },
    } } });
    await stream.frame("taskFrame", 3);
    const outputs = stream.frames.filter((candidate) => candidate.frame?.$case === "taskFrame")
      .map((candidate) => candidate.frame?.$case === "taskFrame" ? candidate.frame.value.output : undefined);
    assert.deepEqual(outputs.map((output) => output?.$case), ["waitFor", "waitFor", "waitFor"]);
    assert.deepEqual(outputs.map((output) => output?.$case === "waitFor" ? output.value.output?.$case : undefined),
      ["heartbeat", "streamWrite", "result"]);
  } finally {
    await worker.close();
    await harness.close();
  }
});

for (const shouldReconnect of [false, true]) {
  test(`close preserves a large final result after ${shouldReconnect ? "busy Reconnect" : "normal completion"}`, async () => {
    const harness = await startHarness();
    let finishTask!: () => void;
    const gate = new Promise<void>((resolve) => { finishTask = resolve; });
    const payload = "x".repeat(3_000_000);
    const worker = harness.worker(async () => {
      await gate;
      return InvokeWorkerRPCResponse.create({ output: Value.create({
        kind: { $case: "stringValue", value: payload },
      }) });
    }, { stopDrainTimeoutMs: 1_000 });
    try {
      worker.start();
      const stream = await harness.nextStream();
      stream.call.write({ frame: { $case: "registered", value: { supportsUnknownNameRejection: false } } });
      stream.call.write(rpcTask("large-result"));
      await until(() => harness.invocationCount === 1);
      if (shouldReconnect) {
        stream.call.write({ frame: { $case: "reconnect", value: { reason: "move" } } });
        stream.call.write({ frame: { $case: "ping", value: {} } });
        await stream.frame("pong");
      }
      finishTask();
      await new Promise<void>((resolve) => setImmediate(resolve));
      const closing = worker.close();
      const result = await stream.frame("taskFrame");
      assert.equal(result.frame?.$case, "taskFrame");
      if (result.frame?.$case === "taskFrame") {
        assert.equal(result.frame.value.output?.$case, "rpc");
        if (result.frame.value.output?.$case === "rpc") {
          assert.equal(result.frame.value.output.value.output?.kind?.$case, "stringValue");
          if (result.frame.value.output.value.output?.kind?.$case === "stringValue") {
            assert.equal(result.frame.value.output.value.output.kind.value.length, payload.length);
          }
        }
      }
      await closing;
    } finally {
      finishTask();
      await worker.close();
      await harness.close();
    }
  });
}

for (const shutdown of ["Stop drain", "busy Reconnect", "busy Reconnect then Stop"] as const) {
  test(`large final result survives a delayed gRPC read during ${shutdown}`, async () => {
    const harness = await startHarness();
    let stream: CapturedStream | undefined;
    let finishTask!: () => void;
    const taskGate = new Promise<void>((resolve) => { finishTask = resolve; });
    const payload = "x".repeat(3_000_000);
    const worker = harness.worker(async () => {
      await taskGate;
      return InvokeWorkerRPCResponse.create({ output: Value.create({
        kind: { $case: "stringValue", value: payload },
      }) });
    }, { stopDrainTimeoutMs: shutdown === "busy Reconnect then Stop" ? 4_000 : 10_000 });
    try {
      worker.start();
      const capturedStream = await harness.nextStream();
      stream = capturedStream;
      capturedStream.call.write({ frame: { $case: "registered", value: { supportsUnknownNameRejection: false } } });
      capturedStream.call.write(rpcTask("delayed-result"));
      await until(() => harness.invocationCount === 1);
      if (shutdown !== "Stop drain") {
        capturedStream.call.write({ frame: { $case: "reconnect", value: { reason: "move" } } });
        capturedStream.call.write({ frame: { $case: "ping", value: {} } });
        await capturedStream.frame("pong");
      }
      capturedStream.call.pause();
      finishTask();
      await new Promise<void>((resolve) => setImmediate(resolve));
      const closing = shutdown === "Stop drain" ? worker.close() : undefined;
      const delayedRead = new Promise<void>((resolve) => setTimeout(() => {
        capturedStream.call.resume();
        resolve();
      }, 6_100));
      const stopDuringReconnect = shutdown === "busy Reconnect then Stop"
        ? new Promise<void>((resolve) => setTimeout(resolve, 3_000)).then(() => worker.close())
        : undefined;
      await delayedRead;
      const result = await capturedStream.frame("taskFrame", 1, 10_000);
      assert.equal(result.frame?.$case, "taskFrame");
      if (result.frame?.$case === "taskFrame") {
        assert.equal(result.frame.value.output?.$case, "rpc");
        if (result.frame.value.output?.$case === "rpc") {
          assert.equal(result.frame.value.output.value.output?.kind?.$case, "stringValue");
          if (result.frame.value.output.value.output?.kind?.$case === "stringValue") {
            assert.equal(result.frame.value.output.value.output.kind.value.length, payload.length);
          }
        }
      }
      await closing;
      await stopDuringReconnect;
    } finally {
      stream?.call.resume();
      finishTask();
      await worker.close();
      await harness.close();
    }
  });
}

test("Worker error frame preserves push status, type, stack, and retry delay", () => {
  const failure = Object.assign(new Error("failed"), { code: status.INVALID_ARGUMENT });
  const pull = workerErrorResponse(failure);
  const push = workerServiceError(failure);
  assert.equal(pull.grpcStatus, status.UNKNOWN);
  assert.equal(push.code, pull.grpcStatus);
  assert.equal(pull.detail, "failed");
  assert.equal(pull.errorType, "Error");
  assert.match(pull.stackTrace, /failed/);
  const delayed = workerErrorResponse(retryAfter(2, failure));
  assert.equal(delayed.grpcStatus, status.UNKNOWN);
  assert.equal(delayed.retryAfterSeconds, 2);
});

test("close drains a task, then force-cancels and waits for a handler that ignores cancellation", async () => {
  const harness = await startHarness();
  let finishTask!: () => void;
  const gate = new Promise<void>((resolve) => { finishTask = resolve; });
  let hasAborted = false;
  const worker = harness.worker(async (_request, signal) => {
    signal.addEventListener("abort", () => { hasAborted = true; });
    await gate;
    return InvokeWorkerRPCResponse.create({});
  }, { stopDrainTimeoutMs: 10 });
  try {
    worker.start();
    const stream = await harness.nextStream();
    stream.call.write({ frame: { $case: "registered", value: { supportsUnknownNameRejection: false } } });
    stream.call.write(rpcTask("blocked"));
    await until(() => harness.invocationCount === 1);
    let hasClosed = false;
    const closing = worker.close().then(() => { hasClosed = true; });
    await until(() => hasAborted);
    assert.equal(hasClosed, false);
    finishTask();
    await closing;
    assert.equal(hasClosed, true);
  } finally {
    finishTask();
    await worker.close();
    await harness.close();
  }
});

interface CapturedStream {
  readonly call: TaskStream;
  readonly frames: PollWorkerTaskRequest[];
  readonly openedAt: number;
  readonly ended: Promise<void>;
  frame(kind: NonNullable<PollWorkerTaskRequest["frame"]>["$case"], occurrence?: number, timeoutMs?: number): Promise<PollWorkerTaskRequest>;
}

async function startHarness() {
  const server = new Server();
  const streams: CapturedStream[] = [];
  let consumed = 0;
  let invocationCount = 0;
  server.addService(WorkerTaskServiceService, {
    pollWorkerTask(call: TaskStream) {
      const frames: PollWorkerTaskRequest[] = [];
      let resolveEnd!: () => void;
      const ended = new Promise<void>((resolve) => { resolveEnd = resolve; });
      call.on("data", (frame: PollWorkerTaskRequest) => frames.push(frame));
      call.on("end", () => { resolveEnd(); call.end(); });
      call.on("error", () => {});
      streams.push({
        call, frames, openedAt: Date.now(), ended,
        async frame(kind, occurrence = 1, timeoutMs = 5_000) {
          await until(() => frames.filter((candidate) => candidate.frame?.$case === kind).length >= occurrence, timeoutMs);
          return frames.filter((candidate) => candidate.frame?.$case === kind)[occurrence - 1]!;
        },
      });
    },
  } as WorkerTaskServiceServer);
  const port = await new Promise<number>((resolve, reject) => {
    server.bindAsync("127.0.0.1:0", ServerCredentials.createInsecure(), (error, boundPort) => {
      if (error !== null) reject(error);
      else resolve(boundPort);
    });
  });
  return {
    get invocationCount() { return invocationCount; },
    get openedStreamCount() { return streams.length; },
    worker(invokeRPC: (request: InvokeWorkerRPCRequest, signal: AbortSignal) => Promise<InvokeWorkerRPCResponse>, options: { maxConcurrentTasks?: number; stopDrainTimeoutMs?: number } = {}) {
      const dispatcher = { invokeRPC: async (request: InvokeWorkerRPCRequest, signal: AbortSignal) => {
        invocationCount++;
        return invokeRPC(request, signal);
      } } as WorkerDispatcher;
      const worker = new PullWorker(new Registry([]), dispatcher, `127.0.0.1:${port}`, options);
      Reflect.set(worker, "registeredFlows", new Map([["TestFlow", {
        steps: [{ name: "TestStep" }], rpcs: [{ name: "getWorker" }],
      }]]));
      return worker;
    },
    workerWithDispatcher(dispatcher: WorkerDispatcher) {
      const worker = new PullWorker(new Registry([]), dispatcher, `127.0.0.1:${port}`, {});
      Reflect.set(worker, "registeredFlows", new Map([["TestFlow", {
        steps: [{ name: "TestStep" }], rpcs: [{ name: "getWorker" }],
      }]]));
      return worker;
    },
    async nextStream(): Promise<CapturedStream> {
      await until(() => streams.length > consumed);
      return streams[consumed++]!;
    },
    async close(): Promise<void> {
      await new Promise<void>((resolve, reject) => server.tryShutdown((error) => error === undefined ? resolve() : reject(error)));
    },
  };
}

function rpcTask(taskId: string, deadline?: Date): PollWorkerTaskResponse {
  return { frame: { $case: "task", value: {
    taskId,
    deadline,
    request: { $case: "rpc", value: InvokeWorkerRPCRequest.create({ flowType: "TestFlow", rpcName: "getWorker" }) },
  } } };
}

function registered(supportsUnknownNameRejection: boolean): PollWorkerTaskResponse {
  return { frame: { $case: "registered", value: { supportsUnknownNameRejection } } };
}

function unknownStepTask(taskId: string): PollWorkerTaskResponse {
  return { frame: { $case: "task", value: {
    taskId,
    deadline: undefined,
    request: { $case: "waitFor", value: InvokeWaitForMethodRequest.create({ flowType: "TestFlow", stepType: "MissingStep" }) },
  } } };
}

function unknownRPCTask(taskId: string): PollWorkerTaskResponse {
  return { frame: { $case: "task", value: {
    taskId,
    deadline: undefined,
    request: { $case: "rpc", value: InvokeWorkerRPCRequest.create({ flowType: "TestFlow", rpcName: "MissingRPC" }) },
  } } };
}

async function until(condition: () => boolean, timeoutMs = 5_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!condition()) {
    if (Date.now() >= deadline) throw new Error("timed out waiting for poll stream event");
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

function restoreEnvironment(name: string, value: string | undefined): void {
  if (value === undefined) delete process.env[name];
  else process.env[name] = value;
}
