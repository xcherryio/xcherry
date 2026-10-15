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

import { Client, Registry, StepList, Worker, retryAfter, type BlobCache, type Flow } from "../src/index.js";
import {
  InvokeWorkerRPCRequest,
  InvokeWorkerRPCResponse,
  InvokeWaitForMethodRequest,
  InvokeWaitForMethodResponse,
  PollWorkerTaskRejectReason,
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
  assert.throws(() => new Worker(registry, cache, { pull: {}, workerTarget: { address: "", pull: true } }), TypeError);
  assert.throws(() => new Worker(registry, cache, { workerTarget: { address: "", pull: true } }), TypeError);
  assert.throws(() => new Worker(registry, cache, { pull: { maxConcurrentTasks: -1 } }), RangeError);
  assert.throws(() => new Worker(registry, cache, { pull: { idleStreamsPerQueue: 1.2 } }), RangeError);
  assert.throws(() => new Worker(registry, cache, { pull: { stopDrainTimeoutMs: -1 } }), RangeError);
  assert.throws(() => new Worker(registry, cache, { pull: { appWorkerID: "bad/id" } }), TypeError);
  assert.throws(() => new Worker(registry, cache, { pull: { appWorkerID: "TestFlow" } }), TypeError);
  assert.throws(() => new Client(registry, cache, { workerTarget: { pull: true, address: "127.0.0.1:8000" } }), TypeError);
  assert.throws(() => new Client(registry, cache, { workerTarget: { address: "", headless: true } }), TypeError);
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
    first.call.write({ frame: { $case: "registered", value: {} } });
    first.call.write({ frame: { $case: "ping", value: {} } });
    await first.frame("pong");
    first.call.write(rpcTask("first"));
    const second = await harness.nextStream();
    second.call.write({ frame: { $case: "registered", value: {} } });
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

test("Reconnect replaces idle streams immediately and drains a busy stream", async () => {
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
    idle.call.write({ frame: { $case: "registered", value: {} } });
    idle.call.write({ frame: { $case: "reconnect", value: { reason: "move" } } });
    const replacement = await harness.nextStream();
    replacement.call.write({ frame: { $case: "registered", value: {} } });
    replacement.call.write(rpcTask("busy"));
    const spare = await harness.nextStream();
    spare.call.write({ frame: { $case: "registered", value: {} } });
    replacement.call.write({ frame: { $case: "reconnect", value: { reason: "move" } } });
    finishTask();
    await replacement.frame("taskFrame");
    spare.call.write(rpcTask("after-reconnect"));
    const continued = await spare.frame("taskFrame");
    assert.equal(continued.frame?.$case, "taskFrame");
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
    stream.call.write({ frame: { $case: "registered", value: {} } });
    stream.call.write(rpcTask("cancel"));
    stream.call.write({ frame: { $case: "cancelTask", value: { taskId: "cancel" } } });
    const cancelled = await stream.frame("taskFrame");
    assert.equal(cancelled.frame?.$case, "taskFrame");
    if (cancelled.frame?.$case === "taskFrame") assert.equal(cancelled.frame.value.output?.$case, "workerError");
    stream.call.write(rpcTask("deadline", new Date(Date.now() + 20)));
    await stream.frame("taskFrame", 2);
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
    stream.call.write({ frame: { $case: "registered", value: {} } });
    stream.call.write({ frame: { $case: "task", value: {
      taskId: "wait-for", deadline: undefined,
      request: { $case: "waitFor", value: InvokeWaitForMethodRequest.create({}) },
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

test("Worker error frame preserves push status, type, stack, and retry delay", () => {
  const failure = Object.assign(new Error("failed"), { code: status.INVALID_ARGUMENT });
  const pull = workerErrorResponse(failure);
  const push = workerServiceError(failure);
  assert.equal(pull.grpcStatus, status.INVALID_ARGUMENT);
  assert.equal(push.code, pull.grpcStatus);
  assert.equal(pull.detail, "failed");
  assert.equal(pull.errorType, "Error");
  assert.match(pull.stackTrace, /failed/);
  const delayed = workerErrorResponse(retryAfter(2, failure));
  assert.equal(delayed.grpcStatus, status.INVALID_ARGUMENT);
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
    stream.call.write({ frame: { $case: "registered", value: {} } });
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
  frame(kind: NonNullable<PollWorkerTaskRequest["frame"]>["$case"], occurrence?: number): Promise<PollWorkerTaskRequest>;
}

async function startHarness() {
  const server = new Server();
  const streams: CapturedStream[] = [];
  let consumed = 0;
  let invocationCount = 0;
  server.addService(WorkerTaskServiceService, {
    pollWorkerTask(call: TaskStream) {
      const frames: PollWorkerTaskRequest[] = [];
      call.on("data", (frame: PollWorkerTaskRequest) => frames.push(frame));
      call.on("error", () => {});
      streams.push({
        call, frames,
        async frame(kind, occurrence = 1) {
          await until(() => frames.filter((candidate) => candidate.frame?.$case === kind).length >= occurrence);
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
    worker(invokeRPC: (request: InvokeWorkerRPCRequest, signal: AbortSignal) => Promise<InvokeWorkerRPCResponse>, options: { maxConcurrentTasks?: number; stopDrainTimeoutMs?: number } = {}) {
      const dispatcher = { invokeRPC: async (request: InvokeWorkerRPCRequest, signal: AbortSignal) => {
        invocationCount++;
        return invokeRPC(request, signal);
      } } as WorkerDispatcher;
      return new PullWorker(new Registry([]), dispatcher, `127.0.0.1:${port}`, options);
    },
    workerWithDispatcher(dispatcher: WorkerDispatcher) {
      return new PullWorker(new Registry([]), dispatcher, `127.0.0.1:${port}`, {});
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
    request: { $case: "rpc", value: InvokeWorkerRPCRequest.create({}) },
  } } };
}

async function until(condition: () => boolean): Promise<void> {
  const deadline = Date.now() + 5_000;
  while (!condition()) {
    if (Date.now() >= deadline) throw new Error("timed out waiting for poll stream event");
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}
