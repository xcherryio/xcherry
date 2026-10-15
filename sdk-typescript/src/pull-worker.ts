// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import { randomBytes } from "node:crypto";
import { availableParallelism, hostname } from "node:os";

import {
  credentials,
  status,
  type ClientDuplexStream,
  type ServiceError,
} from "@grpc/grpc-js";

import { registeredFlow, type Registry } from "./flow.js";
import {
  InvokeExecuteMethodOutput,
  InvokeWaitForMethodOutput,
  PollWorkerTaskRejectReason,
  WorkerTaskServiceClient,
  type PollWorkerTask,
  type PollWorkerTaskFrame,
  type PollWorkerTaskRequest,
  type PollWorkerTaskResponse,
  type StepStreamWrite,
  type Value,
} from "./gen/dex.js";
import { workerErrorResponse } from "./grpc-status.js";
import type { StepOutputEmitter } from "./invocation-context.js";
import type { PullWorkerOptions } from "./options.js";
import { sdkVersion } from "./sdk-version.generated.js";
import { WorkerDispatcher } from "./worker-dispatcher.js";

interface ActiveTask {
  readonly id: string;
  readonly controller: AbortController;
  readonly completion: Promise<void>;
  readonly deadlineCancellation: DeadlineCancellation | undefined;
}

interface PollStream {
  readonly queue: string;
  stream: ClientDuplexStream<PollWorkerTaskRequest, PollWorkerTaskResponse> | undefined;
  task: ActiveTask | undefined;
  retryTimer: ReturnType<typeof setTimeout> | undefined;
  halfCloseTimer: ReturnType<typeof setTimeout> | undefined;
  halfClosePromise: Promise<void> | undefined;
  resolveHalfClose: (() => void) | undefined;
  isRegistered: boolean;
  isDisconnected: boolean;
  isHalfClosed: boolean;
  isDraining: boolean;
  isReconnectRequested: boolean;
  wasBusyReconnect: boolean;
  backoffMs: number;
  failure: ServiceError | undefined;
}

export class PullWorker {
  public readonly appWorkerID: string;
  private readonly client: InstanceType<typeof WorkerTaskServiceClient>;
  private readonly queues: readonly string[];
  private readonly flowTypes: readonly string[];
  private readonly maxConcurrentTasks: number;
  private readonly idleStreamsPerQueue: number;
  private readonly stopDrainTimeoutMs: number;
  private readonly streams = new Set<PollStream>();
  private readonly activeTasks = new Set<Promise<void>>();
  private isStopping = false;
  private isForceStopping = false;
  private hasLoggedUnsupportedServer = false;
  private busyTaskCount = 0;

  public constructor(
    registry: Registry,
    private readonly dispatcher: WorkerDispatcher,
    serverAddress: string,
    options: PullWorkerOptions,
  ) {
    this.maxConcurrentTasks = positiveOption(options.maxConcurrentTasks, availableParallelism(), "maxConcurrentTasks");
    this.idleStreamsPerQueue = positiveOption(options.idleStreamsPerQueue, 1, "idleStreamsPerQueue");
    this.stopDrainTimeoutMs = options.stopDrainTimeoutMs === undefined || options.stopDrainTimeoutMs === 0
      ? 30_000 : options.stopDrainTimeoutMs;
    if (!Number.isFinite(this.stopDrainTimeoutMs) || this.stopDrainTimeoutMs < 0) {
      throw new RangeError("stopDrainTimeoutMs must not be negative");
    }
    this.flowTypes = registry.flows.map((flow) => registeredFlow(registry, flow).name).sort();
    this.appWorkerID = options.appWorkerID || generateAppWorkerID();
    if (!/^[A-Za-z0-9._-]{1,63}$/.test(this.appWorkerID)) {
      throw new TypeError("appWorkerID must contain 1-63 ASCII letters, digits, dots, underscores, or hyphens");
    }
    if (this.flowTypes.includes(this.appWorkerID)) {
      throw new TypeError(`appWorkerID conflicts with Flow type ${this.appWorkerID}`);
    }
    this.queues = [this.appWorkerID, ...this.flowTypes];
    this.client = createWorkerTaskClient(serverAddress);
  }

  public start(): void {
    this.reconcile();
  }

  public async close(): Promise<void> {
    this.isStopping = true;
    for (const poll of this.streams) {
      poll.isDraining = true;
      if (poll.task === undefined) {
        this.closeStream(poll);
      }
    }
    if (this.activeTasks.size !== 0) {
      const drained = Promise.allSettled([...this.activeTasks]);
      let drainTimer: ReturnType<typeof setTimeout> | undefined;
      const didDrain = await Promise.race([
        drained.then(() => true),
        new Promise<boolean>((resolve) => {
          drainTimer = setTimeout(() => resolve(false), this.stopDrainTimeoutMs);
        }),
      ]);
      if (drainTimer !== undefined) clearTimeout(drainTimer);
      if (!didDrain) {
        this.isForceStopping = true;
        for (const poll of this.streams) {
          poll.task?.controller.abort();
          this.closeStream(poll);
        }
        await drained;
      }
    }
    await Promise.all([...this.streams].flatMap((poll) => poll.halfClosePromise === undefined ? [] : [poll.halfClosePromise]));
    for (const poll of this.streams) {
      this.closeStream(poll);
      if (poll.retryTimer !== undefined) {
        clearTimeout(poll.retryTimer);
      }
    }
    this.streams.clear();
    this.client.close();
  }

  private reconcile(): void {
    if (this.isStopping) return;
    for (const queue of this.queues) {
      let idleCount = 0;
      for (const poll of this.streams) {
        if (poll.queue === queue && poll.task === undefined && !poll.isDraining) idleCount++;
      }
      while (idleCount < this.idleStreamsPerQueue) {
        const poll: PollStream = {
          queue, stream: undefined, task: undefined, retryTimer: undefined,
          halfCloseTimer: undefined, halfClosePromise: undefined, resolveHalfClose: undefined,
          isRegistered: false, isDisconnected: false, isHalfClosed: false, isDraining: false,
          isReconnectRequested: false, wasBusyReconnect: false,
          backoffMs: 100, failure: undefined,
        };
        this.streams.add(poll);
        this.openStream(poll);
        idleCount++;
      }
    }
  }

  private openStream(poll: PollStream): void {
    if (this.isStopping) return;
    poll.isRegistered = false;
    poll.isDisconnected = false;
    poll.isHalfClosed = false;
    poll.failure = undefined;
    poll.isDraining = false;
    poll.isReconnectRequested = false;
    poll.wasBusyReconnect = false;
    const stream = this.client.pollWorkerTask();
    poll.stream = stream;
    stream.on("data", (response: PollWorkerTaskResponse) => {
      if (poll.stream === stream) this.handleFrame(poll, response);
    });
    stream.on("error", (failure: ServiceError) => {
      if (poll.stream !== stream) return;
      poll.failure = failure;
      this.onStreamClosed(poll);
    });
    stream.on("end", () => {
      if (poll.stream === stream) this.onStreamClosed(poll);
    });
    try {
      stream.write({ frame: { $case: "register", value: {
        queue: poll.queue,
        appWorkerId: this.appWorkerID,
        flowTypes: [...this.flowTypes],
        sdkName: "typescript",
        sdkVersion,
      } } });
    } catch (failure) {
      console.error("Worker task registration failed", failure);
      this.disconnect(poll);
    }
  }

  private handleFrame(poll: PollStream, response: PollWorkerTaskResponse): void {
    if (poll.isDisconnected || this.isStopping && poll.task === undefined) return;
    const frame = response.frame;
    if (!poll.isRegistered) {
      if (frame?.$case === "reconnect") {
        poll.isReconnectRequested = true;
        this.halfCloseStream(poll);
      } else if (frame?.$case === "registered") {
        poll.isRegistered = true;
        poll.backoffMs = 100;
      } else {
        console.error("Worker task stream did not register");
        this.closeStream(poll);
        this.scheduleReconnect(poll);
      }
      return;
    }
    switch (frame?.$case) {
      case "task":
        this.startTask(poll, frame.value);
        break;
      case "cancelTask":
        if (poll.task?.id === frame.value.taskId) poll.task.controller.abort();
        break;
      case "ping":
        try {
          this.send(poll, { frame: { $case: "pong", value: {} } });
        } catch (failure) {
          console.error("send Worker task Pong", failure);
          this.disconnect(poll);
        }
        break;
      case "reconnect":
        poll.isDraining = true;
        poll.isReconnectRequested = true;
        if (poll.task === undefined) {
          this.halfCloseStream(poll);
        } else {
          poll.wasBusyReconnect = true;
          this.reconcile();
        }
        break;
      default:
        break;
    }
  }

  private startTask(poll: PollStream, task: PollWorkerTask): void {
    if (poll.task !== undefined || poll.isDraining || this.isStopping || this.busyTaskCount >= this.maxConcurrentTasks) {
      try {
        this.send(poll, { frame: { $case: "reject", value: {
          taskId: task.taskId,
          reason: PollWorkerTaskRejectReason.POLL_WORKER_TASK_REJECT_REASON_NO_CAPACITY,
        } } });
      } catch (failure) {
        console.error("reject Worker task", failure);
        this.disconnect(poll);
      }
      return;
    }
    const controller = new AbortController();
    const deadlineMs = task.deadline?.getTime();
    const deadlineCancellation = deadlineMs === undefined
      ? undefined
      : new DeadlineCancellation(deadlineMs, controller);
    const completion = this.executeTask(poll, task, controller.signal).finally(() => this.finishTask(poll));
    poll.task = { id: task.taskId, controller, completion, deadlineCancellation };
    this.activeTasks.add(completion);
    this.busyTaskCount++;
    this.reconcile();
  }

  private async executeTask(poll: PollStream, task: PollWorkerTask, signal: AbortSignal): Promise<void> {
    try {
      switch (task.request?.$case) {
        case "waitFor": {
          const emitter = new PullStepOutputEmitter(poll, task.taskId, "waitFor", (frame) => this.sendTaskFrame(poll, frame));
          const result = await this.dispatcher.invokeWaitFor(task.request.value, signal, emitter);
          this.sendTaskFrame(poll, { taskId: task.taskId, output: {
            $case: "waitFor", value: InvokeWaitForMethodOutput.create({ output: { $case: "result", value: result } }),
          } });
          break;
        }
        case "execute": {
          const emitter = new PullStepOutputEmitter(poll, task.taskId, "execute", (frame) => this.sendTaskFrame(poll, frame));
          const result = await this.dispatcher.invokeExecute(task.request.value, signal, emitter);
          this.sendTaskFrame(poll, { taskId: task.taskId, output: {
            $case: "execute", value: InvokeExecuteMethodOutput.create({ output: { $case: "result", value: result } }),
          } });
          break;
        }
        case "rpc": {
          const result = await this.dispatcher.invokeRPC(task.request.value, signal);
          this.sendTaskFrame(poll, { taskId: task.taskId, output: { $case: "rpc", value: result } });
          break;
        }
        default:
          throw new TypeError("unsupported Worker task request");
      }
    } catch (failure) {
      if (!poll.isDisconnected) {
        try {
          const grpcStatus = poll.task?.deadlineCancellation?.hasExpired === true
            ? status.DEADLINE_EXCEEDED : signal.aborted ? status.CANCELLED : status.UNKNOWN;
          this.sendTaskFrame(poll, { taskId: task.taskId, output: {
            $case: "workerError", value: workerErrorResponse(failure, grpcStatus),
          } });
        } catch (sendFailure) {
          console.error("send Worker error", sendFailure);
          this.disconnect(poll);
        }
      }
    }
  }

  private finishTask(poll: PollStream): void {
    const task = poll.task;
    if (task === undefined) return;
    task.deadlineCancellation?.stop();
    poll.task = undefined;
    this.activeTasks.delete(task.completion);
    this.busyTaskCount--;
    if (poll.isDisconnected) {
      if (!this.isStopping) this.scheduleReconnect(poll);
    } else if (this.isForceStopping) {
      this.closeStream(poll);
    } else if (this.isStopping || poll.isDraining) {
      this.halfCloseStream(poll);
    }
    this.reconcile();
  }

  private sendTaskFrame(poll: PollStream, frame: PollWorkerTaskFrame): void {
    this.send(poll, { frame: { $case: "taskFrame", value: frame } });
  }

  private send(poll: PollStream, request: PollWorkerTaskRequest): void {
    if (poll.isDisconnected || poll.stream === undefined) {
      throw new Error("Worker task stream is closed");
    }
    poll.stream.write(request);
  }

  private disconnect(poll: PollStream): void {
    if (poll.isDisconnected) return;
    poll.isDisconnected = true;
    poll.stream?.cancel();
    poll.task?.controller.abort();
    if (this.isStopping) return;
    if (poll.task === undefined) this.scheduleReconnect(poll);
  }

  private onStreamClosed(poll: PollStream): void {
    if (poll.isHalfClosed) {
      this.completeHalfClose(poll);
      return;
    }
    if (poll.isDisconnected) return;
    poll.isDisconnected = true;
    poll.task?.controller.abort();
    if (!this.isStopping && poll.task === undefined) this.scheduleReconnect(poll);
  }

  private halfCloseStream(poll: PollStream): void {
    if (poll.isHalfClosed || poll.stream === undefined) return;
    poll.isDisconnected = true;
    poll.isHalfClosed = true;
    poll.halfClosePromise = new Promise((resolve) => { poll.resolveHalfClose = resolve; });
    const stream = poll.stream;
    poll.halfCloseTimer = setTimeout(() => {
      stream.cancel();
      this.completeHalfClose(poll);
    }, 5_000);
    poll.halfCloseTimer.unref();
    stream.end();
  }

  private completeHalfClose(poll: PollStream): void {
    if (!poll.isHalfClosed) return;
    poll.isHalfClosed = false;
    if (poll.halfCloseTimer !== undefined) clearTimeout(poll.halfCloseTimer);
    poll.halfCloseTimer = undefined;
    poll.resolveHalfClose?.();
    poll.resolveHalfClose = undefined;
    poll.halfClosePromise = undefined;
    if (!this.isStopping) this.scheduleReconnect(poll);
  }

  private scheduleReconnect(poll: PollStream): void {
    if (this.isStopping || poll.retryTimer !== undefined) return;
    const failure = poll.failure;
    let delayMs: number;
    if (poll.isReconnectRequested && !poll.wasBusyReconnect) {
      delayMs = 0;
    } else if (failure?.code === status.UNIMPLEMENTED) {
      if (!this.hasLoggedUnsupportedServer) {
        this.hasLoggedUnsupportedServer = true;
        console.error("pull Worker polling is unsupported by this Dex server", failure);
      }
      delayMs = 60_000 + Math.floor(Math.random() * 30_000);
    } else if (failure !== undefined && [
      status.UNAUTHENTICATED, status.PERMISSION_DENIED, status.INVALID_ARGUMENT,
      status.RESOURCE_EXHAUSTED,
    ].includes(failure.code)) {
      console.error("Worker task stream rejected", poll.queue, failure);
      delayMs = 60_000 + Math.floor(Math.random() * 30_000);
    } else {
      if (failure !== undefined && failure.code !== status.CANCELLED) {
        console.warn("Worker task stream disconnected", poll.queue, failure);
      }
      delayMs = poll.backoffMs + Math.floor(Math.random() * poll.backoffMs / 2);
      poll.backoffMs = Math.min(poll.backoffMs * 2, 5_000);
    }
    poll.retryTimer = setTimeout(() => {
      poll.retryTimer = undefined;
      this.openStream(poll);
    }, delayMs);
  }

  private closeStream(poll: PollStream): void {
    poll.isDisconnected = true;
    poll.stream?.cancel();
    if (poll.isHalfClosed) this.completeHalfClose(poll);
    if (poll.retryTimer !== undefined) {
      clearTimeout(poll.retryTimer);
      poll.retryTimer = undefined;
    }
  }
}

class PullStepOutputEmitter implements StepOutputEmitter {
  public constructor(
    private readonly poll: PollStream,
    private readonly taskID: string,
    private readonly method: "waitFor" | "execute",
    private readonly sendFrame: (frame: PollWorkerTaskFrame) => void,
  ) {}

  public recordHeartbeat(value: Value | undefined): Promise<void> {
    this.sendOutput({ $case: "heartbeat", value: { value } });
    return Promise.resolve();
  }

  public writeStream(write: StepStreamWrite): void {
    this.sendOutput({ $case: "streamWrite", value: write });
  }

  private sendOutput(output: InvokeWaitForMethodOutput["output"]): void {
    if (this.poll.isDisconnected) throw new Error("Worker task stream is closed");
    const value = this.method === "waitFor"
      ? { $case: "waitFor" as const, value: InvokeWaitForMethodOutput.create({ output }) }
      : { $case: "execute" as const, value: InvokeExecuteMethodOutput.create({ output }) };
    this.sendFrame({ taskId: this.taskID, output: value });
  }
}

class DeadlineCancellation {
  public hasExpired = false;
  private timer: ReturnType<typeof setTimeout> | undefined;

  public constructor(
    private readonly deadlineMs: number,
    private readonly controller: AbortController,
  ) {
    this.schedule();
  }

  public stop(): void {
    if (this.timer !== undefined) clearTimeout(this.timer);
  }

  private schedule(): void {
    const remainingMs = this.deadlineMs - Date.now();
    if (remainingMs <= 0) {
      this.hasExpired = true;
      this.controller.abort();
    } else {
      this.timer = setTimeout(() => this.schedule(), Math.min(remainingMs, 2_147_483_647));
    }
  }
}

function positiveOption(value: number | undefined, defaultValue: number, name: string): number {
  if (value === undefined || value === 0) return defaultValue;
  if (!Number.isSafeInteger(value) || value < 0) throw new RangeError(`${name} must be a non-negative integer`);
  return value;
}

function generateAppWorkerID(): string {
  const identity = [process.env.DEX_WORKER_INSTANCE, process.env.POD_NAME, process.env.HOSTNAME, hostname()]
    .find((candidate) => candidate !== undefined && candidate.trim() !== "") ?? "worker";
  const sanitized = identity.toLowerCase().replace(/[^a-z0-9-]+/g, "-").slice(0, 54).replace(/^-+|-+$/g, "") || "worker";
  const alphabet = "abcdefghijklmnopqrstuvwxyz234567";
  const bytes = randomBytes(5);
  let bits = 0n;
  for (const byte of bytes) bits = bits << 8n | BigInt(byte);
  let suffix = "";
  for (let shift = 35n; shift >= 0n; shift -= 5n) suffix += alphabet[Number(bits >> shift & 31n)];
  return `${sanitized}-${suffix}`;
}

function createWorkerTaskClient(serverAddress: string): InstanceType<typeof WorkerTaskServiceClient> {
  return new WorkerTaskServiceClient(serverAddress, credentials.createInsecure());
}
