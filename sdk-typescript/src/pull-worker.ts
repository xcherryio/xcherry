// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import { debuglog } from "node:util";
import { randomBytes } from "node:crypto";
import { availableParallelism, hostname } from "node:os";

import {
  credentials,
  status,
  type ClientDuplexStream,
  type ServiceError,
} from "@grpc/grpc-js";

import { ApiKeyCredential, credentialInterceptors, type Credential } from "./credential.js";
import { registeredFlow, type RegisteredFlow, type Registry } from "./flow.js";
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
import { PullRegistration } from "./pull-registration.js";
import { sdkVersion } from "./sdk-version.generated.js";
import { WorkerDispatcher } from "./worker-dispatcher.js";

const debug = debuglog("dex:pull");

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
  hasRecordedResult: boolean;
  supportsUnknownNameRejection: boolean;
  isDisconnected: boolean;
  isHalfClosed: boolean;
  isDraining: boolean;
  isRetiring: boolean;
  isReconnectRequested: boolean;
  hasRetriedRejectedToken: boolean;
  backoffMs: number;
  failure: ServiceError | undefined;
}

export class PullWorker {
  public readonly appWorkerID: string;
  public readonly registration: PullRegistration;
  private readonly client: InstanceType<typeof WorkerTaskServiceClient>;
  private readonly hasRefreshableCredential: boolean;
  private readonly queues: readonly string[];
  private readonly flowTypes: readonly string[];
  private readonly registeredFlows: ReadonlyMap<string, RegisteredFlow>;
  private readonly maxConcurrentTasks: number;
  private readonly reservedRpcTasks: number;
  private reservedRpcDeclineCount = 0;
  private hasWarnedRpcCapacity = false;
  private readonly idleStreamsPerQueue: number;
  private readonly stopDrainTimeoutMs: number;
  private readonly streams = new Set<PollStream>();
  private readonly activeTasks = new Set<Promise<void>>();
  private isStopping = false;
  private stopDeadlineMs: number | undefined;
  private hasLoggedUnsupportedServer = false;
  private busyTaskCount = 0;

  public constructor(
    registry: Registry,
    private readonly dispatcher: WorkerDispatcher,
    serverAddress: string,
    private readonly options: PullWorkerOptions,
    credential?: Credential,
    private readonly onPermanentRejection: () => void = () => {},
  ) {
    this.maxConcurrentTasks = positiveOption(options.maxConcurrentTasks, availableParallelism(), "maxConcurrentTasks");
    this.reservedRpcTasks = options.reservedRpcTasks ?? (this.maxConcurrentTasks >= 2 ? 1 : 0);
    if (!Number.isInteger(this.reservedRpcTasks) || this.reservedRpcTasks < 0 || this.reservedRpcTasks >= this.maxConcurrentTasks) {
      throw new RangeError("reservedRpcTasks must be a nonnegative integer less than maxConcurrentTasks");
    }
    this.idleStreamsPerQueue = positiveOption(options.idleStreamsPerQueue, 1, "idleStreamsPerQueue");
    this.stopDrainTimeoutMs = options.stopDrainTimeoutMs === undefined || options.stopDrainTimeoutMs === 0
      ? 30_000 : options.stopDrainTimeoutMs;
    if (!Number.isFinite(this.stopDrainTimeoutMs) || this.stopDrainTimeoutMs < 0) {
      throw new RangeError("stopDrainTimeoutMs must not be negative");
    }
    this.registeredFlows = new Map(registry.flows.map((flow) => {
      const definition = registeredFlow(registry, flow);
      return [definition.name, definition] as const;
    }));
    this.flowTypes = [...this.registeredFlows.keys()].sort();
    this.appWorkerID = options.appWorkerID || generateAppWorkerID();
    if (!/^[A-Za-z0-9._-]{1,63}$/.test(this.appWorkerID)) {
      throw new TypeError("appWorkerID must contain 1-63 ASCII letters, digits, dots, underscores, or hyphens");
    }
    if (this.flowTypes.includes(this.appWorkerID)) {
      throw new TypeError(`appWorkerID conflicts with Flow type ${this.appWorkerID}`);
    }
    this.queues = [this.appWorkerID, ...this.flowTypes];
    this.registration = new PullRegistration(this.queues);
    this.client = createWorkerTaskClient(serverAddress, credential);
    this.hasRefreshableCredential = credential instanceof ApiKeyCredential;
  }

  public get reservedRpcDeclines(): number {
    return this.reservedRpcDeclineCount;
  }

  public start(): void {
    if (this.maxConcurrentTasks === 1 && this.options.reservedRpcTasks === undefined && !this.hasWarnedRpcCapacity) {
      this.hasWarnedRpcCapacity = true;
      console.warn("RPCs may wait behind long-running Steps; use maxConcurrentTasks >= 2 or set reservedRpcTasks explicitly");
    }
    this.reconcile();
  }

  public async close(): Promise<void> {
    const stopDeadlineMs = this.stopDeadlineMs ??= Date.now() + this.stopDrainTimeoutMs;
    this.isStopping = true;
    this.registration.stopped();
    for (const poll of this.streams) {
      poll.isDraining = true;
      if (poll.isHalfClosed) this.armHalfCloseTimer(poll, stopDeadlineMs);
      if (poll.retryTimer !== undefined) {
        clearTimeout(poll.retryTimer);
        poll.retryTimer = undefined;
      }
      if (poll.task === undefined && !poll.isDisconnected) {
        this.halfCloseStream(poll);
      }
    }
    if (this.activeTasks.size !== 0) {
      const drained = Promise.allSettled([...this.activeTasks]);
      let drainTimer: ReturnType<typeof setTimeout> | undefined;
      const didDrain = await Promise.race([
        drained.then(() => true),
        new Promise<boolean>((resolve) => {
          drainTimer = setTimeout(() => resolve(false), Math.max(stopDeadlineMs - Date.now(), 0));
          drainTimer.unref();
        }),
      ]);
      if (drainTimer !== undefined) clearTimeout(drainTimer);
      if (!didDrain) {
        for (const poll of this.streams) {
          if (poll.task !== undefined) {
            poll.task.controller.abort();
          }
        }
        await drained;
      }
    }
    await Promise.all([...this.streams].flatMap((poll) => poll.halfClosePromise === undefined ? [] : [poll.halfClosePromise]));
    for (const poll of this.streams) {
      if (!poll.isDisconnected) this.closeStream(poll);
    }
    this.streams.clear();
    this.client.close();
  }

  private reconcile(): void {
    if (this.isStopping) return;
    for (const poll of this.streams) {
      if (poll.isRegistered && !poll.isDisconnected && poll.task === undefined
        && !poll.isDraining && this.hasRegisteredIdleReplacements(poll)) {
        poll.isDraining = true;
        poll.isRetiring = true;
        this.halfCloseStream(poll);
      }
    }
    for (const queue of this.queues) {
      let idleCount = 0;
      for (const poll of this.streams) {
        if (poll.queue === queue && poll.task === undefined && !poll.isDraining) idleCount++;
      }
      while (idleCount < this.idleStreamsPerQueue) {
        const poll: PollStream = {
          queue, stream: undefined, task: undefined, retryTimer: undefined,
          halfCloseTimer: undefined, halfClosePromise: undefined, resolveHalfClose: undefined,
          isRegistered: false, hasRecordedResult: false, supportsUnknownNameRejection: false, isDisconnected: false, isHalfClosed: false, isDraining: false, isRetiring: false,
          isReconnectRequested: false, hasRetriedRejectedToken: false,
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
    poll.hasRecordedResult = false;
    poll.supportsUnknownNameRejection = false;
    poll.isDisconnected = false;
    poll.isHalfClosed = false;
    poll.failure = undefined;
    poll.isDraining = false;
    poll.isReconnectRequested = false;
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
        this.registration.registered(poll.queue);
        poll.supportsUnknownNameRejection = frame.value.supportsUnknownNameRejection;
        poll.backoffMs = 100;
        poll.hasRetriedRejectedToken = false;
        this.reconcile();
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
          this.reconcile();
        }
        break;
      default:
        break;
    }
  }

  private startTask(poll: PollStream, task: PollWorkerTask): void {
    const unknownReason = this.unknownTaskReason(task);
    if (poll.supportsUnknownNameRejection && unknownReason !== undefined) {
      this.rejectTask(poll, task.taskId, unknownReason);
      return;
    }
    if (poll.task !== undefined || poll.isDraining || this.isStopping || this.busyTaskCount >= this.maxConcurrentTasks) {
      this.rejectTask(poll, task.taskId, PollWorkerTaskRejectReason.POLL_WORKER_TASK_REJECT_REASON_NO_CAPACITY);
      return;
    }
    const isStepTask = task.request?.$case === "waitFor" || task.request?.$case === "execute";
    if (isStepTask && this.reservedRpcTasks > 0 && this.busyTaskCount >= this.maxConcurrentTasks - this.reservedRpcTasks && unknownReason === undefined) {
      this.reservedRpcDeclineCount++;
      debug("decline Step task for reserved RPC capacity: %s", task.request?.value.flowType);
      this.rejectTask(poll, task.taskId, PollWorkerTaskRejectReason.POLL_WORKER_TASK_REJECT_REASON_NO_CAPACITY);
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

  private unknownTaskReason(task: PollWorkerTask): PollWorkerTaskRejectReason | undefined {
    switch (task.request?.$case) {
      case "waitFor":
      case "execute": {
        const request = task.request.value;
        const flow = this.registeredFlows.get(request.flowType);
        if (task.request.$case === "execute" && request.stepType === "sys:timeout_handler") {
          return flow?.hasTimeoutHandler === true
            ? undefined : PollWorkerTaskRejectReason.POLL_WORKER_TASK_REJECT_REASON_UNKNOWN_STEP;
        }
        return flow?.steps.some((step) => step.name === request.stepType) === true
          ? undefined : PollWorkerTaskRejectReason.POLL_WORKER_TASK_REJECT_REASON_UNKNOWN_STEP;
      }
      case "rpc": {
        const request = task.request.value;
        const flow = this.registeredFlows.get(request.flowType);
        return flow?.rpcs.some((rpc) => rpc.name === request.rpcName) === true
          ? undefined : PollWorkerTaskRejectReason.POLL_WORKER_TASK_REJECT_REASON_UNKNOWN_RPC;
      }
      default:
        return undefined;
    }
  }

  private rejectTask(poll: PollStream, taskId: string, reason: PollWorkerTaskRejectReason): void {
    try {
      this.send(poll, { frame: { $case: "reject", value: { taskId, reason } } });
    } catch (failure) {
      console.error("reject Worker task", failure);
      this.disconnect(poll);
    }
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
    if (!poll.isDisconnected && !poll.isDraining && this.hasRegisteredIdleReplacements(poll)) {
      poll.isDraining = true;
      poll.isRetiring = true;
    }
    if (poll.isDisconnected) {
      if (!this.isStopping) this.scheduleReconnect(poll);
    } else if (this.isStopping || poll.isDraining) {
      this.halfCloseStream(poll);
    }
    this.reconcile();
  }

  private hasRegisteredIdleReplacements(poll: PollStream): boolean {
    return [...this.streams].filter((candidate) =>
      candidate !== poll && candidate.queue === poll.queue && candidate.isRegistered
      && candidate.task === undefined && !candidate.isDraining && !candidate.isDisconnected
    ).length >= this.idleStreamsPerQueue;
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
    this.recordStreamResult(poll);
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
    this.recordStreamResult(poll);
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
    this.armHalfCloseTimer(poll, this.stopDeadlineMs ?? Date.now() + this.stopDrainTimeoutMs);
    stream.end();
  }

  private armHalfCloseTimer(poll: PollStream, deadlineMs: number): void {
    if (poll.halfCloseTimer !== undefined) clearTimeout(poll.halfCloseTimer);
    const remainingMs = deadlineMs - Date.now();
    poll.halfCloseTimer = setTimeout(() => {
      if (!poll.isHalfClosed) return;
      if (Date.now() < deadlineMs) {
        this.armHalfCloseTimer(poll, deadlineMs);
        return;
      }
      poll.stream?.cancel();
      this.completeHalfClose(poll);
    }, Math.min(Math.max(remainingMs, 0), 2_147_483_647));
    poll.halfCloseTimer.unref();
  }

  private completeHalfClose(poll: PollStream): void {
    if (!poll.isHalfClosed) return;
    this.recordStreamResult(poll);
    poll.isHalfClosed = false;
    if (poll.halfCloseTimer !== undefined) clearTimeout(poll.halfCloseTimer);
    poll.halfCloseTimer = undefined;
    poll.resolveHalfClose?.();
    poll.resolveHalfClose = undefined;
    poll.halfClosePromise = undefined;
    poll.stream = undefined;
    if (!this.isStopping) {
      const idleStreamCount = [...this.streams].filter((candidate) =>
        candidate !== poll && candidate.queue === poll.queue && candidate.task === undefined && !candidate.isDraining
      ).length;
      if (poll.isRetiring || poll.isReconnectRequested && idleStreamCount >= this.idleStreamsPerQueue) {
        this.streams.delete(poll);
      } else {
        this.scheduleReconnect(poll);
      }
    }
  }

  private scheduleReconnect(poll: PollStream): void {
    if (this.isStopping || poll.retryTimer !== undefined) return;
    if (poll.isReconnectRequested) poll.backoffMs = 100;
    const failure = poll.failure;
    if (failure?.code === status.UNAUTHENTICATED && this.hasRefreshableCredential && !poll.hasRetriedRejectedToken) {
      poll.hasRetriedRejectedToken = true;
      queueMicrotask(() => this.openStream(poll));
      return;
    }
    let delayMs: number;
    if (failure?.code === status.UNIMPLEMENTED) {
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
      poll.hasRetriedRejectedToken = false;
      this.openStream(poll);
    }, delayMs);
  }

  private recordStreamResult(poll: PollStream): void {
    if (poll.hasRecordedResult) return;
    poll.hasRecordedResult = true;
    const failure = poll.failure;
    const isPermanent = failure !== undefined && [
      status.INVALID_ARGUMENT, status.RESOURCE_EXHAUSTED, status.UNIMPLEMENTED,
      status.PERMISSION_DENIED, status.UNAUTHENTICATED,
    ].includes(failure.code) && !(failure.code === status.UNAUTHENTICATED
      && this.hasRefreshableCredential && !poll.hasRetriedRejectedToken);
    const isRejected = this.registration.ended(poll.queue, poll.isRegistered, isPermanent ? failure : undefined);
    poll.isRegistered = false;
    if (isRejected && this.options.stopOnPermanentStreamRejection === true && !this.isStopping) {
      queueMicrotask(this.onPermanentRejection);
    }
  }

  private closeStream(poll: PollStream): void {
    this.recordStreamResult(poll);
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

function createWorkerTaskClient(serverAddress: string, credential: Credential | undefined): InstanceType<typeof WorkerTaskServiceClient> {
  return new WorkerTaskServiceClient(serverAddress, credentials.createInsecure(), {
    interceptors: credentialInterceptors(credential),
  });
}
