// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import {
  Server,
  ServerCredentials,
  Metadata,
  credentials,
  type ServerWritableStream,
  type ServerUnaryCall,
  type sendUnaryData,
} from "@grpc/grpc-js";

import type { BlobCache } from "./blob-cache.js";
import {
  FlowServiceClient,
  InvokeExecuteMethodOutput,
  InvokeWaitForMethodOutput,
  WorkerServiceService,
  type InvokeExecuteMethodRequest,
  type InvokeExecuteMethodResponse,
  type InvokeWaitForMethodRequest,
  type InvokeWaitForMethodResponse,
  type InvokeWorkerRPCRequest,
  type InvokeWorkerRPCResponse,
  type ServerInfo,
  type StepStreamWrite,
  type Value,
  type WorkerServiceServer,
} from "./gen/dex.js";
import { workerServiceError } from "./grpc-status.js";
import { registeredAttributeIndexes, type Registry } from "./flow.js";
import type { StepOutputEmitter } from "./invocation-context.js";
import type { WorkerOptions, WorkerTarget } from "./options.js";
import { negotiateServerProtocol, serverInfoRequestError } from "./server-protocol.js";
import { sdkVersion } from "./sdk-version.generated.js";
import { ValueHydrator } from "./value-mapper.js";
import { WorkerDispatcher } from "./worker-dispatcher.js";
import { PullWorker } from "./pull-worker.js";
import { mapWorkerTarget } from "./worker-target.js";

type WorkerState = "created" | "running" | "stopping" | "stopped" | "closed";

/**
 * Hosts registered Step and RPC handlers through a listener or outbound pull streams.
 *
 * A Worker is one-shot. Call `start` for non-blocking startup or `run` to await
 * shutdown, then call `close`. Concurrent handlers must synchronize shared state.
 * In pull mode, `close` drains for `stopDrainTimeoutMs`, then cancels handlers and
 * waits for them to return. A handler that ignores cancellation can delay shutdown.
 *
 * @example
 * ```ts
 * const worker = new Worker(registry, cache);
 * await worker.start();
 * try { console.log(worker.workerTarget.address); } finally { await worker.close(); }
 * ```
 */
export class Worker {
  /** Effective endpoint advertised to Dex. */
  public readonly workerTarget: WorkerTarget;

  private readonly server = new Server();
  private readonly flowService: InstanceType<typeof FlowServiceClient>;
  private readonly pullWorker: PullWorker | undefined;
  private readonly stopped: Promise<void>;
  private resolveStopped!: () => void;
  private state: WorkerState = "created";
  private negotiatedProtocolVersion = 0;

  /**
   * Constructs a Worker without starting its listener or task streams.
   * @param registry - Flow definitions served by this Worker.
   * @param blobCache - Open cache used to hydrate large handler values.
   * @param options - Networking, pull mode, and startup settings; uses local defaults when omitted.
   * @throws {TypeError} When pull mode conflicts with a listener or has an invalid Worker ID.
   * @throws {RangeError} When a pull capacity or drain timeout is invalid.
   */
  public constructor(
    public readonly registry: Registry,
    public readonly blobCache: BlobCache,
    public readonly options: WorkerOptions = {},
  ) {
    if (options.pull !== undefined && (options.bindAddress !== undefined || options.workerTarget !== undefined)) {
      throw new TypeError("pull Worker cannot configure bindAddress or workerTarget");
    }
    if (options.pull === undefined && options.workerTarget?.pull === true) {
      throw new TypeError("workerTarget.pull requires WorkerOptions.pull");
    }
    const bindAddress = options.bindAddress ?? "127.0.0.1:8803";
    this.workerTarget = options.pull === undefined
      ? options.workerTarget ?? targetFromBindAddress(bindAddress)
      : { address: "", pull: true };
    if (options.pull === undefined) mapWorkerTarget(this.workerTarget);
    this.flowService = new FlowServiceClient(
      options.serverAddress ?? "localhost:8801",
      credentials.createInsecure(),
    );
    const dispatcher = new WorkerDispatcher(
      registry,
      new ValueHydrator(this.flowService, blobCache),
    );
    try {
      this.pullWorker = options.pull === undefined
        ? undefined
        : new PullWorker(registry, dispatcher, options.serverAddress ?? "localhost:8801", options.pull);
    } catch (failure) {
      this.flowService.close();
      throw failure;
    }
    if (this.pullWorker === undefined) {
      this.server.addService(WorkerServiceService, workerService(dispatcher));
    }
    this.stopped = new Promise((resolve) => {
      this.resolveStopped = resolve;
    });
  }

  /**
   * Negotiates the Server protocol, synchronizes indexes, and starts listening or polling.
   * The promise resolves after binding or launching pull streams; call exactly once.
   */
  public async start(): Promise<void> {
    if (this.state !== "created") {
      throw new Error(`Worker cannot start from state ${this.state}`);
    }
    const bindAddress = this.options.bindAddress ?? "127.0.0.1:8803";
    try {
      const timeoutMs = this.options.attributeIndexSyncTimeoutMs ?? 120_000;
      if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
        throw new RangeError("attributeIndexSyncTimeoutMs must be positive");
      }
      let serverInfo: ServerInfo;
      try {
        serverInfo = await getServerInfo(this.flowService, timeoutMs);
      } catch (failure) {
        throw serverInfoRequestError(sdkVersion, failure);
      }
      this.negotiatedProtocolVersion = negotiateServerProtocol(serverInfo, sdkVersion);
      await syncAttributeIndexes(this.flowService, this.registry, timeoutMs);
      if (this.pullWorker === undefined) {
        await bind(this.server, bindAddress);
      } else {
        this.pullWorker.start();
      }
      this.state = "running";
    } catch (failure) {
      this.state = "stopped";
      await this.pullWorker?.close();
      this.flowService.close();
      this.resolveStopped();
      throw new Error(`cannot start TypeScript Worker on ${bindAddress}`, { cause: failure });
    }
  }

  /** Starts the Worker and waits until `close` completes shutdown. */
  public async run(): Promise<void> {
    await this.start();
    await this.stopped;
  }

  /**
   * Gracefully stops the listener or drains pull tasks, then releases connections.
   * A pull Worker force-cancels after its drain timeout and waits for handlers to return.
   * Calls before `start` and repeated calls after shutdown are safe.
   */
  public async close(): Promise<void> {
    if (this.state === "closed") {
      return;
    }
    if (this.state === "created") {
      this.state = "closed";
      await this.pullWorker?.close();
      this.flowService.close();
      this.resolveStopped();
      return;
    }
    if (this.state === "running") {
      this.state = "stopping";
      try {
        if (this.pullWorker === undefined) {
          await shutdown(this.server);
        } else {
          await this.pullWorker.close();
        }
      } finally {
        this.flowService.close();
        this.state = "stopped";
        this.resolveStopped();
      }
    } else {
      await this.stopped;
    }
    this.state = "closed";
  }
}

function getServerInfo(
  service: InstanceType<typeof FlowServiceClient>,
  timeoutMs: number,
): Promise<ServerInfo> {
  return new Promise((resolve, reject) => {
    service.getServerInfo({}, new Metadata(), { deadline: Date.now() + timeoutMs }, (error, info) => {
      if (error !== null) {
        reject(error);
        return;
      }
      resolve(info);
    });
  });
}

function syncAttributeIndexes(
  service: InstanceType<typeof FlowServiceClient>,
  registry: Registry,
  timeoutMs: number,
): Promise<void> {
  return new Promise((resolve, reject) => {
    service.syncAttributeIndexes(
      { attributeIndexes: Object.fromEntries(registeredAttributeIndexes(registry)) },
      new Metadata(),
      { deadline: Date.now() + timeoutMs },
      (error) => {
        if (error !== null) {
          reject(error);
          return;
        }
        resolve();
      },
    );
  });
}

function workerService(dispatcher: WorkerDispatcher): WorkerServiceServer {
  return {
    invokeWaitForMethod(call) {
      invokeStep(
        call,
        (signal, output) => dispatcher.invokeWaitFor(call.request, signal, output),
        waitForOutput,
      );
    },
    invokeExecuteMethod(call) {
      invokeStep(
        call,
        (signal, output) => dispatcher.invokeExecute(call.request, signal, output),
        executeOutput,
      );
    },
    invokeWorkerRpc(call, callback) {
      invoke(call, (signal) => dispatcher.invokeRPC(call.request, signal), callback);
    },
  };
}

interface StepOutputFactory<Response, Output> {
  heartbeat(value: Value | undefined): Output;
  streamWrite(write: StepStreamWrite): Output;
  result(response: Response): Output;
}

class GrpcStepOutputEmitter<Request, Response, Output> implements StepOutputEmitter {
  private isActive = true;

  public constructor(
    private readonly call: ServerWritableStream<Request, Output>,
    private readonly outputs: StepOutputFactory<Response, Output>,
  ) {}

  public recordHeartbeat(value: Value | undefined): Promise<void> {
    this.write(this.outputs.heartbeat(value));
    return Promise.resolve();
  }

  public writeStream(write: StepStreamWrite): void {
    this.write(this.outputs.streamWrite(write));
  }

  public finish(response: Response): void {
    if (!this.isActive) {
      return;
    }
    this.isActive = false;
    this.call.write(this.outputs.result(response));
    this.call.end();
  }

  public fail(failure: unknown): void {
    if (!this.isActive) {
      return;
    }
    this.isActive = false;
    this.call.emit("error", workerServiceError(failure));
  }

  public cancel(): void {
    this.isActive = false;
  }

  private write(output: Output): void {
    if (!this.isActive || this.call.cancelled) {
      throw new Error("Step invocation is no longer active");
    }
    this.call.write(output);
  }
}

function invokeStep<Request, Response, Output>(
  call: ServerWritableStream<Request, Output>,
  invocation: (signal: AbortSignal, output: StepOutputEmitter) => Promise<Response>,
  outputs: StepOutputFactory<Response, Output>,
): void {
  const controller = new AbortController();
  const emitter = new GrpcStepOutputEmitter(call, outputs);
  const cancel = (): void => {
    controller.abort();
    emitter.cancel();
  };
  call.once("cancelled", cancel);
  if (call.cancelled) {
    cancel();
  }
  invocation(controller.signal, emitter)
    .then(
      (response) => emitter.finish(response),
      (failure: unknown) => emitter.fail(failure),
    )
    .finally(() => call.off("cancelled", cancel));
}

const waitForOutput: StepOutputFactory<InvokeWaitForMethodResponse, InvokeWaitForMethodOutput> = {
  heartbeat: (value) => InvokeWaitForMethodOutput.create({
    output: { $case: "heartbeat", value: { value } },
  }),
  streamWrite: (write) => InvokeWaitForMethodOutput.create({
    output: { $case: "streamWrite", value: write },
  }),
  result: (response) => InvokeWaitForMethodOutput.create({
    output: { $case: "result", value: response },
  }),
};

const executeOutput: StepOutputFactory<InvokeExecuteMethodResponse, InvokeExecuteMethodOutput> = {
  heartbeat: (value) => InvokeExecuteMethodOutput.create({
    output: { $case: "heartbeat", value: { value } },
  }),
  streamWrite: (write) => InvokeExecuteMethodOutput.create({
    output: { $case: "streamWrite", value: write },
  }),
  result: (response) => InvokeExecuteMethodOutput.create({
    output: { $case: "result", value: response },
  }),
};

function invoke<Request, Response>(
  call: ServerUnaryCall<Request, Response>,
  invocation: (signal: AbortSignal) => Promise<Response>,
  callback: sendUnaryData<Response>,
): void {
  const controller = new AbortController();
  const cancel = (): void => controller.abort();
  call.once("cancelled", cancel);
  if (call.cancelled) {
    cancel();
  }
  invocation(controller.signal)
    .then(
      (response) => {
        if (!controller.signal.aborted) {
          callback(null, response);
        }
      },
      (failure: unknown) => {
        if (!controller.signal.aborted) {
          callback(workerServiceError(failure), null);
        }
      },
    )
    .finally(() => call.off("cancelled", cancel));
}

function bind(server: Server, address: string): Promise<number> {
  parseAddress(address);
  return new Promise((resolve, reject) => {
    server.bindAsync(address, ServerCredentials.createInsecure(), (error, port) => {
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
    server.tryShutdown((error) => {
      if (error !== undefined) {
        reject(error);
        return;
      }
      resolve();
    });
  });
}

function targetFromBindAddress(address: string): WorkerTarget {
  const parsed = parseAddress(address);
  const host = parsed.host === "" || parsed.host === "0.0.0.0" || parsed.host === "::"
    ? "localhost"
    : parsed.host;
  return { address: host.includes(":") ? `[${host}]:${parsed.port}` : `${host}:${parsed.port}` };
}

function parseAddress(address: string): { host: string; port: number } {
  if (address.trim() !== address || address === "") {
    throw new TypeError("Worker bind address is required without whitespace");
  }
  const bracketed = /^\[([^\]]+)]:(\d+)$/.exec(address);
  const separator = address.lastIndexOf(":");
  const host = bracketed?.[1] ?? (separator < 0 ? "" : address.slice(0, separator));
  const portText = bracketed?.[2] ?? (separator < 0 ? "" : address.slice(separator + 1));
  const port = Number(portText);
  if (!Number.isInteger(port) || port < 1 || port > 65_535) {
    throw new TypeError("Worker bind address requires port 1-65535");
  }
  return { host, port };
}
