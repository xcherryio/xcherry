// Portions of this file are derived from indeedeng/iwf-java-sdk.
// Those portions are licensed under the Apache License, Version 2.0.
// See LICENSES/Apache-2.0.txt and LEGACY_NOTICES.md.
//
// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications are licensed under the Sustainable Use License 1.0.
// Third-Party Materials remain under the Apache License, Version 2.0.
// See LICENSE and LEGACY_NOTICES.md.

import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  Client,
  DexServiceError,
  Registry,
  Worker,
  openBlobCache,
  type Flow,
  type StepExecutionId,
  type TimerId,
} from "../../src/index.js";

import { WorkerPortReservation } from "../worker-port-reservation.js";

export interface TestEnvironment {
  readonly client: Client;
  readonly worker: Worker;
  close(): Promise<void>;
}

export async function startEnvironment(...flows: readonly Flow<any>[]): Promise<TestEnvironment> {
  const serverAddress = process.env.DEX_SERVER_ADDRESS ?? "127.0.0.1:8801";
  const workerMode = process.env.DEX_INTEG_WORKER_MODE ?? "push";
  if (workerMode !== "push" && workerMode !== "pull") {
    throw new Error(`invalid DEX_INTEG_WORKER_MODE: ${workerMode}`);
  }
  const registry = new Registry(flows);
  const cacheDirectory = mkdtempSync(join(tmpdir(), "dex-typescript-integration-cache-"));
  const blobCache = openBlobCache({
    directory: cacheDirectory,
    maxBytes: 64 * 1_024 * 1_024,
  });
  const reservation = workerMode === "push" ? await WorkerPortReservation.reserve() : undefined;
  const worker = new Worker(registry, blobCache, reservation === undefined
    ? { serverAddress, pull: { appWorkerID: flowId("integ-worker") } }
    : { serverAddress, bindAddress: `127.0.0.1:${reservation.port}` });
  try {
    await reservation?.release();
    await worker.start();
  } finally {
    await reservation?.release();
  }
  const client = new Client(registry, blobCache, {
    serverAddress,
    workerTarget: worker.workerTarget,
  });
  return {
    client,
    worker,
    async close() {
      await client.close();
      await worker.close();
      blobCache.close();
      rmSync(cacheDirectory, { recursive: true, force: true });
    },
  };
}

export async function withEnvironment(
  flows: readonly Flow<any>[],
  run: (environment: TestEnvironment) => Promise<void>,
): Promise<void> {
  const environment = await startEnvironment(...flows);
  try {
    await run(environment);
  } finally {
    await environment.close();
  }
}

export function flowId(prefix: string): string {
  return `${prefix}-${randomUUID()}`;
}

export async function skipTimerWhenPending(
  client: Client,
  id: string,
  stepExecutionId: StepExecutionId,
  timerId: TimerId,
): Promise<void> {
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    try {
      await client.skipTimer(id, stepExecutionId, timerId);
      return;
    } catch (failure) {
      if (
        !(failure instanceof DexServiceError) ||
        failure.detail !== "requested timer condition does not exist or is not pending"
      ) {
        throw failure;
      }
    }
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error(`Timer did not become pending for ${id}`);
}

export async function expectError<Failure extends Error>(
  operation: Promise<unknown>,
  errorType: abstract new (...arguments_: any[]) => Failure,
): Promise<Failure> {
  try {
    await operation;
  } catch (failure) {
    assert.ok(failure instanceof errorType);
    return failure;
  }
  assert.fail(`expected ${errorType.name}`);
}
