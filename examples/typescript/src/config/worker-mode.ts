/*
 * Copyright (c) 2022-2026 Super Durable, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {
  Worker,
  type BlobCache,
  type PullWorkerOptions,
  type Registry,
  type WorkerOptions,
} from "@superdurable/dex";

/** Environment variable that selects push or pull Workers. Empty selects push. */
export const WORKER_MODE_ENVIRONMENT_VARIABLE = "DEX_EXAMPLES_WORKER_MODE";

/** How an example Worker receives tasks from Dex Server. */
export type WorkerMode = "push" | "pull";

const PULL_REGISTRATION_TIMEOUT_MS = 30_000;

/** Returns the Worker mode selected by DEX_EXAMPLES_WORKER_MODE, or throws for an unknown value. */
export function readWorkerModeFromEnvironment(): WorkerMode {
  const value = process.env[WORKER_MODE_ENVIRONMENT_VARIABLE] ?? "";
  switch (value) {
    case "":
    case "push":
      return "push";
    case "pull":
      return "pull";
    default:
      throw new Error(
        `${WORKER_MODE_ENVIRONMENT_VARIABLE} must be push or pull, got ${JSON.stringify(value)}`,
      );
  }
}

/** Returns pull options sized for every sample Flow sharing one Worker process. */
export function newExamplePullWorkerOptions(): PullWorkerOptions {
  return { maxConcurrentTasks: 64 };
}

/** Starts a push Worker, or a pull Worker whose task streams Dex Server registered. */
export async function startExampleWorker(
  registry: Registry,
  blobCache: BlobCache,
  workerMode: WorkerMode,
  serverAddress: string,
  pushWorkerOptions: WorkerOptions,
): Promise<Worker> {
  if (workerMode === "push") {
    const worker = new Worker(registry, blobCache, pushWorkerOptions);
    await worker.start();
    return worker;
  }
  const worker = new Worker(registry, blobCache, {
    serverAddress,
    pull: newExamplePullWorkerOptions(),
  });
  try {
    await worker.start();
    await worker.waitPullRegistered({ timeoutMs: PULL_REGISTRATION_TIMEOUT_MS });
  } catch (failure) {
    try {
      await worker.close();
    } catch (cleanupFailure) {
      throw new AggregateError([failure, cleanupFailure], "pull Worker startup and cleanup failed");
    }
    throw failure;
  }
  return worker;
}
