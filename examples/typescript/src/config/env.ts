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

import type { StartFlowOptions } from "@superdurable/dex";

import { readWorkerModeFromEnvironment, type WorkerMode } from "./worker-mode.js";

export const HOUR_MS = 60 * 60 * 1000;
export const DAY_MS = 24 * HOUR_MS;

export function startOptions(overrides: StartFlowOptions = {}): StartFlowOptions {
  return { timeoutMs: HOUR_MS, ...overrides };
}

export function environmentOr(name: string, fallback: string): string {
  const value = process.env[name];
  return value !== undefined && value.length > 0 ? value : fallback;
}

export interface SampleEnv {
  readonly serverAddress: string;
  readonly workerBindAddress: string;
  readonly workerTarget: string | undefined;
  readonly httpAddress: string;
  readonly blobCacheDir: string;
  readonly workerMode: WorkerMode;
}

export function loadEnv(): SampleEnv {
  const workerTarget = process.env.DEX_WORKER_TARGET?.trim();
  return {
    serverAddress: environmentOr("DEX_FLOW_SERVICE_ADDRESS", "localhost:8801"),
    workerBindAddress: environmentOr("DEX_WORKER_BIND_ADDRESS", "127.0.0.1:8803"),
    workerTarget: workerTarget !== undefined && workerTarget.length > 0 ? workerTarget : undefined,
    httpAddress: environmentOr("DEX_EXAMPLES_HTTP_ADDRESS", "127.0.0.1:8080"),
    blobCacheDir: environmentOr(
      "DEX_BLOB_CACHE_DIR",
      `${process.env.TMPDIR ?? "/tmp"}/dex-typescript-examples-blobs`,
    ),
    workerMode: readWorkerModeFromEnvironment(),
  };
}
