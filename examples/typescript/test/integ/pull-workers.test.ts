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

import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { openBlobCache, stringCodec } from "@superdurable/dex";

import { startPullGreeting } from "../../src/production/pull-workers.js";
import {
  acquireIntegEnvironment,
  releaseIntegEnvironment,
} from "./environment.js";

test.before(async () => {
  await acquireIntegEnvironment();
});

test.after(async () => {
  await releaseIntegEnvironment();
});

test("pull Worker serves Greeting Flow", async () => {
  const environment = await acquireIntegEnvironment();
  const cacheDirectory = await mkdtemp(join(tmpdir(), "dex-pull-greeting-"));
  const cache = openBlobCache({ directory: cacheDirectory, maxBytes: 64 * 1024 * 1024 });
  const { flow, worker, client } = await startPullGreeting(
    cache,
    process.env.DEX_FLOW_SERVICE_ADDRESS ?? "127.0.0.1:8801",
  );
  try {
    const flowId = environment.newFlowId("pull-greeting");
    await client.startFlow(flow, flowId, "Dex", environment.startOptions());
    const result = await client.waitForFlow(flowId, 45_000);
    assert.equal(result.singleOutput(stringCodec), "Hello, Dex!");
    assert.equal(await client.invokeRPC(flow.getGreeting, flowId), "Hello, Dex!");
  } finally {
    await client.close();
    await worker.close();
    cache.close();
    await rm(cacheDirectory, { recursive: true, force: true });
    await releaseIntegEnvironment();
  }
});
