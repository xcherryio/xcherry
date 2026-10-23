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
import test from "node:test";

import { stringCodec } from "@superdurable/dex";

import {
  acquireIntegEnvironment,
  releaseIntegEnvironment,
} from "./environment.js";
import { skipWhenServerZd } from "../server-zd.js";

test.before(async () => {
  await acquireIntegEnvironment();
});

test.after(async () => {
  await releaseIntegEnvironment();
});

test("moneyTransferStart", { skip: skipWhenServerZd("failure policies") }, async () => {
  const environment = await acquireIntegEnvironment();
  const flowId = environment.newFlowId("money-transfer");
  const runId = await environment.client.startFlow(
    environment.moneyTransferFlow,
    flowId,
    {
      fromAccount: "from-ci",
      toAccount: "to-ci",
      amount: 42,
      notes: "examples/typescript integration",
    },
    environment.startOptions(),
  );
  assert.ok(runId.length > 0);
  const output = await environment.client.waitForFlow(flowId, 45_000).then((result) =>
    result.singleOutput(stringCodec),
  );
  assert.match(output, /transfer is done/);
  assert.match(output, /from-ci/);
  assert.match(output, /to-ci/);
});
