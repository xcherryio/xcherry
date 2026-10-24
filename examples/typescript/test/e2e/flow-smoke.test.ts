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
import { createServer } from "node:net";
import test from "node:test";

import { loadEnv } from "../../src/config/env.js";
import { startSampleServer, type SampleServer } from "../../src/main.js";
import {
  assertFlowSmokeNoUnexpectedFailures,
  assertFlowSmokeStartStep,
  defaultFlags,
  stepStartMayFailFlags,
  noStartStepFlags,
  type FlowSmokeContext,
  type FlowSmokeEntry,
  newFlowId,
  triggerGet,
  triggerPost,
} from "./flow-smoke-helper.js";
import { skipWhenServerZd } from "../server-zd.js";

let server: SampleServer;
let context: FlowSmokeContext;

test.before(async () => {
  process.env.DEX_EXAMPLES_HTTP_ADDRESS =
    process.env.DEX_SMOKE_HTTP_ADDRESS ?? `127.0.0.1:${await availablePort()}`;
  process.env.DEX_WORKER_BIND_ADDRESS =
    process.env.DEX_SMOKE_WORKER_BIND_ADDRESS ?? `127.0.0.1:${await availablePort()}`;
  server = await startSampleServer();
  const httpAddress = loadEnv().httpAddress;
  context = {
    baseUrl: `http://${httpAddress}`,
    newFlowId,
  };
});

test.after(async () => {
  await server.close();
});

function flowSmokeCatalog(): FlowSmokeEntry[] {
  return [
    { name: "products/engagement", trigger: () => triggerGet(context, "/products/engagement/start"), flags: defaultFlags() },
    {
      name: "products/microservices",
      trigger: () =>
        triggerGet(context, "/products/microservices/start", {
          workflowId: newFlowId("microservices"),
        }),
      flags: defaultFlags(),
    },
    {
      name: "products/money-transfer",
      serverZdUnsupportedFeature: "failure policies",
      trigger: () =>
        triggerGet(context, "/products/money-transfer/start", {
          amount: 100,
          fromAccount: "from-smoke",
          toAccount: "to-smoke",
          notes: "smoke",
        }),
      flags: defaultFlags(),
    },
    {
      name: "products/order-processing",
      serverZdUnsupportedFeature: "failure policies",
      trigger: () => triggerGet(context, "/products/order-processing/start"),
      flags: defaultFlags(),
    },
    {
      name: "products/subscription",
      trigger: () => triggerGet(context, "/products/subscription/start"),
      flags: defaultFlags(),
    },
    {
      name: "products/signup",
      trigger: async () => {
        const username = newFlowId("signup");
        const result = await triggerGet(context, "/products/signup/submit", {
          username,
          email: `${username}@example.com`,
        });
        return {
          flowId: result.flowId || username,
          runId: result.runId,
        };
      },
      flags: defaultFlags(),
    },
    {
      name: "products/job-post",
      trigger: () =>
        triggerGet(context, "/products/job-post/create", {
          title: "Smoke Test Job",
          description: "Smoke test description",
        }),
      flags: noStartStepFlags(),
    },
    {
      name: "patterns/polling",
      serverZdUnsupportedFeature: "failure policies",
      trigger: () =>
        triggerGet(context, "/patterns/polling/start", {
          workflowId: newFlowId("pattern-polling"),
        }),
      flags: defaultFlags(),
    },
    {
      name: "patterns/interruptible",
      trigger: () =>
        triggerGet(context, "/patterns/interruptible/start", {
          workflowId: newFlowId("interruptible"),
        }),
      flags: defaultFlags(),
    },
    {
      name: "patterns/reminders",
      trigger: () => triggerGet(context, "/patterns/reminders/start"),
      flags: defaultFlags(),
    },
    {
      name: "patterns/entity-store",
      serverZdUnsupportedFeature: "Attribute Store sync",
      trigger: async () => {
        const userId = newFlowId("entity-store");
        const result = await triggerPost(context, "/patterns/entity-store/profile", {
          userId,
          displayName: "Smoke Tester",
          email: `${userId}@example.com`,
          marketingOptIn: true,
          credits: 120,
          weight: 59.5,
          lastLoggedInTime: "2026-08-11T15:30:00Z",
          metadata: { source: "smoke", tags: ["example"] },
        });
        return { flowId: result.flowId || userId, runId: result.runId };
      },
      flags: noStartStepFlags(),
    },
    {
      name: "patterns/sequentially-chunked-attribute-map",
      trigger: () =>
        triggerPost(
          context,
          "/patterns/sequentially-chunked-attribute-map/start",
          undefined,
        ),
      flags: noStartStepFlags(),
    },
    {
      name: "patterns/hash-partitioned-attribute-map",
      trigger: () =>
        triggerPost(
          context,
          "/patterns/hash-partitioned-attribute-map/start",
          undefined,
        ),
      flags: noStartStepFlags(),
    },
    {
      name: "patterns/manual-recovery",
      serverZdUnsupportedFeature: "failure policies",
      trigger: () =>
        triggerGet(context, "/patterns/manual-recovery/start", {
          workflowId: newFlowId("manual-recovery"),
        }),
      flags: defaultFlags(),
    },
    {
      name: "patterns/inactiveness-tracker-timer",
      trigger: () =>
        triggerGet(context, "/patterns/inactiveness-tracker-timer/start", {
          workflowId: newFlowId("inactiveness-tracker-timer"),
        }),
      flags: defaultFlags(),
    },
    {
      name: "patterns/parallel/static",
      trigger: () =>
        triggerGet(context, "/patterns/parallel/start/static", {
          workflowId: newFlowId("parallel-static"),
        }),
      flags: defaultFlags(),
    },
    {
      name: "patterns/parallel/dynamic",
      trigger: () =>
        triggerGet(context, "/patterns/parallel/start/dynamic", {
          workflowId: newFlowId("parallel-dynamic"),
        }),
      flags: defaultFlags(),
    },
    {
      name: "patterns/parallel/await",
      trigger: () =>
        triggerGet(context, "/patterns/parallel/start/await", {
          workflowId: newFlowId("parallel-await"),
        }),
      flags: defaultFlags(),
    },
    {
      name: "patterns/parallel/first-win",
      trigger: () =>
        triggerGet(context, "/patterns/parallel/start/first-win", {
          workflowId: newFlowId("parallel-first-win"),
        }),
      flags: defaultFlags(),
    },
    {
      name: "patterns/recovery",
      serverZdUnsupportedFeature: "failure policies",
      trigger: () =>
        triggerGet(context, "/patterns/recovery/start", {
          workflowId: newFlowId("recovery"),
          itemName: "smoke-item",
          quantity: 2,
        }),
      flags: stepStartMayFailFlags(),
    },
    parallelSubFlowsSmokeEntry("basic"),
    parallelSubFlowsSmokeEntry("wait-for-half"),
    parallelSubFlowsSmokeEntry("long-lived-parent"),
    parallelSubFlowsSmokeEntry("short-lived-parent"),
    {
      name: "patterns/drain-channels/internal",
      trigger: () =>
        triggerGet(context, "/patterns/drain-channels/internal/start", {
          workflowId: newFlowId("drain-internal"),
        }),
      flags: defaultFlags(),
    },
    {
      name: "patterns/drain-channels/external-publishing",
      trigger: () =>
        triggerGet(context, "/patterns/drain-channels/external-publishing/start-or-publish", {
          workflowId: newFlowId("drain-external"),
        }),
      flags: defaultFlags(),
    },
    {
      name: "patterns/wait-for-step-completion",
      trigger: () =>
        triggerGet(context, "/patterns/wait-for-step-completion/start", {
          workflowId: newFlowId("wait-for-state"),
        }),
      flags: defaultFlags(),
    },
    {
      name: "patterns/timeout",
      trigger: () =>
        triggerGet(context, "/patterns/timeout/start", {
          workflowId: newFlowId("timeout"),
          successfulWorkflow: true,
        }),
      flags: defaultFlags(),
    },
    {
      name: "primitives/step",
      trigger: () =>
        triggerGet(context, "/primitives/step/start", {
          workflowId: newFlowId("primitive-step"),
          inputNum: 1,
        }),
      flags: defaultFlags(),
    },
    {
      name: "primitives/step/retry",
      trigger: () =>
        triggerGet(context, "/primitives/step/retry/start", {
          workflowId: newFlowId("primitive-step-retry"),
          readyAfterAttempt: 2,
        }),
      flags: defaultFlags(),
    },
    {
      name: "primitives/step/custom-retry",
      trigger: () =>
        triggerGet(context, "/primitives/step/custom-retry/start", {
          workflowId: newFlowId("primitive-step-custom-retry"),
          readyAfterAttempt: 1,
        }),
      flags: defaultFlags(),
    },
    {
      name: "primitives/step/durability",
      trigger: () =>
        triggerGet(context, "/primitives/step/durability/start", {
          workflowId: newFlowId("primitive-step-durability"),
          mode: "sync",
        }),
      flags: defaultFlags(),
    },
    {
      name: "primitives/step/heartbeat",
      trigger: () =>
        triggerGet(context, "/primitives/step/heartbeat/start", {
          workflowId: newFlowId("primitive-step-heartbeat"),
          batches: 0,
        }),
      flags: defaultFlags(),
    },
    {
      name: "primitives/step/options-override",
      serverZdUnsupportedFeature: "failure policies",
      trigger: () =>
        triggerGet(context, "/primitives/step/options-override/start", {
          workflowId: newFlowId("primitive-step-options-override"),
          input: "smoke",
        }),
      flags: defaultFlags(),
    },
    {
      name: "primitives/step/step-decision",
      trigger: () =>
        triggerGet(context, "/primitives/step/step-decision/start", {
          workflowId: newFlowId("primitive-step-decision"),
          mode: "graceful",
        }),
      flags: defaultFlags(),
    },
    {
      name: "primitives/step/wait-types",
      trigger: () =>
        triggerGet(context, "/primitives/step/wait-types/start", {
          workflowId: newFlowId("primitive-step-wait-types"),
          mode: "any",
          timeoutSeconds: 1,
        }),
      flags: defaultFlags(),
    },
    {
      name: "primitives/attribute",
      trigger: () =>
        triggerGet(context, "/primitives/attribute/start", {
          workflowId: newFlowId("primitive-attribute"),
          message: "smoke",
        }),
      flags: defaultFlags(),
    },
    {
      name: "primitives/channel",
      trigger: () =>
        triggerGet(context, "/primitives/channel/start", {
          workflowId: newFlowId("primitive-channel"),
          inputNum: 1,
        }),
      flags: defaultFlags(),
    },
    {
      name: "primitives/stream",
      trigger: () =>
        triggerGet(context, "/primitives/stream/start", {
          workflowId: newFlowId("primitive-stream"),
          input: "smoke",
        }),
      flags: defaultFlags(),
    },
    {
      name: "primitives/timer",
      trigger: () =>
        triggerGet(context, "/primitives/timer/start", {
          workflowId: newFlowId("primitive-timer"),
          seconds: 1,
        }),
      flags: defaultFlags(),
    },
    {
      name: "primitives/rpc",
      trigger: () =>
        triggerGet(context, "/primitives/rpc/start", {
          workflowId: newFlowId("primitive-rpc"),
        }),
      flags: defaultFlags(),
    },
    {
      name: "primitives/subflow",
      trigger: () =>
        triggerGet(context, "/primitives/subflow/start", {
          workflowId: newFlowId("primitive-subflow"),
          inputNum: 1,
        }),
      flags: defaultFlags(),
    },
    {
      name: "primitives/client-apis",
      trigger: () =>
        triggerGet(context, "/primitives/client-apis/start", {
          workflowId: newFlowId("primitive-client-apis"),
          keyword: "smoke",
        }),
      flags: defaultFlags(),
    },
  ];
}

function parallelSubFlowsSmokeEntry(kind: string): FlowSmokeEntry {
  return {
    name: `patterns/parallel-subflows/${kind}`,
    trigger: () =>
      triggerGet(context, `/patterns/parallel-subflows/start/${kind}`, {
        workflowId: newFlowId(`parallel-subflows-${kind}`),
      }),
    flags: defaultFlags(),
  };
}

test("flow smoke catalog size", () => {
  const catalog = flowSmokeCatalog();
  assert.ok(catalog.length > 0, "flow smoke catalog is empty");
});

test("flow smoke all registered flows via controller", async (t) => {
  for (const entry of flowSmokeCatalog()) {
    const skipReason =
      entry.serverZdUnsupportedFeature === undefined
        ? false
        : skipWhenServerZd(entry.serverZdUnsupportedFeature);
    await t.test(entry.name, { skip: skipReason }, async () => {
      try {
        const result = await entry.trigger(context);
        assert.ok(result.flowId, `${entry.name}: controller response did not include flowID`);
        await assertFlowSmokeStartStep(entry, result.flowId, result.runId);
        if (entry.name === "patterns/interruptible") {
          await triggerGet(context, "/patterns/interruptible/cancel", {
            workflowId: result.flowId,
          });
        }
        await assertFlowSmokeNoUnexpectedFailures(entry, result.flowId, result.runId);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        throw new Error(`${entry.name}: ${message}`, { cause: error });
      }
    });
  }
});

function availablePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (address === null || typeof address === "string") {
        server.close();
        reject(new Error("failed to allocate a smoke test port"));
        return;
      }
      server.close((error) => {
        if (error !== undefined) {
          reject(error);
          return;
        }
        resolve(address.port);
      });
    });
  });
}
