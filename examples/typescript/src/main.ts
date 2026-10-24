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

import { pathToFileURL } from "node:url";

import express from "express";

import { Client, Worker, openBlobCache } from "@superdurable/dex";

import { setClient } from "./client-holder.js";
import { startCronSchedule } from "./config/cron-schedule-starter.js";
import { loadEnv } from "./config/env.js";
import { startExampleWorker } from "./config/worker-mode.js";
import { createDrainInternalRouter } from "./patterns/drain-channels/internal/controller.js";
import { createDrainingChannelRouter } from "./patterns/drain-channels/external-publishing/controller.js";
import { createEntityStoreRouter } from "./patterns/entity-store/controller.js";
import { createHashPartitionedAttributeMapRouter } from "./patterns/hash-partitioned-attribute-map/controller.js";
import { createInterruptibleRouter } from "./patterns/interruptible/controller.js";
import { createManualRecoveryRouter } from "./patterns/intervention/controller.js";
import { createParallelRouter } from "./patterns/parallel/controller.js";
import { createParallelSubFlowsRouter } from "./patterns/parallel-subflows/controller.js";
import { createPatternPollingRouter } from "./patterns/polling/controller.js";
import { createRecoveryRouter } from "./patterns/recovery/controller.js";
import { createRemindersRouter } from "./patterns/reminders/controller.js";
import { createSequentiallyChunkedAttributeMapRouter } from "./patterns/sequentially-chunked-attribute-map/controller.js";
import { createInactivenessTrackerTimerRouter } from "./patterns/inactiveness-tracker-timer/controller.js";
import { createTimeoutRouter } from "./patterns/timeout/controller.js";
import { createWaitForStepCompletionRouter } from "./patterns/wait-for-step-completion/controller.js";
import { createAttributeRouter } from "./primitives/attribute/controller.js";
import { createChannelRouter } from "./primitives/channel/controller.js";
import { createClientApisRouter } from "./primitives/client-apis/controller.js";
import { createCustomRetryRouter } from "./primitives/custom-retry/controller.js";
import { createDurabilityRouter } from "./primitives/durability/controller.js";
import { createHeartbeatRouter } from "./primitives/heartbeat/controller.js";
import { createOptionsOverrideRouter } from "./primitives/options-override/controller.js";
import { createRpcRouter } from "./primitives/rpc/controller.js";
import { createFlowRouter } from "./primitives/flow/controller.js";
import { createStepRouter } from "./primitives/step/controller.js";
import { createStepDecisionRouter } from "./primitives/step-decision/controller.js";
import { createStreamRouter } from "./primitives/stream/controller.js";
import { createSubflowRouter } from "./primitives/subflow/controller.js";
import { createTimerRouter } from "./primitives/timer/controller.js";
import { createWaitTypesRouter } from "./primitives/wait-types/controller.js";
import { createEngagementRouter } from "./products/engagement/controller.js";
import { createJobPostingRouter } from "./products/job-post/controller.js";
import { createMicroserviceRouter } from "./products/microservices/controller.js";
import { createMoneyTransferRouter } from "./products/money-transfer/controller.js";
import { createOrderProcessingRouter } from "./products/order-processing/controller.js";
import { createSignupRouter } from "./products/signup/controller.js";
import { createSubscriptionRouter } from "./products/subscription/controller.js";
import {
  createExampleRegistry,
  orderProcessingFlow,
  pollingFlow,
  pollingJobs,
} from "./registry.js";

export interface SampleServer {
  readonly client: Client;
  readonly worker: Worker;
  readonly httpServer: ReturnType<express.Express["listen"]>;
  close(): Promise<void>;
}

export async function startSampleServer(): Promise<SampleServer> {
  const env = loadEnv();
  const registry = createExampleRegistry();
  const blobCache = openBlobCache({
    directory: env.blobCacheDir,
    maxBytes: 1 << 30,
  });
  const worker = await startExampleWorker(registry, blobCache, env.workerMode, env.serverAddress, {
    bindAddress: env.workerBindAddress,
    serverAddress: env.serverAddress,
    ...(env.workerTarget !== undefined
      ? { workerTarget: { address: env.workerTarget } }
      : {}),
  });
  const client = new Client(registry, blobCache, {
    serverAddress: env.serverAddress,
    workerTarget: worker.workerTarget,
  });
  setClient(client);

  const app = express();
  app.use(allowCors);
  app.use(express.json());
  app.use("/products/money-transfer", createMoneyTransferRouter(client));
  app.use("/products/order-processing", createOrderProcessingRouter(client, orderProcessingFlow));
  app.use("/products/microservices", createMicroserviceRouter(client));
  app.use("/products/engagement", createEngagementRouter(client));
  app.use("/products/subscription", createSubscriptionRouter(client));
  app.use("/products/signup", createSignupRouter(client));
  app.use("/products/job-post", createJobPostingRouter(client));
  app.use("/patterns/polling", createPatternPollingRouter(client, pollingFlow, pollingJobs));
  app.use("/patterns/interruptible", createInterruptibleRouter(client));
  app.use("/patterns/reminders", createRemindersRouter(client));
  app.use("/patterns/entity-store", createEntityStoreRouter(client));
  app.use(
    "/patterns/sequentially-chunked-attribute-map",
    createSequentiallyChunkedAttributeMapRouter(client),
  );
  app.use(
    "/patterns/hash-partitioned-attribute-map",
    createHashPartitionedAttributeMapRouter(client),
  );
  app.use("/patterns/manual-recovery", createManualRecoveryRouter(client));
  app.use(
    "/patterns/inactiveness-tracker-timer",
    createInactivenessTrackerTimerRouter(client),
  );
  app.use("/patterns/parallel", createParallelRouter(client));
  app.use("/patterns/parallel-subflows", createParallelSubFlowsRouter(client));
  app.use("/patterns/recovery", createRecoveryRouter(client));
  app.use("/patterns/drain-channels/internal", createDrainInternalRouter(client));
  app.use("/patterns/drain-channels/external-publishing", createDrainingChannelRouter(client));
  app.use("/patterns/wait-for-step-completion", createWaitForStepCompletionRouter(client));
  app.use("/patterns/timeout", createTimeoutRouter(client));
  app.use("/primitives/flow", createFlowRouter(client));
  app.use("/primitives/step", createStepRouter(client));
  app.use("/primitives/step/custom-retry", createCustomRetryRouter(client));
  app.use("/primitives/step/durability", createDurabilityRouter(client));
  app.use("/primitives/step/heartbeat", createHeartbeatRouter(client));
  app.use("/primitives/step/options-override", createOptionsOverrideRouter(client));
  app.use("/primitives/step/step-decision", createStepDecisionRouter(client));
  app.use("/primitives/step/wait-types", createWaitTypesRouter(client));
  app.use("/primitives/attribute", createAttributeRouter(client));
  app.use("/primitives/channel", createChannelRouter(client));
  app.use("/primitives/stream", createStreamRouter(client));
  app.use("/primitives/timer", createTimerRouter(client));
  app.use("/primitives/rpc", createRpcRouter(client));
  app.use("/primitives/subflow", createSubflowRouter(client));
  app.use("/primitives/client-apis", createClientApisRouter(client));
  app.use(
    (
      error: unknown,
      _request: express.Request,
      response: express.Response,
      _next: express.NextFunction,
    ) => {
      const message = error instanceof Error ? error.message : String(error);
      response.status(500).type("text/plain").send(message);
    },
  );
  await startCronSchedule(client);

  const httpServer = await new Promise<ReturnType<express.Express["listen"]>>(
    (resolve, reject) => {
      const { host, port } = parseHttpAddress(env.httpAddress);
      const server = app.listen(port, host, () => {
        resolve(server);
      });
      server.on("error", reject);
    },
  );

  return {
    client,
    worker,
    httpServer,
    async close() {
      await new Promise<void>((resolve, reject) => {
        httpServer.close((error) => {
          if (error !== undefined) {
            reject(error);
            return;
          }
          resolve();
        });
      });
      await Promise.race([
        (async () => {
          await client.close();
          await worker.close();
          blobCache.close();
        })(),
        new Promise<void>((resolve) => {
          setTimeout(resolve, 3_000);
        }),
      ]);
    },
  };
}

function allowCors(
  request: express.Request,
  response: express.Response,
  next: express.NextFunction,
): void {
  response.setHeader("Access-Control-Allow-Origin", "*");
  response.setHeader(
    "Access-Control-Allow-Methods",
    "GET, POST, PUT, PATCH, DELETE, OPTIONS",
  );
  response.setHeader(
    "Access-Control-Allow-Headers",
    "Origin, Content-Type, Accept, Authorization",
  );
  if (request.method === "OPTIONS") {
    response.sendStatus(204);
    return;
  }
  next();
}

function parseHttpAddress(address: string): { host: string; port: number } {
  const [host, portText] = address.includes(":")
    ? (address.split(":") as [string, string])
    : ["127.0.0.1", address];
  return { host, port: Number(portText) };
}

async function main(): Promise<void> {
  const server = await startSampleServer();
  const env = loadEnv();
  console.log(
    `Dex TypeScript examples listening on http://${env.httpAddress} (${
      env.workerMode === "pull" ? "pull worker" : `worker ${env.workerBindAddress}`
    })`,
  );
  const shutdown = async () => {
    await server.close();
    process.exit(0);
  };
  process.on("SIGINT", () => {
    void shutdown();
  });
  process.on("SIGTERM", () => {
    void shutdown();
  });
}

if (import.meta.url === pathToFileURL(process.argv[1] ?? "").href) {
  main().catch((error) => {
    console.error(error);
    process.exit(1);
  });
}
