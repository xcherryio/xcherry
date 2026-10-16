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
import { credentials } from "@grpc/grpc-js";

import {
  FlowServiceClient,
  type GetFlowStateResponse,
  type KV,
  type StepMethodFailure,
} from "../../src/gen/dex.js";

function flowServiceClient(): InstanceType<typeof FlowServiceClient> {
  const serverAddress = process.env.DEX_SERVER_ADDRESS ?? "127.0.0.1:8801";
  return new FlowServiceClient(serverAddress, credentials.createInsecure());
}

function getFlowState(
  client: InstanceType<typeof FlowServiceClient>,
  flowId: string,
  runId: string,
): Promise<GetFlowStateResponse> {
  return new Promise((resolve, reject) => {
    client.getFlowState({ flowId, runId }, (error, response) => {
      if (error !== null && error !== undefined) {
        reject(error);
        return;
      }
      resolve(response ?? { activeStepExecutions: [] });
    });
  });
}

export async function getFlowAttributes(flowId: string, keys: readonly string[]): Promise<readonly KV[]> {
  const client = flowServiceClient();
  try {
    return await new Promise((resolve, reject) => {
      client.getAttributes({ flowId, runId: "", keys: [...keys], allKeys: false }, (error, response) => {
        if (error !== null && error !== undefined) {
          reject(error);
          return;
        }
        resolve(response?.attributes ?? []);
      });
    });
  } finally {
    client.close();
  }
}

export async function awaitLiveWorkerFailure(
  flowId: string,
  runId: string,
  timeoutMilliseconds = 6_000,
): Promise<StepMethodFailure> {
  const client = flowServiceClient();
  const deadline = Date.now() + timeoutMilliseconds;
  while (Date.now() < deadline) {
    const response = await getFlowState(client, flowId, runId);
    for (const step of response.activeStepExecutions) {
      if (step.lastFailureInfo?.details !== undefined) {
        client.close();
        return step.lastFailureInfo;
      }
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  client.close();
  throw new Error("active Step did not expose retry failure");
}

export function assertWorkerFailureStackTrace(
  failure: StepMethodFailure,
  expectedDetail: string,
): void {
  assert.equal(failure.attempt, 1);
  assert.ok(failure.details !== undefined);
  assert.equal(failure.details.originalWorkerErrorDetail, expectedDetail);
  const stackTrace = failure.details.originalWorkerErrorStackTrace;
  assert.ok(stackTrace.length > 0);
  assert.ok(stackTrace.includes(expectedDetail));
}
