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
import test from "node:test";

import { StepExecutionId, stringCodec } from "../../src/index.js";
import { flowId, skipIfServerZD, withEnvironment } from "./environment.js";
import {
  StepCancellationFlow,
  cancellationScenarios,
} from "./step_cancellation_flow.js";

for (const scenario of cancellationScenarios) {
  test(`Step cancellation ${scenario}`, async (context) => {
    if (scenario !== "global-selector" && scenario !== "sibling-selector") {
      // C21: server-zd rejects Step failure policies.
      if (await skipIfServerZD(context, "Step failure policies")) return;
    }
    const flow = new StepCancellationFlow(scenario);
    await withEnvironment([flow], async ({ client }) => {
      const id = flowId(`typescript-cancellation-${scenario}`);
      await client.startFlow(flow, id, scenario);

      if (scenario !== "global-selector" && scenario !== "sibling-selector") {
        await withTimeout(flow.blockingStarted, 10_000);
        const selected = scenario === "heartbeat-wait-for"
          ? flow.blockingWaitFor
          : flow.blockingExecute;
        await client.waitForStepCompletion(id, StepExecutionId.of(selected.getStepType()), {
          requestId: `${id}-wait-selected-step`,
          requestTimeoutMs: 30_000,
        });
      }

      assert.equal(
        await client.waitForFlow(id, 30_000).then((result) => result.singleOutput(stringCodec)),
        scenario,
      );

      if (scenario === "global-selector") {
        assert.equal(flow.firstSelectorExecuted, false);
        assert.equal(flow.secondSelectorExecuted, false);
        return;
      }
      if (scenario === "sibling-selector") {
        assert.equal(flow.firstSelectorExecuted, false);
        assert.equal(flow.secondSelectorExecuted, true);
        return;
      }
      if (scenario === "no-heartbeat") {
        assert.equal(flow.handlerCanceled, false);
        let returned = false;
        void flow.lateHandlerReturned.then(() => { returned = true; });
        await Promise.resolve();
        assert.equal(returned, false);
        await withTimeout(flow.lateHandlerReturned, 8_000);
      } else {
        await withTimeout(flow.cancellationObserved, 8_000);
        assert.equal(flow.handlerCanceled, true);
        assert.equal(flow.contextReportedCancellation, true);
      }
      assert.equal(
        flow.blockingInvocations,
        scenario === "local-execute" || scenario === "local-timeout-fallback" ? 2 : 1,
      );
      assert.equal(flow.recoveryRan, false);
    });
  });
}

async function withTimeout<T>(promise: Promise<T>, milliseconds: number): Promise<T> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`timed out after ${milliseconds}ms`)),
      milliseconds,
    );
    void promise.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (failure: unknown) => {
        clearTimeout(timer);
        reject(failure);
      },
    );
  });
}
