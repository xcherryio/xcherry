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

import {
  FlowErrorType,
  StepExecutionId,
  TimerId,
  doubleCodec,
  stringCodec,
  voidCodec,
} from "../../src/index.js";
import { AnyCombinationFailFlow } from "./any_combination_fail_flow.js";
import { BasicInternalChannelFlow } from "./basic_internal_channel_flow.js";
import { ConditionalCompleteFlow } from "./conditional_complete_flow.js";
import { DeadEndFlow } from "./dead_end_flow.js";
import { ExecuteOnlyFlow } from "./execute_only_flow.js";
import { expectError, flowId, skipIfServerZD, skipTimerWhenPending, withEnvironment } from "./environment.js";
import { NoStartFlow } from "./no_start_flow.js";
import { NoStateFlow } from "./no_state_flow.js";
import { SignalFlow } from "./signal_flow.js";
import { StateOptionsFlow } from "./state_options_flow.js";
import { StateOptionsOverrideFlow } from "./state_options_override_flow.js";
import { StateRecoveryFlow } from "./state_recovery_flow.js";
import { StateRecoveryNoWaitFlow } from "./state_recovery_no_wait_flow.js";
import { TimerFlow } from "./timer_flow.js";
import { WaitingInternalChannelFlow } from "./waiting_internal_channel_flow.js";

test("unknown any-combination condition fails the Flow", async () => {
  const flow = new AnyCombinationFailFlow();
  await withEnvironment([flow], async ({ client }) => {
    const id = flowId("any-combination-fail");
    const runId = await client.startFlow(flow, id, 5);
    const failure = await client.waitForFlow(id, 30_000);
    assert.equal(failure.status, "failed");
    assert.equal(failure.errorType, FlowErrorType.WORKER_API_FAILED);
    assert.match(failure.errorMessage ?? "", /unknown condition ID/i);
    const summary = await client.describeFlow(id);
    assert.equal(summary.runId, runId);
    assert.equal(summary.status, "failed");
  });
});

for (const useSignal of [true, false]) {
  test(`conditional completion through ${useSignal ? "signal" : "internal"} channel`, async () => {
    const flow = new ConditionalCompleteFlow();
    await withEnvironment([flow], async ({ client }) => {
      const id = flowId(`conditional-${useSignal ? "signal" : "internal"}`);
      await client.startFlow(flow, id, useSignal);
      if (useSignal) {
        await client.invokeRPC(flow.publishSignal, id);
      } else {
        await client.invokeRPC(flow.publishToInternalChannel, id);
      }
      assert.equal(await client.waitForFlow(id, 30_000).then((result) => result.singleOutput(doubleCodec)), 1);
    });
  });
}

test("internal channels coordinate concurrent Steps", async () => {
  const flow = new BasicInternalChannelFlow();
  await withEnvironment([flow], async ({ client }) => {
    const id = flowId("basic-internal");
    await client.startFlow(flow, id, 1);
    assert.equal(await client.waitForFlow(id, 30_000).then((result) => result.singleOutput(doubleCodec)), 2);
  });
});

test("external messages satisfy an internal waiting channel", async () => {
  const flow = new WaitingInternalChannelFlow();
  await withEnvironment([flow], async ({ client }) => {
    const id = flowId("waiting-internal");
    await client.startFlow(flow, id, 1);
    await client.invokeRPC(flow.publish, id, 2);
    await client.invokeRPC(flow.publish, id, 3);
    assert.equal(await client.waitForFlow(id, 30_000).then((result) => result.singleOutput(doubleCodec)), 6);
  });
});

test("RPC starts a Flow without a start Step", async () => {
  const flow = new NoStartFlow();
  await withEnvironment([flow], async ({ client }) => {
    const id = flowId("no-start");
    await client.startFlow(flow, id, undefined);
    assert.equal(await client.invokeRPC(flow.invoke, id, "rpc-input"), 100);
    assert.equal(await client.waitForFlow(id, 30_000).then((result) => result.singleOutput(doubleCodec)), 1);
  });
});

test("RPC runs on a Flow with no Steps", async () => {
  const flow = new NoStateFlow();
  await withEnvironment([flow], async ({ client }) => {
    const id = flowId("no-state");
    await client.startFlow(flow, id, undefined);
    assert.equal(await client.invokeRPC(flow.invoke, id, "rpc-input"), 100);
    await client.stopFlow(id);
  });
});

test("RPC moves a dead-end Flow to completion", async () => {
  const flow = new DeadEndFlow();
  await withEnvironment([flow], async ({ client }) => {
    const id = flowId("dead-end");
    await client.startFlow(flow, id, undefined);
    assert.equal(await client.invokeRPC(flow.invoke, id, "rpc-input"), 100);
    assert.equal((await client.waitForFlow(id, 30_000)).completions.length, 0);
  });
});

test("signals, mapped signals, and skipped timer form one combination", async () => {
  const flow = new SignalFlow();
  await withEnvironment([flow], async ({ client }) => {
    const id = flowId("basic-signal");
    await client.startFlow(flow, id, 1);
    await client.invokeRPC(flow.publishFirst, id, 2);
    await client.invokeRPC(flow.publishFirst, id, 3);
    await client.invokeRPC(flow.publishFirst, id, 5);
    await client.invokeRPC(flow.publishThird, id);
    await client.invokeRPC(flow.publishMapped, id, 4);
    await skipTimerWhenPending(
      client,
      id,
      StepExecutionId.of("SignalCombinationStep"),
      TimerId.byConditionId("test-timer-id"),
    );
    assert.equal(await client.waitForFlow(id, 30_000).then((result) => result.singleOutput(doubleCodec)), 6);
  });
});

test("execute-only Steps survive Continue-As-New", async () => {
  const flow = new ExecuteOnlyFlow();
  await withEnvironment([flow], async ({ client }) => {
    const id = flowId("skip-wait-until");
    await client.startFlow(flow, id, 0, {
      configOverride: { continueAsNewThreshold: 1 },
    });
    assert.equal(await client.waitForFlow(id, 30_000).then((result) => result.singleOutput(doubleCodec)), 2);
  });
});

test("movement Step options override defaults", async (context) => {
  // C21: server-zd rejects Step failure policies.
  if (await skipIfServerZD(context, "Step failure policies")) return;
  const flow = new StateOptionsOverrideFlow();
  await withEnvironment([flow], async ({ client }) => {
    const id = flowId("state-options-override");
    await client.startFlow(flow, id, "input");
    assert.equal(
      await client.waitForFlow(id, 30_000).then((result) => result.singleOutput(stringCodec)),
      "input_state1_start_state1_decide_state2_start_state2_decide",
    );
  });
});

test("timeouts, retries, durability, and locks compose", async () => {
  const flow = new StateOptionsFlow();
  await withEnvironment([flow], async ({ client }) => {
    const id = flowId("state-options");
    await client.startFlow(flow, id, undefined);
    assert.equal(await client.waitForFlow(id, 30_000).then((result) => result.singleOutput(stringCodec)), "success");
  });
});

for (const [name, flow] of [
  ["with-wait", new StateRecoveryFlow()],
  ["without-wait", new StateRecoveryNoWaitFlow()],
] as const) {
  test(`execute failure recovery ${name}`, async (context) => {
    // C21: server-zd rejects Step failure policies.
    if (await skipIfServerZD(context, "Step failure policies")) return;
    await withEnvironment([flow], async ({ client }) => {
      const id = flowId(`state-recovery-${name}`);
      await client.startFlow(flow, id, 5);
      assert.equal(await client.waitForFlow(id, 30_000).then((result) => result.singleOutput(doubleCodec)), 10);
    });
  });
}

test("timer completes within the expected wall-clock interval", async () => {
  const flow = new TimerFlow();
  await withEnvironment([flow], async ({ client }) => {
    const id = flowId("basic-timer");
    const startedAt = performance.now();
    await client.startFlow(flow, id, 5);
    await client.waitForStepCompletion(id, StepExecutionId.of("TimerStep"), {
      requestId: `${id}-wait-timer`,
      requestTimeoutMs: 10_000,
    });
    await client.waitForFlow(id);
    const elapsedMs = performance.now() - startedAt;
    assert.ok(elapsedMs >= 4_000 && elapsedMs <= 7_000, `actual duration: ${elapsedMs}`);
  });
});
