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
  FlowNotFoundError,
  TimeTravelType,
  StepExecutionId,
  SubFlowReusePolicy,
  TimerId,
  stringCodec,
  type Client,
} from "../../src/index.js";
import { AbnormalExitFlow } from "./abnormal_exit_flow.js";
import { BasicFlow } from "./basic_flow.js";
import { flowId, skipIfServerZD, skipTimerWhenPending, withEnvironment } from "./environment.js";
import {
  AllSubFlowParent,
  AnySubFlowParent,
  ContinueAsNewSubFlowParent,
  SingleSubFlowParent,
} from "./subflow_flow.js";
import { TimerFlow } from "./timer_flow.js";

test("SubFlow returns identity and output", async () => {
  const child = new BasicFlow();
  const parent = new SingleSubFlowParent(child);
  await withEnvironment([parent, child], async ({ client }) => {
    const id = flowId("sub-flow-parent");
    await client.startFlow(parent, id, 4);
    assert.equal(
      (await client.waitForFlow(id, 30_000)).singleOutput(stringCodec),
      `SubFlow:${id}-ParentStep-1-0|completed|6`,
    );
  });
});

test("SubFlow allOf returns stable terminal results", async () => {
  const child = new BasicFlow();
  const parent = new AllSubFlowParent(child);
  await withEnvironment([parent, child], async ({ client }) => {
    const id = flowId("sub-flow-all");
    await client.startFlow(parent, id, 4);
    const output = (await client.waitForFlow(id, 30_000)).singleOutput(stringCodec);
    assert.deepEqual(output.split(";"), [
      `SubFlow:${id}-ParentStep-1-0|completed|6`,
      `SubFlow:${id}-ParentStep-1-1|completed|16`,
    ]);
  });
});

test("SubFlow anyOf running snapshot can be stopped", async () => {
  const child = new TimerFlow();
  const parent = new AnySubFlowParent(child);
  await withEnvironment([parent, child], async ({ client }) => {
    const id = flowId("sub-flow-any");
    await client.startFlow(parent, id, 300);
    const output = (await client.waitForFlow(id, 30_000)).singleOutput(stringCodec);
    const childId = `SubFlow:${id}-ParentStep-1-0`;
    assert.deepEqual(output.split("|"), [childId, "running", "false", "true"]);
    await client.stopFlow(childId);
    assert.equal((await client.waitForFlow(childId, 30_000)).status, "cancelled");
  });
});

test("SubFlow ATTACH keeps a running execution across parent reset", async () => {
  await assertRunningReuse(SubFlowReusePolicy.ATTACH, false);
});

test("SubFlow ALWAYS_RESTART replaces a running execution across parent reset", async () => {
  await assertRunningReuse(SubFlowReusePolicy.ALWAYS_RESTART, true);
});

test("SubFlow default reuse restarts a failed execution across parent reset", async () => {
  const child = new AbnormalExitFlow();
  const parent = new SingleSubFlowParent(child);
  await withEnvironment([parent, child], async ({ client }) => {
    const id = flowId("sub-flow-abnormal");
    const childId = `SubFlow:${id}-ParentStep-1-0`;
    await client.startFlow(parent, id, 1);
    assert.equal((await client.waitForFlow(id, 30_000)).singleOutput(stringCodec).split("|")[1], "failed");
    const firstChildRunId = (await client.describeFlow(childId)).runId;
    await client.timeTravel(id, { type: TimeTravelType.BEGINNING, reason: "verify SubFlow abnormal reuse" });
    assert.equal((await client.waitForFlow(id, 30_000)).singleOutput(stringCodec).split("|")[1], "failed");
    assert.notEqual((await client.describeFlow(childId)).runId, firstChildRunId);
  });
});

test("SubFlow partial results survive continue-as-new without restart", async (context) => {
  // C3: server-zd never continues a run as new.
  if (await skipIfServerZD(context, "Continue-As-New")) return;
  const completed = new BasicFlow();
  const delayed = new TimerFlow();
  const parent = new ContinueAsNewSubFlowParent(completed, delayed);
  await withEnvironment([parent, completed, delayed], async ({ client }) => {
    const id = flowId("sub-flow-can");
    const completedId = `SubFlow:${id}-ParentStep-1-0`;
    const delayedId = `SubFlow:${id}-ParentStep-1-1`;
    const firstParentRunId = await client.startFlow(parent, id, 4, {
      configOverride: { continueAsNewThreshold: 1 },
    });
    await awaitDifferentRun(client, id, firstParentRunId);
    const completedRunId = (await client.describeFlow(completedId)).runId;
    await skipTimerWhenPending(
      client,
      delayedId,
      StepExecutionId.of("TimerStep"),
      TimerId.byConditionId("test-timer-id"),
    );
    const output = (await client.waitForFlow(id, 30_000)).singleOutput(stringCodec);
    assert.deepEqual(output.split("|"), [completedId, "6", delayedId, "completed"]);
    assert.equal((await client.describeFlow(completedId)).runId, completedRunId);
  });
});

async function assertRunningReuse(
  reusePolicy: typeof SubFlowReusePolicy[keyof typeof SubFlowReusePolicy],
  expectsRestart: boolean,
): Promise<void> {
  const child = new TimerFlow();
  const parent = new SingleSubFlowParent(child, reusePolicy);
  await withEnvironment([parent, child], async ({ client }) => {
    const id = flowId("sub-flow-reuse");
    const childId = `SubFlow:${id}-ParentStep-1-0`;
    await client.startFlow(parent, id, 300);
    const firstRunId = await awaitRunning(client, childId);
    await client.timeTravel(id, { type: TimeTravelType.BEGINNING, reason: "verify SubFlow running reuse" });
    const activeRunId = await awaitRunning(client, childId, expectsRestart ? firstRunId : undefined);
    assert.equal(activeRunId !== firstRunId, expectsRestart);
    await skipTimerWhenPending(
      client,
      childId,
      StepExecutionId.of("TimerStep"),
      TimerId.byConditionId("test-timer-id"),
    );
    const output = (await client.waitForFlow(id, 30_000)).singleOutput(stringCodec);
    assert.deepEqual(output.split("|").slice(0, 2), [childId, "completed"]);
  });
}

async function awaitRunning(client: Client, id: string, excludedRunId?: string): Promise<string> {
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    try {
      const info = await client.describeFlow(id);
      if (info.status === "running" && info.runId !== excludedRunId) return info.runId;
    } catch (error) {
      if (!(error instanceof FlowNotFoundError)) throw error;
    }
    await delay();
  }
  throw new Error(`SubFlow did not reach expected running execution: ${id}`);
}

async function awaitDifferentRun(client: Client, id: string, runId: string): Promise<void> {
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    if ((await client.describeFlow(id)).runId !== runId) return;
    await delay();
  }
  throw new Error(`Flow did not continue as new: ${id}`);
}

function delay(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 10));
}
