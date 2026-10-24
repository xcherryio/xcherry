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
  FlowAlreadyStartedError,
  FlowErrorType,
  FlowNotFoundError,
  IdReusePolicy,
  StepExecutionId,
  doubleCodec,
  stringCodec,
  voidCodec,
  type Flow,
} from "../../src/index.js";
import { AbnormalExitFlow } from "./abnormal_exit_flow.js";
import { BasicFlow } from "./basic_flow.js";
import { EmptyInputFlow } from "./empty_input_flow.js";
import { expectError, flowId, skipIfServerZD, withEnvironment } from "./environment.js";
import { ImmutableStepOptionsFlow } from "./immutable_step_options_flow.js";
import { MixedWaitFlow } from "./mixed_wait_flow.js";
import { ModelInputFlow } from "./model_input_flow.js";
import { MultiOutputFlow } from "./multi_output_flow.js";
import { ProceedOnWaitFailureFlow } from "./proceed_on_wait_failure_flow.js";
import { SignalFlow } from "./signal_flow.js";

test("basic workflow completes and disallows duplicate IDs", async () => {
  const flow = new BasicFlow();
  await withEnvironment([flow], async ({ client }) => {
    const id = flowId("basic");
    const options = { idReusePolicy: IdReusePolicy.DISALLOW };
    await client.startFlow(flow, id, 0, options);
    assert.equal(await client.waitForFlow(id, 30_000).then((result) => result.singleOutput(doubleCodec)), 2);
    await expectError(client.startFlow(flow, id, 0, options), FlowAlreadyStartedError);
  });
});

test("parallel branches return heterogeneous Step completions", async () => {
  const flow = new MultiOutputFlow();
  await withEnvironment([flow], async ({ client }) => {
    const id = flowId("multi-output");
    await client.startFlow(flow, id, undefined);
    const result = await client.waitForFlow(id, 30_000);
    const completions = new Map(
      result.completions.map((completion) => [completion.stepType, completion]),
    );
    assert.equal(completions.size, 2);
    assert.equal(completions.get(flow.stringStep.getStepType())?.decode(stringCodec), "branch-one");
    assert.equal(completions.get(flow.numberStep.getStepType())?.decode(doubleCodec), 42);
    assert.ok(result.completions.every((completion) => completion.stepExecutionId.length > 0));
  });
});

test("failed workflow ID can be reused", async () => {
  const failed = new AbnormalExitFlow();
  const succeeding = new BasicFlow();
  await withEnvironment([failed, succeeding], async ({ client }) => {
    const id = flowId("abnormal-exit-reuse");
    const options = { idReusePolicy: IdReusePolicy.ALLOW_IF_PREVIOUS_FAILED };
    const failedRun = await client.startFlow(failed, id, 0, options);
    const failure = await client.waitForFlow(id, 30_000);
    assert.equal(failure.status, "failed");
    await client.startFlow(succeeding, id, 0, options);
    assert.equal(await client.waitForFlow(id, 30_000).then((result) => result.singleOutput(doubleCodec)), 2);
  });
});

test("empty input and output are preserved", async () => {
  const flow = new EmptyInputFlow();
  await withEnvironment([flow], async ({ client }) => {
    const id = flowId("empty-input");
    await client.startFlow(flow, id, undefined);
    assert.equal((await client.waitForFlow(id, 30_000)).completions.length, 0);
    await expectError(
      client.waitForFlow(flowId("missing"), 1_000),
      FlowNotFoundError,
    );
  });
});

test("explicit Flow type and registered instance are enforced", async () => {
  const flow = new EmptyInputFlow();
  await withEnvironment([flow], async ({ client }) => {
    assert.equal(flow.getFlowType(), "test-customized-flow-type");
    const id = flowId("type-specified");
    await client.startFlow(flow, id, undefined);
    assert.equal((await client.waitForFlow(id, 30_000)).completions.length, 0);
    await assert.rejects(
      client.startFlow(new EmptyInputFlow(), flowId("unregistered"), undefined),
      /Flow instance is not registered/,
    );
  });
});

test("model input is serialized and invalid runtime input is rejected", async () => {
  const flow = new ModelInputFlow();
  await withEnvironment([flow], async ({ client }) => {
    const id = flowId("model-input");
    await client.startFlow(flow, id, { value: 10 });
    assert.equal(await client.waitForFlow(id, 30_000).then((result) => result.singleOutput(doubleCodec)), 10);
    await assert.rejects(
      client.startFlow(flow as Flow<any>, flowId("wrong-input"), "wrong"),
      /invalid ModelInput/,
    );
  });
});

test("Flow config override survives Continue-As-New", async () => {
  const flow = new BasicFlow();
  await withEnvironment([flow], async ({ client }) => {
    const id = flowId("config-override");
    await client.startFlow(flow, id, 0, {
      configOverride: { continueAsNewThreshold: 1 },
    });
    assert.equal(await client.waitForFlow(id, 30_000).then((result) => result.singleOutput(doubleCodec)), 2);
  });
});

test("describing a missing Flow returns structured error", async () => {
  const flow = new BasicFlow();
  await withEnvironment([flow], async ({ client }) => {
    await expectError(client.describeFlow(flowId("missing")), FlowNotFoundError);
  });
});

test("describe reports a running Flow", async () => {
  const flow = new SignalFlow();
  await withEnvironment([flow], async ({ client }) => {
    const id = flowId("running");
    await client.startFlow(flow, id, 0);
    assert.equal((await client.describeFlow(id)).status, "running");
    await client.stopFlow(id);
  });
});

test("waitForStepCompletion observes the requested Step", async () => {
  const flow = new BasicFlow();
  await withEnvironment([flow], async ({ client }) => {
    const id = flowId("wait-step");
    await client.startFlow(flow, id, 5);
    await client.waitForStepCompletion(id, StepExecutionId.of("BasicSecondStep"), {
      requestId: `${id}-wait-second-step`,
      requestTimeoutMs: 30_000,
    });
    assert.equal(await client.waitForFlow(id, 30_000).then((result) => result.singleOutput(doubleCodec)), 7);
  });
});

test("waitFor failure can proceed to execute", async (context) => {
  if (await skipIfServerZD(context, "Step failure policies")) return;
  const flow = new ProceedOnWaitFailureFlow();
  await withEnvironment([flow], async ({ client }) => {
    const id = flowId("proceed-on-wait-failure");
    await client.startFlow(flow, id, "input");
    assert.equal(await client.waitForFlow(id, 30_000).then((result) => result.singleOutput(stringCodec)), "input-recovered");
  });
});

test("Steps with and without waitFor compose", async () => {
  const flow = new MixedWaitFlow();
  await withEnvironment([flow], async ({ client }) => {
    const id = flowId("mixed-wait");
    await client.startFlow(flow, id, 0);
    assert.equal(await client.waitForFlow(id, 30_000).then((result) => result.singleOutput(doubleCodec)), 2);
  });
});

test("movement options do not mutate Step defaults", async (context) => {
  if (await skipIfServerZD(context, "Step failure policies")) return;
  const flow = new ImmutableStepOptionsFlow();
  await withEnvironment([flow], async ({ client }) => {
    const id = flowId("immutable-options");
    await client.startFlow(flow, id, 0);
    const failure = await client.waitForFlow(id, 30_000);
    assert.equal(failure.status, "failed");
    assert.equal(failure.errorType, FlowErrorType.WORKER_API_FAILED);
    assert.equal(failure.errorMessage, "expected wait failure 2");
  });
});
