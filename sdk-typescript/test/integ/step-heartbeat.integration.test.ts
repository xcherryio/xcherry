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

import { stringCodec } from "../../src/index.js";
import { flowId, withEnvironment } from "./environment.js";
import {
  StepHeartbeatFlow,
  type HeartbeatScenario,
} from "./step_heartbeat_flow.js";

test("regular retries restore heartbeat presence and preserve details across Stream writes", async () => {
  const flow = new StepHeartbeatFlow();
  await withEnvironment([flow], async ({ client }) => {
    for (const scenario of ["value", "clear", "null", "stream"] as const) {
      flow.scenario = scenario;
      const id = flowId(`typescript-heartbeat-${scenario}`);
      await client.startFlow(flow, id, scenario);
      assert.equal(
        await client.waitForFlow(id, 30_000).then((result) => result.singleOutput(stringCodec)),
        scenario,
      );
      if (scenario === "stream") {
        const first = await client.readStream(id, flow.progress, "", 30_000);
        const second = await client.readStream(id, flow.progress, first.resumeToken, 30_000);
        assert.equal(first.value, "stream-after-heartbeat-1");
        assert.equal(second.value, "stream-after-heartbeat-2");
        assert.equal(first.source, second.source);
      }
    }
  });
});

test("async local attempts write Streams and restore only regular heartbeats", async () => {
  const flow = new StepHeartbeatFlow();
  flow.scenario = "local";
  await withEnvironment([flow], async ({ client }) => {
    const id = flowId("typescript-heartbeat-local");
    await client.startFlow(flow, id, "local" satisfies HeartbeatScenario);
    const result = await client.waitForFlow(id, 30_000);
    assert.equal(result.status, "completed", result.errorMessage ?? "Flow did not complete");
    assert.equal(result.singleOutput(stringCodec), "local");
    let resumeToken = "";
    let source = "";
    for (let invocation = 1; invocation <= 4; invocation += 1) {
      const message = await client.readStream(id, flow.progress, resumeToken, 30_000);
      assert.equal(message.value, `local-stream-${invocation}`);
      source ||= message.source;
      assert.equal(message.source, source);
      resumeToken = message.resumeToken;
    }
  });
});

test("regular Step without progress frames reaches heartbeat timeout", async () => {
  const flow = new StepHeartbeatFlow();
  flow.scenario = "timeout";
  await withEnvironment([flow], async ({ client }) => {
    const id = flowId("typescript-heartbeat-timeout");
    await client.startFlow(flow, id, "timeout" satisfies HeartbeatScenario);
    assert.equal((await client.waitForFlow(id, 30_000)).status, "failed");
  });
});
