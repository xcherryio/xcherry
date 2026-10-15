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
import { mkdtempSync, rmSync } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import {
  Client,
  Registry,
  StepList,
  SubFlow,
  Wait,
  Worker,
  goTo,
  gracefulComplete,
  openBlobCache,
  rpc,
  stringCodec,
  type BlobCache,
  type Context,
  type Flow,
  type RPCResult,
  type Step,
  type StepDecision,
} from "../../src/index.js";
import { flowId } from "./environment.js";

interface Observation {
  readonly workerID: string;
  readonly method: string;
}

class FirstPullStep implements Step<string> {
  public readonly inputCodec = stringCodec;

  public constructor(
    private readonly workerID: string,
    private readonly observations: Observation[],
    private readonly firstGate: Promise<void>,
  ) {}

  public getStepType(): string { return "FirstPullStep"; }

  public waitFor(): Wait {
    this.observations.push({ workerID: this.workerID, method: "WaitFor" });
    return Wait.skipImmediately();
  }

  public async execute(_context: Context, input: string): Promise<StepDecision> {
    this.observations.push({ workerID: this.workerID, method: "Execute" });
    await this.firstGate;
    return goTo(SecondPullStep, input);
  }
}

class SecondPullStep implements Step<string> {
  public readonly inputCodec = stringCodec;

  public constructor(
    private readonly workerID: string,
    private readonly observations: Observation[],
    private readonly secondGate: Promise<void>,
  ) {}

  public getStepType(): string { return "SecondPullStep"; }

  public waitFor(): Wait {
    this.observations.push({ workerID: this.workerID, method: "SecondWaitFor" });
    return Wait.skipImmediately();
  }

  public async execute(_context: Context, input: string): Promise<StepDecision> {
    this.observations.push({ workerID: this.workerID, method: "SecondExecute" });
    await this.secondGate;
    return gracefulComplete(input);
  }
}

class PullFlow implements Flow<string> {
  private readonly first: FirstPullStep;
  private readonly second: SecondPullStep;

  public constructor(
    private readonly workerID: string,
    observations: Observation[],
    firstGate: Promise<void>,
    secondGate: Promise<void>,
  ) {
    this.first = new FirstPullStep(workerID, observations, firstGate);
    this.second = new SecondPullStep(workerID, observations, secondGate);
  }

  public getFlowType(): string { return "PullFlow"; }
  public getSteps() { return StepList.startStep(this.first).otherSteps(this.second); }

  @rpc({ outputCodec: stringCodec })
  public getWorker(_context: Context): RPCResult<string> {
    return { output: this.workerID };
  }
}

class ProbeStep implements Step<string> {
  public readonly inputCodec = stringCodec;
  public constructor(private readonly workerID: string) {}
  public getStepType(): string { return "ProbeStep"; }
  public execute(_context: Context, _input: string): StepDecision { return gracefulComplete(this.workerID); }
}

class ProbeFlow implements Flow<string> {
  private readonly start: ProbeStep;
  public constructor(workerID: string) { this.start = new ProbeStep(workerID); }
  public getFlowType(): string { return "ProbeFlow"; }
  public getSteps() { return StepList.startStep(this.start); }
}

class PushStep implements Step<string> {
  public readonly inputCodec = stringCodec;
  public getStepType(): string { return "PushStep"; }
  public execute(_context: Context, input: string): StepDecision { return gracefulComplete(input); }
}

class PushFlow implements Flow<string> {
  private readonly start = new PushStep();
  public getFlowType(): string { return "PushFlow"; }
  public getSteps() { return StepList.startStep(this.start); }
}

class PullOverrideParentStep implements Step<string> {
  public readonly inputCodec = stringCodec;
  public constructor(private readonly child: ProbeFlow) {}
  public getStepType(): string { return "PullOverrideParentStep"; }
  public waitFor(_context: Context, input: string): Wait {
    return Wait.until(SubFlow.run(this.child, input, {
      configOverride: { workerTarget: { pull: true } },
    }));
  }
  public execute(context: Context, _input: string): StepDecision {
    return gracefulComplete(SubFlow.getConditionResults(context).singleOutput(stringCodec));
  }
}

class PullOverrideParentFlow implements Flow<string> {
  private readonly start: PullOverrideParentStep;
  public constructor(child: ProbeFlow) { this.start = new PullOverrideParentStep(child); }
  public getFlowType(): string { return "PullOverrideParentFlow"; }
  public getSteps() { return StepList.startStep(this.start); }
}

test("pull Worker executes Steps and RPC with sticky routing, drains Stop, and coexists with push", async () => {
  const serverAddress = process.env.DEX_SERVER_ADDRESS ?? "127.0.0.1:8801";
  const cacheDirectory = mkdtempSync(join(tmpdir(), "dex-typescript-pull-cache-"));
  const cache: BlobCache = openBlobCache({ directory: cacheDirectory, maxBytes: 64 * 1_024 * 1_024 });
  const observations: Observation[] = [];
  let releaseFirst!: () => void;
  let releaseSecond!: () => void;
  const firstGate = new Promise<void>((resolve) => { releaseFirst = resolve; });
  const secondGate = new Promise<void>((resolve) => { releaseSecond = resolve; });
  const flowA = new PullFlow("pull-a", observations, firstGate, secondGate);
  const flowB = new PullFlow("pull-b", observations, firstGate, secondGate);
  const probe = new ProbeFlow("pull-b");
  const pushProbe = new ProbeFlow("push");
  const pushFlow = new PushFlow();
  const parentFlow = new PullOverrideParentFlow(pushProbe);
  const workerA = new Worker(new Registry([flowA]), cache, {
    serverAddress, pull: { appWorkerID: `typescript-a-${flowId("worker")}`, maxConcurrentTasks: 4 },
  });
  const workerB = new Worker(new Registry([flowB, probe]), cache, {
    serverAddress, pull: { appWorkerID: `typescript-b-${flowId("worker")}`, maxConcurrentTasks: 4 },
  });
  const pushWorker = new Worker(new Registry([pushFlow, pushProbe, parentFlow]), cache, {
    serverAddress, bindAddress: `127.0.0.1:${await availablePort()}`,
  });
  const client = new Client(new Registry([flowA, probe, pushFlow, parentFlow]), cache, {
    serverAddress, workerTarget: workerA.workerTarget,
  });
  try {
    await pushWorker.start();
    await workerA.start();
    const id = flowId("pull-typescript");
    await client.startFlow(flowA, id, "completed");
    await until(() => observations.some((entry) => entry.method === "Execute"));
    assert.deepEqual(observations.slice(0, 2), [
      { workerID: "pull-a", method: "WaitFor" },
      { workerID: "pull-a", method: "Execute" },
    ]);

    await workerB.start();
    assert.equal(await runProbe(client, probe), "pull-b");
    const subFlowId = flowId("pull-subflow-override");
    await client.startFlow(parentFlow, subFlowId, "child", {
      configOverride: { workerTarget: pushWorker.workerTarget },
    });
    assert.equal(
      await client.waitForFlow(subFlowId, 30_000).then((result) => result.singleOutput(stringCodec)),
      "pull-b",
    );
    releaseFirst();
    await until(() => observations.some((entry) => entry.method === "SecondExecute"));
    assert.deepEqual(observations.slice(2, 4), [
      { workerID: "pull-a", method: "SecondWaitFor" },
      { workerID: "pull-a", method: "SecondExecute" },
    ]);
    assert.equal(await client.invokeRPC(flowA.getWorker, id), "pull-a");

    let hasStopped = false;
    const stopping = workerA.close().then(() => { hasStopped = true; });
    assert.equal(await runProbe(client, probe), "pull-b");
    assert.equal(hasStopped, false);
    releaseSecond();
    await stopping;
    assert.equal(
      await client.waitForFlow(id, 30_000).then((result) => result.singleOutput(stringCodec)),
      "completed",
    );
    assert.deepEqual(observations.filter((entry) => entry.method === "SecondExecute"), [
      { workerID: "pull-a", method: "SecondExecute" },
    ]);

    const pushId = flowId("push-alongside-pull");
    await client.startFlow(pushFlow, pushId, "push", {
      configOverride: { workerTarget: pushWorker.workerTarget },
    });
    assert.equal(
      await client.waitForFlow(pushId, 30_000).then((result) => result.singleOutput(stringCodec)),
      "push",
    );
  } finally {
    releaseFirst();
    releaseSecond();
    await client.close();
    await workerA.close();
    await workerB.close();
    await pushWorker.close();
    cache.close();
    rmSync(cacheDirectory, { recursive: true, force: true });
  }
});

async function runProbe(client: Client, probe: ProbeFlow): Promise<string> {
  const id = flowId("pull-probe");
  await client.startFlow(probe, id, "pull-b");
  return client.waitForFlow(id, 30_000).then((result) => result.singleOutput(stringCodec));
}

async function availablePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (address === null || typeof address === "string") {
        server.close();
        reject(new Error("failed to allocate a Worker port"));
        return;
      }
      server.close((error) => error === undefined ? resolve(address.port) : reject(error));
    });
  });
}

async function until(condition: () => boolean): Promise<void> {
  const deadline = Date.now() + 30_000;
  while (!condition()) {
    if (Date.now() >= deadline) throw new Error("timed out waiting for pull Worker observation");
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}
