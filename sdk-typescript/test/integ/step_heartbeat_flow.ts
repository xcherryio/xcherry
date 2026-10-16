// Portions of this file are derived from indeedeng/iwf-java-sdk.
// Those portions are licensed under the Apache License, Version 2.0.
// See LICENSES/Apache-2.0.txt and LEGACY_NOTICES.md.
//
// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications are licensed under the Sustainable Use License 1.0.
// Third-Party Materials remain under the Apache License, Version 2.0.
// See LICENSE and LEGACY_NOTICES.md.

import {
  StepList,
  Stream,
  gracefulComplete,
  stringCodec,
  type AsyncContext,
  type Codec,
  type Flow,
  type PersistenceSchema,
  type Step,
  type StepDecision,
  type StepOptions,
} from "../../src/index.js";

export type HeartbeatScenario = "value" | "clear" | "null" | "stream" | "local" | "timeout";

class StepHeartbeatStep implements Step<HeartbeatScenario> {
  public readonly inputCodec = stringCodec as Codec<HeartbeatScenario>;

  public constructor(private readonly flow: StepHeartbeatFlow) {}

  public getStepType(): string {
    return "TypeScriptStepHeartbeat";
  }

  public getStepOptions(): StepOptions {
    return {
      heartbeatTimeoutMs: 10_000,
      executeDurability: this.flow.scenario === "local" ? "async" : "sync",
      ...(this.flow.scenario === "timeout" ? { executeMethodTimeoutMs: 20_000 } : {}),
      executeRetry: {
        initialIntervalMs: 1_000,
        maximumAttempts: this.flow.scenario === "local"
          ? 4
          : this.flow.scenario === "timeout"
            ? 1
            : 2,
        totalDurationMs: 30_000,
      },
    };
  }

  public async execute(
    context: AsyncContext,
    scenario: HeartbeatScenario,
  ): Promise<StepDecision> {
    if (scenario === "local") {
      return this.executeLocal(context);
    }
    if (scenario === "timeout") {
      await new Promise((resolve) => setTimeout(resolve, 12_000));
      return gracefulComplete("unexpected");
    }
    if (context.attempt === 1) {
      await this.recordFirstAttempt(context, scenario);
      throw new Error(`retry ${scenario} after heartbeat`);
    }
    this.verifyRestoredHeartbeat(context, scenario);
    return gracefulComplete(scenario);
  }

  private async executeLocal(context: AsyncContext): Promise<StepDecision> {
    const invocation = (this.flow.localInvocations.get(context.flowId) ?? 0) + 1;
    this.flow.localInvocations.set(context.flowId, invocation);
    await context.recordHeartbeat(`local-${invocation}`, stringCodec);
    this.flow.progress.write(context, `local-stream-${invocation}`);
    if (process.env.DEX_INTEG_WORKER_MODE === "pull" && invocation > 2) {
      if (context.getLastHeartbeatValue(stringCodec) !== `local-${invocation - 1}`) {
        throw new Error("regular retry did not restore its previous heartbeat");
      }
    } else if (context.hasLastHeartbeatValue()) {
      throw new Error("local heartbeat leaked into retry");
    }
    if (invocation < 4) {
      throw new Error(`local attempt ${invocation}`);
    }
    return gracefulComplete("local");
  }

  private async recordFirstAttempt(
    context: AsyncContext,
    scenario: Exclude<HeartbeatScenario, "local" | "timeout">,
  ): Promise<void> {
    if (scenario === "clear") {
      await context.recordHeartbeat("discarded", stringCodec);
      await context.recordHeartbeat(undefined);
      return;
    }
    if (scenario === "null") {
      await context.recordHeartbeat(null);
      return;
    }
    const value = scenario === "stream" ? "stream-heartbeat" : "restored-heartbeat";
    await context.recordHeartbeat(value, stringCodec);
    if (scenario === "stream") {
      this.flow.progress.write(context, "stream-after-heartbeat-1");
      this.flow.progress.write(context, "stream-after-heartbeat-2");
    }
  }

  private verifyRestoredHeartbeat(
    context: AsyncContext,
    scenario: Exclude<HeartbeatScenario, "local" | "timeout">,
  ): void {
    if (scenario === "clear") {
      if (context.hasLastHeartbeatValue()) {
        throw new Error("explicit undefined heartbeat was not cleared");
      }
      return;
    }
    if (!context.hasLastHeartbeatValue()) {
      throw new Error(`${scenario} heartbeat Value was not restored`);
    }
    if (scenario === "null") {
      if (context.getLastHeartbeatValue() !== null) {
        throw new Error("null heartbeat decoded unexpectedly");
      }
      return;
    }
    const expected = scenario === "stream" ? "stream-heartbeat" : "restored-heartbeat";
    if (context.getLastHeartbeatValue(stringCodec) !== expected) {
      throw new Error(`${scenario} heartbeat Value changed during retry`);
    }
  }
}

export class StepHeartbeatFlow implements Flow<HeartbeatScenario> {
  public readonly progress = new Stream("typescript-heartbeat-progress", stringCodec, 1 << 20);
  public readonly localInvocations = new Map<string, number>();
  public scenario: HeartbeatScenario = "value";
  private readonly start = new StepHeartbeatStep(this);

  public getFlowType(): string {
    return "TypeScriptStepHeartbeatFlow";
  }

  public getSteps(): StepList<HeartbeatScenario> {
    return StepList.startStep(this.start);
  }

  public getPersistenceSchema(): PersistenceSchema {
    return { streams: [this.progress] };
  }
}
