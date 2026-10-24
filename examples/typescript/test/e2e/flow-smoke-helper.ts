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

import { execFile, execFileSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { existsSync } from "node:fs";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";

export interface FlowSmokeFlags {
  readonly stepStartMayFail: boolean;
  readonly noStartStep: boolean;
}

export interface FlowSmokeEntry {
  readonly name: string;
  readonly trigger: (context: FlowSmokeContext) => Promise<FlowSmokeTriggerResult>;
  readonly flags: FlowSmokeFlags;
  /** Feature that makes server-zd reject this sample. */
  readonly serverZdUnsupportedFeature?: string;
}

export interface FlowSmokeContext {
  readonly baseUrl: string;
  readonly newFlowId: (prefix: string) => string;
}

export interface FlowSmokeTriggerResult {
  readonly flowId: string;
  readonly runId: string;
}

interface FlowHistoryEvent {
  readonly type: string;
  readonly payload: Record<string, unknown>;
}

interface FlowHistoryPage {
  readonly flowId: string;
  readonly runId: string;
  readonly events: FlowHistoryEvent[];
}

interface FlowStatePage {
  readonly flowStatus: string;
}

const execFileAsync = promisify(execFile);
const runIdPattern = /runId\s+(\S+)/;

export function defaultFlags(): FlowSmokeFlags {
  return { stepStartMayFail: false, noStartStep: false };
}

export function stepStartMayFailFlags(): FlowSmokeFlags {
  return { stepStartMayFail: true, noStartStep: false };
}

export function noStartStepFlags(): FlowSmokeFlags {
  return { stepStartMayFail: false, noStartStep: true };
}

export async function triggerGet(
  context: FlowSmokeContext,
  path: string,
  query: Record<string, string | number | boolean> = {},
): Promise<FlowSmokeTriggerResult> {
  const response = await httpRequest(context.baseUrl, "GET", path, query);
  return parseFlowTriggerResponse(
    response.text,
    String(query.workflowId ?? query.username ?? ""),
  );
}

export async function triggerPost(
  context: FlowSmokeContext,
  path: string,
  body: unknown,
  query: Record<string, string> = {},
): Promise<FlowSmokeTriggerResult> {
  const response = await httpRequest(context.baseUrl, "POST", path, query, body);
  return parseFlowTriggerResponse(response.text, String(query.workflowId ?? ""));
}

export function parseFlowTriggerResponse(
  body: string,
  workflowIdFromQuery: string,
): FlowSmokeTriggerResult {
  const trimmed = body.trim();
  try {
    const json = JSON.parse(trimmed) as {
      flowID?: string;
      runID?: string;
      flowId?: string;
      runId?: string;
    };
    if (json !== null && typeof json === "object" && !Array.isArray(json)) {
      const flowId = json.flowID ?? json.flowId ?? "";
      const runId = json.runID ?? json.runId ?? "";
      return { flowId, runId };
    }
  } catch {
    // plain-text controller response
  }
  const match = runIdPattern.exec(trimmed);
  if (match) {
    return { flowId: workflowIdFromQuery, runId: match[1]! };
  }
  if (trimmed.startsWith("Started workflowId: ")) {
    return { flowId: trimmed.slice("Started workflowId: ".length), runId: "" };
  }
  if (trimmed.startsWith("started workflowId: ")) {
    return { flowId: trimmed.slice("started workflowId: ".length), runId: "" };
  }
  if (workflowIdFromQuery) {
    return { flowId: workflowIdFromQuery, runId: "" };
  }
  return { flowId: "", runId: trimmed };
}

export async function assertFlowSmokeStartStep(
  entry: FlowSmokeEntry,
  flowId: string,
  runId: string,
): Promise<void> {
  if (entry.flags.noStartStep) {
    return;
  }
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    try {
      const history = await runDexcliFlowHistory(flowId, runId);
      const startStepType = flowStartedStartStepType(history.events);
      if (startStepType) {
        if (entry.flags.stepStartMayFail) {
          return;
        }
        if (hasStartStepProgress(history.events, startStepType)) {
          return;
        }
        const state = await runDexcliFlowState(flowId, runId);
        if (state.flowStatus === "FLOW_STATUS_RUNNING" && history.events.length > 1) {
          return;
        }
      }
    } catch (error) {
      if (!isRetryableDexcliError(error)) {
        throw error;
      }
    }
    await sleep(200);
  }
  throw new Error(`start step did not succeed for ${entry.name}`);
}

export async function assertFlowSmokeNoUnexpectedFailures(
  entry: FlowSmokeEntry,
  flowId: string,
  runId: string,
): Promise<void> {
  const history = await runDexcliFlowHistory(flowId, runId);
  for (const event of history.events) {
    switch (event.type) {
      case "StepExecuteFailed":
      case "StepWaitForFailed":
        if (!entry.flags.stepStartMayFail) {
          throw new Error(`unexpected failure event for ${entry.name}: ${event.type}`);
        }
        break;
      case "FlowClosed":
        if (isTerminalFlowClosedFailure(event.payload)) {
          if (entry.flags.stepStartMayFail) {
            continue;
          }
          throw new Error(
            `unexpected terminal flow closure for ${entry.name}: ${JSON.stringify(event.payload)}`,
          );
        }
        break;
      default:
        break;
    }
  }
}

function flowStartedStartStepType(events: FlowHistoryEvent[]): string {
  for (const event of events) {
    if (event.type !== "FlowStartedOrContinued") {
      continue;
    }
    const initialStart = event.payload.initialStart;
    if (typeof initialStart === "object" && initialStart !== null) {
      const startStepType = (initialStart as { startStepType?: string }).startStepType;
      if (startStepType) {
        return startStepType;
      }
    }
  }
  return "";
}

function hasStartStepProgress(events: FlowHistoryEvent[], startStepType: string): boolean {
  for (const event of events) {
    if (
      event.type !== "StepWaitForCompleted" &&
      event.type !== "StepExecuteCompleted" &&
      event.type !== "StepWaitForStarted" &&
      event.type !== "StepExecuteStarted"
    ) {
      continue;
    }
    if (historyEventStepType(event.payload) === startStepType) {
      return true;
    }
  }
  return false;
}

function historyEventStepType(payload: Record<string, unknown>): string {
  if (typeof payload.stepType === "string") {
    return payload.stepType;
  }
  const stepContext = payload.context;
  if (typeof stepContext === "object" && stepContext !== null) {
    const nested = (stepContext as { stepType?: string }).stepType;
    if (nested) {
      return nested;
    }
  }
  const input = payload.input;
  if (typeof input === "object" && input !== null) {
    const nested = (input as { stepType?: string }).stepType;
    if (nested) {
      return nested;
    }
  }
  return "";
}

function isTerminalFlowClosedFailure(payload: Record<string, unknown>): boolean {
  const status = payload.flowStatus;
  if (typeof status === "string") {
    return ![
      "FLOW_STATUS_COMPLETED",
      "FLOW_STATUS_CONTINUED_AS_NEW",
      "FLOW_STATUS_RUNNING",
      "FLOW_STATUS_UNSPECIFIED",
      "",
    ].includes(status);
  }
  if (typeof status === "number") {
    return status !== 0 && status !== 2 && status !== 7;
  }
  const errorType = payload.errorType;
  return typeof errorType === "string" && errorType !== "" && errorType !== "FLOW_ERROR_TYPE_UNSPECIFIED";
}

async function runDexcliFlowHistory(flowId: string, runId: string): Promise<FlowHistoryPage> {
  const args = [
    "flow",
    "history",
    flowId,
    "--server",
    flowServiceAddress(),
    "--output",
    "json",
    "--page-size",
    "50",
  ];
  if (runId) {
    args.push("--run-id", runId);
  }
  return runDexcliJson<FlowHistoryPage>(args);
}

async function runDexcliFlowState(flowId: string, runId: string): Promise<FlowStatePage> {
  const args = ["flow", "state", flowId, "--server", flowServiceAddress(), "--output", "json"];
  if (runId) {
    args.push("--run-id", runId);
  }
  return runDexcliJson<FlowStatePage>(args);
}

async function runDexcliJson<T>(args: string[]): Promise<T> {
  const { stdout, stderr } = await execFileAsync(dexcliPath(), args, {
    encoding: "utf8",
    timeout: 15_000,
  });
  const trimmed = stdout.trim();
  if (!trimmed) {
    throw new Error(`dexcli returned empty output: ${stderr}`);
  }
  try {
    return JSON.parse(trimmed) as T;
  } catch {
    throw new Error(`dexcli returned invalid JSON: ${trimmed.slice(0, 200)}`);
  }
}

function isRetryableDexcliError(error: unknown): boolean {
  if (error instanceof SyntaxError) {
    return true;
  }
  if (!(error instanceof Error)) {
    return false;
  }
  const code = (error as NodeJS.ErrnoException).code;
  if (code === "ETIMEDOUT") {
    return true;
  }
  return (
    error.message.includes("DeadlineExceeded") ||
    error.message.includes("deadline exceeded") ||
    error.message.includes("empty output") ||
    error.message.includes("invalid JSON")
  );
}

function flowServiceAddress(): string {
  return process.env.DEX_FLOW_SERVICE_ADDRESS ?? "127.0.0.1:8801";
}

let cachedDexcliPath: string | undefined;

function dexcliPath(): string {
  if (cachedDexcliPath) {
    return cachedDexcliPath;
  }
  const configured = process.env.DEXCLI_PATH?.trim();
  if (configured) {
    cachedDexcliPath = configured;
    return configured;
  }
  const repoRoot = findRepoRoot();
  const outputPath = join(mkdtempSync(join(tmpdir(), "dexcli-ts-smoke-")), "dexcli");
  execFileSync("go", ["build", "-trimpath", "-o", outputPath, "./cmd/dexcli"], {
    cwd: join(repoRoot, "cli"),
    env: { ...process.env, GOWORK: "off" },
    stdio: "pipe",
  });
  cachedDexcliPath = outputPath;
  return outputPath;
}

function findRepoRoot(): string {
  let directory = process.cwd();
  for (;;) {
    if (existsSync(join(directory, "cli", "cmd", "dexcli", "main.go"))) {
      return directory;
    }
    const parent = join(directory, "..");
    if (parent === directory) {
      throw new Error(`find repository root from ${process.cwd()}`);
    }
    directory = parent;
  }
}

async function httpRequest(
  baseUrl: string,
  method: string,
  path: string,
  query: Record<string, string | number | boolean>,
  body?: unknown,
): Promise<{ status: number; text: string }> {
  const url = new URL(path, baseUrl);
  for (const [key, value] of Object.entries(query)) {
    url.searchParams.set(key, String(value));
  }
  const response = await fetch(url, {
    method,
    headers: body === undefined ? undefined : { "content-type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await response.text();
  if (response.status < 200 || response.status >= 300) {
    throw new Error(`${method} ${path} returned ${response.status}: ${text}`);
  }
  return { status: response.status, text };
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

export function newFlowId(prefix: string): string {
  return `${prefix}-${randomUUID()}`;
}
