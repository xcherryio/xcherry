// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import { loadCompleteHistory } from '@/lib/history';
import { readResponseJSON } from '@/lib/http';
import type {
  ActiveStepExecution,
  FlowHistoryEvent,
  FlowState,
  FlowSummary,
  HistoryPage,
} from '@/lib/types';
import type { FdgConditionKind } from './model/fdg';
import type { PhaseStatus, RunCondition, RunOverlay, StepExecution } from './model/run';

const STEP_EVENT_TYPES = new Set([
  'StepWaitForCompleted',
  'StepWaitForFailed',
  'StepWaitForPending',
  'StepExecuteCompleted',
  'StepExecuteFailed',
  'StepExecutePending',
]);

export interface ExecutionRecord {
  execution: StepExecution;
  activeStep?: ActiveStepExecution;
  waitEvent?: FlowHistoryEvent;
  executeEvent?: FlowHistoryEvent;
  runId: string;
}

export interface RunOverlayBundle {
  overlay: RunOverlay;
  previousRunId: string;
  records: ExecutionRecord[];
}

export async function loadRunHistory(flowId: string, runId: string): Promise<FlowHistoryEvent[]> {
  const complete = await loadCompleteHistory(async (nextPageToken, startInternalEventId) => {
    const params = new URLSearchParams({
      flowId,
      runId,
      startInternalEventId: String(startInternalEventId),
      estimatePageSize: '200',
    });
    if (nextPageToken) params.set('nextPageToken', nextPageToken);
    return readResponseJSON<HistoryPage>(await fetch(`/api/flows/history?${params}`, { cache: 'no-store' }));
  });
  return complete.events;
}

export async function loadCurrentRun(flowId: string): Promise<{
  summary: FlowSummary;
  events: FlowHistoryEvent[];
  state: FlowState | null;
}> {
  const summary = await readResponseJSON<FlowSummary>(
    await fetch(`/api/flows/summary?flowId=${encodeURIComponent(flowId)}`, { cache: 'no-store' }),
  );
  const events = await loadRunHistory(flowId, summary.runId);
  if (summary.flowStatusCode !== 1) {
    return { summary, events, state: null };
  }
  try {
    const state = await readResponseJSON<FlowState>(
      await fetch(
        `/api/flows/state?flowId=${encodeURIComponent(flowId)}&runId=${encodeURIComponent(summary.runId)}`,
        { cache: 'no-store' },
      ),
    );
    return { summary, events, state };
  } catch {
    return { summary, events, state: null };
  }
}

export function previousRunIdFromEvents(events: FlowHistoryEvent[]): string {
  const started = events.find((event) => event.type === 'FlowStartedOrContinued');
  return stringField(asData(started?.payload.continuedStart).previousRunId);
}

export function overlayFromHistory({
  flowId,
  runId,
  status,
  events,
  activeSteps = [],
  now = Date.now(),
}: {
  flowId: string;
  runId: string;
  status: string;
  events: FlowHistoryEvent[];
  activeSteps?: ActiveStepExecution[];
  now?: number;
}): RunOverlayBundle {
  const records = new Map<string, ExecutionRecord>();
  const order: string[] = [];

  for (const event of events) {
    if (!STEP_EVENT_TYPES.has(event.type)) continue;
    const context = asData(event.payload.context);
    const stepExecutionId = stringField(context.stepExecutionId);
    if (!stepExecutionId) continue;
    let record = records.get(stepExecutionId);
    if (!record) {
      record = {
        runId,
        execution: {
          stepExecutionId,
          stepType: stringField(context.stepType) || stepExecutionId,
          ordinal: order.length + 1,
          fromStepExecutionId: stringField(context.fromStepExecutionId) || undefined,
          waitFor: null,
          execute: { status: 'notStarted' },
          attempts: 1,
          startedAt: epochMs(context.startedTime) ?? epochMs(event.eventTime),
        },
      };
      records.set(stepExecutionId, record);
      order.push(stepExecutionId);
    }
    applyEvent(record, event);
  }

  for (const active of activeSteps) {
    let record = records.get(active.stepExecutionId);
    if (!record) {
      record = {
        runId,
        execution: {
          stepExecutionId: active.stepExecutionId,
          stepType: active.stepType || active.stepExecutionId,
          ordinal: order.length + 1,
          fromStepExecutionId: active.fromStepExecutionId || undefined,
          waitFor: null,
          execute: { status: 'notStarted' },
          attempts: 1,
        },
      };
      records.set(active.stepExecutionId, record);
      order.push(active.stepExecutionId);
    }
    applyActive(record, active);
  }

  const executions = order.map((id, index) => {
    const record = records.get(id)!;
    record.execution.ordinal = index + 1;
    return record.execution;
  });

  return {
    previousRunId: previousRunIdFromEvents(events),
    records: order.map((id) => records.get(id)!),
    overlay: {
      runId,
      flowId,
      status,
      executions,
      simulated: false,
      note: runId,
      now,
    },
  };
}

export function prependStepRecords(
  bundle: RunOverlayBundle,
  older: ExecutionRecord[],
  stepType: string,
): RunOverlayBundle {
  if (older.length === 0) return bundle;
  const newer = bundle.records.filter((record) => record.execution.stepType === stepType);
  const others = bundle.records.filter((record) => record.execution.stepType !== stepType);
  const merged = [...older, ...newer].map((record, index) => ({
    ...record,
    execution: { ...record.execution, ordinal: index + 1 },
  }));
  const records = [...others, ...merged];
  return {
    ...bundle,
    records,
    overlay: {
      ...bundle.overlay,
      executions: records
        .map((record) => record.execution)
        .sort((left, right) => left.ordinal - right.ordinal),
    },
  };
}

export function mergeStepExecutions(
  current: RunOverlayBundle,
  previous: RunOverlayBundle,
  stepType: string,
): RunOverlayBundle {
  return {
    ...prependStepRecords(
      current,
      previous.records.filter((record) => record.execution.stepType === stepType),
      stepType,
    ),
    previousRunId: previous.previousRunId,
  };
}

export function recordForExecution(
  bundle: RunOverlayBundle,
  stepType: string,
  executionId: string | null,
): ExecutionRecord | undefined {
  const mine = bundle.records.filter((record) => record.execution.stepType === stepType);
  return mine.find((record) => record.execution.stepExecutionId === executionId) ?? mine.at(-1);
}

export function methodEventFromRecord(
  record: ExecutionRecord | undefined,
): FlowHistoryEvent | undefined {
  return record?.executeEvent ?? record?.waitEvent;
}

export function payloadFromRecord(record: ExecutionRecord | undefined): {
  input: unknown;
  output: unknown;
  context: unknown;
} | null {
  const event = methodEventFromRecord(record);
  if (!event) return null;
  return {
    input: event.payload.input,
    output: event.payload.output,
    context: event.payload.context,
  };
}

/** Prefer executeEvent; Wait tabs can pass waitEvent explicitly. */
export function attemptCountFromRecord(record: ExecutionRecord | undefined): number {
  if (!record) return 0;
  const event = methodEventFromRecord(record);
  if (!event) return 0;
  const context = asData(event.payload.context);
  const fromFinal = numberField(context.finalAttempt);
  if (fromFinal > 0) return fromFinal;
  const fromLastFailure = numberField(asData(context.lastFailureInfo).attempt);
  if (fromLastFailure > 0) return fromLastFailure;
  const fromOutputFailure = numberField(asData(asData(event.payload.output).failure).attempt);
  if (fromOutputFailure > 0) return fromOutputFailure;
  return Math.max(1, record.execution.attempts);
}

function applyEvent(record: ExecutionRecord, event: FlowHistoryEvent): void {
  const context = asData(event.payload.context);
  const failed = event.type.endsWith('Failed');
  const pending = event.type.endsWith('Pending');
  const status: PhaseStatus = failed ? 'failed' : pending ? (event.type.startsWith('StepWaitFor') ? 'pending' : 'running') : 'completed';
  if (event.type.startsWith('StepWaitFor')) {
    record.waitEvent = event;
    record.execution.waitFor = {
      status,
      conditions: conditionsFromEvent(event),
    };
  }
  if (event.type.startsWith('StepExecute')) {
    record.executeEvent = event;
    record.execution.execute = { status };
    const decision = asData(asData(event.payload.output).stepDecision);
    const decisionType = decisionKindFromStepDecision(decision);
    if (decisionType) record.execution.decisionType = decisionType;
    const next = nextStepTypes(decision);
    if (next.length > 0) record.execution.nextStepTypes = next;
    if (failed) {
      record.execution.lastFailure = failureMessage(event.payload.output) ?? record.execution.lastFailure;
    }
  }
  const attempt = Math.max(
    numberField(context.finalAttempt),
    numberField(asData(context.lastFailureInfo).attempt),
    numberField(asData(asData(event.payload.output).failure).attempt),
  );
  if (attempt > 0) record.execution.attempts = Math.max(record.execution.attempts, attempt);
  const started = epochMs(context.startedTime);
  if (started !== undefined) record.execution.startedAt = started;
  if (!pending && (event.type.endsWith('Completed') || failed)) {
    record.execution.endedAt = epochMs(event.eventTime) ?? record.execution.endedAt;
  }
}

function applyActive(record: ExecutionRecord, active: ActiveStepExecution): void {
  record.activeStep = active;
  if (active.phase === 'Waiting') {
    record.execution.waitFor = {
      status: 'waiting',
      conditions: conditionsFromWaiting(active.waitingCondition),
    };
  } else if (record.execution.execute.status === 'notStarted') {
    record.execution.execute = { status: 'running' };
  }
  if (active.lastFailureInfo) {
    record.execution.lastFailure = failureMessage(active.lastFailureInfo) ?? record.execution.lastFailure;
    const attempt = numberField(asData(active.lastFailureInfo).attempt);
    if (attempt > 0) record.execution.attempts = Math.max(record.execution.attempts, attempt);
  }
}

function conditionsFromEvent(event: FlowHistoryEvent): RunCondition[] {
  const output = asData(event.payload.output);
  const waiting = asData(output.waitForCondition);
  const fromOutput = conditionsFromWaiting(waiting);
  if (fromOutput.length > 0) return fromOutput;
  if (event.type === 'StepWaitForPending') {
    return [{ kind: 'unknown', label: 'waiting', satisfied: false }];
  }
  return [];
}

function conditionsFromWaiting(value: unknown): RunCondition[] {
  const waiting = asData(value);
  const rows: RunCondition[] = [];
  pushConditions(rows, waiting.channelConditions, 'channel');
  pushConditions(rows, waiting.timerConditions, 'timer');
  pushConditions(rows, waiting.subFlowConditions, 'subflow');
  return rows;
}

function pushConditions(rows: RunCondition[], value: unknown, kind: FdgConditionKind): void {
  if (!Array.isArray(value)) return;
  for (const entry of value) {
    const data = asData(entry);
    rows.push({
      kind,
      label: stringField(data.channelName)
        || stringField(data.timerId)
        || stringField(data.subFlowId)
        || stringField(data.label)
        || kind,
      satisfied: data.satisfied === true || data.completed === true,
    });
  }
}

function nextStepTypes(decision: Record<string, unknown>): string[] {
  if (!Array.isArray(decision.nextSteps)) return [];
  return decision.nextSteps
    .map((step) => stringField(asData(step).stepType))
    .filter(Boolean);
}

/** Wire StepDecision has no `type`; derive the verb from nextSteps / closeDecision. */
function decisionKindFromStepDecision(decision: Record<string, unknown>): string {
  const legacy = stringField(decision.type);
  if (legacy) return legacy;
  const next = nextStepTypes(decision);
  const closeVerb = closeDecisionVerb(asData(decision.closeDecision).closeDecisionType);
  if (next.length > 0 && closeVerb) return `goTo · ${closeVerb}`;
  if (next.length > 0) return 'goTo';
  return closeVerb;
}

function closeDecisionVerb(value: unknown): string {
  if (value === 1 || value === 'CLOSE_DECISION_TYPE_FORCE_COMPLETE_ON_CHANNELS_EMPTY') {
    return 'forceCompleteIfChannelsEmpty';
  }
  if (value === 2 || value === 'CLOSE_DECISION_TYPE_GRACEFUL_COMPLETE') {
    return 'gracefulComplete';
  }
  if (value === 3 || value === 'CLOSE_DECISION_TYPE_FORCE_COMPLETE') {
    return 'forceComplete';
  }
  if (value === 4 || value === 'CLOSE_DECISION_TYPE_FORCE_FAIL') {
    return 'forceFail';
  }
  if (value === 5 || value === 'CLOSE_DECISION_TYPE_DEAD_END') {
    return 'deadEnd';
  }
  return '';
}

function failureMessage(value: unknown): string | undefined {
  const failure = asData(asData(value).failure);
  const direct = asData(value);
  return stringField(failure.message) || stringField(direct.message) || undefined;
}

function asData(value: unknown): Record<string, unknown> {
  return value && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : {};
}

function stringField(value: unknown): string {
  return typeof value === 'string' ? value : '';
}

function numberField(value: unknown): number {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value === 'string' && value.trim() !== '') {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : 0;
  }
  return 0;
}

function epochMs(value: unknown): number | undefined {
  if (typeof value !== 'string' || value === '') return undefined;
  const parsed = Date.parse(value);
  return Number.isNaN(parsed) ? undefined : parsed;
}
