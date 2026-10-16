// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import type { FlowDefinitionGraph } from '@superdurable/flow-definition-renderer';
import type { FlowV2Definition } from '@superdurable/flow-definition-renderer';

export type {
  FlowDefinitionDiagnostic,
  FlowDefinitionEdge,
  FlowDefinitionGraph,
  FlowDefinitionNode,
  SourceSpan,
} from '@superdurable/flow-definition-renderer';

export const FLOW_STATUS = {
  0: 'Unspecified',
  1: 'Running',
  2: 'Completed',
  3: 'Failed',
  4: 'Timed out',
  5: 'Terminated',
  6: 'Canceled',
  7: 'Continued as new',
} as const;

export type FlowStatus = (typeof FLOW_STATUS)[keyof typeof FLOW_STATUS];

export interface KeyValue {
  key: string;
  value: unknown;
}

export interface FlowExecution {
  flowId: string;
  runId: string;
  flowType: string;
  flowStatus: FlowStatus;
  flowStatusCode: number;
  startTime: string | null;
  closeTime: string | null;
  indexedAttributes: KeyValue[];
}

export interface SearchFlowsResult {
  flows: FlowExecution[];
  nextPageToken: string;
}

export interface FlowSummary {
  flowId: string;
  runId: string;
  firstRunId: string;
  requestId: string;
  flowType: string;
  flowStatus: FlowStatus;
  flowStatusCode: number;
  startTime: string | null;
  closeTime: string | null;
}

export type HistoryEventType =
  | 'FlowStartedOrContinued'
  | 'FlowClosed'
  | 'StepWaitForCompleted'
  | 'StepWaitForFailed'
  | 'StepWaitForPending'
  | 'StepExecuteCompleted'
  | 'StepExecuteFailed'
  | 'StepExecutePending'
  | 'RpcExecutionCompleted'
  | 'ChannelExternalPublish'
  | 'TimeTravelFork';

export interface FlowHistoryEvent {
  eventId: number;
  eventTime: string | null;
  type: HistoryEventType;
  payload: Record<string, unknown>;
}

export interface HistoryPage {
  events: FlowHistoryEvent[];
  nextPageToken: string;
  nextInternalEventId: number;
}

export interface StreamMessage {
  value: unknown;
  resumeToken: string;
  createdTime: string | null;
  source: string;
}

export interface ActiveStepExecution {
  stepExecutionId: string;
  fromStepExecutionId: string;
  stepType: string;
  phase: 'Active' | 'Waiting' | 'Unspecified';
  movement?: Record<string, unknown>;
  waitingCondition?: Record<string, unknown>;
  completedConditions?: Record<string, unknown>;
  stepExecutionLocals: KeyValue[];
  timers: Record<string, unknown>[];
  lastFailureInfo?: Record<string, unknown>;
}

export interface FlowState {
  flowConfig: Record<string, unknown>;
  attributes: KeyValue[];
  activeStepExecutions: ActiveStepExecution[];
  queuedSteps: Record<string, unknown>[];
  pendingChannelMessages: Record<string, unknown>;
  completedSteps: Record<string, unknown>[];
}

export interface StepGraphNode {
  id: string;
  label: string;
  kind: 'source' | 'step' | 'subflow';
  status: 'Source' | 'Active' | 'Waiting' | 'Pending' | 'Completed' | 'Failed' | 'Canceled';
  previousRunId?: string;
  stepType?: string;
  fromStepExecutionId?: string;
  movement?: Record<string, unknown>;
  isPlanned?: boolean;
  waitFor?: FlowHistoryEvent;
  execute?: FlowHistoryEvent;
  pendingWaitFor?: FlowHistoryEvent;
  pendingExecute?: FlowHistoryEvent;
  active?: ActiveStepExecution;
  parentStepId?: string;
  flowId?: string;
  subFlowStatus?: string;
  reusePolicy?: string;
}

export interface StepGraphEdge {
  id: string;
  source: string;
  target: string;
}

export interface FlowDefinitionEntry {
  id: string;
  file: string;
  schemaVersion: '1.0' | '2.0';
  flowName: string;
  sourceLanguage: string;
  sourcePath: string;
  valid: boolean;
  graph: FlowDefinitionGraph;
}

export interface V2CatalogEntry {
  flowType: string;
  definition: FlowV2Definition;
}

export interface V2Catalog {
  enabled: boolean;
  flows: V2CatalogEntry[];
  definitionRevision: string;
  startFlowWorkerTargetPull?: boolean;
}

export interface V2Flow {
  flowId: string;
  flowType: string;
  flowStatus: FlowStatus;
  flowStatusCode: number;
  startTime: string | null;
  closeTime: string | null;
  indexedAttributes: Record<string, unknown>;
  summary?: Record<string, unknown>;
  summaryError?: string;
}

export interface V2SearchResult {
  flows: V2Flow[];
  nextPageToken: string;
}

export interface V2Display {
  flowId: string;
  flowType: string;
  flowStatus: FlowStatus;
  flowStatusCode: number;
  isActive: boolean;
  display: Record<string, unknown>;
  attributeSnapshot: Record<string, unknown>;
  eligibleActions: string[];
}

export interface FlowDefinitionCatalog {
  configured: boolean;
  directory?: string;
  source?: string;
  definitionRevision?: string;
  definitionCount: number;
  definitions: FlowDefinitionEntry[];
}
