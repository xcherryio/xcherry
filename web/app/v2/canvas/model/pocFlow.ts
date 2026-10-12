// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

/**
 * The POC domain model: a Dex Flow as something renderable, derived once from the wire.
 *
 * Every view function consumes this and nothing else. No view reads FDG JSON, and no
 * view invents a fact — everything below is either verbatim from the wire or listed in
 * the derivation register (see `decode.ts`), which holds one rule: a derivation is
 * legal only if it is a pure function of the wire.
 */

import type { AgentRole } from './agentic'
import type { FdgConditionKind, FdgDiagnostic, SourceSpan } from './fdg'

/**
 * Who has to act before this Step can move.
 *
 * Derived from the Step's wait-condition kinds, which form a COMPLETE partition of
 * what can block a Step in Dex — a Channel (an actor outside the Flow must publish),
 * a SubFlow (a child must finish), a Timer (time must pass), or nothing at all. So
 * this is total and never a guess. It is the axis view B is built on.
 *
 * `unknown` is the fifth value and it is not a Dex concept — it means the Step declares
 * a WaitFor whose conditions the analyser could not resolve. dexcli's Go analyser emits
 * exactly this: a `wait` node with `conditions: []`. Collapsing that case into `machine`
 * would call a human gate "machine work", which is a silent misread; a visible gap is
 * strictly better than a confident wrong answer.
 */
export type Actor = 'external' | 'child' | 'clock' | 'machine' | 'unknown'

export interface Condition {
  kind: FdgConditionKind
  /** Verbatim from the wire minus the `.for N` suffix. Identifiers are never paraphrased. */
  label: string
  resourceId?: string
  subFlowId?: string
  expression?: string
}

export interface WaitPhase {
  /** 'until' | 'anyOf' | 'allOf' | 'anyComboOf' | 'skipWaitImmediately' | 'failure' | … */
  type: string
  conditions: Condition[]
  /** One prose line. Framework vocabulary paraphrased, identifiers verbatim. */
  sentence: string
}

export interface Branch {
  /** Grouped: near-identical decisions share one branch. */
  decisionTypes: string[]
  /** The last `and` clause, double negation undone. Null when unconditional. */
  guard: string | null
  /** Every guard that merged into this branch, unsimplified. */
  fullGuards: string[]
  targets: { stepId: string; multiplicity?: string }[]
  closes?: 'complete' | 'fail' | 'deadEnd' | 'completeIfChannelsEmpty'
  cancels?: { stepId: string; scope: 'all' | 'siblings' }[]
  checkedChannels?: string[]
}

export interface ExecutePhase {
  branches: Branch[]
}

export interface ResourceRef {
  resourceId: string
  access: 'read' | 'write' | 'publish' | 'lock'
  label: string
  /** Which phase touches it, from `edge.metadata.phase`. */
  phase?: string
}

/** See `StepModel.recoveryRole`. Derived from the failure relation, never declared. */
export type RecoveryRole = 'none' | 'fallback' | 'sink' | 'dispatcher'

export interface StepModel {
  id: string
  /** The DURABLE identifier. A display label may sit in front of it but never replaces it. */
  stepType: string
  label: string
  isStart: boolean
  actor: Actor
  /**
   * A CONTROL hub: many ordinary transitions arrive here. This is the thing that turns a
   * readable column into hub-and-spoke — superagent's `CheckSteered` takes 17 of its
   * flow's 27 edges.
   */
  isHub: boolean
  /**
   * A RECOVERY hub: many failure edges converge here. Deliberately a different flag,
   * because it is not the same fact. One compensation Step collecting every failure in a
   * saga is correct, expected structure, and marking it as noise would misread the flow.
   */
  isRecoveryHub: boolean
  /**
   * WHICH KIND of failure target this Step is, if any.
   *
   * Dex has NO recovery-step type. There is no class, marker or public flag saying "this Step is for
   * recovery". The whole primitive is one private, per-Step declaration —
   * `StepOptions.on_execute_failure_proceed_to(step)`, stored as `_execute_failure_target` — which
   * routes a Step's own exhausted `execute` retries to exactly ONE other Step. The Wait phase cannot
   * route at all: `WaitForFailurePolicy` offers only FAIL_FLOW or PROCEED.
   *
   * So a Step becomes a recovery Step ONLY by being named by others. Failure fan-OUT is always 1 by
   * construction, and every bit of multiplicity is on the receiving side — which is exactly why these
   * Steps "connect to lots of things" and why the role is an in-degree property rather than a type.
   *
   * The corpus populates three roles, and they want different drawings:
   *
   *   fallback   — failure-in 1.        One Step's Plan B. order-processing's RefundStep.
   *   sink       — failure-in >= 2, no control out. Catches many and stops. money-transfer's
   *                CompensateStep, which is ordinary saga structure and not a defect.
   *   dispatcher — failure-in >= 2, control out >= 1. Catches many AND resumes the flow. The book
   *                pipeline's RecoveryGate: 11 in, 11 out to twelve different Steps.
   *
   * Only the dispatcher has the outbound explosion, and only it needs its outbound quietened.
   */
  recoveryRole: RecoveryRole
  /**
   * True when EVERY Step that could route its exhausted retries here does.
   *
   * Then the failure edges are a POLICY, not a topology. `on_execute_failure_proceed_to(RecoveryGate)`
   * is applied to 11 of the book pipeline's 12 Steps — all 11 eligible ones — so eleven edges express
   * one fact eleven times and none of them discriminates between Steps. Said once on the card it is
   * louder than eleven thin lines, and the lines are still one click away.
   *
   * Floored at 5 as well as shared, the same shape as `isHub`: money-transfer's CompensateStep catches
   * 4 of 5 eligible, which is just as uniform but only four edges and perfectly legible drawn.
   */
  hasUniformFailurePolicy: boolean
  /** Ordinary inbound transitions only. Failure edges are counted separately. */
  inboundCount: number
  inboundFailureCount: number
  /**
   * WHAT THIS STEP IS FOR in an agentic flow, or undefined in an ordinary one.
   *
   * Read from `node.metadata.agentRole`, never derived. A decision Step and a capability Step are both
   * `kind: 'step'` with outgoing transitions, so nothing in the graph distinguishes them — the difference
   * is the author's intent, and inferring it (say, from out-degree) would mislabel any ordinary flow with
   * a wide fan-out. Undefined therefore means "this flow is not agentic", not "role unknown".
   */
  agentRole?: AgentRole
  /** Null when the Step has no WaitFor at all — `phase === 'execute'`. Absent, not empty. */
  waitFor: WaitPhase | null
  execute: ExecutePhase
  resources: ResourceRef[]
  span?: SourceSpan
  /** True when dexcli identified this Step as a Connector SDK factory Step. */
  isConnectorStep: boolean
  /** Static Connector identity emitted by Flow Definition Graph 2.0. */
  connector?: {
    connectorId: string
    operationId: string
    operationKind: string
    connectionName: string
    moduleVersion: string
  }
  /** One-sentence Step purpose from `dex:explanation`, when the FDG carries it. */
  explanation?: string
}

export interface EntryModel {
  id: string
  kind: 'rpc' | 'timeoutHandler'
  name: string
  /** RPC handlers emit `rpcResult` decisions, which may also move the Flow. */
  branches: Branch[]
  resources: ResourceRef[]
  /**
   * Channels this entry publishes to, joined onward to the Steps that wait on them.
   *
   * The wire encodes this NOWHERE as a single edge — every RPC is graph-isolated. It
   * is reconstructed by joining `rpc -publish-> channel` with `channel -condition-> wait`.
   * This is the human-input path, so a UI that does not join it cannot show it.
   */
  opensGates: { channelId: string; stepIds: string[] }[]
  /** The Flow v2 Action this RPC implements, when the definition declares one. */
  action?: { label: string; requiredPermission: string }
  /** The Flow v2 views this RPC serves. Empty for every other entry. */
  views: EntryView[]
  span?: SourceSpan
}

export type EntryView = 'summary' | 'display'

export interface ResourceModel {
  id: string
  kind: 'attribute' | 'channel' | 'stream'
  name: string
  valueType: string
  isMap: boolean
  /** Steps and entries that touch it, by access kind. */
  touchedBy: ResourceRef[]
}

export interface SubFlowRef {
  id: string
  flowType: string
  external: boolean
  startedByStepId?: string
}

export interface Transition {
  id: string
  fromStepId: string
  toStepId: string
  kind: 'transition' | 'failure_transition'
  guard: string | null
  /** Parallel transitions merge; this holds every guard that collapsed into one line. */
  mergedGuards: string[]
  multiplicity?: string
  isSelfLoop: boolean
}

export interface PocFlow {
  flowType: string
  valid: boolean
  source: { language: string; path: string }
  startStepId?: string
  steps: StepModel[]
  entries: EntryModel[]
  resources: ResourceModel[]
  subflows: SubFlowRef[]
  transitions: Transition[]
  diagnostics: FdgDiagnostic[]
  /** Honest provenance, shown in the UI. */
  provenance: {
    generated: boolean
    note: string
  }
  /** Anything the projection had to drop, so the UI can say so rather than look complete. */
  dropped: string[]
}

export const ACTOR_LABEL: Record<Actor, string> = {
  external: 'You / external',
  unknown: 'Waits · unresolved',
  machine: 'Machine',
  clock: 'Clock',
  child: 'Child flow',
}

/**
 * Reading order for lanes and legends: who a human cares about first.
 *
 * `unknown` sits second on purpose. It is the most likely place a human gate is hiding,
 * so burying it next to the machine work would hide the gap it exists to expose.
 */
export const ACTOR_ORDER: Actor[] = ['external', 'unknown', 'machine', 'clock', 'child']

export function stepById(flow: PocFlow, id: string): StepModel | undefined {
  return flow.steps.find((s) => s.id === id)
}

export function resourceById(flow: PocFlow, id: string): ResourceModel | undefined {
  return flow.resources.find((r) => r.id === id)
}

/** Who acts at a Step; an unresolved wait stays a wait, never machine work. */
export type WhoActs = 'person' | 'outside' | 'machine' | 'clock' | 'child' | 'waits'

/** The entries that publish to a Channel this Step waits on. */
export function answeringEntries(flow: PocFlow, step: StepModel): EntryModel[] {
  const channels = new Set(
    (step.waitFor?.conditions ?? []).flatMap((c) =>
      c.kind === 'channel' && c.resourceId !== undefined ? [c.resourceId] : [],
    ),
  )
  return flow.entries.filter((entry) => entry.opensGates.some((gate) => channels.has(gate.channelId)))
}

export function whoActs(flow: PocFlow, step: StepModel): WhoActs {
  switch (step.actor) {
    case 'external':
      return answeringEntries(flow, step).some((entry) => entry.action !== undefined) ? 'person' : 'outside'
    case 'clock':
      return 'clock'
    case 'child':
      return 'child'
    case 'machine':
      return 'machine'
    case 'unknown':
      return 'waits'
  }
}
