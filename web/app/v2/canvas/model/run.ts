// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import type { FdgConditionKind } from './fdg'

/**
 * Run state, as an overlay on a definition — never as a second graph.
 *
 * The whole point of the unified-view decision: the definition is keyed by **StepType**, a run by
 * **StepExecutionID**, and one StepType has 0..N executions. Airflow, Dify, Flowise and LangSmith all
 * converge on ONE node with a count rather than N nodes, and this model is shaped to make that the
 * easy thing: `runStateFor` rolls every execution of a StepType into one card's worth of facts.
 *
 * Kept in a separate file from `PocFlow` on purpose. Definition mode is the ABSENCE of an overlay,
 * not a run with every status nulled — a definition renders every possible path, and a graph where
 * every node reads "pending" would be a lie about what the artifact is.
 */

/**
 * Phase status.
 *
 * NOT A VERIFIED CONTRACT. The Dex docs never enumerate a status enum — not in the glossary, the Flow
 * pages, the Client pages or the CLI reference. This set is normalised from Dex Web's own run view
 * (which distinguishes 7 node statuses from 8 per-method statuses) and is listed as an open question
 * in docs/BACKEND_CONTRACT.md §3.3. An unrecognised value from a real backend must render as
 * "unknown", never crash.
 */
export const PHASE_STATUSES = [
  'notStarted',
  'pending',
  'waiting',
  'running',
  'completed',
  'failed',
  'canceled',
] as const

export type PhaseStatus = (typeof PHASE_STATUSES)[number]

/** Human-facing text. Status is TEXT on the card, not colour alone. */
export const PHASE_LABEL: Record<PhaseStatus, string> = {
  notStarted: 'not started',
  pending: 'pending',
  waiting: 'waiting',
  running: 'running',
  completed: 'done',
  failed: 'failed',
  canceled: 'canceled',
}

export interface RunCondition {
  kind: FdgConditionKind
  label: string
  /**
   * Which condition actually won.
   *
   * Load-bearing, not decoration: Dex consumes messages only from the condition selected to satisfy
   * the Wait and leaves the losing branches queued. So "waiting on A or B" without saying which one
   * fired misreports durable state.
   */
  satisfied: boolean
}

export interface StepExecution {
  stepExecutionId: string
  stepType: string
  /** 1-based, in the order Dex started them. */
  ordinal: number
  /** `__rpc/<name>` when an external call started this Step. */
  fromStepExecutionId?: string
  /** Null when the Step has no WaitFor at all — absent, not merely unstarted. */
  waitFor: { status: PhaseStatus; conditions: RunCondition[] } | null
  execute: { status: PhaseStatus }
  attempts: number
  lastFailure?: string
  startedAt?: number
  endedAt?: number
  /**
   * Where a COMPLETED execution said it would go next.
   *
   * This is what lets the canvas draw the next Step before it exists. Without it a 15-minute run is
   * permanently one step behind, which is the difference between "working on X next" and a
   * dead-looking graph. It is ask #1 in the backend contract.
   */
  nextStepTypes?: string[]
  /**
   * The StepDecision this execution actually returned.
   *
   * Needed because a Step's DEFINITION lists every verb it *could* return, and naming a surprising
   * one from the definition is a lie: `CheckBalanceStep` can `forceFail` on insufficient funds, but
   * saying so on a run where it succeeded is simply wrong. Only the execution knows which verb fired.
   * This is `decision.type` in the backend contract.
   */
  decisionType?: string
}

export interface RunOverlay {
  runId: string
  flowId: string
  status: string
  executions: StepExecution[]
  /**
   * `true` for every hand-authored overlay, and there is still no setter.
   *
   * The rule from the shipping app, carried over deliberately: `simulated: false` is legal only for an
   * overlay observed to carry real backend events. A hand-authored run that can present itself as live is
   * the one failure this whole POC is written to avoid.
   *
   * WIDENED FROM THE LITERAL `true` to `boolean`, which is the smallest change that makes the rule
   * EXPRESSIBLE rather than merely stated. While the type was `true`, no adapter could ever produce a live
   * overlay — so the rule was enforced by making the honest case impossible, which meant a live surface
   * would have had to invent a parallel type and the marker would have been lost with it. `false` is now
   * legal and remains a claim that has to be earned: only the live adapter sets it, and only after it has
   * observed a state transition arrive from the server rather than on the strength of being configured.
   */
  readonly simulated: boolean
  note: string
  /**
   * The run's own clock, so elapsed values are stable and testable.
   *
   * A scripted run must not be measured against the real wall clock — a fixture written last month
   * would report an elapsed of weeks, which is both wrong and untestable.
   */
  now: number
}

/**
 * Everything one card needs to know about a StepType's run state.
 *
 * `current` is the latest execution, which is what the card's status reflects. `bar` is the
 * count-per-status histogram behind the proportional state bar — Airflow's mechanic, where each
 * segment's width IS the count so "about nine hundred succeeded, a sliver failed" is readable
 * without expanding anything.
 */
export interface StepRunState {
  count: number
  current: StepExecution
  bar: { status: PhaseStatus; count: number }[]
  /** True when a predecessor named this Step as next but no execution exists yet. */
  planned: false
}

/** A Step named by a completed execution's decision, with no execution of its own yet. */
export interface PlannedRunState {
  count: 0
  planned: true
  /** Which execution predicted it, so the card can say where the prediction came from. */
  predictedBy: string
}

export type RunState = StepRunState | PlannedRunState

/** Narrows a run state to one with executions, also where `strictNullChecks` is off. */
export function isExecutedRun(run: RunState | null): run is StepRunState {
  return run !== null && !run.planned
}

/** Still in progress: blocked on its WaitFor, or running. */
export function isOpenStatus(status: PhaseStatus): boolean {
  return status === 'waiting' || status === 'running' || status === 'pending'
}

/**
 * The status a whole Step card shows, from its two phases.
 *
 * Encodes Dex's phase asymmetry, which is easy to get backwards: WaitFor is re-evaluated many times
 * so its having run means "started", while Execute runs once so its having run means "completed".
 * The card leads with whichever phase is still open, because that is the actionable one.
 */
export function cardStatus(e: StepExecution): PhaseStatus {
  if (e.execute.status === 'failed' || e.waitFor?.status === 'failed') return 'failed'
  if (e.execute.status === 'canceled' || e.waitFor?.status === 'canceled') return 'canceled'
  if (e.waitFor !== null && (e.waitFor.status === 'waiting' || e.waitFor.status === 'pending')) {
    return e.waitFor.status
  }
  if (e.execute.status === 'completed') return 'completed'
  if (e.execute.status === 'running' || e.waitFor?.status === 'completed') return 'running'
  return e.execute.status
}

export function runStateFor(overlay: RunOverlay, stepType: string): RunState | null {
  const mine = overlay.executions
    .filter((e) => e.stepType === stepType)
    .sort((a, b) => a.ordinal - b.ordinal)

  if (mine.length === 0) {
    // Planned: something completed and named this Step as next, but it has not started.
    const predictor = overlay.executions.find(
      (e) => e.execute.status === 'completed' && (e.nextStepTypes ?? []).includes(stepType),
    )
    if (predictor === undefined) return null
    return { count: 0, planned: true, predictedBy: predictor.stepExecutionId }
  }

  const tally = new Map<PhaseStatus, number>()
  for (const e of mine) {
    const s = cardStatus(e)
    tally.set(s, (tally.get(s) ?? 0) + 1)
  }
  // Ordered so the bar reads the same way every time regardless of arrival order.
  const bar = PHASE_STATUSES.filter((s) => tally.has(s)).map((s) => ({
    status: s,
    count: tally.get(s) as number,
  }))

  return { count: mine.length, current: mine[mine.length - 1] as StepExecution, bar, planned: false }
}

/** Steps whose latest execution is still open, in the order Dex started them. */
export function activeStepTypes(overlay: RunOverlay): string[] {
  const open: PhaseStatus[] = ['waiting', 'running', 'pending']
  return [...new Set(overlay.executions.map((e) => e.stepType))].filter((t) => {
    const mine = overlay.executions.filter((e) => e.stepType === t)
    const last = mine[mine.length - 1]
    return last !== undefined && open.includes(cardStatus(last))
  })
}

/** Executions in commit order — what view C needs to become literal rather than speculative. */
export function commitOrder(overlay: RunOverlay): StepExecution[] {
  return [...overlay.executions].sort(
    (a, b) => (a.startedAt ?? 0) - (b.startedAt ?? 0) || a.ordinal - b.ordinal,
  )
}

// ---------------------------------------------------------------------------
// Card presentation derivations
// ---------------------------------------------------------------------------

/**
 * The two-cell status token.
 *
 * Our Step has TWO phases that are separate atomic commits with independent retry policies and
 * independent failure events, so a single rolled-up token would have to lie about one of them. No
 * surveyed product has this problem — every one has exactly one status per node, and Zapier says the
 * rule out loud: "the Zap run will only display one status".
 *
 * So: ONE token slot rendered as at most TWO cells. The Wait cell is ABSENT — not empty, not greyed —
 * when the Step has no WaitFor, which means a no-Wait Step's card looks exactly like every other
 * product's single-token card and we only pay the complexity where the model has it.
 */
export interface PhaseCell {
  phase: 'wait' | 'execute'
  status: PhaseStatus
  /** The wait-kind glyph, or 'mixed' when a StepType's executions resolved differently. */
  kind?: FdgConditionKind | 'mixed'
  /** Achromatic while blocked: nothing is consuming anything, so nothing should look busy. */
  achromatic: boolean
}

export interface CardToken {
  wait: PhaseCell | null
  execute: PhaseCell
  /**
   * The aggregate, used for the card's emphasis only — never rendered as a third mark.
   *
   * Carries n8n's group-rollup VETO, transplanted: "success is the only status that speaks for every
   * member". A Step whose Wait is satisfied but whose Execute is still queued must never read green.
   */
  aggregate: PhaseStatus
}

const BLOCKED: PhaseStatus[] = ['waiting', 'pending']

/**
 * Which condition kind the Wait cell should draw.
 *
 * One rule: if exactly one kind is relevant, show it; otherwise show `mixed`. "Relevant" means the
 * kind that WON if the wait is satisfied, and the kinds still pending if it is blocked. So a wait
 * racing a channel against a timer reads `mixed` while blocked and settles on the winner's glyph once
 * decided — which makes the glyph itself the answer to "which condition won", a fact that is durable
 * state in Dex and that only one surveyed product exposes at all (Inngest, as a panel field).
 */
export function waitKind(
  exec: StepExecution,
  all: StepExecution[],
): FdgConditionKind | 'mixed' | undefined {
  const w = exec.waitFor
  if (w === null) return undefined
  const relevant = BLOCKED.includes(w.status)
    ? w.conditions.filter((c) => !c.satisfied)
    : w.conditions.filter((c) => c.satisfied)
  const kinds = new Set(relevant.map((c) => c.kind))
  // Across executions: if different executions were satisfied by different kinds, that difference is
  // itself the fact, so the collapsed card reports `mixed` and the breakdown moves to hover/panel.
  if (!BLOCKED.includes(w.status)) {
    for (const e of all) {
      if (e.waitFor === null) continue
      for (const c of e.waitFor.conditions) if (c.satisfied) kinds.add(c.kind)
    }
  }
  if (kinds.size === 0) return undefined
  if (kinds.size > 1) return 'mixed'
  return [...kinds][0]
}

export function cardToken(exec: StepExecution, all: StepExecution[]): CardToken {
  const wait: PhaseCell | null =
    exec.waitFor === null
      ? null
      : {
          phase: 'wait',
          status: exec.waitFor.status,
          kind: waitKind(exec, all),
          achromatic: BLOCKED.includes(exec.waitFor.status),
        }
  const execute: PhaseCell = {
    phase: 'execute',
    status: exec.execute.status,
    achromatic: exec.execute.status === 'notStarted',
  }
  return { wait, execute, aggregate: cardStatus(exec) }
}

/** Coarse elapsed. No seconds above a minute, no minutes above a day — a card is not a stopwatch. */
export function coarseElapsed(fromMs: number, nowMs: number): string {
  const s = Math.max(0, Math.round((nowMs - fromMs) / 1000))
  if (s < 60) return `${s}s`
  const m = Math.floor(s / 60)
  if (m < 60) return `${m}m`
  const h = Math.floor(m / 60)
  if (h < 24) return `${h}h ${m % 60}m`
  const d = Math.floor(h / 24)
  return `${d}d ${h % 24}h`
}

/** Decision verbs that are SURPRISING enough to name on the card. `goTo` never is. */
const SURPRISING: Record<string, string> = {
  deadEnd: 'this branch stops here',
  forceFail: 'the whole flow was failed',
  forceComplete: 'the whole flow was completed early',
  forceCompleteIfChannelsEmpty: 'completed because the queues were empty',
}

export interface Reason {
  text: string
  tone: 'blocked' | 'failed' | 'terminal'
}

/**
 * ONE line of templated prose, rendered ONLY when the Step is blocked or has failed.
 *
 * This is the answer to "the run decoration is too sparse". The hard question is never *what* state —
 * it is *why* that state and what it is waiting for, and no status glyph can say "nothing has
 * published to approve" or "the timer won, the channel is still queued".
 *
 * Two guards stop it becoming a panel: it is CONDITIONAL, so a healthy Step has no strip at all and a
 * green run reads quiet; and it is one line, ellipsized, with the full text in the panel.
 *
 * TEMPLATED, never model-generated. It sits on the canvas where it cannot carry a caveat — which is
 * why Copilot Studio gates its AI rationale behind an explicit "Show rationale" and a "might not be
 * accurate" warning. Generated explanation belongs in the panel, one click away.
 */
export function reasonLine(exec: StepExecution, nowMs: number): Reason | null {
  const w = exec.waitFor
  const since = exec.startedAt === undefined ? '' : ` · ${coarseElapsed(exec.startedAt, nowMs)}`

  if (w !== null && w.status === 'failed') {
    return { text: `the wait failed${exec.lastFailure === undefined ? '' : ` — ${exec.lastFailure}`}`, tone: 'failed' }
  }
  if (exec.execute.status === 'failed') {
    const n = exec.attempts > 1 ? ` after ${exec.attempts} attempts` : ''
    return {
      text: `failed${n}${exec.lastFailure === undefined ? '' : ` — ${exec.lastFailure}`}`,
      tone: 'failed',
    }
  }
  if (w !== null && BLOCKED.includes(w.status)) {
    const pending = w.conditions.filter((c) => !c.satisfied)
    if (pending.length === 0) {
      // The Go analyser resolves no conditions, so a blocked wait can legitimately have none listed.
      return { text: `waiting — the analyser did not say on what${since}`, tone: 'blocked' }
    }
    const chans = pending.filter((c) => c.kind === 'channel').map((c) => c.label)
    const timers = pending.filter((c) => c.kind === 'timer').map((c) => c.label)
    const subs = pending.filter((c) => c.kind === 'subflow').map((c) => c.label)
    const parts: string[] = []
    if (chans.length > 0) parts.push(`someone to publish ${chans.join(' or ')}`)
    if (subs.length > 0) parts.push(`${subs.length} child flow${subs.length === 1 ? '' : 's'}`)
    if (timers.length > 0) parts.push(timers.join(' or '))
    return { text: `waiting for ${parts.join(', or ')}${since}`, tone: 'blocked' }
  }
  return null
}

/**
 * A surprising terminal decision, named — from what the execution ACTUALLY returned.
 *
 * Deliberately takes one verb, not the definition's list. An earlier version scanned the definition's
 * possible verbs and captioned a healthy Step with a `forceFail` it never took.
 */
export function terminalReason(verb: string | undefined): Reason | null {
  if (verb === undefined) return null
  const said = SURPRISING[verb]
  return said === undefined ? null : { text: `${verb} — ${said}`, tone: 'terminal' }
}

/** The hover decode for the segmented bar: a count per status, plus the wait-kind breakdown. */
export function barDecode(
  bar: { status: PhaseStatus; count: number }[],
  execs: StepExecution[],
): string {
  const lines = bar.map((s) => `${s.count} ${PHASE_LABEL[s.status]}`)
  const kinds = new Map<string, number>()
  for (const e of execs) {
    for (const c of e.waitFor?.conditions ?? []) {
      if (c.satisfied) kinds.set(c.kind, (kinds.get(c.kind) ?? 0) + 1)
    }
  }
  if (kinds.size > 0) {
    lines.push('')
    for (const [k, n] of kinds) lines.push(`${n} satisfied by ${k}`)
  }
  lines.push('', 'Click to list every execution')
  return lines.join('\n')
}

/** Every execution of a StepType, in order. Needed for the mixed-kind and breakdown derivations. */
export function executionsOf(overlay: RunOverlay, stepType: string): StepExecution[] {
  return overlay.executions
    .filter((e) => e.stepType === stepType)
    .sort((a, b) => a.ordinal - b.ordinal)
}
