// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import type { AgentRole } from '../model/agentic'
import { answeringEntries, whoActs, type Branch, type PocFlow, type StepModel, type WhoActs } from '../model/pocFlow'
import { fanOutOf } from '../model/routing'
import {
  barDecode,
  cardStatus,
  executionsOf,
  isExecutedRun,
  isOpenStatus,
  reasonLine,
  runStateFor,
  terminalReason,
  type PhaseStatus,
  type RunState,
} from '../model/run'
import type { GraphEdge } from './mainLine'
import type { Box, BoxChip, RunPill, Tile, Tone, ViewOpts } from './types'

/**
 * The Step card: a tile for who acts or which app runs, the Step in words, and docked chips.
 * Heights follow `canvas.css`; change them together.
 */

const STEP_W = 256
/** Padding, a 34px tile and two text lines. */
const HEAD_H = 62
const TITLE_LINE_H = 18
/** Characters of the 13.5px semibold title that fit beside the tile. */
const TITLE_CHARS = 23
const CHIP_ROW_H = 22
const CHIPS_PAD = 8
/** Inside width for chips: the card less its 12px padding each side. */
const CHIP_ROOM = STEP_W - 24
const REASON_H = 24
const BAR_H = 10

/** Connector ids people know by another name, and the letters their tile shows. */
const APPS: Record<string, { name: string; monogram: string }> = {
  github: { name: 'GitHub', monogram: 'GH' },
  gmail: { name: 'Gmail', monogram: 'GM' },
  slack: { name: 'Slack', monogram: 'SL' },
  'google-drive': { name: 'Google Drive', monogram: 'GD' },
  'google-sheets': { name: 'Google Sheets', monogram: 'GS' },
  'google-calendar': { name: 'Google Calendar', monogram: 'GC' },
  notion: { name: 'Notion', monogram: 'NO' },
  stripe: { name: 'Stripe', monogram: 'ST' },
  hubspot: { name: 'HubSpot', monogram: 'HS' },
  linkedin: { name: 'LinkedIn', monogram: 'LI' },
}

/** The `llm` connector reaches any model provider, so its Steps read as AI model work. */
const MODEL_CONNECTORS: ReadonlySet<string> = new Set(['llm'])

/** An identifier split into words, acronyms kept; empty when it has no words. */
function words(identifier: string): string {
  return identifier
    .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
    .replace(/([A-Z]+)([A-Z][a-z])/g, '$1 $2')
    .split(/[\s_-]+/)
    .filter(Boolean)
    .map((word, index) => (index === 0 || /^[A-Z0-9]{2,}$/.test(word) ? word : word.toLowerCase()))
    .join(' ')
}

/** A Step type as people say it: `RecordAIFailure` reads "Record AI failure". */
export function inWords(identifier: string): string {
  const said = words(identifier)
  return said === '' ? identifier : said.charAt(0).toUpperCase() + said.slice(1)
}

/** A Flow type or connector id for people: `TextSummaryFlow` reads "Text summary". */
function processName(flowType: string): string {
  return words(flowType.replace(/Flow$/, '')) || flowType
}

function appOf(connectorId: string): { name: string; monogram: string } {
  const known = APPS[connectorId]
  if (known) return known
  const name = processName(connectorId)
  return {
    name,
    monogram: name
      .replace(/[^A-Za-z0-9]/g, '')
      .slice(0, 2)
      .toUpperCase(),
  }
}

/** What people call the service a connector reaches. */
export function connectorName(connectorId: string): string {
  return MODEL_CONNECTORS.has(connectorId) ? 'AI model' : appOf(connectorId).name
}

const WHO_TILE: Record<WhoActs, Tile> = {
  person: { glyph: 'person', tone: 'attention' },
  outside: { glyph: 'door', tone: 'door' },
  machine: { glyph: 'cpu', tone: 'machine' },
  clock: { glyph: 'clock', tone: 'clock' },
  child: { glyph: 'child', tone: 'child' },
  waits: { glyph: 'wait', tone: 'attention' },
}

/** A Step's tile: its app when a connector runs it, otherwise who acts. */
export function tileOf(flow: PocFlow, step: StepModel): Tile {
  const who = whoActs(flow, step)
  if (who === 'machine' && step.connector) {
    return MODEL_CONNECTORS.has(step.connector.connectorId)
      ? { glyph: 'spark', tone: 'accent' }
      : { monogram: appOf(step.connector.connectorId).monogram, tone: 'app' }
  }
  return WHO_TILE[who]
}

function subtitleOf(flow: PocFlow, step: StepModel): { text: string; tone: Tone } {
  const who = whoActs(flow, step)
  const hasTimer = (step.waitFor?.conditions ?? []).some((c) => c.kind === 'timer')
  if (who === 'machine' && step.connector) {
    const operation = inWords(step.connector.operationId).toLowerCase()
    const isModel = MODEL_CONNECTORS.has(step.connector.connectorId)
    return { text: `${connectorName(step.connector.connectorId)} · ${operation}`, tone: isModel ? 'accent' : 'machine' }
  }
  switch (who) {
    case 'person':
      return { text: hasTimer ? 'Waits for a person or time' : 'Waits for a person', tone: 'attention' }
    case 'outside':
      return { text: hasTimer ? 'Waits for a message or time' : 'Waits for a message', tone: 'door' }
    case 'clock':
      return { text: 'Waits for time', tone: 'clock' }
    case 'child':
      return { text: 'Waits for child flows', tone: 'child' }
    case 'waits':
      return { text: 'Waits · not reported', tone: 'attention' }
    case 'machine':
      return { text: 'Automatic', tone: 'machine' }
  }
}

/**
 * WHAT AN AGENT STEP IS, in one chip, in plain words.
 *
 * The decision step gets a filled diamond against the choice row's hollow one: UML's choice pseudostate
 * says "one of these paths", and filling it says "and something decided WHICH at run time". That is the
 * distinction the whole feature turns on, and it is a shape difference so it survives at thumbnail size
 * and under any colour deficiency.
 *
 * `capability` gets NO CHIP. Eight cards each captioned "a thing the agent can do" is furniture — being
 * reachable from the decision step already says it, and the decision card says it for all of them.
 */
const AGENT_ROLE_ROW: Partial<Record<AgentRole, { glyph: string; text: string }>> = {
  decision: { glyph: '◆', text: 'the agent decides here' },
  'human-gate': { glyph: '✋', text: 'hands over to a person' },
  intake: { glyph: '⬡', text: 'where the request arrives' },
}

/** How a Step ends the Flow: certainly when every branch closes this way, otherwise as one outcome. */
const ENDS: Record<NonNullable<Branch['closes']>, { kind: BoxChip['kind']; always: string; may: string }> = {
  complete: { kind: 'ends', always: '✓ completes', may: '✓ can complete' },
  completeIfChannelsEmpty: { kind: 'ends', always: '✓ completes if queues are empty', may: '✓ can complete' },
  deadEnd: { kind: 'stops', always: '■ stops this branch', may: '■ can stop this branch' },
  fail: { kind: 'fails', always: '✕ fails the flow', may: '✕ can fail the flow' },
}

/** A jump docked to its source, titled with where it leads: a Step, or a closed stage. */
export type DockedJump = GraphEdge & { label: string }

/**
 * Which entry points answer this Step's gate.
 *
 * The wire encodes `rpc -publish-> channel` and `channel -condition-> wait` but never joins them,
 * so every RPC is graph-isolated. This walks the join in reverse. It is the most decision-relevant
 * fact on a gate card and the reason Channels do not need to be nodes: the name plus the answerer
 * fit on two rows of the step that is actually waiting.
 */
export function answeredBy(flow: PocFlow, step: StepModel): string[] {
  return [...new Set(answeringEntries(flow, step).map((entry) => entry.name))]
}

/** Docked chips in reading order: role, who answers, how it starts, where it jumps, how it ends. */
function chipsOf(flow: PocFlow, step: StepModel, jumps: DockedJump[], arriving: number): BoxChip[] {
  const chips: BoxChip[] = []
  const role = step.agentRole === undefined ? undefined : AGENT_ROLE_ROW[step.agentRole]
  if (role !== undefined) chips.push({ kind: 'role', text: `${role.glyph} ${role.text}` })
  const answers = answeringEntries(flow, step)
  for (const entry of answers.slice(0, 4)) {
    chips.push({
      kind: 'answer',
      text: entry.action?.label ?? inWords(entry.name),
      title: `${entry.name} answers this step`,
    })
  }
  if (answers.length > 4) chips.push({ kind: 'answer', text: `+${answers.length - 4} more` })
  for (const entry of flow.entries) {
    if (!entry.branches.some((b) => b.targets.some((t) => t.stepId === step.id))) continue
    chips.push({ kind: 'entry', text: `⚡ ${inWords(entry.name)}`, title: `The call ${entry.name} starts this step` })
  }
  for (const subflow of flow.subflows) {
    if (subflow.startedByStepId !== step.id) continue
    chips.push({
      kind: 'child',
      text: `↳ ${processName(subflow.flowType)}`,
      title: `Starts ${subflow.flowType}`,
    })
  }
  // A fork starts every path at once, which lines alone cannot tell from a choice.
  const fanOut = fanOutOf(step)
  if (fanOut > 0) chips.push({ kind: 'fork', text: fanOut === 2 ? '▮ both paths run' : `▮ all ${fanOut} paths run` })
  if (arriving > 3) {
    chips.push({ kind: 'inbound', text: `← ${arriving} steps lead here`, title: 'One of them is drawn as a line' })
  }
  for (const jump of jumps) {
    const text = `↪ ${jump.label}`
    chips.push(
      jump.failure
        ? { kind: 'failJump', text, targetId: jump.to, title: 'On failure' }
        : { kind: 'jump', text, targetId: jump.to },
    )
  }
  const branches = step.execute.branches
  for (const close of new Set(branches.flatMap((b) => (b.closes ? [b.closes] : [])))) {
    const isAlways = branches.every((b) => b.closes === close)
    chips.push({ kind: ENDS[close].kind, text: isAlways ? ENDS[close].always : ENDS[close].may })
  }
  return chips
}

/** A chip's width at the 10.5px chip size: about 6px a character plus padding. */
function chipWidth(chip: BoxChip, room: number): number {
  return Math.min(room, Math.ceil(chip.text.length * 6.1) + 16)
}

export function packChips(chips: BoxChip[], room = CHIP_ROOM): BoxChip[][] {
  const rows: BoxChip[][] = []
  let row: BoxChip[] = []
  let used = 0
  for (const chip of chips) {
    const w = chipWidth(chip, room)
    if (row.length > 0 && used + w > room) {
      rows.push(row)
      row = []
      used = 0
    }
    row.push(chip)
    used += w + 4
  }
  if (row.length > 0) rows.push(row)
  return rows
}

/** Lines a title takes beside the tile: one, or two when its words do not fit. */
function titleLines(title: string, chars = TITLE_CHARS): 1 | 2 {
  let line = 0
  let lines = 1
  for (const word of title.split(' ')) {
    const need = line === 0 ? word.length : line + 1 + word.length
    if (need > chars && line > 0) {
      lines += 1
      line = word.length
    } else line = need
  }
  return lines > 1 ? 2 : 1
}

const PILL_TEXT: Record<PhaseStatus, string> = {
  notStarted: 'Not started',
  pending: 'Waiting',
  waiting: 'Waiting',
  running: 'Running',
  completed: '✓ Done',
  failed: '✕ Failed',
  canceled: 'Canceled',
}

function pillOf(run: RunState | null, isPerson: boolean): RunPill | undefined {
  if (run === null) return undefined
  if (!isExecutedRun(run)) return { status: 'planned', text: 'Next' }
  const status = cardStatus(run.current)
  const base = (status === 'waiting' || status === 'pending') && isPerson ? 'Waiting for you' : PILL_TEXT[status]
  return { status, text: run.count > 1 ? `${base} ×${run.count}` : base }
}

/** The title a reader sees: the analyzer's display name, or the Step type in words. */
export function stepTitle(step: StepModel): string {
  return step.label === step.stepType ? inWords(step.stepType) : step.label
}

/**
 * The recovery mark, which says WHICH KIND of failure target this is.
 *
 * Dex has no recovery Step type — the whole primitive is one per-Step
 * `on_execute_failure_proceed_to(X)`, so a Step becomes a failure target only by being named by
 * others, and all the multiplicity lands on the receiving side. The three roles that produces want
 * different things said about them, and a count alone does not distinguish the two that matter:
 * whether the flow STOPS here or CARRIES ON from here.
 *
 * Drawn as a second mark rather than folded into the run pill, because a gate can also be a dispatcher
 * and the pill can only hold one fact.
 */
function recoveryMark(flow: PocFlow, step: StepModel): Box['recovery'] {
  const onward = flow.transitions.filter(
    (t) => t.fromStepId === step.id && t.kind !== 'failure_transition' && t.toStepId !== step.id,
  ).length
  if (step.recoveryRole === 'sink') {
    return { glyph: '⇥', title: `Catches failures from ${step.inboundFailureCount} steps, and the flow stops here` }
  }
  if (step.recoveryRole === 'dispatcher') {
    return {
      glyph: '⇉',
      title: step.hasUniformFailurePolicy
        ? `Every step that can fail routes here (${step.inboundFailureCount}), and it can send the flow onward to ${onward}`
        : `Catches failures from ${step.inboundFailureCount} steps, and can send the flow onward to ${onward}`,
    }
  }
  return undefined
}

/** A Step's card at the origin; the layout moves it. */
export function stepBox(
  flow: PocFlow,
  step: StepModel,
  opts: ViewOpts,
  docked: { jumps: DockedJump[]; arriving: number },
): Box {
  const overlay = opts.run
  const run = overlay === null ? null : runStateFor(overlay, step.stepType)
  const executed = isExecutedRun(run) ? run : null
  const status = executed === null ? null : cardStatus(executed.current)
  const title = stepTitle(step)
  const chips = packChips(chipsOf(flow, step, docked.jumps, docked.arriving))
  const bar = executed !== null && executed.bar.length > 1 ? executed.bar : undefined
  const reason =
    executed === null
      ? undefined
      : (reasonLine(executed.current, overlay?.now ?? 0) ?? terminalReason(executed.current.decisionType) ?? undefined)
  const subtitle = subtitleOf(flow, step)
  const h =
    HEAD_H +
    (titleLines(title) - 1) * TITLE_LINE_H +
    (chips.length > 0 ? chips.length * CHIP_ROW_H + CHIPS_PAD : 0) +
    (reason === undefined ? 0 : REASON_H) +
    (bar === undefined ? 0 : BAR_H)
  return {
    id: step.id,
    x: 0,
    y: 0,
    w: STEP_W,
    h,
    kind: 'step',
    title,
    identity: step.stepType,
    subtitle: subtitle.text,
    subtitleTone: subtitle.tone,
    tile: tileOf(flow, step),
    actor: step.actor,
    ...(step.agentRole === undefined ? {} : { agentRole: step.agentRole }),
    chips,
    pill: pillOf(run, whoActs(flow, step) === 'person'),
    reason,
    bar,
    barTitle:
      bar === undefined || overlay === null ? undefined : barDecode(bar, executionsOf(overlay, step.stepType)),
    loops: flow.transitions.some((t) => t.isSelfLoop && t.fromStepId === step.id),
    recovery: recoveryMark(flow, step),
    isStart: step.isStart || undefined,
    emphasis:
      run?.planned === true
        ? 'planned'
        : status === 'failed'
          ? 'failed'
          : status !== null && isOpenStatus(status)
            ? 'active'
            : undefined,
  }
}
