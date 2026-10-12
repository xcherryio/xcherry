// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import type { AgentRole } from '../model/agentic'
import type { Actor, PocFlow } from '../model/pocFlow'
import type { PhaseStatus, Reason, RunOverlay } from '../model/run'
import type { StepGroup } from './groups'

/**
 * A Scene is plain geometry. It is the boundary the whole design rests on: every view produces
 * one, the renderer consumes nothing else, and no React Flow type appears above
 * `render/Stage.tsx`.
 */

/**
 * Layout direction for the topology view.
 *
 * A setting rather than a decision, following Airflow — which persists graph direction per graph
 * precisely because no single answer wins: long pipelines want left-to-right, wide fan-out wants
 * top-to-bottom. n8n hardcodes LR for its main flow, Zapier hardcodes TB. Our real flow is a loop
 * with a seven-way fan, so this is here to be judged rather than asserted.
 *
 * The cost of making it a setting, worth stating: it forfeits Zapier-style positional semantics,
 * where "far right" can *mean* fallback because position is never the user's to change.
 */
export type Direction = 'tb' | 'lr'

export const DIRECTIONS: Direction[] = ['tb', 'lr']

export const DIRECTION_LABEL: Record<Direction, string> = {
  tb: 'Top-down',
  lr: 'Left-right',
}

export interface ViewOpts {
  direction: Direction
  /**
   * Run state, or null for the definition.
   *
   * Null means the ABSENCE of a run, not a run with every status blank. A definition renders every
   * possible path; a graph where every node read "pending" would misrepresent what the artifact is.
   * So the same views serve both, and execution is strictly additive.
   */
  run: RunOverlay | null
  /**
   * Semantic groups, supplied by the caller rather than derived here.
   *
   * Passed in so the view stays a pure function of the flow: the groups are currently mocked per
   * fixture, and a view that looked up a fixture id would be coupled to the harness.
   */
  groups?: StepGroup[]
  /** The stages drawn open: Collapsed opens none, Expanded every one. */
  openStages?: ReadonlySet<string>
}

/** A Step card, a closed stage's card, an RPC card of a Flow without Steps, or a detour pointer. */
export type BoxKind = 'step' | 'stage' | 'rpcCard' | 'pointer'

/** The palette tones a card part may take. */
export type Tone = 'attention' | 'door' | 'machine' | 'clock' | 'child' | 'accent' | 'app'

/** The tile at a card's left: a glyph in the tone of who acts, or an app's monogram. */
export type Tile =
  | { glyph: 'person' | 'door' | 'cpu' | 'clock' | 'child' | 'wait' | 'spark'; tone: Tone }
  | { monogram: string; tone: Tone }

/** Something docked to a card instead of drawn as a box with a line. */
export interface BoxChip {
  kind:
    | 'jump'
    | 'failJump'
    | 'inbound'
    | 'fork'
    | 'ends'
    | 'stops'
    | 'fails'
    | 'answer'
    | 'entry'
    | 'child'
    | 'role'
  text: string
  /** The Step a jump leads to, so a click can show it. */
  targetId?: string
  title?: string
}

export interface BoxRow {
  /** A small leading glyph. Text, not an icon font. */
  glyph?: string
  text: string
  /** Right-aligned trailing text. Identity only — never an expression. */
  trail?: string
  tone?: 'normal' | 'quiet' | 'warn'
}

/** What a closed stage's card says about its Steps. */
export interface StageSummary {
  stepCount: number
  /** Each kind of tile its Steps carry, once, in reading order. */
  tiles: Tile[]
  /** Who it waits for and the apps it uses. */
  facts: string
  /** A transition stays inside the stage, so it repeats. */
  loops: boolean
}

/** A card's run state, in words so it never rests on colour. */
export interface RunPill {
  status: PhaseStatus | 'planned'
  text: string
}

export interface Box {
  id: string
  x: number
  y: number
  w: number
  h: number
  kind: BoxKind
  title: string
  /** The durable identifier behind a friendly title, for hover and the panel. */
  identity?: string
  /** The TYPE, never a value. Airflow shows the operator name here; we show the Step's role. */
  subtitle?: string
  subtitleTone?: Tone
  tile?: Tile
  /**
   * Drives the accent that says who has to act. Taken from the domain rather than restated, so
   * adding an actor cannot leave the renderer silently behind.
   */
  actor?: Actor
  /**
   * What this Step is FOR in an agentic flow: the decision point, a capability, the human gate.
   *
   * A separate channel from `actor`, because they answer different questions and a Step has both: the human
   * gate is `actor: 'external'` (a person must publish) AND `agentRole: 'human-gate'` (it is where the agent
   * hands over). Collapsing them would mean either losing the accent that says who must act or losing the
   * silhouette that says this is a decision rather than work.
   */
  agentRole?: AgentRole
  /** Chips packed into rows that fit the card's width. */
  chips?: BoxChip[][]
  stage?: StageSummary
  /** What an `rpcCard` is in the Flow v2 definition, when it is an Action or a view. */
  rpcRole?: 'action' | 'view'
  pill?: RunPill
  /**
   * ONE line of templated prose, rendered only when blocked or failed. This is the answer to "the run
   * decoration is too sparse": the hard question is never *what* state but *why*.
   */
  reason?: Reason
  rows?: BoxRow[]
  /**
   * BPMN's LOOP MARKER: this step can transition to itself.
   *
   * Drawn on the card as well as as an edge, because the arc is only a few pixels at a small zoom and
   * "this repeats" is too important to depend on tracing it. BPMN puts the same glyph at the bottom of
   * an activity for exactly this reason — the marker states the behaviour, the edge shows the path.
   */
  loops?: boolean
  /**
   * A second mark, for a Step that is also a shared failure target.
   *
   * Separate from `badge` because the two facts are independent and a Step can be both: the book
   * pipeline's RecoveryGate is a human gate AND the dispatcher every failure routes to, and while the
   * badge slot held only one of them it said "needs you" and never mentioned the other.
   */
  recovery?: { glyph: string; title: string }
  /**
   * Where the Flow begins. A DEFINITION fact, known before any run, so it gets its own channel
   * rather than a turn in `emphasis`.
   *
   * `emphasis` holds one value and run state wins it, which is right — "this failed" outranks "this
   * is the first Step". But the two are orthogonal, so sharing the slot meant the start mark
   * vanished exactly when the start Step was planned, failed, or the one being awaited. Same
   * reasoning as `agentRole` beside `actor`.
   */
  isStart?: boolean
  emphasis?: 'active' | 'failed' | 'planned'
  /**
   * The proportional state bar for a StepType that ran more than once.
   *
   * Airflow's mechanic: each segment's width IS the count, with a 2px floor so one failure among a
   * thousand successes stays visible. It is what makes one-card-per-StepType survive a loop.
   */
  bar?: { status: PhaseStatus; count: number }[]
  /** The bar's hover decode: a count per status plus the wait-kind breakdown. */
  barTitle?: string
  /** The Steps of a closed stage that a run is on, so the stage can carry the "now" ring. */
  holds?: string[]
  /** Failure transitions that leave this Step, drawn only when it is selected. Stage cards carry none. */
  failures?: number
  /** Drawn beside this box, a Step or the stage holding it, as part of its failure detour. */
  detourOf?: string
  /** Where a pointer leads: a main-story Step, or the closed stage that holds it. */
  target?: string
}

/**
 * The closed set of edge meanings. A value here, rather than a bare union, so a test can iterate them
 * and prove each one has an arrowhead — the gap that let every edge ship with no direction at all.
 */
export const FAMILIES = ['control', 'failure'] as const

export type LinkFamily = (typeof FAMILIES)[number]

export interface Link {
  id: string
  from: string
  to: string
  family: LinkFamily
  /** The routed polyline in scene coordinates, start to end. */
  points: { x: number; y: number }[]
  /** A case value, always shown where placed; or a merged-condition count, shown only when lit. */
  label?: { text: string; at?: { x: number; y: number }; always: boolean }
}

/** A labelled region around the members of one group, never another Step. */
export interface Band {
  id: string
  groupId: string
  x: number
  y: number
  w: number
  h: number
  label: string
  /** Cycles the group tint, so adjacent regions are distinguishable without meaning anything. */
  hue: number
  /** An open stage of two or more Steps, which its ⊖ closes into one card. */
  collapsible: boolean
}

export interface Scene {
  boxes: Box[]
  links: Link[]
  bands: Band[]
  width: number
  height: number
  /** Shown under the stage. Use it to admit what this view cannot show. */
  notes: string[]
}

export interface ViewSpec {
  id: string
  letter: string
  label: string
  /** The Dex fact this view is derived from. Shown in the UI so the claim travels with it. */
  principle: string
  claim: string
  risk: string
  layout: (flow: PocFlow, opts: ViewOpts) => Promise<Scene>
}

export const emptyScene = (note: string): Scene => ({
  boxes: [],
  links: [],
  bands: [],
  width: 640,
  height: 200,
  notes: [note],
})

export function boundsOf(
  boxes: Box[],
  bands: Band[],
  links: Link[] = [],
  pad = 48,
): { width: number; height: number } {
  let maxX = 0
  let maxY = 0
  for (const b of [...boxes, ...bands]) {
    maxX = Math.max(maxX, b.x + b.w)
    maxY = Math.max(maxY, b.y + b.h)
  }
  // A side lane can run just beside the drawing.
  for (const p of links.flatMap((link) => link.points)) {
    maxX = Math.max(maxX, p.x)
    maxY = Math.max(maxY, p.y)
  }
  return { width: maxX + pad, height: maxY + pad }
}
