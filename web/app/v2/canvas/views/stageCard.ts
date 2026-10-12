// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import { whoActs, type PocFlow, type StepModel } from '../model/pocFlow'
import { cardStatus, isExecutedRun, isOpenStatus, runStateFor, type PhaseStatus } from '../model/run'
import type { StepGroup } from './groups'
import { connectorName, packChips, tileOf } from './stepBox'
import type { Box, BoxChip, RunPill, StageSummary, Tile, ViewOpts } from './types'

/** A closed stage: one card for a `dex:group`, saying what its Steps wait for and use. */

const STAGE_W = 288
/** Padding, the title row, the tile row and the facts line. */
const STAGE_HEAD_H = 98
const CHIP_ROW_H = 22
const CHIPS_PAD = 6

export function stageId(groupId: string): string {
  return `stage:${groupId}`
}

/** Each kind of tile the Steps carry, once, in reading order. */
function distinctTiles(flow: PocFlow, members: StepModel[]): Tile[] {
  const seen = new Set<string>()
  return members
    .map((step) => tileOf(flow, step))
    .filter((tile) => {
      const key = `${'glyph' in tile ? tile.glyph : tile.monogram}|${tile.tone}`
      if (seen.has(key)) return false
      seen.add(key)
      return true
    })
}

function summary(flow: PocFlow, members: StepModel[], loops: boolean): StageSummary {
  const waits = members.filter((step) => ['person', 'outside', 'waits'].includes(whoActs(flow, step))).length
  const apps = new Map<string, number>()
  for (const step of members) {
    if (!step.connector) continue
    const name = connectorName(step.connector.connectorId)
    apps.set(name, (apps.get(name) ?? 0) + 1)
  }
  const facts = [
    waits > 0 ? `${waits} wait${waits === 1 ? 's' : ''} for someone` : '',
    ...[...apps].map(([name, count]) => (count > 1 ? `${name} ×${count}` : name)),
  ].filter(Boolean)
  return {
    stepCount: members.length,
    tiles: distinctTiles(flow, members),
    facts: facts.length > 0 ? facts.join(' · ') : 'Automatic',
    loops,
  }
}

const PILL: Partial<Record<PhaseStatus, string>> = {
  running: 'Running',
  waiting: 'Waiting',
  pending: 'Waiting',
  failed: '✕ Failed',
  completed: '✓ Done',
}

/** A failure wins, then open work, then done; an untouched stage says nothing. */
function pillOf(members: StepModel[], opts: ViewOpts): { pill?: RunPill; holds: string[] } {
  const overlay = opts.run
  if (overlay === null) return { holds: [] }
  const states = members.flatMap((step) => {
    const run = runStateFor(overlay, step.stepType)
    return isExecutedRun(run) ? [{ step, status: cardStatus(run.current) }] : []
  })
  const holds = states.filter((s) => isOpenStatus(s.status) || s.status === 'failed').map((s) => s.step.id)
  const pick =
    states.find((s) => s.status === 'failed') ??
    states.find((s) => isOpenStatus(s.status)) ??
    states[states.length - 1]
  if (pick === undefined) return { holds }
  const text = PILL[pick.status]
  return { holds, ...(text === undefined ? {} : { pill: { status: pick.status, text } }) }
}

/** A closed stage's card at the origin; the layout moves it. */
export function stageBox(
  flow: PocFlow,
  group: StepGroup,
  members: StepModel[],
  opts: ViewOpts,
  docked: { jumps: { to: string; failure: boolean; label: string }[]; loops: boolean },
): Box {
  const chips: BoxChip[] = docked.jumps.map((jump) => ({
    kind: jump.failure ? 'failJump' : 'jump',
    text: `↪ ${jump.label}`,
    targetId: jump.to,
    ...(jump.failure ? { title: 'On failure' } : {}),
  }))
  const ends = new Set(members.flatMap((step) => step.execute.branches.flatMap((b) => (b.closes ? [b.closes] : []))))
  if (ends.has('complete') || ends.has('completeIfChannelsEmpty')) chips.push({ kind: 'ends', text: '✓ completes' })
  const rows = packChips(chips, STAGE_W - 28)
  const run = pillOf(members, opts)
  return {
    id: stageId(group.id),
    x: 0,
    y: 0,
    w: STAGE_W,
    h: STAGE_HEAD_H + (rows.length > 0 ? rows.length * CHIP_ROW_H + CHIPS_PAD : 0),
    kind: 'stage',
    title: group.label,
    identity: `${members.length} steps: ${members.map((step) => step.stepType).join(', ')}`,
    stage: summary(flow, members, docked.loops),
    chips: rows,
    isStart: members.some((step) => step.isStart) || undefined,
    pill: run.pill,
    holds: run.holds,
    actor: members.some((step) => step.actor === 'external') ? 'external' : undefined,
  }
}
