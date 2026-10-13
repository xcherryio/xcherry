// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import { failureHandling, failureRoutes, stepById, type PocFlow, type Transition } from '../model/pocFlow'
import { isExecutedRun, runStateFor, type RunOverlay, type StepRunState } from '../model/run'
import { groupOfStep } from './controlTopology'
import { stageId } from './stageCard'
import { stepBox, stepTitle, type DockedJump } from './stepBox'
import type { Box, Link, Scene, ViewOpts } from './types'

/** Failure handling drawn on demand, beside the selected Step or the Step a run failed from. */

/** Between a card and its detour. */
const GAP = 56
/** Between the cards of one detour. */
const STACK_GAP = 28
const POINTER_H = 30
/** Where a line leaves a card's side: level with its title, not its middle. */
const SIDE_Y = 31

type Rect = { x: number; y: number; w: number; h: number }

/** A main-story Step's handler Steps, and the main-story Steps its failures go straight to. */
interface Detour {
  source: string
  steps: string[]
  pointers: string[]
}

function detourOf(flow: PocFlow, source: string, hidden: ReadonlySet<string>): Detour {
  const steps: string[] = []
  const pointers: string[] = []
  for (const route of failureRoutes(flow, source)) {
    if (!hidden.has(route.toStepId)) {
      if (!pointers.includes(route.toStepId)) pointers.push(route.toStepId)
      continue
    }
    if (steps.includes(route.toStepId)) continue
    // Every failure-handling Step reachable from the handler belongs to this detour.
    steps.push(route.toStepId)
    for (let i = steps.length - 1; i < steps.length; i += 1) {
      for (const t of flow.transitions) {
        if (t.fromStepId !== steps[i] || t.isSelfLoop || !hidden.has(t.toStepId) || steps.includes(t.toStepId))
          continue
        steps.push(t.toStepId)
      }
    }
  }
  return { source, steps, pointers }
}

/** Whether selecting this Step draws a detour: it is in the main story and some failure leaves it. */
export function hasDetour(flow: PocFlow, stepId: string): boolean {
  return !failureHandling(flow).has(stepId) && failureRoutes(flow, stepId).length > 0
}

/** Whether this Step is failure handling, which shows only inside a detour. */
export function isFailureHandling(flow: PocFlow, stepId: string): boolean {
  return failureHandling(flow).has(stepId)
}

/** For each failure-handling Step a run reached, the Step that failed into it. */
export function runDetourSources(flow: PocFlow, run: RunOverlay): string[] {
  const hidden = failureHandling(flow)
  const stateOf = (id: string): StepRunState | null => {
    const step = stepById(flow, id)
    const state = step ? runStateFor(run, step.stepType) : null
    return isExecutedRun(state) ? state : null
  }
  const detours = flow.steps
    .filter((step) => !hidden.has(step.id) && failureRoutes(flow, step.id).length > 0)
    .map((step) => detourOf(flow, step.id, hidden))
  const sources: string[] = []
  for (const step of flow.steps) {
    if (!hidden.has(step.id) || stateOf(step.id) === null) continue
    const through = detours.filter((detour) => detour.steps.includes(step.id))
    const failed = (id: string): boolean => stateOf(id)?.bar.some((seg) => seg.status === 'failed') === true
    const pick =
      through.find((detour) => failed(detour.source)) ??
      through.find((detour) => stateOf(detour.source) !== null) ??
      through[0]
    if (pick && !sources.includes(pick.source)) sources.push(pick.source)
  }
  return sources
}

/** How much of a rectangle the given boxes cover. */
function covered(r: Rect, boxes: Rect[]): number {
  let area = 0
  for (const b of boxes) {
    const w = Math.min(r.x + r.w, b.x + b.w) - Math.max(r.x, b.x)
    const h = Math.min(r.y + r.h, b.y + b.h) - Math.max(r.y, b.y)
    if (w > 0 && h > 0) area += w * h
  }
  return area
}

/** How far a detour may slide along its card's side to find a clear place. */
const SLIDE = 480
const SLIDE_STEP = 40

/** Beside the card, preferring right (below, left-right), sliding until it covers no card. */
function placeBeside(
  anchor: Box,
  size: { w: number; h: number },
  boxes: Box[],
  bands: Scene['bands'],
  lr: boolean,
): { x: number; y: number } {
  // A left-right drawing is the top-down one turned: compute top-down, then turn the answer back.
  const flip = <T extends Rect>(r: T): T => (lr ? { ...r, x: r.y, y: r.x, w: r.h, h: r.w } : r)
  const card = flip({ x: anchor.x, y: anchor.y, w: anchor.w, h: anchor.h })
  const box = flip({ x: 0, y: 0, ...size })
  const cards = boxes.filter((b) => b !== anchor).map(flip)
  const centre = { x: anchor.x + anchor.w / 2, y: anchor.y + anchor.h / 2 }
  const strange = bands
    .filter((b) => !(b.x <= centre.x && centre.x <= b.x + b.w && b.y <= centre.y && centre.y <= b.y + b.h))
    .map(flip)
  const right = card.x + card.w + GAP
  const left = card.x - GAP - box.w
  const tries: { x: number; y: number; far: number }[] = []
  for (let dy = 0; dy <= SLIDE; dy += SLIDE_STEP) {
    for (const y of dy === 0 ? [card.y] : [card.y + dy, card.y - dy]) {
      tries.push({ x: right, y, far: dy * 1.5 }, { x: left, y, far: dy * 1.5 + 24 })
    }
  }
  // Out past every card level with this one, on either side.
  const level = cards.filter((c) => c.y < card.y + box.h && card.y < c.y + c.h)
  tries.push({ x: Math.max(right, ...level.map((c) => c.x + c.w + GAP)), y: card.y, far: 0 })
  tries.push({ x: Math.min(left, ...level.map((c) => c.x - GAP - box.w)), y: card.y, far: 24 })
  let best = tries[0]
  let bestCost = Number.POSITIVE_INFINITY
  for (const t of tries) {
    const r = { ...t, w: box.w, h: box.h }
    const onCards = covered(r, cards)
    const cost =
      (onCards > 0 ? 1e6 + onCards : 0) + covered(r, strange) * 0.02 + t.far + Math.max(0, t.x - right, left - t.x)
    if (cost < bestCost) {
      bestCost = cost
      best = t
    }
  }
  return lr ? { x: best.y, y: best.x } : { x: best.x, y: best.y }
}

/** A line from one card's side to another's: straight when level, otherwise with one jog between. */
function sideLine(a: Rect, b: Rect, lr: boolean): { x: number; y: number }[] {
  if (lr) {
    const down = b.y >= a.y + a.h
    const start = { x: a.x + Math.min(a.w / 2, 40), y: down ? a.y + a.h : a.y }
    // Straight across when the target's side spans the line, as a short pointer's does.
    const level = start.x > b.x + 4 && start.x < b.x + b.w - 4
    const end = { x: level ? start.x : b.x + Math.min(b.w / 2, 40), y: down ? b.y : b.y + b.h }
    if (Math.abs(start.x - end.x) < 1) return [start, end]
    const mid = (start.y + end.y) / 2
    return [start, { x: start.x, y: mid }, { x: end.x, y: mid }, end]
  }
  const right = b.x >= a.x + a.w
  const start = { x: right ? a.x + a.w : a.x, y: a.y + Math.min(a.h / 2, SIDE_Y) }
  const level = start.y > b.y + 4 && start.y < b.y + b.h - 4
  const end = { x: right ? b.x : b.x + b.w, y: level ? start.y : b.y + Math.min(b.h / 2, SIDE_Y) }
  if (Math.abs(start.y - end.y) < 1) return [start, end]
  const mid = (start.x + end.x) / 2
  return [start, { x: mid, y: start.y }, { x: mid, y: end.y }, end]
}

/** A line from one card in a detour to the next one along it. */
function alongLine(a: Rect, b: Rect, lr: boolean): { x: number; y: number }[] {
  if (lr)
    return [
      { x: a.x + a.w, y: a.y + Math.min(a.h / 2, SIDE_Y) },
      { x: b.x, y: a.y + Math.min(a.h / 2, SIDE_Y) },
    ]
  return [
    { x: a.x + Math.min(a.w, b.w) / 2, y: a.y + a.h },
    { x: a.x + Math.min(a.w, b.w) / 2, y: b.y },
  ]
}

/** The Scene with the detours of these main-story Steps drawn beside them. */
export function withDetours(scene: Scene, flow: PocFlow, opts: ViewOpts, sources: string[]): Scene {
  if (sources.length === 0) return scene
  const hidden = failureHandling(flow)
  const lr = opts.direction === 'lr'
  const boxes = [...scene.boxes]
  const links: Link[] = [...scene.links]
  const byId = new Map(boxes.map((box) => [box.id, box]))
  /** The box a Step is drawn as: its own card, or the collapsed stage that holds it. */
  const shownAs = (stepId: string): Box | undefined => {
    const own = byId.get(stepId)
    if (own) return own
    const group = groupOfStep(opts.groups ?? [], stepId)
    return group ? byId.get(stageId(group.id)) : undefined
  }
  for (const source of sources) {
    const anchor = shownAs(source)
    if (anchor === undefined || hidden.has(source)) continue
    const detour = detourOf(flow, source, hidden)
    // A handler another detour already draws is pointed to, never drawn twice.
    const own = detour.steps.filter((id) => !byId.has(id))
    const along = new Set<string>()
    for (let i = 0; i + 1 < own.length; i += 1) {
      const t = flow.transitions.find((u) => u.fromStepId === own[i] && u.toStepId === own[i + 1])
      if (t) along.add(t.id)
    }
    const items: Box[] = own.map((id) => {
      const step = stepById(flow, id)
      if (step === undefined) throw new Error(`detour Step ${id} is not in the Flow`)
      const jumps: DockedJump[] = flow.transitions
        .filter((t) => t.fromStepId === id && !t.isSelfLoop && !along.has(t.id))
        .flatMap((t) => {
          const inside = own.includes(t.toStepId)
          const to = inside ? stepById(flow, t.toStepId) : undefined
          const shown = inside ? undefined : (shownAs(t.toStepId) ?? byId.get(t.toStepId))
          if (to === undefined && shown === undefined) return []
          return [
            {
              id: t.id,
              from: id,
              to: to ? to.id : (shown as Box).id,
              failure: t.kind === 'failure_transition',
              label: to ? stepTitle(to) : (shown as Box).title,
              exit: !hidden.has(t.toStepId),
            },
          ]
        })
      const alsoCatches = new Set(
        flow.transitions
          .filter((t: Transition) => t.kind === 'failure_transition' && t.toStepId === id && !t.isSelfLoop)
          .map((t) => t.fromStepId)
          .filter((from) => from !== source && !detour.steps.includes(from)),
      ).size
      return { ...stepBox(flow, step, opts, { jumps, arriving: 0, alsoCatches }), detourOf: source }
    })
    const routes = failureRoutes(flow, source).map((route) => route.toStepId)
    const ends = [...detour.pointers, ...detour.steps.filter((id) => byId.has(id) && routes.includes(id))]
    for (const id of ends) {
      const to = shownAs(id)
      if (to === undefined) continue
      const title = `↪ ${to.title}`
      items.push({
        id: `detour:${source}->${to.id}`,
        kind: 'pointer',
        title,
        x: 0,
        y: 0,
        w: Math.min(256, Math.ceil(title.length * 6.6) + 28),
        h: POINTER_H,
        detourOf: source,
        target: to.id,
      })
    }
    if (items.length === 0) continue

    // Top-down, a short first item drops so the line from the card's side reaches it level.
    const lead = lr ? 0 : Math.max(0, Math.min(anchor.h / 2, SIDE_Y) - Math.min(items[0].h / 2, SIDE_Y))
    const span =
      lead + items.reduce((sum, item) => sum + (lr ? item.w : item.h), 0) + STACK_GAP * (items.length - 1)
    const across = Math.max(...items.map((item) => (lr ? item.h : item.w)))
    const at = placeBeside(anchor, lr ? { w: span, h: across } : { w: across, h: span }, boxes, scene.bands, lr)
    // Left of its card, a detour hugs the card's side.
    const flushRight = !lr && at.x < anchor.x
    let next = lr ? at.x : at.y + lead
    for (const item of items) {
      if (lr) Object.assign(item, { x: next, y: at.y })
      else Object.assign(item, { x: flushRight ? at.x + across - item.w : at.x, y: next })
      next += (lr ? item.w : item.h) + STACK_GAP
      boxes.push(item)
      byId.set(item.id, item)
    }

    for (const route of failureRoutes(flow, source)) {
      // A handler drawn here, or the pointer to a Step drawn elsewhere.
      const item =
        items.find((box) => box.kind === 'step' && box.id === route.toStepId) ??
        items.find((box) => box.kind === 'pointer' && box.target === shownAs(route.toStepId)?.id)
      if (item === undefined || links.some((link) => link.from === anchor.id && link.to === item.id)) continue
      links.push({
        id: `detour:${source}:${route.id}`,
        from: anchor.id,
        to: item.id,
        family: 'failure',
        points: sideLine(anchor, item, lr),
      })
    }
    for (let i = 0; i + 1 < own.length; i += 1) {
      const t = flow.transitions.find((u) => u.fromStepId === own[i] && u.toStepId === own[i + 1])
      const a = byId.get(own[i])
      const b = byId.get(own[i + 1])
      if (t === undefined || a === undefined || b === undefined) continue
      links.push({
        id: `detour:${source}:${t.id}`,
        from: a.id,
        to: b.id,
        family: t.kind === 'failure_transition' ? 'failure' : 'control',
        points: alongLine(a, b, lr),
      })
    }
  }
  const width = Math.max(scene.width, ...boxes.map((box) => box.x + box.w + 40))
  const height = Math.max(scene.height, ...boxes.map((box) => box.y + box.h + 40))
  return { ...scene, boxes, links, width, height }
}

/** What a screen reader hears when a Step's detour opens. */
export function detourMessage(flow: PocFlow, source: string): string {
  const step = stepById(flow, source)
  const detour = detourOf(flow, source, failureHandling(flow))
  const count = detour.steps.length + detour.pointers.length
  return `Failure handling for ${step ? stepTitle(step) : source} shown, ${count} step${count === 1 ? '' : 's'}`
}
