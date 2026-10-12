// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import type { PocFlow } from '../model/pocFlow'
import type { Band, Box, Direction, Scene } from './types'

/**
 * What a good drawing has to achieve, stated once and measured, so spacing stops being tuned
 * per screenshot. Each principle carries the proxy that can fail a test.
 */
export interface LayoutPrinciple {
  id: PrincipleId
  statement: string
  measure: string
  /** A scene violates the principle when its metric exceeds this. */
  limit: number
}

export type PrincipleId =
  | 'no-collision'
  | 'tight-regions'
  | 'group-cohesion'
  | 'short-edges'
  | 'reading-order'
  | 'clear-lines'

/**
 * Lane coherence was measured and dropped, not forgotten.
 *
 * Grouping by `recoveryRole` claimed the refund flow split a class across lanes. It does not:
 * BillingFailedStep is reachable by three ordinary transitions, so the spine is correct for it.
 * The role is just too coarse to predict a lane, which made the metric accuse a truthful drawing.
 */

export const LAYOUT_PRINCIPLES: readonly LayoutPrinciple[] = [
  {
    id: 'no-collision',
    statement: 'Two cards never occupy the same space.',
    measure: 'Overlapping step-box pairs.',
    limit: 0,
  },
  {
    id: 'tight-regions',
    statement: 'A region has no gap inside it wide enough to hold a card.',
    measure: "Worst region's largest sideways hole, in card widths.",
    /**
     * A HOLE, not a fill fraction, and measured against the cards rather than the band.
     *
     * Two earlier forms were wrong. Area punished the shape a strip is meant to have: five failure
     * Steps at five ranks give a tall thin region that is 81% floor and still correct. Uncovered
     * width then charged every region for its own padding and label strip, which is 0.435 of a
     * single-card band left-right. The ordinary gap between fan members is 0.65 of a card.
     */
    limit: 0.85,
  },
  {
    id: 'group-cohesion',
    statement: 'A group sits in one place where the graph allows it.',
    measure: 'Same-label regions at one rank split across two lanes.',
    limit: 0,
  },
  {
    id: 'short-edges',
    statement: 'An arrow spans about a rank, not the whole drawing.',
    measure: 'Mean edge length in rank-pitch units.',
    // Corpus worst 6.86: one-region stages cost edge length, the tension this limit holds.
    limit: 7,
  },
  {
    id: 'reading-order',
    statement: 'The first card is the start of the flow.',
    measure: 'Cards placed before the start Step.',
    limit: 0,
  },
  {
    id: 'clear-lines',
    statement: 'No line passes under a card it does not join.',
    measure: 'Line segments crossing a card other than their own ends.',
    limit: 0,
  },
] as const

export interface LayoutMetrics {
  collisions: number
  /** Worst region's largest across-axis hole in card widths, or 0 with no region. */
  regionHole: number
  straddledGroups: number
  edgeSpan: number
  beforeStart: number
  linesUnderCards: number
}

export type Violation = { id: PrincipleId; value: number; limit: number }

const METRIC_OF: Record<PrincipleId, keyof LayoutMetrics> = {
  'no-collision': 'collisions',
  'tight-regions': 'regionHole',
  'group-cohesion': 'straddledGroups',
  'short-edges': 'edgeSpan',
  'reading-order': 'beforeStart',
  'clear-lines': 'linesUnderCards',
}

/** Which principles this scene breaks. Empty is the gate every corpus flow has to pass. */
export function violations(flow: PocFlow, scene: Scene, direction: Direction): Violation[] {
  const metrics = measureScene(flow, scene, direction)
  return LAYOUT_PRINCIPLES.filter((principle) => metrics[METRIC_OF[principle.id]] > principle.limit)
    .map((principle) => ({
      id: principle.id,
      value: round(metrics[METRIC_OF[principle.id]]),
      limit: principle.limit,
    }))
}

/**
 * Direction is required, not inferred.
 *
 * Ranks run down the page in 'tb' and across it in 'lr', so a metric that assumed one axis
 * reported nonsense for half the corpus.
 */
export function measureScene(flow: PocFlow, scene: Scene, direction: Direction): LayoutMetrics {
  const cards = scene.boxes.filter((box) => box.kind === 'step' || box.kind === 'stage')
  const axes = direction === 'lr'
    ? {
      along: (card: Box) => card.x,
      extent: (card: Box) => card.w,
      across: (card: Box) => card.y,
      acrossExtent: (card: Box) => card.h,
      spanAlong: (rect: Rect) => ({ min: rect.x, max: rect.x + rect.w }),
    }
    : {
      along: (card: Box) => card.y,
      extent: (card: Box) => card.h,
      across: (card: Box) => card.x,
      acrossExtent: (card: Box) => card.w,
      spanAlong: (rect: Rect) => ({ min: rect.y, max: rect.y + rect.h }),
    }
  return {
    collisions: countCollisions(cards),
    regionHole: worstRegionHole(scene.bands, cards, axes.across, axes.acrossExtent),
    straddledGroups: countStraddledGroups(scene.bands, cards, axes.across, axes.spanAlong),
    edgeSpan: meanEdgeSpan(scene, cards, axes.along),
    beforeStart: countBeforeStart(flow, cards, axes.along, axes.extent),
    linesUnderCards: countLinesUnderCards(scene, cards),
  }
}

type Reader = (card: Box) => number

function countCollisions(cards: readonly Box[]): number {
  let hits = 0
  for (let a = 0; a < cards.length; a++) {
    for (let b = a + 1; b < cards.length; b++) {
      if (overlaps(cards[a] as Box, cards[b] as Box)) hits++
    }
  }
  return hits
}

function overlaps(one: Rect, two: Rect): boolean {
  return one.x < two.x + two.w && two.x < one.x + one.w
    && one.y < two.y + two.h && two.y < one.y + one.h
}

interface Rect { x: number; y: number; w: number; h: number }

/**
 * The widest sideways gap inside a region, divided by the narrowest card in it.
 *
 * Relative to a card because that is what makes a gap read as a hole: a reader sees room for a
 * missing box, not a pixel count. Gaps along the ranks are the corridor a region is meant to cover.
 */
function worstRegionHole(
  bands: readonly Band[],
  cards: readonly Box[],
  across: Reader,
  acrossExtent: Reader,
): number {
  let worst = 0
  for (const region of bands) {
    const inside = cards.filter((card) => centreInside(card, region))
    if (inside.length < 2) continue
    const unit = Math.min(...inside.map(acrossExtent))
    if (unit <= 0) continue
    const spans = inside
      .map((card) => ({ min: across(card), max: across(card) + acrossExtent(card) }))
      .sort((left, right) => left.min - right.min)
    let reach = -Infinity
    for (const span of spans) {
      if (reach > -Infinity && span.min > reach) worst = Math.max(worst, (span.min - reach) / unit)
      reach = Math.max(reach, span.max)
    }
  }
  return worst
}

function centreInside(card: Box, region: Rect): boolean {
  const cx = card.x + card.w / 2
  const cy = card.y + card.h / 2
  return cx >= region.x && cx <= region.x + region.w
    && cy >= region.y && cy <= region.y + region.h
}

/**
 * One group drawn as two regions side by side, at the same point in the flow.
 *
 * That is the shape a reader cannot explain: the same label twice at one rank with a lane between.
 * Members genuinely ranks apart are fine, and stay counted as separate places.
 */
function countStraddledGroups(
  bands: readonly Band[],
  cards: readonly Box[],
  across: Reader,
  spanAlong: (rect: Rect) => { min: number; max: number },
): number {
  const far = Math.max(...cards.map(across), 0)
  const regions = bands
        .map((band) => {
      const inside = cards.filter((card) => centreInside(card, band))
      return {
        label: band.label as string,
        lane: inside.some((card) => across(card) >= far) ? 1 : 0,
        span: spanAlong(band),
      }
    })
  let straddles = 0
  for (let a = 0; a < regions.length; a++) {
    for (let b = a + 1; b < regions.length; b++) {
      const one = regions[a] as (typeof regions)[number]
      const two = regions[b] as (typeof regions)[number]
      if (one.label !== two.label || one.lane === two.lane) continue
      if (one.span.min < two.span.max && two.span.min < one.span.max) straddles++
    }
  }
  return straddles
}

/**
 * Length per edge, divided by the pitch between neighbouring ranks.
 *
 * Normalised so the number means the same thing for a 6-step flow and a 30-step one.
 */
function meanEdgeSpan(scene: Scene, cards: readonly Box[], along: Reader): number {
  const centres = new Map(cards.map((card) => [card.id, {
    x: card.x + card.w / 2,
    y: card.y + card.h / 2,
  }]))
  const spans = scene.links
    .filter((link) => centres.has(link.from) && centres.has(link.to) && link.from !== link.to)
    .map((link) => {
      const from = centres.get(link.from) as { x: number; y: number }
      const to = centres.get(link.to) as { x: number; y: number }
      return Math.abs(from.x - to.x) + Math.abs(from.y - to.y)
    })
  if (spans.length === 0) return 0
  const pitch = rankPitch(cards, along)
  const mean = spans.reduce((sum, span) => sum + span, 0) / spans.length
  return pitch <= 0 ? 0 : mean / pitch
}

/**
 * The TYPICAL gap between neighbouring rows, taken as the median.
 *
 * Not the minimum: a wrapped rank sits deliberately close, and dividing by that made a normal
 * drawing look as though every edge ran the length of the page.
 */
function rankPitch(cards: readonly Box[], along: Reader): number {
  const rows = [...new Set(cards.map((card) => Math.round(along(card))))].sort((a, b) => a - b)
  if (rows.length < 2) return 0
  const gaps = rows.slice(1)
    .map((row, index) => row - (rows[index] as number))
    .filter((gap) => gap > 0)
    .sort((left, right) => left - right)
  if (gaps.length === 0) return 0
  return gaps[Math.floor(gaps.length / 2)] as number
}

function countBeforeStart(
  flow: PocFlow,
  cards: readonly Box[],
  along: Reader,
  extent: Reader,
): number {
  const startId = flow.startStepId ?? flow.steps.find((step) => step.isStart)?.id
  const start = cards.find((card) => card.id === startId) ?? cards.find((card) => card.isStart === true)
  if (start === undefined) return 0
  return cards.filter((card) => along(card) + extent(card) <= along(start)).length
}

/** Segments that cross a card other than their own ends, inset 2px so touching an edge is not crossing. */
function countLinesUnderCards(scene: Scene, cards: readonly Box[]): number {
  let hits = 0
  for (const link of scene.links) {
    for (let i = 1; i < link.points.length; i++) {
      const a = link.points[i - 1] as { x: number; y: number }
      const b = link.points[i] as { x: number; y: number }
      const segment = {
        x: Math.min(a.x, b.x),
        y: Math.min(a.y, b.y),
        w: Math.abs(a.x - b.x),
        h: Math.abs(a.y - b.y),
      }
      for (const card of cards) {
        if (card.id === link.from || card.id === link.to) continue
        const inner = { x: card.x + 2, y: card.y + 2, w: card.w - 4, h: card.h - 4 }
        if (segment.x <= inner.x + inner.w && inner.x <= segment.x + segment.w
          && segment.y <= inner.y + inner.h && inner.y <= segment.y + segment.h) hits++
      }
    }
  }
  return hits
}

function round(value: number): number {
  return Math.round(value * 1000) / 1000
}
