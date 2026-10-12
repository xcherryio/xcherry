// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import type { ELK as ElkEngine, ElkNode, LayoutOptions } from 'elkjs/lib/elk-api'
import type { Direction } from './types'

/** ELK layered placement: orthogonal lines clear of cards, ranks pinned, long Flows never folded. */

export interface PlacedInput {
  id: string
  w: number
  h: number
  /** The longest-path rank, which ELK keeps as the node's layer. */
  rank: number
  /** Members of one group sort together inside a layer; reading order breaks ties. */
  order: number
}

export interface PlacedEdge {
  id: string
  from: string
  to: string
  label?: string
}

/** An open stage: its members are laid out inside one box, so its band is one region. */
export interface PlacedContainer {
  id: string
  members: string[]
}

export interface Placement {
  positions: Map<string, { x: number; y: number }>
  /** Each container's box, by container id. */
  boxes: Map<string, { x: number; y: number; w: number; h: number }>
  routes: Map<string, { x: number; y: number }[]>
  /** The centre of each placed label, by edge id. */
  labels: Map<string, { x: number; y: number }>
  width: number
  height: number
  /** Why every ELK pass failed, when a plain grid drew the Flow instead. */
  degraded?: string
}

/** Below this zoom a card title is under 10px on screen, so a larger drawing opens on its start. */
export const READABLE_ZOOM = 0.74

const CONTAINER_PADDING = '[top=40,left=16,bottom=16,right=16]'

let engine: Promise<ElkEngine> | null = null

function elk(): Promise<ElkEngine> {
  engine ??= import('elkjs/lib/elk.bundled.js').then(({ default: ELK }) => new ELK())
  return engine
}

function options(direction: Direction, isPinned: boolean): LayoutOptions {
  return {
    'elk.algorithm': 'layered',
    'elk.direction': direction === 'lr' ? 'RIGHT' : 'DOWN',
    'elk.edgeRouting': 'ORTHOGONAL',
    'elk.spacing.nodeNode': '36',
    'elk.layered.spacing.nodeNodeBetweenLayers': '72',
    'elk.spacing.edgeNode': '18',
    'elk.spacing.edgeEdge': '10',
    'elk.layered.spacing.edgeNodeBetweenLayers': '18',
    'elk.layered.nodePlacement.strategy': 'BRANDES_KOEPF',
    'elk.layered.nodePlacement.favorStraightEdges': 'true',
    'elk.layered.considerModelOrder.strategy': 'NODES_AND_EDGES',
    'elk.layered.thoroughness': '40',
    'elk.edgeLabels.placement': 'CENTER',
    'elk.edgeLabels.inline': 'true',
    'elk.padding': '[top=44,left=40,bottom=40,right=40]',
    ...(isPinned
      ? { 'elk.layered.layering.strategy': 'INTERACTIVE', 'elk.layered.cycleBreaking.strategy': 'INTERACTIVE' }
      : { 'elk.layered.cycleBreaking.strategy': 'MODEL_ORDER' }),
  }
}

/** About what a 10.5px label takes. */
function labelSize(text: string): { width: number; height: number } {
  return { width: Math.ceil(text.length * 6.2) + 12, height: 16 }
}

function graph(
  nodes: PlacedInput[],
  edges: PlacedEdge[],
  containers: PlacedContainer[],
  direction: Direction,
  isPinned: boolean,
): ElkNode {
  // Interactive layering reads each node's rank from its position along the reading axis.
  const along = (rank: number): { x: number; y: number } =>
    direction === 'lr' ? { x: rank * 400, y: 0 } : { x: 0, y: rank * 400 }
  const leaf = (n: PlacedInput): ElkNode => ({ id: n.id, width: n.w, height: n.h, ...(isPinned ? along(n.rank) : {}) })
  const sorted = [...nodes].sort((a, b) => a.order - b.order)
  const inside = new Map(containers.flatMap((c) => c.members.map((id) => [id, c.id] as const)))
  // A container takes its first member's place in the model order.
  const children: ElkNode[] = []
  for (const n of sorted) {
    const owner = inside.get(n.id)
    if (owner === undefined) children.push(leaf(n))
    else if (!children.some((c) => c.id === owner)) {
      const members = sorted.filter((m) => inside.get(m.id) === owner)
      children.push({
        id: owner,
        // Only the root may set model order; repeating it on a container makes ELK throw.
        layoutOptions: { 'elk.padding': CONTAINER_PADDING },
        children: members.map(leaf),
        ...(isPinned ? along(Math.min(...members.map((m) => m.rank))) : {}),
      })
    }
  }
  return {
    id: 'flow',
    layoutOptions: {
      ...options(direction, isPinned),
      ...(containers.length > 0 ? { 'elk.hierarchyHandling': 'INCLUDE_CHILDREN' } : {}),
    },
    children,
    edges: edges.map((e) => ({
      id: e.id,
      sources: [e.from],
      targets: [e.to],
      ...(e.label ? { labels: [{ id: `${e.id}:label`, text: e.label, ...labelSize(e.label) }] } : {}),
    })),
  }
}

/** Absolute positions: ELK places members and routes, relative to their container. */
function placementOf(laid: ElkNode): Placement {
  const positions = new Map<string, { x: number; y: number }>()
  const boxes = new Map<string, { x: number; y: number; w: number; h: number }>()
  const origin = new Map<string, { x: number; y: number }>([[laid.id, { x: 0, y: 0 }]])
  const walk = (parent: ElkNode, ox: number, oy: number): void => {
    for (const c of parent.children ?? []) {
      const at = { x: ox + (c.x ?? 0), y: oy + (c.y ?? 0) }
      if (c.children && c.children.length > 0) {
        boxes.set(c.id, { ...at, w: c.width ?? 0, h: c.height ?? 0 })
        origin.set(c.id, at)
        walk(c, at.x, at.y)
      } else positions.set(c.id, at)
    }
  }
  walk(laid, 0, 0)
  const routes = new Map<string, { x: number; y: number }[]>()
  const labels = new Map<string, { x: number; y: number }>()
  for (const e of laid.edges ?? []) {
    const shift = origin.get((e as { container?: string }).container ?? laid.id) ?? { x: 0, y: 0 }
    const move = (p: { x: number; y: number }): { x: number; y: number } => ({ x: p.x + shift.x, y: p.y + shift.y })
    const section = e.sections?.[0]
    if (section) routes.set(e.id, [section.startPoint, ...(section.bendPoints ?? []), section.endPoint].map(move))
    const label = e.labels?.[0]
    if (label) {
      labels.set(
        e.id,
        move({ x: (label.x ?? 0) + (label.width ?? 0) / 2, y: (label.y ?? 0) + (label.height ?? 0) / 2 }),
      )
    }
  }
  return { positions, boxes, routes, labels, width: laid.width ?? 0, height: laid.height ?? 0 }
}

/** A plain grid by rank with straight lines: what the canvas draws if every ELK pass fails. */
function rankGrid(nodes: PlacedInput[], edges: PlacedEdge[], direction: Direction): Placement {
  const lr = direction === 'lr'
  const lanes = new Map<number, number>()
  const positions = new Map<string, { x: number; y: number }>()
  for (const n of [...nodes].sort((a, b) => a.order - b.order)) {
    const lane = lanes.get(n.rank) ?? 0
    lanes.set(n.rank, lane + 1)
    positions.set(
      n.id,
      lr ? { x: 40 + n.rank * 320, y: 44 + lane * 150 } : { x: 40 + lane * 290, y: 44 + n.rank * 170 },
    )
  }
  const size = new Map(nodes.map((n) => [n.id, n]))
  const centre = (id: string): { x: number; y: number } => {
    const p = positions.get(id) ?? { x: 0, y: 0 }
    const n = size.get(id)
    return { x: p.x + (n?.w ?? 0) / 2, y: p.y + (n?.h ?? 0) / 2 }
  }
  const routes = new Map(edges.map((e) => [e.id, [centre(e.from), centre(e.to)]]))
  const width = Math.max(...nodes.map((n) => (positions.get(n.id)?.x ?? 0) + n.w)) + 40
  const height = Math.max(...nodes.map((n) => (positions.get(n.id)?.y ?? 0) + n.h)) + 40
  return { positions, boxes: new Map(), routes, labels: new Map(), width, height }
}

/** Each failed pass falls back to a simpler one, the last to a grid, so nothing blanks. */
export async function placeGraph(
  nodes: PlacedInput[],
  edges: PlacedEdge[],
  direction: Direction,
  containers: PlacedContainer[] = [],
): Promise<Placement> {
  if (nodes.length === 0) {
    return { positions: new Map(), boxes: new Map(), routes: new Map(), labels: new Map(), width: 0, height: 0 }
  }
  let layout: ElkEngine
  try {
    layout = await elk()
  } catch (error: unknown) {
    return { ...rankGrid(nodes, edges, direction), degraded: String(error) }
  }
  let failure = ''
  const attempt = async (isPinned: boolean, boxed = containers): Promise<Placement | null> => {
    try {
      return placementOf(await layout.layout(graph(nodes, edges, boxed, direction, isPinned)))
    } catch (error: unknown) {
      failure ||= error instanceof Error ? error.message : String(error)
      return null
    }
  }
  // Without containers the groups fall back to bands that hug their members, before any grid.
  const placed =
    (await attempt(true)) ??
    (await attempt(false)) ??
    (containers.length > 0 ? ((await attempt(true, [])) ?? (await attempt(false, []))) : null)
  return placed ?? { ...rankGrid(nodes, edges, direction), degraded: failure }
}
