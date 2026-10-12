// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import type { EntryModel, EntryView, PocFlow, ResourceModel, ResourceRef } from '../model/pocFlow'
import { resourceById } from '../model/pocFlow'
import type { Box, BoxRow, Scene, ViewOpts } from './types'
import { boundsOf } from './types'

/** A Flow with no Steps is drawn as one card per RPC, listing the state it touches. */

/** Wider than a Step card, because an RPC name carries a verb, a noun, and the envelope glyph. */
export const RPC_CARD_W = 280

const CARD_GAP = 34
const MARGIN = 34

const CARD_MIN_H = 62

/** The height an RPC card needs for its head and rows. */
function anatomyHeight(rowCount: number): number {
  /*
   * Scaled with the type, 1.2x, because every one of these is a measurement OF the type: `head` is a
   * title line over a subtitle line, a row is one line of row text, a section is a label over its rows.
   * Leaving them while the CSS grew would have clipped the text the card is sized to hold.
   */
  const head = 59
  return head + rowCount * 20
}

type Access = ResourceRef['access']

const ACCESS_ORDER: Access[] = ['read', 'write', 'publish', 'lock']

const ACCESS_VERB: Record<Access, string> = {
  read: 'reads',
  write: 'writes',
  publish: 'publishes to',
  lock: 'locks',
}

// Hollow reads and filled writes, from the Geometric Shapes block the card glyphs already use.
const ACCESS_GLYPH: Record<Access, string> = {
  read: '▫',
  write: '▪',
  publish: '✉',
  lock: '▣',
}

const VIEW_NAME: Record<EntryView, string> = {
  summary: 'Summary',
  display: 'Display',
}

const RESOURCE_KIND_NAME: Record<ResourceModel['kind'], string> = {
  attribute: 'Attribute',
  channel: 'Channel',
  stream: 'Stream',
}

interface AccessedResource {
  name: string
  /** Set only when another resource of the Flow has the same name. */
  kindName?: string
}

export interface RpcCardContent {
  subtitle: string
  rows: BoxRow[]
  rpcRole?: 'action' | 'view'
  height: number
}

/** Resources per access kind, in reading order, each list deduplicated by resource id and sorted by name. */
function resourcesByAccess(flow: PocFlow, entry: EntryModel): Map<Access, AccessedResource[]> {
  const nameCounts = new Map<string, number>()
  for (const resource of flow.resources) nameCounts.set(resource.name, (nameCounts.get(resource.name) ?? 0) + 1)

  const out = new Map<Access, AccessedResource[]>()
  for (const access of ACCESS_ORDER) {
    const ids = new Set(entry.resources.filter((ref) => ref.access === access).map((ref) => ref.resourceId))
    const resources = [...ids].map((id): AccessedResource => {
      const resource = resourceById(flow, id)
      if (resource === undefined) return { name: id }
      return (nameCounts.get(resource.name) ?? 0) > 1
        ? { name: resource.name, kindName: RESOURCE_KIND_NAME[resource.kind] }
        : { name: resource.name }
    })
    resources.sort((a, b) => a.name.localeCompare(b.name) || (a.kindName ?? '').localeCompare(b.kindName ?? ''))
    if (resources.length > 0) out.set(access, resources)
  }
  return out
}

function inlineName(resource: AccessedResource): string {
  return resource.kindName === undefined ? resource.name : `${resource.name} (${resource.kindName})`
}

/** About what one 13px row holds at RPC_CARD_W, measured in Chromium. */
const ROW_TEXT_CHARS = 36

/** Every name when they fit; otherwise the first name, with the count in the trail that never truncates. */
function accessRow(kind: Access, resources: AccessedResource[]): BoxRow {
  const names = resources.map(inlineName)
  const everyName = `${ACCESS_VERB[kind]} ${names.join(', ')}`
  if (names.length === 1 || everyName.length <= ROW_TEXT_CHARS) {
    return { glyph: ACCESS_GLYPH[kind], text: everyName, tone: 'quiet' }
  }
  return {
    glyph: ACCESS_GLYPH[kind],
    text: `${ACCESS_VERB[kind]} ${names[0]}`,
    trail: `+${names.length - 1} more`,
    tone: 'quiet',
  }
}

function changesState(access: Map<Access, AccessedResource[]>): boolean {
  return access.has('write') || access.has('publish') || access.has('lock')
}

/** Actions first, then other state changes, reads, and the Summary and Display views last. */
function readingRank(entry: EntryModel, access: Map<Access, AccessedResource[]>): number {
  if (entry.action !== undefined) return 0
  if (entry.views.includes('summary')) return 3
  if (entry.views.includes('display')) return 4
  return changesState(access) ? 1 : 2
}

export function rpcCardContent(flow: PocFlow, entry: EntryModel): RpcCardContent {
  const access = resourcesByAccess(flow, entry)
  const rows: BoxRow[] = []

  if (entry.action !== undefined) {
    const permission = entry.action.requiredPermission
    rows.push({
      glyph: '◈',
      text: permission === '' ? 'requires no permission' : `requires ${permission}`,
      tone: 'quiet',
    })
  }
  // One row per access kind; the panel lists every resource.
  for (const [kind, resources] of access) rows.push(accessRow(kind, resources))

  const subtitle =
    entry.action !== undefined
      ? `Action: ${entry.action.label}`
      : entry.views.length > 0
        ? `${entry.views.map((view) => VIEW_NAME[view]).join(' and ')} view`
        : changesState(access)
          ? 'changes state'
          : access.has('read')
            ? 'reads state'
            : 'reports no state access'
  const rpcRole =
    entry.action !== undefined ? ('action' as const) : entry.views.length > 0 ? ('view' as const) : undefined
  // `anatomyHeight` excludes the border: 2px on an Action card, 1px on every other.
  const borderH = rpcRole === 'action' ? 4 : 2

  return {
    subtitle,
    rows,
    ...(rpcRole === undefined ? {} : { rpcRole }),
    height: Math.max(CARD_MIN_H, anatomyHeight(rows.length) + borderH),
  }
}

/** Lanes hold about √n cards; a lane is a row top-down and a column left-right. */
export function rpcCardsLayout(flow: PocFlow, opts: ViewOpts): Scene {
  const cards = flow.entries
    .filter((entry) => entry.kind === 'rpc')
    .map((entry) => ({
      entry,
      rank: readingRank(entry, resourcesByAccess(flow, entry)),
      content: rpcCardContent(flow, entry),
    }))
    .sort((a, b) => a.rank - b.rank || a.entry.name.localeCompare(b.entry.name))
  const lr = opts.direction === 'lr'
  const perLane = cards.length <= 3 ? cards.length : Math.ceil(Math.sqrt(cards.length))

  const boxes: Box[] = []
  let laneAt = MARGIN
  for (let first = 0; first < cards.length; first += perLane) {
    const lane = cards.slice(first, first + perLane)
    let at = MARGIN
    for (const { entry, content } of lane) {
      boxes.push({
        id: entry.id,
        x: lr ? laneAt : at,
        y: lr ? at : laneAt,
        w: RPC_CARD_W,
        h: content.height,
        kind: 'rpcCard',
        title: entry.name,
        subtitle: content.subtitle,
        rows: content.rows,
        ...(content.rpcRole === undefined ? {} : { rpcRole: content.rpcRole }),
      })
      at += (lr ? content.height : RPC_CARD_W) + CARD_GAP
    }
    laneAt += Math.max(...lane.map(({ content }) => (lr ? RPC_CARD_W : content.height))) + CARD_GAP
  }

  const notes = [
    `This Flow has no Steps, so each of its ${cards.length} RPC${cards.length === 1 ? '' : 's'} is drawn as a card.`,
  ]
  const timeoutHandlers = flow.entries.filter((entry) => entry.kind === 'timeoutHandler').length
  if (timeoutHandlers > 0) {
    notes.push(`${timeoutHandlers} timeout handler${timeoutHandlers === 1 ? ' is' : 's are'} not drawn.`)
  }
  if (flow.dropped.length > 0) notes.push(`${flow.dropped.length} wire elements not modelled.`)

  return { boxes, links: [], bands: [], ...boundsOf(boxes, []), notes }
}
