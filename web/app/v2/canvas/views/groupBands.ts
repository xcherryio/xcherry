// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import type { StepGroup } from './groups'
import type { Band, Box } from './types'

/** Fallback when ELK cannot contain a group: bands hugging its members, never enclosing a stranger. */

const PAD = 12
/** Room above the members for the label strip. */
const LABEL_H = 28

type Rect = { x: number; y: number; w: number; h: number }

function around(members: Box[]): Rect {
  const x = Math.min(...members.map((b) => b.x)) - PAD
  const y = Math.min(...members.map((b) => b.y)) - LABEL_H
  return {
    x,
    y,
    w: Math.max(...members.map((b) => b.x + b.w)) + PAD - x,
    h: Math.max(...members.map((b) => b.y + b.h)) + PAD - y,
  }
}

function isOverlapping(r: Rect, b: Box): boolean {
  return b.x < r.x + r.w && b.x + b.w > r.x && b.y < r.y + r.h && b.y + b.h > r.y
}

/** One band per cluster of drawn members; clusters merge nearest first while enclosing no other box. */
export function groupBands(
  groups: StepGroup[],
  boxes: Box[],
  isDrawn: (group: StepGroup) => boolean,
  isCollapsible: (group: StepGroup) => boolean,
): Band[] {
  const bands: Band[] = []
  groups.forEach((group, index) => {
    if (!isDrawn(group)) return
    const owned = new Set(group.stepIds)
    const members = boxes.filter((b) => b.kind === 'step' && owned.has(b.id))
    if (members.length === 0) return
    const strangers = boxes.filter((b) => !owned.has(b.id))
    const isClean = (r: Rect): boolean => !strangers.some((b) => isOverlapping(r, b))
    const clusters = members.map((b) => [b])
    const centre = (c: Box[]): { x: number; y: number } => {
      const r = around(c)
      return { x: r.x + r.w / 2, y: r.y + r.h / 2 }
    }
    for (let isMerged = true; isMerged; ) {
      isMerged = false
      const pairs: [number, number, number][] = []
      for (let a = 0; a < clusters.length; a += 1) {
        for (let b = a + 1; b < clusters.length; b += 1) {
          const p = centre(clusters[a] as Box[])
          const q = centre(clusters[b] as Box[])
          pairs.push([a, b, Math.hypot(p.x - q.x, p.y - q.y)])
        }
      }
      for (const [a, b] of pairs.sort((x, y) => x[2] - y[2])) {
        const union = [...(clusters[a] as Box[]), ...(clusters[b] as Box[])]
        if (!isClean(around(union))) continue
        clusters.splice(b, 1)
        clusters[a] = union
        isMerged = true
        break
      }
    }
    for (const cluster of clusters) {
      const r = around(cluster)
      bands.push({
        id: `${group.id}:${Math.round(r.x)}:${Math.round(r.y)}`,
        groupId: group.id,
        ...r,
        label: group.label,
        hue: index % 2,
        collapsible: isCollapsible(group),
      })
    }
  })
  return bands
}
