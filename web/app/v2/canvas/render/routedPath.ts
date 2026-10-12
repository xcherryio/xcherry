// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

export type Point = { x: number; y: number }

/** Orthogonal bends rounded at up to `radius`, never more than half of either segment. */
export function roundedPath(points: Point[], radius: number): string {
  const first = points[0] as Point
  let d = `M${first.x},${first.y}`
  for (let i = 1; i < points.length - 1; i += 1) {
    const a = points[i - 1] as Point
    const p = points[i] as Point
    const n = points[i + 1] as Point
    const into = Math.hypot(p.x - a.x, p.y - a.y)
    const out = Math.hypot(n.x - p.x, n.y - p.y)
    const r = Math.min(radius, into / 2, out / 2)
    if (r < 0.5) {
      d += ` L${p.x},${p.y}`
      continue
    }
    const before = { x: p.x - ((p.x - a.x) / into) * r, y: p.y - ((p.y - a.y) / into) * r }
    const after = { x: p.x + ((n.x - p.x) / out) * r, y: p.y + ((n.y - p.y) / out) * r }
    d += ` L${before.x.toFixed(1)},${before.y.toFixed(1)} Q${p.x},${p.y} ${after.x.toFixed(1)},${after.y.toFixed(1)}`
  }
  const last = points[points.length - 1] as Point
  return `${d} L${last.x},${last.y}`
}

/** The middle of the longest segment, where a label sits clear of bends. */
export function labelPoint(points: Point[]): Point {
  let best = { x: 0, y: 0, length: -1 }
  for (let i = 1; i < points.length; i += 1) {
    const a = points[i - 1] as Point
    const b = points[i] as Point
    const length = Math.hypot(b.x - a.x, b.y - a.y)
    if (length > best.length) best = { x: (a.x + b.x) / 2, y: (a.y + b.y) / 2, length }
  }
  return { x: best.x, y: best.y }
}
