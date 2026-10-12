// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import type { Direction, Scene } from './types'

/** Keeps the reader's place when a stage opens or closes, instead of refitting the view. */

/** A card by its box id, or an open stage by its group's bands. */
export type AnchorRef = { box: string } | { group: string }

type Point = { x: number; y: number }

type Rect = { x: number; y: number; w: number; h: number }

/** The leading edge's middle, so a stage opens evenly to both sides of where its card was. */
function edgeMiddle(r: Rect, direction: Direction): Point {
  return direction === 'lr' ? { x: r.x, y: r.y + r.h / 2 } : { x: r.x + r.w / 2, y: r.y }
}

/** The rectangle a group's bands cover, to bring an opened stage into view. */
function groupRect(scene: Scene, groupId: string): Rect | undefined {
  const bands = scene.bands.filter((b) => b.groupId === groupId)
  if (bands.length === 0) return undefined
  const x = Math.min(...bands.map((b) => b.x))
  const y = Math.min(...bands.map((b) => b.y))
  return {
    x,
    y,
    w: Math.max(...bands.map((b) => b.x + b.w)) - x,
    h: Math.max(...bands.map((b) => b.y + b.h)) - y,
  }
}

export function rectOf(scene: Scene, ref: AnchorRef): Rect | undefined {
  if ('box' in ref) {
    const box = scene.boxes.find((b) => b.id === ref.box)
    return box ? { x: box.x, y: box.y, w: box.w, h: box.h } : undefined
  }
  return groupRect(scene, ref.group)
}

export function anchorOf(scene: Scene, ref: AnchorRef, direction: Direction): Point | undefined {
  const rect = rectOf(scene, ref)
  return rect ? edgeMiddle(rect, direction) : undefined
}

/** Every coordinate of a Scene moved by an offset; its size grows to keep the drawing inside. */
export function shiftScene(scene: Scene, offset: Point): Scene {
  if (offset.x === 0 && offset.y === 0) return scene
  const move = <T extends Point>(p: T): T => ({ ...p, x: p.x + offset.x, y: p.y + offset.y })
  return {
    ...scene,
    boxes: scene.boxes.map(move),
    bands: scene.bands.map(move),
    links: scene.links.map((link) => ({
      ...link,
      points: link.points.map(move),
      ...(link.label?.at ? { label: { ...link.label, at: move(link.label.at) } } : {}),
    })),
    width: scene.width + Math.max(0, offset.x),
    height: scene.height + Math.max(0, offset.y),
  }
}

/** Where every card sits, so a re-read that moved nothing animates nothing. */
export function placementKey(scene: Scene): string {
  // A failure detour opens beside its Step without moving anything, so its cards are not counted.
  return scene.boxes
    .filter((b) => b.detourOf === undefined)
    .map((b) => `${b.id}@${Math.round(b.x)},${Math.round(b.y)}`)
    .join('|')
}
