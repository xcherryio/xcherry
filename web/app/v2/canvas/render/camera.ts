// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import { joinedSteps, type PocFlow } from '../model/pocFlow'
import { groupOfStep } from '../views/controlTopology'
import type { StepGroup } from '../views/groups'
import { stageId } from '../views/stageCard'
import type { Box, Direction, Scene } from '../views/types'
import { OPEN_FLOOR } from './zoom'

/** Where the camera looks: React Flow's viewport, in screen pixels and zoom. */
export type CameraView = { x: number; y: number; zoom: number }

/** The part of the pane the drawing may use, in screen pixels. */
export type CameraRoom = { left: number; top: number; width: number; height: number }

type Rect = { x: number; y: number; w: number; h: number }

/** Cards of the Steps a transition joins to the framed Step, and of Steps joined to those. */
export type FramingNeighbours = { near: readonly string[]; twoAway: readonly string[] }

/** The cards of the Steps one and two transitions from `stepId`: its own card, or its closed stage's. */
export function framingNeighbours(flow: PocFlow, groups: StepGroup[], scene: Scene, stepId: string): FramingNeighbours {
  const drawn = new Set(scene.boxes.map((box) => box.id))
  const cardsOf = (stepIds: string[]): string[] => [
    ...new Set(
      stepIds.flatMap((id) => {
        const group = drawn.has(id) ? undefined : groupOfStep(groups, id)
        const card = group ? stageId(group.id) : id
        return drawn.has(card) ? [card] : []
      }),
    ),
  ]
  const near = joinedSteps(flow, stepId)
  return { near: cardsOf(near), twoAway: cardsOf(near.flatMap((id) => joinedSteps(flow, id))) }
}

/** How much of the drawing a framing shows, widest first. */
export type FramingScope = 'whole' | 'start' | 'two-steps-away' | 'connected' | 'nearest'

/** Focus never magnifies: a focused Step shows at its drawn size at most. */
export const FOCUS_MAX_ZOOM = 1

/** Wider context is worth this much of the focused Step's size, and no more. */
export const CONTEXT_ZOOM_SHARE = 0.8

/** Scene units kept clear around a framed rectangle. */
const FRAME_MARGIN = 40

/** Camera moves glide with the cards: one duration, one easing. */
export const CAMERA_MS = 320

/** Ease-out cubic; the stylesheet's `--ease-glide` is the same curve. */
export function easeGlide(t: number): number {
  return 1 - (1 - t) ** 3
}

/** True when the reader asked the system for less motion. */
export function prefersReducedMotion(): boolean {
  return typeof window !== 'undefined' && window.matchMedia?.('(prefers-reduced-motion: reduce)').matches === true
}

/** Viewport animation options: eased and direct, or instant under reduced motion. */
export function cameraMotion(durationMs = CAMERA_MS) {
  return {
    duration: prefersReducedMotion() ? 0 : durationMs,
    ease: easeGlide,
    interpolate: 'linear' as const,
  }
}

/** The pane minus the margins and overlays the drawing must stay clear of. */
export function cameraRoom(paneWidth: number, paneHeight: number, insetRightPx: number | null | undefined): CameraRoom {
  const right = (insetRightPx ?? 0) + paneWidth * 0.04
  const top = paneHeight * 0.08
  const left = paneWidth * 0.04
  return {
    left,
    top,
    width: Math.max(1, paneWidth - left - right),
    height: Math.max(1, paneHeight - top - paneHeight * 0.04),
  }
}

function bounds(rects: readonly Rect[], margin: number): Rect {
  const x = Math.min(...rects.map((r) => r.x)) - margin
  const y = Math.min(...rects.map((r) => r.y)) - margin
  const right = Math.max(...rects.map((r) => r.x + r.w)) + margin
  const bottom = Math.max(...rects.map((r) => r.y + r.h)) + margin
  return { x, y, w: right - x, h: bottom - y }
}

function zoomToFit(rect: Rect, room: CameraRoom): number {
  return Math.min(room.width / rect.w, room.height / rect.h)
}

/** Centres a rectangle in the room at a zoom. */
export function viewCentredOn(rect: Rect, room: CameraRoom, zoom: number): CameraView {
  return {
    x: room.left + room.width / 2 - (rect.x + rect.w / 2) * zoom,
    y: room.top + room.height / 2 - (rect.y + rect.h / 2) * zoom,
    zoom,
  }
}

/** Everything drawn: cards and open stages' bands. */
function drawingBounds(scene: Scene): Rect {
  const rects: Rect[] = [...scene.boxes, ...scene.bands]
  return rects.length === 0 ? { x: 0, y: 0, w: scene.width, h: scene.height } : bounds(rects, FRAME_MARGIN)
}

/** The card a Step is drawn as: its own, or the closed stage that holds it. */
export function boxShowing(scene: Scene, stepId: string): Box | undefined {
  return scene.boxes.find((box) => box.id === stepId || (box.holds ?? []).includes(stepId))
}

/** Cards joined to `boxIds` by a line or a detour, and the `transitionBoxIds` that chips may stand for. */
function joinedTo(scene: Scene, boxIds: ReadonlySet<string>, transitionBoxIds: readonly string[]): Box[] {
  const joined = new Set<string>()
  for (const link of scene.links) {
    if (boxIds.has(link.from)) joined.add(link.to)
    if (boxIds.has(link.to)) joined.add(link.from)
  }
  for (const box of scene.boxes) if (box.detourOf !== undefined && boxIds.has(box.detourOf)) joined.add(box.id)
  for (const id of transitionBoxIds) joined.add(id)
  return scene.boxes.filter((box) => joined.has(box.id) && !boxIds.has(box.id))
}

/** Opens on the whole drawing when it reads, else on its start at the readable floor. */
export function openingView(
  scene: Scene,
  room: CameraRoom,
  direction: Direction,
  maxZoom: number,
): { view: CameraView; scope: FramingScope } {
  const whole = drawingBounds(scene)
  const fit = zoomToFit(whole, room)
  const start = scene.boxes.find((box) => box.isStart) ?? scene.boxes[0]
  if (fit >= OPEN_FLOOR || start === undefined) {
    return { view: viewCentredOn(whole, room, Math.min(maxZoom, fit)), scope: 'whole' }
  }
  // Too large to read whole: open at the readable floor with the start in view; focusView frames "now".
  const zoom = OPEN_FLOOR
  const across = (low: number, span: number, roomSpan: number, at: number): number =>
    span * zoom <= roomSpan
      ? (roomSpan - span * zoom) / 2 - low * zoom
      : Math.max(roomSpan - (low + span) * zoom, Math.min(-low * zoom, roomSpan / 2 - at * zoom))
  const view =
    direction === 'lr'
      ? {
          x: room.left - (start.x - FRAME_MARGIN) * zoom,
          y: room.top + across(whole.y, whole.h, room.height, start.y + start.h / 2),
          zoom,
        }
      : {
          x: room.left + across(whole.x, whole.w, room.width, start.x + start.w / 2),
          y: room.top - (start.y - FRAME_MARGIN) * zoom,
          zoom,
        }
  return { view, scope: 'start' }
}

/** Shows a Step legibly with every card joined to it, widening while that keeps 80% of its size. */
export function focusView(
  scene: Scene,
  stepId: string,
  neighbours: FramingNeighbours,
  room: CameraRoom,
  wholeMaxZoom: number,
): { view: CameraView; scope: FramingScope } | null {
  const focus = boxShowing(scene, stepId)
  if (focus === undefined) return null
  const near = joinedTo(scene, new Set([focus.id]), neighbours.near)
  const connected = bounds([focus, ...near], FRAME_MARGIN)
  const connectedZoom = Math.min(FOCUS_MAX_ZOOM, zoomToFit(connected, room))
  if (connectedZoom < OPEN_FLOOR) {
    const centre = (box: Box): number => Math.hypot(box.x + box.w / 2 - (focus.x + focus.w / 2), box.y + box.h / 2 - (focus.y + focus.h / 2))
    const kept: Box[] = [focus]
    for (const box of [...near].sort((a, b) => centre(a) - centre(b))) {
      if (zoomToFit(bounds([...kept, box], FRAME_MARGIN), room) >= OPEN_FLOOR) kept.push(box)
    }
    const nearest = bounds(kept, FRAME_MARGIN)
    return { view: viewCentredOn(nearest, room, Math.min(FOCUS_MAX_ZOOM, Math.max(OPEN_FLOOR, zoomToFit(nearest, room)))), scope: 'nearest' }
  }
  const enough = Math.max(OPEN_FLOOR, CONTEXT_ZOOM_SHARE * connectedZoom)
  const whole = drawingBounds(scene)
  if (zoomToFit(whole, room) >= enough) {
    return { view: viewCentredOn(whole, room, Math.min(wholeMaxZoom, zoomToFit(whole, room))), scope: 'whole' }
  }
  const farther = joinedTo(scene, new Set([focus.id, ...near.map((box) => box.id)]), neighbours.twoAway)
  const twoAway = bounds([focus, ...near, ...farther], FRAME_MARGIN)
  if (zoomToFit(twoAway, room) >= enough) {
    return { view: viewCentredOn(twoAway, room, Math.min(FOCUS_MAX_ZOOM, zoomToFit(twoAway, room))), scope: 'two-steps-away' }
  }
  return { view: viewCentredOn(connected, room, connectedZoom), scope: 'connected' }
}

/** Whether a card is wholly on screen in a view, with a margin. */
export function isBoxInView(box: Box, view: CameraView, paneWidth: number, paneHeight: number): boolean {
  const margin = 16
  const left = box.x * view.zoom + view.x
  const top = box.y * view.zoom + view.y
  return (
    left >= margin &&
    top >= margin &&
    left + box.w * view.zoom <= paneWidth - margin &&
    top + box.h * view.zoom <= paneHeight - margin
  )
}

/** Entrance pacing: each step of the trace waits this long, and the whole trace at most this long. */
const TRACE_STEP_MS = 70
const TRACE_SPAN_MS = 360
/** A line starts drawing this long after the card it leaves. */
const LINE_LAG_MS = 110

/** When each card, band and line enters, in milliseconds. */
export type EntranceDelays = ReadonlyMap<string, number>

/** Cards enter as a trace from the start reaches them along the lines; lines draw out of their cards. */
export function entranceDelays(scene: Scene, direction: Direction): EntranceDelays {
  const depth = new Map<string, number>()
  const starts = scene.boxes.filter((box) => box.isStart)
  const queue = (starts.length > 0 ? starts : scene.boxes.slice(0, 1)).map((box) => box.id)
  for (const id of queue) depth.set(id, 0)
  for (let i = 0; i < queue.length; i += 1) {
    const id = queue[i] as string
    for (const link of scene.links) {
      if (link.from !== id || depth.has(link.to)) continue
      depth.set(link.to, (depth.get(id) ?? 0) + 1)
      queue.push(link.to)
    }
  }
  const lead = (box: Box): number => (direction === 'lr' ? box.x : box.y)
  const deepest = Math.max(0, ...depth.values())
  for (const box of [...scene.boxes].sort((a, b) => lead(a) - lead(b))) {
    if (depth.has(box.id)) continue
    const parent = box.detourOf === undefined ? undefined : depth.get(box.detourOf)
    depth.set(box.id, parent !== undefined ? parent + 1 : deepest + 1)
  }
  const levels = Math.max(1, ...depth.values())
  const step = Math.min(TRACE_STEP_MS, TRACE_SPAN_MS / levels)
  const delays = new Map<string, number>()
  for (const [id, level] of depth) delays.set(id, Math.round(level * step))
  for (const band of scene.bands) {
    const inside = scene.boxes.filter(
      (box) => box.x >= band.x && box.y >= band.y && box.x + box.w <= band.x + band.w && box.y + box.h <= band.y + band.h,
    )
    const first = Math.min(...inside.map((box) => delays.get(box.id) ?? 0))
    delays.set(`band:${band.id}`, Number.isFinite(first) ? Math.max(0, first - step) : 0)
  }
  for (const link of scene.links) delays.set(link.id, (delays.get(link.from) ?? 0) + LINE_LAG_MS)
  delays.set('__legend', Math.round((levels + 1) * step))
  return delays
}

/** How long the entrance runs, so the delays can be dropped once it is over. */
export function entranceDuration(delays: EntranceDelays): number {
  return Math.max(0, ...delays.values()) + 420
}

/** A routed line's length, for drawing it in. */
export function polylineLength(points: readonly { x: number; y: number }[]): number {
  let length = 0
  for (let i = 1; i < points.length; i += 1) {
    const a = points[i - 1] as { x: number; y: number }
    const b = points[i] as { x: number; y: number }
    length += Math.hypot(b.x - a.x, b.y - a.y)
  }
  return Math.ceil(length) + 2
}
