// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import { useCallback, useEffect, useRef, useState, type RefObject } from 'react'
import type { Direction, Scene } from '../views/types'
import {
  boxShowing,
  cameraMotion,
  cameraRoom,
  entranceDelays,
  entranceDuration,
  focusView,
  isBoxInView,
  openingView,
  prefersReducedMotion,
  type CameraView,
  type EntranceDelays,
  type FramingNeighbours,
  type FramingScope,
} from './camera'

/** The viewport calls the camera makes; React Flow's instance provides them. */
type CameraViewport = {
  getViewport(): CameraView
  setViewport(view: CameraView, options?: ReturnType<typeof cameraMotion>): Promise<boolean>
}

/** Whose view is on screen: the camera's framing, the reader's Fit, or wherever the reader took it. */
export type ViewOwner = 'camera' | 'fit' | 'reader'

type Rect = { x: number; y: number; w: number; h: number }

/** A drawing that never settles still appears after this long. */
const SETTLE_PATIENCE_MS = 1500

/** Frames each subject once it settles, follows the run's Step, and keeps its framing until the reader takes over. */
export function useCamera({
  viewport,
  paneRef,
  scene,
  direction,
  isSettled,
  subjectKey,
  targetId,
  targetNeighbours,
  insetRightPx,
  wholeMaxZoom,
  fitWhole,
  keepSelectionInView,
  onFramed,
}: {
  viewport: CameraViewport
  paneRef: RefObject<HTMLDivElement | null>
  scene: Scene
  direction: Direction
  /** False while the drawing is still on its way, such as a run's history or a stage's layout. */
  isSettled: boolean
  /** Changes when what is shown changes: another run, view or direction. */
  subjectKey: string
  /** The Step to frame: a focus, else the run's current Step. */
  targetId: string | null
  targetNeighbours: FramingNeighbours
  insetRightPx: number | null | undefined
  wholeMaxZoom: number
  fitWhole: () => void
  /** Pans a reader's selection into view; false when there is none. */
  keepSelectionInView: () => boolean
  onFramed?: (scope: FramingScope) => void
}) {
  const [isEntered, setEntered] = useState(false)
  const [delays, setDelays] = useState<EntranceDelays | null>(null)
  const [isOutOfPatience, setOutOfPatience] = useState(false)
  const [paneTick, setPaneTick] = useState(0)
  const framedSubject = useRef<string | null>(null)
  const followedTarget = useRef<string | null>(null)
  const owner = useRef<ViewOwner>('camera')
  // Read when the view re-opens; re-opening on every run poll would fight the reader.
  const latest = useRef({
    scene,
    direction,
    subjectKey,
    targetId,
    targetNeighbours,
    insetRightPx,
    fitWhole,
    keepSelectionInView,
    onFramed,
  })
  useEffect(() => {
    latest.current = {
      scene,
      direction,
      subjectKey,
      targetId,
      targetNeighbours,
      insetRightPx,
      fitWhole,
      keepSelectionInView,
      onFramed,
    }
  })

  /** The drawing's own box: the pane also holds the padding above it. */
  const flowElement = useCallback(
    () => paneRef.current?.querySelector<HTMLElement>('.react-flow') ?? paneRef.current,
    [paneRef],
  )

  const frame = useCallback((): { view: CameraView; scope: FramingScope } | null => {
    const el = flowElement()
    if (el === null || el.clientWidth < 8 || el.clientHeight < 8) return null
    const { scene: shown, direction: reading, targetId: target, targetNeighbours: joined, insetRightPx: inset } = latest.current
    const room = cameraRoom(el.clientWidth, el.clientHeight, inset)
    return (
      (target === null ? null : focusView(shown, target, joined, room, wholeMaxZoom)) ??
      openingView(shown, room, reading, wholeMaxZoom)
    )
  }, [flowElement, wholeMaxZoom])

  useEffect(() => {
    if (isEntered || isSettled) return undefined
    const timer = window.setTimeout(() => setOutOfPatience(true), SETTLE_PATIENCE_MS)
    return () => window.clearTimeout(timer)
  }, [isEntered, isSettled])

  // Frame each settled subject after two quiet frames, so a drawer opening with it has sized the pane.
  useEffect(() => {
    const isReady = isSettled || (!isEntered && isOutOfPatience)
    if (!isReady || framedSubject.current === subjectKey) return undefined
    let raf = 0
    let quietFrames = 0
    const tick = (): void => {
      raf = requestAnimationFrame(() => {
        quietFrames += 1
        if (quietFrames < 2) {
          tick()
          return
        }
        const framed = frame()
        if (framed === null) return
        // An impatient entrance frames again once the drawing settles.
        framedSubject.current = isSettled ? subjectKey : null
        followedTarget.current = latest.current.targetId
        owner.current = 'camera'
        latest.current.onFramed?.(framed.scope)
        if (isEntered) {
          void viewport.setViewport(framed.view, cameraMotion())
          return
        }
        void viewport.setViewport(framed.view, { duration: 0, ease: (t) => t, interpolate: 'linear' })
        setDelays(prefersReducedMotion() ? null : entranceDelays(latest.current.scene, latest.current.direction))
        setEntered(true)
      })
    }
    tick()
    return () => cancelAnimationFrame(raf)
  }, [frame, isEntered, isOutOfPatience, isSettled, paneTick, subjectKey, viewport])

  useEffect(() => {
    if (delays === null) return undefined
    const timer = window.setTimeout(() => setDelays(null), entranceDuration(delays))
    return () => window.clearTimeout(timer)
  }, [delays])

  /** Pans the least that shows a rectangle whole; one larger than the pane shows its start. */
  const panToShow = useCallback(
    (r: Rect) => {
      const el = flowElement()
      if (el === null) return
      const { x, y, zoom } = viewport.getViewport()
      const margin = 24
      const shift = (start: number, size: number, room: number): number => {
        const a = start * zoom
        const b = a + size * zoom
        if (b - a > room - 2 * margin) return margin - a
        if (a < margin) return margin - a
        if (b > room - margin) return room - margin - b
        return 0
      }
      const dx = shift(r.x + x / zoom, r.w, el.clientWidth - (latest.current.insetRightPx ?? 0))
      const dy = shift(r.y + y / zoom, r.h, el.clientHeight)
      if (dx !== 0 || dy !== 0) void viewport.setViewport({ x: x + dx, y: y + dy, zoom }, cameraMotion())
    },
    [flowElement, viewport],
  )

  // As the run moves on, the camera's framing follows it; a reader's own view only pans to show it.
  useEffect(() => {
    if (!isEntered || framedSubject.current !== subjectKey || followedTarget.current === targetId) return
    followedTarget.current = targetId
    if (targetId === null) return
    if (owner.current === 'camera') {
      const framed = frame()
      if (framed !== null) void viewport.setViewport(framed.view, cameraMotion())
      return
    }
    const box = boxShowing(scene, targetId)
    const el = flowElement()
    if (box === undefined || el === null) return
    if (!isBoxInView(box, viewport.getViewport(), el.clientWidth - (insetRightPx ?? 0), el.clientHeight)) panToShow(box)
  }, [flowElement, frame, insetRightPx, isEntered, panToShow, scene, subjectKey, targetId, viewport])

  // Re-open when what is shown changes, never on zoom; a ResizeObserver knows when the pane is sized.
  // A resize keeps whatever owns the view: the camera reframes, Fit refits, a reader's selection stays in view.
  /* The Step panel never refits the view: the reader keeps their place, and the selection pans clear. */
  useEffect(() => {
    const el = paneRef.current
    if (el === null) return undefined
    let raf = 0
    const observer = new ResizeObserver(() => {
      cancelAnimationFrame(raf)
      raf = requestAnimationFrame(() => {
        // A subject still on its way frames itself once it settles, at the pane's new size.
        if (framedSubject.current !== latest.current.subjectKey) {
          setPaneTick((tick) => tick + 1)
          return
        }
        if (latest.current.keepSelectionInView()) return
        if (owner.current === 'fit') {
          latest.current.fitWhole()
          return
        }
        if (owner.current !== 'camera') return
        const framed = frame()
        if (framed !== null) void viewport.setViewport(framed.view, cameraMotion())
      })
    })
    observer.observe(el)
    return () => {
      cancelAnimationFrame(raf)
      observer.disconnect()
    }
  }, [frame, paneRef, viewport])

  const setOwner = useCallback((next: ViewOwner) => {
    owner.current = next
  }, [])

  return { isEntered, delays, panToShow, setOwner }
}

/** The last settled drawing and its "now", kept on screen while the next one is on its way. */
export function useSettledDrawing(isSettled: boolean, scene: Scene, nowNodeId: string | null) {
  const [held, setHeld] = useState({ scene, nowNodeId })
  if (isSettled && (held.scene !== scene || held.nowNodeId !== nowNodeId)) setHeld({ scene, nowNodeId })
  return isSettled ? { scene, nowNodeId } : held
}

/** Within one subject, the run moving on to its next Step travels the line between them, once. */
export function useRunTravel(scene: Scene, subjectKey: string, nowNodeId: string | null, isEntered: boolean) {
  const [travel, setTravel] = useState<{ linkId: string; nonce: number } | null>(null)
  const [traced, setTraced] = useState({ subject: subjectKey, now: nowNodeId })
  if (traced.subject !== subjectKey || traced.now !== nowNodeId) {
    const from = traced.now === null ? undefined : boxShowing(scene, traced.now)?.id
    const to = nowNodeId === null ? undefined : boxShowing(scene, nowNodeId)?.id
    const link =
      traced.subject === subjectKey && isEntered ? scene.links.find((l) => l.from === from && l.to === to) : undefined
    if (link !== undefined) setTravel({ linkId: link.id, nonce: (travel?.nonce ?? 0) + 1 })
    setTraced({ subject: subjectKey, now: nowNodeId })
  }
  useEffect(() => {
    if (travel === null) return undefined
    const timer = window.setTimeout(() => setTravel(null), 1400)
    return () => window.clearTimeout(timer)
  }, [travel])
  return travel
}
