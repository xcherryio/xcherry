// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import {
  Background,
  BackgroundVariant,
  BaseEdge,
  Controls,
  type Edge,
  type EdgeProps,
  Handle,
  type Node,
  type NodeProps,
  Position,
  ReactFlow,
  ReactFlowProvider,
  useReactFlow,
} from '@xyflow/react'
import {
  useCallback,
  useEffect,
  useImperativeHandle,
  useMemo,
  useRef,
  useState,
  type JSX,
  type KeyboardEvent,
  type Ref,
} from 'react'
import { placementKey } from '../views/anchor'
import type { Box, Direction, Scene } from '../views/types'
import { Glyph } from './Glyph'
import { LEGEND_BOX_H, LEGEND_W, legendAnchor } from './legendAnchor'
import type { CanvasViewportHandle } from './viewport'
import { EDGE_MARKER } from './markers'
import { NodeBox } from './NodeBox'
import { labelPoint, roundedPath, type Point } from './routedPath'
import { StageBox } from './StageBox'
import {
  chromeCompensation,
  edgeContrastBoost,
  fitOptions,
  FOCUS_ZOOM,
  MAX_ZOOM,
  MIN_ZOOM,
  OPEN_FLOOR,
} from './zoom'
import '@xyflow/react/dist/style.css'

/**
 * THE ONLY FILE THAT IMPORTS @xyflow.
 *
 * Everything above this consumes a Scene, which is plain geometry. Zoom here is PURELY GEOMETRIC —
 * level of detail arrives as a prop and is decided by an explicit control, never derived from the
 * viewport.
 */

type BoxData = {
  box: Box
  selected: boolean
  hovered: boolean
  referenced: boolean
  now: boolean
  onSelect?: (id: string, additive: boolean) => void
  onHover?: (id: string | null) => void
  onInspect?: (id: string) => void
  onOpen: (id: string) => void
  onJump: (id: string) => void
}

/**
 * Handles live here, not in `NodeBox`.
 *
 * React Flow anchors every edge to a Handle and silently drops any edge whose endpoint has none —
 * which is exactly what happened on the first run: correct geometry, correct edge list, and not one
 * arrow on screen.
 *
 * The routed edge draws the layout's own path, so these are anchors only.
 */
function BoxNode({ data }: NodeProps): JSX.Element {
  const d = data as unknown as BoxData
  return (
    <>
      <Handle type="target" position={Position.Top} className="phandle" />
      {d.box.kind === 'stage' ? (
        <StageBox box={d.box} now={d.now} onOpen={d.onOpen} onJump={d.onJump} />
      ) : (
        <NodeBox
          box={d.box}
          selected={d.selected}
          hovered={d.hovered}
          referenced={d.referenced}
          now={d.now}
          onSelect={d.onSelect}
          onHover={d.onHover}
          onInspect={d.onInspect}
          onJump={d.onJump}
        />
      )}
      {d.now ? (
        <span className="pnow-tag" aria-hidden="true">
          now
        </span>
      ) : null}
      <Handle type="source" position={Position.Bottom} className="phandle" />
    </>
  )
}

/** An open stage's band: its label selects the group and its ⊖ closes the stage into one card. */
function BandNode({ data }: NodeProps): JSX.Element {
  const d = data as unknown as {
    groupId: string
    label: string
    hue: number
    w: number
    h: number
    selected: boolean
    collapsible: boolean
    onPick: (groupId: string, additive: boolean) => void
    onClose: (groupId: string) => void
  }
  return (
    <div
      className="pband pband-group"
      data-hue={d.hue}
      data-band={d.groupId}
      data-selected={d.selected ? 'true' : undefined}
      style={{ width: d.w, height: d.h }}
    >
      <span
        role="button"
        tabIndex={0}
        className="pband-label"
        title={`${d.label}: the steps this stage groups`}
        onClick={(e) => d.onPick(d.groupId, e.metaKey || e.ctrlKey)}
        onKeyDown={(e: KeyboardEvent<HTMLSpanElement>) => {
          if (e.key !== 'Enter' && e.key !== ' ') return
          e.preventDefault()
          d.onPick(d.groupId, e.metaKey || e.ctrlKey)
        }}
      >
        {d.label}
      </span>
      {d.collapsible ? (
        <button
          type="button"
          className="pband-close nodrag nopan"
          aria-label={`Close ${d.label} into one card`}
          title={`Close ${d.label} into one card`}
          onClick={() => d.onClose(d.groupId)}
        >
          <Glyph name="minusCircle" />
        </button>
      ) : null}
    </div>
  )
}

/**
 * The legend, as a NODE.
 *
 * It used to be a window-fixed overlay, which meant it never moved when the drawing did and could sit on
 * top of it. Placed in scene coordinates it belongs to the diagram: it pans and zooms with what it
 * decodes, and a reader who scrolls to a region takes the key with them.
 */
function LegendNode({ data }: NodeProps): JSX.Element {
  const d = data as unknown as { render: () => JSX.Element }
  return <div className="plegend-node nodrag nopan">{d.render()}</div>
}

type RoutedEdgeData = { points: Point[]; labelAt?: Point }

/** A line the layout routed, drawn as given rather than between handles. */
function RoutedEdge({
  id,
  data,
  markerEnd,
  label,
  labelStyle,
  labelBgStyle,
}: EdgeProps<Edge<RoutedEdgeData>>): JSX.Element | null {
  const points = data?.points ?? []
  if (points.length < 2) return null
  const at = data?.labelAt ?? labelPoint(points)
  return (
    <BaseEdge
      id={id}
      path={roundedPath(points, 10)}
      markerEnd={markerEnd}
      label={label}
      labelX={at.x}
      labelY={at.y}
      labelStyle={labelStyle}
      labelBgStyle={labelBgStyle}
      labelBgPadding={[4, 2]}
      labelBgBorderRadius={4}
      interactionWidth={14}
    />
  )
}

const NODE_TYPES = { pbox: BoxNode, band: BandNode, legend: LegendNode }

const EDGE_TYPES = { routed: RoutedEdge }

/** After a stage opens or closes: what takes focus and what to bring into view. */
type StageCue = {
  focusId: string | null
  reveal: { x: number; y: number; w: number; h: number } | null
  nonce: number
}

type StageProps = {
  scene: Scene
  /** Which way the Flow reads, for where a too-large drawing opens. */
  direction: Direction
  selectedId: string | null
  hoveredId?: string | null
  referencedIds?: readonly string[]
  onSelect?: (id: string | null, additive: boolean) => void
  onHover?: (id: string | null) => void
  onInspect?: (id: string) => void
  legend?: () => JSX.Element
  dimUnrelated?: boolean
  /** When set, the view centres this node at reading zoom instead of opening on the whole Flow. */
  focusNodeId?: string | null
  /** The Step, or the stage holding it, that the shown run is at now. */
  nowNodeId?: string | null
  handleRef?: Ref<CanvasViewportHandle>
  /** A band label selects its group. `additive` is Cmd (macOS) or Ctrl. */
  onSelectGroup?: (id: string | null, additive: boolean) => void
  selectedGroupId?: string | null
  onOpenStage?: (stageBoxId: string) => void
  onCloseStage?: (groupId: string) => void
  cue?: StageCue | null
  interactive: boolean
  insetRightPx?: number | null
  /** Changing this re-opens the view; opening a stage keeps the reader's place, so it is not part of it. */
  fitKey: string
}

function Inner({
  scene,
  direction,
  dimUnrelated = true,
  focusNodeId,
  nowNodeId = null,
  insetRightPx,
  legend,
  handleRef,
  onSelectGroup,
  selectedGroupId,
  onOpenStage,
  onCloseStage,
  cue = null,
  selectedId,
  hoveredId,
  referencedIds,
  onSelect,
  onHover,
  onInspect,
  interactive,
  fitKey,
}: StageProps): JSX.Element {
  const rf = useReactFlow()
  const paneRef = useRef<HTMLDivElement | null>(null)
  const [selectedLinkId, setSelectedLinkId] = useState<string | null>(null)

  useEffect(() => {
    if (selectedId !== null) setSelectedLinkId(null)
  }, [selectedId])

  /** A jump chip selects where it leads at a readable size; a jump into a closed stage opens it. */
  const jump = useCallback(
    (id: string) => {
      const node = rf.getNode(id)
      if (node === undefined) return
      const kind = (node.data as BoxData | undefined)?.box.kind
      if (kind === 'step') onSelect?.(id, false)
      if (kind === 'stage') onOpenStage?.(id)
      void rf.setCenter(node.position.x + (node.width ?? 0) / 2, node.position.y + (node.height ?? 0) / 2, {
        zoom: Math.max(rf.getZoom(), OPEN_FLOOR),
        duration: 220,
      })
    },
    [onOpenStage, onSelect, rf],
  )
  const openStage = useCallback((id: string) => onOpenStage?.(id), [onOpenStage])
  const closeStage = useCallback((groupId: string) => onCloseStage?.(groupId), [onCloseStage])
  const pickBand = useCallback(
    (groupId: string, additive: boolean) => onSelectGroup?.(groupId, additive),
    [onSelectGroup],
  )

  // Sizes come from the Scene, so fitting never measures unrendered nodes.
  const nodes = useMemo<Node[]>(() => {
    const bands: Node[] = scene.bands.map((b) => ({
      id: `band:${b.id}`,
      type: 'band',
      position: { x: b.x, y: b.y },
      width: b.w,
      height: b.h,
      data: { ...b, selected: b.groupId === selectedGroupId, onPick: pickBand, onClose: closeStage },
      draggable: false,
      selectable: false,
      zIndex: 0,
      style: { zIndex: 0 },
    }))
    const boxes: Node[] = scene.boxes.map((b) => ({
      id: b.id,
      type: 'pbox',
      position: { x: b.x, y: b.y },
      width: b.w,
      height: b.h,
      data: {
        box: b,
        selected: b.id === selectedId,
        hovered: b.id === hoveredId,
        referenced: referencedIds !== undefined && referencedIds.includes(b.id),
        now: nowNodeId !== null && (b.id === nowNodeId || (b.holds ?? []).includes(nowNodeId)),
        onSelect: (id: string, additive: boolean) => onSelect?.(id, additive),
        onHover: (id: string | null) => onHover?.(id),
        onInspect: (id: string) => onInspect?.(id),
        onOpen: openStage,
        onJump: jump,
      } satisfies BoxData,
      draggable: false,
      // A failure detour sits above the drawing and its legend, beside the card it opened from.
      zIndex: b.detourOf === undefined ? 2 : 4,
      style: { zIndex: b.detourOf === undefined ? 2 : 4 },
    }))
    const legendNode: Node[] = []
    const anchor = legendAnchor(
      scene.boxes.filter((b) => b.detourOf === undefined),
      scene.bands,
    )
    if (legend !== undefined && anchor !== undefined) {
      legendNode.push({
        id: '__legend',
        type: 'legend',
        position: anchor,
        width: LEGEND_W,
        height: LEGEND_BOX_H,
        data: { render: legend },
        draggable: false,
        selectable: false,
        zIndex: 3,
        style: { zIndex: 3, width: LEGEND_W, height: LEGEND_BOX_H },
      })
    }
    return [...bands, ...boxes, ...legendNode]
  }, [
    scene,
    selectedId,
    hoveredId,
    referencedIds,
    nowNodeId,
    onSelect,
    onHover,
    onInspect,
    selectedGroupId,
    pickBand,
    closeStage,
    openStage,
    jump,
    legend,
  ])

  const edges = useMemo<Edge<RoutedEdgeData>[]>(
    () =>
      scene.links.map((l) => {
        const isLit = selectedId !== null && (l.from === selectedId || l.to === selectedId)
        const isPicked = l.id === selectedLinkId
        const isDim = dimUnrelated && selectedId !== null && !isLit && !isPicked
        return {
          id: l.id,
          source: l.from,
          target: l.to,
          type: 'routed',
          data: { points: l.points, ...(l.label?.at ? { labelAt: l.label.at } : {}) },
          markerEnd: EDGE_MARKER[l.family],
          selected: isPicked,
          zIndex: isPicked || isLit ? 2 : 1,
          className: `pedge pedge-${l.family}${isLit || isPicked ? ' pedge-lit' : ''}${isDim ? ' pedge-dim' : ''}`,
          // A case value always shows; a merged-condition count only while an end is selected.
          label: l.label && (l.label.always || isLit || isPicked) ? l.label.text : undefined,
          labelStyle: { fill: 'var(--p-ink-1)', fontSize: 10.5, fontWeight: 500 },
          labelBgStyle: { fill: 'var(--p-surface-2)' },
        }
      }),
    [scene, selectedId, selectedLinkId, dimUnrelated],
  )

  useImperativeHandle(
    handleRef,
    () => ({
      zoomIn: () => void rf.zoomIn({ duration: 120 }),
      zoomOut: () => void rf.zoomOut({ duration: 120 }),
      fit: () => void rf.fitView({ ...fitOptions(insetRightPx, paneRef.current?.clientWidth ?? 0), minZoom: MIN_ZOOM }),
      zoom: () => rf.getZoom(),
      focus: (nodeId: string) => {
        const id = rf.getNode(nodeId) !== undefined ? nodeId : `band:${nodeId}`
        if (rf.getNode(id) === undefined) return
        void rf.fitView({ nodes: [{ id }], padding: 0.45, maxZoom: FOCUS_ZOOM, duration: 260 })
      },
      reveal: (nodeId: string) => {
        const id = rf.getNode(nodeId) !== undefined ? nodeId : `band:${nodeId}`
        if (rf.getNode(id) === undefined) return
        void rf.fitView({ nodes: [{ id }], padding: 0.4, maxZoom: rf.getZoom(), duration: 160 })
      },
    }),
    [rf, insetRightPx],
  )

  // After the view opens, a resize with a selection only pans that selection back into view.
  const keepSelectionInView = useRef<() => boolean>(() => false)
  // Read when the view re-opens; re-opening on every run poll would fight the reader.
  const latest = useRef({ scene, direction, nowNodeId, focusNodeId, insetRightPx })
  useEffect(() => {
    latest.current = { scene, direction, nowNodeId, focusNodeId, insetRightPx }
  })

  // Re-open when what is shown changes, never on zoom; a ResizeObserver knows when the pane is sized.
  useEffect(() => {
    const el = paneRef.current
    if (el === null) return
    let raf = 0
    let isOpened = false
    const open = (): void => {
      cancelAnimationFrame(raf)
      raf = requestAnimationFrame(() => {
        const { scene: shown, direction: reading, nowNodeId: now, focusNodeId: focus, insetRightPx } = latest.current
        const paneW = el.clientWidth - (insetRightPx ?? 0)
        const paneH = el.clientHeight
        if (paneW < 8 || paneH < 8) return
        if (isOpened && keepSelectionInView.current()) return
        isOpened = true
        const focused = focus === null || focus === undefined ? null : rf.getNode(focus) !== undefined ? focus : null
        if (focused !== null) {
          void rf.fitView({ nodes: [{ id: focused }], padding: 0.45, maxZoom: FOCUS_ZOOM, duration: 160 })
          return
        }
        const zoomToFit = Math.min((paneW * 0.92) / shown.width, (paneH * 0.9) / shown.height)
        if (zoomToFit >= OPEN_FLOOR || shown.boxes.length === 0) {
          void rf.fitView({ ...fitOptions(insetRightPx, el.clientWidth), duration: 160 })
          return
        }
        // Too large to read whole: open at the readable floor with the start, or "now", in view.
        const anchor =
          shown.boxes.find((b) => now !== null && (b.id === now || (b.holds ?? []).includes(now))) ??
          shown.boxes.find((b) => b.isStart) ??
          (shown.boxes[0] as Box)
        const zoom = OPEN_FLOOR
        const across = (span: number, room: number, at: number): number =>
          span * zoom <= room ? (room - span * zoom) / 2 : Math.min(32, room / 2 - at * zoom)
        const x = reading === 'lr' ? 32 - anchor.x * zoom : across(shown.width, paneW, anchor.x + anchor.w / 2)
        const y = reading === 'lr' ? across(shown.height, paneH, anchor.y + anchor.h / 2) : 24 - anchor.y * zoom
        void rf.setViewport({ x, y, zoom }, { duration: 160 })
      })
    }
    open()
    const ro = new ResizeObserver(open)
    ro.observe(el)
    return () => {
      cancelAnimationFrame(raf)
      ro.disconnect()
    }
    /* The Step panel never refits the view: the reader keeps their place, and the selection pans clear. */
  }, [rf, fitKey, focusNodeId])

  const applyZoomVars = useCallback((zoom: number) => {
    const el = paneRef.current
    if (el === null || !Number.isFinite(zoom)) return
    el.style.setProperty('--zoom-comp', chromeCompensation(zoom).toFixed(3))
    el.style.setProperty('--edge-boost', edgeContrastBoost(zoom).toFixed(3))
  }, [])

  useEffect(() => {
    applyZoomVars(rf.getZoom())
  }, [applyZoomVars, rf, fitKey])

  /** Pans the least that shows a rectangle whole; one larger than the pane shows its start. */
  const panToShow = useCallback(
    (r: { x: number; y: number; w: number; h: number }) => {
      // The drawing's own box: the pane also holds the padding above it.
      const el = paneRef.current?.querySelector<HTMLElement>('.react-flow') ?? paneRef.current
      if (el === null || el === undefined) return
      const { x, y, zoom } = rf.getViewport()
      const margin = 24
      const shift = (start: number, size: number, room: number): number => {
        const a = start * zoom
        const b = a + size * zoom
        if (b - a > room - 2 * margin) return margin - a
        if (a < margin) return margin - a
        if (b > room - margin) return room - margin - b
        return 0
      }
      const dx = shift(r.x + x / zoom, r.w, el.clientWidth - (insetRightPx ?? 0))
      const dy = shift(r.y + y / zoom, r.h, el.clientHeight)
      if (dx !== 0 || dy !== 0) void rf.setViewport({ x: x + dx, y: y + dy, zoom }, { duration: 280 })
    },
    [insetRightPx, rf],
  )

  // After a stage opens or closes: focus what the reader acted on and pan just enough to show it.
  const appliedCue = useRef(0)
  useEffect(() => {
    if (cue === null || cue.nonce === appliedCue.current) return
    const raf = requestAnimationFrame(() => {
      appliedCue.current = cue.nonce
      if (cue.focusId !== null) {
        paneRef.current
          ?.querySelector<HTMLElement>(`.react-flow__node[data-id="${CSS.escape(cue.focusId)}"] [role="button"]`)
          ?.focus({ preventScroll: true })
      }
      if (cue.reveal !== null) panToShow(cue.reveal)
    })
    return () => cancelAnimationFrame(raf)
  }, [cue, panToShow])

  // Pan once per reader selection to show it and its failure detour beside the Step panel.
  const selection = useMemo(() => {
    if (!dimUnrelated || selectedId === null) return null
    const parts = scene.boxes.filter((b) => b.id === selectedId || b.detourOf === selectedId)
    if (parts.length === 0) return null
    const x = Math.min(...parts.map((b) => b.x))
    const y = Math.min(...parts.map((b) => b.y))
    const w = Math.max(...parts.map((b) => b.x + b.w)) - x
    const h = Math.max(...parts.map((b) => b.y + b.h)) - y
    const key = `${selectedId}@${Math.round(x)},${Math.round(y)},${Math.round(w)},${Math.round(h)}|${insetRightPx ?? 0}`
    return { key, x, y, w, h }
  }, [dimUnrelated, insetRightPx, scene, selectedId])
  const shownSelection = useRef('')
  useEffect(() => {
    keepSelectionInView.current = () => {
      if (selection === null) return false
      panToShow(selection)
      return true
    }
  })
  useEffect(() => {
    if (selection === null || selection.key === shownSelection.current) return
    const raf = requestAnimationFrame(() => {
      shownSelection.current = selection.key
      panToShow(selection)
    })
    return () => cancelAnimationFrame(raf)
  }, [selection, panToShow])

  // While cards glide to new places, the lines between them fade out and back.
  const placed = placementKey(scene)
  const [observedPlacement, setObservedPlacement] = useState(placed)
  const [isMoving, setMoving] = useState(false)
  if (observedPlacement !== placed) {
    setObservedPlacement(placed)
    setMoving(true)
  }
  useEffect(() => {
    if (!isMoving) return
    const timer = window.setTimeout(() => setMoving(false), 300)
    return () => window.clearTimeout(timer)
  }, [isMoving])

  const handleMove = useCallback(
    (_: unknown, viewport: { zoom: number }) => applyZoomVars(viewport.zoom),
    [applyZoomVars],
  )

  return (
    <div className="pstage-pane" ref={paneRef} data-relayout={isMoving ? 'true' : undefined}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={NODE_TYPES}
        edgeTypes={EDGE_TYPES}
        minZoom={MIN_ZOOM}
        maxZoom={MAX_ZOOM}
        nodesDraggable={false}
        nodesConnectable={false}
        elementsSelectable={interactive}
        panOnDrag={interactive}
        zoomOnScroll={interactive}
        zoomOnPinch={interactive}
        zoomOnDoubleClick={false}
        preventScrolling={interactive}
        proOptions={{ hideAttribution: true }}
        onMove={handleMove}
        onPaneClick={() => {
          setSelectedLinkId(null)
          onSelect?.(null, false)
        }}
        onEdgeClick={(_, edge) => {
          setSelectedLinkId(edge.id)
          onSelect?.(null, false)
        }}
      >
        <Background variant={BackgroundVariant.Dots} gap={26} size={1} />
        {interactive ? <Controls showInteractive={false} /> : null}
      </ReactFlow>
    </div>
  )
}

export function Stage(props: Omit<StageProps, 'interactive'> & { interactive?: boolean }): JSX.Element {
  const { interactive = true, ...rest } = props
  return (
    <div className="pstage">
      <ReactFlowProvider>
        <Inner {...rest} interactive={interactive} />
      </ReactFlowProvider>
    </div>
  )
}
