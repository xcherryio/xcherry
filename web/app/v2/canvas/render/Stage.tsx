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
  type CSSProperties,
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
  boxShowing,
  cameraMotion,
  cameraRoom,
  entranceStyle,
  focusView,
  polylineLength,
  type FramingNeighbours,
} from './camera'
import { useCamera, useRunTravel, useSettledDrawing } from './useCamera'
import { chromeCompensation, edgeContrastBoost, FIT_MAX_ZOOM, fitOptions, MAX_ZOOM, MIN_ZOOM, OPEN_FLOOR } from './zoom'
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

/** Declared anchors, so lines survive rebuilt nodes: React Flow forgets measured handles on each new node object. */
function anchorHandles(w: number, h: number): NonNullable<Node['handles']> {
  return [
    { type: 'target', position: Position.Top, x: w / 2, y: 0, width: 1, height: 1 },
    { type: 'source', position: Position.Bottom, x: w / 2, y: h, width: 1, height: 1 },
  ]
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

type RoutedEdgeData = { points: Point[]; labelAt?: Point; travel?: number }

/** A line the layout routed, drawn as given rather than between handles. */
function RoutedEdge({
  id,
  data,
  style,
  markerEnd,
  label,
  labelStyle,
  labelBgStyle,
}: EdgeProps<Edge<RoutedEdgeData>>): JSX.Element | null {
  const points = data?.points ?? []
  if (points.length < 2) return null
  const at = data?.labelAt ?? labelPoint(points)
  const path = roundedPath(points, 10)
  const length = { '--len': `${polylineLength(points)}px` } as CSSProperties
  return (
    <>
    <BaseEdge
      id={id}
      path={path}
      style={{ ...style, ...length }}
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
      {data?.travel ? (
        // The run moving on along this line: a head with a fading trail, like the running mark.
        <g key={data.travel} className="pedge-travel" aria-hidden="true">
          <path d={path} className="pedge-travel-trail" style={length} />
          <path d={path} className="pedge-travel-head" style={length} />
        </g>
      ) : null}
    </>
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
  /** When set, the view frames this node with its neighbours instead of opening on the whole Flow. */
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
  /** False while the drawing is on its way; the last settled one stays on screen meanwhile. */
  isSettled?: boolean
  /** Steps a transition joins to the focus, or to "now", which its framing keeps in view. */
  targetNeighbours?: FramingNeighbours
}

function Inner({
  direction,
  dimUnrelated = true,
  focusNodeId,
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
  isSettled = true,
  targetNeighbours,
  scene: latestScene,
  nowNodeId: latestNowNodeId = null,
}: StageProps): JSX.Element {
  const rf = useReactFlow()
  const paneRef = useRef<HTMLDivElement | null>(null)
  const [selectedLinkId, setSelectedLinkId] = useState<string | null>(null)
  // Hold the last settled drawing, so a run still loading never draws half-way states.
  const { scene, nowNodeId } = useSettledDrawing(isSettled, latestScene, latestNowNodeId)

  useEffect(() => {
    if (selectedId !== null) setSelectedLinkId(null)
  }, [selectedId])

  const fitWhole = useCallback(() => {
    const el = paneRef.current?.querySelector<HTMLElement>('.react-flow') ?? paneRef.current
    void rf.fitView({ ...fitOptions(insetRightPx, el?.clientWidth ?? 0), minZoom: MIN_ZOOM, ...cameraMotion() })
  }, [insetRightPx, rf])
  // After the view opens, a resize with a selection only pans that selection back into view.
  const keepSelectionInView = useRef<() => boolean>(() => false)
  const camera = useCamera({
    viewport: rf,
    paneRef,
    scene,
    direction,
    isSettled,
    subjectKey: fitKey,
    targetId: focusNodeId ?? null,
    targetNeighbours,
    insetRightPx,
    wholeMaxZoom: FIT_MAX_ZOOM,
    fitWhole,
    keepSelectionInView: () => keepSelectionInView.current(),
  })
  const { delays, isEntered, panToShow, setOwner } = camera

  /** A jump chip selects where it leads at a readable size; a jump into a closed stage opens it. */
  const jump = useCallback(
    (id: string) => {
      const node = rf.getNode(id)
      if (node === undefined) return
      const kind = (node.data as BoxData | undefined)?.box.kind
      if (kind === 'step') onSelect?.(id, false)
      if (kind === 'stage') onOpenStage?.(id)
      setOwner('reader')
      void rf.setCenter(node.position.x + (node.width ?? 0) / 2, node.position.y + (node.height ?? 0) / 2, {
        zoom: Math.max(rf.getZoom(), OPEN_FLOOR),
        ...cameraMotion(),
      })
    },
    [onOpenStage, onSelect, rf, setOwner],
  )
  const openStage = useCallback((id: string) => onOpenStage?.(id), [onOpenStage])
  const closeStage = useCallback((groupId: string) => onCloseStage?.(groupId), [onCloseStage])
  const pickBand = useCallback(
    (groupId: string, additive: boolean) => onSelectGroup?.(groupId, additive),
    [onSelectGroup],
  )

  const travel = useRunTravel(scene, fitKey, nowNodeId, isEntered)

  // Sizes come from the Scene, so fitting never measures unrendered nodes.
  const nodes = useMemo<Node[]>(() => {
    // Nothing is drawn until the camera has framed it, so the drawing enters in place.
    if (!isEntered) return []
    const travelledTo = travel === null ? undefined : scene.links.find((l) => l.id === travel.linkId)?.to
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
      style: { zIndex: 0, ...entranceStyle(delays, `band:${b.id}`) },
    }))
    const boxes: Node[] = scene.boxes.map((b) => ({
      id: b.id,
      type: 'pbox',
      position: { x: b.x, y: b.y },
      width: b.w,
      height: b.h,
      handles: anchorHandles(b.w, b.h),
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
      style: {
        zIndex: b.detourOf === undefined ? 2 : 4,
        ...entranceStyle(delays, b.id, travelledTo === b.id),
      },
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
        style: { zIndex: 3, width: LEGEND_W, height: LEGEND_BOX_H, ...entranceStyle(delays, '__legend') },
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
    isEntered,
    delays,
    travel,
  ])

  const edges = useMemo<Edge<RoutedEdgeData>[]>(
    () =>
      (isEntered ? scene.links : []).map((l) => {
        const isLit = selectedId !== null && (l.from === selectedId || l.to === selectedId)
        const isPicked = l.id === selectedLinkId
        const isDim = dimUnrelated && selectedId !== null && !isLit && !isPicked
        return {
          id: l.id,
          source: l.from,
          target: l.to,
          type: 'routed',
          data: {
            points: l.points,
            ...(l.label?.at ? { labelAt: l.label.at } : {}),
            ...(travel?.linkId === l.id ? { travel: travel.nonce } : {}),
          },
          style: entranceStyle(delays, l.id),
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
    [scene, selectedId, selectedLinkId, dimUnrelated, isEntered, delays, travel],
  )

  useImperativeHandle(
    handleRef,
    () => ({
      zoomIn: () => {
        setOwner('reader')
        void rf.zoomIn(cameraMotion(200))
      },
      zoomOut: () => {
        setOwner('reader')
        void rf.zoomOut(cameraMotion(200))
      },
      fit: () => {
        setOwner('fit')
        fitWhole()
      },
      zoom: () => rf.getZoom(),
      focus: (nodeId: string) => {
        const el = paneRef.current?.querySelector<HTMLElement>('.react-flow')
        if (el === null || el === undefined) return
        const room = cameraRoom(el.clientWidth, el.clientHeight, insetRightPx)
        const framed = focusView(scene, nodeId, { near: [], twoAway: [] }, room, FIT_MAX_ZOOM)
        if (framed === null) return
        setOwner('camera')
        void rf.setViewport(framed.view, cameraMotion())
      },
      reveal: (nodeId: string) => {
        const box = boxShowing(scene, nodeId)
        if (box !== undefined) panToShow(box)
      },
    }),
    [fitWhole, insetRightPx, panToShow, rf, scene, setOwner],
  )

  const applyZoomVars = useCallback((zoom: number) => {
    const el = paneRef.current
    if (el === null || !Number.isFinite(zoom)) return
    el.style.setProperty('--zoom-comp', chromeCompensation(zoom).toFixed(3))
    el.style.setProperty('--edge-boost', edgeContrastBoost(zoom).toFixed(3))
  }, [])

  useEffect(() => {
    applyZoomVars(rf.getZoom())
  }, [applyZoomVars, rf, fitKey])

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
    // Before the entrance nothing is drawn, so nothing moves.
    if (isEntered) setMoving(true)
  }
  useEffect(() => {
    if (!isMoving) return
    const timer = window.setTimeout(() => setMoving(false), 320)
    return () => window.clearTimeout(timer)
  }, [isMoving])

  const handleMove = useCallback(
    (_: unknown, viewport: { zoom: number }) => applyZoomVars(viewport.zoom),
    [applyZoomVars],
  )

  return (
    <div
      className="pstage-pane"
      ref={paneRef}
      data-relayout={isMoving ? 'true' : undefined}
      data-pending={isEntered && !isSettled ? 'true' : undefined}
      data-entering={delays !== null ? 'true' : undefined}
    >
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
        onMoveEnd={(event) => {
          // Only the reader's own pan or zoom carries an event; it takes the view over from the camera.
          if (event) setOwner('reader')
        }}
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
        {isEntered ? null : (
          <p className="pstage-wait" role="status">
            Drawing the Flow…
          </p>
        )}
        {interactive ? (
          <Controls
            showInteractive={false}
            fitViewOptions={{
              ...fitOptions(insetRightPx, paneRef.current?.clientWidth ?? 0),
              minZoom: MIN_ZOOM,
              ...cameraMotion(),
            }}
            onFitView={() => setOwner('fit')}
            onZoomIn={() => setOwner('reader')}
            onZoomOut={() => setOwner('reader')}
          />
        ) : null}
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
