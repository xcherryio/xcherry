// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import { useCallback, useEffect, useMemo, useRef, useState, type CSSProperties } from 'react';
import { hydrateBlobs } from '@/lib/blobs';
import { readResponseJSON } from '@/lib/http';
import type { FlowDefinitionCatalog, FlowDefinitionEntry, FlowHistoryEvent, FlowSummary } from '@/lib/types';
import { safeDecode } from './canvas/model/decode';
import {
  activeStepTypes,
  executionsOf,
  reasonLine,
  type RunOverlay,
} from './canvas/model/run';
import {
  loadCurrentRun,
  loadRunHistory,
  attemptCountFromRecord,
  overlayFromHistory,
  prependStepRecords,
  recordForExecution,
  type ExecutionRecord,
  type RunOverlayBundle,
} from './canvas/overlayFromHistory';
import { DetailPanel } from './canvas/panel/DetailPanel';
import { useWebCatalog } from './WebCatalogProvider';
import { buildPanel, type SectionId } from './canvas/panel/panelModel';
import { ArrowDefs } from './canvas/render/ArrowDefs';
import { framingNeighbours, NO_FRAMING_NEIGHBOURS } from './canvas/render/camera';
import { Stage } from './canvas/render/Stage';
import type { CanvasViewportHandle } from './canvas/render/viewport';
import { useStageScene } from './canvas/useStageScene';
import { groupOfStep } from './canvas/views/controlTopology';
import { detourMessage, hasDetour, isFailureHandling, runDetourSources, withDetours } from './canvas/views/failureDetours';
import { stageId } from './canvas/views/stageCard';
import type { Direction } from './canvas/views/types';
import { Controls } from './flow/Controls';
import { Legend } from './flow/Legend';
import { groupsFromDefinition } from './groupsFromGraph';
import { actionableSteps, type ActionableStep } from './run/actionableSteps';
import type { StepBand } from './run/RunDetailDrawer';
import { stepContext, type StepContextView } from './run/stepContext';
import {
  PANEL_WIDTH_DEFAULT,
  PANEL_WIDTH_KEY,
  V2SplitHandle,
  readStoredPixels,
  writeStoredPixels,
} from './V2SplitHandle';

export function V2Canvas({
  flowType,
  flowId = '',
  definition,
  onActionable,
  onBand,
  onStepContext,
  onSummary,
  onTick,
  deselectKey = 0,
  refocusKey = 0,
  focusBlockingStep = false,
  showStepPanel = true,
}: {
  flowType: string;
  flowId?: string;
  /** Draws this definition file instead of the catalog's valid one for `flowType`. */
  definition?: FlowDefinitionEntry;
  /** Bumped by the host to clear the Step selection. */
  deselectKey?: number;
  /** Bumped when the open run is picked again: the reader's selection goes and the view returns to the run. */
  refocusKey?: number;
  /** Zoom in on the waiting Step instead of merely panning to it. */
  focusBlockingStep?: boolean;
  /** False when the host renders step detail itself, so the canvas keeps its width. */
  showStepPanel?: boolean;
  /** What the canvas is showing, so a host drawer can label it without owning selection. */
  onBand?: (band: StepBand | null) => void;
  /** Flow-level meaning of the selected Step, for a host that explains it. */
  onStepContext?: (context: StepContextView | null) => void;
  /** Every Step of this Flow that waits for a person, with its progress. */
  onActionable?: (steps: ActionableStep[]) => void;
  onSummary?: (summary: FlowSummary | null) => void;
  /** Fired on every run poll, so a host can refresh on the same beat. */
  onTick?: () => void;
}) {
  const { definitions } = useWebCatalog();
  const canvasRef = useRef<HTMLDivElement>(null);
  const viewportRef = useRef<CanvasViewportHandle | null>(null);
  const [catalog, setCatalog] = useState<FlowDefinitionCatalog | null>(null);
  const [direction, setDirection] = useState<Direction>('tb');
  const [selectedId, setSelectedId] = useState<string | null>(null);
  /** Auto-focus selects a Step for the reader; only their own click should fade the rest. */
  const [selectedByReader, setSelectedByReader] = useState(false);
  const [selectedGroupId, setSelectedGroupId] = useState<string | null>(null);
  const [selectedExecutionId, setSelectedExecutionId] = useState<string | null>(null);
  const [inspectSection, setInspectSection] = useState<SectionId | null>(null);
  const [legendOpen, setLegendOpen] = useState(false);
  const [panelWidth, setPanelWidth] = useState(() => {
    const stored = readStoredPixels(PANEL_WIDTH_KEY);
    return Number.isFinite(stored) ? stored : PANEL_WIDTH_DEFAULT;
  });
  const [error, setError] = useState('');
  const [runError, setRunError] = useState('');
  /** The run whose history last loaded or failed to; the drawing waits for the shown one. */
  const [loadedFlowId, setLoadedFlowId] = useState('');
  const [currentBundle, setCurrentBundle] = useState<RunOverlayBundle | null>(null);
  const [olderByStep, setOlderByStep] = useState<Record<string, ExecutionRecord[]>>({});
  const [previousCursorByStep, setPreviousCursorByStep] = useState<Record<string, string>>({});
  const [loadPreviousBusy, setLoadPreviousBusy] = useState(false);
  const [previousEmpty, setPreviousEmpty] = useState(false);
  const [historyEvents, setHistoryEvents] = useState<FlowHistoryEvent[]>([]);
  const [hydratedWaitEvent, setHydratedWaitEvent] = useState<FlowHistoryEvent | null>(null);
  const [hydratedExecuteEvent, setHydratedExecuteEvent] = useState<FlowHistoryEvent | null>(null);
  const blobCache = useRef(new Map<string, unknown>());

  useEffect(() => {
    setCatalog(definitions);
    setError('');
  }, [definitions]);

  useEffect(() => {
    setCurrentBundle(null);
    setOlderByStep({});
    setPreviousCursorByStep({});
    setPreviousEmpty(false);
    setHistoryEvents([]);
    setHydratedWaitEvent(null);
    setHydratedExecuteEvent(null);
    setSelectedExecutionId(null);
    setRunError('');
    if (!flowId) return undefined;
    let cancelled = false;
    let timer: number | undefined;
    const load = async () => {
      try {
        const { summary, events, state } = await loadCurrentRun(flowId);
        if (cancelled) return;
        setHistoryEvents(events);
        setCurrentBundle(overlayFromHistory({
          flowId,
          runId: summary.runId,
          status: summary.flowStatus,
          events,
          activeSteps: state?.activeStepExecutions ?? [],
        }));
        setRunError('');
        setLoadedFlowId(flowId);
        onSummary?.(summary);
        onTick?.();
        if (summary.flowStatusCode === 1 && timer === undefined) {
          timer = window.setInterval(() => { void load(); }, 5000);
        }
      } catch (loadError: unknown) {
        if (!cancelled) {
          setLoadedFlowId(flowId);
          setRunError(loadError instanceof Error ? loadError.message : 'Run history failed to load');
        }
      }
    };
    void load();
    return () => {
      cancelled = true;
      if (timer !== undefined) window.clearInterval(timer);
    };
  }, [flowId]);

  const selected = useMemo(
    () => definition ?? catalog?.definitions.find(
      (candidate) => candidate.flowName === flowType && candidate.schemaVersion === '2.0' && candidate.valid,
    ),
    [catalog, definition, flowType],
  );

  const flow = useMemo(() => {
    if (!selected) return null;
    return safeDecode(selected.graph, {
      generated: true,
      note: selected.file,
    });
  }, [selected]);

  const groups = useMemo(() => {
    if (!selected || !flow) return [];
    return groupsFromDefinition(selected.graph, flow);
  }, [flow, selected]);

  const bundle = useMemo(() => {
    if (!currentBundle) return null;
    return Object.entries(olderByStep).reduce(
      (next, [stepType, older]) => prependStepRecords(next, older, stepType),
      currentBundle,
    );
  }, [currentBundle, olderByStep]);

  const overlay: RunOverlay | null = bundle?.overlay ?? null;
  const selectedStep = flow?.steps.find((step) => step.id === selectedId) ?? null;

  /** The Step the run is actually waiting on, which is what an Admin came to see. */
  const blockingStepType = overlay ? (activeStepTypes(overlay)[0] ?? null) : null;
  // The stages the reader opened in place, each opened or closed without losing their place.
  const stages = useStageScene({ flow, groups, direction, run: overlay });
  const { revealStage } = stages;
  /** The Step whose failure detour is drawn: the last selected Step that can fail. */
  const [detourFor, setDetourFor] = useState<string | null>(null);
  /** The main story, with the failure detours of the selected Step and of every one the run went through. */
  const scene = useMemo(() => {
    if (stages.scene === null || !flow) return stages.scene;
    const sources = [...new Set([...(overlay ? runDetourSources(flow, overlay) : []), ...(detourFor ? [detourFor] : [])])];
    return withDetours(stages.scene, flow, { direction, run: overlay, groups, openStages: stages.openStages }, sources);
  }, [detourFor, direction, flow, groups, overlay, stages.openStages, stages.scene]);
  // Nothing selected, so no detour: however the selection was cleared.
  if (selectedId === null && detourFor !== null) setDetourFor(null);

  // Picking the open run again drops the reader's selection before the run's Step is revealed below.
  const firstRefocus = useRef(refocusKey);
  useEffect(() => {
    if (refocusKey === firstRefocus.current) return;
    setSelectedId(null);
    setSelectedByReader(false);
    setSelectedGroupId(null);
    setDetourFor(null);
  }, [refocusKey]);

  // Choosing a run should land on where it stopped, once, without fighting later clicks.
  const revealedFor = useRef('');
  const [revealedKey, setRevealedKey] = useState('');
  const revealKey = `${flowId}|${blockingStepType}|${String(focusBlockingStep)}|${refocusKey}`;
  useEffect(() => {
    if (!flow || blockingStepType === null) return;
    const key = revealKey;
    if (revealedFor.current === key) return;
    const step = flow.steps.find((candidate) => candidate.stepType === blockingStepType);
    if (!step) return;
    revealedFor.current = key;
    setRevealedKey(key);
    // The stage holding where the run waits opens, so its "now" ring is on a Step.
    const holding = isFailureHandling(flow, step.id) ? undefined : groupOfStep(groups, step.id);
    if (holding) revealStage(holding.id);
    // Selection only. The zoom is declarative via focusNodeId, so the pane resize that
    // follows the drawer opening refits to the Step instead of racing an imperative call.
    setSelectedId(focusBlockingStep ? step.id : null);
    setSelectedByReader(false);
    setSelectedGroupId(null);
  }, [blockingStepType, flow, focusBlockingStep, groups, revealKey, revealStage]);
  // The drawing settles once the shown run has loaded, its Step's stage is open and the layout is done.
  const isSettled = stages.isSettled
    && (!flowId || loadedFlowId === flowId)
    && (blockingStepType === null || revealedKey === revealKey);

  // The host closed its drawer, so nothing is being explained any more.
  const firstDeselect = useRef(deselectKey);
  useEffect(() => {
    if (deselectKey === firstDeselect.current) return;
    setSelectedId(null);
    setSelectedGroupId(null);
    setDetourFor(null);
  }, [deselectKey]);

  const focusNodeId = useMemo(() => {
    if (!focusBlockingStep || !flow || blockingStepType === null) return null;
    return flow.steps.find((step) => step.stepType === blockingStepType)?.id ?? null;
  }, [blockingStepType, flow, focusBlockingStep]);

  useEffect(() => {
    if (!onActionable) return;
    onActionable(flow ? actionableSteps(flow, overlay) : []);
  }, [flow, onActionable, overlay]);

  useEffect(() => {
    if (!onStepContext) return;
    onStepContext(
      flow && selectedStep ? stepContext(flow, selectedStep, groups) : null,
    );
  }, [flow, groups, onStepContext, selectedStep]);

  useEffect(() => {
    if (!onBand) return;
    if (!selectedStep || !overlay) {
      onBand(null);
      return;
    }
    const executions = executionsOf(overlay, selectedStep.stepType);
    const latest = executions[executions.length - 1];
    const reason = latest ? reasonLine(latest, Date.now()) : null;
    onBand({
      stepType: selectedStep.stepType,
      reason: reason?.text ?? null,
      tone: reason?.tone ?? null,
      isBlocking: selectedStep.stepType === blockingStepType,
    });
  }, [blockingStepType, onBand, overlay, selectedStep]);

  const nowNodeId = useMemo(() => {
    if (!flow || blockingStepType === null) return null;
    return flow.steps.find((step) => step.stepType === blockingStepType)?.id ?? null;
  }, [blockingStepType, flow]);

  // Picking a run frames its Step; Collapsed, Expanded or a new direction then return to the opening view.
  const runPick = `${flowId}|${refocusKey}`;
  const [picked, setPicked] = useState({ runPick, view: stages.view, isLeft: false });
  if (picked.runPick !== runPick) setPicked({ runPick, view: stages.view, isLeft: false });
  else if (picked.view === '' && stages.view !== '') setPicked({ ...picked, view: stages.view });
  else if (!picked.isLeft && stages.view !== picked.view) setPicked({ ...picked, isLeft: true });
  const frameStepId = picked.isLeft ? null : (focusNodeId ?? nowNodeId);

  /** What the camera keeps in view around the framed Step: the cards of Steps one and two transitions away. */
  const targetNeighbours = useMemo(() => {
    if (!flow || !scene || frameStepId === null) return NO_FRAMING_NEIGHBOURS;
    return framingNeighbours(flow, groups, scene, frameStepId);
  }, [flow, frameStepId, groups, scene]);

  const { collapseStage, openStage: openStageInPlace } = stages;
  const closeStage = useCallback(
    (groupId: string) => {
      collapseStage(groupId);
      setSelectedGroupId(null);
    },
    [collapseStage],
  );
  // Opening a stage marks its band, so the eye lands where the card was.
  const openStage = useCallback(
    (boxId: string) => {
      openStageInPlace(boxId);
      setSelectedGroupId(groups.find((group) => stageId(group.id) === boxId)?.id ?? null);
      setSelectedId(null);
    },
    [groups, openStageInPlace],
  );

  const selectedRecord = selectedStep && bundle
    ? recordForExecution(bundle, selectedStep.stepType, selectedExecutionId)
    : undefined;
  const selectedWaitEvent = selectedRecord?.waitEvent ?? null;
  const selectedExecuteEvent = selectedRecord?.executeEvent ?? null;
  const methodEventKey = [
    selectedWaitEvent
      ? `${selectedWaitEvent.eventId}|${selectedWaitEvent.type}`
      : '',
    selectedExecuteEvent
      ? `${selectedExecuteEvent.eventId}|${selectedExecuteEvent.type}`
      : '',
    selectedRecord?.execution.stepExecutionId ?? '',
  ].join('|');

  useEffect(() => {
    if (!flowId || (!selectedWaitEvent && !selectedExecuteEvent)) {
      setHydratedWaitEvent(null);
      setHydratedExecuteEvent(null);
      return undefined;
    }
    const controller = new AbortController();
    const waitRaw = selectedWaitEvent;
    const executeRaw = selectedExecuteEvent;
    setHydratedWaitEvent(waitRaw);
    setHydratedExecuteEvent(executeRaw);
    const hydrateOne = async (
      raw: FlowHistoryEvent | null,
      setValue: (value: FlowHistoryEvent | null) => void,
    ) => {
      if (!raw) {
        setValue(null);
        return;
      }
      try {
        const result = await hydrateBlobs(flowId, raw, blobCache.current, controller.signal);
        if (!controller.signal.aborted) setValue(result.value);
      } catch {
        if (!controller.signal.aborted) setValue(raw);
      }
    };
    void hydrateOne(waitRaw, setHydratedWaitEvent);
    void hydrateOne(executeRaw, setHydratedExecuteEvent);
    return () => controller.abort();
  }, [flowId, methodEventKey, selectedExecuteEvent, selectedWaitEvent]);

  const panel = useMemo(() => {
    if (!flow || !selectedStep) return null;
    return buildPanel(
      flow,
      selectedStep,
      overlay,
      selectedRecord?.execution.stepExecutionId ?? selectedExecutionId,
      Boolean(selectedRecord?.waitEvent),
      Boolean(selectedRecord?.executeEvent),
      attemptCountFromRecord(selectedRecord),
    );
  }, [
    flow,
    overlay,
    selectedExecutionId,
    selectedRecord,
    selectedStep,
  ]);

  const previousCursor = selectedStep
    ? (previousCursorByStep[selectedStep.stepType] ?? bundle?.previousRunId ?? '')
    : '';

  const loadPrevious = useCallback(async () => {
    if (!flowId || !selectedStep || !previousCursor) return;
    setLoadPreviousBusy(true);
    setPreviousEmpty(false);
    try {
      const events = await loadRunHistory(flowId, previousCursor);
      const hop = overlayFromHistory({
        flowId,
        runId: previousCursor,
        status: 'Continued as new',
        events,
      });
      const older = hop.records.filter((record) => record.execution.stepType === selectedStep.stepType);
      setHistoryEvents((current) => [...events, ...current]);
      setOlderByStep((current) => ({
        ...current,
        [selectedStep.stepType]: [...older, ...(current[selectedStep.stepType] ?? [])],
      }));
      setPreviousCursorByStep((current) => ({
        ...current,
        [selectedStep.stepType]: hop.previousRunId,
      }));
      setPreviousEmpty(older.length === 0);
    } catch (loadError: unknown) {
      setRunError(loadError instanceof Error ? loadError.message : 'Previous run failed to load');
    } finally {
      setLoadPreviousBusy(false);
    }
  }, [flowId, previousCursor, selectedStep]);

  const commitPanelWidth = useCallback((width: number) => {
    const next = Math.round(width);
    setPanelWidth(next);
    writeStoredPixels(PANEL_WIDTH_KEY, next);
  }, []);

  if (error) return <div className="v2-empty">{error}</div>;
  if (!catalog && !definition) return <div className="v2-empty">Loading Flow definition…</div>;
  if (!selected || !flow) {
    return <div className="v2-empty">No valid Flow Definition Graph 2.0 file for this Flow type.</div>;
  }
  if (!scene) return <div className="v2-empty">Drawing the Flow…</div>;

  const ownsPanel = showStepPanel && panel !== null;
  const canvasStyle = ownsPanel
    ? ({ '--v2-panel-w': `${Math.round(panelWidth)}px` } as CSSProperties)
    : undefined;

  return (
    <div className="pcanvas" ref={canvasRef} style={canvasStyle} data-panel={ownsPanel ? 'true' : undefined}>
      <ArrowDefs />
      <div className="pctlbar">
        <Controls
          stages={stages.hasStages ? stages.state : null}
          direction={direction}
          onCollapseAll={stages.collapseAll}
          onExpandAll={stages.expandAll}
          onDirection={setDirection}
        />
      </div>
      {runError ? <p className="v2-error v2-run-error">{runError}</p> : null}
      <Stage
        // A new drawing enters afresh; another run of the same drawing moves into place.
        key={`${flowType}|${selected.file}`}
        isSettled={isSettled}
        targetNeighbours={targetNeighbours}
        dimUnrelated={selectedByReader}
        focusNodeId={frameStepId}
        nowNodeId={nowNodeId}
        handleRef={viewportRef}
        scene={scene}
        direction={direction}
        onOpenStage={openStage}
        onCloseStage={closeStage}
        cue={stages.cue}
        selectedId={selectedId}
        selectedGroupId={selectedGroupId}
        insetRightPx={ownsPanel ? panelWidth : null}
        onSelectGroup={(id) => {
          setSelectedGroupId(id);
          setSelectedId(null);
          setSelectedExecutionId(null);
          setInspectSection(null);
        }}
        onSelect={(id) => {
          setSelectedId(id);
          setSelectedByReader(id !== null);
          // Selecting a Step that can fail draws its detour; selecting inside the detour keeps it.
          if (id === null || !isFailureHandling(flow, id)) setDetourFor(id !== null && hasDetour(flow, id) ? id : null);
          setSelectedGroupId(null);
          setSelectedExecutionId(null);
          setInspectSection(null);
          setPreviousEmpty(false);
        }}
        onInspect={(id) => {
          setSelectedId(id);
          setSelectedGroupId(null);
          setInspectSection('execute-context');
        }}
        legend={() => (
          <div className="plegend-card" data-open={legendOpen ? 'true' : undefined}>
            <button
              type="button"
              className="plegend-toggle"
              onClick={() => setLegendOpen((open) => !open)}
              aria-expanded={legendOpen}
            >
              Legend {legendOpen ? '▾' : '▸'}
            </button>
            {legendOpen ? (
              <>
                <Legend scene={scene} flow={flow} />
                {scene.notes.length > 0 ? (
                  <div className="plegend-notes">
                    {scene.notes.map((note) => <p key={note}>{note}</p>)}
                  </div>
                ) : null}
              </>
            ) : null}
          </div>
        )}
        // Opening or closing a stage, or the Step panel, keeps the reader's place, so neither refits.
        fitKey={`${flowType}|${flowId}|${stages.view}|${selected.file}|${refocusKey}`}
      />
      <p className="sr-only" aria-live="polite">
        {stages.cue?.message}
      </p>
      <p className="sr-only" aria-live="polite">
        {detourFor ? detourMessage(flow, detourFor) : ''}
      </p>
      {ownsPanel && panel ? (
        <>
          <V2SplitHandle
            axis="column"
            cssVariable="--v2-panel-w"
            edge="start"
            invert
            targetRef={canvasRef}
            measureRef={canvasRef}
            value={panelWidth}
            ariaLabel="Resize the Step detail panel"
            onCommit={commitPanelWidth}
          />
          <DetailPanel
            key={`${panel.stepType}|${selectedRecord?.execution.stepExecutionId ?? ''}`}
            model={panel}
            selectedExecutionId={selectedRecord?.execution.stepExecutionId ?? selectedExecutionId}
            initialSection={inspectSection}
            waitEvent={hydratedWaitEvent ?? selectedWaitEvent}
            executeEvent={hydratedExecuteEvent ?? selectedExecuteEvent}
            history={historyEvents}
            parentFlowId={flowId}
            onSection={setInspectSection}
            onClose={() => {
              setSelectedId(null);
              setSelectedExecutionId(null);
              setInspectSection(null);
            }}
            onSelectExecution={setSelectedExecutionId}
            canLoadPrevious={Boolean(previousCursor)}
            loadPreviousBusy={loadPreviousBusy}
            previousEmpty={previousEmpty}
            onLoadPrevious={() => { void loadPrevious(); }}
          />
        </>
      ) : null}
    </div>
  );
}
