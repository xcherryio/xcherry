// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import { useCallback, useEffect, useMemo, useRef, useState, type CSSProperties } from 'react';
import { Navigate, useNavigate, useParams } from 'react-router-dom';
import type { FlowSummary } from '@/lib/types';
import { v2HomePath, v2RunPath } from './contract';
import './css/v2.css';
import { RUN_COPY } from './run/copy';
import { RunDetailDrawer, type StepBand } from './run/RunDetailDrawer';
import type { ActionableStep } from './run/actionableSteps';
import type { StepContextView } from './run/stepContext';
import { RunList } from './workspace/RunList';
import { V2Canvas } from './V2Canvas';
import {
  DRAWER_WIDTH_DEFAULT,
  DRAWER_WIDTH_KEY,
  LIST_WIDTH_DEFAULT,
  LIST_WIDTH_KEY,
  V2SplitHandle,
  useCollapsibleColumn,
} from './V2SplitHandle';
import { useWebCatalog } from './WebCatalogProvider';
import { RunSearch } from './workspace/RunSearch';
import { useFlowSearch } from './workspace/useFlowSearch';
import { useRunQuery } from './workspace/useRunQuery';
import { useStrandedRuns } from './workspace/useStrandedRuns';
import { permissionActionLabels, permissionsOf } from './workspace/permissions';
import { WORK_QUEUE_COPY } from './work-queue/copy';
import { StartFlowDialog } from './start/StartFlowDialog';

export function HomePage() {
  const { ready, canUseV2, error } = useWebCatalog();
  if (error) return <div className="page-shell"><div className="error-banner">{error}</div></div>;
  if (!ready) return <div className="page-loading">Loading Dex Web…</div>;
  return <Navigate to={v2HomePath(canUseV2)} replace />;
}

/**
 * The Admin semantic view: pick a run, see where it is on the canvas, act on it in the drawer.
 *
 * Selection is narrow and the canvas is wide on purpose. Scoping a search belongs to the Queue,
 * and the technical record belongs to the Deep Dive.
 */
export function RunWorkspace() {
  const { flowType = '', flowId = '' } = useParams();
  const navigate = useNavigate();
  const {
    ready, canUseV2, catalog, error, definitionUpdateKey, handleDefinitionError,
  } = useWebCatalog();
  const entry = catalog?.flows.find((candidate) => candidate.flowType === flowType);
  const runQuery = useRunQuery(entry?.definition);
  const [permission, setPermission] = useState('');
  const [refocusKey, setRefocusKey] = useState(0);
  const workQueuePermissions = useMemo(
    () => permission === '' ? [] : [permission],
    [permission],
  );
  const search = useFlowSearch(
    flowType || undefined,
    entry?.definition,
    runQuery.appliedFilters,
    workQueuePermissions,
  );
  const { strandedFlowIDs, rememberStranded } = useStrandedRuns();
  const [band, setBand] = useState<StepBand | null>(null);
  const [stepView, setStepView] = useState<StepContextView | null>(null);
  const [actionable, setActionable] = useState<ActionableStep[]>([]);
  const [summary, setSummary] = useState<FlowSummary | null>(null);
  const [tick, setTick] = useState(0);
  /** Bumped to tell the canvas to drop its Step selection. */
  const [deselectKey, setDeselectKey] = useState(0);
  /** Dismissing the drawer keeps the run on the canvas; it just hands the width back. */
  const [drawerOpen, setDrawerOpen] = useState(true);
  const [startOpen, setStartOpen] = useState(false);
  const [startNotice, setStartNotice] = useState('');
  const shellRef = useRef<HTMLDivElement>(null);
  const bodyRef = useRef<HTMLDivElement>(null);
  const listPane = useCollapsibleColumn(LIST_WIDTH_KEY, LIST_WIDTH_DEFAULT);
  const drawerPane = useCollapsibleColumn(DRAWER_WIDTH_KEY, DRAWER_WIDTH_DEFAULT);

  // One clock: the canvas owns the run poll and the drawer refreshes on the same beat.
  const onTick = useCallback(() => setTick((previous) => previous + 1), []);

  // A newly chosen run always opens its drawer, even if the last one was dismissed.
  useEffect(() => { setDrawerOpen(true); }, [flowId]);
  useEffect(() => {
    if (definitionUpdateKey > 0) {
      setStartOpen(false);
      navigate(v2RunPath(flowType || undefined), { replace: true });
    }
  }, [definitionUpdateKey, flowType, navigate]);
  useEffect(() => { setStartOpen(false); setStartNotice(''); }, [flowType, catalog?.definitionRevision]);

  if (!ready) return <div className="page-loading">Loading Dex Web…</div>;
  if (!canUseV2) return <Navigate to="/v1/flows" replace />;
  if (error) return <div className="page-shell"><div className="error-banner">{error}</div></div>;
  if (!catalog) return <div className="page-loading">Loading Dex Web…</div>;
  if (catalog.flows.length === 0) {
    return (
      <div className="v2-shell sv">
        <div className="v2-empty">Load a valid Flow Definition Graph 2.0 file to search runs in v2.</div>
      </div>
    );
  }
  if (!flowType) return <Navigate to={v2RunPath(catalog.flows[0].flowType)} replace />;
  if (!entry) return <Navigate to={v2RunPath()} replace />;

  const selectedFlow = search.flows.find((flow) => flow.flowId === flowId);
  // A Step click alone opens the drawer: its Flow-level meaning needs no run.
  const showRun = Boolean(flowId) && drawerOpen;
  const showDrawer = showRun || stepView !== null;
  const paneStyle = {
    '--v2-list-w': `${listPane.width}px`,
    '--v2-drawer-w': `${drawerPane.width}px`,
  } as CSSProperties;
  return (
    <div className="v2-shell v2-run" ref={shellRef} style={paneStyle}>
      <div className="v2-run-body" data-has-drawer={showDrawer ? 'true' : undefined} ref={bodyRef}>
        <RunList
          collapsed={listPane.isCollapsed}
          emptyText={RUN_COPY.noRuns}
          entry={entry}
          flowTypes={catalog.flows}
          heading={RUN_COPY.runsHeading}
          headerAction={entry.definition.start ? (
            <button className="button primary sq-start" onClick={() => setStartOpen(true)} type="button">
              Start Flow
            </button>
          ) : undefined}
          onExpand={listPane.expand}
          permissionControl={permissionsOf(entry.definition).length > 0 ? (
            <label className="rsw-permission">
              <span className="rsw-zonehead">{WORK_QUEUE_COPY.permissionLabel}</span>
              <select
                aria-label={WORK_QUEUE_COPY.permissionLabel}
                value={permission}
                onChange={(event) => setPermission(event.target.value)}
              >
                <option value="">{WORK_QUEUE_COPY.anyPermission}</option>
                {permissionsOf(entry.definition).map((candidate) => (
                  <option key={candidate} value={candidate}>{candidate}</option>
                ))}
              </select>
              {permission !== '' && (
                <small className="rsw-permission-note">
                  {WORK_QUEUE_COPY.permissionActions(
                    permissionActionLabels(entry.definition, permission),
                  )}
                </small>
              )}
            </label>
          ) : undefined}
          scope={(
            <RunSearch
              busy={search.loading}
              definition={entry.definition}
              query={runQuery.query}
              onChange={runQuery.setQuery}
              onClear={runQuery.clear}
              onSubmit={runQuery.submit}
            />
          )}
          search={search}
          selectedFlowID={flowId}
          strandedFlowIDs={strandedFlowIDs}
          onSelectFlowType={(next) => navigate(v2RunPath(next))}
          onSelectRun={(nextFlowID) => {
            // Picking the open run again takes the canvas back to where the run is.
            if (nextFlowID === flowId) setRefocusKey((key) => key + 1);
            navigate(v2RunPath(entry.flowType, nextFlowID));
          }}
        />
        <V2SplitHandle
          axis="column"
          cssVariable="--v2-list-w"
          edge="end"
          pane="list"
          targetRef={shellRef}
          measureRef={bodyRef}
          value={listPane.width}
          ariaLabel={RUN_COPY.resizeList}
          onCommit={listPane.commit}
          onToggle={listPane.isCollapsed ? listPane.expand : listPane.collapse}
        />
        <section className="v2-canvas" aria-label="Flow definition">
          <V2Canvas
            flowId={flowId}
            flowType={entry.flowType}
            onActionable={setActionable}
            onBand={setBand}
            onStepContext={setStepView}
            deselectKey={deselectKey}
            focusBlockingStep={showRun}
            refocusKey={refocusKey}
            onSummary={setSummary}
            showStepPanel={false}
            onTick={onTick}
          />
        </section>
        {showDrawer ? (
          <>
            <V2SplitHandle
              axis="column"
              cssVariable="--v2-drawer-w"
              edge="start"
              invert
              pane="drawer"
              targetRef={shellRef}
              measureRef={bodyRef}
              value={drawerPane.width}
              ariaLabel={RUN_COPY.resizeDrawer}
              onCommit={drawerPane.commit}
              onToggle={drawerPane.isCollapsed ? drawerPane.expand : drawerPane.collapse}
            />
            {drawerPane.isCollapsed ? (
              <button
                className="v2-rail"
                onClick={drawerPane.expand}
                title={RUN_COPY.expandDrawer}
                type="button"
              >
                {flowId || RUN_COPY.thisStep}
              </button>
            ) : (
              <RunDetailDrawer
                actionable={actionable}
                band={band}
                definition={entry.definition}
                flowId={showRun ? flowId : ''}
                flowStatusCode={selectedFlow?.flowStatusCode}
                flowType={entry.flowType}
                reloadKey={tick}
                stepContext={stepView}
                summary={summary}
                onClose={() => {
                  setDrawerOpen(false);
                  setDeselectKey((previous) => previous + 1);
                }}
                onStopped={search.runSearch}
                onStranded={rememberStranded}
                workQueuePermissions={workQueuePermissions}
              />
            )}
          </>
        ) : null}
      </div>
      {startNotice && <div className="sfd-notice" role="status">{startNotice}</div>}
      {startOpen && entry.definition.start && (
        <StartFlowDialog
          definition={entry.definition.start}
          definitionRevision={catalog.definitionRevision}
          flowType={entry.flowType}
          onClose={() => setStartOpen(false)}
          onDefinitionChanged={handleDefinitionError}
          onStarted={(startedFlowID) => {
            setStartOpen(false);
            setStartNotice(`Started Flow ${startedFlowID}.`);
            void search.runSearch();
            navigate(v2RunPath(entry.flowType, startedFlowID));
          }}
        />
      )}
    </div>
  );
}
