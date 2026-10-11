// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import { Link } from 'react-router-dom';
import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState, type ReactNode } from 'react';
import type {
  CSSProperties,
  KeyboardEvent as ReactKeyboardEvent,
  PointerEvent as ReactPointerEvent,
} from 'react';
import { formatDate, formatDuration } from '@/lib/format';
import { hydrateBlobs } from '@/lib/blobs';
import { loadCompleteHistory } from '@/lib/history';
import { isTransientGatewayResponse, readResponseJSON } from '@/lib/http';
import { VALUE_BLOB_UNAVAILABLE } from '@/lib/unavailable';
import type {
  FlowHistoryEvent,
  FlowState,
  FlowSummary,
} from '@/lib/types';
import { StatusBadge } from '../components/StatusBadge';
import { LoadingIndicator } from '../components/LoadingIndicator';
import { usePreferences } from '../providers';
import { FlowOverview } from './details/FlowOverview';
import { FlowStatePanel } from './details/FlowStatePanel';
import { TimeTravelDialog } from './details/TimeTravelDialog';
import { StateWriteDialog, type StateWriteKind } from './details/StateWriteDialog';
import { StopFlowDialog } from './details/StopFlowDialog';
import { ExecutionGraph } from './details/ExecutionGraph';
import { StreamPanel } from './details/StreamPanel';
import { Timeline } from './details/Timeline';

type RunTab = 'overview' | 'steps' | 'timeline' | 'stream';

interface SelectedEventConnector {
  height: number;
  path: string;
  tone: 'default' | 'execute' | 'wait-for';
  width: number;
}

const terminalStatuses = new Set([2, 3, 4, 5, 6, 7]);
const sidebarWidthStorageKey = 'dex.run.selected-event-width';
const runTabStorageKey = 'dex.run.selected-tab';
const runTabs = new Set<RunTab>(['overview', 'steps', 'timeline', 'stream']);
const defaultSidebarWidth = 340;
const minimumSidebarWidth = 280;
const maximumSidebarWidth = 720;

function clampSidebarWidth(width: number): number {
  return Math.min(maximumSidebarWidth, Math.max(minimumSidebarWidth, width));
}

function storedSidebarWidth(): number {
  if (typeof window === 'undefined') return defaultSidebarWidth;
  try {
    const width = Number.parseInt(window.localStorage.getItem(sidebarWidthStorageKey) || '', 10);
    return Number.isFinite(width) ? clampSidebarWidth(width) : defaultSidebarWidth;
  } catch (storageError) {
    console.warn('Unable to read the selected event width preference.', storageError);
    return defaultSidebarWidth;
  }
}

function persistSidebarWidth(width: number) {
  try {
    window.localStorage.setItem(sidebarWidthStorageKey, String(width));
  } catch (storageError) {
    console.warn('Unable to save the selected event width preference.', storageError);
  }
}

function storedRunTab(): RunTab {
  if (typeof window === 'undefined') return 'steps';
  try {
    const tab = window.localStorage.getItem(runTabStorageKey);
    return tab && runTabs.has(tab as RunTab) ? tab as RunTab : 'steps';
  } catch (storageError) {
    console.warn('Unable to read the selected run tab preference.', storageError);
    return 'steps';
  }
}

function persistRunTab(tab: RunTab) {
  try {
    window.localStorage.setItem(runTabStorageKey, tab);
  } catch (storageError) {
    console.warn('Unable to save the selected run tab preference.', storageError);
  }
}

function selectedEventConnectorTone(event: FlowHistoryEvent): SelectedEventConnector['tone'] {
  if (event.type.startsWith('StepWaitFor')) return 'wait-for';
  if (event.type.startsWith('StepExecute')) return 'execute';
  return 'default';
}

export function RunDetailsPage({
  flowId,
  runId,
  breadcrumb,
  runPath,
}: {
  flowId: string;
  runId: string;
  /** Supplied by the host shell, which owns where "up" goes. */
  breadcrumb: ReactNode;
  /** Where a sibling run in this chain lives under the host's routes. */
  runPath: (chainRunID: string) => string;
}) {
  const { timezone } = usePreferences();
  const [summary, setSummary] = useState<FlowSummary | null>(null);
  const [history, setHistory] = useState<FlowHistoryEvent[]>([]);
  const [state, setState] = useState<FlowState | null>(null);
  const [nextInternalEventId, setNextInternalEventId] = useState(0);
  const [tab, setTab] = useState<RunTab>(storedRunTab);
  const [selectedEvent, setSelectedEvent] = useState<FlowHistoryEvent | null>(null);
  const [loading, setLoading] = useState(true);
  const [pendingRunRequestCount, setPendingRunRequestCount] = useState(0);
  const [deletingChannelMessage, setDeletingChannelMessage] = useState('');
  const [error, setError] = useState('');
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [timeTravelOpen, setTimeTravelOpen] = useState(false);
  const [timeTravelEvent, setTimeTravelEvent] = useState<FlowHistoryEvent | null>(null);
  const [stopOpen, setStopOpen] = useState(false);
  const [stateWriteKind, setStateWriteKind] = useState<StateWriteKind | null>(null);
  const [waitCycle, setWaitCycle] = useState(0);
  const [hydratedEvents, setHydratedEvents] = useState<Record<number, FlowHistoryEvent>>({});
  const [dataWarnings, setDataWarnings] = useState<string[]>([]);
  const [sidebarWidth, setSidebarWidth] = useState(storedSidebarWidth);
  const [resizingSidebar, setResizingSidebar] = useState(false);
  const [selectedEventConnector, setSelectedEventConnector] = useState<SelectedEventConnector | null>(null);
  const waitGeneration = useRef(0);
  const blobCache = useRef(new Map<string, unknown>());
  const hydratingEventIDs = useRef(new Set<number>());
  const hydratedEventIDs = useRef(new Set<number>());
  const sidebarWidthRef = useRef(sidebarWidth);
  const sidebarResizeStart = useRef({ pointerX: 0, width: sidebarWidth });
  const runContent = useRef<HTMLDivElement>(null);

  const summaryURL = `/api/flows/summary?flowId=${encodeURIComponent(flowId)}&runId=${encodeURIComponent(runId)}`;
  const stateURL = `/api/flows/state?flowId=${encodeURIComponent(flowId)}&runId=${encodeURIComponent(runId)}`;
  const addDataWarning = useCallback((warning: string) => {
    setDataWarnings((current) => current.includes(warning) ? current : [...current, warning]);
  }, []);
  const updateSidebarWidth = useCallback((width: number) => {
    const nextWidth = clampSidebarWidth(width);
    sidebarWidthRef.current = nextWidth;
    setSidebarWidth(nextWidth);
    return nextWidth;
  }, []);
  const startSidebarResize = useCallback((event: ReactPointerEvent<HTMLButtonElement>) => {
    if (event.button !== 0) return;
    event.preventDefault();
    sidebarResizeStart.current = {
      pointerX: event.clientX,
      width: sidebarWidthRef.current,
    };
    setResizingSidebar(true);
  }, []);
  const resizeSidebarWithKeyboard = useCallback((event: ReactKeyboardEvent<HTMLButtonElement>) => {
    let nextWidth = sidebarWidthRef.current;
    if (event.key === 'ArrowLeft') nextWidth += 24;
    else if (event.key === 'ArrowRight') nextWidth -= 24;
    else if (event.key === 'Home') nextWidth = minimumSidebarWidth;
    else if (event.key === 'End') nextWidth = maximumSidebarWidth;
    else return;
    event.preventDefault();
    persistSidebarWidth(updateSidebarWidth(nextWidth));
  }, [updateSidebarWidth]);

  useEffect(() => {
    if (!resizingSidebar) return;
    const resize = (event: PointerEvent) => {
      const start = sidebarResizeStart.current;
      updateSidebarWidth(start.width + start.pointerX - event.clientX);
    };
    const finish = () => {
      persistSidebarWidth(sidebarWidthRef.current);
      setResizingSidebar(false);
    };
    window.addEventListener('pointermove', resize);
    window.addEventListener('pointerup', finish, { once: true });
    window.addEventListener('pointercancel', finish, { once: true });
    return () => {
      window.removeEventListener('pointermove', resize);
      window.removeEventListener('pointerup', finish);
      window.removeEventListener('pointercancel', finish);
    };
  }, [resizingSidebar, updateSidebarWidth]);

  const loadState = useCallback(async (statusCode: number) => {
    if (terminalStatuses.has(statusCode)) {
      setState(null);
      return;
    }
    try {
      const rawState = await readResponseJSON<FlowState>(await fetch(stateURL, { cache: 'no-store' }));
      setState(rawState);
      const hydrated = await hydrateBlobs(flowId, rawState, blobCache.current);
      setState(hydrated.value);
      if (hydrated.error) addDataWarning(hydrated.error);
    } catch (stateError) {
      setError(stateError instanceof Error ? stateError.message : 'State query failed');
    }
  }, [addDataWarning, flowId, stateURL]);

  const deleteChannelMessage = useCallback(async (channelName: string, messageId: string) => {
    if (!summary) return;
    setDeletingChannelMessage(messageId);
    setError('');
    try {
      await readResponseJSON(await fetch('/api/flows/channels/messages', {
        method: 'DELETE',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ flowId, runId, channelName, messageId }),
      }));
    } catch (deleteError) {
      setError(deleteError instanceof Error ? deleteError.message : 'Channel deletion failed');
    } finally {
      setDeletingChannelMessage('');
      await loadState(summary.flowStatusCode);
    }
  }, [flowId, loadState, runId, summary]);

  const hydrateEvent = useCallback(async (event: FlowHistoryEvent) => {
    if (hydratedEventIDs.current.has(event.eventId)
      || hydratingEventIDs.current.has(event.eventId)) return;
    hydratingEventIDs.current.add(event.eventId);
    try {
      const hydrated = await hydrateBlobs(flowId, event, blobCache.current);
      setHydratedEvents((current) => ({ ...current, [event.eventId]: hydrated.value }));
      hydratedEventIDs.current.add(event.eventId);
      if (hydrated.error) addDataWarning(hydrated.error);
    } catch {
      addDataWarning(VALUE_BLOB_UNAVAILABLE);
    } finally {
      hydratingEventIDs.current.delete(event.eventId);
    }
  }, [addDataWarning, flowId]);

  const loadSummary = useCallback(async () => {
    const value = await readResponseJSON<FlowSummary>(await fetch(summaryURL, { cache: 'no-store' }));
    setSummary(value);
    return value;
  }, [summaryURL]);

  const loadHistory = useCallback(async (
    initialPageToken: string,
    initialInternalEventId: number,
    append: boolean,
  ) => {
    setPendingRunRequestCount((current) => current + 1);
    try {
      const complete = await loadCompleteHistory(async (nextPageToken, startInternalEventId) => {
        const params = new URLSearchParams({
          flowId,
          runId,
          startInternalEventId: String(startInternalEventId),
          estimatePageSize: '200',
        });
        if (nextPageToken) params.set('nextPageToken', nextPageToken);
        return readResponseJSON(
          await fetch(`/api/flows/history?${params}`, { cache: 'no-store' }),
        );
      }, initialPageToken, initialInternalEventId);
      setHistory((current) => {
        const combined = append ? [...current, ...complete.events] : complete.events;
        return [...new Map(combined.map((event) => [event.eventId, event])).values()]
          .sort((left, right) => left.eventId - right.eventId);
      });
      setNextInternalEventId(complete.nextInternalEventId);
      return complete;
    } finally {
      setPendingRunRequestCount((current) => current - 1);
    }
  }, [flowId, runId]);

  const refresh = useCallback(async () => {
    setPendingRunRequestCount((current) => current + 1);
    setError('');
    try {
      const latestSummary = await loadSummary();
      await Promise.all([
        loadHistory('', 0, false),
        loadState(latestSummary.flowStatusCode),
      ]);
    } catch (refreshError) {
      setError(refreshError instanceof Error ? refreshError.message : 'Run refresh failed');
    } finally {
      setPendingRunRequestCount((current) => current - 1);
    }
  }, [loadHistory, loadState, loadSummary]);

  useEffect(() => {
    let active = true;
    setSummary(null);
    setHistory([]);
    setState(null);
    setNextInternalEventId(0);
    setSelectedEvent(null);
    setHydratedEvents({});
    setDataWarnings([]);
    blobCache.current.clear();
    hydratingEventIDs.current.clear();
    hydratedEventIDs.current.clear();
    setError('');
    setLoading(true);
    void refresh().finally(() => {
      if (active) setLoading(false);
    });
    return () => {
      active = false;
      waitGeneration.current += 1;
    };
  }, [refresh]);

  useEffect(() => {
    if (
      !autoRefresh
      || !summary
      || terminalStatuses.has(summary.flowStatusCode)
      || nextInternalEventId <= 0
    ) return;

    const generation = ++waitGeneration.current;
    const controller = new AbortController();
    const params = new URLSearchParams({
      flowId,
      runId,
      nextInternalEventId: String(nextInternalEventId),
    });
    void fetch(`/api/flows/wait?${params}`, { cache: 'no-store', signal: controller.signal })
      .then(async (response) => {
        if (generation !== waitGeneration.current) return;
        if (response.status === 408) {
          setWaitCycle((current) => current + 1);
          return;
        }
        if (isTransientGatewayResponse(response)) {
          window.setTimeout(() => setWaitCycle((current) => current + 1), 1000);
          return;
        }
        await readResponseJSON(response);
        const latestSummary = await loadSummary();
        await Promise.all([
          loadHistory('', nextInternalEventId, true),
          loadState(latestSummary.flowStatusCode),
        ]);
      })
      .catch((waitError) => {
        if (waitError instanceof DOMException && waitError.name === 'AbortError') return;
        setError(waitError instanceof Error ? waitError.message : 'History wait failed');
        window.setTimeout(() => setWaitCycle((current) => current + 1), 1000);
      });
    return () => controller.abort();
  }, [
    autoRefresh,
    flowId,
    loadHistory,
    loadState,
    loadSummary,
    nextInternalEventId,
    runId,
    summary,
    waitCycle,
  ]);

  const tabs: Array<[RunTab, string]> = [
    ['overview', 'Overview'],
    ['steps', 'Execution graph'],
    ['timeline', 'Timeline'],
    ['stream', 'Streams'],
  ];
  const latestEvent = history.at(-1);
  const selectedRaw = selectedEvent ?? latestEvent ?? null;
  const selected = selectedRaw ? hydratedEvents[selectedRaw.eventId] ?? selectedRaw : null;
  const displayedHistory = useMemo(
    () => history.map((event) => hydratedEvents[event.eventId] ?? event),
    [history, hydratedEvents],
  );

  useEffect(() => {
    if (selectedRaw) void hydrateEvent(selectedRaw);
  }, [hydrateEvent, selectedRaw?.eventId]);

  useEffect(() => {
    if (tab !== 'overview') return;
    const startEvent = history.find((event) => event.type === 'FlowStartedOrContinued');
    if (startEvent) void hydrateEvent(startEvent);
  }, [hydrateEvent, history, tab]);

  useLayoutEffect(() => {
    const content = runContent.current;
    const selectedEventID = selectedEvent?.eventId;
    if (!content || tab !== 'timeline' || !selectedEventID) {
      setSelectedEventConnector(null);
      return;
    }
    const source = content.querySelector<HTMLElement>(`[data-event-id="${selectedEventID}"] .event-card`);
    const target = content.querySelector<HTMLElement>('[data-selected-event-target]');
    if (!source || !target) {
      setSelectedEventConnector(null);
      return;
    }
    let animationFrame = 0;
    const update = () => {
      animationFrame = 0;
      if (window.innerWidth <= 1100) {
        setSelectedEventConnector(null);
        return;
      }
      const contentRect = content.getBoundingClientRect();
      const sourceRect = source.getBoundingClientRect();
      const targetRect = target.getBoundingClientRect();
      const startX = sourceRect.right - contentRect.left + 4;
      const startY = sourceRect.top + sourceRect.height / 2 - contentRect.top;
      const endX = targetRect.left - contentRect.left - 4;
      const endY = targetRect.top + Math.min(48, targetRect.height / 2) - contentRect.top;
      const bendX = startX + (endX - startX) / 2;
      setSelectedEventConnector({
        height: content.scrollHeight,
        path: `M ${startX} ${startY} H ${bendX} V ${endY} H ${endX}`,
        tone: selectedEventConnectorTone(selectedEvent),
        width: content.scrollWidth,
      });
    };
    const scheduleUpdate = () => {
      if (!animationFrame) animationFrame = window.requestAnimationFrame(update);
    };
    const observer = new ResizeObserver(scheduleUpdate);
    observer.observe(content);
    observer.observe(source);
    observer.observe(target);
    window.addEventListener('resize', scheduleUpdate);
    window.addEventListener('scroll', scheduleUpdate, true);
    scheduleUpdate();
    return () => {
      if (animationFrame) window.cancelAnimationFrame(animationFrame);
      observer.disconnect();
      window.removeEventListener('resize', scheduleUpdate);
      window.removeEventListener('scroll', scheduleUpdate, true);
    };
  }, [selectedEvent, sidebarWidth, tab]);

  const runChain = useMemo(() => {
    if (!summary) return [];
    return [...new Set([summary.firstRunId, summary.runId].filter(Boolean))];
  }, [summary]);
  const continuedToRunId = useMemo(() => {
    const closed = [...history].reverse().find((event) => event.type === 'FlowClosed');
    const nextRunId = closed?.payload.continuedToRunId;
    return typeof nextRunId === 'string' ? nextRunId : '';
  }, [history]);

  const openTimeTravel = useCallback((event: FlowHistoryEvent | null) => {
    setTimeTravelEvent(event);
    setTimeTravelOpen(true);
  }, []);

  const closeTimeTravel = useCallback(() => {
    setTimeTravelOpen(false);
    setTimeTravelEvent(null);
  }, []);

  if (loading && !summary) return <LoadingIndicator label="Loading flow run…" />;
  const isWaitingForHistory = history.length === 0 && (loading || pendingRunRequestCount > 0);

  return (
    <div className="run-page">
      <section className="run-header">
        {breadcrumb}
        <div className="run-title-row">
          <div>
            <p className="eyebrow">{summary?.flowType || 'Flow execution'}</p>
            <h1>{flowId}</h1>
          </div>
          <div className="run-actions">
            {continuedToRunId && (
              <Link className="button primary" to={runPath(continuedToRunId)}>
                Next run
              </Link>
            )}
            <label className="toggle-label">
              <input
                type="checkbox"
                checked={autoRefresh}
                onChange={(event) => setAutoRefresh(event.target.checked)}
              />
              Live
            </label>
            <button className="button ghost" onClick={() => void navigator.clipboard.writeText(window.location.href)}>
              Copy link
            </button>
            <button className="button ghost" onClick={() => void refresh()}>Refresh</button>
            <button
              className="button danger"
              disabled={!summary || terminalStatuses.has(summary.flowStatusCode)}
              onClick={() => setStopOpen(true)}
            >
              Stop
            </button>
            <button
              className="button danger"
              onClick={() => openTimeTravel(selectedEvent ? selected : null)}
            >
              Time Travel
            </button>
          </div>
        </div>
        {summary && (
          <div className="summary-strip">
            <div><span>Status</span><StatusBadge status={summary.flowStatus} /></div>
            <div><span>Started</span><b>{formatDate(summary.startTime, timezone)}</b></div>
            <div><span>Closed</span><b>{formatDate(summary.closeTime, timezone)}</b></div>
            <div><span>Duration</span><b>{formatDuration(summary.startTime, summary.closeTime)}</b></div>
            <div><span>Run chain</span><b>{runChain.length}</b></div>
            <div><span>Semantic events</span><b>{history.length}</b></div>
          </div>
        )}
      </section>

      {error && <div className="error-banner run-error">{error}</div>}
      {dataWarnings.map((warning) => (
        <div className="warning-banner run-error" key={warning}>{warning}</div>
      ))}

      <div className="run-tabs" role="tablist">
        {tabs.map(([id, label]) => (
          <button
            key={id}
            className={tab === id ? 'active' : ''}
            onClick={() => {
              setTab(id);
              persistRunTab(id);
            }}
          >
            {label}
          </button>
        ))}
      </div>

      <div
        className={[
          'run-content',
          tab === 'overview' ? 'run-content--overview' : '',
          resizingSidebar ? 'is-resizing-sidebar' : '',
        ].filter(Boolean).join(' ')}
        ref={runContent}
        style={{ '--run-sidebar-width': `${sidebarWidth}px` } as CSSProperties}
      >
        {selectedEventConnector && (
          <svg
            aria-hidden="true"
            className={`selected-event-connector tone-${selectedEventConnector.tone}`}
            height={selectedEventConnector.height}
            viewBox={`0 0 ${selectedEventConnector.width} ${selectedEventConnector.height}`}
            width={selectedEventConnector.width}
          >
            <path d={selectedEventConnector.path} />
          </svg>
        )}
        <section className="run-primary">
          {isWaitingForHistory && (tab === 'steps' || tab === 'timeline') && (
            <LoadingIndicator className="card page-loading" label="Loading events…" />
          )}
          {tab === 'overview' && summary && (
            <FlowOverview
              summary={summary}
              events={displayedHistory}
              state={state}
              selectedEvent={selected}
              deletingChannelMessage={deletingChannelMessage}
              onDeleteChannelMessage={deleteChannelMessage}
              onOpenStateWrite={setStateWriteKind}
            />
          )}
          {tab === 'steps' && !isWaitingForHistory && (
            <ExecutionGraph
              flowId={flowId}
              events={displayedHistory}
              state={state}
              selectedEvent={selectedEvent}
              onSelectEvent={setSelectedEvent}
            />
          )}
          {tab === 'timeline' && !isWaitingForHistory && (
            <Timeline
              flowId={flowId}
              events={displayedHistory}
              selectedEvent={selectedEvent}
              onSelectEvent={setSelectedEvent}
            />
          )}
          {tab === 'stream' && summary && (
            <StreamPanel flowId={flowId} flowType={summary.flowType} />
          )}
        </section>
        {(tab === 'steps' || tab === 'timeline') && (
          <aside className="run-sidebar">
            <button
              type="button"
              className="run-sidebar-resizer"
              role="separator"
              aria-label="Resize selected event panel"
              aria-orientation="vertical"
              aria-valuemin={minimumSidebarWidth}
              aria-valuemax={maximumSidebarWidth}
              aria-valuenow={sidebarWidth}
              onDoubleClick={() => persistSidebarWidth(updateSidebarWidth(defaultSidebarWidth))}
              onKeyDown={resizeSidebarWithKeyboard}
              onPointerDown={startSidebarResize}
            />
            <FlowStatePanel
              selectedEvent={selected}
              history={displayedHistory}
              parentFlowId={flowId}
            />
          </aside>
        )}
      </div>

      {summary && (
        <TimeTravelDialog
          key={timeTravelEvent?.eventId ?? 'manual'}
          open={timeTravelOpen}
          summary={summary}
          events={history}
          initialEvent={timeTravelEvent}
          onClose={closeTimeTravel}
        />
      )}
      {summary && (
        <StopFlowDialog
          open={stopOpen}
          summary={summary}
          onClose={() => setStopOpen(false)}
          onStopped={() => void refresh()}
        />
      )}
      {summary && (
        <StateWriteDialog
          key={stateWriteKind ?? 'closed'}
          kind={stateWriteKind}
          summary={summary}
          onClose={() => setStateWriteKind(null)}
          onWritten={() => loadState(summary.flowStatusCode)}
        />
      )}
    </div>
  );
}
