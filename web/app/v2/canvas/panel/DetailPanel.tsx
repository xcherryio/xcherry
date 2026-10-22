// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import { useCallback, useEffect, useRef, useState, type CSSProperties, type JSX } from 'react'
import {
  StepMethodContextSection,
  StepMethodInputSection,
  StepMethodOutputSection,
} from '@/app/flows/details/StepMethodSections'
import { storedValueJSONReplacer } from '@/lib/blobs'
import { PullRouting } from '@/app/flows/details/PullRouting'
import type { ActiveStepExecution, FlowHistoryEvent } from '@/lib/types'
import {
  DEF_HEIGHT_DEFAULT,
  DEF_HEIGHT_KEY,
  V2SplitHandle,
  readStoredPixels,
  writeStoredPixels,
} from '../../V2SplitHandle'
import type {
  ExecutionPhase,
  PanelModel,
  PhaseId,
  PhaseTab,
  PhaseTabId,
  Row,
  SectionId,
  TabBody,
} from './panelModel'
import { parseSectionId, VERDICT_TEXT } from './panelModel'

const KIND_GLYPH: Record<string, string> = {
  channel: '✉',
  timer: '⏱',
  subflow: '↳',
  unknown: '?',
}

function Rows({ rows }: { rows: Row[] }): JSX.Element {
  return (
    <dl className="ppan-rows">
      {rows.map((row, index) => (
        <div className="ppan-row" data-tone={row.tone} key={`${row.label}-${index}`}>
          <dt>{row.label}</dt>
          <dd className={row.pre ? 'ppan-pre' : undefined}>{row.value}</dd>
        </div>
      ))}
    </dl>
  )
}

function StepMethodPane({
  part,
  event,
  history,
  parentFlowId,
}: {
  part: 'input' | 'output' | 'context'
  event: FlowHistoryEvent
  history: FlowHistoryEvent[]
  parentFlowId: string
}): JSX.Element {
  const [view, setView] = useState<'details' | 'raw'>('details')
  useEffect(() => {
    setView('details')
  }, [event.eventId, event.type, part])

  return (
    <div className="ppan-step-method">
      <div className="ppan-view-tabs" role="tablist" aria-label="Step method view">
        <button
          type="button"
          role="tab"
          aria-selected={view === 'details'}
          className={view === 'details' ? 'active' : undefined}
          onClick={() => setView('details')}
        >
          Details
        </button>
        <button
          type="button"
          role="tab"
          aria-selected={view === 'raw'}
          className={view === 'raw' ? 'active' : undefined}
          onClick={() => setView('raw')}
        >
          Raw JSON
        </button>
      </div>
      {view === 'raw' ? (
        <pre className="ppan-pre ppan-json">
          {JSON.stringify(event.payload, storedValueJSONReplacer, 2)}
        </pre>
      ) : (
        <div className="semantic-event ppan-semantic">
          {part === 'input' ? (
            <StepMethodInputSection
              event={event}
              history={history}
              parentFlowId={parentFlowId}
              wrapSection={false}
            />
          ) : part === 'output' ? (
            <StepMethodOutputSection
              event={event}
              history={history}
              parentFlowId={parentFlowId}
              wrapSection={false}
            />
          ) : (
            <StepMethodContextSection
              event={event}
              history={history}
              parentFlowId={parentFlowId}
              wrapSection={false}
            />
          )}
        </div>
      )}
    </div>
  )
}

function WaitOutputBody({
  body,
  event,
  history,
  parentFlowId,
}: {
  body: Extract<TabBody, { kind: 'waitOutput' }>
  event: FlowHistoryEvent | null
  history: FlowHistoryEvent[]
  parentFlowId: string
}): JSX.Element {
  return (
    <>
      <table className="ppan-table">
        <thead>
          <tr>
            <th>Condition</th>
            <th>Verdict</th>
          </tr>
        </thead>
        <tbody>
          {body.rows.map((row) => (
            <tr key={`${row.kind}-${row.label}`} data-verdict={row.verdict}>
              <td>
                <span className="ppan-glyph">{KIND_GLYPH[row.kind] ?? '?'}</span> {row.label}
              </td>
              <td>{VERDICT_TEXT[row.verdict]}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {body.winner !== null ? (
        <p className="ppan-note">
          Satisfied by <strong>{body.winner}</strong>. Dex consumes only from the condition that
          won, so anything the losers had queued stays queued.
        </p>
      ) : null}
      {body.answeredBy.length > 0 ? (
        <p className="ppan-note">
          Answered through <strong>{body.answeredBy.join(', ')}</strong>.
        </p>
      ) : null}
      {event !== null ? (
        <>
          <h3>WaitFor payload</h3>
          <StepMethodPane
            part="output"
            event={event}
            history={history.length > 0 ? history : [event]}
            parentFlowId={parentFlowId}
          />
        </>
      ) : (
        <p className="ppan-note">No WaitFor event is loaded for this execution yet.</p>
      )}
    </>
  )
}

function ExecuteOutputBody({
  body,
  event,
  history,
  parentFlowId,
  canLoadPrevious,
}: {
  body: Extract<TabBody, { kind: 'executeOutput' }>
  event: FlowHistoryEvent | null
  history: FlowHistoryEvent[]
  parentFlowId: string
  canLoadPrevious: boolean
}): JSX.Element {
  if (!body.hasExecuteEvent) {
    return (
      <p className="ppan-note">
        {body.isRunningWithoutEvent
          ? 'Execute is running — its completion event is not in history yet.'
          : body.executeStatus === 'notStarted'
            ? 'Execute has not started for this instance yet.'
            : 'Loaded history has no Execute event for this Step yet.'}
        {canLoadPrevious
          ? ' Use Load more from previous run if Continue-as-New moved it.'
          : ''}
      </p>
    )
  }

  return (
    <>
      {body.attempts.length === 0 ? (
        <p className="ppan-note">This step has not run in this run.</p>
      ) : (
        <table className="ppan-table">
          <thead>
            <tr>
              <th>Attempt</th>
              <th>Outcome</th>
            </tr>
          </thead>
          <tbody>
            {body.attempts.map((attempt) => (
              <tr key={attempt.n}>
                <td>{attempt.n}</td>
                <td>{attempt.status}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      <Rows
        rows={[
          { label: 'Retries', value: body.attemptsLeft },
          { label: 'Decision', value: body.decision ?? 'not returned yet' },
          {
            label: 'Next steps',
            value:
              body.nextStepTypes.length > 0
                ? body.nextStepTypes.join(', ')
                : 'none — this closes',
          },
        ]}
      />
      {body.error !== undefined ? (
        <>
          <h3>Error</h3>
          <p className="ppan-pre ppan-error">{body.error}</p>
        </>
      ) : null}
      {event !== null ? (
        <>
          <h3>Execute payload</h3>
          <StepMethodPane
            part="output"
            event={event}
            history={history.length > 0 ? history : [event]}
            parentFlowId={parentFlowId}
          />
        </>
      ) : null}
    </>
  )
}

function TabContent({
  tab,
  event,
  history,
  parentFlowId,
  canLoadPrevious,
  phaseLabel,
}: {
  tab: PhaseTab
  event: FlowHistoryEvent | null
  history: FlowHistoryEvent[]
  parentFlowId: string
  canLoadPrevious: boolean
  phaseLabel: string
}): JSX.Element {
  if (tab.body.kind === 'stepMethod') {
    if (event === null) {
      return (
        <p className="ppan-note">
          No {phaseLabel} event is loaded for this execution.
        </p>
      )
    }
    return (
      <StepMethodPane
        part={tab.body.part}
        event={event}
        history={history.length > 0 ? history : [event]}
        parentFlowId={parentFlowId}
      />
    )
  }
  if (tab.body.kind === 'waitOutput') {
    return (
      <WaitOutputBody
        body={tab.body}
        event={event}
        history={history}
        parentFlowId={parentFlowId}
      />
    )
  }
  return (
    <ExecuteOutputBody
      body={tab.body}
      event={event}
      history={history}
      parentFlowId={parentFlowId}
      canLoadPrevious={canLoadPrevious}
    />
  )
}

function PhaseBlock({
  phase,
  openTab,
  onSelectTab,
  event,
  history,
  parentFlowId,
  canLoadPrevious,
}: {
  phase: ExecutionPhase
  openTab: PhaseTabId
  onSelectTab: (tab: PhaseTabId) => void
  event: FlowHistoryEvent | null
  history: FlowHistoryEvent[]
  parentFlowId: string
  canLoadPrevious: boolean
}): JSX.Element {
  const tab = phase.tabs.find((candidate) => candidate.id === openTab) ?? phase.tabs[0]
  return (
    <section className="ppan-phase" aria-label={phase.label}>
      <header className="ppan-phase-head">
        <h4>{phase.label}</h4>
      </header>
      <nav className="ppan-tabs" aria-label={`${phase.label} sections`}>
        {phase.tabs.map((candidate) => (
          <button
            key={candidate.id}
            type="button"
            className="ppan-tab"
            data-on={candidate.id === tab.id ? 'true' : undefined}
            onClick={() => onSelectTab(candidate.id)}
          >
            {candidate.label}
          </button>
        ))}
      </nav>
      <div className="ppan-phase-body">
        {tab === undefined ? null : (
          <TabContent
            tab={tab}
            event={event}
            history={history}
            parentFlowId={parentFlowId}
            canLoadPrevious={canLoadPrevious}
            phaseLabel={phase.label}
          />
        )}
      </div>
    </section>
  )
}

function resolveOpenTabs(
  model: PanelModel,
  section: SectionId | null | undefined,
): Record<PhaseId, PhaseTabId> {
  const defaults = Object.fromEntries(
    model.phases.map((phase) => [phase.id, phase.defaultTab]),
  ) as Record<PhaseId, PhaseTabId>
  if (section == null) return defaults
  const { phase, tab } = parseSectionId(section)
  if (model.phases.some((candidate) => candidate.id === phase)) {
    defaults[phase] = tab
  }
  return defaults
}

export function DetailPanel({
  model,
  onClose,
  onSelectExecution,
  selectedExecutionId,
  initialSection,
  onSection,
  canLoadPrevious = false,
  loadPreviousBusy = false,
  previousEmpty = false,
  onLoadPrevious,
  waitEvent = null,
  executeEvent = null,
  history = [],
  parentFlowId = '',
  activeStep,
  isPullFlow = false,
}: {
  model: PanelModel
  onClose: () => void
  onSelectExecution: (id: string) => void
  selectedExecutionId: string | null
  initialSection?: SectionId | null
  onSection?: (id: SectionId) => void
  canLoadPrevious?: boolean
  loadPreviousBusy?: boolean
  previousEmpty?: boolean
  onLoadPrevious?: () => void
  waitEvent?: FlowHistoryEvent | null
  executeEvent?: FlowHistoryEvent | null
  history?: FlowHistoryEvent[]
  parentFlowId?: string
  isPullFlow?: boolean
  activeStep?: ActiveStepExecution
}): JSX.Element {
  const panelRef = useRef<HTMLElement>(null)
  const bodyRef = useRef<HTMLDivElement>(null)
  const [openTabs, setOpenTabs] = useState<Record<PhaseId, PhaseTabId>>(() =>
    resolveOpenTabs(model, initialSection ?? model.defaultSection),
  )
  const [definitionOpen, setDefinitionOpen] = useState(true)
  const [branchesExpanded, setBranchesExpanded] = useState(false)
  const [defHeight, setDefHeight] = useState(() => {
    const stored = readStoredPixels(DEF_HEIGHT_KEY)
    return Number.isFinite(stored) ? stored : DEF_HEIGHT_DEFAULT
  })

  const selectedId = selectedExecutionId
    ?? model.executions[model.executions.length - 1]?.id
    ?? ''
  const selectionKey = `${model.stepType}|${selectedId}`

  // Keep the user's tab across history polls; only reset when the Step instance changes.
  useEffect(() => {
    setOpenTabs(resolveOpenTabs(model, initialSection ?? model.defaultSection))
    // Polls rebuild `model`; tab clicks update `initialSection`. Neither should reset tabs.
    // eslint-disable-next-line react-hooks/exhaustive-deps -- selectionKey only
  }, [selectionKey])

  useEffect(() => {
    setBranchesExpanded(false)
  }, [model.stepType])

  const commitDefHeight = useCallback((height: number) => {
    const next = Math.round(height)
    setDefHeight(next)
    writeStoredPixels(DEF_HEIGHT_KEY, next)
  }, [])

  const branchCount = model.definition.branches.length
  const branchesCollapsed = branchCount > 3 && !branchesExpanded
  const panelStyle = {
    '--v2-def-h': `${Math.round(defHeight)}px`,
  } as CSSProperties

  function selectTab(phase: PhaseId, tab: PhaseTabId): void {
    setOpenTabs((current) => ({ ...current, [phase]: tab }))
    onSection?.(`${phase}-${tab}`)
  }

  function eventForPhase(phase: PhaseId): FlowHistoryEvent | null {
    return phase === 'wait' ? waitEvent : executeEvent
  }

  return (
    <aside
      className="ppan"
      ref={panelRef}
      style={panelStyle}
      aria-label={`Detail for ${model.stepType}`}
    >
      <header className="ppan-head">
        <div>
          <h2>{model.title}</h2>
          <p className="ppan-type">{model.stepType}</p>
        </div>
        <button type="button" className="ppan-close" onClick={onClose} aria-label="Close panel">
          ✕
        </button>
      </header>

      <div className="ppan-stack" ref={bodyRef}>
        {activeStep && (isPullFlow || activeStep.waitForPullRouting || activeStep.executePullRouting) && <div className="semantic-event">
          <PullRouting value={activeStep.waitForPullRouting} label="WaitFor routing" />
          <PullRouting value={activeStep.executePullRouting} label="Execute routing" />
        </div>}
        <section
          className="ppan-def"
          data-collapsed={definitionOpen ? undefined : 'true'}
          aria-label="Step definition"
        >
          <button
            type="button"
            className="ppan-def-toggle"
            aria-expanded={definitionOpen}
            onClick={() => setDefinitionOpen((openNow) => !openNow)}
          >
            <span>Definition</span>
            <span className="ppan-def-chevron">{definitionOpen ? '▾' : '▸'}</span>
          </button>
          {definitionOpen ? (
            <div className="ppan-def-body">
              <h3>Explanation</h3>
              {model.definition.explanation === null ? (
                <p className="ppan-note">No dex:explanation on this Step.</p>
              ) : (
                <p className="ppan-def-explanation">{model.definition.explanation}</p>
              )}
              <h3>WaitFor</h3>
              {model.definition.waitFor === null ? (
                <p className="ppan-note">No WaitFor — Execute runs immediately.</p>
              ) : (
                <>
                  <p className="ppan-def-line">{model.definition.waitFor}</p>
                  {model.definition.waitConditions.length > 0 ? (
                    <ul className="ppan-def-list">
                      {model.definition.waitConditions.map((condition) => (
                        <li key={condition}>{condition}</li>
                      ))}
                    </ul>
                  ) : null}
                </>
              )}
              <h3>Execute branches</h3>
              {branchCount === 0 ? (
                <p className="ppan-note">No Execute branches in the definition.</p>
              ) : branchesCollapsed ? (
                <button
                  type="button"
                  className="ppan-branches-toggle"
                  onClick={() => setBranchesExpanded(true)}
                >
                  {branchCount} branches · expand
                </button>
              ) : (
                <>
                  <Rows rows={model.definition.branches} />
                  {branchCount > 3 ? (
                    <button
                      type="button"
                      className="ppan-branches-toggle"
                      onClick={() => setBranchesExpanded(false)}
                    >
                      Collapse branches
                    </button>
                  ) : null}
                </>
              )}
            </div>
          ) : null}
        </section>

        {definitionOpen ? (
          <V2SplitHandle
            axis="row"
            cssVariable="--v2-def-h"
            edge="between"
            invert
            targetRef={panelRef}
            measureRef={bodyRef}
            value={defHeight}
            ariaLabel="Resize Definition and Execution"
            onCommit={commitDefHeight}
          />
        ) : null}

        <section className="ppan-exec" aria-label="Step execution">
          <div className="ppan-exec-head">
            <h3>Execution</h3>
            {model.executions.length > 1 ? (
              <label className="ppan-exec-pick">
                <span>Instance</span>
                <select
                  aria-label="Step execution"
                  value={selectedId}
                  onChange={(change) => onSelectExecution(change.target.value)}
                >
                  {model.executions.map((execution, index) => (
                    <option key={execution.id} value={execution.id}>
                      {`${index + 1}/${model.executions.length} · ${execution.id} · ${execution.execute}`}
                    </option>
                  ))}
                </select>
              </label>
            ) : model.executions.length === 1 ? (
              <p className="ppan-exec-one" title={model.executions[0].id}>
                {model.executions[0].id}
              </p>
            ) : (
              <p className="ppan-note">No executions loaded for this Step.</p>
            )}
          </div>

          <div className="ppan-body">
            {model.phases.map((phase) => (
              <PhaseBlock
                key={phase.id}
                phase={phase}
                openTab={openTabs[phase.id] ?? phase.defaultTab}
                onSelectTab={(tab) => selectTab(phase.id, tab)}
                event={eventForPhase(phase.id)}
                history={history}
                parentFlowId={parentFlowId}
                canLoadPrevious={canLoadPrevious}
              />
            ))}

            {canLoadPrevious ? (
              <div className="ppan-more">
                <button
                  type="button"
                  className="pchip"
                  disabled={loadPreviousBusy || onLoadPrevious === undefined}
                  onClick={onLoadPrevious}
                >
                  {loadPreviousBusy ? 'Loading…' : 'Load more from previous run'}
                </button>
                {previousEmpty ? (
                  <p className="ppan-note">That earlier run has no executions of this step.</p>
                ) : (
                  <p className="ppan-note">
                    Continue-as-New keeps the Flow ID. This loads only this step from the previous run.
                  </p>
                )}
              </div>
            ) : null}
          </div>
        </section>
      </div>
    </aside>
  )
}
