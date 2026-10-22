// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

'use client';

import { useState } from 'react';
import { Link } from 'react-router-dom';
import { storedValueJSONReplacer } from '@/lib/blobs';
import type { FlowHistoryEvent } from '@/lib/types';
import { flowErrorTypeLabel, flowStatusLabel } from '@/lib/semantic';
import {
  ChannelMessages,
  DetailSection,
  EffectsView,
  Fields,
  FlowConfigView,
  FlowTimeoutHandlerOptionsView,
  KeyValues,
  StepDecisionView,
  StepMethodDetails,
  StepMovements,
  StepOptionsView,
  StepOutputs,
  ValueBlock,
  WaitingConditionView,
  asData,
  asDataArray,
  displayValue,
  flowTimeoutFields,
  hasData,
  isPresent,
  type Data,
} from './StepMethodSections';

export { FailureContent } from './StepMethodSections';

const eventTitles: Record<FlowHistoryEvent['type'], string> = {
  FlowStartedOrContinued: 'Flow started',
  FlowClosed: 'Flow closed',
  StepWaitForCompleted: 'WaitForCondition started',
  StepWaitForFailed: 'WaitFor failed',
  StepWaitForPending: 'WaitFor pending',
  StepExecuteCompleted: 'Execute completed',
  StepExecuteFailed: 'Execute failed',
  StepExecutePending: 'Execute pending',
  RpcExecutionCompleted: 'RPC completed',
  ChannelExternalPublish: 'Channel published',
  TimeTravelFork: 'Time Travel fork',
};

export function eventTitle(event: FlowHistoryEvent): string {
  if (isSetAttributesEvent(event)) return 'Attributes updated';
  if (event.type === 'FlowStartedOrContinued' && hasData(asData(event.payload.continuedStart))) {
    return 'Flow continued';
  }
  return eventTitles[event.type];
}

export function eventTypeLabel(event: FlowHistoryEvent): string {
  return isSetAttributesEvent(event) ? 'SetAttributes' : event.type;
}

function isSetAttributesEvent(event: FlowHistoryEvent): boolean {
  return event.type === 'RpcExecutionCompleted'
    && event.payload.isSetAttributeApi === true;
}

function InitialStartDetails({ payload, showHeading = true }: { payload: Data; showHeading?: boolean }) {
  const start = asData(payload.initialStart);
  return (
    <>
      <DetailSection title={showHeading ? 'Initial start' : undefined}>
        <Fields values={[
          ...flowTimeoutFields(payload),
          ['Start step', start.startStepType],
        ]} />
        <ValueBlock label="Step input" value={start.stepInput} />
        <StepOptionsView value={start.stepOptions} />
      </DetailSection>
      {Array.isArray(start.initialAttributes) && start.initialAttributes.length > 0 && (
        <DetailSection title="Initial attributes"><KeyValues values={start.initialAttributes} /></DetailSection>
      )}
      <FlowConfigView value={payload.flowConfig} />
      <FlowTimeoutHandlerOptionsView value={payload.timeoutHandlerOptions} />
    </>
  );
}

function ContinuedStartDetails({
  payload,
  parentFlowId,
  showHeading = true,
}: {
  payload: Data;
  parentFlowId: string;
  showHeading?: boolean;
}) {
  const continued = asData(payload.continuedStart);
  const resumes = asDataArray(continued.stepsToResume);
  const pendingChannels = asData(continued.pendingChannelMessages);
  return (
    <>
      <DetailSection title={showHeading ? 'Continued run' : undefined}>
        <Fields values={[
          ['Previous run ID', continued.previousRunId],
          ...flowTimeoutFields(payload),
        ]} />
      </DetailSection>
      {Array.isArray(continued.stepsToStart) && continued.stepsToStart.length > 0 && (
        <DetailSection title="Steps to start"><StepMovements values={continued.stepsToStart} /></DetailSection>
      )}
      {resumes.length > 0 && (
        <DetailSection title="Steps to resume">
          <div className="semantic-records">
            {resumes.map((resume, index) => (
              <div className="semantic-record" key={`${String(resume.stepExecutionId)}-${index}`}>
                <Fields values={[['Execution ID', resume.stepExecutionId]]} />
                <StepMovements values={[resume.step]} />
                <WaitingConditionView
                  value={resume.waitingCondition}
                  parentFlowId={parentFlowId}
                  stepExecutionId={String(resume.stepExecutionId ?? '')}
                />
                <KeyValues values={resume.stepExeLocals} />
              </div>
            ))}
          </div>
        </DetailSection>
      )}
      {Object.keys(pendingChannels).length > 0 && (
        <DetailSection title="Pending channels">
          <ChannelMessages values={Object.entries(pendingChannels).flatMap(([channelName, entry]) => (
            asDataArray(asData(entry).values).map((value) => ({ channelName, value }))
          ))} />
        </DetailSection>
      )}
      {Array.isArray(continued.attributes) && continued.attributes.length > 0 && (
        <DetailSection title="Attributes"><KeyValues values={continued.attributes} /></DetailSection>
      )}
      {Array.isArray(continued.completedSteps) && continued.completedSteps.length > 0 && (
        <DetailSection title="Completed steps"><StepOutputs values={continued.completedSteps} /></DetailSection>
      )}
      <FlowConfigView value={payload.flowConfig} />
      <FlowTimeoutHandlerOptionsView value={payload.timeoutHandlerOptions} />
    </>
  );
}

function FlowClosedDetails({ payload }: { payload: Data }) {
  const errorType = isPresent(payload.errorType) ? flowErrorTypeLabel(payload.errorType) : undefined;
  return (
    <>
      <DetailSection title="Outcome">
        <Fields values={[
          ['Status', flowStatusLabel(payload.flowStatus)],
          ['Continued to run', payload.continuedToRunId],
        ]} />
      </DetailSection>
      {(isPresent(payload.errorMessage) || (errorType && errorType !== 'Unspecified')) && (
        <DetailSection title="Failure">
          <div className="semantic-alert">
            <strong>{displayValue(payload.errorMessage)}</strong>
            <Fields values={[['Type', errorType]]} />
          </div>
        </DetailSection>
      )}
      {Array.isArray(payload.results) && payload.results.length > 0 && (
        <DetailSection title="Flow results"><StepOutputs values={payload.results} /></DetailSection>
      )}
    </>
  );
}

function TimeTravelForkDetails({ payload, parentFlowId }: { payload: Data; parentFlowId: string }) {
  const previousRunId = typeof payload.previousRunId === 'string' ? payload.previousRunId : '';
  return (
    <DetailSection title="Fork origin">
      <p className="semantic-run-link-row">
        <span>Previous run</span>
        {previousRunId ? (
          <Link
            className="event-run-link"
            title={previousRunId}
            to={`/v1/flows/${encodeURIComponent(parentFlowId)}/${encodeURIComponent(previousRunId)}`}
          >
            <code>{previousRunId}</code>
          </Link>
        ) : <span>—</span>}
      </p>
    </DetailSection>
  );
}

function RPCDetails({ payload, isPull }: { payload: Data; isPull: boolean }) {
  return (
    <>
      <DetailSection title="RPC call">
        <Fields values={[['RPC name', payload.rpcName], ['Serving app Worker ID', isPull ? payload.servedByAppWorkerId || '—' : undefined]]} />
        <ValueBlock label="Input" value={payload.input} />
        <ValueBlock label="Output" value={payload.output} />
      </DetailSection>
      <StepDecisionView value={payload.stepDecision} />
      <EffectsView value={payload} />
    </>
  );
}

function SetAttributesDetails({ payload }: { payload: Data }) {
  return (
    <DetailSection title="Updated attributes">
      <KeyValues values={payload.upsertAttributes} emptyLabel="No attributes updated" />
    </DetailSection>
  );
}

export function SemanticEventDetails({
  event,
  history = [event],
  parentFlowId = '',
  showStartHeading = true,
}: {
  event: FlowHistoryEvent;
  history?: FlowHistoryEvent[];
  parentFlowId?: string;
  showStartHeading?: boolean;
}) {
  if (event.type.startsWith('StepWaitFor') || event.type.startsWith('StepExecute')) {
    return <StepMethodDetails event={event} history={history} parentFlowId={parentFlowId} />;
  }
  if (event.type === 'FlowStartedOrContinued') {
    return hasData(asData(event.payload.continuedStart))
      ? <ContinuedStartDetails
          payload={event.payload}
          parentFlowId={parentFlowId}
          showHeading={showStartHeading}
        />
      : <InitialStartDetails payload={event.payload} showHeading={showStartHeading} />;
  }
  if (event.type === 'FlowClosed') return <FlowClosedDetails payload={event.payload} />;
  if (event.type === 'TimeTravelFork') {
    return <TimeTravelForkDetails payload={event.payload} parentFlowId={parentFlowId} />;
  }
  if (isSetAttributesEvent(event)) return <SetAttributesDetails payload={event.payload} />;
  if (event.type === 'RpcExecutionCompleted') return <RPCDetails payload={event.payload} isPull={Boolean(event.payload.servedByAppWorkerId) || asData(asData(history.find((candidate) => candidate.type === 'FlowStartedOrContinued')?.payload.flowConfig).workerTarget).isPullMode === true} />;
  return (
    <DetailSection title="Published messages">
      <ChannelMessages values={event.payload.messages} />
    </DetailSection>
  );
}

export function EventDetails({
  event,
  history,
  parentFlowId,
}: {
  event: FlowHistoryEvent;
  history: FlowHistoryEvent[];
  parentFlowId: string;
}) {
  const [view, setView] = useState<'details' | 'raw'>('details');
  return (
    <div className="event-details">
      <div className="event-details-content">
        <div className="event-detail-tabs" role="tablist" aria-label="Event payload view">
          <button aria-selected={view === 'details'} className={view === 'details' ? 'active' : ''} role="tab" type="button" onClick={() => setView('details')}>Details</button>
          <button aria-selected={view === 'raw'} className={view === 'raw' ? 'active' : ''} role="tab" type="button" onClick={() => setView('raw')}>Raw JSON</button>
        </div>
        {view === 'details'
          ? <div className="semantic-event"><SemanticEventDetails event={event} history={history} parentFlowId={parentFlowId} /></div>
          : <pre className="raw-event-json">{JSON.stringify(event.payload, storedValueJSONReplacer, 2)}</pre>}
      </div>
    </div>
  );
}
