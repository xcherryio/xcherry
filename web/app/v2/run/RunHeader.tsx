// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import { FlowRouting, type FlowRoutingData } from '@/app/flows/details/PullRouting';
import { useState } from 'react';
import { Link } from 'react-router-dom';
import { StopFlowDialog } from '@/app/flows/details/StopFlowDialog';
import { formatDate, formatDuration } from '@/lib/format';
import type { FlowSummary } from '@/lib/types';
import { usePreferences } from '../../providers';
import { v2DebugPath } from '../contract';
import { isOpenFlowStatusCode } from '../work-queue/liveness';
import { RUN_COPY } from './copy';

/**
 * The run's own identity, which v2 never rendered even though the canvas already fetches it.
 * Stop lives here because v2 could not stop a run at all before.
 */
export function RunHeader({
  flowType,
  flowId,
  summary,
  routing,
  onStopped,
}: {
  flowType: string;
  flowId: string;
  summary: FlowSummary | null;
  routing?: FlowRoutingData | null;
  onStopped: () => void;
}) {
  const { timezone } = usePreferences();
  const [stopOpen, setStopOpen] = useState(false);
  const isOpen = isOpenFlowStatusCode(summary?.flowStatusCode);
  return (
    <div className="rhd">
      <div className="rhd-line">
        <span className="rhd-id t-mono" title={flowId}>{flowId}</span>
        {summary && <span className="sc-status">{summary.flowStatus}</span>}
        <Link
          className="rhd-inspect t-mono"
          title={RUN_COPY.inspectHint}
          to={v2DebugPath(flowType, flowId, summary?.runId)}
        >
          {RUN_COPY.inspect}
          <span aria-hidden="true">›</span>
        </Link>
      </div>
      {summary && (
        <dl className="rhd-facts">
          <div><dt>Run</dt><dd className="t-mono">{summary.runId}</dd></div>
          <div><dt>Started</dt><dd>{formatDate(summary.startTime, timezone)}</dd></div>
          <div>
            <dt>{isOpen ? RUN_COPY.elapsed : RUN_COPY.closed}</dt>
            <dd>
              {isOpen
                ? formatDuration(summary.startTime, null)
                : formatDate(summary.closeTime, timezone)}
            </dd>
          </div>
        </dl>
      )}
      {routing && <FlowRouting routing={routing} flowType={flowType} />}
      {summary && isOpen && (
        <button className="rhd-stop" onClick={() => setStopOpen(true)} type="button">
          {RUN_COPY.stop}
        </button>
      )}
      {summary && (
        <StopFlowDialog
          open={stopOpen}
          summary={summary}
          onClose={() => setStopOpen(false)}
          onStopped={() => {
            setStopOpen(false);
            onStopped();
          }}
        />
      )}
    </div>
  );
}
