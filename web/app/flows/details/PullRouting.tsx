// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import type { FlowHistoryEvent, FlowState } from '@/lib/types';
import { asData, Fields } from './StepMethodSections';

export interface FlowRoutingData {
  workerTarget: Record<string, unknown>;
  stickyAppWorkerId?: string;
}

export function flowRoutingData(state: FlowState | null, events: FlowHistoryEvent[]): FlowRoutingData {
  const start = events.find((event) => event.type === 'FlowStartedOrContinued');
  const config = asData(state?.flowConfig ?? start?.payload.flowConfig);
  const closed = events.findLast((event) => event.type === 'FlowClosed');
  const lastServed = events.findLast((event) =>
    event.type.endsWith('Completed') && event.type.startsWith('Step')
    && typeof asData(asData(event.payload.context).pullRouting).servedByAppWorkerId === 'string');
  const stickyWorkerID = state?.stickyAppWorkerId
    ?? closed?.payload.stickyAppWorkerId
    ?? asData(asData(lastServed?.payload.context).pullRouting).servedByAppWorkerId
    ?? asData(start?.payload.continuedStart).stickyAppWorkerId;
  return {
    workerTarget: asData(config.workerTarget),
    stickyAppWorkerId: typeof stickyWorkerID === 'string' ? stickyWorkerID : undefined,
  };
}

export function FlowRouting({ routing, flowType }: { routing: FlowRoutingData; flowType: string }) {
  const target = routing.workerTarget;
  const isPull = target.isPullMode === true;
  return <Fields compact values={[
    ['Worker target', isPull ? 'Pull' : target.address ? `${target.address}${target.isHeadlessAddress === true ? ' (headless)' : ''}` : '—'],
    ...(isPull ? [
      ['Flow type queue', flowType],
      ['Sticky app Worker ID', routing.stickyAppWorkerId || '—'],
    ] as [string, unknown][] : []),
  ]} />;
}

export { PullRouting } from './StepMethodSections';
