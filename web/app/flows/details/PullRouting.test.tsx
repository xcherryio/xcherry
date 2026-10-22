// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import { renderToStaticMarkup } from 'react-dom/server';
import { describe, expect, it } from 'vitest';
import { FlowRouting, PullRouting, flowRoutingData } from './PullRouting';

describe('pull routing', () => {
  it('shows sticky and fallback routing with the dispatch sticky ID', () => {
    const sticky = renderToStaticMarkup(<PullRouting value={{ servedByAppWorkerId: 'worker-a', stickyAppWorkerId: 'worker-a', source: 1 }} />);
    expect(sticky).toContain('Sticky queue');
    expect(sticky).toContain('worker-a');
    const fallback = renderToStaticMarkup(<PullRouting value={{ servedByAppWorkerId: 'worker-b', stickyAppWorkerId: 'worker-a', source: 2 }} />);
    expect(fallback).toContain('Flow type queue');
    expect(fallback).toContain('worker-b');
    expect(fallback).toContain('worker-a');
  });
  it('shows blanks for old routing and only an address for push targets', () => {
    const missing = renderToStaticMarkup(<PullRouting value={undefined} />);
    expect(missing.match(/—/g)).toHaveLength(3);
    const push = renderToStaticMarkup(<FlowRouting flowType="PaymentFlow" routing={{ workerTarget: { address: 'worker:8803', isHeadlessAddress: true } }} />);
    expect(push).toContain('worker:8803 (headless)');
    expect(push).not.toContain('Flow type queue');
    expect(push).not.toContain('Sticky app Worker ID');
  });
  it('shows the Flow type queue and restores closed-run sticky data from Step history', () => {
    const routing = flowRoutingData(null, [{ eventId: 1, eventTime: null, type: 'FlowStartedOrContinued', payload: { flowConfig: { workerTarget: { isPullMode: true } } } }, { eventId: 2, eventTime: null, type: 'StepExecuteCompleted', payload: { context: { pullRouting: { servedByAppWorkerId: 'last-worker' } } } }]);
    const markup = renderToStaticMarkup(<FlowRouting flowType="PaymentFlow" routing={routing} />);
    expect(markup).toContain('PaymentFlow');
    expect(markup).toContain('last-worker');
    expect(markup).toContain('Pull');
  });
  it('uses closed-run sticky state after an RPC and ignores failed method Workers', () => {
    const routing = flowRoutingData(null, [
      { eventId: 1, eventTime: null, type: 'FlowStartedOrContinued', payload: { flowConfig: { workerTarget: { isPullMode: true } } } },
      { eventId: 2, eventTime: null, type: 'StepExecuteFailed', payload: { context: { pullRouting: { servedByAppWorkerId: 'failed-worker' } } } },
      { eventId: 3, eventTime: null, type: 'FlowClosed', payload: { stickyAppWorkerId: 'rpc-worker' } },
    ]);
    expect(routing.stickyAppWorkerId).toBe('rpc-worker');
  });

});
