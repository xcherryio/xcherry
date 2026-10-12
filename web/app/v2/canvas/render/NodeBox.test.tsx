// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import { renderToStaticMarkup } from 'react-dom/server'
import { describe, expect, it } from 'vitest'
import { NodeBox } from './NodeBox'

describe('NodeBox', () => {
  it("renders an app's monogram for a Connector Step and a who-acts glyph otherwise", () => {
    const connector = renderToStaticMarkup(
      <NodeBox
        box={{ id: 'connector', x: 0, y: 0, w: 256, h: 62, kind: 'step', title: 'Post', tile: { monogram: 'SL', tone: 'app' } }}
        selected={false}
      />,
    )
    const person = renderToStaticMarkup(
      <NodeBox
        box={{ id: 'review', x: 0, y: 0, w: 256, h: 62, kind: 'step', title: 'Review', tile: { glyph: 'person', tone: 'attention' } }}
        selected={false}
      />,
    )

    expect(connector).toContain('<span class="pcard-monogram">SL</span>')
    expect(person).toContain('class="pcard-tile" data-tone="attention"')
    expect(person).not.toContain('pcard-monogram')
  })

  it('docks a jump as a button and every other chip as a label', () => {
    const markup = renderToStaticMarkup(
      <NodeBox
        box={{
          id: 'step:a',
          x: 0,
          y: 0,
          w: 256,
          h: 92,
          kind: 'step',
          title: 'Check',
          chips: [[{ kind: 'jump', text: '↪ Retry', targetId: 'step:retry' }, { kind: 'ends', text: '✓ completes' }]],
        }}
        selected={false}
      />,
    )

    expect(markup).toContain('<button type="button" class="pdock nodrag nopan" data-chip="jump"')
    expect(markup).toContain('<span class="pdock" data-chip="ends">✓ completes</span>')
  })

  it('says "now" in its accessible name for the Step a run is at', () => {
    const markup = renderToStaticMarkup(
      <NodeBox box={{ id: 'step:a', x: 0, y: 0, w: 256, h: 62, kind: 'step', title: 'Approve' }} selected={false} now />,
    )

    expect(markup).toContain('aria-label="Approve, now"')
    expect(markup).toContain('data-now="true"')
  })

  it('renders an RPC card as a card and hands its role to the stylesheet', () => {
    const action = renderToStaticMarkup(
      <NodeBox
        box={{
          id: 'rpc:RemoveEntry',
          x: 0,
          y: 0,
          w: 280,
          h: 100,
          kind: 'rpcCard',
          title: 'RemoveEntry',
          subtitle: 'Action: Remove entry',
          rpcRole: 'action',
          rows: [
            { glyph: '◈', text: 'requires entries.manage', tone: 'quiet' },
            { glyph: '▪', text: 'writes entries', trail: '+1 more', tone: 'quiet' },
          ],
        }}
        selected={false}
      />,
    )

    expect(action).toContain('pbox pbox-rpcCard')
    expect(action).toContain('data-rpc-role="action"')
    expect(action).toContain('requires entries.manage')
    expect(action).toContain('<span class="pbox-text">writes entries</span><span class="pbox-trail">+1 more</span>')
    expect(action).not.toContain('pbox-inner')
  })
})
