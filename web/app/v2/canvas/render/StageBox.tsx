// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import type { JSX } from 'react'
import type { Box } from '../views/types'
import { ChipRows, keyActivates, RunPillView, StartPill, TileView } from './CardParts'
import { Glyph } from './Glyph'
import { FailureCount } from './NodeBox'

/** A closed stage's card; activating it opens the stage in place. Measurements are `views/stageCard.ts`'s. */
export function StageBox({
  box,
  now,
  onOpen,
  onJump,
}: {
  box: Box
  /** The stage holds the Step the run is at now. */
  now: boolean
  onOpen: (id: string) => void
  onJump: (id: string) => void
}): JSX.Element | null {
  const stage = box.stage
  if (stage === undefined) return null
  const open = (): void => onOpen(box.id)
  return (
    <div
      role="button"
      tabIndex={0}
      className="pstage-card nodrag nopan"
      data-kind="stage"
      data-actor={box.actor}
      data-now={now ? 'true' : undefined}
      aria-label={`${box.title}, ${stage.stepCount} steps${box.pill ? `, ${box.pill.text}` : ''}. Open the stage.`}
      title={`${box.title}: ⊕ opens it to show its ${stage.stepCount} steps`}
      style={{ width: `${box.w}px`, height: `${box.h}px` }}
      onClick={open}
      onKeyDown={(e) => keyActivates(e, open)}
    >
      {box.isStart ? <StartPill /> : null}
      {box.pill ? <RunPillView pill={box.pill} /> : null}
      <span className="pstage-head">
        <b className="pstage-title">{box.title}</b>
        {stage.loops ? (
          <span className="pcard-mark" title="Some steps here repeat">
            <Glyph name="refresh" />
          </span>
        ) : null}
        {box.failures ? <FailureCount count={box.failures} /> : null}
        <span className="pstage-count">
          {stage.stepCount} step{stage.stepCount === 1 ? '' : 's'}
        </span>
        <span className="pstage-open" aria-hidden="true">
          <Glyph name="plusCircle" />
        </span>
      </span>
      <span className="pstage-tiles" aria-hidden="true">
        {stage.tiles.map((tile, i) => (
          <TileView key={i} tile={tile} isSmall />
        ))}
      </span>
      <span className="pstage-facts">{stage.facts}</span>
      <ChipRows rows={box.chips ?? []} onJump={onJump} />
    </div>
  )
}
