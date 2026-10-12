// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import type { JSX, KeyboardEvent } from 'react'
import type { BoxChip, RunPill, Tile } from '../views/types'
import { Glyph } from './Glyph'

/** Parts a Step card and a stage card share; colour always repeats a glyph or a word. */

export function keyActivates(e: KeyboardEvent<HTMLElement>, act: () => void): void {
  if (e.key !== 'Enter' && e.key !== ' ') return
  e.preventDefault()
  act()
}

export function TileView({ tile, isSmall = false }: { tile: Tile; isSmall?: boolean }): JSX.Element {
  if (isSmall) {
    // A stage card's row of member tiles: the glyph alone, an app as a plug.
    return (
      <span className="pcard-minitile" data-tone={tile.tone}>
        <Glyph name={'glyph' in tile ? tile.glyph : 'plug'} />
      </span>
    )
  }
  return (
    <span className="pcard-tile" data-tone={tile.tone} aria-hidden="true">
      {'glyph' in tile ? <Glyph name={tile.glyph} /> : <span className="pcard-monogram">{tile.monogram}</span>}
    </span>
  )
}

/** Chip rows as packed; a jump chip is a button that shows where it leads. */
export function ChipRows({ rows, onJump }: { rows: BoxChip[][]; onJump: (id: string) => void }): JSX.Element | null {
  if (rows.length === 0) return null
  return (
    <span className="pcard-chips">
      {rows.map((row, i) => (
        <span className="pcard-chip-row" key={i}>
          {row.map((chip, j) =>
            chip.targetId !== undefined ? (
              <button
                type="button"
                key={j}
                className="pdock nodrag nopan"
                data-chip={chip.kind}
                title={`${chip.title ? `${chip.title}: ` : ''}goes to ${chip.text.replace(/^(↪ |↩ back to )/, '')}. Click to show it.`}
                onClick={(e) => {
                  e.stopPropagation()
                  onJump(chip.targetId as string)
                }}
              >
                {chip.text}
              </button>
            ) : (
              <span key={j} className="pdock" data-chip={chip.kind} title={chip.title}>
                {chip.text}
              </span>
            ),
          )}
        </span>
      ))}
    </span>
  )
}

/** Where the Flow begins, on the card's top edge like a trigger. */
export function StartPill(): JSX.Element {
  return (
    <span className="pcard-start" data-start-pill="true">
      <Glyph name="bolt" />
      Start
    </span>
  )
}

/** A run's state on the card's top edge, in words. */
export function RunPillView({ pill }: { pill: RunPill }): JSX.Element {
  return (
    <span className="pcard-pill" data-status={pill.status}>
      {pill.text}
    </span>
  )
}
