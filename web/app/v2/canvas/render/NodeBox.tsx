// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import type { JSX, KeyboardEvent } from 'react'
import type { Box } from '../views/types'
import { ChipRows, RunPillView, StartPill, TileView } from './CardParts'
import { Glyph } from './Glyph'

/**
 * One Step or RPC card as a `div[role=button]`: its jump chips and outcome bar are buttons themselves.
 * Its measurements are `views/stepBox.ts`'s.
 */

type Select = (id: string, additive: boolean) => void

const isAdditive = (e: { metaKey: boolean; ctrlKey: boolean }): boolean => e.metaKey || e.ctrlKey

/** An RPC of a Flow without Steps, listing the state it reads and writes. */
function RpcCard({ box, selected, onSelect }: { box: Box; selected: boolean; onSelect?: Select }): JSX.Element {
  return (
    <div
      role="button"
      tabIndex={0}
      className="pbox pbox-rpcCard nodrag nopan"
      data-rpc-role={box.rpcRole}
      data-selected={selected ? 'true' : undefined}
      style={{ width: `${box.w}px`, height: `${box.h}px` }}
      onClick={(e) => onSelect?.(box.id, isAdditive(e))}
      onKeyDown={(e: KeyboardEvent<HTMLDivElement>) => {
        if (e.key !== 'Enter' && e.key !== ' ') return
        e.preventDefault()
        onSelect?.(box.id, isAdditive(e))
      }}
    >
      <span className="pbox-title">{box.title}</span>
      {box.subtitle ? <span className="pbox-sub">{box.subtitle}</span> : null}
      {box.rows && box.rows.length > 0 ? (
        <span className="pbox-rows">
          {box.rows.map((r, i) => (
            <span className="pbox-row" data-tone={r.tone} key={`${r.text}-${i}`}>
              {r.glyph ? <span className="pbox-glyph">{r.glyph}</span> : null}
              <span className="pbox-text">{r.text}</span>
              {r.trail ? <span className="pbox-trail">{r.trail}</span> : null}
            </span>
          ))}
        </span>
      ) : null}
    </div>
  )
}

/** The failure count beside a card's title: how many failures leave it, drawn when it is selected. */
export function FailureCount({ count }: { count: number }): JSX.Element {
  const text = `${count} failure route${count === 1 ? '' : 's'} leave${count === 1 ? 's' : ''} here. Select to see where ${count === 1 ? 'it goes' : 'they go'}.`
  return (
    <span className="pcard-failures" title={text} data-failures={count}>
      ⚠ {count}
    </span>
  )
}

/** In a failure detour: a failure that goes straight to a main-story Step drawn elsewhere. */
function Pointer({ box, onJump }: { box: Box; onJump?: (id: string) => void }): JSX.Element {
  return (
    <button
      type="button"
      className="pdetour-pointer nodrag nopan"
      data-kind="pointer"
      title={`On failure: goes to ${box.title.replace(/^↪ /, '')}. Click to show it.`}
      style={{ width: `${box.w}px`, height: `${box.h}px` }}
      onClick={() => {
        if (box.target !== undefined) onJump?.(box.target)
      }}
    >
      {box.title}
    </button>
  )
}

export function NodeBox({
  box,
  selected,
  hovered,
  referenced,
  now = false,
  onSelect,
  onHover,
  onInspect,
  onJump,
}: {
  box: Box
  selected: boolean
  hovered?: boolean
  referenced?: boolean
  /** The Step the shown run is at now: ringed and labelled. */
  now?: boolean
  onSelect?: Select
  onHover?: (id: string | null) => void
  onInspect?: (id: string) => void
  /** A jump chip was clicked: show where it leads. */
  onJump?: (id: string) => void
}): JSX.Element {
  if (box.kind === 'rpcCard') return <RpcCard box={box} selected={selected} onSelect={onSelect} />
  if (box.kind === 'pointer') return <Pointer box={box} onJump={onJump} />
  const activate = (e: { metaKey: boolean; ctrlKey: boolean }): void => onSelect?.(box.id, isAdditive(e))
  return (
    <div
      role="button"
      tabIndex={0}
      className="pcard nodrag nopan"
      data-kind={box.kind}
      data-step-type={box.identity}
      data-actor={box.actor}
      data-agent={box.agentRole}
      data-start={box.isStart ? 'true' : undefined}
      data-emphasis={box.emphasis}
      data-now={now ? 'true' : undefined}
      data-selected={selected ? 'true' : undefined}
      data-hover={hovered === true ? 'true' : undefined}
      data-ref={referenced === true ? 'true' : undefined}
      data-detour={box.detourOf !== undefined ? 'true' : undefined}
      aria-label={`${box.title}${box.subtitle ? `, ${box.subtitle}` : ''}${box.pill ? `, ${box.pill.text}` : ''}${now ? ', now' : ''}${box.failures ? `, ${box.failures} failure route${box.failures === 1 ? '' : 's'}` : ''}${box.detourOf !== undefined ? ', failure handling' : ''}`}
      title={box.identity}
      style={{ width: `${box.w}px`, height: `${box.h}px` }}
      onClick={activate}
      onKeyDown={(e: KeyboardEvent<HTMLDivElement>) => {
        if (e.key !== 'Enter' && e.key !== ' ') return
        e.preventDefault()
        activate(e)
      }}
      onPointerEnter={() => onHover?.(box.id)}
      onPointerLeave={() => onHover?.(null)}
      onFocus={() => onHover?.(box.id)}
      onBlur={() => onHover?.(null)}
    >
      {box.isStart ? <StartPill /> : null}
      {box.pill ? <RunPillView pill={box.pill} /> : null}
      <span className="pcard-head">
        {box.tile ? <TileView tile={box.tile} /> : null}
        <span className="pcard-words">
          <span className="pcard-title">{box.title}</span>
          {box.subtitle ? (
            <span className="pcard-sub" data-tone={box.subtitleTone}>
              {box.subtitle}
            </span>
          ) : null}
        </span>
        {/* BPMN's loop marker: the Step can run again on its own. */}
        {box.loops === true ? (
          <span className="pcard-mark" title="This step can run again — it transitions to itself">
            <Glyph name="refresh" />
          </span>
        ) : null}
        {box.failures ? <FailureCount count={box.failures} /> : null}
        {box.recovery ? (
          <span className="pcard-recovery" title={box.recovery.title}>
            {box.recovery.glyph}
          </span>
        ) : null}
      </span>

      <ChipRows rows={box.chips ?? []} onJump={(id) => onJump?.(id)} />

      {/* Each segment's width is its count, so one failure among many stays visible. */}
      {box.bar && box.bar.length > 0 ? (
        <button
          type="button"
          className="pbox-bar"
          title={box.barTitle}
          aria-label={box.barTitle ?? 'execution outcomes'}
          onClick={(e) => {
            e.stopPropagation()
            onSelect?.(box.id, false)
            onInspect?.(box.id)
          }}
        >
          {box.bar.map((seg) => (
            <span className="pbox-seg" data-status={seg.status} key={seg.status} style={{ flexGrow: seg.count }} />
          ))}
        </button>
      ) : null}

      {box.reason ? (
        <span className="pcard-reason" data-tone={box.reason.tone} title={box.reason.text}>
          {box.reason.text}
        </span>
      ) : null}
    </div>
  )
}
