// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import type { JSX } from 'react'

/** Card line icons on one 24-unit grid, drawn as paths so no platform substitutes an emoji. */
const PATHS = {
  plusCircle: (
    <>
      <circle cx="12" cy="12" r="8.5" />
      <path d="M12 8.5v7M8.5 12h7" />
    </>
  ),
  minusCircle: (
    <>
      <circle cx="12" cy="12" r="8.5" />
      <path d="M8.5 12h7" />
    </>
  ),
  plug: (
    <>
      <path d="M9 3.5V8M15 3.5V8M6.5 8h11v3a5.5 5.5 0 0 1-11 0z" />
      <path d="M12 16.5v4" />
    </>
  ),
  spark: (
    <path d="M12 3.5v4M12 16.5v4M3.5 12h4M16.5 12h4M6.2 6.2l2.6 2.6M15.2 15.2l2.6 2.6M6.2 17.8l2.6-2.6M15.2 8.8l2.6-2.6" />
  ),
  person: (
    <>
      <circle cx="12" cy="8" r="3.5" />
      <path d="M5 20c1-3.8 3.7-6 7-6s6 2.2 7 6" />
    </>
  ),
  door: (
    <>
      <path d="M14 4h4a1 1 0 0 1 1 1v14a1 1 0 0 1-1 1h-4" />
      <path d="M4 12h10M10 8l4 4-4 4" />
    </>
  ),
  cpu: (
    <>
      <rect x="6" y="6" width="12" height="12" rx="2" />
      <path d="M9.5 2.8v3M14.5 2.8v3M9.5 18.2v3M14.5 18.2v3M2.8 9.5h3M2.8 14.5h3M18.2 9.5h3M18.2 14.5h3" />
    </>
  ),
  clock: (
    <>
      <circle cx="12" cy="12" r="8.5" />
      <path d="M12 7.5V12l3 2" />
    </>
  ),
  wait: (
    <>
      <circle cx="12" cy="12" r="8.5" strokeDasharray="3 2.4" />
      <path d="M12 8v4.5" />
    </>
  ),
  bolt: <path d="M13 3 5 13.5h6L10 21l8-10.5h-6z" />,
  child: (
    <>
      <rect x="3.5" y="4" width="8" height="6" rx="1.2" />
      <rect x="12.5" y="14" width="8" height="6" rx="1.2" />
      <path d="M7.5 10v7h5" />
    </>
  ),
  refresh: (
    <>
      <path d="M19.5 11a7.5 7.5 0 1 0-2.2 5.3" />
      <path d="M19.5 4.5V11H13" />
    </>
  ),
}

export type GlyphName = keyof typeof PATHS

/** Decoration only: the control holding it carries the name. */
export function Glyph({ name, className }: { name: GlyphName; className?: string }): JSX.Element {
  return (
    <svg
      aria-hidden="true"
      focusable="false"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.6"
      strokeLinecap="round"
      strokeLinejoin="round"
      className={className === undefined ? 'pglyph' : `pglyph ${className}`}
    >
      {PATHS[name]}
    </svg>
  )
}
