// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import { READABLE_ZOOM } from '../views/elkPlacement'

/**
 * Zoom is PURELY GEOMETRIC. It scales the picture and nothing else.
 *
 * There used to be a four-tier ladder derived from the zoom level, with hysteresis to stop it
 * strobing at a boundary. It is gone. Level of detail is now an explicit two-position control
 * (Collapsed and Expanded stages), which is what nine surveyed products do — Copilot Studio
 * lists `Expand/Collapse` as toolbar item 1 and zoom as items 2-3, LangGraph puts detail on a
 * slider decoupled from the canvas, and n8n ties nothing to zoom but a single threshold for group
 * descriptions.
 *
 * What remains here is the two tricks that make a zoomed-out graph readable WITHOUT a tier,
 * both borrowed from n8n. They are better than a tier for the same reason: a tier change reflows
 * the graph, and these do not.
 */

export const MIN_ZOOM = 0.14
export const MAX_ZOOM = 2.2

/**
 * Fit may zoom past 1:1, up to this. The cap used to be 1, which left a wide canvas mostly
 * empty whenever the graph already fitted; a small flow still must not fill a wall.
 */
export const FIT_MAX_ZOOM = 1.7

/** Focus on one Step zooms in properly — the point is to read it without reaching for zoom. */
export const FOCUS_ZOOM = 1.45

/** The canvas opens on the whole Flow only at a readable size; a larger one opens on its start. */
export const OPEN_FLOOR = READABLE_ZOOM

/**
 * Edge contrast RISES as you zoom out.
 *
 * At 0.2 an edge drawn for 1:1 is a grey suggestion, and the graph loses its skeleton exactly when
 * the skeleton is all you can use. Gamma-corrected (2.2) because lightness is perceptual, not
 * linear — a naive lerp overshoots in the middle.
 *
 * Returns a multiplier to apply to edge opacity, in [1, 1.9].
 */
export function edgeContrastBoost(zoom: number): number {
  if (!Number.isFinite(zoom)) return 1
  const t = Math.min(1, Math.max(0, (1 - zoom) / (1 - 0.2)))
  const eased = t ** (1 / 2.2)
  return 1 + eased * 0.9
}

/**
 * Chrome COUNTER-SCALES so it does not shrink at the same rate as the graph.
 *
 * Stroke widths and edge labels are multiplied by this, so at 0.3 zoom an edge is still visible as
 * a line rather than a hairline. Clamped so it never inflates above 1:1, where the graph is already
 * legible and thicker strokes would just look clumsy.
 */
export function chromeCompensation(zoom: number): number {
  if (!Number.isFinite(zoom) || zoom >= 1) return 1
  return Math.min(2.6, 1 / Math.max(zoom, 0.2))
}

const PADDING = { top: '8%', bottom: '4%', left: '4%', right: '4%' } as const

/** The right padding reserves the overlaid panel's share of the pane, so nothing draws under it. */
export function fitOptions(insetRightPx: number | null | undefined, paneWidth: number) {
  const right: `${number}%` =
    insetRightPx == null || insetRightPx <= 0 || paneWidth <= 0
      ? PADDING.right
      : `${Math.min(58, Math.max(18, (insetRightPx / paneWidth) * 100 + 4))}%`
  return { padding: { ...PADDING, right }, minZoom: OPEN_FLOOR, maxZoom: FIT_MAX_ZOOM } as const
}
