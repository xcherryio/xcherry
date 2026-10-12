// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import type { Link } from '../views/types'

/**
 * The family -> arrowhead map, in a plain module.
 *
 * Split out of `ArrowDefs.tsx` so that file exports only a component: mixing constants into a
 * component module breaks React fast refresh, and these are imported by `Stage` and by tests that
 * have no business loading JSX.
 */

/**
 * Which marker each edge family terminates in.
 *
 * Exported and exhaustive over `Link['family']` so that adding a family without giving it an arrowhead
 * is a type error rather than a silently directionless edge — which is precisely the state every edge
 * on this canvas was in until now.
 */
export const EDGE_MARKER: Record<Link['family'], string> = {
  // Sequence flow: the flow proceeds. Hue comes from CSS, so these share one definition.
  control: 'p-arrow-solid',
  failure: 'p-arrow-solid',
}

/** The source-end marker, used only by message flow. BPMN's small hollow circle. */
export const RPC_TAIL = 'p-tail-hollow'

/** Every id this component actually defines, so a test can prove the map points at real markers. */
export const MARKER_IDS = ['p-arrow-solid', 'p-arrow-hollow', 'p-arrow-open', RPC_TAIL] as const
