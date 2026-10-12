// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import type { PocFlow } from '../model/pocFlow'
import type { StepGroup } from './groups'

/** Only a run or a choice opens as one stage; the analyzer's group-shape warning applies the same test. */

/** Returns the group's Steps split into the parts its inner transitions join, in group order. */
export function groupParts(flow: PocFlow, group: StepGroup): string[][] {
  const known = new Set(flow.steps.map((step) => step.id))
  const members = group.stepIds.filter((id) => known.has(id))
  const root = new Map(members.map((id) => [id, id]))
  const find = (id: string): string => {
    let at = id
    while (root.get(at) !== at) at = root.get(at) as string
    return at
  }
  for (const t of flow.transitions) {
    if (t.isSelfLoop || !root.has(t.fromStepId) || !root.has(t.toStepId)) continue
    root.set(find(t.fromStepId), find(t.toStepId))
  }
  const parts = new Map<string, string[]>()
  for (const id of members) parts.set(find(id), [...(parts.get(find(id)) ?? []), id])
  return [...parts.values()]
}

/** Whether one Step outside the group leads into every part. */
function isOneChoice(flow: PocFlow, parts: string[][]): boolean {
  const members = new Set(parts.flat())
  const sourcesOf = parts.map((part) => {
    const inPart = new Set(part)
    return new Set(
      flow.transitions
        .filter((t) => inPart.has(t.toStepId) && !members.has(t.fromStepId))
        .map((t) => t.fromStepId),
    )
  })
  const [first, ...rest] = sourcesOf
  return [...(first ?? [])].some((source) => rest.every((sources) => sources.has(source)))
}

/** Whether a group is drawn as a stage: two or more Steps forming one run or one choice. */
export function isStageGroup(flow: PocFlow, group: StepGroup): boolean {
  const parts = groupParts(flow, group)
  const steps = parts.reduce((count, part) => count + part.length, 0)
  return steps >= 2 && (parts.length === 1 || isOneChoice(flow, parts))
}
