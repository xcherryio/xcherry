// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

/**
 * WHAT SHAPE a Step's outgoing branches have, when they have one worth saying out loud.
 *
 * Two shapes exist in Dex and the drawing has until now told them apart nowhere at all:
 *
 *   ALL OF THEM  `goToMany` starts several Steps together. One decision, several targets.
 *   ONE OF THEM  several `goTo` decisions, each with its own guard. First match wins.
 *
 * Measured across the five fixtures: of the 14 steps with more than one possible next step, 13 take
 * ONE path and 1 takes ALL of them. So the fan is the EXCEPTION and is what gets marked; a step that
 * takes one path already declares itself by having a guard on every branch.
 *
 * The specs agree on where that distinction belongs, and it is not colour. UML 2.5.1 draws a fork as
 * "a short heavy bar" and rules that "the segments outgoing from a fork vertex must not have guards
 * or triggers"; a choice is a diamond with a guard on each outgoing edge. BPMN 2.0 §10.5.2 makes the
 * exclusive gateway's X marker OPTIONAL — "identical in meaning" either way — so the meaning cannot
 * have been living in the marker. Both put it on the NODE. Our data satisfies UML's rule for free:
 * the one fan-out has no guard, and every choice has exactly one guard per branch.
 *
 * THE SWITCH CASE. UML 2.5.1 §14.2.4.6 also gives the shorthand for our two big fans: when the
 * outgoing guards share a common left operand, "The left operand MAY be placed inside the
 * diamond-shaped symbol and the rest of the Guard expressions placed on the outgoing Transitions."
 * `book-volume`'s RecoveryGate is twelve branches of `target == <NAME>` and `ai-agent`'s CheckSteered
 * is seven of `input == <name>`; both satisfy the precondition exactly.
 *
 * WE HOIST BUT DO NOT PUT THE REMAINDER ON THE EDGES, which is where we part from the figure, and the
 * reason is our data. In both switches the case value NAMES ITS DESTINATION — `INIT` selects
 * `InitStep`, `continueRouteTool` selects `RouteTool` — so an edge labelled with it restates the card
 * it points at, and the line runs through the text besides. Naming the discriminant on the source card
 * is the half that carries information; the values stay in the panel with the guards.
 *
 * DETECTED BY SHAPE, NEVER BY COUNT. No surveyed product documents a branch count at which its
 * rendering changes — Step Functions, XState, n8n, Node-RED, Camunda and Graphviz all have none, and
 * the only numbers anywhere are capacity caps (Logic Apps 25 cases, Zapier 10 per group). So the
 * trigger is "do these guards all test the same name", which is a property of the flow rather than a
 * threshold somebody picked. It fires on exactly 2 of our 14 branching steps, and would improve a
 * two-branch switch just as much as a twelve-branch one.
 *
 * WHY THE RIGHT-HAND SIDE IS RESTRICTED to a single bare token, in `caseGuard` below: the shape test
 * has to reject expressions outright. `len(executions) == 0` looks like a case and is a call, and a
 * detector that accepted it would hoist source syntax onto a card — the exact leak the card's
 * no-expressions guard exists to catch.
 */

import { simplifyGuard } from './decode'
import type { Branch, StepModel } from './pocFlow'

export interface CaseGuard {
  /** The name every conforming branch tests. Hoisted onto the card. */
  key: string
  /**
   * The one name it is compared against.
   *
   * Not drawn anywhere today — see the header on why the edges do not carry it. It is kept because it
   * is what makes a branch's conformance checkable: `branchCase` has to prove every merged guard tests
   * the same name, and it cannot do that without reading the values apart from the key.
   */
  value: string
}

/**
 * A dotted identifier and nothing else. No spaces, no parens, no operators — so `len(executions)`
 * is rejected as the discriminant it superficially resembles, and any guard with a second operator
 * in it fails here rather than needing a separate check.
 */
const KEY = /^[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*$/

/**
 * One token: an identifier, a number, or a quoted name.
 *
 * Quoted forms are ACCEPTED and keep their quotes. `current.preflight == 'pass'` is an enum member
 * spelled as a string because the source language had no enum, and `'pass'` is as much a name as
 * `INIT` is. Stripping the quotes would be paraphrasing an identifier, which this codebase does not
 * do anywhere else either.
 */
const VALUE = /^(?:[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*|-?\d+(?:\.\d+)?|'[^']*'|"[^"]*")$/

/** `target == INIT` -> `{ key: 'target', value: 'INIT' }`. Null for anything else. */
export function caseGuard(guard: string | null | undefined): CaseGuard | null {
  if (guard === null || guard === undefined) return null
  const parts = guard.split(' == ')
  if (parts.length !== 2) return null
  const key = (parts[0] ?? '').trim()
  const value = (parts[1] ?? '').trim()
  if (!KEY.test(key) || !VALUE.test(value)) return null
  return { key, value }
}

/**
 * The case this branch is, or null when it is not one.
 *
 * Reads `fullGuards` rather than `guard`, because `guard` is already lossy for a grouped branch:
 * `groupBranches` replaces it with the string `"N conditions"` when several decisions reached an
 * identical outcome by different routes. A grouped branch conforms only when EVERY guard that
 * merged into it is a case on the same name, which is the honest test — RecoveryGate's self-loop
 * merges three unrelated conditions and rightly fails it.
 */
export function branchCase(branch: Branch): { key: string; values: string[] } | null {
  if (branch.fullGuards.length === 0) return null
  const cases = branch.fullGuards.map((g) => caseGuard(simplifyGuard(g)))
  const first = cases[0]
  if (first === null || first === undefined) return null
  if (cases.some((c) => c === null || c.key !== first.key)) return null
  return { key: first.key, values: cases.map((c) => (c as CaseGuard).value) }
}

/**
 * The one name this Step's outgoing branches route on, or null.
 *
 * A STRICT MAJORITY of the moving branches must agree, and at least two must. Both floors matter and
 * neither is arbitrary:
 *
 *  - Two, because one `X == V` beside one `X != V` is a yes/no test, not a switch, and hoisting a
 *    name over a pair of branches buys nothing. That is what keeps 11 of our 13 choosing steps
 *    untouched.
 *  - A majority, because the card names the discriminant and the edges answer it. RecoveryGate is
 *    11 of 12 and CheckSteered 5 of 7, so a non-conforming branch is a real case and the copy has to
 *    survive it — which is why the card says the path DEPENDS ON the name rather than claiming the
 *    name decides every path. The odd branch keeps whatever label it already had, and nothing on
 *    screen makes a claim about it.
 */
export function discriminantOf(step: StepModel): string | null {
  const moving = step.execute.branches.filter((b) => b.targets.length > 0)
  if (moving.length < 2) return null
  const tally = new Map<string, number>()
  for (const b of moving) {
    const c = branchCase(b)
    if (c === null) continue
    tally.set(c.key, (tally.get(c.key) ?? 0) + 1)
  }
  let best: string | null = null
  let bestCount = 0
  for (const [key, count] of tally) {
    if (count > bestCount) {
      best = key
      bestCount = count
    }
  }
  if (best === null || bestCount < 2) return null
  return bestCount * 2 > moving.length ? best : null
}

/**
 * How many Steps one decision starts AT ONCE, or 0 when nothing fans out.
 *
 * Read off the structure — a single decision with several targets — rather than off the decision
 * type. `goToMany` is the Dex verb that produces it, but the fact that matters is "these all
 * proceed", and that is visible in the targets whatever the verb is called.
 *
 * Distinct target ids, so multiplicity on one target is not miscounted as a fan.
 */
export function fanOutOf(step: StepModel): number {
  let most = 0
  for (const b of step.execute.branches) {
    most = Math.max(most, new Set(b.targets.map((t) => t.stepId)).size)
  }
  return most > 1 ? most : 0
}

/** The case values sending this Step to `targetStepId`, as a line label, when every such branch is a case. */
export function caseLabel(step: StepModel, targetStepId: string): string | null {
  const key = discriminantOf(step)
  if (key === null) return null
  const values: string[] = []
  for (const branch of step.execute.branches) {
    if (!branch.targets.some((t) => t.stepId === targetStepId)) continue
    const c = branchCase(branch)
    if (c === null || c.key !== key) return null
    values.push(...c.values.map((v) => v.replace(/^["']|["']$/g, '')))
  }
  return values.length > 0 ? [...new Set(values)].join(' / ') : null
}
