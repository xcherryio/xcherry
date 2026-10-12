// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

/**
 * FDG v1 -> PocFlow. The only file that touches the wire.
 *
 * THE DERIVATION REGISTER. Everything this file computes rather than reads is listed
 * here, and each obeys one rule: a derivation is legal only if it is a pure function of
 * the wire. Anything needing outside knowledge would be a backend field, not a heuristic.
 *
 *  1. Step->Step topology   remap `transition.from` through `decision.parentId`.
 *                           ALL transitions start at a decision; ZERO start at a step.
 *  2. `actor`               from wait-condition kinds, which completely partition what
 *                           can block a Step. channel -> external, subflow -> child,
 *                           timer-only -> clock, no wait -> machine.
 *  3. `waitFor === null`    from `phase`. A Step with no WaitFor renders the lane ABSENT.
 *  4. wait sentence         paraphrase framework words (anyOf, .for 1, skipWaitImmediately);
 *                           keep Channel and Timer identifiers verbatim.
 *  5. guard label           last ` and ` clause, double negation undone.
 *  6. branch grouping       decisions keyed on {type, targets, checkedChannels, cancels}.
 *  7. `isHub`               inbound control degree. A hub dominates a naive drawing.
 *  8. `opensGates`          join rpc-publish->channel with channel-condition->wait.
 *  9. transition merging    parallel edges collapse, keeping every guard.
 *                           Kept in source order for the main line; the analyzer
 *                           lists edges by node id instead.
 * 10. `action`, `views`     join an rpc node's name with the Flow v2 Action and view RPC names.
 *
 * Tolerant by construction: never throws, and an unknown node kind survives as a
 * labelled box rather than disappearing. A silently dropped node is a lie about the flow.
 */

import type { FdgEdge, FdgNode, FlowDefinitionGraph } from './fdg'
import type {
  Actor,
  Branch,
  Condition,
  EntryModel,
  EntryView,
  ExecutePhase,
  PocFlow,
  ResourceModel,
  ResourceRef,
  StepModel,
  SubFlowRef,
  Transition,
  WaitPhase,
  RecoveryRole,
} from './pocFlow'

const CONTROL_KINDS = new Set(['transition', 'failure_transition'])
const RESOURCE_KINDS = new Set(['attribute', 'channel', 'stream'])
const OWNER_KINDS = new Set(['step', 'rpc', 'timeout_handler'])

/** Maps a decision type onto how it closes, or undefined when it moves instead. */
const CLOSES: Record<string, Branch['closes']> = {
  gracefulComplete: 'complete',
  forceComplete: 'complete',
  forceFail: 'fail',
  deadEnd: 'deadEnd',
  forceCompleteIfChannelsEmpty: 'completeIfChannelsEmpty',
}

// ---------------------------------------------------------------------------
// guards
// ---------------------------------------------------------------------------

const FLIPS: [string, string][] = [
  ['!=', '=='],
  ['==', '!='],
  ['>=', '<'],
  ['<=', '>'],
  ['>', '<='],
  ['<', '>='],
]

function stripOuterParens(s: string): string {
  let out = s.trim()
  while (out.startsWith('(') && out.endsWith(')')) {
    // Only strip when the leading paren actually closes at the end, so
    // `(a) and (b)` is left alone.
    let depth = 0
    let matches = true
    for (let i = 0; i < out.length; i += 1) {
      if (out[i] === '(') depth += 1
      else if (out[i] === ')') {
        depth -= 1
        if (depth === 0 && i !== out.length - 1) {
          matches = false
          break
        }
      }
    }
    if (!matches) break
    out = out.slice(1, -1).trim()
  }
  return out
}

/**
 * A guard arrives as every preceding branch negated and ANDed with this one, so an
 * `else` clause reads as `not (not records)`. Only the last clause is this branch's
 * own, and a doubled negation is worth undoing before showing it to anyone.
 */
export function simplifyGuard(raw: string | undefined): string | null {
  if (raw === undefined) return null
  const trimmed = raw.trim()
  if (trimmed === '' || trimmed === 'otherwise') return trimmed === '' ? null : 'otherwise'

  // Split on top-level ` and ` only; a nested `and` inside parens is part of one clause.
  const parts: string[] = []
  let depth = 0
  let start = 0
  for (let i = 0; i < trimmed.length; i += 1) {
    const ch = trimmed[i]
    if (ch === '(') depth += 1
    else if (ch === ')') depth -= 1
    else if (depth === 0 && trimmed.startsWith(' and ', i)) {
      parts.push(trimmed.slice(start, i))
      start = i + 5
      i += 4
    }
  }
  parts.push(trimmed.slice(start))

  let clause = stripOuterParens(parts[parts.length - 1] ?? trimmed)

  // `not not x` -> `x`, repeatedly.
  let changed = true
  while (changed) {
    changed = false
    const inner = /^not\s+(.*)$/.exec(clause)
    if (inner) {
      const body = stripOuterParens(inner[1])
      const nested = /^not\s+(.*)$/.exec(body)
      if (nested) {
        clause = stripOuterParens(nested[1])
        changed = true
        continue
      }
      // `not (a != b)` -> `a == b`, for one top-level comparison only.
      for (const [from, to] of FLIPS) {
        const at = topLevelIndexOf(body, ` ${from} `)
        if (at !== -1) {
          clause = `${body.slice(0, at)} ${to} ${body.slice(at + from.length + 2)}`
          changed = true
          break
        }
      }
    }
  }
  return clause === '' ? null : clause
}

function topLevelIndexOf(s: string, needle: string): number {
  let depth = 0
  for (let i = 0; i < s.length; i += 1) {
    if (s[i] === '(') depth += 1
    else if (s[i] === ')') depth -= 1
    else if (depth === 0 && s.startsWith(needle, i)) return i
  }
  return -1
}

// ---------------------------------------------------------------------------
// wait prose
// ---------------------------------------------------------------------------

/**
 * `SteeredUserMessages.for 1…MAX` -> `SteeredUserMessages`. Framework suffix, not an identifier.
 *
 * Also drops the RECEIVER CHAIN. The Python analyser records the expression it found, so a SubFlow held
 * as an attribute of the Flow arrives as `self.curate` and a timer as `self.config.gate_reminder timer`.
 * That is an access path, not a name, and putting it on a card is source syntax on the canvas — caught
 * by the no-expressions guard when the real fanyi pipeline became a fixture.
 *
 * Only the receiver is dropped. The final identifier is the developer's own name for the thing and stays
 * verbatim, because it is the handle somebody has to act on.
 */
function cleanConditionLabel(label: string): string {
  const trimmed = label.replace(/\.for\s.*$/, '').trim()
  // Keep any trailing framework word (`timer`) while stripping the chain in front of the identifier.
  return trimmed.replace(/\b(?:self|cls)(?:\.[A-Za-z_][\w]*)*\.([A-Za-z_][\w]*)/g, '$1')
}

/**
 * One prose line for a WaitFor.
 *
 * The line between paraphrase and verbatim: framework vocabulary goes (`anyOf`,
 * `skipWaitImmediately`, `.for 1`), operator vocabulary stays (Channel and Timer names
 * are the handles somebody has to publish to, and inventing prose for them would
 * misrepresent what has to happen).
 */
export function waitSentence(type: string, conditions: Condition[]): string {
  const channels = conditions.filter((c) => c.kind === 'channel').map((c) => c.label)
  const timers = conditions.filter((c) => c.kind === 'timer').map((c) => c.label)
  const subflows = conditions.filter((c) => c.kind === 'subflow').map((c) => c.label)

  if (type === 'skipWaitImmediately') return 'does not wait'

  const clauses: string[] = []
  if (channels.length > 0) {
    clauses.push(`waits for a message on ${channels.join(' or ')}`)
  }
  if (subflows.length > 0) {
    const every = type === 'allOf' ? 'every' : 'any'
    clauses.push(`waits for ${every} of ${subflows.join(', ')}`)
  }
  if (timers.length > 0) {
    clauses.push(
      channels.length > 0 || subflows.length > 0
        ? `or ${timers.join(' or ')} passes`
        : `waits ${timers.join(' or ')}`,
    )
  }
  // A declared wait with no resolved conditions. Say that, rather than "waits", which
  // would read as a complete answer.
  if (clauses.length === 0) return 'waits — conditions not resolved by the analyser'
  return clauses.join(' ')
}

// ---------------------------------------------------------------------------
// projection
// ---------------------------------------------------------------------------

function branchKey(b: Branch): string {
  return JSON.stringify([
    [...b.decisionTypes].sort(),
    b.targets.map((t) => `${t.stepId}${t.multiplicity ?? ''}`).sort(),
    (b.checkedChannels ?? []).slice().sort(),
    (b.cancels ?? []).map((c) => `${c.stepId}:${c.scope}`).sort(),
    b.closes ?? '',
  ])
}

function groupBranches(raw: Branch[]): Branch[] {
  const byKey = new Map<string, Branch>()
  for (const b of raw) {
    const key = branchKey(b)
    const seen = byKey.get(key)
    if (seen === undefined) {
      byKey.set(key, { ...b, fullGuards: [...b.fullGuards] })
      continue
    }
    seen.fullGuards.push(...b.fullGuards)
    // Multiple guards reaching an identical outcome: the count is the honest label.
    if (seen.guard !== b.guard) seen.guard = `${seen.fullGuards.length} conditions`
  }
  return [...byKey.values()]
}

export function decodeFlow(
  graph: FlowDefinitionGraph,
  provenance: PocFlow['provenance'],
): PocFlow {
  const dropped: string[] = []
  const byId = new Map<string, FdgNode>()
  for (const n of graph.nodes) byId.set(n.id, n)

  /** Walk `parentId` up to the nearest step / rpc / timeout_handler. Cycle-safe. */
  const ownerOf = (id: string): FdgNode | undefined => {
    let cur = byId.get(id)
    let hops = 0
    while (cur !== undefined && hops < 8) {
      if (OWNER_KINDS.has(cur.kind)) return cur
      if (cur.parentId === undefined) return undefined
      cur = byId.get(cur.parentId)
      hops += 1
    }
    return undefined
  }

  // ---- resources -----------------------------------------------------------
  const resources: ResourceModel[] = graph.nodes
    .filter((n) => RESOURCE_KINDS.has(n.kind))
    .map((n) => ({
      id: n.id,
      kind: n.kind as ResourceModel['kind'],
      name: n.name,
      valueType: n.resource?.valueType ?? 'unknown',
      isMap: n.resource?.map === true,
      touchedBy: [],
    }))
  const resourceIndex = new Map(resources.map((r) => [r.id, r]))

  // ---- resource edges fold into their owner -------------------------------
  const refsByOwner = new Map<string, ResourceRef[]>()
  const ACCESS: Record<string, ResourceRef['access']> = {
    resource_read: 'read',
    resource_write: 'write',
    resource_publish: 'publish',
    resource_lock: 'lock',
  }
  for (const e of graph.edges) {
    const access = ACCESS[e.kind]
    if (access === undefined) continue
    // `resource_read` points resource -> consumer; the writes point actor -> resource.
    const resourceEnd = access === 'read' ? e.from : e.to
    const actorEnd = access === 'read' ? e.to : e.from
    const resource = resourceIndex.get(resourceEnd)
    const owner = ownerOf(actorEnd)
    if (resource === undefined || owner === undefined) {
      dropped.push(`${e.kind} ${e.id}`)
      continue
    }
    const ref: ResourceRef = {
      resourceId: resource.id,
      access,
      label: e.label ?? access,
      phase: typeof e.metadata?.phase === 'string' ? e.metadata.phase : undefined,
    }
    const list = refsByOwner.get(owner.id) ?? []
    list.push(ref)
    refsByOwner.set(owner.id, list)
    resource.touchedBy.push({ ...ref, resourceId: owner.id })
  }

  // ---- waits ---------------------------------------------------------------
  const waitByStep = new Map<string, WaitPhase>()
  const waitNodeToStep = new Map<string, string>()
  for (const n of graph.nodes) {
    if (n.kind !== 'wait' || n.wait === undefined) continue
    const owner = ownerOf(n.parentId ?? n.id)
    if (owner === undefined) {
      dropped.push(`wait ${n.id}`)
      continue
    }
    waitNodeToStep.set(n.id, owner.id)
    const conditions: Condition[] = n.wait.conditions.map((c) => ({
      kind: c.kind,
      label: cleanConditionLabel(c.label),
      resourceId: c.resourceId,
      subFlowId: c.subFlowId,
      expression: c.expression,
    }))
    const existing = waitByStep.get(owner.id)
    // A Step may declare up to 2 waits; merge them into one phase for reading.
    if (existing === undefined) {
      waitByStep.set(owner.id, {
        type: n.wait.type,
        conditions,
        sentence: waitSentence(n.wait.type, conditions),
      })
    } else {
      const merged = [...existing.conditions, ...conditions]
      waitByStep.set(owner.id, {
        type: existing.type,
        conditions: merged,
        sentence: waitSentence(existing.type, merged),
      })
    }
  }

  // ---- decisions -> branches, grouped by owner -----------------------------
  const targetsOf = (decisionId: string): Branch['targets'] =>
    graph.edges
      .filter((e) => e.kind === 'transition' && e.from === decisionId)
      .map((e) => ({ stepId: e.to, multiplicity: e.multiplicity }))

  const branchesByOwner = new Map<string, Branch[]>()
  for (const n of graph.nodes) {
    if (n.kind !== 'decision' || n.decision === undefined) continue
    const owner = ownerOf(n.parentId ?? n.id)
    if (owner === undefined) {
      dropped.push(`decision ${n.id}`)
      continue
    }
    const guard = simplifyGuard(n.condition)
    const branch: Branch = {
      decisionTypes: [n.decision.type],
      guard,
      fullGuards: n.condition === undefined ? [] : [n.condition],
      targets: targetsOf(n.id),
      closes: CLOSES[n.decision.type],
      cancels: n.decision.cancellations,
      checkedChannels: n.decision.checkedChannels,
    }
    const list = branchesByOwner.get(owner.id) ?? []
    list.push(branch)
    branchesByOwner.set(owner.id, list)
  }

  // ---- control edges, collapsed to owner -> step ---------------------------
  const rawTransitions: Transition[] = []
  for (const e of graph.edges.filter((edge) => CONTROL_KINDS.has(edge.kind)).sort(bySourcePosition)) {
    const fromOwner = ownerOf(e.from)
    const toNode = byId.get(e.to)
    if (fromOwner === undefined || toNode === undefined) {
      dropped.push(`${e.kind} ${e.id}`)
      continue
    }
    const decision = byId.get(e.from)
    rawTransitions.push({
      id: e.id,
      fromStepId: fromOwner.id,
      toStepId: toNode.id,
      kind: e.kind as Transition['kind'],
      guard: simplifyGuard(decision?.condition),
      mergedGuards: decision?.condition === undefined ? [] : [decision.condition],
      multiplicity: e.multiplicity,
      isSelfLoop: fromOwner.id === toNode.id,
    })
  }

  // Merge parallel edges: several branches converging on one target read as one arrow.
  const transitions: Transition[] = []
  const seenPair = new Map<string, Transition>()
  for (const t of rawTransitions) {
    const key = `${t.fromStepId}->${t.toStepId}:${t.kind}`
    const prior = seenPair.get(key)
    if (prior === undefined) {
      const copy = { ...t, mergedGuards: [...t.mergedGuards] }
      seenPair.set(key, copy)
      transitions.push(copy)
      continue
    }
    prior.mergedGuards.push(...t.mergedGuards)
    prior.guard = `${prior.mergedGuards.length} conditions`
  }

  // Control and failure in-degree are counted separately, because they mean different
  // things. Many transitions arriving is a legibility problem; many failure edges
  // arriving is an ordinary compensation target.
  const inbound = new Map<string, number>()
  const inboundFailure = new Map<string, number>()
  /** Ordinary outbound, for telling a recovery SINK from a recovery DISPATCHER. */
  const outboundControl = new Map<string, number>()
  for (const t of transitions) {
    if (t.isSelfLoop) continue
    const bucket = t.kind === 'failure_transition' ? inboundFailure : inbound
    bucket.set(t.toStepId, (bucket.get(t.toStepId) ?? 0) + 1)
    if (t.kind !== 'failure_transition') {
      outboundControl.set(t.fromStepId, (outboundControl.get(t.fromStepId) ?? 0) + 1)
    }
  }

  // ---- steps ---------------------------------------------------------------
  const actorOf = (wait: WaitPhase | null): Actor => {
    if (wait === null) return 'machine'
    const kinds = new Set(wait.conditions.map((c) => c.kind))
    if (kinds.has('channel')) return 'external'
    if (kinds.has('subflow')) return 'child'
    if (kinds.has('timer')) return 'clock'
    // Declares a wait, but the analyser resolved no conditions. Do not round this down
    // to 'machine' — that is how a human gate becomes invisible.
    return 'unknown'
  }

  const stepCount = graph.nodes.filter((n) => n.kind === 'step').length
  const steps: StepModel[] = graph.nodes
    .filter((n) => n.kind === 'step')
    .map((n) => {
      // `phase` is a perfect proxy: every 'wait_for+execute' step has a wait child,
      // every 'execute' step has none. Fall back to the wait map when phase is absent.
      const declaresWait = n.phase?.includes('wait_for') ?? waitByStep.has(n.id)
      const waitFor = declaresWait ? (waitByStep.get(n.id) ?? null) : null
      const execute: ExecutePhase = { branches: groupBranches(branchesByOwner.get(n.id) ?? []) }
      const count = inbound.get(n.id) ?? 0
      const failures = inboundFailure.get(n.id) ?? 0
      return {
        id: n.id,
        stepType: n.name,
        label: n.name,
        isStart: n.start === true || graph.flow.startStepId === n.id,
        actor: actorOf(waitFor),
        /**
         * A hub is defined RELATIVE to the graph, not by an absolute count.
         *
         * An absolute threshold does not survive contact with the data: in the AI-agent
         * flow `CheckSteered` takes 16 of 27 transitions while `AwaitUser` takes 4, and
         * any constant that catches the first at 4 also catches the second — which then
         * dashes the border of an ordinary Step. The share of the graph is the real
         * signal: 59% versus 15%.
         */
        isHub: count >= 5 && count >= transitions.length * 0.25,
        isRecoveryHub: failures >= 2,
        /**
         * Exact, not heuristic. Dex names no Step as "recovery", so the role is read off the failure
         * relation: how many Steps route their exhausted retries here, and whether this Step can send
         * the flow onward afterwards.
         */
        /**
         * A blanket failure policy: every eligible Step routes here. Floor of 5 so a small saga's four
         * legible edges keep being drawn — the test is "are these edges telling me anything different
         * from each other", and four are still countable at a glance.
         */
        hasUniformFailurePolicy: failures >= 5 && failures >= (stepCount - 1) * 0.8,
        recoveryRole: ((): RecoveryRole => {
          if (failures === 0) return 'none'
          if (failures === 1) return 'fallback'
          return (outboundControl.get(n.id) ?? 0) === 0 ? 'sink' : 'dispatcher'
        })(),
        inboundCount: count,
        inboundFailureCount: failures,
        waitFor,
        execute,
        resources: refsByOwner.get(n.id) ?? [],
        span: n.span,
        isConnectorStep: n.metadata?.connectorFactory === true,
        connector: connectorIdentity(n.metadata?.connector),
        explanation: typeof n.metadata?.explanation === 'string' ? n.metadata.explanation : undefined,
      }
    })
    .sort((a, b) => (a.isStart ? -1 : b.isStart ? 1 : a.label.localeCompare(b.label) || a.stepType.localeCompare(b.stepType)))

  // ---- entries, and the human-input join ----------------------------------
  const waitConditionEdges = graph.edges.filter((e) => e.kind === 'wait_condition')
  const stepsWaitingOn = (channelId: string): string[] => {
    const out = new Set<string>()
    for (const e of waitConditionEdges) {
      if (e.from !== channelId) continue
      const stepId = waitNodeToStep.get(e.to)
      if (stepId !== undefined) out.add(stepId)
    }
    return [...out]
  }

  const actionByRpcName = new Map<string, NonNullable<EntryModel['action']>>()
  for (const action of Array.isArray(graph.v2?.actions) ? graph.v2.actions : []) {
    actionByRpcName.set(action.rpcName, {
      label: action.label,
      requiredPermission: action.requiredPermission,
    })
  }
  const viewsOf = (rpcName: string): EntryView[] =>
    (['summary', 'display'] as const).filter((view) => graph.v2?.[view]?.rpcName === rpcName)

  const entries: EntryModel[] = graph.nodes
    .filter((n) => n.kind === 'rpc' || n.kind === 'timeout_handler')
    .map((n) => {
      const refs = refsByOwner.get(n.id) ?? []
      const opensGates = refs
        .filter((r) => r.access === 'publish')
        .map((r) => ({ channelId: r.resourceId, stepIds: stepsWaitingOn(r.resourceId) }))
        .filter((g) => g.stepIds.length > 0)
      const isRpc = n.kind === 'rpc'
      const action = isRpc ? actionByRpcName.get(n.name) : undefined
      return {
        id: n.id,
        kind: isRpc ? ('rpc' as const) : ('timeoutHandler' as const),
        name: n.name,
        branches: groupBranches(branchesByOwner.get(n.id) ?? []),
        resources: refs,
        opensGates,
        ...(action === undefined ? {} : { action }),
        views: isRpc ? viewsOf(n.name) : [],
        span: n.span,
      }
    })
    .sort((a, b) => a.name.localeCompare(b.name))

  // ---- subflows ------------------------------------------------------------
  const subflows: SubFlowRef[] = graph.nodes
    .filter((n) => n.kind === 'subflow')
    .map((n) => {
      const edge = graph.edges.find((e) => e.kind === 'subflow' && e.to === n.id)
      return {
        id: n.id,
        /**
         * `self.` is the analyser's ACCESSOR, not part of the name.
         *
         * The Python analyser records a SubFlow declared as an attribute of the Flow verbatim, so the
         * real fanyi pipeline names its children `self.curate` and `self.produce`. Rendering that puts
         * a fragment of source syntax on a card, which the no-expressions guard caught. The identifier
         * itself is kept verbatim; only the receiver is dropped.
         */
        flowType: n.name.replace(/^self\./, ''),
        external: n.external === true,
        startedByStepId: edge === undefined ? undefined : waitNodeToStep.get(edge.from),
      }
    })

  // Anything we did not model at all is named rather than silently absent.
  for (const n of graph.nodes) {
    const known =
      n.kind === 'step' ||
      n.kind === 'rpc' ||
      n.kind === 'timeout_handler' ||
      n.kind === 'wait' ||
      n.kind === 'decision' ||
      n.kind === 'subflow' ||
      RESOURCE_KINDS.has(n.kind) ||
      // Dispatch nodes are edgeless markers, re-derived per grouped section instead.
      n.kind === 'wait_dispatch' ||
      n.kind === 'decision_dispatch'
    if (!known) dropped.push(`${n.kind} ${n.id}`)
  }

  return {
    flowType: graph.flow.name,
    valid: graph.valid,
    source: graph.source,
    startStepId: graph.flow.startStepId,
    steps,
    entries,
    resources,
    subflows,
    transitions,
    diagnostics: graph.diagnostics ?? [],
    provenance,
    dropped,
  }
}

function connectorIdentity(value: unknown): StepModel['connector'] {
  if (typeof value !== 'object' || value === null) return undefined
  const connector = value as Record<string, unknown>
  if (typeof connector.connectorId !== 'string' || typeof connector.operationId !== 'string'
    || typeof connector.operationKind !== 'string' || typeof connector.connectionName !== 'string'
    || typeof connector.moduleVersion !== 'string') return undefined
  return {
    connectorId: connector.connectorId,
    operationId: connector.operationId,
    operationKind: connector.operationKind,
    connectionName: connector.connectionName,
    moduleVersion: connector.moduleVersion,
  }
}

/** Source position order; an edge without a span keeps wire order after positioned ones. */
function bySourcePosition(a: FdgEdge, b: FdgEdge): number {
  if (a.span === undefined || b.span === undefined) {
    return Number(a.span === undefined) - Number(b.span === undefined)
  }
  return a.span.startLine - b.span.startLine || a.span.startColumn - b.span.startColumn
}

/** Never throws. A malformed fixture yields an empty flow that says so. */
export function safeDecode(
  raw: unknown,
  provenance: PocFlow['provenance'],
): PocFlow {
  try {
    const g = raw as FlowDefinitionGraph
    if (!Array.isArray(g?.nodes) || !Array.isArray(g?.edges)) {
      throw new Error('nodes/edges missing')
    }
    return decodeFlow({ ...g, diagnostics: g.diagnostics ?? [] }, provenance)
  } catch (err) {
    return {
      flowType: 'unreadable',
      valid: false,
      source: { language: 'unknown', path: '' },
      steps: [],
      entries: [],
      resources: [],
      subflows: [],
      transitions: [],
      diagnostics: [
        {
          severity: 'error',
          code: 'decode_failed',
          message: err instanceof Error ? err.message : String(err),
        },
      ],
      provenance,
      dropped: [],
    }
  }
}

export type { FdgEdge }
