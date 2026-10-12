// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

/** One control edge between view nodes: Steps, or closed stages. */
export interface GraphEdge {
  id: string
  from: string
  to: string
  failure: boolean
}

/** Breadth-first from the start over transitions, then failures, then the unreached, in given order. */
export function readingOrder(ids: string[], edges: GraphEdge[], startId: string | undefined): string[] {
  const seed = startId !== undefined && ids.includes(startId) ? startId : ids[0]
  if (seed === undefined) return []
  const order = [seed]
  const seen = new Set(order)
  const walk = (useFailures: boolean): void => {
    for (let i = 0; i < order.length; i += 1) {
      for (const e of edges) {
        if (e.from !== order[i] || e.from === e.to || seen.has(e.to) || (!useFailures && e.failure)) continue
        seen.add(e.to)
        order.push(e.to)
      }
    }
  }
  walk(false)
  walk(true)
  for (const id of ids) if (!seen.has(id)) order.push(id)
  return order
}

/** Longest-path ranks without back edges; an unreached node sits one rank before its first target. */
export function longestPathRanks(order: string[], edges: GraphEdge[]): Map<string, number> {
  const out = new Map<string, string[]>()
  for (const e of edges) if (e.from !== e.to) out.set(e.from, [...(out.get(e.from) ?? []), e.to])
  const onStack = new Set<string>()
  const seen = new Set<string>()
  const back = new Set<string>()
  const key = (a: string, b: string): string => `${a}\u0000${b}`
  const visit = (v: string): void => {
    seen.add(v)
    onStack.add(v)
    for (const w of out.get(v) ?? []) {
      if (onStack.has(w)) back.add(key(v, w))
      else if (!seen.has(w)) visit(w)
    }
    onStack.delete(v)
  }
  for (const id of order) if (!seen.has(id)) visit(id)

  const forward = edges.filter((e) => e.from !== e.to && !back.has(key(e.from, e.to)))
  const inbound = new Set(forward.map((e) => e.to))
  const rank = new Map(order.map((id) => [id, 0]))
  for (let round = 0; round < order.length; round += 1) {
    let isMoved = false
    for (const e of forward) {
      const want = (rank.get(e.from) ?? 0) + 1
      if (want > (rank.get(e.to) ?? 0)) {
        rank.set(e.to, want)
        isMoved = true
      }
    }
    if (!isMoved) break
  }
  const start = order[0]
  for (const id of order) {
    if (id === start || inbound.has(id)) continue
    const next = forward.filter((e) => e.from === id).map((e) => rank.get(e.to) ?? 0)
    if (next.length > 0) rank.set(id, Math.max(0, Math.min(...next) - 1))
  }
  return rank
}

/** Each node's one drawn line: from the latest node leading to it, preferring its own stage. */
export function mainLines(
  order: string[],
  edges: GraphEdge[],
  ranks: Map<string, number>,
  stageOf: (id: string) => string | undefined,
): Set<string> {
  const position = new Map(order.map((id, i) => [id, i]))
  const at = (id: string): number => position.get(id) ?? order.length
  const rank = (id: string): number => ranks.get(id) ?? 0
  const real = edges.filter((e) => e.from !== e.to && e.to !== order[0])
  // Stable sorts keep each source's edges in source order.
  const latestFirst = [...real].sort((a, b) => rank(b.from) - rank(a.from) || at(a.from) - at(b.from))
  const readingFirst = [...real].sort((a, b) => at(a.from) - at(b.from))
  const main = new Map<string, string>()
  const take = (list: GraphEdge[], isEligible: (e: GraphEdge) => boolean): void => {
    for (const e of list) if (!main.has(e.to) && isEligible(e)) main.set(e.to, e.id)
  }
  const isSameStage = (e: GraphEdge): boolean => stageOf(e.to) !== undefined && stageOf(e.from) === stageOf(e.to)
  take(latestFirst, (e) => !e.failure && rank(e.from) < rank(e.to) && isSameStage(e))
  take(latestFirst, (e) => !e.failure && rank(e.from) < rank(e.to))
  take(latestFirst, (e) => rank(e.from) < rank(e.to))
  take(readingFirst, () => true)
  return new Set(main.values())
}
