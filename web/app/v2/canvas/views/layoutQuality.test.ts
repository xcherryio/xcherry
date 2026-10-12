// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import { describe, expect, it } from 'vitest';
import { groupsFromDefinition } from '../../groupsFromGraph';
import { safeDecode } from '../model/decode';
import { controlTopologyView } from './controlTopology';
import { LAYOUT_PRINCIPLES, measureScene, violations } from './layoutQuality';
import type { Direction, Scene, ViewOpts } from './types';

/**
 * Every Flow the docs ship, drawn four ways. Spacing constants are judged here rather than
 * against one screenshot, so a change that helps one graph cannot quietly break twelve.
 */
/** The two decoders disagree on their graph type, and a fixture only has to satisfy both. */
type CorpusGraph = { nodes?: unknown[] };

const CORPUS = (import.meta as unknown as {
  glob: (pattern: string, opts: object) => Record<string, CorpusGraph>;
}).glob('../../../../../docs/src/data/flow-definitions/**/*.json', {
  eager: true,
  import: 'default',
});

const DETAILS = ['collapsed', 'expanded'] as const;
const DIRECTIONS: Direction[] = ['tb', 'lr'];

async function scenesOf(graph: CorpusGraph) {
  const flow = safeDecode(graph as never, { generated: true, note: 'corpus' });
  const groups = groupsFromDefinition(graph as never, flow);
  const scenes: { detail: (typeof DETAILS)[number]; direction: Direction; flow: typeof flow; scene: Scene }[] = [];
  for (const detail of DETAILS) {
    for (const direction of DIRECTIONS) {
      // Collapsed closes every stage and Expanded opens every one.
      const openStages = new Set(detail === 'expanded' ? groups.map((group) => group.id) : []);
      const opts: ViewOpts = { direction, run: null, groups, openStages };
      scenes.push({ detail, direction, flow, scene: await controlTopologyView.layout(flow, opts) });
    }
  }
  return scenes;
}

const NAMED = Object.entries(CORPUS)
  .map(([path, graph]) => ({ name: path.split('/').slice(-1)[0] as string, graph }))
  .filter(({ graph }) => Array.isArray(graph.nodes) && graph.nodes.length > 0)
  .sort((left, right) => left.name.localeCompare(right.name));

describe('layout principles hold across the shipped corpus', () => {
  it('found the corpus', () => {
    expect(NAMED.length).toBeGreaterThan(20);
  });

  it.each(NAMED)('$name obeys every principle in all four view modes', async ({ graph }) => {
    for (const { detail, direction, flow, scene } of await scenesOf(graph)) {
      const broken = violations(flow, scene, direction);
      expect(broken, `${detail}/${direction}: ${JSON.stringify(broken)}`).toEqual([]);
    }
  });

  /**
   * Regions come from `dex:group`, an FDG 2.0 directive, so a 1.0 definition has none and passes the
   * region principles vacuously. Pinned so stale regenerated JSON cannot quietly empty the corpus.
   */
  it('still has definitions that carry groups', async () => {
    const carrying: string[] = [];
    for (const { name, graph } of NAMED) {
      if ((await scenesOf(graph)).some(({ scene }) => scene.bands.length > 0)) carrying.push(name);
    }
    expect(carrying).toEqual([
      'connector-factory.json',
      'customer-refund-agentic.json',
      'customer-refund.json',
    ]);
  });

  it('reports the worst metric per principle, so a regression has a number', async () => {
    const worst = new Map<string, { value: number; where: string }>();
    let regionScenes = 0;
    for (const { name, graph } of NAMED) {
      for (const { detail, direction, flow, scene } of await scenesOf(graph)) {
        if (scene.bands.length > 0) regionScenes++;
        const metrics = measureScene(flow, scene, direction);
        for (const [key, value] of Object.entries(metrics)) {
          const prior = worst.get(key);
          if (prior === undefined || value > prior.value) {
            worst.set(key, { value, where: `${name} ${detail}/${direction}` });
          }
        }
      }
    }
    const lines = [...worst.entries()].map(([key, { value, where }]) =>
      `${key.padEnd(12)} ${value.toFixed(3).padStart(8)}  ${where}`);
    const scenes = NAMED.length * DETAILS.length * DIRECTIONS.length;
    // eslint-disable-next-line no-console
    console.log(
      `\n${LAYOUT_PRINCIPLES.length} principles over ${NAMED.length} flows (${scenes} scenes)`
      + `\nregion principles exercised by ${regionScenes}/${scenes} scenes — the rest declare no groups`
      + `\n${lines.join('\n')}`,
    );
    expect(lines.length).toBeGreaterThan(0);
  });
});
