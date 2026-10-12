// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import { describe, expect, it } from 'vitest';
import refundGraph from '../../../../../docs/src/data/flow-definitions/customer-refund-agentic.json';
import { groupsFromDefinition } from '../../groupsFromGraph';
import { safeDecode } from '../model/decode';
import type { FlowDefinitionGraph } from '../model/fdg';
import type { PocFlow } from '../model/pocFlow';
import { controlTopologyView } from './controlTopology';
import type { StepGroup } from './groups';
import { longestPathRanks, mainLines, readingOrder, type GraphEdge } from './mainLine';
import { stageId } from './stageCard';
import type { Box, Direction, Scene, ViewOpts } from './types';

const DIRECTIONS: Direction[] = ['tb', 'lr'];

describe('control topology stages', () => {
  it('draws each group of two or more Steps as one stage card when collapsed', async () => {
    const { flow, groups } = refund();
    const scene = await layout(flow, groups, 'tb', new Set());
    const stages = scene.boxes.filter((box) => box.kind === 'stage');

    expect(stages.map((box) => box.title).sort()).toEqual(['Control', 'Evidence', 'Failure', 'Resolution']);
    expect(stages.find((box) => box.title === 'Resolution')?.stage?.stepCount).toBe(11);
    // A one-Step group is drawn as its Step, inside its own labelled band.
    expect(scene.bands.map((band) => band.label).sort()).toEqual(['Close', 'Intake', 'Reasoning']);
    expect(scene.bands.every((band) => !band.collapsible)).toBe(true);
  });

  it('draws every open stage as one region with a close control', async () => {
    const { flow, groups } = refund();
    for (const direction of DIRECTIONS) {
      const scene = await layout(flow, groups, direction, new Set(groups.map((group) => group.id)));
      const labels = scene.bands.map((band) => band.label);

      expect(scene.boxes.some((box) => box.kind === 'stage'), direction).toBe(false);
      expect(new Set(labels).size, direction).toBe(labels.length);
      expect(scene.bands.filter((band) => band.collapsible).map((band) => band.label).sort()).toEqual([
        'Control',
        'Evidence',
        'Failure',
        'Resolution',
      ]);
    }
  });

  it('never overlaps two cards or two bands, in any view', async () => {
    const { flow, groups } = refund();
    for (const { label, open } of views(groups)) {
      for (const direction of DIRECTIONS) {
        const scene = await layout(flow, groups, direction, open);

        expect(overlappingPairs(scene.boxes), `${label}/${direction}`).toEqual([]);
        expect(overlappingPairs(scene.bands.map((band) => ({ ...band, id: band.label }))), `${label}/${direction}`)
          .toEqual([]);
      }
    }
  });

  it('marks a stage that holds the start, and never draws a line to the start', async () => {
    const { flow, groups } = refund();
    for (const { label, open } of views(groups)) {
      const scene = await layout(flow, groups, 'tb', open);
      const start = scene.boxes.find((box) => box.isStart === true);

      expect(start, label).toBeDefined();
      expect(scene.links.filter((link) => link.to === start?.id), label).toEqual([]);
    }
  });

  it('draws the OpenAI Connector Step as AI model work', async () => {
    const { flow, groups } = refund();
    const scene = await layout(flow, groups, 'tb', new Set(groups.map((group) => group.id)));
    const connector = scene.boxes.find((box) => box.id === 'step:GenerateCustomerMessageStep');

    expect(connector?.tile).toEqual({ glyph: 'spark', tone: 'accent' });
    expect(connector?.subtitle).toBe('AI model · create response');
  });
});

describe('control topology main lines', () => {
  it('draws every Step that something leads to with exactly one line in', async () => {
    const { flow, groups } = refund();
    const scene = await layout(flow, groups, 'tb', new Set(groups.map((group) => group.id)));
    const led = new Set(flow.transitions.filter((t) => !t.isSelfLoop).map((t) => t.toStepId));
    led.delete(flow.startStepId as string);

    for (const id of led) {
      expect(scene.links.filter((link) => link.to === id).length, id).toBe(1);
    }
  });

  it('draws in every view exactly the main lines between Steps that cross between its cards', async () => {
    const { flow, groups } = refund();
    const main = stepMainLines(flow, groups);
    for (const { label, open } of views(groups)) {
      const scene = await layout(flow, groups, 'tb', open);
      const onScreen = new Set(scene.boxes.map((box) => box.id));
      const nodeOf = (stepId: string): string => {
        if (onScreen.has(stepId)) return stepId;
        const group = groups.find((candidate) => candidate.stepIds.includes(stepId));
        return group === undefined ? stepId : stageId(group.id);
      };
      const expected = new Set(
        flow.transitions
          .filter((t) => main.has(t.id) && nodeOf(t.fromStepId) !== nodeOf(t.toStepId))
          .map((t) => `${nodeOf(t.fromStepId)}->${nodeOf(t.toStepId)}`),
      );
      const drawn = new Set(scene.links.map((link) => `${link.from}->${link.to}`));

      expect([...drawn].sort(), label).toEqual([...expected].sort());
    }
  });

  it('docks every transition that is not drawn as a jump chip on the card it leaves', async () => {
    const { flow, groups } = refund();
    const scene = await layout(flow, groups, 'tb', new Set(groups.map((group) => group.id)));
    const drawn = new Set(scene.links.map((link) => `${link.from}->${link.to}`));
    const jumps = flow.transitions.filter((t) => !t.isSelfLoop && !drawn.has(`${t.fromStepId}->${t.toStepId}`));
    const chips = scene.boxes.flatMap((box) =>
      (box.chips ?? []).flat().filter((chip) => chip.targetId !== undefined).map((chip) => `${box.id}->${chip.targetId}`));

    expect(chips.sort()).toEqual(jumps.map((t) => `${t.fromStepId}->${t.toStepId}`).sort());
  });

  it('keeps the main lines when the analyzer lists edges in another order or code moves down the file', () => {
    const graph = refundGraph as unknown as FlowDefinitionGraph;
    const { flow, groups } = refund();
    const base = keysOf(flow, stepMainLines(flow, groups));
    const reversed = { ...graph, edges: [...graph.edges].reverse() };
    const shifted = {
      ...graph,
      edges: graph.edges.map((e) => (e.span ? { ...e, span: { ...e.span, startLine: e.span.startLine + 137 } } : e)),
    };
    for (const variant of [reversed, shifted]) {
      const other = safeDecode(variant as never, { generated: true, note: 'variant' });
      const otherGroups = groupsFromDefinition(variant as never, other);

      expect(keysOf(other, stepMainLines(other, otherGroups))).toEqual(base);
    }
  });

  it('routes no line under a card it does not join', async () => {
    const { flow, groups } = refund();
    for (const { label, open } of views(groups)) {
      for (const direction of DIRECTIONS) {
        const scene = await layout(flow, groups, direction, open);

        expect(linesUnderCards(scene), `${label}/${direction}`).toEqual([]);
      }
    }
  });
});

describe('control topology for a Flow with no Steps', () => {
  it('draws one card per RPC, Actions first and the Summary and Display views last', async () => {
    const scene = await entityListScene('tb');

    expect(scene.boxes.map((box) => [box.title, box.kind, box.subtitle, box.rpcRole])).toEqual([
      ['RemoveEntry', 'rpcCard', 'Action: Remove entry', 'action'],
      ['AddEntry', 'rpcCard', 'changes state', undefined],
      ['ListEntries', 'rpcCard', 'reads state', undefined],
      ['GetDexSummary', 'rpcCard', 'Summary view', 'view'],
      ['GetDexDisplay', 'rpcCard', 'Display view', 'view'],
    ]);
    expect(scene.links).toEqual([]);
    expect(scene.bands).toEqual([]);
    expect(scene.notes).toEqual(['This Flow has no Steps, so each of its 5 RPCs is drawn as a card.']);
  });

  it('says each kind of access in one row', async () => {
    const addEntry = cardOf(await entityListScene('tb'), 'rpc:AddEntry');

    expect(addEntry.rows?.map((row) => [row.text, row.trail])).toEqual([
      ['reads entries', undefined],
      ['writes entries, entry-count', undefined],
      ['publishes to entry-events', undefined],
    ]);
  });

  // The text ellipsizes and the trail does not, so a long first name cannot hide the count.
  it('puts the count of the names a row has no room for in its trail', async () => {
    const graph = entityListGraph();
    graph.nodes = graph.nodes.map((node) => node.id === 'resource:attribute:entryCount'
      ? { ...node, name: 'entity-list-entry-count-by-region-and-day' }
      : node);
    const flow = safeDecode(graph, { generated: true, note: 'test' });
    const scene = await controlTopologyView.layout(flow, { direction: 'tb', run: null, groups: [] });

    expect(cardOf(scene, 'rpc:AddEntry').rows).toContainEqual({
      glyph: '▪',
      text: 'writes entity-list-entry-count-by-region-and-day',
      trail: '+1 more',
      tone: 'quiet',
    });
  });

  it('keeps apart two resources that share a name, and names their kinds', async () => {
    const graph = entityListGraph();
    graph.nodes.push({ id: 'resource:stream:entries', kind: 'stream', name: 'entries', resource: { valueType: 'string' } });
    graph.edges.push(
      { id: 'edge:stream', kind: 'resource_write', from: 'rpc:AddEntry', to: 'resource:stream:entries', metadata: { phase: 'rpc' } },
      { id: 'edge:again', kind: 'resource_write', from: 'rpc:AddEntry', to: 'resource:attribute:entries', metadata: { phase: 'rpc' } },
    );
    const flow = safeDecode(graph, { generated: true, note: 'test' });
    const scene = await controlTopologyView.layout(flow, { direction: 'tb', run: null, groups: [] });

    expect(cardOf(scene, 'rpc:AddEntry').rows?.map((row) => [row.text, row.trail])).toEqual([
      ['reads entries (Attribute)', undefined],
      ['writes entries (Attribute)', '+2 more'],
      ['publishes to entry-events', undefined],
    ]);
  });

  it('notes the timeout handlers it does not draw', async () => {
    const graph = entityListGraph();
    graph.nodes.push({ id: 'timeout_handler:OnFlowTimeout', kind: 'timeout_handler', name: 'OnFlowTimeout' });
    const flow = safeDecode(graph, { generated: true, note: 'test' });
    const scene = await controlTopologyView.layout(flow, { direction: 'tb', run: null, groups: [] });

    expect(scene.boxes.map((box) => box.id)).not.toContain('timeout_handler:OnFlowTimeout');
    expect(scene.notes).toEqual([
      'This Flow has no Steps, so each of its 5 RPCs is drawn as a card.',
      '1 timeout handler is not drawn.',
    ]);
  });

  it('marks an Action with its required permission', async () => {
    expect(cardOf(await entityListScene('tb'), 'rpc:RemoveEntry').rows?.[0]).toEqual({
      glyph: '◈',
      text: 'requires entries.manage',
      tone: 'quiet',
    });
  });

  it('never overlaps two cards, and keeps every card inside the scene, in both directions', async () => {
    for (const direction of DIRECTIONS) {
      const scene = await entityListScene(direction);

      expect(scene.boxes).toHaveLength(5);
      expect(overlappingPairs(scene.boxes), direction).toEqual([]);
      for (const box of scene.boxes) {
        expect(box.x + box.w).toBeLessThanOrEqual(scene.width);
        expect(box.y + box.h).toBeLessThanOrEqual(scene.height);
      }
    }
  });

  it('fills rows top-down and columns left-right', async () => {
    const topDown = (await entityListScene('tb')).boxes;
    const leftRight = (await entityListScene('lr')).boxes;

    expect(new Set(topDown.slice(0, 3).map((box) => box.y)).size).toBe(1);
    expect(topDown[3].y).toBeGreaterThan(topDown[0].y);
    expect(new Set(leftRight.slice(0, 3).map((box) => box.x)).size).toBe(1);
    expect(leftRight[3].x).toBeGreaterThan(leftRight[0].x);
  });

  it('still reports no Steps for a Flow with neither Steps nor RPCs', async () => {
    const flow = safeDecode(
      {
        schemaVersion: '2.0',
        valid: true,
        source: { language: 'go', path: 'test.go' },
        flow: { name: 'TestFlow', startStepId: 'step:none' },
        nodes: [],
        edges: [],
        diagnostics: [],
      },
      { generated: true, note: 'test' },
    );

    expect(await controlTopologyView.layout(flow, { direction: 'tb', run: null, groups: [] })).toEqual({
      boxes: [], links: [], bands: [], width: 640, height: 200, notes: ['No steps.'],
    });
  });
});

describe('control topology for a Flow with Steps', () => {
  // Pins every box, so a change to card or layout geometry has to update the snapshot on purpose.
  it.each([
    ['collapsed', 'tb'],
    ['collapsed', 'lr'],
    ['expanded', 'tb'],
    ['expanded', 'lr'],
  ] as const)('keeps the geometry of the shipped refund graph %s %s', async (view, direction) => {
    const { flow, groups } = refund();
    const open = view === 'expanded' ? new Set(groups.map((group) => group.id)) : new Set<string>();
    const scene = await layout(flow, groups, direction, open);

    expect({
      size: `${Math.round(scene.width)}x${Math.round(scene.height)}`,
      boxes: scene.boxes.map((box) => `${box.id} ${Math.round(box.x)},${Math.round(box.y)} ${box.w}x${box.h}`),
    }).toMatchSnapshot();
  });

  // The agentic refund graph declares four Actions and both views, so the join could leak here.
  it('draws a Flow with Steps as Steps whether or not it declares a Flow v2 contract', async () => {
    const graph = refundGraph as unknown as FlowDefinitionGraph;
    for (const direction of DIRECTIONS) {
      const { flow, groups } = refund();
      const bare = safeDecode({ ...graph, v2: undefined }, { generated: true, note: 'bare' });
      const withContract = await layout(flow, groups, direction, new Set());
      const withoutContract = await layout(bare, groupsFromDefinition(graph as never, bare), direction, new Set());
      const shape = (scene: Scene) => ({
        boxes: scene.boxes.map((box) => `${box.kind} ${box.id}`),
        links: scene.links.map((link) => `${link.from}->${link.to}`),
      });

      expect(withContract.boxes.some((box) => box.kind === 'rpcCard')).toBe(false);
      expect(shape(withContract)).toEqual(shape(withoutContract));
    }
  });
});

function refund(): { flow: PocFlow; groups: StepGroup[] } {
  const graph = refundGraph as unknown as FlowDefinitionGraph;
  const flow = safeDecode(graph, { generated: true, note: 'customer-refund-agentic' });
  return { flow, groups: groupsFromDefinition(graph as never, flow) };
}

function layout(flow: PocFlow, groups: StepGroup[], direction: Direction, open: ReadonlySet<string>): Promise<Scene> {
  const opts: ViewOpts = { direction, run: null, groups, openStages: open };
  return controlTopologyView.layout(flow, opts);
}

/** Collapsed, each stage opened alone, and Expanded. */
function views(groups: StepGroup[]): { label: string; open: ReadonlySet<string> }[] {
  return [
    { label: 'collapsed', open: new Set<string>() },
    ...groups.map((group) => ({ label: `open ${group.label}`, open: new Set([group.id]) })),
    { label: 'expanded', open: new Set(groups.map((group) => group.id)) },
  ];
}

function stepMainLines(flow: PocFlow, groups: StepGroup[]): Set<string> {
  const ids = new Set(flow.steps.map((step) => step.id));
  const edges: GraphEdge[] = flow.transitions
    .filter((t) => ids.has(t.fromStepId) && ids.has(t.toStepId))
    .map((t) => ({ id: t.id, from: t.fromStepId, to: t.toStepId, failure: t.kind === 'failure_transition' }));
  const order = readingOrder(flow.steps.map((step) => step.id), edges, flow.startStepId);
  return mainLines(order, edges, longestPathRanks(order, edges), (id) =>
    groups.find((group) => group.stepIds.includes(id))?.id);
}

function keysOf(flow: PocFlow, main: Set<string>): string[] {
  return flow.transitions.filter((t) => main.has(t.id)).map((t) => `${t.fromStepId}->${t.toStepId}:${t.kind}`).sort();
}

function linesUnderCards(scene: Scene): string[] {
  const hits: string[] = [];
  for (const link of scene.links) {
    for (let i = 1; i < link.points.length; i += 1) {
      const a = link.points[i - 1];
      const b = link.points[i];
      for (const card of scene.boxes) {
        if (card.id === link.from || card.id === link.to) continue;
        const isCrossing = Math.min(a.x, b.x) <= card.x + card.w - 2 && card.x + 2 <= Math.max(a.x, b.x)
          && Math.min(a.y, b.y) <= card.y + card.h - 2 && card.y + 2 <= Math.max(a.y, b.y);
        if (isCrossing) hits.push(`${link.id} under ${card.id}`);
      }
    }
  }
  return hits;
}

function entityListScene(direction: Direction): Promise<Scene> {
  const flow = safeDecode(entityListGraph(), { generated: true, note: 'entity-list' });
  return controlTopologyView.layout(flow, { direction, run: null, groups: [] });
}

function cardOf(scene: { boxes: Box[] }, id: string): Box {
  const box = scene.boxes.find((candidate) => candidate.id === id);
  if (box === undefined) throw new Error(`no card ${id}`);
  return box;
}

/** A step-less entity Flow in the shape `dexcli visualize` emits for a Go Flow with only RPCs. */
function entityListGraph(): FlowDefinitionGraph {
  const rpc = (name: string) => [
    { id: `rpc:${name}`, kind: 'rpc', name },
    { id: `decision:rpc:${name}`, kind: 'decision', name: 'rpcResult', parentId: `rpc:${name}`, decision: { type: 'rpcResult' } },
  ];
  const edges: [string, string, string][] = [
    ['resource_read', 'resource:attribute:entries', 'rpc:AddEntry'],
    ['resource_write', 'rpc:AddEntry', 'resource:attribute:entryCount'],
    ['resource_write', 'rpc:AddEntry', 'resource:attribute:entries'],
    ['resource_publish', 'rpc:AddEntry', 'resource:channel:entryEvents'],
    ['resource_read', 'resource:attribute:entries', 'rpc:RemoveEntry'],
    ['resource_write', 'rpc:RemoveEntry', 'resource:attribute:entryCount'],
    ['resource_write', 'rpc:RemoveEntry', 'resource:attribute:entries'],
    ['resource_read', 'resource:attribute:entries', 'rpc:ListEntries'],
    ['resource_read', 'resource:attribute:entryCount', 'rpc:GetDexSummary'],
    ['resource_read', 'resource:attribute:entryCount', 'rpc:GetDexDisplay'],
    ['resource_read', 'resource:attribute:entries', 'rpc:GetDexDisplay'],
  ];
  return {
    schemaVersion: '2.0',
    valid: true,
    source: { language: 'go', path: 'entity_list_flow.go' },
    flow: { name: 'EntityListFlow' },
    nodes: [
      { id: 'resource:attribute:entries', kind: 'attribute', name: 'entries', resource: { valueType: '[]string' } },
      { id: 'resource:attribute:entryCount', kind: 'attribute', name: 'entry-count', resource: { valueType: 'int64' } },
      { id: 'resource:channel:entryEvents', kind: 'channel', name: 'entry-events', resource: { valueType: 'string' } },
      ...['AddEntry', 'RemoveEntry', 'ListEntries', 'GetDexSummary', 'GetDexDisplay'].flatMap(rpc),
    ],
    edges: edges.map(([kind, from, to], i) => ({ id: `edge:${i}`, kind, from, to, metadata: { phase: 'rpc' } })),
    diagnostics: [],
    v2: {
      summary: { rpcName: 'GetDexSummary' },
      display: { rpcName: 'GetDexDisplay' },
      actions: [{ rpcName: 'RemoveEntry', label: 'Remove entry', requiredPermission: 'entries.manage' }],
    },
  };
}

interface Rect { id: string; x: number; y: number; w: number; h: number }

/** Pairs sharing more than a hairline of area, named so a failure says which collided. */
function overlappingPairs(rects: readonly Rect[]): string[] {
  const hits: string[] = [];
  for (let left = 0; left < rects.length; left += 1) {
    for (let right = left + 1; right < rects.length; right += 1) {
      const a = rects[left];
      const b = rects[right];
      const overlapX = Math.min(a.x + a.w, b.x + b.w) - Math.max(a.x, b.x);
      const overlapY = Math.min(a.y + a.h, b.y + b.h) - Math.max(a.y, b.y);
      if (overlapX > 0.5 && overlapY > 0.5) hits.push(`${a.id} + ${b.id}`);
    }
  }
  return hits;
}

