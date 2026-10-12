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
import { failureHandling, failureRoutes, stepById, type PocFlow } from '../model/pocFlow';
import type { RunOverlay, StepExecution } from '../model/run';
import { controlTopologyView } from './controlTopology';
import { hasDetour, runDetourSources, withDetours } from './failureDetours';
import type { StepGroup } from './groups';
import type { Box, Direction, Scene, ViewOpts } from './types';

const DIRECTIONS: Direction[] = ['tb', 'lr'];

describe('failure handling', () => {
  it('leaves out the Steps only failures lead to, in every view', async () => {
    const { flow, groups } = refund();
    const hidden = failureHandling(flow);

    expect([...hidden].sort()).toEqual(['step:agenticEmailFailed', 'step:agenticSubscriptionFailed']);
    for (const open of [new Set<string>(), new Set(groups.map((group) => group.id))]) {
      for (const direction of DIRECTIONS) {
        const scene = await layout(flow, groups, direction, open);

        expect(scene.boxes.filter((box) => hidden.has(box.id))).toEqual([]);
        expect(scene.links.filter((link) => link.family === 'failure')).toEqual([]);
      }
    }
  });

  it('counts the failures that leave each card, on the card', async () => {
    const { flow, groups } = refund();
    const scene = await layout(flow, groups, 'tb', new Set(groups.map((group) => group.id)));

    for (const box of scene.boxes) {
      expect(box.failures ?? 0, box.id).toBe(failureRoutes(flow, box.id).length);
    }
    expect(scene.boxes.some((box) => (box.failures ?? 0) > 0)).toBe(true);
  });

  it("draws a selected Step's detour beside it, moving nothing and covering no card", async () => {
    const { flow, groups } = refund();
    const sources = flow.steps.filter((step) => hasDetour(flow, step.id));

    expect(sources.length).toBeGreaterThan(0);
    for (const direction of DIRECTIONS) {
      const opts = options(groups, direction, new Set(groups.map((group) => group.id)));
      const scene = await controlTopologyView.layout(flow, opts);
      for (const source of sources) {
        const shown = withDetours(scene, flow, opts, [source.id]);
        const added = shown.boxes.filter((box) => box.detourOf === source.id);
        const label = `${direction} ${source.id}`;

        expect(shown.boxes.slice(0, scene.boxes.length), label).toEqual(scene.boxes);
        expect(added.length, label).toBeGreaterThan(0);
        expect(added.filter((box) => scene.boxes.some((other) => overlaps(box, other))), label).toEqual([]);
        expect(shown.links.filter((link) => link.from === source.id && link.family === 'failure').length, label)
          .toBe(failureRoutes(flow, source.id).length);
      }
    }
  });

  it('points to a main-story Step that a failure goes straight to', async () => {
    const { flow, groups } = refund();
    const hidden = failureHandling(flow);
    const opts = options(groups, 'tb', new Set(groups.map((group) => group.id)));
    const scene = await controlTopologyView.layout(flow, opts);
    const route = flow.transitions.find((t) => t.kind === 'failure_transition' && !hidden.has(t.toStepId));
    if (route === undefined) throw new Error('the refund graph has a failure route into its main story');

    const shown = withDetours(scene, flow, opts, [route.fromStepId]);
    const pointer = shown.boxes.find((box) => box.kind === 'pointer' && box.detourOf === route.fromStepId);

    expect(pointer?.target).toBe(route.toStepId);
    expect(pointer?.title).toBe(`↪ ${scene.boxes.find((box) => box.id === route.toStepId)?.title}`);
  });

  it('opens the detour a run went through, from the Step that failed into it', () => {
    const { flow } = refund();
    const handler = stepById(flow, 'step:agenticEmailFailed');
    const route = flow.transitions.find(
      (t) => t.kind === 'failure_transition' && t.toStepId === 'step:agenticEmailFailed',
    );
    const source = route === undefined ? undefined : stepById(flow, route.fromStepId);
    if (handler === undefined || source === undefined) throw new Error('the refund graph handles a failed email');

    const run = overlay([
      execution(1, source.stepType, 'failed'),
      execution(2, handler.stepType, 'completed'),
    ]);

    expect(runDetourSources(flow, run)).toEqual([source.id]);
    expect(runDetourSources(flow, overlay([execution(1, source.stepType, 'completed')]))).toEqual([]);
  });
});

function refund(): { flow: PocFlow; groups: StepGroup[] } {
  const graph = refundGraph as unknown as FlowDefinitionGraph;
  const flow = safeDecode(graph, { generated: true, note: 'customer-refund-agentic' });
  return { flow, groups: groupsFromDefinition(graph as never, flow) };
}

function options(groups: StepGroup[], direction: Direction, open: ReadonlySet<string>): ViewOpts {
  return { direction, run: null, groups, openStages: open };
}

function layout(flow: PocFlow, groups: StepGroup[], direction: Direction, open: ReadonlySet<string>): Promise<Scene> {
  return controlTopologyView.layout(flow, options(groups, direction, open));
}

function overlaps(a: Box, b: Box): boolean {
  return a.x < b.x + b.w && b.x < a.x + a.w && a.y < b.y + b.h && b.y < a.y + a.h;
}

function execution(ordinal: number, stepType: string, status: 'completed' | 'failed'): StepExecution {
  return {
    stepExecutionId: `e${ordinal}`,
    stepType,
    ordinal,
    waitFor: null,
    execute: { status },
    attempts: status === 'failed' ? 3 : 1,
  };
}

function overlay(executions: StepExecution[]): RunOverlay {
  return { runId: 'r1', flowId: 'f1', status: 'Running', executions, simulated: true, note: 'test', now: 0 };
}
