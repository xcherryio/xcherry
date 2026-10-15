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
import { failureHandling, joinedSteps, type PocFlow } from '../model/pocFlow';
import { controlTopologyView } from '../views/controlTopology';
import type { StepGroup } from '../views/groups';
import { stageId } from '../views/stageCard';
import type { Box, Scene } from '../views/types';
import {
  boxShowing,
  cameraRoom,
  entranceDelays,
  FOCUS_MAX_ZOOM,
  focusView,
  framingNeighbours,
  openingView,
  type CameraView,
} from './camera';
import { OPEN_FLOOR } from './zoom';

describe('focus framing', () => {
  it('shows every card a transition joins to the Step, legibly and never magnified', async () => {
    const { flow, groups, scene } = await expanded();
    const room = cameraRoom(1400, 900, null);
    let framedWithEveryNeighbour = 0;
    for (const step of mainSteps(flow, scene)) {
      const neighbours = framingNeighbours(flow, groups, scene, step.id);
      const framed = focusView(scene, step.id, neighbours, room, FOCUS_MAX_ZOOM);
      if (framed === null) throw new Error(`${step.id} is not drawn`);
      expect(framed.view.zoom).toBeLessThanOrEqual(FOCUS_MAX_ZOOM);
      expect(framed.view.zoom).toBeGreaterThanOrEqual(OPEN_FLOOR - 1e-9);
      expect(inRoom(boxShowing(scene, step.id) as Box, framed.view, room)).toBe(true);
      if (framed.scope === 'nearest') continue;
      framedWithEveryNeighbour += 1;
      for (const id of neighbours.near) {
        expect(inRoom(scene.boxes.find((box) => box.id === id) as Box, framed.view, room), `${step.id} → ${id}`).toBe(true);
      }
    }
    expect(framedWithEveryNeighbour).toBeGreaterThan(5);
  });

  it('keeps the Step legible and its nearest neighbours in view when they cannot all fit', async () => {
    const { flow, groups, scene } = await expanded();
    const room = cameraRoom(520, 420, null);
    const busiest = mainSteps(flow, scene).sort(
      (a, b) => framingNeighbours(flow, groups, scene, b.id).near.length - framingNeighbours(flow, groups, scene, a.id).near.length,
    )[0] as Box;
    const framed = focusView(scene, busiest.id, framingNeighbours(flow, groups, scene, busiest.id), room, FOCUS_MAX_ZOOM);
    expect(framed?.scope).toBe('nearest');
    expect(framed?.view.zoom).toBeGreaterThanOrEqual(OPEN_FLOOR);
    expect(inRoom(busiest, framed?.view as CameraView, room)).toBe(true);
  });

  it('shows the whole drawing when that keeps most of the Step\'s size', async () => {
    const { flow, groups, scene } = await collapsed();
    const room = cameraRoom(2400, 1600, null);
    const step = mainSteps(flow, scene)[0] as Box;
    const framed = focusView(scene, step.id, framingNeighbours(flow, groups, scene, step.id), room, 1.7);
    expect(framed?.scope).toBe('whole');
    expect(framed?.view).toEqual(openingView(scene, room, 'tb', 1.7).view);
  });

  it('frames a Step inside a closed stage by the stage card', async () => {
    const { flow, groups, scene } = await collapsed();
    const stage = scene.boxes.find((box) => box.kind === 'stage') as Box;
    const member = groups.find((group) => stageId(group.id) === stage.id)?.stepIds[0];
    const neighbours = framingNeighbours(flow, groups, scene, member as string);
    expect(neighbours.near.every((id) => scene.boxes.some((box) => box.id === id))).toBe(true);
    expect(neighbours.near).not.toContain(stage.id);
  });
});

describe('entrance', () => {
  it('reaches each card along the lines from the start, and draws each line after its card', async () => {
    const { scene } = await expanded();
    const delays = entranceDelays(scene, 'tb');
    const start = scene.boxes.find((box) => box.isStart) as Box;
    expect(delays.get(start.id)).toBe(0);
    for (const link of scene.links) {
      expect(delays.get(link.id) as number).toBeGreaterThan(delays.get(link.from) as number);
    }
    expect(Math.max(...scene.boxes.map((box) => delays.get(box.id) as number))).toBeLessThanOrEqual(440);
  });
});

function inRoom(box: Box, view: CameraView, room: ReturnType<typeof cameraRoom>): boolean {
  const left = box.x * view.zoom + view.x;
  const top = box.y * view.zoom + view.y;
  const slack = 0.5;
  return (
    left >= room.left - slack &&
    top >= room.top - slack &&
    left + box.w * view.zoom <= room.left + room.width + slack &&
    top + box.h * view.zoom <= room.top + room.height + slack
  );
}

function mainSteps(flow: PocFlow, scene: Scene): Box[] {
  const hidden = failureHandling(flow);
  return scene.boxes.filter((box) => box.kind === 'step' && !hidden.has(box.id) && joinedSteps(flow, box.id).length > 0);
}

function refund(): { flow: PocFlow; groups: StepGroup[] } {
  const graph = refundGraph as unknown as FlowDefinitionGraph;
  const flow = safeDecode(graph, { generated: true, note: 'customer-refund-agentic' });
  return { flow, groups: groupsFromDefinition(graph as never, flow) };
}

async function expanded(): Promise<{ flow: PocFlow; groups: StepGroup[]; scene: Scene }> {
  const { flow, groups } = refund();
  const open = new Set(groups.map((group) => group.id));
  return { flow, groups, scene: await controlTopologyView.layout(flow, { direction: 'tb', run: null, groups, openStages: open }) };
}

async function collapsed(): Promise<{ flow: PocFlow; groups: StepGroup[]; scene: Scene }> {
  const { flow, groups } = refund();
  return { flow, groups, scene: await controlTopologyView.layout(flow, { direction: 'tb', run: null, groups, openStages: new Set() }) };
}
