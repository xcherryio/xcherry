// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import type { FlowDefinitionGraph } from '@superdurable/flow-definition-renderer';
import type { PocFlow } from './canvas/model/pocFlow';
import type { StepGroup } from './canvas/views/groups';

export function groupsFromDefinition(graph: FlowDefinitionGraph, flow: PocFlow): StepGroup[] {
  return (graph.groups ?? []).map((group) => {
    const members = group.stepIds.flatMap((stepId) => {
      const step = flow.steps.find((candidate) => candidate.id === stepId);
      return step === undefined ? [] : [step];
    });
    return {
      id: group.id,
      label: group.label,
      reason: group.label,
      stepTypes: members.map((step) => step.stepType),
      stepIds: members.map((step) => step.id),
    };
  });
}
