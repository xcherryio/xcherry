// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Super Durable Source License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Super-Durable-1.0

import {useCallback, useMemo, useState} from 'react';
import type {FlowDefinitionGraph} from '@superdurable/flow-definition-renderer';
import {safeDecode} from '@dex-web-v2/canvas/model/decode';
import {ArrowDefs} from '@dex-web-v2/canvas/render/ArrowDefs';
import {Stage} from '@dex-web-v2/canvas/render/Stage';
import {useStageScene} from '@dex-web-v2/canvas/useStageScene';
import {stageId} from '@dex-web-v2/canvas/views/stageCard';
import {stepTitle} from '@dex-web-v2/canvas/views/stepBox';
import type {Direction} from '@dex-web-v2/canvas/views/types';
import {Controls} from '@dex-web-v2/flow/Controls';
import {Legend} from '@dex-web-v2/flow/Legend';
import {groupsFromDefinition} from '@dex-web-v2/groupsFromGraph';
import '@dex-web-v2/css/tokens.css';
import '@dex-web-v2/css/canvas.css';

/** A Version 2.0 definition drawn with Dex Web's Flow canvas, without a run. */
export default function DocsFlowCanvas({graph}: {graph: FlowDefinitionGraph}) {
  const flow = useMemo(() => safeDecode(graph, {generated: true, note: graph.flow.name}), [graph]);
  const groups = useMemo(() => groupsFromDefinition(graph, flow), [flow, graph]);
  const [direction, setDirection] = useState<Direction>('tb');
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [selectedGroupId, setSelectedGroupId] = useState<string | null>(null);
  const [isLegendOpen, setLegendOpen] = useState(false);
  const stages = useStageScene({flow, groups, direction, run: null});
  const {scene, collapseStage, openStage: openStageInPlace} = stages;
  const openStage = useCallback(
    (boxId: string) => {
      openStageInPlace(boxId);
      setSelectedGroupId(groups.find((group) => stageId(group.id) === boxId)?.id ?? null);
      setSelectedId(null);
    },
    [groups, openStageInPlace],
  );
  const selected = flow.steps.find((step) => step.id === selectedId);
  const destinations = selected
    ? [...new Set(flow.transitions.filter((t) => t.fromStepId === selected.id && !t.isSelfLoop).map((t) => t.toStepId))]
        .map((id) => flow.steps.find((step) => step.id === id))
        .flatMap((step) => (step ? [stepTitle(step)] : []))
    : [];

  return (
    <div className="v2-shell docs-flow-canvas">
      <div className="pcanvas">
        <ArrowDefs />
        <div className="pctlbar">
          <Controls
            stages={stages.hasStages ? stages.state : null}
            direction={direction}
            onCollapseAll={stages.collapseAll}
            onExpandAll={stages.expandAll}
            onDirection={setDirection}
          />
        </div>
        {scene ? (
          <Stage
            scene={scene}
            direction={direction}
            selectedId={selectedId}
            selectedGroupId={selectedGroupId}
            dimUnrelated={selectedId !== null}
            cue={stages.cue}
            onSelect={(id) => {
              setSelectedId(id);
              setSelectedGroupId(null);
            }}
            onSelectGroup={(id) => {
              setSelectedGroupId(id);
              setSelectedId(null);
            }}
            onOpenStage={openStage}
            onCloseStage={(groupId) => {
              collapseStage(groupId);
              setSelectedGroupId(null);
            }}
            legend={() => (
              <div className="plegend-card" data-open={isLegendOpen ? 'true' : undefined}>
                <button
                  type="button"
                  className="plegend-toggle"
                  onClick={() => setLegendOpen((open) => !open)}
                  aria-expanded={isLegendOpen}
                >
                  Legend {isLegendOpen ? '▾' : '▸'}
                </button>
                {isLegendOpen ? (
                  <>
                    <Legend scene={scene} flow={flow} />
                    {scene.notes.length > 0 ? (
                      <div className="plegend-notes">
                        {scene.notes.map((note) => (
                          <p key={note}>{note}</p>
                        ))}
                      </div>
                    ) : null}
                  </>
                ) : null}
              </div>
            )}
            fitKey={`${graph.flow.name}|${stages.view}`}
          />
        ) : (
          <div className="v2-empty">Drawing the Flow…</div>
        )}
      </div>
      {selected ? (
        <div className="docs-flow-canvas-selection">
          <strong>{stepTitle(selected)}</strong>
          <code>{selected.stepType}</code>
          {selected.explanation ? <p>{selected.explanation}</p> : null}
          <p>{selected.waitFor ? `Waits for ${selected.waitFor.sentence}` : 'No WaitFor phase'}</p>
          <p>{destinations.length > 0 ? `Can proceed to ${destinations.join(', ')}` : 'No outgoing Step transition'}</p>
        </div>
      ) : null}
      <p className="sr-only" aria-live="polite">
        {stages.cue?.message}
      </p>
    </div>
  );
}
