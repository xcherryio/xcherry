# Flow Definition renderer

This private package owns both Flow Definition Graph TypeScript contracts and
renderers. `FlowDefinitionGraphView` renders Version 1 with its compound layout.
`ProcessCanvasView` renders Version 2 with ordered group bands, collapsed or
expanded Step cards, top-down or left-right layout, and selection details for
product documentation. Dex Web draws Version 2 with its own **v2** canvas
(`web/app/v2/canvas`) and imports this package for Version 1 and the graph
types.

Consumers must provide React 19, React Flow, and Dagre. The repository uses a
local file dependency and preserves symlinks in Vite, Docusaurus, and
TypeScript so each consumer resolves its own locked dependency versions.

Version 2 display fields expose optional `uiSlot` placement. Action nodes expose
the `requiredPermission` projected by the Go SDK for Work Queue searches.

```tsx
import {
  FlowDefinitionGraphView,
  type FlowDefinitionGraph,
} from '@superdurable/flow-definition-renderer';

<FlowDefinitionGraphView displayName={graph.flow.name} graph={graph} />
```

The renderer fits the full definition into the viewport by default. Channel,
Attribute, and Stream relations remain hidden until their resource or a related
Step, WaitFor, Execute decision, RPC, or timeout handler is selected. The RPC
legend control affects only RPC nodes; Flow timeout handlers always remain
visible as part of the Flow.

Both renderers accept optional `subFlowLinks`, mapping a SubFlow node ID to its
child definition URL. Without a matching URL the child name remains plain text.
Wait descriptions show each SubFlow's zero-based source argument index.
