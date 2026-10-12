# Flow Definition renderer

This private package owns both Flow Definition Graph TypeScript contracts and
the Version 1 renderer. `FlowDefinitionGraphView` renders Version 1 with its
compound layout. Version 2 is drawn by Dex Web's **v2** canvas
(`web/app/v2/canvas`), which product documentation imports from source, so Dex
Web and the docs draw a Version 2 definition the same way.

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

The renderer accepts optional `subFlowLinks`, mapping a SubFlow node ID to its
child definition URL. Without a matching URL the child name remains plain text.
Wait descriptions show each SubFlow's zero-based source argument index.
