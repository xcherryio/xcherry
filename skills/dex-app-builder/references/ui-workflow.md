# Application surface workflow

Use this only after discovery completes the management UI capability mapping.
Design Summary RPC, Display RPC, Action RPC, Indexed Attribute, Work Queue, and
permission coverage before selecting a mode. Needing an admin or management
interface does not by itself justify Custom UI; that mode requires a recorded
Dex Web v2 capability gap.

## No custom UI

Dex Web owns process management: `dexcli dev` locally and the Dex Server's
embedded Dex Web when deployed. With no separate participant UI requested,
select this mode without a redundant question. The application ships its Go
Worker and, only for confirmed integration ingress, a minimal HTTP server.

Do not add process state views, management operations, Action/Attribute proxy
endpoints, dashboards, forms, mock lifecycle state, mock controls, fixtures, or
related tests. No application page may present approval, display, status,
list, search, detail, retry, or escalation controls.

Retain a webhook only when it is confirmed integration ingress. An external
provider webhook belongs to its dedicated Connector Trigger. An internal
system may use the generic HTTP webhook connector.

Verify through Dex Web that Runs, Summary and Display fields, Work Queue
discovery, and Actions cover the confirmed process, and that no management
route exists in the application. If custom behavior is requested later, follow
the workflow below before connecting it to production.

## Custom UI

Enter this mode only for the unsupported interaction named in the confirmed
management UI capability mapping. Keep operations already covered by Dex Web v2
on its native Run, Work Queue, detail, Action, edit, timeline, and graph
surfaces.

### 1. Resolve material interaction choices

A clear implementation request already authorizes its stated page, fields and
behavior. Implement those directly. Use the optional static checkpoint below
only when a consequential interaction choice remains unresolved; ask about that
choice, without requiring another approval of already supplied requirements.

Agree only on the information architecture needed to unblock backend design:

- role-specific entry pages and ordinary link navigation;
- necessary list, detail, create, action, and completion pages;
- labels, inputs, buttons, and placeholder regions on each page;
- which Dex Web v2 surfaces remain available to maintainers.

Implement those pages as static markup in the application's frontend; a new
frontend defaults to React/Vite. React remains the rendering shell, but this
checkpoint has no hooks or application state, API imports, generated-client
calls, local storage, timers, mock lifecycle, mock controls, or provider
behavior. Use no images, generated art, custom icons, animation, branding,
gradients, or decorative effects. Keep CSS neutral and minimal. Inputs and
buttons are inert; use ordinary links or link-styled buttons only to
demonstrate navigation between directly openable page URLs.

Run the frontend development server on a stable local port and verify that every
page URL renders. Give the user a page inventory with each name, purpose, and
direct URL. Ask about the unresolved page set, navigation, fields or actions.
Do not ask the user to approve colors, typography, imagery,
responsive refinements, dynamic states, or production behavior at this point.

### 2. Contract and backend design

Once interaction requirements are known, design the Flow, Connector
capabilities, and application HTTP API contract together. Map every approved UI
action to an application operation backed by a Flow start, typed RPC, query, or
confirmed ingress. Define authentication and permission enforcement,
idempotency, asynchronous status, validation, response and error shapes, retry,
and terminal outcomes.

The API contract describes the application's business boundary. It must not expose Dex
Steps, Channels, Attributes, connection credentials, or other runtime internals.
Write it as an OpenAPI document (or the project's existing contract format) and
generate the Go server interfaces and TypeScript client from it before
implementing HTTP handlers. The Go backend implements the generated server
interfaces, and the browser later consumes the generated TypeScript client. Do
not hand-write parallel transport types or hand-edit generated code; follow
the project's convention for whether generated output is committed or
regenerated as a build step.

Implement and verify the Go Flows and required Connectors before returning to
dynamic frontend work. A missing public Connector capability follows the
Connector Contributor workflow and remains subject to its authorization and
release requirements.

### 3. Integration and durable verification

Implement the Go boundary and UI against the same generated API contract.
Replace any wireframe's inert controls with generated TypeScript client calls.
Missing private credentials defer real execution, not source implementation.
Add the confirmed loading, validation, empty, success, failure, retry, recovery,
and terminal behavior. The browser calls the application API; it never imports
a Dex client or accesses raw Dex primitives.

Follow a real-dependency test policy. Exercise loading, validation,
failure, retry and terminal states through real APIs; when a case cannot be
executed, record its coverage gap. Do not add component mocks, intercepted API
responses, fake providers, an application-level mock API or mock controls.
Prove waits, RPCs, retries, Worker replacement, provider effects and terminal
behavior through the actual Dex and Connector end-to-end path on a local
`dexcli dev` stack.

Do not claim application-level UI controls provide authorization. A permission
selector filters work; it does not grant permission. Enforce identity-to-
permission mapping at the trusted application boundary. Keep credentials and
provider secrets server-side.

### 4. Visual polish

Only after the real end-to-end journey passes may the UI add imagery, custom
icons, branding, animation, refined typography, decorative styling, and
responsive fine-tuning. Preserve the generated client boundary and confirmed
behavior. Then rerun the real end-to-end journey,
accessibility checks, and the production build.
