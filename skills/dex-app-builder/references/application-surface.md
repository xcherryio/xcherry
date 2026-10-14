# Stage 2: establish the confirmed application surface

Read [UI workflow](ui-workflow.md), then use exactly one mode.
When Dex Web covers process management and no separate participant UI is
requested, select No custom UI and continue without a UI-choice question. Keep
the business contract review proportional to the application.
A request for an admin portal, dashboard, or management backend is not itself a
reason to select Custom UI. Select it only when the completed management UI
capability mapping records a specific interaction Dex Web v2 cannot provide.

### No custom UI

Use Dex Web v2 as the only process-management UI: `dexcli dev` during local
development, and the Dex Web embedded in Dex Server behind the deployment's
authenticated proxy. Define indexed Attributes, `GetDexSummary`,
`GetDexDisplay`, editable fields, human-action Steps, and Action RPCs with
conditions and permissions so Run and Work Queue modes cover the confirmed
process.

The application needs only its Go Worker in this mode. Do not add an
application HTTP server, frontend, or generated client unless a confirmed
integration ingress requires one. When an existing repository already contains
a non-business frontend shell, keep it non-business rather than growing it into
a management surface.

Do not build any custom process-management operation or surface: approval,
rejection, retry, escalation, status, display, list, search, detail, Action or
Attribute proxies, dashboards, forms, queues, lifecycle mock state, or mock
controls. Remove such scaffolding when it exists only as unused sample code. Do
not keep speculative endpoints.

When the process needs a trigger webhook, retain only that HTTP operation and the verification and correlation code it requires. Do not turn the webhook server into a second management backend. External-provider webhooks use a dedicated Connector Trigger; the generic HTTP connector is internal-only.

Do not run the custom-UI wireframe checkpoint in this mode. If the product
later needs custom UI behavior, enter the custom-UI workflow before
implementing it.

### Custom UI

Require the confirmed management UI capability mapping to name the unsupported
interaction and keep all covered management operations in Dex Web v2. Valid
gaps include a participant portal, a fundamentally different navigation model,
or a complex interaction Dex Web does not provide. “The project needs a
backend” is not a capability gap.

If a material interaction choice is unresolved, create a low-fidelity static
wireframe in the application's frontend to clarify that choice. A new frontend
defaults to a React/Vite application in its own directory. A clear request
already authorizes its stated page, fields and behavior; implement it directly
without requiring this optional checkpoint.
Represent the necessary pages with headings, labels, placeholder boxes, inputs,
buttons, and ordinary links. Multiple pages must have stable, directly openable
local URLs and working link navigation.

The wireframe may use React for static markup, but it must not use application
state or effects, API or generated-client calls, local storage, timers, a mock
server, mock controls, provider integrations, images, custom icons, animation,
branding, or visual polish. Buttons and inputs remain inert unless a link is
needed to show the approved navigation path. Use minimal neutral styling and do
not model loading, validation, success, failure, or recovery behavior yet.

Run only the frontend development server, verify that each page URL renders,
and give the user a compact inventory of page names, purposes, and direct links.
Ask only about the unresolved page, field, action placement or navigation choice.
Do not turn this checkpoint into an aesthetic review or iterate on
visual design.
