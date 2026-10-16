# Build, test, and handoff

For write/read boundaries, follow the shared [SDK consistency matrix](../../dex-sdk/references/core/read-after-write.md). Assert direct-state RPC reads on the first read; confirm triggered business work separately. Do not add callbacks or polling to strong write/read pairs.

## Implementation loop

Use the project's own commands. For a Custom UI, change the API contract source,
then regenerate; never hand-edit generated Go or TypeScript clients. Follow the
project's convention for generated output: when it is a build output, keep it
ignored and never stage, commit, or include it in a pull request.

When a repository contains starter or sample Flows, replace that scaffold for
the first business feature rather than retaining it alongside the requested
Flow. Remove unused sample Steps, RPCs and registrations. Preserve unrelated
business Flows in imported or already-developed applications.

In a fresh checkout, restore locked dependencies (Go modules and, for a
frontend, `npm ci` or the project's equivalent) before generation or checks. A
missing generator executable requires dependency restoration, not repeated
failing builds or a package upgrade. Reuse the installed dependencies until
their manifests or lockfiles change.

Generate API packages before resolving their imports with `go mod tidy`.
When the host commit tool owns the full source gate, use focused checks during
editing and avoid repeating that full gate immediately before handoff. The
source gate regenerates any API contracts, renders and validates every strict
FDG 2.0 definition, runs `gofmt`, `go vet` and `go build`, and builds the
production backend and any frontend. Keep it as one project command; do not
invent a test target the project does not have.

A source handoff and real business acceptance are separate milestones. Default
applications do not add integration/browser test suites, mocks, fixtures, or
test-framework dependencies.
Generate application tests only when the user explicitly requests them.
Preserve unrelated tests in imported applications.

Source that is complete and checked can be handed off without a private
Connector key or an isolated test stack.
Missing configuration does not make otherwise complete source blocked.
Record real execution as pending and
complete the requested journey once configuration exists; never call
compilation a provider or deployment success. An explicitly requested executed
test remains incomplete until its real dependencies pass. Repair actual
source-gate failures before handoff. This default does not remove
Connector-library verification.

Keep the application's `go.mod`, `go.sum`, frontend lockfile, Go toolchain,
`dexcli` expectation, and generators unchanged unless the user explicitly
requested the corresponding stack or dependency change. A newer `DEX_BASELINE`
in the skill repository is not an application upgrade instruction.

For **No custom UI**, do not add an application API, frontend, or generated
client beyond confirmed integration ingress, and remove process-management
routes, mock-server routes, mock lifecycle code, mock controls, mock launch
scripts, and mock E2E. Verify the process through Dex Web.

For **Custom UI** with unresolved consequential interaction choices, run the
frontend development server for the optional low-fidelity static
checkpoint and verify only that each direct page URL and navigation path renders.
Do not introduce a mock server, model lifecycle states, generate visual assets,
or polish the surface before the page inventory is confirmed.
The approved discovery artifact must name the Dex Web v2 capability gap that
requires each custom management surface. Keep management operations covered by
Summary RPC, Display RPC, Action RPC, Indexed Attributes, Work Queue, editable
fields, timeline, or graph inspection out of the custom backend and UI.

During Flow and Connector design, finalize the API contract, regenerate, and
implement the generated server interfaces before wiring the generated
TypeScript client into the UI. Run real Dex and Connector journeys
before visual polish; this does not require generating an application test suite. For
explicitly requested tests, follow a real-dependency policy: no unit tests,
component mocks, fake providers, or intercepted API responses. Trigger cases
through actual APIs, or record the unverified invariant. Check both production
artifacts after an API change; a successful Go compile does not prove that the
frontend's generated imports still exist. Run relevant source checks after each
edit batch and keep the requested real acceptance result separate from source
readiness.

Before installing a database, cache, ORM, or separate read model, review the
storage decision matrix and name the exact query, concurrency, transaction, or
analytics requirement that Dex Attributes, AttributeMaps, indexes/search,
typed RPCs, partitioning/chunking, blob storage, and Attribute Store projection
cannot satisfy. Remove an unneeded dependency when Dex meets the requirement.
When external storage is justified, test its declared authority boundary,
projection/synchronization lag, retry behavior, outage handling, and
reconciliation instead of treating a successful happy-path write as proof. For a
`rowKey` store, also test the Flow ID to key-column mapping and an ID that does
not match its template.

Only actual dependency calls establish Dex durability, Worker replacement,
Timer, RPC, retry, provider or application E2E behavior.

Show Dex Web early. Once the first Flow graph renders, keep one user-facing `dexcli dev` stack running with a persistent `--flow-rendering-dir`, stable ports, and persistent state, and give the user its URL before continuing. Continue implementation and source checks; use isolated test stacks only for explicitly requested automated tests, and never let them reuse or stop the user's stack. Do not wait until verification passes to start it. Report its URL again at handoff.

## Local CLI

Run `dexcli version` before rendering and follow the
[version pairing](workspace-bootstrap.md#version-pairing) rule, so the rendered
Flow and Step type names match the Worker. Do not add or update a project-local
skill submodule; the coding-agent host supplies the Dex Skills release.

Validate every Flow file, not only the one being edited; see [Dex Web v2 validation](dex-web-v2.md#validation).

## Durable verification

This section guides requested runtime acceptance, not a requirement to generate
test scaffolding in every application. Use the local `dexcli dev` stack and the
real Worker. SDK and Connector contributors retain their own verification
requirements. Use a real Dex Server when behavior crosses a Client, Worker,
wait, RPC, Channel, Timer, Stream, retry, provider, or process boundary.

Cover:

- start and typed terminal output;
- duplicate start/request behavior and [StartFlow-first ordering](../../dex-sdk/references/core/testing.md#startflow-ordering-and-retry-identity), with reconciliation reads confined to relevant error branches;
- durable wait and Worker replacement;
- when pull routing is selected, no inbound Worker listener, startup after an unavailable dispatch, sticky fallback to a peer, at-least-once idempotency, Step retry consumption, heartbeat timeout and cancellation, and bounded Stop drain;
- when Dex authentication is enabled, Client, Worker, CLI, and Dex Web credentials, token rotation, rejected missing credentials, and protection of the Web port;
- accepted-start responses without defensive identity/Attribute rereads, and [durable downstream start recovery](../../dex-sdk/references/core/testing.md#downstream-start-recovery) with no dependent API-side second start;
- Action eligibility, valid action, duplicate/late action, and terminal rejection;
- role-to-permission mapping, unauthorized Action rejection, and multi-permission work discovery at the application boundary;
- concurrent Action-source writes without projection-only locks and cumulative permission history after state changes and completion;
- retry and exhausted-recovery behavior;
- provider idempotency and unknown-outcome reconciliation;
- summary/display reads before, during, and after terminal completion;
- terminal-readable entity snapshots through typed `Get*` RPCs after closure, following the SDK's [terminal entity read scenario](../../dex-sdk/references/core/testing.md#terminal-entity-reads) and [terminal read RPC rule](../../dex-sdk/references/core/error-handling.md#terminal-read-rpc-rule);
- connector Trigger Flow/RPC routing and correlation when used.
- RPC-to-Connector-Step routing when an application RPC requests provider work;
- application integration against any uncommitted local connector `replace`,
  plus the matching connector module's provider and real-Dex tests.

For a No custom UI application, also assert that no approval, display, status,
list, search, detail, retry, escalation, Action-proxy, or Attribute-proxy HTTP
route remains. Test each retained webhook independently from Dex Web management.

Use deadline-based polling and report Flow IDs and status on failure. Do not hide or skip a failing check.

## Handoff

Ensure:

- application dependencies, the Go toolchain, lockfiles, `dexcli` pairing, and
  commands match the recorded stack unless an explicit user-approved deviation
  is recorded;
- every top-level Flow is justified by its authoritative owner,
  retention/cleanup, independent waits or Timers, and terminal lifecycle;
- no two Flows exchange RPCs or messages on frequent events, such as every
  operation, callback or poll;
- parallel work remains in Steps by default, and every SubFlow has concrete
  evolution evidence, rejected Step/batching/RPC alternatives, a defined
  parent-child lifecycle, and explicit user confirmation;
- temporary validation, failure, and expiry state cannot pollute a long-lived
  authoritative Attribute or AttributeMap owner;
- the authorization model starts with `admin`, keeps Action permissions
  granular, and justifies every additional role with a distinct authenticated
  membership and visibility or operation boundary;
- the confirmed UI mode is recorded;
- the management UI capability mapping was completed before the UI-mode
  decision and designs Summary, Display, Action, Indexed Attribute, Work Queue,
  and permission coverage;
- every Custom UI surface names a specific remaining Dex Web v2 capability gap;
- a No custom UI application exposes no business controls or management routes;
- a Custom UI has an approved static page inventory, navigation, fields, and
  actions before backend work, with no early visual-polish artifacts;
- its API contract was designed with the Flow and Connector boundaries,
  both generated clients and production bundles reproduce locally, generated
  output is never hand-edited, and the Go HTTP boundary implements the
  generated server interfaces;
- its dynamic UI uses only the generated TypeScript client, and visual polish
  followed a passing real Dex and
  Connector end-to-end journey;
- secrets are absent from files, logs, generated values, and archives;
- dependencies and released connector versions are pinned;
- every connector capability matrix records the canonical published catalog,
  exact capability name and kind, component tag, and immutable manifest URL;
- every public-provider boundary reuses an available released connector
  capability or records the contributor PR and release blocker;
- every internal-service boundary records the existing/new internal connector
  library decision or the reason generic HTTP remains appropriate;
- each configurable Connector Step has a static connection name matching its generated Connection;
- each Connector Step branch receives only its current operation result, while application Attributes retain domain context;
- every Connector factory uses pure `MapToOperationInput` and graph-only `Annotations`;
- each Connector Trigger binding has a static binding name, application-owned Flow ID resolver, and typed target;
- for local configuration, the application starts with the configuration its
  pinned connector release reads (the displayed **DEX_CONNECTOR_CONFIG_FILE**
  launch command; `sdkgo` v0.28.0 or later for v0.21.0 or later connectors)
  and survives a Dex Web restart, or the gap is an explicit blocker;
- every Flow source file renders to a `valid: true` FDG 2.0 definition through
  one project command, and the rendered Flow and Step type names match the
  Worker's registrations;
- for a deployment, the packaged definitions and the project configuration that
  `projectconfig.LoadFromEnvironment` loads keep credentials out of application
  code and Flow state;
- for a deployment using pull Workers, Matching remains available, streams fit the configured limits, the app Worker ID changes after restart, and Stop drains within the configured deadline;
- for a Dex Server upgrade, removed multi-tenant YAML is migrated to `auth`, `internal`, and `worker.wake` before startup, with single-tenant data preserved;
- requested acceptance for known token expiry, concurrent calls, credential rotation,
  lost exchange responses and Worker restart records actual results or explicit
  gaps, without recreating the released Connector's test suite in the app.
  Retries join an admitted refresh, and an unclassified 401 refreshes only after
  the stored expiry has elapsed;
- connector fork/PR status and any release blocker are explicit;
- no `go.work`, local `replace`, branch, pseudo-version, or commit SHA remains
  in the production dependency graph;
- the repository has a clean, reviewable commit;
- limitations and unimplemented integrations are explicit.
