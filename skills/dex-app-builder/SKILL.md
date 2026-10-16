---
name: dex-app-builder
description: Primary entry point for designing and building Dex applications and process products with open-source Dex. Use for Dex product, application, or workflow requests, including converting an exported n8n workflow, but not standalone SDK or official connector-library work. Orchestrates business discovery, Flow and FDG modeling, the Dex Web or custom UI decision, a Go backend with released connectors, and local verification with dexcli dev.
---

# Dex App Builder

Build the smallest coherent Dex product that satisfies the user's request. A
clear implementation request authorizes its stated UI and routine engineering
choices. Ask only for missing business facts, access or consequential choices;
do not turn discovery, design, wireframes or source checks into repeated gates.

The toolchain is open-source Dex: the Dex Go SDK, `dexcli` (including
`dexcli dev` and the FDG analyzer behind `dexcli visualize`), Dex Server, the
Dex Web embedded in Server and CLI, and released official connectors.

## Session start and routing

Follow the shared [version check](../dex-sdk/references/core/plugin-version-check.md)
once. Prefer the lifecycle hook; its unavailable fallback must not block work.
Confirm a writable workspace and file/command tools before claiming implementation.
Without them, provide discovery and a handoff for a repository-backed host.

- Standalone official Connector work belongs to [Connector Contributor](../dex-connector-contributor/SKILL.md).
- Standalone SDK implementation, debugging or operations belongs to [Dex SDK](../dex-sdk/SKILL.md).
- Product work stays here; read Dex SDK when implementing its backend. A missing
  released capability routes that contribution to Connector Contributor while
  preserving application ownership here.
- Honor an explicitly invoked specialist. Missing sibling entrypoints mean an
  incomplete installation; update the three-skill bundle instead of searching
  arbitrary caches or falling back to superseded skills.

## Source authority and efficient discovery

Inspect the existing manifests and source before selecting tools. Existing
projects keep their pinned Go, Dex SDK, `dexcli`, Server and frontend versions.
Read [workspace bootstrap](references/workspace-bootstrap.md) for an empty
repository, dependency changes or setup failures.

Read only references relevant to the next implementation decision, using their
headings to select a bounded span. Retain verified version/API facts and do not
reread whole guides after every tool call. Batch independent read-only requests
when the host supports it; discover filenames before reading module source.
Implement once the required types and lifecycle are understood. Let concrete
compiler, strict FDG or provider diagnostics drive further investigation.

For external products, search the [official catalog](https://superdurable.github.io/dex-connectors-library/catalog.yaml)
using a bounded catalog tool when available. Select a released operation and
kind, then inspect that exact module's `connector.yaml`, generated operation factory and
input/output types. Return only matching catalog entries to model context.
The catalog identifies capabilities and current releases; it is not provider
business data or a historical release feed. Do not guess APIs or substitute
low-level Connector SDK wire types for the released operation factory.
Read the relevant [Connector architecture](references/connector-architecture.md)
section for external integration, text generation, Trigger ingress or deployed
configuration. A missing module is resolved at its exact released version.

## Essential product constraints

- Go backend only; strict FDG 2.0 only. The FDG 2.0 analyzer and the
  application Connector SDK are Go-only.
- Dex Web owns Runs, Work Queue, Actions, edits, history and diagnostics:
  `dexcli dev` locally, the Dex Server's embedded Dex Web when deployed. Do not
  recreate management APIs or UI. A separate participant interaction is a valid
  custom UI; a backend requirement alone is not.
- Use typed Flow Attributes and exact-loaded AttributeMaps as the default
  authoritative store. Justify an external store with a concrete access/scale
  gap and one owner per fact. No speculative database, projection or cache.
- Use parallel Steps for one lifecycle, independent top-level Flows for separate
  owners, and no SubFlows unless the Core gate and explicit confirmation apply.
  Never split when the parts would exchange RPCs or messages on frequent events
  (every operation, callback or poll): each call between Flows adds to both
  histories and costs an engine operation, even when it only reads.
- Put provider effects in Execute, with identity, bounded timeout and recovery.
  WaitFor declares durable conditions and performs no provider or Dex mutation.
- Stable FlowID, complete RequestID, explicit reuse policy and typed error
  recovery govern starts. An accepted start returns known identity without a
  preflight or success-path state read. Streams are best-effort observations.
- Define each Flow's identity, lifecycle, Steps, typed RPCs, all primitives,
  selective loads, locks/CAS, retention, retries and uncertain-effect handling
  in the application before coding. Preserve open execution compatibility.
- Keep credentials outside code, Flow state, logs and definitions. Local
  development configures connections in Dex Web, and a Go application reads
  them through the Connector SDK's `sdkgo/projectconfig`, whose configuration a
  hosting platform or operator provides (Dex hosts none).
- For Dex Server authentication and Worker deployment, follow the SDK
  [operations](../dex-sdk/references/core/operations.md#public-api-authentication)
  and [pull Worker](../dex-sdk/references/core/pull-workers.md) references.
- Browser permission selection grants no authority. Enforce actor permissions
  at the trusted server boundary; use stable granular Action permissions.
- Do not mutate another repository or publish/deploy without authorization.

## Read the matching implementation reference

| Decision | Reference and scope |
| --- | --- |
| Empty repository, stack, dependencies or local tooling | [Workspace bootstrap](references/workspace-bootstrap.md) |
| Exported workflow from another tool, such as n8n JSON | [Workflow import](references/workflow-import.md): fidelity ledger, secrets first, Dex redesign proposals with one optimize-or-exact question, golden parity, then the normal stages |
| Missing business requirements, actors or ownership | [Business contract](references/business-contract.md); record concise decisions instead of requesting already supplied facts |
| Participant/custom UI or management coverage | [Application surface](references/application-surface.md) and [UI workflow](references/ui-workflow.md); clear requests need no extra static-wireframe approval |
| Go Flow rendering, Summary/Display or Work Queue Actions | [FDG authoring](references/fdg-authoring.md): exact syntax and focused diagnostics |
| Flow, persistence, RPC, Connector or HTTP API changes | [Backend implementation](references/backend-implementation.md), selecting the relevant headings; use the Dex SDK Core/Go routing rather than loading all references |
| Local verification, FDG packaging or deployment | [Verification and handoff](references/verification-handoff.md); distinguish source readiness from configured real execution |

Keep the design proportional and in the application's owning documentation.
Do not add application-specific tests, mocks or framework dependencies merely
to satisfy a generic scaffold. Existing/requested real acceptance remains
required. Generate API packages before resolving their imports.
The host commit tool may own the final complete source gate; use focused checks
while editing, then consume its actual diagnostics rather than running the same
full gate immediately twice. Never report compilation as provider/E2E success.
