# Changelog

## 0.41.0 - 2026-10-21

- Advance Go, Server, and CLI baselines to their published Dex 1.13.0 tags. Pin Java, Python, TypeScript, and Rust references and examples to their own latest published SDK v1.12.0 tags, with per-language baseline metadata and source validation.
- Document Go pull Worker startup gating with WaitPullRegistered, stable PullRegistrationState probes, PullRegistrationError, and opt-in StopOnPermanentStreamRejection. Distinguish surplus-stream rejection from a rejected queue with no other registered stream, preserve readiness through reconnects, and remove stopping Workers from service separately. Add deployment and verification guidance to Core and App Builder.
- State the readiness API limitation in the other four SDK handbooks and the process-health and rejection-log workaround. Document Server 1.13.0 ResourceExhausted preservation, available retry hints, bounded backoff, and mutation reconciliation. Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.41.0.

## 0.40.0 - 2026-10-17

- Advance the SDK, Server, and CLI baselines to the published Dex 1.12.0 tags and refresh every pinned Dex source link. The example dependency manifests at those tags select SDK 1.11.0; application lockfiles remain authoritative.
- Document pull Worker routing in the core and all five language references: immediate Flow type fallback when the prior app Worker ID has no connected streams; capability-gated rejection of an unregistered Step type or RPC name before application code; the 10-second regular and RPC wait, 3-second local Step wait, named Step failure, named RPC FailedPrecondition, and rolling-deployment compatibility rule. Add corresponding production and real-Server integration scenarios.
- Preserve SubFlow start rejection detail from WaitFor in error handling, and update App Builder guidance for pull Worker deployments and Dex Web v2 Start Flow targets, including the pull target default and request fields. Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.40.0.

## 0.39.0 - 2026-10-16

- Advance the SDK, Server, and CLI baselines to the published Dex 1.11.0 tags and refresh pinned Dex source links, including the Go pull Worker example's new `production/pull-workers` path. The example manifests at those tags select SDK 1.10.1; application lockfiles remain authoritative.
- Add hard-delete Attribute guidance to the core references and all five language references: short-lived secrets kept out of Flow history and permanently deleted on delete or null, string and object values only, no index, Attribute Store sync, WaitForAttributeMatch, or Action conditions, the Server v1.11.0 and Blob Store requirement, fail-closed SDK behavior against older Servers, matching initial-value definitions, inline delivery without BlobCache, and every limitation of the one in-place stored value per Flow and key. Each language reference shows the definition and deletion from the pinned SDK README or integration test.
- Note hard-delete visibility in read-after-write consistency, the WaitForAttributeMatch restriction in the revision-watermark pattern, and Blob Store backup and versioning retention in operations.
- Update Python credential guidance for SDK 1.11.0: `async def` tokens on the event loop, the SDK-owned executor for synchronous tokens, unchanged Client exceptions, and Worker stop cancelling a pending lookup.
- Update App Builder's storage, credential, and FDG guidance for hard-delete secrets. Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.39.0.

## 0.38.0 - 2026-10-15

- Advance the SDK, Server, and CLI baselines to the published Dex 1.10.1 tags and refresh pinned Dex source links. The example manifests at those tags still select SDK 1.10.0; application lockfiles remain authoritative.
- Add pull Worker guidance for all five SDKs: Worker options and targets, Client defaults and Flow overrides, process-unique app Worker IDs, sticky fallback, unavailable Step retries, at-least-once delivery, one-attempt ASYNC local execution, heartbeat timeout and cancellation, stream capacity, graceful Stop, and the wake hook.
- Document public API authentication, SDK bearer credentials, CLI credential-file precedence, Dex Web's FlowService credential and separate Web port protection, the private internal listener, Matching and membership settings, and migration from removed multi-tenant configuration while preserving single-tenant data.
- Update App Builder's Worker and access topology, deployment checks, and Dex Web guidance. Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.38.0.

## 0.37.1 - 2026-10-15

- Replace the plugin logo (`assets/logo.png`) with the Dex mark, the pipe operator |> set as a D. There is no skill content change.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.37.1.

## 0.37.0 - 2026-10-14

- Move Dex Skills development and validation to superdurable/dex `skills/`. The dex-skills repository is a read-only mirror and remains the plugin installation and release source. Remove the mirror pull request template and its obsolete package check.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.37.0.

## 0.36.0 - RELEASE_DATE

- Describe Step group shapes in the Go FDG authoring reference. A group of two or more Steps does one thing: one run, where ordinary transitions inside the group join all of its Steps, or one choice, where one Step outside the group leads into every part, such as an agent's decision Step and its tools. Dex Web folds only such a group into a stage that opens in place and shows any other group's Steps in its band; the analyzer warns about it with `v2_step_group_shape`, which names the parts and never makes a definition invalid. The diagnostics table gains the row.
- Describe failure handling as Dex Web draws it. A Step that only failure transitions reach, directly or through other such Steps, is failure handling: Dex Web leaves it out of the drawing, counts the failures on each Step card they leave, and draws them beside that Step when it is selected or a run went through them. Groups are judged on the main story, so a failure transition joins no group and a failure-handling Step shares the group of the Step it handles. A Step that only records breakage, such as exhausted retries or an uncertain outcome, belongs on the failing Step's Execute failure route (`ProceedToOnExecuteFailure`); business rejections and connector outcome branches stay ordinary decisions. The Dex Web v2 reference names the canvas among the surfaces that groups drive.
- Move `DEX_CLI_BASELINE` and `DEX_SERVER_BASELINE` to the release that contains superdurable/dex#629 (CLI_TAG and SERVER_TAG), and point the FDG authoring links at it. The baseline check requires the analyzer's `v2_step_group_shape`.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.36.0.

## 0.35.1 - 2026-10-12

- Replace the local Connector configuration blocker. With Connector SDK `sdkgo` v0.28.0 or later, `projectconfig.LoadFromEnvironment` reads the Dex Web `connections.json` and `use-configurations.json` when **DEX_CONNECTOR_CONFIG_FILE** names the file, so a connector at v0.21.0 or later works with the launch command Dex Web displays and needs no `DEX_PROJECT_*` variables or object storage (dex-connectors-library#230). Settings, Trigger bindings, and operation settings are read at startup and need an application restart to change; reauthorizing in Dex Web does not. A refreshed credential is written back to the file and Trigger inboxes persist beside it, so run one application per directory. An application that pins an older `sdkgo` is still reported as a compatibility blocker. Core operations, Connector architecture, the verification and build handoffs, and the Go handbook state this.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.35.1.

## 0.35.0 - 2026-10-11

- Document the three optional settings Dex Server v1.6.0 adds to PostgreSQL and MySQL Attribute Store entries. Core data handling gains a section on when to use each: `rowKey` splits a Flow ID into the table's primary-key columns and so makes the Flow ID format a storage contract (placeholder rules, a non-matching Flow ID skips its batch, a primary key of exactly the template's columns, several stores writing different columns of one row), `columnNaming: snake_case` maps Attribute and instance field names to snake_case columns, and `attributeMaps` writes each AttributeMap instance as one row of its own table (a write replaces the row and a delete removes it, a non-object value goes to the `value` column, fields without a column are skipped with a warning, one transaction per batch, no foreign keys, and no row cleanup when a Flow closes). Databricks, Snowflake, and MongoDB stores, and any Server before v1.6.0, reject the new keys at startup.
- Correct the Core list of built-in targets, which named only PostgreSQL and MySQL, to the five Server destinations, and replace the note that an AttributeMap never becomes rows.
- Add an Attribute Store destinations section to Core operations (create destinations first, startup and schema-refresh validation, upgrade every Server before adding the settings, what to watch in the Server log), and update Core modeling, patterns, read-after-write, and testing: the Flow ID format belongs to the projection contract, a Flow ID that does not match a `rowKey` template never converges, and tests cover key columns, unmatched IDs, and instance replace and delete against a real table.
- State in each language data-handling reference which serialization names the columns of an AttributeMap table (Go `json` tags or exported field names, Python dataclass field names, Java Jackson property names, TypeScript `jsonCodec` object keys, Rust serde names), that a non-object value goes to the `value` column, that the instance key is stored as written, and that deleting an instance deletes its row while deleting a plain opted-in Attribute still writes `NULL`. There is no SDK API change.
- Ask App Builder discovery to record the Flow ID format and any `rowKey` template when it chooses an Attribute Store projection, and its build handoff to test the Flow ID to key-column mapping and a non-matching ID.
- Move `DEX_SERVER_BASELINE` from `server/v1.3.0` to `server/v1.6.1` and `DEX_CLI_BASELINE` from `cli-v1.5.0` to `cli-v1.7.0`. The CLI release contains both Server releases, as the baseline check requires, and `DEX_BASELINE` stays `sdk-go/v1.5.0`, the newest Go SDK release. Every pinned Server and CLI source link, the bundle baselines, and the baseline check now name the new releases. The check drops the needles for contracts those releases removed and adds needles for the contracts the skills now cite: the definition directory loader, the Attribute Store settings and unknown-column refresh, the connection OAuth settings and address policy, release candidates, and the new state-load, Stream, and start-input diagnostics. `check-package.py` rejects any skill or README text that cites a removed Dex Web contract again.
- Remove the Dex Web contracts that Server releases after v1.3.0 deleted: the native project configuration protocol (`web/PROJECT_CONFIGURATION.md`), the trusted-header Work Queue permission mode and its permission header, embedded mounts and CSRF tokens, trusted Start Flow with its recovery endpoint, and the blobstore definition source with atomic definition bundles. Core operations replaces "Native hosted project configuration" with "Deployed Connector configuration", which states that Connector setup runs only in loopback `dexcli dev` and that Dex (Server and Web) provides no deployed Connector configuration, OAuth, or credential store. The five language handbooks rename their "Hosted configuration" section to match, and App Builder drops the Dex Server project Connector mode from Connector architecture, backend implementation, the build handoff, the n8n semantics reference, and its entrypoint.
- Document the Connector SDK path that a deployed Go application uses, which the Dex Web removal (superdurable/dex#587) did not touch: `projectconfig.LoadFromEnvironment` in `sdkgo/projectconfig` (pinned to `sdkgo/v0.25.0`) reads the `DEX_PROJECT_*` variables and the pinned configuration snapshot from S3-compatible object storage that a hosting platform or operator writes, and each connector's generated `NewProjectConnection(project, "connection-name")` builds the connection. Settings are read once at startup and credentials are resolved on every call; from sdkgo v0.25.0 an entry may carry `sharedConnectionId`, and the application then reads the credential from the credential broker the document declares. Refresh is admitted once, a retry joins the admitted attempt, and an unclassified 401 refreshes only after the stored expiry has elapsed. The non-Go handbooks say plainly that no equivalent SDK exists today, and `check-package.py` requires these statements.
- Describe shared connections as `sdkgo` v0.26.0 and v0.27.0 define them. A host platform lets users configure a connection once; the application declares connection names only, and different settings need different connection names. The broker never sends host-kept credential fields (refresh tokens, client secrets, service account keys), and a connector decodes a shared credential without them through `UnmarshalSharedCredential`. `CONNECTION_NOT_SELECTED` is always a terminal error: a new selection or a settings edit takes effect at the next Preview start or Live deployment. Slack v1.0.0 makes `app_token` optional: a shared connection without it receives events relayed by the host over HTTP to the receiver the application mounts at `slack.RelayedTriggerPath`, and a connection with it keeps Socket Mode. The local configuration blocker now cites dex-connectors-library#230, which is still open.
- Replace the `localconfig` and `NewLocalConnection` guidance with the release boundary the sources show. Connector releases before v0.21.0 generate `NewLocalConnection` over `localconfig`, which reads the Dex Web `connections.json`; `sdkgo/localconfig` exists through sdkgo v0.21.0 and is absent from v0.22.0. Every connector from v0.21.0 generates only `NewProjectConnection`. `cli-v1.7.0` Dex Web writes only `connections.json` and `use-configurations.json` and `sdkgo/v0.25.0` has no code that reads them, so a v0.21.0 or later connector cannot read what the Connectors page saves. The skills report that as a compatibility blocker and never write a reader. The Slack and Gmail example links move to the v0.21.0 releases.
- Rewrite the App Builder Dex Web reference for the one remaining model. Dex Web neither authenticates nor authorizes: `Working as` and `workQueuePermissions` select work, the Action permission check reads the request's own list, and a trusted application boundary in front of a deployed Dex Web authenticates users and maps roles to permissions. Start Flow takes a reachable Worker address, generates a new request ID for every start, and sets no ID reuse policy, so the Server default rejects a second submission of the same Flow ID; after an uncertain start response, read the Flow ID with `dexcli flow summary` before submitting again. A deployed Dex Web loads Flow Definition Graph JSON from `web.flowRenderingDirectory` only, rescans it on every definition-dependent request, and returns a 503 and fails `/readyz` for an invalid set, so verification checks `/readyz` and the catalog after publishing.
- Update the local Connector setup guidance for the connection OAuth settings and the release behavior in `dexcli` v1.7.0: a release for an authorization server the connection owner chooses writes its endpoints and scopes as connection settings, Dex Web checks them before authorization, an endpoint typed into a setting may use `http` only on loopback or private addresses, an OAuth client secret is memory-only unless the release maps it to a stored credential, Dex Web stores a refresh token only when the release maps one and never refreshes, and automatic setup accepts an exact published release or `-rc.N` release candidate.
- Describe the Dex Server v1.6.1 Attribute Store behavior: a batch that writes a column missing from the current schema, in the store's table or an AttributeMap table, refreshes that store's schema before it writes, so a migration that adds a column needs no Dex restart as long as it completes before the Workers that write the column start. A Server before v1.6.1 discovers the column at the periodic refresh.
- Add the diagnostics that `dexcli` v1.7.0 reports as blocking in every mode to the Go FDG authoring table (unselected AttributeMap and Channel state reads, Stream output outside a Step, and a `StartFlow` payload that does not match the starting Step), widen `unused_resource` to same-package helpers, and move the FDG directive links from `cli-v1.3.0` to `cli-v1.7.0`, where `v2_directives.go` is unchanged.
- The Attribute Store text links the published Custom Attribute Store page and names the Server version it requires.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.35.0.

## 0.34.0 - 2026-10-07

- Add a fourth Flow-boundary question to Core modeling and App Builder product discovery: would the split make the Flows exchange RPCs or Channel messages on frequent events, such as every operation, external callback or poll? A yes rules the split out, because every call between Flows adds to both Flows' history and every RPC, including a read-only one, is a separate engine operation with its own latency and cost. The example is a separate Flow that tracks another Flow's inactivity, which needs a message from every operation; that deadline belongs in the Flow where the operations happen. Calls between Flows stay fine for rare events.
- Add the rule to the App Builder entrypoint, a column for messages per frequent event to the Flow-boundary table, the messages each business event causes to the business contract, and a check to the build handoff and the modeling review checklist.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.34.0.

## 0.33.0 - 2026-10-06

- Propose a Dex redesign automatically. The inventory drafts the workflow's intent contract and detects n8n shapes that Dex primitives express better (per-entity Flows instead of schedule polling, cancellable waits, Polling Steps, Dex Web Actions for approvals, Flow identity instead of state tables, per-item recovery, parallel joins, structured generation, typed configuration, collapsed glue nodes) plus source defects worth fixing, as `P` rows. Workflow import then asks the user once whether to optimize for Dex with those improvements or keep the exact n8n behavior and migrate first, together with the remaining facts and their defaults, instead of asking question by question.
- Add the intent contract draft, the `P` redesign-proposal rows (`adopted`, `deferred`, or `pending`), and a first decisions row that holds the single mode question to the n8n inventory ledger; `verify` checks `P` references, and `--strict` fails while a proposal is pending. Secure-first changes are stated for both modes instead of asked.
- Add a "Propose a Dex redesign and ask once" step to workflow import, renumber the later steps, make the design confirmation report rather than ask again, and add a redesign-pattern table to `n8n-semantics.md`.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.33.0.

## 0.32.0 - 2026-10-05

- Add an App Builder workflow for importing an exported workflow definition, starting with n8n JSON. The new `workflow-import.md` reference sets a fidelity contract: the exported configuration is the behavior authority, and every node, connection output, expression, credential, setting and export metadata field, claim, edge behavior, connector branch, and finding ends as `mapped`, `diverged`, `dropped`, `blocked`, or `pending` (a proposal awaiting the user), with no silent drops or fixes. A default or failure claim cites the n8n source at the instance's release; a claim from memory stays `blocked`. The workflow orders the work as inventory, secrets first, behavior over labels, execution-model mapping with Connector Step composition rules, released-connector mapping, golden parity, confirmation, and cutover.
- Add `n8n-semantics.md`, checked against the n8n source: export anatomy and provenance, the item model and zero-item stops, paired-item lineage, the pre-execution parameter check that fails a whole execution, fan-out order and Wait pauses, parameter expressions (n8n swallows every error except its own `ExpressionError`, and mixed templates render null and undefined as empty text), Code node item rules, IF null handling, Luxon-to-Go time mapping, schedule rules (defaults, second jitter, missed occurrences, deduplication), a node-to-Dex mapping table including Webhook, Wait, Merge, and polling, version-dependent defaults with attribution footers, and credential mapping.
- Add `dex-app-builder/scripts/n8n_inventory.py` (Python standard library). It writes a fidelity ledger (`ledger.md` and `inventory.json`) with redacted literal secrets and IDs for every row, a Mermaid graph, a connector capability matrix, a Dex plan draft that applies the composition rules, and a decisions table grouped by kind. Its findings cover secrets in requests, schedules that repeat effects, Webhook output shape, template placeholders, fan-out order, Waits that pause branches or stand in for polling, zero-item stops, first-item-only Code, locale and timezone dependence, unreachable Merge inputs, inconsistent authentication, managed credentials, hard-coded identifiers, placeholder nodes that make every execution fail, open triggers, and version defaults. `verify` fails on unresolved rows, missing generated rows, unknown decision references, placeholder rows, and `mapped` rows that rest on unverified claims, and counts rows mapped to specified behavior as conditional; `--strict` also fails on `pending` rows, and `--accept`, the cutover gate, on `blocked` rows too.
- Add `dex-app-builder/scripts/n8n_schedule_golden.mjs`, which prints a schedule's fire times, including DST days, from the `cron` package n8n's scheduler uses at the pinned version.
- Add `dex-app-builder/scripts/n8n_code_golden.mjs` and `n8n_expression_golden.mjs` (Node standard library). They capture golden output from a Code node or an expression parameter over synthetic fixtures, following n8n's item and template rules: first-item reads in all-items mode, rejected `$input.all()` in per-item mode, swallowed expression errors reported as `swallowedErrors`, empty rendering in mixed templates, an `--expression` override for a proposed fix, and a configurable Luxon location.
- Update Connector architecture: text generation follows the released `llm` connector (package `llm`, one provider per connection, unprefixed model IDs, Gemini's path-segment model rule, six routed branches); the module path comes from the connector's `go.mod` at the tag; and a local clone may supply release-tagged connector files through `git show` at the tag when the web view is unreachable.
- Tune the import over three evaluation rounds, in which fresh agents migrated four exports with only this workflow and independent judges scored the plans (average fidelity 67, then 78, then 80.5, with no high or critical misses in the last round). Every adopted fact was checked against the n8n source or the released connector SDK.
- From a first end-to-end migration: per-item processing through a Connector Step uses a persisted cursor, because Connector results carry no item context; run Flow IDs derive from the scheduler's Flow ID and the occurrence, because Flow IDs reject `/`, `$`, and `:`; a scheduler skips late occurrences and sets a long retry window on its waiting Step; and an application tests an unreleased connector through a `go.work` `replace` of the pinned version.
- Route exported-workflow requests from the App Builder entrypoint and description, and validate the new references and scripts in `check-package.py`, with unit tests on synthetic exports.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.32.0.

## 0.31.1 - 2026-10-05

- Bound every Polling wait only with the polling Step's StepOptions, which the engine enforces: the Execute method timeout is the maximum wait for one attempt, and the Execute retry total duration is the maximum wait across all attempts, measured from the first attempt and also cutting the in-flight attempt. Always set the total duration, because an omitted value uses the four-hour server default. Keep the one-minute heartbeat timeout.
- Remove the in-code business deadline from Core, the `dex-sdk` entrypoint, and the Go, Java, Python, TypeScript, and Rust Polling guidance, including the Python advice to pass an absolute deadline as Step input. The heartbeat checkpoint holds only resume state, such as the last reported status.
- Route an expired wait through each SDK's Execute-failure recovery option to a Step that reads the recovery error (detail and error type) and records the business failure; without that route the Flow fails. A non-duration business condition, such as an externally owned lease expiring, is still checked in the loop.
- Add the anti-pattern of a hand-written deadline that duplicates the method timeout or the retry total duration.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.31.1.

## 0.31.0 - 2026-10-05

- Make `dex-app-builder` a generic Dex application-building skill that depends only on open-source Dex: the Dex Go SDK, `dexcli` including `dexcli dev`, Dex Server, Dex Web, the FDG analyzer, and released official connectors. It discovers the business process, models Flows and FDG, implements a Go backend, uses Dex Web or builds a custom UI, and verifies locally.
- Replace the application-template bootstrap with a generic Go stack: an existing application keeps its pins; an empty repository gets a Go module on an exact released Dex Go SDK paired with the installed `dexcli`, checked with `dexcli version check` before connecting to a remote Server.
- Remove guidance specific to a downstream product built on Dex from App Builder and Dex SDK, including its application template contract, release and publishing semantics, and hosting-platform configuration.
- Add local verification on one long-lived `dexcli dev` stack, packaging of validated FDG 2.0 definitions for a deployed Dex Web (definition directory or atomic `active-manifest` bundle), and an on-request deployment section for the Dex Server image behind a `trusted-header` proxy with project Connector configuration.
- Remove the application template baseline mechanism: `TEMPLATE_BASELINE`, its scheduled update workflow, updater script and tests, and the template checks in `check-upstream-baselines.py`, `check-package.py`, and both CI workflows. Dex Server and CLI baseline checks remain.
- Add the open-source product boundary to the repository agent rules and contributing guide, enforced by a package check, and remove downstream product wording from the changelog history.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.31.0.

## 0.30.2 - 2026-10-05

- Describe only Dex concepts and guarantees in the skills. Remove every Continue-As-New mention from Core, the `dex-sdk` entrypoint, the Java and TypeScript versioning guidance, App Builder's Dex Web reference, and the versioning, testing, and read-after-write guidance.
- Stop telling developers to exclude `ContinuedAsNew` runs from application searches in Core primitives, Core operations, the `dex-sdk` entrypoint, and all five language data-handling references. Application run searches constrain FlowType.
- Rewrite Core read-after-write as a Dex-level guarantee table: which write/read pairs are strong, which are eventual or asynchronous, and what to do for each, without backend messaging mechanisms. State once that the strong pairs hold on Temporal-backed deployments and that Cadence-backed deployments confirm with a bounded read loop and do not support locked RPCs, Step-completion waits, or Attribute-match waits.
- Replace backend internals in application guidance with Dex behavior: RPC routing and closed-Flow rejection in Core primitives, error handling, and testing; accepted durable waits and handler generations for Step-completion and Attribute-match waits in Core and all five language references; regular Step executions in Core StepOptions; and direct-state readback in the `dex-sdk` entrypoint, App Builder handoff, and language error-handling references.
- Add the Dex concept boundary to the repository agent rules. Deployment and operations guidance keeps naming the backend and its configuration.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.30.2.

## 0.30.0 - 2026-10-04

- Replace the Timer, retry-backoff, and Iteration polling patterns with one Polling pattern: a single long-running Step's Execute owns every wait on an external system. Each round calls the provider with its own timeout, keeps “not ready” in the loop, writes a Stream frame or heartbeat, and sleeps with the language's plain sleep. The business deadline comes from the first attempt, and the Step keeps the one-minute heartbeat timeout with `interval + call timeout <= HeartbeatTimeout - 10s`.
- Forbid a WaitFor Timer plus self-`GoTo` loop, retry policy or RetryAfter as a polling loop, a loop without heartbeats or progress, an external call without its own timeout, and a page token passed to the next Step execution.
- Keep Go and Rust RetryAfter guidance as explicit backoff for transient errors, not a polling loop. Route Timer, WaitFor, and Go `time.Sleep` guidance to the Polling pattern.
- Remove the source-checked polling excerpts from Go and Rust, and link the published [Polling design pattern](https://docs.superdurable.io/design-patterns/polling). Source-checked snippets will follow once a Dex release contains the new runnable example.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.30.0.

## 0.29.26 - 2026-10-03

- Advance the Dex SDK baseline from `sdk-go/v1.2.1` to `sdk-go/v1.5.0`. Python, TypeScript, Java, and Rust sources at that tag match their v1.4.0 releases.
- Document Go SDK v1.5.0 default Flow and Step type names, which omit the Go package, the required override for generic types, and how to upgrade production Flows that use package-qualified names.
- Require a domain Flow type name such as `ApprovalFlow`, never just `Flow`, across Core and all five language references.
- Refresh pinned source links, example dependency versions, and the TypeScript concrete-error excerpt.
- Advance the Dex CLI baseline from `cli-v1.4.2` to `cli-v1.5.0`, whose FDG analyzer uses the same Go type names as Go SDK v1.5.0.
- Pair Go SDK and `dexcli` releases by naming rule, and keep applications that pin a Go SDK before v1.5.0 on a `dexcli` before v1.5.0.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.29.26.

## 0.29.25

- Advance the template baseline to released v1.9.6, preserving application source
  identity and canonical single-method Connector authorization IDs.

- Route Go Flow rendering, Summary/Display and Action work directly to a bounded
  FDG 2.0 authoring reference with exact field/input syntax and focused diagnostics.
- Keep one syntax reference for Dex Web, avoiding parser
  source discovery for ordinary authoring. No business-specific patterns, Flow
  lifecycle changes, database schema changes, or SDK upgrades are introduced.


All notable changes to Dex Skills are documented here.

## 0.29.24 - 2026-10-02

- Keep the App Builder entrypoint within 8 KiB and move detailed bootstrap, business, UI, backend and handoff rules into linked references with complete package membership validation.
- Use bounded catalog and exact installed-module reads, retain verified API facts, and implement before repeating already resolved discovery.
- Clear participant UI requests do not require another wireframe approval. Missing private credentials defer real execution rather than source implementation.
- Let a host commit tool own the final full source gate; generate API packages before module import resolution.

- Advance the basic-process template baseline from `v1.9.4` to `v1.9.5`.
- Require the released template to ship without project-local skills.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.29.24.

## 0.29.23 - 2026-10-02

- Validate the one-Step ExampleFlow and require replacing starter Flows for the first business feature.

- Advance the basic-process template baseline from `v1.9.3` to `v1.9.4`.
- Require the released template to ship without project-local skills.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.29.23.

## 0.29.22 - 2026-10-02

- Advance the basic-process template baseline from `v1.9.2` to `v1.9.3`.
- Restore locked template dependencies before generation and source checks in a fresh checkout.
- Require the exact published Connector `modulePath` consistently.
- Keep metered Query retries aligned with provider idempotency and outcome-reconciliation guarantees; unknown paid outcomes are not blindly repeated.
- Require the released template to ship without project-local skills.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.29.22.

## 0.29.21 - 2026-10-02

- Advance the basic-process template baseline from `v1.9.1` to `v1.9.2`.
- Default application authoring to source checks and production builds without generated integration/browser suites, mocks, or test-framework dependencies.
- Separate source readiness from configured real execution; missing private credentials remain an explicit runtime acceptance gap, not a source handoff blocker.
- Preserve requested real acceptance, imported application tests, and the independent SDK/Connector verification scope.
- Require the released template to ship without project-local skills.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.29.21.

## 0.29.20 - 2026-10-02

- Advance the basic-process template baseline from `v1.9.0` to `v1.9.1`.
- Require the released template to ship without project-local skills.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.29.20.

## 0.29.19 - 2026-10-01

- Advance the CLI baseline and CLI source links to released cli-v1.4.2; keep
  SDK, Server, and template baselines unchanged.
- Describe raw Temporal payload inspection with the local protobuf Codec
  Server, including automatic local UI ports and accepted loopback origins.
- Link the shared codec-server guidance from all five language handbooks and
  synchronize plugin and marketplace manifests.

## 0.29.18 - 2026-10-01

- Move released Connector discovery to the application entry checkpoint, before
  low-level SDK investigation; link text-generation selection at that boundary.
- Distinguish application use of generated operation factories from provider
  protocol implementation, and separate source authoring from credential entry.
- Keep exact template, Server, CLI and SDK baselines unchanged.

## 0.29.17 - 2026-10-01

- Treat clear application requests and existing host choices as implementation
  authorization; ask only for missing user-owned business decisions.
- Separate production-source verification from real acceptance and keep
  both Go and frontend generation/build paths in the source gate.
- Distinguish connection settings, operation configuration and credential codecs
  at the official project bootstrap; retain actual dependency verification.
- Align application guidance with the template's real-dependency test policy.
- Advance the template baseline to released `v1.9.0` (`8d845f98de1fff9dde5b321d931ac1f5cdbcf085`), whose application bootstrap consumes canonical project configuration without changing the template Go or Dex SDK pins.

- Correct project-scoped application bootstrap to the released standard Connector SDK loader with trusted `DEX_PROJECT_*` scope, canonical key, exact object version/digest and AWS identity.
- Remove obsolete mounted configuration-file and credential-broker guidance; keep tokens, actual-use refresh and uncertain exchange recovery inside the official SDK boundary.
- Synchronize all five plugin/marketplace manifests.

## 0.29.15 - 2026-10-01

- Advance the immutable Server and CLI baselines to v1.3.0; retain the SDK source and application template baselines.
- Describe native project configuration, default AWS credential resolution, separately versioned secrets, validated deployment snapshots, and uncertain OAuth recovery.
- Describe trusted hosted Start admission, graph-derived Start phase, fixed targets, original-operation reconciliation, and embedded mutation CSRF requirements.
- Keep recovery on the original Flow when safe; distinguish a valid time-travel boundary from a closed execution's separately authorized corrected start.
- Synchronize Core, all five language boundaries, App Builder guidance, and plugin manifests.

## 0.29.14 - 2026-09-30

- Route OpenAI, Claude, and Gemini text generation through the `llm` connector (`connectors/superdurable/llm/v0.2.0`), whose model picker reads each provider's live model list so new lab models need no application change.
- Map generic, named-model, named-provider, and provider-native LLM intents to compositions, and record that classification in the connector capability matrix.
- Describe one `llm` connection holding every added provider with its own key, a connection default model from their live lists, and `defect` for a provider the connection has not added.

## 0.29.13 - 2026-09-30

- Advance the Dex Server and CLI baselines to `v1.2.0`, whose Dex Web renames the Connections tab to **Connectors** at `/v2/connectors`, shows connectors by display name, and lets one connection hold several auth methods.
- Refresh the pinned Server and CLI source links to the `v1.2.0` tags.
- Validate the CLI baseline's renamed Connectors page source.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.29.13.

## 0.29.12 - 2026-09-30

- Advance the immutable SDK source baseline and all SDK source links to the published sdk-go/v1.2.1 release, which contains the five-SDK missing/inactive error rename.
- Use FlowNotActiveOrNotFoundError, Java FlowNotActiveOrNotFoundException, and Rust SdkError::FlowNotActiveOrNotFound; preserve query-only missing translation, active-required mutation interpretation, and distinct service failures.
- Distinguish the released SDK API baseline from older dependency pins retained in the tagged example manifests; require installed-version evidence before changing existing applications.
- Add exact cross-language error/source references and public-type/metadata verification; synchronize plugin manifests while retaining Server, CLI, and template baselines.

## 0.29.11 - 2026-09-30

- Keep Go SDK-derived Flow/Step type names; allow custom GetFlowType/GetStepType only for unavoidable renames of production definitions while preserving their existing durable identities.
- Remove the blanket Dex Web type-name override workaround and route metadata mismatches to analyzer/SDK diagnosis.
- Add old-execution rollout verification for that exception and synchronize plugin manifests without changing dependency baselines.

## 0.29.10 - 2026-09-30

- Map typed missing/not-active failures directly to the business Get contract's not-found result only for confirmed query-only RPCs; retained closed executions remain readable.
- Prohibit lifecycle probes, short-timeout waits, retries, and history lookups solely to distinguish missing from closed on that read path, while preserving active-only mutation semantics and genuine service failures.
- Add real-server missing-target verification and synchronize core, all five language error references, and plugin manifests without changing dependency baselines.

## 0.29.9 - 2026-09-30

- Document Signal acceptance, strong Temporal RPC direct-state readback, eventual search/projection visibility, and asynchronous business completion as separate contracts.
- Add a version-pinned write/read matrix with backend, lifecycle, delayed-start, Worker-routing, timeout, and retry limits; require it at relevant SDK and application boundaries.
- Require first-read integration assertions for strong paths and controlled completion/projection gates for asynchronous behavior.
- Extend source validation across SDK, Server, and CLI baselines; synchronize Codex, Claude Code, and Cursor manifests without advancing dependency pins.

## 0.29.8 - 2026-09-30

- Require an Attribute-match or Step-completion wait after a successful or deduplicated start only when the response promises a critical business milestone, such as a committed database source-of-truth write.
- Keep acceptance-only starts free of waits; define post-commit markers, bounded wait outcomes, and real-server verification for both new and deduplicated starts.
- Synchronize core and all five language error-handling references and plugin manifests without changing dependency baselines.

## 0.29.7 - 2026-09-30

- Return accepted-start responses from validated request/known initial fields without defensive post-success identity or Attribute rereads; distinguish acknowledgement from explicit admission/completion results.
- Require the owning Flow to durably sequence dependent work, with downstream starts in Execute and crash recovery across acceptance/Step commit instead of API-side start/read/start orchestration.
- Require StartFlow before any retry-protection read RPC, search, or status lookup; reconcile only after a relevant failure and preserve reads independently required by the business response contract.
- Clarify the Request ID/ignore-already-started result matrix, SDK-generated IDs, bounded replay after unknown acceptance, and real-server call-order verification.
- Promote terminal read-only RPC semantics to an explicit SDK error-handling rule and design-review gate, including registration/invocation locks, transactions, handler effects, Server policy, and retention boundaries.
- Prohibit business snapshot fallbacks through `FlowNotActiveError`, `WaitForFlow`, and historical Step-output decoding; preserve explicit engine-status, completion-output, and mutation-reconciliation uses.
- Require real Dex/Temporal post-closure typed snapshot reads with assertions that the application read uses no lifecycle/history fallback, and link App Builder verification to the shared SDK guidance.
- Synchronize the Codex, Claude Code, and Cursor manifests; dependency baselines are unchanged.

## 0.29.4 - 2026-09-29

- Require application run searches to constrain FlowType and exclude ContinuedAsNew runs, matching Dex Web's visibility filter.
- Preserve both predicates when composing caller filters with OR; reserve continued-run searches for explicit execution-chain inspection.
- Synchronize SDK core, five language references, and plugin manifests at version 0.29.4 without changing source baselines.

## 0.29.3 - 2026-09-29

- Align App Builder's indexed Attribute metadata example with the shared generic slot policy, using explicit keyword2 in both the annotation and SDK definition.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.29.3.

## 0.29.2 - 2026-09-29

- Require explicit generic numbered Search Attribute slots shared across Flow types, while retaining business names on Attributes and reserving Dex system indexes.
- Document singleton bool coordination Attributes for map-wide write invariants, including participation by all writers, independent loading, and SDK-managed lock ownership.
- Synchronize core and all five language references without changing pinned baseline source excerpts.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.29.2.

## 0.29.1 - 2026-09-29

- Advance the Dex Server and CLI baselines to `v1.1.2` for release-scoped hosted Connector setup commands.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.29.1.

## 0.29.0 - 2026-09-29

- Advance the basic-process template baseline to `v1.8.0`.
- Require provider token isolation from application code and Flow state.
- Document local on-demand OAuth refresh, hosted refresh-token rotation, multi-auth selection, fail-closed readiness, and real refresh E2E evidence.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.29.0.

## 0.28.2 - 2026-09-28

- Default new Flow designs to parallel Steps and require an evidence-backed, explicitly confirmed evolution gate before introducing SubFlows.
- Separate independent top-level Flows by authoritative owner, retention and cleanup, waits, Timers, terminal outcomes, and state-pollution risk.
- Start App Builder authorization design with one `admin` role while retaining granular Action permissions and requiring evidence for every additional role.
- Add generic regression assertions for authoritative versus temporary lifecycle state, typed cross-Flow coordination, and the distinction between top-level Flows and SubFlows.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.28.2.

## 0.28.1 - 2026-09-28

- Reduce Dex Connector Contributor to a thin official-repository bootstrap that loads the target checkout's current agent rules, documentation, and acceptance criteria.
- Move connector implementation, configuration, checked-in example, live-provider verification, release, and pull-request authority to `superdurable/dex-connectors-library` instead of shipping a duplicate snapshot.
- Remove the Connector Contributor references, connector snapshot baseline, source-excerpt checker, and corresponding CI checkout.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.28.1.

## 0.28.0 - 2026-09-28

- Require connector contributors to audit every authorization, operation, and Trigger configuration field exposed by every runnable example.
- Require provider start URLs, exact page paths, value provenance, formats, units, secrecy, blank behavior, derived claims, read-only pickers, and contextual UI-unit descriptions.
- Document Dex Web parenthesized manifest defaults and prevent applications from reintroducing free-text fields for provider-derived values.
- Advance Dex Server and CLI baselines to `v0.14.1`, the basic-process template baseline to `v1.7.2`, and the connector snapshot to Slack `v0.11.0`.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.28.0.

## 0.27.1 - 2026-09-28

- Advance the basic-process template baseline from `v1.7.0` to `v1.7.1`.
- Require the released template to ship without project-local skills.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.27.1.

## 0.27.0 - 2026-09-27

- Advance the basic-process template baseline from `v1.6.2` to `v1.7.0`.
- Treat generated Go and TypeScript OpenAPI clients as ignored local build outputs that never enter application commits or pull requests.
- Replace the default application-level mock backend with component-level generated-client mocks and narrowly scoped Playwright request interception for browser-only edge cases.
- Require real Dex, Connector, and application E2E evidence while removing `make check-generated`, `make mock`, and `make test-mock-e2e` from App Builder guidance.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.27.0.

## 0.26.0 - 2026-09-27

- Require App Builder discovery to map every management operation to Dex Web v2 before choosing a custom UI.
- Define Runs, Indexed Attributes, Summary RPCs, Display RPCs, Action metadata, Work Queue, editable scalars, timeline, and graph views as the native management surface.
- Require every Custom UI decision to record a specific Dex Web v2 capability gap instead of treating the need for an admin backend as sufficient.
- Document optional QR capture for user-sourced string Action inputs, including manual fallback, secure-context and embedding policy, resource cleanup, and unchanged authorization and submission semantics.
- Clarify across Core and all five language handbooks that the current FDG 2.0 management-interface analyzer reads Go source.
- Advance the Dex Server baseline to `server/v0.14.0`, Dex CLI baseline to `cli-v0.14.0`, and basic-process template baseline to `v1.6.2`.
- Add package and upstream regressions for the management-interface decision order and QR capture contract.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.26.0.

## 0.25.12 - 2026-09-27

- Allow all three Dex Skills to route from precise natural-language descriptions while keeping App Builder the product-workflow default.
- Keep Codex Plugin starter prompts in natural language without standalone Skill invocation syntax.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.25.12.

## 0.25.11 - 2026-09-27

- Document Plugin and standalone Skills as mutually exclusive installation paths with host-specific invocation syntax.
- Make copied standalone installations self-contained by moving shared version guidance and bundle baselines under `dex-sdk`.
- Make App Builder the only implicitly invoked skill and keep Dex SDK and Connector Contributor explicit or App Builder-loaded specialists.
- Route standalone SDK and official connector-library requests before product discovery, including connector work that starts from another project checkout.
- Make Codex Plugin starter prompts explicitly invoke the owning Dex skill.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.25.11.

## 0.25.10 - 2026-09-27

- Advance the basic-process template baseline from `v1.6.0` to `v1.6.1`.
- Load the installed Dex Skills release through the current coding-agent host.
- Remove fixed-path assumptions from App Builder guidance.
- Require the released template to ship without project-local skills.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.25.10.

## 0.25.9 - 2026-09-27

- Advance the basic-process template baseline from `v0.2.1` to `v1.6.0`.
- Require the released template to ship without project-local skills.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.25.9.

## 0.25.8 - 2026-09-27

- Remove project-local skill-submodule guidance and make the coding-agent host's installed Dex Skills release the only agent-skill authority.
- Add scheduled template-release discovery that opens a reviewed baseline pull request and prepares a matching Dex Skills release.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.25.8.

## 0.25.7 - 2026-09-26

- Make Dex Flow Attributes and AttributeMaps the default durable application storage instead of introducing a database, cache, ORM, outbox, or shadow read model by default.
- Add a storage decision matrix covering ownership, access paths, scale, contention, Dex primitives, and evidence for any external-store gap.
- Prefer a stable domain/entity Flow and typed operations for Dex-owned shared domain data before treating cross-Flow reuse as a database requirement.
- Limit external stores to confirmed needs such as complex indexes/search, hot-record concurrency, cross-record joins or transactions, and analytical scans, with explicit authority and reconciliation semantics.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.25.7.

## 0.25.6 - 2026-09-26

- Describe the Codex SessionStart hook as a read-only Dex Skills version check in the trust-review metadata.
- Show `Checking for Dex Skills updates` while the hook runs, making its purpose clear without implying that it installs updates.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.25.6.

## 0.25.5 - 2026-09-26

- Add dedicated Codex, Claude Code, and Cursor lifecycle hooks that compare the installed plugin version with GitHub's latest stable release at session start.
- Cache release metadata for 15 minutes with ETag revalidation and a one-second network timeout, while keeping all hook and cache failures silent and non-blocking.
- Inject an internal current, outdated, or unavailable status so update notices appear only in the first Dex-related response and Skill-level checks remain a compatibility fallback.
- Document hook trust, privacy, disablement, unsupported environments, and the one-time manual upgrade needed from 0.25.4 and earlier.
- Add deterministic hook tests and package checks for client-specific events, output schemas, paths, timeouts, and the absence of a shared auto-discovered hook file.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.25.5.

## 0.25.4 - 2026-09-26

- Replace the Custom UI mock-first approval gate with a low-fidelity static React wireframe checkpoint limited to pages, fields, actions, and navigation.
- Require Flow, Connector, and application OpenAPI contracts to be designed together, with generated Go server interfaces and TypeScript clients established before backend handlers.
- Put real Go/Dex/Connector implementation and end-to-end verification before dynamic UI wiring, mock-server expansion, imagery, branding, animation, or visual polish.
- Add packaging regressions that enforce the wireframe, contract/backend, integration, and polish sequence.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.25.4.

## 0.25.3 - 2026-09-26

- Make the pinned basic-process template release the single default technology-stack authority for new or effectively empty Dex applications.
- Preserve the template's exact Go SDK, Server, CLI, toolchains, dependencies, lockfiles, layout, generators, and commands unless the user explicitly requests a stack change.
- Clarify that TypeScript is optional frontend code only while Dex application backends, Flows, and Connector integrations remain Go-only.
- Make the published Connector catalog the mandatory first source for selecting released provider capabilities.
- Require exact Trigger, Query, Mutation, and UI capability matches to be verified against the immutable release-tagged `connector.yaml` before integration code is written.
- Fail closed when the catalog or release manifest cannot be verified, and route confirmed public-provider gaps to Connector Contributor instead of falling back to direct provider code.
- Put Connector Contributor second in the Codex starter examples while retaining App Builder as the primary first example.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.25.3.

## 0.25.2 - 2026-09-26

- Add a verified maintainer branch path for Connector Contributor alongside the default fork path, replace the Codex-only `codex/` branch prefix with the repository branch convention, and replace the undefined `$opr` step with concrete `gh` publish, check-watch, and fix-and-push instructions.
- Document connector-library version bumps for additive work, the hard-coded `cmd/connectorctl` registry tests, a new-connector checklist, one operation per Step with bounded requests, Query classification for stateless compute, and exact-scope and paid-API live testing.
- Reconcile the five-second SYNC heuristic with the seven-second ASYNC local-phase limit and add heartbeat guidance for silent non-streaming provider calls.
- Add Dex Web v2 Start Flow requirements, headless start and connection endpoints, safe pre-release connector verification without polluting `GOMODCACHE`, multi-Flow validation, and an FDG 2.0 analyzer rules table observed with Dex CLI v0.13.8.
- Document advancing all basic-process template pins with its contract test, checking the local `dexcli` version, application-owned local artifacts, Go SDK pin selection with released connectors, crash simulation with a killed Worker subprocess, and superseded `dex-developer` direct installs.
- Require starting a long-lived, user-facing `dexcli dev` stack with a persistent `--flow-rendering-dir` as soon as the first Flow graph renders, sharing the Dex Web URL immediately, and keeping it running while tests use isolated stacks (App Builder, Dex SDK, and Connector Contributor examples).
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.25.2.

## 0.25.1 - 2026-09-26

- Publish a patch version so installations on 0.25.0 can exercise the once-per-chat newer-version notice against the repository version.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.25.1.

## 0.25.0 - 2026-09-26

- Add a once-per-chat, best-effort plugin version check to all three Dex skills.
- Notify users in the first substantive response only when the repository has a newer stable version, without blocking the requested task or requesting network approval solely for the check.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.25.0.

## 0.24.2 - 2026-09-26

- Require Dex App Builder to initialize effectively empty repositories from the pinned basic-process template before selecting a stack or installing dependencies.
- Preserve the target repository and intentional files while rejecting ad hoc TypeScript backends, npm scaffolds, and SDK inference from neighboring workspaces.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.24.2.

## 0.24.1 - 2026-09-26

- Make Dex App Builder the primary workflow for Dex product, application, and process requests.
- Mark Dex SDK and Dex Connector Contributor as explicit-only specialist sub-skills in Codex while preserving App Builder's internal routing to them.
- Put Dex App Builder first in starter prompts and synchronized skill lists across Codex, Claude Code, and Cursor.
- Require a repository-backed coding workspace before implementation and limit ordinary chat use to discovery, architecture, and handoff.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.24.1.

## 0.24.0 - 2026-09-26

- Add sequentially chunked and hash-partitioned AttributeMap patterns across Core and all five language handbooks.
- Document stable ASCII email canonicalization, wrapping FNV-1a 32-bit partitioning, collision handling, and partition migration.
- Add additive invocation-time exact AttributeMap locks, AttributeMap loads, and ChannelMap loads through each SDK's RPC invocation options.
- Upgrade the Dex SDK source baseline to `sdk-go/v0.13.1` and refresh pinned source links.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.24.0.

## 0.23.0 - 2026-09-26

- Make Dex App Builder prefer released Connector Query/Mutation Steps and typed Trigger targets for Flow starts and RPC delivery before application-local provider code.
- Route missing public-product connectors, operations, and Triggers to `dex-connector-contributor`, with immediate local application testing through an uncommitted Go replacement while the upstream PR is reviewed.
- Add an explicit internal-service decision for reusing or establishing an organization-owned connector library with the unified Connector SDK before generic HTTP fallback.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.23.0.

## 0.22.0 - 2026-09-26

- Add `dex-connector-contributor` for manifest-first official Connector operations, Triggers, configuration UI units, examples, complete validation, and upstream PR delivery.
- Add guided GitHub fork discovery and creation handoff before cloning the user's verified fork and branching from official `upstream/main`.
- Add immutable connector-library source validation pinned to Connector SDK v0.8.0, Slack v0.9.0, Gmail v0.10.0, and Google Sheets v0.7.0.
- Expand Dex Web v2 guidance for Start Flow, Connections, local release overrides, operation configuration, Host API 0.2 composition, provider command brokering, and credential isolation.
- Route missing or defective application connectors from Dex App Builder to the contributor workflow and upgrade the Dex CLI baseline to v0.13.8.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.22.0.

## 0.21.0 - 2026-09-25

- Replace Connector mapper guidance with pure `MapToOperationInput` callbacks and document optional `ResultAttribute` use.
- Replace error-returning Trigger filters with pure `TriggerFilter`, `FlowInputMapper`, and `RPCInputMapper` application callbacks.
- Upgrade Slack example references to v0.6.0, Gmail to v0.7.0, Connector SDK guidance to v0.6.0, and Dex CLI to v0.13.5.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.21.0.

## 0.20.0 - 2026-09-25

- Require application-owned typed filters before Connector Trigger Flow starts and RPC invocations.
- Document filtered-event consumption, retryable filter failures, replay determinism, and provider matcher boundaries.
- Upgrade Slack example references to v0.5.0, Gmail to v0.6.0, and Connector SDK guidance to v0.4.0.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.20.0.

## 0.19.0 - 2026-09-25

- Define Connector Steps as result-only boundaries with application-owned durable context.
- Replace `Presentation` and `BuildInput` guidance with graph-only `Annotations` and `BuildOperationInput`.
- Upgrade Slack example references to v0.4.0, Gmail to v0.5.0, and Connector SDK guidance to v0.3.0.
- Upgrade the Dex Server baseline to v0.13.2 and CLI baseline to v0.13.4.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.19.0.

## 0.18.0 - 2026-09-24

- Remove the retired Connector SDK `TriggerRPC` policy layer from application guidance.
- Make applications own typed RPC registration, RPC options, locks, and bounded redelivery handling.
- Upgrade the Dex CLI baseline to v0.13.2 for released `sdkgo` Connector analysis.
- Upgrade the released Slack example reference to v0.3.0 and Gmail to v0.4.0.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.18.0.

## 0.17.1 - 2026-09-24

- Upgrade the Dex Server and CLI baselines to v0.13.1 for released Connector Trigger discovery and setup.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.17.1.

## 0.17.0 - 2026-09-24

- Add provider-neutral Connector Trigger guidance with application-owned typed Flow and RPC routing.
- Document binding identity, at-least-once delivery, durable event deduplication, and separate connection and Trigger configuration.
- Add released Slack thread and Gmail thread examples for Trigger, Query, typed RPC, and Mutation integration.
- Upgrade the Dex Server and CLI baselines to v0.13.0 and document their embedded Connector Studio delivery.
- Accept the supported template's older embedded Server and CLI tags when they are ancestors of the App Builder baselines.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.17.0.

## 0.16.0 - 2026-09-24

- Replace durable-wait maximum-time guidance with caller-visible Request Timeout and advanced Internal Handler Timeout semantics.
- Explain transparent transport reattachment, typed Request Timeout errors, and reattachment after caller timeout.
- Document when nonzero Internal Handler Timeout protects Temporal's 10 in-flight Update limit and its 2,000-Update history tradeoff.
- Upgrade the Dex SDK source baseline to v0.12.1 and refresh pinned source links.
- Decouple the independently released SDK and CLI baseline validation.
- Synchronize the Codex, Claude Code, and Cursor plugin manifests at version 0.16.0.

## 0.15.1 - 2026-09-24

- Remove the standalone Dex Web baseline and pin its embedded hosted and local delivery through Server v0.12.0 and CLI v0.12.0.
- Upgrade the basic-process baseline to v0.2.1 and template contract 1.4.1.

## 0.15.0 - 2026-09-24

- Add the Dex Web v2 Connections workflow for exact released Connector modules and statically named connections.
- Document the persistent local JSON store, configurable directory, restart behavior, and application launch contract.
- Define Go-only local Connector loading and explicit unsupported boundaries for Python, Java, TypeScript, and Rust.
- Upgrade source baselines to Dex Go SDK v0.12.0, Dex Web v2 v0.3.0, and basic-process v0.2.0.
- Refresh pinned Dex source links and synchronized Codex, Claude Code, and Cursor plugin manifests.

## 0.14.0 - 2026-09-24

- Require Dex Flow ID, start Request ID, and ID reuse policy to own root-start deduplication; prohibit dedicated database admission tables or other shadow deduplication mechanisms.
- Upgrade Dex App Builder to Server v0.11.4, Dex Web v2 v0.2.0, Go SDK v0.11.3, and basic-process release v0.1.0 with template contract v1.3.0.
- Require an explicit No custom UI or Custom UI decision during process discovery.
- Keep only a Hello World/OpenAPI architecture shell when Dex Web provides the complete management experience.
- Reserve generic HTTP for controlled internal systems and require released dedicated connectors for external providers.
- Add the authorized connector fork, upstream pull request, local verification, and release-blocker workflow.
- Refresh all pinned Dex SDK source links and language baseline versions.

## 0.13.0 - 2026-09-21

- Teach Dex App Builder to separate business roles from the permissions required by human Actions.
- Require one stable permission per Action and keep identity-to-permission authorization at the trusted application boundary.
- Document typed Go Action registration, UI slots, Work Queue discovery, and multi-permission search for Dex applications.
- Explain Server-managed Action permission projection and remove projection-only Attribute locking from application guidance.
- Pin SDK source guidance to `sdk-go/v0.10.2` and Dex Web v2 guidance to merged pull request 517.

## 0.12.1 - 2026-09-21

- Guide Attribute designs to reuse namespace-level typed Search Attribute slots and scope raw generic-key queries with FlowType.
- Document stable domain-key exceptions, persistent local schema changes, and the prohibition on indexing PII.

## 0.12.0 - 2026-09-21

- Add the basic-process template's Go mock server and Mock Controls as the required custom-frontend interaction checkpoint before connecting a real Dex backend.
- Pin the supported template to version `1.2.0` and validate its `make mock` and `make test-mock-e2e` commands.
- Shorten the Codex plugin display name and GitHub release title to `Dex` while retaining plugin ID `superdurable-dex` and publisher `Super Durable`.
- Upgrade the Codex, Claude Code, and Cursor package metadata to `0.12.0`.

## 0.11.0 - 2026-09-21

- Rename the technical skill from `dex-developer` to `dex-sdk`.
- Add `dex-app-builder` for business discovery, optional UI prototyping, Go backend implementation, strict Dex Web v2 rendering, and local verification.
- Publish both skills from the root of the `dex-skills` monorepo in one `superdurable-dex` plugin for Codex, Claude Code, and Cursor.
- Remove the nested plugin wrapper, backend companion skill, vendored SDK references, and cross-repository synchronization workflow.

## 0.10.2 - 2026-09-18

- Document the Indexed Attribute capacity left by Dex system indexes in local **dexcli dev** SQLite.
- Distinguish local SQLite slot exhaustion from production visibility-backend limits.

## 0.10.1 - 2026-09-17

- Distinguish query-only RPCs from Signal and Update paths after Flow termination.
- Clarify that successful read-only RPCs do not prove the Flow is active.
- Require terminal-path tests for query reads, returned effects, and transactional execution.

## 0.10.0 - 2026-09-17

- Document compact lowercase Base36 day offsets in Blob references.
- Explain readable, percent-escaped Flow IDs in physical Blob paths.
- Pin runnable sources to Dex release `sdk-go/v0.10.0`.

## 0.9.1 - 2026-09-17

- Make Flow-level durability defaults and method-level StepOptions a first-class Flow design decision.
- Document the ASYNC local phase, regular fallback, shared retry budget, and strict child-deadline requirement.
- Add the five-second ShortRunning/LongRunning heuristic and one-minute heartbeat guidance across all five SDKs.

## 0.8.2 - 2026-09-17

- Document explicit Go RPC registration and definition-time options.

## 0.8.1 - 2026-09-17

- Add race-safe handling for not-active Flow interactions and terminal child cleanup.
- Move shared Client failure policy into a pre-implementation core error-handling guide and route all Client-boundary work through it.
- Document stable start Request IDs and duplicate-start semantics for every SDK.
- Clarify Java's public remote-only `DexServiceException` boundary while preferring concrete domain exceptions.
- Document each SDK's distinct remote-error shape and best-effort Stream boundary.
- Clarify best-effort Stream failure handling and immutable admission projections for fast-closing Flows.
- Pin runnable sources to Dex commit `d5529248`.

## 0.8.0 - 2026-09-16

- Document Server and SDK protocol-interval negotiation during Worker startup.
- Explain first-time Server-first upgrades, compatible rolling upgrades, and breaking maintenance windows.
- Distinguish transport protocol compatibility from open-Flow graph and payload compatibility.
- Pin runnable sources to Dex commit `e93b803a`.

## 0.7.7 - 2026-09-15

- Document that successful ASYNC local Step input snapshots are opt-in and disabled by default.
- Clarify that snapshot storage affects semantic-history input availability, not execution or recovery.
- Pin runnable sources to Dex commit `4c18c7d0`.

## 0.7.6 - 2026-09-15

- Remove protocol-level minimum and maximum Blob object ID lengths.
- Document that readers accept any nonempty lowercase Base36 object ID.
- Pin runnable sources to Dex commit `068926a0`.

## 0.7.5 - 2026-09-15

- Document the Value null arm as the ordinary null representation across all five SDKs.
- Preserve null's boundary-specific meanings: Attribute deletion and omitted Flow completion output.
- Pin runnable sources to Dex commit `6ac5c0af`.

## 0.7.4 - 2026-09-15

- Document the final compact Blob reference shape, including six-digit UTC dates and lowercase Base36 object IDs.
- Explain that Object Blobs store the complete EncodedObject and references have no encoding suffix.
- Restore the explicit `json` and `raw` wire encodings without a compatibility format or versioned path.
- Pin runnable sources to Dex commit `52d43dc7`.

## 0.7.3 - 2026-09-15

- Document the 100-byte default Blob offload threshold and compact lowercase object identifiers.
- Treat internal Blob references as opaque, Flow-owned values with Flow-scoped SDK cache keys.
- Explain automatic Blob ownership transfer across Flow boundaries and the `j`/`r` standard wire encodings.
- Pin runnable sources to Dex commit `d806a958`.

## 0.7.2 - 2026-09-15

- Document deterministic **AnyOf** selection across ready Timer, Channel, and SubFlow conditions.
- Preserve declaration order within each condition kind while warning that mixed kinds have canonical order.
- Recommend returning only the active high-priority condition when a Flow requires strict priority.
- Pin runnable sources to Dex commit `61fa53c1`.

## 0.7.1 - 2026-09-14

- Document server-derived namespaced Request IDs for Step and Attribute waits.
- Document automatic `-N` generations after a durable wait handler times out.
- Recommend zero maximum wait time for ordinary waits and explain the in-flight Update tradeoff.
- Clarify actual matched Attribute values and pin runnable sources to Dex commit `905f39b6`.

## 0.7.0 - 2026-09-14

- Document durable Step and Attribute waits with required caller-owned Request IDs.
- Explain automatic transport reattachment, total handler budgets, infinite-wait lifecycle, and typed handler-timeout errors.
- Show that non-equal Attribute waits return the actual matched value for all five SDKs.
- Pin runnable sources to Dex commit `7ca1878dc`.

## 0.6.1 - 2026-09-13

- Document externally managed attribute indexes for Temporal Cloud API-key deployments.
- Require the three Dex system indexes and every indexed application Attribute to be provisioned before startup.
- Pin runnable sources to Dex commit `1f85cb52`.

## 0.6.0 - 2026-09-12

- Document the Dex Server image's default single-process Web, API, and Interpreter topology.
- Explain independent component deployment with `start --services` and Web-only remote FlowService configuration.
- Clarify Web liveness, upstream recovery, plaintext gRPC, ports, and split Interpreter-to-API wiring.
- Route deployment questions explicitly and distinguish reusable public test bootstrap from repository-only fixtures.
- Cover shared Redis and Blob Store requirements for replicated Server components.
- Tighten manual-command idempotency and Rust revision/recovery composition after isolated language evaluations.
- Pin runnable sources to Dex commit `a1f5f538`.

## 0.5.2 - 2026-09-11

- Prefer direct Flow-first orchestration for multi-step API mutations over database outbox dispatchers and generic event-driven coordinators.
- Retain domain data and read projections in the database while Dex owns durable execution, retries, waits, recovery, and cleanup.

## 0.5.1 - 2026-09-11

- Clarify that `Execute` and `WaitFor` are Flow-modeling phases rather than SDK capability boundaries.
- Allow safely retried provider queries or mutations in either phase when they establish or reconcile a transition.
- Explain when a provider action merits its own Step checkpoint, retry policy, recovery route, or audit boundary.

## 0.5.0 - 2026-09-11

- Make typed Flow RPCs the application boundary for Attribute, AttributeMap, Channel, and ChannelMap reads and writes.
- Retain Attribute match as the blocking observation API while removing guidance for deleted Client state APIs.
- Require action-verb RPC names and complete, precise names across application-facing definitions.
- Warn that Step and RPC pending-message snapshots can race with concurrent Channel mutations.
- Pin runnable examples to Dex commit `24f3a42a` and SDK releases 0.6.0, with Go at 0.6.1.

## 0.4.1 - 2026-09-10

- Prefer dedicated read-only RPCs for responses that combine multiple Attributes or AttributeMap instances.
- Preserve narrow read models instead of combining unrelated views to reduce reads.

## 0.4.0 - 2026-09-10

- Add reverse, non-blocking retained Stream pagination guidance for Python, Go, Java, TypeScript, and Rust.
- Distinguish best-effort newest-first listing from forward, resumable Stream consumption.
- Pin runnable listing examples and the released Dex SDK 0.5.0 dependencies to Dex commit `ffe799a3`.

## 0.3.1 - 2026-09-09

- Add the Super Durable brand mark to Codex and Cursor plugin surfaces.

## 0.3.0 - 2026-09-09

- Replace equality-only Attribute waits with typed Attribute matches across Python, Go, Java, TypeScript, and Rust.
- Document locked revision Attributes as coalescing watermarks for responsive application refreshes.
- Pin runnable matcher and read-RPC examples to Dex commit `4881ef2c`.

## 0.2.0 - 2026-09-09

- Add progressive-disclosure core and language handbooks for Python, Go, Java, TypeScript, and Rust.
- Cover the complete official Dex design-pattern catalog with language-native runnable sources.
- Pin exact API excerpts to a Dex source baseline and validate snippet fidelity in CI.
- Add detailed testing, errors, data, observability, versioning, gotchas, and advanced-feature guidance.
- Add isolated five-language behavior evaluations to the release verification process.

## 0.1.0 - 2026-09-09

- Extract the Dex Developer skill from Dex commit `05be5c42`.
- Package the skill as the portable `dex` Agent Plugin.
- Add Codex, Claude Code, and Cursor marketplace metadata.
- Document installation, invocation, caching, and update behavior.
