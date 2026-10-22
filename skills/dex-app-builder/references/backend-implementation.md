# Stage 3: design the Flow, Connectors, and API contract, then implement the backend

Read the sibling [Dex SDK skill](../../dex-sdk/SKILL.md) completely, then follow its progressive-disclosure routing with [Go](../../dex-sdk/references/go/go.md) as the only language. Load only the Core and Go references required by the approved design.

Dex SDK supplies the public SDK guidance. The App Builder constraints in this skill are stricter and take precedence: use only the Go SDK, keep external effects in `Execute`, keep `WaitFor` free of provider or Dex mutations, and require strict FDG 2.0 rendering.

Read [Flow modeling](../../dex-sdk/references/core/modeling.md), [pattern
selection](../../dex-sdk/references/core/patterns.md), and [Go FDG
authoring](fdg-authoring.md) before editing a Flow. State the Flow identity,
input/output, Steps, transitions, Attributes, Channels, RPCs, timers, retries,
recovery, and connector boundaries before code. New applications default to no
SubFlows: use parallel Steps inside one lifecycle, and use independently
started top-level Flows plus typed RPCs or Channels when the confirmed data
lifecycle requires another owner. Implement a SubFlow only after the Core gate
is satisfied and the user explicitly confirms it.

### Dex-first backend storage

Use Dex Flow state as the default durable application store. Read the shared
[data-handling guidance](../../dex-sdk/references/core/data-handling.md) and the Go
[data-handling](../../dex-sdk/references/go/data-handling.md) and
[entity-state patterns](../../dex-sdk/references/go/patterns.md) before choosing a
database, cache, or ORM.

- Keep cohesive Flow-owned current state in typed Attributes.
- Use AttributeMaps with exact instance loads for independently accessed
  records, bounded chunks, partitions, and keyed entities.
- Use indexed Attributes and Dex search for supported list and lookup paths,
  typed RPCs for application reads and mutations, Channels for queued intent,
  and Dex's blob path for large durable values.
- When a Flow must carry a short-lived secret, such as an API key supplied at
  start, use a Go
  [hard-delete Attribute](../../dex-sdk/references/go/data-handling.md#hard-delete-attributes)
  and delete it after its last use. Never index it, sync it, use it in an Action
  condition, or expose it in Summary, Display, or anything else Dex Web shows to
  users. Return it only from an RPC whose callers are authorized to hold the
  secret. Model the Step to request a fresh secret when the value is absent.
- When several processes need the same durable domain facts, prefer a dedicated
  stable domain/entity Flow that owns those Attributes or AttributeMaps and
  exposes typed application operations. Cross-Flow reuse alone is not a reason
  to introduce a database or duplicate state in each process Flow.

Do not add an external database, cache, ORM, outbox, or shadow read model merely
because the product has durable domain data or may need more queries later. Add
external storage only after a concrete confirmed requirement exceeds Dex's
storage and access model, such as complex or ad-hoc secondary indexes,
full-text/vector search, high-concurrency reads and writes against one hot
record that cannot accept Attribute-lock serialization, relational joins or
transactions across independently owned records, or analytics that require
large cross-Flow scans.

Before adding application-managed external storage, evaluate
Attribute/AttributeMap partitioning, bounded chunking, exact loads, indexed
Attributes, typed RPCs, and—when the state remains Dex-authoritative but needs
a separate query shape—Dex Attribute Store synchronization. If an external
store is still required, document the exact unsupported operation and expected
scale/SLO, assign one authoritative owner for every fact, define projection or
synchronization and recovery semantics, and test failure and reconciliation.
“Future flexibility” is not a sufficient reason.

### Runs that can be measured and improved

Build every Flow so that its own history answers which runs succeeded, for whom,
at what cost, and what a different decision would have done. These rules cost
little in the first version and are expensive to retrofit:

- **Outcomes are visible in the run.** Each business outcome the contract names
  (approved, paid, activated, replied, resolved) is a Step the run enters, an
  Action it accepts, or an Attribute value. When the outcome happens in another
  system, record it when the application learns it, through a Trigger, an Action
  or a Step, rather than leaving it to be inferred later.
- **Segment facts have their own scalar Attributes.** A fact the business would
  compare runs by, such as a plan, region, source, tier or customer size, is a
  scalar Attribute set once when it is known, not only a field inside a JSON
  value or a display string. Index it only when a list needs it; index slots are
  few.
- **Decisions are plain functions.** Routing, scoring, thresholds, lane choices
  and timer durations are computed by plain functions of the run's recorded facts
  and configuration, which the Step calls and records the inputs of. The same
  function can then be run over past runs to see what a changed rule would have
  done.
- **Model calls are their own Steps.** Each model call is an `llm` connector Step
  ([text generation](connector-architecture.md#text-generation)), so its result,
  with its token usage, is in the run's history.
- **Fields keep their meaning across releases.** A release adds a new Attribute
  for new data instead of changing what an existing one means, so runs from
  different releases stay comparable.

Keep personal data in these Attributes to what the process needs.

For Custom UI, design the application HTTP API contract in the same pass as the
Flow and Connector boundaries. Map each approved wireframe action to an
application-level operation backed by a Flow start, typed RPC, query, or
confirmed integration ingress. Define authentication and permission checks,
idempotency, asynchronous status, request and response types, validation,
errors, retry behavior, and terminal outcomes. Keep Dex Steps, Channels,
Attributes, and other execution internals out of the public HTTP contract.

Before implementing backend handlers, update the OpenAPI document (or the
project's existing contract source) and run the project's generation command.
Implement the Go HTTP boundary through the generated server interfaces and
reserve the generated TypeScript client for the later UI integration stage.
Never hand-write parallel request, response, or client types, and never
hand-edit generated output.
Do not resume dynamic frontend work while the Go Flow, Connector, and
application boundary are being implemented.

### Local Dex stack

Start Dex Web as soon as the first Flow graph exists. The moment a Flow source
first renders with `dexcli visualize --schema-version 2.0 --json` (even with
warnings), and before finishing implementation or tests:

1. render every Flow file into one persistent `--flow-rendering-dir` directory with `dexcli visualize path/to/flow.go --schema-version 2.0 --json --out DIRECTORY/<flow-name>`, which writes `DIRECTORY/<flow-name>.json`;
2. start one long-lived `dexcli dev` for the user in the background on stable ports with persistent state, passing that directory and any `--connector-release-override` a local connector needs;
3. give the user the Dex Web URL right away so they can inspect the graph, Connections, and Run views while you continue;
4. start the application Worker against that stack as soon as it compiles, so Runs appear live.

Keep that stack running for the rest of the task. Re-render the graphs after each Flow change and restart only that stack when Dex Web does not pick them up. Run automated tests on their own isolated stacks with free ports and temporary state, so tests never disturb or replace the stack the user is watching. `dexcli dev` binds the next free port when a default port is taken and prints the selected addresses; point the Worker and later CLI commands at those addresses.

Model each human operation as a typed Go Action with one **ActionRequiresPermission** option. Use lowercase domain keys such as **refund.manage** or **refund.message**. An Action RPC rechecks current state and uses business locks when its state check and effect must commit atomically.

Do not add projection-only locks. The Server atomically adds newly matched Action permissions to the Flow execution's Work Queue permission history. A historical match supports discovery; it does not prove current eligibility or caller authorization.

Keep external effects in `Execute`. `WaitFor` only declares durable conditions and must not query or mutate providers or Dex state.

For pull Workers in this Go application workflow, start **Worker.Start** on a goroutine and wait with a bounded **WaitPullRegistered(ctx)** before declaring startup complete. Readiness probes combine **PullRegistrationState().IsReady** with application lifecycle health. Wait for replacement registration before stopping old Workers; Stop alone does not clear the snapshot. Continue handling **Start** failures, inspect **PullRegistrationError** by queue and underlying status, and use **StopOnPermanentStreamRejection** only as an explicit deployment choice. Follow the shared [registration and drain contract](../../dex-sdk/references/core/pull-workers.md#startup-registration-and-readiness), including the distinction between rejected surplus streams and a queue with no other registered stream.

For deployment, choose the listening or [pull Worker](../../dex-sdk/references/core/pull-workers.md) target before wiring the Client. Pull mode needs `WorkerOptions.Pull` and an advertised pull WorkerTarget, not a bind address. Set the Client's default target or a StartFlow/SubFlow override consistently. Use separate Client and Worker bearer credentials when Dex authentication is enabled. A pull Step without an available Worker consumes a retry, so budget startup and dispatch waits with the Step retry policy. When a release adds a Step type or RPC name, start the new pull Workers before stopping the old ones; compatible Server and Worker SDK releases can reassign an unregistered name without consuming the Step attempt. Keep the old logic compatible for names the old Workers still register. Long-running handlers that must observe cancellation record heartbeats within their HeartbeatTimeout.

For connectors:

1. fetch the canonical published catalog at
   `https://superdurable.github.io/dex-connectors-library/catalog.yaml` before
   selecting a connector or writing provider integration code;
2. match every required Trigger, Query, Mutation, and UI capability by exact
   kind and name; a provider or connector name alone is not a match;
3. derive the component tag from the catalog's `directory` and `version`,
   verify that published tag, and inspect that tag's immutable
   `connector.yaml` for auth, configuration, input/output, branches,
   idempotency, Trigger, UI, and generated Go package contracts;
4. record the catalog URL, connector ID, exact capability, version/tag, and
   immutable manifest URL in the connector capability matrix. If the catalog,
   tag, or manifest cannot be read and verified, stop connector-dependent
   implementation and report the blocker; do not infer support from memory,
   an untagged branch, or a provider SDK;
5. compose the Flow from the latest compatible released capabilities wherever
   possible: Query/Mutation factories become Connector Steps, a Trigger target
   starts a typed Flow or invokes a typed RPC, and an application RPC that
   requests provider work commits its state and schedules a Connector Step
   instead of calling the provider itself;
6. do not add an application-local provider client, webhook adapter, or direct
   official SDK call when the connector library already supplies the required
   capability;
7. give every operation-specific factory a static `ConnectionName` matching
   the generated Connection's runtime name, and open each connection with the
   generated `NewProjectConnection(project, connectionName)` over
   `projectconfig.LoadFromEnvironment` (connector releases before v0.21.0
   generate `NewLocalConnection` instead);
8. for a public external product with a documented API or official SDK, require
   dedicated Connector Trigger, Query, Mutation, and UI capabilities as needed;
9. when that public connector or required operation/Trigger is absent or
   defective, read sibling
   [Connector Contributor](../../dex-connector-contributor/SKILL.md) completely
   and ask for authorization to use `dex-connector-contributor` to create or
   modify it; never bypass the gap with direct provider code;
10. after the local connector change builds, immediately test the application
   against its local module using an uncommitted `go.work` or temporary Go
   `replace`, while the contributor workflow pushes its contribution branch
   (the user's fork by default) and opens the upstream PR against the official
   connector library;
11. never commit a branch, commit SHA, pseudo-version, `go.work`, or local
   replacement as a production dependency; after release, remove the local
   override, pin the exact connector tag, and rerun real integration and E2E
   coverage;
12. for an organization-controlled internal service, ask whether an internal
   connector library already exists and whether the user wants to create one
   when it does not; a new internal library uses the unified Connector SDK,
   manifest/codegen contract, credential boundary, and module-level tests;
13. use the generic HTTP connector only when the service is genuinely internal
    and the user does not choose a reusable internal connector capability.

Each Connector Step passes only its current operation result to a branch target. Use generated result aliases such as `ListThreadMessagesResult` or `PostThreadReplyResult`. Persist thread identity, customer input, recovery context, and other business state in an application Step and Attribute before entering the Connector Step. Map the provider operation input with the pure `MapToOperationInput`; never recover upstream context from a result envelope. Use `Annotations` only for graph grouping and explanation metadata. Omit `ResultAttribute` unless a display, RPC, audit, recovery operator, or another path must read the raw result outside the transition chain.

For every Trigger binding, give the generated factory a static connection name and binding name. Keep its matcher configuration separate from workspace credentials. Supply an application-owned `TriggerFilter` and `FlowIDResolver`, then choose either `NewDexFlowTriggerTarget` with a typed Flow and `FlowInputMapper` or `NewDexRPCTriggerTarget` with a typed RPC and `RPCInputMapper`. The filter is the final admission rule for channel, sender, text, tenant, required routing fields, and other domain constraints. Returning false consumes the event without calling Dex. Filter, resolver, and mapper callbacks are pure functions without error results. Do not add manifest-level Flow/RPC Trigger kinds or a configurable RPC-name string.

For an RPC target, register the application's bound method with application-owned `dex.RPCOptions`, then pass that method directly to the target. The application decides whether redelivery needs deduplication, which bounded domain state records it, and which business state or effect requires a lock. Do not add connector-owned persistence or locks to every Trigger RPC.

Make connector mutations idempotent and reconcile unknown outcomes query-first. A missing connector release blocks production handoff. Never invent an unreleased connector API.

Use the manifest-selected authorization method. The user chooses among the
methods declared by the exact connector release; application code must not
hard-code OAuth, API-key, or service-account fields outside that contract.
Each provider call resolves the newest credential through the connection that
`NewProjectConnection` binds and lets the Connector SDK perform supported
on-demand refresh. A deployment reads its configuration and credentials through
the Connector SDK's `sdkgo/projectconfig`, as in
[Connector architecture](connector-architecture.md#deployed-configuration-and-credential-boundary);
business code never handles tokens. Rotation takes effect on the next Connector
call without changing Flow state or rebuilding the app.
