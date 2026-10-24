# Dex CLI

Use `dexcli flow channel-messages FLOW_ID --channel NAME` to list pending FIFO
messages with their server-assigned IDs and decoded values. Delete one with
`dexcli flow delete-channel-message FLOW_ID --channel NAME --message-id ID --yes`.
Raw `dexcli api call InvokeRPC` accepts the `isTransactional` field.
It also accepts `loadAttributeMapInstances`, `loadChannelNames`, and
`loadChannelMapInstances`. For map instances, `MapName/` loads every current
instance and `MapName/<encoded-instance>` loads one slash-free instance. The raw
API expects the protocol-encoded instance; SDK typed loads encode logical instances.

`dexcli` starts a complete local Dex development environment with one command.
It also gives humans and AI agents a JSON-first interface to every public Dex
FlowService API without requiring a browser.

## Install

```bash
brew install superdurable/tap/dexcli
```

The Homebrew formula installs all runtime dependencies. Node.js is not required
at runtime.

When a released `dexcli` starts a local development environment, it checks the
published `cli-v*` GitHub Releases in the background. Every version entry point
also checks: `dexcli version`, `dexcli --version`, `dexcli -v`, and
`dexcli version check`. If newer versions are available, the terminal shows a
colored upgrade reminder and the release notes for every skipped version:

```bash
brew update && brew upgrade dexcli
```

The update reminder and release notes are written to stderr, so version output
and the JSON returned by `dexcli version check` remain script-safe. The
`Breaking Changes` section is displayed in bold bright red. The `dev` check
never delays startup. Checks are skipped for development builds and when GitHub
cannot be reached.

## Start locally

```bash
dexcli dev
```

The local port conventions are:

| Component | Default endpoint | Purpose |
|---|---|---|
| Dex Server | `127.0.0.1:8801` | FlowService and InternalService gRPC |
| Dex Web | `http://127.0.0.1:8802` | Dex Web HTTP UI and API |
| SDK Worker | port `8803` | Application-owned WorkerService gRPC |
| Codec Server | `http://127.0.0.1:8804` | Temporal payload Codec HTTP API |
| Temporal Server | `127.0.0.1:7233` | Local Temporal gRPC |
| Temporal Web | `http://127.0.0.1:8233` | Local Temporal UI |

`dexcli dev` starts Dex Server, Dex Web, Temporal Server, and Temporal Web. An
application starts its own SDK Worker, while `dexcli codec-server` starts the
Codec Server separately.

Select the Rust zd engine with `dexcli dev --engine zd`. It starts
`dex-server-zd` and Dex Web without Temporal. Install `dex-server-zd` on PATH,
set `DEX_SERVER_ZD_BINARY`, or pass `--zd-binary /path/to/dex-server-zd`.
The CLI stores zd state in `$HOME/.dex/zd/<dex-port>` by default. Pass
`--zd-data-dir ./zd-state` to choose another directory. It waits for the zd
gRPC health services before opening Dex Web. zd supports pull Workers only.
Options for Temporal or the in-process Go Server, including
`--sqlite-db-filename`, `--external-temporal-address`,
`--verbose-engine-log`, `--attribute-store-config`, and `--blob-store-dir`,
cannot be combined with `--engine zd`.

If a `dexcli dev` port is already in use, it binds the next free port and prints
the Dex addresses it selected. Running several `dexcli dev` processes on the
same machine therefore starts isolated stacks: distinct Dex ports, a distinct
local Temporal server, and a distinct SQLite database. Point later CLI commands
at a non-default stack with `--server` or `DEX_FLOW_SERVICE_ADDRESS`.

Dex Web opens after every component becomes ready. Pass `--open=false` to skip
that. Ctrl+C stops Dex Web, Dex Server, and all internal processes owned by
`dexcli`.

With the default Temporal engine, local state persists in
`$HOME/.dex/dev/<port>/dex.sqlite.db`.
Dex Web step inputs and values larger than 1 KB persist next to that database
in `dex.blobs`, so execution history and its blobs share a lifecycle. Override
the database path with:

```bash
dexcli dev --sqlite-db-filename ./dex.sqlite.db
```

Choose another blob directory with:

```bash
dexcli dev --blob-store-dir ./dex-blobs
```

Local Connector setup stores named connections in
`$HOME/.dex/connectors/connections.json` by default. All local stacks share
that file. Isolate a stack with:

```bash
dexcli dev \
  --flow-rendering-dir ./build \
  --connector-config-dir ./local-dex-secrets
```

Dex resolves the directory to an absolute path and shows the resulting
`connections.json` path in Web. Restarting Dex Web reloads that file; it does
not clear existing connections. In-progress OAuth and UI sessions are
memory-only and must be restarted after a Web restart.

Connector authors can load an unreleased local Studio bundle while a Flow uses
`go.work` or a local module replacement:

```bash
dexcli dev \
  --flow-rendering-dir ./build \
  --connector-release-override slack=/tmp/slack-local-release
```

The override directory must contain `connector-release.json`, its
`connector-release.json.sha256` digest, and the UI artifact named by that
metadata. The flag is repeatable for different Connector IDs. Dex still checks
the metadata digest, official module identity, exact version, Studio host API
range, UI checksum, and archive paths. The explicit local bundle replaces
GitHub release resolution only for that Connector ID; it does not weaken the
default release path.

`dexcli dev` enables the in-memory Stream Store without Redis. Stream messages
are discarded when the CLI process stops. Repeated source values append
independent messages.

`--blob-store-dir` takes precedence over the directory derived from
`--sqlite-db-filename`.

With the Temporal engine, server logs go to a temp folder that `dexcli` prints
on readiness and deletes when it exits. Keep them with:

```bash
dexcli dev --server-log-folder ./dex-logs
```

With zd, logs go to stderr by default. `--server-log-folder` also writes them
to `dex-server-zd.log` in that folder.

When investigating the local Temporal Server, add `--verbose-engine-log`. It
writes Temporal Server info logs to `temporal-engine-server.log` in that folder.
Use `--server-log-folder` if you need the file after `dexcli` exits.

Configure local Attribute Store projection with the same YAML section accepted
by Dex Server:

```bash
dexcli dev --attribute-store-config ./attribute-store.yaml
```

```yaml
attributeStore:
  stores:
    entityStore:
      type: postgres
      dsn: postgres://user:password@localhost:5432/database
      tableName: public.user_profiles
```

Only `attributeStore` is read from this file. `dexcli` continues to own its local
API, Temporal, Web, and blob-store settings. The destination database and table
must exist and be reachable before startup.

## Application-facing flags

```text
--attribute-store-config string    Dex YAML file supplying Attribute Store settings
--bind-address string              local bind IP (default 127.0.0.1)
--blob-store-dir string            persistent Dex blob storage directory (default $HOME/.dex/blobs)
--connector-config-dir string      local Connector config directory (default $HOME/.dex/connectors)
--connector-release-override value local Connector release as connector-id=artifact-directory (repeatable)
--dex-port int                     Dex gRPC port (default 8801)
--engine string                    Dex Server engine: temporal or zd (default temporal)
--external-temporal-address string      external Temporal host:port
--external-temporal-namespace string    external Temporal namespace (default default)
--flow-rendering-dir string        directory containing Flow Definition Graph JSON files
--open                             open Dex Web after readiness (default true)
--server-log-folder string         keep server logs (Temporal defaults to temp; zd logs to stderr)
--sqlite-db-filename string        local SQLite file (default $HOME/.dex/dev/<port>/dex.sqlite.db)
--verbose-engine-log               write Temporal Server logs to temporal-engine-server.log
--web-port int                     Dex Web port (default 8802)
--zd-binary string                 dex-server-zd executable (default DEX_SERVER_ZD_BINARY or PATH)
--zd-data-dir string               zd state directory (default $HOME/.dex/zd/<dex-port>)
```

Local Temporal gRPC and Web ports start at `7233` and `8233` and move to free
ports automatically. Operators can point Dex at an existing Temporal with
`--external-temporal-address` and
`--external-temporal-namespace`. Dex does not print those endpoints, and
application developers do not need them.

## View Temporal protobuf payloads

Start the local protobuf Codec Server:

```bash
dexcli codec-server
```

It listens on `http://127.0.0.1:8804`. Use `--address` to select another
loopback address. In Temporal Cloud, choose **Configure Codec Server**, select
**Use my browser setting and ignore Namespace-level setting**, and enter that
URL. Allow Chrome Local Network Access when prompted. Stop the server with
Ctrl+C.

For a local Temporal Web UI, configure the same Codec Server URL in its
settings. Local UI ports may vary; no additional server arguments are needed.

The browser connects directly to this local process. The server accepts
`https://cloud.temporal.io` and HTTP/HTTPS origins on `localhost` or loopback IP
addresses, including `127.0.0.1` and `[::1]`, on any port. Other browser origins
are rejected. The server must not be exposed beyond the local machine.

## Operate flows

Commands connect to `127.0.0.1:8801` by default. Override the target with
`--server host:port` or `DEX_FLOW_SERVICE_ADDRESS`; the flag takes precedence.

```bash
dexcli health
dexcli flow start order-123 --flow-type OrderFlow --start-step-type StartOrder --input '{"order":123}' --yes
dexcli flow search --query 'ExecutionStatus = "Running"'
dexcli flow inspect order-123 --all-history
dexcli flow watch order-123 --follow-runs
```

JSON is the default and stable machine-readable output. Use `--output table`
for terminal-oriented output. Large strings and objects are loaded from Dex's
blob store by default. To inspect only their references and avoid the additional
request, pass `--no-hydrate`:

```bash
dexcli flow history order-123 --no-hydrate
```

RPC input and output use the same Blob Store hydration as other history values.
Pass `--no-hydrate` to retain their references.

### Authenticate

A Dex server with authentication enabled requires a bearer credential on every
FlowService call. Configure either a bearer token or an API key, not both.

For a bearer token, pass `--credential-file path`, or set `DEX_CREDENTIAL_FILE`
or `DEX_CREDENTIAL`. The flag takes precedence over both variables, and
`DEX_CREDENTIAL_FILE` takes precedence over `DEX_CREDENTIAL`.

For an API key, `dexcli` exchanges the key at a token endpoint for a short-lived
access token and sends that token:

```bash
export DEX_API_KEY="$(cat ~/.dex/api-key)"
export DEX_TOKEN_URL=https://auth.example.com/oauth/token
export DEX_SERVER_ID=orders-production
dexcli flow search --query 'ExecutionStatus = "Running"'

dexcli health --api-key-file ~/.dex/api-key \
  --token-url https://auth.example.com/oauth/token --server-id orders-production
```

- `--api-key-file path` reads the API key from a file and takes precedence over
  `DEX_API_KEY`. There is no flag for the key value itself, so the key stays out
  of shell history and process listings. Surrounding whitespace is ignored.
- `--token-url` or `DEX_TOKEN_URL` names the token endpoint. It must use
  `https`; `http` is allowed only for `localhost`, `127.0.0.0/8`, and `::1`.
- `--server-id` or `DEX_SERVER_ID` names the server that the token endpoint
  scopes the access token to. `dexcli` never derives it from `--server`.

`dexcli` posts `{"server_id":"<server ID>","api_key":"<API key>"}` as JSON to the
token URL before the first call and expects HTTP 200 with `access_token`,
`token_type` `Bearer`, and a positive `expires_in`. It does not follow
redirects. A command that makes several calls reuses the cached token and
refreshes it after 80% of its lifetime. A failed exchange fails the command before any FlowService
call, with the token URL, the HTTP status, and the endpoint's `error` and
`error_description` fields. Errors never include the API key or an access token.

Combining API key settings with a bearer token setting, or setting `--token-url`
or `--server-id` without an API key, is a usage error that exits with status 2.

## Visualize Flow source

Render a static possible-path graph from one Go or Python Flow source file in
your local browser:

```bash
dexcli visualize ./order_flow.go
```

The command starts a local Flow Rendering page, opens it in your default
browser, and keeps serving the graph until you press Ctrl+C. Use **--json**
when you need the versioned Flow Definition Graph for Dex Web or another tool.
Without **--out**, **--json** writes the graph to stdout. Choose another prefix
with **--out**:

```bash
dexcli visualize ./order_flow.py --json
dexcli visualize ./order_flow.go --json --out ./build/order-flow
dexcli visualize ./refund_flow.go --schema-version 2.0 --json --out ./build/refund
dexcli dev --flow-rendering-dir ./build
```

Open **Flow Rendering** in Dex Web to browse the loaded definitions. The page
renders the graph interactively. A Flow frame contains Step frames, with WaitFor
paths above Execute decisions. Conditional returns are grouped below dispatch
diamonds. Channels, Attributes, RPCs, Streams, folded SubFlows, and diagnostics
can be shown or hidden independently. Streams are hidden by default. Definitions
are loaded once during startup; restart **dexcli dev** after changing the
directory.

Flow Definition Graph 2.0 also records the registered Go start Step type and a
recursive start-input schema. The analyzer follows JSON field names, ignored
fields, pointers, and `omitempty`; it recognizes nested and imported structs,
string and integer enums, RFC3339 `time.Time`, slices, fixed arrays, and
string-key maps. Integer bounds are emitted as decimal strings.

Recursive types, nesting beyond 32 levels, non-string map keys, interfaces,
functions, channels, `[]byte`, conflicting anonymous fields, JSON `,string`,
and custom JSON codecs are not form-safe. The analyzer reports a
`v2_start_input` warning and omits `v2.start`; the rest of the Version 2 graph
remains available.

Python analysis requires Python 3.11 or newer. Pass `--python /path/to/python`
to select an interpreter. The analyzer parses Python with the standard-library
AST and never imports or executes the application module. It resolves static
Flow resource aliases and records RPC and Step Attribute locks as
`resource_lock` relations.

Go analysis requires a local Go toolchain and a module/package that passes type
checking. Version 1 accepts one Flow per file. Step registration, transitions,
waits, RPC next Steps, execute-failure recovery targets, and persistence
resource definitions must be directly visible in that file. A resource,
registered Step, Step handler, Flow method, or RPC declared in another file in
the package produces a blocking diagnostic naming that file. Resource access may
also be in functions and methods of the same package that a handler calls. The
analyzer follows those calls up to six levels deep, including calls inside loops
and function literals, and binds a resource argument to the matching parameter.
Such an edge records the helper as `metadata.via: helper` and
`metadata.function`. Wait conditions and Execute decisions are structured node
details rather than labels inferred from edges. Go SubFlow conditions include the
registered child Flow type and zero-based argument index within Until, AnyOf,
or AllOf. Condition helpers in the same package are followed up to six levels
when they have one return expression. Recursive or dynamic helpers remain
unknown. A SubFlow node's metadata.flowType and metadata.sourcePath reference
the child's registered type and source file, with the path relative to the
parent source file. When both definitions are loaded, the child name links to
its graph. Pending child waits survive parent run switches. Channel edges run from
publishers through the Channel to consuming WaitFor paths. Attribute edges run
from writers through the Attribute group to readers. Terminal decisions and
cancellation remain inside their Execute card. The graph also records repeatable
best-effort Step Stream writes. Python synchronous Step generators and
asynchronous Step handlers are both recognized. Heartbeat checkpoints are
runtime details and are omitted. Step Stream progress from an RPC or Flow
timeout handler produces the blocking `step_progress_outside_step` diagnostic,
including access through same-package helpers or buffered writer aliases. Creating
a `NewBufferedTextStream` in either handler also fails, even before its first
`Write`. The diagnostic names the handler, Stream, and helper location. Move the
output to a Step; an RPC can publish a Channel message or return `NextSteps`.
Business helpers may remain in
other files, but they must not hide Dex control flow. Dynamic targets produce an
Unknown node and a blocking diagnostic. The default renderer still shows the
partial graph. With **--json**, a partial JSON artifact is written, and the
command exits with status 1.

Graph Flow and Step names are the names the Go SDK registers. Without an
override, a Flow or Step is named by its Go type without package or pointer,
such as `ThreadApprovalFlow` or `initializeThread`. Name each Flow type after
its domain, never just `Flow`. A generic Flow or Step has no default name and
needs a `GetFlowType` or `GetStepType` override. `GetFlowType` and
`GetStepType` overrides are exempt from the same-file rule: they may be declared
in any file of the package or promoted from an embedded type, and every return
must be the same compile-time string. An empty string keeps the default name.
Dex rejects Step types that contain `/`, `$`, or `:`.
Names the analyzer cannot know produce `dynamic_type_name`,
`generic_flow_type_name`, or `generic_step_type_name`. Step nodes are
keyed by Go type, so registering one Go type or generic Step more than once
produces `duplicate_step_type`. Regenerate definitions after upgrading **dexcli** so
Dex Web v2 keys match the Worker.

Version 2 is Go-only and adds ordered Step groups plus a Dex Web v2 contract.
The contract declares Indexed Attributes, the fixed `GetDexSummary` and
`GetDexDisplay` RPCs, editable Display fields, and conditional Action RPCs.
Every Version 2 Step must declare exactly one `dex:group` and one
`dex:explanation text:"..."` directive. The explanation is one sentence that
states what the Step does; it is stored on the Step node as
`metadata.explanation`. Directives use unordered named `name:value` arguments.
Repeated directive lines retain source order; no `order` property is generated.
Every Version 2 Step, Attribute, RPC, input struct, and directive must be in the
same source file.

Version 2 also recognizes operation-specific Connector factories such as
`openai.NewCreateResponseStep`. Recognition uses the config's canonical
Connector SDK marker and generated field metadata, not a list of connector or
function names. The generic SDK `MustNewQueryStep` and `MustNewMutationStep`
escape hatches remain supported. Put the factory call directly in `DefineStep`
or `DefineStartStep` and use an inline config literal. `StepType`, annotations,
named `GoTo` branch targets (or generic `GoToBranch` targets), `StepRef` names,
Result Attributes, Streams, and `ProceedToOnExecuteFailure` targets must be
statically visible. Factory annotations supply the Step group and
explanation, so factory Steps do not need local directives. Branches become
labeled transition edges; Result Attributes and structured/text Streams become
write edges. Dynamic config, branch slices, targets, or resource references
produce blocking diagnostics.

Connector Steps and Trigger bindings may also declare a static
`ConfigurationUI`. Each entry selects a manifest-owned reusable UI unit and
maps its output ports to JSON Pointers in that use's configuration object.
Version 2 copies this composition into `metadata.connector.configurationUI` or
the Trigger binding definition. Unit IDs, instance IDs, bindings, and their
ordering must be visible in the inline Go composite literal; dynamic
configuration produces a blocking diagnostic.

Version 2 reads Action labels, conditions, and required permissions from direct
Go `RPCOptions.Action` registrations. Each Action must use exactly one
`ActionRequiresPermission`. Permission keys start with a lowercase letter and
contain lowercase letters, digits, dots, or hyphens. Display field directives
use `ui-slot`, emitted as `uiSlot`; the former `slot`, Action, and condition
directives are not supported.

An Action input directive may add `capture:qr-code` to a `source:user`
`value-type:string` field. The analyzer copies this optional hint into the
Version 2 catalog. Unknown capture modes, non-string fields, and
Attribute-sourced fields are blocking diagnostics. Omitting `capture` preserves
the existing text input contract.

Go `StartFlow` calls with a statically known starting Step input produce
`start_flow_input_type_mismatch` when their payload is not assignable to that
input. This is always an error, under both schemas and with or without
`--lint app`: the SDK rejects it locally before sending the start request.
Pass the actual typed value, or declare `dex.None` and pass `nil` when the
starting Step needs no input. Pointer/value mismatches remain distinct;
legal `nil` pointer, map, slice, or interface inputs are accepted.

The check covers calls in the Flow file and same-package helpers reached by
handlers. It reads one `DefineStartStep` from a directly returned Step slice
literal in the target Flow's `GetSteps`, including target Flows in other
files or imported packages. Dynamically selected Flows, generic Flow types,
interface-valued payloads, and unresolved registrations remain unchecked.

Go Step methods, RPCs, and timeout handlers receive AttributeMap values and
pending Channel messages only when their options select them. A read of
unselected state fails at run time, so visualize reports it as a blocking
diagnostic in every schema version:

| Code | Read |
|---|---|
| `attribute_map_read_not_loaded` | `AttributeMap.Get` with no whole-map or instance load of the map |
| `attribute_map_enumeration_not_loaded` | `AllInstanceKeys` or `MapSize` without a whole-map load |
| `attribute_map_instance_not_loaded` | `Get` of a constant key that no constant instance load names |
| `channel_messages_not_loaded` | `PendingMessages` or `FindPendingMessage` with no Channel or ChannelMap load |

Writes, `Size`, ChannelMap keys and sizes, wait conditions, and
`GetConditionResults` need no load. Loads come from the Flow's package:
`GetStepOptions`, `WithStepOptions` on every `GoTo` or `MovementOf` into the
Step, and `ProceedToOnExecuteFailure` targets for Steps; `RPCOptions` in
`DefineRPC`, and `Load`/`LoadMessages` selections in any call that passes the
RPC method value (such as `InvokeRPCWithOptions`) for RPCs; and
`FlowTimeoutHandlerOptions` values for `HandleTimeout`. Option values may be
literals, package variables, local variables with field assignments, or
same-package function results. Self-appending option slices and cyclic local
aliases are evaluated as a union of their known contributions. This includes
appends to option fields, assignments to helper parameters, pointer copies,
and empty slices created with `make`. Dependencies are expanded iteratively;
each traversal deduplicates references to the same variable and scope.
Recursive helpers, helper calls deeper than six levels, and expansions
exceeding 10,000 expressions are treated as unresolved options. A read in a
helper is reported at the handler's call in the Flow file, and the message
names the helper's file and line.

When another package selects an RPC's map instance at invocation, declare it
on the RPC method with `// dex:invocation-load attribute-map:<name>` or
`// dex:invocation-load channel-map:<name>`, using the map's Dex name. The
directive satisfies instance reads, not enumeration. An unknown name produces
`invocation_load_directive`. Options that cannot be evaluated, such as values
returned by another package, keep the default mode silent for that read.

`--lint app` (Go only) adds checks that applications opt into. The state-load and start-input
errors above are reported in every mode.

| Code | Check |
|---|---|
| `state_load_unresolved` | a read needs a load, but the handler's load options cannot be evaluated |
| `connector_branches_merged_unread` | a Step receives two or more branches of one Connector Step, including `defect` or `uncertain`, uses the result, and never reads `result.Branch` or `result.Failure` |
| `action_state_not_rechecked` | an Action RPC never reads its `WhenAttributeMatches` Attribute or an Attribute it locks |
| `start_flow_request_id_missing` | `StartFlow` in the Flow file or a reached helper has literal options without `RequestID` |
| `cross_flow_read_not_found_unhandled` | a Step calls `InvokeRPC*` with no `errors.As` for `FlowNotActiveOrNotFoundError` or `FlowNotFoundError` on its call path |

```text
dexcli visualize SOURCE [--language auto|go|python] [--schema-version 1.0|2.0]
                         [--lint app]
                         [--open=true|false]
                         [--json [--out PATH_PREFIX|-]]
                         [--python PYTHON_PATH]
```

Invalid command usage exits with status 2. The JSON contracts are documented by
[`schema/flow-definition-graph.v1.schema.json`](schema/flow-definition-graph.v1.schema.json)
and [`schema/flow-definition-graph.v2.schema.json`](schema/flow-definition-graph.v2.schema.json).

The friendly Flow commands are:

```text
dexcli flow start FLOW_ID --flow-type TYPE [--start-step-type TYPE]
                  [--input JSON|@FILE|-] [--attributes JSON|@FILE|-]
                  [--config JSON|@FILE|-] [--retry-policy JSON|@FILE|-]
                  [--step-options PROTOBUF_JSON|@FILE|-]
                  [--flow-timeout DURATION] [--flow-timeout-policy fail|cancel|handler]
                  [--id-reuse-policy previous-failed|not-running|disallow|terminate-running]
                  [--start-delay DURATION] [--ignore-already-started] [--request-id ID] --yes
dexcli flow wait FLOW_ID [--needs-results] [--wait-time DURATION]
dexcli flow search [--query QUERY] [--page-size N] [--page-token TOKEN] [--all]
dexcli flow summary FLOW_ID [--run-id RUN_ID]
dexcli flow state FLOW_ID [--run-id RUN_ID]
dexcli flow history FLOW_ID [--run-id RUN_ID] [--start-event-id N]
                    [--page-size N] [--page-token BASE64] [--all]
dexcli flow inspect FLOW_ID [--run-id RUN_ID] [--all-history]
dexcli flow watch FLOW_ID [--run-id RUN_ID] [--from-event-id N] [--follow-runs]
dexcli flow stop FLOW_ID [--run-id RUN_ID]
                 [--type cancel|terminate|fail] [--reason TEXT] --yes
dexcli flow skip-timer FLOW_ID --step-type TYPE [--execution N]
                    (--condition-id ID|--condition-index N) --yes
dexcli flow wait-step FLOW_ID --step-type TYPE [--execution N] [--request-timeout DURATION]
dexcli flow update-config FLOW_ID --config JSON|@FILE|- --yes
dexcli flow trigger-continue-as-new FLOW_ID --yes
dexcli flow time-travel FLOW_ID [--run-id RUN_ID]
                        --type beginning|history-event-time|step-type|step-execution-id
                        [--target VALUE] [--step-method wait-for|execute]
                        [--reason TEXT] --yes
```

With the default JSON output, `watch` writes one object per line and exits when
the run becomes terminal.
`--follow-runs` continues into the current run after Continue-As-New. Stop and
time travel operate on the current run by default; pass `--run-id` to target an
exact run. All mutating Flow commands require `--yes`, including in
non-interactive use.
`--step-method` is required with `--type step-execution-id` and invalid for other types.

`start --input` and every `value` in `--attributes` use natural JSON. Strings,
numbers, booleans, nulls, objects, and arrays are converted to Dex Values using
the SDK encoding rules. For example:

```bash
dexcli flow start order-123 --flow-type OrderFlow --start-step-type StartOrder \
  --input '{"orderId":"123","items":["book"]}' \
  --attributes '[{"key":"status","value":"new","index":{"type":"keyword"},"sync":true}]' \
  --yes
```

`--config` accepts a JSON object with optional `activeStepSearchMode` (`all`,
`wait-for`, or `disabled`), Continue-As-New thresholds, `stepDurability`
(`sync` or `async`), `workerTarget`, and `attributeStoreNames`. `--retry-policy`
uses `initialInterval`, `backoffCoefficient`, `maximumInterval`, and
`maximumAttempts`; intervals are Go duration strings. `--step-options` accepts
protobuf JSON because it mirrors the full nested StepOptions message. When a
Flow timeout is set without `--flow-timeout-policy`, the server's `fail` default
applies because dexcli cannot inspect the application's Flow registry.
Only one option in a `start` command may use `-`, because stdin can be consumed
once.

## Call any FlowService API

The installed binary contains the protobuf descriptor for its FlowService
version. Agents can discover and invoke every public RPC without server-side
gRPC reflection:

```bash
dexcli api list
dexcli api describe GetAttributes
dexcli api call GetAttributes --data '{"flowId":"order-123","allKeys":true}'
dexcli api call SetAttributes --data @request.json --yes
printf '%s' '{"flowId":"order-123"}' | dexcli api call GetFlowSummary --data -
```

`api call` accepts canonical protobuf JSON: lower-camel-case field names, enum
names, and base64-encoded bytes. Unlike friendly Flow commands, it deliberately
does not hydrate or reshape values. Mutating RPCs require `--yes`.

Successful commands write only their result to stdout. Failures write a JSON
object to stderr with the operation, error kind, gRPC code/name, and status
details when supplied. Usage and missing-confirmation failures exit with status
2; connection and RPC failures exit with status 1.

## Build and test

Requirements are Go 1.24+, Node.js 22+, and the workflow-backend CLI used by
the local supervisor.

```bash
make -C cli build
make -C cli test
make -C cli integration-test
make -C cli integration-test TEST_ARGS='-run TestVisualizeSubFlow'
```

The Web build is embedded into the resulting `cli/dexcli` binary. Running that
binary does not read `web/`, `node_modules`, or the source tree.

## Troubleshooting

- “workflow backend CLI was not found”: reinstall `dexcli` with Homebrew.
- “address is already in use”: an explicit `--dex-port` or `--web-port` is
  taken. Stop that process or pass another port. Unspecified Dex ports and all
  local Temporal ports are assigned automatically.
- “attribute index synchronization failed”: operators should verify that Dex
  can list and add backend visibility indexes. Workers never require a manual
  registration command.

## Version compatibility

`dexcli version` continues to print build metadata. Use `dexcli version check`
to call `GetServerInfo` and compare dexcli's inclusive Server protocol interval
with the Server interval:

```shell
dexcli version check --server dex.example.internal:8801 --output json
```

The command reports both artifact versions, both intervals, the highest common
protocol, and `compatible`. It exits 0 only for a valid overlapping interval.
Connection failures, an unimplemented RPC, invalid intervals, and nonoverlapping
intervals produce a structured error and exit 1. The dexcli artifact version is
injected by the release build and is diagnostic only.

## License

[Sustainable Use License 1.0](LICENSE), with legacy portions under their
original terms as described in [LEGACY_NOTICES.md](LEGACY_NOTICES.md).
