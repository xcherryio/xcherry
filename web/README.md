# Dex Web

The live Flow overview lists pending Channel messages with their server-assigned
IDs and decoded values. Operators can delete a pending message. Temporal performs
the deletion atomically; the Cadence query-plus-signal fallback is best effort and
can race message consumption.

Flow Definition Graphs show resource-read edges for AttributeMap and Channel
message snapshots explicitly selected by Python RPC decorators. Channel size
metadata does not add a resource-read edge because every RPC receives it.

Dex Web searches flows and displays Dex semantic history, step topology,
live flow state, time travel points, and stop controls.

The production Web server is Go. It serves an embedded React SPA and translates
same-origin HTTP/JSON requests under `/api/` to Dex `FlowService` gRPC calls.
Temporal/Cadence credentials and backend history never enter the browser.

`GetHistoryEvents` returns the same `input`, `output`, and `context` step-event
shape for sync, async, and async-fallback execution. Timeline, Execution graph,
Time Travel and Selected event use that common structure.

`POST /api/blobs/load` batches Dex `LoadBlobs` calls. The browser recursively
hydrates the selected event and current flow state, dedupes by blob kind and ID,
and caches loaded values across tabs. Missing values are labeled
`Value blob unavailable`. Terminal async failures without a retained invocation
snapshot explain the short-retry behavior in Selected event without raising a
page-level data warning. Neither state exposes blob IDs or storage paths.

## Run through dexcli

```bash
dexcli dev
```

Open [http://127.0.0.1:8802](http://127.0.0.1:8802). No Node.js process runs in
this mode.

To load Flow Definition Graph JSON files into the independent **Flow Rendering**
page, provide a directory at startup:

```bash
dexcli dev --flow-rendering-dir ./build/flow-definitions
```

Dex recursively scans JSON files on every definition-dependent request. Generate them with `dexcli visualize SOURCE
--json --out ./build/flow-definitions/name`; changes become visible without a
Dex Web restart. Invalid JSON, unsupported schema versions, and source read
failures return a typed 503 and make `/readyz` fail.

Starting Dex with `--flow-rendering-dir` opens **v2** at `/v2`. The top-right
Version menu returns to **v1**. Without that directory, Dex Web opens **v1** at
`/v1/flows`. v2 lists current runs for one Flow type on the left and renders
that type's Flow Definition Graph on the right. A Flow with no Steps renders
one card per RPC. Each card lists the Attributes the RPC reads and writes and
the Channels it publishes to. Action RPCs show their Action label and required
permission, and Summary and Display RPCs are marked as views. Run lists omit
executions closed by continue-as-new. Summary, Display, edits, and
Actions target the current run without accepting a Run ID. Version 1 and
Version 2 files can coexist. Each definition's Flow type is the one Workers
register, so Go Flows need no `GetFlowType` or `GetStepType` override unless
`dexcli visualize` reports a Flow or Step name diagnostic.
Duplicate valid Version 2 definitions for one Flow type, such as two
package-main Flows named `main.Flow`, make the definition source invalid, and
the error names both files. Invalid analyzer output remains visible on **v1** Flow
Rendering but does not appear as a **v2** Flow type.

### Start Flow

The v2 Run workspace shows **Start Flow** when the selected definition contains
a `v2.start` schema. The dialog asks for a Flow ID, a plaintext Worker gRPC address, and the generated
start input. It recursively renders objects, arrays, string-key maps, enums,
booleans, datetime values, optional fields, and nullable fields. Integer input
stays as decimal text until the request is serialized, so int64 values do not
pass through JavaScript floating-point numbers.

Dex Web checks the Worker `host:port` from the BFF when the address loses focus
and again before starting. An unreachable port shows a warning and blocks the
start until the operator explicitly selects **Bypass worker health check**.
The probe opens a TCP connection and does not invoke a Worker method.

`POST /api/v2/start` accepts `flowType`, `flowId`, `workerTargetAddress`, and the
raw JSON `input`. It also accepts `bypassWorkerHealthCheck`; false performs the
server-side port check and returns `WORKER_UNHEALTHY` with HTTP 412 when the
target is unreachable. `POST /api/v2/worker-health` checks the same address for
the dialog. Start requires `X-Dex-Flow-Definition-Revision`, reloads the current
definition snapshot, validates every input field again, and rejects unknown
fields. The Server chooses the start Step type and generates the request ID. It derives
`skipWaitFor` from the validated graph's Start Step phase: `execute` starts at
Execute, while `wait_for+execute` retains WaitFor. Missing, repeated, or unknown
Start phases are invalid definitions; the browser cannot supply Step options.
The browser cannot set Worker headless routing.

Configure headless routing once for every Flow started from this Dex Web
instance:

```yaml
web:
  startFlowWorkerTargetHeadless: false
```

The equivalent environment variable is
`DEX_WEB_START_FLOW_WORKER_TARGET_HEADLESS`. The default is false and the value
is immutable after startup. Set it to true when the entered Worker addresses
are Kubernetes headless Service targets.

The v2 Work Queue is available at `/v2/work-queue`. Its **Working as** control
selects one Action permission and filters on the Server-maintained
`DexWorkQueuePermissions` Search Attribute. The Worker submits the complete
Action permission mapping only when an invocation writes an Action condition
source. Dex Web submits the same mapping only when `SetAttributes` edits one of
those sources. The Server overlays the writes on authoritative Attribute state
and atomically adds newly matching permissions to the projection. Once added,
a permission remains searchable for that Flow execution. `POST
/api/v2/search` also accepts several `workQueuePermissions`; they are matched
with OR, then combined with the Flow type and other filters using AND. This
selector does not grant authorization.

The local v2 Connectors page is available at `/v2/connectors` when Dex Web
runs through loopback-bound `dexcli dev` with a local Flow Definition source.
`/v2/connections` redirects there and keeps its sub-path, query, and hash.
The page uses the Run and Work Queue layout: a resizable
sidebar lists the named connections, and the selected one's setup fills the
main panel. The **Local store** zone at the bottom of the main panel shows the
store directory, the file names inside it, and the launch command, with
**Copy** buttons that copy the absolute values. The page follows the Dex Web theme.

The page groups Connector Steps by Connector ID and static connection name,
resolves the exact official Connector release or release candidate declared by the graph, verifies
release and Studio UI checksums, and loads UI bundles in opaque-origin sandbox
iframes. Each connection view in `GET /api/v2/connector-connections` carries
`displayName`, the release manifest's `metadata.displayName`, from a local
override or a verified official release. Verified
release metadata is cached in memory for the process, and the list waits at
most five seconds for uncached metadata. Without release metadata the field is
omitted, and the page names the Connector by its ID. The connection name is
shown beside it as **Connection** *name*.
Without a Studio bundle, it renders the manifest fields directly. The generic
form renders a connector-owned authorization guide with an external provider
link and ordered setup steps. Fields that require user input appear first. Fields
that can be omitted or use manifest defaults remain visible under **Optional
settings**. Every non-secret manifest default appears below its field in
parentheses; leaving that input blank uses the displayed default. URLs in field
guidance are clickable.

During Connector development, the repeatable
`--connector-release-override connector-id=artifact-directory` flag on
`dexcli dev` can replace release resolution for selected Connector IDs. This
allows Flow graphs produced from `go.work` modules to use the local release
metadata and Studio UI. Connectors labels these sessions as **Local
override**. Overrides are accepted only by loopback local Connector setup and
retain the normal metadata, checksum, Host API, and safe-archive validation.

The setup tabs expand vertically below the selected named connection. They put
authorization first and disable every Flow configuration until the connection
is ready. Each Connector Step or Trigger binding that declares
`ConfigurationUI` gets one tab. Dex Web mounts only the selected tab's
sandboxed unit iframes and marks the tab configured after its scoped record has
been saved. The Connector bundle renders only the small pickers and inputs and
reports bounded content heights through a nonce-bound `connector.frame.resize`
message. Dex Web validates the message source and resizes each iframe without
reading its opaque-origin DOM. Unit ports are merged at their declared JSON
Pointers, so Flow code controls composition without coupling a Connector's
connection screen to one example. The host protocol for this contract is 0.2.

A release with several `spec.auth.methods` normally selects one of them. With
`spec.auth.selection: multiple`, one connection holds a non-empty subset of the
methods, such as several model providers. The form then shows one card per
added method, with its guide, credential fields, and configuration fields, and
a Remove button. An Add menu lists the methods not yet added. The section
title and menu use `spec.auth.methodLabel`, such as **Providers** and **Add
provider**; without it they read **Authentication methods**. Save needs at
least one method. A method's `configuration.fields` are non-secret fields that
are validated, and required, only while that method is selected.

The connection write sends `authMethodId` for single selection or
`authMethodIds`, in add order, for multiple selection. Dex Web stamps the
selection into the stored credentials as `auth_method` or `auth_methods`, a
JSON string array. A request that sets either name in `credentials` is
rejected with `CONNECTOR_REQUEST_INVALID`. Configuration is validated against
`spec.configuration.fields` plus the selected methods' configuration fields.
Credentials are validated against the selected methods' fields, and each
method's required fields must be present. Each write replaces the stored
record, so removing a method drops its credential and configuration fields.

Editing a connection prefills its stored non-secret configuration. Stored
credential values are never returned. The connection view lists their names in
`storedCredentialFields`, and the form shows each stored field as **Stored -
leave blank to keep**. A blank stored field is sent in `keepCredentialFields`.
Dex Web copies each listed field from the stored record only when that record
exists, the field belongs to a selected method, and the request does not also
set it. Any other name is a `CONNECTOR_REQUEST_INVALID` error. OAuth methods
still authorize again with new client credentials.

A `spec.configuration.fields` entry with `studioUnit: {unit, port}` renders
that Studio unit inline once the connection is Ready. Before that, the form
shows a note to save the connection first. A required field without a default
stays a plain input, so the first save can succeed. The frame target is
`{kind: "connection", unitId, bindings: [{port, jsonPointer: "/<field>"}],
value: {<field>: stored}}`. A `use.configuration.save` from it writes the
connection again with the current methods, the stored configuration merged
with the new value, and every stored credential field of the selected methods
in `keepCredentialFields`.

The `connector.host.ready` message also carries three optional fields. `theme` is
the theme the frame paints, `light` or `dark`. Dex Web sends the theme the page
paints. When the Dex Web theme changes, it sends every loaded frame a new ready
message, and the bundle repaints in place. It also sets the iframe element's
`color-scheme` to the same theme, because a frame whose scheme differs from its
element gets an opaque canvas. `themeTokens` is a flat map from `--studio-*` CSS
custom property names to values. Dex Web reads each value from an allowlisted
v2 token in `app/v2/css/tokens.css`, resolved from the ramps of the sent theme,
so a Dex Web restyle reaches every bundle without a Connector release. The
`:root` and `:root[data-theme='dark']` defaults in the stylesheet below match
those light and dark ramps, and `connectorStudioTheme.test.ts` checks that they
stay equal.

Each value must match one grammar, which `sdk/react` in dex-connectors-library
applies too:

- Colors are `#` with 3, 4, 6, or 8 hex digits, `rgb(R, G, B)`, or
  `rgba(R, G, B, A)`, in comma syntax. Channels are integers from 0 to 255
  without leading zeros. Alpha is 0 to 1 with at most four decimals.
- Radii are `0` or a non-negative `px` length with at most three digits on
  each side of the decimal point.
- `--studio-focus-ring` is one box shadow: an optional `inset`, two to four
  `px` lengths that may be negative, and one color.
- `--studio-duration` is up to four digits of `ms`, or seconds with at most two
  integer digits and three decimals, such as `160ms` or `.16s`.
- A font stack has 1 to 16 comma-separated families. A family is a quoted name
  of letters, digits, spaces, and hyphens, or unquoted words that each start
  with a letter, optionally after one hyphen. The CSS-wide keywords, `default`,
  and `none` are accepted only when quoted.
- A value is at most 256 characters.

Dex Web drops any other value, and the bundle keeps its built-in value for that
property.

`stylesheet` is the whole Connector Studio stylesheet: layout, spacing,
component rules, and the default token values for `light` and `dark`. Its only
source is `app/v2/connectors/connectorStudio.css`. Dex Web imports that file as
text with Vite's `?raw` suffix, removes its comments, and sends the result in
every ready message along with `theme` and `themeTokens`. A bundle built with
`sdk/react` replaces its compiled `connectorStudioStyles` with this text and
then applies `themeTokens` on top. A restyle of any Studio rule is therefore a
Dex Web change only. The compiled copy in `sdk/react` is the fallback for hosts
that omit the field, so a restyle does not need to change it. Only new markup
or new classes need a Connector release.

Bundles render only the `studio-*` class contract, which `sdk/react` exports as
`connectorStudioClassNames`: `studio-surface`, `studio-header`, `studio-muted`,
`studio-field`, `studio-actions`, `studio-button`, `studio-button-primary`,
`studio-notice`, `studio-notice-info`, `studio-notice-success`,
`studio-notice-error`, `studio-notice-attention`, `studio-options`,
`studio-option`, `studio-option-label`, `studio-option-id`,
`studio-option-detail`, `studio-badges`, `studio-badge`, and `studio-checkbox`.
The sent stylesheet must style every class in the contract and no other
`studio-*` class. It must also follow these rules:

- It is printable ASCII and at most 65,536 characters. It never contains `</`,
  so it cannot close the frame's `<style>` element. A bundle ignores a longer
  stylesheet or one containing `</`, and keeps its compiled one.
- It has a `.studio-<name>` selector for every class in the contract. A bundle
  also ignores a stylesheet that lacks a selector for any class in the
  `connectorStudioClassNames` it was built with. Renaming or dropping a class
  in `connectorStudio.css` therefore sends every released bundle back to its
  compiled stylesheet, and those bundles miss every later restyle until their
  connectors are released again. Keep every class, and add new ones only.
- It has no at-rules such as `@import` or `@media`, no comments, no backslash
  escapes, no `!important`, and no `url()` or `expression()`.
- A selector is a `:root` or `:root[data-theme='dark']` token block, `html`,
  `body`, or starts with a `studio-*` class. Element names, attribute
  selectors, and the `:hover`, `:focus-visible`, `:focus-within`, `:disabled`,
  `:checked`, and `:not()` pseudo-classes appear only after that class.
- Token blocks declare only `color-scheme` and the allowlisted `--studio-*`
  properties, and every value matches the `themeTokens` grammar. `:root`
  declares every allowlisted property.
- Every other declaration uses only numbers with `px`, `fr`, `%`, `ms`, or
  `s`, `var()` references to allowlisted properties without fallbacks, and the
  layout keywords the test lists. No other function is allowed, and the only
  color keyword on that list is `transparent`, so every other color comes
  from a token. Named colors such as `red` and `currentcolor` are rejected.

`themeTokens` are inline properties on the frame's root element, so they
outrank the stylesheet's token blocks. `!important` is banned because it
would reverse that. `connectorStudioTheme.test.ts` checks every rule above.
Vitest replaces CSS imports with empty strings, so `vite.config.ts` lets
`connectorStudio.css?raw` through for those tests.

Bundles that ignore the fields keep their built-in theme and stylesheet.

The ready message's `connection` also carries `authMethodIds` and
`configuration` for every frame: Step units, Trigger units, and connection
field units. `authMethodIds` lists the added methods in add order. A
single-selection connection reports its one method, or an empty list when the
release declares no methods. `configuration` is the stored non-secret
configuration. The protocol version stays 0.2.0.

A release whose `studio.setup.backendCapabilities` declares
`connection.write` renders its whole connection setup in the Connector UI.
Dex Web shows that bundle's `{kind: "connection"}` frame in place of the
manifest form, grants `connection.write`, and lists the stored credential
field names, never values, as `connection.storedCredentialFields` in the ready
message. The bundle's `connection.save` command carries `configuration`,
`credentials`, `keepCredentialFields`, and an optional `authMethodId`; Dex Web
sends them in the same connection write as the form, so the server validates
them identically, then reloads and confirms **Connection saved** above the
frame. Such a bundle may also add `credentials` to `provider.command.execute`:
values the user typed but has not saved. The broker overlays them on the stored
credentials for that one call and stores nothing, so a model list loads before
the first save. A session without `connection.write` that sends `credentials`
is refused with 403. Dex Web serves a `connection.write` bundle with
`img-src 'self' data:` instead of also allowing `https:` images, so a typed
credential cannot leave through an image request; every bundle already gets
`connect-src 'none'` and `form-action 'none'`.

Provider resource reads use release-declared Studio commands. The manifest
pins an HTTPS GET URL, a credential field and scheme, fixed query values,
optional fixed headers, and the only path or query parameters the iframe may
supply. The `bearer` scheme sends `Authorization: Bearer <secret>`. The
`header` scheme sends the raw secret in one named header, such as
`x-goog-api-key`. `fixedHeaders` holds non-secret values such as
`anthropic-version: 2023-06-01`.

Header names must be RFC 7230 tokens. Dex Web compares them
case-insensitively and treats `_` as `-`, because CGI-style servers merge the
two. They cannot be `Authorization`, `Accept`, `Host`, `Content-Length`,
`Transfer-Encoding`, `Connection`, `Keep-Alive`, `TE`, `Trailer`, `Upgrade`,
`Cookie`, `Set-Cookie`, `Origin`, `Referer`, `Forwarded`, `Proxy-*`,
`X-Forwarded-*`, `X-HTTP-Method`, `X-HTTP-Method-Override`, or
`X-Method-Override`. A fixed header cannot repeat the credential header or
another fixed header. Its value must be 1-256 printable ASCII characters
without leading or trailing spaces and must not contain the credential.

Dex Web checks each command's declaration before it sends that command's
request. A command with an unknown credential scheme or an invalid header
fails with `502`, and nothing is sent. The rest of the release still works:
saving credentials, OAuth, UI sessions, and the release's other commands. A
local release override with an invalid declaration fails when Dex Web starts.

Dex Web's generic broker validates the active session and capability, injects
the credential, sends `Accept: application/json`, bounds a JSON-object
response, and rejects responses containing credential material in any JSON
string or object key. It removes spaces around the stored credential before
sending it, because HTTP drops them on the wire, and it checks responses for
the value it sent. The credential must be printable ASCII.
Provider pagination, filtering, and response projection stay in the
Connector-owned UI bundle; Dex Web contains no provider-specific adapters.

A failed command reaches the iframe as a `connector.command.result` with
`ok: false`. Only the Connector UI shows a failed `provider.command.execute`.
This includes a provider error, a missing credential, and a Dex Web rejection
such as an expired UI session. A failed `use.configuration.save`, such as one
with a stale Flow Definition revision, also appears in the page error banner.
An unsupported command is reported only to the Connector UI.

Connections persist in `$HOME/.dex/connectors/connections.json` by default.
Use `--connector-config-dir` to select another directory. Restarting Dex Web
reloads the file and cached artifacts. OAuth state, PKCE verifier, client
secret, and UI session nonce remain memory-only and are discarded on restart.
The page displays the exact OAuth Redirect URI before authorization. A release
may derive required non-secret credentials from a declared identity endpoint,
which must use HTTPS unless a setting supplies it (see below); Dex accepts a
derived claim only after any declared boolean verification claim is true, and
does not render a duplicate input.
The **Local store** zone shows the JSON file inside the store directory and
the `DEX_CONNECTOR_CONFIG_FILE=... <your-app-command>` launch command, and
**Copy** copies each absolute value. It never returns credential values. Deleting a local credential does not revoke the
provider grant.

Dex Web exchanges the authorization code at the release's `tokenEndpoint`. It
accepts any 2xx JSON response without an `error` field. The grant must include
every requested scope. A `scope` string in the response lists the granted
scopes, separated by spaces or commas. Without `scope`, Dex Web reads a
`scopes` string array, which HubSpot returns. A response with neither field
grants the requested scopes, as RFC 6749 section 5.1 allows. A non-empty
`refresh_token` also proves `offline_access`, and a non-empty `id_token` proves
`openid`. The Microsoft identity platform leaves both out of `scope`. A grant
that lacks a requested scope fails with `CONNECTOR_OAUTH_SCOPE_INSUFFICIENT`.
Requested `userScopes` are checked against Slack's `authed_user` object the
same way. An `authed_user` without either field must carry an `access_token`.

Two optional `oauth2` fields shape the code exchange request.
`tokenRequestEncoding` is `form`, the default, or `json`. `form` sends
`application/x-www-form-urlencoded`. `json` sends one JSON object of string
parameters, as Atlassian and Zendesk document. `tokenEndpointAuthMethod` uses
the RFC 7591 names `client_secret_post`, the default, and
`client_secret_basic`. `client_secret_post` sends `client_id` and
`client_secret` in the body. `client_secret_basic` sends them only as HTTP
Basic credentials, as Zoom, Calendly, and Notion document. Dex Web encodes
`client_id:client_secret` without form-encoding either value first. Any
other value fails OAuth start with `CONNECTOR_RELEASE_INVALID`, before the
browser leaves for the provider.

`scopes` may be empty for providers such as Help Scout and Notion that define
no OAuth scopes. Dex Web then omits the `scope` authorization parameter,
requires no granted scope, and does not list requested scopes in the form.

A release for an authorization server that the connection owner chooses, such
as a company identity provider, cannot fix its URLs. It writes
`authorizationEndpoint`, `tokenEndpoint`, `scopes`, or a
`credentialDerivations` `endpoint` as `{"fromSetting": "<field name>"}`
instead of a constant. `tokenEndpointAuthMethod` and `tokenRequestEncoding`
accept the same form. The name must match a required configuration field of
the selected auth method: type `url` for an endpoint and `stringList` for
`scopes`. Dex Web resolves every reference when OAuth starts, from the
configuration in the start request, which the callback saves under the same
field names. The OAuth session keeps the resolved copy, so the authorization
URL, the code exchange, the granted-scope check, and the identity derivation use
one set of values. A field default applies when the request has no value. A
blank token option also takes the field default, and an empty option selects
the documented default. A missing, null, or blank setting, an empty scope list,
an endpoint that is not an absolute URL, or a token option other than
`client_secret_post`, `client_secret_basic`, `form`, or `json` fails OAuth start
with `400` `CONNECTOR_OAUTH_SETTING_INVALID`. The message names the setting and
never repeats its value. Dex Web does not read the `oidc` block or `protocol`,
so it performs no discovery and ignores references there. It does not refresh
tokens: a Connector that renews its own credential reads the saved settings.

An endpoint that a release writes as a constant must use HTTPS, as before. An
endpoint that a setting supplies may use `http` or `https`. It needs a host and
cannot carry user information or a fragment. Every request that Dex Web sends to
such an endpoint, the code exchange and the identity derivation, follows an
address policy. Dex Web resolves the host once, checks every resolved address,
and dials only an address it checked. It ignores proxies and follows no
redirect for these requests.

- Plain `http` connects only when every resolved address is loopback or
  private.
- Link-local addresses (169.254.0.0/16 and fe80::/10, which hold the cloud
  metadata service), `fd00:ec2::254`, and unspecified and multicast addresses
  are refused even over `https`.
- One refused address refuses the whole host. Over plain `http`, a single public
  address is enough.
- Every other address needs `https`.

OAuth start resolves the token and identity hosts and refuses before the
browser leaves for the provider. The callback checks again when it dials, so a
host that resolves differently later is still refused. Both fail with `400`
`CONNECTOR_OAUTH_SETTING_INVALID`, name the setting and the rule, and send no
request. Dex Web checks the authorization endpoint only for its shape, because
the browser opens it. The OAuth 2.0 HTTP Connector applies the same rule to its
own requests, and `connector_address_policy_test.go` repeats the address table
of that connector's test so the two cannot drift apart. Even over a permitted
`http` address, the authorization code, client secret, and tokens cross the
network unencrypted.

Trigger-binding configuration remains in `connections.json`. Connector Step
operation configuration is stored in the sibling non-secret
`use-configurations.json`, keyed by connector, connection, operation, Flow
type, and Step type. Writes accept only JSON Pointer paths declared by the
Flow definition. Applications load both files as a startup snapshot through
the Connector SDK; changing configuration requires an application restart.

## Definition revisions

Environment variables override YAML:

```text
DEX_WEB_FLOW_RENDERING_DIRECTORY
DEX_WEB_START_FLOW_WORKER_TARGET_HEADLESS
```

`GET /api/flow-definitions` and `GET /api/v2/catalog` return the active revision
as `ETag`; the v2 catalog also returns `definitionRevision`. The browser sends
that revision as `X-Dex-Flow-Definition-Revision` for Search, Display, edits,
and Actions. A stale request receives `409 FLOW_DEFINITION_CHANGED`; the UI
reloads the catalog, clears stale operation state, and requires Action
confirmation again.

## Run through the Dex Server image

The `dex-server` Docker image embeds these production assets and starts Dex Web,
API, and Interpreter in one process by default. It listens for Web traffic on
port 8802 and FlowService traffic on port 8801.

Run only Web when it needs to scale independently:

```bash
docker run IMAGE --services web
```

Set `web.flowServiceTarget` in the mounted server YAML to the API service
address. Web starts even when that upstream is unavailable. `/healthz` reports
process liveness. `/readyz` validates the current definition snapshot and a
FlowService search, returning `definitionRevision`, source, and definition
count; `/api/*` returns an upstream error until FlowService is available.

To populate Web with a 90-execution Flow containing serial, fan-out, and fan-in
sections, run the [Large Step Graph demo](./demo/large-step-graph).
To exercise the widest layout, run the [90-way fan-out demo](./demo/fan-out-90),
which creates `Step1`, 90 parallel Steps, and no close-decision graph edges.

## Frontend development

Requirements are Node.js 22+ and a local `dexcli` build.

Terminal one starts the Go API bridge on port 8902:

```bash
./cli/dexcli dev --web-port 8902
```

Terminal two starts Vite with hot reload on port 8802:

```bash
cd web
npm install
npm run dev
```

Vite proxies `/api/*` and `/healthz` to `http://127.0.0.1:8902`.

## Build

```bash
cd web
npm ci
npm run check
npm run build
GOWORK=off go test ./...
```

Vite writes production assets to `assets/dist/`. `assets/embed.go` embeds that
directory into the Go module and ultimately into `dexcli`.

## Pages

The v2 list compiles typed filter controls into visibility queries,
loads current-run Summary RPCs with bounded concurrency, and displays Indexed
Attributes before ordered Summary fields. Selecting a run validates the Display
contract, supports inline edits for declared primitive fields, and renders
conditional none/object Actions beside the Flow Definition Graph.
Attribute-sourced Action inputs remain hidden. The UI condition is
presentational; Action RPCs must re-check current state.
Version 2 definitions use `uiSlot` for reusable display placement and expose
each Action's `requiredPermission` from the Go SDK Action registration.
User-sourced string Action inputs marked with `capture: "qr-code"` retain their
text field and add a **Scan QR code** button. The scanner is loaded only after a
user opens it, prefers the rear camera, and copies the decoded text into the
field without submitting the Action. Manual input remains available when the
camera is missing, denied, or blocked. Camera access requires HTTPS or localhost;
an embedding host must also allow camera access through its Permissions Policy.
Scanning does not grant an Action permission or bypass its condition check.

v1 pages live under `/v1/flows` and `/v1/rendering`. The Flows page provides Basic and Advanced visibility queries, pagination,
saved queries, configurable columns, Indexed Attributes, and timezone
preferences.

The Flow Rendering page displays the currently active source definition JSON. Its
legend independently filters control flow, WaitFor, RPCs, resources, SubFlows,
and diagnostics. Control flow, WaitFor, RPCs, Channels, Attributes, SubFlows,
and diagnostics start visible. Streams start hidden. Flow timeout handlers are
always visible as part of the Flow rather than a legend layer.

The Flow Definition renderer uses a compound layout separate from the runtime
Execution graph. The lavender Flow frame contains blue Step frames. Each Step
stacks orange WaitFor paths above its Execute decisions, with dispatch diamonds
for conditional returns. Channels occupy the upper-left resource rail,
Attributes share one box, RPC hexagons cross the Flow's left boundary, and
folded SubFlows occupy the right rail. Publishers point to Channels, Channels
point to consuming WaitFor paths, Attribute writers point to the Attribute box,
and the box points to readers. Channel and Attribute relations are hidden until
their resource or a related Step or RPC is selected. Resource and SubFlow
relations are dashed.
Transitions are solid; Execute-failure recovery is solid magenta.
Long labels wrap inside their shapes. Backward transitions and self-loops use
outer routing lanes. Select an edge to emphasize it and show its complete
condition beside the path; its endpoints and source location remain below the
canvas. The Mini Map starts collapsed. Flow timeout handlers use a compact
Step-like card with their timeout decision, rather than the RPC hexagon.
Viewport controls use visible Zoom In, Zoom Out, and Fit View labels. The
collapsed Mini Map uses a visible Show Mini Map button.
Each graph fits the complete definition into the viewport when it first loads
and after its visible layers change.

Flow Definition Graph 2.0 on **v1** Flow Rendering uses Process Canvas. **v2**
renders the same JSON with the Control Topology canvas. Version 1 continues to
use the original definition renderer on **v1**.

The **v2** canvas lays out with `elkjs` (pinned, EPL-2.0), loaded only with the
canvas. Each Step is drawn with one line in; every other transition is a ↪ chip
on the card it leaves. A `dex:group` of two or more Steps is a stage that
**Collapsed** draws as one card and **Expanded** opens; ⊕ and ⊖ open and close
one stage in place. Its drawing rules and guardrails are in
[`app/v2/canvas/views/README.md`](app/v2/canvas/views/README.md).

The graph contract, compound layout, React components, and styles live in the
shared [`flow-definition-renderer`](../packages/flow-definition-renderer)
package. Product docs import the same renderer for checked-in Python example
graphs.

The Run page remembers the last selected Overview, Execution graph, Timeline, or
Streams tab across browser refreshes. It automatically loads the complete semantic
history for the selected run. A continued-as-new run remains on screen until the
operator follows **Next run**; Dex Web never combines two runs into one graph or timeline.
Timeline and Execution graph show an animated loading indicator while waiting
for history. Empty states appear after loading finishes. Refreshing retains
events already on screen. The loading indicator respects reduced-motion settings.
The page also provides Live Flow State beside Selected event, Run input beside Identity,
attributes, timers, queued
steps, channels, completed outputs, stop, and time travel. Timeline and Execution graph keep
Selected event in the sidebar, where the critical-action **Review time travel**
entry opens the operation with that Step execution and its WaitFor or Execute
method already selected. Backend history IDs remain internal pagination and
correlation details. A Time Travel-created run includes a **TimeTravelFork**
event whose source run links back to the preserved history.
AttributeMap and ChannelMap instances with decimal-only names appear before
other instances and sort by numeric value in live state and event details.
Continued runs link to their previous run from Timeline and Execution graph.
Timeline connects each Step execution's first method event to the Flow start,
Flow continued, RPC, Step decision, or recovery event that scheduled it.
Selecting the first event reveals that source link; selecting a
WaitFor event also reveals its outgoing WaitFor-to-Execute link.
Execution graph nodes separate WaitFor and Execute with distinct colors, channel names, condition icons, and individual event details.
Selecting a Step execution emphasizes it in green, its previous Step in blue, its next Steps in orange, and their connecting arrows.
Close decisions remain in event details and do not create misleading Execution graph edges.
Large Execution graphs extend down the page at readable size. Fan-out ranks spread
horizontally and scale down to fit when possible, with a minimum one-third zoom.
Persisted Step decisions add planned graph branches. A selector-matched branch without a Step event appears Canceled with no execution ID.
SubFlow conditions appear as linked leaf nodes and compact WaitFor cards. Running
and terminal nodes display and link by their generated Flow ID. Original WaitFor
events retain the configured reuse policy; continue-as-new state retains only
the Condition ID and stable list position.
Methods interrupted by a forced close remain visible as Pending Timeline events
with their last persisted Scheduled or Started phase.
Timeline and Execution graph share structured event details for flow, step method, RPC,
and channel events. A Raw JSON tab preserves the complete server payload.
Successful Temporal RPC Updates appear as the same RPC events as result signals.
Blob-backed RPC input and output hydrate through the existing selected-event loader.
The Streams tab accepts a Stream name, reads its retained messages from the beginning,
and then continuously long-polls for the next message. Each message displays its creation
time and source. Stream retention is best effort, so older messages can be trimmed.
External attribute writes appear as Attributes updated events, with a
SetAttributes type label and the changed values in the event details.
Raw JSON shows hydrated values; missing retained data is labeled unavailable.
SYNC step Context shows the immediately preceding retry failure when available,
including its backend-native error, worker details, named and numeric gRPC
status, and an optional Worker-provided stack. Live Flow State shows the latest
pending failure on its matching active Step and expands that stack by default.
While a run is active, **Set attribute** writes one Attribute and **Publish
message** publishes one Channel message to that run. Each takes a JSON,
string, int64, double, or boolean value. Int64 values travel as decimal text so
large values keep their precision.
Selected event Context uses the same failure view with the stack collapsed.
## License

[Sustainable Use License 1.0](LICENSE), with legacy portions under their
original terms as described in [LEGACY_NOTICES.md](LEGACY_NOTICES.md).
