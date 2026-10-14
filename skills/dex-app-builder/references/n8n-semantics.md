# n8n export semantics for a Dex import

Read this with [workflow import](workflow-import.md). The defaults below tell you
what to check, not what to assume. Confirm each one in the n8n source at the
instance's release and the node's exported `typeVersion`; some engine defaults
change between releases without a `typeVersion` change.

## Export anatomy

- `nodes[]`: each node's `name` is unique and is how expressions refer to it.
  The node also has a `type` (`n8n-nodes-base.<node>`, a LangChain node, or a
  community package), `typeVersion`, `parameters`, and `credentials`. The
  credentials are only a credential type with an ID and a display name, never
  the secret. A reference marked `__aiGatewayManaged` is an n8n-managed
  credential: the call goes through n8n's AI gateway on a licensed instance and
  is billed to gateway credits, so nothing about the account carries over, and
  the Dex call to the provider with the user's own key changes the endpoint
  and billing. A reference that is a
  plain string or has a null `id` is the legacy name-only form, which n8n
  resolves to the user's own credential of that type by display name.
- `position` has no behavior, except that execution order `v1` runs fan-out
  branches top to bottom by it (see below).
- Execution settings on a node: `disabled`, `retryOnFail` with `maxTries`
  and `waitBetweenTries` (the engine makes `min(5, max(2, maxTries || 3))`
  attempts, `min(5000, waitBetweenTries || 1000)` ms apart, so an exported 0
  wait means 1000 ms and `maxTries` 10 means 5 attempts), `onError`
  (`stopWorkflow`, `continueRegularOutput`, or `continueErrorOutput`; the
  legacy form is `continueOnFail`), `alwaysOutputData`, and `executeOnce`.
- `connections`: keyed by source node name, then connection type (`main`, or
  `ai_*` for LangChain sub-nodes), then output index, then targets with the
  target's input `index`. IF output 0 is true and output 1 is false. Filter
  output 0 keeps items. In Switch rules mode from version 2, output N is rule
  N, and a version 3 `fallbackOutput` of `extra` is the last output; in
  version 1 each rule names its own `output`, and in expression mode the
  `output` expression picks the index. Loop Over Items
  version 3 emits done on output 0 and loop on output 1.
- `settings`: `executionOrder`, `timezone`, `errorWorkflow`, and save and
  caller policies. When `timezone` is absent, the instance's
  `GENERIC_TIMEZONE` applies, and the export does not record which zone that
  is; an instance that never set it uses America/New_York.
- Provenance: `active` and `triggerCount` say whether a trigger was ever live
  (`triggerCount` 0 means no). `meta.templateId` marks a gallery template, a
  specification that may never have run, so there may be no execution history
  to compare against. `id` is the workflow ID; exports often omit it.
- `pinData` is editor test data, not production behavior, and may contain
  personal data. `staticData` is trigger runtime state, such as polling cursors
  and schedule recurrence, not configuration, but its keys are evidence: a key
  that only a later release writes bounds the release and shows the trigger
  was activated on it.
- A `collection` parameter defaults to an empty object, and its children are
  not filled with their displayed defaults, so a code-level fallback for the
  whole collection never applies. Read how the node's code treats an unset
  child, such as a timeout, and record the effective value.
- A parameter value that starts with `=` is an expression. Each `{{ }}` segment
  is JavaScript, evaluated under the rules in
  [expressions](#parameter-expressions).

## Release markers

Each of these bounds the source release from below. Extend the table only with
markers whose first release, and the last release without them, you cited from
the n8n tags.

| Marker | First release |
| --- | --- |
| `settings.binaryMode` | n8n 2.5.0 |
| A Schedule Trigger `staticData` key `recurrenceRuleSignatures` | n8n 2.29.0, and the trigger was activated there |
| Schedule Trigger `typeVersion` 1.4 | n8n 2.36.0 |

Release-gated behavior to recheck once bounded: server-side activation
validation of parameters and credentials from n8n 2.8, deduplicated scheduled
executions from 2.19, stable schedule seconds from 2.19, and the default
expression engine: `legacy` through n8n 2.34, the isolated `vm` engine from
2.35.0, and `quickjs` selectable from 2.43.0. `legacy` and `vm` share the rules
below; read the `quickjs` bridge at the release before relying on them.

## Where engine behavior lives

Cite these files at the bounded release tags (kebab-case paths are from about
n8n 1.96; earlier tags use PascalCase names):

| Behavior | Source |
| --- | --- |
| Pre-execution parameter check, execution order, retry clamps | `packages/core/src/execution-engine/workflow-execute.ts` |
| Required-parameter issues | `packages/workflow/src/node-helpers.ts` |
| Activation and publish validation | `packages/cli/src/workflows/workflow-validation.service.ts` |
| Expression rendering and error swallowing | `packages/workflow/src/expression.ts`; the isolated engine's error handler in `packages/@n8n/expression-runtime/src/bridge/isolated-vm-bridge.ts` |
| Default expression engine | `packages/@n8n/config/src/configs/expression-engine.config.ts` |
| Parameter defaults filled before a node runs | `packages/workflow/src/workflow.ts` (the Workflow constructor fills declared defaults) |
| Code task runner mode and environment | `packages/@n8n/config/src/configs/runners.config.ts`; `packages/cli/src/task-runners/task-runner-process-js.ts` |
| IF, Filter, and Switch operators | `packages/workflow/src/node-parameters/filter-parameter.ts` |
| Waiting executions and their resume | `packages/cli/src/wait-tracker.ts`; the short in-process wait in the Wait node or, on recent releases, `packages/core/src/execution-engine/node-execution-context/base-execute-context.ts` |
| Schedule cron expression and stable values | `packages/nodes-base/nodes/Schedule/GenericFunctions.ts` |
| `$now`, `$today`, and `$input` | `packages/workflow/src/workflow-data-proxy.ts` |

## Item model

- A node receives a list of items and runs its operation once per item by
  default. Each output item keeps a paired-item link to the input item that
  produced it.
- When a node outputs zero items, downstream nodes do not run, unless that node
  sets `alwaysOutputData`. A read that returns nothing ends that branch, and
  the execution succeeds. A downstream fallback for an empty list is dead code.
- A node that returns a provider collection field the response omits, as a
  not-found lookup often does, can emit one item with empty `json` instead of
  zero items. Downstream nodes then run with empty values; read the node's
  `execute()` to tell which.
- A disabled node passes its items through unchanged. `executeOnce` runs a node
  for the first item only.
- In Dex, a small bounded item list becomes a typed slice in Step input. Items
  that are tracked or updated independently become AttributeMap entries keyed
  by a stable source ID. A per-item side effect becomes a dynamic parallel
  Step, unless it passes through a Connector Step: that Step's result carries
  no item context, so loop over those items with a persisted cursor.

## Node references and paired items

- `$('Name').item` returns the item of node `Name` that is linked to the current
  item through paired-item lineage. `.first()`, `.last()`, and `.all()` ignore
  lineage. `$node["Name"].json` is a legacy accessor that resolves by item
  index.
- Reading a node that has not run, or a broken paired-item link, raises an n8n
  `ExpressionError`, which fails the node.
- Dex has no implicit lineage. Carry every upstream field a later Step reads in
  that Step's input or in the item's AttributeMap entry.

## Execution order and failure

- The editor already refuses to activate (1.x) or publish (2.x) a workflow
  while a connected, enabled node shows an issue; only the public API or CLI
  could activate one. Ask how the
  workflow was activated before recording a path that answers and then fails.
- Before the first node runs, n8n checks every enabled node reachable from the
  starting trigger over `main` connections for parameter issues, or an unknown
  node type. Parameter issues are: a required parameter displayed at the node's
  version that is empty, counted only for string, options, multi-options,
  date-time, and resource-locator types (an empty required number, JSON,
  boolean, or collection does not block); a displayed resource-locator value
  that fails its mode's validation; missing or mistyped resource-mapper fields;
  a value that fails the property's `validateType`; and a fixed collection
  outside its minimum or maximum entry count. IF, Filter, and Switch
  conditions add no issue here: a literal operand of the wrong type fails only
  when the node evaluates it. An expression counts as filled
  and skips validation. If any node has an issue, the whole execution fails
  before any node runs, in every execution mode. Credentials are not part of
  this check. So an export with such an issue downstream of a trigger has no
  effect for that trigger's executions; record that as the source behavior and
  offer no "faithful" option that assumes later nodes run. A Webhook in
  `onReceived` mode has already answered the caller. From n8n 2.8 the server
  also refuses to activate or publish a workflow whose trigger reaches a node
  with a parameter issue or a missing required credential, so its production
  trigger never registers (a production webhook answers 404); record the
  source behavior per execution mode.
- Operation and other option values are not checked against the node's option
  list, before or during the run. What an unknown value does is node-specific:
  an explicit error, a lookup that throws, an `if` chain that makes no call and
  emits an empty item, or a request built from the remaining parameters. Read
  that node's `execute()` or routing at the release.
- Each node processes all of its items before the next node starts. Execution
  order `v1` runs each branch to completion before the next, ordered top to
  bottom by canvas position. Legacy `v0` interleaves nodes across branches. The
  order matters when branches have effects that interact: a failure in an
  earlier branch stops the later ones.
- A Wait pauses the whole execution, including sibling branches that have not
  run yet. A wait of 65 seconds or more saves the execution and resumes it
  later on the workflow snapshot taken when the execution started, so edits
  made during the wait do not apply to it. A shorter time wait sleeps inside
  the running execution.
- A failing node stops the execution unless its `onError` setting continues.
  Effects already performed remain, and later nodes run for no item. A Code
  node fails on a thrown error. A parameter expression usually does not: see
  [parameter expressions](#parameter-expressions).
- Nothing retries unless `retryOnFail` is set. An `errorWorkflow` runs a
  separate workflow with the error; map it to Execute-failure recovery Steps.
- Executions may overlap. Triggers other than the schedule do not deduplicate
  deliveries: a webhook starts one execution per request.

## Parameter expressions

- Expressions are JavaScript with Luxon. `$now`, and `$today` (the start of
  the day), use the workflow timezone and are evaluated separately in each
  node, so replacing them with one instant, such as the occurrence time, is a
  `pending` row with a decision, unless that instant always equals the node's
  run time at the precision used. Define the instant for manual and late runs. Other globals include `DateTime`, `$json`,
  `$input`, `$('Name')`, `$vars`, `$env`, `$execution`, `$workflow`,
  `$itemIndex`, and `$runIndex`. `$vars` and `$env` values are not in the
  export.
- n8n's expression engine swallows every error inside a `{{ }}` segment except
  its own `ExpressionError` and `ExpressionExtensionError` and syntax errors. n8n also adds extension methods
  to values, such as `.isEmpty()`, `.toNumber()`, and `.first()`, and extended
  functions such as `$ifEmpty`, so a call that would throw in plain JavaScript
  can return a value in n8n. A TypeError from calling a
  method on a missing field, such as `{{ $json.title.toLowerCase() }}` on an
  item without `title`, leaves that segment empty and the node continues; a
  whole-value expression yields `undefined`. Trace the empty value to its
  consumer: a predicate turns false, a provider call gets an empty parameter
  that can drop a filter, or a later node fails. The editor preview can show an
  error that a production run swallows.
- A whole-value expression returns its JavaScript value, including
  `undefined`. In a mixed template (text with segments), a segment that yields
  `null`, `undefined`, `NaN`, or an empty string renders as empty text;
  `false` and `0` render as text, objects as `[object Object]`, and arrays
  through `toString`, such as `1,,x`.
- An expression that returns a Luxon DateTime is serialized as ISO 8601 with
  its offset. Luxon macro formats such as `toFormat('DDD')` and
  `toLocaleString()` depend on the instance locale.
- Luxon maps to Go as follows. `toFormat('yyyy-MM-dd')` is
  `Format("2006-01-02")`. Calendar arithmetic (`plus` and `minus` with days,
  weeks, or months), `set` of an hour, and `startOf('day')` keep the wall clock
  in the same `*time.Location`, so they map to `AddDate` and `time.Date`, except
  where the result falls in a DST gap: Luxon moves forward by the gap, while
  Go can normalize backward and change the date. Port through one helper that
  shifts forward, and golden the day before, the day of, and arithmetic that
  lands inside each transition of the source zone.
- IF and Filter from version 2, and Switch from version 3: null and undefined
  pass strict type validation. String operators compare `leftValue ?? ''`, and `exists` and
  `notExists` test null, undefined, and NaN. Only a present value of the wrong
  type fails strict validation. The golden harness does not evaluate
  operators, so write predicate expectations from `filter-parameter.ts` at the
  release and cite the lines. Switch 2 is the legacy node: it compares
  `value1` and `value2` per `dataType` in `SwitchV2.node.ts`; port those rules
  instead.

## Code nodes

- A Code node runs in `runOnceForAllItems` mode by default and returns an array
  of items. In that mode, `$input.item` and `$json` return the first input item
  only. In `runOnceForEachItem` mode it returns one object per input item, and
  code that mentions `$input.all()`, `.first()`, `.last()`, or
  `.itemMatching()` is rejected before it runs.
- JavaScript semantics to preserve in a port:
  - A thrown error, such as a method call on a missing field, fails the node.
  - `String.replace` with a string pattern replaces only the first occurrence.
  - Template literals render `null` as `null`, `undefined` as `undefined`, and
    objects as `[object Object]`. `NaN` in an output item serializes as null.
  - `""`, `0`, `null`, and `undefined` are all falsy.
  - `toLowerCase`, `trim`, and `toLocaleString` differ from Go's standard
    library; port them with golden cases.
- `console.log` output is not saved with the execution: a manual run sends it to
  the browser console, and other runs discard it unless `CODE_ENABLE_STDOUT` is
  true. The Code node runs sloppy-mode
  JavaScript, so an undeclared assignment creates a global instead of failing.

## Node mapping

| n8n node | Dex mapping |
| --- | --- |
| Schedule Trigger, legacy Cron, Interval | A scheduler Flow on the Cron pattern. It computes each next occurrence in the workflow's IANA zone, waits on a Timer, and starts one run Flow per occurrence. See [schedule rules](#schedule-rules). |
| Manual Trigger | Dex Web Start Flow with typed start input. |
| Webhook, Respond to Webhook | The webhook connector Trigger. Each n8n request starts one execution with one item `{headers, params, query, body, webhookUrl, executionMode}`; the Dex Trigger accepts only authenticated POST requests with a JSON or urlencoded body up to its size limit, takes its event ID from a delivery-ID header or body pointer, else a digest of the body, and so deduplicates a byte-identical re-POST while the Flow is retained. Read its request decoding at the tag. Record each difference; the authentication change is mandatory. Also check n8n's CORS handling (OPTIONS) and body size limit (`N8N_PAYLOAD_SIZE_MAX`) at the release, compressed and XML bodies, and repeated form keys, and ask whether the caller is a server or a browser, since a browser cannot hold a signing secret. A synchronous response body needs a capability check. |
| App Trigger, such as Gmail Trigger | The matching released connector Trigger, with the provider event ID as the request ID. If it is missing, contribute it. A trigger that accepts anyone, such as a Telegram Trigger without chat or user restrictions, is source behavior to record. |
| Form Trigger | A Dex Web Start Flow form, or a participant Custom UI when the form is participant-facing. |
| Error Trigger, workflow `errorWorkflow` | An Execute-failure route to an explicit recovery Step. |
| Set (Edit Fields) | A typed Go mapping inside the consuming application Step. |
| IF, Filter, Switch | A Go predicate in an application Step that chooses the movement. An unconnected output ends without a movement. |
| Merge | An await-all join with a Channel count, where each branch's success and failure routes both publish, so each branch counts once. Replicate the merge mode exactly: append, combine by key or position, or choose a branch. Under execution order `v1`, choose-branch requires inputs 1 and 2, so when one never receives items the Merge and everything after it never run and the execution ends without an error; the other modes run with the missing input empty. |
| Loop Over Items | Bounded dynamic parallel Steps when the body is application Steps, or batched Steps when the source paces a rate limit. When the body reaches a Connector Step, process items one at a time with a persisted cursor, because a Connector result carries no item context. |
| Split Out, Aggregate, Sort, Limit, Remove Duplicates, Date & Time | Pure Go transforms. Deduplication across executions needs durable state. |
| Code, Function, Function Item | A pure Go function in an application Step, with golden parity tests. |
| HTTP Request | The dedicated connector operation for that provider. A call to an organization-controlled internal service follows the [internal connector library decision](connector-architecture.md#internal-connector-library-decision); check the catalog before planning a generic HTTP connector, which the current release does not include. |
| Wait (`resume` timeInterval) | An application Step whose WaitFor returns `dex.Until(dex.Timer(amount × unit))`, measured from when that Step starts, as n8n measures from when the Wait node runs; its Execute builds the next Step's input. A Connector Step cannot wait. Never compute or persist a deadline in WaitFor. n8n resumes an overdue wait whenever the instance comes back, however late, so give the waiting Step an explicit Execute retry total duration that covers the longest tolerated Worker outage, or an Execute-failure route that records the outcome. |
| Wait (`resume` specificTime) | An earlier application Step's Execute computes the instant from typed input in the workflow timezone; the waiting Step's Timer covers the remaining duration. |
| Wait (`resume` webhook or form) | A Channel or typed RPC resume. Without `limitWaitTime` (off by default), n8n waits forever. |
| Wait used as a delay before reading a submitted job's result | The Dex [Polling pattern](../../dex-sdk/references/core/patterns.md#polling): after the start Step, one long-running Step's Execute owns the whole wait, bounded only by its Execute method timeout and retry total duration, never by a Timer loop. A Connector Step makes one provider call per execution, so without a released operation that waits for the job to finish the row is `blocked` on a connector gap. |
| Execute Workflow | Steps in the same Flow, or an independent top-level Flow under the Core boundary rules. Never a SubFlow by default. |
| No Operation, Sticky Note | No behavior. Mark it `dropped` and check what a note claims. |
| App node, such as Gmail, Google Calendar, or Slack | The released connector operation for the node's resource and operation. |
| LangChain agent, chain, or model | The `llm` connector `generateText` Query or a durable Dex agent; each tool becomes a Step. An agent without tools is one generation: its system message maps to instructions and its `output` to the generated text; an output parser adds a formatting tool, so read the agent at the release. `generateText` takes text messages only, so image input needs a provider connector operation that accepts images. An empty finished generation, or one whose finish reason the connector's wire format does not map, selects `invalidResponse`, where n8n forwards the text; `generateText` joins the text parts of an answer with no separator, and a `truncated` answer still carries the text LangChain would forward, so route it as the source did. Use a provider connector, such as a native Gemini one, only for a provider-native feature. n8n's output parser checks the answer after generation and fails the node on a mismatch, while `llm` structured output is enforced by the provider, so a schema-mismatch failure path in the source is a `diverged` row. Record every exit path of a tools agent at the release, including the max-iterations stop (10 iterations unless `options.maxIterations` is set), whose fixed text skips the output parser. Model sub-nodes can retry inside the model client and set their own timeout: read the node's options and the LangChain library at the version n8n pins. |

When the source waits for hours or days, or the port adds a long-lived Flow
such as a scheduler, choose a versioning strategy from the Dex SDK
[versioning guide](../../dex-sdk/references/core/versioning.md) before the
first deploy, since Flows will be open across deploys: keep Step, RPC,
Attribute, and Channel names and payloads additive, and treat the run Flow's
start input as a contract the open scheduler writes.

A source trigger's admission rules, such as a chat or user allow-list or the
subscribed event types, map to the connector Trigger's `TriggerFilter`, which
consumes a rejected event without starting a Flow. A check in the start Step
instead creates failed Flows, which is a `diverged` row.

## Schedule rules

- A `scheduleTrigger` lists rules in `rule.interval[]`. Each rule's `field` is
  one of these:
  - `seconds` with `secondsInterval`
  - `minutes` with `minutesInterval`
  - `hours` with `hoursInterval` and `triggerAtMinute`
  - `days` with `daysInterval`, `triggerAtHour`, and `triggerAtMinute`
  - `weeks` with `weeksInterval`, `triggerAtDay`, and a time
  - `months` with `monthsInterval`, `triggerAtDayOfMonth`, and a time
  - `cronExpression` with `expression`

  An absent `field` means `days`, and an omitted hour or minute is 0, because
  n8n fills declared defaults before the node runs (`workflow.ts`), so the
  Schedule node's own fallback for an absent minute or hour never applies. Each rule fires
  independently. The second depends on the release: 0 on early 1.x releases,
  then a random second picked at each activation, and from n8n 2.19 a stable
  second derived from the workflow and node IDs; `seconds` and cron rules are
  never jittered. Check the Schedule node's `GenericFunctions.ts` at the
  release.
- A node name such as "Every morning" proves nothing. The inventory reports a
  mismatch between the name and the rule.
- Compare the schedule's interval with the time window each execution reads.
  When the window is longer, such as an hourly rule reading today's events,
  every execution repeats the same effects for the same records. That
  repetition is source behavior to record and decide on.
- Derive the run Flow ID from the scheduler's own Flow ID plus the occurrence,
  such as `<scheduler-flow-id>-run-2026-10-06` for a daily rule, and bind the
  request ID to that start. Flow IDs cannot contain `/`, `$`, or `:`, so do not
  embed an RFC 3339 time; a sub-daily rule appends a colon-free UTC time to the
  second, such as `<scheduler-flow-id>-run-20261006T140930Z`, because a local
  time repeats in
  the DST fall-back hour. Scoping by the scheduler keeps two schedulers, such
  as one per calendar, from deduplicating each other's runs. Start with
  `IDReuseDisallow` and ignore-already-started, so a retried start lands on the
  same run.
- A manual "Execute workflow" maps to a run-now Action with its own identity,
  such as `<scheduler-flow-id>-run-manual-<request ID>`, so it never attaches
  to a closed occurrence run. Record it as an added capability. The Action
  publishes a typed request carrying that request ID to a Channel in the
  waiting Step's AnyOf, and the waiting Step's Execute reads which condition
  fired: a Timer starts the occurrence run, a reschedule recomputes and
  re-arms, and a run-now request starts the manual run and leaves the pending
  occurrence untouched. RPC and Action handlers validate, write Attributes,
  and publish; they never start a Flow. Record whether repeated requests
  coalesce while one is queued.
- Decide what happens to an occurrence the Worker reaches late, such as after
  downtime. n8n's default scheduler never runs an occurrence missed while the
  instance was down, but a running process whose timer fires late, such as
  after a blocked event loop or a host suspend, still fires one tick however
  late, without replaying ticks missed during the stall; skip one that is more than a bounded
  lateness old and record both cases. The opt-in durable scheduler (from
  n8n 2.34) takes over only when `N8N_SCHEDULER_ENABLED` and
  `N8N_USE_WORKFLOW_PUBLICATION_SERVICE` are both on, and can run the latest
  missed occurrence: on 2.34 and 2.35 it always does, and from Schedule Trigger
  1.4 (n8n 2.36), `misfirePolicy` (default skip) and
  `misfireGraceSeconds` (0 means the instance's `N8N_SCHEDULER_MISFIRE_GRACE`,
  default 60 seconds) decide it. A lateness bound other than the source's is a
  `diverged` row.
- From n8n 2.19 (opt-in) and 2.28 (always), n8n deduplicates scheduled
  executions per workflow, node, and scheduled time; per-occurrence run
  identity reproduces that. Executions for different occurrences can still
  overlap.
- When an operator can edit a field that shapes the schedule, such as the zone
  or the time, add a reschedule Action that publishes to a Channel in the
  waiting Step's AnyOf, so its Execute recomputes the next occurrence: an
  Attribute edit does not wake a Timer. Use a date-only run ID only for a fixed
  daily schedule, because a same-day edit to a later time would otherwise be
  deduplicated into the earlier run.
- Give the waiting Step an explicit long Execute retry total duration. The
  default is four hours, so a longer Worker outage across an occurrence would
  fail that Step and end the scheduler.
- Decide how local times that a DST change skips or repeats are handled before
  you compute occurrences. The source's own DST behavior comes from the cron
  library at the version n8n pins, which can fire twice in a repeated hour or
  move a skipped time; capture it with `n8n_schedule_golden.mjs` and record it
  as the faithful default.

## Version-dependent defaults to confirm

| Node | What to confirm at the exported version |
| --- | --- |
| Set | Before 3.3, input fields pass through next to the set fields; from 3.3 they are dropped unless `includeOtherFields` is on. A string field that resolves to null or undefined becomes the text `null` or `undefined` at 3.0, fails the node at 3.1 unless `ignoreConversionErrors` is on, and becomes null from 3.2; a field of another type becomes null. Binary data is dropped through 3.3 unless `includeBinary` is set, and from 3.4 is kept while input fields are kept. |
| Gmail send | From 2.1, the footer "This email was sent automatically with n8n" is appended unless `options.appendAttribution` is false; reply never appends it. `emailType` defaults to html (text before n8n 1.10), and html mail has no text/plain part at all. The message is trimmed. The mail composer turns line breaks in the subject into spaces; check it at the nodemailer version n8n pins. The Dex `gmail` `sendMessage` requires a non-blank text body and builds its own headers, so an HTML-only source message is a recorded difference. |
| Send Email | From 2.1, the same footer is appended unless `appendAttribution` is false. The Dex email connector sends plain text only, from the sender fixed on its connection, and its connection needs IMAP and SMTP hosts even for sending; read its Go types at the tag. |
| Telegram send message | `parse_mode` is Markdown when unset, so the text is parsed as markup. From 1.1, "This message was sent automatically with n8n" is appended for Markdown or HTML unless `appendAttribution` is false; from 1.2, link previews are off by default. |
| Slack post or update | From 2.1, an "Automated with this n8n workflow" link is appended unless `includeLinkToWorkflow` is false. |
| Microsoft Teams create message | From 1.1, a "Powered by this n8n workflow" link is appended, as HTML, unless `includeLinkToWorkflow` is false. |
| Send-and-wait operations | They append an n8n attribution by default; check the release and the node. |
| Telegram Trigger | It accepts updates from anyone. Chat and user restrictions exist from 1.2; 1.1 shows them but ignores them. With the download option on, a message carrying a photo, document, or video is returned before those restrictions run, so they do not apply to media. Its restrictions and event-type filters map to the application's `TriggerFilter`. |
| Google Calendar event `getAll` | Without `returnAll`, it returns one page of `limit` events (default 50). The order is unspecified unless `options.orderBy` is set. |
| HTTP Request | At every version, every item's request starts at once and all are awaited together; batching options only space the starts. n8n hands the rendered URL to its HTTP client, which parses it as a URL: a `#` starts a fragment that is never sent, dropping every later query parameter, tab, CR, and LF are deleted, and unsafe characters are percent-encoded; the expression harness prints the URL actually sent. From v4, with `options.redirect` unset, redirects are followed up to the HTTP client's limit, and before v4.4 credentials are also sent on a cross-origin redirect; below v4, a redirect is followed only when the option is set. A non-2xx response fails the node after every request settles, unless `neverError` is on. From 3, without `options.timeout`, the timeout is 300,000 ms. Values concatenated into `url` are not form-encoded: spaces become `%20`, but `&`, `#`, and `+` in a value change the query. |
| Webhook | `httpMethod` defaults to GET, `authentication` to none, and `responseMode` to `onReceived`, which answers 200 `{"message":"Workflow was started"}`. A JSON body is parsed into `body`; urlencoded and multipart fields land in `body` as strings, multipart files in `binary`; from 1.1 an unparsed body becomes a binary file. |
| Wait | At 1, `amount` defaults to 1 and `unit` to hours; from 1.1, to 5 and seconds. A day is exactly 86,400 seconds, not a calendar day. |
| IF, Filter, Switch | IF and Filter from 2, and Switch from 3: strict type validation, case sensitivity, and the AND/OR combinator come from `conditions.options` and `combinator`; see [parameter expressions](#parameter-expressions) for null handling. |
| Code | The default mode, and the language (`javaScript` or Python). Python output cannot be captured by the golden harness. |
| LangChain model and option nodes | Option defaults can change between releases without a `typeVersion` change; record the release. |

## Dex redesign patterns

The inventory proposes these when it detects the n8n shape. Each one changes
the source's structure to serve the same intent; the user adopts or defers it
through the single mode question in [workflow import](workflow-import.md).

| n8n shape | Dex design | What changes for the user |
| --- | --- | --- |
| A schedule that re-reads a time window and acts on what it finds | One Flow per entity, identified by the provider's entity ID and timed to the moment the intent names; the schedule or a Trigger only discovers entities | Each entity is handled once, at the intended time |
| A read window longer than the schedule interval | The last successful run's instant in an Attribute; read only newer records | Each record is processed once |
| A chain of long Waits | One Flow per entity whose waits are AnyOf(Timer, cancel Channel), with progress in Attributes and Actions that cancel or skip | A sequence can be cancelled and inspected per entity |
| A fixed Wait before reading a submitted job | One Polling Step bounded by its StepOptions, or a provider callback on a Channel | Slow jobs no longer fail, and a paid submit is never repeated |
| Send-and-wait, resume webhooks, and forms | A Dex Web Action with a permission, backed by an RPC and a Channel, with Summary and Display | Approvals are authenticated and recorded |
| Remove Duplicates, node static data, or a table used as workflow state | Flow identity (a stable Flow ID, IDReuseDisallow, and a request ID) with Attributes and AttributeMaps | No state store to maintain; duplicates are rejected at start |
| An error workflow, continue-on-fail, retry-on-fail, or one failure stopping a batch | StepOptions retry policies, Execute-failure recovery Steps, per-item isolation, and uncertain sends routed to recovery | One item's failure no longer stops the rest |
| Fan-out, Merge, and Loop Over Items | Dynamic parallel Steps over an AttributeMap with a Channel-count join and an ordering stated on purpose | Independent branches run concurrently; canvas position no longer decides order |
| An agent output parser, or Code that parses model text | `llm` structured output with typed results and every branch routed | No parse failure after a paid call |
| Literal addresses and IDs, or a configuration Set node | Typed start input and validated Actions | Values are editable without code; no behavior change |
| Chains of Set, IF, and Code nodes | One typed application Step per real commit boundary | Fewer, testable Steps; no behavior change |

Source defects the inventory finds, such as a schedule that contradicts its
name or unescaped provider text in HTML, are proposed as fixes in the same
list.

## Credentials and identities

- Each credential type and account becomes a connection for the matching
  connector. Authorize it in Dex Web **Connectors** for local development; Dex
  has no deployed Connector configuration, so a deployment's source is
  settled separately. When reading and
  sending use different accounts, keep two connections. Same-type nodes with
  different `authentication` settings may act for different accounts.
- A literal key in a parameter, such as an API key in a Set node or a URL, is
  a secret leak, not configuration. A key that travels from workflow data into
  a request URL, header, or body belongs to the connector connection. Follow
  the secure-first rules in
  [workflow import](workflow-import.md#2-secure-the-source-before-mapping-it).
