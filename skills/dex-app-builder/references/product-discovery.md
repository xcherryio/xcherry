# Product discovery

Resolve the business model from the request, retained answers and host context.
A clear request to build the product authorizes routine implementation choices.
Record reasonable defaults and continue; ask only when an unresolved business,
authorization or integration decision materially changes the outcome. Do not ask
users to choose SDK methods, compile fixes or another approval of the same scope.

## Stack checkpoint

Classify the repository before proposing a language or installing anything. A
repository containing only a README, license, or editor files is effectively
empty and defaults to the exact stack described in
[workspace bootstrap](workspace-bootstrap.md): a Go backend on an exact released
Dex Go SDK paired with the installed `dexcli`, plus a React/Vite frontend only
for a confirmed custom UI. Record the selected releases as the stack decision;
do not offer TypeScript, Node, or another SDK language as an equivalent default.

Only an explicit user request can replace the Go backend for a new application.
Before accepting that request, explain that Dex App Builder, strict FDG 2.0,
Dex Web v2 management metadata, and the current Connector SDK require a Go
backend. A requested non-Go backend becomes a standalone `dex-sdk` project
without Connector SDK support, not a modified App Builder default.

## Actor, role, operation, and permission matrix

Inventory actors first, then derive the minimum authorization model. Start
with one `admin` role for the trusted maintainers and operators. A requester,
subscriber, participant, or external system is an actor, not automatically an
authorization role.

| Actor | Authentication boundary | Goal and visibility | Actions and permissions | Role | Evidence for another role |
| --- | --- | --- | --- | --- | --- |
| Trusted maintainer or operator | authenticated host or reverse proxy | all required operational state | granular approval, edit, recovery, and configuration permissions | `admin` | none by default |
| Requester or participant | product-specific | only confirmed participant-facing state | typed start, response, or Action when required | none by default | a distinct authenticated membership boundary plus different visibility or allowed operations |
| External system | Connector or integration identity | only the required exchange | typed Trigger, RPC, or event | none | never derive a human role from an integration identity |

Add another role only when a confirmed, separately authenticated group needs a
different view or a different set of allowed operations. Do not create one role
per actor, lifecycle stage, Action, or permission. Keep Action permissions
granular even when `admin` initially holds all of them. Record authentication,
authorization, audit, and reason requirements, and justify every non-`admin`
role in the confirmation artifact; collapse an unjustified role back into
`admin`.

## Lifecycle questions

Confirm:

- business identity and unique Flow ID source;
- start trigger and duplicate-start behavior;
- typed start input and completion output;
- happy path and terminal business outcomes;
- human decisions, required fields, deadlines, reminders, and escalation;
- retryable provider failures versus business rejection;
- unknown external mutation outcomes and query-first reconciliation;
- cancellation, recovery, cleanup, and operator intervention;
- searchable/indexed fields and detailed display fields;
- sensitive data that must not enter IDs, logs, Streams, or generated artifacts.

## Flow-boundary decisions

Treat three shapes as distinct decisions:

- parallel Steps share one Flow identity and lifecycle;
- independent top-level Flows start separately and coordinate through typed
  RPCs or Channels;
- SubFlows have an explicit parent-child lifecycle.

First capture the data-lifecycle boundary:

| Candidate data or process | Authoritative owner | Retention and cleanup | Independent waits, Timers, and terminal outcomes | Pollution if kept in the existing Flow | RPCs or messages between the Flows per frequent event | Top-level Flow decision |
| --- | --- | --- | --- | --- | --- | --- |
| Product-specific | stable business owner | product-specific | product-specific | unrelated state the existing Flow would retain or coordinate | none, or the split is wrong | keep or split, with evidence |

Use these questions together:

1. Does the data have a different authoritative owner and a different
   retention or cleanup lifecycle?
2. Does the process have its own waits, Timers, and terminal outcomes such as
   `verified`, `expired`, or `cancelled`?
3. Would putting it in an existing Flow force that Flow to retain, clean up, or
   coordinate state unrelated to its primary lifecycle?
4. Would the split make the Flows exchange RPCs or Channel messages on frequent
   events, such as every user operation, every external callback or every poll?

A yes to question 4 rules the split out, whatever the other answers. Every call
between Flows adds entries to both Flows' history, and every RPC, including a
read-only one, is a separate engine operation with its own latency and, on
metered backends, its own cost. A separate Flow that tracks another Flow's
inactivity, for example, needs a message from every operation; keep that
deadline in the Flow where the operations happen. Calls between Flows are fine
for rare events: a start, a completion, a cancellation or an explicit user
command.

When the answers to questions 1 to 3 are collectively yes and the answer to
question 4 is no, create an independent top-level Flow.
Otherwise keep the work in the existing Flow's Steps, Attributes, or
AttributeMaps. Field count, source size, number of stages, retry policy, code
reuse, provider neutrality, or ordinary parallelism do not establish a new
top-level Flow. Keep long-lived authoritative facts in their stable owner and
temporary validation or failure state in its short-lived owner; temporary
state must not pollute the authoritative store.

Then capture the execution-shape decision:

| Candidate work | Same business lifecycle | Parallel-Step or batching option | Observed complexity or more than 200 concurrent Step executions | Explicit SubFlow confirmation | Decision and rejected alternatives |
| --- | --- | --- | --- | --- | --- |
| Product-specific | yes or no | concrete option | measured requirement, not speculation | required before SubFlow implementation | Step, independent top-level Flow, or confirmed SubFlow |

New applications default to no SubFlows. Use static or dynamic parallel Steps,
Channels for joins or quorum, batching, typed RPCs, and ordinary language
helpers or interfaces first. Provider abstraction, research stages, model
calls, code reuse, an independent retry policy, or ordinary bounded fan-out do
not justify a SubFlow.

Propose a SubFlow only after all of these are true:

1. an existing single-Flow design or running system provides evidence, rather
   than a concern that it may become complex later;
2. the main graph is already impractical to review, evolve, or operate, or one
   fan-out genuinely requires more than 200 concurrent Step executions;
3. parallel Steps, batching, Channel coordination, RPCs, and ordinary code
   abstraction have been evaluated and rejected with reasons;
4. child identity, input/output, parent completion, cancellation, failure,
   retry, duplicate submission, and concurrency semantics are defined; and
5. the user explicitly confirms the SubFlow design.

The 200-Step value is an architecture-review threshold, not a claimed Dex
Server limit. Crossing it permits a SubFlow proposal; it never selects one
automatically.

## Storage decision

Default each durable fact to Dex before proposing an external dependency.
Capture a storage decision matrix:

| Fact or collection | Owning Flow/business identity | Retention and cleanup | Reads and writes | Volume/contention | Dex primitive and access path | Proven external-store gap |
| --- | --- | --- | --- | --- | --- | --- |
| Process state | Process Flow ID | follows the process lifecycle | Step and Action updates, status/detail reads | product-specific | typed Attribute plus RPC/index when needed | normally none |
| Keyed or growing records | Stable domain/entity owner | explicit record retention and deletion | exact lookup, bounded page, independent mutation | product-specific | partitioned or chunked AttributeMap with exact loads and locks | only a confirmed query or contention limit |
| Shared domain facts | Stable domain/entity Flow | owned independently of callers | reused by several process/API paths | product-specific | one owner with typed application operations | cross-Flow reuse alone is not a gap |

Do not select PostgreSQL, another database, a cache, an ORM, or a shadow read
model merely because data must persist. Dex already provides durable
Attributes, AttributeMaps, indexes/search, locks, exact instance loading, blob
offload, and optional Attribute Store projections. An Attribute Store
projection still introduces an external database; use it only for a confirmed
query gap, and prefer its server-managed projection over application dual
writes when Dex remains authoritative. When a projection is chosen, record the
Flow ID format with any `rowKey` template, which makes that format a storage
contract, and whether keyed AttributeMap records need rows of their own
(Dex Server v1.6.0 or later; see [row keys and AttributeMap tables](../../dex-sdk/references/core/data-handling.md#row-keys-column-names-and-attributemap-tables)). Require an external store only for a
specific unsupported need such as complex/ad-hoc indexes, full-text or vector
search, high-concurrency access to one hot record, relational joins or
multi-record transactions, or large analytical scans. Record the exact
operation, expected scale/SLO, authority, synchronization, failure, and
reconciliation design. “We may need it later” is not evidence.

## Management UI capability mapping

Business process products commonly need a management interface. Design that
interface through Dex Web v2 first. For each management operation, define its
Indexed Attributes, Summary RPC fields, Display RPC fields, Action RPC input and
metadata, condition, and permission before deciding that a custom surface is
necessary.

| Management need | Dex Web v2 capability |
| --- | --- |
| List and filter business records | Model each record as a Run, expose supported search criteria as Indexed Attributes, and return additional list fields from the Summary RPC. |
| Inspect one record | Return detail fields and UI slots from the Display RPC. |
| Perform a business operation | Register an RPC with Action metadata, a typed input form, current-state condition, and one required permission. |
| Find assigned or actionable work | Use Work Queue permission history for discovery; opening a Run rechecks current Action eligibility. |
| Change simple business fields | Declare editable scalar Attributes in the Display RPC. |
| Inspect progress and failures | Use the Run timeline, Step graph, execution details, and recovery information. |

Record this mapping as a discovery artifact. If a need is fully covered, keep
it in Dex Web v2 even when another need requires Custom UI. If it is not
covered, name the exact missing interaction and its user. A general desire for
an admin portal, dashboard, management backend, or branded shell is not a
capability gap.

## UI-mode decision

Make the UI-mode decision only after completing the management UI capability
mapping and designing the Summary RPC, Display RPC, Action RPCs, Indexed
Attributes, and permissions.

Choose **No custom UI** when those surfaces satisfy operators and maintainers.
The application then needs no frontend of its own. Confirm what supplies
authentication, role-to-permission mapping, tenant isolation, and the trusted
reverse proxy in front of a deployed Dex Web. Those controls may still be
required, but they are not a second process management backend.

Choose **Custom UI** only for a recorded Dex Web v2 capability gap, such as a
participant-facing journey, a fundamentally different navigation model, domain
visualization, or a complex interaction Dex Web does not provide. Record
which requirement forces the custom surface.
When consequential interaction choices remain unresolved, an optional UI
checkpoint clarifies the necessary pages, navigation, fields and actions through
a low-fidelity static wireframe. A clear implementation request already supplies
confirmation for its stated behavior. It does not authorize an
interactive mock, visual system, imagery, animation, or other polish before the
Flow, Connector, and API contract are designed.

## Connector decision

Create a connector capability matrix for every Trigger, Query, Mutation, and
integration UI. Start from the canonical published catalog at
`https://superdurable.github.io/dex-connectors-library/catalog.yaml`. For a
public external product, match each need to an exact released connector
capability—not merely a connector name—then verify the complete contract in the
component tag's immutable `connector.yaml`. Record the catalog URL, connector
ID, capability kind and name, version/tag, immutable manifest URL, and reuse or
gap decision.

If the published catalog or immutable release manifest cannot be verified,
stop connector-dependent implementation and report the blocker. A missing
connector, operation, Trigger, or UI unit with a documented API or official SDK
becomes a `dex-connector-contributor` work item and production release blocker;
it is not permission to install the provider SDK or write local integration
code.

Mark a service internal only when the organization owns and controls it. Ask
whether an internal connector library already exists, whether this application
should contribute to it, and whether the user wants to establish one with the
unified Connector SDK when none exists. Record who owns that decision. Use the
generic HTTP connector only for a controlled internal service when the user
does not choose a reusable internal connector capability.

## Confirmation artifact

Before code, provide:

1. the actor-to-role-to-permission matrix, with every non-`admin` role justified;
2. a numbered lifecycle with decisions and terminal outcomes;
3. the data-lifecycle and execution-shape boundary matrices, including every
   independent top-level Flow, every candidate collapsed into a Step, RPC, or
   helper, and the complete gate for any proposed SubFlow;
4. the management UI capability mapping, including proposed Summary, Display,
   Action, Indexed Attribute, Work Queue, and permission contracts;
5. the confirmed **No custom UI** or **Custom UI** mode and any recorded Dex Web
   v2 capability gap;
6. proposed Flow, Step, state, message, timer, RPC, and connector boundaries;
7. the storage decision matrix and any evidence-backed external-store gap;
8. connector capability reuse, public fork/PR, internal-library decision, and
   release status;
9. unresolved tradeoffs.

Ask for explicit confirmation. A casual discussion response is not approval to implement.
