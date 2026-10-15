# Stage 1: confirm the business process and application surface

For unresolved product scope or actor choices, use the relevant section of
[product discovery](product-discovery.md). Do not reread discovery after those
facts have been recorded in the application's contract.

Read the request and retained context first. Resolve the following design facts
from the supplied intent, existing host and source. Discuss only unresolved facts
that materially change the product; a short business request need not become a
questionnaire before implementation. Identify:

- process maintainer;
- managers, operators, approvers, terminal users, participants, and external
  systems as actors before deriving roles;
- one `admin` role by default, every actor's authentication boundary, and the
  distinct membership, visibility, or operation difference that would justify
  any additional role;
- each human Action's stable permission, kept granular even when `admin`
  initially holds every permission;
- external systems that trigger or participate in the process;
- whether each integration is a public external product or an
  organization-controlled internal service;
- start triggers, inputs, outputs, deadlines, waits, approvals, retries, recovery, audit, search, and sensitive data;
- durable facts, their owning business identity, expected read/write paths,
  cross-Flow reuse, volume, contention, search, retention, and cleanup;
- candidate top-level Flows, their independent waits, Timers, and terminal
  outcomes, whether embedding their temporary state would pollute a
  longer-lived owner, and the RPCs or messages between Flows that each
  business event would cause;
- parallel-Step alternatives for every proposed SubFlow and the observed
  complexity or greater-than-200 concurrent-Step requirement behind it;
- every management list, detail, action, assignment, edit, progress, and
  recovery need;
- what authenticates users and maps roles to trusted permissions in front of
  a deployed Dex Web, such as an existing identity provider or reverse proxy;
- whether the user needs any custom process UI beyond Dex Web.

Produce a compact actor/role/operation/permission matrix, lifecycle proposal,
data-lifecycle and execution-shape boundary matrices, storage decision matrix,
management UI capability mapping, UI decision, and connector capability
matrix. Complete the management UI capability mapping before making
the UI decision. First design the Indexed Attributes, Summary RPC, Display RPC,
Action RPC metadata and inputs, and permissions that each management operation
would require. Then map each operation to Run search/list, run detail, Actions,
Work Queue, editable display fields, or timeline and graph inspection. For every
durable fact, record its owner, access pattern, scale/contention expectation,
Dex primitive, and any proven reason an external store is required. For every
integration, record its classification, required Trigger/Query/Mutation/UI capabilities, matching
released connector capability, and any contribution or internal-library gap.
For each LLM use, record whether it is generic, a named model, a named
provider, or provider-native.
Actors describe participants; roles group authenticated people with the same
visibility and allowed operations; permissions describe individual Actions.
Default to `admin` or the existing host authorization model and justify every
additional role. Existing implementation authorization and stated requirements
count as confirmation; do not ask again merely to approve routine design defaults. The boundary matrices must distinguish
parallel Steps, independently started top-level Flows that communicate through
typed RPCs or Channels, and explicitly confirmed parent-child SubFlows.

If the process begins from Slack, email, a webhook, or another external source, model it as a Connector Trigger. The application decides whether the event starts a Flow or invokes a typed RPC. Read [connector architecture](connector-architecture.md) whenever an external integration is involved.
