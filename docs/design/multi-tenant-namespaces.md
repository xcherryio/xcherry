# Multi-tenant namespaces

Status: proposed design, not implemented. This is an engine design document: it
describes stored workflow identifiers, task queues, activities and search attributes,
which are Dex internals. Source references are to commit `6f5c2a0e3`.

## Problem

A Dex server serves one Temporal (or Cadence) namespace, and every caller of its API can
reach every Flow in that namespace. An operator who runs many independent applications
therefore needs one namespace and one Dex server per application. Hosted Temporal limits
namespaces per account, and each server costs a Pod even while its application is idle.
This design lets one Dex server, with its shared Workers, serve many **tenants** in one
namespace. Each tenant sees exactly what it would see with a namespace of its own.

Goals:

- A tenant is a first-class server concept. The server derives it from the caller's
  credential on every request. No request field can name or change it.
- Applications and SDKs keep their FlowIDs unchanged. The server maps them to stored
  identifiers and back, so a tenant cannot address another tenant's Flows.
- Every namespace-wide resource is partitioned by tenant or justified as needing none:
  internal RPCs, visibility, search attribute registration, Worker dispatch and
  capacity, blobs, Attribute Stores, Streams, Dex Web and limits.
- Single-tenant servers behave exactly as today.

Non-goals:

- moving a tenant between namespaces;
- tenants on the Cadence backend, which tenant mode refuses at startup;
- per-tenant engine retention, which stays a namespace setting shared by every tenant.

## Today

| Concern | Behavior | Source |
|---|---|---|
| Authentication | None. One gRPC server serves FlowService and InternalService. Its interceptor chain has only recover and log | `server/service/api/routers.go:106-118` |
| Internal RPC | `InternalService.DumpFlowForContinueAsNew` is served on the same listener as FlowService and takes any FlowID | `routers.go:117`, `server/service/api/service.go:1807` |
| Search attribute registration | `SyncAttributeIndexes` creates any requested search attribute in the namespace unless `attributeIndexesManagedExternally` is set | `service.go:1321`, `server/service/indexsync/synchronizer.go:71-80` |
| Flow identity | The stored WorkflowID is the request's FlowID. Server-generated SubFlow IDs are `SubFlow:<parent FlowID>-<step execution ID>-<index>` | `service.go:221-223`, `server/service/const.go:69-71`; `server/service/identifiers.go:19-33` reserves `/`, `$` and `:` |
| Task queue | One queue, `DEFAULT`, polled by the interpreter Workers of every server of the namespace | `const.go:24`, `server/service/bootstrap/temporal.go:119` |
| Worker target | Each Flow persists the address of the application Worker it dispatches to. A request may override it through `FlowConfig.worker_target` | `service.go:200-219,291-306,1009`; `protos/dex.proto:320-338` |
| Visibility | `SearchFlows` passes the caller's query through and ANDs a status clause by string concatenation | `service.go:1287-1320` |
| Search attributes written by the server | `FlowType`, `DexParentFlowID`, `ActiveStepTypes`, `DexWorkQueuePermissions` (Keyword array) | `const.go:43-46`, `server/service/interpreter/persistence.go:257-265` |
| Blobs | Key `<namespace>/<yymmdd>$<escaped FlowID>[$<run ID>]/<blob ID>`. Cleanup lists workflow paths, describes the FlowID parsed from each path, and deletes the path's objects on NotFound | `server/service/common/blobstore/store_impl.go:45,125,511`, `interfaces.go:38-58`, `server/service/interpreter/activityImpl.go:1066-1090` |
| Streams | Budget scope `(flowType, streamName)`, shared by every Flow instance of that type. The capacity comes from the write request. The `memory` backend lives in one process | `server/service/common/streamstore/store.go:448-452`, `memory.go:122-130`, `protos/dex.proto:398-405` |
| Attribute Stores | A Flow selects server-configured stores by name; rows are keyed by FlowID | `server/config/config.go:251-274`, `service.go:1075` |
| Worker connections | One pool per process, 1,000 connections by default | `server/config/config.go:66`, `server/service/common/workerclient/worker_client_pool.go:428` |
| Dex Web | Renders definitions from one `flowRenderingDirectory` and lists all Flows | `server/config/config.go:338` |

## Design

### Configuration

A new `tenancy` section, empty by default (single-tenant):

```yaml
tenancy:
  mode: multi                       # single (default) | multi
  authenticators:
    - kind: jwt
      issuer: https://issuer.example
      audiences: [dex]
      jwksURL: https://issuer.example/keys   # or OIDC discovery from issuer
      # The whole subject must match; the group "tenant" is the tenant ID.
      subjectPattern: '^system:serviceaccount:apps-(?P<tenant>[a-z0-9]{1,32}):app$'
      claims: {namespace: 'apps-{{ .tenant }}'}
    - kind: staticTokens            # SHA-256 hashes, file reloaded on change
      file: /etc/dex/tenant-tokens.yaml
  operators:                        # principals allowed on TenantAdminService only
    - kind: jwt
      subject: system:serviceaccount:platform:controller
  internal:
    listen: 127.0.0.1:8803          # InternalService only
    tokenFile: /etc/dex/internal-token
  workerTarget:
    default: 'worker.{{ .namespace }}.svc.cluster.local:9000'
    allowed: '^worker\.{{ .namespace }}\.svc\.cluster\.local:9000$'
  wake: {url: http://operator.example/worker-wake, budget: 120s}   # optional
  limits: {see "Limits"}
```

Tenant mode refuses to start when any of these holds:

- the backend is Cadence;
- `attributeIndexesManagedExternally` is false;
- an Attribute Store is not tenant-scoped;
- a tenant pattern can capture anything outside `[a-z0-9]{1,32}`.

### Authentication and principals

- Every FlowService RPC carries `authorization: Bearer <credential>` metadata. A new
  interceptor runs first in the chain and resolves one **principal**:
  - `tenant{id, claims}` for a tenant credential;
  - `operator` for an operator credential, which is allowed only on
    `TenantAdminService`;
  - `internal` for the server's own Workers, which is allowed only on
    `InternalService`.

  A missing or invalid credential is `UNAUTHENTICATED`. A principal on a service it is
  not allowed on is `PERMISSION_DENIED`.
- `jwt` verifies the signature, issuer, audience and expiry locally against cached keys.
  It refreshes the key set at most once a minute; an unknown key ID triggers one bounded
  refresh. The tenant is derived **fail-closed**: `subjectPattern` must match the whole
  subject and capture `tenant`. A subject that does not match is `UNAUTHENTICATED`.
  Transformations that pass a non-matching value through unchanged, such as trimming a
  prefix, are not offered.
- `staticTokens` maps the SHA-256 of a token to a tenant, for deployments without an
  identity provider.
- Tenant IDs match `[a-z0-9]{1,32}`. They never come from a request field.
- No authenticator makes a remote call per request.
- Credential rotation belongs to the issuer. Projected tokens rotate on their own. A
  static token is replaced by adding the new hash before removing the old one.
- **Tombstones**: `TenantAdminService.TombstoneTenant` adds the tenant ID to one tombstone
  object in the blob store, `<namespace>/_tenancy/tombstones`, written with a conditional
  PUT. Every replica rereads that one object every 10 seconds, whatever the number of
  tenants. A tombstoned tenant is `PERMISSION_DENIED` on every call within 10 seconds.
  The tombstone is removed `tombstoneRetention` (default 2 hours, at least the longest
  credential lifetime) after its purge reported `done`. After that, nothing of the tenant
  remains and no credential issued before the tombstone is valid, so the ID may be
  reused. Offline JWT validation would otherwise accept a valid one-hour token after the
  tenant was deleted, so purge requires a tombstone first.

### Every RPC and its tenant rule

`T` is the tenant of the principal. "Maps" means the server maps FlowIDs in to
`T:<T>:<FlowID>` and out again ([Identity mapping](#identity-mapping)).

| RPC | Principal | Tenant rule | Test |
|---|---|---|---|
| FlowService.GetServerInfo, HealthCheck | tenant | no tenant data | rejects a missing credential |
| StartFlow | tenant | maps; sets `DexTenantID = T`; resolves the worker target for T; refuses a `DexTenantID` attribute write | same FlowID in two tenants |
| PublishToChannel, GetChannelMessages, DeleteChannelMessage | tenant | maps | another tenant's FlowID is NotFound |
| WriteStream, ReadStream, ListStreamMessages | tenant | maps; Stream scope includes T; capacity capped | separate budgets for equal Flow type and Stream name |
| StopFlow, ResetFlow, SkipTimer, TriggerContinueAsNew | tenant | maps | another tenant's FlowID is NotFound |
| GetAttributes, SetAttributes, WaitForAttribute | tenant | maps; refuses a `DexTenantID` index key | refuses the index key |
| LoadBlobs | tenant | locators must lie under T's blob prefix | a forged locator is refused |
| WaitForFlow, WaitForStepCompletion, WaitForHistoryEvent | tenant | maps; SubFlow IDs in results map out | SubFlow result IDs unprefixed |
| GetFlowSummary, GetFlowState, GetHistoryEvents | tenant | maps; IDs in events map out; a SubFlow of another tenant cannot appear | history shows only unprefixed IDs |
| SearchFlows | tenant | query parsed, rewritten and ANDed with `DexTenantID = T` ([Visibility](#visibility)) | injection attempts |
| InvokeRPC | tenant | maps | another tenant's FlowID is NotFound |
| UpdateFlowConfig | tenant | maps; the worker target is resolved for T | another tenant's target is refused |
| SyncAttributeIndexes | tenant | `PERMISSION_DENIED`: search attributes are registered by the operator, outside Dex (`attributeIndexesManagedExternally` is required) | refused in tenant mode |
| InternalService.DumpFlowForContinueAsNew | internal | served only on `tenancy.internal.listen`, never on the public listener; the caller's token must equal `internal.tokenFile` | a tenant credential on the public port gets `UNIMPLEMENTED`; on the internal port without the token, `UNAUTHENTICATED` |
| TenantAdminService.TombstoneTenant, PurgeTenant, DescribeTenant | operator | names the tenant explicitly; never returns Flow data | a tenant credential is refused |
| WorkerService (the server calls the Worker) | n/a | the server sends only mapped-out IDs of the Flow's own tenant to the tenant's resolved target | dispatch never reaches another tenant's Worker |
| grpc.health.v1.Health | none | no tenant data | unchanged |

### Identity mapping

- **Stored ID**: the WorkflowID of a tenant's Flow is `T:<tenant>:<FlowID>`. Applications
  cannot create it, because FlowIDs never contain `:` (`identifiers.go:33`).
- **SubFlows**: the application-visible SubFlow ID keeps today's form built from the
  parent's **application** FlowID, `SubFlow:<parent FlowID>-<step execution ID>-<index>`.
  Its stored ID is `T:<tenant>:` plus that. The prefix appears once, however deep the
  nesting.
- **Mapping**: the server maps in on every RPC in the table that takes a FlowID, parent
  FlowID or SubFlow ID. It maps out on every response and Worker request that carries
  one, including `DexParentFlowID` values. A stored identifier with another tenant's
  prefix is never mapped out: the server treats it as not found.
- **Tenant in the workflow**: `InterpreterWorkflowInput` gains `tenant_id`, which the
  server sets at start and copies to SubFlows. The workflow and its activities read the
  tenant from the input, never by parsing. An activity rejects a dispatch whose input
  tenant disagrees with its WorkflowID prefix.
- **Length**: Temporal limits identifiers to 1,000 bytes
  ([Temporal Cloud limits](https://docs.temporal.io/cloud/limits), 2026-10-12). The prefix
  is at most 36 bytes (`T:` + 32 + `:`), so tenant mode limits a FlowID to 964 bytes. A
  SubFlow whose stored ID would exceed 1,000 bytes fails its Step with
  `INVALID_ARGUMENT` before any start. Nesting already grows SubFlow IDs today; the prefix
  adds no growth per level.

### Visibility

- **Tenant attribute**: a Keyword search attribute `DexTenantID` is set by the server on
  every start and SubFlow start. An Attribute write with that index key is
  `INVALID_ARGUMENT`.
- **Query rewriting**: `SearchFlows` and every server-side list parse the caller's query
  into an AST with the visibility grammar. String concatenation would let a query such as
  `a = 1) OR (b = 2` escape the tenant clause.
  - The server accepts only comparisons, `IN`, `BETWEEN`, `STARTS_WITH`, `IS NULL`, `AND`,
    `OR`, `NOT`, parentheses, `ORDER BY` and literals. Any other construct is
    `INVALID_ARGUMENT`.
  - It maps `WorkflowId` and `DexParentFlowID` literals in, refuses any reference to
    `DexTenantID`, and **serializes a new query from the AST**:
    `(<serialized query>) AND DexTenantID = "<T>"` plus today's status clause.
  - Each returned entry's tenant prefix is checked again before it is mapped out.
- **Internal workflows**: server-internal workflows (blob cleanup, tombstones) carry no
  `DexTenantID` and never match a tenant query.
- **Registration**: search attributes are registered only by the operator, outside Dex,
  because tenant mode requires `attributeIndexesManagedExternally`. A tenant cannot
  create namespace-wide attributes or use up their slots.
  - Budget: Temporal Cloud allows 40 Keyword and 5 KeywordList custom attributes per
    namespace (same source).
  - Dex uses 3 Keyword attributes (`FlowType`, `DexParentFlowID`, `DexTenantID`) and 2
    KeywordList attributes (`ActiveStepTypes`, `DexWorkQueuePermissions`).
  - Application index slots are namespace-wide and shared by every tenant with the same
    types. The tenant clause keeps their values apart.

### Worker dispatch and capacity

- **Shared Workers**: every interpreter Worker of the namespace polls the one task queue,
  so adding a tenant needs no redeploy, no new task queue and no Worker registration.
- **Worker target**: tenant isolation is enforced at the worker target.
  - At start, `UpdateFlowConfig` and SubFlow start, the server resolves the Flow's target
    from the principal: `workerTarget.default` rendered with the principal's claims, or a
    requested target that matches `workerTarget.allowed` rendered with the same claims.
  - Any other target is `PERMISSION_DENIED`. An operator may set `allowed` to exactly one
    stable endpoint per tenant.
  - At each dispatch, the activity matches the persisted target against the pattern of
    its input tenant again. So **a Flow of one tenant is never dispatched to another
    tenant's Worker**, even after a configuration change.
- **Fairness**: the server sets the Temporal **fairness key** of every workflow and
  activity task to the tenant ID. Temporal's task queue fairness gives each key a virtual
  queue served round-robin within the shared queue. It is generally available on
  Temporal Cloud since May 2026 as a paid, per-namespace setting, and on self-hosted
  servers through `matching.enableFairness`
  ([Task Queue Priority and Fairness](https://docs.temporal.io/develop/task-queue-priority-fairness),
  2026-10-12). Fairness spreads dispatch but is not hard isolation, so the server also
  enforces the next two bounds.
- **Concurrent dispatch cap**: each process holds at most
  `limits.maxConcurrentDispatchesPerTenant` (default 32) WaitFor, Execute and Worker RPC
  dispatches per tenant. A dispatch over the cap waits for a slot inside the activity,
  heartbeating, without counting an attempt. Long Executes and WaitFors of one tenant
  therefore cannot occupy every interpreter slot.
- **Worker connections**: the pool keeps its global bound (`maxWorkerConnections`) and
  adds `limits.maxWorkerConnectionsPerTenant`. Idle connections close after
  `workerConnectionIdleTimeout`.
- **Wake hook** (optional): this lets an operator scale an idle application to zero.
  - When a dispatch dial fails with `Unavailable`, or the target resolves to no address,
    and `wake.url` is set, the server POSTs `{tenant, target, kind: wake}`. It does so at
    most once per target per 10 seconds, authenticated with the internal token.
  - The activity then **holds the dispatch**: it redials every 2 seconds, heartbeating,
    for up to `wake.budget`, **without counting an attempt**. Only after the budget
    passes does the attempt fail into the Step's retry policy.
  - After a successful dispatch the server POSTs `kind: active`, at most once per target
    per minute, so the operator can count Worker activity as use.
  - The hook is best effort: a failed POST is logged.

### Blobs

- **Key**: with a tenant, the object key is
  `<s3KeyPrefix><namespace>/<tenant>/<yymmdd>$<escaped app FlowID>[$<run ID>]/<blob ID>`.
  `s3KeyPrefix` is a new per-storage option, empty by default. The tenant sits before the
  date, so every tenant has one prefix that IAM can scope and purge can delete.
- **Cleanup**: the cleanup workflow lists tenants under `<namespace>/` with a `/`
  delimiter, then workflow paths per tenant.
  - It rebuilds the **stored** ID `T:<tenant>:<app FlowID>` from the path's tenant segment
    and FlowID, and describes that ID. It never describes the application FlowID. With
    the application FlowID, every describe would answer NotFound and delete the live blobs
    of every tenant.
  - A path whose tenant segment fails the tenant pattern is skipped and logged, never
    deleted.
  - Deletion also requires the describe to answer NotFound for the stored ID twice, 60
    seconds apart, which guards against visibility lag.
- **Reads**: `LoadBlobs` and the attribute blob cache refuse a locator outside the
  caller's tenant prefix.

### Attribute Stores

- A store entry gains `tenantColumn`. In tenant mode every configured store must name
  one.
- The server writes the tenant ID into that column and the application FlowID into
  `flowIdColumn`, and makes the pair the row key.
- A Flow may select only stores listed in `tenancy.attributeStores` (all tenant-scoped
  stores by default).

### Streams

- **Scope**: the budget scope becomes `(tenant, flowType, streamName)`. Redis keys gain the
  prefix `t:<tenant>:`.
- **Caps**: `limits.maxStreamCapacityBytes` caps the capacity a write requests.
  `limits.maxStreamBytesPerTenant` caps a tenant's buffered bytes per process.
- **Replicas**: the `memory` backend is per process. A namespace served by more than one
  replica needs the `redis` backend today, or Stream routing.
- **Stream routing (STREAMROUTE)**: each replica owns the Streams of the FlowIDs that a
  consistent hash over the live replica set assigns to it. A replica that receives a
  Stream read or write for a FlowID it does not own forwards it to the owner over the
  internal listener. The replica set comes from the headless Service of the server
  Deployment. When the set changes, ownership moves and the moved Streams restart empty.
  Readers resume from the Flow's durable state, as after any Stream loss.
- Streams stay best effort.

### Flow definitions and Dex Web

- **Flow types**: the server holds no Flow definitions. Workers define Flows, and the
  interpreter dispatches by Flow type to the Flow's worker target. A tenant's types never
  reach another tenant's Worker, so Flow types need no prefix: two tenants may both
  define `OrderFlow`.
- **Dex Web**: it renders from one directory and lists the whole namespace. In tenant mode
  it starts only with `web.tenant` set, and then serves that one tenant through its own
  tenant credential. Otherwise it is disabled.
- **dexcli**: it authenticates with a tenant credential like any other client.

### Timers, Channels, RPCs and schedules

All of these are per Flow, so the identity mapping isolates them. Dex has no namespace
schedules.

### Limits

| Limit | Default | Why |
|---|---|---|
| `startFlowsPerSecond` per tenant | 20, burst 100 | each start is engine actions |
| `writesPerSecond` per tenant (RPC, Channel, Attribute, Stream writes) | 100, burst 500 | engine actions |
| `searchesPerSecond` per tenant | 1, burst 5 | Temporal Cloud allows **30 visibility calls per second per namespace, not configurable**, shared by every tenant (Temporal Cloud limits, 2026-10-12) |
| `maxConcurrentDispatchesPerTenant` | 32 | interpreter capacity |
| `maxStreamCapacityBytes`, `maxStreamBytesPerTenant` | 1 MiB, 8 MiB | memory per process |
| `maxWorkerConnectionsPerTenant` | 8 | pool fairness |
| `purgeExecutionsPerSecond` per namespace | 10 | purge must not take a large share of the namespace's actions |

- The limits are token buckets and semaphores per process, held in an LRU bounded at
  100,000 tenants. With N replicas the effective limit is N times the setting. A rejected
  call is `RESOURCE_EXHAUSTED`.
- Temporal Cloud's namespace action rate (500 actions per second by default, scaling on
  demand; same source) is shared by every tenant. These limits and the fairness key keep
  one tenant from taking all of it. Two tenants at their full start and write rates can
  still use most of a namespace's default rate. That is the operator's sizing decision:
  the number of tenants per namespace, and the rate limits themselves.

### Tenant purge

`TenantAdminService.PurgeTenant(tenant)` is for operators only, requires a tombstone, is
idempotent and returns progress. The caller repeats it until it reports `done`. Each call:

1. terminates up to 100 open executions of the tenant, found by the tenant query;
2. deletes up to 100 closed executions;
3. deletes up to 1,000 blob objects under the tenant prefix, then the tenant's Stream keys
   and Attribute Store rows.

Pacing:

- Executions are processed at `purgeExecutionsPerSecond`. Batch operations are not used,
  because Temporal Cloud runs one batch job per namespace at a time (same source).
- Visibility is eventually consistent. `done` is reported only after two tenant queries,
  60 seconds apart, both find nothing.
- A tenant ID can be reused only after its tombstone is removed ([Authentication and
  principals](#authentication-and-principals)). Until then, its credentials are refused.

### Compatibility

- Single-tenant mode is unchanged.
- Tenant mode starts on an empty namespace. It refuses to start when a sample list finds
  executions without `DexTenantID`, because migrating existing un-prefixed Flows is out
  of scope.

## API changes

- `protos/dex.proto`:
  - new `TenantAdminService { TombstoneTenant, PurgeTenant, DescribeTenant }`;
  - `InterpreterWorkflowInput.tenant_id`;
  - FlowService messages are unchanged.
- SDKs:
  - a client option for a credential source. In sdk-go this is `ClientOptions.Credential`,
    a token file reread when it changes and sent as per-RPC metadata;
  - the Worker's client for in-Step calls inherits it;
  - other SDKs follow in their own releases.

## Tests

Integration tests in `server/integ/`, against a real Temporal dev server with fairness
enabled, plus the SDK integration suites:

- **One test per row** of [Every RPC and its tenant rule](#every-rpc-and-its-tenant-rule),
  with two tenants that start the same FlowID and the same Flow type.
- **Internal service**: InternalService is unreachable on the public listener, and
  refused without the internal token on the internal one.
- **Search**: `SyncAttributeIndexes` is refused in tenant mode. SearchFlows rejects
  unknown constructs and parenthesis or `OR` injection, and returns only the caller's Flows
  with unprefixed IDs.
- **SubFlows**: nested SubFlows show unprefixed IDs to the application and carry exactly
  one prefix when stored. A SubFlow over the length limit fails its Step before starting.
- **Blob cleanup**:
  - it keeps the blobs of a running Flow in each of two tenants;
  - it deletes those of a deleted Flow only after two NotFound answers;
  - it skips a path with an invalid tenant segment.
- **Dispatch**: a requested worker target outside the tenant's pattern is refused at
  start, update and SubFlow start, and a dispatch never reaches another tenant's Worker.
- **Dispatch cap**: one tenant's long Executes are capped, and another tenant's Steps
  still run.
- **Wake**: the dispatch is held within the wake budget without counting an attempt, then
  completes after the Worker starts. `active` is sent at most once a minute per target.
- **Streams**: two tenants have separate Stream budgets. With STREAMROUTE, a Stream written
  through one replica can be read through another, and a change of the replica set loses
  only the moved Streams.
- **Limits**: a tenant over a limit gets `RESOURCE_EXHAUSTED` without affecting another
  tenant.
- **Tombstone and purge**:
  - a tombstoned tenant's valid token is refused, and the ID is accepted again only after
    `tombstoneRetention` following `done`;
  - PurgeTenant removes executions, blobs, Stream keys and rows, is idempotent, keeps its
    rate, and reports done only after both empty queries.
- **Credentials**: an expired, wrongly signed, audience-mismatched or pattern-mismatched
  credential is `UNAUTHENTICATED`.
- **Single-tenant mode**: passes the existing suite unchanged.

## Documentation

- A new product docs page "Multi-tenant namespaces" under `docs/content/`, in English and
  Simplified Chinese.
- The configuration reference in `server/config/README.md`.
- The SDK READMEs for the credential option.

## UI/UX

Dex Web in tenant mode serves one tenant or is disabled. No other UI.

## Release plan

| Item | Content |
|---|---|
| TENANTAUTH | the `tenancy` configuration, authenticators, principals, the internal listener and token, tombstones |
| TENANTID | identity mapping in and out, SubFlow IDs, `tenant_id` in the workflow input, length rules |
| TENANTSEARCH | `DexTenantID`, AST rewriting, result checks, `SyncAttributeIndexes` refusal |
| TENANTDISPATCH | worker target resolution and re-check, fairness key, dispatch cap, per-tenant pool bound, wake hook with held dispatch |
| TENANTDATA | blob prefix and `s3KeyPrefix`, cleanup by stored ID, Stream scope and caps, tenant-scoped Attribute Stores, Dex Web tenant mode |
| TENANTLIMITS | per-tenant token buckets |
| TENANTPURGE | `TenantAdminService` purge and describe |
| SDKCREDENTIAL | sdk-go credential option, then the other SDKs |
| TENANTRELEASE | a server and sdk-go release with every item above, and the product docs |
| STREAMROUTE | later: Stream routing to the owning replica, so the `memory` backend works with several replicas |
