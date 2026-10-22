# Named pull task queues

Status: proposed design. No implementation is committed yet.

## Summary

A pull WorkerTarget may name a task queue, for example `WorkerTarget{Pull: true, TaskQueue: "billing"}`. A pull Worker declares the named queues it polls. A Flow dispatched on queue `billing` only reaches Workers polling `billing`. Leaving the queue unset keeps today's behavior exactly: one queue per Flow type, plus the app Worker ID sticky queue.

A named queue is a routing namespace, not a shared bucket. Matching keys each queue by (queue name, Flow type). Workers therefore keep one stream set per (queue, Flow type) pair, as they do per Flow type today. Flow types only need to be unique within a queue.

## Motivation

The stream count is no longer the reason; after #740 a Worker holds about `M + (F + 1) × I` streams. The reason is isolation:

1. **Test isolation on a shared server.** Today a pull Flow cannot be confined to one Worker process, so concurrent test runs against one Server can steal each other's tasks. A queue per test run keeps them apart.
2. **Several applications on one Dex Server.** Today pull Flow types must be globally unique on a Server. Named queues let independent applications deploy the same Flow type names side by side.
3. **Execution classes.** Long-running Steps, such as model calls or long polling, can run on dedicated Workers with their own concurrency budget, through a per-Step queue override. They then no longer compete with RPCs and short Steps for one MaxConcurrentTasks budget. For RPC starvation alone, reserved RPC capacity (below) is the smaller fix and ships first.

Sharing one stream set across many low-traffic Flow types is not a goal; see the rejected alternatives.

## Model

### Matching keys

Matching addresses queues by a structured key `(queue, name)`:

| Purpose | Key today | Key with a named queue Q |
|---|---|---|
| Flow type queue | `flowType` | `(Q, flowType)` |
| Sticky queue | `appWorkerID` | `(Q, appWorkerID)` |

The default queue is the empty name, and its keys hash and compare exactly as today's strings. Existing streams, dispatches and persisted Flows are unaffected. The in-memory queue map moves from `string` keys to a struct key, so that a queue name, a Flow type and an app Worker ID can never collide. For the default queue, the placement input stays the bare Flow type or app Worker ID string, byte-identical to today, so that old and new matching nodes agree on owners during a rolling upgrade. For a named queue, placement hashes an encoding of both parts. Placement collisions are harmless, because they only co-locate queues.

### Worker registration

- `PullWorkerOptions` gains `TaskQueues []PullTaskQueueOptions{Name string, MaxConcurrentTasks int}`. Its default is one entry for the default queue. A per-queue MaxConcurrentTasks of zero means only the Worker-wide MaxConcurrentTasks applies. A non-zero value caps that queue's active handlers, and the Worker-wide value remains the overall cap. When a queue is at its cap, the Worker declines that queue's tasks with `NO_CAPACITY`, as it does at the Worker-wide cap today. Per-queue budgets are what make execution classes fix starvation inside one process. Running two Workers, with two app Worker IDs, in one process remains supported and is tested as an equivalent pattern.
- A Worker may poll several queues, for example the default queue plus `long-steps`. For each polled queue Q, it registers one sticky stream set `(Q, appWorkerID)` and one stream set `(Q, flowType)` per registered Flow type.
- `PollWorkerTaskRegister` gains `string task_queue = 6`. Its existing `queue` field keeps naming the Flow type or the app Worker ID within that task queue.
- `PollWorkerTaskRegistered` gains `string task_queue = 2`. The queue owner echoes the task queue it registered the stream under; see Compatibility.
- Validation requires `queue == app_worker_id` or a declared Flow type, as today.

### Flow routing

- `WorkerTarget` gains `string task_queue = 4`. It is valid only with `is_pull_mode`; it is rejected with an address or a headless target.
- The task queue is persisted in the Flow's `FlowConfig.WorkerTarget`. SubFlows inherit it, because `buildSubFlowConfig` already copies the parent's `WorkerTarget`. Client defaults and StartFlow overrides work as they do for `is_pull_mode` today.
- A SubFlow override today replaces the whole WorkerTarget, which would silently drop a child out of a per-test queue. Rule: a pull override with an empty `task_queue` inherits the parent's task queue when the parent is also pull. To target the default queue explicitly, an override names the reserved queue `dex.default`. An address override replaces the target as today.
- UpdateFlowConfig can change the queue, like any other WorkerTarget change. The rule is explicit for every case:
  - A Step scheduled after the update dispatches on the new queue.
  - A Step already scheduled before the update keeps the queue captured in its activity input for all of its retries, because the interpreter passes the WorkerTarget in the activity input (`input.GetWorkerTarget()` in `activityImpl.go`). This matches how address changes behave today.
  - RPCs read the current FlowConfig, so they use the new queue from the next call.
  - The sticky key is always formed with the queue of the dispatch being made, so a stale sticky Worker on the old queue has no streams under the new key and falls back immediately (#717).
- `DispatchTaskRequest` gains `string task_queue = 16`. Matching uses it for both the sticky key and the fallback key. `dispatchQueue` rebuilds the forwarded `remoteRequest` field by field, so `task_queue` must be added to that literal. An owner treats a missing `task_queue` as the default queue; with the rollout flag below, that case only arises for default-queue traffic.
- Every dispatch path reads the Flow's `WorkerTarget.task_queue`, from the same FlowConfig source that decides `is_pull_mode` today: the interpreter Step dispatch (`pullTaskRequest`), the API-direct RPC path (`server/service/common/rpc/pull.go`), and the transactional RPC activity.

### Per-Step queue override (execution classes)

`StepOptions` gains an optional `task_queue` string. When it is set, that Step's WaitFor and Execute dispatches use `(stepQueue, flowType)` and `(stepQueue, lastWorkerID)` instead of the Flow's queue. RPCs always use the Flow's queue. A dedicated Worker pool polls `stepQueue` and registers the Flow type there. It may register only the long-running Step types. Any other Step that reaches it is declined with the existing `UNKNOWN_STEP` reassignment (#711), without consuming an attempt. If nothing polls `stepQueue`, that Step gets `NO_WORKER_AVAILABLE` after the dispatch wait, like any queue without Workers.

Dex applies no derivation rule to the name. The application already builds its Flows and their StepOptions in the Worker process, so it owns isolation. **When tests or tenants share a Server, make per-Step queue names configuration rather than constants in Flow code.** For example, a Flow reads its long-Step queue from its own configuration: `long-steps` in production, and `test-abc-long` in a test process whose Workers poll `test-abc` and `test-abc-long`. The test's long Steps then never reach the production pool.

```go
type SessionAgentFlow struct {
	LongStepQueue string // from application configuration
}

func (flow *SessionAgentFlow) GetStepOptions(step string) *dex.StepOptions {
	if step == "CallModel" {
		return &dex.StepOptions{TaskQueue: flow.LongStepQueue}
	}
	return nil
}
```

The product docs repeat this guidance next to the option, with a runnable example in each SDK.

### Reserved RPC capacity (ships first, independent of task queues)

This targets RPC starvation directly and needs no routing change, no protocol change and no Server change.

- New option `PullWorkerOptions.ReservedRPCTasks` (R), default 0. It must be less than MaxConcurrentTasks (M).
- A stream learns a task's kind only when it receives the task. When a WaitFor or Execute task arrives while M − R handlers are already busy, the Worker declines it with the existing `NO_CAPACITY` rejection. RPC tasks are accepted up to M.
- `NO_CAPACITY` already means "reassign without consuming an attempt". Matching backs that stream off (300 ms to 2 s) and tries other eligible streams within the same dispatch wait. No new reason is needed, and old Servers handle it already.
- Bound: the dispatch wait. If every Worker stays at its Step limit until the wait ends, the Step gets `NO_WORKER_AVAILABLE` and follows its retry policy, exactly as when every Worker is full today. It cannot loop, because the per-stream backoff and the dispatch deadline bound it.
- Each Worker keeps at least I idle streams per queue even at capacity, so an RPC arriving on the sticky or Flow type queue still finds a stream to accept it.
- Readiness is unaffected. Reserve declines are not registration failures.
- Observability: an SDK metric or log counter for reserve declines by Flow type, and the existing server-side `NO_CAPACITY` and `NO_WORKER_AVAILABLE` signals.
- Rollout: SDK-only, Go first, then parity. It is usable with any Server that supports pull mode.

### Queues without Workers, and leaked Flows

A test or tenant queue whose Worker process exits leaves open Flows that dispatch to `NO_WORKER_AVAILABLE` and use up their retries, as with push today. To let harnesses detect this:

- The wake notification already fires for an unavailable queue after the sticky wait. Its payload gains `task_queue`, so a controller or harness can tell that a queue has work but no registered Worker.
- Matching exports a metric for dispatches that ended with no registered stream, labeled with the task queue. Label cardinality is bounded by `maxQueuesPerWorker` and the queue-name limits. Operators can turn the queue label off.
- A per-queue "registered stream count" admin read is possible, but it needs a fan-out across Matching owners. It is deferred.

### Unknown Flow type or Step on a queue

- No Worker has registered `(Q, flowType)`. The Flow type stream set is empty, so dispatch waits for the dispatch wait and then returns `NO_WORKER_AVAILABLE`, exactly as today when no Worker serves a Flow type. The wake hook gets the queue name in addition to the Flow type.
- A Worker on Q has the Flow type but not the Step or RPC. Unknown-name reassignment (#711) works unchanged: the exclusion set is per dispatch, and every candidate is already within Q. Because keys include the Flow type, reassignment never bounces between Workers that lack the Flow type entirely.

### Sticky routing

The sticky key is `(Q, lastAppWorkerID)`, where Q is the queue of the dispatch being made. A Worker that last served the Flow on a different queue therefore has no streams under this key. #717 then falls back to `(Q, flowType)` immediately. Isolation holds even when one process polls several queues, and the per-Step override cannot leak Steps through stickiness. The interpreter tracks one last-served Worker per Flow. After a long Step on a dedicated pool, the next regular Step's sticky key is usually empty and falls back immediately. That costs stickiness, not correctness. Tracking the last Worker per task queue is a possible refinement.

### Hot queues and partitions

Load per key is the same as today's per-Flow-type queue, since a key is still per Flow type. A named queue does not concentrate several Flow types on one owner. A single very hot `(Q, flowType)` has the same limit as a hot Flow type today. Partitioning one key across owners, as Temporal's queue partitions do, is future work. It would need forwarding between partitions and is not required for the motivations above.

### Validation and limits

- A queue name has 1 to 63 characters from `[A-Za-z0-9._-]`, the same alphabet as app Worker IDs. The empty name is the default queue. Names starting with `dex.` are reserved; `dex.default` names the default queue explicitly in overrides.
- Registration: `len(Queues) <= matching.maxQueuesPerWorker` (new; default 16). The existing limits on Flow type count and length apply per queue.
- `maxConcurrentTasksCap` stays per app Worker ID per owner, counted across all of that Worker's queues on the owner. `maxStreams` is unchanged.
- WorkerTarget validation rejects a `task_queue` that is not valid as a queue name.

### Readiness

`WaitPullRegistered` and `PullRegistrationState` (#727, #737, #738) cover every `(Q, name)` pair the Worker registers. Each `PullQueueRegistrationState` entry gains the task queue (`TaskQueue`), next to the existing Flow type or app Worker ID `Queue`. The rule is unchanged: IsReady needs one registered stream per pair, and a permanent rejection counts only when its pair has no other registered stream.

## Compatibility

Named queues must fail closed. Silently falling back to the default queue would defeat isolation.

- **Owner-scoped echo (the enforcing gate).** As #720 showed, `ServerInfo` comes from the API node, and in a mixed cluster the queue owner can be older. An old owner ignores `task_queue` and would register the stream under the bare Flow type key, so an isolated Worker would silently serve default-queue traffic. Therefore the owner echoes `PollWorkerTaskRegistered.task_queue`. A named-queue stream whose Registered frame lacks the matching echo is closed by the SDK and treated as a permanent rejection for that pair, which readiness then reports. Default-queue streams need no echo.
- **New SDK, old Server.** As additional gates, the Worker refuses to start with a named queue unless `ServerInfo` reports protocol version 4 or later; this feature bumps `CurrentProtocolVersion` to 4. The Client applies the same check before sending a WorkerTarget that has `task_queue`.
- **Mixed Server versions during a rolling upgrade.** An old interpreter or API node would drop `task_queue`. New config `matching.namedQueuesEnabled` defaults to false. While it is false, API nodes reject a non-empty `task_queue` with FailedPrecondition, and matching rejects named-queue registrations. Operators turn it on after every node runs the new version.
- **Rollback.** A persisted `WorkerTarget.task_queue` is ignored by an older Server, which then routes on the default queue. Document this: stop named-queue Flows, or keep a default-queue Worker for them, before rolling back.
- **Wire and storage.** All fields are additive, with new numbers. Default-queue keys are byte-identical to today's.
- **SDK parity.** Go first. Java, Python, TypeScript and Rust follow with the same option, WorkerTarget field and readiness fields, each gated on protocol 4.

## Rejected alternatives

- **Multi-queue streams.** One stream serves several queues, and matching splits the forward between owners. Two owners could assign the same stream at the same time, which needs a claim protocol. After #740 the stream savings are small. Rejected.
- **Temporal-style shared queues keyed by queue only.** Every Flow type on Q shares one key. This concentrates all of Q's load on one owner, which would need partitions early. Reassignment would also bounce through every Worker lacking the Flow type before finding one, since #711 exclusion is per Worker. In exchange it saves only the per-Flow-type stream sets.
- **Server-side tenancy.** Dex removed multi-tenancy. Queues give routing isolation without access control. Access control remains the job of authentication in front of the API.

## Tests

- Matching integration (`server/service/matching`):
  - default-queue keys are unchanged, and default-queue streams and dispatches behave as before;
  - a named queue only serves its own streams, both locally and through a forwarded owner;
  - sticky `(Q, worker)` with an empty sticky queue falls back immediately (#717);
  - the registration limits hold;
  - with `namedQueuesEnabled=false`, named registrations and dispatches are rejected;
  - a legacy owner that ignores `task_queue` and returns Registered without the echo makes the SDK close that stream and report a permanent rejection for the pair;
  - a forwarded dispatch carries `task_queue` through `remoteRequest`.
- Server integration on Temporal and Cadence (`server/integ`):
  - two Workers registering the same Flow type on queues A and B, with Flows started on A only reaching the A Worker, including SubFlow inheritance;
  - UpdateFlowConfig moving a Flow from A to B;
  - a per-Step override sending a long Step to a dedicated pool, while unknown-name reassignment keeps the other Steps on the main pool;
  - a named queue with no Worker returning `NO_WORKER_AVAILABLE`.
- SDK integration (Go first, then each SDK):
  - fail-closed start against a protocol-3 Server;
  - readiness covering every `(queue, name)` pair;
  - Client rejection of `task_queue` on old Servers;
  - SubFlow override inheritance and the explicit `dex.default` target;
  - a per-Step `task_queue` override reaching a dedicated pool, with other Steps reassigned via `UNKNOWN_STEP`;
  - a configured per-Step queue name keeping a test's long Steps on that test's Workers;
  - per-queue MaxConcurrentTasks declining with `NO_CAPACITY` at the queue cap;
  - the two-Workers-in-one-process pattern, with readiness and Stop for both.
- Reserved RPC capacity (its own PR, first):
  - at M − R busy, Steps are declined and RPCs accepted, with no attempt consumed;
  - when every Worker is at its Step limit, the Step ends with `NO_WORKER_AVAILABLE` at the dispatch wait;
  - readiness is unchanged.
- Compatibility: a rolling-upgrade test with one old and one new Server node, with the flag off and then on.

## Documentation

- Product docs `docs/content/production/pull-workers.mdx` (en and zh-Hans):
  - named queues and their use cases;
  - per-Step override;
  - sticky and readiness behavior;
  - limits;
  - the `namedQueuesEnabled` rollout and rollback note;
  - SdkTabs snippets copied from new runnable examples.
- `docs/content/production/server-operations.mdx`: the new matching settings.
- SDK READMEs.
- Skills are updated after the release that ships the feature.

## UI/UX

Dex Web shows task queues wherever Temporal Web shows task queues: on the Flow, on each Step execution and on each RPC.

- **Start dialog.** An optional task queue name next to the pull target.
- **Flow detail.** The Flow's WorkerTarget (address or pull) and its task queue, with `default` shown for the empty name. It also shows the last app Worker ID that served the Flow (the sticky Worker).
- **Step executions.** For each WaitFor and Execute attempt: the task queue it dispatched on (the Flow's queue or the Step's `task_queue` override), whether it was served from the sticky queue or the Flow type queue, and the serving app Worker ID. A failed dispatch shows its reason: `NO_WORKER_AVAILABLE`, an `UNKNOWN_*` reassignment that ran out of time, or a disconnect.
- **RPCs.** In the RPC history or detail view, where Dex Web shows RPCs: the task queue and the serving app Worker ID.
- **Filtering.** List Flows by task queue, through a search attribute or the existing Flow search.

Data: the interpreter already receives `served_by_app_worker_id` on WaitFor, Execute and RPC activity outputs, and records the sticky Worker (`RecordServedByAppWorkerID`). The implementation adds the resolved task queue and the sticky-or-fallback source to those outputs. It records them on the Step execution and RPC history records that Dex Web reads, and exposes them through the Web API.

Even before named queues ship, showing today's routing on the default queue (the Flow type queue, the sticky app Worker ID and the serving app Worker ID) is useful on its own. It can be built first as a separate, smaller change.
