# Testing durable behavior

Verify [read-after-write consistency](read-after-write.md#verification) with first-read assertions for direct RPC state, including loaded maps and queues. Use a controlled Step or projection gate to demonstrate that readable state does not imply downstream completion. Reserve bounded polling for documented eventual views and explicit asynchronous completion.

Use a real Dex Server integration whenever behavior crosses a Worker, Client, persistence boundary, wait, retry, Timer, RPC, Stream, or SubFlow. A handler-only unit test cannot prove durable coordination.

## Minimum integration harness

Start an isolated Dex development environment, construct the same registry and payload/blob configuration used by the application, start the Worker, and use a Client to drive the Flow through public APIs. Give every test a unique Flow ID and cleanly stop owned processes.

Use deadline-based polling or the SDK's long-poll result API. Do not use a fixed sleep to guess when asynchronous state has converged.

## Required scenarios

- Start a Flow and verify typed terminal output.
- Replace or restart the Worker while the Flow is waiting, then verify continuation from durable state.
- Force a retryable Execute failure and verify retry count, heartbeat recovery, and exhausted-retry routing.
- Publish Channel and ChannelMap messages through typed Flow RPCs and verify ordering, single consumption, stale message IDs, and terminal rejection.
- Invoke read-only and mutating RPCs. After terminal status, verify query-only reads, not-active rejection of a non-transactional RPC's returned effects, and rejection of transactional or locked RPCs before handler execution. For transactional RPCs, verify all effects commit or none do.
- Fire and skip Timers where supported; verify the business deadline and timeout-handler path.
- Exercise parallel branches and SubFlows with a deliberate failure and cancellation policy.
- Lose or reconnect a Stream consumer and recover canonical state through a typed snapshot RPC.
- Retry the same logical start with the same Flow ID and Request ID and verify it attaches or converges without duplicate effects.
- Start the same Flow ID with a different Request ID and assert the language SDK's typed conflict.
- Make start acceptance ambiguous, retry with the same identities, and verify correctness without an application-owned start-deduplication table.
- Interact after terminal completion and assert the not-active or terminal behavior.

### StartFlow ordering and retry identity

Exercise the application's start boundary against a real Dex Server with its production registry and reuse policy. Observe calls without replacing the real start/RPC behavior. For a normal accepted start with no independently required business read, assert StartFlow is the first Dex operation and no read RPC, search, or status lookup occurs solely for duplicate/retry protection. Assert a successful accepted-start response performs no defensive follow-up RPC/status read to verify supplied identity or initial Attributes and launches no dependent Flow from the API handler. Check the DTO against validated input/known initial fields. Only an explicit admission/completion-result contract may add a subsequent wait or result call.

When the reuse policy disallows another execution, verify the [start result matrix](error-handling.md#start-first-reconcile-only-after-an-error): the same Request ID with ignore-already-started enabled returns the existing execution without duplicate effects, both while active and after closure; the same ID with the option disabled still errors; a different ID still conflicts with the option enabled. Where the SDK generates Request IDs, omit the ID on separate calls and assert the conflict is not silently ignored.

Inject a lost start response after Server acceptance, then retry with identical Flow ID, Request ID, and options and verify convergence without a mandatory preflight read. For an AlreadyStarted branch whose business contract permits reconciliation, assert that the typed snapshot RPC follows the failed start and that the returned facts actually establish the requested condition; test an existing snapshot that does not satisfy that condition as a conflict or unknown outcome. A spy/counter can verify ordering, but mock-only success cannot establish Server deduplication.

### Required post-start milestones

For an API that promises a database source-of-truth write, exercise both a new start and matching-request deduplication against the real Server. Delay the commit and assert the response cannot claim that milestone before the committed Attribute or selected persistence Step completes. Once the wait succeeds, verify the actual business row. Cover wait-budget expiry while the Flow continues and retry with the same start identity; expiry must not create a replacement execution or report an unproven failed write. For the acceptance-only contract, assert no Attribute or Step-completion wait is issued.

### Downstream start recovery

For an operation that launches separately owned work, run a real-server application test that accepts the owning Flow, ends the API request, and then replaces the Worker before the downstream start executes; the owning Flow must still launch the work. Also inject a Worker crash after downstream acceptance but before the launching Step commits, replace it, and verify that the retried start uses the same identities/options and creates no duplicate execution or business effects. Assert downstream retry exhaustion reaches the declared recovery outcome and that the API never issues the dependent start. Use the selected language's crash harness guidance; a graceful Worker drain alone does not simulate this commit gap.

### Terminal entity reads

For every Flow-owned business entity whose contract permits reads after closure, run a real Dex Server integration test with the application's registry, Worker, and Server routing policy. Persist the final business snapshot, close the Flow, establish terminal status in the test harness, then call its typed `Get*` read-only RPC directly through the application read boundary. Cover each readable terminal outcome supported by the entity contract and assert the returned snapshot includes the final committed Attributes/AttributeMaps.

For a nonexistent Flow ID, call the same query-only Get through the real application boundary and assert its typed missing/not-active error becomes the declared not-found result directly. Assert no lifecycle/status probe, search, history call, retry, or short-timeout wait is issued. Keep a Worker/service failure case distinct from not-found, and cover the application's retention/unavailable contract when relevant.

Also assert that the business read performs no `WaitForFlow`, history lookup, or historical Step-output decoding. A test-side spy/counter or a guard that fails on those calls may observe the real Client boundary; keep the snapshot RPC on the real Server/Worker path. A harness wait used to establish closure is allowed and must be counted separately. A mock-only test or a handler invoked directly cannot prove terminal query support. If production policy forces active-only RPC execution, resolve that conflict explicitly before claiming the entity remains terminal-readable; do not make a history fallback pass the test.

## Data and deployment scenarios

For large Attributes, replace the serving Worker and verify cold BlobCache hydration. For AttributeMap concurrency, race writers on the same instance and verify the lock-protected invariant. For Attribute Store synchronization, verify the Flow remains authoritative and the projection can reconcile after a transient failure. Against a real destination table, also verify that each Flow ID format a `rowKey` store accepts lands in the expected key columns, that a Flow ID that does not match writes no row, and that rewriting and deleting an AttributeMap instance replaces and removes its row in an AttributeMap table.

For version changes, run an open Flow on the old Worker, deploy the new registry, and finish that same execution. A test that starts only after deployment does not prove open-Flow compatibility.

## Failure assertions

Assert the user-visible SDK failure type, final Flow status, recovery Step, and durable state. Do not merely assert that an exception occurred. When an external effect has an unknown outcome, record and test that state rather than assuming success or failure.

Read the selected language's **testing.md** for its harness, commands, and runnable sources.

At the SDK API baseline, assert the combined missing/inactive type from the [public error table](error-handling.md#missing-or-inactive-target-errors) for a missing query target and for closed active-required operations. Keep separate Flow-not-found mappings distinct. Verify the selected language's metadata and cause/service extraction remain available; include sync and async surfaces where that SDK exposes both. These assertions do not permit lifecycle probes in the application query-only Get path.
