# IWF integration port

This directory mirrors the complete Java integration inventory. Every Flow has
its own module;
`iwf_flows.ts` only creates the shared instances used by scenarios.

The `*.integration.test.ts` files port all 58 Java assertions and run through
the TypeScript Client and Worker against `dexcli dev`. The older scenario
functions remain compile-contract coverage for representative call sites.

Static verification:

```shell
npm run typecheck
```

Full integration verification:

```shell
./run-integration-tests.sh
DEX_INTEG_WORKER_MODE=pull ./run-integration-tests.sh
```

The default mode uses a listening Worker. Pull mode runs the same suite with
outbound Worker streams and pull routing as the Client default. Each invocation
starts its own `dexcli dev` environment. Both modes run in CI with coverage.

## Programming experience

- Flow and Step are interfaces, while explicit codecs provide the runtime type
  information erased by TypeScript.
- `Wait.until(Timer.byDuration(...))` and domain factories keep nested calls
  readable.
- Decorated RPC method references preserve their input and output types through
  `Client.invokeRPC`.
- Attribute-map locks retain the instance through
  `items.lock("order-1")`.
- Client methods return Promise because blocking the Node event loop is unsafe.
- Step / waitFor / RPC handlers may be sync or async; several ported fixtures and
  `mixed_sync_async_flow.ts` exercise both styles on one Worker.
- Persistence integration covers singleton Attribute equality waits; local
  contracts cover Condition IDs and buffered map introspection.
- Step streaming covers repeated and cross-Stream writes, duplicate source
  metadata, heartbeat detail recovery and clearing, local-activity fallback, and
  buffered text timer/final-result ordering.

## Error coverage

| Scenario | Error |
| --- | --- |
| Duplicate start | `FlowAlreadyStartedError` |
| Missing describe, attribute read, or Flow wait | `FlowNotFoundError` |
| Missing query-only RPC target | `FlowNotActiveOrNotFoundError` |
| Read-only RPC on retained terminal execution | Retained snapshot |
| Missing or closed mutation | `FlowNotActiveOrNotFoundError` |
| Worker handler failure | `WorkerInvocationError` |
| Locking RPC contention | `RpcLockConflictError` |
| Long-poll expiry | `LongPollTimeoutError` |
| Terminal Flow result | `FlowResult` for every terminal status |
| Durable SubFlow condition | Identity, all/any, reuse, reset, and continue-as-new |

Local contract tests also cover malformed rich details, registration failures,
value mapping, and invalid Step results.
