# dexzd-worker-dispatch

## Purpose

Pull Worker protocol server for server-zd: Worker registration, task queues, matching, cancellation, and keepalive.

`WorkerDispatch` owns in-memory Worker streams. Construct it with `MatchingConfig` and a `WallClock`, then register `service_for_tenant(0)` with tonic. The tenant ID comes from trusted server routing; Workers send the existing protocol unchanged.

`dispatch` accepts a Flow ID, Flow type, optional sticky Worker ID, method deadline, and one unmodified WaitFor, Execute, or RPC request. Its handle yields ordered non-final frames and one final outcome. Dropping or cancelling the handle requests task cancellation; an unresponsive Worker stream closes after one second. `shutdown` asks idle Workers to reconnect immediately and busy Workers to reconnect after their current task.

The registry is local to one node and is intentionally not persisted. The engine must redispatch unfinished tasks after a restart.
