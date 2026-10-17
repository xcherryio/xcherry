# Worker pull mode and Stream routing

Worker pull mode lets a Worker request dispatches from Dex instead of exposing a WorkerService address. Matching assigns each request to an available Worker for its Flow type. The Go, Python, TypeScript, and Rust SDKs implement pull polling. Each pull Worker registers streams for its app Worker ID queue and every Flow type in its Registry. Client defaults, StartFlow overrides, and SubFlow overrides select pull dispatch with a pull WorkerTarget.

## Routing and delivery

Matching first tries the app Worker ID that served the Flow's previous call. After the sticky wait, it falls back to the Flow type queue for the remainder of the dispatch wait. Each stream handles one task at a time. Workers replenish idle streams while tasks run and keep those idle streams even at maximum task capacity. A Worker that receives an assignment while full replies with `Reject{NO_CAPACITY}`. Matching excludes that stream with exponential backoff starting at 300 ms and capped at 2 seconds, then tries another eligible stream within the same dispatch budget.

If a stream ends before its first output frame, Matching may assign the task again while time remains. This makes delivery at least once. After an output frame, a disconnect ends that dispatch attempt rather than replaying partial output. Worker SDKs reconnect failed streams with backoff starting at 100 ms plus jitter. A server-directed reconnect lets an active task finish before that stream reconnects.

Matching sends a final response frame for a completed WaitFor or Execute result, an RPC result, or a Worker error. The final task frame completes that task but does not half-close its reusable queue stream. A half-close of the Worker request side ends the stream; it is not a substitute for a final task frame. Forwarding proxies propagate the half-close to their upstream stream and drain the downstream response. `WorkerErrorResponse.grpc_status` carries the gRPC code that listening mode would have returned. The interpreter maps a valid non-OK code to the same status and preserves Worker error details; zero or out-of-range codes become `Unknown`.

A pull Worker with the Flow type but without the requested Step type or RPC name declines before handler execution with `UNKNOWN_STEP` or `UNKNOWN_RPC`. Matching permanently excludes that app Worker ID for this dispatch across its sticky and Flow type queues. A sticky rejection moves immediately to the Flow type queue, without waiting for the sticky timer. No Step attempt is consumed by a rejection. If no capable Worker accepts before the dispatch wait ends, Matching returns an error naming the Step type or RPC name. The Step then follows its configured failure and retry policy; an RPC caller receives the error directly. The regular dispatch wait defaults to 10 seconds and the local dispatch wait defaults to 3 seconds. An older SDK that returns a Worker error instead of a rejection retains its existing behavior, and push Worker calls continue to return their ordinary missing-name error.

When no Worker is available by the end of the dispatch wait, the interpreter returns `Unavailable`. That error is retryable and consumes a Step retry. An RPC caller receives `Unavailable` directly. Pull dispatch uses the matching wait settings, with shorter local waits to reserve execution time. The wake hook can notify an external controller for the Flow type queue after the sticky wait; it does not extend the dispatch budget.

## Capacity and lifecycle

For `F` registered Flow types, a Worker may retain up to `(1 + F) * (idleStreamsPerQueue + maxConcurrentTasks)` streams. The first queue is its app Worker ID queue. Registration is bounded by `matching.maxConcurrentTasksCap` per app Worker ID on each owner and `matching.maxStreams` per Matching instance. Queue-name length, Flow-type count, and Flow-type length have separate registration limits.

Pull Worker Stop closes idle streams and waits for in-flight handlers for `stopDrainTimeout`, 30 seconds by default. At the deadline, it requests cancellation and waits for handlers to return. Server shutdown closes the interpreter worker first, then Matching, then membership, then API serving. This prevents new dispatches from entering a Matching owner that is already closing.

API instances receive public Worker streams and forward them to the Matching queue owner. Multi-node routing uses membership and a private internal listener. A single Server process uses a local ring. Matching, API, and interpreter roles join the same membership cluster; at least one Matching instance must run.

## Stream routing (STREAMROUTE)

The memory backend is local to each server replica, so deployments with several replicas require Redis today. A consistent hash of Flow ID over the live server replicas would let the memory backend serve multiple replicas by assigning each Stream to one replica. A replica receiving a read or write for a Stream it does not own would forward it to the owner over the internal listener. The replica set would come from a headless Service. When membership changes, moved Streams would restart empty. Readers could resume from the Flow's durable state. Streams remain best effort. This is a later design, not current server behavior.
