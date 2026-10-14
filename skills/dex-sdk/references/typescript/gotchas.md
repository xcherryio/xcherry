# TypeScript gotchas

- `getFlowType` and `getStepType` are required. Return a domain Flow type such as `ApprovalFlow`, never just `Flow`, and keep each Flow type unique within one Worker.
- Types are erased. A `Codec<T>` is the runtime contract; generic annotations do not validate input.
- `Wait` is a Dex description, not a Promise. Return it from `waitFor`; do not `await` it.
- Async `execute`, `waitFor`, and RPC handlers may await Client calls and yield the Node event loop. Synchronous CPU work blocks all Worker calls.
- Register the exact Flow instance passed to Client methods. A structurally identical new instance is not registered.
- Register every Step and persistence definition exactly once with stable names.
- Millisecond options are plain numbers. Keep units visible in names and calculations.
- A bounded `waitForFlow` can return a running snapshot; check status before decoding output.
- `cancellationSignal.aborted` is cooperative and does not undo external effects.
- Staged state commits only with the final successful result; module globals and remote calls do not.
- `anyOf` does not automatically model every loser cleanup requirement.
- Attribute locks coordinate only handlers using the same locks.
- Pending Channel messages require explicit load options. Snapshot reads do not refresh after staged mutations.
- Map instance keys must be non-empty and contain no slash.
- Stream delivery is best effort and retention-bound; always keep a durable source of truth.
- `void` needs `voidCodec` when used as an explicit Step input or recovery target.
- Await `Worker.start()` before serving traffic; index synchronization can fail startup.
- Close Client/Worker asynchronously, then close BlobCache.
- Catch exported SDK errors, not `error.message` text.
- Avoid `void somePromise` unless rejection ownership is explicit.
- Temporal Cloud API-key deployments require externally provisioned indexes and `attributeIndexesManagedExternally: true`.

When API shape is uncertain, inspect installed declarations and the pinned SDK source before writing code.
