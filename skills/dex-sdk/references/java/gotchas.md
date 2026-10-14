# Java gotchas

- The default Flow and Step types are the simple class names. Name each Flow class after its domain, such as `ApprovalFlow`, never just `Flow`; the Java package does not separate two classes with the same simple name.
- `Step<I>` requires a concrete `Class<I>`; use a holder class for generic data.
- Flows with `@RPC` methods and those methods cannot be `final`, because typed stubs intercept them. Kotlin equivalents must be `open`.
- Register every Step instance once in `StepList` and every persisted definition in `PersistenceSchema`.
- `waitFor` and `execute` have independent retry, timeout, load, and lock options.
- `Thread.sleep` inside a handler occupies Worker capacity; use a Timer when the wait is durable business time.
- Java thread interruption and `Context.isCancellationRequested()` are cooperative. Neither rolls back an external call.
- Attribute and Channel writes are staged; ordinary Java objects and external systems are not.
- `Wait.anyOf` losers can remain active unless the chosen pattern cancels them.
- A bounded `waitForFlow` may return a nonterminal snapshot. Check terminal status before reading output.
- RPC loads are snapshots. Writes staged later in the same RPC do not change pending-message snapshots.
- Attribute locks coordinate only participants requesting the same lock; they are not database transactions.
- Channel deletion requires transactional RPC configuration when a missing message must abort other writes.
- BlobCache directories must be writable, durable enough for the process lifecycle, and shared by the paired Client and Worker.
- Stream messages are best effort and retention-bound. Never rebuild business state only from a Stream.
- Worker startup synchronizes indexed Attributes; startup failure should fail the service rather than accept traffic with a partial Registry.
- Catch SDK exception classes, not strings or numeric diagnostic sub-status.
- Flow, Step, Attribute, Channel, Stream, and RPC names are persisted compatibility identifiers.
- Temporal Cloud API-key deployments require externally provisioned indexes and `attributeIndexesManagedExternally: true`.

When in doubt, compare the application lockfile to the pinned SDK source and run a real integration scenario.
