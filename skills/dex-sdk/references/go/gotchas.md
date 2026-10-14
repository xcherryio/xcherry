# Go gotchas

- Define Attributes, Channels, Streams, and maps once at package scope; definitions carry identity.
- Register every Step emitted by `GoTo`/`MovementOf`, and register both parent and child Flows.
- Use `StepDefaultsNoWaitFor[T]` only for a true no-wait Step.
- From Go SDK v1.5.0, default Flow and Step types are the Go type name without its package (`OrderFlow`, `shipOrder`); earlier releases use package-qualified names (`orders.OrderFlow`). Name every Flow type after its domain, such as `ApprovalFlow`, never just `Flow`. A generic Flow or Step must implement `GetFlowType` or `GetStepType`. Otherwise do not implement them for new definitions or as a Dex Web workaround; only a [required production rename that preserves the existing durable identity](versioning.md#default-flow-and-step-type-names) justifies an override.
- Return every Context state, heartbeat, and Stream error.
- Do not coordinate durable work with process-local mutexes, goroutines, or maps.
- Put durable business time (TTL, reminders, inactivity) in a WaitFor Timer. `time.Sleep` inside Execute occupies the attempt; use it only between rounds of a heartbeating [Polling](patterns.md#polling) Step.
- Preserve omitted versus explicit zero in pointer-valued options.
- `dex.None` is nil-only; pass `nil`, not an invented empty payload.
- Graceful and force terminal decisions have different parallel/cancellation semantics.
- Give Client operations deadlines; long-poll expiry is not Flow failure.
- Validate map instance names: non-empty and no `/`.
- Keep schema/Flow packages below registry composition to avoid import cycles; inject late Client providers.
- Temporal Cloud API-key deployments require externally provisioned indexes and `attributeIndexesManagedExternally: true`.

Prefer pinned [compile contracts](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/sdk-go/dex/contracts_test.go) and runnable examples over remembered names.
