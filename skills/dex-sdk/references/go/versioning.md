# Versioning Go applications

## Check first

```bash
go list -m github.com/superdurable/dex/sdk-go
go list -m -json github.com/superdurable/dex/sdk-go
```

The selected SDK source is authoritative. Baseline snippets are evidence only for the pinned Dex commit.

## Server protocol compatibility

The Go Worker obtains its diagnostic artifact version from Go build information. On startup it calls `GetServerInfo`, negotiates the highest common protocol, synchronizes Attribute indexes, and then binds WorkerService. A missing RPC, invalid interval, or disjoint interval fails startup before the listener opens. Upgrade a legacy Server first; stop running Workers before a breaking Server upgrade because they do not renegotiate.

## Default Flow and Step type names

Use the SDK's default Go type names. From Go SDK v1.5.0 the default is the Go type name without its package or pointer, so `*orders.OrderFlow` registers as `OrderFlow`. Earlier releases register the package-qualified name, such as `orders.OrderFlow`. Embed `dex.FlowDefaults` and the appropriate Step defaults; do not add custom `GetFlowType` or `GetStepType` implementations for new or not-yet-production definitions, display labels, naming style, or speculative future refactors.

Name every Flow type after its domain, such as `ApprovalFlow` or `ReplyFlow`; never name it just `Flow`. Flow types must be unique within one Registry, and Step types must be unique within one Flow. From v1.5.0 the package does not separate them, so `NewRegistry` rejects two same-named Go types from different packages; rename one Go type before it reaches production. A generic Flow or Step type has no default name and must implement `GetFlowType` or `GetStepType` with one compile-time string.

The only exception is an unavoidable Go type rename, or a package rename before v1.5.0, for a Flow or Step already deployed to production, where its previously registered durable identity must remain unchanged. In that case, override only the affected method and return the exact existing production type string as a literal or constant. Identify that persisted string from the deployed registration or real executions before editing. Do not substitute the new Go name or change the persisted type identity as part of the rename. Preserve an existing production override when it already owns that identity.

Upgrading a production application from a Go SDK before v1.5.0 changes every default Flow and Step type the Worker registers. Open executions keep their package-qualified types, and a v1.5.0 Worker cannot dispatch them. Before that upgrade, let affected executions finish, or apply the production-rename exception to each affected Flow and Step with its exact package-qualified string. Update stored Flow type queries and Step type strings to the registered names.

Dex Web or FDG metadata mismatch is not a reason to override every type. Compare generated metadata with the SDK's registered defaults and diagnose the analyzer/SDK version or metadata issue. Pair the analyzer with the installed SDK: Go SDK v1.5.0 or later needs `dexcli` v1.5.0 or later, and an earlier Go SDK needs a `dexcli` release before v1.5.0. When the production-rename exception applies, verify an old execution resumes on the renamed Worker before rollout.

## Open-Flow compatibility

An open Flow can resume on new Worker code. Preserve Flow type, reachable Step type strings, registered schemas, and decodable payloads. Do not rename a Step as a refactor. Add a new Flow/Step type for incompatible behavior and route new starts there.

Additive fields with defaults are usually safer. Removing registered Steps, changing state types, repurposing enum values, or changing retry/terminal invariants is risky.

## Rollout

Deploy a Worker able to serve old and new runs, then move traffic deliberately. Updating a process does not replace durable execution state.

[Pinned runnable source](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/go/primitives/flow/controller.go)
<!-- dex-source: examples/go/primitives/flow/controller.go -->
```go
func rerouteActiveFlow(ctx context.Context, client *sdk.Client, flowID string) error {
	return client.UpdateFlowConfig(ctx, flowID, sdk.FlowConfig{
		WorkerTarget: &sdk.WorkerTarget{Address: "worker-canary:8803"},
	})
}
```

Choose ID reuse and already-started behavior as product semantics. Test active and closed prior runs. Before rollout, start representative Flows on old code, stop at durable boundaries, replace the Worker, and complete them on new code. Include maps, Channels, Timer, RPC, retry, and timeout handler.
