# Java versioning

An open Flow can execute code after a deployment. Treat Flow type names, Step type classes, persistence definition names, RPC names, input shapes, and decision behavior as durable compatibility surface.

## Server protocol compatibility

Published Java SDK JARs expose a diagnostic implementation version. Worker startup calls `GetServerInfo`, negotiates the highest common protocol, synchronizes Attribute indexes, and then binds WorkerService. A missing RPC, invalid interval, or disjoint interval fails before binding. Upgrade a legacy Server first, and stop running Workers before a breaking Server release because they do not renegotiate.

## Before changing code

1. Inspect the application's resolved `io.superdurable:dex-sdk` version.
2. Verify APIs against that tag/commit, not this handbook alone.
3. Inventory open Flows by Flow type and Worker target/version.
4. Classify the change as additive, behavior-changing, or incompatible.

Additive changes are safest when old paths never reference the new Step or field. Renaming a Step class changes its default registered type; override `getStepType()` with the old stable name or retain the old Step while open executions can reference it. Removing a Step, persistence definition, or RPC can break an open Flow waiting to invoke it. Keep the serialized field name stable when renaming a Java DTO member, and test decoding of persisted old values with the configured codec.

## Deployment strategies

- Keep old and new Flow implementations registered while old executions drain.
- Introduce a new Flow type or Worker target for incompatible behavior.
- Persist a business version Attribute at Flow start and branch decisions from that durable value.
- Route only new Flows to the new version, then remove old code after proving no running, waiting, retrying, or recoverable Flow needs it.
- Test an old-open/new-worker case against a real Server.

A Flow retry creates a new run and may receive a fresh timeout budget. Do not assume it migrates application data automatically.

The canonical operational guidance is the [Application Operations versioning section](https://docs.superdurable.io/production/application-operations#versioning-flow-code).
