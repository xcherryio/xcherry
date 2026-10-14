# Versioning and open Flows

An open Flow is a durable contract between its recorded state and future Worker code. Treat stable Flow, Step, RPC, Attribute, Channel, Stream, and map names as data, not local refactoring details.

## Source authority

Read the package manifest and lockfile first. Preserve the installed SDK version unless the user requests an upgrade. Validate exact signatures against installed source or a tag/commit matching that version, and identify that path or immutable revision when presenting exact code. The examples in this bundle are pinned by the repository's `DEX_BASELINE`; do not mix them into a different SDK without verification. When matching source is unavailable, stop at the version-independent Flow model and request access to the installed package or tag before emitting exact API code.

## Server protocol compatibility

Server-to-SDK compatibility is based on inclusive protocol intervals, not artifact version ordering. Worker startup calls `GetServerInfo`, chooses the highest version in the interval intersection, synchronizes Attribute indexes, and only then binds WorkerService. A new SDK can use an older Server when it retains the old protocol. A new Server can use an older SDK while their intervals still overlap.

Upgrade a Server that lacks `GetServerInfo` before deploying Workers with this check. Treat `UNIMPLEMENTED`, invalid intervals, and disjoint intervals as startup failures. A breaking Server release raises its minimum protocol; stop running Workers before the Server upgrade because they do not renegotiate in place.

Protocol negotiation protects the Worker-to-Server API. It does not make open Flow type names, reachable Step graphs, schemas, or persisted payloads compatible. Apply both checks independently.

## Compatible changes

Prefer additive evolution:

- add a new Step type and route only new executions to it
- add optional Attributes, Channels, Streams, or RPCs
- retain old Step and RPC handlers while open executions can still reach them
- add a version Attribute or start-input field that selects the graph at a durable boundary
- deploy readers before writers when changing a serialized payload shape

Do not rename or remove a durable resource merely because application source no longer references it. Search for open executions and recorded paths first.

## Incompatible changes

When compatibility cannot be preserved, choose one explicit strategy: let old executions finish on an old Worker pool, keep versioned handlers in one registry, migrate at a safe application boundary, or terminate/recover selected executions with user authorization. Do not add an unreviewed fallback that silently interprets old data as the new type.

Changing a language SDK and changing the Flow model are separate decisions. Upgrade the dependency, regenerate or adapt compile-time code, and then test one execution started before deployment plus one started after deployment.

## Verification

- inspect open Flow types and active Steps before removing registrations
- compile or type-check every handler retained for old executions
- start on the old version, wait, replace the Worker, then complete on the new version
- verify payload and error compatibility through the actual codec
- verify timeout handlers, failure targets, and SubFlow options survive Flow retry

Official operations guidance: https://docs.superdurable.io/production/application-operations#versioning-flow-code
