# Dex-first data storage and Worker locality

Read this guide when a Flow stores large documents, conversation history, model context, or API/MCP results, especially with replicated Workers.

## Dex-first storage decision

For a Dex application, begin with Dex as the durable system of record for every
fact its Flow model can own. A cohesive process snapshot belongs in typed
Attributes. Dynamically keyed or independently loaded records belong in
AttributeMaps using stable instances, bounded chunks, or fixed partitions.
Use indexed Attributes and Dex search for the lookup paths they support, typed
RPCs for application reads and mutations, Channels for queued intent, and the
built-in blob path for large values.

Data shared by several processes does not automatically need a database. Give
shared facts one stable domain/entity Flow owner and expose typed application
operations around its Attributes or AttributeMaps. Avoid turning an arbitrary
process Flow into a global database, but also avoid duplicating the same
authoritative fact across process Flows.

Do not add a database, cache, ORM, outbox, or shadow read model for hypothetical
future flexibility. An external store needs a concrete unmet requirement, such
as complex or ad-hoc secondary indexes, full-text/vector search, high-contention
reads and writes to one hot record that cannot tolerate Attribute-lock
serialization, relational joins or transactions across independently owned
records, or analytics requiring large cross-Flow scans. Record the operation,
expected scale, consistency/SLO, and the Dex alternative evaluated.

When Dex can remain authoritative and only the query shape is missing, prefer a
reconcilable Attribute Store projection over application-managed dual writes.
When the external store must instead own a fact, define that boundary
explicitly and keep the Flow's Attributes to process state, durable references,
and recovery facts. One fact must never have two ambiguous authorities.

## Use the built-in large-value path

Dex durably owns an Attribute even when its serialized value is stored as a blob. With server-side blob offload and lazy loading enabled, handler requests carry an opaque blob ID instead of repeatedly transferring the full value.

The SDK hydrates each blob-backed value before application code uses it:

1. Check the local BlobCache by owning Flow ID and opaque blob reference.
2. Deduplicate IDs needed by the invocation.
3. Fetch cache misses together through Dex.
4. Store fetched values in the bounded disk cache.
5. Decode the hydrated values for the handler.

A cache hit avoids another remote blob fetch. It still incurs local cache reading and decoding. Treat the BlobCache as disposable acceleration; Dex remains the durable source of truth.

Create one cache-owned, writable directory per Worker replica. Share the opened BlobCache between that process's Worker and Client when the SDK supports it, size it for the hot set, and close it after both stop. A persistent volume can preserve entries across process restarts, but correctness must not depend on that volume or on cache admission.

Use the installed SDK and its version-matched examples for exact BlobCache constructors and lifecycle APIs.

Do not parse, construct, persist, or forward internal blob references in application code. A reference omits its owning Flow ID. Dex and the SDK supply that identity from trusted context, and the cache isolates identical-looking references by Flow. When an internal StartFlow, SubFlow, or RPC boundary moves a blob-backed value to another Flow, Dex copies or inlines the payload before the destination records it. The destination therefore remains recoverable after the source Blob is deleted.

The default server threshold keeps payloads through 100 bytes inline and offloads payloads from 101 bytes. String and Object references have the same compact shape, such as `p1|c/ab3de7kp2x`; their Value arms distinguish the types. The date segment is the lowercase Base36 day offset from September 1, 2026 UTC, without leading zeros. An Object Blob stores the complete EncodedObject, including `json`, `raw`, or a custom encoding, so the reference has no encoding suffix.

ASYNC local Step input snapshots are separate from normal payload offload and are disabled by default. Enable `blobStore.asyncStepInputSnapshotsEnabled` only when semantic history needs those exact inputs; execution and recovery do not require them.

The Value null arm represents an ordinary application null in inputs, Channel messages, RPC values, and other general Value positions. SDKs encode and decode it as the language's native null value. Two narrower boundaries intentionally assign it control semantics: an Attribute write with null deletes the Attribute, and Flow completion discards null output instead of storing an empty result. Do not use null when the distinction between “completed with null” and “completed without output” matters; return an explicit result object instead.

## Combine caching with headless Worker locality

For a headless Worker target, Dex resolves individual Worker endpoints and prefers the endpoint that most recently handled the same Flow ID. Repeated Steps and RPCs for one Flow therefore tend to reach a Worker whose BlobCache is already warm.

Configure a target as headless only when its **host:port** resolves to individual Worker replicas, such as a Kubernetes headless Service. A normal load-balanced target does not give Dex direct control over replica selection.

Sticky routing is a performance hint, not a placement guarantee. Dex may select another Worker after a transport failure, endpoint removal, routing-entry eviction, or server restart. The replacement Worker loads each missing blob once and warms its own cache. Never keep Flow correctness state only in Worker memory or its cache.

For API/MCP agents that do not need a code sandbox, this combination is often sufficient:

```text
Flow Attributes -> Dex blob storage -> sticky Worker -> local BlobCache -> handler
```

Do not build application-level blob references, download logic, deduplication, or cache eviction unless product requirements exceed this path.

## Model updates for cache reuse

BlobCache entries are keyed by owning Flow ID and immutable blob reference. Updating a large Attribute produces a new value and therefore a new blob reference. A single ever-growing array, such as one **messages** Attribute, rewrites the full array and creates a cold blob version on each append. Stickiness cannot remove that write amplification.

Prefer immutable or infrequently changed values:

- store messages, tool results, or document parts in an AttributeMap
- group small records into immutable bounded chunks
- keep only the active window or summary in a small mutable Attribute
- update old chunks only when the product operation truly edits history

Old chunks retain their blob IDs and stay reusable in the local cache; one newly added chunk causes only its own first hydration miss.

### Use AttributeMap deliberately

Choose one Attribute when the value is a cohesive snapshot that handlers normally read and replace together. Choose an AttributeMap when keys are known only at runtime and instances are read, written, or deleted independently. Each instance is a separate blob, so changing one message chunk, tool result, item, or document part does not rewrite the others.

Use stable domain identifiers or monotonic chunk IDs as instance keys. Do not place volatile or unbounded user text in keys. Group very small records into bounded chunks when per-instance overhead would dominate, and define when old instances are deleted.

For append-only history, keep a bounded active instance such as `current`. Append there until it reaches the fixed chunk size, archive it under the zero-padded first sequence, and install a new `current` value in the same commit. Archived chunks stay immutable. A page token identifies exactly one chunk, so the reader uses invocation-time exact loading rather than loading the whole map. This bounds rewrite amplification but serializes all appenders on the active instance.

For keyed lookup, use a fixed partition count and a stable cross-language hash. The official pattern trims ASCII whitespace, lowercases ASCII `A-Z`, rejects empty or non-ASCII input, computes 32-bit wrapping FNV-1a over the canonical bytes, and selects `hash % 1000` as `partition-000` through `partition-999`. Store a dictionary keyed by the full canonical value inside the partition. A collision chooses the same bucket; it does not replace another key. Changing the partition count or canonicalization requires a complete rehash or a new Flow version.

Inside a handler, map size and instance-key enumeration include buffered writes and deletes. They do not provide server-side pagination. An unbounded map can therefore avoid blob rewrite amplification while still becoming expensive to enumerate, transfer, and decode.

AttributeMap locks are scoped to one instance. Lock the exact instance when parallel Steps or RPCs perform a read-modify-write on the same value; unrelated instances can remain concurrent.

#### Whole-map coordination

When a write invariant spans multiple instances or the whole collection, define a separate non-map, non-indexed bool Attribute as the coordination key. For example, register both `currentMessages` (an AttributeMap) and `currentMessagesLock` (a singleton bool Attribute) in the Flow's `PersistenceSchema`.

Every cooperating Step, timeout handler, and RPC that writes, deletes, archives, or otherwise changes the protected map must acquire the same singleton Attribute lock through its method-specific StepOptions, timeout options, or registration-time RPCOptions. A writer that acquires only a map-instance lock does not conflict with this singleton lock and can bypass the invariant. Use instance locks for independent updates or the shared coordination lock for a map-wide invariant; adding a shared lock serializes all participating writers within that Flow execution, not other Flow executions.

The bool value is not the lock state. Do not implement acquisition by reading or toggling it, and do not manually release it. Dex owns acquisition and release around the invocation. The coordination Attribute does not need to be indexed or have a persisted value to identify the lock. Locks do not load map data: select exactly the instances read, or the whole map when an operation must enumerate it. Treat RPC lock conflict as contention and apply the application's retry policy.

#### RPC selections

RPC registration options describe fixed locks and loads. Invocation options can add exact AttributeMap locks, exact AttributeMap loads, and exact ChannelMap loads chosen from request data. The SDK unions, sorts, and deduplicates both sets; invocation options cannot remove a registered requirement. Locking and loading are independent, so every dynamic read-modify-write selects the same AttributeMap instance in both collections.

An indexed AttributeMap does not create one searchable entry per instance. Every instance writes the same fixed Flow search field, a later instance can replace the prior indexed value, and the instance key is not searchable. Use a regular Attribute with a keyword-array index for a bounded searchable collection, or an external projection for per-instance queries.

## Project Attributes for external queries

When data needs relational queries, independent pagination, cross-Flow access, analytics, or a different retention lifecycle, first consider Dex Attribute Store synchronization instead of application-managed dual writes.

Attribute Store synchronization is opt-in for each Attribute or AttributeMap, and a Flow selects one or more Server-configured stores. Dex asynchronously projects latest-state writes and retries transient failures. A projection failure does not roll back the Flow Attribute, and retry exhaustion does not automatically replay or backfill the missed batch. Keep Dex authoritative unless the documented storage decision assigns a fact to an external system of record, and make important projections reconcilable.

The built-in Attribute Store targets are PostgreSQL, MySQL, Databricks, Snowflake, and MongoDB. A SQL target projects into the columns of an existing table and MongoDB into the fields of one document per Flow; Dex never creates, migrates, or backfills the destination. They are not direct Elasticsearch or vector-store connectors, and synchronization does not generate embeddings or transform documents.

For Elasticsearch or a vector database, choose among:

- synchronize a suitable SQL projection, then use CDC or another downstream indexer
- run an explicit durable projection Step when chunking, embedding, deletion, or version checks are required
- add a Dex Attribute Store backend when direct projection is a product requirement

By default an AttributeMap instance is projected as an ordinary Attribute named by the map and the instance key, so an instance without a matching destination column is filtered. Either verify how every physical instance name maps to the destination schema before enabling synchronization for a dynamic map, or give the map its own table as described under row keys, column names, and AttributeMap tables below.

Do not introduce an external projection merely to avoid repeat reads of unchanged large Attributes; BlobCache and Worker locality already address that path.

### Row keys, column names, and AttributeMap tables

Dex Server v1.6.0 or later adds three optional settings to a PostgreSQL or MySQL store: `rowKey`, `columnNaming`, and `attributeMaps`. A store without them behaves as before. Databricks, Snowflake, and MongoDB stores reject all three at startup, and so does a Server before v1.6.0, which does not know the keys. The full contract is in the [Custom Attribute Store](https://docs.superdurable.io/production/attribute-store) operations page.

```yaml
attributeStore:
  stores:
    orders:
      type: postgres
      dsn: postgres://user:password@host/database
      tableName: public.orders
      rowKey: order-{customer_id}-{order_id}
      columnNaming: snake_case
      attributeMaps:
        LineItems:
          tableName: public.order_line_items
          instanceKeyColumn: line_item_id
```

- **`rowKey` makes the Flow ID format a storage contract.** The template splits each Flow ID into the table's primary-key columns instead of storing the whole ID in one key column. Use it when a business ID is part of the Flow ID and consumers query by that ID, or when several Flow types describe one entity.
  - A template is literal text and placeholders. `{column}` stores the captured text in that key column, and `{*}` matches without storing. Each placeholder captures up to the next literal text and the last one captures the rest. Literal text must separate two placeholders and no capture may be empty: `order-{customer_id}-{order_id}` splits `order-c1-o-7` into `customer_id` c1 and `order_id` o-7.
  - A Flow ID that does not match the template skips its whole batch with an error log, and a retry cannot change that. Select a `rowKey` store only on Flow types whose IDs match, and settle the Flow ID format and the template together before the first write. Dex never migrates or backfills rows, so a later change to either is a data migration the application owns.
  - Startup requires the table's primary key to be exactly the template's columns, all character columns. Attributes cannot write a key column.
  - Several stores may name one table, and each upsert sets only the columns of its own batch. `customer-{customer_id}` and `onboarding-{customer_id}-{*}` therefore write different columns of one customer row; give each column one owning Flow type.
- **`columnNaming: snake_case`** writes each Attribute, and each field of an AttributeMap instance, to its snake_case column: `DeploymentURL` becomes `deployment_url`. Two names in one batch that map to the same column are both filtered. Without it, names must equal the column names exactly.
- **`attributeMaps`** writes each instance of a named AttributeMap as one row of its own table. The table's primary key is the store's key columns plus `instanceKeyColumn`, which holds the instance key as the application wrote it. For the `orders` store above, `LineItems` needs the key `customer_id`, `order_id`, `line_item_id`.
  - Writing an instance replaces its row, and deleting the instance deletes the row. A JSON object value writes each top-level field to the column of the same name, and columns the value omits return to their default or NULL. Any other value, such as a string, number, or list, goes to a column named `value`.
  - Write each instance as one complete value, because the row always equals its latest value and a partial write erases the omitted columns.
  - A field without a column is skipped with a one-time warning, a field that maps to a key column is skipped, and a value that does not convert is logged and skipped while the row still records the instance. Keep value field names aligned with the table's columns.
  - The store row and all its instance rows are written in one transaction. Do not add foreign keys between the tables: a violated constraint fails the whole batch and holds back the Flow's later writes until the retry budget runs out.
  - Dex never deletes rows when a Flow closes, so the destination needs its own retention. A Flow writes each AttributeMap only to the selected stores that declare it; other selected stores project its instances as ordinary Attributes.

A migration that adds a column to a store table or an AttributeMap table needs no Dex restart, as long as it completes before the Workers that write the column start. From Dex Server v1.6.1 a batch that writes a column missing from the current schema refreshes the store's schema before it writes, and an earlier Server picks the column up at its periodic refresh (one minute by default).

The tables stay an eventual, latest-state projection. Never read them to make a Flow decision.

## Verify the deployment

Exercise the actual topology with an integration test:

- invoke the same Flow repeatedly and confirm unchanged values hydrate correctly
- replace the serving Worker and confirm the Flow continues after a cold-cache load
- update one chunk and confirm old state remains readable
- verify the cache directory is writable, bounded, and not treated as durable state

When diagnosing latency, distinguish remote blob loads from local cache reads and value decoding. Worker affinity can improve the first; data shape determines how much work remains on every invocation.
