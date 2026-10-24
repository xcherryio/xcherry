# dexzd-search

## Purpose

SearchFlows backends for server-zd: a builtin evaluator over local search documents and a ParadeDB projection.

## ParadeDB projection

`ParadeDbSearchBackend::connect` creates the visibility schema and opens exactly
`pool_size` database connections. Construct `ParadeDbProjector` from an `Arc` of
that backend. Searches and projection batches share those connections; a busy
pool queues work, including with pool sizes one and two. The caller's request
deadline should cancel the search future if it remains queued too long.

Every query and projection write uses unnamed, explicitly typed statements.
This is required for PgBouncer transaction pooling, which can route consecutive
transactions to different Postgres sessions. Schema setup uses simple SQL
inside a transaction and refuses a schema version newer than this binary.

Forward only committed apply outcomes to the projector. Extract their document
changes with `extract_search_document_changes`, then call `apply_batch` with the
partition and applied log position. The method writes data and progress in one
transaction, ignores replayed positions, and reports a gap. On a gap or queue
overflow, obtain a consistent committed local view and call `rebuild_partition`
with its applied position. Projection failures must not affect command commits.
