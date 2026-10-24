# dexzd-search

## Purpose

SearchFlows backends for server-zd: a builtin evaluator over local search documents and a ParadeDB projection.

## ParadeDB projection

`ParadeDbSearchBackend::connect` creates the visibility schema and opens exactly
`pool_size` database connections. Construct `ParadeDbProjector` from an `Arc` of
that backend. Searches and projection batches share those connections; a busy
pool queues work, including with pool sizes one and two. The caller's request
deadline should cancel the search future if it remains queued too long.
An entire partition rebuild holds one connection until its transaction commits;
use `pool_size >= 2` when searches must continue during a large rebuild.

Every query and projection write uses unnamed, explicitly typed statements.
This is required for PgBouncer transaction pooling, which can route consecutive
transactions to different Postgres sessions. Schema setup uses simple SQL
inside a transaction and refuses a schema version newer than this binary.
The first setup needs a role that can create `pg_search`, or the extension must
already be installed. One database schema belongs to one Dex cluster because
projection progress and rebuilds are partition scoped. Upgrading from the
earlier schema clears derived rows and progress, so rebuild every partition.

Forward only committed apply outcomes to the projector. Concatenate every
outcome of one partition frame in apply order with
`extract_batch_search_document_changes`, then call `apply_batch` once for the
frame with the partition's previous and new full `LogPosition` values. The
method writes data and progress in one transaction. Sparse log sequences are
normal; a stored position that differs from the caller's previous position
requires a rebuild. On a mismatch or queue overflow, obtain a consistent
committed local view and call `rebuild_partition` with that view's applied
position. Projection failures must not affect command commits.
