# dexzd-apply

## Purpose

Deterministic state machine that applies one server-zd command to Flow state and returns its writes, effects, and reply.

The crate also derives run search documents and exposes pure reads for summaries,
Attributes, Channels, current state, history, wait predicates, and stored values.
Callers provide a consistent `FlowStateReader` snapshot for each read.
