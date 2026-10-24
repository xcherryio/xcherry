# dexzd-visibility-query

## Purpose

Parse, type-check, and evaluate Dex SearchFlows visibility queries.

The crate accepts a caller-provided registry of application index fields and includes the Dex system fields. It returns byte-offset errors for invalid queries. Search documents provide typed values to the evaluator, and callers can compare documents using the parsed query's sort order.
