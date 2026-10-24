# dexzd-visibility-query

## Purpose

Parse, type-check, and evaluate Dex SearchFlows visibility queries.

The crate accepts a caller-provided registry of application index fields and includes the Dex system fields. It returns byte-offset errors for invalid queries. INT fields also accept a quoted whole number, and DOUBLE fields accept a quoted finite decimal number. Quoted numbers with surrounding whitespace are rejected. Search documents provide typed values to the evaluator, and callers can compare documents using the parsed query's sort order.
