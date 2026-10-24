# dexzd-visibility-query

## Purpose

Parse, type-check, and evaluate Dex SearchFlows visibility queries.

The crate accepts a caller-provided registry of application index fields and includes the Dex system fields. It returns byte-offset errors for invalid queries. Quoted INT values accept signed decimal and exponent forms. Integral values are converted exactly to `i64`; fractional values cannot satisfy equality, are ignored in membership lists, and produce exact integer range bounds. DOUBLE fields accept a quoted finite decimal number. Quoted numbers with surrounding whitespace are rejected. Search documents provide typed values to the evaluator, and callers can compare documents using the parsed query's sort order.
