# dexzd-visibility-query

## Purpose

Parse, type-check, and evaluate Dex SearchFlows visibility queries.

The crate accepts a caller-provided registry of application index fields and includes the Dex system fields. It returns byte-offset errors for invalid queries. Quoted INT values accept signed decimal and exponent forms. Values are truncated exactly to `i64`, while fractional detection follows Elasticsearch's binary64 rule; fractional values cannot satisfy equality, are ignored in membership lists, and produce integer range bounds. Quoted DOUBLE values accept finite decimal and hexadecimal floats with a `p` exponent. For hexadecimal DOUBLE values, Dex follows Elasticsearch conversion, even where Temporal's SQL visibility backends interpret the quoted text differently. Quoted numbers with surrounding whitespace, underscores, suffixes, or non-finite values are rejected. Search documents provide typed values to the evaluator, and callers can compare documents using the parsed query's sort order.
