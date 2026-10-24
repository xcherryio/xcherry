# dexzd-metrics

## Purpose

Own the fixed, process-local OpenMetrics registry and typed recording handles for a server-zd node.

The registry is built once per node. Recording uses atomic counters and gauges without an engine lock. Linux scrapes include resident memory, CPU time, open file descriptors, and process start time from `/proc/self`; these four process metrics are absent on other platforms.
