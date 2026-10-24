# dex-blob-cache

## Purpose

Persistent, bounded local cache for Dex blob payloads.

The cache is never authoritative. Writes close a temporary file before atomic
rename, without synchronizing it to disk. After a crash, recovery discards
incomplete or corrupt entries; callers fetch missing blobs again.
