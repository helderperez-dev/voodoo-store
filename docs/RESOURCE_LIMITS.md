# Store resource limits

The Python binding accepts an optional `max_bytes` argument on
`Store.open(...)`. When configured, every native log append is checked against
the limit before writing. A write that would exceed the limit returns a
`VoodooStoreError`; committed state remains readable and the rejected mutation
is not applied.

`Store.open(...)` also accepts `namespace_max_bytes`. This applies a live-value
byte ceiling independently to each derived user or internal Store namespace
and is checked at the local transaction commit boundary. A rejected
transaction does not become visible after reopen and is reported as
`NamespaceQuotaError`.

`health_report()` exposes `max_bytes` and `within_limit` alongside storage
accounting. The Framework Runtime passes `VOODOO_STORE_MAX_BYTES` through to the
native binding and also rejects an already oversized file during startup.

The same health report includes aggregate native queue depth (`queue_ready`,
`queue_leased`, `queue_dead`, and `queue_total`) plus
`queue_oldest_ready_at_ms`. Queue-specific callers can use `queue_stats(...,
now_ms=...)` to derive bounded ready-message lag without exposing payloads or
internal key encodings.

The limit is a file-growth guardrail, not a filesystem quota or replication
policy. Filesystem `ENOSPC`/quota failures are normalized to the binding's
`DiskFullError`, so callers can stop accepting work and surface an actionable
capacity signal. Operators must still provision encrypted storage with
sufficient headroom for backups, compaction, and recovery logs.
