# Native Store Primitives

Use a native domain when it has semantics beyond key/value storage.

Examples:

- Collection indexes own query/index mechanics.
- Jobs own durable leasing/retry/history mechanics.
- Topics/Streams own ordered append/read mechanics.
- Objects own content-addressed bytes and named references.
- Workflow owns workflow state, signals, timers and history.

Do not implement one native domain internally by abusing another public domain merely to reduce API surface.

## Cross-domain transactions

When supported, heterogeneous operations can share one local Store commit boundary.

Describe this precisely as local Store transaction semantics.

Do not infer distributed exactly-once or consensus.
