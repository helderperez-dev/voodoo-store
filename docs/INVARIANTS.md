# Voodoo Store Correctness Invariants

These invariants are normative. Implementations may change, but these rules must remain true unless the specification is deliberately versioned.

## Durability

1. A transaction reported as committed must survive process restart after the durability boundary promised by the active sync mode.
2. An uncommitted transaction must never become visible as committed during recovery.
3. Recovery must be deterministic for the same durable byte sequence.
4. `Durability::Relaxed` must never be documented as guaranteeing survival across power loss.

## Integrity

5. Corrupt records must be detected before their payload is applied.
6. A partially written trailing record must never be interpreted as valid state.
7. A physically incomplete trailing record may be truncated only when it is the final physical tail of the file.
8. Corruption inside the previously valid durable prefix must fail recovery; it must never be silently converted into tail repair.
9. Record sequence numbers within a log generation must be strictly monotonic.
10. Unknown mandatory record kinds must fail safely rather than be silently applied.
11. Once a transaction has a commit record, no later record in the same generation may reuse that transaction identifier.

## Atomicity

12. A transaction is applied completely or not at all.
13. A commit marker is the authority that makes a transaction recoverably visible.
14. Recovery must never expose a prefix of a transaction as committed state.
15. In-memory visible state is updated only after the commit record has been written and the configured durability action has completed successfully.

## Identity and ordering

16. Transaction identifiers identify one logical transaction within the store generation.
17. Durable mutation order is defined by log sequence, not wall-clock time.
18. Replaying valid committed records in log order must produce the same logical state.
19. Exactly one process may hold the writable Store lock for a file at a time.

## Backup and compaction

20. A physical backup represents a consistent source-store byte sequence after the source has crossed a full sync boundary.
21. Compact-copy must never modify its source file.
22. Compact-copy must never overwrite an existing destination.
23. A successful compact-copy must reopen as a valid Store and expose the same committed logical key/value state as the source at the compaction boundary.
24. Compact-copy may discard obsolete log history, deleted values, rolled-back work, and other state that is not logically visible.
25. In-place atomic replacement is not equivalent to compact-copy and must not be introduced until replacement semantics are fault-tested on every supported platform.

## Queue invariants

26. Queue push must atomically persist both the message and advancement of the queue-local next identifier.
27. A successfully acknowledged queue item must not be delivered again in the same logical queue history.
28. An expired lease may make an unacknowledged item eligible for redelivery.
29. Each successful claim advances the message lease generation.
30. A stale worker must not acknowledge, nack, or dead-letter work after a newer claim generation exists.
31. Delayed messages are not eligible before `available_at_ms`.
32. Among eligible messages in the current queue mode, higher priority wins; equal priority is resolved by lower message identifier.
33. Dead messages are not claimable unless an explicit future operation changes their state.
34. Queue records must remain durable across Store reopen according to the Store durability mode.

## Namespace isolation

35. Engine-owned metadata namespaces must not be writable through the public user KV API.
36. Higher-level Store primitives may use engine-owned namespaces only through internal APIs that preserve their invariants.
37. User scans and user KV access must not accidentally expose engine-internal representation as application data once namespace isolation is enabled.

## Future object invariants

38. Content-addressed object identifiers must correspond to the exact object bytes under the declared hash algorithm.
39. Metadata must never claim an object is durable before the object durability boundary has been satisfied.

## Development rule

Every feature that changes durable state must document which invariants it relies on, which invariants it preserves, and how tests exercise failure at the relevant boundary.

The implementation sequence remains:

```text
SPEC
 ↓
INVARIANTS
 ↓
IMPLEMENTATION
 ↓
TESTS
 ↓
FUZZING
 ↓
BENCHMARK
 ↓
OPTIMIZATION
```
