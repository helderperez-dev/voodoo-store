# Voodoo Store Correctness Invariants

These invariants are normative. Implementations may change, but these rules must remain true unless the specification is deliberately versioned.

## Durability

1. A transaction reported as committed must survive process restart after the durability boundary promised by the active sync mode.
2. An uncommitted transaction must never become visible as committed during recovery.
3. Recovery must be deterministic for the same durable byte sequence.

## Integrity

4. Corrupt records must be detected before their payload is applied.
5. A partially written trailing record must never be interpreted as valid state.
6. Record sequence numbers within a log generation must be monotonic.
7. Unknown mandatory record kinds must fail safely rather than be silently applied.

## Atomicity

8. A transaction is applied completely or not at all.
9. A commit marker is the authority that makes a transaction recoverably visible.
10. Recovery must never expose a prefix of a transaction as committed state.

## Identity and ordering

11. Transaction identifiers identify one logical transaction within the store generation.
12. Durable mutation order is defined by log sequence, not wall-clock time.
13. Replaying valid committed records in log order must produce the same logical state.

## Future queue invariants

14. A successfully acknowledged queue item must not be delivered again in the same logical queue history.
15. An expired lease may make an unacknowledged item eligible for redelivery.
16. Queue ordering guarantees must be explicit per queue mode and never inferred accidentally from implementation details.

## Future object invariants

17. Content-addressed object identifiers must correspond to the exact object bytes under the declared hash algorithm.
18. Metadata must never claim an object is durable before the object durability boundary has been satisfied.

## Development rule

Every feature that changes durable state must document which invariants it relies on, which invariants it preserves, and how tests exercise failure at the relevant boundary.
