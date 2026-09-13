# Voodoo Store Roadmap

## M0 — Durable log

Goal: establish the byte-level foundation.

- [x] Rust workspace
- [x] first record format
- [x] CRC validation
- [x] encode/decode tests
- [x] correctness invariants
- [ ] append-only file writer
- [ ] log scanner
- [ ] truncated-tail recovery
- [ ] transaction commit recovery
- [ ] fsync policy
- [ ] failure-injection tests
- [ ] fuzz record decoder

## M1 — Embedded KV engine

Goal: useful durable state without SQL.

- [ ] canonical key/value payload format
- [ ] put/get/delete
- [ ] in-memory index rebuilt from log
- [ ] atomic transactions
- [ ] snapshots
- [ ] compaction
- [ ] TTL semantics
- [ ] benchmark harness

## M2 — Collections and indexes

Goal: structured application data.

- [ ] typed values
- [ ] collections
- [ ] primary keys
- [ ] secondary indexes
- [ ] iterators and range scans
- [ ] query primitives

## M3 — Native queues

Goal: durable application jobs without external infrastructure.

- [ ] queue namespaces
- [ ] push
- [ ] claim/lease
- [ ] ack/nack
- [ ] retries
- [ ] delayed delivery
- [ ] priorities
- [ ] dead-letter queues

## M4 — Events and streams

Goal: make changes observable and reactive.

- [ ] append-only streams
- [ ] subscriptions
- [ ] cursors
- [ ] change feeds
- [ ] reactive query hooks
- [ ] worker integration

## M5 — Objects

Goal: local S3-like object semantics.

- [ ] content-addressed blobs
- [ ] SHA-256 object identity
- [ ] streaming writes/reads
- [ ] metadata
- [ ] deduplication
- [ ] integrity verification

## M6 — Sync and replication

Goal: connect local stores safely.

- [ ] replication protocol specification
- [ ] log shipping
- [ ] replica checkpoints
- [ ] conflict model
- [ ] Voodoo Protocol integration
- [ ] edge/runtime synchronization

## Explicitly deferred

These are intentionally out of scope until the single-node engine is demonstrably correct:

- multi-writer distributed consensus
- global transactions
- Raft/Paxos-style clustering
- SQL compatibility as a primary architecture constraint
