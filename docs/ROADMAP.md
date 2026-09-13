# Voodoo Store Roadmap

The roadmap is intentionally bottom-up. Features that depend on durability, ordering, or transactions do not advance until the layer below has crash/fault coverage.

## M0 — Durable log and recovery

Goal: establish the byte-level source of truth.

- [x] Rust workspace
- [x] first record format
- [x] CRC validation
- [x] encode/decode tests
- [x] append-only writer
- [x] log scanner
- [x] transaction commit recovery
- [x] basic KV API
- [x] Rust public API
- [x] initial stable C ABI
- [x] correctness invariants
- [ ] torn-write recovery tests
- [ ] explicit fsync/sync policy
- [ ] file locking and single-writer enforcement
- [ ] store header / store UUID / format negotiation
- [ ] corruption classification
- [ ] failure-injection harness
- [ ] fuzz record decoder and recovery scanner

## M1 — Embedded KV and storage lifecycle

Goal: make the durable core efficient and operationally safe.

- [ ] canonical key/value payload format v1
- [ ] namespaced keys
- [ ] in-memory index rebuilt from log
- [ ] atomic compare-and-swap
- [ ] atomic counters
- [ ] TTL semantics
- [ ] snapshots/checkpoints
- [ ] log generations
- [ ] compaction
- [ ] hot backup / restore
- [ ] verify / inspect / repair primitives
- [ ] quotas and storage accounting
- [ ] benchmark harness

## M2 — Collections, schema, indexes and query

Goal: structured application data without making SQL the internal architecture.

- [ ] collection metadata
- [ ] typed/schema-aware values through explicit codecs
- [ ] schema versioning and migrations
- [ ] primary keys
- [ ] secondary indexes
- [ ] composite / unique indexes
- [ ] iterators and range scans
- [ ] query primitives
- [ ] query planner baseline
- [ ] full-text index prototype
- [ ] change-set generation for committed mutations

## M3 — Jobs, queues and time

Goal: durable background work and scheduling without external infrastructure.

- [ ] canonical message/job identifiers
- [ ] queue namespaces
- [ ] push / claim / ack / nack
- [ ] leases
- [ ] retries and configurable backoff
- [ ] delayed delivery
- [ ] priorities
- [ ] idempotency keys
- [ ] dead-letter queues
- [ ] one-shot schedules
- [ ] recurring schedules
- [ ] cron expressions
- [ ] durable execution history
- [ ] trigger metadata and trigger-to-job routing
- [ ] deadlines and timeout metadata

## M4 — Messaging, streams and reactive feeds

Goal: support decoupled communication, replay and live state.

- [ ] language-neutral message envelope
- [ ] topics / publish-subscribe
- [ ] durable subscriptions
- [ ] append-only streams
- [ ] stream offsets/cursors
- [ ] consumer groups
- [ ] replay
- [ ] request/reply metadata
- [ ] correlation / causation / trace identifiers
- [ ] partition keys
- [ ] explicit delivery-semantics API
- [ ] change data capture
- [ ] change feeds
- [ ] live-query invalidation/deltas
- [ ] watcher/subscription API

## M5 — Objects

Goal: embedded object/blob storage with transaction-aware references.

- [ ] content-addressed blobs
- [ ] SHA-256 object identity
- [ ] streaming writes/reads
- [ ] object metadata
- [ ] deduplication
- [ ] integrity verification
- [ ] object lifecycle policies
- [ ] transactional object references
- [ ] orphan detection / garbage collection

## M6 — Durable workflow state

Goal: persist orchestration state without turning Store into a code executor.

- [ ] workflow instance state
- [ ] step history
- [ ] durable timers
- [ ] signals
- [ ] waiting states
- [ ] compensation metadata
- [ ] parent/child execution identifiers
- [ ] restart/resume semantics

The workflow executor remains outside Voodoo Store.

## M7 — Security and operations

Goal: make Store safe to operate as embedded infrastructure.

- [ ] encryption-at-rest design
- [ ] key-rotation metadata
- [ ] namespaces and capability metadata
- [ ] structured health API
- [ ] transaction/read/write metrics
- [ ] fsync latency metrics
- [ ] queue depth and oldest-work metrics
- [ ] consumer lag
- [ ] compaction backlog
- [ ] tracing hooks
- [ ] CLI inspection tooling

## M8 — Sync, replication and Voodoo Protocol

Goal: connect correct local stores safely.

- [ ] replication protocol specification
- [ ] node/store identity
- [ ] log shipping / logical change shipping
- [ ] replica checkpoints
- [ ] offline/online synchronization
- [ ] conflict model
- [ ] distributed leases
- [ ] leader-election primitive
- [ ] Voodoo Protocol integration
- [ ] Voodoo Edge / Runtime synchronization

## Ecosystem integration milestones

These do not change core semantics; they validate portability.

- [x] Rust API
- [x] C ABI foundation
- [ ] Voodoo Framework adapter replacing SQLite/local defaults where appropriate
- [ ] Python binding package
- [ ] Node.js binding package
- [ ] Go binding
- [ ] Swift binding
- [ ] Java/.NET compatibility proof
- [ ] cross-language compatibility fixtures for the same `.vstore`

## Explicitly deferred

These remain out of scope until the single-node engine is demonstrably correct:

- generic globally distributed multi-writer consensus
- global transactions
- Raft/Paxos-style clustering as a prerequisite for local use
- SQL compatibility as a primary architecture constraint
- arbitrary user-code execution inside the storage engine
- generic exactly-once claims across external side effects
