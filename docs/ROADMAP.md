# Voodoo Store Roadmap

The roadmap is intentionally bottom-up. Features that depend on durability, ordering, or transactions do not advance until the layer below has crash/fault coverage.

## Current release target — v0.1 functional embedded core

The first functional release is intentionally narrower than the full Voodoo Store vision. It establishes a standalone engine that can already persist application state and run durable background queues without an external service.

Implemented today:

- [x] versioned `.vstore` header and persistent store identity
- [x] checksummed append-only log
- [x] atomic transactions and deterministic recovery
- [x] torn-tail detection and repair
- [x] single-writer locking
- [x] explicit durability modes
- [x] transactional KV + prefix scan
- [x] verify and physical backup primitives
- [x] durable queue with lease, delayed delivery, priority, nack/retry and dead-letter state
- [x] stale-ACK protection through lease generations
- [x] standalone CLI
- [x] initial C ABI
- [x] formatting + Clippy + workspace tests in CI

The remaining items below move the engine from a functional embedded core toward the broader goal: Voodoo + Voodoo Store as a zero-required-infrastructure application platform.

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
- [x] initial C ABI
- [x] correctness invariants
- [x] torn-write recovery tests
- [x] explicit fsync/sync policy
- [x] file locking and single-writer enforcement
- [x] store header / store identity / format negotiation
- [x] corruption surfaced rather than silently accepted in the durable prefix
- [ ] deterministic failure-injection harness
- [ ] fuzz record decoder and recovery scanner
- [ ] MSRV CI lane
- [ ] cross-platform CI lane for Linux/macOS/Windows

## M1 — Embedded KV and storage lifecycle

Goal: make the durable core efficient and operationally safe.

- [x] canonical byte-oriented key/value payload format v1
- [x] in-memory index rebuilt from log
- [x] prefix scans
- [x] consistent physical backup
- [x] verify / inspect foundation
- [ ] protected internal/user namespaces
- [ ] atomic compare-and-swap
- [ ] atomic counters
- [ ] TTL semantics
- [ ] snapshots/checkpoints
- [ ] log generations
- [ ] safe cross-platform compaction
- [ ] restore workflow
- [ ] repair tooling beyond torn-tail repair
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

- [x] queue namespaces
- [x] push / claim / ack / nack
- [x] leases and lease expiry
- [x] delayed delivery
- [x] priorities
- [x] durable retry through nack/requeue
- [x] dead-letter state
- [x] stale-ACK rejection
- [x] queue stats and dead-message purge
- [ ] canonical globally unique message/job identifiers
- [ ] idempotency keys
- [ ] configurable max-attempt and backoff policies
- [ ] dedicated jobs abstraction over queues
- [ ] one-shot schedules
- [ ] recurring schedules
- [ ] cron expressions
- [ ] durable execution history
- [ ] trigger metadata and trigger-to-job routing
- [ ] deadlines and timeout metadata
- [ ] transaction API that can atomically mutate application data and enqueue work in one commit

## M4 — Messaging, streams and reactive feeds

Goal: support decoupled communication, replay and live state.

- [x] language-neutral message envelope model
- [x] delivery-semantics model
- [x] correlation / causation / execution / trace identifier model
- [x] partition-key model
- [ ] topics / publish-subscribe
- [ ] durable subscriptions
- [ ] append-only streams
- [ ] stream offsets/cursors
- [ ] consumer groups
- [ ] replay
- [ ] request/reply implementation
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
- [x] queue depth/state statistics
- [ ] oldest-work metrics
- [ ] consumer lag
- [ ] compaction backlog
- [ ] tracing hooks
- [x] CLI inspection and operation foundation

## M8 — Sync, replication and Voodoo Protocol

Goal: connect correct local stores safely.

- [ ] replication protocol specification
- [ ] stronger node/store identity generation for distributed use
- [ ] log shipping / logical change shipping
- [ ] replica checkpoints
- [ ] offline/online synchronization
- [ ] conflict model
- [ ] distributed leases
- [ ] leader-election primitive
- [ ] Voodoo Protocol integration
- [ ] Voodoo Edge / Runtime synchronization

## Ecosystem integration milestones

These do not change core semantics; they validate portability and make Voodoo Store the default infrastructure substrate of the Voodoo ecosystem.

- [x] Rust API
- [x] C ABI foundation
- [x] standalone CLI
- [ ] C ABI transactions and queue API
- [ ] Voodoo Framework adapter replacing SQLite/local defaults where appropriate
- [ ] Python binding package
- [ ] Node.js binding package
- [ ] Go binding
- [ ] Swift binding
- [ ] Java/.NET compatibility proof
- [ ] cross-language compatibility fixtures for the same `.vstore`

## Zero External Infrastructure milestone

Voodoo + Voodoo Store reaches the product North Star when a normal application can use all of the following without a required external service:

- [x] durable KV/application state foundation
- [x] durable background queue foundation
- [ ] persistent collections/models and indexes
- [ ] cache/TTL primitives
- [ ] jobs and workers
- [ ] scheduler / delayed jobs / cron
- [ ] events / topics / streams
- [ ] object/blob storage
- [ ] durable execution and HITL waiting state
- [ ] workflow persistence
- [ ] operational inspect/backup/verify/compact tooling

External providers can remain optional adapters for workloads that outgrow the embedded deployment model.

## Explicitly deferred

These remain out of scope until the single-node engine is demonstrably correct:

- generic globally distributed multi-writer consensus
- global transactions
- Raft/Paxos-style clustering as a prerequisite for local use
- SQL compatibility as a primary architecture constraint
- arbitrary user-code execution inside the storage engine
- generic exactly-once claims across external side effects
