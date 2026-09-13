# Voodoo Store Roadmap

The roadmap is intentionally bottom-up. Features that depend on durability, ordering, or transactions do not advance until the layer below has crash/fault coverage.

## Current release target — v0.1 functional embedded core

The first functional release is intentionally narrower than the full Voodoo Store vision. It establishes a standalone engine that can already persist application state and run durable background queues without an external service.

Implemented today:

- [x] versioned `.vstore` header and persistent store identity
- [x] checksummed append-only log
- [x] atomic transactions and deterministic recovery
- [x] torn-tail detection and repair
- [x] single-writer locking across supported desktop/server targets
- [x] explicit durability modes
- [x] transactional KV + prefix scan
- [x] protected engine-internal mutation namespace
- [x] atomic compare-and-swap and signed counters
- [x] verify, physical backup, verified restore-copy and compact-copy primitives
- [x] durable queue with lease, delayed delivery, priority, nack/retry and dead-letter state
- [x] stale-ACK protection through lease generations
- [x] standalone CLI
- [x] initial C ABI
- [x] deterministic torn-write, corruption and process-crash tests
- [x] formatting + Clippy + workspace tests in CI
- [x] Linux, macOS, Windows and Rust 1.85 MSRV CI coverage

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
- [x] deterministic process-crash failure-injection harness
- [x] deterministic byte-mutation and truncation sweeps for header/record decoding
- [ ] fuzz record decoder and recovery scanner
- [x] MSRV CI lane
- [x] cross-platform CI lane for Linux/macOS/Windows

## M1 — Embedded KV and storage lifecycle

Goal: make the durable core efficient and operationally safe.

- [x] canonical byte-oriented key/value payload format v1
- [x] in-memory index rebuilt from log
- [x] prefix scans
- [x] consistent physical backup
- [x] verify / inspect foundation
- [x] protected internal/user mutation namespaces
- [x] atomic compare-and-swap
- [x] atomic counters
- [ ] TTL semantics
- [ ] snapshots/checkpoints
- [ ] log generations
- [x] safe verified compact-copy generation
- [ ] atomic in-place cross-platform compaction / generation replacement
- [x] verified create-only restore workflow
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
- [x] CLI verify / backup / restore-copy / compact-copy lifecycle commands

## M7.5 — Voodoo Store Studio

Goal: provide a first-class local visual administration experience for every major Store primitive.

The Studio is a local web application started by the Store CLI and later exposed through the main Voodoo CLI. It must operate directly against Voodoo Store APIs rather than introducing a second storage model.

Planned command surface:

- [ ] `voodoo-store studio app.vstore`
- [ ] `voodoo store studio` from a Voodoo project, with automatic store discovery
- [ ] configurable local port and `--no-open` mode
- [ ] read-only mode for safe inspection
- [ ] explicit opt-in for destructive mutations

Planned visual surfaces:

- [ ] Overview: store identity, format version, size, durability mode, health and recovery status
- [ ] Data: collections/models, KV namespaces, filtering, sorting and record editing
- [ ] Schema: fields, indexes, migrations and relationships
- [ ] Queues: depth, ready/leased/dead messages, payload inspection, retry, nack and purge
- [ ] Jobs: status, attempts, execution history, retry policy and manual re-run
- [ ] Scheduler: one-shot jobs, recurring schedules and cron timeline
- [ ] Messaging: topics, subscriptions, streams, offsets and replay
- [ ] Objects: blob metadata, size, references, integrity and lifecycle state
- [ ] Workflows: durable instances, current step, waits, timers, signals and history
- [ ] Operations: verify, backup, restore, compaction, retention and storage accounting
- [ ] Observability: recent transactions, traces, queue latency, consumer lag and health metrics

Architecture requirements:

- [ ] Studio UI remains a separate consumer of stable Store APIs
- [ ] no business logic duplicated between CLI and Studio
- [ ] localhost-only by default
- [ ] mutation APIs protected by explicit capability boundaries
- [ ] future remote mode must require authentication and must not weaken the local embedded security model
- [ ] design system aligned with the broader Voodoo visual language so the same shell can later power Runtime/Builder tooling

## M8 — Sync, replication and Voodoo Protocol

Goal: connect correct local stores safely.

- [ ] replication protocol specification
- [x] cryptographically strong store identity generation from OS entropy
- [ ] canonical node identity for distributed deployments
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
- [ ] Voodoo Store Studio local web application
- [ ] main `voodoo` CLI integration for Store Studio and operational commands

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
- [x] operational inspect/backup/verify/restore-copy/compact-copy tooling
- [ ] visual local administration through Voodoo Store Studio

External providers can remain optional adapters for workloads that outgrow the embedded deployment model.

## Explicitly deferred

These remain out of scope until the single-node engine is demonstrably correct:

- generic globally distributed multi-writer consensus
- global transactions
- Raft/Paxos-style clustering as a prerequisite for local use
- SQL compatibility as a primary architecture constraint
- arbitrary user-code execution inside the storage engine
- generic exactly-once claims across external side effects
