# Voodoo Store Roadmap

The roadmap is intentionally bottom-up. Features that depend on durability, ordering, or transactions do not advance until the layer below has crash/fault coverage.

## Current release target — v0.1 functional embedded application-state engine

Voodoo Store is already beyond a KV/queue prototype. The current single-node engine persists structured data, background work, scheduling, messaging, objects and workflow state in one `.vstore` without requiring external infrastructure.

Implemented today:

- [x] versioned `.vstore` header and persistent store identity
- [x] checksummed append-only log
- [x] atomic transactions and deterministic recovery
- [x] torn-tail detection and repair
- [x] single-writer locking across Linux/macOS/Windows
- [x] explicit durability modes
- [x] transactional KV + prefix scan
- [x] protected engine-internal mutation namespace
- [x] compare-and-swap, signed counters and TTL
- [x] verify, backup, restore-copy, compact-copy and logical snapshot primitives
- [x] collections with schema-version/codec metadata and secondary/unique indexes
- [x] durable queues, jobs, one-shot/interval schedules and cron schedules
- [x] durable triggers routing into Jobs
- [x] topics, streams, durable subscriptions and replay
- [x] content-addressed object storage with deduplication, verification and orphan GC
- [x] durable workflow state with signals, timers, waits and history
- [x] storage/namespace health accounting
- [x] first typed cross-domain transaction primitive (`KV + Job` in one commit)
- [x] standalone CLI
- [x] C ABI v2 foundation with transactions, errors and panic containment
- [x] deterministic torn-write, corruption and process-crash tests
- [x] Format + Clippy + Linux/macOS/Windows + Rust 1.85 CI

The remaining work moves this single-node engine from broad functional coverage toward a hardened 1.0 and later distributed operation.

## M0 — Durable log and recovery

Goal: establish the byte-level source of truth.

- [x] Rust workspace
- [x] record format + CRC
- [x] append-only writer and scanner
- [x] transaction commit recovery
- [x] explicit durability policy
- [x] file locking and single-writer enforcement
- [x] format header / store identity / compatibility baseline
- [x] corruption surfaced rather than silently accepted in the durable prefix
- [x] deterministic torn-write and process-crash harness
- [x] deterministic byte-mutation/truncation sweeps
- [x] MSRV 1.85 CI
- [x] Linux/macOS/Windows CI
- [ ] continuous fuzzing of decoder/recovery scanner
- [ ] long-running durability soak tests

## M1 — Embedded KV and storage lifecycle

Goal: make the durable core efficient and operationally safe.

- [x] canonical byte-oriented KV format
- [x] in-memory index rebuilt from log
- [x] prefix scans
- [x] protected user/internal namespaces
- [x] CAS and signed counters
- [x] TTL semantics and expiration purge
- [x] physical backup
- [x] verified create-only restore
- [x] verified compact-copy
- [x] logical create-only snapshots
- [x] storage accounting and amplification reporting
- [ ] identity-preserving checkpoints
- [ ] log generations
- [ ] atomic in-place cross-platform generation replacement / compaction
- [ ] repair tooling beyond torn-tail repair
- [ ] quotas / per-namespace limits
- [ ] benchmark harness and performance budgets

## M2 — Collections, schema, indexes and query

Goal: structured application data without making SQL the internal architecture.

- [x] collection metadata
- [x] explicit codec metadata
- [x] schema-version baseline
- [x] primary keys
- [x] secondary indexes
- [x] unique indexes
- [x] exact secondary-index lookup
- [ ] migration/evolution tooling
- [ ] iterators and range scans
- [ ] composite indexes
- [ ] query expressions and projections
- [ ] query planner baseline
- [ ] full-text index prototype
- [ ] committed change-set metadata / CDC integration

## M3 — Jobs, queues, time and triggers

Goal: durable background work and scheduling without external infrastructure.

Queues:

- [x] push / claim / ack / nack
- [x] leases and lease expiry
- [x] delayed delivery
- [x] priorities
- [x] durable retry and dead state
- [x] stale-ACK rejection
- [x] queue statistics and dead-message purge

Jobs and time:

- [x] dedicated durable Jobs abstraction
- [x] 128-bit job identifiers
- [x] job idempotency keys
- [x] attempts, retry/backoff, leases and deadlines
- [x] durable execution history
- [x] one-shot schedules
- [x] interval schedules
- [x] five-field UTC cron parser (`*`, lists, ranges, steps)
- [x] durable cron schedules with occurrence idempotency
- [x] trigger definitions and trigger-to-job routing
- [x] atomic job state + history transitions
- [x] atomic one-shot/interval schedule fire + schedule advance
- [x] first cross-domain transaction API: application KV mutation + Job enqueue in one commit
- [ ] remove duplicate Job wire encoding from transactional helper by centralizing internal codec
- [ ] extend cross-domain transactions to Queue / Stream / Topic / Object references
- [ ] trigger firing fully transactional with trigger metadata update
- [ ] richer retry policies and jitter
- [ ] timezone-aware cron as an optional layer (core remains deterministic UTC)

## M4 — Messaging, streams and reactive feeds

Goal: support decoupled communication, replay and live state.

- [x] language-neutral message envelope model
- [x] delivery-semantics model
- [x] correlation / causation / execution / trace IDs
- [x] partition-key model
- [x] topics / publish-subscribe baseline
- [x] durable subscriptions/cursors
- [x] append-only streams
- [x] stream offsets
- [x] replay
- [ ] consumer groups with ownership/leases
- [ ] request/reply implementation
- [ ] RPC correlation helpers
- [ ] change data capture from committed transactions
- [ ] change feeds
- [ ] live-query invalidation/deltas
- [ ] watcher/subscription API

## M5 — Objects

Goal: embedded object/blob storage with transaction-aware references.

- [x] content-addressed blobs
- [x] SHA-256 object identity
- [x] deduplication
- [x] integrity verification
- [x] object references
- [x] orphan detection / garbage collection
- [ ] streaming writes/reads
- [ ] richer object metadata
- [ ] retention/lifecycle policies
- [ ] object-reference operations inside public cross-domain transactions

## M6 — Durable workflow state

Goal: persist orchestration state without turning Store into a code executor.

- [x] workflow instance state
- [x] current step/state payload
- [x] step/history log
- [x] durable timers
- [x] signals / HITL waits
- [x] waiting states and resume
- [x] parent/child correlation
- [x] restart/reopen semantics
- [ ] compensation metadata/policies
- [ ] workflow operations integrated with the public cross-domain transaction surface

The workflow executor remains outside Voodoo Store.

## M7 — Security and operations

Goal: make Store safe to operate as embedded infrastructure.

- [x] reserved internal namespaces
- [x] strong store identity from OS entropy
- [x] structured health API
- [x] storage and namespace accounting
- [x] queue state statistics
- [x] CLI inspection and operation surface
- [x] CLI verify / backup / restore / compaction / snapshot commands
- [x] CLI Collections / Queue / Messaging / Objects / Jobs / Scheduler / Cron / Trigger / Workflow operations
- [ ] encryption-at-rest design and key rotation metadata
- [ ] namespace capability metadata
- [ ] transaction/read/write counters and latency metrics
- [ ] fsync latency metrics
- [ ] oldest-work / consumer-lag metrics
- [ ] compaction backlog metrics
- [ ] tracing hooks
- [ ] quotas

## M7.5 — Voodoo Store Studio

Goal: provide a first-class local visual administration experience for every major Store primitive.

Principle: if Voodoo Store replaces multiple infrastructure services, Studio should replace the multiple dashboards normally required to operate them.

Planned command surface:

- [ ] `voodoo-store studio app.vstore`
- [ ] `voodoo store studio` with automatic project-store discovery
- [ ] configurable local port and `--no-open`
- [ ] read-only mode
- [ ] explicit capability gate for destructive mutations

Planned surfaces:

- [ ] Overview / health / storage
- [ ] KV / Collections / Schema / Indexes
- [ ] Queues / Jobs / Scheduler / Cron / Triggers
- [ ] Topics / Streams / Subscriptions
- [ ] Objects
- [ ] Workflows / signals / timers / history
- [ ] Backup / restore / compaction / snapshots
- [ ] Observability

Architecture requirements:

- [ ] Studio is a separate consumer of stable Store APIs
- [ ] Studio is built with Voodoo when the framework capability is mature enough
- [ ] Store core never depends on Voodoo
- [ ] localhost-only by default
- [ ] remote mode requires authentication/capabilities
- [ ] no business logic duplicated between CLI and Studio

## M8 — Sync, replication and Voodoo Protocol

Goal: connect correct local stores safely.

- [x] cryptographically strong store identity
- [ ] replication protocol specification
- [ ] canonical node identity
- [ ] logical change shipping / log shipping
- [ ] replica checkpoints
- [ ] offline/online synchronization
- [ ] conflict model
- [ ] distributed leases
- [ ] leader-election primitive
- [ ] Voodoo Protocol integration
- [ ] Voodoo Edge / Runtime synchronization

## Ecosystem integration milestones

- [x] Rust API
- [x] C ABI v2 foundation
- [x] C ABI transactions and `last_error`
- [x] standalone CLI
- [ ] complete C ABI coverage for Queue / Collections / Jobs / Streams / Objects / Workflows
- [ ] Python binding package
- [ ] Node.js binding package
- [ ] Go binding
- [ ] Swift binding
- [ ] Java/.NET compatibility proof
- [ ] cross-language compatibility fixtures for the same `.vstore`
- [ ] Voodoo Framework adapter
- [ ] Voodoo Store Studio
- [ ] main `voodoo` CLI integration

## Zero External Infrastructure milestone

A normal single-node Voodoo application should be able to use the following with no required external service:

- [x] durable KV / application state
- [x] cache / TTL
- [x] collections and indexes
- [x] queues
- [x] durable jobs
- [x] delayed / recurring / cron scheduling
- [x] triggers
- [x] topics / streams / replay
- [x] object/blob storage baseline
- [x] durable HITL waiting state
- [x] workflow persistence
- [x] operational health / verify / backup / restore / compact-copy / snapshot tooling
- [ ] full cross-domain transactional surface for all primitives
- [ ] CDC/live feeds
- [ ] visual administration through Store Studio

External providers remain optional adapters for workloads that outgrow the embedded deployment model.

## Explicitly deferred

These remain out of scope until the single-node engine is demonstrably production-ready:

- generic globally distributed multi-writer consensus
- global transactions
- Raft/Paxos-style clustering as a prerequisite for local use
- SQL compatibility as a primary architecture constraint
- arbitrary user-code execution inside the storage engine
- generic exactly-once claims across external side effects
