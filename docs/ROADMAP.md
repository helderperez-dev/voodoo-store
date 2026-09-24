# Voodoo Store Roadmap

The roadmap is intentionally bottom-up. Features that depend on durability, ordering, or transactions do not advance until the layer below has crash/fault coverage.

## Current position — Python v0.2.2 published; 0.3 Native Surface Convergence active

Voodoo Store is already beyond a KV/queue prototype. The current single-node engine persists structured data, background work, scheduling, messaging, objects and workflow state in one `.vstore` without requiring external infrastructure.

Implemented today:

- [x] versioned `.vstore` header and persistent store identity
- [x] checksummed append-only log
- [x] atomic transactions and deterministic recovery
- [x] transactional staged reads and staged prefix scans
- [x] torn-tail detection and repair
- [x] single-writer locking across Linux/macOS/Windows
- [x] explicit durability modes
- [x] transactional KV + prefix scan
- [x] protected engine-internal mutation namespace
- [x] compare-and-swap, signed counters and TTL
- [x] verify, backup, restore-copy, compact-copy and logical snapshot primitives
- [x] identity-preserving physical checkpoints
- [x] identity-preserving compact generations with transaction/sequence high-water continuity
- [x] recoverable offline generation activation with stale-generation rejection and retained rollback backup
- [x] collections with schema-version/codec metadata and secondary/unique indexes
- [x] exact and byte-range secondary-index queries with ordering and limits
- [x] durable queues, jobs, one-shot/interval schedules and cron schedules
- [x] durable triggers routing into Jobs
- [x] fully transactional trigger fire + Job + trigger metadata update
- [x] topics, streams, durable subscriptions, replay and consumer groups
- [x] durable request/reply correlation and RPC state
- [x] transactional Outbox events
- [x] automatic committed-transaction CDC/change feed
- [x] content-addressed object storage with deduplication, verification and orphan GC
- [x] durable workflow state with signals, timers, waits and history
- [x] workflow mutations integrated with the shared transaction surface
- [x] storage/namespace health accounting
- [x] typed cross-domain transactions spanning application KV, Collections, Jobs, Queues, Streams/Topics, Objects, Outbox, RPC and Workflows
- [x] transaction-aware Job idempotency, including prior staged jobs
- [x] Jobs-owned v1 wire codec path shared by normal and transactional submission
- [x] standalone CLI
- [x] C ABI v2 foundation with transactions, errors and panic containment
- [x] deterministic torn-write, corruption and process-crash tests
- [x] deterministic transactional state-model stress test with repeated reopen/recovery
- [x] Format + Clippy + Linux/macOS/Windows + Rust 1.85 CI baseline
- [x] executable application-state example and adoption quickstart
- [x] language binding contract defining ownership, bytes-first semantics and framework boundaries

The Python binding is published at **v0.2.2** and Voodoo Framework 3.0 already consumes it as the default durable substrate. The next milestone is **0.3 Native Surface Convergence**: expose existing Rust-core primitives directly through the binding and remove Framework compatibility implementations that currently fall back to generic KV.

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
- [x] deterministic transactional state-model stress test
- [x] MSRV 1.85 CI
- [x] Linux/macOS/Windows CI
- [ ] continuous fuzzing of decoder/recovery scanner
- [ ] long-running durability soak tests

## M1 — Embedded KV and storage lifecycle

Goal: make the durable core efficient and operationally safe.

- [x] canonical byte-oriented KV format
- [x] in-memory index rebuilt from log
- [x] prefix scans
- [x] transaction-local staged reads / prefix scans
- [x] protected user/internal namespaces
- [x] CAS and signed counters
- [x] TTL semantics and expiration purge
- [x] physical backup
- [x] verified create-only restore
- [x] verified compact-copy
- [x] logical create-only snapshots
- [x] storage accounting and amplification reporting
- [x] identity-preserving physical checkpoints
- [x] compact log generations retaining Store ID
- [x] high-water marker preventing transaction/sequence reuse after generation activation
- [x] offline generation activation with stale-generation detection
- [x] recovery helper for interrupted two-rename activation
- [ ] directory-fsync/power-loss proof for generation activation on every supported filesystem
- [ ] repair tooling beyond torn-tail and generation-activation recovery
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
- [x] unique-index enforcement across staged writes in the same transaction
- [x] exact secondary-index lookup
- [x] byte-range secondary-index query
- [x] inclusive/exclusive bounds
- [x] deterministic ascending/descending ordering and limits
- [x] transactional record/index mutation surface
- [x] committed change-set metadata / CDC integration
- [ ] migration/evolution tooling beyond version compare-and-swap
- [ ] physical ordered-index range seek (current baseline decodes/sorts logical index entries)
- [ ] composite-index encoding helpers
- [ ] query expressions and projections
- [ ] query planner baseline
- [ ] full-text index prototype

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
- [x] queue enqueue in shared cross-domain transactions

Jobs and time:

- [x] dedicated durable Jobs abstraction
- [x] 128-bit job identifiers
- [x] job idempotency keys
- [x] transaction-aware idempotency lookup across committed and staged Jobs
- [x] attempts, retry/backoff, leases and deadlines
- [x] durable execution history
- [x] one-shot schedules
- [x] interval schedules
- [x] five-field UTC cron parser (`*`, lists, ranges, steps)
- [x] durable cron schedules with occurrence idempotency
- [x] trigger definitions and trigger-to-job routing
- [x] atomic job state + history transitions
- [x] atomic one-shot/interval schedule fire + schedule advance
- [x] application KV + Job enqueue in one transaction
- [x] cross-domain transaction operations for Queue / Stream / Topic / Objects
- [x] trigger firing fully transactional with trigger metadata update
- [x] central Jobs-owned v1 wire codec/staging path used by transactional enqueue
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
- [x] consumer groups with ownership/leases, generation-based stale-ACK protection and redelivery
- [x] durable request/reply implementation
- [x] RPC correlation helpers and durable deadlines
- [x] transactional Outbox events with explicit ACK
- [x] automatic CDC from committed transactions
- [x] ordered change feed by transaction and mutation sequence
- [x] CDC pruning without recursive maintenance events
- [ ] live-query invalidation/deltas
- [ ] watcher/subscription convenience API above the durable feed

## M5 — Objects

Goal: embedded object/blob storage with transaction-aware references.

- [x] content-addressed blobs
- [x] SHA-256 object identity
- [x] deduplication
- [x] integrity verification
- [x] object references
- [x] orphan detection / garbage collection
- [x] object creation/reference primitives in cross-domain transactions
- [ ] streaming writes/reads (current engine materializes values in memory)
- [ ] richer object metadata
- [ ] retention/lifecycle policies

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
- [x] workflow creation/step/wait/signal/finalization integrated with shared transactions
- [x] staged workflow history sequence allocation
- [ ] compensation metadata/policies

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
- [x] physical checkpoint and generation APIs in core
- [x] deterministic state-model stress coverage
- [ ] encryption-at-rest design and key rotation metadata
- [ ] namespace capability metadata
- [ ] transaction/read/write counters and latency metrics
- [ ] fsync latency metrics
- [ ] oldest-work / consumer-lag metrics
- [ ] compaction backlog metrics
- [ ] tracing hooks
- [ ] quotas

## Pre-Python integration gate

Goal: stop Rust-core churn before exposing the engine as the default state layer of Voodoo.

Required before starting the Python/Voodoo implementation:

- [x] byte-oriented core independent of Python/Voodoo
- [x] single shared transaction boundary for primary application-state primitives
- [x] transaction-local staged reads and scans
- [x] automatic durable CDC/change feed
- [x] Collections exact/range query baseline
- [x] durable Jobs/Queues/Messaging/RPC/Objects/Workflow state
- [x] transactional Trigger -> Job routing
- [x] physical checkpoint primitive
- [x] compact identity-preserving generation primitive
- [x] stale-generation-safe offline activation and recovery helper
- [x] deterministic cross-platform crash/fault/state-model tests
- [x] C ABI foundation proving language-neutral engine ownership
- [x] centralize Job v1 wire codec/staging before freezing the binding-facing job contract
- [x] final core CI gate after the codec cleanup
- [x] write the binding-surface contract / ownership rules

**This gate is closed. The Python package is published and the Voodoo Framework adapter is live. New work should now converge the binding on already-implemented Rust semantics rather than expanding the core opportunistically.**

## M7.25 — Python/native surface convergence (0.3)

Goal: make the published Python/Voodoo surface reflect the capabilities that already exist in the Rust core.

This milestone is intentionally about **convergence, not new storage semantics**.

- [x] Python KV and transaction baseline
- [x] Python Collections CRUD and exact secondary-index lookup
- [x] Python Jobs, Scheduler/Cron and Triggers
- [x] Python native range-query surface
- [x] Python native Streams / Topics / durable subscription cursors
- [x] Python native Objects / references / orphan GC
- [x] Python Consumer Groups
- [x] Python Outbox
- [x] Python RPC
- [x] Python CDC/change-feed inspection
- [x] Python operational health/storage/checkpoint/compaction/restore surfaces
- [ ] Python cross-domain transaction surface matching the Rust transaction model
- [ ] Voodoo Events adapter migrated from KV compatibility storage to native messaging
- [ ] Voodoo ObjectStore adapter migrated from KV compatibility storage to native Objects
- [ ] Voodoo Model query path uses native indexes/range queries where declared
- [ ] end-to-end crash/rollback acceptance proving heterogeneous operations share one commit boundary

The 0.3 gate closes when application code can reach the Store's differentiating primitives without bypassing Runtime ownership or depending on internal KV encodings.

---

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
- [ ] Topics / Streams / Subscriptions / Consumer Groups / RPC / Outbox
- [ ] Objects
- [ ] Workflows / signals / timers / history
- [ ] Backup / restore / compaction / snapshots / generations
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
- [x] durable ordered local change feed suitable as a replication source primitive
- [x] physical identity-preserving checkpoints
- [ ] replication protocol specification
- [ ] canonical node identity beyond Store ID
- [ ] logical change shipping / log shipping protocol
- [ ] replica checkpoints and catch-up protocol
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
- [x] language-binding ownership/bytes/error contract
- [ ] complete C ABI coverage for Queue / Collections / Jobs / Streams / Objects / Workflows
- [x] Python binding package — **v0.2.2 published**
- [x] Voodoo Framework Store adapter — **default substrate in Voodoo 3.0**
- [ ] Python/native surface convergence — **0.3 milestone**
- [ ] Node.js binding package
- [ ] Go binding
- [ ] Swift binding
- [ ] Java/.NET compatibility proof
- [ ] cross-language compatibility fixtures for the same `.vstore`
- [ ] Voodoo Store Studio
- [ ] main `voodoo` CLI integration

## Zero External Infrastructure milestone

A normal single-node Voodoo application should be able to use the following with no required external service:

- [x] durable KV / application state
- [x] cache / TTL
- [x] collections and indexes
- [x] range-query baseline
- [x] queues
- [x] durable jobs
- [x] delayed / recurring / cron scheduling
- [x] triggers
- [x] topics / streams / replay / consumer groups
- [x] durable request/reply and transactional Outbox
- [x] object/blob storage baseline
- [x] durable HITL waiting state
- [x] workflow persistence
- [x] shared cross-domain transaction surface for the primary primitives
- [x] automatic CDC/change feeds
- [x] operational health / verify / backup / restore / compact-copy / snapshot / checkpoint / generation tooling
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
