# Voodoo Store

> **Application infrastructure in a store.**

Voodoo Store is a **standalone, 100% Rust embedded application-state engine** built around one goal:

> **A Voodoo application should be able to run robustly with Voodoo + Voodoo Store and no mandatory external infrastructure by default.**

It is being built for the Voodoo ecosystem, but the core is not coupled to Voodoo Framework. The engine and `.vstore` format are designed to remain language-neutral and independently usable.

**SQLite made the database a file. Voodoo Store aims to make application infrastructure a store.**

## Status

**Current published Python binding: v0.2.2. Core and bindings remain pre-1.0.**

Voodoo Store is pre-1.0, but the Rust core and the published Python binding are already suitable for controlled single-node development and production experiments where the current compatibility and operational limits are understood. The next integration line is **0.3 Native Surface Convergence**: exposing more of the existing Rust primitives directly to Python and Voodoo rather than reimplementing them over KV.

The current engine includes:

- versioned, checksummed `.vstore` files and strong persistent Store identity;
- checksummed append-only logging, atomic transactions and deterministic crash recovery;
- Linux/macOS/Windows single-writer locking and explicit durability modes;
- byte-oriented KV, CAS, counters, prefix scans and TTL;
- Collections with schema/codec metadata and secondary/unique indexes;
- durable Queues with leases, delay, priority, retry/NACK, dead state and stale-ACK protection;
- durable Jobs with 128-bit IDs, idempotency, retry/backoff, deadlines and execution history;
- one-shot, interval and deterministic UTC Cron scheduling;
- durable Triggers;
- Topics, Streams, replay, durable subscriptions and Consumer Groups;
- durable request/reply correlation and RPC state;
- transactional Outbox events;
- content-addressed SHA-256 object storage, deduplication, verification, references and orphan GC;
- durable Workflow/HITL state with waits, signals, timers and history;
- verify, backup, create-only restore, logical snapshots and compact-copy;
- structured health/storage accounting;
- standalone `voodoo-store` CLI;
- C ABI v2 foundation with transactions, `last_error` and panic containment;
- deterministic corruption, torn-write and process-crash testing;
- CI across Format, Clippy, Linux, macOS, Windows and Rust 1.85 MSRV.

This is **not yet a production 1.0**. File/API compatibility should still be considered pre-1.0, and important data should be backed up before upgrading experimental deployments.

## Start here

Build and test:

```bash
cargo build --workspace
cargo test --workspace
```

Run the executable application-state example:

```bash
cargo run -p voodoo-store-core --example application_state -- application.vstore
```

The example commits application state, a durable Job and an Outbox Event through one transaction:

```text
BEGIN
  PUT order:42:status = paid
  ENQUEUE JOB email.send_receipt(order:42)
  EMIT EVENT order.paid(order:42)
COMMIT
```

If that transaction does not commit, none of those staged mutations become visible after recovery.

See `docs/QUICKSTART.md` for the full walkthrough.

## North Star

The standard Voodoo deployment is intentionally small:

```text
Application
    |
    +-- Voodoo Runtime
    |
    `-- application.vstore
```

A normal application should not need Redis, PostgreSQL, RabbitMQ, Kafka, Celery, a separate cron service, or a separate local object service merely to get robust application infrastructure.

External infrastructure remains available as optional adapters when scale or deployment topology genuinely requires it.

## Architecture

```text
Applications / Frameworks
        |
        +-- Voodoo Runtime / Framework
        +-- Rust
        +-- C / native bindings
        +-- Python binding
        +-- future Node / Go / Swift bindings
        |
Stable APIs / bindings
        |
voodoo-store-core
        |
        +-- Data: KV / TTL / Collections / Indexes
        +-- Work: Queues / Jobs / Scheduler / Cron / Triggers
        +-- Messaging: Topics / Streams / Consumer Groups / RPC / Outbox
        +-- Objects
        +-- Workflow state
        +-- Operations / health / lifecycle
        |
Transaction / Commit Layer
        |
Append-only Checksummed Log
        |
Recovery / Verification
        |
Versioned .vstore Header
        |
Filesystem + File Locking
```

The Store persists durable semantics. Voodoo Runtime executes application code, HTTP handlers, AI inference, external calls and Identity/Auth behavior.

## Cross-domain transactions

A major Voodoo Store goal is to remove the split-brain normally created by a database plus external work infrastructure.

Current typed transaction primitives already allow application state, Jobs, Outbox Events and durable RPC Requests to share a Store transaction.

```rust
use voodoo_store_core::{JobSpec, Store};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut store = Store::open("application.vstore")?;

    let mut tx = store.begin()?;
    tx.put(b"order:42:status", b"paid")?;
    tx.enqueue_job(JobSpec::new(b"email.send_receipt", b"order:42"), 1_000)?;
    tx.emit_event(b"order.paid", b"order:42", 1_000)?;
    tx.request_rpc(b"payments.capture", b"order:42", 1_000, Some(31_000))?;
    tx.commit()?;

    Ok(())
}
```

The public cross-domain transaction surface will continue expanding to additional primitives before 1.0.

## Durability

```rust
use voodoo_store_core::{Durability, Store, StoreOptions};

let store = Store::open_with_options(
    "application.vstore",
    StoreOptions {
        durability: Durability::Strict,
        repair_torn_tail: true,
    },
)?;
```

- `Strict` uses `sync_all()` on commit.
- `Data` uses `sync_data()` on commit and is the default.
- `Relaxed` relies on later operating-system flushing.

Only an incomplete physical tail is automatically repairable. Corruption inside the durable prefix is surfaced as an error rather than silently discarded.

## Current good-fit workloads

The current pre-1.0 line is a reasonable target for controlled use in:

- Voodoo Runtime development;
- SaaS/internal-tool prototypes and early deployments;
- desktop/local-first applications;
- AI agent and automation state;
- edge gateways and robotics controllers;
- single-node APIs that want durable Jobs/Queues without deploying an infrastructure stack.

Pin the exact Store version and keep backups for important pre-1.0 stores.

## Known pre-1.0 limits

The main remaining work before a 1.0 claim includes:

- continuous fuzzing and long-running durability soak tests;
- filesystem/power-loss proof for generation activation and stronger repair tooling;
- Python/native surface convergence for messaging, objects, CDC, operations and cross-domain transactions;
- richer Collection migration/composite-index/query-planner support and physical ordered range seeks;
- live-query/watch convenience APIs above the durable change feed;
- quotas, richer metrics and tracing;
- streaming object I/O and lifecycle policies;
- complete C ABI coverage and additional first-class language bindings;
- encryption-at-rest/key-rotation design;
- later replication/sync and Voodoo Protocol integration.

Store Studio and distributed operation are later milestones and do not block controlled single-node use.

## CLI

The workspace includes the standalone `voodoo-store` binary. Examples:

```bash
cargo run -p voodoo-store-cli -- put application.vstore hello world
cargo run -p voodoo-store-cli -- get application.vstore hello
cargo run -p voodoo-store-cli -- health application.vstore
cargo run -p voodoo-store-cli -- verify application.vstore
cargo run -p voodoo-store-cli -- backup application.vstore application.backup.vstore
```

The CLI also exposes lifecycle, Queue, Collection, Messaging, Object, Job, Scheduler, Cron, Trigger and Workflow operations. See `docs/QUICKSTART.md` for usage guidance.

## C ABI

`voodoo-store-ffi` is the portability foundation for non-Rust bindings. The current ABI includes Store handles, KV operations, buffered transactions, thread-local `last_error` reporting and panic containment. Higher-level primitive coverage is still expanding before the ABI is considered complete.

Public header:

```text
include/voodoo_store.h
```

## Workspace

```text
crates/
  voodoo-store-core/   # correctness-critical embedded engine
  voodoo-store-ffi/    # C ABI portability layer
  voodoo-store-cli/    # standalone inspection and operation CLI

include/
  voodoo_store.h

docs/
  ARCHITECTURE.md
  SPEC.md
  INVARIANTS.md
  ROADMAP.md
  QUICKSTART.md
```

## Voodoo integration

Voodoo Framework/Runtime already consumes the published Python binding behind higher-level primitives while the engine stays independently usable. The 0.3 convergence work removes remaining compatibility implementations where the Rust core already has a native primitive.

```text
Voodoo Model       -> Store Collections / data
Voodoo @task       -> Store Jobs / Queues
Voodoo Scheduler   -> Store schedules / Cron
Voodoo events      -> Store Topics / Streams / Outbox
Voodoo ObjectStore -> Store Objects
Execution / HITL   -> Store Workflow state
Runtime Identity   -> durable state persisted through Store
```

Identity/Auth semantics remain in Voodoo Runtime, not in Voodoo Store.

## Compatibility principle

The lowest-level durable contract is bytes. Voodoo Store does not persist host-specific Python pickle, Java serialization, V8 objects or Go gob as its core format. Typed codecs and schemas are layered above the engine so a compatible Store can be accessed from multiple runtimes.

## Development principle

Correctness comes before feature count and benchmarks:

```text
SPEC
  -> INVARIANTS
  -> IMPLEMENTATION
  -> TESTS
  -> FAULT INJECTION
  -> FUZZING
  -> BENCHMARKS
  -> OPTIMIZATION
```

## License

Apache-2.0
