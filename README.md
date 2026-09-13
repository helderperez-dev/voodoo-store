# Voodoo Store

> **Application infrastructure in a store.**

Voodoo Store is a **standalone, 100% Rust embedded application-state engine** built around one goal:

> **A Voodoo application should be able to run robustly with Voodoo + Voodoo Store and no mandatory external infrastructure by default.**

It is being built for the Voodoo ecosystem, but it is not coupled to Voodoo Framework. The engine and `.vstore` format are designed to be usable independently from Rust, C, C++, Python, JavaScript/TypeScript, Go, Swift, Kotlin, Java, .NET, and other runtimes.

**SQLite made the database a file. Voodoo Store aims to make application infrastructure a store.**

## Status

**v0.1 functional embedded core.**

The repository now contains a working single-node embedded engine with:

- versioned and checksummed `.vstore` file header;
- persistent store identity;
- checksummed append-only durable log;
- atomic transactions with commit markers;
- deterministic crash recovery;
- torn-tail detection and repair;
- single-writer file locking;
- configurable durability (`Strict`, `Data`, `Relaxed`);
- transactional byte-oriented KV storage;
- prefix scans;
- verification and consistent physical backup;
- durable queues with leases, delayed delivery, priorities, retry/nack, dead-letter state, and stale-ACK protection;
- standalone `voodoo-store` CLI;
- initial C ABI for KV access.

CI validates formatting, Clippy with warnings denied, and the Rust workspace tests.

This is a functional development release, **not yet a production 1.0**. Collections/indexes/query, scheduler/cron, topics/streams, objects, compaction, replication, fuzzing, and broader language bindings remain under active development.

## North Star

The default deployment we are working toward is intentionally small:

```text
Application
    |
    +-- Voodoo Runtime
    |
    `-- app.vstore
```

No Redis, RabbitMQ, Celery, external cron service, Kafka, or separate local object service should be required merely to get a robust Voodoo application running.

External infrastructure remains possible when a workload genuinely requires it. It is not the default architecture.

## Architecture

```text
Applications / Frameworks
        |
        +-- Voodoo Framework
        +-- Rust / C / C++
        +-- Python / Node / Go
        +-- Swift / Kotlin / Java / .NET
        |
Language bindings / adapters
        |
Stable C ABI (voodoo-store-ffi)
        |
Rust API
        |
voodoo-store-core
        |
        +-- KV
        +-- Durable Queue / Leases
        +-- Messaging model primitives
        +-- Automation model primitives
        |
Transaction / Commit Layer
        |
Append-only Checksummed Log
        |
Recovery / Verification / Backup
        |
Versioned .vstore Header
        |
Filesystem + File Locking
```

Rust consumers use the core directly. Other runtimes can use the C ABI or native bindings layered on top of the same storage semantics.

## Rust quick start

```rust
use voodoo_store_core::Store;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut store = Store::open("app.vstore")?;

    // Autocommit KV
    store.put(b"user:1", b"Helder")?;
    assert_eq!(store.get(b"user:1"), Some(b"Helder".as_slice()));

    // Atomic multi-operation transaction
    let mut tx = store.begin()?;
    tx.put(b"user:2", b"Bruna")?;
    tx.put(b"settings:theme", b"dark")?;
    tx.commit()?;

    // Durable embedded queue
    let mut queue = store.queue(b"emails")?;
    queue.push(b"welcome:user:1")?;

    let job = queue.claim(1_000, 30_000)?.expect("queued message");
    queue.ack(job.id, job.lease_generation)?;

    Ok(())
}
```

Committed state and queue state survive process restarts.

## Durability

The engine exposes explicit durability policy:

```rust
use voodoo_store_core::{Durability, Store, StoreOptions};

let store = Store::open_with_options(
    "app.vstore",
    StoreOptions {
        durability: Durability::Strict,
        repair_torn_tail: true,
    },
)?;
```

- `Strict` uses `sync_all()` on commit.
- `Data` uses `sync_data()` on commit and is the default.
- `Relaxed` relies on later operating-system flushing.

A torn final record is treated as an interrupted write. With tail repair enabled, Voodoo Store truncates the file back to the last fully valid record before accepting new writes.

## Queue semantics

The current durable queue supports:

```text
push
push_at
priority
claim + lease
lease expiry / reclaim
ack
nack + delayed retry
dead-letter
queue stats
purge dead
```

Every successful claim increments a lease generation. An old worker cannot acknowledge a message after its lease expired and another worker reclaimed the same message.

Delivery is therefore designed around explicit at-least-once semantics with durable identity rather than unsafe exactly-once claims.

## CLI

The workspace includes the `voodoo-store` binary.

```bash
cargo run -p voodoo-store-cli -- put app.vstore hello world
cargo run -p voodoo-store-cli -- get app.vstore hello
cargo run -p voodoo-store-cli -- verify app.vstore
cargo run -p voodoo-store-cli -- backup app.vstore app.backup.vstore

cargo run -p voodoo-store-cli -- queue-push app.vstore emails 'welcome:user:1'
cargo run -p voodoo-store-cli -- queue-claim app.vstore emails 1000 30000
cargo run -p voodoo-store-cli -- queue-stats app.vstore emails
```

See `docs/QUICKSTART.md` for a complete walkthrough.

## Verification and backup

```rust
let report = Store::verify("app.vstore")?;
println!("records = {}", report.records);
println!("keys = {}", report.keys);
println!("torn tail = {}", report.has_torn_tail());

let store = Store::open("app.vstore")?;
store.backup_to("app.backup.vstore")?;
```

Verification checks the header, record framing, checksums, transaction ordering, and recoverable state without mutating the source file.

## C ABI

The initial ABI lives in `voodoo-store-ffi`, with the public header at:

```text
include/voodoo_store.h
```

Current foundation:

```text
vds_abi_version
vds_open
vds_close
vds_put
vds_get
vds_delete
```

Higher-level queue, transaction, messaging, and automation bindings will be added without changing the language-neutral storage contract.

## Where this is going

The target application-state engine is broader than KV + queue:

```text
Voodoo Store
├── Data
│   ├── KV
│   ├── Collections
│   ├── Schema
│   ├── Indexes
│   └── Query
├── Messaging
│   ├── Queues
│   ├── Topics
│   ├── Streams
│   ├── Subscriptions
│   └── Request/Reply
├── Automation
│   ├── Jobs
│   ├── Scheduler
│   ├── Cron
│   ├── Delayed work
│   └── Triggers
├── Objects
├── Durable workflow state
├── Change feeds / live queries
├── Snapshots / compaction / backup
└── Sync / replication / Voodoo Protocol
```

These capabilities share one transactional foundation rather than behaving as unrelated services.

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

Voodoo Framework will eventually make Store the zero-infrastructure default behind its existing high-level primitives. Application code should not need to know which internal Store records implement a model, task, event, or schedule.

Conceptually:

```text
Voodoo Model       -> Voodoo Store data
Voodoo @task       -> Voodoo Store jobs/queue
Voodoo Scheduler   -> Voodoo Store time/schedules
Voodoo Mesh        -> Voodoo Store durable messaging
Voodoo ObjectStore -> Voodoo Store objects
Execution/HITL     -> Voodoo Store durable runtime state
```

The engine itself remains usable without Voodoo.

## Compatibility principle

A store written from one supported language must be readable from another supported language using a compatible engine version.

The durable format cannot depend on Python pickle, Java serialization, V8 objects, Go gob, or another host-specific representation. The lowest-level contract is bytes; typed codecs and schemas are layered above it.

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
