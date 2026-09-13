# Voodoo Store v0.1 Quickstart

Voodoo Store v0.1 is a usable embedded application-state engine. It is still pre-1.0, but the current single-node engine already provides durable application data, transactions, TTL, collections/indexes, queues, jobs, scheduling, cron, triggers, streams/topics, consumer groups, request/reply state, objects, workflows, transactional outbox events, backup/restore/snapshots and operational health without requiring Redis, PostgreSQL, Kafka, RabbitMQ, Celery or S3 by default.

The core remains framework-agnostic. Voodoo Runtime, language bindings and external adapters sit above it.

## 1. Build and test

```bash
cargo build --workspace
cargo test --workspace
```

The CI baseline covers Format, Clippy, Linux, macOS, Windows and Rust 1.85 MSRV.

## 2. Run the application-state example

The fastest way to see the current architecture is the executable example:

```bash
cargo run -p voodoo-store-core --example application_state -- application.vstore
```

It commits application state, a durable Job and an Outbox Event through one Store transaction:

```text
BEGIN
  PUT order:42:status = paid
  ENQUEUE JOB email.send_receipt(order:42)
  EMIT EVENT order.paid(order:42)
COMMIT
```

If that commit does not exist after recovery, none of those staged mutations become visible.

## 3. Basic KV from the CLI

A store is created when first opened or written:

```bash
cargo run -p voodoo-store-cli -- put app.vstore app:name demo
cargo run -p voodoo-store-cli -- get app.vstore app:name
```

Expected value:

```text
demo
```

## 4. Inspect health

```bash
cargo run -p voodoo-store-cli -- health app.vstore
```

Health/storage accounting is shared by the CLI, future Store Studio and Runtime integrations. It reports store identity, format, physical/live storage and subsystem namespaces.

## 5. Verify durability/recovery

```bash
cargo run -p voodoo-store-cli -- verify app.vstore
```

Verification validates the header, checksums and recoverable transaction prefix without mutating the store.

## 6. Backup, restore, snapshot and compact copy

```bash
cargo run -p voodoo-store-cli -- backup app.vstore app.backup.vstore
cargo run -p voodoo-store-cli -- restore-copy app.backup.vstore restored.vstore
cargo run -p voodoo-store-cli -- snapshot app.vstore snapshot.vstore
cargo run -p voodoo-store-cli -- compact-copy app.vstore compacted.vstore
```

These create explicit files rather than replacing the live Store in place. Atomic identity-preserving generation replacement remains a later lifecycle milestone.

## 7. Durable queue

Push work:

```bash
cargo run -p voodoo-store-cli -- queue-push app.vstore emails 'welcome:user:1'
```

Inspect state:

```bash
cargo run -p voodoo-store-cli -- queue-stats app.vstore emails
```

Claim with `now_ms` and a lease duration:

```bash
cargo run -p voodoo-store-cli -- queue-claim app.vstore emails 1000 30000
```

A claim returns an ID and lease generation. ACK must include both, protecting against a stale worker acknowledging a lease that has already expired and been reassigned.

```bash
cargo run -p voodoo-store-cli -- queue-ack app.vstore emails <id> <lease_generation>
```

Delayed delivery, priority, NACK/retry and dead state are also supported.

## 8. Rust API: transactions

```rust
use voodoo_store_core::{JobSpec, Store};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut store = Store::open("application.vstore")?;

    let mut tx = store.begin()?;
    tx.put(b"order:42:status", b"paid")?;
    tx.enqueue_job(JobSpec::new(b"email.send_receipt", b"order:42"), 1_000)?;
    tx.emit_event(b"order.paid", b"order:42", 1_000)?;
    tx.commit()?;

    Ok(())
}
```

This is the core Voodoo Store advantage: related application state and durable infrastructure work can share one durability boundary instead of coordinating a database and a separate broker/outbox service.

## 9. TTL / cache

```rust
let mut store = Store::open("application.vstore")?;
store.put_with_ttl(b"cache:user:42", b"cached", 10_000)?;
let value = store.get_at(b"cache:user:42", 9_000)?;
let sweep = store.purge_expired(10_000, 100)?;
```

Expiration metadata is versioned so an old expiration cannot delete a newer value accidentally.

## 10. Collections and indexes

Collections provide structured record namespaces and explicit codec/schema-version metadata. Secondary and unique indexes are maintained transactionally by the Store. The core remains byte-oriented: JSON, MessagePack, CBOR, Protobuf or Voodoo Schema are binding/framework choices rather than hard-coded on-disk host formats.

## 11. Jobs and scheduling

The Store owns durable Job state, attempts, leases, retry/backoff, deadlines, idempotency and execution history. The Runtime/worker executes the actual application code.

Scheduling currently includes:

- one-shot schedules
- interval schedules
- deterministic five-field UTC cron
- durable triggers routing into Jobs

## 12. Streams, topics and consumer groups

Streams are append-only durable event sequences with offsets and replay. Topics provide fan-out semantics through durable subscriptions. Consumer groups add owner leases, generations, ACK/NACK and redelivery after lease expiry.

The initial consumer-group model is intentionally single-partition per stream/group; future partitioning can build on the same lease/cursor semantics.

## 13. Durable request/reply

Voodoo Store can persist request/reply correlation without executing RPC code itself. A Runtime or worker consumes pending requests and atomically replaces the request with a durable response. Requests can have deadlines and can be staged inside an application transaction.

## 14. Objects

The embedded object layer currently provides SHA-256 content-addressed identity, deduplication, integrity verification, references and orphan garbage collection. Streaming I/O and richer lifecycle/retention policies remain later work.

## 15. Workflows / HITL

Workflow persistence includes durable instance state, current step, history, timers, signals, waits and restart/resume state. Voodoo Store persists orchestration state; the workflow executor remains outside the storage engine.

## 16. What is safe to use in v0.1

Good current targets:

- local/single-node applications
- Voodoo Runtime development
- SaaS and internal-tool prototypes
- desktop/local-first software
- AI agents and automation state
- edge gateways and robotics controllers
- applications that benefit from durable Jobs/Queues without deploying external infrastructure

Treat v0.1 as pre-1.0 for file-format/API compatibility. Back up important stores before upgrades and pin the exact Store version in production experiments.

## 17. What still blocks a 1.0 claim

The remaining hardening work includes continuous fuzzing/soak tests, identity-preserving log generations and in-place compaction, richer query/range/composite indexes, CDC/live feeds, full cross-domain transaction coverage, quotas/metrics/tracing, complete C ABI/bindings, encryption-at-rest design and later replication/sync.

Those are 1.0/distributed milestones. They do not prevent beginning controlled use of the current single-node v0.1 engine today.
