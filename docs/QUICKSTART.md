# Voodoo Store v0.1 Quickstart

This quickstart exercises the current functional embedded core: durable KV, transactions, recovery verification, backup, and the embedded queue.

## 1. Build and test

```bash
cargo build --workspace
cargo test --workspace
```

## 2. Create a store implicitly

A store is created the first time it is opened or written.

```bash
cargo run -p voodoo-store-cli -- put app.vstore app:name demo
cargo run -p voodoo-store-cli -- get app.vstore app:name
```

Expected value:

```text
demo
```

The resulting `app.vstore` contains a versioned checksummed header followed by the durable transaction log.

## 3. Verify the file

```bash
cargo run -p voodoo-store-cli -- verify app.vstore
```

The command reports:

```text
store_id=...
format=1.0
file_bytes=...
valid_bytes=...
records=...
committed_transactions=...
pending_transactions=...
keys=...
torn_tail=false
```

Verification does not mutate the store.

## 4. Create a backup

```bash
cargo run -p voodoo-store-cli -- backup app.vstore app.backup.vstore
cargo run -p voodoo-store-cli -- verify app.backup.vstore
```

The backup preserves the same durable file contents and store identity.

## 5. Use the durable queue

Push a job:

```bash
cargo run -p voodoo-store-cli -- queue-push app.vstore emails 'welcome:user:1'
```

Inspect queue state:

```bash
cargo run -p voodoo-store-cli -- queue-stats app.vstore emails
```

Claim work. The two numbers are `now_ms` and lease duration in milliseconds:

```bash
cargo run -p voodoo-store-cli -- queue-claim app.vstore emails 1000 30000
```

The response includes a message ID and `lease_generation`, for example:

```text
id=1
lease_generation=1
attempts=1
priority=0
lease_until_ms=31000
payload=welcome:user:1
```

Acknowledge it using both identifiers:

```bash
cargo run -p voodoo-store-cli -- queue-ack app.vstore emails 1 1
```

The lease generation is important: if a worker stalls, its lease expires, and another worker reclaims the message, the old worker cannot later ACK the new lease accidentally.

## 6. Delayed and priority work

Push a message available at timestamp 5000 with priority 10:

```bash
cargo run -p voodoo-store-cli -- queue-push app.vstore jobs urgent 5000 10
```

A claim before timestamp 5000 will not return it. Among simultaneously eligible messages, higher priority is selected first; equal-priority messages are selected in queue ID order.

## 7. Retry with nack

After claiming a message, return it to the queue for a later time:

```bash
cargo run -p voodoo-store-cli -- queue-nack app.vstore jobs <id> <lease_generation> 10000
```

It becomes eligible again at timestamp 10000. Each future claim increments the delivery attempt / lease generation.

## 8. Dead-letter a message

```bash
cargo run -p voodoo-store-cli -- queue-dead app.vstore jobs <id> <lease_generation>
```

Dead messages remain durable and visible in queue statistics until purged through the Rust API.

## 9. Rust API

```rust
use voodoo_store_core::Store;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut store = Store::open("app.vstore")?;

    store.put(b"user:1", b"Helder")?;

    let mut tx = store.begin()?;
    tx.put(b"user:2", b"Bruna")?;
    tx.put(b"settings:theme", b"dark")?;
    tx.commit()?;

    let mut queue = store.queue(b"jobs")?;
    queue.push(b"generate-report")?;

    if let Some(job) = queue.claim(1_000, 30_000)? {
        // Application/runtime executes the actual code here.
        queue.ack(job.id, job.lease_generation)?;
    }

    Ok(())
}
```

Voodoo Store persists the durable work semantics. It does not execute arbitrary user code inside the storage engine.

## 10. What v0.1 proves

The current release demonstrates the architecture needed for the larger Voodoo goal:

```text
application data
      +
durable background work
      +
crash recovery
      +
operational verify/backup
      |
      v
one embedded .vstore engine
```

The next milestones add collections/indexes/query, jobs/scheduler/cron, topics/streams, objects, lifecycle compaction, and Voodoo Framework adapters on the same transaction foundation.
