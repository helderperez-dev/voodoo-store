# Voodoo Store Language Binding Contract

Status: living binding contract. Python binding v0.2.2 is published; 0.3 is converging the Python surface on the existing Rust-core primitives.

This document defines the boundary between `voodoo-store-core` and language-specific bindings. It exists so Python, Node.js, Go, Swift, Java/.NET, and the Voodoo Framework can integrate without leaking host-runtime assumptions into the storage engine.

## Principle

> The Store owns bytes, durability, ordering, and transactional semantics. Bindings own host-language ergonomics.

The Rust core must not know about Python objects, JavaScript values, Go structs, Voodoo components, HTTP requests, JWTs, application models, or framework lifecycle hooks.

The persisted contract remains language-neutral bytes.

## Required binding architecture

```text
application / framework
        |
        v
language adapter
        |
        v
stable binding surface
        |
        v
voodoo-store-core
        |
        v
.vstore
```

Bindings MUST be adapters over the core. They MUST NOT reimplement persistence semantics, transaction ordering, leases, retries, CDC, queue selection, workflow transitions, or on-disk codecs.

## Ownership and lifetime

A binding must model these ownership rules explicitly:

1. `Store` owns the open file handle, process writer guard, OS lock, in-memory recovered state, and transaction counters.
2. At most one writable `Store` instance may own a `.vstore` in a process and across cooperating processes.
3. `Transaction` borrows one writable `Store` and cannot outlive it.
4. A transaction is consumed by `commit` or `rollback`.
5. A committed or rolled-back transaction handle must become unusable in the host language.
6. Bindings must close/drop the native Store deterministically when their public `close()`/context-manager/dispose equivalent runs.
7. Bindings must never duplicate or bypass the core writer lock.

## Bytes-first API

The lowest-level binding surface must accept and return byte strings/buffers.

Examples by ecosystem:

- Python: `bytes`
- Node.js: `Buffer` / `Uint8Array`
- Go: `[]byte`
- Swift: `Data`
- Java: `byte[]` / `ByteBuffer`
- .NET: `byte[]` / `ReadOnlyMemory<byte>`

Bindings may offer optional JSON/MessagePack/CBOR/Protobuf helpers above this layer, but codec choice must never change core semantics.

Bindings MUST NOT persist host-native serialization formats such as Python pickle, Java serialization, Go gob, or V8 object serialization as an implicit default.

## Error model

The native error category must survive the binding boundary.

Bindings should expose stable semantic classes/codes for at least:

- I/O failure
- store already open / writer lock contention
- corrupt header or log
- incompatible format
- torn/corrupt durable prefix
- reserved internal key
- transaction finished
- counter/time overflow
- not found where the operation requires existence
- stale lease / generation mismatch
- unique-index conflict
- invalid schema/index/query definition
- invalid job/queue/workflow transition
- verification mismatch

Bindings may attach the Rust error message for diagnostics, but application code should not have to parse strings.

No Rust panic may cross an FFI boundary.

## Store baseline

Every first-class binding should eventually expose these baseline operations:

```text
open(path, options)
close()
get(key)
put(key, value)
delete(key)
contains(key)
scan_prefix(prefix)
begin()
flush()
verify(path)
health()
storage_stats()
```

The first Python binding may stage subsystem coverage incrementally, but the API shape must preserve this model.

## Transactions

Transactions are the primary integration primitive, not an advanced optional feature.

A binding must preserve the ability to combine heterogeneous Store mutations under one commit boundary.

Conceptually:

```text
with store.transaction() as tx:
    tx.put(...)
    tx.collection_upsert(...)
    tx.enqueue_job(...)
    tx.queue_push(...)
    tx.stream_append(...)
    tx.publish(...)
    tx.link_object(...)
    tx.workflow_...(...)
    tx.emit_event(...)
    tx.request_rpc(...)
```

A binding MUST NOT translate these calls into independent autocommit operations.

Rollback must make all staged domains invisible.

## Internal namespace

Keys beginning with the Store internal namespace (`0xff + "vds:"`) are implementation details.

Bindings MUST NOT expose a normal mutation API that lets applications write arbitrary internal keys.

Bindings should expose typed subsystem APIs instead.

Read/inspection tooling may eventually expose internal records in an explicitly unsafe/diagnostic surface, but this must not be the normal application API.

## Collections

Bindings should expose typed wrappers around the existing byte-oriented collection primitives:

- create/read collection definition
- schema-version migration guard
- define/read secondary indexes
- upsert/get/delete records
- scan collection
- exact index lookup
- range query with inclusive/exclusive bounds, order, and limit

The binding may map records to host-language objects only through an explicit codec layer.

## Jobs, queues, scheduler, and triggers

Bindings must use core-owned state machines and codecs.

They must not reproduce Job or Queue wire formats.

Required semantic preservation includes:

- idempotency
- priority
- delayed availability
- attempts/max attempts
- retry timing
- leases and generation checks
- stale ACK rejection
- deadline terminalization
- durable history
- one-shot/interval/cron schedules
- transactional trigger-to-job firing

The Runtime executes handlers. Voodoo Store only persists and arbitrates durable work state.

## Messaging and RPC

Bindings should expose Streams, Topics, durable subscriptions, consumer groups, Outbox, and RPC as durable state primitives.

They must preserve:

- monotonic offsets
- replay
- durable cursors
- lease owner/generation protection
- request/response correlation
- deadlines
- explicit outbox acknowledgement

A language binding must not claim network delivery or exactly-once external side effects merely because the Store transaction committed.

## CDC

Committed change records are generated by the core transaction path.

Bindings may expose polling, iteration, async iteration, callbacks, or framework-reactive adapters, but those mechanisms must consume the same core change feed.

A callback/async API is delivery ergonomics, not a second CDC implementation.

## Objects

Object identifiers remain content hashes over bytes.

Bindings must preserve content-addressing and immutable object identity. They may expose stream/file-like convenience APIs later without changing Object IDs or reference semantics.

## Workflows

Bindings expose persisted workflow state; they do not turn Store into a code executor.

The Runtime/Voodoo layer owns:

- executing workflow/application code
- deciding which handler/function to invoke
- external API calls
- AI/model inference
- compensation policy execution

The Store owns:

- durable workflow state
- waits/signals/timers
- history
- transactional state transitions
- parent/child correlation

## Lifecycle operations

Bindings must preserve the distinction between:

- logical snapshot: equivalent logical state, independent Store identity
- physical checkpoint: physical copy preserving Store identity/history
- compact generation: compact identity-preserving candidate generation
- offline generation activation: replacement protocol with retained backup/recovery semantics

Language adapters must not collapse all four into a generic `backup()` call.

Generation activation is an offline operation. A binding must ensure the writable Store handle is closed before invoking activation/recovery APIs.

## Threading and async runtimes

Bindings may expose synchronous and asynchronous APIs, but async wrappers must not change Store correctness semantics.

Rules:

- no two mutable operations may concurrently alias one Rust `Store` without an explicit synchronization layer
- blocking filesystem work must not accidentally execute on an event-loop thread when the host runtime requires offloading
- cancellation must not report a transaction as rolled back after its native commit succeeded
- host-runtime task cancellation cannot interrupt native durability at an unsafe point

## Python binding target

The Python package is thin and Rust-backed using PyO3/maturin. The binding must continue expanding by exposing core-owned semantics rather than reproducing them in Python.

Suggested public shape:

```python
from voodoo_store import Store

with Store.open("application.vstore") as store:
    store.put(b"user:1", b"...")

    with store.transaction() as tx:
        tx.put(b"order:42", b"paid")
        job_id = tx.jobs.enqueue(
            handler=b"email.receipt",
            payload=b"order:42",
            idempotency_key=b"receipt:42",
        )
```

This example is ergonomic guidance only. The Rust core remains the semantic source of truth.

## Voodoo Framework adapter target

The Voodoo adapter should sit above the Python/native binding rather than become part of `voodoo-store-core`.

The adapter may provide conventions such as:

- project store discovery (`application.vstore`)
- application lifecycle open/close
- model/collection codecs
- background handler registration
- reactive consumption of CDC
- framework scheduler/workflow executors
- developer tooling integration

But a user must remain able to use Voodoo Store without Voodoo Framework.

## Compatibility requirements before publishing bindings

Before a binding is called stable, the repository should contain compatibility fixtures proving that:

1. a store written by Rust can be read by the binding;
2. a store written through the binding reopens correctly in Rust;
3. cross-domain transactions preserve atomicity through the binding;
4. binary keys and values survive round trips exactly;
5. errors retain semantic categories;
6. the same `.vstore` fixture is portable across supported operating systems;
7. no binding-specific serialization leaks into the native format.

## Non-goals for bindings

Bindings do not own:

- SQL compatibility
- distributed consensus
- HTTP servers
- authentication policy
- JWT/OAuth semantics
- Voodoo UI/runtime behavior
- application business logic
- network transport for RPC
- generic exactly-once guarantees for external side effects

## Gate for beginning Python/Voodoo integration

The Rust core is ready to cross into the first Python binding when all of the following are true:

- core workspace formats cleanly;
- Clippy is clean with warnings denied;
- tests pass on Linux, macOS, and Windows;
- MSRV check passes;
- transactional Job staging uses the Jobs-owned wire codec/path;
- Windows lifecycle locking tests pass;
- binding contract is committed.

At that point new Python/Voodoo integration work should consume the stable Rust semantics rather than change them opportunistically.
