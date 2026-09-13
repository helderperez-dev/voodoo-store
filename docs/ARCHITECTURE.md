# Voodoo Store Architecture

## Core principle

Voodoo Store is an independent embedded application-state engine.

It is **not** a component that requires Voodoo Framework. Voodoo Framework is a first-class consumer of Voodoo Store, but the engine must remain useful from Rust, C, C++, Python, JavaScript/TypeScript, Go, Java, Swift, Kotlin, .NET, and other runtimes.

The architectural rule is:

> The storage format and semantics belong to Voodoo Store, not to any host language or framework.

## Layering

```text
Applications
    |
    +-- Voodoo Framework (Python)
    +-- Python applications
    +-- Node.js / Bun / Deno
    +-- Go
    +-- C / C++
    +-- Swift / Kotlin
    +-- Java / .NET
    |
Language bindings / adapters
    |
Stable C ABI (voodoo-store-ffi)
    |
Rust public API
    |
voodoo-store-core
    |
Storage format / transaction log / indexes / object store / queues / streams
    |
Filesystem / OS
```

Rust applications may use `voodoo-store-core` directly. Other language bindings should normally target the stable C ABI rather than duplicate engine logic.

## Crate boundaries

### `voodoo-store-core`

Owns correctness.

Responsibilities:

- on-disk format;
- transaction log;
- crash recovery;
- transaction semantics;
- KV and collection primitives;
- indexes;
- queues and leases;
- streams and event log;
- object metadata and blob references;
- compaction;
- synchronization primitives;
- replication protocol primitives.

Must not depend on:

- Python;
- Node.js;
- Voodoo Framework;
- HTTP servers;
- a cloud provider;
- a specific UI.

### `voodoo-store-ffi`

Owns the stable C ABI.

Responsibilities:

- opaque handles;
- ABI-safe primitive types;
- lifecycle functions;
- error codes;
- memory ownership rules;
- version negotiation.

The C ABI is the portability bridge for bindings.

### Future binding packages

Bindings are adapters, not alternate engines.

Examples:

```text
bindings/python     -> Python package / PyO3 or C ABI wrapper
bindings/node       -> Node package / napi-rs or C ABI wrapper
bindings/go         -> cgo wrapper
bindings/swift      -> C ABI
bindings/java       -> JNI/JNA/Panama
bindings/dotnet     -> P/Invoke
```

A native binding may use Rust-specific tooling for ergonomics, but semantics must remain identical.

## Voodoo Framework integration

Voodoo should expose Voodoo Store as a native application primitive:

```python
from voodoo import store

users = store.collection("users")
queue = store.queue("emails")
objects = store.objects("uploads")
events = store.stream("events")
```

The framework may add:

- dependency injection;
- reactive UI integration;
- workers;
- automatic event subscriptions;
- schema declarations;
- development tooling;
- observability UI.

None of those behaviors may be required to open or operate a `.vstore` file.

## Compatibility contract

A `.vstore` created by one supported language must be readable by every other supported language using a compatible engine version.

For example:

```text
Python writes app.vstore
        |
        +--> Rust reads it
        +--> Go reads it
        +--> Node reads it
        +--> Voodoo Runtime reads it
```

No language-specific serialization may leak into the mandatory storage format.

Application values are bytes at the lowest level. Higher-level codecs (JSON, MessagePack, Protobuf, typed schemas, etc.) are layered above the byte primitive and explicitly identified when persisted.

## API levels

Voodoo Store will expose multiple levels of API.

### Level 0: bytes

The universal compatibility layer.

```text
put(key: bytes, value: bytes)
get(key: bytes) -> bytes?
delete(key: bytes)
```

### Level 1: structured data

Collections, records, indexes, queries and schemas.

### Level 2: infrastructure primitives

Queues, delayed jobs, leases, streams, pub/sub, object storage, TTL and change feeds.

### Level 3: distributed state

Sync, replication and Voodoo Protocol integration.

Each level builds on lower-level invariants rather than bypassing them.

## Design rule for new features

Before a feature is merged, ask:

1. Can it work without Voodoo Framework?
2. Is its durable representation language-neutral?
3. Can it be represented through the Rust API and C ABI?
4. Does crash recovery have deterministic behavior?
5. Is backward compatibility defined?

If not, the design is incomplete.
