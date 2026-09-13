# Voodoo Store

> Application infrastructure in a store.

Voodoo Store is a **standalone, 100% Rust embedded application-state engine**. It is being built for the Voodoo ecosystem, but it is not coupled to Voodoo Framework and is intended to be embedded independently from Rust, C, C++, Python, JavaScript/TypeScript, Go, Swift, Kotlin, Java, .NET, and other runtimes.

The long-term idea is simple:

**SQLite made the database a file. Voodoo Store aims to make application infrastructure a store.**

## Status

Early experimental development. The current milestone is the durable core: append-only log, transactions, commit semantics, crash recovery, and a language-neutral ABI.

The repository already contains a minimal persistent transactional KV engine and an initial C ABI.

## Design goals

- 100% Rust implementation
- independent from Voodoo Framework
- embedded and zero-infrastructure by default
- durable transactional core
- deterministic crash recovery
- first-class KV, collections, queues, event streams, and object storage
- local-first architecture with future replication and sync
- language-neutral on-disk specification
- stable C ABI for foreign-language bindings
- strong testing, fuzzing, failure injection, and compatibility tests
- simple enough to embed in desktop, server, mobile, Voodoo Runtime, and edge-class Linux applications

## Architecture

```text
Applications / Frameworks
        |
        +-- Voodoo Framework
        +-- Rust
        +-- Python
        +-- Node / Bun / Deno
        +-- Go
        +-- C / C++
        +-- Swift / Kotlin / Java / .NET
        |
Language bindings
        |
Stable C ABI (voodoo-store-ffi)
        |
Rust API
        |
voodoo-store-core
        |
        +-- KV / Collections
        +-- Queue / Leases
        +-- Streams / Events
        +-- Objects
        |
Transaction / Commit Layer
        |
Append-only Durable Log
        |
Recovery / Indexes / Compaction
        |
Filesystem
```

Rust consumers can use the core directly. Other runtimes can use the C ABI or native bindings layered on top of the same engine.

## Current Rust API

```rust
use voodoo_store_core::Store;

let mut store = Store::open("app.vstore")?;

let mut tx = store.begin();
tx.put(b"user:1", b"Helder")?;
tx.put(b"user:2", b"Bruna")?;
tx.commit()?;

assert_eq!(store.get(b"user:1"), Some(b"Helder".as_slice()));
```

Committed state survives process restarts. Operations without a durable `COMMIT` marker are ignored during recovery.

## C ABI

The initial ABI lives in `voodoo-store-ffi`, with the public header at:

```text
include/voodoo_store.h
```

Initial functions:

```text
vds_abi_version
vds_open
vds_close
vds_put
vds_get
vds_delete
```

The C ABI is intentionally small. Higher-level language bindings will add ergonomic APIs without changing storage semantics.

## Planned application primitives

```text
store.db / collections
store.kv
store.queue
store.stream
store.objects
```

These primitives will share the same transactional storage foundation rather than behaving as unrelated services.

## Workspace

```text
crates/
  voodoo-store-core/   # durable engine and correctness-critical logic
  voodoo-store-ffi/    # stable C ABI for external runtimes

include/
  voodoo_store.h       # public C header

docs/
  ARCHITECTURE.md      # standalone engine and language-binding architecture
  SPEC.md              # evolving storage specification
  INVARIANTS.md        # correctness rules the implementation must preserve
  ROADMAP.md           # staged development plan
```

## Voodoo integration

Voodoo Framework should eventually provide an idiomatic high-level layer such as:

```python
from voodoo import store

users = store.collection("users")
emails = store.queue("emails")
assets = store.objects("assets")
events = store.stream("events")
```

But `.vstore` files and the underlying engine must remain usable without Voodoo Framework.

## Compatibility principle

A store written from one supported language must be readable from another supported language using a compatible engine version.

The durable format therefore cannot depend on Python pickle, Java serialization, V8 objects, Go gob, or any host-language-specific representation. The lowest-level contract is bytes; typed codecs and schemas are layered above it.

## Development principle

Correctness comes before features and benchmarks. Every storage mutation must be explainable in terms of explicit invariants and recoverable durable state.

## License

Apache-2.0
