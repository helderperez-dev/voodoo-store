# Voodoo Store

> Application infrastructure in a store.

Voodoo Store is a Rust-native embedded application state engine for the Voodoo ecosystem. It is designed to unify durable data, key-value state, queues, event streams, and object metadata behind one local-first engine.

The long-term idea is simple:

**SQLite made the database a file. Voodoo Store aims to make application infrastructure a store.**

## Status

Early experimental development. The current focus is the durable core: log format, transactional semantics, crash recovery, and invariants.

## Design goals

- 100% Rust implementation
- embedded and zero-infrastructure by default
- durable transactional core
- deterministic crash recovery
- first-class queues and event streams
- local-first architecture with future replication and sync
- language-neutral on-disk specification
- strong testing, fuzzing, and failure injection
- simple enough to embed in Voodoo Runtime and edge-class Linux devices

## Initial architecture

```text
Voodoo Store API
      |
      +-- KV
      +-- Collections
      +-- Queue
      +-- Events
      +-- Objects
      |
Transaction / Commit Layer
      |
Append-only Log
      |
Recovery + Indexes + Compaction
      |
Filesystem
```

## Workspace

```text
crates/
  voodoo-store-core/   # core engine primitives and durable log

docs/
  SPEC.md              # evolving storage specification
  INVARIANTS.md        # correctness rules that implementation must preserve
  ROADMAP.md            # staged development plan
```

## Development principle

Correctness comes before features and benchmarks. Every storage mutation must be explainable in terms of explicit invariants and recoverable durable state.

## License

Apache-2.0
