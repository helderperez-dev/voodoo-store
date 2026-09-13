# Voodoo Store Architecture

## Core principle

Voodoo Store is an independent embedded application-state engine written in Rust.

It is **not** a component that requires Voodoo Framework. Voodoo Framework is a first-class consumer of Voodoo Store, but the engine must remain useful from Rust, C, C++, Python, JavaScript/TypeScript, Go, Java, Swift, Kotlin, .NET, and other runtimes.

> The storage format and semantics belong to Voodoo Store, not to any host language or framework.

## Alignment with Voodoo

Voodoo's runtime model is:

```text
Entity -> State -> Intent -> Capability -> Execution -> Effect -> State
```

Voodoo Store does not own or duplicate that ontology. It is the durable substrate beneath the pieces that need persistence, ordering, time, delivery, recovery, retention, or replay.

```text
Voodoo Framework / Runtime

Entity / State / Execution / Effect / Time / Event
                     |
                     v
               Voodoo Store
                     |
    +----------------+----------------+
    |                |                |
   Data           Messaging       Automation
    |                |                |
    +----------------+----------------+
                     |
             Transaction Core
                     |
            Durable Ordered Log
                     |
          Recovery / Index / Pages
                     |
                 Filesystem
```

### Boundary rule

If a capability primarily concerns **state, time, delivery, consistency, ordering, durability, retention, or replay**, it is a candidate for Voodoo Store.

If it primarily concerns **executing user code, HTTP, UI rendering, model inference, orchestration policy, network topology, or application-specific authorization decisions**, it belongs outside the Store.

Therefore:

- Store persists execution state; Voodoo Runtime executes work.
- Store persists schedules; a worker/runtime dispatches handlers.
- Store persists messages; Voodoo Mesh or another transport moves them between nodes.
- Store may persist capability metadata; Voodoo Runtime evaluates authorization policy.
- Store exposes change feeds; Voodoo UI decides how to render them.
- Store stores workflow state; a workflow executor runs code steps.

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
Storage format / transaction log / indexes / objects / messaging / time
    |
Filesystem / OS
```

Rust applications may use `voodoo-store-core` directly. Other language bindings should normally target the stable C ABI rather than duplicate engine logic.

## Crate boundaries

### `voodoo-store-core`

Owns correctness and portable semantics.

Responsibilities:

- on-disk format
- transaction log
- crash recovery
- transaction semantics
- KV and collection primitives
- indexes and query substrate
- queues, topics, streams and leases
- schedules, delayed jobs and trigger metadata
- object metadata and blob references
- snapshots and compaction
- synchronization primitives
- replication protocol primitives
- operational inspection data

Must not depend on:

- Python
- Node.js
- Voodoo Framework
- HTTP servers
- model providers
- a cloud provider
- a specific UI

### `voodoo-store-ffi`

Owns the stable C ABI:

- opaque handles
- ABI-safe primitive types
- lifecycle functions
- error codes
- memory ownership rules
- version negotiation

The C ABI is the portability bridge for bindings.

### Future bindings

Bindings are adapters, not alternate engines.

```text
bindings/python  -> Python package
bindings/node    -> Node package
bindings/go      -> cgo wrapper
bindings/swift   -> C ABI
bindings/java    -> JNI/JNA/Panama
bindings/dotnet  -> P/Invoke
```

## Voodoo integration map

Voodoo should be able to map its current provider protocols onto Store without changing application semantics:

```text
Voodoo Model             -> Store Collections / Query
VoodooDatabase           -> Store Data
VoodooQueue / @task      -> Store Queue + Job
Voodoo Scheduler         -> Store Time + Schedule
VoodooEventBus / Mesh    -> Store Topic / Stream / Subscription
Voodoo Execution         -> Store durable execution state
VoodooObjectStore        -> Store Objects
VoodooCache              -> Store KV + TTL
Voodoo telemetry         -> Store trace/correlation metadata
HITL waiting             -> Store durable waiting state / signal
```

The Voodoo adapter may add dependency injection, decorators, Python schemas, reactive UI integration, worker execution, automatic subscriptions, and development tooling. None of those may be required to open or operate a `.vstore` store.

## Capability domains

### Data

- byte-oriented KV
- typed collections through explicit codecs/schema layers
- primary, secondary and composite indexes
- uniqueness and range constraints
- query engine
- schema metadata and migrations
- TTL
- compare-and-swap and atomic counters
- full-text search as a later optional index type

### Objects

- large blobs outside normal page records
- streaming reads/writes
- content addressing
- integrity hashes
- metadata
- references that can participate in transactions

### Messaging

Messaging is broader than queues:

- durable queues
- topics and publish/subscribe
- streams
- replay
- consumer groups
- durable subscriptions
- request/reply metadata
- correlation and causation IDs
- idempotency keys
- delivery attempts
- dead-letter routing

A canonical message envelope should be language-neutral and carry destination, payload, headers, creation/availability time, tracing identifiers, reply metadata, partition key, and idempotency identity.

The Store must not make an unqualified generic `exactly once` claim. It should expose explicit delivery semantics and support effectively-once effects through durable message identity plus transactional idempotency.

### Automation and time

Time is a first-class storage dimension because the Voodoo runtime treats scheduling, retries, deadlines, waiting states and lifecycle as meaningful execution concerns.

Store should support durable representations for:

- one-shot jobs
- delayed jobs
- recurring schedules
- cron expressions
- leases
- retry/backoff policies
- triggers
- execution history
- `available_at`
- deadlines
- lease expiration
- retry time
- retention deadlines
- TTL

Store owns the durable intent to run work. It does not execute arbitrary application code.

### Workflows

Store may persist:

- durable state-machine state
- timers
- signals
- waiting states
- compensation metadata
- step history

The workflow executor remains outside Store.

### Reactive state

- change data capture
- change feeds
- subscriptions
- live-query invalidation/deltas
- watchers

Feeds are storage primitives; UI/WebSocket behavior stays outside Store.

### Reliability

- transactions across primitives
- deterministic crash recovery
- file locking / single-writer policy
- configurable sync policy
- snapshots/checkpoints
- compaction
- hot backup
- restore
- verification and repair tooling
- fault injection and fuzzing

### Lifecycle

- TTL
- stream retention
- queue retention
- object lifecycle policies
- log generation rotation
- compaction policies
- quotas

### Security primitives

- encryption-at-rest hooks
- checksums/integrity
- namespaces
- opaque capability/authorization metadata
- key-rotation metadata

Store does not become Voodoo's policy engine.

### Operations

- health
- stats
- queue depth
- consumer lag
- oldest pending work
- fsync latency
- transaction rates
- compaction backlog
- storage/page/object size accounting
- structured inspection APIs

### Distributed evolution

Future layers may add:

- replication
- offline/online synchronization
- conflict resolution
- distributed leases
- leader election
- node identity
- Voodoo Protocol integration

Consensus and global multi-writer transactions are deliberately deferred until local correctness is mature.

## Transactional convergence

A defining feature is that application-infrastructure primitives eventually share one transaction boundary:

```text
BEGIN
  collection insert
  KV update
  enqueue job
  publish event
  append stream record
  create schedule
  link object
COMMIT
```

After a successful durable commit, all effects exist. Before commit, none are externally visible. Recovery must never expose a partial committed transaction.

This allows embedded deployments to avoid entire classes of transactional-outbox and cross-service consistency problems.

## Observability metadata

Store records should be able to carry opaque operational identifiers without understanding their application meaning:

- trace ID
- correlation ID
- causation ID
- execution ID
- parent execution ID
- source/node ID

This aligns with Voodoo's observability model while preserving Store independence.

## Compatibility contract

A `.vstore` created by one supported language must be readable by every other supported language using a compatible engine version.

```text
Python writes app.vstore
        |
        +--> Rust reads it
        +--> Go reads it
        +--> Node reads it
        +--> Voodoo Runtime reads it
```

No host-language serialization may leak into mandatory storage semantics. At the lowest layer keys and values are bytes. Higher-level codecs such as JSON, MessagePack, CBOR, Protobuf or schema-defined values are explicit layers above bytes.

## API levels

### Level 0: bytes

```text
put(key: bytes, value: bytes)
get(key: bytes) -> bytes?
delete(key: bytes)
```

### Level 1: structured data

Collections, records, indexes, schemas, migrations and queries.

### Level 2: infrastructure primitives

Queues, jobs, schedules, triggers, topics, streams, pub/sub, object storage, TTL and change feeds.

### Level 3: durable orchestration state

Workflows, signals, waiting states, live queries and operational histories.

### Level 4: distributed state

Sync, replication and Voodoo Protocol integration.

Each level builds on lower-level invariants rather than bypassing them.

## Development order

1. Durable ordered log and crash recovery.
2. Transactional KV and storage metadata.
3. Locking, sync policy, snapshots and compaction.
4. Collections/index/query substrate.
5. Durable jobs, queues, schedules and triggers.
6. Topics, streams, subscriptions and change feeds.
7. Objects and content addressing.
8. Live queries and workflow persistence.
9. Replication/sync and Voodoo Protocol integration.

Every layer must preserve published invariants and have crash/fault tests before becoming a dependency of the next layer.

## Design rule for new features

Before a feature is merged, ask:

1. Can it work without Voodoo Framework?
2. Is its durable representation language-neutral?
3. Can it be represented through the Rust API and C ABI?
4. Does crash recovery have deterministic behavior?
5. Is backward compatibility defined?
6. Can it participate in a common transaction model where appropriate?
7. Does it preserve the boundary between durable state and code execution?

If not, the design is incomplete.

## Non-goals

Voodoo Store is not a web framework, HTTP server, AI runtime, UI framework, arbitrary code-execution engine, or a claim to replace globally distributed hyperscale databases for every workload.

The goal is an embedded application-infrastructure engine that can replace multiple external components for a broad class of local-first, edge, desktop, service and moderate-scale server workloads.
