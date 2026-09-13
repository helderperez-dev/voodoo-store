# Voodoo Store Specification v0.1-draft

## 1. Scope

This document defines the first durable layer of Voodoo Store and the compatibility rules that future capabilities must preserve.

Version 0.1 focuses on an append-only log, record integrity, transaction identity, commit markers, deterministic recovery semantics, and language-neutral core types. Collections, indexes, queues, schedules, messaging, objects, streams, workflows, replication, and query execution build on this layer.

Voodoo Store is an independent embedded application-state engine. Voodoo Framework is a first-class consumer, not a dependency of the storage engine.

## 2. Principles

- The on-disk format is language-neutral even though the implementation is 100% Rust.
- Correctness and recoverability take precedence over throughput.
- The log is the initial source of truth for durable mutations.
- Wall-clock time is not used to define durable ordering.
- Format changes require explicit versioning.
- Bindings are adapters over one engine, not separate implementations of semantics.
- Host-language serialization is never mandatory storage format.
- Durable state and code execution remain separate concerns.
- Capabilities that share a transaction must obey the same atomicity and recovery rules.

## 3. Record format

All integer fields are little-endian.

```text
+----------------+----------+
| Field          | Size     |
+----------------+----------+
| magic          | 4 bytes  |
| kind           | 1 byte   |
| tx_id          | 8 bytes  |
| sequence       | 8 bytes  |
| payload_length | 4 bytes  |
| payload        | N bytes  |
| crc32          | 4 bytes  |
+----------------+----------+
```

`magic` is currently the ASCII byte sequence `VDS1`.

The CRC32 covers every byte from `magic` through the final payload byte. It does not include the checksum field itself.

## 4. Record kinds

Current draft values:

```text
1 = PUT
2 = DELETE
3 = COMMIT
```

Unknown record kinds are not applied by a reader that does not understand them.

Future record families may represent collection mutations, queue/job state, stream appends, subscriptions, schedules, object references, checkpoints, and metadata. New durable semantics require explicit format/version rules.

## 5. Transactions

A mutation record belongs to a logical transaction identified by `tx_id`.

A transaction becomes recoverably visible only when a valid `COMMIT` record for the transaction is present within the durable log according to the configured sync mode.

Until then, its mutation records are provisional.

A recovery implementation must never expose only a prefix of the mutations belonging to a committed transaction.

### 5.1 Cross-primitive transactions

The long-term transaction model is intentionally broader than database rows. A single transaction may eventually contain operations such as:

```text
BEGIN
  collection insert
  KV update
  enqueue job
  publish topic message
  append stream item
  create schedule
  link object
COMMIT
```

If the transaction commits, every supported durable effect becomes visible according to its primitive semantics. If it does not commit, none become externally visible.

This is a core architectural requirement, not a Voodoo Framework-specific feature.

## 6. Ordering

`sequence` defines durable record ordering inside a log generation.

Sequence values must be monotonic. The first production writer uses strictly increasing sequence numbers.

Wall-clock timestamps may be stored as metadata, but they do not define storage ordering.

Streams, queues, schedules, and replication may introduce additional ordering scopes; each must define whether ordering is global, per stream, per partition, per destination, or unspecified.

## 7. Recovery

A reader scans records in log order and validates structural length, magic, kind, and checksum before interpreting a record.

A trailing incomplete record is treated as an interrupted write and is ignored after the last fully valid record. Corruption in previously durable regions must surface as an integrity error rather than be silently ignored.

Recovery groups mutation records by transaction and applies only transactions that have a valid commit marker.

Future recovery rules for queues, schedules, leases, streams, snapshots, objects, and replication must reduce to deterministic reconstruction from committed durable state.

## 8. File layout

The first implementation may use a single append-only log file. Future revisions may introduce generations, manifests, indexes, snapshots, compaction files, and object directories without changing the logical transaction guarantees described here.

A future store directory may resemble:

```text
my-app.vstore/
  MANIFEST
  log/
  index/
  objects/
  snapshots/
```

The exact directory layout is not normative yet.

## 9. Compatibility

Readers must reject incompatible mandatory format versions explicitly.

Once a released format version is declared stable, later implementations must either read it correctly or provide an explicit migration path.

A store created through one supported binding must be readable through another compatible binding because all bindings operate the same Rust engine/file semantics.

No mandatory field may depend on Python pickle, Java serialization, V8 object layout, Go gob, or another host-specific representation.

## 10. Portable value model

The lowest compatibility layer is bytes:

```text
key   = bytes
value = bytes
```

Structured codecs are explicit layers above that primitive. Candidate codecs include JSON, MessagePack, CBOR, Protobuf, and schema-defined binary representations.

Persisted structured values must identify enough metadata for compatible readers to determine the representation when that representation matters to semantics.

## 11. Messaging model requirements

Messaging is a first-class domain and is not synonymous with queues.

Future messaging capabilities include:

- queues
- topics / pub-sub
- streams
- durable subscriptions
- consumer groups
- replay
- request/reply metadata
- dead-letter routing

A canonical message envelope must remain language-neutral. At minimum the design must accommodate:

```text
id
destination kind
destination
payload
created_at
available_at
correlation_id
causation_id
trace_id
reply_to
partition_key
idempotency_key
attempts
```

The engine must not make an unqualified generic exactly-once guarantee. Delivery modes are explicit; effectively-once application effects may be built from durable message identity and transactional idempotency.

## 12. Automation and time requirements

The engine must be able to represent durable time-dependent work without executing arbitrary host-language code.

Future durable primitives include:

- jobs
- delayed jobs
- one-shot schedules
- recurring schedules
- cron expressions
- retries/backoff
- leases
- deadlines
- triggers
- execution history
- waiting states/signals

The Store owns the durable fact that work is eligible, scheduled, leased, retried, completed, failed, or dead. A runtime/worker owns the application code that performs the work.

Wall-clock timestamps affect eligibility/lifecycle but do not replace durable sequence ordering.

## 13. Observability metadata

Durable records and envelopes may carry opaque identifiers required by higher-level runtimes:

- trace ID
- correlation ID
- causation ID
- execution ID
- parent execution ID
- source/node ID

The Store transports and persists these identifiers without assigning Voodoo-specific business meaning to them.

## 14. Voodoo alignment contract

Voodoo may map framework/runtime concepts onto Store primitives:

```text
Model              -> Collection / Query
@task              -> Job / Queue
Scheduler          -> Schedule / Time
Mesh/EventBus      -> Topic / Stream / Subscription
Execution          -> Durable execution/waiting state
ObjectStore        -> Objects
Cache              -> KV + TTL
Telemetry          -> trace/correlation metadata
```

This mapping is adapter-level behavior. `voodoo-store-core` must not import Voodoo Framework or encode Voodoo decorators/classes into mandatory durable format.

## 15. Security and operational requirements

Future format/API revisions must leave room for:

- integrity verification
- encryption-at-rest metadata/hooks
- namespaces
- opaque authorization/capability metadata
- quotas
- retention policies
- snapshots
- backup/restore
- verification/repair
- health/stats/inspection APIs

Policy evaluation remains outside the storage core unless a future specification explicitly defines a portable storage-level rule.

## 16. Distributed evolution

Replication and synchronization are future layers over a correct local engine.

Planned design areas include:

- store/node identity
- replication checkpoints
- change/log shipping
- offline/online sync
- conflict resolution
- distributed leases
- leader election
- Voodoo Protocol integration

Generic global multi-writer consensus is not a prerequisite for the local engine and remains explicitly deferred.

## 17. Next specification work

The next revisions should define:

- store header, UUID and feature/version negotiation
- transaction begin/commit framing and transaction size limits
- durable sync modes and exact fsync guarantees
- file locking and concurrency model
- log generation and rotation
- canonical key/value mutation payloads
- recovery behavior for corruption before the trailing write region
- snapshot and compaction semantics
- canonical identifiers and metadata encoding
- queue/job state transitions and lease semantics
- schedule/cron representation and timezone rules
- message envelope binary representation
- content-addressed object metadata
- change-feed semantics
