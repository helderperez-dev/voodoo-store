# Voodoo Store Specification v0.1-draft

## 1. Scope

This document defines the first durable layer of Voodoo Store. Version 0.1 focuses on an append-only log, record integrity, transaction identity, commit markers, and deterministic recovery semantics.

The format is intentionally small. Collections, indexes, queues, objects, streams, replication, and query execution will build on this layer.

## 2. Principles

- The on-disk format is language-neutral even though the reference implementation is 100% Rust.
- Correctness and recoverability take precedence over throughput.
- The log is the initial source of truth for durable mutations.
- Wall-clock time is not used to define durable ordering.
- Format changes require explicit versioning.

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

## 5. Transactions

A mutation record belongs to a logical transaction identified by `tx_id`.

A transaction becomes recoverably visible only when a valid `COMMIT` record for the transaction is present within the durable log according to the configured sync mode.

Until then, its mutation records are provisional.

A recovery implementation must never expose only a prefix of the mutations belonging to a committed transaction.

## 6. Ordering

`sequence` defines durable record ordering inside a log generation.

Sequence values must be monotonic. The first production writer will use strictly increasing sequence numbers.

Wall-clock timestamps may later be stored as metadata, but they do not define storage ordering.

## 7. Recovery

A reader scans records in log order and validates structural length, magic, kind, and checksum before interpreting a record.

A trailing incomplete record is treated as an interrupted write and is ignored after the last fully valid record. Corruption in previously durable regions must surface as an integrity error rather than be silently ignored.

Recovery groups mutation records by transaction and applies only transactions that have a valid commit marker.

## 8. File layout

The first implementation may use a single append-only log file. Future revisions may introduce generations, manifests, indexes, snapshots, compaction files, and object directories without changing the logical transaction guarantees described here.

A future store directory may resemble:

```text
my-app.vstore/
  MANIFEST
  log/
  index/
  objects/
```

The exact directory layout is not normative yet.

## 9. Compatibility

Readers must reject incompatible mandatory format versions explicitly.

Once a released format version is declared stable, later implementations must either read it correctly or provide an explicit migration path.

## 10. Next specification work

The next revisions should define:

- transaction begin/commit framing and transaction size limits
- durable sync modes and exact fsync guarantees
- log generation and rotation
- canonical key/value mutation payloads
- recovery behavior for corruption before the trailing write region
- snapshot and compaction semantics
- concurrency model
- queue record semantics
- content-addressed object metadata
