---
name: voodoo-store-development
description: Develop Voodoo Store across the Rust core, native primitives, transactions, durability, PyO3 Python binding, tests, packaging, and releases. Use when modifying voodoo-store, adding Store primitives, exposing Rust capabilities to Python, changing cross-domain transactions, or preparing Store releases.
---

# Voodoo Store Development

Voodoo Store owns durable mechanics. It must remain usable independently from Voodoo Framework.

## Core principles

1. Preserve standalone Rust core semantics.
2. Keep the `.vstore` format language-neutral.
3. Keep local-first/single-writer semantics explicit.
4. Expose differentiating native primitives rather than forcing users through KV.
5. Keep cross-domain atomicity honest: local Store commit boundary, not global consensus.
6. Python bindings should be deliberate, typed, and Pythonic.
7. Framework semantics must not leak into Store authority.

Read [primitives.md](references/primitives.md), [python-binding.md](references/python-binding.md), and [release.md](references/release.md).

## Current native domains

The Python surface is intended to converge on core support for:

- KV
- Collections and indexes
- Jobs / Queue
- Scheduler / Cron / Triggers
- Streams / Topics
- Consumer Groups
- Objects / refs / orphan GC
- Outbox
- RPC
- CDC
- Workflow / HITL state, signals, timers, history, parent/child
- operational health/storage/checkpoint/compaction/restore
- heterogeneous cross-domain transactions where supported

When adding a primitive, define the Rust semantics first, then expose the binding, then let Framework consume it through Runtime ownership.
