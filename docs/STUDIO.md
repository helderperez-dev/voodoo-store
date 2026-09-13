# Voodoo Store Studio

Voodoo Store Studio is the visual administration surface for Voodoo Store.

Its purpose is broader than a database table viewer. Voodoo Store is intended to own application state across data, queues, jobs, schedules, messaging, objects, workflows and operational metadata, so Studio must expose those primitives through one coherent local interface.

## Product principle

> If Voodoo Store replaces multiple infrastructure services, Studio should replace the multiple dashboards normally required to operate them.

A developer should be able to run:

```bash
voodoo-store studio app.vstore
```

and receive a local browser application for inspecting and administering the store.

Inside a Voodoo project, the preferred experience becomes:

```bash
voodoo store studio
```

The main Voodoo CLI discovers the configured store automatically and launches the same Studio implementation.

## Architecture

```text
Browser
   |
   | localhost HTTP / WebSocket
   v
Voodoo Store Studio Server
   |
   | stable Rust API / future public administration API
   v
Voodoo Store
   |
   +-- Data
   +-- Queues / Jobs
   +-- Scheduler
   +-- Messaging / Streams
   +-- Objects
   +-- Workflows
   +-- Operations / Metrics
```

Studio is a client of Store. It must not introduce a parallel persistence layer or duplicate correctness-critical logic.

The CLI and Studio should share the same operations API so a button such as `Retry message` executes the same underlying operation available from command-line tooling.

## Local-first security model

Studio starts on loopback only by default.

Initial rules:

- bind to `127.0.0.1` / `::1` only;
- never expose the server publicly by default;
- support explicit read-only mode;
- require deliberate user actions for destructive operations;
- keep raw engine repair functions distinct from normal data-edit operations;
- do not execute arbitrary application code from Studio;
- remote access, if added later, requires authentication and capability-scoped authorization.

## Information architecture

### Overview

The landing page should answer whether the Store is healthy before showing individual records.

Display:

- store identity;
- file path;
- format major/minor;
- file size;
- creation timestamp;
- active durability mode;
- valid log bytes versus physical bytes;
- transaction count;
- key count;
- queue summary;
- pending/dead work;
- compaction opportunity;
- latest backup information when available;
- health warnings.

### Data

Once collections exist, Data becomes the application-data workspace.

Capabilities:

- collection browser;
- record table;
- structured record inspector;
- filters;
- sorting;
- pagination/range navigation;
- related-record navigation;
- insert/edit/delete;
- transaction-aware batch edits;
- raw KV namespace inspector for advanced users.

The default interface should prefer application models/collections over exposing raw internal keys.

### Schema

Display:

- collections;
- field definitions;
- primary keys;
- indexes;
- uniqueness constraints;
- schema versions;
- migration history;
- relationships/references;
- migration diffs.

### Queues

Queues should be operational, not merely visible.

Display:

- queue names;
- ready count;
- leased count;
- delayed count;
- failed/dead count;
- oldest ready message;
- attempt counts;
- lease owner and expiry;
- payload and headers;
- trace/correlation metadata.

Actions:

- enqueue;
- retry/requeue;
- nack;
- move to dead state;
- purge dead messages;
- inspect lease history when available.

### Jobs

Once the Jobs abstraction exists:

- job definition/name;
- state;
- input payload;
- attempts;
- retry/backoff policy;
- created/available/started/completed timestamps;
- execution history;
- error details;
- correlation and trace identifiers;
- manual retry/re-run.

Studio does not execute application handlers directly. It manipulates durable job state and asks the Runtime/worker system to execute through the normal protocol.

### Scheduler

Display time-oriented infrastructure visually:

- one-shot schedules;
- delayed jobs;
- recurring schedules;
- cron expressions;
- next run;
- previous runs;
- paused/active state;
- calendar/timeline views.

### Messaging and Streams

Display:

- topics;
- durable subscriptions;
- stream partitions if introduced;
- offsets/cursors;
- consumer groups;
- consumer lag;
- message envelope;
- replay ranges;
- correlation/causation chain.

Useful developer action:

```text
Replay from offset X
```

must always make its delivery consequences explicit before execution.

### Objects

Display:

- object identifier/hash;
- content type;
- byte size;
- metadata;
- references;
- creation/access lifecycle;
- integrity status;
- orphan status.

Allow safe download/export and eventually upload/import.

### Workflows

Durable workflow inspection should make orchestration understandable:

- workflow instance;
- current state/step;
- completed steps;
- timers;
- signals;
- waits;
- retries;
- compensation state;
- parent/child executions;
- event timeline.

This becomes especially important for AI-generated Voodoo applications because Studio can explain what the generated system is doing at runtime.

### Operations

Expose engine maintenance intentionally:

- verify;
- backup;
- restore;
- compaction;
- retention;
- storage accounting;
- corruption warnings;
- format compatibility;
- future replication/sync state.

Dangerous repair operations must be visually separated from normal administration.

### Observability

Studio should eventually unify operational debugging across the Store:

- transactions;
- reads/writes;
- commit latency;
- fsync latency;
- queue latency;
- consumer lag;
- job attempts;
- trace/correlation chains;
- store growth;
- compaction backlog.

## Relationship to Voodoo Runtime and Builder

Studio should use the same Voodoo visual language and Design System as future Runtime and Builder tooling, but remain independently launchable.

Long term:

```text
Voodoo Builder
Voodoo Runtime Console
Voodoo Store Studio
        |
        +-- shared Design System
        +-- shared inspection primitives
        +-- shared execution/trace vocabulary
```

This is strategically important: an AI application builder needs a reliable way to inspect the state it generated, understand queues/jobs/workflows, and diagnose runtime behavior without requiring the developer to open PostgreSQL, Redis, RabbitMQ and object-storage dashboards separately.

## Development sequence

Studio should not block correctness work in the engine. Build it progressively as stable primitives become available.

### Studio 0.1

- local HTTP server;
- Overview;
- raw KV browser;
- Store verification report;
- queue browser and queue stats;
- dead-message inspection/retry;
- backup trigger;
- read-only mode.

### Studio 0.2

After Collections/Schema:

- collection browser;
- structured editing;
- filtering/sorting;
- schema/index inspector;
- migrations view.

### Studio 0.3

After Jobs/Scheduler/Messaging:

- Jobs;
- Scheduler;
- Topics;
- Streams;
- subscriptions;
- traces and correlation chains.

### Studio 0.4

After Objects/Workflows:

- object browser;
- workflow timeline;
- workflow signals/waits;
- operational dashboards.

## Non-goals

Studio is not:

- the persistence engine;
- a replacement for Store APIs;
- an arbitrary code execution console;
- a required dependency for applications;
- a reason to couple Voodoo Store to Voodoo Framework.

A `.vstore` must remain fully usable headlessly and from any supported language without Studio installed.
