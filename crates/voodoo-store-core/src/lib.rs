//! Core primitives and embedded engine for Voodoo Store.
//!
//! `voodoo-store-core` is intentionally framework-agnostic. It contains no
//! dependency on Voodoo Framework, Python, Node.js, or any foreign runtime.
//! Language bindings must sit above this crate so the on-disk format and core
//! semantics remain portable and independently usable.

pub mod atomic;
pub mod collection;
pub mod compaction;
pub mod engine;
pub mod header;
pub mod jobs;
pub mod log;
pub mod messaging;
pub mod model;
pub mod objects;
pub mod queue;
pub mod restore;
pub mod ttl;
pub mod workflow;

pub use atomic::{AtomicError, CasOutcome};
pub use collection::{
    CollectionDefinition, CollectionError, CollectionRecord, IndexDefinition, IndexValue,
};
pub use compaction::CompactionReport;
pub use engine::{Durability, EngineError, Store, StoreOptions, Transaction, VerificationReport};
pub use header::{FORMAT_MAJOR, FORMAT_MINOR, HeaderError, STORE_HEADER_LEN, StoreHeader};
pub use jobs::{
    DurableJob, DurableJobState, DurableSchedule, JobError, JobHistoryEntry, JobHistoryKind, JobId,
    JobSpec, ScheduleId, ScheduleMode, SchedulerTickReport,
};
pub use log::{LogRecord, RecordKind, StoreError};
pub use messaging::{MessagingError, Stream, StreamEntry, SubscriptionState, Topic};
pub use model::{
    DeliverySemantics, DestinationKind, JobState, MessageEnvelope, ScheduleKind, TraceContext,
};
pub use objects::{ObjectError, ObjectGcReport, ObjectId, ObjectInfo};
pub use queue::{PushOptions, Queue, QueueError, QueueMessage, QueueState, QueueStats};
pub use restore::RestoreReport;
pub use ttl::{TtlError, TtlInfo, TtlSweepReport};
pub use workflow::{
    WorkflowError, WorkflowHistoryEntry, WorkflowHistoryKind, WorkflowId, WorkflowInstance,
    WorkflowResumeReport, WorkflowStatus, WorkflowWait,
};
