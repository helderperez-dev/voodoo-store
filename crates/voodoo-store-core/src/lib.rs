//! Core primitives and embedded engine for Voodoo Store.
//!
//! `voodoo-store-core` is intentionally framework-agnostic. It contains no
//! dependency on Voodoo Framework, Python, Node.js, or any foreign runtime.
//! Language bindings must sit above this crate so the on-disk format and core
//! semantics remain portable and independently usable.

pub mod atomic;
pub mod compaction;
pub mod engine;
pub mod header;
pub mod log;
pub mod model;
pub mod queue;

pub use atomic::{AtomicError, CasOutcome};
pub use compaction::CompactionReport;
pub use engine::{Durability, EngineError, Store, StoreOptions, Transaction, VerificationReport};
pub use header::{FORMAT_MAJOR, FORMAT_MINOR, HeaderError, STORE_HEADER_LEN, StoreHeader};
pub use log::{LogRecord, RecordKind, StoreError};
pub use model::{
    DeliverySemantics, DestinationKind, JobState, MessageEnvelope, ScheduleKind, TraceContext,
};
pub use queue::{PushOptions, Queue, QueueError, QueueMessage, QueueState, QueueStats};
