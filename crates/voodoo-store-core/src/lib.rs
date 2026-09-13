//! Core primitives and embedded engine for Voodoo Store.
//!
//! `voodoo-store-core` is intentionally framework-agnostic. It contains no
//! dependency on Voodoo Framework, Python, Node.js, or any foreign runtime.
//! Language bindings must sit above this crate so the on-disk format and core
//! semantics remain portable and independently usable.

pub mod atomic;
pub mod cdc;
pub mod collection;
pub mod compaction;
pub mod consumer_group;
pub mod cron;
pub mod cron_scheduler;
pub mod engine;
pub mod header;
pub mod jobs;
pub mod log;
pub mod messaging;
pub mod model;
pub mod objects;
pub mod operations;
pub mod outbox;
pub mod query;
pub mod queue;
pub mod restore;
pub mod rpc;
pub mod snapshot;
pub mod transaction_domains;
pub mod transactional;
pub mod triggers;
pub mod ttl;
pub mod workflow;

pub use atomic::{AtomicError, CasOutcome};
pub use cdc::{ChangeFeedError, ChangeKind, ChangeRecord};
pub use collection::{
    CollectionDefinition, CollectionError, CollectionRecord, IndexDefinition, IndexValue,
};
pub use compaction::CompactionReport;
pub use consumer_group::{ConsumerGroupDelivery, ConsumerGroupError, ConsumerGroupState};
pub use cron::{CronError, CronExpression};
pub use cron_scheduler::{CronScheduleId, CronSchedulerError, CronTickReport, DurableCronSchedule};
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
pub use operations::{HealthReport, NamespaceStats, OperationsError, StorageStats};
pub use outbox::{OutboxError, OutboxEvent, OutboxEventId};
pub use query::{IndexRangeQuery, IndexedRecord, QueryBound, QueryError, QueryOrder};
pub use queue::{PushOptions, Queue, QueueError, QueueMessage, QueueState, QueueStats};
pub use restore::RestoreReport;
pub use rpc::{RpcError, RpcExpireReport, RpcRequest, RpcRequestId, RpcResponse};
pub use snapshot::{SnapshotError, SnapshotReport};
pub use transaction_domains::DomainTransactionError;
pub use transactional::TransactionalError;
pub use triggers::{DurableTrigger, TriggerError, TriggerId, TriggerSource};
pub use ttl::{TtlError, TtlInfo, TtlSweepReport};
pub use workflow::{
    WorkflowError, WorkflowHistoryEntry, WorkflowHistoryKind, WorkflowId, WorkflowInstance,
    WorkflowResumeReport, WorkflowStatus, WorkflowWait,
};
