//! Core primitives for Voodoo Store.
//!
//! This crate intentionally starts small. The first milestone is a durable,
//! append-only record log with deterministic validation and recovery rules.

pub mod log;

pub use log::{LogRecord, RecordKind, StoreError};
