//! Operational health and storage accounting shared by CLI, Studio, and Runtime.

use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

use thiserror::Error;

use crate::{EngineError, Store, StoreMetrics, queue};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageStats {
    pub file_bytes: u64,
    pub live_keys: usize,
    pub user_keys: usize,
    pub internal_keys: usize,
    pub key_bytes: u64,
    pub value_bytes: u64,
}

impl StorageStats {
    pub const fn live_bytes(self) -> u64 {
        self.key_bytes + self.value_bytes
    }

    pub fn amplification_ratio(self) -> f64 {
        let live = self.live_bytes();
        if live == 0 {
            if self.file_bytes == 0 {
                1.0
            } else {
                self.file_bytes as f64
            }
        } else {
            self.file_bytes as f64 / live as f64
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceStats {
    pub name: &'static str,
    pub keys: usize,
    pub value_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthReport {
    pub store_id: [u8; 16],
    pub format_major: u16,
    pub format_minor: u16,
    pub storage: StorageStats,
    pub namespaces: Vec<NamespaceStats>,
    pub max_bytes: Option<u64>,
    pub namespace_max_bytes: Option<u64>,
    pub metrics: StoreMetrics,
    /// Process-wide count of rejected writer-lock attempts.
    pub lock_contentions_total: u64,
    pub object_reads_total: u64,
    pub object_bytes_read_total: u64,
    /// Bytes currently attributable to log history and framing overhead.
    /// This is a conservative compaction-backlog estimate; a rewrite can
    /// reclaim at most this amount without changing visible keys/values.
    pub compaction_backlog_bytes: u64,
    /// Aggregate queue depth across all native queues.
    pub queue_ready: u64,
    pub queue_leased: u64,
    pub queue_dead: u64,
    pub queue_total: u64,
    /// Oldest ready message timestamp, when at least one ready message exists.
    pub queue_oldest_ready_at_ms: Option<i64>,
    /// Bounded current lag for the oldest ready message, when one exists.
    pub queue_ready_lag_ms: Option<u64>,
    pub within_limit: bool,
}

impl Store {
    pub fn storage_stats(&self) -> Result<StorageStats, OperationsError> {
        let file_bytes = fs::metadata(self.path())?.len();
        let entries = self.scan_prefix(b"");
        let mut user_keys = 0usize;
        let mut internal_keys = 0usize;
        let mut key_bytes = 0u64;
        let mut value_bytes = 0u64;

        for (key, value) in &entries {
            if key.starts_with(crate::engine::INTERNAL_KEY_PREFIX) {
                internal_keys += 1;
            } else {
                user_keys += 1;
            }
            key_bytes = key_bytes
                .checked_add(key.len() as u64)
                .ok_or(OperationsError::AccountingOverflow)?;
            value_bytes = value_bytes
                .checked_add(value.len() as u64)
                .ok_or(OperationsError::AccountingOverflow)?;
        }

        Ok(StorageStats {
            file_bytes,
            live_keys: entries.len(),
            user_keys,
            internal_keys,
            key_bytes,
            value_bytes,
        })
    }

    pub fn health_report(&self) -> Result<HealthReport, OperationsError> {
        let header = self.header();
        let storage = self.storage_stats()?;
        let namespaces = [
            ("ttl", b"\xffvds:ttl:".as_slice()),
            ("collections", b"\xffvds:col:".as_slice()),
            ("queues", b"\xffvds:q:".as_slice()),
            ("jobs", b"\xffvds:job:".as_slice()),
            ("schedules", b"\xffvds:schedule:".as_slice()),
            ("cron", b"\xffvds:cron:".as_slice()),
            ("triggers", b"\xffvds:trigger:".as_slice()),
            ("outbox", b"\xffvds:outbox:".as_slice()),
            ("rpc", b"\xffvds:rpc:".as_slice()),
            ("cdc", b"\xffvds:cdc:".as_slice()),
            ("streams", b"\xffvds:stream:".as_slice()),
            ("subscriptions", b"\xffvds:sub:".as_slice()),
            ("consumer_groups", b"\xffvds:cg:".as_slice()),
            ("objects", b"\xffvds:obj:".as_slice()),
            ("workflows", b"\xffvds:wf:".as_slice()),
        ]
        .into_iter()
        .map(|(name, prefix)| {
            let entries = self.scan_prefix(prefix);
            let value_bytes = entries.iter().try_fold(0u64, |total, (_, value)| {
                total
                    .checked_add(value.len() as u64)
                    .ok_or(OperationsError::AccountingOverflow)
            })?;
            Ok(NamespaceStats {
                name,
                keys: entries.len(),
                value_bytes,
            })
        })
        .collect::<Result<Vec<_>, OperationsError>>()?;

        let compaction_backlog_bytes = storage.file_bytes.saturating_sub(storage.live_bytes());
        let queues = queue::aggregate_stats(self)?;
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
            .unwrap_or(0);
        let queue_ready_lag_ms = queues.oldest_ready_at_ms.map(|timestamp| {
            let ready_at_ms = u64::try_from(timestamp).unwrap_or(0);
            now_ms.saturating_sub(ready_at_ms)
        });

        Ok(HealthReport {
            store_id: header.store_id,
            format_major: header.format_major,
            format_minor: header.format_minor,
            storage,
            namespaces,
            max_bytes: self.options().max_bytes,
            namespace_max_bytes: self.options().namespace_max_bytes,
            metrics: self.metrics(),
            lock_contentions_total: Store::lock_contentions_total(),
            object_reads_total: Store::object_reads_total(),
            object_bytes_read_total: Store::object_bytes_read_total(),
            compaction_backlog_bytes,
            queue_ready: queues.ready,
            queue_leased: queues.leased,
            queue_dead: queues.dead,
            queue_total: queues.total,
            queue_oldest_ready_at_ms: queues.oldest_ready_at_ms,
            queue_ready_lag_ms,
            within_limit: self
                .options()
                .max_bytes
                .is_none_or(|max| storage.file_bytes <= max),
        })
    }
}

#[derive(Debug, Error)]
pub enum OperationsError {
    #[error("store error: {0}")]
    Store(#[from] EngineError),
    #[error("queue health error: {0}")]
    Queue(#[from] crate::QueueError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("storage accounting overflow")]
    AccountingOverflow,
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    fn temp_store_path(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("voodoo-store-operations-{name}-{nonce}.vstore"))
    }

    #[test]
    fn storage_accounting_separates_user_and_internal_keys() {
        let path = temp_store_path("stats");
        let mut store = Store::open(&path).unwrap();
        store.put(b"user", b"value").unwrap();
        store.put_with_ttl(b"session", b"x", 10).unwrap();
        let stats = store.storage_stats().unwrap();
        assert_eq!(stats.user_keys, 2);
        assert!(stats.internal_keys >= 3);
        assert_eq!(stats.live_keys, stats.user_keys + stats.internal_keys);
        assert!(stats.file_bytes > 0);
        drop(store);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn health_report_inventory_tracks_subsystems() {
        let path = temp_store_path("health");
        let mut store = Store::open(&path).unwrap();
        store.put_with_ttl(b"session", b"x", 10).unwrap();
        {
            let mut queue = store.queue(b"work").unwrap();
            queue.push(b"payload").unwrap();
        }
        let report = store.health_report().unwrap();
        assert_eq!(report.store_id, store.header().store_id);
        assert!(
            report
                .namespaces
                .iter()
                .any(|entry| entry.name == "ttl" && entry.keys > 0)
        );
        assert!(
            report
                .namespaces
                .iter()
                .any(|entry| entry.name == "queues" && entry.keys > 0)
        );
        assert!(report.namespaces.iter().any(|entry| entry.name == "cron"));
        assert!(
            report
                .namespaces
                .iter()
                .any(|entry| entry.name == "triggers")
        );
        assert!(report.namespaces.iter().any(|entry| entry.name == "outbox"));
        assert!(report.namespaces.iter().any(|entry| entry.name == "rpc"));
        assert!(
            report
                .namespaces
                .iter()
                .any(|entry| entry.name == "cdc" && entry.keys > 0)
        );
        assert!(
            report
                .namespaces
                .iter()
                .any(|entry| entry.name == "consumer_groups")
        );
        assert!(report.metrics.commits > 0);
        assert!(report.metrics.puts > 0);
        assert!(report.metrics.bytes_appended > 0);
        assert!(report.metrics.commit_latency_total_us >= report.metrics.commit_latency_max_us);
        assert!(report.metrics.fsync_latency_total_us >= report.metrics.fsync_latency_max_us);
        assert!(report.compaction_backlog_bytes <= report.storage.file_bytes);
        assert_eq!(report.queue_total, 1);
        assert_eq!(report.queue_ready, 1);
        assert_eq!(report.queue_leased, 0);
        assert_eq!(report.queue_dead, 0);
        assert_eq!(report.queue_oldest_ready_at_ms, Some(0));
        assert!(report.queue_ready_lag_ms.unwrap_or(0) > 0);
        drop(store);
        let _ = fs::remove_file(path);
    }
}
