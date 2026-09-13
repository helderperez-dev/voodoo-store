//! Operational health and storage accounting shared by CLI, Studio, and Runtime.

use std::fs;

use thiserror::Error;

use crate::{EngineError, Store};

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
            ("triggers", b"\xffvds:trigger:".as_slice()),
            ("streams", b"\xffvds:stream:".as_slice()),
            ("subscriptions", b"\xffvds:sub:".as_slice()),
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

        Ok(HealthReport {
            store_id: header.store_id,
            format_major: header.format_major,
            format_minor: header.format_minor,
            storage,
            namespaces,
        })
    }
}

#[derive(Debug, Error)]
pub enum OperationsError {
    #[error("store error: {0}")]
    Store(#[from] EngineError),
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
        assert_eq!(stats.internal_keys, 1);
        assert_eq!(stats.live_keys, 3);
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
        assert!(report.namespaces.iter().any(|entry| entry.name == "triggers"));
        drop(store);
        let _ = fs::remove_file(path);
    }
}
