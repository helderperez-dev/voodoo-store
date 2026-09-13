//! Verified logical snapshots for Voodoo Store.
//!
//! A snapshot is a create-only compact representation of the currently visible
//! committed state. It preserves the store identity and is verified before the
//! operation succeeds. In-place generation replacement remains a separate
//! lifecycle primitive.

use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::{CompactionReport, EngineError, Store};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotReport {
    pub destination: PathBuf,
    pub store_id: [u8; 16],
    pub keys: usize,
    pub source_bytes: u64,
    pub snapshot_bytes: u64,
}

impl Store {
    pub fn snapshot_to(
        &self,
        destination: impl AsRef<Path>,
    ) -> Result<SnapshotReport, SnapshotError> {
        let destination = destination.as_ref();
        let CompactionReport {
            source_bytes,
            compacted_bytes,
            keys,
        } = self.compact_copy_to(destination)?;
        let verification = Store::verify(destination)?;
        if verification.header.store_id != self.header().store_id
            || verification.keys != keys
            || verification.has_torn_tail()
        {
            return Err(SnapshotError::VerificationFailed);
        }
        Ok(SnapshotReport {
            destination: destination.to_path_buf(),
            store_id: verification.header.store_id,
            keys,
            source_bytes,
            snapshot_bytes: compacted_bytes,
        })
    }
}

#[derive(Debug, Error)]
pub enum SnapshotError {
    #[error("store error: {0}")]
    Store(#[from] EngineError),
    #[error("compaction error: {0}")]
    Compaction(#[from] crate::compaction::CompactionError),
    #[error("snapshot verification failed")]
    VerificationFailed,
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    fn temp_path(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("voodoo-store-snapshot-{name}-{nonce}.vstore"))
    }

    #[test]
    fn snapshot_preserves_visible_state_and_store_identity() {
        let source = temp_path("source");
        let snapshot = temp_path("copy");
        let store_id;
        {
            let mut store = Store::open(&source).unwrap();
            store_id = store.header().store_id;
            store.put(b"a", b"one").unwrap();
            store.put(b"b", b"two").unwrap();
            store.put(b"a", b"three").unwrap();
            let report = store.snapshot_to(&snapshot).unwrap();
            assert_eq!(report.store_id, store_id);
            assert_eq!(report.keys, 2);
            assert!(report.snapshot_bytes <= report.source_bytes);
        }
        {
            let restored = Store::open(&snapshot).unwrap();
            assert_eq!(restored.header().store_id, store_id);
            assert_eq!(restored.get(b"a"), Some(b"three".as_slice()));
            assert_eq!(restored.get(b"b"), Some(b"two".as_slice()));
        }
        let _ = fs::remove_file(source);
        let _ = fs::remove_file(snapshot);
    }

    #[test]
    fn snapshot_never_overwrites_existing_destination() {
        let source = temp_path("source-existing");
        let destination = temp_path("destination-existing");
        let mut store = Store::open(&source).unwrap();
        store.put(b"key", b"value").unwrap();
        fs::write(&destination, b"keep-me").unwrap();
        assert!(store.snapshot_to(&destination).is_err());
        assert_eq!(fs::read(&destination).unwrap(), b"keep-me");
        let _ = fs::remove_file(source);
        let _ = fs::remove_file(destination);
    }
}
