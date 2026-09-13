use std::fs;
use std::io;
use std::path::Path;

use crate::{EngineError, Store};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionReport {
    pub source_bytes: u64,
    pub compacted_bytes: u64,
    pub keys: usize,
}

impl CompactionReport {
    pub const fn bytes_reclaimed(&self) -> u64 {
        self.source_bytes.saturating_sub(self.compacted_bytes)
    }
}

impl Store {
    /// Writes the current committed state into a new, compact `.vstore` file.
    ///
    /// The source store is never modified. The destination receives a new store
    /// identity because both files may coexist safely. A future atomic-replace
    /// operation can preserve identity once platform-specific replacement
    /// semantics are implemented and fault-tested.
    ///
    /// Compaction is a maintenance rewrite: it preserves existing CDC records
    /// but does not generate a second change feed describing the rewrite itself.
    pub fn compact_copy_to(
        &self,
        destination: impl AsRef<Path>,
    ) -> Result<CompactionReport, EngineError> {
        let destination = destination.as_ref();
        if destination == self.path() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "compaction destination must differ from source store",
            )
            .into());
        }
        if destination.exists() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "compaction destination already exists",
            )
            .into());
        }

        let source_bytes = fs::metadata(self.path())?.len();
        let entries = self.scan_prefix([]);
        let keys = entries.len();

        {
            let mut compacted = Store::open(destination)?;
            if !entries.is_empty() {
                let mut tx = compacted.begin_without_cdc()?;
                for (key, value) in entries {
                    tx.put_internal(key, value)?;
                }
                tx.commit()?;
            }
            compacted.flush()?;
        }

        let verification = Store::verify(destination)?;
        if verification.has_torn_tail() || verification.keys != keys {
            let _ = fs::remove_file(destination);
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "compacted store failed verification",
            )
            .into());
        }

        Ok(CompactionReport {
            source_bytes,
            compacted_bytes: verification.file_bytes,
            keys,
        })
    }
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
        std::env::temp_dir().join(format!("voodoo-store-{name}-{nonce}.vstore"))
    }

    #[test]
    fn compact_copy_preserves_visible_state_and_reduces_history() {
        let source = temp_store_path("compact-source");
        let destination = temp_store_path("compact-destination");

        {
            let mut store = Store::open(&source).unwrap();
            for index in 0..25 {
                store
                    .put(b"key", format!("value-{index}").as_bytes())
                    .unwrap();
            }
            store.put(b"keep", b"yes").unwrap();
            store.put(b"remove", b"later").unwrap();
            store.delete(b"remove").unwrap();

            let expected_keys = store.len();
            let expected_changes = store.changes_after(None, usize::MAX).unwrap();
            let report = store.compact_copy_to(&destination).unwrap();
            assert_eq!(report.keys, expected_keys);
            assert!(report.compacted_bytes < report.source_bytes);
            assert!(report.bytes_reclaimed() > 0);

            let compacted = Store::open(&destination).unwrap();
            assert_eq!(
                compacted.changes_after(None, usize::MAX).unwrap(),
                expected_changes
            );
        }

        let compacted = Store::open(&destination).unwrap();
        assert_eq!(compacted.get(b"key"), Some(b"value-24".as_slice()));
        assert_eq!(compacted.get(b"keep"), Some(b"yes".as_slice()));
        assert_eq!(compacted.get(b"remove"), None);
        drop(compacted);

        let _ = fs::remove_file(source);
        let _ = fs::remove_file(destination);
    }

    #[test]
    fn compact_copy_never_overwrites_destination() {
        let source = temp_store_path("compact-source-existing");
        let destination = temp_store_path("compact-existing");

        let store = Store::open(&source).unwrap();
        fs::write(&destination, b"do-not-touch").unwrap();

        let error = store.compact_copy_to(&destination).unwrap_err();
        match error {
            EngineError::Io(error) => assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists),
            other => panic!("unexpected error: {other}"),
        }
        assert_eq!(fs::read(&destination).unwrap(), b"do-not-touch");
        drop(store);

        let _ = fs::remove_file(source);
        let _ = fs::remove_file(destination);
    }

    #[test]
    fn compact_copy_preserves_queue_internal_state() {
        let source = temp_store_path("compact-queue-source");
        let destination = temp_store_path("compact-queue-destination");

        {
            let mut store = Store::open(&source).unwrap();
            let mut queue = store.queue(b"emails").unwrap();
            queue.push(b"hello").unwrap();
            drop(queue);
            store.compact_copy_to(&destination).unwrap();
        }

        let mut compacted = Store::open(&destination).unwrap();
        let mut queue = compacted.queue(b"emails").unwrap();
        let message = queue.claim(0, 1_000).unwrap().unwrap();
        assert_eq!(message.payload, b"hello");
        queue.ack(message.id, message.lease_generation).unwrap();
        drop(queue);
        drop(compacted);

        let _ = fs::remove_file(source);
        let _ = fs::remove_file(destination);
    }
}
