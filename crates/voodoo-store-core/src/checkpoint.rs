//! Identity-preserving physical checkpoints.
//!
//! A checkpoint is an exact, verified copy of the durable store file at a
//! synchronization boundary. Unlike a logical snapshot, it preserves Store ID,
//! log history, pending physical records, CDC, and every internal namespace.

use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::{EngineError, Store};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointReport {
    pub source_store_id: [u8; 16],
    pub checkpoint_store_id: [u8; 16],
    pub file_bytes: u64,
    pub valid_bytes: u64,
    pub keys: usize,
    pub committed_transactions: u64,
    pub pending_transactions: u64,
}

impl Store {
    /// Creates a verified physical checkpoint without overwriting an existing
    /// destination. The exact store identity and log are preserved.
    ///
    /// The physical source image is produced through `backup_to`, which clones
    /// the Store's already-locked file handle. This is important on Windows,
    /// where reopening the source pathname while an exclusive byte-range lock
    /// is held can fail even in the same process.
    pub fn checkpoint_to(
        &self,
        destination: impl AsRef<Path>,
    ) -> Result<CheckpointReport, CheckpointError> {
        self.flush()?;
        let destination = destination.as_ref();
        ensure_distinct_destination(self.path(), destination)?;

        let temporary = reserve_temporary_sibling(destination)?;
        let result = self.checkpoint_from_temporary(&temporary, destination);
        let _ = fs::remove_file(&temporary);
        result
    }

    fn checkpoint_from_temporary(
        &self,
        temporary: &Path,
        destination: &Path,
    ) -> Result<CheckpointReport, CheckpointError> {
        self.backup_to(temporary)?;

        let mut source = File::open(temporary)?;
        let mut target = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(destination)
            .map_err(|error| {
                if error.kind() == io::ErrorKind::AlreadyExists {
                    CheckpointError::DestinationExists
                } else {
                    CheckpointError::Io(error)
                }
            })?;

        let copy_result = (|| -> Result<u64, CheckpointError> {
            let bytes = io::copy(&mut source, &mut target)?;
            target.sync_all()?;
            Ok(bytes)
        })();
        drop(target);

        let copied_bytes = match copy_result {
            Ok(bytes) => bytes,
            Err(error) => {
                let _ = fs::remove_file(destination);
                return Err(error);
            }
        };

        let report = match Store::verify(destination) {
            Ok(report) => report,
            Err(error) => {
                let _ = fs::remove_file(destination);
                return Err(CheckpointError::Store(error));
            }
        };
        if report.header.store_id != self.header().store_id {
            let _ = fs::remove_file(destination);
            return Err(CheckpointError::IdentityMismatch);
        }
        if report.file_bytes != copied_bytes || report.has_torn_tail() {
            let _ = fs::remove_file(destination);
            return Err(CheckpointError::VerificationMismatch);
        }

        Ok(CheckpointReport {
            source_store_id: self.header().store_id,
            checkpoint_store_id: report.header.store_id,
            file_bytes: report.file_bytes,
            valid_bytes: report.valid_bytes,
            keys: report.keys,
            committed_transactions: report.committed_transactions,
            pending_transactions: report.pending_transactions,
        })
    }
}

fn reserve_temporary_sibling(destination: &Path) -> Result<PathBuf, CheckpointError> {
    let parent = destination.parent().unwrap_or_else(|| Path::new("."));
    let base = destination
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("checkpoint");

    for _ in 0..16 {
        let mut nonce = [0u8; 8];
        getrandom::fill(&mut nonce).map_err(|error| CheckpointError::Randomness(error.to_string()))?;
        let suffix = u64::from_le_bytes(nonce);
        let candidate = parent.join(format!(".{base}.checkpoint-{suffix:016x}.tmp"));
        match OpenOptions::new().write(true).create_new(true).open(&candidate) {
            Ok(file) => {
                drop(file);
                return Ok(candidate);
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(CheckpointError::Io(error)),
        }
    }
    Err(CheckpointError::TemporaryNameExhausted)
}

fn ensure_distinct_destination(source: &Path, destination: &Path) -> Result<(), CheckpointError> {
    if source == destination {
        return Err(CheckpointError::DestinationIsSource);
    }
    if destination.exists() {
        if same_existing_path(source, destination)? {
            return Err(CheckpointError::DestinationIsSource);
        }
        return Err(CheckpointError::DestinationExists);
    }
    Ok(())
}

fn same_existing_path(source: &Path, destination: &Path) -> Result<bool, CheckpointError> {
    let source = canonical(source)?;
    let destination = canonical(destination)?;
    Ok(source == destination)
}

fn canonical(path: &Path) -> Result<PathBuf, CheckpointError> {
    fs::canonicalize(path).map_err(CheckpointError::Io)
}

#[derive(Debug, Error)]
pub enum CheckpointError {
    #[error("store error: {0}")]
    Store(#[from] EngineError),
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("checkpoint destination already exists")]
    DestinationExists,
    #[error("checkpoint destination must differ from source store")]
    DestinationIsSource,
    #[error("checkpoint store identity does not match source")]
    IdentityMismatch,
    #[error("checkpoint verification does not match copied bytes")]
    VerificationMismatch,
    #[error("failed to obtain randomness for checkpoint temporary file: {0}")]
    Randomness(String),
    #[error("could not reserve a checkpoint temporary file")]
    TemporaryNameExhausted,
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    fn temp_store_path(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("voodoo-store-checkpoint-{name}-{nonce}.vstore"))
    }

    #[test]
    fn checkpoint_preserves_identity_state_and_change_feed() {
        let source = temp_store_path("source");
        let checkpoint = temp_store_path("copy");
        let source_id;
        let expected_changes;
        {
            let mut store = Store::open(&source).unwrap();
            source_id = store.header().store_id;
            store.put(b"a", b"one").unwrap();
            store.put(b"b", b"two").unwrap();
            store.put(b"a", b"three").unwrap();
            expected_changes = store.changes_after(None, usize::MAX).unwrap();
            let report = store.checkpoint_to(&checkpoint).unwrap();
            assert_eq!(report.source_store_id, source_id);
            assert_eq!(report.checkpoint_store_id, source_id);
            assert_eq!(report.pending_transactions, 0);
        }
        {
            let copy = Store::open(&checkpoint).unwrap();
            assert_eq!(copy.header().store_id, source_id);
            assert_eq!(copy.get(b"a"), Some(b"three".as_slice()));
            assert_eq!(copy.get(b"b"), Some(b"two".as_slice()));
            assert_eq!(
                copy.changes_after(None, usize::MAX).unwrap(),
                expected_changes
            );
        }
        let _ = fs::remove_file(source);
        let _ = fs::remove_file(checkpoint);
    }

    #[test]
    fn checkpoint_never_overwrites_existing_destination() {
        let source = temp_store_path("source-existing");
        let checkpoint = temp_store_path("existing");
        let mut store = Store::open(&source).unwrap();
        store.put(b"a", b"one").unwrap();
        fs::write(&checkpoint, b"do-not-touch").unwrap();
        assert!(matches!(
            store.checkpoint_to(&checkpoint),
            Err(CheckpointError::DestinationExists)
        ));
        assert_eq!(fs::read(&checkpoint).unwrap(), b"do-not-touch");
        drop(store);
        let _ = fs::remove_file(source);
        let _ = fs::remove_file(checkpoint);
    }

    #[test]
    fn checkpoint_rejects_source_as_destination() {
        let source = temp_store_path("same");
        let store = Store::open(&source).unwrap();
        assert!(matches!(
            store.checkpoint_to(&source),
            Err(CheckpointError::DestinationIsSource)
        ));
        drop(store);
        let _ = fs::remove_file(source);
    }
}
