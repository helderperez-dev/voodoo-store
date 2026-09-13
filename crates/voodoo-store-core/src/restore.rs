//! Verified restore workflow for Voodoo Store files.
//!
//! Restore is deliberately create-only in this first version: an existing
//! destination is never overwritten. This keeps restore failure semantics
//! simple and cross-platform while atomic in-place replacement remains a later
//! storage-lifecycle milestone.

use std::fs::{self, OpenOptions};
use std::io;
use std::path::Path;

use crate::{EngineError, Store, VerificationReport};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreReport {
    pub bytes: u64,
    pub keys: usize,
    pub store_id: [u8; 16],
}

impl Store {
    /// Restores a verified `.vstore` source into a new destination path.
    ///
    /// The source is verified before copying and the destination is verified
    /// again after copying. Existing destinations are never overwritten.
    pub fn restore_copy(
        source: impl AsRef<Path>,
        destination: impl AsRef<Path>,
    ) -> Result<RestoreReport, EngineError> {
        let source = source.as_ref();
        let destination = destination.as_ref();
        if source == destination {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "restore destination must differ from source store",
            )
            .into());
        }

        let source_report = Self::verify(source)?;
        if source_report.has_torn_tail() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "restore source has a torn tail",
            )
            .into());
        }

        let result = restore_verified(source, destination, &source_report);
        if result.is_err() {
            let _ = fs::remove_file(destination);
        }
        result
    }
}

fn restore_verified(
    source: &Path,
    destination: &Path,
    source_report: &VerificationReport,
) -> Result<RestoreReport, EngineError> {
    let mut source_file = OpenOptions::new().read(true).open(source)?;
    let mut destination_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(destination)?;
    let bytes = io::copy(&mut source_file, &mut destination_file)?;
    destination_file.sync_all()?;
    drop(destination_file);

    let restored = Store::verify(destination)?;
    if restored.has_torn_tail()
        || restored.file_bytes != source_report.file_bytes
        || restored.valid_bytes != source_report.valid_bytes
        || restored.keys != source_report.keys
        || restored.header != source_report.header
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "restored store does not match verified source",
        )
        .into());
    }

    Ok(RestoreReport {
        bytes,
        keys: restored.keys,
        store_id: restored.header.store_id,
    })
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
        std::env::temp_dir().join(format!("voodoo-store-restore-{name}-{nonce}.vstore"))
    }

    #[test]
    fn restore_copy_preserves_identity_data_and_queue_state() {
        let source = temp_store_path("source");
        let destination = temp_store_path("destination");
        let source_id;

        {
            let mut store = Store::open(&source).unwrap();
            source_id = store.header().store_id;
            store.put(b"user:1", b"Helder").unwrap();
            let mut queue = store.queue(b"emails").unwrap();
            queue.push(b"welcome").unwrap();
        }

        let report = Store::restore_copy(&source, &destination).unwrap();
        assert_eq!(report.store_id, source_id);
        assert_eq!(report.keys, 3);

        let mut restored = Store::open(&destination).unwrap();
        assert_eq!(restored.header().store_id, source_id);
        assert_eq!(restored.get(b"user:1"), Some(b"Helder".as_slice()));
        let mut queue = restored.queue(b"emails").unwrap();
        let message = queue.claim(0, 1_000).unwrap().unwrap();
        assert_eq!(message.payload, b"welcome");
        queue.ack(message.id, message.lease_generation).unwrap();
        drop(queue);
        drop(restored);

        let _ = fs::remove_file(source);
        let _ = fs::remove_file(destination);
    }

    #[test]
    fn restore_never_overwrites_existing_destination() {
        let source = temp_store_path("existing-source");
        let destination = temp_store_path("existing-destination");
        {
            let mut store = Store::open(&source).unwrap();
            store.put(b"safe", b"yes").unwrap();
        }
        fs::write(&destination, b"keep-me").unwrap();

        let error = Store::restore_copy(&source, &destination).unwrap_err();
        match error {
            EngineError::Io(error) => assert_eq!(error.kind(), io::ErrorKind::AlreadyExists),
            other => panic!("unexpected error: {other}"),
        }
        assert_eq!(fs::read(&destination).unwrap(), b"keep-me");

        let _ = fs::remove_file(source);
        let _ = fs::remove_file(destination);
    }
}
