//! Identity-preserving compact store generations.
//!
//! A generation rewrites the current committed state into a compact file while
//! retaining the source Store ID. It is deliberately create-only and is not
//! activated automatically. A synthetic empty commit advances the physical log
//! high-water marks beyond both source and rewritten logs, preventing future
//! transaction/sequence reuse after the generation is opened.

use std::fs::{self, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

use thiserror::Error;

use crate::log::{HEADER_LEN, encoded_record_len_from_prefix};
use crate::{EngineError, LogRecord, RecordKind, STORE_HEADER_LEN, Store, StoreError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GenerationReport {
    pub store_id: [u8; 16],
    pub source_bytes: u64,
    pub generation_bytes: u64,
    pub keys: usize,
    pub high_water_tx_id: u64,
    pub high_water_sequence: u64,
}

impl GenerationReport {
    pub const fn bytes_reclaimed(&self) -> u64 {
        self.source_bytes.saturating_sub(self.generation_bytes)
    }
}

impl Store {
    /// Builds a compact, independently verified generation that keeps this
    /// store's identity. The active source file is never modified or replaced.
    pub fn compact_generation_to(
        &self,
        destination: impl AsRef<Path>,
    ) -> Result<GenerationReport, GenerationError> {
        self.flush()?;
        let destination = destination.as_ref();
        if destination == self.path() {
            return Err(GenerationError::DestinationIsSource);
        }

        reserve_destination(destination)?;
        let result = self.build_generation(destination);
        if result.is_err() {
            let _ = fs::remove_file(destination);
        }
        result
    }

    fn build_generation(&self, destination: &Path) -> Result<GenerationReport, GenerationError> {
        let source_bytes = fs::metadata(self.path())?.len();
        let (source_max_tx, source_max_sequence) = log_high_water(self.path())?;
        let entries = self.scan_prefix([]);
        let keys = entries.len();

        {
            let mut generation = Store::open(destination)?;
            if !entries.is_empty() {
                let mut tx = generation.begin_without_cdc()?;
                for (key, value) in entries {
                    tx.put_internal(key, value)?;
                }
                tx.commit()?;
            }
            generation.flush()?;
        }

        let (generation_max_tx, generation_max_sequence) = log_high_water(destination)?;
        let high_water_tx_id = source_max_tx
            .max(generation_max_tx)
            .checked_add(1)
            .ok_or(GenerationError::CounterExhausted)?;
        let high_water_sequence = source_max_sequence
            .max(generation_max_sequence)
            .checked_add(1)
            .ok_or(GenerationError::CounterExhausted)?;

        let marker = LogRecord::new(
            RecordKind::Commit,
            high_water_tx_id,
            high_water_sequence,
            Vec::new(),
        )
        .encode()?;

        let mut file = OpenOptions::new().read(true).write(true).open(destination)?;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&self.header().encode())?;
        file.seek(SeekFrom::End(0))?;
        file.write_all(&marker)?;
        file.sync_all()?;
        drop(file);

        let verification = Store::verify(destination)?;
        if verification.has_torn_tail()
            || verification.keys != keys
            || verification.header.store_id != self.header().store_id
        {
            return Err(GenerationError::VerificationMismatch);
        }

        Ok(GenerationReport {
            store_id: self.header().store_id,
            source_bytes,
            generation_bytes: verification.file_bytes,
            keys,
            high_water_tx_id,
            high_water_sequence,
        })
    }
}

fn reserve_destination(destination: &Path) -> Result<(), GenerationError> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(destination)
        .map(|_| ())
        .map_err(|error| {
            if error.kind() == io::ErrorKind::AlreadyExists {
                GenerationError::DestinationExists
            } else {
                GenerationError::Io(error)
            }
        })
}

fn log_high_water(path: &Path) -> Result<(u64, u64), GenerationError> {
    let mut file = OpenOptions::new().read(true).open(path)?;
    file.seek(SeekFrom::Start(STORE_HEADER_LEN as u64))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;

    let mut offset = 0usize;
    let mut max_tx_id = 0u64;
    let mut max_sequence = 0u64;
    while offset < bytes.len() {
        let remaining = &bytes[offset..];
        if remaining.len() < HEADER_LEN {
            break;
        }
        let record_len = match encoded_record_len_from_prefix(remaining) {
            Ok(len) => len,
            Err(StoreError::TruncatedRecord) => break,
            Err(error) => return Err(error.into()),
        };
        if remaining.len() < record_len {
            break;
        }
        let record = LogRecord::decode(&remaining[..record_len])?;
        max_tx_id = max_tx_id.max(record.tx_id);
        max_sequence = max_sequence.max(record.sequence);
        offset = offset
            .checked_add(record_len)
            .ok_or(GenerationError::CounterExhausted)?;
    }
    Ok((max_tx_id, max_sequence))
}

#[derive(Debug, Error)]
pub enum GenerationError {
    #[error("store error: {0}")]
    Store(#[from] EngineError),
    #[error("log error: {0}")]
    Log(#[from] StoreError),
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("generation destination already exists")]
    DestinationExists,
    #[error("generation destination must differ from source store")]
    DestinationIsSource,
    #[error("generation high-water counter exhausted")]
    CounterExhausted,
    #[error("generated store failed identity/state verification")]
    VerificationMismatch,
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    fn temp_store_path(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("voodoo-store-generation-{name}-{nonce}.vstore"))
    }

    #[test]
    fn compact_generation_preserves_identity_state_and_cdc_continuity() {
        let source = temp_store_path("source");
        let generation = temp_store_path("generation");
        let store_id;
        let expected_changes;
        let last_change_tx;
        {
            let mut store = Store::open(&source).unwrap();
            store_id = store.header().store_id;
            for index in 0..20 {
                store
                    .put(b"key", format!("value-{index}").as_bytes())
                    .unwrap();
            }
            store.put(b"keep", b"yes").unwrap();
            expected_changes = store.changes_after(None, usize::MAX).unwrap();
            last_change_tx = expected_changes.last().unwrap().tx_id;
            let report = store.compact_generation_to(&generation).unwrap();
            assert_eq!(report.store_id, store_id);
            assert!(report.high_water_tx_id > last_change_tx);
            assert!(report.generation_bytes < report.source_bytes);
        }

        {
            let mut compacted = Store::open(&generation).unwrap();
            assert_eq!(compacted.header().store_id, store_id);
            assert_eq!(compacted.get(b"key"), Some(b"value-19".as_slice()));
            assert_eq!(compacted.get(b"keep"), Some(b"yes".as_slice()));
            assert_eq!(
                compacted.changes_after(None, usize::MAX).unwrap(),
                expected_changes
            );
            compacted.put(b"after-generation", b"safe").unwrap();
            let new_changes = compacted
                .changes_after(Some(last_change_tx), usize::MAX)
                .unwrap();
            assert_eq!(new_changes.len(), 1);
            assert!(new_changes[0].tx_id > last_change_tx);
            assert_eq!(new_changes[0].key, b"after-generation");
        }

        let _ = fs::remove_file(source);
        let _ = fs::remove_file(generation);
    }

    #[test]
    fn compact_generation_never_overwrites_destination() {
        let source = temp_store_path("existing-source");
        let generation = temp_store_path("existing-generation");
        let store = Store::open(&source).unwrap();
        fs::write(&generation, b"do-not-touch").unwrap();
        assert!(matches!(
            store.compact_generation_to(&generation),
            Err(GenerationError::DestinationExists)
        ));
        assert_eq!(fs::read(&generation).unwrap(), b"do-not-touch");
        drop(store);
        let _ = fs::remove_file(source);
        let _ = fs::remove_file(generation);
    }
}
