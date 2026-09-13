//! Recoverable offline activation for compact store generations.
//!
//! Activation is deliberately an offline operation: both the active store and
//! generation must be closed. The active file is first moved to a caller-chosen
//! backup path, then the generation is moved into the active path. The backup is
//! retained after success. `recover_generation_activation` can complete or roll
//! back the small rename window after an interrupted activation.
//!
//! This is crash-recoverable at the file-layout level, but does not yet claim
//! absolute power-loss atomicity for directory metadata on every filesystem.

use std::fs::{self, OpenOptions};
use std::io::{self, Read};
use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::{LogRecord, RecordKind, STORE_HEADER_LEN, Store, StoreError};

const LOG_HEADER_LEN: usize = 25;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GenerationActivationReport {
    pub store_id: [u8; 16],
    pub active_path: PathBuf,
    pub backup_path: PathBuf,
    pub active_bytes: u64,
    pub backup_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivationRecovery {
    NotStarted,
    Completed,
    CompletedInterruptedActivation,
    RestoredBackup,
}

/// Activates an identity-preserving compact generation while retaining the
/// previous active file as a rollback backup.
///
/// The generation is rejected as stale when the active store has committed any
/// transaction since the generation was built.
pub fn activate_generation_offline(
    active: impl AsRef<Path>,
    generation: impl AsRef<Path>,
    backup: impl AsRef<Path>,
) -> Result<GenerationActivationReport, ActivationError> {
    let active = active.as_ref();
    let generation = generation.as_ref();
    let backup = backup.as_ref();
    validate_distinct_paths(active, generation, backup)?;
    if backup.exists() {
        return Err(ActivationError::BackupExists);
    }

    let active_report = Store::verify(active)?;
    let generation_report = Store::verify(generation)?;
    if active_report.has_torn_tail() || generation_report.has_torn_tail() {
        return Err(ActivationError::TornStore);
    }
    if active_report.header.store_id != generation_report.header.store_id {
        return Err(ActivationError::IdentityMismatch);
    }

    let active_log = inspect_log(active)?;
    let generation_log = inspect_log(generation)?;
    let marker = generation_log
        .last_record
        .as_ref()
        .ok_or(ActivationError::MissingGenerationMarker)?;
    if marker.kind != RecordKind::Commit || !marker.payload.is_empty() {
        return Err(ActivationError::MissingGenerationMarker);
    }
    if marker.tx_id != generation_log.max_tx_id || marker.sequence != generation_log.max_sequence {
        return Err(ActivationError::InvalidGenerationMarker);
    }
    let expected_marker_tx = active_log
        .max_tx_id
        .checked_add(1)
        .ok_or(ActivationError::CounterExhausted)?;
    if marker.tx_id != expected_marker_tx {
        return Err(ActivationError::StaleGeneration {
            active_tx_id: active_log.max_tx_id,
            marker_tx_id: marker.tx_id,
        });
    }

    fs::rename(active, backup)?;
    if let Err(error) = fs::rename(generation, active) {
        let rollback = fs::rename(backup, active);
        return match rollback {
            Ok(()) => Err(ActivationError::Io(error)),
            Err(rollback_error) => Err(ActivationError::RollbackFailed {
                activation: error.to_string(),
                rollback: rollback_error.to_string(),
            }),
        };
    }

    if let Err(error) = verify_activated(active, backup, active_report.header.store_id) {
        let rollback = rollback_activation(active, generation, backup);
        return match rollback {
            Ok(()) => Err(error),
            Err(rollback_error) => Err(ActivationError::RollbackFailed {
                activation: error.to_string(),
                rollback: rollback_error.to_string(),
            }),
        };
    }

    Ok(GenerationActivationReport {
        store_id: active_report.header.store_id,
        active_path: active.to_path_buf(),
        backup_path: backup.to_path_buf(),
        active_bytes: fs::metadata(active)?.len(),
        backup_bytes: fs::metadata(backup)?.len(),
    })
}

/// Repairs the known file layouts left by an interrupted two-rename activation.
/// It never overwrites an existing file.
pub fn recover_generation_activation(
    active: impl AsRef<Path>,
    generation: impl AsRef<Path>,
    backup: impl AsRef<Path>,
) -> Result<ActivationRecovery, ActivationError> {
    let active = active.as_ref();
    let generation = generation.as_ref();
    let backup = backup.as_ref();
    validate_distinct_paths(active, generation, backup)?;

    match (active.exists(), generation.exists(), backup.exists()) {
        (true, true, false) => Ok(ActivationRecovery::NotStarted),
        (true, false, true) => {
            let active_report = Store::verify(active)?;
            let backup_report = Store::verify(backup)?;
            if active_report.header.store_id != backup_report.header.store_id {
                return Err(ActivationError::IdentityMismatch);
            }
            Ok(ActivationRecovery::Completed)
        }
        (false, true, true) => {
            let generation_report = Store::verify(generation)?;
            let backup_report = Store::verify(backup)?;
            if generation_report.header.store_id != backup_report.header.store_id {
                return Err(ActivationError::IdentityMismatch);
            }
            fs::rename(generation, active)?;
            verify_activated(active, backup, generation_report.header.store_id)?;
            Ok(ActivationRecovery::CompletedInterruptedActivation)
        }
        (false, false, true) => {
            fs::rename(backup, active)?;
            Store::verify(active)?;
            Ok(ActivationRecovery::RestoredBackup)
        }
        _ => Err(ActivationError::AmbiguousLayout),
    }
}

fn verify_activated(
    active: &Path,
    backup: &Path,
    store_id: [u8; 16],
) -> Result<(), ActivationError> {
    let active_report = Store::verify(active)?;
    let backup_report = Store::verify(backup)?;
    if active_report.has_torn_tail() || backup_report.has_torn_tail() {
        return Err(ActivationError::TornStore);
    }
    if active_report.header.store_id != store_id || backup_report.header.store_id != store_id {
        return Err(ActivationError::IdentityMismatch);
    }
    Ok(())
}

fn rollback_activation(active: &Path, generation: &Path, backup: &Path) -> Result<(), io::Error> {
    if generation.exists() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "generation path unexpectedly exists during rollback",
        ));
    }
    fs::rename(active, generation)?;
    fs::rename(backup, active)?;
    Ok(())
}

fn validate_distinct_paths(
    active: &Path,
    generation: &Path,
    backup: &Path,
) -> Result<(), ActivationError> {
    if active == generation || active == backup || generation == backup {
        return Err(ActivationError::PathsMustDiffer);
    }
    Ok(())
}

#[derive(Debug)]
struct LogInspection {
    max_tx_id: u64,
    max_sequence: u64,
    last_record: Option<LogRecord>,
}

fn inspect_log(path: &Path) -> Result<LogInspection, ActivationError> {
    let mut file = OpenOptions::new().read(true).open(path)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    if bytes.len() < STORE_HEADER_LEN {
        return Err(ActivationError::TornStore);
    }
    let bytes = &bytes[STORE_HEADER_LEN..];
    let mut offset = 0usize;
    let mut max_tx_id = 0u64;
    let mut max_sequence = 0u64;
    let mut last_record = None;

    while offset < bytes.len() {
        let remaining = &bytes[offset..];
        if remaining.len() < LOG_HEADER_LEN {
            return Err(ActivationError::TornStore);
        }
        let payload_len = u32::from_le_bytes(
            remaining[21..25]
                .try_into()
                .map_err(|_| ActivationError::TornStore)?,
        ) as usize;
        let record_len = LOG_HEADER_LEN
            .checked_add(payload_len)
            .and_then(|value| value.checked_add(4))
            .ok_or(ActivationError::CounterExhausted)?;
        if remaining.len() < record_len {
            return Err(ActivationError::TornStore);
        }
        let record = LogRecord::decode(&remaining[..record_len])?;
        max_tx_id = max_tx_id.max(record.tx_id);
        max_sequence = max_sequence.max(record.sequence);
        last_record = Some(record);
        offset = offset
            .checked_add(record_len)
            .ok_or(ActivationError::CounterExhausted)?;
    }

    Ok(LogInspection {
        max_tx_id,
        max_sequence,
        last_record,
    })
}

#[derive(Debug, Error)]
pub enum ActivationError {
    #[error("store verification error: {0}")]
    Store(#[from] crate::EngineError),
    #[error("log error: {0}")]
    Log(#[from] StoreError),
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("active, generation, and backup paths must all differ")]
    PathsMustDiffer,
    #[error("activation backup path already exists")]
    BackupExists,
    #[error("active store and generation identities differ")]
    IdentityMismatch,
    #[error("active store or generation has a torn physical tail")]
    TornStore,
    #[error("generation does not end in the required empty commit marker")]
    MissingGenerationMarker,
    #[error("generation marker is not the physical high-water record")]
    InvalidGenerationMarker,
    #[error(
        "generation is stale: active tx high-water is {active_tx_id}, marker tx is {marker_tx_id}"
    )]
    StaleGeneration {
        active_tx_id: u64,
        marker_tx_id: u64,
    },
    #[error("activation/recovery file layout is ambiguous")]
    AmbiguousLayout,
    #[error("activation counter exhausted")]
    CounterExhausted,
    #[error("activation failed ({activation}) and rollback also failed ({rollback})")]
    RollbackFailed {
        activation: String,
        rollback: String,
    },
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
        std::env::temp_dir().join(format!("voodoo-store-activation-{name}-{nonce}.vstore"))
    }

    #[test]
    fn offline_activation_keeps_backup_and_new_store_is_writable() {
        let active = temp_store_path("active");
        let generation = temp_store_path("generation");
        let backup = temp_store_path("backup");
        let store_id;
        {
            let mut store = Store::open(&active).unwrap();
            store_id = store.header().store_id;
            for index in 0..10 {
                store.put(b"key", format!("v{index}").as_bytes()).unwrap();
            }
            store.compact_generation_to(&generation).unwrap();
        }

        let report = activate_generation_offline(&active, &generation, &backup).unwrap();
        assert_eq!(report.store_id, store_id);
        assert!(backup.exists());
        assert!(!generation.exists());

        let mut activated = Store::open(&active).unwrap();
        assert_eq!(activated.header().store_id, store_id);
        assert_eq!(activated.get(b"key"), Some(b"v9".as_slice()));
        activated.put(b"after", b"ok").unwrap();
        drop(activated);

        let previous = Store::open(&backup).unwrap();
        assert_eq!(previous.header().store_id, store_id);
        assert_eq!(previous.get(b"after"), None);
        drop(previous);
        let _ = fs::remove_file(active);
        let _ = fs::remove_file(backup);
    }

    #[test]
    fn activation_rejects_generation_when_active_store_advanced() {
        let active = temp_store_path("stale-active");
        let generation = temp_store_path("stale-generation");
        let backup = temp_store_path("stale-backup");
        {
            let mut store = Store::open(&active).unwrap();
            store.put(b"a", b"1").unwrap();
            store.compact_generation_to(&generation).unwrap();
            store.put(b"newer", b"2").unwrap();
        }

        assert!(matches!(
            activate_generation_offline(&active, &generation, &backup),
            Err(ActivationError::StaleGeneration { .. })
        ));
        assert!(active.exists());
        assert!(generation.exists());
        assert!(!backup.exists());
        let active_store = Store::open(&active).unwrap();
        assert_eq!(active_store.get(b"newer"), Some(b"2".as_slice()));
        drop(active_store);
        let _ = fs::remove_file(active);
        let _ = fs::remove_file(generation);
    }

    #[test]
    fn recovery_completes_interrupted_first_rename() {
        let active = temp_store_path("recover-active");
        let generation = temp_store_path("recover-generation");
        let backup = temp_store_path("recover-backup");
        let store_id;
        {
            let mut store = Store::open(&active).unwrap();
            store_id = store.header().store_id;
            store.put(b"key", b"value").unwrap();
            store.compact_generation_to(&generation).unwrap();
        }
        fs::rename(&active, &backup).unwrap();

        assert_eq!(
            recover_generation_activation(&active, &generation, &backup).unwrap(),
            ActivationRecovery::CompletedInterruptedActivation
        );
        let recovered = Store::open(&active).unwrap();
        assert_eq!(recovered.header().store_id, store_id);
        assert_eq!(recovered.get(b"key"), Some(b"value".as_slice()));
        drop(recovered);
        let _ = fs::remove_file(active);
        let _ = fs::remove_file(backup);
    }
}
