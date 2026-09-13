//! Typed cross-domain transaction operations.
//!
//! These methods let one Store transaction combine ordinary application KV
//! mutations with durable infrastructure mutations without exposing the
//! reserved `\xffvds:*` namespace to callers.

use thiserror::Error;

use crate::{EngineError, JobError, JobId, JobSpec, Transaction};

impl Transaction<'_> {
    /// Enqueues a durable job in this transaction.
    ///
    /// The job record and its initial history entry become visible if and only
    /// if the surrounding transaction commits. This means application state
    /// and background work can share one durability boundary.
    ///
    /// Job validation, v1 wire encoding, history encoding, and idempotency
    /// lookup are owned by the Jobs module. This wrapper deliberately contains
    /// no persisted Job-format knowledge.
    pub fn enqueue_job(&mut self, spec: JobSpec, now_ms: i64) -> Result<JobId, TransactionalError> {
        crate::jobs::enqueue_job_tx(self, spec, now_ms, b"transactional").map_err(map_job_error)
    }
}

#[derive(Debug, Error)]
pub enum TransactionalError {
    #[error("store error: {0}")]
    Store(#[from] EngineError),
    #[error("job handler cannot be empty")]
    EmptyHandler,
    #[error("max attempts must be at least one")]
    InvalidMaxAttempts,
    #[error("job field is too large")]
    FieldTooLarge,
    #[error("job record is corrupt or unsupported")]
    CorruptJobRecord,
    #[error("operating-system entropy is unavailable")]
    EntropyUnavailable,
    #[error("job transaction error: {0}")]
    Job(String),
}

fn map_job_error(error: JobError) -> TransactionalError {
    match error {
        JobError::Store(error) => TransactionalError::Store(error),
        JobError::EmptyHandler => TransactionalError::EmptyHandler,
        JobError::InvalidMaxAttempts => TransactionalError::InvalidMaxAttempts,
        JobError::FieldTooLarge => TransactionalError::FieldTooLarge,
        JobError::CorruptRecord => TransactionalError::CorruptJobRecord,
        JobError::EntropyUnavailable => TransactionalError::EntropyUnavailable,
        other => TransactionalError::Job(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::{JobHistoryKind, Store};

    use super::*;

    fn temp_store_path(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("voodoo-store-xdomain-{name}-{nonce}.vstore"))
    }

    #[test]
    fn application_state_and_job_commit_atomically() {
        let path = temp_store_path("commit");
        let job_id;
        {
            let mut store = Store::open(&path).unwrap();
            let mut tx = store.begin().unwrap();
            tx.put(b"order:42", b"paid").unwrap();
            job_id = tx
                .enqueue_job(JobSpec::new(b"email.receipt", b"order:42"), 100)
                .unwrap();
            tx.commit().unwrap();

            assert_eq!(store.get(b"order:42"), Some(b"paid".as_slice()));
            let job = store.get_job(&job_id).unwrap().unwrap();
            assert_eq!(job.handler, b"email.receipt");
            let history = store.job_history(&job_id).unwrap();
            assert_eq!(history.len(), 1);
            assert_eq!(history[0].kind, JobHistoryKind::Submitted);
            assert_eq!(history[0].detail, b"transactional");
        }
        let _ = fs::remove_file(path);
    }

    #[test]
    fn rollback_hides_application_state_and_job() {
        let path = temp_store_path("rollback");
        let job_id;
        {
            let mut store = Store::open(&path).unwrap();
            let mut tx = store.begin().unwrap();
            tx.put(b"order:42", b"paid").unwrap();
            job_id = tx
                .enqueue_job(JobSpec::new(b"email.receipt", b"order:42"), 100)
                .unwrap();
            tx.rollback().unwrap();

            assert_eq!(store.get(b"order:42"), None);
            assert!(store.get_job(&job_id).unwrap().is_none());
        }
        {
            let store = Store::open(&path).unwrap();
            assert_eq!(store.get(b"order:42"), None);
            assert!(store.get_job(&job_id).unwrap().is_none());
        }
        let _ = fs::remove_file(path);
    }

    #[test]
    fn idempotency_reuses_committed_job_in_transaction() {
        let path = temp_store_path("idempotency-committed");
        let mut store = Store::open(&path).unwrap();
        let mut spec = JobSpec::new(b"invoice", b"42");
        spec.idempotency_key = Some(b"invoice:42".to_vec());
        let first = store.submit_job(spec.clone(), 1).unwrap();

        let mut tx = store.begin().unwrap();
        let second = tx.enqueue_job(spec, 2).unwrap();
        assert_eq!(first, second);
        tx.commit().unwrap();
        assert_eq!(store.job_history(&first).unwrap().len(), 1);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn idempotency_reuses_job_staged_earlier_in_same_transaction() {
        let path = temp_store_path("idempotency-staged");
        let mut store = Store::open(&path).unwrap();
        let mut spec = JobSpec::new(b"invoice", b"42");
        spec.idempotency_key = Some(b"invoice:42".to_vec());

        let mut tx = store.begin().unwrap();
        let first = tx.enqueue_job(spec.clone(), 1).unwrap();
        let second = tx.enqueue_job(spec, 2).unwrap();
        assert_eq!(first, second);
        tx.commit().unwrap();
        assert_eq!(store.job_history(&first).unwrap().len(), 1);
        let _ = fs::remove_file(path);
    }
}
