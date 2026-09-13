//! Typed cross-domain transaction operations.
//!
//! These methods let one Store transaction combine ordinary application KV
//! mutations with durable infrastructure mutations without exposing the
//! reserved `\xffvds:*` namespace to callers.

use thiserror::Error;

use crate::{EngineError, JobId, JobSpec, Transaction};

const JOB_PREFIX: &[u8] = b"\xffvds:job:data:";
const HISTORY_PREFIX: &[u8] = b"\xffvds:job:history:";
const JOB_VERSION: u8 = 1;

impl Transaction<'_> {
    /// Enqueues a durable job in this transaction.
    ///
    /// The job record and its initial history entry become visible if and only
    /// if the surrounding transaction commits. This means application state
    /// and background work can share one durability boundary.
    pub fn enqueue_job(&mut self, spec: JobSpec, now_ms: i64) -> Result<JobId, TransactionalError> {
        validate_job_spec(&spec)?;
        let id = random_id()?;
        let encoded = encode_new_job(id, &spec)?;
        self.put_internal(job_key(&id), encoded)?;
        self.put_internal(
            history_key(&id, 0),
            encode_submitted_history(now_ms, b"transactional")?,
        )?;
        Ok(id)
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
    #[error("operating-system entropy is unavailable")]
    EntropyUnavailable,
}

fn validate_job_spec(spec: &JobSpec) -> Result<(), TransactionalError> {
    if spec.handler.is_empty() {
        return Err(TransactionalError::EmptyHandler);
    }
    if spec.max_attempts == 0 {
        return Err(TransactionalError::InvalidMaxAttempts);
    }
    Ok(())
}

fn random_id() -> Result<JobId, TransactionalError> {
    let mut id = [0u8; 16];
    getrandom::fill(&mut id).map_err(|_| TransactionalError::EntropyUnavailable)?;
    Ok(id)
}

fn job_key(id: &JobId) -> Vec<u8> {
    let mut key = Vec::with_capacity(JOB_PREFIX.len() + id.len());
    key.extend_from_slice(JOB_PREFIX);
    key.extend_from_slice(id);
    key
}

fn history_key(id: &JobId, sequence: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(HISTORY_PREFIX.len() + id.len() + 8);
    key.extend_from_slice(HISTORY_PREFIX);
    key.extend_from_slice(id);
    key.extend_from_slice(&sequence.to_be_bytes());
    key
}

fn append_len(out: &mut Vec<u8>, bytes: &[u8]) -> Result<(), TransactionalError> {
    let len = u32::try_from(bytes.len()).map_err(|_| TransactionalError::FieldTooLarge)?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

/// Encodes exactly the v1 representation consumed by `jobs::decode_job`.
fn encode_new_job(id: JobId, spec: &JobSpec) -> Result<Vec<u8>, TransactionalError> {
    let mut out = Vec::new();
    out.push(JOB_VERSION);
    out.push(0); // DurableJobState::Ready
    out.extend_from_slice(&id);
    out.extend_from_slice(&spec.available_at_ms.to_le_bytes());
    out.extend_from_slice(&spec.deadline_ms.unwrap_or(i64::MIN).to_le_bytes());
    out.extend_from_slice(&spec.priority.to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes()); // attempts
    out.extend_from_slice(&spec.max_attempts.to_le_bytes());
    out.extend_from_slice(&spec.retry_backoff_ms.to_le_bytes());
    out.extend_from_slice(&0i64.to_le_bytes()); // lease_until_ms
    out.extend_from_slice(&0u32.to_le_bytes()); // lease_generation
    append_len(&mut out, &spec.handler)?;
    append_len(&mut out, &spec.payload)?;
    match &spec.idempotency_key {
        Some(key) => {
            out.push(1);
            append_len(&mut out, key)?;
        }
        None => out.push(0),
    }
    Ok(out)
}

/// Encodes exactly the v1 representation consumed by `jobs::decode_history`.
fn encode_submitted_history(at_ms: i64, detail: &[u8]) -> Result<Vec<u8>, TransactionalError> {
    let mut out = Vec::new();
    out.push(JOB_VERSION);
    out.extend_from_slice(&0u64.to_le_bytes()); // sequence
    out.extend_from_slice(&at_ms.to_le_bytes());
    out.push(0); // JobHistoryKind::Submitted
    append_len(&mut out, detail)?;
    Ok(out)
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
}
