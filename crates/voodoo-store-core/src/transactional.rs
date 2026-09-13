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
const JOB_FIXED_LEN: usize = 66;

impl Transaction<'_> {
    /// Enqueues a durable job in this transaction.
    ///
    /// The job record and its initial history entry become visible if and only
    /// if the surrounding transaction commits. This means application state
    /// and background work can share one durability boundary.
    ///
    /// Idempotency lookup uses the transaction's staged view, so an existing
    /// committed job or a job enqueued earlier in this same transaction is
    /// reused rather than duplicated.
    pub fn enqueue_job(&mut self, spec: JobSpec, now_ms: i64) -> Result<JobId, TransactionalError> {
        validate_job_spec(&spec)?;
        if let Some(key) = spec.idempotency_key.as_deref() {
            if let Some(existing) = find_job_by_idempotency(self, key)? {
                return Ok(existing);
            }
        }

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
    #[error("job record is corrupt or unsupported")]
    CorruptJobRecord,
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

fn read_len<'a>(bytes: &'a [u8], cursor: &mut usize) -> Result<&'a [u8], TransactionalError> {
    let length_end = cursor
        .checked_add(4)
        .ok_or(TransactionalError::CorruptJobRecord)?;
    let length_bytes = bytes
        .get(*cursor..length_end)
        .ok_or(TransactionalError::CorruptJobRecord)?;
    let len = u32::from_le_bytes(
        length_bytes
            .try_into()
            .map_err(|_| TransactionalError::CorruptJobRecord)?,
    ) as usize;
    *cursor = length_end;
    let end = cursor
        .checked_add(len)
        .ok_or(TransactionalError::CorruptJobRecord)?;
    let value = bytes
        .get(*cursor..end)
        .ok_or(TransactionalError::CorruptJobRecord)?;
    *cursor = end;
    Ok(value)
}

fn find_job_by_idempotency(
    tx: &Transaction<'_>,
    key: &[u8],
) -> Result<Option<JobId>, TransactionalError> {
    for (_, encoded) in tx.scan_prefix_internal(JOB_PREFIX) {
        let (id, idempotency_key) = decode_job_identity(&encoded)?;
        if idempotency_key.as_deref() == Some(key) {
            return Ok(Some(id));
        }
    }
    Ok(None)
}

fn decode_job_identity(bytes: &[u8]) -> Result<(JobId, Option<Vec<u8>>), TransactionalError> {
    if bytes.len() < JOB_FIXED_LEN || bytes[0] != JOB_VERSION {
        return Err(TransactionalError::CorruptJobRecord);
    }
    let id = bytes[2..18]
        .try_into()
        .map_err(|_| TransactionalError::CorruptJobRecord)?;
    let mut cursor = JOB_FIXED_LEN;
    let _handler = read_len(bytes, &mut cursor)?;
    let _payload = read_len(bytes, &mut cursor)?;
    let flag = *bytes
        .get(cursor)
        .ok_or(TransactionalError::CorruptJobRecord)?;
    cursor += 1;
    let idempotency_key = match flag {
        0 => None,
        1 => Some(read_len(bytes, &mut cursor)?.to_vec()),
        _ => return Err(TransactionalError::CorruptJobRecord),
    };
    if cursor != bytes.len() {
        return Err(TransactionalError::CorruptJobRecord);
    }
    Ok((id, idempotency_key))
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
