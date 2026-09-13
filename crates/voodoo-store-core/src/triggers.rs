//! Durable trigger metadata and trigger-to-job routing.
//!
//! Triggers describe *when* durable work should be enqueued. Voodoo Store
//! persists definitions and routes fires into durable jobs, but never executes
//! application code or evaluates application-specific predicates.

use thiserror::Error;

use crate::{
    EngineError, JobError, JobId, JobSpec, Store, Transaction, TransactionalError,
};

const TRIGGER_PREFIX: &[u8] = b"\xffvds:trigger:data:";
const VERSION: u8 = 1;

pub type TriggerId = [u8; 16];

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TriggerSource {
    Manual,
    Collection {
        collection: Vec<u8>,
        operation: Vec<u8>,
    },
    Stream {
        stream: Vec<u8>,
    },
    Topic {
        topic: Vec<u8>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableTrigger {
    pub id: TriggerId,
    pub name: Vec<u8>,
    pub source: TriggerSource,
    pub job: JobSpec,
    pub enabled: bool,
    pub fire_count: u64,
    pub last_fired_at_ms: Option<i64>,
}

#[derive(Debug, Error)]
pub enum TriggerError {
    #[error("store error: {0}")]
    Store(#[from] EngineError),
    #[error("job error: {0}")]
    Job(#[from] JobError),
    #[error("transactional job error: {0}")]
    Transactional(#[from] TransactionalError),
    #[error("trigger not found")]
    NotFound,
    #[error("trigger name cannot be empty")]
    EmptyName,
    #[error("trigger record is corrupt")]
    CorruptRecord,
    #[error("trigger fire count overflow")]
    FireCountOverflow,
    #[error("OS entropy unavailable")]
    EntropyUnavailable,
}

impl Store {
    pub fn create_trigger(
        &mut self,
        name: impl AsRef<[u8]>,
        source: TriggerSource,
        job: JobSpec,
    ) -> Result<TriggerId, TriggerError> {
        let name = name.as_ref();
        if name.is_empty() {
            return Err(TriggerError::EmptyName);
        }
        validate_source(&source)?;
        validate_job(&job)?;
        let id = random_id()?;
        let trigger = DurableTrigger {
            id,
            name: name.to_vec(),
            source,
            job,
            enabled: true,
            fire_count: 0,
            last_fired_at_ms: None,
        };
        self.put_internal(trigger_key(&id), encode_trigger(&trigger)?)?;
        Ok(id)
    }

    pub fn get_trigger(&self, id: &TriggerId) -> Result<Option<DurableTrigger>, TriggerError> {
        self.get(trigger_key(id)).map(decode_trigger).transpose()
    }

    pub fn list_triggers(&self) -> Result<Vec<DurableTrigger>, TriggerError> {
        let mut triggers = self
            .scan_prefix(TRIGGER_PREFIX)
            .into_iter()
            .map(|(_, value)| decode_trigger(&value))
            .collect::<Result<Vec<_>, _>>()?;
        triggers.sort_by(|a, b| a.name.cmp(&b.name).then_with(|| a.id.cmp(&b.id)));
        Ok(triggers)
    }

    pub fn set_trigger_enabled(
        &mut self,
        id: &TriggerId,
        enabled: bool,
    ) -> Result<bool, TriggerError> {
        let Some(mut trigger) = self.get_trigger(id)? else {
            return Ok(false);
        };
        trigger.enabled = enabled;
        self.put_internal(trigger_key(id), encode_trigger(&trigger)?)?;
        Ok(true)
    }

    /// Atomically creates the trigger's durable job and advances trigger fire
    /// metadata in one Store transaction.
    pub fn fire_trigger(
        &mut self,
        id: &TriggerId,
        event_payload: impl AsRef<[u8]>,
        now_ms: i64,
    ) -> Result<Option<JobId>, TriggerError> {
        let mut tx = self.begin()?;
        let job_id = tx.fire_trigger(id, event_payload, now_ms)?;
        tx.commit()?;
        Ok(job_id)
    }
}

impl Transaction<'_> {
    /// Stages a trigger fire in the surrounding transaction. The job record,
    /// submitted history, fire counter, and timestamp become visible together.
    pub fn fire_trigger(
        &mut self,
        id: &TriggerId,
        event_payload: impl AsRef<[u8]>,
        now_ms: i64,
    ) -> Result<Option<JobId>, TriggerError> {
        let Some(encoded) = self.get_internal(trigger_key(id)) else {
            return Err(TriggerError::NotFound);
        };
        let mut trigger = decode_trigger(encoded)?;
        if !trigger.enabled {
            return Ok(None);
        }

        let mut spec = trigger.job.clone();
        let event_payload = event_payload.as_ref();
        if !event_payload.is_empty() {
            spec.payload = event_payload.to_vec();
        }
        validate_job(&spec)?;

        let job_id = self.enqueue_job(spec, now_ms)?;
        trigger.fire_count = trigger
            .fire_count
            .checked_add(1)
            .ok_or(TriggerError::FireCountOverflow)?;
        trigger.last_fired_at_ms = Some(now_ms);
        self.put_internal(trigger_key(id), encode_trigger(&trigger)?)?;
        Ok(Some(job_id))
    }
}

fn validate_job(spec: &JobSpec) -> Result<(), TriggerError> {
    if spec.handler.is_empty() || spec.max_attempts == 0 {
        return Err(TriggerError::CorruptRecord);
    }
    Ok(())
}

fn validate_source(source: &TriggerSource) -> Result<(), TriggerError> {
    match source {
        TriggerSource::Manual => Ok(()),
        TriggerSource::Collection {
            collection,
            operation,
        } if !collection.is_empty() && !operation.is_empty() => Ok(()),
        TriggerSource::Stream { stream } if !stream.is_empty() => Ok(()),
        TriggerSource::Topic { topic } if !topic.is_empty() => Ok(()),
        _ => Err(TriggerError::CorruptRecord),
    }
}

fn random_id() -> Result<[u8; 16], TriggerError> {
    let mut id = [0u8; 16];
    getrandom::fill(&mut id).map_err(|_| TriggerError::EntropyUnavailable)?;
    Ok(id)
}

fn trigger_key(id: &TriggerId) -> Vec<u8> {
    let mut key = Vec::with_capacity(TRIGGER_PREFIX.len() + id.len());
    key.extend_from_slice(TRIGGER_PREFIX);
    key.extend_from_slice(id);
    key
}

fn encode_trigger(trigger: &DurableTrigger) -> Result<Vec<u8>, TriggerError> {
    let mut out = Vec::new();
    out.push(VERSION);
    out.extend_from_slice(&trigger.id);
    write_bytes(&mut out, &trigger.name)?;
    encode_source(&mut out, &trigger.source)?;
    encode_job_spec(&mut out, &trigger.job)?;
    out.push(u8::from(trigger.enabled));
    out.extend_from_slice(&trigger.fire_count.to_le_bytes());
    match trigger.last_fired_at_ms {
        Some(value) => {
            out.push(1);
            out.extend_from_slice(&value.to_le_bytes());
        }
        None => out.push(0),
    }
    Ok(out)
}

fn decode_trigger(bytes: &[u8]) -> Result<DurableTrigger, TriggerError> {
    let mut cursor = Cursor::new(bytes);
    if cursor.u8()? != VERSION {
        return Err(TriggerError::CorruptRecord);
    }
    let id = cursor.array_16()?;
    let name = cursor.bytes()?;
    let source = decode_source(&mut cursor)?;
    let job = decode_job_spec(&mut cursor)?;
    let enabled = match cursor.u8()? {
        0 => false,
        1 => true,
        _ => return Err(TriggerError::CorruptRecord),
    };
    let fire_count = cursor.u64()?;
    let last_fired_at_ms = match cursor.u8()? {
        0 => None,
        1 => Some(cursor.i64()?),
        _ => return Err(TriggerError::CorruptRecord),
    };
    if !cursor.finished() || name.is_empty() {
        return Err(TriggerError::CorruptRecord);
    }
    validate_source(&source)?;
    validate_job(&job)?;
    Ok(DurableTrigger {
        id,
        name,
        source,
        job,
        enabled,
        fire_count,
        last_fired_at_ms,
    })
}

fn encode_source(out: &mut Vec<u8>, source: &TriggerSource) -> Result<(), TriggerError> {
    match source {
        TriggerSource::Manual => out.push(0),
        TriggerSource::Collection {
            collection,
            operation,
        } => {
            out.push(1);
            write_bytes(out, collection)?;
            write_bytes(out, operation)?;
        }
        TriggerSource::Stream { stream } => {
            out.push(2);
            write_bytes(out, stream)?;
        }
        TriggerSource::Topic { topic } => {
            out.push(3);
            write_bytes(out, topic)?;
        }
    }
    Ok(())
}

fn decode_source(cursor: &mut Cursor<'_>) -> Result<TriggerSource, TriggerError> {
    match cursor.u8()? {
        0 => Ok(TriggerSource::Manual),
        1 => Ok(TriggerSource::Collection {
            collection: cursor.bytes()?,
            operation: cursor.bytes()?,
        }),
        2 => Ok(TriggerSource::Stream {
            stream: cursor.bytes()?,
        }),
        3 => Ok(TriggerSource::Topic {
            topic: cursor.bytes()?,
        }),
        _ => Err(TriggerError::CorruptRecord),
    }
}

fn encode_job_spec(out: &mut Vec<u8>, spec: &JobSpec) -> Result<(), TriggerError> {
    write_bytes(out, &spec.handler)?;
    write_bytes(out, &spec.payload)?;
    out.extend_from_slice(&spec.available_at_ms.to_le_bytes());
    match spec.deadline_ms {
        Some(value) => {
            out.push(1);
            out.extend_from_slice(&value.to_le_bytes());
        }
        None => out.push(0),
    }
    out.extend_from_slice(&spec.priority.to_le_bytes());
    out.extend_from_slice(&spec.max_attempts.to_le_bytes());
    out.extend_from_slice(&spec.retry_backoff_ms.to_le_bytes());
    match &spec.idempotency_key {
        Some(value) => {
            out.push(1);
            write_bytes(out, value)?;
        }
        None => out.push(0),
    }
    Ok(())
}

fn decode_job_spec(cursor: &mut Cursor<'_>) -> Result<JobSpec, TriggerError> {
    let handler = cursor.bytes()?;
    let payload = cursor.bytes()?;
    let available_at_ms = cursor.i64()?;
    let deadline_ms = match cursor.u8()? {
        0 => None,
        1 => Some(cursor.i64()?),
        _ => return Err(TriggerError::CorruptRecord),
    };
    let priority = cursor.i32()?;
    let max_attempts = cursor.u32()?;
    let retry_backoff_ms = cursor.u64()?;
    let idempotency_key = match cursor.u8()? {
        0 => None,
        1 => Some(cursor.bytes()?),
        _ => return Err(TriggerError::CorruptRecord),
    };
    Ok(JobSpec {
        handler,
        payload,
        available_at_ms,
        deadline_ms,
        priority,
        max_attempts,
        retry_backoff_ms,
        idempotency_key,
    })
}

fn write_bytes(out: &mut Vec<u8>, value: &[u8]) -> Result<(), TriggerError> {
    let len = u32::try_from(value.len()).map_err(|_| TriggerError::CorruptRecord)?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(value);
    Ok(())
}

struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], TriggerError> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or(TriggerError::CorruptRecord)?;
        let value = self
            .bytes
            .get(self.pos..end)
            .ok_or(TriggerError::CorruptRecord)?;
        self.pos = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, TriggerError> {
        Ok(*self.take(1)?.first().ok_or(TriggerError::CorruptRecord)?)
    }

    fn u32(&mut self) -> Result<u32, TriggerError> {
        Ok(u32::from_le_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| TriggerError::CorruptRecord)?,
        ))
    }

    fn i32(&mut self) -> Result<i32, TriggerError> {
        Ok(i32::from_le_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| TriggerError::CorruptRecord)?,
        ))
    }

    fn u64(&mut self) -> Result<u64, TriggerError> {
        Ok(u64::from_le_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| TriggerError::CorruptRecord)?,
        ))
    }

    fn i64(&mut self) -> Result<i64, TriggerError> {
        Ok(i64::from_le_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| TriggerError::CorruptRecord)?,
        ))
    }

    fn array_16(&mut self) -> Result<[u8; 16], TriggerError> {
        self.take(16)?
            .try_into()
            .map_err(|_| TriggerError::CorruptRecord)
    }

    fn bytes(&mut self) -> Result<Vec<u8>, TriggerError> {
        let len = usize::try_from(self.u32()?).map_err(|_| TriggerError::CorruptRecord)?;
        Ok(self.take(len)?.to_vec())
    }

    fn finished(&self) -> bool {
        self.pos == self.bytes.len()
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
        std::env::temp_dir().join(format!("voodoo-store-trigger-{name}-{nonce}.vstore"))
    }

    #[test]
    fn trigger_survives_reopen_and_routes_to_job() {
        let path = temp_store_path("route");
        let trigger_id;
        {
            let mut store = Store::open(&path).unwrap();
            trigger_id = store
                .create_trigger(
                    b"invoice-created",
                    TriggerSource::Collection {
                        collection: b"invoices".to_vec(),
                        operation: b"insert".to_vec(),
                    },
                    JobSpec::new(b"send-invoice".to_vec(), b"template".to_vec()),
                )
                .unwrap();
        }
        {
            let mut store = Store::open(&path).unwrap();
            let job_id = store
                .fire_trigger(&trigger_id, b"invoice-42", 100)
                .unwrap()
                .unwrap();
            let job = store.get_job(&job_id).unwrap().unwrap();
            assert_eq!(job.handler, b"send-invoice");
            assert_eq!(job.payload, b"invoice-42");
            let trigger = store.get_trigger(&trigger_id).unwrap().unwrap();
            assert_eq!(trigger.fire_count, 1);
            assert_eq!(trigger.last_fired_at_ms, Some(100));
        }
        let _ = fs::remove_file(path);
    }

    #[test]
    fn disabled_trigger_does_not_create_work() {
        let path = temp_store_path("disabled");
        let mut store = Store::open(&path).unwrap();
        let id = store
            .create_trigger(
                b"manual",
                TriggerSource::Manual,
                JobSpec::new(b"handler".to_vec(), b"payload".to_vec()),
            )
            .unwrap();
        assert!(store.set_trigger_enabled(&id, false).unwrap());
        assert_eq!(store.fire_trigger(&id, b"event", 1).unwrap(), None);
        assert_eq!(store.get_trigger(&id).unwrap().unwrap().fire_count, 0);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn trigger_fire_can_share_application_transaction() {
        let path = temp_store_path("transactional");
        let mut store = Store::open(&path).unwrap();
        let id = store
            .create_trigger(
                b"manual",
                TriggerSource::Manual,
                JobSpec::new(b"handler".to_vec(), b"template".to_vec()),
            )
            .unwrap();
        let job_id;
        {
            let mut tx = store.begin().unwrap();
            tx.put(b"app:event", b"accepted").unwrap();
            job_id = tx.fire_trigger(&id, b"event", 10).unwrap().unwrap();
            tx.commit().unwrap();
        }
        assert_eq!(store.get(b"app:event"), Some(b"accepted".as_slice()));
        assert_eq!(store.get_job(&job_id).unwrap().unwrap().payload, b"event");
        let trigger = store.get_trigger(&id).unwrap().unwrap();
        assert_eq!(trigger.fire_count, 1);
        assert_eq!(trigger.last_fired_at_ms, Some(10));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn trigger_fire_rollback_hides_job_and_metadata_update() {
        let path = temp_store_path("rollback");
        let mut store = Store::open(&path).unwrap();
        let id = store
            .create_trigger(
                b"manual",
                TriggerSource::Manual,
                JobSpec::new(b"handler".to_vec(), b"template".to_vec()),
            )
            .unwrap();
        let job_id;
        {
            let mut tx = store.begin().unwrap();
            job_id = tx.fire_trigger(&id, b"event", 10).unwrap().unwrap();
            tx.rollback().unwrap();
        }
        assert!(store.get_job(&job_id).unwrap().is_none());
        let trigger = store.get_trigger(&id).unwrap().unwrap();
        assert_eq!(trigger.fire_count, 0);
        assert_eq!(trigger.last_fired_at_ms, None);
        let _ = fs::remove_file(path);
    }
}
