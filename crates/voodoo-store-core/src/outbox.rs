//! Transactional outbox events.
//!
//! An outbox event can be emitted from the same `Transaction` that mutates
//! application state and enqueues jobs. The Runtime can later dispatch these
//! durable events to external systems or Voodoo messaging and acknowledge them
//! only after the external side effect succeeds.

use thiserror::Error;

use crate::{EngineError, Store, Transaction};

const OUTBOX_PREFIX: &[u8] = b"\xffvds:outbox:event:";
const VERSION: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OutboxEventId {
    pub tx_id: u64,
    pub nonce: [u8; 16],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxEvent {
    pub id: OutboxEventId,
    pub topic: Vec<u8>,
    pub payload: Vec<u8>,
    pub created_at_ms: i64,
}

#[derive(Debug, Error)]
pub enum OutboxError {
    #[error("store error: {0}")]
    Store(#[from] EngineError),
    #[error("outbox topic cannot be empty")]
    EmptyTopic,
    #[error("outbox field is too large")]
    FieldTooLarge,
    #[error("outbox record is corrupt")]
    CorruptRecord,
    #[error("operating-system entropy is unavailable")]
    EntropyUnavailable,
}

impl Transaction<'_> {
    /// Emits an event that becomes visible if and only if this transaction
    /// commits.
    pub fn emit_event(
        &mut self,
        topic: impl AsRef<[u8]>,
        payload: impl AsRef<[u8]>,
        created_at_ms: i64,
    ) -> Result<OutboxEventId, OutboxError> {
        let topic = topic.as_ref();
        if topic.is_empty() {
            return Err(OutboxError::EmptyTopic);
        }
        let mut nonce = [0u8; 16];
        getrandom::fill(&mut nonce).map_err(|_| OutboxError::EntropyUnavailable)?;
        let id = OutboxEventId {
            tx_id: self.id(),
            nonce,
        };
        let event = OutboxEvent {
            id,
            topic: topic.to_vec(),
            payload: payload.as_ref().to_vec(),
            created_at_ms,
        };
        self.put_internal(event_key(id), encode_event(&event)?)?;
        Ok(id)
    }
}

impl Store {
    /// Emits one outbox event in its own durable transaction.
    ///
    /// Cross-domain callers should prefer `Transaction::emit_event` so the
    /// event can share a commit boundary with application state and work.
    pub fn emit_outbox_event(
        &mut self,
        topic: impl AsRef<[u8]>,
        payload: impl AsRef<[u8]>,
        created_at_ms: i64,
    ) -> Result<OutboxEventId, OutboxError> {
        let mut tx = self.begin()?;
        let id = tx.emit_event(topic, payload, created_at_ms)?;
        tx.commit()?;
        Ok(id)
    }

    /// Returns pending outbox events ordered by transaction id and event id.
    pub fn outbox_events_after(
        &self,
        after_tx_id: Option<u64>,
        limit: usize,
    ) -> Result<Vec<OutboxEvent>, OutboxError> {
        let mut events = Vec::new();
        for (_, encoded) in self.scan_prefix(OUTBOX_PREFIX) {
            let event = decode_event(&encoded)?;
            if after_tx_id.is_some_and(|after| event.id.tx_id <= after) {
                continue;
            }
            events.push(event);
            if events.len() == limit {
                break;
            }
        }
        Ok(events)
    }

    /// Acknowledges a dispatched event by deleting it durably.
    pub fn ack_outbox_event(&mut self, id: OutboxEventId) -> Result<bool, OutboxError> {
        let key = event_key(id);
        if !self.contains_key(&key) {
            return Ok(false);
        }
        self.delete_internal(key)?;
        Ok(true)
    }

    pub fn outbox_len(&self) -> usize {
        self.scan_prefix(OUTBOX_PREFIX).len()
    }
}

fn event_key(id: OutboxEventId) -> Vec<u8> {
    let mut key = Vec::with_capacity(OUTBOX_PREFIX.len() + 8 + 16);
    key.extend_from_slice(OUTBOX_PREFIX);
    key.extend_from_slice(&id.tx_id.to_be_bytes());
    key.extend_from_slice(&id.nonce);
    key
}

fn encode_event(event: &OutboxEvent) -> Result<Vec<u8>, OutboxError> {
    let mut out = Vec::new();
    out.push(VERSION);
    out.extend_from_slice(&event.id.tx_id.to_le_bytes());
    out.extend_from_slice(&event.id.nonce);
    out.extend_from_slice(&event.created_at_ms.to_le_bytes());
    write_bytes(&mut out, &event.topic)?;
    write_bytes(&mut out, &event.payload)?;
    Ok(out)
}

fn decode_event(bytes: &[u8]) -> Result<OutboxEvent, OutboxError> {
    let mut cursor = Cursor::new(bytes);
    if cursor.u8()? != VERSION {
        return Err(OutboxError::CorruptRecord);
    }
    let tx_id = cursor.u64()?;
    let nonce = cursor.array_16()?;
    let created_at_ms = cursor.i64()?;
    let topic = cursor.bytes()?;
    let payload = cursor.bytes()?;
    if topic.is_empty() || !cursor.finished() {
        return Err(OutboxError::CorruptRecord);
    }
    Ok(OutboxEvent {
        id: OutboxEventId { tx_id, nonce },
        topic,
        payload,
        created_at_ms,
    })
}

fn write_bytes(out: &mut Vec<u8>, value: &[u8]) -> Result<(), OutboxError> {
    let len = u32::try_from(value.len()).map_err(|_| OutboxError::FieldTooLarge)?;
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

    fn take(&mut self, len: usize) -> Result<&'a [u8], OutboxError> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or(OutboxError::CorruptRecord)?;
        let value = self
            .bytes
            .get(self.pos..end)
            .ok_or(OutboxError::CorruptRecord)?;
        self.pos = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, OutboxError> {
        Ok(*self.take(1)?.first().ok_or(OutboxError::CorruptRecord)?)
    }

    fn u32(&mut self) -> Result<u32, OutboxError> {
        Ok(u32::from_le_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| OutboxError::CorruptRecord)?,
        ))
    }

    fn u64(&mut self) -> Result<u64, OutboxError> {
        Ok(u64::from_le_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| OutboxError::CorruptRecord)?,
        ))
    }

    fn i64(&mut self) -> Result<i64, OutboxError> {
        Ok(i64::from_le_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| OutboxError::CorruptRecord)?,
        ))
    }

    fn array_16(&mut self) -> Result<[u8; 16], OutboxError> {
        self.take(16)?
            .try_into()
            .map_err(|_| OutboxError::CorruptRecord)
    }

    fn bytes(&mut self) -> Result<Vec<u8>, OutboxError> {
        let len = usize::try_from(self.u32()?).map_err(|_| OutboxError::CorruptRecord)?;
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

    use crate::{JobSpec, Store};

    fn temp_store_path(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("voodoo-store-outbox-{name}-{nonce}.vstore"))
    }

    #[test]
    fn state_job_and_event_share_one_commit() {
        let path = temp_store_path("commit");
        let event_id;
        {
            let mut store = Store::open(&path).unwrap();
            let mut tx = store.begin().unwrap();
            tx.put(b"invoice:42", b"paid").unwrap();
            tx.enqueue_job(JobSpec::new(b"receipt", b"42"), 10).unwrap();
            event_id = tx.emit_event(b"invoice.paid", b"42", 10).unwrap();
            tx.commit().unwrap();

            assert_eq!(store.get(b"invoice:42"), Some(b"paid".as_slice()));
            let events = store.outbox_events_after(None, 10).unwrap();
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].id, event_id);
            assert_eq!(events[0].topic, b"invoice.paid");
            assert_eq!(events[0].payload, b"42");
            assert!(store.ack_outbox_event(event_id).unwrap());
            assert_eq!(store.outbox_len(), 0);
        }
        let _ = fs::remove_file(path);
    }

    #[test]
    fn rollback_hides_outbox_event() {
        let path = temp_store_path("rollback");
        {
            let mut store = Store::open(&path).unwrap();
            let mut tx = store.begin().unwrap();
            tx.put(b"state", b"new").unwrap();
            tx.emit_event(b"state.changed", b"new", 20).unwrap();
            tx.rollback().unwrap();
            assert_eq!(store.get(b"state"), None);
            assert_eq!(store.outbox_len(), 0);
        }
        {
            let store = Store::open(&path).unwrap();
            assert_eq!(store.outbox_len(), 0);
        }
        let _ = fs::remove_file(path);
    }

    #[test]
    fn events_are_ordered_by_transaction() {
        let path = temp_store_path("order");
        let mut store = Store::open(&path).unwrap();
        let first = {
            let mut tx = store.begin().unwrap();
            let id = tx.emit_event(b"events", b"one", 1).unwrap();
            tx.commit().unwrap();
            id
        };
        let second = {
            let mut tx = store.begin().unwrap();
            let id = tx.emit_event(b"events", b"two", 2).unwrap();
            tx.commit().unwrap();
            id
        };
        let events = store.outbox_events_after(Some(first.tx_id), 10).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].id, second);
        let _ = fs::remove_file(path);
    }
}
