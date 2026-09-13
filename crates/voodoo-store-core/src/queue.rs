//! Durable queue semantics built directly on the transactional Store core.
//!
//! Queue state is represented with language-neutral byte records and updated
//! through normal Store transactions. A lease generation prevents a stale
//! worker from acknowledging a message after another worker reclaimed it.

use thiserror::Error;

use crate::{EngineError, Store};

const QUEUE_PREFIX: &[u8] = b"\xffvds:q:";
const MESSAGE_VERSION: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum QueueState {
    Ready = 0,
    Leased = 1,
    Dead = 2,
}

impl TryFrom<u8> for QueueState {
    type Error = QueueError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Ready),
            1 => Ok(Self::Leased),
            2 => Ok(Self::Dead),
            _ => Err(QueueError::InvalidEncoding),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PushOptions {
    pub available_at_ms: i64,
    pub priority: i32,
}

impl Default for PushOptions {
    fn default() -> Self {
        Self {
            available_at_ms: 0,
            priority: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueMessage {
    pub id: u64,
    pub payload: Vec<u8>,
    pub attempts: u32,
    pub priority: i32,
    pub available_at_ms: i64,
    pub lease_until_ms: i64,
    pub lease_generation: u32,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueueStats {
    pub ready: u64,
    pub leased: u64,
    pub dead: u64,
    pub total: u64,
}

pub struct Queue<'a> {
    store: &'a mut Store,
    name: Vec<u8>,
    meta_key: Vec<u8>,
    message_prefix: Vec<u8>,
}

impl Store {
    pub fn queue(&mut self, name: impl AsRef<[u8]>) -> Result<Queue<'_>, QueueError> {
        Queue::new(self, name.as_ref())
    }
}

impl<'a> Queue<'a> {
    fn new(store: &'a mut Store, name: &[u8]) -> Result<Self, QueueError> {
        if name.is_empty() {
            return Err(QueueError::EmptyName);
        }
        let name_len = u32::try_from(name.len()).map_err(|_| QueueError::NameTooLong)?;
        let mut namespace = Vec::with_capacity(QUEUE_PREFIX.len() + 4 + name.len());
        namespace.extend_from_slice(QUEUE_PREFIX);
        namespace.extend_from_slice(&name_len.to_be_bytes());
        namespace.extend_from_slice(name);

        let mut meta_key = namespace.clone();
        meta_key.extend_from_slice(b":next");

        let mut message_prefix = namespace;
        message_prefix.extend_from_slice(b":m:");

        Ok(Self {
            store,
            name: name.to_vec(),
            meta_key,
            message_prefix,
        })
    }

    pub fn name(&self) -> &[u8] {
        &self.name
    }

    pub fn push(&mut self, payload: impl AsRef<[u8]>) -> Result<u64, QueueError> {
        self.push_with_options(payload, PushOptions::default())
    }

    pub fn push_at(
        &mut self,
        payload: impl AsRef<[u8]>,
        available_at_ms: i64,
    ) -> Result<u64, QueueError> {
        self.push_with_options(
            payload,
            PushOptions {
                available_at_ms,
                priority: 0,
            },
        )
    }

    pub fn push_with_options(
        &mut self,
        payload: impl AsRef<[u8]>,
        options: PushOptions,
    ) -> Result<u64, QueueError> {
        let id = self.next_id()?;
        let next_id = id.checked_add(1).ok_or(QueueError::IdExhausted)?;
        let message = StoredMessage {
            state: QueueState::Ready,
            id,
            payload: payload.as_ref().to_vec(),
            attempts: 0,
            priority: options.priority,
            available_at_ms: options.available_at_ms,
            lease_until_ms: 0,
        };

        let key = self.message_key(id);
        let mut tx = self.store.begin()?;
        tx.put(&self.meta_key, next_id.to_le_bytes())?;
        tx.put(key, encode_message(&message)?)?;
        tx.commit()?;
        Ok(id)
    }

    pub fn claim(
        &mut self,
        now_ms: i64,
        lease_duration_ms: u64,
    ) -> Result<Option<QueueMessage>, QueueError> {
        let lease_duration =
            i64::try_from(lease_duration_ms).map_err(|_| QueueError::TimeOverflow)?;
        let lease_until_ms = now_ms
            .checked_add(lease_duration)
            .ok_or(QueueError::TimeOverflow)?;

        let mut candidate: Option<(Vec<u8>, StoredMessage)> = None;
        for (key, value) in self.store.scan_prefix(&self.message_prefix) {
            let message = decode_message(&value)?;
            let eligible = match message.state {
                QueueState::Ready => message.available_at_ms <= now_ms,
                QueueState::Leased => message.lease_until_ms <= now_ms,
                QueueState::Dead => false,
            };
            if !eligible {
                continue;
            }

            let replace = candidate.as_ref().is_none_or(|(_, current)| {
                message.priority > current.priority
                    || (message.priority == current.priority && message.id < current.id)
            });
            if replace {
                candidate = Some((key, message));
            }
        }

        let Some((key, mut message)) = candidate else {
            return Ok(None);
        };

        message.attempts = message
            .attempts
            .checked_add(1)
            .ok_or(QueueError::AttemptExhausted)?;
        message.state = QueueState::Leased;
        message.lease_until_ms = lease_until_ms;

        let encoded = encode_message(&message)?;
        self.store.put(key, encoded)?;

        Ok(Some(QueueMessage {
            id: message.id,
            payload: message.payload,
            attempts: message.attempts,
            priority: message.priority,
            available_at_ms: message.available_at_ms,
            lease_until_ms: message.lease_until_ms,
            lease_generation: message.attempts,
        }))
    }

    pub fn ack(&mut self, id: u64, lease_generation: u32) -> Result<(), QueueError> {
        let key = self.message_key(id);
        let message = self.load_message(&key)?.ok_or(QueueError::NotFound(id))?;
        validate_lease(&message, lease_generation)?;
        self.store.delete(key)?;
        Ok(())
    }

    pub fn nack(
        &mut self,
        id: u64,
        lease_generation: u32,
        available_at_ms: i64,
    ) -> Result<(), QueueError> {
        let key = self.message_key(id);
        let mut message = self.load_message(&key)?.ok_or(QueueError::NotFound(id))?;
        validate_lease(&message, lease_generation)?;
        message.state = QueueState::Ready;
        message.available_at_ms = available_at_ms;
        message.lease_until_ms = 0;
        self.store.put(key, encode_message(&message)?)?;
        Ok(())
    }

    pub fn dead_letter(&mut self, id: u64, lease_generation: u32) -> Result<(), QueueError> {
        let key = self.message_key(id);
        let mut message = self.load_message(&key)?.ok_or(QueueError::NotFound(id))?;
        validate_lease(&message, lease_generation)?;
        message.state = QueueState::Dead;
        message.lease_until_ms = 0;
        self.store.put(key, encode_message(&message)?)?;
        Ok(())
    }

    pub fn stats(&self) -> Result<QueueStats, QueueError> {
        let mut stats = QueueStats::default();
        for (_, value) in self.store.scan_prefix(&self.message_prefix) {
            let message = decode_message(&value)?;
            stats.total = stats
                .total
                .checked_add(1)
                .ok_or(QueueError::CountOverflow)?;
            match message.state {
                QueueState::Ready => {
                    stats.ready = stats
                        .ready
                        .checked_add(1)
                        .ok_or(QueueError::CountOverflow)?;
                }
                QueueState::Leased => {
                    stats.leased = stats
                        .leased
                        .checked_add(1)
                        .ok_or(QueueError::CountOverflow)?;
                }
                QueueState::Dead => {
                    stats.dead = stats.dead.checked_add(1).ok_or(QueueError::CountOverflow)?;
                }
            }
        }
        Ok(stats)
    }

    pub fn purge_dead(&mut self) -> Result<u64, QueueError> {
        let mut keys = Vec::new();
        for (key, value) in self.store.scan_prefix(&self.message_prefix) {
            if decode_message(&value)?.state == QueueState::Dead {
                keys.push(key);
            }
        }

        if keys.is_empty() {
            return Ok(0);
        }

        let count = u64::try_from(keys.len()).map_err(|_| QueueError::CountOverflow)?;
        let mut tx = self.store.begin()?;
        for key in keys {
            tx.delete(key)?;
        }
        tx.commit()?;
        Ok(count)
    }

    fn next_id(&self) -> Result<u64, QueueError> {
        let Some(value) = self.store.get(&self.meta_key) else {
            return Ok(1);
        };
        if value.len() != 8 {
            return Err(QueueError::InvalidEncoding);
        }
        Ok(u64::from_le_bytes(
            value.try_into().map_err(|_| QueueError::InvalidEncoding)?,
        ))
    }

    fn load_message(&self, key: &[u8]) -> Result<Option<StoredMessage>, QueueError> {
        self.store.get(key).map(decode_message).transpose()
    }

    fn message_key(&self, id: u64) -> Vec<u8> {
        let mut key = self.message_prefix.clone();
        key.extend_from_slice(&id.to_be_bytes());
        key
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StoredMessage {
    state: QueueState,
    id: u64,
    payload: Vec<u8>,
    attempts: u32,
    priority: i32,
    available_at_ms: i64,
    lease_until_ms: i64,
}

fn validate_lease(message: &StoredMessage, lease_generation: u32) -> Result<(), QueueError> {
    if message.state != QueueState::Leased {
        return Err(QueueError::NotLeased(message.id));
    }
    if message.attempts != lease_generation {
        return Err(QueueError::LeaseMismatch {
            id: message.id,
            expected: message.attempts,
            provided: lease_generation,
        });
    }
    Ok(())
}

fn encode_message(message: &StoredMessage) -> Result<Vec<u8>, QueueError> {
    let payload_len =
        u32::try_from(message.payload.len()).map_err(|_| QueueError::PayloadTooLarge)?;
    let mut out = Vec::with_capacity(1 + 1 + 8 + 4 + 4 + 8 + 8 + 4 + message.payload.len());
    out.push(MESSAGE_VERSION);
    out.push(message.state as u8);
    out.extend_from_slice(&message.id.to_le_bytes());
    out.extend_from_slice(&message.attempts.to_le_bytes());
    out.extend_from_slice(&message.priority.to_le_bytes());
    out.extend_from_slice(&message.available_at_ms.to_le_bytes());
    out.extend_from_slice(&message.lease_until_ms.to_le_bytes());
    out.extend_from_slice(&payload_len.to_le_bytes());
    out.extend_from_slice(&message.payload);
    Ok(out)
}

fn decode_message(bytes: &[u8]) -> Result<StoredMessage, QueueError> {
    const FIXED: usize = 1 + 1 + 8 + 4 + 4 + 8 + 8 + 4;
    if bytes.len() < FIXED || bytes[0] != MESSAGE_VERSION {
        return Err(QueueError::InvalidEncoding);
    }

    let state = QueueState::try_from(bytes[1])?;
    let id = u64::from_le_bytes(
        bytes[2..10]
            .try_into()
            .map_err(|_| QueueError::InvalidEncoding)?,
    );
    let attempts = u32::from_le_bytes(
        bytes[10..14]
            .try_into()
            .map_err(|_| QueueError::InvalidEncoding)?,
    );
    let priority = i32::from_le_bytes(
        bytes[14..18]
            .try_into()
            .map_err(|_| QueueError::InvalidEncoding)?,
    );
    let available_at_ms = i64::from_le_bytes(
        bytes[18..26]
            .try_into()
            .map_err(|_| QueueError::InvalidEncoding)?,
    );
    let lease_until_ms = i64::from_le_bytes(
        bytes[26..34]
            .try_into()
            .map_err(|_| QueueError::InvalidEncoding)?,
    );
    let payload_len = u32::from_le_bytes(
        bytes[34..38]
            .try_into()
            .map_err(|_| QueueError::InvalidEncoding)?,
    ) as usize;
    if bytes.len() != FIXED + payload_len {
        return Err(QueueError::InvalidEncoding);
    }

    Ok(StoredMessage {
        state,
        id,
        payload: bytes[FIXED..].to_vec(),
        attempts,
        priority,
        available_at_ms,
        lease_until_ms,
    })
}

#[derive(Debug, Error)]
pub enum QueueError {
    #[error("store error: {0}")]
    Store(#[from] EngineError),
    #[error("queue name cannot be empty")]
    EmptyName,
    #[error("queue name is too long")]
    NameTooLong,
    #[error("queue payload is too large")]
    PayloadTooLarge,
    #[error("queue record has invalid encoding")]
    InvalidEncoding,
    #[error("queue message id space is exhausted")]
    IdExhausted,
    #[error("queue delivery attempt counter is exhausted")]
    AttemptExhausted,
    #[error("queue time value overflowed")]
    TimeOverflow,
    #[error("queue count overflowed")]
    CountOverflow,
    #[error("queue message {0} was not found")]
    NotFound(u64),
    #[error("queue message {0} is not currently leased")]
    NotLeased(u64),
    #[error("lease generation mismatch for message {id}: expected {expected}, provided {provided}")]
    LeaseMismatch {
        id: u64,
        expected: u32,
        provided: u32,
    },
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
        std::env::temp_dir().join(format!("voodoo-queue-{name}-{nonce}.vstore"))
    }

    #[test]
    fn queue_survives_reopen() {
        let path = temp_store_path("reopen");
        {
            let mut store = Store::open(&path).unwrap();
            let mut queue = store.queue(b"emails").unwrap();
            queue.push(b"hello").unwrap();
        }
        {
            let mut store = Store::open(&path).unwrap();
            let mut queue = store.queue(b"emails").unwrap();
            let message = queue.claim(100, 1_000).unwrap().unwrap();
            assert_eq!(message.payload, b"hello");
            queue.ack(message.id, message.lease_generation).unwrap();
            assert_eq!(queue.stats().unwrap().total, 0);
        }
        let _ = fs::remove_file(path);
    }

    #[test]
    fn delayed_and_priority_delivery_work() {
        let path = temp_store_path("priority");
        let mut store = Store::open(&path).unwrap();
        let mut queue = store.queue(b"jobs").unwrap();
        queue.push(b"normal").unwrap();
        queue
            .push_with_options(
                b"urgent",
                PushOptions {
                    available_at_ms: 0,
                    priority: 10,
                },
            )
            .unwrap();
        queue.push_at(b"later", 5_000).unwrap();

        let first = queue.claim(100, 1_000).unwrap().unwrap();
        assert_eq!(first.payload, b"urgent");
        queue.ack(first.id, first.lease_generation).unwrap();

        let second = queue.claim(100, 1_000).unwrap().unwrap();
        assert_eq!(second.payload, b"normal");
        queue.ack(second.id, second.lease_generation).unwrap();
        assert!(queue.claim(100, 1_000).unwrap().is_none());
        assert!(queue.claim(5_000, 1_000).unwrap().is_some());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn expired_lease_can_be_reclaimed_and_stale_ack_is_rejected() {
        let path = temp_store_path("lease");
        let mut store = Store::open(&path).unwrap();
        let mut queue = store.queue(b"jobs").unwrap();
        queue.push(b"work").unwrap();

        let first = queue.claim(0, 100).unwrap().unwrap();
        let second = queue.claim(101, 100).unwrap().unwrap();
        assert_eq!(first.id, second.id);
        assert!(matches!(
            queue.ack(first.id, first.lease_generation),
            Err(QueueError::LeaseMismatch { .. })
        ));
        queue.ack(second.id, second.lease_generation).unwrap();
        let _ = fs::remove_file(path);
    }

    #[test]
    fn nack_and_dead_letter_are_durable() {
        let path = temp_store_path("retry");
        {
            let mut store = Store::open(&path).unwrap();
            let mut queue = store.queue(b"jobs").unwrap();
            queue.push(b"work").unwrap();
            let first = queue.claim(0, 10).unwrap().unwrap();
            queue.nack(first.id, first.lease_generation, 50).unwrap();
            assert!(queue.claim(49, 10).unwrap().is_none());
            let second = queue.claim(50, 10).unwrap().unwrap();
            queue
                .dead_letter(second.id, second.lease_generation)
                .unwrap();
            assert_eq!(queue.stats().unwrap().dead, 1);
        }
        {
            let mut store = Store::open(&path).unwrap();
            let mut queue = store.queue(b"jobs").unwrap();
            assert_eq!(queue.stats().unwrap().dead, 1);
            assert_eq!(queue.purge_dead().unwrap(), 1);
        }
        let _ = fs::remove_file(path);
    }
}
