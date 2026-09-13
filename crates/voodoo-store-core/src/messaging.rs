//! Durable append-only streams, topics, and subscription cursors.
//!
//! Topics are streams with named durable cursors. The storage core owns
//! ordering, replay, and cursor durability; networking and remote fan-out remain
//! responsibilities of Voodoo Protocol / Runtime.

use thiserror::Error;

use crate::{EngineError, Store};

const STREAM_PREFIX: &[u8] = b"\xffvds:stream:";
const SUB_PREFIX: &[u8] = b"\xffvds:sub:";
const ENTRY_VERSION: u8 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamEntry {
    pub offset: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubscriptionState {
    pub next_offset: u64,
}

pub struct Stream<'a> {
    store: &'a mut Store,
    name: Vec<u8>,
    next_key: Vec<u8>,
    entry_prefix: Vec<u8>,
}

pub struct Topic<'a> {
    stream: Stream<'a>,
}

impl Store {
    pub fn stream(&mut self, name: impl AsRef<[u8]>) -> Result<Stream<'_>, MessagingError> {
        Stream::new(self, name.as_ref())
    }

    pub fn topic(&mut self, name: impl AsRef<[u8]>) -> Result<Topic<'_>, MessagingError> {
        Ok(Topic {
            stream: Stream::new(self, name.as_ref())?,
        })
    }
}

impl<'a> Stream<'a> {
    fn new(store: &'a mut Store, name: &[u8]) -> Result<Self, MessagingError> {
        validate_name(name)?;
        let namespace = tuple_key(STREAM_PREFIX, &[name])?;
        let mut next_key = namespace.clone();
        next_key.extend_from_slice(b":next");
        let mut entry_prefix = namespace;
        entry_prefix.extend_from_slice(b":e:");
        Ok(Self {
            store,
            name: name.to_vec(),
            next_key,
            entry_prefix,
        })
    }

    pub fn name(&self) -> &[u8] {
        &self.name
    }

    pub fn append(&mut self, payload: impl AsRef<[u8]>) -> Result<u64, MessagingError> {
        let offset = self.next_offset()?;
        let next = offset
            .checked_add(1)
            .ok_or(MessagingError::OffsetExhausted)?;
        let key = self.entry_key(offset);
        let value = encode_entry(payload.as_ref())?;
        let mut tx = self.store.begin()?;
        tx.put_internal(&self.next_key, next.to_le_bytes())?;
        tx.put_internal(key, value)?;
        tx.commit()?;
        Ok(offset)
    }

    pub fn read_from(&self, offset: u64, limit: usize) -> Result<Vec<StreamEntry>, MessagingError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut entries = Vec::new();
        for (key, value) in self.store.scan_prefix(&self.entry_prefix) {
            let entry_offset = decode_entry_offset(&self.entry_prefix, &key)?;
            if entry_offset < offset {
                continue;
            }
            entries.push(StreamEntry {
                offset: entry_offset,
                payload: decode_entry(&value)?.to_vec(),
            });
            if entries.len() == limit {
                break;
            }
        }
        entries.sort_unstable_by_key(|entry| entry.offset);
        Ok(entries)
    }

    pub fn tail_offset(&self) -> Result<Option<u64>, MessagingError> {
        let next = self.next_offset()?;
        Ok(next.checked_sub(1))
    }

    fn next_offset(&self) -> Result<u64, MessagingError> {
        match self.store.get(&self.next_key) {
            None => Ok(0),
            Some(bytes) => {
                let encoded: [u8; 8] = bytes
                    .try_into()
                    .map_err(|_| MessagingError::CorruptMetadata)?;
                Ok(u64::from_le_bytes(encoded))
            }
        }
    }

    fn entry_key(&self, offset: u64) -> Vec<u8> {
        let mut key = self.entry_prefix.clone();
        key.extend_from_slice(&offset.to_be_bytes());
        key
    }
}

impl Topic<'_> {
    pub fn name(&self) -> &[u8] {
        self.stream.name()
    }

    pub fn publish(&mut self, payload: impl AsRef<[u8]>) -> Result<u64, MessagingError> {
        self.stream.append(payload)
    }

    pub fn read_from(&self, offset: u64, limit: usize) -> Result<Vec<StreamEntry>, MessagingError> {
        self.stream.read_from(offset, limit)
    }

    pub fn subscription_state(
        &self,
        subscription: impl AsRef<[u8]>,
    ) -> Result<SubscriptionState, MessagingError> {
        let key = subscription_key(self.name(), subscription.as_ref())?;
        match self.stream.store.get(key) {
            None => Ok(SubscriptionState { next_offset: 0 }),
            Some(bytes) => {
                let encoded: [u8; 8] = bytes
                    .try_into()
                    .map_err(|_| MessagingError::CorruptMetadata)?;
                Ok(SubscriptionState {
                    next_offset: u64::from_le_bytes(encoded),
                })
            }
        }
    }

    pub fn poll(
        &self,
        subscription: impl AsRef<[u8]>,
        limit: usize,
    ) -> Result<Vec<StreamEntry>, MessagingError> {
        let state = self.subscription_state(subscription)?;
        self.stream.read_from(state.next_offset, limit)
    }

    /// Advances a durable subscription cursor. Cursors cannot move backwards.
    pub fn acknowledge_through(
        &mut self,
        subscription: impl AsRef<[u8]>,
        offset: u64,
    ) -> Result<SubscriptionState, MessagingError> {
        let subscription = subscription.as_ref();
        validate_name(subscription)?;
        let current = self.subscription_state(subscription)?;
        let next_offset = offset
            .checked_add(1)
            .ok_or(MessagingError::OffsetExhausted)?;
        if next_offset < current.next_offset {
            return Err(MessagingError::CursorRegression {
                current: current.next_offset,
                requested: next_offset,
            });
        }
        let key = subscription_key(self.name(), subscription)?;
        self.stream
            .store
            .put_internal(key, next_offset.to_le_bytes())?;
        Ok(SubscriptionState { next_offset })
    }

    /// Explicit replay/reset operation. Unlike acknowledgement, this may move
    /// the cursor backwards and is intended for administrative tooling.
    pub fn reset_subscription(
        &mut self,
        subscription: impl AsRef<[u8]>,
        next_offset: u64,
    ) -> Result<(), MessagingError> {
        let subscription = subscription.as_ref();
        validate_name(subscription)?;
        let key = subscription_key(self.name(), subscription)?;
        self.stream
            .store
            .put_internal(key, next_offset.to_le_bytes())?;
        Ok(())
    }
}

fn subscription_key(topic: &[u8], subscription: &[u8]) -> Result<Vec<u8>, MessagingError> {
    validate_name(subscription)?;
    tuple_key(SUB_PREFIX, &[topic, subscription])
}

fn tuple_key(prefix: &[u8], components: &[&[u8]]) -> Result<Vec<u8>, MessagingError> {
    let mut key = Vec::from(prefix);
    for component in components {
        let len = u32::try_from(component.len()).map_err(|_| MessagingError::NameTooLong)?;
        key.extend_from_slice(&len.to_be_bytes());
        key.extend_from_slice(component);
    }
    Ok(key)
}

fn encode_entry(payload: &[u8]) -> Result<Vec<u8>, MessagingError> {
    let len = u32::try_from(payload.len()).map_err(|_| MessagingError::PayloadTooLarge)?;
    let mut encoded = Vec::with_capacity(5 + payload.len());
    encoded.push(ENTRY_VERSION);
    encoded.extend_from_slice(&len.to_le_bytes());
    encoded.extend_from_slice(payload);
    Ok(encoded)
}

fn decode_entry(encoded: &[u8]) -> Result<&[u8], MessagingError> {
    if encoded.len() < 5 || encoded[0] != ENTRY_VERSION {
        return Err(MessagingError::CorruptEntry);
    }
    let len = u32::from_le_bytes(encoded[1..5].try_into().expect("stream payload length")) as usize;
    if encoded.len() != 5 + len {
        return Err(MessagingError::CorruptEntry);
    }
    Ok(&encoded[5..])
}

fn decode_entry_offset(prefix: &[u8], key: &[u8]) -> Result<u64, MessagingError> {
    if !key.starts_with(prefix) || key.len() != prefix.len() + 8 {
        return Err(MessagingError::CorruptMetadata);
    }
    Ok(u64::from_be_bytes(
        key[prefix.len()..]
            .try_into()
            .expect("8-byte stream offset"),
    ))
}

fn validate_name(name: &[u8]) -> Result<(), MessagingError> {
    if name.is_empty() {
        Err(MessagingError::EmptyName)
    } else {
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum MessagingError {
    #[error("store error: {0}")]
    Store(#[from] EngineError),
    #[error("stream/topic/subscription name must not be empty")]
    EmptyName,
    #[error("stream/topic/subscription name is too long")]
    NameTooLong,
    #[error("stream payload is too large")]
    PayloadTooLarge,
    #[error("stream offset exhausted")]
    OffsetExhausted,
    #[error("stream metadata is corrupt")]
    CorruptMetadata,
    #[error("stream entry is corrupt or unsupported")]
    CorruptEntry,
    #[error("subscription cursor cannot regress from {current} to {requested}")]
    CursorRegression { current: u64, requested: u64 },
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
        std::env::temp_dir().join(format!("voodoo-store-messaging-{name}-{nonce}.vstore"))
    }

    #[test]
    fn stream_offsets_are_durable_and_replayable() {
        let path = temp_store_path("stream");
        {
            let mut store = Store::open(&path).unwrap();
            let mut stream = store.stream(b"events").unwrap();
            assert_eq!(stream.append(b"a").unwrap(), 0);
            assert_eq!(stream.append(b"b").unwrap(), 1);
            assert_eq!(stream.append(b"c").unwrap(), 2);
        }
        {
            let mut store = Store::open(&path).unwrap();
            let stream = store.stream(b"events").unwrap();
            let entries = stream.read_from(1, 10).unwrap();
            assert_eq!(entries.len(), 2);
            assert_eq!(entries[0].offset, 1);
            assert_eq!(entries[0].payload, b"b");
            assert_eq!(entries[1].payload, b"c");
        }
        let _ = fs::remove_file(path);
    }

    #[test]
    fn topic_subscription_cursor_is_durable_and_supports_replay_reset() {
        let path = temp_store_path("topic");
        {
            let mut store = Store::open(&path).unwrap();
            let mut topic = store.topic(b"orders").unwrap();
            topic.publish(b"one").unwrap();
            topic.publish(b"two").unwrap();
            let entries = topic.poll(b"billing", 10).unwrap();
            assert_eq!(entries.len(), 2);
            topic
                .acknowledge_through(b"billing", entries[0].offset)
                .unwrap();
            let remaining = topic.poll(b"billing", 10).unwrap();
            assert_eq!(remaining.len(), 1);
            assert_eq!(remaining[0].payload, b"two");
        }
        {
            let mut store = Store::open(&path).unwrap();
            let mut topic = store.topic(b"orders").unwrap();
            let remaining = topic.poll(b"billing", 10).unwrap();
            assert_eq!(remaining.len(), 1);
            topic.reset_subscription(b"billing", 0).unwrap();
            assert_eq!(topic.poll(b"billing", 10).unwrap().len(), 2);
        }
        let _ = fs::remove_file(path);
    }

    #[test]
    fn acknowledgement_cannot_move_cursor_backwards() {
        let path = temp_store_path("cursor");
        let mut store = Store::open(&path).unwrap();
        let mut topic = store.topic(b"events").unwrap();
        topic.publish(b"0").unwrap();
        topic.publish(b"1").unwrap();
        topic.acknowledge_through(b"worker", 1).unwrap();
        assert!(matches!(
            topic.acknowledge_through(b"worker", 0),
            Err(MessagingError::CursorRegression { .. })
        ));
        drop(topic);
        drop(store);
        let _ = fs::remove_file(path);
    }
}
