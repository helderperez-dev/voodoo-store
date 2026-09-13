//! Durable consumer groups for Store streams.
//!
//! The initial model is intentionally single-partition per stream/group: one
//! offset may be leased at a time. Lease generations reject stale ACK/NACK and
//! expired deliveries are redelivered without advancing the durable cursor.

use thiserror::Error;

use crate::{EngineError, MessagingError, Store, StreamEntry};

const GROUP_PREFIX: &[u8] = b"\xffvds:cg:state:";
const VERSION: u8 = 1;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConsumerGroupState {
    pub next_offset: u64,
    pub leased_offset: Option<u64>,
    pub lease_until_ms: i64,
    pub lease_generation: u64,
    pub owner: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupDelivery {
    pub entry: StreamEntry,
    pub lease_generation: u64,
    pub lease_until_ms: i64,
}

#[derive(Debug, Error)]
pub enum ConsumerGroupError {
    #[error("store error: {0}")]
    Store(#[from] EngineError),
    #[error("messaging error: {0}")]
    Messaging(#[from] MessagingError),
    #[error("stream/group/consumer names cannot be empty")]
    EmptyName,
    #[error("consumer-group record is corrupt")]
    CorruptRecord,
    #[error("lease generation overflow")]
    LeaseGenerationOverflow,
    #[error("time value overflow")]
    TimeOverflow,
    #[error("group has no active lease")]
    NotLeased,
    #[error("lease owner mismatch")]
    OwnerMismatch,
    #[error("lease generation mismatch: expected {expected}, provided {provided}")]
    LeaseMismatch { expected: u64, provided: u64 },
    #[error("leased offset mismatch: expected {expected}, provided {provided}")]
    OffsetMismatch { expected: u64, provided: u64 },
}

impl Store {
    pub fn consumer_group_state(
        &self,
        stream: impl AsRef<[u8]>,
        group: impl AsRef<[u8]>,
    ) -> Result<ConsumerGroupState, ConsumerGroupError> {
        let stream = stream.as_ref();
        let group = group.as_ref();
        validate_names(stream, group, b"reader")?;
        self.get(group_key(stream, group)?)
            .map(decode_state)
            .transpose()
            .map(|state| state.unwrap_or_default())
    }

    pub fn consumer_group_claim(
        &mut self,
        stream_name: impl AsRef<[u8]>,
        group: impl AsRef<[u8]>,
        consumer: impl AsRef<[u8]>,
        now_ms: i64,
        lease_duration_ms: u64,
    ) -> Result<Option<ConsumerGroupDelivery>, ConsumerGroupError> {
        let stream_name = stream_name.as_ref();
        let group = group.as_ref();
        let consumer = consumer.as_ref();
        validate_names(stream_name, group, consumer)?;

        let key = group_key(stream_name, group)?;
        let mut state = self
            .get(&key)
            .map(decode_state)
            .transpose()?
            .unwrap_or_default();

        if state.leased_offset.is_some() && state.lease_until_ms > now_ms {
            return Ok(None);
        }

        let entry = {
            let stream = self.stream(stream_name)?;
            stream.read_from(state.next_offset, 1)?.into_iter().next()
        };
        let Some(entry) = entry else {
            if state.leased_offset.is_some() {
                state.leased_offset = None;
                state.owner.clear();
                state.lease_until_ms = 0;
                self.put_internal(key, encode_state(&state)?)?;
            }
            return Ok(None);
        };

        let lease_duration =
            i64::try_from(lease_duration_ms).map_err(|_| ConsumerGroupError::TimeOverflow)?;
        let lease_until_ms = now_ms
            .checked_add(lease_duration)
            .ok_or(ConsumerGroupError::TimeOverflow)?;
        state.lease_generation = state
            .lease_generation
            .checked_add(1)
            .ok_or(ConsumerGroupError::LeaseGenerationOverflow)?;
        state.leased_offset = Some(entry.offset);
        state.lease_until_ms = lease_until_ms;
        state.owner = consumer.to_vec();
        let generation = state.lease_generation;
        self.put_internal(key, encode_state(&state)?)?;

        Ok(Some(ConsumerGroupDelivery {
            entry,
            lease_generation: generation,
            lease_until_ms,
        }))
    }

    pub fn consumer_group_ack(
        &mut self,
        stream: impl AsRef<[u8]>,
        group: impl AsRef<[u8]>,
        consumer: impl AsRef<[u8]>,
        offset: u64,
        lease_generation: u64,
    ) -> Result<(), ConsumerGroupError> {
        let stream = stream.as_ref();
        let group = group.as_ref();
        let consumer = consumer.as_ref();
        validate_names(stream, group, consumer)?;
        let key = group_key(stream, group)?;
        let mut state = self
            .get(&key)
            .map(decode_state)
            .transpose()?
            .ok_or(ConsumerGroupError::NotLeased)?;
        validate_lease(&state, consumer, offset, lease_generation)?;
        state.next_offset = offset
            .checked_add(1)
            .ok_or(ConsumerGroupError::TimeOverflow)?;
        clear_lease(&mut state);
        self.put_internal(key, encode_state(&state)?)?;
        Ok(())
    }

    pub fn consumer_group_nack(
        &mut self,
        stream: impl AsRef<[u8]>,
        group: impl AsRef<[u8]>,
        consumer: impl AsRef<[u8]>,
        offset: u64,
        lease_generation: u64,
    ) -> Result<(), ConsumerGroupError> {
        let stream = stream.as_ref();
        let group = group.as_ref();
        let consumer = consumer.as_ref();
        validate_names(stream, group, consumer)?;
        let key = group_key(stream, group)?;
        let mut state = self
            .get(&key)
            .map(decode_state)
            .transpose()?
            .ok_or(ConsumerGroupError::NotLeased)?;
        validate_lease(&state, consumer, offset, lease_generation)?;
        clear_lease(&mut state);
        self.put_internal(key, encode_state(&state)?)?;
        Ok(())
    }

    pub fn consumer_group_reset(
        &mut self,
        stream: impl AsRef<[u8]>,
        group: impl AsRef<[u8]>,
        next_offset: u64,
    ) -> Result<(), ConsumerGroupError> {
        let stream = stream.as_ref();
        let group = group.as_ref();
        validate_names(stream, group, b"reset")?;
        let key = group_key(stream, group)?;
        let mut state = self
            .get(&key)
            .map(decode_state)
            .transpose()?
            .unwrap_or_default();
        state.next_offset = next_offset;
        clear_lease(&mut state);
        self.put_internal(key, encode_state(&state)?)?;
        Ok(())
    }
}

fn validate_names(stream: &[u8], group: &[u8], consumer: &[u8]) -> Result<(), ConsumerGroupError> {
    if stream.is_empty() || group.is_empty() || consumer.is_empty() {
        return Err(ConsumerGroupError::EmptyName);
    }
    Ok(())
}

fn clear_lease(state: &mut ConsumerGroupState) {
    state.leased_offset = None;
    state.lease_until_ms = 0;
    state.owner.clear();
}

fn validate_lease(
    state: &ConsumerGroupState,
    consumer: &[u8],
    offset: u64,
    generation: u64,
) -> Result<(), ConsumerGroupError> {
    let leased = state.leased_offset.ok_or(ConsumerGroupError::NotLeased)?;
    if state.owner != consumer {
        return Err(ConsumerGroupError::OwnerMismatch);
    }
    if state.lease_generation != generation {
        return Err(ConsumerGroupError::LeaseMismatch {
            expected: state.lease_generation,
            provided: generation,
        });
    }
    if leased != offset {
        return Err(ConsumerGroupError::OffsetMismatch {
            expected: leased,
            provided: offset,
        });
    }
    Ok(())
}

fn group_key(stream: &[u8], group: &[u8]) -> Result<Vec<u8>, ConsumerGroupError> {
    let stream_len = u32::try_from(stream.len()).map_err(|_| ConsumerGroupError::CorruptRecord)?;
    let group_len = u32::try_from(group.len()).map_err(|_| ConsumerGroupError::CorruptRecord)?;
    let mut key = Vec::with_capacity(GROUP_PREFIX.len() + 8 + stream.len() + group.len());
    key.extend_from_slice(GROUP_PREFIX);
    key.extend_from_slice(&stream_len.to_be_bytes());
    key.extend_from_slice(stream);
    key.extend_from_slice(&group_len.to_be_bytes());
    key.extend_from_slice(group);
    Ok(key)
}

fn encode_state(state: &ConsumerGroupState) -> Result<Vec<u8>, ConsumerGroupError> {
    let mut out = Vec::new();
    out.push(VERSION);
    out.extend_from_slice(&state.next_offset.to_le_bytes());
    match state.leased_offset {
        Some(offset) => {
            out.push(1);
            out.extend_from_slice(&offset.to_le_bytes());
        }
        None => out.push(0),
    }
    out.extend_from_slice(&state.lease_until_ms.to_le_bytes());
    out.extend_from_slice(&state.lease_generation.to_le_bytes());
    let owner_len =
        u32::try_from(state.owner.len()).map_err(|_| ConsumerGroupError::CorruptRecord)?;
    out.extend_from_slice(&owner_len.to_le_bytes());
    out.extend_from_slice(&state.owner);
    Ok(out)
}

fn decode_state(bytes: &[u8]) -> Result<ConsumerGroupState, ConsumerGroupError> {
    if bytes.len() < 1 + 8 + 1 + 8 + 8 + 4 || bytes[0] != VERSION {
        return Err(ConsumerGroupError::CorruptRecord);
    }
    let mut pos = 1usize;
    let next_offset = read_u64(bytes, &mut pos)?;
    let leased_offset = match read_u8(bytes, &mut pos)? {
        0 => None,
        1 => Some(read_u64(bytes, &mut pos)?),
        _ => return Err(ConsumerGroupError::CorruptRecord),
    };
    let lease_until_ms = read_i64(bytes, &mut pos)?;
    let lease_generation = read_u64(bytes, &mut pos)?;
    let owner_len = usize::try_from(read_u32(bytes, &mut pos)?)
        .map_err(|_| ConsumerGroupError::CorruptRecord)?;
    let end = pos
        .checked_add(owner_len)
        .ok_or(ConsumerGroupError::CorruptRecord)?;
    let owner = bytes
        .get(pos..end)
        .ok_or(ConsumerGroupError::CorruptRecord)?
        .to_vec();
    if end != bytes.len() {
        return Err(ConsumerGroupError::CorruptRecord);
    }
    Ok(ConsumerGroupState {
        next_offset,
        leased_offset,
        lease_until_ms,
        lease_generation,
        owner,
    })
}

fn read_u8(bytes: &[u8], pos: &mut usize) -> Result<u8, ConsumerGroupError> {
    let value = *bytes.get(*pos).ok_or(ConsumerGroupError::CorruptRecord)?;
    *pos += 1;
    Ok(value)
}

fn read_u32(bytes: &[u8], pos: &mut usize) -> Result<u32, ConsumerGroupError> {
    let end = pos
        .checked_add(4)
        .ok_or(ConsumerGroupError::CorruptRecord)?;
    let value = bytes
        .get(*pos..end)
        .ok_or(ConsumerGroupError::CorruptRecord)?;
    *pos = end;
    Ok(u32::from_le_bytes(
        value
            .try_into()
            .map_err(|_| ConsumerGroupError::CorruptRecord)?,
    ))
}

fn read_u64(bytes: &[u8], pos: &mut usize) -> Result<u64, ConsumerGroupError> {
    let end = pos
        .checked_add(8)
        .ok_or(ConsumerGroupError::CorruptRecord)?;
    let value = bytes
        .get(*pos..end)
        .ok_or(ConsumerGroupError::CorruptRecord)?;
    *pos = end;
    Ok(u64::from_le_bytes(
        value
            .try_into()
            .map_err(|_| ConsumerGroupError::CorruptRecord)?,
    ))
}

fn read_i64(bytes: &[u8], pos: &mut usize) -> Result<i64, ConsumerGroupError> {
    let end = pos
        .checked_add(8)
        .ok_or(ConsumerGroupError::CorruptRecord)?;
    let value = bytes
        .get(*pos..end)
        .ok_or(ConsumerGroupError::CorruptRecord)?;
    *pos = end;
    Ok(i64::from_le_bytes(
        value
            .try_into()
            .map_err(|_| ConsumerGroupError::CorruptRecord)?,
    ))
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
        std::env::temp_dir().join(format!("voodoo-store-cg-{name}-{nonce}.vstore"))
    }

    #[test]
    fn claim_ack_advances_group_cursor_durably() {
        let path = temp_store_path("ack");
        {
            let mut store = Store::open(&path).unwrap();
            {
                let mut stream = store.stream(b"events").unwrap();
                stream.append(b"one").unwrap();
                stream.append(b"two").unwrap();
            }
            let first = store
                .consumer_group_claim(b"events", b"workers", b"a", 0, 100)
                .unwrap()
                .unwrap();
            assert_eq!(first.entry.payload, b"one");
            store
                .consumer_group_ack(
                    b"events",
                    b"workers",
                    b"a",
                    first.entry.offset,
                    first.lease_generation,
                )
                .unwrap();
        }
        {
            let mut store = Store::open(&path).unwrap();
            let second = store
                .consumer_group_claim(b"events", b"workers", b"b", 1, 100)
                .unwrap()
                .unwrap();
            assert_eq!(second.entry.payload, b"two");
        }
        let _ = fs::remove_file(path);
    }

    #[test]
    fn expired_lease_redelivers_and_rejects_stale_ack() {
        let path = temp_store_path("lease");
        let mut store = Store::open(&path).unwrap();
        {
            let mut stream = store.stream(b"events").unwrap();
            stream.append(b"one").unwrap();
        }
        let first = store
            .consumer_group_claim(b"events", b"workers", b"a", 0, 10)
            .unwrap()
            .unwrap();
        assert!(
            store
                .consumer_group_claim(b"events", b"workers", b"b", 9, 10)
                .unwrap()
                .is_none()
        );
        let second = store
            .consumer_group_claim(b"events", b"workers", b"b", 10, 10)
            .unwrap()
            .unwrap();
        assert_eq!(second.entry.offset, first.entry.offset);
        assert!(matches!(
            store.consumer_group_ack(
                b"events",
                b"workers",
                b"a",
                first.entry.offset,
                first.lease_generation,
            ),
            Err(ConsumerGroupError::OwnerMismatch) | Err(ConsumerGroupError::LeaseMismatch { .. })
        ));
        store
            .consumer_group_ack(
                b"events",
                b"workers",
                b"b",
                second.entry.offset,
                second.lease_generation,
            )
            .unwrap();
        let _ = fs::remove_file(path);
    }

    #[test]
    fn nack_releases_without_advancing() {
        let path = temp_store_path("nack");
        let mut store = Store::open(&path).unwrap();
        {
            let mut stream = store.stream(b"events").unwrap();
            stream.append(b"one").unwrap();
        }
        let first = store
            .consumer_group_claim(b"events", b"workers", b"a", 0, 100)
            .unwrap()
            .unwrap();
        store
            .consumer_group_nack(
                b"events",
                b"workers",
                b"a",
                first.entry.offset,
                first.lease_generation,
            )
            .unwrap();
        let second = store
            .consumer_group_claim(b"events", b"workers", b"b", 1, 100)
            .unwrap()
            .unwrap();
        assert_eq!(second.entry.offset, first.entry.offset);
        let _ = fs::remove_file(path);
    }
}
