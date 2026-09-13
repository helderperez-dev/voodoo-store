//! Cross-domain transaction operations for queues, messaging and objects.
//!
//! These operations use the transaction's staged read view so multiple pushes
//! or appends in the same transaction reserve IDs/offsets correctly. They write
//! the exact v1 records consumed by the existing Store APIs.

use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{EngineError, ObjectId, PushOptions, Transaction};

const QUEUE_PREFIX: &[u8] = b"\xffvds:q:";
const QUEUE_MESSAGE_VERSION: u8 = 1;
const STREAM_PREFIX: &[u8] = b"\xffvds:stream:";
const STREAM_ENTRY_VERSION: u8 = 1;
const OBJECT_PREFIX: &[u8] = b"\xffvds:obj:data:";
const OBJECT_REF_PREFIX: &[u8] = b"\xffvds:obj:ref:";
const OBJECT_VERSION: u8 = 1;

impl Transaction<'_> {
    /// Pushes a queue message as part of this transaction.
    pub fn push_queue(
        &mut self,
        queue: impl AsRef<[u8]>,
        payload: impl AsRef<[u8]>,
        options: PushOptions,
    ) -> Result<u64, DomainTransactionError> {
        let queue = queue.as_ref();
        if queue.is_empty() {
            return Err(DomainTransactionError::EmptyName);
        }
        let namespace = queue_namespace(queue)?;
        let mut meta_key = namespace.clone();
        meta_key.extend_from_slice(b":next");
        let id = match self.get_internal(&meta_key) {
            None => 1,
            Some(bytes) => decode_u64(bytes)?,
        };
        let next = id
            .checked_add(1)
            .ok_or(DomainTransactionError::CounterExhausted)?;

        let mut message_prefix = namespace;
        message_prefix.extend_from_slice(b":m:");
        let mut message_key = message_prefix;
        message_key.extend_from_slice(&id.to_be_bytes());

        self.put_internal(meta_key, next.to_le_bytes())?;
        self.put_internal(
            message_key,
            encode_queue_message(id, payload.as_ref(), options)?,
        )?;
        Ok(id)
    }

    /// Appends an entry to a durable stream in this transaction.
    pub fn append_stream(
        &mut self,
        stream: impl AsRef<[u8]>,
        payload: impl AsRef<[u8]>,
    ) -> Result<u64, DomainTransactionError> {
        let stream = stream.as_ref();
        if stream.is_empty() {
            return Err(DomainTransactionError::EmptyName);
        }
        let namespace = tuple_key(STREAM_PREFIX, &[stream])?;
        let mut next_key = namespace.clone();
        next_key.extend_from_slice(b":next");
        let offset = match self.get_internal(&next_key) {
            None => 0,
            Some(bytes) => decode_u64(bytes)?,
        };
        let next = offset
            .checked_add(1)
            .ok_or(DomainTransactionError::CounterExhausted)?;

        let mut entry_key = namespace;
        entry_key.extend_from_slice(b":e:");
        entry_key.extend_from_slice(&offset.to_be_bytes());

        self.put_internal(next_key, next.to_le_bytes())?;
        self.put_internal(entry_key, encode_stream_entry(payload.as_ref())?)?;
        Ok(offset)
    }

    /// Topics share the stream representation, so transactional publication is
    /// exactly a stream append with topic semantics layered above it.
    pub fn publish_topic(
        &mut self,
        topic: impl AsRef<[u8]>,
        payload: impl AsRef<[u8]>,
    ) -> Result<u64, DomainTransactionError> {
        self.append_stream(topic, payload)
    }

    /// Stores an immutable content-addressed object in this transaction.
    pub fn put_object(
        &mut self,
        content: impl AsRef<[u8]>,
    ) -> Result<ObjectId, DomainTransactionError> {
        let content = content.as_ref();
        let id: ObjectId = Sha256::digest(content).into();
        let key = object_key(&id);
        if self.get_internal(&key).is_none() {
            self.put_internal(key, encode_object(content)?)?;
        }
        Ok(id)
    }

    /// Links an existing object (including one staged earlier in this same
    /// transaction) to a durable named reference.
    pub fn link_object(
        &mut self,
        namespace: impl AsRef<[u8]>,
        name: impl AsRef<[u8]>,
        id: &ObjectId,
    ) -> Result<(), DomainTransactionError> {
        if self.get_internal(object_key(id)).is_none() {
            return Err(DomainTransactionError::ObjectNotFound);
        }
        self.put_internal(reference_key(namespace.as_ref(), name.as_ref())?, id)?;
        Ok(())
    }

    /// Removes a durable object reference as part of this transaction.
    pub fn unlink_object(
        &mut self,
        namespace: impl AsRef<[u8]>,
        name: impl AsRef<[u8]>,
    ) -> Result<bool, DomainTransactionError> {
        let key = reference_key(namespace.as_ref(), name.as_ref())?;
        let existed = self.get_internal(&key).is_some();
        if existed {
            self.delete_internal(key)?;
        }
        Ok(existed)
    }
}

#[derive(Debug, Error)]
pub enum DomainTransactionError {
    #[error("store error: {0}")]
    Store(#[from] EngineError),
    #[error("queue/stream/topic name cannot be empty")]
    EmptyName,
    #[error("name is too long")]
    NameTooLong,
    #[error("payload/object is too large")]
    PayloadTooLarge,
    #[error("transactional metadata is corrupt")]
    CorruptMetadata,
    #[error("transactional counter/offset is exhausted")]
    CounterExhausted,
    #[error("object does not exist")]
    ObjectNotFound,
    #[error("object reference namespace/name must not be empty")]
    EmptyReferenceName,
    #[error("object reference namespace/name is too long")]
    ReferenceNameTooLong,
}

fn queue_namespace(name: &[u8]) -> Result<Vec<u8>, DomainTransactionError> {
    let len = u32::try_from(name.len()).map_err(|_| DomainTransactionError::NameTooLong)?;
    let mut namespace = Vec::with_capacity(QUEUE_PREFIX.len() + 4 + name.len());
    namespace.extend_from_slice(QUEUE_PREFIX);
    namespace.extend_from_slice(&len.to_be_bytes());
    namespace.extend_from_slice(name);
    Ok(namespace)
}

fn encode_queue_message(
    id: u64,
    payload: &[u8],
    options: PushOptions,
) -> Result<Vec<u8>, DomainTransactionError> {
    let payload_len =
        u32::try_from(payload.len()).map_err(|_| DomainTransactionError::PayloadTooLarge)?;
    let mut out = Vec::with_capacity(38 + payload.len());
    out.push(QUEUE_MESSAGE_VERSION);
    out.push(0); // QueueState::Ready
    out.extend_from_slice(&id.to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes()); // attempts
    out.extend_from_slice(&options.priority.to_le_bytes());
    out.extend_from_slice(&options.available_at_ms.to_le_bytes());
    out.extend_from_slice(&0i64.to_le_bytes()); // lease_until_ms
    out.extend_from_slice(&payload_len.to_le_bytes());
    out.extend_from_slice(payload);
    Ok(out)
}

fn tuple_key(prefix: &[u8], components: &[&[u8]]) -> Result<Vec<u8>, DomainTransactionError> {
    let mut key = Vec::from(prefix);
    for component in components {
        let len =
            u32::try_from(component.len()).map_err(|_| DomainTransactionError::NameTooLong)?;
        key.extend_from_slice(&len.to_be_bytes());
        key.extend_from_slice(component);
    }
    Ok(key)
}

fn encode_stream_entry(payload: &[u8]) -> Result<Vec<u8>, DomainTransactionError> {
    let len = u32::try_from(payload.len()).map_err(|_| DomainTransactionError::PayloadTooLarge)?;
    let mut encoded = Vec::with_capacity(5 + payload.len());
    encoded.push(STREAM_ENTRY_VERSION);
    encoded.extend_from_slice(&len.to_le_bytes());
    encoded.extend_from_slice(payload);
    Ok(encoded)
}

fn object_key(id: &ObjectId) -> Vec<u8> {
    let mut key = Vec::with_capacity(OBJECT_PREFIX.len() + id.len());
    key.extend_from_slice(OBJECT_PREFIX);
    key.extend_from_slice(id);
    key
}

fn reference_key(namespace: &[u8], name: &[u8]) -> Result<Vec<u8>, DomainTransactionError> {
    if namespace.is_empty() || name.is_empty() {
        return Err(DomainTransactionError::EmptyReferenceName);
    }
    let namespace_len =
        u32::try_from(namespace.len()).map_err(|_| DomainTransactionError::ReferenceNameTooLong)?;
    let name_len =
        u32::try_from(name.len()).map_err(|_| DomainTransactionError::ReferenceNameTooLong)?;
    let mut key = Vec::with_capacity(OBJECT_REF_PREFIX.len() + 8 + namespace.len() + name.len());
    key.extend_from_slice(OBJECT_REF_PREFIX);
    key.extend_from_slice(&namespace_len.to_be_bytes());
    key.extend_from_slice(namespace);
    key.extend_from_slice(&name_len.to_be_bytes());
    key.extend_from_slice(name);
    Ok(key)
}

fn encode_object(content: &[u8]) -> Result<Vec<u8>, DomainTransactionError> {
    let len = u64::try_from(content.len()).map_err(|_| DomainTransactionError::PayloadTooLarge)?;
    let mut encoded = Vec::with_capacity(9 + content.len());
    encoded.push(OBJECT_VERSION);
    encoded.extend_from_slice(&len.to_le_bytes());
    encoded.extend_from_slice(content);
    Ok(encoded)
}

fn decode_u64(bytes: &[u8]) -> Result<u64, DomainTransactionError> {
    let encoded: [u8; 8] = bytes
        .try_into()
        .map_err(|_| DomainTransactionError::CorruptMetadata)?;
    Ok(u64::from_le_bytes(encoded))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::{JobSpec, Store};

    use super::*;

    fn temp_store_path(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("voodoo-store-domains-{name}-{nonce}.vstore"))
    }

    #[test]
    fn state_job_queue_stream_object_and_event_share_one_commit() {
        let path = temp_store_path("commit");
        let job_id;
        let object_id;
        {
            let mut store = Store::open(&path).unwrap();
            let mut tx = store.begin().unwrap();
            tx.put(b"order:42", b"paid").unwrap();
            job_id = tx.enqueue_job(JobSpec::new(b"receipt", b"42"), 10).unwrap();
            assert_eq!(
                tx.push_queue(b"emails", b"receipt:42", PushOptions::default())
                    .unwrap(),
                1
            );
            assert_eq!(
                tx.push_queue(b"emails", b"audit:42", PushOptions::default())
                    .unwrap(),
                2
            );
            assert_eq!(tx.append_stream(b"orders", b"paid:42").unwrap(), 0);
            assert_eq!(tx.publish_topic(b"notifications", b"order:42").unwrap(), 0);
            object_id = tx.put_object(b"invoice bytes").unwrap();
            tx.link_object(b"invoices", b"42", &object_id).unwrap();
            tx.emit_event(b"order.paid", b"42", 10).unwrap();
            tx.commit().unwrap();

            assert_eq!(store.get(b"order:42"), Some(b"paid".as_slice()));
            assert!(store.get_job(&job_id).unwrap().is_some());
            assert_eq!(
                store.resolve_object_ref(b"invoices", b"42").unwrap(),
                Some(object_id)
            );
            assert_eq!(
                store.get_object(&object_id).unwrap(),
                Some(b"invoice bytes".as_slice())
            );
            {
                let queue = store.queue(b"emails").unwrap();
                assert_eq!(queue.stats().unwrap().total, 2);
            }
            {
                let stream = store.stream(b"orders").unwrap();
                assert_eq!(stream.read_from(0, 10).unwrap().len(), 1);
            }
            {
                let topic = store.topic(b"notifications").unwrap();
                assert_eq!(topic.read_from(0, 10).unwrap().len(), 1);
            }
        }
        let _ = fs::remove_file(path);
    }

    #[test]
    fn rollback_hides_all_transactional_domains() {
        let path = temp_store_path("rollback");
        let object_id;
        {
            let mut store = Store::open(&path).unwrap();
            let mut tx = store.begin().unwrap();
            tx.put(b"state", b"new").unwrap();
            tx.push_queue(b"q", b"message", PushOptions::default())
                .unwrap();
            tx.append_stream(b"s", b"entry").unwrap();
            object_id = tx.put_object(b"object").unwrap();
            tx.link_object(b"refs", b"one", &object_id).unwrap();
            tx.rollback().unwrap();

            assert_eq!(store.get(b"state"), None);
            assert!(store.get_object(&object_id).unwrap().is_none());
            assert!(store.resolve_object_ref(b"refs", b"one").unwrap().is_none());
            assert_eq!(store.queue(b"q").unwrap().stats().unwrap().total, 0);
            assert!(
                store
                    .stream(b"s")
                    .unwrap()
                    .read_from(0, 10)
                    .unwrap()
                    .is_empty()
            );
        }
        let _ = fs::remove_file(path);
    }
}
