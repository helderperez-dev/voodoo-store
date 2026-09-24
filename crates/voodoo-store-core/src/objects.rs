//! Content-addressed object/blob storage built on the transactional Store.
//!
//! Object identity is SHA-256(content). Objects are immutable and deduplicated.
//! Named references live in the same transaction log and can be garbage-collected
//! when no reference points at an object anymore.

use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{EngineError, Store};

const OBJECT_PREFIX: &[u8] = b"\xffvds:obj:data:";
const REF_PREFIX: &[u8] = b"\xffvds:obj:ref:";
const OBJECT_VERSION: u8 = 1;

pub type ObjectId = [u8; 32];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectInfo {
    pub id: ObjectId,
    pub size: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectGcReport {
    pub scanned: usize,
    pub referenced: usize,
    pub removed: usize,
}

impl Store {
    pub fn put_object(&mut self, content: impl AsRef<[u8]>) -> Result<ObjectId, ObjectError> {
        let content = content.as_ref();
        let id = object_id(content);
        let key = object_key(&id);
        if self.get(&key).is_none() {
            self.put_internal(key, encode_object(content)?)?;
        }
        Ok(id)
    }

    pub fn get_object(&self, id: &ObjectId) -> Result<Option<&[u8]>, ObjectError> {
        self.get(object_key(id)).map(decode_object).transpose()
    }

    pub fn object_info(&self, id: &ObjectId) -> Result<Option<ObjectInfo>, ObjectError> {
        Ok(self.get_object(id)?.map(|content| ObjectInfo {
            id: *id,
            size: content.len() as u64,
        }))
    }

    pub fn verify_object(&self, id: &ObjectId) -> Result<bool, ObjectError> {
        let Some(content) = self.get_object(id)? else {
            return Ok(false);
        };
        Ok(object_id(content) == *id)
    }

    pub fn link_object(
        &mut self,
        namespace: impl AsRef<[u8]>,
        name: impl AsRef<[u8]>,
        id: &ObjectId,
    ) -> Result<(), ObjectError> {
        if self.get_object(id)?.is_none() {
            return Err(ObjectError::ObjectNotFound);
        }
        let key = reference_key(namespace.as_ref(), name.as_ref())?;
        self.put_internal(key, id)?;
        Ok(())
    }

    pub fn resolve_object_ref(
        &self,
        namespace: impl AsRef<[u8]>,
        name: impl AsRef<[u8]>,
    ) -> Result<Option<ObjectId>, ObjectError> {
        let key = reference_key(namespace.as_ref(), name.as_ref())?;
        self.get(key)
            .map(|bytes| bytes.try_into().map_err(|_| ObjectError::CorruptReference))
            .transpose()
    }

    pub fn unlink_object(
        &mut self,
        namespace: impl AsRef<[u8]>,
        name: impl AsRef<[u8]>,
    ) -> Result<bool, ObjectError> {
        let key = reference_key(namespace.as_ref(), name.as_ref())?;
        if self.get(&key).is_none() {
            return Ok(false);
        }
        self.delete_internal(key)?;
        Ok(true)
    }

    pub fn list_object_refs(
        &self,
        namespace: impl AsRef<[u8]>,
        name_prefix: impl AsRef<[u8]>,
    ) -> Result<Vec<(Vec<u8>, ObjectId)>, ObjectError> {
        let namespace = namespace.as_ref();
        let name_prefix = name_prefix.as_ref();
        if namespace.is_empty() {
            return Err(ObjectError::EmptyReferenceName);
        }

        let scan_prefix = reference_namespace_prefix(namespace)?;
        let mut refs = Vec::new();
        for (key, value) in self.scan_prefix(&scan_prefix) {
            let name = decode_reference_name(&key, namespace)?;
            if !name.starts_with(name_prefix) {
                continue;
            }
            let id: ObjectId = value
                .as_slice()
                .try_into()
                .map_err(|_| ObjectError::CorruptReference)?;
            refs.push((name, id));
        }
        refs.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        Ok(refs)
    }

    pub fn gc_orphan_objects(&mut self, limit: usize) -> Result<ObjectGcReport, ObjectError> {
        let mut referenced = std::collections::HashSet::new();
        for (_, value) in self.scan_prefix(REF_PREFIX) {
            let id: ObjectId = value
                .as_slice()
                .try_into()
                .map_err(|_| ObjectError::CorruptReference)?;
            referenced.insert(id);
        }

        let objects = self.scan_prefix(OBJECT_PREFIX);
        let scanned = objects.len();
        let referenced_count = referenced.len();
        if limit == 0 {
            return Ok(ObjectGcReport {
                scanned,
                referenced: referenced_count,
                removed: 0,
            });
        }

        let mut to_remove = Vec::new();
        for (key, _) in objects {
            let id = decode_object_key(&key)?;
            if !referenced.contains(&id) {
                to_remove.push(key);
                if to_remove.len() == limit {
                    break;
                }
            }
        }
        let removed = to_remove.len();
        if removed > 0 {
            let mut tx = self.begin()?;
            for key in to_remove {
                tx.delete_internal(key)?;
            }
            tx.commit()?;
        }

        Ok(ObjectGcReport {
            scanned,
            referenced: referenced_count,
            removed,
        })
    }
}

fn object_id(content: &[u8]) -> ObjectId {
    let digest = Sha256::digest(content);
    digest.into()
}

fn object_key(id: &ObjectId) -> Vec<u8> {
    let mut key = Vec::with_capacity(OBJECT_PREFIX.len() + id.len());
    key.extend_from_slice(OBJECT_PREFIX);
    key.extend_from_slice(id);
    key
}

fn decode_object_key(key: &[u8]) -> Result<ObjectId, ObjectError> {
    if !key.starts_with(OBJECT_PREFIX) || key.len() != OBJECT_PREFIX.len() + 32 {
        return Err(ObjectError::CorruptObject);
    }
    key[OBJECT_PREFIX.len()..]
        .try_into()
        .map_err(|_| ObjectError::CorruptObject)
}

fn reference_namespace_prefix(namespace: &[u8]) -> Result<Vec<u8>, ObjectError> {
    if namespace.is_empty() {
        return Err(ObjectError::EmptyReferenceName);
    }
    let namespace_len =
        u32::try_from(namespace.len()).map_err(|_| ObjectError::ReferenceNameTooLong)?;
    let mut key = Vec::with_capacity(REF_PREFIX.len() + 4 + namespace.len());
    key.extend_from_slice(REF_PREFIX);
    key.extend_from_slice(&namespace_len.to_be_bytes());
    key.extend_from_slice(namespace);
    Ok(key)
}

fn decode_reference_name(key: &[u8], namespace: &[u8]) -> Result<Vec<u8>, ObjectError> {
    let prefix = reference_namespace_prefix(namespace)?;
    if !key.starts_with(&prefix) {
        return Err(ObjectError::CorruptReference);
    }
    let rest = &key[prefix.len()..];
    if rest.len() < 4 {
        return Err(ObjectError::CorruptReference);
    }
    let name_len = usize::try_from(u32::from_be_bytes(
        rest[..4]
            .try_into()
            .map_err(|_| ObjectError::CorruptReference)?,
    ))
    .map_err(|_| ObjectError::CorruptReference)?;
    if rest.len() != 4 + name_len {
        return Err(ObjectError::CorruptReference);
    }
    Ok(rest[4..].to_vec())
}

fn reference_key(namespace: &[u8], name: &[u8]) -> Result<Vec<u8>, ObjectError> {
    if namespace.is_empty() || name.is_empty() {
        return Err(ObjectError::EmptyReferenceName);
    }
    let namespace_len =
        u32::try_from(namespace.len()).map_err(|_| ObjectError::ReferenceNameTooLong)?;
    let name_len = u32::try_from(name.len()).map_err(|_| ObjectError::ReferenceNameTooLong)?;
    let mut key = Vec::with_capacity(REF_PREFIX.len() + 8 + namespace.len() + name.len());
    key.extend_from_slice(REF_PREFIX);
    key.extend_from_slice(&namespace_len.to_be_bytes());
    key.extend_from_slice(namespace);
    key.extend_from_slice(&name_len.to_be_bytes());
    key.extend_from_slice(name);
    Ok(key)
}

fn encode_object(content: &[u8]) -> Result<Vec<u8>, ObjectError> {
    let len = u64::try_from(content.len()).map_err(|_| ObjectError::ObjectTooLarge)?;
    let mut encoded = Vec::with_capacity(9 + content.len());
    encoded.push(OBJECT_VERSION);
    encoded.extend_from_slice(&len.to_le_bytes());
    encoded.extend_from_slice(content);
    Ok(encoded)
}

fn decode_object(encoded: &[u8]) -> Result<&[u8], ObjectError> {
    if encoded.len() < 9 || encoded[0] != OBJECT_VERSION {
        return Err(ObjectError::CorruptObject);
    }
    let len = u64::from_le_bytes(encoded[1..9].try_into().expect("object length"));
    let len = usize::try_from(len).map_err(|_| ObjectError::CorruptObject)?;
    if encoded.len() != 9 + len {
        return Err(ObjectError::CorruptObject);
    }
    Ok(&encoded[9..])
}

#[derive(Debug, Error)]
pub enum ObjectError {
    #[error("store error: {0}")]
    Store(#[from] EngineError),
    #[error("object does not exist")]
    ObjectNotFound,
    #[error("object is too large")]
    ObjectTooLarge,
    #[error("object data is corrupt or unsupported")]
    CorruptObject,
    #[error("object reference is corrupt")]
    CorruptReference,
    #[error("object reference namespace/name must not be empty")]
    EmptyReferenceName,
    #[error("object reference namespace/name is too long")]
    ReferenceNameTooLong,
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
        std::env::temp_dir().join(format!("voodoo-store-object-{name}-{nonce}.vstore"))
    }

    #[test]
    fn objects_are_content_addressed_deduplicated_and_durable() {
        let path = temp_store_path("content");
        let id;
        {
            let mut store = Store::open(&path).unwrap();
            id = store.put_object(b"hello world").unwrap();
            assert_eq!(id, store.put_object(b"hello world").unwrap());
            assert!(store.verify_object(&id).unwrap());
            store.link_object(b"avatars", b"u1", &id).unwrap();
        }
        {
            let store = Store::open(&path).unwrap();
            assert_eq!(
                store.get_object(&id).unwrap(),
                Some(b"hello world".as_slice())
            );
            assert_eq!(
                store.resolve_object_ref(b"avatars", b"u1").unwrap(),
                Some(id)
            );
        }
        let _ = fs::remove_file(path);
    }

    #[test]
    fn orphan_gc_preserves_referenced_objects() {
        let path = temp_store_path("gc");
        let mut store = Store::open(&path).unwrap();
        let keep = store.put_object(b"keep").unwrap();
        let remove = store.put_object(b"remove").unwrap();
        store.link_object(b"docs", b"primary", &keep).unwrap();
        let report = store.gc_orphan_objects(100).unwrap();
        assert_eq!(report.removed, 1);
        assert!(store.get_object(&keep).unwrap().is_some());
        assert!(store.get_object(&remove).unwrap().is_none());
        drop(store);
        let _ = fs::remove_file(path);
    }
}
