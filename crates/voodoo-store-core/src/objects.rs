//! Content-addressed object/blob storage built on the transactional Store.
//!
//! Object identity is SHA-256(content). Objects are immutable and deduplicated.
//! Named references live in the same transaction log and can be garbage-collected
//! when no reference points at an object anymore.

use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{EngineError, Store};

const OBJECT_PREFIX: &[u8] = b"\xffvds:obj:data:";
const METADATA_PREFIX: &[u8] = b"\xffvds:obj:meta:";
const REF_PREFIX: &[u8] = b"\xffvds:obj:ref:";
const OBJECT_VERSION: u8 = 1;

pub type ObjectId = [u8; 32];
type MetadataEntries = Vec<(Vec<u8>, Vec<u8>)>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectInfo {
    pub id: ObjectId,
    pub size: u64,
    pub metadata: Vec<(Vec<u8>, Vec<u8>)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectGcReport {
    pub scanned: usize,
    pub referenced: usize,
    pub removed: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectRetentionReport {
    pub scanned: usize,
    pub expired: usize,
    pub referenced: usize,
    pub removed: usize,
}

impl Store {
    pub fn put_object(&mut self, content: impl AsRef<[u8]>) -> Result<ObjectId, ObjectError> {
        self.put_object_with_metadata(content, std::iter::empty())
    }

    pub fn put_object_with_metadata(
        &mut self,
        content: impl AsRef<[u8]>,
        metadata: impl IntoIterator<Item = (Vec<u8>, Vec<u8>)>,
    ) -> Result<ObjectId, ObjectError> {
        let content = content.as_ref();
        let metadata = normalize_metadata(metadata)?;
        let id = object_id(content);
        let key = object_key(&id);
        if self.get(&key).is_none() {
            self.put_internal(key, encode_object(content)?)?;
            if !metadata.is_empty() {
                self.put_internal(metadata_key(&id), encode_metadata(&metadata)?)?;
            }
            self.record_object_put(content.len());
        }
        Ok(id)
    }

    pub fn get_object(&self, id: &ObjectId) -> Result<Option<&[u8]>, ObjectError> {
        let content = self.get(object_key(id)).map(decode_object).transpose()?;
        if let Some(content) = content {
            crate::Store::record_object_read(content.len());
        }
        Ok(content)
    }

    /// Read a bounded object slice without requiring callers to transfer the
    /// complete object at once. Offsets past the end return an empty slice;
    /// missing objects still return `None` so absence remains distinguishable.
    pub fn get_object_chunk(
        &self,
        id: &ObjectId,
        offset: usize,
        limit: usize,
    ) -> Result<Option<&[u8]>, ObjectError> {
        let Some(content) = self.get(object_key(id)).map(decode_object).transpose()? else {
            return Ok(None);
        };
        let start = offset.min(content.len());
        let end = start.saturating_add(limit).min(content.len());
        crate::Store::record_object_read(end.saturating_sub(start));
        Ok(Some(&content[start..end]))
    }

    pub fn object_info(&self, id: &ObjectId) -> Result<Option<ObjectInfo>, ObjectError> {
        let Some(content) = self.get_object(id)? else {
            return Ok(None);
        };
        let metadata = self
            .get(metadata_key(id))
            .map(decode_metadata)
            .transpose()?
            .unwrap_or_default();
        Ok(Some(ObjectInfo {
            id: *id,
            size: content.len() as u64,
            metadata,
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
                let id = decode_object_key(&key)?;
                tx.delete_internal(key)?;
                tx.delete_internal(metadata_key(&id))?;
            }
            tx.commit()?;
        }
        self.record_object_gc(removed);

        Ok(ObjectGcReport {
            scanned,
            referenced: referenced_count,
            removed,
        })
    }

    /// Remove expired, unreferenced objects using the reserved
    /// `expires_at_unix` metadata field. Referenced objects are retained and
    /// reported so lifecycle cleanup cannot invalidate a live reference.
    pub fn purge_expired_objects(
        &mut self,
        now_unix: i64,
        limit: usize,
    ) -> Result<ObjectRetentionReport, ObjectError> {
        let mut references = std::collections::HashSet::new();
        for (_, value) in self.scan_prefix(REF_PREFIX) {
            let id: ObjectId = value
                .as_slice()
                .try_into()
                .map_err(|_| ObjectError::CorruptReference)?;
            references.insert(id);
        }
        let objects = self.scan_prefix(OBJECT_PREFIX);
        let scanned = objects.len();
        let mut expired = 0usize;
        let mut referenced = 0usize;
        let mut to_remove = Vec::new();
        for (key, _) in objects {
            let id = decode_object_key(&key)?;
            let metadata = self
                .get(metadata_key(&id))
                .map(decode_metadata)
                .transpose()?
                .unwrap_or_default();
            let Some((_, value)) = metadata.iter().find(|(key, _)| key == b"expires_at_unix")
            else {
                continue;
            };
            let Ok(expiry) = std::str::from_utf8(value)
                .ok()
                .and_then(|value| value.parse::<i64>().ok())
                .ok_or(())
            else {
                return Err(ObjectError::CorruptMetadata);
            };
            if expiry > now_unix {
                continue;
            }
            expired += 1;
            if references.contains(&id) {
                referenced += 1;
                continue;
            }
            if limit == 0 || to_remove.len() < limit {
                to_remove.push((key, id));
            }
        }
        if !to_remove.is_empty() {
            let mut tx = self.begin()?;
            for (key, id) in &to_remove {
                tx.delete_internal(key.clone())?;
                tx.delete_internal(metadata_key(id))?;
            }
            tx.commit()?
        }
        self.record_object_gc(to_remove.len());
        Ok(ObjectRetentionReport {
            scanned,
            expired,
            referenced,
            removed: to_remove.len(),
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

fn metadata_key(id: &ObjectId) -> Vec<u8> {
    let mut key = Vec::with_capacity(METADATA_PREFIX.len() + id.len());
    key.extend_from_slice(METADATA_PREFIX);
    key.extend_from_slice(id);
    key
}

fn normalize_metadata(
    metadata: impl IntoIterator<Item = (Vec<u8>, Vec<u8>)>,
) -> Result<MetadataEntries, ObjectError> {
    let mut values: Vec<_> = metadata.into_iter().collect();
    values.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    let mut total = 0usize;
    for (index, (key, value)) in values.iter().enumerate() {
        if key.is_empty() || key.len() > 4096 || value.len() > 64 * 1024 {
            return Err(ObjectError::MetadataTooLarge);
        }
        if index > 0 && values[index - 1].0 == *key {
            return Err(ObjectError::DuplicateMetadata);
        }
        total = total
            .checked_add(key.len() + value.len())
            .ok_or(ObjectError::MetadataTooLarge)?;
    }
    if total > 256 * 1024 {
        return Err(ObjectError::MetadataTooLarge);
    }
    Ok(values)
}

fn encode_metadata(metadata: &[(Vec<u8>, Vec<u8>)]) -> Result<Vec<u8>, ObjectError> {
    let count = u32::try_from(metadata.len()).map_err(|_| ObjectError::MetadataTooLarge)?;
    let mut encoded = vec![OBJECT_VERSION];
    encoded.extend_from_slice(&count.to_le_bytes());
    for (key, value) in metadata {
        let key_len = u32::try_from(key.len()).map_err(|_| ObjectError::MetadataTooLarge)?;
        let value_len = u32::try_from(value.len()).map_err(|_| ObjectError::MetadataTooLarge)?;
        encoded.extend_from_slice(&key_len.to_le_bytes());
        encoded.extend_from_slice(&value_len.to_le_bytes());
        encoded.extend_from_slice(key);
        encoded.extend_from_slice(value);
    }
    Ok(encoded)
}

fn decode_metadata(encoded: &[u8]) -> Result<MetadataEntries, ObjectError> {
    if encoded.len() < 5 || encoded[0] != OBJECT_VERSION {
        return Err(ObjectError::CorruptMetadata);
    }
    let count = u32::from_le_bytes(encoded[1..5].try_into().expect("metadata count")) as usize;
    let mut offset = 5usize;
    let mut metadata = Vec::with_capacity(count);
    for _ in 0..count {
        if encoded.len() < offset + 8 {
            return Err(ObjectError::CorruptMetadata);
        }
        let key_len = u32::from_le_bytes(encoded[offset..offset + 4].try_into().unwrap()) as usize;
        let value_len =
            u32::from_le_bytes(encoded[offset + 4..offset + 8].try_into().unwrap()) as usize;
        offset += 8;
        let end = offset
            .checked_add(key_len)
            .and_then(|value_start| value_start.checked_add(value_len))
            .ok_or(ObjectError::CorruptMetadata)?;
        if end > encoded.len() {
            return Err(ObjectError::CorruptMetadata);
        }
        metadata.push((
            encoded[offset..offset + key_len].to_vec(),
            encoded[offset + key_len..end].to_vec(),
        ));
        offset = end;
    }
    if offset != encoded.len() {
        return Err(ObjectError::CorruptMetadata);
    }
    normalize_metadata(metadata)
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
    #[error("object metadata is too large or invalid")]
    MetadataTooLarge,
    #[error("object metadata contains a duplicate key")]
    DuplicateMetadata,
    #[error("object metadata is corrupt or unsupported")]
    CorruptMetadata,
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
            assert_eq!(store.metrics().object_puts, 1);
            assert_eq!(store.metrics().object_bytes_written, 11);
            assert!(store.health_report().unwrap().object_bytes_read_total >= 11);
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
    fn bounded_object_chunks_preserve_order_and_absence_semantics() {
        let path = temp_store_path("chunks");
        let mut store = Store::open(&path).unwrap();
        let id = store.put_object(b"0123456789").unwrap();

        assert_eq!(
            store.get_object_chunk(&id, 0, 4).unwrap(),
            Some(&b"0123"[..])
        );
        assert_eq!(
            store.get_object_chunk(&id, 4, 4).unwrap(),
            Some(&b"4567"[..])
        );
        assert_eq!(
            store.get_object_chunk(&id, 8, 100).unwrap(),
            Some(&b"89"[..])
        );
        assert_eq!(store.get_object_chunk(&id, 100, 4).unwrap(), Some(&b""[..]));
        assert_eq!(store.get_object_chunk(&[9; 32], 0, 4).unwrap(), None);

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

    #[test]
    fn retention_purge_fails_closed_on_corrupt_reference() {
        let path = temp_store_path("retention-corrupt-reference");
        let mut store = Store::open(&path).unwrap();
        let _object = store
            .put_object_with_metadata(
                b"payload",
                vec![(b"expires_at_unix".to_vec(), b"1".to_vec())],
            )
            .unwrap();
        let key = reference_key(b"docs", b"broken").unwrap();
        store.put_internal(key, b"not-an-object-id").unwrap();

        let result = store.purge_expired_objects(1, 100);
        assert!(matches!(result, Err(ObjectError::CorruptReference)));
        drop(store);
        let _ = fs::remove_file(path);
    }
}
