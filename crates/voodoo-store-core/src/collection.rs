//! Structured collections implemented on the Voodoo Store transactional KV.
//!
//! The core remains codec-agnostic: records, primary keys, and index values are
//! bytes. Bindings/frameworks can layer JSON, MessagePack, CBOR, Protobuf, or a
//! Voodoo schema codec without changing the storage format.

use thiserror::Error;

use crate::{EngineError, Store, Transaction};

const META_PREFIX: &[u8] = b"\xffvds:col:meta:";
const RECORD_PREFIX: &[u8] = b"\xffvds:col:record:";
const INDEX_META_PREFIX: &[u8] = b"\xffvds:col:index-meta:";
const INDEX_PREFIX: &[u8] = b"\xffvds:col:index:";
const FORMAT_VERSION: u8 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectionDefinition {
    pub schema_version: u32,
    pub codec: Vec<u8>,
}

impl Default for CollectionDefinition {
    fn default() -> Self {
        Self {
            schema_version: 1,
            codec: b"bytes".to_vec(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexDefinition {
    pub name: Vec<u8>,
    pub unique: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexValue {
    pub index: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectionRecord {
    pub primary_key: Vec<u8>,
    pub value: Vec<u8>,
    pub indexes: Vec<IndexValue>,
}

impl Store {
    pub fn create_collection(
        &mut self,
        name: impl AsRef<[u8]>,
        definition: &CollectionDefinition,
    ) -> Result<bool, CollectionError> {
        let name = name.as_ref();
        validate_name(name)?;
        let key = meta_key(name)?;
        if self.get(&key).is_some() {
            return Ok(false);
        }
        self.put_internal(key, encode_collection_definition(definition)?)?;
        Ok(true)
    }

    pub fn collection_definition(
        &self,
        name: impl AsRef<[u8]>,
    ) -> Result<Option<CollectionDefinition>, CollectionError> {
        let key = meta_key(name.as_ref())?;
        self.get(key).map(decode_collection_definition).transpose()
    }

    pub fn migrate_collection_schema(
        &mut self,
        name: impl AsRef<[u8]>,
        expected_version: u32,
        next_version: u32,
        codec: impl AsRef<[u8]>,
    ) -> Result<bool, CollectionError> {
        let name = name.as_ref();
        let key = meta_key(name)?;
        let current = self.get(&key).ok_or(CollectionError::CollectionNotFound)?;
        let definition = decode_collection_definition(current)?;
        if definition.schema_version != expected_version {
            return Ok(false);
        }
        if next_version <= expected_version {
            return Err(CollectionError::InvalidSchemaVersion);
        }
        let next = CollectionDefinition {
            schema_version: next_version,
            codec: codec.as_ref().to_vec(),
        };
        self.put_internal(key, encode_collection_definition(&next)?)?;
        Ok(true)
    }

    pub fn define_index(
        &mut self,
        collection: impl AsRef<[u8]>,
        definition: &IndexDefinition,
    ) -> Result<bool, CollectionError> {
        let collection = collection.as_ref();
        ensure_collection(self, collection)?;
        validate_name(&definition.name)?;
        let key = index_meta_key(collection, &definition.name)?;
        if self.get(&key).is_some() {
            return Ok(false);
        }
        self.put_internal(key, [FORMAT_VERSION, u8::from(definition.unique)])?;
        Ok(true)
    }

    pub fn upsert_record(
        &mut self,
        collection: impl AsRef<[u8]>,
        primary_key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        indexes: &[IndexValue],
    ) -> Result<(), CollectionError> {
        let mut tx = self.begin()?;
        tx.upsert_record(collection, primary_key, value, indexes)?;
        tx.commit()?;
        Ok(())
    }

    pub fn get_record(
        &self,
        collection: impl AsRef<[u8]>,
        primary_key: impl AsRef<[u8]>,
    ) -> Result<Option<CollectionRecord>, CollectionError> {
        let collection = collection.as_ref();
        let primary_key = primary_key.as_ref();
        let key = record_key(collection, primary_key)?;
        self.get(key)
            .map(|encoded| {
                let decoded = decode_record_value(encoded)?;
                Ok(CollectionRecord {
                    primary_key: primary_key.to_vec(),
                    value: decoded.value,
                    indexes: decoded.indexes,
                })
            })
            .transpose()
    }

    pub fn delete_record(
        &mut self,
        collection: impl AsRef<[u8]>,
        primary_key: impl AsRef<[u8]>,
    ) -> Result<bool, CollectionError> {
        let mut tx = self.begin()?;
        let deleted = tx.delete_record(collection, primary_key)?;
        tx.commit()?;
        Ok(deleted)
    }

    pub fn scan_collection(
        &self,
        collection: impl AsRef<[u8]>,
    ) -> Result<Vec<CollectionRecord>, CollectionError> {
        let collection = collection.as_ref();
        let prefix = record_prefix(collection)?;
        let mut records = Vec::new();
        for (key, encoded) in self.scan_prefix(&prefix) {
            let primary_key = decode_record_primary_key(&prefix, &key)?;
            let decoded = decode_record_value(&encoded)?;
            records.push(CollectionRecord {
                primary_key,
                value: decoded.value,
                indexes: decoded.indexes,
            });
        }
        Ok(records)
    }

    pub fn query_index_exact(
        &self,
        collection: impl AsRef<[u8]>,
        index: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<Vec<CollectionRecord>, CollectionError> {
        let collection = collection.as_ref();
        let index = index.as_ref();
        index_definition(self, collection, index)?;
        let prefix = index_value_prefix(collection, index, value.as_ref())?;
        let mut records = Vec::new();
        for (_, primary_key) in self.scan_prefix(prefix) {
            if let Some(record) = self.get_record(collection, &primary_key)? {
                records.push(record);
            }
        }
        records.sort_unstable_by(|left, right| left.primary_key.cmp(&right.primary_key));
        Ok(records)
    }
}

impl Transaction<'_> {
    /// Stages a collection record and all secondary-index mutations in this
    /// transaction. Unique checks see committed state plus prior staged writes.
    pub fn upsert_record(
        &mut self,
        collection: impl AsRef<[u8]>,
        primary_key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        indexes: &[IndexValue],
    ) -> Result<(), CollectionError> {
        let collection = collection.as_ref();
        let primary_key = primary_key.as_ref();
        ensure_collection_tx(self, collection)?;
        if primary_key.is_empty() {
            return Err(CollectionError::EmptyPrimaryKey);
        }

        let record_key = record_key(collection, primary_key)?;
        let existing = self
            .get_internal(&record_key)
            .map(decode_record_value)
            .transpose()?;

        for index in indexes {
            let definition = index_definition_tx(self, collection, &index.index)?;
            if definition.unique {
                let prefix = index_value_prefix(collection, &index.index, &index.value)?;
                for (_, owner) in self.scan_prefix_internal(prefix) {
                    if owner != primary_key {
                        return Err(CollectionError::UniqueIndexViolation {
                            index: index.index.clone(),
                        });
                    }
                }
            }
        }

        if let Some(existing) = existing {
            for old_index in existing.indexes {
                self.delete_internal(index_entry_key(
                    collection,
                    &old_index.index,
                    &old_index.value,
                    primary_key,
                )?)?;
            }
        }

        self.put_internal(&record_key, encode_record_value(value.as_ref(), indexes)?)?;
        for index in indexes {
            self.put_internal(
                index_entry_key(collection, &index.index, &index.value, primary_key)?,
                primary_key,
            )?;
        }
        Ok(())
    }

    /// Stages a collection record deletion and all of its secondary-index
    /// deletions in this transaction.
    pub fn delete_record(
        &mut self,
        collection: impl AsRef<[u8]>,
        primary_key: impl AsRef<[u8]>,
    ) -> Result<bool, CollectionError> {
        let collection = collection.as_ref();
        let primary_key = primary_key.as_ref();
        let key = record_key(collection, primary_key)?;
        let Some(existing) = self
            .get_internal(&key)
            .map(decode_record_value)
            .transpose()?
        else {
            return Ok(false);
        };

        self.delete_internal(key)?;
        for index in existing.indexes {
            self.delete_internal(index_entry_key(
                collection,
                &index.index,
                &index.value,
                primary_key,
            )?)?;
        }
        Ok(true)
    }
}

#[derive(Debug)]
struct StoredRecord {
    value: Vec<u8>,
    indexes: Vec<IndexValue>,
}

fn ensure_collection(store: &Store, name: &[u8]) -> Result<(), CollectionError> {
    if store.get(meta_key(name)?).is_none() {
        return Err(CollectionError::CollectionNotFound);
    }
    Ok(())
}

fn ensure_collection_tx(tx: &Transaction<'_>, name: &[u8]) -> Result<(), CollectionError> {
    if tx.get_internal(meta_key(name)?).is_none() {
        return Err(CollectionError::CollectionNotFound);
    }
    Ok(())
}

fn index_definition(
    store: &Store,
    collection: &[u8],
    index: &[u8],
) -> Result<IndexDefinition, CollectionError> {
    let encoded = store
        .get(index_meta_key(collection, index)?)
        .ok_or(CollectionError::IndexNotFound)?;
    decode_index_definition(index, encoded)
}

fn index_definition_tx(
    tx: &Transaction<'_>,
    collection: &[u8],
    index: &[u8],
) -> Result<IndexDefinition, CollectionError> {
    let encoded = tx
        .get_internal(index_meta_key(collection, index)?)
        .ok_or(CollectionError::IndexNotFound)?;
    decode_index_definition(index, encoded)
}

fn decode_index_definition(
    index: &[u8],
    encoded: &[u8],
) -> Result<IndexDefinition, CollectionError> {
    if encoded.len() != 2 || encoded[0] != FORMAT_VERSION || encoded[1] > 1 {
        return Err(CollectionError::CorruptMetadata);
    }
    Ok(IndexDefinition {
        name: index.to_vec(),
        unique: encoded[1] == 1,
    })
}

fn meta_key(name: &[u8]) -> Result<Vec<u8>, CollectionError> {
    component_key(META_PREFIX, &[name])
}

fn record_prefix(collection: &[u8]) -> Result<Vec<u8>, CollectionError> {
    component_key(RECORD_PREFIX, &[collection])
}

fn record_key(collection: &[u8], primary_key: &[u8]) -> Result<Vec<u8>, CollectionError> {
    component_key(RECORD_PREFIX, &[collection, primary_key])
}

fn index_meta_key(collection: &[u8], index: &[u8]) -> Result<Vec<u8>, CollectionError> {
    component_key(INDEX_META_PREFIX, &[collection, index])
}

fn index_value_prefix(
    collection: &[u8],
    index: &[u8],
    value: &[u8],
) -> Result<Vec<u8>, CollectionError> {
    component_key(INDEX_PREFIX, &[collection, index, value])
}

fn index_entry_key(
    collection: &[u8],
    index: &[u8],
    value: &[u8],
    primary_key: &[u8],
) -> Result<Vec<u8>, CollectionError> {
    component_key(INDEX_PREFIX, &[collection, index, value, primary_key])
}

fn component_key(prefix: &[u8], components: &[&[u8]]) -> Result<Vec<u8>, CollectionError> {
    let mut key = Vec::from(prefix);
    for component in components {
        let len = u32::try_from(component.len()).map_err(|_| CollectionError::ComponentTooLarge)?;
        key.extend_from_slice(&len.to_be_bytes());
        key.extend_from_slice(component);
    }
    Ok(key)
}

fn decode_record_primary_key(prefix: &[u8], key: &[u8]) -> Result<Vec<u8>, CollectionError> {
    if !key.starts_with(prefix) {
        return Err(CollectionError::CorruptMetadata);
    }
    let rest = &key[prefix.len()..];
    if rest.len() < 4 {
        return Err(CollectionError::CorruptMetadata);
    }
    let len = u32::from_be_bytes(rest[..4].try_into().expect("4-byte primary key length")) as usize;
    if rest.len() != 4 + len {
        return Err(CollectionError::CorruptMetadata);
    }
    Ok(rest[4..].to_vec())
}

fn encode_collection_definition(
    definition: &CollectionDefinition,
) -> Result<Vec<u8>, CollectionError> {
    let codec_len =
        u32::try_from(definition.codec.len()).map_err(|_| CollectionError::ComponentTooLarge)?;
    let mut encoded = Vec::with_capacity(9 + definition.codec.len());
    encoded.push(FORMAT_VERSION);
    encoded.extend_from_slice(&definition.schema_version.to_le_bytes());
    encoded.extend_from_slice(&codec_len.to_le_bytes());
    encoded.extend_from_slice(&definition.codec);
    Ok(encoded)
}

fn decode_collection_definition(encoded: &[u8]) -> Result<CollectionDefinition, CollectionError> {
    if encoded.len() < 9 || encoded[0] != FORMAT_VERSION {
        return Err(CollectionError::CorruptMetadata);
    }
    let schema_version = u32::from_le_bytes(encoded[1..5].try_into().expect("schema version"));
    let codec_len = u32::from_le_bytes(encoded[5..9].try_into().expect("codec length")) as usize;
    if encoded.len() != 9 + codec_len {
        return Err(CollectionError::CorruptMetadata);
    }
    Ok(CollectionDefinition {
        schema_version,
        codec: encoded[9..].to_vec(),
    })
}

fn encode_record_value(value: &[u8], indexes: &[IndexValue]) -> Result<Vec<u8>, CollectionError> {
    let value_len = u32::try_from(value.len()).map_err(|_| CollectionError::ComponentTooLarge)?;
    let index_count = u16::try_from(indexes.len()).map_err(|_| CollectionError::TooManyIndexes)?;
    let mut encoded = Vec::new();
    encoded.push(FORMAT_VERSION);
    encoded.extend_from_slice(&value_len.to_le_bytes());
    encoded.extend_from_slice(&index_count.to_le_bytes());
    encoded.extend_from_slice(value);
    for index in indexes {
        let name_len =
            u16::try_from(index.index.len()).map_err(|_| CollectionError::ComponentTooLarge)?;
        let value_len =
            u32::try_from(index.value.len()).map_err(|_| CollectionError::ComponentTooLarge)?;
        encoded.extend_from_slice(&name_len.to_le_bytes());
        encoded.extend_from_slice(&index.index);
        encoded.extend_from_slice(&value_len.to_le_bytes());
        encoded.extend_from_slice(&index.value);
    }
    Ok(encoded)
}

fn decode_record_value(encoded: &[u8]) -> Result<StoredRecord, CollectionError> {
    if encoded.len() < 7 || encoded[0] != FORMAT_VERSION {
        return Err(CollectionError::CorruptRecord);
    }
    let value_len =
        u32::from_le_bytes(encoded[1..5].try_into().expect("record value length")) as usize;
    let index_count = u16::from_le_bytes(encoded[5..7].try_into().expect("index count")) as usize;
    let value_end = 7usize
        .checked_add(value_len)
        .ok_or(CollectionError::CorruptRecord)?;
    if value_end > encoded.len() {
        return Err(CollectionError::CorruptRecord);
    }
    let value = encoded[7..value_end].to_vec();
    let mut cursor = value_end;
    let mut indexes = Vec::with_capacity(index_count);
    for _ in 0..index_count {
        if cursor + 2 > encoded.len() {
            return Err(CollectionError::CorruptRecord);
        }
        let name_len = u16::from_le_bytes(
            encoded[cursor..cursor + 2]
                .try_into()
                .expect("index name length"),
        ) as usize;
        cursor += 2;
        let name_end = cursor
            .checked_add(name_len)
            .ok_or(CollectionError::CorruptRecord)?;
        if name_end + 4 > encoded.len() {
            return Err(CollectionError::CorruptRecord);
        }
        let name = encoded[cursor..name_end].to_vec();
        cursor = name_end;
        let index_value_len = u32::from_le_bytes(
            encoded[cursor..cursor + 4]
                .try_into()
                .expect("index value length"),
        ) as usize;
        cursor += 4;
        let index_value_end = cursor
            .checked_add(index_value_len)
            .ok_or(CollectionError::CorruptRecord)?;
        if index_value_end > encoded.len() {
            return Err(CollectionError::CorruptRecord);
        }
        indexes.push(IndexValue {
            index: name,
            value: encoded[cursor..index_value_end].to_vec(),
        });
        cursor = index_value_end;
    }
    if cursor != encoded.len() {
        return Err(CollectionError::CorruptRecord);
    }
    Ok(StoredRecord { value, indexes })
}

fn validate_name(name: &[u8]) -> Result<(), CollectionError> {
    if name.is_empty() {
        Err(CollectionError::EmptyName)
    } else {
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum CollectionError {
    #[error("store error: {0}")]
    Store(#[from] EngineError),
    #[error("collection does not exist")]
    CollectionNotFound,
    #[error("index does not exist")]
    IndexNotFound,
    #[error("collection/index name must not be empty")]
    EmptyName,
    #[error("primary key must not be empty")]
    EmptyPrimaryKey,
    #[error("collection component is too large")]
    ComponentTooLarge,
    #[error("record has too many indexes")]
    TooManyIndexes,
    #[error("schema version must increase")]
    InvalidSchemaVersion,
    #[error("collection metadata is corrupt or unsupported")]
    CorruptMetadata,
    #[error("collection record is corrupt or unsupported")]
    CorruptRecord,
    #[error("unique index violation")]
    UniqueIndexViolation { index: Vec<u8> },
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
        std::env::temp_dir().join(format!("voodoo-store-collection-{name}-{nonce}.vstore"))
    }

    fn unique_email() -> IndexDefinition {
        IndexDefinition {
            name: b"email".to_vec(),
            unique: true,
        }
    }

    fn email(value: &[u8]) -> IndexValue {
        IndexValue {
            index: b"email".to_vec(),
            value: value.to_vec(),
        }
    }

    #[test]
    fn collection_records_and_indexes_survive_reopen() {
        let path = temp_store_path("durable");
        {
            let mut store = Store::open(&path).unwrap();
            assert!(
                store
                    .create_collection(b"users", &CollectionDefinition::default())
                    .unwrap()
            );
            assert!(store.define_index(b"users", &unique_email()).unwrap());
            store
                .upsert_record(
                    b"users",
                    b"u1",
                    br#"{"name":"Ada"}"#,
                    &[email(b"ada@example.com")],
                )
                .unwrap();
        }
        {
            let store = Store::open(&path).unwrap();
            let record = store.get_record(b"users", b"u1").unwrap().unwrap();
            assert_eq!(record.value, br#"{"name":"Ada"}"#);
            let found = store
                .query_index_exact(b"users", b"email", b"ada@example.com")
                .unwrap();
            assert_eq!(found.len(), 1);
            assert_eq!(found[0].primary_key, b"u1");
        }
        let _ = fs::remove_file(path);
    }

    #[test]
    fn unique_index_rejects_conflicting_owner_and_update_rewrites_index() {
        let path = temp_store_path("unique");
        let mut store = Store::open(&path).unwrap();
        store
            .create_collection(b"users", &CollectionDefinition::default())
            .unwrap();
        store.define_index(b"users", &unique_email()).unwrap();
        let old = [email(b"old@example.com")];
        store.upsert_record(b"users", b"u1", b"A", &old).unwrap();
        assert!(matches!(
            store.upsert_record(b"users", b"u2", b"B", &old),
            Err(CollectionError::UniqueIndexViolation { .. })
        ));
        let next = [email(b"new@example.com")];
        store.upsert_record(b"users", b"u1", b"A2", &next).unwrap();
        assert!(
            store
                .query_index_exact(b"users", b"email", b"old@example.com")
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            store
                .query_index_exact(b"users", b"email", b"new@example.com")
                .unwrap()
                .len(),
            1
        );
        drop(store);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn schema_migration_is_compare_and_swap_like() {
        let path = temp_store_path("schema");
        let mut store = Store::open(&path).unwrap();
        store
            .create_collection(b"events", &CollectionDefinition::default())
            .unwrap();
        assert!(
            !store
                .migrate_collection_schema(b"events", 9, 10, b"cbor")
                .unwrap()
        );
        assert!(
            store
                .migrate_collection_schema(b"events", 1, 2, b"cbor")
                .unwrap()
        );
        let definition = store.collection_definition(b"events").unwrap().unwrap();
        assert_eq!(definition.schema_version, 2);
        assert_eq!(definition.codec, b"cbor");
        drop(store);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn transaction_combines_collection_job_and_kv_atomically() {
        let path = temp_store_path("cross-domain");
        let mut store = Store::open(&path).unwrap();
        store
            .create_collection(b"orders", &CollectionDefinition::default())
            .unwrap();
        store
            .define_index(
                b"orders",
                &IndexDefinition {
                    name: b"status".to_vec(),
                    unique: false,
                },
            )
            .unwrap();

        let job_id;
        {
            let mut tx = store.begin().unwrap();
            tx.put(b"counter:orders", b"1").unwrap();
            tx.upsert_record(
                b"orders",
                b"42",
                b"paid",
                &[IndexValue {
                    index: b"status".to_vec(),
                    value: b"paid".to_vec(),
                }],
            )
            .unwrap();
            job_id = tx
                .enqueue_job(JobSpec::new(b"receipt", b"42"), 100)
                .unwrap();
            tx.commit().unwrap();
        }

        assert_eq!(store.get(b"counter:orders"), Some(b"1".as_slice()));
        assert_eq!(
            store.get_record(b"orders", b"42").unwrap().unwrap().value,
            b"paid"
        );
        assert!(store.get_job(&job_id).unwrap().is_some());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn transaction_unique_index_sees_prior_staged_writes() {
        let path = temp_store_path("staged-unique");
        let mut store = Store::open(&path).unwrap();
        store
            .create_collection(b"users", &CollectionDefinition::default())
            .unwrap();
        store.define_index(b"users", &unique_email()).unwrap();

        let mut tx = store.begin().unwrap();
        tx.upsert_record(b"users", b"u1", b"A", &[email(b"same@example.com")])
            .unwrap();
        assert!(matches!(
            tx.upsert_record(b"users", b"u2", b"B", &[email(b"same@example.com")]),
            Err(CollectionError::UniqueIndexViolation { .. })
        ));
        tx.rollback().unwrap();
        assert!(store.scan_collection(b"users").unwrap().is_empty());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn staged_index_delete_releases_unique_value_inside_same_transaction() {
        let path = temp_store_path("release-unique");
        let mut store = Store::open(&path).unwrap();
        store
            .create_collection(b"users", &CollectionDefinition::default())
            .unwrap();
        store.define_index(b"users", &unique_email()).unwrap();
        store
            .upsert_record(b"users", b"u1", b"A", &[email(b"old@example.com")])
            .unwrap();

        let mut tx = store.begin().unwrap();
        tx.upsert_record(b"users", b"u1", b"A2", &[email(b"new@example.com")])
            .unwrap();
        tx.upsert_record(b"users", b"u2", b"B", &[email(b"old@example.com")])
            .unwrap();
        tx.commit().unwrap();

        assert_eq!(
            store
                .query_index_exact(b"users", b"email", b"old@example.com")
                .unwrap()[0]
                .primary_key,
            b"u2"
        );
        let _ = fs::remove_file(path);
    }
}
