//! Byte-oriented query primitives for structured collections.
//!
//! The first query engine intentionally prioritizes semantic correctness over
//! a new on-disk index format. Existing collection index entries are decoded,
//! filtered by byte bounds, ordered deterministically, and limited. A later
//! ordered-index format can optimize range seeks without changing this API.

use thiserror::Error;

use crate::{CollectionError, CollectionRecord, Store};

const INDEX_META_PREFIX: &[u8] = b"\xffvds:col:index-meta:";
const INDEX_PREFIX: &[u8] = b"\xffvds:col:index:";
const COLLECTION_FORMAT_VERSION: u8 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryBound {
    Unbounded,
    Included(Vec<u8>),
    Excluded(Vec<u8>),
}

impl Default for QueryBound {
    fn default() -> Self {
        Self::Unbounded
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum QueryOrder {
    #[default]
    Ascending,
    Descending,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexRangeQuery {
    pub start: QueryBound,
    pub end: QueryBound,
    pub order: QueryOrder,
    pub limit: Option<usize>,
}

impl Default for IndexRangeQuery {
    fn default() -> Self {
        Self {
            start: QueryBound::Unbounded,
            end: QueryBound::Unbounded,
            order: QueryOrder::Ascending,
            limit: None,
        }
    }
}

impl IndexRangeQuery {
    pub fn between(start: impl Into<Vec<u8>>, end: impl Into<Vec<u8>>) -> Self {
        Self {
            start: QueryBound::Included(start.into()),
            end: QueryBound::Included(end.into()),
            ..Self::default()
        }
    }

    pub fn with_order(mut self, order: QueryOrder) -> Self {
        self.order = order;
        self
    }

    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedRecord {
    pub index_value: Vec<u8>,
    pub record: CollectionRecord,
}

impl Store {
    /// Queries one secondary index using lexicographic byte ordering.
    ///
    /// This baseline scans the selected index namespace and then orders decoded
    /// values. It is correct for arbitrary bytes, but it is not yet a physical
    /// range seek. Callers should choose sortable byte encodings when numeric or
    /// domain-specific ordering is required.
    pub fn query_index_range(
        &self,
        collection: impl AsRef<[u8]>,
        index: impl AsRef<[u8]>,
        query: &IndexRangeQuery,
    ) -> Result<Vec<IndexedRecord>, QueryError> {
        let collection = collection.as_ref();
        let index = index.as_ref();
        if collection.is_empty() || index.is_empty() {
            return Err(QueryError::EmptyName);
        }
        validate_index_exists(self, collection, index)?;

        if query.limit == Some(0) {
            return Ok(Vec::new());
        }

        let prefix = component_key(INDEX_PREFIX, &[collection, index])?;
        let mut matches = Vec::new();
        for (key, owner) in self.scan_prefix(&prefix) {
            let (index_value, primary_key) = decode_index_entry(&prefix, &key)?;
            if owner != primary_key {
                return Err(QueryError::CorruptIndexEntry);
            }
            if within_bounds(&index_value, &query.start, &query.end) {
                matches.push((index_value, primary_key));
            }
        }

        matches.sort_unstable_by(|left, right| {
            left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1))
        });
        if query.order == QueryOrder::Descending {
            matches.reverse();
        }
        if let Some(limit) = query.limit {
            matches.truncate(limit);
        }

        let mut records = Vec::with_capacity(matches.len());
        for (index_value, primary_key) in matches {
            let record = self
                .get_record(collection, &primary_key)?
                .ok_or(QueryError::DanglingIndexEntry)?;
            records.push(IndexedRecord {
                index_value,
                record,
            });
        }
        Ok(records)
    }
}

fn validate_index_exists(store: &Store, collection: &[u8], index: &[u8]) -> Result<(), QueryError> {
    let key = component_key(INDEX_META_PREFIX, &[collection, index])?;
    let encoded = store.get(key).ok_or(QueryError::IndexNotFound)?;
    if encoded.len() != 2 || encoded[0] != COLLECTION_FORMAT_VERSION || encoded[1] > 1 {
        return Err(QueryError::CorruptIndexMetadata);
    }
    Ok(())
}

fn within_bounds(value: &[u8], start: &QueryBound, end: &QueryBound) -> bool {
    let after_start = match start {
        QueryBound::Unbounded => true,
        QueryBound::Included(bound) => value >= bound.as_slice(),
        QueryBound::Excluded(bound) => value > bound.as_slice(),
    };
    let before_end = match end {
        QueryBound::Unbounded => true,
        QueryBound::Included(bound) => value <= bound.as_slice(),
        QueryBound::Excluded(bound) => value < bound.as_slice(),
    };
    after_start && before_end
}

fn component_key(prefix: &[u8], components: &[&[u8]]) -> Result<Vec<u8>, QueryError> {
    let mut key = Vec::from(prefix);
    for component in components {
        let len = u32::try_from(component.len()).map_err(|_| QueryError::ComponentTooLarge)?;
        key.extend_from_slice(&len.to_be_bytes());
        key.extend_from_slice(component);
    }
    Ok(key)
}

fn decode_index_entry(prefix: &[u8], key: &[u8]) -> Result<(Vec<u8>, Vec<u8>), QueryError> {
    let mut cursor = prefix.len();
    if !key.starts_with(prefix) {
        return Err(QueryError::CorruptIndexEntry);
    }
    let index_value = read_component(key, &mut cursor)?;
    let primary_key = read_component(key, &mut cursor)?;
    if cursor != key.len() {
        return Err(QueryError::CorruptIndexEntry);
    }
    Ok((index_value, primary_key))
}

fn read_component(input: &[u8], cursor: &mut usize) -> Result<Vec<u8>, QueryError> {
    let len_end = cursor.checked_add(4).ok_or(QueryError::CorruptIndexEntry)?;
    let len_bytes = input
        .get(*cursor..len_end)
        .ok_or(QueryError::CorruptIndexEntry)?;
    let len = u32::from_be_bytes(
        len_bytes
            .try_into()
            .map_err(|_| QueryError::CorruptIndexEntry)?,
    ) as usize;
    *cursor = len_end;
    let end = cursor
        .checked_add(len)
        .ok_or(QueryError::CorruptIndexEntry)?;
    let value = input
        .get(*cursor..end)
        .ok_or(QueryError::CorruptIndexEntry)?
        .to_vec();
    *cursor = end;
    Ok(value)
}

#[derive(Debug, Error)]
pub enum QueryError {
    #[error("collection error: {0}")]
    Collection(#[from] CollectionError),
    #[error("collection and index names must not be empty")]
    EmptyName,
    #[error("index does not exist")]
    IndexNotFound,
    #[error("query component is too large")]
    ComponentTooLarge,
    #[error("index metadata is corrupt or unsupported")]
    CorruptIndexMetadata,
    #[error("index entry is corrupt")]
    CorruptIndexEntry,
    #[error("index entry points to a missing record")]
    DanglingIndexEntry,
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::{CollectionDefinition, IndexDefinition, IndexValue};

    use super::*;

    fn temp_store_path(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("voodoo-store-query-{name}-{nonce}.vstore"))
    }

    fn setup_store(path: &PathBuf) -> Store {
        let mut store = Store::open(path).unwrap();
        store
            .create_collection(b"products", &CollectionDefinition::default())
            .unwrap();
        store
            .define_index(
                b"products",
                &IndexDefinition {
                    name: b"price".to_vec(),
                    unique: false,
                },
            )
            .unwrap();
        for (id, price) in [
            (&b"a"[..], &b"010"[..]),
            (&b"b"[..], &b"020"[..]),
            (&b"c"[..], &b"020"[..]),
            (&b"d"[..], &b"030"[..]),
            (&b"e"[..], &b"040"[..]),
        ] {
            store
                .upsert_record(
                    b"products",
                    id,
                    id,
                    &[IndexValue {
                        index: b"price".to_vec(),
                        value: price.to_vec(),
                    }],
                )
                .unwrap();
        }
        store
    }

    #[test]
    fn range_query_honors_inclusive_and_exclusive_bounds() {
        let path = temp_store_path("bounds");
        let store = setup_store(&path);
        let query = IndexRangeQuery {
            start: QueryBound::Included(b"020".to_vec()),
            end: QueryBound::Excluded(b"040".to_vec()),
            ..IndexRangeQuery::default()
        };
        let result = store
            .query_index_range(b"products", b"price", &query)
            .unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].record.primary_key, b"b");
        assert_eq!(result[1].record.primary_key, b"c");
        assert_eq!(result[2].record.primary_key, b"d");
        drop(store);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn range_query_supports_descending_order_and_limit() {
        let path = temp_store_path("order-limit");
        let store = setup_store(&path);
        let query = IndexRangeQuery::between(b"010".to_vec(), b"040".to_vec())
            .with_order(QueryOrder::Descending)
            .with_limit(2);
        let result = store
            .query_index_range(b"products", b"price", &query)
            .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].index_value, b"040");
        assert_eq!(result[1].index_value, b"030");
        drop(store);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn undefined_index_is_rejected_even_when_no_entries_exist() {
        let path = temp_store_path("missing-index");
        let mut store = Store::open(&path).unwrap();
        store
            .create_collection(b"products", &CollectionDefinition::default())
            .unwrap();
        assert!(matches!(
            store.query_index_range(b"products", b"missing", &IndexRangeQuery::default()),
            Err(QueryError::IndexNotFound)
        ));
        drop(store);
        let _ = fs::remove_file(path);
    }
}
