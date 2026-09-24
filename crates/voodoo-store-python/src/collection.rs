use pyo3::prelude::*;
use pyo3::types::PyBytes;
use voodoo_store_core::{
    CollectionDefinition, CollectionError, CollectionRecord, IndexDefinition, IndexRangeQuery,
    IndexValue, QueryBound, QueryError, QueryOrder,
};

use super::{
    PendingOperation, PyStore, PyTransaction, VoodooStoreError, with_store, with_store_mut,
};

type PyRecord = (Py<PyBytes>, Py<PyBytes>, Vec<(Py<PyBytes>, Py<PyBytes>)>);
type PyIndexedRecord = (Py<PyBytes>, PyRecord);

fn map_collection_error(error: CollectionError) -> PyErr {
    VoodooStoreError::new_err(error.to_string())
}

fn map_query_error(error: QueryError) -> PyErr {
    VoodooStoreError::new_err(error.to_string())
}

fn py_record(py: Python<'_>, record: CollectionRecord) -> PyRecord {
    let indexes = record
        .indexes
        .into_iter()
        .map(|index| {
            (
                PyBytes::new(py, &index.index).unbind(),
                PyBytes::new(py, &index.value).unbind(),
            )
        })
        .collect();
    (
        PyBytes::new(py, &record.primary_key).unbind(),
        PyBytes::new(py, &record.value).unbind(),
        indexes,
    )
}

fn core_indexes(indexes: Vec<(Vec<u8>, Vec<u8>)>) -> Vec<IndexValue> {
    indexes
        .into_iter()
        .map(|(index, value)| IndexValue { index, value })
        .collect()
}

#[pymethods]
impl PyStore {
    #[pyo3(signature = (name, *, schema_version = 1, codec = None))]
    fn create_collection(
        &self,
        name: &[u8],
        schema_version: u32,
        codec: Option<Vec<u8>>,
    ) -> PyResult<bool> {
        let definition = CollectionDefinition {
            schema_version,
            codec: codec.unwrap_or_else(|| b"bytes".to_vec()),
        };
        with_store_mut(&self.slot, |store| {
            store
                .create_collection(name, &definition)
                .map_err(map_collection_error)
        })
    }

    fn collection_definition(
        &self,
        py: Python<'_>,
        name: &[u8],
    ) -> PyResult<Option<(u32, Py<PyBytes>)>> {
        with_store(&self.slot, |store| {
            store
                .collection_definition(name)
                .map(|definition| {
                    definition.map(|definition| {
                        (
                            definition.schema_version,
                            PyBytes::new(py, &definition.codec).unbind(),
                        )
                    })
                })
                .map_err(map_collection_error)
        })
    }

    #[pyo3(signature = (collection, name, *, unique = false))]
    fn define_index(&self, collection: &[u8], name: &[u8], unique: bool) -> PyResult<bool> {
        let definition = IndexDefinition {
            name: name.to_vec(),
            unique,
        };
        with_store_mut(&self.slot, |store| {
            store
                .define_index(collection, &definition)
                .map_err(map_collection_error)
        })
    }

    #[pyo3(signature = (collection, primary_key, value, *, indexes = Vec::new()))]
    fn upsert_record(
        &self,
        collection: &[u8],
        primary_key: &[u8],
        value: &[u8],
        indexes: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> PyResult<()> {
        let indexes = core_indexes(indexes);
        with_store_mut(&self.slot, |store| {
            store
                .upsert_record(collection, primary_key, value, &indexes)
                .map_err(map_collection_error)
        })
    }

    fn get_record(
        &self,
        py: Python<'_>,
        collection: &[u8],
        primary_key: &[u8],
    ) -> PyResult<Option<PyRecord>> {
        with_store(&self.slot, |store| {
            store
                .get_record(collection, primary_key)
                .map(|record| record.map(|record| py_record(py, record)))
                .map_err(map_collection_error)
        })
    }

    fn delete_record(&self, collection: &[u8], primary_key: &[u8]) -> PyResult<bool> {
        with_store_mut(&self.slot, |store| {
            store
                .delete_record(collection, primary_key)
                .map_err(map_collection_error)
        })
    }

    fn scan_collection(&self, py: Python<'_>, collection: &[u8]) -> PyResult<Vec<PyRecord>> {
        with_store(&self.slot, |store| {
            store
                .scan_collection(collection)
                .map(|records| {
                    records
                        .into_iter()
                        .map(|record| py_record(py, record))
                        .collect()
                })
                .map_err(map_collection_error)
        })
    }

    fn query_index_exact(
        &self,
        py: Python<'_>,
        collection: &[u8],
        index: &[u8],
        value: &[u8],
    ) -> PyResult<Vec<PyRecord>> {
        with_store(&self.slot, |store| {
            store
                .query_index_exact(collection, index, value)
                .map(|records| {
                    records
                        .into_iter()
                        .map(|record| py_record(py, record))
                        .collect()
                })
                .map_err(map_collection_error)
        })
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        collection,
        index,
        *,
        start = None,
        end = None,
        start_inclusive = true,
        end_inclusive = true,
        descending = false,
        limit = None
    ))]
    fn query_index_range(
        &self,
        py: Python<'_>,
        collection: &[u8],
        index: &[u8],
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
        start_inclusive: bool,
        end_inclusive: bool,
        descending: bool,
        limit: Option<usize>,
    ) -> PyResult<Vec<PyIndexedRecord>> {
        let start = match start {
            Some(value) if start_inclusive => QueryBound::Included(value),
            Some(value) => QueryBound::Excluded(value),
            None => QueryBound::Unbounded,
        };
        let end = match end {
            Some(value) if end_inclusive => QueryBound::Included(value),
            Some(value) => QueryBound::Excluded(value),
            None => QueryBound::Unbounded,
        };
        let query = IndexRangeQuery {
            start,
            end,
            order: if descending {
                QueryOrder::Descending
            } else {
                QueryOrder::Ascending
            },
            limit,
        };

        with_store(&self.slot, |store| {
            store
                .query_index_range(collection, index, &query)
                .map(|records| {
                    records
                        .into_iter()
                        .map(|indexed| {
                            (
                                PyBytes::new(py, &indexed.index_value).unbind(),
                                py_record(py, indexed.record),
                            )
                        })
                        .collect()
                })
                .map_err(map_query_error)
        })
    }
}

#[pymethods]
impl PyTransaction {
    #[pyo3(signature = (collection, primary_key, value, *, indexes = Vec::new()))]
    fn upsert_record(
        &mut self,
        collection: &[u8],
        primary_key: &[u8],
        value: &[u8],
        indexes: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> PyResult<()> {
        self.ensure_open()?;
        self.operations.push(PendingOperation::UpsertRecord {
            collection: collection.to_vec(),
            primary_key: primary_key.to_vec(),
            value: value.to_vec(),
            indexes: core_indexes(indexes),
        });
        Ok(())
    }
}
