use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use voodoo_store_core::{ChangeFeedError, ChangeKind, ChangeRecord};

use super::{PyStore, VoodooStoreError, with_store, with_store_mut};

fn map_error(error: ChangeFeedError) -> PyErr {
    VoodooStoreError::new_err(error.to_string())
}

fn py_change(py: Python<'_>, change: ChangeRecord) -> PyResult<Py<PyDict>> {
    let result = PyDict::new(py);
    result.set_item("tx_id", change.tx_id)?;
    result.set_item("sequence", change.sequence)?;
    result.set_item(
        "kind",
        match change.kind {
            ChangeKind::Put => "put",
            ChangeKind::Delete => "delete",
        },
    )?;
    result.set_item("key", PyBytes::new(py, &change.key))?;
    result.set_item("value_len", change.value_len)?;
    match change.value_sha256 {
        Some(hash) => result.set_item("value_sha256", PyBytes::new(py, &hash))?,
        None => result.set_item("value_sha256", py.None())?,
    }
    Ok(result.unbind())
}

fn py_changes(py: Python<'_>, changes: Vec<ChangeRecord>) -> PyResult<Vec<Py<PyDict>>> {
    changes
        .into_iter()
        .map(|change| py_change(py, change))
        .collect()
}

#[pymethods]
impl PyStore {
    #[pyo3(signature = (after_tx_id = None, limit = 100))]
    fn changes_after(
        &self,
        py: Python<'_>,
        after_tx_id: Option<u64>,
        limit: usize,
    ) -> PyResult<Vec<Py<PyDict>>> {
        with_store(&self.slot, |store| {
            store
                .changes_after(after_tx_id, limit)
                .map_err(map_error)
                .and_then(|changes| py_changes(py, changes))
        })
    }

    fn changes_for_transaction(&self, py: Python<'_>, tx_id: u64) -> PyResult<Vec<Py<PyDict>>> {
        with_store(&self.slot, |store| {
            store
                .changes_for_transaction(tx_id)
                .map_err(map_error)
                .and_then(|changes| py_changes(py, changes))
        })
    }

    #[pyo3(signature = (tx_id, limit = 100))]
    fn prune_changes_through(&self, tx_id: u64, limit: usize) -> PyResult<usize> {
        with_store_mut(&self.slot, |store| {
            store.prune_changes_through(tx_id, limit).map_err(map_error)
        })
    }
}
