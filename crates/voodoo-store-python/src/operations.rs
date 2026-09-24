use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use voodoo_store_core::{HealthReport, OperationsError, StorageStats};

use super::{PyStore, VoodooStoreError, with_store};

fn map_operations_error(error: OperationsError) -> PyErr {
    VoodooStoreError::new_err(error.to_string())
}

fn py_storage_stats(py: Python<'_>, stats: StorageStats) -> PyResult<Py<PyDict>> {
    let result = PyDict::new(py);
    result.set_item("file_bytes", stats.file_bytes)?;
    result.set_item("live_keys", stats.live_keys)?;
    result.set_item("user_keys", stats.user_keys)?;
    result.set_item("internal_keys", stats.internal_keys)?;
    result.set_item("key_bytes", stats.key_bytes)?;
    result.set_item("value_bytes", stats.value_bytes)?;
    result.set_item("live_bytes", stats.live_bytes())?;
    result.set_item("amplification_ratio", stats.amplification_ratio())?;
    Ok(result.unbind())
}

fn py_health_report(py: Python<'_>, report: HealthReport) -> PyResult<Py<PyDict>> {
    let result = PyDict::new(py);
    result.set_item("store_id", PyBytes::new(py, &report.store_id))?;
    result.set_item("format_major", report.format_major)?;
    result.set_item("format_minor", report.format_minor)?;
    result.set_item("storage", py_storage_stats(py, report.storage)?)?;

    let namespaces = report
        .namespaces
        .into_iter()
        .map(|namespace| {
            let item = PyDict::new(py);
            item.set_item("name", namespace.name)?;
            item.set_item("keys", namespace.keys)?;
            item.set_item("value_bytes", namespace.value_bytes)?;
            Ok(item.unbind())
        })
        .collect::<PyResult<Vec<Py<PyDict>>>>()?;
    result.set_item("namespaces", namespaces)?;
    Ok(result.unbind())
}

#[pymethods]
impl PyStore {
    fn storage_stats(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        with_store(&self.slot, |store| {
            let stats = store.storage_stats().map_err(map_operations_error)?;
            py_storage_stats(py, stats)
        })
    }

    fn health_report(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        with_store(&self.slot, |store| {
            let report = store.health_report().map_err(map_operations_error)?;
            py_health_report(py, report)
        })
    }
}
