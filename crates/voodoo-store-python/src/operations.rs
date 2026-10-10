use std::path::PathBuf;

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use voodoo_store_core::{
    CheckpointError, CheckpointReport, CompactionReport, GenerationError, GenerationReport,
    HealthReport, OperationsError, RestoreReport, SnapshotError, SnapshotReport, StorageStats,
    Store as CoreStore,
};

use super::{PyStore, VoodooStoreError, map_engine_error, with_store};

fn map_operations_error(error: OperationsError) -> PyErr {
    VoodooStoreError::new_err(error.to_string())
}

fn map_checkpoint_error(error: CheckpointError) -> PyErr {
    VoodooStoreError::new_err(error.to_string())
}

fn map_snapshot_error(error: SnapshotError) -> PyErr {
    VoodooStoreError::new_err(error.to_string())
}

fn map_generation_error(error: GenerationError) -> PyErr {
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

fn py_health(py: Python<'_>, report: HealthReport) -> PyResult<Py<PyDict>> {
    let result = PyDict::new(py);
    result.set_item("store_id", PyBytes::new(py, &report.store_id))?;
    result.set_item("format_major", report.format_major)?;
    result.set_item("format_minor", report.format_minor)?;
    result.set_item("max_bytes", report.max_bytes)?;
    result.set_item("namespace_max_bytes", report.namespace_max_bytes)?;
    result.set_item("compaction_backlog_bytes", report.compaction_backlog_bytes)?;
    result.set_item("lock_contentions_total", report.lock_contentions_total)?;
    result.set_item("object_reads_total", report.object_reads_total)?;
    result.set_item("object_bytes_read_total", report.object_bytes_read_total)?;
    result.set_item("queue_ready", report.queue_ready)?;
    result.set_item("queue_leased", report.queue_leased)?;
    result.set_item("queue_dead", report.queue_dead)?;
    result.set_item("queue_total", report.queue_total)?;
    result.set_item("queue_oldest_ready_at_ms", report.queue_oldest_ready_at_ms)?;
    result.set_item("queue_ready_lag_ms", report.queue_ready_lag_ms)?;
    let metrics = PyDict::new(py);
    metrics.set_item("commits", report.metrics.commits)?;
    metrics.set_item("puts", report.metrics.puts)?;
    metrics.set_item("deletes", report.metrics.deletes)?;
    metrics.set_item("records_appended", report.metrics.records_appended)?;
    metrics.set_item("bytes_appended", report.metrics.bytes_appended)?;
    metrics.set_item("recovery_records", report.metrics.recovery_records)?;
    metrics.set_item(
        "commit_latency_total_us",
        report.metrics.commit_latency_total_us,
    )?;
    metrics.set_item(
        "commit_latency_max_us",
        report.metrics.commit_latency_max_us,
    )?;
    metrics.set_item(
        "fsync_latency_total_us",
        report.metrics.fsync_latency_total_us,
    )?;
    metrics.set_item("fsync_latency_max_us", report.metrics.fsync_latency_max_us)?;
    metrics.set_item("commit_errors", report.metrics.commit_errors)?;
    metrics.set_item("queue_claims", report.metrics.queue_claims)?;
    metrics.set_item("queue_acks", report.metrics.queue_acks)?;
    metrics.set_item("object_puts", report.metrics.object_puts)?;
    metrics.set_item("object_bytes_written", report.metrics.object_bytes_written)?;
    metrics.set_item("object_gc_runs", report.metrics.object_gc_runs)?;
    metrics.set_item("object_gc_removed", report.metrics.object_gc_removed)?;
    result.set_item("metrics", metrics)?;
    result.set_item("within_limit", report.within_limit)?;
    result.set_item("storage", py_storage_stats(py, report.storage)?)?;

    let namespaces = PyList::empty(py);
    for namespace in report.namespaces {
        let item = PyDict::new(py);
        item.set_item("name", namespace.name)?;
        item.set_item("keys", namespace.keys)?;
        item.set_item("value_bytes", namespace.value_bytes)?;
        namespaces.append(item)?;
    }
    result.set_item("namespaces", namespaces)?;
    Ok(result.unbind())
}

fn py_checkpoint(py: Python<'_>, report: CheckpointReport) -> PyResult<Py<PyDict>> {
    let result = PyDict::new(py);
    result.set_item("source_store_id", PyBytes::new(py, &report.source_store_id))?;
    result.set_item(
        "checkpoint_store_id",
        PyBytes::new(py, &report.checkpoint_store_id),
    )?;
    result.set_item("file_bytes", report.file_bytes)?;
    result.set_item("valid_bytes", report.valid_bytes)?;
    result.set_item("keys", report.keys)?;
    result.set_item("committed_transactions", report.committed_transactions)?;
    result.set_item("pending_transactions", report.pending_transactions)?;
    Ok(result.unbind())
}

fn py_compaction(py: Python<'_>, report: CompactionReport) -> PyResult<Py<PyDict>> {
    let result = PyDict::new(py);
    result.set_item("source_bytes", report.source_bytes)?;
    result.set_item("compacted_bytes", report.compacted_bytes)?;
    result.set_item("bytes_reclaimed", report.bytes_reclaimed())?;
    result.set_item("keys", report.keys)?;
    Ok(result.unbind())
}

fn py_restore(py: Python<'_>, report: RestoreReport) -> PyResult<Py<PyDict>> {
    let result = PyDict::new(py);
    result.set_item("bytes", report.bytes)?;
    result.set_item("keys", report.keys)?;
    result.set_item("store_id", PyBytes::new(py, &report.store_id))?;
    Ok(result.unbind())
}

fn py_snapshot(py: Python<'_>, report: SnapshotReport) -> PyResult<Py<PyDict>> {
    let result = PyDict::new(py);
    result.set_item("destination", report.destination)?;
    result.set_item("source_store_id", PyBytes::new(py, &report.source_store_id))?;
    result.set_item(
        "snapshot_store_id",
        PyBytes::new(py, &report.snapshot_store_id),
    )?;
    result.set_item("keys", report.keys)?;
    result.set_item("source_bytes", report.source_bytes)?;
    result.set_item("snapshot_bytes", report.snapshot_bytes)?;
    Ok(result.unbind())
}

fn py_generation(py: Python<'_>, report: GenerationReport) -> PyResult<Py<PyDict>> {
    let result = PyDict::new(py);
    result.set_item("store_id", PyBytes::new(py, &report.store_id))?;
    result.set_item("source_bytes", report.source_bytes)?;
    result.set_item("generation_bytes", report.generation_bytes)?;
    result.set_item("bytes_reclaimed", report.bytes_reclaimed())?;
    result.set_item("keys", report.keys)?;
    result.set_item("high_water_tx_id", report.high_water_tx_id)?;
    result.set_item("high_water_sequence", report.high_water_sequence)?;
    Ok(result.unbind())
}

#[pymethods]
impl PyStore {
    fn storage_stats(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        with_store(&self.slot, |store| {
            store
                .storage_stats()
                .map_err(map_operations_error)
                .and_then(|stats| py_storage_stats(py, stats))
        })
    }

    fn health_report(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        with_store(&self.slot, |store| {
            store
                .health_report()
                .map_err(map_operations_error)
                .and_then(|report| py_health(py, report))
        })
    }

    fn backup_to(&self, destination: PathBuf) -> PyResult<u64> {
        with_store(&self.slot, |store| {
            store.backup_to(destination).map_err(map_engine_error)
        })
    }

    #[staticmethod]
    fn restore_copy(py: Python<'_>, source: PathBuf, destination: PathBuf) -> PyResult<Py<PyDict>> {
        CoreStore::restore_copy(source, destination)
            .map_err(map_engine_error)
            .and_then(|report| py_restore(py, report))
    }

    fn checkpoint_to(&self, py: Python<'_>, destination: PathBuf) -> PyResult<Py<PyDict>> {
        with_store(&self.slot, |store| {
            store
                .checkpoint_to(destination)
                .map_err(map_checkpoint_error)
                .and_then(|report| py_checkpoint(py, report))
        })
    }

    fn compact_copy_to(&self, py: Python<'_>, destination: PathBuf) -> PyResult<Py<PyDict>> {
        with_store(&self.slot, |store| {
            store
                .compact_copy_to(destination)
                .map_err(map_engine_error)
                .and_then(|report| py_compaction(py, report))
        })
    }

    fn snapshot_to(&self, py: Python<'_>, destination: PathBuf) -> PyResult<Py<PyDict>> {
        with_store(&self.slot, |store| {
            store
                .snapshot_to(destination)
                .map_err(map_snapshot_error)
                .and_then(|report| py_snapshot(py, report))
        })
    }

    fn compact_generation_to(&self, py: Python<'_>, destination: PathBuf) -> PyResult<Py<PyDict>> {
        with_store(&self.slot, |store| {
            store
                .compact_generation_to(destination)
                .map_err(map_generation_error)
                .and_then(|report| py_generation(py, report))
        })
    }
}
