use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use voodoo_store_core::{PushOptions, QueueError, QueueMessage, QueueState};

use super::{PyStore, VoodooStoreError, with_store_mut};

fn map_error(error: QueueError) -> PyErr {
    VoodooStoreError::new_err(error.to_string())
}

fn state_name(state: QueueState) -> &'static str {
    match state {
        QueueState::Ready => "ready",
        QueueState::Leased => "leased",
        QueueState::Dead => "dead",
    }
}

fn py_message(py: Python<'_>, message: QueueMessage) -> PyResult<Py<PyDict>> {
    let result = PyDict::new(py);
    result.set_item("id", message.id)?;
    result.set_item("payload", PyBytes::new(py, &message.payload))?;
    result.set_item("attempts", message.attempts)?;
    result.set_item("priority", message.priority)?;
    result.set_item("available_at_ms", message.available_at_ms)?;
    result.set_item("lease_until_ms", message.lease_until_ms)?;
    result.set_item("lease_generation", message.lease_generation)?;
    Ok(result.unbind())
}

#[pymethods]
impl PyStore {
    #[pyo3(signature = (name, payload, *, available_at_ms = 0, priority = 0))]
    fn push_queue(
        &self,
        name: &[u8],
        payload: &[u8],
        available_at_ms: i64,
        priority: i32,
    ) -> PyResult<u64> {
        with_store_mut(&self.slot, |store| {
            let mut queue = store.queue(name).map_err(map_error)?;
            queue
                .push_with_options(
                    payload,
                    PushOptions {
                        available_at_ms,
                        priority,
                    },
                )
                .map_err(map_error)
        })
    }

    fn claim_queue(
        &self,
        py: Python<'_>,
        name: &[u8],
        now_ms: i64,
        lease_duration_ms: u64,
    ) -> PyResult<Option<Py<PyDict>>> {
        with_store_mut(&self.slot, |store| {
            let mut queue = store.queue(name).map_err(map_error)?;
            queue
                .claim(now_ms, lease_duration_ms)
                .map_err(map_error)?
                .map(|message| py_message(py, message))
                .transpose()
        })
    }

    fn ack_queue(
        &self,
        name: &[u8],
        id: u64,
        lease_generation: u32,
    ) -> PyResult<()> {
        with_store_mut(&self.slot, |store| {
            let mut queue = store.queue(name).map_err(map_error)?;
            queue.ack(id, lease_generation).map_err(map_error)
        })
    }

    fn nack_queue(
        &self,
        name: &[u8],
        id: u64,
        lease_generation: u32,
        available_at_ms: i64,
    ) -> PyResult<()> {
        with_store_mut(&self.slot, |store| {
            let mut queue = store.queue(name).map_err(map_error)?;
            queue
                .nack(id, lease_generation, available_at_ms)
                .map_err(map_error)
        })
    }

    fn dead_letter_queue(
        &self,
        name: &[u8],
        id: u64,
        lease_generation: u32,
    ) -> PyResult<()> {
        with_store_mut(&self.slot, |store| {
            let mut queue = store.queue(name).map_err(map_error)?;
            queue
                .dead_letter(id, lease_generation)
                .map_err(map_error)
        })
    }

    fn queue_stats(&self, py: Python<'_>, name: &[u8]) -> PyResult<Py<PyDict>> {
        with_store_mut(&self.slot, |store| {
            let queue = store.queue(name).map_err(map_error)?;
            let stats = queue.stats().map_err(map_error)?;
            let result = PyDict::new(py);
            result.set_item("ready", stats.ready)?;
            result.set_item("leased", stats.leased)?;
            result.set_item("dead", stats.dead)?;
            result.set_item("total", stats.total)?;
            Ok(result.unbind())
        })
    }

    fn purge_dead_queue(&self, name: &[u8]) -> PyResult<u64> {
        with_store_mut(&self.slot, |store| {
            let mut queue = store.queue(name).map_err(map_error)?;
            queue.purge_dead().map_err(map_error)
        })
    }
}
