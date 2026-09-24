use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use voodoo_store_core::{OutboxError, OutboxEvent, OutboxEventId};

use super::{PyStore, VoodooStoreError, with_store, with_store_mut};

fn map_error(error: OutboxError) -> PyErr {
    VoodooStoreError::new_err(error.to_string())
}

fn parse_id(tx_id: u64, nonce: &[u8]) -> PyResult<OutboxEventId> {
    let nonce = nonce
        .try_into()
        .map_err(|_| VoodooStoreError::new_err("outbox nonce must be exactly 16 bytes"))?;
    Ok(OutboxEventId { tx_id, nonce })
}

fn py_id(py: Python<'_>, id: OutboxEventId) -> (u64, Py<PyBytes>) {
    (id.tx_id, PyBytes::new(py, &id.nonce).unbind())
}

fn py_event(py: Python<'_>, event: OutboxEvent) -> PyResult<Py<PyDict>> {
    let result = PyDict::new(py);
    result.set_item("id", py_id(py, event.id))?;
    result.set_item("tx_id", event.id.tx_id)?;
    result.set_item("nonce", PyBytes::new(py, &event.id.nonce))?;
    result.set_item("topic", PyBytes::new(py, &event.topic))?;
    result.set_item("payload", PyBytes::new(py, &event.payload))?;
    result.set_item("created_at_ms", event.created_at_ms)?;
    Ok(result.unbind())
}

#[pymethods]
impl PyStore {
    fn emit_outbox_event(
        &self,
        py: Python<'_>,
        topic: &[u8],
        payload: &[u8],
        created_at_ms: i64,
    ) -> PyResult<(u64, Py<PyBytes>)> {
        with_store_mut(&self.slot, |store| {
            store
                .emit_outbox_event(topic, payload, created_at_ms)
                .map(|id| py_id(py, id))
                .map_err(map_error)
        })
    }

    #[pyo3(signature = (after_tx_id = None, limit = 100))]
    fn outbox_events_after(
        &self,
        py: Python<'_>,
        after_tx_id: Option<u64>,
        limit: usize,
    ) -> PyResult<Vec<Py<PyDict>>> {
        with_store(&self.slot, |store| {
            store
                .outbox_events_after(after_tx_id, limit)
                .map_err(map_error)?
                .into_iter()
                .map(|event| py_event(py, event))
                .collect()
        })
    }

    fn ack_outbox_event(&self, tx_id: u64, nonce: &[u8]) -> PyResult<bool> {
        let id = parse_id(tx_id, nonce)?;
        with_store_mut(&self.slot, |store| {
            store.ack_outbox_event(id).map_err(map_error)
        })
    }

    fn outbox_len(&self) -> PyResult<usize> {
        with_store(&self.slot, |store| Ok(store.outbox_len()))
    }
}
