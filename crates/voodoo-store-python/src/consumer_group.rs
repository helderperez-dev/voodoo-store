use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use voodoo_store_core::{ConsumerGroupDelivery, ConsumerGroupError, ConsumerGroupState};

use super::{PyStore, VoodooStoreError, with_store, with_store_mut};

fn map_error(error: ConsumerGroupError) -> PyErr {
    VoodooStoreError::new_err(error.to_string())
}

fn py_state(py: Python<'_>, state: ConsumerGroupState) -> PyResult<Py<PyDict>> {
    let result = PyDict::new(py);
    result.set_item("next_offset", state.next_offset)?;
    result.set_item("leased_offset", state.leased_offset)?;
    result.set_item("lease_until_ms", state.lease_until_ms)?;
    result.set_item("lease_generation", state.lease_generation)?;
    result.set_item("owner", PyBytes::new(py, &state.owner))?;
    Ok(result.unbind())
}

fn py_delivery(py: Python<'_>, delivery: ConsumerGroupDelivery) -> PyResult<Py<PyDict>> {
    let result = PyDict::new(py);
    result.set_item("offset", delivery.entry.offset)?;
    result.set_item("payload", PyBytes::new(py, &delivery.entry.payload))?;
    result.set_item("lease_generation", delivery.lease_generation)?;
    result.set_item("lease_until_ms", delivery.lease_until_ms)?;
    Ok(result.unbind())
}

#[pymethods]
impl PyStore {
    fn consumer_group_state(
        &self,
        py: Python<'_>,
        stream: &[u8],
        group: &[u8],
    ) -> PyResult<Py<PyDict>> {
        with_store(&self.slot, |store| {
            store
                .consumer_group_state(stream, group)
                .map_err(map_error)
                .and_then(|state| py_state(py, state))
        })
    }

    fn consumer_group_claim(
        &self,
        py: Python<'_>,
        stream: &[u8],
        group: &[u8],
        consumer: &[u8],
        now_ms: i64,
        lease_duration_ms: u64,
    ) -> PyResult<Option<Py<PyDict>>> {
        with_store_mut(&self.slot, |store| {
            store
                .consumer_group_claim(stream, group, consumer, now_ms, lease_duration_ms)
                .map_err(map_error)
                .and_then(|delivery| delivery.map(|item| py_delivery(py, item)).transpose())
        })
    }

    fn consumer_group_ack(
        &self,
        stream: &[u8],
        group: &[u8],
        consumer: &[u8],
        offset: u64,
        lease_generation: u64,
    ) -> PyResult<()> {
        with_store_mut(&self.slot, |store| {
            store
                .consumer_group_ack(stream, group, consumer, offset, lease_generation)
                .map_err(map_error)
        })
    }

    fn consumer_group_nack(
        &self,
        stream: &[u8],
        group: &[u8],
        consumer: &[u8],
        offset: u64,
        lease_generation: u64,
    ) -> PyResult<()> {
        with_store_mut(&self.slot, |store| {
            store
                .consumer_group_nack(stream, group, consumer, offset, lease_generation)
                .map_err(map_error)
        })
    }

    fn consumer_group_reset(&self, stream: &[u8], group: &[u8], next_offset: u64) -> PyResult<()> {
        with_store_mut(&self.slot, |store| {
            store
                .consumer_group_reset(stream, group, next_offset)
                .map_err(map_error)
        })
    }
}
