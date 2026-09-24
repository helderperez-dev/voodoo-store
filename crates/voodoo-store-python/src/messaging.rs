use pyo3::prelude::*;
use pyo3::types::PyBytes;
use voodoo_store_core::MessagingError;

use super::{PyStore, VoodooStoreError, with_store_mut};

type PyStreamEntry = (u64, Py<PyBytes>);

fn map_messaging_error(error: MessagingError) -> PyErr {
    VoodooStoreError::new_err(error.to_string())
}

fn py_entries(py: Python<'_>, entries: Vec<voodoo_store_core::StreamEntry>) -> Vec<PyStreamEntry> {
    entries
        .into_iter()
        .map(|entry| (entry.offset, PyBytes::new(py, &entry.payload).unbind()))
        .collect()
}

#[pymethods]
impl PyStore {
    fn append_stream(&self, name: &[u8], payload: &[u8]) -> PyResult<u64> {
        with_store_mut(&self.slot, |store| {
            let mut stream = store.stream(name).map_err(map_messaging_error)?;
            stream.append(payload).map_err(map_messaging_error)
        })
    }

    #[pyo3(signature = (name, offset = 0, limit = 100))]
    fn read_stream(
        &self,
        py: Python<'_>,
        name: &[u8],
        offset: u64,
        limit: usize,
    ) -> PyResult<Vec<PyStreamEntry>> {
        with_store_mut(&self.slot, |store| {
            let stream = store.stream(name).map_err(map_messaging_error)?;
            let entries = stream
                .read_from(offset, limit)
                .map_err(map_messaging_error)?;
            Ok(py_entries(py, entries))
        })
    }

    fn stream_tail_offset(&self, name: &[u8]) -> PyResult<Option<u64>> {
        with_store_mut(&self.slot, |store| {
            let stream = store.stream(name).map_err(map_messaging_error)?;
            stream.tail_offset().map_err(map_messaging_error)
        })
    }

    fn publish_topic(&self, name: &[u8], payload: &[u8]) -> PyResult<u64> {
        with_store_mut(&self.slot, |store| {
            let mut topic = store.topic(name).map_err(map_messaging_error)?;
            topic.publish(payload).map_err(map_messaging_error)
        })
    }

    #[pyo3(signature = (name, offset = 0, limit = 100))]
    fn read_topic(
        &self,
        py: Python<'_>,
        name: &[u8],
        offset: u64,
        limit: usize,
    ) -> PyResult<Vec<PyStreamEntry>> {
        with_store_mut(&self.slot, |store| {
            let topic = store.topic(name).map_err(map_messaging_error)?;
            let entries = topic
                .read_from(offset, limit)
                .map_err(map_messaging_error)?;
            Ok(py_entries(py, entries))
        })
    }

    fn topic_subscription_offset(&self, topic: &[u8], subscription: &[u8]) -> PyResult<u64> {
        with_store_mut(&self.slot, |store| {
            let topic = store.topic(topic).map_err(map_messaging_error)?;
            topic
                .subscription_state(subscription)
                .map(|state| state.next_offset)
                .map_err(map_messaging_error)
        })
    }

    #[pyo3(signature = (topic, subscription, limit = 100))]
    fn poll_topic(
        &self,
        py: Python<'_>,
        topic: &[u8],
        subscription: &[u8],
        limit: usize,
    ) -> PyResult<Vec<PyStreamEntry>> {
        with_store_mut(&self.slot, |store| {
            let topic = store.topic(topic).map_err(map_messaging_error)?;
            let entries = topic
                .poll(subscription, limit)
                .map_err(map_messaging_error)?;
            Ok(py_entries(py, entries))
        })
    }

    fn acknowledge_topic_through(
        &self,
        topic: &[u8],
        subscription: &[u8],
        offset: u64,
    ) -> PyResult<u64> {
        with_store_mut(&self.slot, |store| {
            let mut topic = store.topic(topic).map_err(map_messaging_error)?;
            topic
                .acknowledge_through(subscription, offset)
                .map(|state| state.next_offset)
                .map_err(map_messaging_error)
        })
    }

    fn reset_topic_subscription(
        &self,
        topic: &[u8],
        subscription: &[u8],
        next_offset: u64,
    ) -> PyResult<()> {
        with_store_mut(&self.slot, |store| {
            let mut topic = store.topic(topic).map_err(map_messaging_error)?;
            topic
                .reset_subscription(subscription, next_offset)
                .map_err(map_messaging_error)
        })
    }
}
