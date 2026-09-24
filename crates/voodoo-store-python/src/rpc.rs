use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use voodoo_store_core::{RpcError, RpcRequest, RpcRequestId, RpcResponse};

use super::{PyStore, VoodooStoreError, with_store, with_store_mut};

fn map_error(error: RpcError) -> PyErr {
    VoodooStoreError::new_err(error.to_string())
}

fn parse_id(tx_id: u64, nonce: &[u8]) -> PyResult<RpcRequestId> {
    let nonce = nonce
        .try_into()
        .map_err(|_| VoodooStoreError::new_err("RPC nonce must be exactly 16 bytes"))?;
    Ok(RpcRequestId { tx_id, nonce })
}

fn py_id(py: Python<'_>, id: RpcRequestId) -> (u64, Py<PyBytes>) {
    (id.tx_id, PyBytes::new(py, &id.nonce).unbind())
}

fn py_request(py: Python<'_>, request: RpcRequest) -> PyResult<Py<PyDict>> {
    let result = PyDict::new(py);
    result.set_item("id", py_id(py, request.id))?;
    result.set_item("tx_id", request.id.tx_id)?;
    result.set_item("nonce", PyBytes::new(py, &request.id.nonce))?;
    result.set_item("method", PyBytes::new(py, &request.method))?;
    result.set_item("payload", PyBytes::new(py, &request.payload))?;
    result.set_item("created_at_ms", request.created_at_ms)?;
    result.set_item("deadline_ms", request.deadline_ms)?;
    Ok(result.unbind())
}

fn py_response(py: Python<'_>, response: RpcResponse) -> PyResult<Py<PyDict>> {
    let result = PyDict::new(py);
    result.set_item("request_id", py_id(py, response.request_id))?;
    result.set_item("tx_id", response.request_id.tx_id)?;
    result.set_item("nonce", PyBytes::new(py, &response.request_id.nonce))?;
    result.set_item("payload", PyBytes::new(py, &response.payload))?;
    result.set_item("completed_at_ms", response.completed_at_ms)?;
    result.set_item("is_error", response.is_error)?;
    Ok(result.unbind())
}

#[pymethods]
impl PyStore {
    #[pyo3(signature = (method, payload, created_at_ms, deadline_ms = None))]
    fn submit_rpc_request(
        &self,
        py: Python<'_>,
        method: &[u8],
        payload: &[u8],
        created_at_ms: i64,
        deadline_ms: Option<i64>,
    ) -> PyResult<(u64, Py<PyBytes>)> {
        with_store_mut(&self.slot, |store| {
            store
                .submit_rpc_request(method, payload, created_at_ms, deadline_ms)
                .map(|id| py_id(py, id))
                .map_err(map_error)
        })
    }

    #[pyo3(signature = (after_tx_id = None, limit = 100))]
    fn pending_rpc_requests_after(
        &self,
        py: Python<'_>,
        after_tx_id: Option<u64>,
        limit: usize,
    ) -> PyResult<Vec<Py<PyDict>>> {
        with_store(&self.slot, |store| {
            store
                .pending_rpc_requests_after(after_tx_id, limit)
                .map_err(map_error)?
                .into_iter()
                .map(|request| py_request(py, request))
                .collect()
        })
    }

    fn get_rpc_response(
        &self,
        py: Python<'_>,
        tx_id: u64,
        nonce: &[u8],
    ) -> PyResult<Option<Py<PyDict>>> {
        let id = parse_id(tx_id, nonce)?;
        with_store(&self.slot, |store| {
            store
                .get_rpc_response(id)
                .map_err(map_error)?
                .map(|response| py_response(py, response))
                .transpose()
        })
    }

    #[pyo3(signature = (tx_id, nonce, payload, completed_at_ms, is_error = false))]
    fn respond_rpc(
        &self,
        tx_id: u64,
        nonce: &[u8],
        payload: &[u8],
        completed_at_ms: i64,
        is_error: bool,
    ) -> PyResult<()> {
        let id = parse_id(tx_id, nonce)?;
        with_store_mut(&self.slot, |store| {
            store
                .respond_rpc(id, payload, completed_at_ms, is_error)
                .map_err(map_error)
        })
    }

    fn ack_rpc_response(&self, tx_id: u64, nonce: &[u8]) -> PyResult<bool> {
        let id = parse_id(tx_id, nonce)?;
        with_store_mut(&self.slot, |store| {
            store.ack_rpc_response(id).map_err(map_error)
        })
    }

    #[pyo3(signature = (now_ms, limit = 100))]
    fn expire_rpc_requests(
        &self,
        py: Python<'_>,
        now_ms: i64,
        limit: usize,
    ) -> PyResult<Py<PyDict>> {
        with_store_mut(&self.slot, |store| {
            let report = store
                .expire_rpc_requests(now_ms, limit)
                .map_err(map_error)?;
            let result = PyDict::new(py);
            result.set_item("scanned", report.scanned)?;
            result.set_item("expired", report.expired)?;
            Ok(result.unbind())
        })
    }
}
