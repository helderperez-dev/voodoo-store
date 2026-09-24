use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use voodoo_store_core::{ObjectError, ObjectId};

use super::{PyStore, VoodooStoreError, with_store, with_store_mut};

fn map_object_error(error: ObjectError) -> PyErr {
    VoodooStoreError::new_err(error.to_string())
}

fn parse_object_id(id: &[u8]) -> PyResult<ObjectId> {
    id.try_into()
        .map_err(|_| VoodooStoreError::new_err("object id must be exactly 32 bytes"))
}

#[pymethods]
impl PyStore {
    fn put_object(&self, py: Python<'_>, content: &[u8]) -> PyResult<Py<PyBytes>> {
        with_store_mut(&self.slot, |store| {
            store
                .put_object(content)
                .map(|id| PyBytes::new(py, &id).unbind())
                .map_err(map_object_error)
        })
    }

    fn get_object(&self, py: Python<'_>, id: &[u8]) -> PyResult<Option<Py<PyBytes>>> {
        let id = parse_object_id(id)?;
        with_store(&self.slot, |store| {
            store
                .get_object(&id)
                .map(|content| content.map(|value| PyBytes::new(py, value).unbind()))
                .map_err(map_object_error)
        })
    }

    fn object_info(&self, py: Python<'_>, id: &[u8]) -> PyResult<Option<Py<PyDict>>> {
        let id = parse_object_id(id)?;
        with_store(&self.slot, |store| {
            let info = store.object_info(&id).map_err(map_object_error)?;
            let Some(info) = info else {
                return Ok(None);
            };
            let result = PyDict::new(py);
            result.set_item("id", PyBytes::new(py, &info.id))?;
            result.set_item("size", info.size)?;
            Ok(Some(result.unbind()))
        })
    }

    fn verify_object(&self, id: &[u8]) -> PyResult<bool> {
        let id = parse_object_id(id)?;
        with_store(&self.slot, |store| {
            store.verify_object(&id).map_err(map_object_error)
        })
    }

    fn link_object(&self, namespace: &[u8], name: &[u8], id: &[u8]) -> PyResult<()> {
        let id = parse_object_id(id)?;
        with_store_mut(&self.slot, |store| {
            store
                .link_object(namespace, name, &id)
                .map_err(map_object_error)
        })
    }

    fn resolve_object_ref(
        &self,
        py: Python<'_>,
        namespace: &[u8],
        name: &[u8],
    ) -> PyResult<Option<Py<PyBytes>>> {
        with_store(&self.slot, |store| {
            store
                .resolve_object_ref(namespace, name)
                .map(|id| id.map(|value| PyBytes::new(py, &value).unbind()))
                .map_err(map_object_error)
        })
    }

    fn unlink_object(&self, namespace: &[u8], name: &[u8]) -> PyResult<bool> {
        with_store_mut(&self.slot, |store| {
            store
                .unlink_object(namespace, name)
                .map_err(map_object_error)
        })
    }

    #[pyo3(signature = (limit = 100))]
    fn gc_orphan_objects(&self, py: Python<'_>, limit: usize) -> PyResult<Py<PyDict>> {
        with_store_mut(&self.slot, |store| {
            let report = store
                .gc_orphan_objects(limit)
                .map_err(map_object_error)?;
            let result = PyDict::new(py);
            result.set_item("scanned", report.scanned)?;
            result.set_item("referenced", report.referenced)?;
            result.set_item("removed", report.removed)?;
            Ok(result.unbind())
        })
    }
}
