use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use voodoo_store_core::{
    WorkflowError, WorkflowHistoryEntry, WorkflowHistoryKind, WorkflowId, WorkflowInstance,
    WorkflowStatus, WorkflowWait,
};

use super::{PyStore, VoodooStoreError, with_store, with_store_mut};

fn map_error(error: WorkflowError) -> PyErr {
    VoodooStoreError::new_err(error.to_string())
}

fn parse_id(id: &[u8]) -> PyResult<WorkflowId> {
    id.try_into()
        .map_err(|_| VoodooStoreError::new_err("workflow id must be exactly 16 bytes"))
}

fn parse_optional_id(id: Option<Vec<u8>>) -> PyResult<Option<WorkflowId>> {
    id.map(|value| parse_id(&value)).transpose()
}

fn status_name(status: WorkflowStatus) -> &'static str {
    match status {
        WorkflowStatus::Running => "running",
        WorkflowStatus::Waiting => "waiting",
        WorkflowStatus::Completed => "completed",
        WorkflowStatus::Failed => "failed",
        WorkflowStatus::Cancelled => "cancelled",
    }
}

fn history_name(kind: WorkflowHistoryKind) -> &'static str {
    match kind {
        WorkflowHistoryKind::Created => "created",
        WorkflowHistoryKind::StepChanged => "step_changed",
        WorkflowHistoryKind::WaitingSignal => "waiting_signal",
        WorkflowHistoryKind::SignalReceived => "signal_received",
        WorkflowHistoryKind::WaitingTimer => "waiting_timer",
        WorkflowHistoryKind::TimerFired => "timer_fired",
        WorkflowHistoryKind::Completed => "completed",
        WorkflowHistoryKind::Failed => "failed",
        WorkflowHistoryKind::Cancelled => "cancelled",
    }
}

fn py_instance(py: Python<'_>, instance: WorkflowInstance) -> PyResult<Py<PyDict>> {
    let result = PyDict::new(py);
    result.set_item("id", PyBytes::new(py, &instance.id))?;
    result.set_item("workflow_type", PyBytes::new(py, &instance.workflow_type))?;
    result.set_item("status", status_name(instance.status))?;
    result.set_item("current_step", PyBytes::new(py, &instance.current_step))?;
    result.set_item("state", PyBytes::new(py, &instance.state))?;
    match instance.parent_id {
        Some(parent_id) => result.set_item("parent_id", PyBytes::new(py, &parent_id))?,
        None => result.set_item("parent_id", py.None())?,
    }
    match instance.wait {
        None => result.set_item("wait", py.None())?,
        Some(WorkflowWait::Signal { name }) => {
            let wait = PyDict::new(py);
            wait.set_item("kind", "signal")?;
            wait.set_item("name", PyBytes::new(py, &name))?;
            result.set_item("wait", wait)?;
        }
        Some(WorkflowWait::Timer { resume_at_ms }) => {
            let wait = PyDict::new(py);
            wait.set_item("kind", "timer")?;
            wait.set_item("resume_at_ms", resume_at_ms)?;
            result.set_item("wait", wait)?;
        }
    }
    result.set_item("created_at_ms", instance.created_at_ms)?;
    result.set_item("updated_at_ms", instance.updated_at_ms)?;
    Ok(result.unbind())
}

fn py_history(py: Python<'_>, entry: WorkflowHistoryEntry) -> PyResult<Py<PyDict>> {
    let result = PyDict::new(py);
    result.set_item("sequence", entry.sequence)?;
    result.set_item("at_ms", entry.at_ms)?;
    result.set_item("kind", history_name(entry.kind))?;
    result.set_item("detail", PyBytes::new(py, &entry.detail))?;
    Ok(result.unbind())
}

#[pymethods]
impl PyStore {
    #[pyo3(signature = (workflow_type, initial_step, initial_state, now_ms, parent_id = None))]
    fn create_workflow(
        &self,
        py: Python<'_>,
        workflow_type: &[u8],
        initial_step: &[u8],
        initial_state: &[u8],
        now_ms: i64,
        parent_id: Option<Vec<u8>>,
    ) -> PyResult<Py<PyBytes>> {
        let parent_id = parse_optional_id(parent_id)?;
        with_store_mut(&self.slot, |store| {
            store
                .create_workflow(
                    workflow_type,
                    initial_step,
                    initial_state,
                    parent_id,
                    now_ms,
                )
                .map(|id| PyBytes::new(py, &id).unbind())
                .map_err(map_error)
        })
    }

    fn get_workflow(&self, py: Python<'_>, id: &[u8]) -> PyResult<Option<Py<PyDict>>> {
        let id = parse_id(id)?;
        with_store(&self.slot, |store| {
            store
                .get_workflow(&id)
                .map_err(map_error)?
                .map(|instance| py_instance(py, instance))
                .transpose()
        })
    }

    fn set_workflow_step(
        &self,
        id: &[u8],
        step: &[u8],
        state: &[u8],
        now_ms: i64,
    ) -> PyResult<()> {
        let id = parse_id(id)?;
        with_store_mut(&self.slot, |store| {
            store
                .set_workflow_step(&id, step, state, now_ms)
                .map_err(map_error)
        })
    }

    fn wait_for_signal(&self, id: &[u8], signal: &[u8], now_ms: i64) -> PyResult<()> {
        let id = parse_id(id)?;
        with_store_mut(&self.slot, |store| {
            store.wait_for_signal(&id, signal, now_ms).map_err(map_error)
        })
    }

    fn signal_workflow(
        &self,
        id: &[u8],
        signal: &[u8],
        payload: &[u8],
        now_ms: i64,
    ) -> PyResult<bool> {
        let id = parse_id(id)?;
        with_store_mut(&self.slot, |store| {
            store
                .signal_workflow(&id, signal, payload, now_ms)
                .map_err(map_error)
        })
    }

    fn wait_until(
        &self,
        id: &[u8],
        resume_at_ms: i64,
        now_ms: i64,
    ) -> PyResult<()> {
        let id = parse_id(id)?;
        with_store_mut(&self.slot, |store| {
            store
                .wait_until(&id, resume_at_ms, now_ms)
                .map_err(map_error)
        })
    }

    #[pyo3(signature = (now_ms, limit = 100))]
    fn resume_due_workflows(
        &self,
        py: Python<'_>,
        now_ms: i64,
        limit: usize,
    ) -> PyResult<Py<PyDict>> {
        with_store_mut(&self.slot, |store| {
            let report = store
                .resume_due_workflows(now_ms, limit)
                .map_err(map_error)?;
            let result = PyDict::new(py);
            result.set_item("scanned", report.scanned)?;
            result.set_item("resumed", report.resumed)?;
            Ok(result.unbind())
        })
    }

    fn complete_workflow(&self, id: &[u8], final_state: &[u8], now_ms: i64) -> PyResult<()> {
        let id = parse_id(id)?;
        with_store_mut(&self.slot, |store| {
            store
                .complete_workflow(&id, final_state, now_ms)
                .map_err(map_error)
        })
    }

    fn fail_workflow(&self, id: &[u8], error: &[u8], now_ms: i64) -> PyResult<()> {
        let id = parse_id(id)?;
        with_store_mut(&self.slot, |store| {
            store.fail_workflow(&id, error, now_ms).map_err(map_error)
        })
    }

    fn cancel_workflow(&self, id: &[u8], reason: &[u8], now_ms: i64) -> PyResult<bool> {
        let id = parse_id(id)?;
        with_store_mut(&self.slot, |store| {
            store
                .cancel_workflow(&id, reason, now_ms)
                .map_err(map_error)
        })
    }

    fn workflow_history(
        &self,
        py: Python<'_>,
        id: &[u8],
    ) -> PyResult<Vec<Py<PyDict>>> {
        let id = parse_id(id)?;
        with_store(&self.slot, |store| {
            store
                .workflow_history(&id)
                .map_err(map_error)?
                .into_iter()
                .map(|entry| py_history(py, entry))
                .collect()
        })
    }

    fn workflow_children(
        &self,
        py: Python<'_>,
        parent_id: &[u8],
    ) -> PyResult<Vec<Py<PyDict>>> {
        let parent_id = parse_id(parent_id)?;
        with_store(&self.slot, |store| {
            store
                .workflow_children(&parent_id)
                .map_err(map_error)?
                .into_iter()
                .map(|instance| py_instance(py, instance))
                .collect()
        })
    }
}
