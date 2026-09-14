use pyo3::prelude::*;
use pyo3::types::PyBytes;
use voodoo_store_core::{
    DurableJob, DurableJobState, JobError, JobHistoryEntry, JobHistoryKind, JobSpec,
};

use super::{PyStore, VoodooStoreError, with_store, with_store_mut};

type PyJob = (
    Py<PyBytes>,
    String,
    Py<PyBytes>,
    Py<PyBytes>,
    i64,
    Option<i64>,
    i32,
    u32,
    u32,
    u64,
    i64,
    u32,
    Option<Py<PyBytes>>,
);

type PyHistory = (u64, i64, String, Py<PyBytes>);

fn map_job_error(error: JobError) -> PyErr {
    VoodooStoreError::new_err(error.to_string())
}

fn state_name(state: DurableJobState) -> &'static str {
    match state {
        DurableJobState::Ready => "ready",
        DurableJobState::Leased => "leased",
        DurableJobState::Completed => "completed",
        DurableJobState::Dead => "dead",
        DurableJobState::Cancelled => "cancelled",
    }
}

fn history_name(kind: JobHistoryKind) -> &'static str {
    match kind {
        JobHistoryKind::Submitted => "submitted",
        JobHistoryKind::Claimed => "claimed",
        JobHistoryKind::Completed => "completed",
        JobHistoryKind::RetryScheduled => "retry_scheduled",
        JobHistoryKind::Dead => "dead",
        JobHistoryKind::Cancelled => "cancelled",
    }
}

fn py_job(py: Python<'_>, job: DurableJob) -> PyJob {
    (
        PyBytes::new(py, &job.id).unbind(),
        state_name(job.state).to_string(),
        PyBytes::new(py, &job.handler).unbind(),
        PyBytes::new(py, &job.payload).unbind(),
        job.available_at_ms,
        job.deadline_ms,
        job.priority,
        job.attempts,
        job.max_attempts,
        job.retry_backoff_ms,
        job.lease_until_ms,
        job.lease_generation,
        job.idempotency_key
            .as_deref()
            .map(|key| PyBytes::new(py, key).unbind()),
    )
}

fn py_history(py: Python<'_>, entry: JobHistoryEntry) -> PyHistory {
    (
        entry.sequence,
        entry.at_ms,
        history_name(entry.kind).to_string(),
        PyBytes::new(py, &entry.detail).unbind(),
    )
}

fn parse_job_id(id: &[u8]) -> PyResult<[u8; 16]> {
    id.try_into()
        .map_err(|_| VoodooStoreError::new_err("job id must be exactly 16 bytes"))
}

#[pymethods]
impl PyStore {
    #[pyo3(signature = (
        handler,
        payload,
        now_ms,
        *,
        available_at_ms = 0,
        deadline_ms = None,
        priority = 0,
        max_attempts = 3,
        retry_backoff_ms = 1000,
        idempotency_key = None
    ))]
    fn submit_job(
        &self,
        py: Python<'_>,
        handler: &[u8],
        payload: &[u8],
        now_ms: i64,
        available_at_ms: i64,
        deadline_ms: Option<i64>,
        priority: i32,
        max_attempts: u32,
        retry_backoff_ms: u64,
        idempotency_key: Option<Vec<u8>>,
    ) -> PyResult<Py<PyBytes>> {
        let mut spec = JobSpec::new(handler.to_vec(), payload.to_vec());
        spec.available_at_ms = available_at_ms;
        spec.deadline_ms = deadline_ms;
        spec.priority = priority;
        spec.max_attempts = max_attempts;
        spec.retry_backoff_ms = retry_backoff_ms;
        spec.idempotency_key = idempotency_key;
        with_store_mut(&self.slot, |store| {
            store
                .submit_job(spec, now_ms)
                .map(|id| PyBytes::new(py, &id).unbind())
                .map_err(map_job_error)
        })
    }

    fn get_job(&self, py: Python<'_>, id: &[u8]) -> PyResult<Option<PyJob>> {
        let id = parse_job_id(id)?;
        with_store(&self.slot, |store| {
            store
                .get_job(&id)
                .map(|job| job.map(|job| py_job(py, job)))
                .map_err(map_job_error)
        })
    }

    fn claim_job(
        &self,
        py: Python<'_>,
        now_ms: i64,
        lease_duration_ms: u64,
    ) -> PyResult<Option<PyJob>> {
        with_store_mut(&self.slot, |store| {
            store
                .claim_job(now_ms, lease_duration_ms)
                .map(|job| job.map(|job| py_job(py, job)))
                .map_err(map_job_error)
        })
    }

    fn complete_job(
        &self,
        id: &[u8],
        lease_generation: u32,
        now_ms: i64,
    ) -> PyResult<()> {
        let id = parse_job_id(id)?;
        with_store_mut(&self.slot, |store| {
            store
                .complete_job(&id, lease_generation, now_ms)
                .map_err(map_job_error)
        })
    }

    fn fail_job(
        &self,
        id: &[u8],
        lease_generation: u32,
        now_ms: i64,
        detail: &[u8],
    ) -> PyResult<String> {
        let id = parse_job_id(id)?;
        with_store_mut(&self.slot, |store| {
            store
                .fail_job(&id, lease_generation, now_ms, detail)
                .map(|state| state_name(state).to_string())
                .map_err(map_job_error)
        })
    }

    fn cancel_job(&self, id: &[u8], now_ms: i64) -> PyResult<bool> {
        let id = parse_job_id(id)?;
        with_store_mut(&self.slot, |store| {
            store.cancel_job(&id, now_ms).map_err(map_job_error)
        })
    }

    fn job_history(&self, py: Python<'_>, id: &[u8]) -> PyResult<Vec<PyHistory>> {
        let id = parse_job_id(id)?;
        with_store(&self.slot, |store| {
            store
                .job_history(&id)
                .map(|entries| {
                    entries
                        .into_iter()
                        .map(|entry| py_history(py, entry))
                        .collect()
                })
                .map_err(map_job_error)
        })
    }
}
