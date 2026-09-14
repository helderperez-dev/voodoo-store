use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use voodoo_store_core::{
    CronSchedulerError, DurableCronSchedule, DurableSchedule, DurableTrigger, JobSpec, ScheduleMode,
    TriggerError, TriggerSource,
};

use super::{PyStore, VoodooStoreError, with_store, with_store_mut};

fn map_error(error: impl std::fmt::Display) -> PyErr {
    VoodooStoreError::new_err(error.to_string())
}

fn parse_id(id: &[u8], kind: &str) -> PyResult<[u8; 16]> {
    id.try_into()
        .map_err(|_| VoodooStoreError::new_err(format!("{kind} id must be exactly 16 bytes")))
}

#[allow(clippy::too_many_arguments)]
fn job_spec(
    handler: &[u8],
    payload: &[u8],
    available_at_ms: i64,
    deadline_ms: Option<i64>,
    priority: i32,
    max_attempts: u32,
    retry_backoff_ms: u64,
    idempotency_key: Option<Vec<u8>>,
) -> JobSpec {
    let mut spec = JobSpec::new(handler.to_vec(), payload.to_vec());
    spec.available_at_ms = available_at_ms;
    spec.deadline_ms = deadline_ms;
    spec.priority = priority;
    spec.max_attempts = max_attempts;
    spec.retry_backoff_ms = retry_backoff_ms;
    spec.idempotency_key = idempotency_key;
    spec
}

fn py_job_template(py: Python<'_>, spec: &JobSpec) -> PyResult<Py<PyDict>> {
    let result = PyDict::new(py);
    result.set_item("handler", PyBytes::new(py, &spec.handler))?;
    result.set_item("payload", PyBytes::new(py, &spec.payload))?;
    result.set_item("available_at_ms", spec.available_at_ms)?;
    result.set_item("deadline_ms", spec.deadline_ms)?;
    result.set_item("priority", spec.priority)?;
    result.set_item("max_attempts", spec.max_attempts)?;
    result.set_item("retry_backoff_ms", spec.retry_backoff_ms)?;
    match &spec.idempotency_key {
        Some(key) => result.set_item("idempotency_key", PyBytes::new(py, key))?,
        None => result.set_item("idempotency_key", py.None())?,
    }
    Ok(result.unbind())
}

fn py_schedule(py: Python<'_>, schedule: DurableSchedule) -> PyResult<Py<PyDict>> {
    let result = PyDict::new(py);
    result.set_item("id", PyBytes::new(py, &schedule.id))?;
    result.set_item("job", py_job_template(py, &schedule.job)?)?;
    match schedule.mode {
        ScheduleMode::Once => {
            result.set_item("mode", "once")?;
            result.set_item("every_ms", py.None())?;
        }
        ScheduleMode::Interval { every_ms } => {
            result.set_item("mode", "interval")?;
            result.set_item("every_ms", every_ms)?;
        }
    }
    result.set_item("next_run_ms", schedule.next_run_ms)?;
    result.set_item("enabled", schedule.enabled)?;
    Ok(result.unbind())
}

fn py_cron(py: Python<'_>, schedule: DurableCronSchedule) -> PyResult<Py<PyDict>> {
    let result = PyDict::new(py);
    result.set_item("id", PyBytes::new(py, &schedule.id))?;
    result.set_item("expression", PyBytes::new(py, &schedule.expression))?;
    result.set_item("job", py_job_template(py, &schedule.job)?)?;
    result.set_item("next_run_ms", schedule.next_run_ms)?;
    result.set_item("enabled", schedule.enabled)?;
    result.set_item("fire_count", schedule.fire_count)?;
    result.set_item("last_fired_at_ms", schedule.last_fired_at_ms)?;
    Ok(result.unbind())
}

fn source_name(source: &TriggerSource) -> &'static str {
    match source {
        TriggerSource::Manual => "manual",
        TriggerSource::Collection { .. } => "collection",
        TriggerSource::Stream { .. } => "stream",
        TriggerSource::Topic { .. } => "topic",
    }
}

fn py_trigger(py: Python<'_>, trigger: DurableTrigger) -> PyResult<Py<PyDict>> {
    let result = PyDict::new(py);
    result.set_item("id", PyBytes::new(py, &trigger.id))?;
    result.set_item("name", PyBytes::new(py, &trigger.name))?;
    result.set_item("source", source_name(&trigger.source))?;
    match &trigger.source {
        TriggerSource::Manual => {
            result.set_item("source_name", py.None())?;
            result.set_item("operation", py.None())?;
        }
        TriggerSource::Collection {
            collection,
            operation,
        } => {
            result.set_item("source_name", PyBytes::new(py, collection))?;
            result.set_item("operation", PyBytes::new(py, operation))?;
        }
        TriggerSource::Stream { stream } => {
            result.set_item("source_name", PyBytes::new(py, stream))?;
            result.set_item("operation", py.None())?;
        }
        TriggerSource::Topic { topic } => {
            result.set_item("source_name", PyBytes::new(py, topic))?;
            result.set_item("operation", py.None())?;
        }
    }
    result.set_item("job", py_job_template(py, &trigger.job)?)?;
    result.set_item("enabled", trigger.enabled)?;
    result.set_item("fire_count", trigger.fire_count)?;
    result.set_item("last_fired_at_ms", trigger.last_fired_at_ms)?;
    Ok(result.unbind())
}

fn trigger_source(kind: &str, source_name: Option<Vec<u8>>, operation: Option<Vec<u8>>) -> PyResult<TriggerSource> {
    match kind {
        "manual" => Ok(TriggerSource::Manual),
        "collection" => Ok(TriggerSource::Collection {
            collection: source_name.ok_or_else(|| {
                VoodooStoreError::new_err("collection trigger requires source_name")
            })?,
            operation: operation.ok_or_else(|| {
                VoodooStoreError::new_err("collection trigger requires operation")
            })?,
        }),
        "stream" => Ok(TriggerSource::Stream {
            stream: source_name
                .ok_or_else(|| VoodooStoreError::new_err("stream trigger requires source_name"))?,
        }),
        "topic" => Ok(TriggerSource::Topic {
            topic: source_name
                .ok_or_else(|| VoodooStoreError::new_err("topic trigger requires source_name"))?,
        }),
        _ => Err(VoodooStoreError::new_err(
            "trigger source must be one of: manual, collection, stream, topic",
        )),
    }
}

#[allow(clippy::too_many_arguments)]
#[pymethods]
impl PyStore {
    #[pyo3(signature = (
        handler,
        payload,
        first_run_ms,
        *,
        mode = "once",
        every_ms = None,
        available_at_ms = 0,
        deadline_ms = None,
        priority = 0,
        max_attempts = 3,
        retry_backoff_ms = 1000,
        idempotency_key = None
    ))]
    fn create_schedule(
        &self,
        py: Python<'_>,
        handler: &[u8],
        payload: &[u8],
        first_run_ms: i64,
        mode: &str,
        every_ms: Option<u64>,
        available_at_ms: i64,
        deadline_ms: Option<i64>,
        priority: i32,
        max_attempts: u32,
        retry_backoff_ms: u64,
        idempotency_key: Option<Vec<u8>>,
    ) -> PyResult<Py<PyBytes>> {
        let mode = match mode {
            "once" => ScheduleMode::Once,
            "interval" => ScheduleMode::Interval {
                every_ms: every_ms.ok_or_else(|| {
                    VoodooStoreError::new_err("interval schedule requires every_ms")
                })?,
            },
            _ => {
                return Err(VoodooStoreError::new_err(
                    "schedule mode must be 'once' or 'interval'",
                ));
            }
        };
        let spec = job_spec(
            handler,
            payload,
            available_at_ms,
            deadline_ms,
            priority,
            max_attempts,
            retry_backoff_ms,
            idempotency_key,
        );
        with_store_mut(&self.slot, |store| {
            store
                .create_schedule(spec, mode, first_run_ms)
                .map(|id| PyBytes::new(py, &id).unbind())
                .map_err(map_error)
        })
    }

    fn get_schedule(&self, py: Python<'_>, id: &[u8]) -> PyResult<Option<Py<PyDict>>> {
        let id = parse_id(id, "schedule")?;
        with_store(&self.slot, |store| {
            match store.get_schedule(&id).map_err(map_error)? {
                Some(schedule) => Ok(Some(py_schedule(py, schedule)?)),
                None => Ok(None),
            }
        })
    }

    fn set_schedule_enabled(&self, id: &[u8], enabled: bool) -> PyResult<bool> {
        let id = parse_id(id, "schedule")?;
        with_store_mut(&self.slot, |store| {
            store.set_schedule_enabled(&id, enabled).map_err(map_error)
        })
    }

    fn tick_schedules(&self, now_ms: i64, limit: usize) -> PyResult<(usize, usize)> {
        with_store_mut(&self.slot, |store| {
            store
                .tick_schedules(now_ms, limit)
                .map(|report| (report.scanned, report.fired))
                .map_err(map_error)
        })
    }

    #[pyo3(signature = (
        expression,
        handler,
        payload,
        after_ms,
        *,
        available_at_ms = 0,
        deadline_ms = None,
        priority = 0,
        max_attempts = 3,
        retry_backoff_ms = 1000,
        idempotency_key = None
    ))]
    fn create_cron_schedule(
        &self,
        py: Python<'_>,
        expression: &str,
        handler: &[u8],
        payload: &[u8],
        after_ms: i64,
        available_at_ms: i64,
        deadline_ms: Option<i64>,
        priority: i32,
        max_attempts: u32,
        retry_backoff_ms: u64,
        idempotency_key: Option<Vec<u8>>,
    ) -> PyResult<Py<PyBytes>> {
        let spec = job_spec(
            handler,
            payload,
            available_at_ms,
            deadline_ms,
            priority,
            max_attempts,
            retry_backoff_ms,
            idempotency_key,
        );
        with_store_mut(&self.slot, |store| {
            store
                .create_cron_schedule(expression, spec, after_ms)
                .map(|id| PyBytes::new(py, &id).unbind())
                .map_err(|error: CronSchedulerError| map_error(error))
        })
    }

    fn get_cron_schedule(&self, py: Python<'_>, id: &[u8]) -> PyResult<Option<Py<PyDict>>> {
        let id = parse_id(id, "cron schedule")?;
        with_store(&self.slot, |store| {
            match store.get_cron_schedule(&id).map_err(map_error)? {
                Some(schedule) => Ok(Some(py_cron(py, schedule)?)),
                None => Ok(None),
            }
        })
    }

    fn list_cron_schedules(&self, py: Python<'_>) -> PyResult<Vec<Py<PyDict>>> {
        with_store(&self.slot, |store| {
            store
                .list_cron_schedules()
                .map_err(map_error)?
                .into_iter()
                .map(|schedule| py_cron(py, schedule))
                .collect()
        })
    }

    fn set_cron_schedule_enabled(&self, id: &[u8], enabled: bool) -> PyResult<bool> {
        let id = parse_id(id, "cron schedule")?;
        with_store_mut(&self.slot, |store| {
            store
                .set_cron_schedule_enabled(&id, enabled)
                .map_err(map_error)
        })
    }

    fn tick_cron_schedules(&self, now_ms: i64, limit: usize) -> PyResult<(usize, usize)> {
        with_store_mut(&self.slot, |store| {
            store
                .tick_cron_schedules(now_ms, limit)
                .map(|report| (report.scanned, report.fired))
                .map_err(map_error)
        })
    }

    #[pyo3(signature = (
        name,
        source,
        handler,
        payload,
        *,
        source_name = None,
        operation = None,
        available_at_ms = 0,
        deadline_ms = None,
        priority = 0,
        max_attempts = 3,
        retry_backoff_ms = 1000,
        idempotency_key = None
    ))]
    fn create_trigger(
        &self,
        py: Python<'_>,
        name: &[u8],
        source: &str,
        handler: &[u8],
        payload: &[u8],
        source_name: Option<Vec<u8>>,
        operation: Option<Vec<u8>>,
        available_at_ms: i64,
        deadline_ms: Option<i64>,
        priority: i32,
        max_attempts: u32,
        retry_backoff_ms: u64,
        idempotency_key: Option<Vec<u8>>,
    ) -> PyResult<Py<PyBytes>> {
        let source = trigger_source(source, source_name, operation)?;
        let spec = job_spec(
            handler,
            payload,
            available_at_ms,
            deadline_ms,
            priority,
            max_attempts,
            retry_backoff_ms,
            idempotency_key,
        );
        with_store_mut(&self.slot, |store| {
            store
                .create_trigger(name, source, spec)
                .map(|id| PyBytes::new(py, &id).unbind())
                .map_err(|error: TriggerError| map_error(error))
        })
    }

    fn get_trigger(&self, py: Python<'_>, id: &[u8]) -> PyResult<Option<Py<PyDict>>> {
        let id = parse_id(id, "trigger")?;
        with_store(&self.slot, |store| {
            match store.get_trigger(&id).map_err(map_error)? {
                Some(trigger) => Ok(Some(py_trigger(py, trigger)?)),
                None => Ok(None),
            }
        })
    }

    fn list_triggers(&self, py: Python<'_>) -> PyResult<Vec<Py<PyDict>>> {
        with_store(&self.slot, |store| {
            store
                .list_triggers()
                .map_err(map_error)?
                .into_iter()
                .map(|trigger| py_trigger(py, trigger))
                .collect()
        })
    }

    fn set_trigger_enabled(&self, id: &[u8], enabled: bool) -> PyResult<bool> {
        let id = parse_id(id, "trigger")?;
        with_store_mut(&self.slot, |store| {
            store.set_trigger_enabled(&id, enabled).map_err(map_error)
        })
    }

    fn fire_trigger(
        &self,
        py: Python<'_>,
        id: &[u8],
        event_payload: &[u8],
        now_ms: i64,
    ) -> PyResult<Option<Py<PyBytes>>> {
        let id = parse_id(id, "trigger")?;
        with_store_mut(&self.slot, |store| {
            store
                .fire_trigger(&id, event_payload, now_ms)
                .map(|job_id| job_id.map(|value| PyBytes::new(py, &value).unbind()))
                .map_err(map_error)
        })
    }
}
