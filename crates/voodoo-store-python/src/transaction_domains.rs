use pyo3::prelude::*;
use voodoo_store_core::JobSpec;

use super::{PendingOperation, PyTransaction};

#[pymethods]
impl PyTransaction {
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
    fn enqueue_job(
        &mut self,
        handler: &[u8],
        payload: &[u8],
        now_ms: i64,
        available_at_ms: i64,
        deadline_ms: Option<i64>,
        priority: i32,
        max_attempts: u32,
        retry_backoff_ms: u64,
        idempotency_key: Option<Vec<u8>>,
    ) -> PyResult<()> {
        self.ensure_open()?;
        let mut spec = JobSpec::new(handler.to_vec(), payload.to_vec());
        spec.available_at_ms = available_at_ms;
        spec.deadline_ms = deadline_ms;
        spec.priority = priority;
        spec.max_attempts = max_attempts;
        spec.retry_backoff_ms = retry_backoff_ms;
        spec.idempotency_key = idempotency_key;
        self.operations
            .push(PendingOperation::EnqueueJob { spec, now_ms });
        Ok(())
    }

    fn append_stream(&mut self, stream: &[u8], payload: &[u8]) -> PyResult<()> {
        self.ensure_open()?;
        self.operations.push(PendingOperation::AppendStream {
            stream: stream.to_vec(),
            payload: payload.to_vec(),
        });
        Ok(())
    }

    fn publish_topic(&mut self, topic: &[u8], payload: &[u8]) -> PyResult<()> {
        self.ensure_open()?;
        self.operations.push(PendingOperation::PublishTopic {
            topic: topic.to_vec(),
            payload: payload.to_vec(),
        });
        Ok(())
    }

    fn emit_event(
        &mut self,
        topic: &[u8],
        payload: &[u8],
        created_at_ms: i64,
    ) -> PyResult<()> {
        self.ensure_open()?;
        self.operations.push(PendingOperation::EmitEvent {
            topic: topic.to_vec(),
            payload: payload.to_vec(),
            created_at_ms,
        });
        Ok(())
    }
}
