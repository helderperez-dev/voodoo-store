use pyo3::prelude::*;
use voodoo_store_core::JobSpec;

use super::{PendingOperation, PyTransaction, VoodooStoreError};

fn parse_object_id(id: &[u8]) -> PyResult<[u8; 32]> {
    id.try_into()
        .map_err(|_| VoodooStoreError::new_err("object id must be exactly 32 bytes"))
}

fn parse_workflow_id(id: &[u8]) -> PyResult<[u8; 16]> {
    id.try_into()
        .map_err(|_| VoodooStoreError::new_err("workflow id must be exactly 16 bytes"))
}

fn parse_optional_workflow_id(id: Option<Vec<u8>>) -> PyResult<Option<[u8; 16]>> {
    id.map(|value| parse_workflow_id(&value)).transpose()
}

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
    #[allow(clippy::too_many_arguments)]
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
    ) -> PyResult<usize> {
        let mut spec = JobSpec::new(handler.to_vec(), payload.to_vec());
        spec.available_at_ms = available_at_ms;
        spec.deadline_ms = deadline_ms;
        spec.priority = priority;
        spec.max_attempts = max_attempts;
        spec.retry_backoff_ms = retry_backoff_ms;
        spec.idempotency_key = idempotency_key;
        self.stage_operation(PendingOperation::EnqueueJob { spec, now_ms })
    }

    #[pyo3(signature = (queue, payload, *, available_at_ms = 0, priority = 0))]
    fn push_queue(
        &mut self,
        queue: &[u8],
        payload: &[u8],
        available_at_ms: i64,
        priority: i32,
    ) -> PyResult<usize> {
        self.stage_operation(PendingOperation::PushQueue {
            queue: queue.to_vec(),
            payload: payload.to_vec(),
            available_at_ms,
            priority,
        })
    }

    fn append_stream(&mut self, stream: &[u8], payload: &[u8]) -> PyResult<usize> {
        self.stage_operation(PendingOperation::AppendStream {
            stream: stream.to_vec(),
            payload: payload.to_vec(),
        })
    }

    fn publish_topic(&mut self, topic: &[u8], payload: &[u8]) -> PyResult<usize> {
        self.stage_operation(PendingOperation::PublishTopic {
            topic: topic.to_vec(),
            payload: payload.to_vec(),
        })
    }

    fn emit_event(&mut self, topic: &[u8], payload: &[u8], created_at_ms: i64) -> PyResult<usize> {
        self.stage_operation(PendingOperation::EmitEvent {
            topic: topic.to_vec(),
            payload: payload.to_vec(),
            created_at_ms,
        })
    }

    fn put_object(&mut self, content: &[u8]) -> PyResult<usize> {
        self.stage_operation(PendingOperation::PutObject {
            content: content.to_vec(),
        })
    }

    fn put_linked_object(
        &mut self,
        content: &[u8],
        namespace: &[u8],
        name: &[u8],
    ) -> PyResult<usize> {
        self.stage_operation(PendingOperation::PutLinkedObject {
            content: content.to_vec(),
            namespace: namespace.to_vec(),
            name: name.to_vec(),
        })
    }

    fn link_object(
        &mut self,
        namespace: &[u8],
        name: &[u8],
        id: &[u8],
    ) -> PyResult<usize> {
        let id = parse_object_id(id)?;
        self.stage_operation(PendingOperation::LinkObject {
            namespace: namespace.to_vec(),
            name: name.to_vec(),
            id,
        })
    }

    fn unlink_object(&mut self, namespace: &[u8], name: &[u8]) -> PyResult<usize> {
        self.stage_operation(PendingOperation::UnlinkObject {
            namespace: namespace.to_vec(),
            name: name.to_vec(),
        })
    }

    #[pyo3(signature = (method, payload, created_at_ms, deadline_ms = None))]
    fn request_rpc(
        &mut self,
        method: &[u8],
        payload: &[u8],
        created_at_ms: i64,
        deadline_ms: Option<i64>,
    ) -> PyResult<usize> {
        self.stage_operation(PendingOperation::RequestRpc {
            method: method.to_vec(),
            payload: payload.to_vec(),
            created_at_ms,
            deadline_ms,
        })
    }

    #[pyo3(signature = (workflow_type, initial_step, initial_state, now_ms, parent_id = None))]
    fn create_workflow(
        &mut self,
        workflow_type: &[u8],
        initial_step: &[u8],
        initial_state: &[u8],
        now_ms: i64,
        parent_id: Option<Vec<u8>>,
    ) -> PyResult<usize> {
        let parent_id = parse_optional_workflow_id(parent_id)?;
        self.stage_operation(PendingOperation::CreateWorkflow {
            workflow_type: workflow_type.to_vec(),
            initial_step: initial_step.to_vec(),
            initial_state: initial_state.to_vec(),
            parent_id,
            now_ms,
        })
    }

    fn set_workflow_step(
        &mut self,
        id: &[u8],
        step: &[u8],
        state: &[u8],
        now_ms: i64,
    ) -> PyResult<usize> {
        let id = parse_workflow_id(id)?;
        self.stage_operation(PendingOperation::SetWorkflowStep {
            id,
            step: step.to_vec(),
            state: state.to_vec(),
            now_ms,
        })
    }

    fn wait_for_signal(&mut self, id: &[u8], signal: &[u8], now_ms: i64) -> PyResult<usize> {
        let id = parse_workflow_id(id)?;
        self.stage_operation(PendingOperation::WaitForSignal {
            id,
            signal: signal.to_vec(),
            now_ms,
        })
    }

    fn signal_workflow(
        &mut self,
        id: &[u8],
        signal: &[u8],
        payload: &[u8],
        now_ms: i64,
    ) -> PyResult<usize> {
        let id = parse_workflow_id(id)?;
        self.stage_operation(PendingOperation::SignalWorkflow {
            id,
            signal: signal.to_vec(),
            payload: payload.to_vec(),
            now_ms,
        })
    }

    fn wait_until(
        &mut self,
        id: &[u8],
        resume_at_ms: i64,
        now_ms: i64,
    ) -> PyResult<usize> {
        let id = parse_workflow_id(id)?;
        self.stage_operation(PendingOperation::WaitUntil {
            id,
            resume_at_ms,
            now_ms,
        })
    }

    fn complete_workflow(
        &mut self,
        id: &[u8],
        final_state: &[u8],
        now_ms: i64,
    ) -> PyResult<usize> {
        let id = parse_workflow_id(id)?;
        self.stage_operation(PendingOperation::CompleteWorkflow {
            id,
            final_state: final_state.to_vec(),
            now_ms,
        })
    }

    fn fail_workflow(&mut self, id: &[u8], error: &[u8], now_ms: i64) -> PyResult<usize> {
        let id = parse_workflow_id(id)?;
        self.stage_operation(PendingOperation::FailWorkflow {
            id,
            error: error.to_vec(),
            now_ms,
        })
    }

    fn cancel_workflow(&mut self, id: &[u8], reason: &[u8], now_ms: i64) -> PyResult<usize> {
        let id = parse_workflow_id(id)?;
        self.stage_operation(PendingOperation::CancelWorkflow {
            id,
            reason: reason.to_vec(),
            now_ms,
        })
    }
}
