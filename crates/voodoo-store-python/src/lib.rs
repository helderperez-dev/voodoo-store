use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};

use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyModule};
use voodoo_store_core::{Durability, EngineError, JobSpec, Store, StoreOptions, VerificationReport};

create_exception!(_native, VoodooStoreError, PyException);
create_exception!(_native, StoreClosedError, VoodooStoreError);
create_exception!(_native, StoreBusyError, VoodooStoreError);
create_exception!(_native, AlreadyOpenError, VoodooStoreError);
create_exception!(_native, ReservedKeyError, VoodooStoreError);
create_exception!(_native, TransactionFinishedError, VoodooStoreError);
create_exception!(_native, CorruptionError, VoodooStoreError);
create_exception!(_native, IoError, VoodooStoreError);

#[derive(Debug)]
enum StoreSlot {
    Open(Store),
    InTransaction,
    Closed,
}

#[pyclass(name = "VerificationReport", frozen)]
struct PyVerificationReport {
    #[pyo3(get)]
    file_bytes: u64,
    #[pyo3(get)]
    valid_bytes: u64,
    #[pyo3(get)]
    records: u64,
    #[pyo3(get)]
    committed_transactions: u64,
    #[pyo3(get)]
    pending_transactions: u64,
    #[pyo3(get)]
    keys: usize,
    #[pyo3(get)]
    has_torn_tail: bool,
    store_id: [u8; 16],
}

#[pymethods]
impl PyVerificationReport {
    #[getter]
    fn store_id(&self, py: Python<'_>) -> Py<PyBytes> {
        PyBytes::new(py, &self.store_id).unbind()
    }

    fn __repr__(&self) -> String {
        format!(
            "VerificationReport(file_bytes={}, valid_bytes={}, records={}, committed_transactions={}, pending_transactions={}, keys={}, has_torn_tail={})",
            self.file_bytes,
            self.valid_bytes,
            self.records,
            self.committed_transactions,
            self.pending_transactions,
            self.keys,
            self.has_torn_tail,
        )
    }
}

impl From<VerificationReport> for PyVerificationReport {
    fn from(report: VerificationReport) -> Self {
        Self {
            file_bytes: report.file_bytes,
            valid_bytes: report.valid_bytes,
            records: report.records,
            committed_transactions: report.committed_transactions,
            pending_transactions: report.pending_transactions,
            keys: report.keys,
            has_torn_tail: report.has_torn_tail(),
            store_id: report.header.store_id,
        }
    }
}

#[pyclass(name = "Store")]
struct PyStore {
    slot: Arc<Mutex<StoreSlot>>,
}

#[pymethods]
impl PyStore {
    #[staticmethod]
    #[pyo3(signature = (path, *, durability = "data", repair_torn_tail = true))]
    fn open(path: PathBuf, durability: &str, repair_torn_tail: bool) -> PyResult<Self> {
        let options = StoreOptions {
            durability: parse_durability(durability)?,
            repair_torn_tail,
        };
        let store = Store::open_with_options(path, options).map_err(map_engine_error)?;
        Ok(Self {
            slot: Arc::new(Mutex::new(StoreSlot::Open(store))),
        })
    }

    #[staticmethod]
    fn verify(path: PathBuf) -> PyResult<PyVerificationReport> {
        Store::verify(path)
            .map(PyVerificationReport::from)
            .map_err(map_engine_error)
    }

    fn close(&self) -> PyResult<()> {
        let mut slot = lock_slot(&self.slot)?;
        match &*slot {
            StoreSlot::InTransaction => Err(StoreBusyError::new_err(
                "cannot close store while a transaction is active",
            )),
            StoreSlot::Closed => Ok(()),
            StoreSlot::Open(_) => {
                *slot = StoreSlot::Closed;
                Ok(())
            }
        }
    }

    fn get(&self, py: Python<'_>, key: &[u8]) -> PyResult<Option<Py<PyBytes>>> {
        with_store(&self.slot, |store| {
            Ok(store.get(key).map(|value| PyBytes::new(py, value).unbind()))
        })
    }

    fn contains(&self, key: &[u8]) -> PyResult<bool> {
        with_store(&self.slot, |store| Ok(store.contains_key(key)))
    }

    fn put(&self, key: &[u8], value: &[u8]) -> PyResult<()> {
        with_store_mut(&self.slot, |store| {
            store.put(key, value).map_err(map_engine_error)
        })
    }

    fn delete(&self, key: &[u8]) -> PyResult<()> {
        with_store_mut(&self.slot, |store| {
            store.delete(key).map_err(map_engine_error)
        })
    }

    fn scan_prefix(
        &self,
        py: Python<'_>,
        prefix: &[u8],
    ) -> PyResult<Vec<(Py<PyBytes>, Py<PyBytes>)>> {
        with_store(&self.slot, |store| {
            Ok(store
                .scan_prefix(prefix)
                .into_iter()
                .map(|(key, value)| {
                    (
                        PyBytes::new(py, &key).unbind(),
                        PyBytes::new(py, &value).unbind(),
                    )
                })
                .collect())
        })
    }

    fn flush(&self) -> PyResult<()> {
        with_store_mut(&self.slot, |store| store.flush().map_err(map_engine_error))
    }

    fn transaction(&self) -> PyResult<PyTransaction> {
        let mut slot = lock_slot(&self.slot)?;
        let store = match std::mem::replace(&mut *slot, StoreSlot::InTransaction) {
            StoreSlot::Open(store) => store,
            StoreSlot::InTransaction => {
                *slot = StoreSlot::InTransaction;
                return Err(StoreBusyError::new_err("a transaction is already active"));
            }
            StoreSlot::Closed => {
                *slot = StoreSlot::Closed;
                return Err(StoreClosedError::new_err("store is closed"));
            }
        };

        Ok(PyTransaction {
            slot: Arc::clone(&self.slot),
            store: Some(store),
            operations: Vec::new(),
            finished: false,
        })
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(
        &self,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_value: Option<&Bound<'_, PyAny>>,
        _traceback: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        self.close()?;
        Ok(false)
    }
}

#[derive(Debug)]
enum PendingOperation {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
    UpsertRecord {
        collection: Vec<u8>,
        primary_key: Vec<u8>,
        value: Vec<u8>,
        indexes: Vec<voodoo_store_core::IndexValue>,
    },
    EnqueueJob {
        spec: JobSpec,
        now_ms: i64,
    },
    PushQueue {
        queue: Vec<u8>,
        payload: Vec<u8>,
        available_at_ms: i64,
        priority: i32,
    },
    AppendStream {
        stream: Vec<u8>,
        payload: Vec<u8>,
    },
    PublishTopic {
        topic: Vec<u8>,
        payload: Vec<u8>,
    },
    EmitEvent {
        topic: Vec<u8>,
        payload: Vec<u8>,
        created_at_ms: i64,
    },
    PutObject {
        content: Vec<u8>,
    },
    LinkObject {
        namespace: Vec<u8>,
        name: Vec<u8>,
        object: PendingObjectReference,
    },
    UnlinkObject {
        namespace: Vec<u8>,
        name: Vec<u8>,
    },
    RequestRpc {
        method: Vec<u8>,
        payload: Vec<u8>,
        created_at_ms: i64,
        deadline_ms: Option<i64>,
    },
    CreateWorkflow {
        workflow_type: Vec<u8>,
        initial_step: Vec<u8>,
        initial_state: Vec<u8>,
        parent_id: Option<voodoo_store_core::WorkflowId>,
        now_ms: i64,
    },
    SetWorkflowStep {
        workflow: PendingWorkflowReference,
        step: Vec<u8>,
        state: Vec<u8>,
        now_ms: i64,
    },
    WaitWorkflowSignal {
        workflow: PendingWorkflowReference,
        signal: Vec<u8>,
        now_ms: i64,
    },
    WaitWorkflowUntil {
        workflow: PendingWorkflowReference,
        resume_at_ms: i64,
        now_ms: i64,
    },
    CompleteWorkflow {
        workflow: PendingWorkflowReference,
        final_state: Vec<u8>,
        now_ms: i64,
    },
}

#[derive(Debug, Clone)]
enum PendingObjectReference {
    Id(voodoo_store_core::ObjectId),
    Operation(usize),
}

#[derive(Debug, Clone)]
enum PendingWorkflowReference {
    Id(voodoo_store_core::WorkflowId),
    Operation(usize),
}

#[derive(Debug, Clone)]
enum OperationResult {
    JobId([u8; 16]),
    QueueId(u64),
    StreamOffset(u64),
    TopicOffset(u64),
    OutboxId {
        tx_id: u64,
        nonce: [u8; 16],
    },
    ObjectId(voodoo_store_core::ObjectId),
    RpcId {
        tx_id: u64,
        nonce: [u8; 16],
    },
    WorkflowId(voodoo_store_core::WorkflowId),
    Boolean {
        kind: &'static str,
        value: bool,
    },
}

enum PendingLookup<'a> {
    Value(&'a [u8]),
    Deleted,
    Unchanged,
}

#[pyclass(name = "Transaction")]
struct PyTransaction {
    slot: Arc<Mutex<StoreSlot>>,
    store: Option<Store>,
    operations: Vec<PendingOperation>,
    finished: bool,
}

#[pymethods]
impl PyTransaction {
    fn get(&self, py: Python<'_>, key: &[u8]) -> PyResult<Option<Py<PyBytes>>> {
        self.ensure_open()?;
        match self.lookup_pending(key) {
            PendingLookup::Value(value) => Ok(Some(PyBytes::new(py, value).unbind())),
            PendingLookup::Deleted => Ok(None),
            PendingLookup::Unchanged => Ok(self
                .store
                .as_ref()
                .and_then(|store| store.get(key))
                .map(|value| PyBytes::new(py, value).unbind())),
        }
    }

    fn contains(&self, key: &[u8]) -> PyResult<bool> {
        self.ensure_open()?;
        match self.lookup_pending(key) {
            PendingLookup::Value(_) => Ok(true),
            PendingLookup::Deleted => Ok(false),
            PendingLookup::Unchanged => Ok(self
                .store
                .as_ref()
                .is_some_and(|store| store.contains_key(key))),
        }
    }

    fn scan_prefix(
        &self,
        py: Python<'_>,
        prefix: &[u8],
    ) -> PyResult<Vec<(Py<PyBytes>, Py<PyBytes>)>> {
        self.ensure_open()?;
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| TransactionFinishedError::new_err("transaction is finished"))?;
        let mut entries: BTreeMap<Vec<u8>, Vec<u8>> =
            store.scan_prefix(prefix).into_iter().collect();

        for operation in &self.operations {
            match operation {
                PendingOperation::Put(key, value) if key.starts_with(prefix) => {
                    entries.insert(key.clone(), value.clone());
                }
                PendingOperation::Delete(key) if key.starts_with(prefix) => {
                    entries.remove(key);
                }
                _ => {}
            }
        }

        Ok(entries
            .into_iter()
            .map(|(key, value)| {
                (
                    PyBytes::new(py, &key).unbind(),
                    PyBytes::new(py, &value).unbind(),
                )
            })
            .collect())
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> PyResult<()> {
        self.ensure_open()?;
        self.operations
            .push(PendingOperation::Put(key.to_vec(), value.to_vec()));
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> PyResult<()> {
        self.ensure_open()?;
        self.operations.push(PendingOperation::Delete(key.to_vec()));
        Ok(())
    }

    fn commit(&mut self) -> PyResult<()> {
        self.commit_operations().map(|_| ())
    }

    fn commit_with_results(&mut self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let results = self.commit_operations()?;
        let output = PyDict::new(py);
        for (index, result) in results.into_iter().enumerate() {
            if let Some(result) = result {
                output.set_item(index, py_operation_result(py, result)?)?;
            }
        }
        Ok(output.unbind())
    }

    fn rollback(&mut self) -> PyResult<()> {
        self.ensure_open()?;
        self.finished = true;
        let store = self
            .store
            .take()
            .ok_or_else(|| TransactionFinishedError::new_err("transaction is finished"))?;
        self.restore_store(store)
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(
        &mut self,
        exc_type: Option<&Bound<'_, PyAny>>,
        _exc_value: Option<&Bound<'_, PyAny>>,
        _traceback: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        if self.finished {
            return Ok(false);
        }
        if exc_type.is_some() {
            self.rollback()?;
        } else {
            self.commit()?;
        }
        Ok(false)
    }
}

impl PyTransaction {
    fn stage_operation(&mut self, operation: PendingOperation) -> PyResult<usize> {
        self.ensure_open()?;
        let index = self.operations.len();
        self.operations.push(operation);
        Ok(index)
    }

    fn commit_operations(&mut self) -> PyResult<Vec<Option<OperationResult>>> {
        self.ensure_open()?;
        let mut store = self
            .store
            .take()
            .ok_or_else(|| TransactionFinishedError::new_err("transaction is finished"))?;

        let result = (|| {
            let mut tx = store.begin().map_err(map_engine_error)?;
            let mut results = Vec::with_capacity(self.operations.len());
            for operation in &self.operations {
                let operation_result = match operation {
                    PendingOperation::Put(key, value) => {
                        tx.put(key, value).map_err(map_engine_error)?;
                        None
                    }
                    PendingOperation::Delete(key) => {
                        tx.delete(key).map_err(map_engine_error)?;
                        None
                    }
                    PendingOperation::UpsertRecord {
                        collection,
                        primary_key,
                        value,
                        indexes,
                    } => {
                        tx.upsert_record(collection, primary_key, value, indexes)
                            .map_err(|error| VoodooStoreError::new_err(error.to_string()))?;
                        None
                    }
                    PendingOperation::EnqueueJob { spec, now_ms } => {
                        let id = tx
                            .enqueue_job(spec.clone(), *now_ms)
                            .map_err(|error| VoodooStoreError::new_err(error.to_string()))?;
                        Some(OperationResult::JobId(id))
                    }
                    PendingOperation::PushQueue {
                        queue,
                        payload,
                        available_at_ms,
                        priority,
                    } => {
                        let id = tx
                            .push_queue(
                                queue,
                                payload,
                                voodoo_store_core::PushOptions {
                                    available_at_ms: *available_at_ms,
                                    priority: *priority,
                                },
                            )
                            .map_err(|error| VoodooStoreError::new_err(error.to_string()))?;
                        Some(OperationResult::QueueId(id))
                    }
                    PendingOperation::AppendStream { stream, payload } => {
                        let offset = tx
                            .append_stream(stream, payload)
                            .map_err(|error| VoodooStoreError::new_err(error.to_string()))?;
                        Some(OperationResult::StreamOffset(offset))
                    }
                    PendingOperation::PublishTopic { topic, payload } => {
                        let offset = tx
                            .publish_topic(topic, payload)
                            .map_err(|error| VoodooStoreError::new_err(error.to_string()))?;
                        Some(OperationResult::TopicOffset(offset))
                    }
                    PendingOperation::EmitEvent {
                        topic,
                        payload,
                        created_at_ms,
                    } => {
                        let id = tx
                            .emit_event(topic, payload, *created_at_ms)
                            .map_err(|error| VoodooStoreError::new_err(error.to_string()))?;
                        Some(OperationResult::OutboxId {
                            tx_id: id.tx_id,
                            nonce: id.nonce,
                        })
                    }
                    PendingOperation::PutObject { content } => {
                        let id = tx
                            .put_object(content)
                            .map_err(|error| VoodooStoreError::new_err(error.to_string()))?;
                        Some(OperationResult::ObjectId(id))
                    }
                    PendingOperation::LinkObject {
                        namespace,
                        name,
                        object,
                    } => {
                        let id = resolve_object_reference(object, &results)?;
                        tx.link_object(namespace, name, &id)
                            .map_err(|error| VoodooStoreError::new_err(error.to_string()))?;
                        None
                    }
                    PendingOperation::UnlinkObject { namespace, name } => {
                        let removed = tx
                            .unlink_object(namespace, name)
                            .map_err(|error| VoodooStoreError::new_err(error.to_string()))?;
                        Some(OperationResult::Boolean {
                            kind: "object_unlink",
                            value: removed,
                        })
                    }
                    PendingOperation::RequestRpc {
                        method,
                        payload,
                        created_at_ms,
                        deadline_ms,
                    } => {
                        let id = tx
                            .request_rpc(method, payload, *created_at_ms, *deadline_ms)
                            .map_err(|error| VoodooStoreError::new_err(error.to_string()))?;
                        Some(OperationResult::RpcId {
                            tx_id: id.tx_id,
                            nonce: id.nonce,
                        })
                    }
                    PendingOperation::CreateWorkflow {
                        workflow_type,
                        initial_step,
                        initial_state,
                        parent_id,
                        now_ms,
                    } => {
                        let id = tx
                            .create_workflow(
                                workflow_type,
                                initial_step,
                                initial_state,
                                *parent_id,
                                *now_ms,
                            )
                            .map_err(|error| VoodooStoreError::new_err(error.to_string()))?;
                        Some(OperationResult::WorkflowId(id))
                    }
                    PendingOperation::SetWorkflowStep {
                        workflow,
                        step,
                        state,
                        now_ms,
                    } => {
                        let id = resolve_workflow_reference(workflow, &results)?;
                        tx.set_workflow_step(&id, step, state, *now_ms)
                            .map_err(|error| VoodooStoreError::new_err(error.to_string()))?;
                        None
                    }
                    PendingOperation::WaitWorkflowSignal {
                        workflow,
                        signal,
                        now_ms,
                    } => {
                        let id = resolve_workflow_reference(workflow, &results)?;
                        tx.wait_for_signal(&id, signal, *now_ms)
                            .map_err(|error| VoodooStoreError::new_err(error.to_string()))?;
                        None
                    }
                    PendingOperation::WaitWorkflowUntil {
                        workflow,
                        resume_at_ms,
                        now_ms,
                    } => {
                        let id = resolve_workflow_reference(workflow, &results)?;
                        tx.wait_until(&id, *resume_at_ms, *now_ms)
                            .map_err(|error| VoodooStoreError::new_err(error.to_string()))?;
                        None
                    }
                    PendingOperation::CompleteWorkflow {
                        workflow,
                        final_state,
                        now_ms,
                    } => {
                        let id = resolve_workflow_reference(workflow, &results)?;
                        tx.complete_workflow(&id, final_state, *now_ms)
                            .map_err(|error| VoodooStoreError::new_err(error.to_string()))?;
                        None
                    }
                };
                results.push(operation_result);
            }
            tx.commit().map_err(map_engine_error)?;
            Ok(results)
        })();

        self.finished = true;
        self.restore_store(store)?;
        result
    }

    fn ensure_open(&self) -> PyResult<()> {
        if self.finished || self.store.is_none() {
            Err(TransactionFinishedError::new_err("transaction is finished"))
        } else {
            Ok(())
        }
    }

    fn lookup_pending(&self, key: &[u8]) -> PendingLookup<'_> {
        for operation in self.operations.iter().rev() {
            match operation {
                PendingOperation::Put(staged_key, value) if staged_key.as_slice() == key => {
                    return PendingLookup::Value(value);
                }
                PendingOperation::Delete(staged_key) if staged_key.as_slice() == key => {
                    return PendingLookup::Deleted;
                }
                _ => {}
            }
        }
        PendingLookup::Unchanged
    }

    fn restore_store(&mut self, store: Store) -> PyResult<()> {
        let mut slot = lock_slot(&self.slot)?;
        match &*slot {
            StoreSlot::InTransaction => {
                *slot = StoreSlot::Open(store);
                Ok(())
            }
            StoreSlot::Open(_) => Err(PyRuntimeError::new_err(
                "internal binding error: store unexpectedly available",
            )),
            StoreSlot::Closed => Err(PyRuntimeError::new_err(
                "internal binding error: store closed during transaction",
            )),
        }
    }
}

impl Drop for PyTransaction {
    fn drop(&mut self) {
        if let Some(store) = self.store.take() {
            if let Ok(mut slot) = self.slot.lock() {
                if matches!(*slot, StoreSlot::InTransaction) {
                    *slot = StoreSlot::Open(store);
                }
            }
        }
    }
}

fn resolve_object_reference(
    reference: &PendingObjectReference,
    results: &[Option<OperationResult>],
) -> PyResult<voodoo_store_core::ObjectId> {
    match reference {
        PendingObjectReference::Id(id) => Ok(*id),
        PendingObjectReference::Operation(index) => match results.get(*index).and_then(Option::as_ref)
        {
            Some(OperationResult::ObjectId(id)) => Ok(*id),
            _ => Err(VoodooStoreError::new_err(
                "object reference token does not point to a committed put_object operation",
            )),
        },
    }
}

fn resolve_workflow_reference(
    reference: &PendingWorkflowReference,
    results: &[Option<OperationResult>],
) -> PyResult<voodoo_store_core::WorkflowId> {
    match reference {
        PendingWorkflowReference::Id(id) => Ok(*id),
        PendingWorkflowReference::Operation(index) => {
            match results.get(*index).and_then(Option::as_ref) {
                Some(OperationResult::WorkflowId(id)) => Ok(*id),
                _ => Err(VoodooStoreError::new_err(
                    "workflow reference token does not point to a committed create_workflow operation",
                )),
            }
        }
    }
}

fn py_operation_result(py: Python<'_>, result: OperationResult) -> PyResult<Py<PyDict>> {
    let output = PyDict::new(py);
    match result {
        OperationResult::JobId(id) => {
            output.set_item("kind", "job")?;
            output.set_item("id", PyBytes::new(py, &id))?;
        }
        OperationResult::QueueId(id) => {
            output.set_item("kind", "queue")?;
            output.set_item("id", id)?;
        }
        OperationResult::StreamOffset(offset) => {
            output.set_item("kind", "stream")?;
            output.set_item("offset", offset)?;
        }
        OperationResult::TopicOffset(offset) => {
            output.set_item("kind", "topic")?;
            output.set_item("offset", offset)?;
        }
        OperationResult::OutboxId { tx_id, nonce } => {
            output.set_item("kind", "outbox")?;
            output.set_item("tx_id", tx_id)?;
            output.set_item("nonce", PyBytes::new(py, &nonce))?;
        }
        OperationResult::ObjectId(id) => {
            output.set_item("kind", "object")?;
            output.set_item("id", PyBytes::new(py, &id))?;
        }
        OperationResult::RpcId { tx_id, nonce } => {
            output.set_item("kind", "rpc")?;
            output.set_item("tx_id", tx_id)?;
            output.set_item("nonce", PyBytes::new(py, &nonce))?;
        }
        OperationResult::WorkflowId(id) => {
            output.set_item("kind", "workflow")?;
            output.set_item("id", PyBytes::new(py, &id))?;
        }
        OperationResult::Boolean { kind, value } => {
            output.set_item("kind", kind)?;
            output.set_item("value", value)?;
        }
    }
    Ok(output.unbind())
}

fn parse_durability(value: &str) -> PyResult<Durability> {
    match value {
        "strict" => Ok(Durability::Strict),
        "data" => Ok(Durability::Data),
        "relaxed" => Ok(Durability::Relaxed),
        _ => Err(PyValueError::new_err(
            "durability must be one of: 'strict', 'data', 'relaxed'",
        )),
    }
}

fn lock_slot(slot: &Arc<Mutex<StoreSlot>>) -> PyResult<MutexGuard<'_, StoreSlot>> {
    slot.lock()
        .map_err(|_| PyRuntimeError::new_err("Voodoo Store binding lock was poisoned"))
}

fn with_store<T>(
    slot: &Arc<Mutex<StoreSlot>>,
    function: impl FnOnce(&Store) -> PyResult<T>,
) -> PyResult<T> {
    let guard = lock_slot(slot)?;
    match &*guard {
        StoreSlot::Open(store) => function(store),
        StoreSlot::InTransaction => Err(StoreBusyError::new_err(
            "store is exclusively borrowed by an active transaction",
        )),
        StoreSlot::Closed => Err(StoreClosedError::new_err("store is closed")),
    }
}

fn with_store_mut<T>(
    slot: &Arc<Mutex<StoreSlot>>,
    function: impl FnOnce(&mut Store) -> PyResult<T>,
) -> PyResult<T> {
    let mut guard = lock_slot(slot)?;
    match &mut *guard {
        StoreSlot::Open(store) => function(store),
        StoreSlot::InTransaction => Err(StoreBusyError::new_err(
            "store is exclusively borrowed by an active transaction",
        )),
        StoreSlot::Closed => Err(StoreClosedError::new_err("store is closed")),
    }
}

fn map_engine_error(error: EngineError) -> PyErr {
    let message = error.to_string();
    match error {
        EngineError::AlreadyOpen => AlreadyOpenError::new_err(message),
        EngineError::ReservedKey => ReservedKeyError::new_err(message),
        EngineError::TransactionFinished => TransactionFinishedError::new_err(message),
        EngineError::Io(_) => IoError::new_err(message),
        EngineError::Header(_)
        | EngineError::Log(_)
        | EngineError::InvalidOperationPayload
        | EngineError::NonMonotonicSequence { .. }
        | EngineError::RecordAfterCommit(_) => CorruptionError::new_err(message),
        _ => VoodooStoreError::new_err(message),
    }
}

#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyStore>()?;
    module.add_class::<PyTransaction>()?;
    module.add_class::<PyVerificationReport>()?;
    module.add("VoodooStoreError", module.py().get_type::<VoodooStoreError>())?;
    module.add("StoreClosedError", module.py().get_type::<StoreClosedError>())?;
    module.add("StoreBusyError", module.py().get_type::<StoreBusyError>())?;
    module.add("AlreadyOpenError", module.py().get_type::<AlreadyOpenError>())?;
    module.add("ReservedKeyError", module.py().get_type::<ReservedKeyError>())?;
    module.add(
        "TransactionFinishedError",
        module.py().get_type::<TransactionFinishedError>(),
    )?;
    module.add("CorruptionError", module.py().get_type::<CorruptionError>())?;
    module.add("IoError", module.py().get_type::<IoError>())?;
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
