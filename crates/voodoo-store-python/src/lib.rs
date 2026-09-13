use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};

use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyRuntimeError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyModule};
use voodoo_store_core::{EngineError, Store};

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

#[pyclass(name = "Store")]
struct PyStore {
    slot: Arc<Mutex<StoreSlot>>,
}

#[pymethods]
impl PyStore {
    #[staticmethod]
    fn open(path: PathBuf) -> PyResult<Self> {
        let store = Store::open(path).map_err(map_engine_error)?;
        Ok(Self {
            slot: Arc::new(Mutex::new(StoreSlot::Open(store))),
        })
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
            Ok(store
                .get(key)
                .map(|value| PyBytes::new(py, value).unbind()))
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
        with_store(&self.slot, |store| store.flush().map_err(map_engine_error))
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
        self.ensure_open()?;
        let mut store = self
            .store
            .take()
            .ok_or_else(|| TransactionFinishedError::new_err("transaction is finished"))?;

        let result = (|| {
            let mut tx = store.begin().map_err(map_engine_error)?;
            for operation in &self.operations {
                match operation {
                    PendingOperation::Put(key, value) => {
                        tx.put(key, value).map_err(map_engine_error)?;
                    }
                    PendingOperation::Delete(key) => {
                        tx.delete(key).map_err(map_engine_error)?;
                    }
                }
            }
            tx.commit().map_err(map_engine_error)
        })();

        self.finished = true;
        self.restore_store(store)?;
        result
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
    fn ensure_open(&self) -> PyResult<()> {
        if self.finished || self.store.is_none() {
            Err(TransactionFinishedError::new_err("transaction is finished"))
        } else {
            Ok(())
        }
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
        if let Some(store) = self.store.take()
            && let Ok(mut slot) = self.slot.lock()
            && matches!(*slot, StoreSlot::InTransaction)
        {
            *slot = StoreSlot::Open(store);
        }
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
    Ok(())
}
