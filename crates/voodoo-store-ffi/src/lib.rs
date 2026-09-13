use std::cell::RefCell;
use std::ffi::{CStr, c_char};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;
use std::slice;

use voodoo_store_core::Store;

pub const VDS_OK: i32 = 0;
pub const VDS_NOT_FOUND: i32 = 1;
pub const VDS_BUFFER_TOO_SMALL: i32 = 2;
pub const VDS_INVALID_ARGUMENT: i32 = -1;
pub const VDS_INVALID_UTF8: i32 = -2;
pub const VDS_ENGINE_ERROR: i32 = -3;
pub const VDS_PANIC: i32 = -4;

thread_local! {
    static LAST_ERROR: RefCell<String> = const { RefCell::new(String::new()) };
}

#[repr(C)]
pub struct VdsHandle {
    store: Store,
}

#[derive(Debug)]
enum FfiOperation {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

#[repr(C)]
pub struct VdsTransaction {
    handle: *mut VdsHandle,
    operations: Vec<FfiOperation>,
}

#[unsafe(no_mangle)]
pub extern "C" fn vds_abi_version() -> u32 {
    2
}

/// Copies the calling thread's last Voodoo Store FFI error into caller-owned memory.
///
/// `out_len` is set to the required byte count excluding any NUL terminator. The
/// returned message is UTF-8 but is not NUL-terminated.
///
/// # Safety
/// `out_len` must be a writable pointer. When non-null, `out_ptr` must be
/// writable for `out_capacity` bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn vds_last_error_message(
    out_ptr: *mut u8,
    out_capacity: usize,
    out_len: *mut usize,
) -> i32 {
    ffi_guard(|| {
        if out_len.is_null() {
            return VDS_INVALID_ARGUMENT;
        }
        LAST_ERROR.with(|slot| {
            let message = slot.borrow();
            unsafe { ptr::write(out_len, message.len()) };
            if message.is_empty() {
                return VDS_OK;
            }
            if out_ptr.is_null() || out_capacity < message.len() {
                return VDS_BUFFER_TOO_SMALL;
            }
            unsafe { ptr::copy_nonoverlapping(message.as_ptr(), out_ptr, message.len()) };
            VDS_OK
        })
    })
}

/// Opens or creates a Voodoo Store file.
///
/// # Safety
/// `path` must point to a valid NUL-terminated UTF-8 string and `out_handle`
/// must be a valid writable pointer. On success, the caller owns the returned
/// handle and must release it with [`vds_close`]. A handle must not be used
/// concurrently from multiple threads without external synchronization.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn vds_open(path: *const c_char, out_handle: *mut *mut VdsHandle) -> i32 {
    ffi_guard(|| {
        if path.is_null() || out_handle.is_null() {
            return invalid_argument("path and out_handle are required");
        }

        let path = match unsafe { CStr::from_ptr(path) }.to_str() {
            Ok(path) => path,
            Err(error) => {
                set_last_error(format!("store path is not valid UTF-8: {error}"));
                return VDS_INVALID_UTF8;
            }
        };

        let store = match Store::open(path) {
            Ok(store) => store,
            Err(error) => return engine_error(error),
        };

        clear_last_error();
        let handle = Box::new(VdsHandle { store });
        unsafe { ptr::write(out_handle, Box::into_raw(handle)) };
        VDS_OK
    })
}

/// Closes a handle returned by [`vds_open`].
///
/// # Safety
/// `handle` must either be null or a live handle returned by `vds_open` that
/// has not already been closed. No transaction may still reference the handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn vds_close(handle: *mut VdsHandle) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if !handle.is_null() {
            drop(unsafe { Box::from_raw(handle) });
        }
    }))
    .map_err(|_| set_last_error("panic crossed vds_close boundary"));
}

/// Stores a byte key/value pair using an automatically committed transaction.
///
/// # Safety
/// `handle` must be valid. Non-zero-length key/value buffers must point to
/// readable memory for the supplied length.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn vds_put(
    handle: *mut VdsHandle,
    key_ptr: *const u8,
    key_len: usize,
    value_ptr: *const u8,
    value_len: usize,
) -> i32 {
    ffi_guard(|| {
        let Some(handle) = (unsafe { handle.as_mut() }) else {
            return invalid_argument("handle is required");
        };
        let Some(key) = (unsafe { bytes_from_raw(key_ptr, key_len) }) else {
            return invalid_argument("key pointer is null for non-empty key");
        };
        let Some(value) = (unsafe { bytes_from_raw(value_ptr, value_len) }) else {
            return invalid_argument("value pointer is null for non-empty value");
        };

        match handle.store.put(key, value) {
            Ok(()) => success(),
            Err(error) => engine_error(error),
        }
    })
}

/// Deletes a key using an automatically committed transaction.
///
/// # Safety
/// `handle` must be valid. A non-zero-length key buffer must point to readable
/// memory for the supplied length.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn vds_delete(
    handle: *mut VdsHandle,
    key_ptr: *const u8,
    key_len: usize,
) -> i32 {
    ffi_guard(|| {
        let Some(handle) = (unsafe { handle.as_mut() }) else {
            return invalid_argument("handle is required");
        };
        let Some(key) = (unsafe { bytes_from_raw(key_ptr, key_len) }) else {
            return invalid_argument("key pointer is null for non-empty key");
        };

        match handle.store.delete(key) {
            Ok(()) => success(),
            Err(error) => engine_error(error),
        }
    })
}

/// Reads a value into caller-owned memory.
///
/// `out_len` is always set to the required value length when the key exists.
/// Pass a null `out_ptr` or a too-small capacity to query the required size;
/// the function returns `VDS_BUFFER_TOO_SMALL` without copying.
///
/// # Safety
/// `handle` and `out_len` must be valid. A non-zero-length key buffer must be
/// readable. When non-null, `out_ptr` must be writable for `out_capacity` bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn vds_get(
    handle: *const VdsHandle,
    key_ptr: *const u8,
    key_len: usize,
    out_ptr: *mut u8,
    out_capacity: usize,
    out_len: *mut usize,
) -> i32 {
    ffi_guard(|| {
        let Some(handle) = (unsafe { handle.as_ref() }) else {
            return invalid_argument("handle is required");
        };
        if out_len.is_null() {
            return invalid_argument("out_len is required");
        }
        let Some(key) = (unsafe { bytes_from_raw(key_ptr, key_len) }) else {
            return invalid_argument("key pointer is null for non-empty key");
        };

        let Some(value) = handle.store.get(key) else {
            unsafe { ptr::write(out_len, 0) };
            clear_last_error();
            return VDS_NOT_FOUND;
        };

        unsafe { ptr::write(out_len, value.len()) };
        if value.is_empty() {
            return success();
        }
        if out_ptr.is_null() || out_capacity < value.len() {
            clear_last_error();
            return VDS_BUFFER_TOO_SMALL;
        }

        unsafe { ptr::copy_nonoverlapping(value.as_ptr(), out_ptr, value.len()) };
        success()
    })
}

/// Creates a buffered C transaction.
///
/// The transaction owns copies of staged keys and values. `handle` must remain
/// alive until commit or rollback. Transactions are not thread-safe.
///
/// # Safety
/// `handle` and `out_tx` must be valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn vds_tx_begin(
    handle: *mut VdsHandle,
    out_tx: *mut *mut VdsTransaction,
) -> i32 {
    ffi_guard(|| {
        if handle.is_null() || out_tx.is_null() {
            return invalid_argument("handle and out_tx are required");
        }
        let transaction = Box::new(VdsTransaction {
            handle,
            operations: Vec::new(),
        });
        unsafe { ptr::write(out_tx, Box::into_raw(transaction)) };
        success()
    })
}

/// Stages a put in a transaction.
///
/// # Safety
/// `tx` must be a live transaction. Non-empty buffers must be readable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn vds_tx_put(
    tx: *mut VdsTransaction,
    key_ptr: *const u8,
    key_len: usize,
    value_ptr: *const u8,
    value_len: usize,
) -> i32 {
    ffi_guard(|| {
        let Some(tx) = (unsafe { tx.as_mut() }) else {
            return invalid_argument("transaction is required");
        };
        let Some(key) = (unsafe { bytes_from_raw(key_ptr, key_len) }) else {
            return invalid_argument("key pointer is null for non-empty key");
        };
        let Some(value) = (unsafe { bytes_from_raw(value_ptr, value_len) }) else {
            return invalid_argument("value pointer is null for non-empty value");
        };
        tx.operations
            .push(FfiOperation::Put(key.to_vec(), value.to_vec()));
        success()
    })
}

/// Stages a delete in a transaction.
///
/// # Safety
/// `tx` must be a live transaction. A non-empty key buffer must be readable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn vds_tx_delete(
    tx: *mut VdsTransaction,
    key_ptr: *const u8,
    key_len: usize,
) -> i32 {
    ffi_guard(|| {
        let Some(tx) = (unsafe { tx.as_mut() }) else {
            return invalid_argument("transaction is required");
        };
        let Some(key) = (unsafe { bytes_from_raw(key_ptr, key_len) }) else {
            return invalid_argument("key pointer is null for non-empty key");
        };
        tx.operations.push(FfiOperation::Delete(key.to_vec()));
        success()
    })
}

/// Atomically commits all staged operations and consumes the transaction.
///
/// # Safety
/// `tx` must be a live transaction returned by `vds_tx_begin` and its Store
/// handle must still be alive. The transaction must not be used again.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn vds_tx_commit(tx: *mut VdsTransaction) -> i32 {
    ffi_guard(|| {
        if tx.is_null() {
            return invalid_argument("transaction is required");
        }
        let tx = unsafe { Box::from_raw(tx) };
        let Some(handle) = (unsafe { tx.handle.as_mut() }) else {
            return invalid_argument("transaction store handle is no longer valid");
        };
        let mut transaction = match handle.store.begin() {
            Ok(transaction) => transaction,
            Err(error) => return engine_error(error),
        };
        for operation in tx.operations {
            let result = match operation {
                FfiOperation::Put(key, value) => transaction.put(key, value),
                FfiOperation::Delete(key) => transaction.delete(key),
            };
            if let Err(error) = result {
                return engine_error(error);
            }
        }
        match transaction.commit() {
            Ok(()) => success(),
            Err(error) => engine_error(error),
        }
    })
}

/// Discards all staged operations and consumes the transaction.
///
/// # Safety
/// `tx` must either be null or a live transaction returned by `vds_tx_begin`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn vds_tx_rollback(tx: *mut VdsTransaction) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if !tx.is_null() {
            drop(unsafe { Box::from_raw(tx) });
        }
    }))
    .map_err(|_| set_last_error("panic crossed vds_tx_rollback boundary"));
}

fn ffi_guard(operation: impl FnOnce() -> i32) -> i32 {
    match catch_unwind(AssertUnwindSafe(operation)) {
        Ok(status) => status,
        Err(_) => {
            set_last_error("panic caught at Voodoo Store C ABI boundary");
            VDS_PANIC
        }
    }
}

fn success() -> i32 {
    clear_last_error();
    VDS_OK
}

fn invalid_argument(message: impl Into<String>) -> i32 {
    set_last_error(message);
    VDS_INVALID_ARGUMENT
}

fn engine_error(error: impl std::fmt::Display) -> i32 {
    set_last_error(error.to_string());
    VDS_ENGINE_ERROR
}

fn set_last_error(message: impl Into<String>) {
    let message = message.into();
    LAST_ERROR.with(|slot| *slot.borrow_mut() = message);
}

fn clear_last_error() {
    LAST_ERROR.with(|slot| slot.borrow_mut().clear());
}

unsafe fn bytes_from_raw<'a>(ptr: *const u8, len: usize) -> Option<&'a [u8]> {
    if len == 0 {
        return Some(&[]);
    }
    if ptr.is_null() {
        return None;
    }
    Some(unsafe { slice::from_raw_parts(ptr, len) })
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    fn temp_path(name: &str) -> (std::path::PathBuf, CString) {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("vds-ffi-{name}-{nonce}.vstore"));
        let c_path = CString::new(path.to_string_lossy().as_bytes()).unwrap();
        (path, c_path)
    }

    #[test]
    fn ffi_round_trip_works() {
        let (path, c_path) = temp_path("roundtrip");
        let mut handle = ptr::null_mut();

        assert_eq!(unsafe { vds_open(c_path.as_ptr(), &mut handle) }, VDS_OK);
        assert_eq!(
            unsafe { vds_put(handle, b"name".as_ptr(), 4, b"Voodoo".as_ptr(), 6) },
            VDS_OK
        );

        let mut required = 0usize;
        assert_eq!(
            unsafe {
                vds_get(
                    handle,
                    b"name".as_ptr(),
                    4,
                    ptr::null_mut(),
                    0,
                    &mut required,
                )
            },
            VDS_BUFFER_TOO_SMALL
        );
        assert_eq!(required, 6);

        let mut output = vec![0u8; required];
        assert_eq!(
            unsafe {
                vds_get(
                    handle,
                    b"name".as_ptr(),
                    4,
                    output.as_mut_ptr(),
                    output.len(),
                    &mut required,
                )
            },
            VDS_OK
        );
        assert_eq!(&output, b"Voodoo");

        unsafe { vds_close(handle) };
        let _ = fs::remove_file(path);
    }

    #[test]
    fn ffi_transaction_commits_atomically() {
        let (path, c_path) = temp_path("transaction");
        let mut handle = ptr::null_mut();
        assert_eq!(unsafe { vds_open(c_path.as_ptr(), &mut handle) }, VDS_OK);
        let mut tx = ptr::null_mut();
        assert_eq!(unsafe { vds_tx_begin(handle, &mut tx) }, VDS_OK);
        assert_eq!(
            unsafe { vds_tx_put(tx, b"a".as_ptr(), 1, b"1".as_ptr(), 1) },
            VDS_OK
        );
        assert_eq!(
            unsafe { vds_tx_put(tx, b"b".as_ptr(), 1, b"2".as_ptr(), 1) },
            VDS_OK
        );
        assert_eq!(unsafe { vds_tx_commit(tx) }, VDS_OK);
        assert_eq!(unsafe { vds_delete(handle, b"a".as_ptr(), 1) }, VDS_OK);
        unsafe { vds_close(handle) };

        let store = Store::open(&path).unwrap();
        assert_eq!(store.get(b"a"), None);
        assert_eq!(store.get(b"b"), Some(b"2".as_slice()));
        drop(store);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn ffi_last_error_is_thread_local_and_queryable() {
        let status = unsafe { vds_put(ptr::null_mut(), ptr::null(), 0, ptr::null(), 0) };
        assert_eq!(status, VDS_INVALID_ARGUMENT);
        let mut required = 0usize;
        assert_eq!(
            unsafe { vds_last_error_message(ptr::null_mut(), 0, &mut required) },
            VDS_BUFFER_TOO_SMALL
        );
        assert!(required > 0);
        let mut output = vec![0u8; required];
        assert_eq!(
            unsafe { vds_last_error_message(output.as_mut_ptr(), output.len(), &mut required) },
            VDS_OK
        );
        assert!(String::from_utf8(output).unwrap().contains("handle"));
    }
}
