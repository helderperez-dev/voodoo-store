use std::ffi::{CStr, c_char};
use std::ptr;
use std::slice;

use voodoo_store_core::Store;

pub const VDS_OK: i32 = 0;
pub const VDS_NOT_FOUND: i32 = 1;
pub const VDS_BUFFER_TOO_SMALL: i32 = 2;
pub const VDS_INVALID_ARGUMENT: i32 = -1;
pub const VDS_INVALID_UTF8: i32 = -2;
pub const VDS_ENGINE_ERROR: i32 = -3;

#[repr(C)]
pub struct VdsHandle {
    store: Store,
}

#[unsafe(no_mangle)]
pub extern "C" fn vds_abi_version() -> u32 {
    1
}

/// Opens or creates a Voodoo Store file.
///
/// # Safety
/// `path` must point to a valid NUL-terminated UTF-8 string and `out_handle`
/// must be a valid writable pointer. On success, the caller owns the returned
/// handle and must release it with [`vds_close`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn vds_open(
    path: *const c_char,
    out_handle: *mut *mut VdsHandle,
) -> i32 {
    if path.is_null() || out_handle.is_null() {
        return VDS_INVALID_ARGUMENT;
    }

    let path = match unsafe { CStr::from_ptr(path) }.to_str() {
        Ok(path) => path,
        Err(_) => return VDS_INVALID_UTF8,
    };

    let store = match Store::open(path) {
        Ok(store) => store,
        Err(_) => return VDS_ENGINE_ERROR,
    };

    let handle = Box::new(VdsHandle { store });
    unsafe { ptr::write(out_handle, Box::into_raw(handle)) };
    VDS_OK
}

/// Closes a handle returned by [`vds_open`].
///
/// # Safety
/// `handle` must either be null or a live handle returned by `vds_open` that
/// has not already been closed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn vds_close(handle: *mut VdsHandle) {
    if !handle.is_null() {
        drop(unsafe { Box::from_raw(handle) });
    }
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
    let Some(handle) = (unsafe { handle.as_mut() }) else {
        return VDS_INVALID_ARGUMENT;
    };
    let Some(key) = (unsafe { bytes_from_raw(key_ptr, key_len) }) else {
        return VDS_INVALID_ARGUMENT;
    };
    let Some(value) = (unsafe { bytes_from_raw(value_ptr, value_len) }) else {
        return VDS_INVALID_ARGUMENT;
    };

    let result = (|| {
        let mut tx = handle.store.begin();
        tx.put(key, value)?;
        tx.commit()
    })();

    match result {
        Ok(()) => VDS_OK,
        Err(_) => VDS_ENGINE_ERROR,
    }
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
    let Some(handle) = (unsafe { handle.as_mut() }) else {
        return VDS_INVALID_ARGUMENT;
    };
    let Some(key) = (unsafe { bytes_from_raw(key_ptr, key_len) }) else {
        return VDS_INVALID_ARGUMENT;
    };

    let result = (|| {
        let mut tx = handle.store.begin();
        tx.delete(key)?;
        tx.commit()
    })();

    match result {
        Ok(()) => VDS_OK,
        Err(_) => VDS_ENGINE_ERROR,
    }
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
    let Some(handle) = (unsafe { handle.as_ref() }) else {
        return VDS_INVALID_ARGUMENT;
    };
    if out_len.is_null() {
        return VDS_INVALID_ARGUMENT;
    }
    let Some(key) = (unsafe { bytes_from_raw(key_ptr, key_len) }) else {
        return VDS_INVALID_ARGUMENT;
    };

    let Some(value) = handle.store.get(key) else {
        unsafe { ptr::write(out_len, 0) };
        return VDS_NOT_FOUND;
    };

    unsafe { ptr::write(out_len, value.len()) };

    if value.is_empty() {
        return VDS_OK;
    }
    if out_ptr.is_null() || out_capacity < value.len() {
        return VDS_BUFFER_TOO_SMALL;
    }

    unsafe { ptr::copy_nonoverlapping(value.as_ptr(), out_ptr, value.len()) };
    VDS_OK
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

    #[test]
    fn ffi_round_trip_works() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("vds-ffi-{nonce}.vstore"));
        let c_path = CString::new(path.to_string_lossy().as_bytes()).unwrap();
        let mut handle = ptr::null_mut();

        assert_eq!(unsafe { vds_open(c_path.as_ptr(), &mut handle) }, VDS_OK);
        assert_eq!(unsafe { vds_put(handle, b"name".as_ptr(), 4, b"Voodoo".as_ptr(), 6) }, VDS_OK);

        let mut required = 0usize;
        assert_eq!(
            unsafe { vds_get(handle, b"name".as_ptr(), 4, ptr::null_mut(), 0, &mut required) },
            VDS_BUFFER_TOO_SMALL
        );
        assert_eq!(required, 6);

        let mut output = vec![0u8; required];
        assert_eq!(
            unsafe { vds_get(handle, b"name".as_ptr(), 4, output.as_mut_ptr(), output.len(), &mut required) },
            VDS_OK
        );
        assert_eq!(&output, b"Voodoo");

        unsafe { vds_close(handle) };
        let _ = fs::remove_file(path);
    }
}
