//! Atomic single-store mutation primitives built on Voodoo Store transactions.
//!
//! These operations rely on the Store's exclusive writer lock and `&mut Store`
//! access. They do not introduce a new on-disk record kind: the resulting
//! mutation is persisted through the normal transactional Put/Delete log.

use thiserror::Error;

use crate::{EngineError, Store};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CasOutcome {
    Swapped,
    Mismatch,
}

impl CasOutcome {
    pub const fn swapped(self) -> bool {
        matches!(self, Self::Swapped)
    }
}

impl Store {
    /// Atomically replaces or deletes `key` only when its current value matches
    /// `expected`.
    ///
    /// `expected = None` means the key must not exist. `replacement = None`
    /// deletes the key when the comparison succeeds.
    pub fn compare_and_swap(
        &mut self,
        key: impl AsRef<[u8]>,
        expected: Option<&[u8]>,
        replacement: Option<&[u8]>,
    ) -> Result<CasOutcome, EngineError> {
        let key = key.as_ref();
        if self.get(key) != expected {
            return Ok(CasOutcome::Mismatch);
        }

        let mut tx = self.begin()?;
        match replacement {
            Some(value) => tx.put(key, value)?,
            None => tx.delete(key)?,
        }
        tx.commit()?;
        Ok(CasOutcome::Swapped)
    }

    /// Atomically adds `delta` to a signed 64-bit little-endian counter.
    ///
    /// A missing key starts at zero. Existing values must be exactly eight
    /// bytes and are interpreted as a little-endian `i64`.
    pub fn increment_i64(
        &mut self,
        key: impl AsRef<[u8]>,
        delta: i64,
    ) -> Result<i64, AtomicError> {
        let key = key.as_ref();
        let current = match self.get(key) {
            Some(bytes) => {
                let encoded: [u8; 8] = bytes.try_into().map_err(|_| AtomicError::InvalidCounter)?;
                i64::from_le_bytes(encoded)
            }
            None => 0,
        };
        let next = current.checked_add(delta).ok_or(AtomicError::Overflow)?;
        self.put(key, next.to_le_bytes())?;
        Ok(next)
    }
}

#[derive(Debug, Error)]
pub enum AtomicError {
    #[error("store error: {0}")]
    Store(#[from] EngineError),
    #[error("counter value is not a valid little-endian i64")]
    InvalidCounter,
    #[error("counter overflow")]
    Overflow,
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    fn temp_store_path(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("voodoo-store-atomic-{name}-{nonce}.vstore"))
    }

    #[test]
    fn compare_and_swap_requires_exact_expected_value() {
        let path = temp_store_path("cas");
        let mut store = Store::open(&path).unwrap();

        assert!(
            store
                .compare_and_swap(b"state", None, Some(b"ready"))
                .unwrap()
                .swapped()
        );
        assert_eq!(store.get(b"state"), Some(b"ready".as_slice()));

        assert_eq!(
            store
                .compare_and_swap(b"state", Some(b"wrong"), Some(b"running"))
                .unwrap(),
            CasOutcome::Mismatch
        );
        assert_eq!(store.get(b"state"), Some(b"ready".as_slice()));

        assert!(
            store
                .compare_and_swap(b"state", Some(b"ready"), Some(b"running"))
                .unwrap()
                .swapped()
        );
        assert!(
            store
                .compare_and_swap(b"state", Some(b"running"), None)
                .unwrap()
                .swapped()
        );
        assert_eq!(store.get(b"state"), None);

        drop(store);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn counter_is_atomic_durable_and_supports_negative_deltas() {
        let path = temp_store_path("counter");
        {
            let mut store = Store::open(&path).unwrap();
            assert_eq!(store.increment_i64(b"count", 5).unwrap(), 5);
            assert_eq!(store.increment_i64(b"count", 7).unwrap(), 12);
            assert_eq!(store.increment_i64(b"count", -2).unwrap(), 10);
        }
        {
            let store = Store::open(&path).unwrap();
            let bytes: [u8; 8] = store.get(b"count").unwrap().try_into().unwrap();
            assert_eq!(i64::from_le_bytes(bytes), 10);
        }
        let _ = fs::remove_file(path);
    }

    #[test]
    fn invalid_or_overflowing_counter_is_rejected_without_mutation() {
        let path = temp_store_path("counter-errors");
        let mut store = Store::open(&path).unwrap();
        store.put(b"invalid", b"123").unwrap();
        assert!(matches!(
            store.increment_i64(b"invalid", 1),
            Err(AtomicError::InvalidCounter)
        ));

        store.put(b"max", i64::MAX.to_le_bytes()).unwrap();
        assert!(matches!(
            store.increment_i64(b"max", 1),
            Err(AtomicError::Overflow)
        ));
        let bytes: [u8; 8] = store.get(b"max").unwrap().try_into().unwrap();
        assert_eq!(i64::from_le_bytes(bytes), i64::MAX);

        drop(store);
        let _ = fs::remove_file(path);
    }
}
