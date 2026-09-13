//! Durable TTL metadata for application keys.
//!
//! TTL is stored inside the protected engine namespace so it is committed in
//! the same log as the user value. Reads can be deterministic by supplying a
//! caller-owned clock (`now_ms`), while `purge_expired` physically removes
//! expired values and metadata in one transaction.
//!
//! Semantics are explicit: a normal `Store::put` preserves an existing TTL.
//! Call `clear_ttl` to make a key persistent, or `put_with_ttl` to replace both
//! value and expiration atomically.

use thiserror::Error;

use crate::{EngineError, Store};

const TTL_PREFIX: &[u8] = b"\xffvds:ttl:";
const TTL_VALUE_VERSION: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TtlInfo {
    pub expires_at_ms: i64,
}

impl TtlInfo {
    pub const fn is_expired(self, now_ms: i64) -> bool {
        now_ms >= self.expires_at_ms
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TtlSweepReport {
    pub scanned: usize,
    pub expired: usize,
    pub removed: usize,
}

impl Store {
    /// Atomically stores `value` and attaches an absolute expiration timestamp.
    pub fn put_with_ttl(
        &mut self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        expires_at_ms: i64,
    ) -> Result<(), TtlError> {
        let key = key.as_ref();
        let metadata_key = ttl_key(key)?;
        let metadata_value = encode_ttl(TtlInfo { expires_at_ms });

        let mut tx = self.begin()?;
        tx.put(key, value)?;
        tx.put_internal(metadata_key, metadata_value)?;
        tx.commit()?;
        Ok(())
    }

    /// Returns the value only when it is live at `now_ms`.
    ///
    /// This method does not mutate storage. Use `purge_expired` to reclaim
    /// expired entries physically.
    pub fn get_at(
        &self,
        key: impl AsRef<[u8]>,
        now_ms: i64,
    ) -> Result<Option<&[u8]>, TtlError> {
        let key = key.as_ref();
        let value = match self.get(key) {
            Some(value) => value,
            None => return Ok(None),
        };
        match self.ttl(key)? {
            Some(ttl) if ttl.is_expired(now_ms) => Ok(None),
            _ => Ok(Some(value)),
        }
    }

    /// Returns TTL metadata when present.
    pub fn ttl(&self, key: impl AsRef<[u8]>) -> Result<Option<TtlInfo>, TtlError> {
        let metadata_key = ttl_key(key.as_ref())?;
        self.get(metadata_key)
            .map(decode_ttl)
            .transpose()
    }

    /// Removes TTL metadata without changing the current value.
    pub fn clear_ttl(&mut self, key: impl AsRef<[u8]>) -> Result<bool, TtlError> {
        let metadata_key = ttl_key(key.as_ref())?;
        if self.get(&metadata_key).is_none() {
            return Ok(false);
        }
        self.delete_internal(metadata_key)?;
        Ok(true)
    }

    /// Physically removes up to `limit` expired keys.
    ///
    /// A limit of zero performs no work. Malformed TTL metadata is surfaced as
    /// corruption rather than silently discarded.
    pub fn purge_expired(
        &mut self,
        now_ms: i64,
        limit: usize,
    ) -> Result<TtlSweepReport, TtlError> {
        if limit == 0 {
            return Ok(TtlSweepReport {
                scanned: 0,
                expired: 0,
                removed: 0,
            });
        }

        let metadata = self.scan_prefix(TTL_PREFIX);
        let mut scanned = 0usize;
        let mut expired_entries = Vec::new();

        for (metadata_key, metadata_value) in metadata {
            scanned += 1;
            let ttl = decode_ttl(&metadata_value)?;
            if ttl.is_expired(now_ms) {
                let user_key = decode_ttl_key(&metadata_key)?;
                expired_entries.push((metadata_key, user_key));
                if expired_entries.len() == limit {
                    break;
                }
            }
        }

        let expired = expired_entries.len();
        if expired == 0 {
            return Ok(TtlSweepReport {
                scanned,
                expired: 0,
                removed: 0,
            });
        }

        let mut tx = self.begin()?;
        let mut removed = 0usize;
        for (metadata_key, user_key) in expired_entries {
            if tx.store_get(&user_key).is_some() {
                tx.delete(&user_key)?;
                removed += 1;
            }
            tx.delete_internal(metadata_key)?;
        }
        tx.commit()?;

        Ok(TtlSweepReport {
            scanned,
            expired,
            removed,
        })
    }
}

fn ttl_key(key: &[u8]) -> Result<Vec<u8>, TtlError> {
    let key_len = u32::try_from(key.len()).map_err(|_| TtlError::KeyTooLarge)?;
    let mut encoded = Vec::with_capacity(TTL_PREFIX.len() + 4 + key.len());
    encoded.extend_from_slice(TTL_PREFIX);
    encoded.extend_from_slice(&key_len.to_be_bytes());
    encoded.extend_from_slice(key);
    Ok(encoded)
}

fn decode_ttl_key(encoded: &[u8]) -> Result<Vec<u8>, TtlError> {
    if !encoded.starts_with(TTL_PREFIX) {
        return Err(TtlError::CorruptMetadata);
    }
    let rest = &encoded[TTL_PREFIX.len()..];
    if rest.len() < 4 {
        return Err(TtlError::CorruptMetadata);
    }
    let key_len = u32::from_be_bytes(rest[..4].try_into().expect("4-byte TTL key length")) as usize;
    if rest.len() != 4 + key_len {
        return Err(TtlError::CorruptMetadata);
    }
    Ok(rest[4..].to_vec())
}

fn encode_ttl(ttl: TtlInfo) -> [u8; 9] {
    let mut encoded = [0u8; 9];
    encoded[0] = TTL_VALUE_VERSION;
    encoded[1..].copy_from_slice(&ttl.expires_at_ms.to_le_bytes());
    encoded
}

fn decode_ttl(encoded: &[u8]) -> Result<TtlInfo, TtlError> {
    if encoded.len() != 9 || encoded[0] != TTL_VALUE_VERSION {
        return Err(TtlError::CorruptMetadata);
    }
    let expires_at_ms = i64::from_le_bytes(encoded[1..].try_into().expect("8-byte expiration"));
    Ok(TtlInfo { expires_at_ms })
}

#[derive(Debug, Error)]
pub enum TtlError {
    #[error("store error: {0}")]
    Store(#[from] EngineError),
    #[error("TTL key is too large")]
    KeyTooLarge,
    #[error("TTL metadata is corrupt or unsupported")]
    CorruptMetadata,
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
        std::env::temp_dir().join(format!("voodoo-store-ttl-{name}-{nonce}.vstore"))
    }

    #[test]
    fn ttl_is_durable_and_hides_expired_values() {
        let path = temp_store_path("durable");
        {
            let mut store = Store::open(&path).unwrap();
            store.put_with_ttl(b"session", b"active", 1_000).unwrap();
            assert_eq!(store.get_at(b"session", 999).unwrap(), Some(b"active".as_slice()));
            assert_eq!(store.get_at(b"session", 1_000).unwrap(), None);
        }
        {
            let store = Store::open(&path).unwrap();
            assert_eq!(store.ttl(b"session").unwrap().unwrap().expires_at_ms, 1_000);
            assert_eq!(store.get_at(b"session", 1_001).unwrap(), None);
        }
        let _ = fs::remove_file(path);
    }

    #[test]
    fn clear_ttl_makes_value_persistent() {
        let path = temp_store_path("clear");
        let mut store = Store::open(&path).unwrap();
        store.put_with_ttl(b"token", b"abc", 10).unwrap();
        assert!(store.clear_ttl(b"token").unwrap());
        assert!(!store.clear_ttl(b"token").unwrap());
        assert_eq!(store.get_at(b"token", 999).unwrap(), Some(b"abc".as_slice()));
        drop(store);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn purge_expired_removes_value_and_metadata_atomically() {
        let path = temp_store_path("purge");
        let mut store = Store::open(&path).unwrap();
        store.put_with_ttl(b"expired", b"x", 10).unwrap();
        store.put_with_ttl(b"live", b"y", 100).unwrap();

        let report = store.purge_expired(50, 100).unwrap();
        assert_eq!(report.expired, 1);
        assert_eq!(report.removed, 1);
        assert_eq!(store.get(b"expired"), None);
        assert_eq!(store.ttl(b"expired").unwrap(), None);
        assert_eq!(store.get_at(b"live", 50).unwrap(), Some(b"y".as_slice()));

        drop(store);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn ordinary_put_explicitly_preserves_existing_ttl() {
        let path = temp_store_path("preserve");
        let mut store = Store::open(&path).unwrap();
        store.put_with_ttl(b"key", b"v1", 100).unwrap();
        store.put(b"key", b"v2").unwrap();
        assert_eq!(store.get_at(b"key", 99).unwrap(), Some(b"v2".as_slice()));
        assert_eq!(store.get_at(b"key", 100).unwrap(), None);
        drop(store);
        let _ = fs::remove_file(path);
    }
}
