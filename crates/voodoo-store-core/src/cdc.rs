//! Durable change-data-capture metadata generated automatically on commit.
//!
//! CDC records describe every logical key mutation that participated in a
//! committed transaction. Put records carry value length and SHA-256 digest,
//! not a duplicate copy of the value; this keeps large objects from being
//! duplicated while still providing deterministic invalidation/change metadata.

use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::engine::Operation;
use crate::{EngineError, Store, Transaction};

pub(crate) const CDC_PREFIX: &[u8] = b"\xffvds:cdc:change:";
const VERSION: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ChangeKind {
    Put = 0,
    Delete = 1,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeRecord {
    pub tx_id: u64,
    pub sequence: u32,
    pub kind: ChangeKind,
    pub key: Vec<u8>,
    pub value_len: Option<u64>,
    pub value_sha256: Option<[u8; 32]>,
}

#[derive(Debug, Error)]
pub enum ChangeFeedError {
    #[error("store error: {0}")]
    Store(#[from] EngineError),
    #[error("CDC record is corrupt or unsupported")]
    CorruptRecord,
    #[error("CDC field is too large")]
    FieldTooLarge,
}

/// Stages CDC metadata for the mutations that existed before the commit path
/// started adding CDC records. CDC's own namespace is deliberately excluded to
/// avoid recursive feeds and to let pruning remain silent.
pub(crate) fn stage_changes(
    tx: &mut Transaction<'_>,
    operations: &[Operation],
) -> Result<(), EngineError> {
    let mut sequence = 0u32;
    for operation in operations {
        let (key, encoded) = match operation {
            Operation::Put(key, value) if !key.starts_with(CDC_PREFIX) => {
                let record = ChangeRecord {
                    tx_id: tx.id(),
                    sequence,
                    kind: ChangeKind::Put,
                    key: key.clone(),
                    value_len: Some(value.len() as u64),
                    value_sha256: Some(Sha256::digest(value).into()),
                };
                (change_key(tx.id(), sequence), encode_change(&record).map_err(map_cdc_error)?)
            }
            Operation::Delete(key) if !key.starts_with(CDC_PREFIX) => {
                let record = ChangeRecord {
                    tx_id: tx.id(),
                    sequence,
                    kind: ChangeKind::Delete,
                    key: key.clone(),
                    value_len: None,
                    value_sha256: None,
                };
                (change_key(tx.id(), sequence), encode_change(&record).map_err(map_cdc_error)?)
            }
            _ => continue,
        };
        tx.put_internal(key, encoded)?;
        sequence = sequence
            .checked_add(1)
            .ok_or(EngineError::CounterExhausted)?;
    }
    Ok(())
}

impl Store {
    /// Returns committed changes ordered by transaction id and operation order.
    pub fn changes_after(
        &self,
        after_tx_id: Option<u64>,
        limit: usize,
    ) -> Result<Vec<ChangeRecord>, ChangeFeedError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut changes = Vec::new();
        for (_, encoded) in self.scan_prefix(CDC_PREFIX) {
            let change = decode_change(&encoded)?;
            if after_tx_id.is_some_and(|after| change.tx_id <= after) {
                continue;
            }
            changes.push(change);
            if changes.len() == limit {
                break;
            }
        }
        Ok(changes)
    }

    pub fn changes_for_transaction(
        &self,
        tx_id: u64,
    ) -> Result<Vec<ChangeRecord>, ChangeFeedError> {
        let mut changes = Vec::new();
        for (_, encoded) in self.scan_prefix(change_tx_prefix(tx_id)) {
            changes.push(decode_change(&encoded)?);
        }
        changes.sort_unstable_by_key(|change| change.sequence);
        Ok(changes)
    }

    /// Deletes at most `limit` CDC records up to and including `tx_id`.
    /// Pruning does not recursively create new CDC records.
    pub fn prune_changes_through(
        &mut self,
        tx_id: u64,
        limit: usize,
    ) -> Result<usize, ChangeFeedError> {
        if limit == 0 {
            return Ok(0);
        }
        let mut keys = Vec::new();
        for (key, encoded) in self.scan_prefix(CDC_PREFIX) {
            let change = decode_change(&encoded)?;
            if change.tx_id > tx_id {
                break;
            }
            keys.push(key);
            if keys.len() == limit {
                break;
            }
        }
        if keys.is_empty() {
            return Ok(0);
        }
        let removed = keys.len();
        let mut tx = self.begin()?;
        for key in keys {
            tx.delete_internal(key)?;
        }
        tx.commit()?;
        Ok(removed)
    }
}

fn map_cdc_error(error: ChangeFeedError) -> EngineError {
    match error {
        ChangeFeedError::Store(error) => error,
        ChangeFeedError::CorruptRecord | ChangeFeedError::FieldTooLarge => {
            EngineError::InvalidOperationPayload
        }
    }
}

fn change_tx_prefix(tx_id: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(CDC_PREFIX.len() + 8);
    key.extend_from_slice(CDC_PREFIX);
    key.extend_from_slice(&tx_id.to_be_bytes());
    key
}

fn change_key(tx_id: u64, sequence: u32) -> Vec<u8> {
    let mut key = change_tx_prefix(tx_id);
    key.extend_from_slice(&sequence.to_be_bytes());
    key
}

fn encode_change(change: &ChangeRecord) -> Result<Vec<u8>, ChangeFeedError> {
    let key_len = u32::try_from(change.key.len()).map_err(|_| ChangeFeedError::FieldTooLarge)?;
    let mut out = Vec::with_capacity(1 + 8 + 4 + 1 + 4 + change.key.len() + 8 + 32);
    out.push(VERSION);
    out.extend_from_slice(&change.tx_id.to_le_bytes());
    out.extend_from_slice(&change.sequence.to_le_bytes());
    out.push(change.kind as u8);
    out.extend_from_slice(&key_len.to_le_bytes());
    out.extend_from_slice(&change.key);
    match (change.value_len, change.value_sha256) {
        (Some(len), Some(hash)) => {
            out.push(1);
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(&hash);
        }
        (None, None) => out.push(0),
        _ => return Err(ChangeFeedError::CorruptRecord),
    }
    Ok(out)
}

fn decode_change(encoded: &[u8]) -> Result<ChangeRecord, ChangeFeedError> {
    let mut cursor = Cursor::new(encoded);
    if cursor.u8()? != VERSION {
        return Err(ChangeFeedError::CorruptRecord);
    }
    let tx_id = cursor.u64()?;
    let sequence = cursor.u32()?;
    let kind = match cursor.u8()? {
        0 => ChangeKind::Put,
        1 => ChangeKind::Delete,
        _ => return Err(ChangeFeedError::CorruptRecord),
    };
    let key = cursor.bytes()?;
    let (value_len, value_sha256) = match cursor.u8()? {
        0 if kind == ChangeKind::Delete => (None, None),
        1 if kind == ChangeKind::Put => (Some(cursor.u64()?), Some(cursor.array_32()?)),
        _ => return Err(ChangeFeedError::CorruptRecord),
    };
    if !cursor.finished() {
        return Err(ChangeFeedError::CorruptRecord);
    }
    Ok(ChangeRecord {
        tx_id,
        sequence,
        kind,
        key,
        value_len,
        value_sha256,
    })
}

struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], ChangeFeedError> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or(ChangeFeedError::CorruptRecord)?;
        let value = self
            .bytes
            .get(self.pos..end)
            .ok_or(ChangeFeedError::CorruptRecord)?;
        self.pos = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, ChangeFeedError> {
        Ok(*self
            .take(1)?
            .first()
            .ok_or(ChangeFeedError::CorruptRecord)?)
    }

    fn u32(&mut self) -> Result<u32, ChangeFeedError> {
        Ok(u32::from_le_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| ChangeFeedError::CorruptRecord)?,
        ))
    }

    fn u64(&mut self) -> Result<u64, ChangeFeedError> {
        Ok(u64::from_le_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| ChangeFeedError::CorruptRecord)?,
        ))
    }

    fn bytes(&mut self) -> Result<Vec<u8>, ChangeFeedError> {
        let len = usize::try_from(self.u32()?).map_err(|_| ChangeFeedError::CorruptRecord)?;
        Ok(self.take(len)?.to_vec())
    }

    fn array_32(&mut self) -> Result<[u8; 32], ChangeFeedError> {
        self.take(32)?
            .try_into()
            .map_err(|_| ChangeFeedError::CorruptRecord)
    }

    fn finished(&self) -> bool {
        self.pos == self.bytes.len()
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::PushOptions;

    use super::*;

    fn temp_store_path(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("voodoo-store-cdc-{name}-{nonce}.vstore"))
    }

    #[test]
    fn commit_automatically_emits_ordered_changes() {
        let path = temp_store_path("commit");
        let tx_id;
        {
            let mut store = Store::open(&path).unwrap();
            let mut tx = store.begin().unwrap();
            tx_id = tx.id();
            tx.put(b"user:1", b"Ada").unwrap();
            tx.put(b"user:2", b"Grace").unwrap();
            tx.delete(b"missing").unwrap();
            tx.commit().unwrap();

            let changes = store.changes_for_transaction(tx_id).unwrap();
            assert_eq!(changes.len(), 3);
            assert_eq!(changes[0].key, b"user:1");
            assert_eq!(changes[0].kind, ChangeKind::Put);
            assert_eq!(changes[1].key, b"user:2");
            assert_eq!(changes[2].kind, ChangeKind::Delete);
        }
        let _ = fs::remove_file(path);
    }

    #[test]
    fn rollback_never_emits_changes() {
        let path = temp_store_path("rollback");
        let tx_id;
        {
            let mut store = Store::open(&path).unwrap();
            let mut tx = store.begin().unwrap();
            tx_id = tx.id();
            tx.put(b"ghost", b"value").unwrap();
            tx.rollback().unwrap();
            assert!(store.changes_for_transaction(tx_id).unwrap().is_empty());
        }
        let _ = fs::remove_file(path);
    }

    #[test]
    fn cross_domain_changes_are_visible_without_copying_object_payloads() {
        let path = temp_store_path("domains");
        let tx_id;
        {
            let mut store = Store::open(&path).unwrap();
            let mut tx = store.begin().unwrap();
            tx_id = tx.id();
            tx.put(b"state", b"ready").unwrap();
            tx.push_queue(b"jobs", b"work", PushOptions::default())
                .unwrap();
            let object = tx.put_object(vec![7u8; 1024]).unwrap();
            tx.link_object(b"files", b"one", &object).unwrap();
            tx.commit().unwrap();

            let changes = store.changes_for_transaction(tx_id).unwrap();
            assert!(changes.len() >= 5);
            assert!(changes.iter().all(|change| change.key != CDC_PREFIX));
            let object_change = changes
                .iter()
                .find(|change| change.key.starts_with(b"\xffvds:obj:data:"))
                .unwrap();
            assert_eq!(object_change.value_len, Some(1033));
            assert!(object_change.value_sha256.is_some());
        }
        let _ = fs::remove_file(path);
    }

    #[test]
    fn pruning_is_silent_and_bounded() {
        let path = temp_store_path("prune");
        let tx_id;
        {
            let mut store = Store::open(&path).unwrap();
            store.put(b"a", b"1").unwrap();
            let mut tx = store.begin().unwrap();
            tx_id = tx.id();
            tx.put(b"b", b"2").unwrap();
            tx.put(b"c", b"3").unwrap();
            tx.commit().unwrap();
            let before = store.changes_after(None, 100).unwrap().len();
            assert!(before >= 3);
            assert_eq!(store.prune_changes_through(tx_id, 1).unwrap(), 1);
            let after = store.changes_after(None, 100).unwrap().len();
            assert_eq!(after + 1, before);
        }
        let _ = fs::remove_file(path);
    }
}
