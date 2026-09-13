use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::log::{HEADER_LEN, LogRecord, RecordKind, StoreError, encoded_record_len_from_prefix};

#[derive(Debug, Clone, PartialEq, Eq)]
enum Operation {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

#[derive(Debug)]
pub struct Store {
    path: PathBuf,
    file: File,
    state: HashMap<Vec<u8>, Vec<u8>>,
    next_tx_id: u64,
    next_sequence: u64,
}

impl Store {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, EngineError> {
        let path = path.as_ref().to_path_buf();
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)?;

        let recovery = recover(&mut file)?;
        file.seek(SeekFrom::End(0))?;

        Ok(Self {
            path,
            file,
            state: recovery.state,
            next_tx_id: recovery.max_tx_id.saturating_add(1).max(1),
            next_sequence: recovery.max_sequence.saturating_add(1).max(1),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn get(&self, key: impl AsRef<[u8]>) -> Option<&[u8]> {
        self.state.get(key.as_ref()).map(Vec::as_slice)
    }

    pub fn contains_key(&self, key: impl AsRef<[u8]>) -> bool {
        self.state.contains_key(key.as_ref())
    }

    pub fn len(&self) -> usize {
        self.state.len()
    }

    pub fn is_empty(&self) -> bool {
        self.state.is_empty()
    }

    pub fn begin(&mut self) -> Transaction<'_> {
        let tx_id = self.next_tx_id;
        self.next_tx_id = self.next_tx_id.saturating_add(1);

        Transaction {
            store: self,
            tx_id,
            operations: Vec::new(),
            finished: false,
        }
    }

    fn append(
        &mut self,
        kind: RecordKind,
        tx_id: u64,
        payload: Vec<u8>,
    ) -> Result<(), EngineError> {
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        let bytes = LogRecord::new(kind, tx_id, sequence, payload).encode()?;
        self.file.write_all(&bytes)?;
        Ok(())
    }
}

pub struct Transaction<'a> {
    store: &'a mut Store,
    tx_id: u64,
    operations: Vec<Operation>,
    finished: bool,
}

impl Transaction<'_> {
    pub fn id(&self) -> u64 {
        self.tx_id
    }

    pub fn put(
        &mut self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<(), EngineError> {
        self.ensure_open()?;
        let key = key.as_ref().to_vec();
        let value = value.as_ref().to_vec();
        self.store
            .append(RecordKind::Put, self.tx_id, encode_put(&key, &value)?)?;
        self.operations.push(Operation::Put(key, value));
        Ok(())
    }

    pub fn delete(&mut self, key: impl AsRef<[u8]>) -> Result<(), EngineError> {
        self.ensure_open()?;
        let key = key.as_ref().to_vec();
        self.store
            .append(RecordKind::Delete, self.tx_id, encode_delete(&key)?)?;
        self.operations.push(Operation::Delete(key));
        Ok(())
    }

    pub fn commit(mut self) -> Result<(), EngineError> {
        self.ensure_open()?;
        self.store
            .append(RecordKind::Commit, self.tx_id, Vec::new())?;
        self.store.file.sync_data()?;
        apply_operations(&mut self.store.state, &self.operations);
        self.finished = true;
        Ok(())
    }

    pub fn rollback(mut self) -> Result<(), EngineError> {
        self.ensure_open()?;
        // There is intentionally no rollback record in v0.1. Operations already
        // appended to the log remain uncommitted and are ignored during recovery.
        self.finished = true;
        Ok(())
    }

    fn ensure_open(&self) -> Result<(), EngineError> {
        if self.finished {
            Err(EngineError::TransactionFinished)
        } else {
            Ok(())
        }
    }
}

#[derive(Debug)]
struct Recovery {
    state: HashMap<Vec<u8>, Vec<u8>>,
    max_tx_id: u64,
    max_sequence: u64,
}

fn recover(file: &mut File) -> Result<Recovery, EngineError> {
    file.seek(SeekFrom::Start(0))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;

    let mut state = HashMap::new();
    let mut pending: HashMap<u64, Vec<Operation>> = HashMap::new();
    let mut offset = 0usize;
    let mut max_tx_id = 0u64;
    let mut max_sequence = 0u64;

    while offset < bytes.len() {
        let remaining = &bytes[offset..];

        // A partial final record is a valid crash artifact. It is ignored.
        if remaining.len() < HEADER_LEN {
            break;
        }

        let record_len = match encoded_record_len_from_prefix(remaining) {
            Ok(len) => len,
            Err(StoreError::TruncatedRecord) => break,
            Err(error) => return Err(error.into()),
        };

        if remaining.len() < record_len {
            break;
        }

        let record = LogRecord::decode(&remaining[..record_len])?;

        if record.sequence <= max_sequence && max_sequence != 0 {
            return Err(EngineError::NonMonotonicSequence {
                previous: max_sequence,
                current: record.sequence,
            });
        }

        max_sequence = record.sequence;
        max_tx_id = max_tx_id.max(record.tx_id);

        match record.kind {
            RecordKind::Put => {
                pending
                    .entry(record.tx_id)
                    .or_default()
                    .push(decode_put(&record.payload)?);
            }
            RecordKind::Delete => {
                pending
                    .entry(record.tx_id)
                    .or_default()
                    .push(decode_delete(&record.payload)?);
            }
            RecordKind::Commit => {
                if let Some(operations) = pending.remove(&record.tx_id) {
                    apply_operations(&mut state, &operations);
                }
            }
        }

        offset += record_len;
    }

    Ok(Recovery {
        state,
        max_tx_id,
        max_sequence,
    })
}

fn apply_operations(state: &mut HashMap<Vec<u8>, Vec<u8>>, operations: &[Operation]) {
    for operation in operations {
        match operation {
            Operation::Put(key, value) => {
                state.insert(key.clone(), value.clone());
            }
            Operation::Delete(key) => {
                state.remove(key);
            }
        }
    }
}

fn encode_put(key: &[u8], value: &[u8]) -> Result<Vec<u8>, EngineError> {
    let key_len = u32::try_from(key.len()).map_err(|_| EngineError::KeyTooLarge)?;
    let mut payload = Vec::with_capacity(4 + key.len() + value.len());
    payload.extend_from_slice(&key_len.to_le_bytes());
    payload.extend_from_slice(key);
    payload.extend_from_slice(value);
    Ok(payload)
}

fn decode_put(payload: &[u8]) -> Result<Operation, EngineError> {
    if payload.len() < 4 {
        return Err(EngineError::InvalidOperationPayload);
    }
    let key_len = u32::from_le_bytes(payload[..4].try_into().expect("4-byte key length")) as usize;
    let key_end = 4usize
        .checked_add(key_len)
        .ok_or(EngineError::InvalidOperationPayload)?;
    if key_end > payload.len() {
        return Err(EngineError::InvalidOperationPayload);
    }
    Ok(Operation::Put(
        payload[4..key_end].to_vec(),
        payload[key_end..].to_vec(),
    ))
}

fn encode_delete(key: &[u8]) -> Result<Vec<u8>, EngineError> {
    let key_len = u32::try_from(key.len()).map_err(|_| EngineError::KeyTooLarge)?;
    let mut payload = Vec::with_capacity(4 + key.len());
    payload.extend_from_slice(&key_len.to_le_bytes());
    payload.extend_from_slice(key);
    Ok(payload)
}

fn decode_delete(payload: &[u8]) -> Result<Operation, EngineError> {
    if payload.len() < 4 {
        return Err(EngineError::InvalidOperationPayload);
    }
    let key_len = u32::from_le_bytes(payload[..4].try_into().expect("4-byte key length")) as usize;
    if payload.len() != 4 + key_len {
        return Err(EngineError::InvalidOperationPayload);
    }
    Ok(Operation::Delete(payload[4..].to_vec()))
}

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("log error: {0}")]
    Log(#[from] StoreError),
    #[error("key is too large")]
    KeyTooLarge,
    #[error("invalid operation payload")]
    InvalidOperationPayload,
    #[error("transaction is already finished")]
    TransactionFinished,
    #[error("non-monotonic log sequence: previous {previous}, current {current}")]
    NonMonotonicSequence { previous: u64, current: u64 },
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    fn temp_store_path(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("voodoo-store-{name}-{nonce}.vstore"))
    }

    #[test]
    fn committed_values_survive_reopen() {
        let path = temp_store_path("reopen");
        {
            let mut store = Store::open(&path).unwrap();
            let mut tx = store.begin();
            tx.put(b"user:1", b"Helder").unwrap();
            tx.put(b"user:2", b"Bruna").unwrap();
            tx.commit().unwrap();
        }
        {
            let store = Store::open(&path).unwrap();
            assert_eq!(store.get(b"user:1"), Some(b"Helder".as_slice()));
            assert_eq!(store.get(b"user:2"), Some(b"Bruna".as_slice()));
        }
        let _ = fs::remove_file(path);
    }

    #[test]
    fn uncommitted_values_do_not_appear_after_recovery() {
        let path = temp_store_path("uncommitted");
        {
            let mut store = Store::open(&path).unwrap();
            let mut tx = store.begin();
            tx.put(b"ghost", b"must-not-exist").unwrap();
            // Simulates a process dying before COMMIT by dropping the transaction/store.
        }
        {
            let store = Store::open(&path).unwrap();
            assert_eq!(store.get(b"ghost"), None);
        }
        let _ = fs::remove_file(path);
    }

    #[test]
    fn delete_is_transactional_and_durable() {
        let path = temp_store_path("delete");
        {
            let mut store = Store::open(&path).unwrap();
            let mut tx = store.begin();
            tx.put(b"key", b"value").unwrap();
            tx.commit().unwrap();

            let mut tx = store.begin();
            tx.delete(b"key").unwrap();
            tx.commit().unwrap();
            assert_eq!(store.get(b"key"), None);
        }
        let store = Store::open(&path).unwrap();
        assert_eq!(store.get(b"key"), None);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn rollback_never_changes_visible_state() {
        let path = temp_store_path("rollback");
        {
            let mut store = Store::open(&path).unwrap();
            let mut tx = store.begin();
            tx.put(b"a", b"1").unwrap();
            tx.rollback().unwrap();
            assert_eq!(store.get(b"a"), None);
        }
        let store = Store::open(&path).unwrap();
        assert_eq!(store.get(b"a"), None);
        let _ = fs::remove_file(path);
    }
}
