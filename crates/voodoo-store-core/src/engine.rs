use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use thiserror::Error;

use crate::header::{HeaderError, STORE_HEADER_LEN, StoreHeader};
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
    header: StoreHeader,
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
            .write(true)
            .open(&path)?;

        let header = load_or_initialize_header(&mut file)?;
        let recovery = recover(&mut file)?;

        // A torn final record is a valid crash artifact. Repair the physical
        // tail before accepting new appends so corruption never accumulates.
        let physical_len = file.metadata()?.len();
        if recovery.valid_end < physical_len {
            file.set_len(recovery.valid_end)?;
            file.sync_data()?;
        }
        file.seek(SeekFrom::End(0))?;

        Ok(Self {
            path,
            file,
            header,
            state: recovery.state,
            next_tx_id: next_counter(recovery.max_tx_id)?,
            next_sequence: next_counter(recovery.max_sequence)?,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub const fn header(&self) -> &StoreHeader {
        &self.header
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

    pub fn begin(&mut self) -> Result<Transaction<'_>, EngineError> {
        let tx_id = self.next_tx_id;
        self.next_tx_id = tx_id.checked_add(1).ok_or(EngineError::CounterExhausted)?;

        Ok(Transaction {
            store: self,
            tx_id,
            operations: Vec::new(),
            finished: false,
        })
    }

    fn append(
        &mut self,
        kind: RecordKind,
        tx_id: u64,
        payload: Vec<u8>,
    ) -> Result<(), EngineError> {
        let sequence = self.next_sequence;
        self.next_sequence = sequence
            .checked_add(1)
            .ok_or(EngineError::CounterExhausted)?;
        let bytes = LogRecord::new(kind, tx_id, sequence, payload).encode()?;
        self.file.seek(SeekFrom::End(0))?;
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
    valid_end: u64,
}

fn load_or_initialize_header(file: &mut File) -> Result<StoreHeader, EngineError> {
    let len = file.metadata()?.len();
    if len == 0 {
        let header = StoreHeader::new(generate_store_id(), unix_time_ms()?);
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&header.encode())?;
        file.sync_all()?;
        return Ok(header);
    }

    if len < STORE_HEADER_LEN as u64 {
        return Err(HeaderError::Truncated.into());
    }

    let mut bytes = [0u8; STORE_HEADER_LEN];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut bytes)?;
    Ok(StoreHeader::decode(&bytes)?)
}

fn recover(file: &mut File) -> Result<Recovery, EngineError> {
    file.seek(SeekFrom::Start(STORE_HEADER_LEN as u64))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;

    let mut state = HashMap::new();
    let mut pending: HashMap<u64, Vec<Operation>> = HashMap::new();
    let mut committed = std::collections::HashSet::new();
    let mut offset = 0usize;
    let mut max_tx_id = 0u64;
    let mut max_sequence = 0u64;

    while offset < bytes.len() {
        let remaining = &bytes[offset..];
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
        if committed.contains(&record.tx_id) {
            return Err(EngineError::RecordAfterCommit(record.tx_id));
        }

        max_sequence = record.sequence;
        max_tx_id = max_tx_id.max(record.tx_id);

        match record.kind {
            RecordKind::Put => pending
                .entry(record.tx_id)
                .or_default()
                .push(decode_put(&record.payload)?),
            RecordKind::Delete => pending
                .entry(record.tx_id)
                .or_default()
                .push(decode_delete(&record.payload)?),
            RecordKind::Commit => {
                if let Some(operations) = pending.remove(&record.tx_id) {
                    apply_operations(&mut state, &operations);
                }
                committed.insert(record.tx_id);
            }
        }

        offset += record_len;
    }

    Ok(Recovery {
        state,
        max_tx_id,
        max_sequence,
        valid_end: STORE_HEADER_LEN as u64 + offset as u64,
    })
}

fn next_counter(max: u64) -> Result<u64, EngineError> {
    if max == 0 {
        Ok(1)
    } else {
        max.checked_add(1).ok_or(EngineError::CounterExhausted)
    }
}

fn unix_time_ms() -> Result<i64, EngineError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| EngineError::ClockBeforeUnixEpoch)?;
    i64::try_from(duration.as_millis()).map_err(|_| EngineError::TimestampOverflow)
}

fn generate_store_id() -> [u8; 16] {
    // This is a persistent uniqueness token, not a cryptographic secret. Avoiding
    // a runtime RNG dependency keeps the core small; replication can later define
    // stronger identity-generation requirements without changing the header width.
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_nanos())
        .unwrap_or_default();
    let pid = u128::from(std::process::id());
    (now ^ (pid << 64)).to_le_bytes()
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
    #[error("store header error: {0}")]
    Header(#[from] HeaderError),
    #[error("log error: {0}")]
    Log(#[from] StoreError),
    #[error("key is too large")]
    KeyTooLarge,
    #[error("invalid operation payload")]
    InvalidOperationPayload,
    #[error("transaction is already finished")]
    TransactionFinished,
    #[error("transaction or sequence counter exhausted")]
    CounterExhausted,
    #[error("system clock is before the Unix epoch")]
    ClockBeforeUnixEpoch,
    #[error("system timestamp does not fit the store format")]
    TimestampOverflow,
    #[error("non-monotonic log sequence: previous {previous}, current {current}")]
    NonMonotonicSequence { previous: u64, current: u64 },
    #[error("log contains a record after transaction {0} was committed")]
    RecordAfterCommit(u64),
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    fn temp_store_path(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("voodoo-store-{name}-{nonce}.vstore"))
    }

    #[test]
    fn new_store_has_persistent_header_identity() {
        let path = temp_store_path("header");
        let first_id = Store::open(&path).unwrap().header().store_id;
        let second_id = Store::open(&path).unwrap().header().store_id;
        assert_eq!(first_id, second_id);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn committed_values_survive_reopen() {
        let path = temp_store_path("reopen");
        {
            let mut store = Store::open(&path).unwrap();
            let mut tx = store.begin().unwrap();
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
            let mut tx = store.begin().unwrap();
            tx.put(b"ghost", b"must-not-exist").unwrap();
        }
        {
            let store = Store::open(&path).unwrap();
            assert_eq!(store.get(b"ghost"), None);
        }
        let _ = fs::remove_file(path);
    }

    #[test]
    fn torn_tail_is_removed_on_reopen() {
        let path = temp_store_path("torn-tail");
        {
            let mut store = Store::open(&path).unwrap();
            let mut tx = store.begin().unwrap();
            tx.put(b"safe", b"value").unwrap();
            tx.commit().unwrap();
        }
        let valid_len = fs::metadata(&path).unwrap().len();
        {
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            file.write_all(b"VDS1\x01\x02").unwrap();
        }
        assert!(fs::metadata(&path).unwrap().len() > valid_len);
        let store = Store::open(&path).unwrap();
        assert_eq!(store.get(b"safe"), Some(b"value".as_slice()));
        assert_eq!(fs::metadata(&path).unwrap().len(), valid_len);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn delete_is_transactional_and_durable() {
        let path = temp_store_path("delete");
        {
            let mut store = Store::open(&path).unwrap();
            let mut tx = store.begin().unwrap();
            tx.put(b"key", b"value").unwrap();
            tx.commit().unwrap();

            let mut tx = store.begin().unwrap();
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
            let mut tx = store.begin().unwrap();
            tx.put(b"a", b"1").unwrap();
            tx.rollback().unwrap();
            assert_eq!(store.get(b"a"), None);
        }
        let store = Store::open(&path).unwrap();
        assert_eq!(store.get(b"a"), None);
        let _ = fs::remove_file(path);
    }
}
