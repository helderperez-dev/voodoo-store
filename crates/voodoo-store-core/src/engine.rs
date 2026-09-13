use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use fs2::FileExt;
use thiserror::Error;

use crate::header::{HeaderError, STORE_HEADER_LEN, StoreHeader};
use crate::log::{HEADER_LEN, LogRecord, RecordKind, StoreError, encoded_record_len_from_prefix};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    Strict,
    Data,
    Relaxed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreOptions {
    pub durability: Durability,
    pub repair_torn_tail: bool,
}

impl Default for StoreOptions {
    fn default() -> Self {
        Self {
            durability: Durability::Data,
            repair_torn_tail: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerificationReport {
    pub header: StoreHeader,
    pub file_bytes: u64,
    pub valid_bytes: u64,
    pub records: u64,
    pub committed_transactions: u64,
    pub pending_transactions: u64,
    pub keys: usize,
}

impl VerificationReport {
    pub const fn has_torn_tail(&self) -> bool {
        self.valid_bytes < self.file_bytes
    }
}

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
    options: StoreOptions,
    state: HashMap<Vec<u8>, Vec<u8>>,
    next_tx_id: u64,
    next_sequence: u64,
}

impl Store {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, EngineError> {
        Self::open_with_options(path, StoreOptions::default())
    }

    pub fn open_with_options(
        path: impl AsRef<Path>,
        options: StoreOptions,
    ) -> Result<Self, EngineError> {
        let path = path.as_ref().to_path_buf();
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)?;

        FileExt::try_lock_exclusive(&file).map_err(map_lock_error)?;
        let header = load_or_initialize_header(&mut file)?;
        let recovery = recover(&mut file)?;

        let physical_len = file.metadata()?.len();
        if recovery.valid_end < physical_len && options.repair_torn_tail {
            file.set_len(recovery.valid_end)?;
            sync_file(&file, options.durability)?;
        }
        file.seek(SeekFrom::End(0))?;

        Ok(Self {
            path,
            file,
            header,
            options,
            state: recovery.state,
            next_tx_id: next_counter(recovery.max_tx_id)?,
            next_sequence: next_counter(recovery.max_sequence)?,
        })
    }

    pub fn verify(path: impl AsRef<Path>) -> Result<VerificationReport, EngineError> {
        let mut file = OpenOptions::new().read(true).open(path)?;
        FileExt::try_lock_shared(&file).map_err(map_lock_error)?;

        let mut header_bytes = [0u8; STORE_HEADER_LEN];
        file.read_exact(&mut header_bytes)?;
        let header = StoreHeader::decode(&header_bytes)?;
        let file_bytes = file.metadata()?.len();
        let recovery = recover(&mut file)?;

        Ok(VerificationReport {
            header,
            file_bytes,
            valid_bytes: recovery.valid_end,
            records: recovery.records,
            committed_transactions: recovery.committed_transactions,
            pending_transactions: recovery.pending_transactions,
            keys: recovery.state.len(),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub const fn header(&self) -> &StoreHeader {
        &self.header
    }

    pub const fn options(&self) -> StoreOptions {
        self.options
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

    pub fn scan_prefix(&self, prefix: impl AsRef<[u8]>) -> Vec<(Vec<u8>, Vec<u8>)> {
        let prefix = prefix.as_ref();
        let mut entries: Vec<_> = self
            .state
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        entries
    }

    pub fn put(
        &mut self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<(), EngineError> {
        let mut tx = self.begin()?;
        tx.put(key, value)?;
        tx.commit()
    }

    pub fn delete(&mut self, key: impl AsRef<[u8]>) -> Result<(), EngineError> {
        let mut tx = self.begin()?;
        tx.delete(key)?;
        tx.commit()
    }

    pub fn flush(&self) -> Result<(), EngineError> {
        sync_file(&self.file, self.options.durability)
    }

    pub fn backup_to(&self, destination: impl AsRef<Path>) -> Result<u64, EngineError> {
        self.file.sync_all()?;
        let destination = destination.as_ref();
        if destination == self.path {
            return Err(EngineError::BackupDestinationIsSource);
        }
        Ok(fs::copy(&self.path, destination)?)
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

impl Drop for Store {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
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
        sync_file(&self.store.file, self.store.options.durability)?;
        apply_operations(&mut self.store.state, &self.operations);
        self.finished = true;
        Ok(())
    }

    pub fn rollback(mut self) -> Result<(), EngineError> {
        self.ensure_open()?;
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
    records: u64,
    committed_transactions: u64,
    pending_transactions: u64,
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
    let mut committed = HashSet::new();
    let mut offset = 0usize;
    let mut max_tx_id = 0u64;
    let mut max_sequence = 0u64;
    let mut records = 0u64;

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
        records = records
            .checked_add(1)
            .ok_or(EngineError::CounterExhausted)?;
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
        records,
        committed_transactions: committed.len() as u64,
        pending_transactions: pending.len() as u64,
    })
}

fn sync_file(file: &File, durability: Durability) -> Result<(), EngineError> {
    match durability {
        Durability::Strict => file.sync_all()?,
        Durability::Data => file.sync_data()?,
        Durability::Relaxed => {}
    }
    Ok(())
}

fn map_lock_error(error: std::io::Error) -> EngineError {
    if error.kind() == std::io::ErrorKind::WouldBlock {
        EngineError::AlreadyOpen
    } else {
        EngineError::Io(error)
    }
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
    #[error("store is already open by another writer")]
    AlreadyOpen,
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
    #[error("backup destination must differ from source store")]
    BackupDestinationIsSource,
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
    fn second_writer_is_rejected() {
        let path = temp_store_path("lock");
        let first = Store::open(&path).unwrap();
        assert!(matches!(Store::open(&path), Err(EngineError::AlreadyOpen)));
        drop(first);
        assert!(Store::open(&path).is_ok());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn autocommit_and_prefix_scan_work() {
        let path = temp_store_path("autocommit");
        let mut store = Store::open(&path).unwrap();
        store.put(b"user:2", b"B").unwrap();
        store.put(b"user:1", b"A").unwrap();
        store.put(b"other", b"X").unwrap();
        let entries = store.scan_prefix(b"user:");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0], (b"user:1".to_vec(), b"A".to_vec()));
        assert_eq!(entries[1], (b"user:2".to_vec(), b"B".to_vec()));
        drop(store);
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
            store.put(b"safe", b"value").unwrap();
        }
        let valid_len = fs::metadata(&path).unwrap().len();
        {
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            file.write_all(b"VDS1\x01\x02").unwrap();
        }
        assert!(fs::metadata(&path).unwrap().len() > valid_len);
        {
            let store = Store::open(&path).unwrap();
            assert_eq!(store.get(b"safe"), Some(b"value".as_slice()));
            assert_eq!(fs::metadata(&path).unwrap().len(), valid_len);
        }
        let _ = fs::remove_file(path);
    }

    #[test]
    fn verify_reports_valid_store() {
        let path = temp_store_path("verify");
        {
            let mut store = Store::open(&path).unwrap();
            store.put(b"a", b"1").unwrap();
            store.put(b"b", b"2").unwrap();
        }
        let report = Store::verify(&path).unwrap();
        assert_eq!(report.keys, 2);
        assert_eq!(report.committed_transactions, 2);
        assert!(!report.has_torn_tail());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn backup_is_reopenable() {
        let path = temp_store_path("backup-source");
        let backup = temp_store_path("backup-target");
        {
            let mut store = Store::open(&path).unwrap();
            store.put(b"durable", b"yes").unwrap();
            store.backup_to(&backup).unwrap();
        }
        let copy = Store::open(&backup).unwrap();
        assert_eq!(copy.get(b"durable"), Some(b"yes".as_slice()));
        drop(copy);
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(backup);
    }

    #[test]
    fn delete_is_transactional_and_durable() {
        let path = temp_store_path("delete");
        {
            let mut store = Store::open(&path).unwrap();
            store.put(b"key", b"value").unwrap();
            store.delete(b"key").unwrap();
            assert_eq!(store.get(b"key"), None);
        }
        let store = Store::open(&path).unwrap();
        assert_eq!(store.get(b"key"), None);
        drop(store);
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
        drop(store);
        let _ = fs::remove_file(path);
    }
}
