use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use voodoo_store_core::{EngineError, LogRecord, RecordKind, STORE_HEADER_LEN, Store, StoreError};

fn temp_store_path(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("voodoo-store-fault-{name}-{nonce}.vstore"))
}

fn encode_put_payload(key: &[u8], value: &[u8]) -> Vec<u8> {
    let key_len = u32::try_from(key.len()).unwrap();
    let mut payload = Vec::with_capacity(4 + key.len() + value.len());
    payload.extend_from_slice(&key_len.to_le_bytes());
    payload.extend_from_slice(key);
    payload.extend_from_slice(value);
    payload
}

#[test]
fn every_incomplete_final_record_prefix_is_repaired_without_losing_committed_state() {
    let baseline = temp_store_path("baseline");
    {
        let mut store = Store::open(&baseline).unwrap();
        store.put(b"safe", b"committed").unwrap();
    }

    let baseline_bytes = fs::read(&baseline).unwrap();
    let trailing_record = LogRecord::new(
        RecordKind::Put,
        2,
        3,
        encode_put_payload(b"ghost", b"uncommitted"),
    )
    .encode()
    .unwrap();

    for cut in 1..trailing_record.len() {
        let path = temp_store_path(&format!("torn-{cut}"));
        let mut bytes = baseline_bytes.clone();
        bytes.extend_from_slice(&trailing_record[..cut]);
        fs::write(&path, bytes).unwrap();

        let store = Store::open(&path).unwrap();
        assert_eq!(store.get(b"safe"), Some(b"committed".as_slice()));
        assert_eq!(store.get(b"ghost"), None);
        drop(store);

        assert_eq!(
            fs::metadata(&path).unwrap().len(),
            baseline_bytes.len() as u64
        );
        let _ = fs::remove_file(path);
    }

    let _ = fs::remove_file(baseline);
}

#[test]
fn complete_uncommitted_record_is_ignored_but_not_misreported_as_committed() {
    let path = temp_store_path("uncommitted-valid-record");
    {
        let mut store = Store::open(&path).unwrap();
        store.put(b"safe", b"committed").unwrap();
    }

    let record = LogRecord::new(
        RecordKind::Put,
        2,
        3,
        encode_put_payload(b"ghost", b"pending"),
    )
    .encode()
    .unwrap();
    let mut file = OpenOptions::new().append(true).open(&path).unwrap();
    file.write_all(&record).unwrap();
    file.sync_all().unwrap();
    drop(file);

    let report = Store::verify(&path).unwrap();
    assert_eq!(report.pending_transactions, 1);
    assert!(!report.has_torn_tail());

    let store = Store::open(&path).unwrap();
    assert_eq!(store.get(b"safe"), Some(b"committed".as_slice()));
    assert_eq!(store.get(b"ghost"), None);
    drop(store);

    let _ = fs::remove_file(path);
}

#[test]
fn corruption_inside_the_durable_prefix_fails_instead_of_being_truncated() {
    let path = temp_store_path("durable-prefix-corruption");
    {
        let mut store = Store::open(&path).unwrap();
        store.put(b"safe", b"committed").unwrap();
    }

    let original_len = fs::metadata(&path).unwrap().len();
    let mut bytes = fs::read(&path).unwrap();
    let payload_byte = STORE_HEADER_LEN + 25 + 4;
    bytes[payload_byte] ^= 0x40;
    fs::write(&path, bytes).unwrap();

    assert!(matches!(
        Store::open(&path),
        Err(EngineError::Log(StoreError::ChecksumMismatch { .. }))
    ));
    assert_eq!(fs::metadata(&path).unwrap().len(), original_len);

    let _ = fs::remove_file(path);
}
