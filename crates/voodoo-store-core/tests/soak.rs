//! Deterministic long-running durability smoke for the local Store writer.

use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

use voodoo_store_core::Store;

fn temp_path() -> std::path::PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!("voodoo-store-soak-{nonce}.vstore"))
}

#[test]
fn repeated_commit_reopen_and_verify_preserves_state() {
    let path = temp_path();
    const RECORDS: usize = 5_000;
    const REOPENS: usize = 5;

    {
        let mut store = Store::open(&path).expect("open soak store");
        for index in 0..RECORDS {
            let key = format!("soak:{index:05}");
            let value = format!("value-{index}");
            store.put(key.as_bytes(), value.as_bytes()).expect("commit");
        }
        assert_eq!(store.storage_stats().expect("stats").user_keys, RECORDS);
    }

    for reopen in 0..REOPENS {
        let store = Store::open(&path).expect("reopen soak store");
        let key = format!("soak:{:05}", reopen * 997);
        let expected = format!("value-{}", reopen * 997);
        assert_eq!(store.get(key.as_bytes()), Some(expected.as_bytes()));
        assert_eq!(store.storage_stats().expect("stats").user_keys, RECORDS);
        drop(store);
        let report = Store::verify(&path).expect("verify soak store");
        assert_eq!(report.pending_transactions, 0);
        assert_eq!(report.valid_bytes, report.file_bytes);
    }

    fs::remove_file(path).expect("remove soak store");
}
