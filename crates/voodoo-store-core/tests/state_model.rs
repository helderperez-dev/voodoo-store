use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use voodoo_store_core::Store;

fn temp_store_path(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("voodoo-store-model-{name}-{nonce}.vstore"))
}

#[derive(Clone, Copy)]
struct Prng(u64);

impl Prng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn usize(&mut self, upper: usize) -> usize {
        (self.next() as usize) % upper
    }
}

#[test]
fn deterministic_transaction_model_survives_repeated_reopen() {
    let path = temp_store_path("transactions");
    let mut expected = BTreeMap::<Vec<u8>, Vec<u8>>::new();
    let mut rng = Prng(0x5eed_cafe_f00d_beef);

    let mut store = Store::open(&path).unwrap();
    let mut last_committed_tx = 0u64;

    for round in 0..500usize {
        let should_commit = rng.usize(5) != 0;
        let operation_count = 1 + rng.usize(8);
        let mut staged = expected.clone();
        let tx_id;
        {
            let mut tx = store.begin().unwrap();
            tx_id = tx.id();
            for operation in 0..operation_count {
                let key = format!("key:{:02}", rng.usize(32)).into_bytes();
                if rng.usize(4) == 0 {
                    tx.delete(&key).unwrap();
                    staged.remove(&key);
                } else {
                    let value = format!("r{round}:o{operation}:{}", rng.next()).into_bytes();
                    tx.put(&key, &value).unwrap();
                    staged.insert(key, value);
                }
            }
            if should_commit {
                tx.commit().unwrap();
            } else {
                tx.rollback().unwrap();
            }
        }

        if should_commit {
            expected = staged;
            assert!(tx_id > last_committed_tx);
            last_committed_tx = tx_id;
            let changes = store.changes_for_transaction(tx_id).unwrap();
            assert_eq!(changes.len(), operation_count);
            for (sequence, change) in changes.iter().enumerate() {
                assert_eq!(change.tx_id, tx_id);
                assert_eq!(change.sequence as usize, sequence);
            }
        } else {
            assert!(store.changes_for_transaction(tx_id).unwrap().is_empty());
        }

        if round % 37 == 0 {
            drop(store);
            store = Store::open(&path).unwrap();
            assert_user_state(&store, &expected);
            let report = Store::verify(&path).unwrap_err();
            assert!(report.to_string().contains("already open"));
        }
    }

    assert_user_state(&store, &expected);
    drop(store);

    let reopened = Store::open(&path).unwrap();
    assert_user_state(&reopened, &expected);
    let all_changes = reopened.changes_after(None, usize::MAX).unwrap();
    assert!(all_changes.windows(2).all(|window| {
        window[0].tx_id < window[1].tx_id
            || (window[0].tx_id == window[1].tx_id && window[0].sequence < window[1].sequence)
    }));
    drop(reopened);
    let _ = fs::remove_file(path);
}

fn assert_user_state(store: &Store, expected: &BTreeMap<Vec<u8>, Vec<u8>>) {
    let actual: BTreeMap<_, _> = store.scan_prefix(b"key:").into_iter().collect();
    assert_eq!(&actual, expected);
}
