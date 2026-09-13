use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use voodoo_store_core::Store;

const CHILD_ENV: &str = "VOODOO_STORE_CRASH_CHILD";
const PATH_ENV: &str = "VOODOO_STORE_CRASH_PATH";
const MODE_ENV: &str = "VOODOO_STORE_CRASH_MODE";

fn temp_store_path(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("voodoo-store-process-crash-{name}-{nonce}.vstore"))
}

fn run_child(path: &PathBuf, mode: &str) -> std::process::ExitStatus {
    Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("crash_child")
        .arg("--nocapture")
        .env(CHILD_ENV, "1")
        .env(PATH_ENV, path)
        .env(MODE_ENV, mode)
        .status()
        .unwrap()
}

#[test]
fn crash_child() {
    if std::env::var_os(CHILD_ENV).is_none() {
        return;
    }

    let path = PathBuf::from(std::env::var_os(PATH_ENV).expect("crash path"));
    let mode = std::env::var(MODE_ENV).expect("crash mode");

    match mode.as_str() {
        "before-commit" => {
            let mut store = Store::open(&path).unwrap();
            let mut tx = store.begin().unwrap();
            tx.put(b"crash:before", b"must-not-appear").unwrap();
            std::process::exit(91);
        }
        "after-commit" => {
            let mut store = Store::open(&path).unwrap();
            store.put(b"crash:after", b"must-survive").unwrap();
            std::process::exit(92);
        }
        other => panic!("unknown crash mode: {other}"),
    }
}

#[test]
fn process_exit_before_commit_never_exposes_partial_transaction() {
    if std::env::var_os(CHILD_ENV).is_some() {
        return;
    }

    let path = temp_store_path("before-commit");
    {
        let mut store = Store::open(&path).unwrap();
        store.put(b"safe", b"committed").unwrap();
    }

    let status = run_child(&path, "before-commit");
    assert_eq!(status.code(), Some(91));

    let store = Store::open(&path).unwrap();
    assert_eq!(store.get(b"safe"), Some(b"committed".as_slice()));
    assert_eq!(store.get(b"crash:before"), None);
    drop(store);

    let report = Store::verify(&path).unwrap();
    assert_eq!(report.pending_transactions, 1);

    let _ = fs::remove_file(path);
}

#[test]
fn process_exit_after_commit_preserves_committed_transaction() {
    if std::env::var_os(CHILD_ENV).is_some() {
        return;
    }

    let path = temp_store_path("after-commit");
    {
        let mut store = Store::open(&path).unwrap();
        store.put(b"safe", b"committed").unwrap();
    }

    let status = run_child(&path, "after-commit");
    assert_eq!(status.code(), Some(92));

    let store = Store::open(&path).unwrap();
    assert_eq!(store.get(b"safe"), Some(b"committed".as_slice()));
    assert_eq!(store.get(b"crash:after"), Some(b"must-survive".as_slice()));
    drop(store);

    let _ = fs::remove_file(path);
}
