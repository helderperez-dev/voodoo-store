//! Cross-language restore coverage for the retained 0.2.2 Store artifact.

use std::fs;
use std::path::PathBuf;

use voodoo_store_core::Store;

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/legacy-0.2.2.vstore")
}

#[test]
fn native_engine_verifies_retained_legacy_fixture() {
    let fixture = fixture_path();
    let report = Store::verify(&fixture).expect("legacy fixture must verify");
    assert_eq!(report.header.format_major, 1);
    assert_eq!(report.header.format_minor, 0);
    assert!(report.keys > 0);

    let bytes = fs::read(&fixture).expect("legacy fixture must be readable");
    assert!(!bytes.is_empty());
}
