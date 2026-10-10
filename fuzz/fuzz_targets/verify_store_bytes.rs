#![no_main]

use std::io::Write;

use libfuzzer_sys::fuzz_target;
use tempfile::NamedTempFile;
use voodoo_store_core::Store;

fuzz_target!(|bytes: &[u8]| {
    let Ok(mut file) = NamedTempFile::new() else {
        return;
    };
    if file.write_all(bytes).is_err() {
        return;
    }
    // Verification must reject malformed input or return a bounded report;
    // it must never panic, hang, or claim validity for arbitrary bytes.
    let _ = Store::verify(file.path());
});
