# Voodoo Store fuzzing

This directory contains the continuous decoder/recovery fuzz target. It feeds
arbitrary bytes into the read-only `Store::verify` path, which exercises header,
record, checksum, and recovery-boundary handling without opening a live writer.

Install `cargo-fuzz` and run locally:

```bash
cargo fuzz run verify_store_bytes --sanitizer none -- -max_total_time=60
```

The scheduled workflow runs a bounded smoke campaign and retains crashes as
artifacts. Every minimized crash should become a permanent regression fixture
in `crates/voodoo-store-core/tests/` before release.
