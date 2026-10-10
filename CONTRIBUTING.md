# Contributing

Run `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
`cargo test --workspace --exclude voodoo-store-python`, and the Python binding
tests before submitting changes. Durable-format changes require a compatibility
note, a migration/fixture decision, and recovery regression coverage.
