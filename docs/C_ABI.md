# Voodoo Store C ABI

The `voodoo-store-ffi` crate is the language-neutral portability layer. The
Python extension remains the release-critical binding; the C ABI is maintained
as a stable foundation for additional bindings.

## Contract

- `vds_abi_version()` returns the integer ABI version (`2` currently).
- `vds_open` transfers ownership of a handle to the caller; release it exactly
  once with `vds_close`.
- Handles are not internally synchronized. A caller must serialize concurrent
  access to one handle or use separate handles.
- `vds_get` uses a two-call size-query contract. It never writes beyond the
  caller-provided capacity and distinguishes missing keys from empty values.
- Functions return `VDS_OK` or a documented negative/positive status code.
  `vds_last_error_message` copies a thread-local diagnostic into caller-owned
  memory; diagnostics are not required to remain valid after the next call.
- Rust panics are contained at the ABI boundary and return `VDS_PANIC`.

The checked-in `examples/c_consumer.c` is compiled and executed by
`scripts/c_abi_smoke.py`, Store CI, and the root release contract. This keeps
the header and exported symbols exercised by a real external-language caller.
