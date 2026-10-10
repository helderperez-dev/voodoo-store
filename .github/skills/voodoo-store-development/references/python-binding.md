# Python Binding

The Python binding is a first-class public surface, not an incidental FFI wrapper.

For every new capability:

1. define stable Rust semantics;
2. expose a public Rust operation;
3. design Python argument and return types intentionally;
4. use clear bytes/string/UUID ownership;
5. expose useful receipts/results for generated identifiers;
6. test Python behavior on supported platforms;
7. document failure/exception behavior.

Avoid forcing Python callers to know internal key encodings.

Framework should consume the binding rather than duplicating Store internals.
