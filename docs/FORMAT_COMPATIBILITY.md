# Store file-format compatibility

The current file format is **major 1, minor 0**. The header is checksummed and
the engine rejects an unknown major before replaying any records.

- Readers accept files only from the same major format line.
- Minor revisions are additive and must remain readable by the current major
  reader; writers emit the current minor.
- A backup is restored only after `Store.verify()` succeeds.
- A major-format change requires an explicit migration tool and a retained
  fixture from the previous major. It is never an implicit in-place upgrade.

The machine-readable contract lives in
[`FORMAT_COMPATIBILITY.json`](FORMAT_COMPATIBILITY.json). Release automation
must publish that contract with the binding so operators can select a compatible
restore binary before touching a production file.

The retained 0.2.2 release used format `1.0`, so 0.3.x can read its durable
files under the same-major policy. The compatibility contract test keeps this
historical release metadata executable; future major changes require an
explicit migration rather than an implicit restore.

Collection schema evolution is tracked separately in
[`SCHEMA_COMPATIBILITY.json`](SCHEMA_COMPATIBILITY.json). Its
compare-and-swap steps are exercised by the Python binding tests across
close/reopen boundaries; a stale migration cannot overwrite a newer schema
version.
