# Voodoo Store version and binding policy

The current release line is **0.3.x** (`voodoo-store` Python binding and Rust
workspace). The binding supports Python **3.9–3.13** through the `abi3` PyO3
surface. The on-disk `.vstore` format is versioned independently from the
package and must be checked with `Store.verify()` before an upgrade or restore.

## Compatibility rules

- Patch releases preserve the documented 0.3 format and Python API contract.
- Minor releases may add format records but must retain forward recovery of
  committed data and publish compatibility fixtures before release.
- A format-major change requires an explicit migration tool and a new release
  profile; opening the store must fail closed when the major version is not
  supported.
- Native Rust APIs are documented in `docs/SPEC.md`; Python binding behavior is
  documented in `docs/BINDING_CONTRACT.md`.
- Pin the exact package and keep a verified backup before upgrading a pre-1.0
  deployment.
- The only supported previous release for the current 0.3.x line is **0.2.2**;
  it is retained as a checksum-verified fixture and exercised by the upgrade,
  backup, and fresh-path rollback test. Releases not listed in the compatibility
  manifest are outside the supported upgrade promise.

## Upgrade and rollback procedure

1. Run `Store.verify()` and verify the backup manifest before touching the
   active file.
2. Restore the backup or retained fixture to a new path and open that copy with
   the target binding.
3. Exercise representative reads/writes, then create and verify a new backup
   manifest before activation.
4. Keep the original artifact unchanged. Roll back by restoring it to another
   fresh path; never overwrite an active store in place.

The repository exercises this procedure against the retained `0.2.2` fixture
in `python/tests/test_operations.py`. Every additional supported previous
minor must add its own retained fixture and cycle before release.

Release workflows validate versions, generate artifact manifests, and attest
published wheels and source archives.
