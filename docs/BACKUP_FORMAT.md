# Backup and restore contract

The Store 0.3 Python binding exposes a verified single-node backup contract:

1. `Store.backup_to(path)` copies the active store and flushes it before
   returning the copied byte count.
2. `Store.verify(path)` validates checksums and returns the store identity and
   format metadata. Corrupt or incomplete files must raise `CorruptionError`.
3. `Store.restore_copy(source, destination)` verifies the source before making
   the destination visible and returns the restored store identity.

Backups are format-compatible artifacts, not an application migration system.
Production runbooks must record the framework/store compatibility line and test
restores into a disposable empty directory before upgrading a live instance.

## Operational manifest

The Python binding exposes `create_backup_manifest(store, destination)` and
`verify_backup_manifest(backup, manifest)`. The JSON sidecar is versioned and
records `manifest_version`, creation time, byte count, SHA-256, Store identity,
format major/minor, application version, and the same-major/verify-before-restore
policies. The sidecar is written only after native verification succeeds.

## At-rest confidentiality

Store checksums and verification establish integrity only. Production files and
backups must be placed on an encrypted volume with owner-only permissions;
Store 0.3 does not claim native record encryption or application-level field
encryption.
