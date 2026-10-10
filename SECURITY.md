# Security

Report suspected vulnerabilities privately to the repository maintainers before
opening a public issue. Include the affected version, reproduction steps, and
impact; do not include secrets or production data.

The Store security boundary covers checksummed durable files, single-writer
locking, backup verification, corruption recovery, dependency audits, and
release provenance. Store files should be protected by filesystem permissions
and backups should be verified before restore.
