"""Operational backup manifest helpers for Voodoo Store."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ._native import Store

__all__ = ["create_backup_manifest", "verify_backup_manifest"]


def create_backup_manifest(
    store: Store,
    destination: str | Path,
    *,
    manifest: str | Path | None = None,
    application_version: str | None = None,
) -> dict[str, Any]:
    """Create and verify a backup plus its portable JSON manifest.

    The manifest is written only after the native verifier accepts the copied
    store. This keeps an operational manifest from claiming a backup that is
    incomplete or corrupt.
    """
    destination_path = Path(destination)
    manifest_path = (
        Path(manifest)
        if manifest is not None
        else Path(f"{destination}.manifest.json")
    )
    copied_bytes = store.backup_to(destination_path)
    verification = Store.verify(destination_path)
    health = store.health_report()
    digest = hashlib.sha256(destination_path.read_bytes()).hexdigest()
    payload: dict[str, Any] = {
        "manifest_version": 1,
        "created_at": datetime.now(UTC).isoformat(),
        "backup_path": destination_path.name,
        "bytes": copied_bytes,
        "sha256": digest,
        "store_id": verification.store_id.hex(),
        "format_major": health["format_major"],
        "format_minor": health["format_minor"],
        "application_version": application_version,
        "read_policy": "same-major",
        "restore_policy": "verify-before-restore",
    }
    manifest_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return payload


def verify_backup_manifest(
    backup: str | Path, manifest: str | Path
) -> dict[str, Any]:
    """Verify a manifest’s checksum and native Store metadata."""
    backup_path = Path(backup)
    payload = json.loads(Path(manifest).read_text())
    digest = hashlib.sha256(backup_path.read_bytes()).hexdigest()
    if digest != payload["sha256"]:
        raise ValueError("backup manifest checksum does not match the backup")
    verification = Store.verify(backup_path)
    if verification.store_id.hex() != payload["store_id"]:
        raise ValueError("backup manifest store identity does not match the backup")
    with Store.open(backup_path) as store:
        health = store.health_report()
    if (
        health["format_major"] != payload["format_major"]
        or health["format_minor"] != payload["format_minor"]
    ):
        raise ValueError("backup manifest format metadata does not match the backup")
    return payload
