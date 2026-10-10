"""Deterministic disposable disaster-recovery game-day drill."""

from __future__ import annotations

import argparse
import hashlib
import json
import tempfile
from importlib import metadata
from pathlib import Path

from voodoo_store import (
    CorruptionError,
    Store,
    create_backup_manifest,
    verify_backup_manifest,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--fixture",
        type=Path,
        help="verified retained Store artifact to restore before the drill",
    )
    parser.add_argument(
        "--artifact-version",
        help="require the installed voodoo-store distribution to match this version",
    )
    parser.add_argument(
        "--validate-evidence",
        action="store_true",
        help="validate the emitted evidence against the DR contract before printing",
    )
    args = parser.parse_args()

    distribution = metadata.distribution("voodoo-store")
    installed_version = distribution.version
    record = distribution.read_text("RECORD")
    if record is None:
        raise SystemExit("installed voodoo-store artifact has no RECORD metadata")
    artifact_record_sha256 = hashlib.sha256(record.encode("utf-8")).hexdigest()
    if args.artifact_version is not None and installed_version != args.artifact_version:
        raise SystemExit(
            "installed voodoo-store artifact version does not match requested "
            f"{args.artifact_version}: {installed_version}"
        )

    with tempfile.TemporaryDirectory(prefix="voodoo-store-dr-") as root:
        root_path = Path(root)
        source = root_path / "application.vstore"
        backup = root_path / "backup.vstore"
        manifest = root_path / "backup.manifest.json"
        restored = root_path / "restored.vstore"
        compacted = root_path / "compacted.vstore"

        if args.fixture is not None:
            fixture = args.fixture.resolve()
            if not fixture.is_file():
                raise SystemExit(f"fixture does not exist: {fixture}")
            Store.restore_copy(fixture, source)

        with Store.open(source) as store:
            for index in range(25):
                store.put(f"order:{index}".encode(), b"paid")
            create_backup_manifest(
                store,
                backup,
                manifest=manifest,
                application_version="game-day",
            )
        manifest_payload = verify_backup_manifest(backup, manifest)

        # Corruption must fail closed before a destination is made visible.
        corrupt = root_path / "corrupt.vstore"
        corrupt.write_bytes(backup.read_bytes()[:-1] + b"!")
        try:
            Store.verify(corrupt)
        except CorruptionError:
            pass
        else:
            raise SystemExit("corruption was accepted by Store.verify")

        report = Store.restore_copy(backup, restored)
        if report["keys"] < 25:
            raise SystemExit("restore lost durable records")
        with Store.open(restored) as store:
            if store.get(b"order:24") != b"paid":
                raise SystemExit("restored record mismatch")
            compact_report = store.compact_copy_to(compacted)
            if compact_report["keys"] < 25:
                raise SystemExit("compaction lost durable records")

        # Reopen after the restore/compaction process boundary.
        with Store.open(compacted) as reopened:
            health = reopened.health_report()
            if reopened.get(b"order:0") != b"paid":
                raise SystemExit("reopened compacted store mismatch")

    evidence = {
                "status": "passed",
                "restored_keys": report["keys"],
                "compacted_keys": compact_report["keys"],
                "format": f"{health['format_major']}.{health['format_minor']}",
                "manifest_version": manifest_payload["manifest_version"],
                "manifest_store_id": manifest_payload["store_id"],
                "manifest_sha256": manifest_payload["sha256"],
                "fixture": str(args.fixture) if args.fixture is not None else None,
                "artifact_version": installed_version,
                "artifact_record_sha256": artifact_record_sha256,
            }
    if args.validate_evidence:
        from check_dr_evidence import validate

        validate(evidence)
    print(json.dumps(evidence, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
