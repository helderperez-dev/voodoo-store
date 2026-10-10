from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
from voodoo_store import (
    CorruptionError,
    IoError,
    Store,
    VoodooStoreError,
    create_backup_manifest,
    verify_backup_manifest,
)


def test_format_compatibility_contract_matches_current_store(tmp_path: Path) -> None:
    contract = json.loads(
        (Path(__file__).parents[2] / "docs" / "FORMAT_COMPATIBILITY.json").read_text()
    )
    path = tmp_path / "compatibility.vstore"
    with Store.open(path) as store:
        store.put(b"compatibility", b"verified")
        health = store.health_report()
    assert health["format_major"] == contract["format_major"]
    assert health["format_minor"] == contract["current_format_minor"]
    assert contract["read_policy"] == "same-major"
    assert contract["backup_policy"] == "verify-before-restore"
    historical = {item["release"]: item for item in contract["historical_releases"]}
    assert historical["0.2.2"]["format_major"] == contract["format_major"]
    assert historical["0.2.2"]["format_minor"] == contract["current_format_minor"]


def test_storage_stats_and_health_report(tmp_path: Path) -> None:
    path = tmp_path / "operations.vstore"

    with Store.open(path) as store:
        store.put(b"user:1", b"Ada")
        store.append_stream(b"events", b"created")

        stats = store.storage_stats()
        assert stats["file_bytes"] > 0
        assert stats["live_keys"] >= 2
        assert stats["user_keys"] >= 1
        assert stats["internal_keys"] >= 1
        assert stats["live_bytes"] == stats["key_bytes"] + stats["value_bytes"]
        assert stats["amplification_ratio"] > 0

        health = store.health_report()
        assert len(health["store_id"]) == 16
        assert health["format_major"] >= 0
        assert health["namespace_max_bytes"] is None
        assert health["metrics"]["commits"] >= 1
        assert health["metrics"]["bytes_appended"] > 0
        assert health["metrics"]["commit_latency_max_us"] >= 0
        assert health["metrics"]["fsync_latency_max_us"] >= 0
        assert health["queue_total"] == 0
        assert health["queue_ready"] == 0
        assert health["queue_leased"] == 0
        assert health["queue_dead"] == 0
        assert health["queue_oldest_ready_at_ms"] is None
        assert health["storage"]["live_keys"] == stats["live_keys"]
        namespaces = {item["name"]: item for item in health["namespaces"]}
        assert namespaces["streams"]["keys"] >= 1


def test_native_storage_quota_is_reported_and_enforced(tmp_path: Path) -> None:
    path = tmp_path / "quota.vstore"
    with Store.open(path) as store:
        initial_bytes = store.health_report()["storage"]["file_bytes"]

    with Store.open(path, max_bytes=initial_bytes) as store:
        health = store.health_report()
        assert health["max_bytes"] == initial_bytes
        assert health["within_limit"] is True
        with pytest.raises(VoodooStoreError, match="quota"):
            store.put(b"over-quota", b"value")


def test_backup_restore_and_checkpoint_preserve_store_identity(tmp_path: Path) -> None:
    source = tmp_path / "source.vstore"
    backup = tmp_path / "backup.vstore"
    restored = tmp_path / "restored.vstore"
    checkpoint = tmp_path / "checkpoint.vstore"

    with Store.open(source) as store:
        store.put(b"order:42", b"paid")
        source_id = store.health_report()["store_id"]

        copied_bytes = store.backup_to(backup)
        assert copied_bytes > 0

        checkpoint_report = store.checkpoint_to(checkpoint)
        assert checkpoint_report["source_store_id"] == source_id
        assert checkpoint_report["checkpoint_store_id"] == source_id

    assert Store.verify(backup).store_id == source_id

    restore_report = Store.restore_copy(backup, restored)
    assert restore_report["store_id"] == source_id
    assert Store.verify(restored).store_id == source_id

    with Store.open(restored) as store:
        assert store.get(b"order:42") == b"paid"

    with Store.open(checkpoint) as store:
        assert store.get(b"order:42") == b"paid"


def test_backup_manifest_is_verified_and_portable(tmp_path: Path) -> None:
    source = tmp_path / "source.vstore"
    backup = tmp_path / "backup.vstore"
    manifest = tmp_path / "backup.manifest.json"
    with Store.open(source) as store:
        store.put(b"critical", b"value")
        payload = create_backup_manifest(
            store,
            backup,
            manifest=manifest,
            application_version="3.2.1",
        )

    assert payload["manifest_version"] == 1
    assert payload["application_version"] == "3.2.1"
    assert verify_backup_manifest(backup, manifest)["sha256"] == payload["sha256"]

    manifest_payload = json.loads(manifest.read_text())
    manifest_payload["format_major"] += 1
    manifest.write_text(json.dumps(manifest_payload))
    with pytest.raises(ValueError, match="format metadata"):
        verify_backup_manifest(backup, manifest)

    backup.write_bytes(backup.read_bytes() + b"tampered")
    with pytest.raises(ValueError, match="checksum"):
        verify_backup_manifest(backup, manifest)


def test_repeated_backup_restore_soak_preserves_latest_state(tmp_path: Path) -> None:
    """Exercise the operational backup path across repeated reopen boundaries."""
    source = tmp_path / "soak-source.vstore"
    with Store.open(source) as store:
        for iteration in range(20):
            store.put(b"counter", str(iteration).encode())
            backup = tmp_path / f"backup-{iteration}.vstore"
            restored = tmp_path / f"restored-{iteration}.vstore"
            manifest = tmp_path / f"backup-{iteration}.manifest.json"
            create_backup_manifest(store, backup, manifest=manifest)
            verify_backup_manifest(backup, manifest)
            report = Store.restore_copy(backup, restored)
            assert report["store_id"] == store.health_report()["store_id"]
            with Store.open(restored) as reopened:
                assert reopened.get(b"counter") == str(iteration).encode()


def test_restore_retained_legacy_0_2_2_fixture(tmp_path: Path) -> None:
    """The current binding restores a real file emitted by the 0.2.2 tag."""
    fixture = Path(__file__).parents[2] / "tests" / "fixtures" / "legacy-0.2.2.vstore"
    manifest = json.loads(
        (fixture.parent / "legacy-0.2.2.manifest.json").read_text()
    )
    assert manifest["release"] == "0.2.2"
    assert manifest["format_major"] == 1
    assert hashlib.sha256(fixture.read_bytes()).hexdigest() == manifest["sha256"]
    destination = tmp_path / "restored-legacy.vstore"
    report = Store.restore_copy(fixture, destination)
    assert report["keys"] >= 1
    with Store.open(destination) as store:
        assert store.get(b"legacy-key") == b"legacy-value"


def test_upgrade_and_rollback_cycle_from_retained_0_2_2_fixture(tmp_path: Path) -> None:
    """Exercise the documented non-destructive upgrade/rollback procedure."""
    fixture = Path(__file__).parents[2] / "tests" / "fixtures" / "legacy-0.2.2.vstore"
    upgraded = tmp_path / "upgraded.vstore"
    upgrade_backup = tmp_path / "upgrade-backup.vstore"
    upgrade_manifest = tmp_path / "upgrade-backup.manifest.json"
    rollback = tmp_path / "rollback.vstore"

    # The upgrade path starts from a verified copy and writes only through the
    # current binding. The original fixture remains untouched for rollback.
    Store.restore_copy(fixture, upgraded)
    with Store.open(upgraded) as store:
        assert store.get(b"legacy-key") == b"legacy-value"
        store.put(b"current-key", b"current-value")
        create_backup_manifest(store, upgrade_backup, manifest=upgrade_manifest)
        verify_backup_manifest(upgrade_backup, upgrade_manifest)

    with Store.open(upgraded) as store:
        assert store.get(b"current-key") == b"current-value"

    # Rollback restores the retained pre-upgrade artifact into a fresh path;
    # no in-place overwrite is attempted.
    Store.restore_copy(fixture, rollback)
    with Store.open(rollback) as store:
        assert store.get(b"legacy-key") == b"legacy-value"
        assert store.get(b"current-key") is None



def test_corrupt_backup_is_rejected_before_restore(tmp_path: Path) -> None:
    source = tmp_path / "source.vstore"
    backup = tmp_path / "backup.vstore"
    with Store.open(source) as store:
        store.put(b"critical", b"value")
        store.backup_to(backup)

    payload = bytearray(backup.read_bytes())
    payload[-1] ^= 0xFF
    backup.write_bytes(payload)

    with pytest.raises(CorruptionError):
        Store.verify(backup)


def test_incomplete_backup_is_rejected_before_restore(tmp_path: Path) -> None:
    source = tmp_path / "source.vstore"
    backup = tmp_path / "backup.vstore"
    with Store.open(source) as store:
        store.put(b"critical", b"value")
        store.backup_to(backup)

    backup.write_bytes(backup.read_bytes()[:16])
    with pytest.raises((CorruptionError, IoError)):
        Store.verify(backup)

def test_compact_snapshot_and_generation_are_verified(tmp_path: Path) -> None:
    source = tmp_path / "source.vstore"
    compacted = tmp_path / "compacted.vstore"
    snapshot = tmp_path / "snapshot.vstore"
    generation = tmp_path / "generation.vstore"

    with Store.open(source) as store:
        for index in range(10):
            store.put(b"counter", str(index).encode())
        store.put(b"stable", b"value")
        source_id = store.health_report()["store_id"]

        compact_report = store.compact_copy_to(compacted)
        assert compact_report["source_bytes"] >= compact_report["compacted_bytes"]
        assert compact_report["keys"] >= 2

        snapshot_report = store.snapshot_to(snapshot)
        assert snapshot_report["source_store_id"] == source_id
        assert snapshot_report["snapshot_store_id"] != source_id
        assert snapshot_report["keys"] >= 2

        generation_report = store.compact_generation_to(generation)
        assert generation_report["store_id"] == source_id
        assert generation_report["source_bytes"] >= generation_report["generation_bytes"]
        assert generation_report["high_water_tx_id"] > 0
        assert generation_report["high_water_sequence"] > 0

    with Store.open(compacted) as store:
        assert store.get(b"counter") == b"9"
        assert store.get(b"stable") == b"value"

    with Store.open(snapshot) as store:
        assert store.get(b"counter") == b"9"

    with Store.open(generation) as store:
        assert store.health_report()["store_id"] == source_id
        assert store.get(b"counter") == b"9"
