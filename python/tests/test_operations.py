from __future__ import annotations

from pathlib import Path

from voodoo_store import Store


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
        assert health["storage"]["live_keys"] == stats["live_keys"]
        namespaces = {item["name"]: item for item in health["namespaces"]}
        assert namespaces["streams"]["keys"] >= 1


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
