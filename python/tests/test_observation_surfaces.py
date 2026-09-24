from __future__ import annotations

import pytest

from voodoo_store import Store, VoodooStoreError


def test_consumer_group_claim_ack_and_reopen(tmp_path):
    path = tmp_path / "consumer-groups.vstore"

    with Store.open(path) as store:
        store.append_stream(b"events", b"one")
        store.append_stream(b"events", b"two")

        delivery = store.consumer_group_claim(
            b"events", b"workers", b"consumer-a", 0, 100
        )
        assert delivery is not None
        assert delivery["offset"] == 0
        assert delivery["payload"] == b"one"
        assert delivery["lease_generation"] == 1
        assert delivery["lease_until_ms"] == 100

        store.consumer_group_ack(
            b"events",
            b"workers",
            b"consumer-a",
            delivery["offset"],
            delivery["lease_generation"],
        )
        state = store.consumer_group_state(b"events", b"workers")
        assert state["next_offset"] == 1
        assert state["leased_offset"] is None
        assert state["owner"] == b""

    with Store.open(path) as reopened:
        delivery = reopened.consumer_group_claim(
            b"events", b"workers", b"consumer-b", 1, 100
        )
        assert delivery is not None
        assert delivery["offset"] == 1
        assert delivery["payload"] == b"two"


def test_consumer_group_nack_redelivers_and_stale_ack_fails(tmp_path):
    with Store.open(tmp_path / "consumer-redelivery.vstore") as store:
        store.append_stream(b"events", b"one")

        first = store.consumer_group_claim(b"events", b"workers", b"a", 0, 10)
        assert first is not None
        store.consumer_group_nack(
            b"events",
            b"workers",
            b"a",
            first["offset"],
            first["lease_generation"],
        )

        second = store.consumer_group_claim(b"events", b"workers", b"b", 1, 10)
        assert second is not None
        assert second["offset"] == first["offset"]
        assert second["lease_generation"] > first["lease_generation"]

        with pytest.raises(VoodooStoreError):
            store.consumer_group_ack(
                b"events",
                b"workers",
                b"a",
                first["offset"],
                first["lease_generation"],
            )


def test_change_feed_exposes_committed_mutations_and_can_prune(tmp_path):
    with Store.open(tmp_path / "cdc.vstore") as store:
        store.put(b"user:1", b"Ada")
        store.put(b"user:2", b"Grace")
        store.delete(b"user:2")

        changes = store.changes_after()
        assert changes
        assert {change["kind"] for change in changes} >= {"put", "delete"}
        assert any(change["key"] == b"user:1" for change in changes)

        first_tx = changes[0]["tx_id"]
        transaction_changes = store.changes_for_transaction(first_tx)
        assert transaction_changes
        assert all(change["tx_id"] == first_tx for change in transaction_changes)
        assert all(
            change["value_sha256"] is None or len(change["value_sha256"]) == 32
            for change in transaction_changes
        )

        before = len(store.changes_after())
        removed = store.prune_changes_through(first_tx)
        assert removed >= 1
        assert len(store.changes_after()) == before - removed


def test_storage_stats_and_health_report_inventory_native_domains(tmp_path):
    with Store.open(tmp_path / "health.vstore") as store:
        store.put(b"user", b"value")
        store.append_stream(b"events", b"payload")
        object_id = store.put_object(b"object")
        store.link_object(b"files", b"one", object_id)

        stats = store.storage_stats()
        assert stats["file_bytes"] > 0
        assert stats["live_keys"] == stats["user_keys"] + stats["internal_keys"]
        assert stats["live_bytes"] == stats["key_bytes"] + stats["value_bytes"]
        assert stats["amplification_ratio"] > 0

        health = store.health_report()
        assert isinstance(health["store_id"], bytes)
        assert len(health["store_id"]) == 16
        assert health["format_major"] >= 1
        namespaces = {item["name"]: item for item in health["namespaces"]}
        assert namespaces["streams"]["keys"] > 0
        assert namespaces["objects"]["keys"] > 0
        assert namespaces["cdc"]["keys"] > 0
