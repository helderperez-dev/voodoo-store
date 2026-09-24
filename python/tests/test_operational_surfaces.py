from __future__ import annotations

import pytest

from voodoo_store import Store, VoodooStoreError


def test_consumer_group_claim_nack_redelivery_and_stale_ack(tmp_path):
    path = tmp_path / "consumer-groups.vstore"

    with Store.open(path) as store:
        store.append_stream(b"work", b"first")
        store.append_stream(b"work", b"second")

        first = store.consumer_group_claim(
            b"work", b"workers", b"worker-a", 1_000, 100
        )
        assert first is not None
        assert first["offset"] == 0
        assert first["payload"] == b"first"

        assert (
            store.consumer_group_claim(
                b"work", b"workers", b"worker-b", 1_050, 100
            )
            is None
        )

        store.consumer_group_nack(
            b"work",
            b"workers",
            b"worker-a",
            first["offset"],
            first["lease_generation"],
        )

        redelivered = store.consumer_group_claim(
            b"work", b"workers", b"worker-b", 1_051, 100
        )
        assert redelivered is not None
        assert redelivered["offset"] == 0
        assert redelivered["payload"] == b"first"
        assert redelivered["lease_generation"] > first["lease_generation"]

        with pytest.raises(VoodooStoreError):
            store.consumer_group_ack(
                b"work",
                b"workers",
                b"worker-a",
                first["offset"],
                first["lease_generation"],
            )

        store.consumer_group_ack(
            b"work",
            b"workers",
            b"worker-b",
            redelivered["offset"],
            redelivered["lease_generation"],
        )
        state = store.consumer_group_state(b"work", b"workers")
        assert state["next_offset"] == 1
        assert state["leased_offset"] is None

        second = store.consumer_group_claim(
            b"work", b"workers", b"worker-a", 1_100, 100
        )
        assert second is not None
        assert second["offset"] == 1
        store.consumer_group_reset(b"work", b"workers", 0)
        assert store.consumer_group_state(b"work", b"workers")["next_offset"] == 0


def test_cdc_exposes_committed_changes_and_supports_pruning(tmp_path):
    path = tmp_path / "cdc.vstore"

    with Store.open(path) as store:
        store.put(b"alpha", b"one")
        store.put(b"beta", b"two")
        store.delete(b"alpha")

        changes = store.changes_after(None, 100)
        user_changes = [item for item in changes if not item["key"].startswith(b"\xffvds:")]
        assert [item["key"] for item in user_changes] == [b"alpha", b"beta", b"alpha"]
        assert [item["kind"] for item in user_changes] == ["put", "put", "delete"]
        assert user_changes[0]["value_len"] == 3
        assert len(user_changes[0]["value_sha256"]) == 32
        assert user_changes[-1]["value_len"] is None
        assert user_changes[-1]["value_sha256"] is None

        first_tx = user_changes[0]["tx_id"]
        first_tx_changes = store.changes_for_transaction(first_tx)
        assert any(item["key"] == b"alpha" for item in first_tx_changes)

        final_tx = max(item["tx_id"] for item in changes)
        removed = store.prune_changes_through(final_tx, 1_000)
        assert removed == len(changes)
        assert store.changes_after(None, 10) == []


def test_outbox_emit_list_ack_and_reopen(tmp_path):
    path = tmp_path / "outbox.vstore"

    with Store.open(path) as store:
        event_id = store.emit_outbox_event(b"orders.paid", b"42", 1_000)
        assert isinstance(event_id[0], int)
        assert len(event_id[1]) == 16
        assert store.outbox_len() == 1

        events = store.outbox_events_after(None, 10)
        assert len(events) == 1
        assert events[0]["id"] == event_id
        assert events[0]["topic"] == b"orders.paid"
        assert events[0]["payload"] == b"42"
        assert events[0]["created_at_ms"] == 1_000

    with Store.open(path) as reopened:
        assert reopened.outbox_len() == 1
        assert reopened.ack_outbox_event(*event_id) is True
        assert reopened.outbox_len() == 0
        assert reopened.ack_outbox_event(*event_id) is False


def test_rpc_request_response_ack_and_expiration(tmp_path):
    path = tmp_path / "rpc.vstore"

    with Store.open(path) as store:
        request_id = store.submit_rpc_request(
            b"payments.capture", b"order:42", 1_000, 2_000
        )
        pending = store.pending_rpc_requests_after(None, 10)
        assert len(pending) == 1
        assert pending[0]["id"] == request_id
        assert pending[0]["method"] == b"payments.capture"
        assert pending[0]["payload"] == b"order:42"

        store.respond_rpc(*request_id, b"captured", 1_100)
        assert store.pending_rpc_requests_after(None, 10) == []

        response = store.get_rpc_response(*request_id)
        assert response is not None
        assert response["request_id"] == request_id
        assert response["payload"] == b"captured"
        assert response["is_error"] is False

        assert store.ack_rpc_response(*request_id) is True
        assert store.get_rpc_response(*request_id) is None

        expiring_id = store.submit_rpc_request(
            b"inventory.reserve", b"sku:1", 2_000, 2_100
        )
        report = store.expire_rpc_requests(2_101, 10)
        assert report["expired"] == 1
        expired = store.get_rpc_response(*expiring_id)
        assert expired is not None
        assert expired["is_error"] is True
        assert expired["payload"] == b"deadline exceeded"
