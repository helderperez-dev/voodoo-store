from __future__ import annotations

import pytest

from voodoo_store import Store


def test_heterogeneous_operations_share_one_commit(tmp_path):
    path = tmp_path / "cross-domain.vstore"

    with Store.open(path) as store:
        with store.transaction() as tx:
            tx.put(b"order:42:status", b"paid")
            tx.enqueue_job(
                b"email.send_receipt",
                b"order:42",
                1_000,
                idempotency_key=b"receipt:42",
            )
            tx.append_stream(b"orders", b"order:42:paid")
            tx.publish_topic(b"notifications", b"order:42")
            tx.emit_event(b"order.paid", b"order:42", 1_000)

        assert store.get(b"order:42:status") == b"paid"

        jobs = store.list_jobs()
        assert len(jobs) == 1
        assert jobs[0]["handler"] == b"email.send_receipt"
        assert jobs[0]["payload"] == b"order:42"
        assert jobs[0]["idempotency_key"] == b"receipt:42"

        assert store.read_stream(b"orders", 0, 10) == [(0, b"order:42:paid")]
        assert store.read_topic(b"notifications", 0, 10) == [(0, b"order:42")]

        outbox = store.outbox_events_after(None, 10)
        assert len(outbox) == 1
        assert outbox[0]["topic"] == b"order.paid"
        assert outbox[0]["payload"] == b"order:42"

        changes = store.changes_after(None, 100)
        assert changes
        assert len({change["tx_id"] for change in changes}) == 1


def test_heterogeneous_transaction_rollback_hides_every_domain(tmp_path):
    path = tmp_path / "cross-domain-rollback.vstore"

    with Store.open(path) as store:
        with pytest.raises(RuntimeError, match="rollback"):
            with store.transaction() as tx:
                tx.put(b"order:42:status", b"paid")
                tx.enqueue_job(b"email.send_receipt", b"order:42", 1_000)
                tx.append_stream(b"orders", b"order:42:paid")
                tx.publish_topic(b"notifications", b"order:42")
                tx.emit_event(b"order.paid", b"order:42", 1_000)
                raise RuntimeError("rollback")

        assert store.get(b"order:42:status") is None
        assert store.list_jobs() == []
        assert store.read_stream(b"orders", 0, 10) == []
        assert store.read_topic(b"notifications", 0, 10) == []
        assert store.outbox_len() == 0
        assert store.changes_after(None, 100) == []


def test_cross_domain_transaction_survives_reopen(tmp_path):
    path = tmp_path / "cross-domain-reopen.vstore"

    with Store.open(path) as store:
        with store.transaction() as tx:
            tx.put(b"state", b"committed")
            tx.enqueue_job(b"worker", b"payload", 10)
            tx.append_stream(b"audit", b"committed")
            tx.emit_event(b"state.changed", b"committed", 10)

    with Store.open(path) as reopened:
        assert reopened.get(b"state") == b"committed"
        assert len(reopened.list_jobs()) == 1
        assert reopened.read_stream(b"audit", 0, 10) == [(0, b"committed")]
        assert reopened.outbox_len() == 1
