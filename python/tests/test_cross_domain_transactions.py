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
            tx.push_queue(b"receipts", b"order:42", priority=5)
            tx.append_stream(b"orders", b"order:42:paid")
            tx.publish_topic(b"notifications", b"order:42")
            tx.emit_event(b"order.paid", b"order:42", 1_000)

        assert store.get(b"order:42:status") == b"paid"

        jobs = store.list_jobs()
        assert len(jobs) == 1
        assert jobs[0]["handler"] == b"email.send_receipt"
        assert jobs[0]["payload"] == b"order:42"
        assert jobs[0]["idempotency_key"] == b"receipt:42"

        changes = store.changes_after(None, 100)
        assert changes
        assert len({change["tx_id"] for change in changes}) == 1

        queued = store.claim_queue(b"receipts", 1_000, 100)
        assert queued is not None
        assert queued["payload"] == b"order:42"
        assert queued["priority"] == 5

        assert store.read_stream(b"orders", 0, 10) == [(0, b"order:42:paid")]
        assert store.read_topic(b"notifications", 0, 10) == [(0, b"order:42")]

        outbox = store.outbox_events_after(None, 10)
        assert len(outbox) == 1
        assert outbox[0]["topic"] == b"order.paid"
        assert outbox[0]["payload"] == b"order:42"


def test_heterogeneous_transaction_rollback_hides_every_domain(tmp_path):
    path = tmp_path / "cross-domain-rollback.vstore"

    with Store.open(path) as store:
        with pytest.raises(RuntimeError, match="rollback"):
            with store.transaction() as tx:
                tx.put(b"order:42:status", b"paid")
                tx.enqueue_job(b"email.send_receipt", b"order:42", 1_000)
                tx.push_queue(b"receipts", b"order:42")
                tx.append_stream(b"orders", b"order:42:paid")
                tx.publish_topic(b"notifications", b"order:42")
                tx.emit_event(b"order.paid", b"order:42", 1_000)
                raise RuntimeError("rollback")

        assert store.get(b"order:42:status") is None
        assert store.list_jobs() == []
        assert store.queue_stats(b"receipts")["total"] == 0
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


def test_cross_domain_commit_returns_generated_receipts(tmp_path):
    path = tmp_path / "cross-domain-results.vstore"

    with Store.open(path) as store:
        tx = store.transaction()
        tx.put(b"order:7", b"paid")
        job_op = tx.enqueue_job(b"receipt", b"order:7", 100)
        queue_op = tx.push_queue(b"receipts", b"order:7", priority=4)
        stream_op = tx.append_stream(b"orders", b"order:7:paid")
        topic_op = tx.publish_topic(b"notifications", b"order:7")
        outbox_op = tx.emit_event(b"order.paid", b"order:7", 100)

        results = tx.commit_with_results()

        assert results[job_op]["kind"] == "job"
        assert len(results[job_op]["id"]) == 16

        assert results[queue_op] == {
            "kind": "queue",
            "id": 1,
        }
        assert results[stream_op] == {
            "kind": "stream",
            "offset": 0,
        }
        assert results[topic_op] == {
            "kind": "topic",
            "offset": 0,
        }
        assert results[outbox_op]["kind"] == "outbox"
        assert isinstance(results[outbox_op]["tx_id"], int)
        assert len(results[outbox_op]["nonce"]) == 16

        jobs = store.list_jobs()
        assert len(jobs) == 1
        assert jobs[0]["id"] == results[job_op]["id"]

        queued = store.claim_queue(b"receipts", 100, 100)
        assert queued is not None
        assert queued["id"] == results[queue_op]["id"]

        outbox = store.outbox_events_after(None, 10)
        assert len(outbox) == 1
        assert outbox[0]["tx_id"] == results[outbox_op]["tx_id"]
        assert outbox[0]["nonce"] == results[outbox_op]["nonce"]


def test_cross_domain_generated_results_can_be_chained(tmp_path):
    path = tmp_path / "cross-domain-chaining.vstore"

    with Store.open(path) as store:
        tx = store.transaction()
        tx.put(b"order:99", b"accepted")

        object_op = tx.put_object(b"invoice-99")
        tx.link_object(b"invoices", b"99", object_op)

        rpc_op = tx.request_rpc(
            b"payments.capture",
            b"order:99",
            1_000,
            2_000,
        )

        workflow_op = tx.create_workflow(
            b"order",
            b"accepted",
            b"99",
            1_000,
        )
        tx.wait_for_workflow_signal(workflow_op, b"payment", 1_001)

        results = tx.commit_with_results()

        object_id = results[object_op]["id"]
        assert results[object_op]["kind"] == "object"
        assert len(object_id) == 32
        assert store.resolve_object_ref(b"invoices", b"99") == object_id
        assert store.get_object(object_id) == b"invoice-99"

        assert results[rpc_op]["kind"] == "rpc"
        pending = store.pending_rpc_requests_after(None, 10)
        assert len(pending) == 1
        assert pending[0]["tx_id"] == results[rpc_op]["tx_id"]
        assert pending[0]["nonce"] == results[rpc_op]["nonce"]

        workflow_id = results[workflow_op]["id"]
        assert results[workflow_op]["kind"] == "workflow"
        workflow = store.get_workflow(workflow_id)
        assert workflow is not None
        assert workflow["status"] == "waiting"
        assert workflow["wait"] == {"kind": "signal", "name": b"payment"}

        changes = store.changes_after(None, 500)
        assert changes
        assert len({change["tx_id"] for change in changes}) == 1


def test_chained_cross_domain_rollback_hides_generated_domains(tmp_path):
    path = tmp_path / "cross-domain-chaining-rollback.vstore"

    with Store.open(path) as store:
        with pytest.raises(RuntimeError, match="rollback"):
            with store.transaction() as tx:
                tx.put(b"order:99", b"accepted")
                object_op = tx.put_object(b"invoice-99")
                tx.link_object(b"invoices", b"99", object_op)
                tx.request_rpc(b"payments.capture", b"order:99", 1_000)
                workflow_op = tx.create_workflow(
                    b"order",
                    b"accepted",
                    b"99",
                    1_000,
                )
                tx.wait_for_workflow_signal(workflow_op, b"payment", 1_001)
                raise RuntimeError("rollback")

        assert store.get(b"order:99") is None
        assert store.list_object_refs(b"invoices") == []
        assert store.pending_rpc_requests_after(None, 10) == []
        assert store.changes_after(None, 100) == []
