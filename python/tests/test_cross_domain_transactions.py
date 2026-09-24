from __future__ import annotations

import subprocess
import sys
import textwrap

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


def test_cross_domain_results_cover_objects_rpc_and_workflow(tmp_path):
    path = tmp_path / "cross-domain-full-results.vstore"

    with Store.open(path) as store:
        tx = store.transaction()
        tx.put(b"order:9", b"awaiting-approval")
        object_op = tx.put_linked_object(
            b"invoice-bytes",
            b"invoices",
            b"order:9",
        )
        rpc_op = tx.request_rpc(
            b"payments.capture",
            b"order:9",
            1_000,
            2_000,
        )
        workflow_op = tx.create_workflow(
            b"order-approval",
            b"review",
            b"pending",
            1_000,
        )

        results = tx.commit_with_results()

        object_id = results[object_op]["id"]
        assert results[object_op]["kind"] == "object"
        assert len(object_id) == 32
        assert store.get_object(object_id) == b"invoice-bytes"
        assert store.resolve_object_ref(b"invoices", b"order:9") == object_id

        assert results[rpc_op]["kind"] == "rpc"
        rpc_id = (results[rpc_op]["tx_id"], results[rpc_op]["nonce"])
        pending = store.pending_rpc_requests_after(None, 10)
        assert len(pending) == 1
        assert pending[0]["id"] == rpc_id
        assert pending[0]["method"] == b"payments.capture"

        workflow_id = results[workflow_op]["id"]
        assert results[workflow_op]["kind"] == "workflow"
        assert len(workflow_id) == 16
        workflow = store.get_workflow(workflow_id)
        assert workflow is not None
        assert workflow["workflow_type"] == b"order-approval"
        assert workflow["status"] == "running"

        changes = store.changes_after(None, 1_000)
        assert changes
        assert len({change["tx_id"] for change in changes}) == 1


def test_existing_workflow_mutation_shares_application_transaction(tmp_path):
    path = tmp_path / "cross-domain-workflow-mutation.vstore"

    with Store.open(path) as store:
        workflow_id = store.create_workflow(
            b"approval",
            b"review",
            b"pending",
            10,
        )

        tx = store.transaction()
        tx.put(b"approval:state", b"waiting")
        tx.wait_for_signal(workflow_id, b"approved", 11)
        tx.commit()

        assert store.get(b"approval:state") == b"waiting"
        workflow = store.get_workflow(workflow_id)
        assert workflow is not None
        assert workflow["status"] == "waiting"
        assert workflow["wait"] == {"kind": "signal", "name": b"approved"}

        with pytest.raises(RuntimeError, match="rollback"):
            with store.transaction() as rollback:
                rollback.put(b"approval:state", b"should-not-stick")
                rollback.signal_workflow(
                    workflow_id,
                    b"approved",
                    b"yes",
                    12,
                )
                raise RuntimeError("rollback")

        assert store.get(b"approval:state") == b"waiting"
        workflow = store.get_workflow(workflow_id)
        assert workflow is not None
        assert workflow["status"] == "waiting"
        assert workflow["state"] == b"pending"


def test_abrupt_process_exit_does_not_leak_staged_cross_domain_state(tmp_path):
    path = tmp_path / "cross-domain-crash.vstore"
    script = textwrap.dedent(
        """
        import os
        import sys
        from pathlib import Path

        from voodoo_store import Store

        store = Store.open(Path(sys.argv[1]))
        tx = store.transaction()
        tx.put(b"crash:state", b"uncommitted")
        tx.enqueue_job(b"crash.job", b"payload", 100)
        tx.push_queue(b"crash.queue", b"payload")
        tx.append_stream(b"crash.stream", b"payload")
        tx.publish_topic(b"crash.topic", b"payload")
        tx.emit_event(b"crash.event", b"payload", 100)
        tx.put_linked_object(b"blob", b"crash.refs", b"one")
        tx.request_rpc(b"crash.rpc", b"payload", 100, 200)
        tx.create_workflow(b"crash.workflow", b"start", b"state", 100)
        os._exit(0)
        """
    )

    subprocess.run(
        [sys.executable, "-c", script, str(path)],
        check=True,
    )

    with Store.open(path) as store:
        assert store.get(b"crash:state") is None
        assert store.list_jobs() == []
        assert store.queue_stats(b"crash.queue")["total"] == 0
        assert store.read_stream(b"crash.stream", 0, 10) == []
        assert store.read_topic(b"crash.topic", 0, 10) == []
        assert store.outbox_len() == 0
        assert store.resolve_object_ref(b"crash.refs", b"one") is None
        assert store.pending_rpc_requests_after(None, 10) == []
        assert store.changes_after(None, 1_000) == []
