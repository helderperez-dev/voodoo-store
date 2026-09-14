from __future__ import annotations

from voodoo_store import Store


def test_durable_job_submit_claim_complete_and_history(tmp_path):
    store = Store.open(tmp_path / "jobs.vstore")

    job_id = store.submit_job(
        b"email.send",
        b'{"to":"ada@example.com"}',
        1_000,
        max_attempts=3,
        idempotency_key=b"email:ada:1",
    )
    assert len(job_id) == 16

    submitted = store.get_job(job_id)
    assert submitted is not None
    assert submitted["id"] == job_id
    assert submitted["state"] == "ready"
    assert submitted["handler"] == b"email.send"
    assert submitted["payload"] == b'{"to":"ada@example.com"}'
    assert submitted["attempts"] == 0
    assert submitted["max_attempts"] == 3

    claimed = store.claim_job(1_000, 30_000)
    assert claimed is not None
    assert claimed["id"] == job_id
    assert claimed["state"] == "leased"
    assert claimed["attempts"] == 1
    assert claimed["lease_until_ms"] == 31_000
    assert claimed["lease_generation"] == 1

    store.complete_job(job_id, claimed["lease_generation"], 1_250)
    completed = store.get_job(job_id)
    assert completed is not None
    assert completed["state"] == "completed"

    history = store.job_history(job_id)
    assert [entry[2] for entry in history] == ["submitted", "claimed", "completed"]
    store.close()


def test_durable_job_failure_retries_with_backoff(tmp_path):
    store = Store.open(tmp_path / "retry.vstore")
    job_id = store.submit_job(
        b"report.build",
        b"{}",
        5_000,
        max_attempts=2,
        retry_backoff_ms=2_000,
    )

    first = store.claim_job(5_000, 10_000)
    assert first is not None
    assert first["id"] == job_id
    assert (
        store.fail_job(job_id, first["lease_generation"], 5_100, b"temporary")
        == "ready"
    )

    assert store.claim_job(7_099, 10_000) is None
    second = store.claim_job(7_100, 10_000)
    assert second is not None
    assert second["id"] == job_id
    assert second["attempts"] == 2
    assert (
        store.fail_job(job_id, second["lease_generation"], 7_200, b"still failing")
        == "dead"
    )

    dead = store.get_job(job_id)
    assert dead is not None
    assert dead["state"] == "dead"
    assert [entry[2] for entry in store.job_history(job_id)] == [
        "submitted",
        "claimed",
        "retry_scheduled",
        "claimed",
        "dead",
    ]
    store.close()


def test_durable_job_idempotency_applies_only_while_active(tmp_path):
    store = Store.open(tmp_path / "idempotency.vstore")

    first = store.submit_job(
        b"sync.contact",
        b'{"id":1}',
        10,
        idempotency_key=b"contact:1",
    )
    duplicate = store.submit_job(
        b"sync.contact",
        b'{"id":1}',
        11,
        idempotency_key=b"contact:1",
    )
    assert duplicate == first

    claimed = store.claim_job(11, 10)
    assert claimed is not None
    store.complete_job(first, claimed["lease_generation"], 12)

    next_job = store.submit_job(
        b"sync.contact",
        b'{"id":1}',
        13,
        idempotency_key=b"contact:1",
    )
    assert next_job != first
    store.close()


def test_durable_job_cancel(tmp_path):
    store = Store.open(tmp_path / "cancel.vstore")
    job_id = store.submit_job(b"sync.contact", b"{}", 10)

    assert store.cancel_job(job_id, 20) is True
    assert store.cancel_job(job_id, 21) is False
    cancelled = store.get_job(job_id)
    assert cancelled is not None
    assert cancelled["state"] == "cancelled"
    assert [entry[2] for entry in store.job_history(job_id)] == ["submitted", "cancelled"]
    store.close()


def test_claim_filters_handlers_without_leasing_other_job_types(tmp_path):
    store = Store.open(tmp_path / "handlers.vstore")
    email_id = store.submit_job(b"email.send", b"{}", 0)
    report_id = store.submit_job(b"report.build", b"{}", 0)

    claimed = store.claim_job(0, 1_000, handlers=[b"report.build"])
    assert claimed is not None
    assert claimed["id"] == report_id
    assert claimed["handler"] == b"report.build"
    assert store.get_job(email_id)["state"] == "ready"
    store.close()


def test_heartbeat_release_expiry_retry_list_and_stats(tmp_path):
    store = Store.open(tmp_path / "queue-contract.vstore")
    job_id = store.submit_job(b"work", b"{}", 0, max_attempts=2)

    first = store.claim_job(0, 10)
    assert first is not None
    generation = first["lease_generation"]
    assert store.heartbeat_job(job_id, generation, 5, 20) is True
    assert store.get_job(job_id)["lease_until_ms"] == 25
    assert store.heartbeat_job(job_id, generation + 1, 5, 20) is False

    assert store.release_job(job_id, generation, 6) is True
    released = store.get_job(job_id)
    assert released["state"] == "ready"
    assert released["attempts"] == 1

    second = store.claim_job(6, 10)
    assert second is not None
    assert second["attempts"] == 2
    assert store.release_expired_jobs(16) == 1
    dead = store.get_job(job_id)
    assert dead["state"] == "dead"

    retried = store.retry_job(job_id, 20)
    assert retried is not None
    assert retried["state"] == "ready"
    assert retried["attempts"] == 0

    jobs = store.list_jobs()
    assert len(jobs) == 1
    assert jobs[0]["id"] == job_id
    stats = store.job_stats()
    assert stats == {
        "total": 1,
        "ready": 1,
        "leased": 0,
        "completed": 0,
        "dead": 0,
        "cancelled": 0,
    }
    store.close()
