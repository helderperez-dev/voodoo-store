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
    assert submitted[0] == job_id
    assert submitted[1] == "ready"
    assert submitted[2] == b"email.send"
    assert submitted[3] == b'{"to":"ada@example.com"}'
    assert submitted[7] == 0
    assert submitted[8] == 3

    claimed = store.claim_job(1_000, 30_000)
    assert claimed is not None
    assert claimed[0] == job_id
    assert claimed[1] == "leased"
    assert claimed[7] == 1
    assert claimed[10] == 31_000
    assert claimed[11] == 1

    store.complete_job(job_id, claimed[11], 1_250)
    completed = store.get_job(job_id)
    assert completed is not None
    assert completed[1] == "completed"

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
    assert first[0] == job_id
    assert store.fail_job(job_id, first[11], 5_100, b"temporary") == "ready"

    assert store.claim_job(7_099, 10_000) is None
    second = store.claim_job(7_100, 10_000)
    assert second is not None
    assert second[0] == job_id
    assert second[7] == 2
    assert store.fail_job(job_id, second[11], 7_200, b"still failing") == "dead"

    dead = store.get_job(job_id)
    assert dead is not None
    assert dead[1] == "dead"
    assert [entry[2] for entry in store.job_history(job_id)] == [
        "submitted",
        "claimed",
        "retry_scheduled",
        "claimed",
        "dead",
    ]
    store.close()


def test_durable_job_idempotency_and_cancel(tmp_path):
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

    assert store.cancel_job(first, 20) is True
    assert store.cancel_job(first, 21) is False
    cancelled = store.get_job(first)
    assert cancelled is not None
    assert cancelled[1] == "cancelled"
    assert [entry[2] for entry in store.job_history(first)] == ["submitted", "cancelled"]
    store.close()
