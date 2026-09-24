from __future__ import annotations

from voodoo_store import Store


def test_queue_priority_ack_and_stats(tmp_path):
    path = tmp_path / "queue.vstore"

    with Store.open(path) as store:
        low = store.push_queue(b"mail", b"low", priority=1)
        high = store.push_queue(b"mail", b"high", priority=10)
        assert high > low
        assert store.queue_stats(b"mail") == {
            "ready": 2,
            "leased": 0,
            "dead": 0,
            "total": 2,
        }

        claimed = store.claim_queue(b"mail", 1_000, 100)
        assert claimed is not None
        assert claimed["id"] == high
        assert claimed["payload"] == b"high"
        assert claimed["priority"] == 10
        assert claimed["lease_generation"] == 1

        store.ack_queue(
            b"mail",
            claimed["id"],
            claimed["lease_generation"],
        )
        assert store.queue_stats(b"mail")["total"] == 1


def test_queue_nack_dead_letter_and_purge(tmp_path):
    path = tmp_path / "queue-retry.vstore"

    with Store.open(path) as store:
        store.push_queue(b"work", b"retry-me")
        claimed = store.claim_queue(b"work", 10, 100)
        assert claimed is not None

        store.nack_queue(
            b"work",
            claimed["id"],
            claimed["lease_generation"],
            50,
        )
        assert store.claim_queue(b"work", 49, 100) is None

        retried = store.claim_queue(b"work", 50, 100)
        assert retried is not None
        assert retried["attempts"] == 2

        store.dead_letter_queue(
            b"work",
            retried["id"],
            retried["lease_generation"],
        )
        stats = store.queue_stats(b"work")
        assert stats["dead"] == 1
        assert stats["ready"] == 0
        assert store.purge_dead_queue(b"work") == 1
        assert store.queue_stats(b"work")["total"] == 0


def test_queue_survives_reopen(tmp_path):
    path = tmp_path / "queue-reopen.vstore"

    with Store.open(path) as store:
        message_id = store.push_queue(
            b"delayed",
            b"payload",
            available_at_ms=100,
            priority=3,
        )

    with Store.open(path) as reopened:
        assert reopened.claim_queue(b"delayed", 99, 100) is None
        claimed = reopened.claim_queue(b"delayed", 100, 100)
        assert claimed is not None
        assert claimed["id"] == message_id
        assert claimed["payload"] == b"payload"
