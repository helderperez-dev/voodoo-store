from __future__ import annotations

import pytest

from voodoo_store import Store, iter_object_chunks


def test_native_objects_are_content_addressed_and_durable(tmp_path):
    path = tmp_path / "objects.vstore"

    with Store.open(path) as store:
        object_id = store.put_object(b"invoice")
        assert len(object_id) == 32
        assert store.put_object(b"invoice") == object_id
        assert store.get_object(object_id) == b"invoice"
        assert store.verify_object(object_id) is True
        assert store.object_info(object_id) == {"id": object_id, "size": 7}

        store.link_object(b"invoices", b"order-42", object_id)
        assert store.resolve_object_ref(b"invoices", b"order-42") == object_id

    with Store.open(path) as reopened:
        assert reopened.get_object(object_id) == b"invoice"
        assert reopened.resolve_object_ref(b"invoices", b"order-42") == object_id


def test_object_chunks_are_bounded_ordered_and_fail_closed_for_missing(tmp_path):
    with Store.open(tmp_path / "object-chunks.vstore") as store:
        object_id = store.put_object(b"0123456789")
        assert list(iter_object_chunks(store, object_id, chunk_size=3)) == [
            b"012",
            b"345",
            b"678",
            b"9",
        ]
        with pytest.raises(KeyError, match="object does not exist"):
            list(iter_object_chunks(store, b"x" * 32, chunk_size=3))
        with pytest.raises(ValueError, match="positive integer"):
            list(iter_object_chunks(store, object_id, chunk_size=0))


def test_object_chunk_iterator_rejects_provider_bound_violation():
    class MisbehavingStore:
        def get_object_chunk(self, object_id, offset, limit):
            return b"x" * (limit + 1)

    with pytest.raises(ValueError, match="larger than the requested limit"):
        list(iter_object_chunks(MisbehavingStore(), b"object", chunk_size=4))


def test_object_chunk_iterator_uses_metadata_size_as_total_bound():
    class RepeatingStore:
        def object_info(self, object_id):
            return {"id": object_id, "size": 5}

        def get_object_chunk(self, object_id, offset, limit):
            return b"x" * limit

    assert list(iter_object_chunks(RepeatingStore(), b"object", chunk_size=2)) == [
        b"xx",
        b"xx",
        b"x",
    ]


def test_object_chunk_iterator_rejects_metadata_truncation():
    class TruncatedStore:
        def object_info(self, object_id):
            return {"id": object_id, "size": 5}

        def get_object_chunk(self, object_id, offset, limit):
            return b"abc" if offset == 0 else b""

    with pytest.raises(ValueError, match="before its metadata size"):
        list(iter_object_chunks(TruncatedStore(), b"object", chunk_size=3))


def test_object_chunk_iterator_rejects_malformed_metadata_size():
    class MalformedStore:
        def object_info(self, object_id):
            return {"id": object_id, "size": "5"}

        def get_object_chunk(self, object_id, offset, limit):
            return b"x"

    with pytest.raises(ValueError, match="invalid object size metadata"):
        list(iter_object_chunks(MalformedStore(), b"object", chunk_size=3))


def test_native_object_gc_only_removes_orphans(tmp_path):
    with Store.open(tmp_path / "object-gc.vstore") as store:
        keep = store.put_object(b"keep")
        remove = store.put_object(b"remove")
        store.link_object(b"docs", b"primary", keep)

        report = store.gc_orphan_objects()
        assert report == {"scanned": 2, "referenced": 1, "removed": 1}
        assert store.get_object(keep) == b"keep"
        assert store.get_object(remove) is None


def test_native_object_unlink_makes_object_collectable(tmp_path):
    with Store.open(tmp_path / "object-unlink.vstore") as store:
        object_id = store.put_object(b"payload")
        store.link_object(b"files", b"one", object_id)

        assert store.unlink_object(b"files", b"one") is True
        assert store.unlink_object(b"files", b"one") is False
        assert store.resolve_object_ref(b"files", b"one") is None

        report = store.gc_orphan_objects()
        assert report["removed"] == 1
        assert store.get_object(object_id) is None


def test_native_object_refs_can_be_listed_by_namespace_and_prefix(tmp_path):
    with Store.open(tmp_path / "object-list.vstore") as store:
        a = store.put_object(b"a")
        b = store.put_object(b"b")
        c = store.put_object(b"c")
        store.link_object(b"files", b"reports/2024.pdf", a)
        store.link_object(b"files", b"reports/2025.pdf", b)
        store.link_object(b"files", b"images/logo.png", c)
        store.link_object(b"avatars", b"user-1", a)

        assert store.list_object_refs(b"files") == [
            (b"images/logo.png", c),
            (b"reports/2024.pdf", a),
            (b"reports/2025.pdf", b),
        ]
        assert store.list_object_refs(b"files", b"reports/") == [
            (b"reports/2024.pdf", a),
            (b"reports/2025.pdf", b),
        ]
        assert store.list_object_refs(b"avatars") == [(b"user-1", a)]


def test_native_object_metadata_is_durable_and_does_not_change_content_identity(tmp_path):
    path = tmp_path / "object-metadata.vstore"
    with Store.open(path) as store:
        object_id = store.put_object_with_metadata(
            b"invoice", {"content_type": "application/pdf", "owner": "billing"}
        )
        assert store.put_object(b"invoice") == object_id
        assert store.object_info(object_id) == {
            "id": object_id,
            "size": 7,
            "metadata": {"content_type": "application/pdf", "owner": "billing"},
        }

    with Store.open(path) as reopened:
        assert reopened.object_info(object_id)["metadata"] == {
            "content_type": "application/pdf",
            "owner": "billing",
        }


def test_native_object_retention_purges_only_expired_unreferenced_objects(tmp_path):
    with Store.open(tmp_path / "object-retention.vstore") as store:
        expired = store.put_object_with_metadata(
            b"expired", {"expires_at_unix": "100"}
        )
        referenced = store.put_object_with_metadata(
            b"referenced", {"expires_at_unix": "100"}
        )
        store.link_object(b"docs", b"keep", referenced)
        live = store.put_object_with_metadata(b"live", {"expires_at_unix": "200"})

        assert store.purge_expired_objects(100, limit=100) == {
            "scanned": 3,
            "expired": 2,
            "referenced": 1,
            "removed": 1,
        }
        assert store.get_object(expired) is None
        assert store.get_object(referenced) == b"referenced"
        assert store.get_object(live) == b"live"
