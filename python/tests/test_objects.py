from __future__ import annotations

from voodoo_store import Store


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
