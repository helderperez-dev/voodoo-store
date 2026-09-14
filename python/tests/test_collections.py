from __future__ import annotations

import pytest

from voodoo_store import Store, VoodooStoreError


def test_native_collection_crud_and_scan(tmp_path):
    store = Store.open(tmp_path / "collections.vstore")

    assert store.create_collection(b"lead", schema_version=1, codec=b"json") is True
    assert store.create_collection(b"lead", schema_version=1, codec=b"json") is False
    assert store.collection_definition(b"lead") == (1, b"json")

    store.upsert_record(b"lead", b"1", b'{"name":"Ada"}')
    store.upsert_record(b"lead", b"2", b'{"name":"Grace"}')

    first = store.get_record(b"lead", b"1")
    assert first is not None
    assert first[0] == b"1"
    assert first[1] == b'{"name":"Ada"}'
    assert first[2] == []

    rows = store.scan_collection(b"lead")
    assert [row[0] for row in rows] == [b"1", b"2"]

    assert store.delete_record(b"lead", b"1") is True
    assert store.delete_record(b"lead", b"1") is False
    assert store.get_record(b"lead", b"1") is None
    store.close()


def test_native_collection_secondary_index(tmp_path):
    store = Store.open(tmp_path / "index.vstore")
    store.create_collection(b"lead", codec=b"json")
    assert store.define_index(b"lead", b"email") is True

    store.upsert_record(
        b"lead",
        b"1",
        b'{"email":"ada@example.com"}',
        indexes=[(b"email", b"ada@example.com")],
    )
    store.upsert_record(
        b"lead",
        b"2",
        b'{"email":"ada@example.com"}',
        indexes=[(b"email", b"ada@example.com")],
    )

    rows = store.query_index_exact(b"lead", b"email", b"ada@example.com")
    assert [row[0] for row in rows] == [b"1", b"2"]
    store.close()


def test_native_collection_unique_index(tmp_path):
    store = Store.open(tmp_path / "unique.vstore")
    store.create_collection(b"user", codec=b"json")
    store.define_index(b"user", b"email", unique=True)

    store.upsert_record(
        b"user",
        b"1",
        b'{"email":"ada@example.com"}',
        indexes=[(b"email", b"ada@example.com")],
    )

    with pytest.raises(VoodooStoreError):
        store.upsert_record(
            b"user",
            b"2",
            b'{"email":"ada@example.com"}',
            indexes=[(b"email", b"ada@example.com")],
        )

    assert store.get_record(b"user", b"2") is None
    store.close()
