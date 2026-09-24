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


def test_transaction_commits_kv_and_native_record_atomically(tmp_path):
    store = Store.open(tmp_path / "atomic.vstore")
    store.create_collection(b"lead", codec=b"json")

    tx = store.transaction()
    tx.put(b"data:lead:meta:next_id", b"2")
    tx.upsert_record(b"lead", b"1", b'{"name":"Ada"}')
    tx.commit()

    assert store.get(b"data:lead:meta:next_id") == b"2"
    record = store.get_record(b"lead", b"1")
    assert record is not None
    assert record[1] == b'{"name":"Ada"}'
    store.close()


def test_transaction_rollback_discards_native_record(tmp_path):
    store = Store.open(tmp_path / "rollback.vstore")
    store.create_collection(b"lead", codec=b"json")

    tx = store.transaction()
    tx.put(b"data:lead:meta:next_id", b"2")
    tx.upsert_record(b"lead", b"1", b'{"name":"Ada"}')
    tx.rollback()

    assert store.get(b"data:lead:meta:next_id") is None
    assert store.get_record(b"lead", b"1") is None
    store.close()


def test_native_collection_range_query(tmp_path):
    store = Store.open(tmp_path / "range-query.vstore")
    store.create_collection(b"product", codec=b"json")
    store.define_index(b"product", b"price")

    for primary_key, price in [
        (b"a", b"010"),
        (b"b", b"020"),
        (b"c", b"020"),
        (b"d", b"030"),
        (b"e", b"040"),
    ]:
        store.upsert_record(
            b"product",
            primary_key,
            primary_key,
            indexes=[(b"price", price)],
        )

    rows = store.query_index_range(
        b"product",
        b"price",
        start=b"020",
        end=b"040",
        end_inclusive=False,
    )
    assert [(index_value, record[0]) for index_value, record in rows] == [
        (b"020", b"b"),
        (b"020", b"c"),
        (b"030", b"d"),
    ]

    descending = store.query_index_range(
        b"product",
        b"price",
        start=b"010",
        end=b"040",
        descending=True,
        limit=2,
    )
    assert [(index_value, record[0]) for index_value, record in descending] == [
        (b"040", b"e"),
        (b"030", b"d"),
    ]
    store.close()
