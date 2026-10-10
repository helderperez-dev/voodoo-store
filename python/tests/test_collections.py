from __future__ import annotations

import json
from pathlib import Path

import pytest
from voodoo_store import Store, VoodooStoreError, migrate_collection


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


def test_native_collection_schema_migration_is_compare_and_swap(tmp_path):
    store = Store.open(tmp_path / "schema-migration.vstore")
    store.create_collection(b"lead", schema_version=1, codec=b"json")

    assert store.migrate_collection_schema(
        b"lead", 1, 2, codec=b"json-v2"
    ) is True
    assert store.collection_definition(b"lead") == (2, b"json-v2")
    assert store.collection_migrations(b"lead") == [(1, 2, b"json-v2")]
    # A stale expected version must not overwrite a newer schema.
    assert store.migrate_collection_schema(b"lead", 1, 3, codec=b"json-v3") is False
    assert store.collection_definition(b"lead") == (2, b"json-v2")
    store.close()


def test_schema_compatibility_fixture_survives_reopen_and_rejects_stale_step(
    tmp_path,
):
    """Exercise every declared schema step as a durable compatibility fixture."""
    contract = json.loads(
        (Path(__file__).parents[2] / "docs" / "SCHEMA_COMPATIBILITY.json").read_text()
    )
    assert contract["policy"] == "compare-and-swap"
    assert contract["rollback_policy"] == "forward-only-restore-or-reverse-migration"
    path = tmp_path / "schema-compatibility.vstore"
    for collection in contract["collections"]:
        name = collection["name"].encode()
        versions = collection["versions"]
        first = versions[0]
        with Store.open(path) as store:
            assert store.create_collection(
                name,
                schema_version=first["from"],
                codec=collection["codec"].encode(),
            ) is True
            current = first["from"]
            for step in versions:
                assert step["from"] == current
                assert store.migrate_collection_schema(
                    name,
                    step["from"],
                    step["to"],
                    codec=step["codec"].encode(),
                ) is True
                assert store.migrate_collection_schema(
                    name,
                    step["from"],
                    step["to"] + 1,
                    codec=b"stale",
                ) is False
                current = step["to"]
        with Store.open(path) as reopened:
            assert reopened.collection_definition(name) == (
                versions[-1]["to"],
                versions[-1]["codec"].encode(),
            )


def test_declared_schema_steps_transform_values_and_survive_reopen(tmp_path):
    """Exercise every declared transition through the resumable value runner."""
    contract = json.loads(
        (Path(__file__).parents[2] / "docs" / "SCHEMA_COMPATIBILITY.json").read_text()
    )
    for collection in contract["collections"]:
        name = collection["name"].encode()
        versions = collection["versions"]
        path = tmp_path / f"{collection['name']}-values.vstore"
        with Store.open(path) as store:
            first = versions[0]
            assert store.create_collection(
                name,
                schema_version=first["from"],
                codec=collection["codec"].encode(),
            ) is True
            for key in (b"1", b"2"):
                store.upsert_record(
                    name,
                    key,
                    json.dumps(
                        {"name": key.decode(), "schema_version": first["from"]},
                        sort_keys=True,
                    ).encode(),
                )

            current = first["from"]
            for step in versions:
                assert step["from"] == current

                def transform(key, value, indexes, target=step["to"]):
                    payload = json.loads(value)
                    payload["schema_version"] = target
                    return json.dumps(payload, sort_keys=True).encode(), indexes

                report = migrate_collection(
                    store,
                    name,
                    expected_version=step["from"],
                    next_version=step["to"],
                    codec=step["codec"].encode(),
                    transform=transform,
                    batch_size=1,
                )
                assert report.records_seen == 2
                assert report.records_migrated == 2
                assert report.batches_committed == 2
                for key in (b"1", b"2"):
                    record = store.get_record(name, key)
                    assert record is not None
                    assert json.loads(record[1])["schema_version"] == step["to"]
                current = step["to"]

        with Store.open(path) as reopened:
            assert reopened.collection_definition(name) == (
                versions[-1]["to"],
                versions[-1]["codec"].encode(),
            )
            for key in (b"1", b"2"):
                record = reopened.get_record(name, key)
                assert record is not None
                assert json.loads(record[1])["schema_version"] == versions[-1]["to"]


def test_collection_migration_runner_transforms_bounded_batches(tmp_path):
    path = tmp_path / "migration-runner.vstore"
    with Store.open(path) as store:
        store.create_collection(b"lead", schema_version=1, codec=b"json")
        store.upsert_record(b"lead", b"1", b"ada")
        store.upsert_record(b"lead", b"2", b"grace")

        report = migrate_collection(
            store,
            b"lead",
            expected_version=1,
            next_version=2,
            codec=b"json-v2",
            batch_size=1,
            transform=lambda key, value, indexes: value.upper(),
        )

        assert report.records_seen == 2
        assert report.records_migrated == 2
        assert report.batches_committed == 2
        assert store.collection_definition(b"lead") == (2, b"json-v2")
        assert store.collection_migrations(b"lead") == [(1, 2, b"json-v2")]
        assert store.get_record(b"lead", b"1")[1] == b"ADA"


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


def test_collection_migration_failure_keeps_schema_and_history_at_prior_version(
    tmp_path,
):
    path = tmp_path / "migration-failure.vstore"
    with Store.open(path) as store:
        store.create_collection(b"lead", schema_version=1, codec=b"json")
        store.upsert_record(b"lead", b"1", b"ada")
        store.upsert_record(b"lead", b"2", b"grace")

        calls = 0

        def fail_after_first(key, value, indexes):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise RuntimeError("migration transform failed")
            return value.upper()

        with pytest.raises(RuntimeError, match="migration transform failed"):
            migrate_collection(
                store,
                b"lead",
                expected_version=1,
                next_version=2,
                codec=b"json-v2",
                batch_size=1,
                transform=fail_after_first,
            )

        assert store.collection_definition(b"lead") == (1, b"json")
        assert store.collection_migrations(b"lead") == []
        # A committed batch may remain physically present; the documented
        # recovery path is a verified restore or reverse migration, not an
        # implicit in-place rollback.
        assert store.get_record(b"lead", b"1")[1] == b"ADA"
        assert store.get_record(b"lead", b"2")[1] == b"grace"


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
