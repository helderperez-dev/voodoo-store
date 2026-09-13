from pathlib import Path

import pytest

from voodoo_store import (
    ReservedKeyError,
    Store,
    StoreBusyError,
    StoreClosedError,
    TransactionFinishedError,
)


def test_binary_round_trip_and_prefix_scan(tmp_path: Path) -> None:
    path = tmp_path / "application.vstore"
    with Store.open(path) as store:
        store.put(b"user:\x00", b"\x00\xffpayload")
        store.put(b"user:\x01", b"second")
        store.put(b"other", b"ignored")

        assert store.get(b"user:\x00") == b"\x00\xffpayload"
        assert store.contains(b"user:\x01") is True
        assert store.scan_prefix(b"user:") == [
            (b"user:\x00", b"\x00\xffpayload"),
            (b"user:\x01", b"second"),
        ]

    with Store.open(path) as reopened:
        assert reopened.get(b"user:\x00") == b"\x00\xffpayload"


def test_transaction_commit_and_rollback(tmp_path: Path) -> None:
    path = tmp_path / "tx.vstore"
    with Store.open(path) as store:
        with store.transaction() as tx:
            tx.put(b"a", b"1")
            tx.put(b"b", b"2")

        assert store.get(b"a") == b"1"
        assert store.get(b"b") == b"2"

        with pytest.raises(RuntimeError):
            with store.transaction() as tx:
                tx.put(b"ghost", b"no")
                raise RuntimeError("rollback")

        assert store.get(b"ghost") is None


def test_transaction_reads_own_staged_mutations(tmp_path: Path) -> None:
    path = tmp_path / "transaction-view.vstore"
    with Store.open(path) as store:
        store.put(b"user:1", b"old")
        store.put(b"user:2", b"keep")
        store.put(b"user:3", b"remove")
        store.put(b"other:1", b"ignored")

        with store.transaction() as tx:
            assert tx.get(b"user:1") == b"old"
            assert tx.contains(b"user:3") is True

            tx.put(b"user:1", b"new")
            tx.put(b"user:4", b"created")
            tx.delete(b"user:3")

            assert tx.get(b"user:1") == b"new"
            assert tx.get(b"user:4") == b"created"
            assert tx.get(b"user:3") is None
            assert tx.contains(b"user:4") is True
            assert tx.contains(b"user:3") is False
            assert tx.scan_prefix(b"user:") == [
                (b"user:1", b"new"),
                (b"user:2", b"keep"),
                (b"user:4", b"created"),
            ]

        assert store.get(b"user:1") == b"new"
        assert store.get(b"user:3") is None
        assert store.get(b"user:4") == b"created"


def test_transaction_latest_staged_mutation_wins(tmp_path: Path) -> None:
    path = tmp_path / "transaction-order.vstore"
    with Store.open(path) as store:
        store.put(b"item", b"committed")
        tx = store.transaction()

        tx.delete(b"item")
        assert tx.get(b"item") is None

        tx.put(b"item", b"restored")
        assert tx.get(b"item") == b"restored"

        tx.put(b"item", b"latest")
        assert tx.get(b"item") == b"latest"

        tx.rollback()
        assert store.get(b"item") == b"committed"


def test_store_is_exclusive_during_transaction(tmp_path: Path) -> None:
    path = tmp_path / "exclusive.vstore"
    with Store.open(path) as store:
        tx = store.transaction()
        tx.put(b"a", b"1")
        with pytest.raises(StoreBusyError):
            store.put(b"b", b"2")
        tx.rollback()
        store.put(b"b", b"2")
        assert store.get(b"b") == b"2"


def test_finished_transaction_is_unusable(tmp_path: Path) -> None:
    path = tmp_path / "finished.vstore"
    with Store.open(path) as store:
        tx = store.transaction()
        tx.put(b"a", b"1")
        tx.commit()
        with pytest.raises(TransactionFinishedError):
            tx.put(b"b", b"2")
        with pytest.raises(TransactionFinishedError):
            tx.get(b"a")
        with pytest.raises(TransactionFinishedError):
            tx.contains(b"a")
        with pytest.raises(TransactionFinishedError):
            tx.scan_prefix(b"")


def test_reserved_namespace_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "reserved.vstore"
    with Store.open(path) as store:
        with pytest.raises(ReservedKeyError):
            store.put(b"\xffvds:test", b"value")


def test_closed_store_is_unusable(tmp_path: Path) -> None:
    path = tmp_path / "closed.vstore"
    store = Store.open(path)
    store.close()
    with pytest.raises(StoreClosedError):
        store.get(b"key")


def test_open_accepts_durability_options(tmp_path: Path) -> None:
    for durability in ("strict", "data", "relaxed"):
        path = tmp_path / f"{durability}.vstore"
        with Store.open(path, durability=durability, repair_torn_tail=False) as store:
            store.put(b"mode", durability.encode())
        with Store.open(path) as reopened:
            assert reopened.get(b"mode") == durability.encode()


def test_open_rejects_unknown_durability(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        Store.open(tmp_path / "invalid.vstore", durability="unknown")


def test_verify_reports_native_store_metadata(tmp_path: Path) -> None:
    path = tmp_path / "verify.vstore"
    with Store.open(path) as store:
        store.put(b"alpha", b"1")
        with store.transaction() as tx:
            tx.put(b"beta", b"2")
            tx.put(b"gamma", b"3")

    report = Store.verify(path)
    assert report.file_bytes == report.valid_bytes
    assert report.records > 0
    assert report.committed_transactions == 2
    assert report.pending_transactions == 0
    assert report.keys >= 3
    assert report.has_torn_tail is False
    assert isinstance(report.store_id, bytes)
    assert len(report.store_id) == 16
    assert report.store_id != b"\x00" * 16
