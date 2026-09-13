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
