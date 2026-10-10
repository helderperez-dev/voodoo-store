"""Provider-neutral collection data migration helpers.

The native Store owns schema metadata and atomic transactions; this module owns
application-level value transformation. Batches are individually atomic, and
the schema version advances only after every batch succeeds. A transformer must
therefore be idempotent so an interrupted migration can be safely resumed.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

from ._native import Store

MigrationResult = bytes | tuple[bytes, Sequence[tuple[bytes, bytes]]]
Transformer = Callable[[bytes, bytes, Sequence[tuple[bytes, bytes]]], MigrationResult]


class MigrationError(ValueError):
    """Invalid migration definition or transformation result."""


class MigrationConflict(MigrationError):
    """The collection changed schema before this migration could commit."""


@dataclass(frozen=True)
class MigrationReport:
    collection: bytes
    from_version: int
    to_version: int
    records_seen: int
    records_migrated: int
    batches_committed: int
    dry_run: bool = False

    def describe(self) -> dict[str, Any]:
        return {
            "collection": self.collection,
            "from_version": self.from_version,
            "to_version": self.to_version,
            "records_seen": self.records_seen,
            "records_migrated": self.records_migrated,
            "batches_committed": self.batches_committed,
            "dry_run": self.dry_run,
        }


def migrate_collection(
    store: Store,
    collection: bytes,
    *,
    expected_version: int,
    next_version: int,
    codec: bytes,
    transform: Transformer,
    batch_size: int = 100,
    dry_run: bool = False,
) -> MigrationReport:
    """Transform records and advance a collection schema safely.

    ``transform`` receives ``(primary_key, value, indexes)`` and returns a new
    value or ``(value, indexes)``. Every committed batch is one native Store
    transaction. The schema transition and migration-history entry are written
    only after all transformed batches complete.
    """
    if not isinstance(collection, bytes) or not collection:
        raise MigrationError("collection must be non-empty bytes")
    if not isinstance(codec, bytes) or not codec:
        raise MigrationError("codec must be non-empty bytes")
    if expected_version < 1 or next_version <= expected_version:
        raise MigrationError("next_version must be greater than expected_version")
    if batch_size < 1:
        raise MigrationError("batch_size must be positive")
    if not callable(transform):
        raise MigrationError("transform must be callable")

    definition = store.collection_definition(collection)
    if definition is None:
        raise MigrationError(f"collection does not exist: {collection!r}")
    current_version, _current_codec = definition
    if current_version != expected_version:
        raise MigrationConflict(
            f"expected collection {collection!r} at schema {expected_version}, "
            f"found {current_version}"
        )

    pending: list[tuple[bytes, bytes, list[tuple[bytes, bytes]]]] = []
    seen = migrated = batches = 0

    def commit_batch() -> None:
        nonlocal batches
        if dry_run or not pending:
            return
        with store.transaction() as tx:
            for primary_key, value, indexes in pending:
                tx.upsert_record(collection, primary_key, value, indexes=indexes)
        batches += 1

    for primary_key, value, indexes in store.scan_collection(collection):
        seen += 1
        result = transform(bytes(primary_key), bytes(value), tuple(indexes))
        if isinstance(result, tuple):
            if len(result) != 2:
                raise MigrationError("transform tuple result must be (value, indexes)")
            new_value, new_indexes = result
        else:
            new_value, new_indexes = result, indexes
        if not isinstance(new_value, bytes):
            raise MigrationError("transform value must be bytes")
        normalized_indexes = []
        for item in new_indexes:
            if not isinstance(item, tuple) or len(item) != 2:
                raise MigrationError("transform indexes must contain (name, value) tuples")
            index_name, index_value = item
            if not isinstance(index_name, bytes) or not isinstance(index_value, bytes):
                raise MigrationError("transform indexes must contain bytes")
            normalized_indexes.append((index_name, index_value))
        pending.append((bytes(primary_key), new_value, normalized_indexes))
        migrated += 1
        if len(pending) >= batch_size:
            commit_batch()
            pending.clear()
    commit_batch()
    pending.clear()

    if not dry_run:
        if not store.migrate_collection_schema(
            collection,
            expected_version,
            next_version,
            codec=codec,
        ):
            raise MigrationConflict(
                f"collection {collection!r} changed during migration"
            )
    return MigrationReport(
        collection=collection,
        from_version=expected_version,
        to_version=next_version,
        records_seen=seen,
        records_migrated=migrated,
        batches_committed=batches,
        dry_run=dry_run,
    )
