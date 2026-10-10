"""Bounded object transfer helpers for the Voodoo Store binding."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ._native import Store


def iter_object_chunks(
    store: "Store",
    object_id: bytes,
    *,
    chunk_size: int = 64 * 1024,
) -> Iterator[bytes]:
    """Yield an immutable object in bounded chunks.

    The native Store keeps content-addressed values durable and verifies the
    object ID on writes. This helper limits each Python transfer and preserves
    missing-object semantics by raising ``KeyError`` instead of silently
    yielding an empty stream. A provider that returns more than ``chunk_size``
    bytes fails closed with ``ValueError``. ``chunk_size`` must be positive.
    """
    if not isinstance(chunk_size, int) or isinstance(chunk_size, bool) or chunk_size <= 0:
        raise ValueError("chunk_size must be a positive integer")

    expected_size: int | None = None
    object_info = getattr(store, "object_info", None)
    if callable(object_info):
        info = object_info(object_id)
        if info is None:
            raise KeyError("object does not exist")
        if not isinstance(info, Mapping):
            raise ValueError("object provider returned invalid metadata")
        size = info.get("size")
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise ValueError("object provider returned invalid object size metadata")
        expected_size = size

    offset = 0
    while expected_size is None or offset < expected_size:
        limit = (
            chunk_size
            if expected_size is None
            else min(chunk_size, expected_size - offset)
        )
        chunk = store.get_object_chunk(object_id, offset, limit)
        if chunk is None:
            if offset == 0:
                raise KeyError("object does not exist")
            if expected_size is not None:
                raise ValueError("object provider ended before its metadata size")
            return
        if not chunk:
            if expected_size is not None:
                raise ValueError("object provider ended before its metadata size")
            return
        if len(chunk) > limit:
            raise ValueError(
                "object provider returned a chunk larger than the requested limit"
            )
        yield bytes(chunk)
        offset += len(chunk)


__all__ = ["iter_object_chunks"]
