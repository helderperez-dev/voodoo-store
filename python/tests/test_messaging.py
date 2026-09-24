from __future__ import annotations

import pytest

from voodoo_store import Store, VoodooStoreError


def test_native_stream_append_read_and_reopen(tmp_path):
    path = tmp_path / "streams.vstore"

    with Store.open(path) as store:
        assert store.append_stream(b"orders", b"created") == 0
        assert store.append_stream(b"orders", b"paid") == 1
        assert store.stream_tail_offset(b"orders") == 1
        assert store.read_stream(b"orders", 1, 10) == [(1, b"paid")]

    with Store.open(path) as reopened:
        assert reopened.read_stream(b"orders", 0, 10) == [
            (0, b"created"),
            (1, b"paid"),
        ]


def test_native_topic_subscription_cursor_and_replay(tmp_path):
    path = tmp_path / "topics.vstore"

    with Store.open(path) as store:
        assert store.publish_topic(b"events", b"one") == 0
        assert store.publish_topic(b"events", b"two") == 1
        assert store.publish_topic(b"events", b"three") == 2

        assert store.topic_subscription_offset(b"events", b"billing") == 0
        assert store.poll_topic(b"events", b"billing", 2) == [
            (0, b"one"),
            (1, b"two"),
        ]
        assert store.acknowledge_topic_through(b"events", b"billing", 1) == 2
        assert store.topic_subscription_offset(b"events", b"billing") == 2
        assert store.poll_topic(b"events", b"billing", 10) == [(2, b"three")]

        store.reset_topic_subscription(b"events", b"billing", 0)
        assert store.poll_topic(b"events", b"billing", 10) == [
            (0, b"one"),
            (1, b"two"),
            (2, b"three"),
        ]

    with Store.open(path) as reopened:
        assert reopened.topic_subscription_offset(b"events", b"billing") == 0


def test_topic_cursor_rejects_regression_without_explicit_reset(tmp_path):
    with Store.open(tmp_path / "topic-cursor.vstore") as store:
        store.publish_topic(b"events", b"one")
        store.publish_topic(b"events", b"two")
        assert store.acknowledge_topic_through(b"events", b"consumer", 1) == 2

        with pytest.raises(VoodooStoreError):
            store.acknowledge_topic_through(b"events", b"consumer", 0)
