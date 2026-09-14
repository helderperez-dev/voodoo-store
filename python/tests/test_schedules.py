from __future__ import annotations

from voodoo_store import Store


def test_once_and_interval_schedules_create_durable_jobs(tmp_path):
    store = Store.open(tmp_path / "schedule.vstore")

    once_id = store.create_schedule(b"email.send", b"once", 100, mode="once")
    interval_id = store.create_schedule(
        b"heartbeat",
        b"interval",
        50,
        mode="interval",
        every_ms=100,
    )

    assert store.get_schedule(once_id)["mode"] == "once"
    interval = store.get_schedule(interval_id)
    assert interval["mode"] == "interval"
    assert interval["every_ms"] == 100

    assert store.tick_schedules(49, 10) == (2, 0)
    assert store.tick_schedules(100, 10) == (2, 2)

    first = store.claim_job(100, 1_000)
    second = store.claim_job(100, 1_000)
    assert {first["handler"], second["handler"]} == {b"email.send", b"heartbeat"}

    once = store.get_schedule(once_id)
    assert once["enabled"] is False
    interval = store.get_schedule(interval_id)
    assert interval["enabled"] is True
    assert interval["next_run_ms"] == 150
    store.close()


def test_cron_schedule_ticks_into_jobs_without_framework_code(tmp_path):
    store = Store.open(tmp_path / "cron.vstore")

    cron_id = store.create_cron_schedule(
        "* * * * *",
        b"report.build",
        b"{}",
        0,
    )
    cron = store.get_cron_schedule(cron_id)
    assert cron is not None
    assert cron["expression"] == b"* * * * *"
    assert cron["next_run_ms"] == 60_000
    assert cron["fire_count"] == 0

    assert store.tick_cron_schedules(59_999, 10) == (1, 0)
    assert store.tick_cron_schedules(60_000, 10) == (1, 1)

    fired = store.get_cron_schedule(cron_id)
    assert fired["fire_count"] == 1
    assert fired["last_fired_at_ms"] == 60_000
    job = store.claim_job(60_000, 1_000, handlers=[b"report.build"])
    assert job is not None
    assert job["handler"] == b"report.build"
    store.close()


def test_manual_trigger_atomically_routes_event_to_durable_job(tmp_path):
    store = Store.open(tmp_path / "trigger.vstore")

    trigger_id = store.create_trigger(
        b"invoice.created",
        "manual",
        b"invoice.process",
        b"template",
        max_attempts=2,
    )
    trigger = store.get_trigger(trigger_id)
    assert trigger is not None
    assert trigger["source"] == "manual"
    assert trigger["fire_count"] == 0

    job_id = store.fire_trigger(trigger_id, b'{"invoice":42}', 1_000)
    assert job_id is not None

    fired = store.get_trigger(trigger_id)
    assert fired["fire_count"] == 1
    assert fired["last_fired_at_ms"] == 1_000

    job = store.get_job(job_id)
    assert job is not None
    assert job["handler"] == b"invoice.process"
    assert job["payload"] == b'{"invoice":42}'
    assert job["max_attempts"] == 2

    claimed = store.claim_job(1_000, 1_000, handlers=[b"invoice.process"])
    assert claimed is not None
    assert claimed["id"] == job_id
    store.close()


def test_collection_stream_and_topic_trigger_metadata_round_trip(tmp_path):
    store = Store.open(tmp_path / "trigger-sources.vstore")

    collection_id = store.create_trigger(
        b"user-upsert",
        "collection",
        b"sync.user",
        b"{}",
        source_name=b"users",
        operation=b"upsert",
    )
    stream_id = store.create_trigger(
        b"audit-stream",
        "stream",
        b"audit.consume",
        b"{}",
        source_name=b"audit",
    )
    topic_id = store.create_trigger(
        b"orders-topic",
        "topic",
        b"order.consume",
        b"{}",
        source_name=b"orders",
    )

    collection = store.get_trigger(collection_id)
    assert collection["source"] == "collection"
    assert collection["source_name"] == b"users"
    assert collection["operation"] == b"upsert"

    stream = store.get_trigger(stream_id)
    assert stream["source"] == "stream"
    assert stream["source_name"] == b"audit"

    topic = store.get_trigger(topic_id)
    assert topic["source"] == "topic"
    assert topic["source_name"] == b"orders"

    assert len(store.list_triggers()) == 3
    assert store.set_trigger_enabled(topic_id, False) is True
    assert store.fire_trigger(topic_id, b"ignored", 2_000) is None
    store.close()
