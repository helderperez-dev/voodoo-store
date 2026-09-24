from __future__ import annotations

from voodoo_store import Store


def test_workflow_signal_wait_survives_reopen(tmp_path):
    path = tmp_path / "workflow-signal.vstore"

    with Store.open(path) as store:
        workflow_id = store.create_workflow(
            b"approval",
            b"request",
            b"pending",
            1_000,
        )
        assert len(workflow_id) == 16

        created = store.get_workflow(workflow_id)
        assert created is not None
        assert created["status"] == "running"
        assert created["current_step"] == b"request"
        assert created["state"] == b"pending"
        assert created["wait"] is None

        store.wait_for_signal(workflow_id, b"approved", 1_001)
        waiting = store.get_workflow(workflow_id)
        assert waiting is not None
        assert waiting["status"] == "waiting"
        assert waiting["wait"] == {"kind": "signal", "name": b"approved"}

    with Store.open(path) as reopened:
        waiting = reopened.get_workflow(workflow_id)
        assert waiting is not None
        assert waiting["status"] == "waiting"
        assert waiting["wait"] == {"kind": "signal", "name": b"approved"}

        assert reopened.signal_workflow(
            workflow_id,
            b"wrong",
            b"ignored",
            1_002,
        ) is False
        assert reopened.signal_workflow(
            workflow_id,
            b"approved",
            b"yes",
            1_003,
        ) is True

        resumed = reopened.get_workflow(workflow_id)
        assert resumed is not None
        assert resumed["status"] == "running"
        assert resumed["state"] == b"yes"
        assert resumed["wait"] is None

        history = reopened.workflow_history(workflow_id)
        assert [entry["kind"] for entry in history] == [
            "created",
            "waiting_signal",
            "signal_received",
        ]


def test_workflow_timer_step_and_completion(tmp_path):
    path = tmp_path / "workflow-timer.vstore"

    with Store.open(path) as store:
        workflow_id = store.create_workflow(
            b"report",
            b"prepare",
            b"initial",
            10,
        )
        store.set_workflow_step(workflow_id, b"sleep", b"queued", 11)
        store.wait_until(workflow_id, 100, 12)

        waiting = store.get_workflow(workflow_id)
        assert waiting is not None
        assert waiting["status"] == "waiting"
        assert waiting["current_step"] == b"sleep"
        assert waiting["wait"] == {"kind": "timer", "resume_at_ms": 100}

        assert store.resume_due_workflows(99, 10)["resumed"] == 0
        report = store.resume_due_workflows(100, 10)
        assert report["resumed"] == 1

        resumed = store.get_workflow(workflow_id)
        assert resumed is not None
        assert resumed["status"] == "running"
        assert resumed["wait"] is None

        store.complete_workflow(workflow_id, b"done", 101)
        completed = store.get_workflow(workflow_id)
        assert completed is not None
        assert completed["status"] == "completed"
        assert completed["state"] == b"done"

        history = store.workflow_history(workflow_id)
        assert [entry["kind"] for entry in history] == [
            "created",
            "step_changed",
            "waiting_timer",
            "timer_fired",
            "completed",
        ]


def test_workflow_parent_child_and_cancel(tmp_path):
    path = tmp_path / "workflow-tree.vstore"

    with Store.open(path) as store:
        parent = store.create_workflow(b"parent", b"start", b"", 1)
        child = store.create_workflow(
            b"child",
            b"start",
            b"",
            2,
            parent_id=parent,
        )

        children = store.workflow_children(parent)
        assert len(children) == 1
        assert children[0]["id"] == child
        assert children[0]["parent_id"] == parent

        assert store.cancel_workflow(child, b"no longer needed", 3) is True
        assert store.cancel_workflow(child, b"again", 4) is False
        cancelled = store.get_workflow(child)
        assert cancelled is not None
        assert cancelled["status"] == "cancelled"
