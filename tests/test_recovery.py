"""Process-restart recovery on SQLite: resume, no reruns, no omissions."""
from __future__ import annotations

import asyncio

from app.models import TaskState
from app.orchestrator import Orchestrator
from app.store_sqlite import SqliteTaskStore

from .conftest import wait_state


async def _hard_crash(orch: Orchestrator) -> None:
    """Cancel every running coroutine, stop loop, close DB without completion."""
    for t in list(orch._engine._running_tasks.values()):
        t.cancel()
    await orch._engine.stop()
    orch._store._conn.close()


async def test_running_task_resumes_after_restart(tmp_path):
    db = str(tmp_path / "r.db")
    store = SqliteTaskStore(db)
    o1 = Orchestrator(store=store, max_concurrency=4)
    await o1.start()
    await o1.trigger_workflow(
        [
            {"id": "a", "handler": "sleep", "payload": {"seconds": 0.05}},
            {
                "id": "b",
                "handler": "sleep",
                "payload": {"seconds": 0.3},
                "deps": ["a"],
                "max_attempts": 3,
                "backoff": {"base_delay": 0.01},
            },
            {"id": "c", "handler": "noop", "deps": ["b"]},
        ]
    )
    await wait_state(o1, "b", {TaskState.RUNNING})
    await _hard_crash(o1)

    # b was mid-flight; on restart it retries from attempt 2, then c runs
    store2 = SqliteTaskStore(db)
    o2 = Orchestrator(store=store2, max_concurrency=4)
    await o2.start()
    b = await wait_state(store2 and o2, "b")
    c = await wait_state(o2, "c")
    assert b.state is TaskState.SUCCEEDED
    assert len(b.attempts) == 2
    assert c.state is TaskState.SUCCEEDED
    hist = await o2.get_history("b")
    assert any(e.type.value == "RECOVERED" for e in hist)
    await o2.stop()


async def test_completed_tasks_are_not_rerun(tmp_path):
    db = str(tmp_path / "r.db")
    store = SqliteTaskStore(db)
    o1 = Orchestrator(store=store, max_concurrency=4)
    await o1.start()
    invocations = []

    async def counting(ctx):
        invocations.append(ctx.task_id)
        return {"ran": len(invocations)}

    o1.register_handler("counting", counting)
    await o1.trigger("counting", task_id="done")
    await wait_state(o1, "done")
    assert invocations == ["done"]
    await _hard_crash(o1)

    store2 = SqliteTaskStore(db)
    o2 = Orchestrator(store=store2, max_concurrency=4)
    o2.register_handler("counting", counting)
    await o2.start()
    await asyncio.sleep(0.2)
    rec = await o2.get_status("done")
    assert rec.record.state is TaskState.SUCCEEDED
    assert invocations == ["done"]  # not executed again
    assert len(rec.record.attempts) == 1
    await o2.stop()


async def test_failed_workflow_members_are_not_missed_on_restart(tmp_path):
    # A predecessor is executing (RUNNING) when the process dies with no
    # retry budget; after restart it ends FAILED and the successor must still
    # be evaluated and SKIPPED (nothing is silently dropped).
    from .helpers import Recorder

    db = str(tmp_path / "r.db")
    store = SqliteTaskStore(db)
    o1 = Orchestrator(store=store, max_concurrency=4)
    rec = Recorder()
    o1.register_handler("gated", rec.make_handler(wait_for_gate=True))
    await o1.start()
    await o1.trigger_workflow(
        [
            {"id": "f", "handler": "gated", "max_attempts": 1},
            {"id": "s", "handler": "noop", "deps": ["f"]},
        ]
    )
    await wait_state(o1, "f", {TaskState.RUNNING})
    await _hard_crash(o1)

    store2 = SqliteTaskStore(db)
    o2 = Orchestrator(store=store2, max_concurrency=4)
    o2.register_handler("gated", rec.make_handler(wait_for_gate=True))
    await o2.start()
    f = await wait_state(o2, "f")
    s = await wait_state(o2, "s")
    assert f.state is TaskState.FAILED  # interrupted attempt used the budget
    assert f.error_type == "InterruptedByRestart"
    assert s.state is TaskState.SKIPPED
    assert "f" in (s.skip_reason or "")
    await o2.stop()


async def test_triggers_during_recovery_have_deterministic_outcome(tmp_path):
    db = str(tmp_path / "r.db")
    store = SqliteTaskStore(db)
    o1 = Orchestrator(store=store, max_concurrency=4)
    await o1.start()
    await o1.trigger(
        "sleep", task_id="old", payload={"seconds": 0.3},
        max_attempts=3, backoff={"base_delay": 0.01},
    )
    await wait_state(o1, "old", {TaskState.RUNNING})
    await _hard_crash(o1)

    store2 = SqliteTaskStore(db)
    o2 = Orchestrator(store=store2, max_concurrency=4)
    await o2.start()
    # immediately (concurrently with recovery scheduling) trigger new tasks
    await o2.trigger_workflow(
        [
            {"id": "new-a", "handler": "noop"},
            {"id": "new-b", "handler": "noop", "deps": ["new-a"]},
        ]
    )
    await o2.trigger("noop", task_id="new-c")
    old = await wait_state(o2, "old")
    for tid in ("new-a", "new-b", "new-c"):
        rec = await wait_state(o2, tid)
        assert rec.state is TaskState.SUCCEEDED, tid
    assert old.state is TaskState.SUCCEEDED
    # every task present exactly once in persisted list
    tasks = await o2.list_tasks()
    ids = [t.record.id for t in tasks]
    assert sorted(ids) == ["new-a", "new-b", "new-c", "old"]
    await o2.stop()


async def test_retry_wait_elapsed_during_downtime_retries_immediately(tmp_path):
    # Crash right after attempt 1 failed into RETRY_WAIT with a short
    # backoff; by the time the new process is up, not_before has already
    # passed, so the task must go READY -> RUNNING immediately (no extra wait).
    db = str(tmp_path / "r.db")
    store = SqliteTaskStore(db)
    o1 = Orchestrator(store=store, max_concurrency=4)
    await o1.start()
    await o1.trigger(
        "fail", task_id="t", max_attempts=2,
        backoff={"base_delay": 0.05, "multiplier": 1},
    )
    await wait_state(o1, "t", {TaskState.RETRY_WAIT})
    await _hard_crash(o1)

    # sleep longer than the backoff while the process is down
    await asyncio.sleep(0.15)

    store2 = SqliteTaskStore(db)
    o2 = Orchestrator(store=store2, max_concurrency=4)
    await o2.start()
    rec = await wait_state(o2, "t")
    assert rec.state is TaskState.FAILED  # second attempt also fails
    assert len(rec.attempts) == 2
    # a RECOVERED audit event exists for the resumed RETRY_WAIT task
    hist = await o2.get_history("t")
    assert any(e.type.value == "RECOVERED" for e in hist)
    await o2.stop()
