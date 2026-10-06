"""Concurrency cap, priority scheduling, mutex groups, stable FIFO order."""
from __future__ import annotations

import asyncio

from .conftest import wait_state
from .helpers import Recorder


async def test_max_concurrency_is_respected(concurrency_1):
    orch = concurrency_1
    rec = Recorder()
    orch.register_handler("gated", rec.make_handler(wait_for_gate=True))
    for tid in ("t1", "t2", "t3"):
        await orch.trigger("gated", task_id=tid)
    # t1 starts and blocks; t2/t3 must stay queued
    await asyncio.sleep(0.1)
    assert rec.started == ["t1"]
    rec.open("t1")
    await asyncio.sleep(0.05)
    assert rec.started[:2] == ["t1", "t2"]
    rec.open("t2")
    rec.open("t3")
    for tid in ("t1", "t2", "t3"):
        await wait_state(orch, tid)
    assert rec.started == ["t1", "t2", "t3"]
    assert rec.max_active == 1


async def test_priority_order_with_concurrency_cap():
    from app.orchestrator import Orchestrator
    from app.store import InMemoryTaskStore

    store = InMemoryTaskStore()
    orch = Orchestrator(store=store, max_concurrency=1)
    await orch.start()
    try:
        rec = Recorder()
        orch.register_handler("gated", rec.make_handler(wait_for_gate=True))
        # low priority tasks created first, then a high priority one.
        await orch.trigger("gated", task_id="low1", priority=0)
        await asyncio.sleep(0.05)  # let low1 take the single slot
        await orch.trigger("gated", task_id="low2", priority=0)
        await orch.trigger("gated", task_id="high", priority=10)
        await orch.trigger("gated", task_id="mid", priority=5)
        await asyncio.sleep(0.05)
        assert rec.started == ["low1"]
        rec.open("low1")
        await asyncio.sleep(0.05)
        # after low1: high(10) then mid(5) then low2(0)
        assert rec.started[1] == "high"
        rec.open("high")
        await asyncio.sleep(0.05)
        assert rec.started[2] == "mid"
        rec.open("mid")
        rec.open("low2")
        for tid in ("high", "mid", "low2"):
            await wait_state(orch, tid)
        assert rec.started == ["low1", "high", "mid", "low2"]
    finally:
        await orch.stop()


async def test_equal_priority_is_stable_fifo(concurrency_1):
    orch = concurrency_1
    rec = Recorder()
    orch.register_handler("gated", rec.make_handler(wait_for_gate=True))
    ids = [f"eq-{i}" for i in range(8)]
    # occupy the slot first
    await orch.trigger("gated", task_id="holder")
    await asyncio.sleep(0.05)
    for tid in ids:
        await orch.trigger("gated", task_id=tid, priority=3)
    rec.open("holder")
    for tid in ids:
        await asyncio.sleep(0.03)
        rec.open(tid)
    for tid in ["holder", *ids]:
        await wait_state(orch, tid)
    # holder first; then exact creation order for equal priority
    assert rec.started == ["holder", *ids]


async def test_mutex_group_tasks_never_overlap(orch):
    rec = Recorder()
    orch.register_handler("gated", rec.make_handler(wait_for_gate=True))
    # Two mutex tasks share group M; a third task is independent.
    await orch.trigger("gated", task_id="m1", mutex_group="M")
    await orch.trigger("gated", task_id="m2", mutex_group="M")
    await orch.trigger("gated", task_id="free")
    await asyncio.sleep(0.1)
    # m1 and free can run together (cap is 8), m2 must wait for m1
    assert set(rec.started) == {"m1", "free"}
    rec.open("free")
    rec.open("m1")
    await asyncio.sleep(0.1)
    assert "m2" in rec.started
    rec.open("m2")
    for tid in ("m1", "m2", "free"):
        await wait_state(orch, tid)
    # verify no overlap by finish/start ordering in the recorded trace
    assert rec.finished.index("m1") < rec.finished.index("m2") or (
        rec.started.index("m1") < rec.started.index("m2")
    )


async def test_different_mutex_groups_run_concurrently(orch):
    rec = Recorder()
    orch.register_handler("gated", rec.make_handler(wait_for_gate=True))
    await orch.trigger("gated", task_id="g1", mutex_group="A")
    await orch.trigger("gated", task_id="g2", mutex_group="B")
    await asyncio.sleep(0.1)
    assert set(rec.started) == {"g1", "g2"}
    rec.open("g1")
    rec.open("g2")
    await wait_state(orch, "g1")
    await wait_state(orch, "g2")


async def test_priority_ordering_is_rebuilt_after_restart(tmp_path):
    """Equal-priority FIFO for queued READY tasks must survive a restart."""
    from app.orchestrator import Orchestrator
    from app.store_sqlite import SqliteTaskStore

    db = str(tmp_path / "sched.db")
    store = SqliteTaskStore(db)
    o1 = Orchestrator(store=store, max_concurrency=0 + 1)
    await o1.start()
    rec = Recorder()
    o1.register_handler("gated", rec.make_handler(wait_for_gate=True))
    # occupy the single slot with holder, then queue the p-* tasks
    await o1.trigger(
        "gated", task_id="holder", max_attempts=3,
        backoff={"base_delay": 0.01, "multiplier": 1, "max_delay": 0.05},
    )
    import asyncio as _aio

    await _aio.sleep(0.05)
    for i in range(4):
        await o1.trigger("gated", task_id=f"p-{i}", priority=1)
    # Wait until all p-* are READY/queued (holder is RUNNING, they must not start)
    await _aio.sleep(0.1)
    assert rec.started == ["holder"]

    # Simulate crash BEFORE releasing holder: p-* never started (stay READY).
    for t in list(o1._engine._running_tasks.values()):
        t.cancel()
    await o1._engine.stop()
    store._conn.close()

    store2 = SqliteTaskStore(db)
    rec2 = Recorder()
    o2 = Orchestrator(store=store2, max_concurrency=1)
    o2.register_handler("gated", rec2.make_handler(wait_for_gate=True))
    await o2.start()
    await _aio.sleep(0.15)
    # holder resumes (it was RUNNING at crash) -> retries; release it first
    rec2.open("holder")
    await _aio.sleep(0.1)
    for i in range(4):
        await _aio.sleep(0.03)
        rec2.open(f"p-{i}")
    for i in range(4):
        from .conftest import wait_state as _ws
        await _ws(o2, f"p-{i}")
    # p-* were all READY at crash and never ran; their post-restart order is FIFO
    assert rec2.started[:4] == [f"p-{i}" for i in range(4)], rec2.started
    await o2.stop()
