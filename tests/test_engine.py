"""Scenario tests for the orchestration engine.

Each test builds its own engine; async bodies are driven with asyncio.run
so no pytest-asyncio dependency is needed.
"""
import asyncio
import json
import os
import subprocess
import sys
import time

import pytest

from orchestrator import (
    BackoffStrategy,
    DependencyFailurePolicy,
    OrchestrationEngine,
    RetryPolicy,
    TaskDefinition,
    TaskRegistry,
    TaskState,
    TaskStore,
)


def make_engine(registry, max_concurrency=4, db_path=":memory:"):
    return OrchestrationEngine(TaskStore(db_path), registry, max_concurrency)


def states_of(snap):
    return {t.task_name: t.state for t in snap.tasks}


# --------------------------------------------------------------------- #
# 1. dependency handling
# --------------------------------------------------------------------- #
def test_dependency_failure_skips_dependents_with_reason():
    async def main():
        reg = TaskRegistry()

        async def fail(i, t):
            raise RuntimeError("upstream broke")

        async def never(i, t):
            pytest.fail("dependent must not run")

        reg.register(TaskDefinition("fail", fail))
        reg.register(TaskDefinition("mid", never, dependencies=("fail",)))
        reg.register(TaskDefinition("leaf", never, dependencies=("mid",)))
        eng = make_engine(reg)
        await eng.start()
        r = await eng.trigger("leaf")
        snap = await eng.wait_run_done(r.run_id)
        s = states_of(snap)
        assert s["fail"] == TaskState.FAILED
        assert s["mid"] == TaskState.SKIPPED
        assert s["leaf"] == TaskState.SKIPPED
        reasons = {t.task_name: t.skip_reason for t in snap.tasks}
        assert "fail" in reasons["mid"] and "FAILED" in reasons["mid"]
        assert "mid" in reasons["leaf"]  # skip propagates down the chain
        await eng.stop()

    asyncio.run(main())


def test_dependency_failure_cancel_policy():
    async def main():
        reg = TaskRegistry()

        async def fail(i, t):
            raise RuntimeError("nope")

        async def never(i, t):
            pytest.fail("dependent must not run")

        reg.register(TaskDefinition("fail", fail))
        reg.register(TaskDefinition(
            "child", never, dependencies=("fail",),
            on_dependency_failure=DependencyFailurePolicy.CANCEL,
        ))
        eng = make_engine(reg)
        await eng.start()
        r = await eng.trigger("child")
        snap = await eng.wait_run_done(r.run_id)
        s = states_of(snap)
        assert s["child"] == TaskState.CANCELLED
        child = next(t for t in snap.tasks if t.task_name == "child")
        assert "fail" in (child.error or "")
        await eng.stop()

    asyncio.run(main())


def test_dependency_waits_for_all_predecessors():
    async def main():
        reg = TaskRegistry()
        done = []

        def mk(name, delay):
            async def h(i, t):
                await asyncio.sleep(delay)
                done.append(name)
                return name
            return h

        reg.register(TaskDefinition("a", mk("a", 0.05)))
        reg.register(TaskDefinition("b", mk("b", 0.10)))
        reg.register(TaskDefinition("join", mk("join", 0.0), dependencies=("a", "b")))
        eng = make_engine(reg)
        await eng.start()
        r = await eng.trigger("join")
        snap = await eng.wait_run_done(r.run_id)
        assert all(s == TaskState.SUCCEEDED for s in states_of(snap).values())
        assert done.index("join") > done.index("a")
        assert done.index("join") > done.index("b")
        await eng.stop()

    asyncio.run(main())


# --------------------------------------------------------------------- #
# 2. concurrency, priority, mutex
# --------------------------------------------------------------------- #
def test_concurrency_limit_and_stable_priority_order():
    async def main():
        reg = TaskRegistry()
        started = []
        running = 0
        max_running = 0

        def mk(name, prio):
            async def h(i, t):
                nonlocal running, max_running
                running += 1
                max_running = max(max_running, running)
                started.append(name)
                await asyncio.sleep(0.03)
                running -= 1
                return name
            return TaskDefinition(name, h, priority=prio)

        # blocker occupies the single slot while the others queue up
        async def blocker(i, t):
            await asyncio.sleep(0.10)

        reg.register(TaskDefinition("blocker", blocker, priority=100))
        for name, prio in [("p0a", 0), ("p0b", 0), ("p5", 5), ("p0c", 0), ("p1", 1)]:
            reg.register(mk(name, prio))
        eng = make_engine(reg, max_concurrency=1)
        await eng.start()
        rb = await eng.trigger("blocker")
        await asyncio.sleep(0.02)  # ensure blocker is RUNNING
        results = [await eng.trigger(n) for n in ["p0a", "p0b", "p5", "p0c", "p1"]]
        await eng.wait_run_done(rb.run_id)
        for r in results:
            await eng.wait_run_done(r.run_id)
        order = [n for n in started]
        assert order == ["p5", "p1", "p0a", "p0b", "p0c"], order
        assert max_running == 1
        await eng.stop()

    asyncio.run(main())


def test_mutex_group_never_runs_concurrently():
    async def main():
        reg = TaskRegistry()
        in_group = 0
        max_in_group = 0

        def mk(name):
            async def h(i, t):
                nonlocal in_group, max_in_group
                in_group += 1
                max_in_group = max(max_in_group, in_group)
                await asyncio.sleep(0.03)
                in_group -= 1
            return TaskDefinition(name, h, mutex_group="exclusive")

        for n in ["m1", "m2", "m3"]:
            reg.register(mk(n))
        eng = make_engine(reg, max_concurrency=4)
        await eng.start()
        rs = [await eng.trigger(n) for n in ["m1", "m2", "m3"]]
        for r in rs:
            await eng.wait_run_done(r.run_id)
        assert max_in_group == 1
        await eng.stop()

    asyncio.run(main())


# --------------------------------------------------------------------- #
# 3. duplicate trigger & retry
# --------------------------------------------------------------------- #
def test_duplicate_trigger_is_idempotent_and_side_effect_free():
    async def main():
        reg = TaskRegistry()
        calls = 0

        async def h(i, t):
            nonlocal calls
            calls += 1
            await asyncio.sleep(0.02)
            return 1

        reg.register(TaskDefinition("t", h))
        eng = make_engine(reg)
        await eng.start()
        # fire the same trigger concurrently
        results = await asyncio.gather(*[
            eng.trigger("t", payload={"a": 1}, idempotency_key="same-key")
            for _ in range(10)
        ])
        run_ids = {r.run_id for r in results}
        assert len(run_ids) == 1
        assert sum(1 for r in results if r.created) == 1
        snap = await eng.wait_run_done(results[0].run_id)
        assert states_of(snap)["t"] == TaskState.SUCCEEDED
        assert calls == 1, f"handler ran {calls} times"
        await eng.stop()

    asyncio.run(main())


def test_retry_with_backoff_eventually_succeeds():
    async def main():
        reg = TaskRegistry()
        attempts = []
        t0 = time.monotonic()

        async def flaky(i, t):
            attempts.append(time.monotonic() - t0)
            if len(attempts) < 3:
                raise RuntimeError(f"fail #{len(attempts)}")
            return "finally"

        reg.register(TaskDefinition("flaky", flaky, retry_policy=RetryPolicy(
            max_retries=3, backoff=BackoffStrategy.EXPONENTIAL, base_delay=0.05,
        )))
        eng = make_engine(reg)
        await eng.start()
        r = await eng.trigger("flaky")
        snap = await eng.wait_run_done(r.run_id)
        task = snap.tasks[0]
        assert task.state == TaskState.SUCCEEDED
        assert task.attempt == 3
        assert task.result == "finally"
        # exponential backoff: gaps should grow (0.05, 0.10)
        gaps = [b - a for a, b in zip(attempts, attempts[1:])]
        assert gaps[1] > gaps[0], gaps
        # history shows the full retry chain
        hist = eng.get_history(task.instance_id)
        chain = [(h.from_state, h.to_state) for h in hist]
        assert chain[0] == (None, TaskState.PENDING)
        assert chain[-1] == (TaskState.RUNNING, TaskState.SUCCEEDED)
        assert sum(1 for _, to in chain if to == TaskState.RUNNING) == 3
        await eng.stop()

    asyncio.run(main())


def test_retry_exhaustion_leads_to_failed():
    async def main():
        reg = TaskRegistry()

        async def always(i, t):
            raise RuntimeError("permanent")

        reg.register(TaskDefinition("always", always, retry_policy=RetryPolicy(
            max_retries=2, base_delay=0.01,
        )))
        eng = make_engine(reg)
        await eng.start()
        r = await eng.trigger("always")
        snap = await eng.wait_run_done(r.run_id)
        task = snap.tasks[0]
        assert task.state == TaskState.FAILED
        assert task.attempt == 3  # 1 initial + 2 retries
        assert "permanent" in (task.error or "")
        await eng.stop()

    asyncio.run(main())


# --------------------------------------------------------------------- #
# 4. cancel & timeout
# --------------------------------------------------------------------- #
def test_timeout_is_distinct_from_failure():
    async def main():
        reg = TaskRegistry()

        async def slow(i, t):
            await asyncio.sleep(5)

        async def boom(i, t):
            raise RuntimeError("crash")

        reg.register(TaskDefinition("slow", slow, timeout=0.05))
        reg.register(TaskDefinition("boom", boom))
        eng = make_engine(reg)
        await eng.start()
        r1 = await eng.trigger("slow")
        r2 = await eng.trigger("boom")
        s1 = await eng.wait_run_done(r1.run_id)
        s2 = await eng.wait_run_done(r2.run_id)
        assert s1.tasks[0].state == TaskState.TIMED_OUT
        assert "timeout" in (s1.tasks[0].error or "")
        assert s2.tasks[0].state == TaskState.FAILED
        assert "crash" in (s2.tasks[0].error or "")
        await eng.stop()

    asyncio.run(main())


def test_cancel_running_task_stops_and_writes_no_result():
    async def main():
        reg = TaskRegistry()
        observed_cancel = asyncio.Event()

        async def h(i, t):
            for _ in range(200):
                await asyncio.sleep(0.01)
                if t.cancelled:
                    observed_cancel.set()
                    t.throw_if_cancelled()
            return "should-never-be-stored"

        reg.register(TaskDefinition("t", h))
        eng = make_engine(reg)
        await eng.start()
        r = await eng.trigger("t")
        iid = r.instance_ids[0]
        # wait until RUNNING
        for _ in range(200):
            snap = eng.get_task(iid)
            if snap and snap.state == TaskState.RUNNING:
                break
            await asyncio.sleep(0.01)
        assert eng.get_task(iid).state == TaskState.RUNNING
        cancelled = await eng.cancel(r.run_id)
        assert cancelled
        snap = await eng.wait_run_done(r.run_id)
        task = snap.tasks[0]
        assert task.state == TaskState.CANCELLED
        assert task.result is None  # no result written after cancel
        await asyncio.wait_for(observed_cancel.wait(), timeout=2)
        # history shows the cancellation
        hist = eng.get_history(iid)
        assert hist[-1].to_state == TaskState.CANCELLED
        await eng.stop()

    asyncio.run(main())


def test_cancel_queued_task_and_dependents_follow_policy():
    async def main():
        reg = TaskRegistry()
        ran = []

        async def blocker(i, t):
            await asyncio.sleep(0.15)

        async def queued(i, t):
            ran.append("queued")

        async def dependent(i, t):
            ran.append("dependent")

        reg.register(TaskDefinition("blocker", blocker, priority=10))
        reg.register(TaskDefinition("queued", queued, priority=0))
        reg.register(TaskDefinition("dependent", dependent, dependencies=("queued",)))
        eng = make_engine(reg, max_concurrency=1)
        await eng.start()
        rb = await eng.trigger("blocker")
        await asyncio.sleep(0.03)  # blocker occupies the only slot
        rq = await eng.trigger("dependent")  # creates queued + dependent
        # 'queued' has no deps: it is READY, waiting for the occupied slot
        assert eng.get_task(f"{rq.run_id}:queued").state == TaskState.READY
        cancelled = await eng.cancel(rq.run_id, task_name="queued")
        assert cancelled
        snap = await eng.wait_run_done(rq.run_id)
        s = states_of(snap)
        assert s["queued"] == TaskState.CANCELLED
        assert s["dependent"] == TaskState.SKIPPED  # default policy: skip
        assert ran == []
        await eng.wait_run_done(rb.run_id)
        await eng.stop()

    asyncio.run(main())


# --------------------------------------------------------------------- #
# 5. restart recovery
# --------------------------------------------------------------------- #
def test_restart_recovers_unfinished_and_never_repeats_finished(tmp_path):
    db = str(tmp_path / "orch.db")
    marker = str(tmp_path / "marker.json")
    # phase 1 runs in a child process that crashes (os._exit) while one
    # task is RUNNING and another has completed
    child = os.path.join(os.path.dirname(__file__), "crash_child.py")
    project_root = os.path.dirname(os.path.dirname(__file__))
    env = dict(os.environ, PYTHONPATH=project_root)
    proc = subprocess.Popen(
        [sys.executable, child, db, marker],
        cwd=project_root, env=env,
    )
    deadline = time.time() + 30
    while not os.path.exists(marker):
        assert time.time() < deadline, "child did not reach RUNNING state"
        time.sleep(0.05)
    proc.wait(timeout=10)
    with open(marker) as f:
        ids = json.load(f)
    run_done, run_int = ids["run_done"], ids["run_int"]

    async def phase2():
        reg = TaskRegistry()
        calls = {"done": 0, "interrupted": 0}

        async def done_task(i, t):
            calls["done"] += 1
            return "finished-again"

        async def interrupted(i, t):
            calls["interrupted"] += 1
            return "recovered"

        async def fresh(i, t):
            return "new"

        reg.register(TaskDefinition("done", done_task))
        reg.register(TaskDefinition("interrupted", interrupted))
        reg.register(TaskDefinition("fresh", fresh))
        eng = make_engine(reg, db_path=db)
        # a new trigger arrives concurrently with recovery
        new_trigger = asyncio.create_task(eng.trigger("fresh"))
        await eng.start()
        r_new = await new_trigger
        s_int = await eng.wait_run_done(run_int)
        s_done = await eng.wait_run_done(run_done)
        s_new = await eng.wait_run_done(r_new.run_id)
        # interrupted task resumed and completed
        assert s_int.tasks[0].state == TaskState.SUCCEEDED
        assert s_int.tasks[0].result == "recovered"
        # completed task was NOT re-executed after restart
        assert calls["done"] == 0
        assert s_done.tasks[0].result == "finished"
        # recovered task ran exactly once more
        assert calls["interrupted"] == 1
        # history records the recovery reset
        hist = eng.get_history(f"{run_int}:interrupted")
        resets = [h for h in hist if h.reason and "recovered after restart" in h.reason]
        assert resets and resets[0].from_state == TaskState.RUNNING
        assert resets[0].to_state == TaskState.READY
        assert s_new.tasks[0].state == TaskState.SUCCEEDED
        await eng.stop()

    asyncio.run(phase2())


def test_restart_recovers_pending_chain(tmp_path):
    db = str(tmp_path / "chain.db")
    # simulate a crash with a full PENDING chain persisted
    store = TaskStore(db)
    store.open()
    from orchestrator.models import TaskInstance
    run_id = "run-chain"
    for name in ["a", "b", "c"]:
        store.insert_instance(TaskInstance(
            instance_id=f"{run_id}:{name}", run_id=run_id, task_name=name,
        ))
    store.close()

    async def main():
        reg = TaskRegistry()
        order = []

        def mk(name):
            async def h(i, t):
                order.append(name)
                return name
            return h

        reg.register(TaskDefinition("a", mk("a")))
        reg.register(TaskDefinition("b", mk("b"), dependencies=("a",)))
        reg.register(TaskDefinition("c", mk("c"), dependencies=("b",)))
        eng = make_engine(reg, db_path=db)
        await eng.start()
        snap = await eng.wait_run_done(run_id)
        assert all(s == TaskState.SUCCEEDED for s in states_of(snap).values())
        assert order == ["a", "b", "c"]
        await eng.stop()

    asyncio.run(main())


# --------------------------------------------------------------------- #
# 6. history / snapshot consistency
# --------------------------------------------------------------------- #
def test_history_reconstructs_full_lifecycle_including_cancel():
    async def main():
        reg = TaskRegistry()

        async def h(i, t):
            await asyncio.sleep(5)

        reg.register(TaskDefinition("t", h))
        eng = make_engine(reg)
        await eng.start()
        r = await eng.trigger("t")
        iid = r.instance_ids[0]
        for _ in range(200):
            if eng.get_task(iid).state == TaskState.RUNNING:
                break
            await asyncio.sleep(0.01)
        await eng.cancel(r.run_id)
        snap = await eng.wait_run_done(r.run_id)
        hist = eng.get_history(iid)
        # monotonic seq, coherent from->to chain from creation to terminal
        assert [x.seq for x in hist] == sorted(x.seq for x in hist)
        assert hist[0].from_state is None
        for prev, cur in zip(hist, hist[1:]):
            assert cur.from_state == prev.to_state
        assert hist[-1].to_state == TaskState.CANCELLED
        assert snap.tasks[0].state == TaskState.CANCELLED
        await eng.stop()

    asyncio.run(main())


def test_threaded_concurrent_triggers_same_key():
    """Triggers from multiple threads (as uvicorn workers would do)."""
    async def main():
        reg = TaskRegistry()
        calls = 0

        async def h(i, t):
            nonlocal calls
            calls += 1
            return 1

        reg.register(TaskDefinition("t", h))
        eng = make_engine(reg)
        await eng.start()
        loop = asyncio.get_event_loop()
        results = await asyncio.gather(*[
            loop.run_in_executor(None, lambda: asyncio.run(
                eng.trigger("t", idempotency_key="thread-key")
            ))
            for _ in range(8)
        ])
        assert len({r.run_id for r in results}) == 1
        await eng.wait_run_done(results[0].run_id)
        assert calls == 1
        await eng.stop()

    asyncio.run(main())
