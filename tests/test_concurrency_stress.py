"""High-concurrency stress: many triggers/cancels/queries at once."""
from __future__ import annotations

import asyncio

from app.models import TERMINAL_STATES, TaskState

from .conftest import wait_state


VALID_STATES = {
    TaskState.PENDING,
    TaskState.READY,
    TaskState.RETRY_WAIT,
    TaskState.RUNNING,
    TaskState.SUCCEEDED,
    TaskState.FAILED,
    TaskState.SKIPPED,
    TaskState.CANCELLED,
    TaskState.TIMED_OUT,
}


async def test_many_concurrent_triggers_each_runs_once(orch):
    n = 60
    invocations = asyncio.Queue()

    async def counting(ctx):
        await invocations.put(ctx.task_id)
        return None

    orch.register_handler("counting", counting)
    await asyncio.gather(
        *[
            orch.trigger(
                "counting",
                task_id=f"t{i}",
                priority=(i % 5),
                mutex_group=("M" if i % 7 == 0 else None),
            )
            for i in range(n)
        ]
    )
    await asyncio.gather(*[wait_state(orch, f"t{i}") for i in range(n)])
    seen = []
    while not invocations.empty():
        seen.append(await invocations.get())
    assert sorted(seen) == sorted(f"t{i}" for i in range(n))
    assert len(seen) == n
    snaps = await orch.list_tasks()
    assert len(snaps) == n
    assert all(s.record.state is TaskState.SUCCEEDED for s in snaps)


async def test_concurrent_cancels_are_consistent(concurrency_1):
    orch = concurrency_1
    from .helpers import Recorder

    rec = Recorder()
    orch.register_handler("gated", rec.make_handler(wait_for_gate=True))
    await orch.trigger("gated", task_id="holder")
    await asyncio.sleep(0.05)
    # queue many tasks then cancel them from many coroutines simultaneously
    for i in range(20):
        await orch.trigger("gated", task_id=f"q{i}")
    outcomes = await asyncio.gather(
        *[orch.cancel(f"q{i}", reason="storm") for i in range(20)],
        return_exceptions=True,
    )
    # every queued task ends CANCELLED exactly; every cancel returns a bool
    assert all(isinstance(o, bool) for o in outcomes)
    for i in range(20):
        snap = await orch.get_status(f"q{i}")
        assert snap.record.state is TaskState.CANCELLED
    # holder can still finish normally
    rec.open("holder")
    await wait_state(orch, "holder")
    # queued tasks never ran
    assert rec.started == ["holder"]


async def test_snapshots_are_always_consistent_during_mutations(orch):
    stop = {"v": False}

    async def noisy(ctx):
        if ctx.attempt < 2:
            raise RuntimeError("retry me")
        return "ok"

    orch.register_handler("noisy", noisy)

    async def reader():
        while not stop["v"]:
            snaps = await orch.list_tasks()
            for snap in snaps:
                assert snap.record.state in VALID_STATES
                seqs = [e.seq for e in snap.events]
                assert seqs == list(range(1, len(seqs) + 1))
                # terminal tasks must end on an event matching their state
                if snap.record.state in TERMINAL_STATES:
                    terminal_events = {
                        "SUCCEEDED",
                        "FAILED",
                        "SKIPPED",
                        "CANCELLED",
                        "TIMED_OUT",
                    }
                    finals = [
                        e for e in snap.events if e.type.value in terminal_events
                    ]
                    assert finals, snap.record.id
                    # the final event is the terminal one and matches state;
                    # earlier FAILED/TIMED_OUT events are legitimate retries
                    last = snap.events[-1]
                    assert last.type.value == snap.record.state.value, (
                        snap.record.id,
                        last.type,
                        snap.record.state,
                    )
            await asyncio.sleep(0)

    reader_task = asyncio.create_task(reader())
    await asyncio.gather(
        *[
            orch.trigger(
                "noisy",
                task_id=f"n{i}",
                max_attempts=2,
                backoff={"base_delay": 0.001},
            )
            for i in range(30)
        ]
    )
    await asyncio.gather(*[wait_state(orch, f"n{i}") for i in range(30)])
    stop["v"] = True
    await reader_task


async def test_concurrent_trigger_and_cancel_same_task_is_terminal(orch):
    from .helpers import Recorder

    rec = Recorder()
    orch.register_handler("gated", rec.make_handler(wait_for_gate=True))

    async def trigger():
        await orch.trigger("gated", task_id="only")

    async def cancel():
        await asyncio.sleep(0.02)
        try:
            await orch.cancel("only")
        except Exception:
            pass

    await asyncio.gather(trigger(), cancel())
    # regardless of interleaving the task must end in exactly one terminal state
    await asyncio.sleep(0.05)
    snap = await orch.get_status("only")
    assert snap.record.state in TERMINAL_STATES
    if snap.record.state is TaskState.RUNNING:
        rec.open("only")
        await wait_state(orch, "only")
