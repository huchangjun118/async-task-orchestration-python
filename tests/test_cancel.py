"""Cancellation semantics and timeout-vs-failure separation."""
from __future__ import annotations

import asyncio

from app.models import EventType, TaskState

from .conftest import wait_state
from .helpers import Recorder


async def test_cancel_queued_task(concurrency_1):
    orch = concurrency_1
    rec = Recorder()
    orch.register_handler("gated", rec.make_handler(wait_for_gate=True))
    await orch.trigger("gated", task_id="holder")
    await asyncio.sleep(0.05)
    await orch.trigger("gated", task_id="queued")
    await asyncio.sleep(0.05)
    assert (await orch.get_status("queued")).record.state is TaskState.READY
    changed = await orch.cancel("queued", reason="no longer needed")
    assert changed is True
    snap = await orch.get_status("queued")
    assert snap.record.state is TaskState.CANCELLED
    assert snap.record.cancel_reason == "no longer needed"
    assert snap.record.attempts == []  # never ran
    rec.open("holder")
    await wait_state(orch, "holder")
    assert "queued" not in rec.started  # never executed


async def test_cancel_running_task_stops_handler_and_writes_no_result(orch):
    side_effects: list[str] = []

    async def careful(ctx):
        try:
            await asyncio.sleep(5)
            side_effects.append("result-after-sleep")  # must never happen
            return {"bad": True}
        except asyncio.CancelledError:
            # handler is allowed to run cleanup, but must not write a result
            side_effects.append("cleanup-only")
            raise

    orch.register_handler("careful", careful)
    await orch.trigger("careful", task_id="t")
    await asyncio.sleep(0.1)
    assert (await orch.get_status("t")).record.state is TaskState.RUNNING
    changed = await orch.cancel("t")
    assert changed is True
    await asyncio.sleep(0.1)
    snap = await orch.get_status("t")
    assert snap.record.state is TaskState.CANCELLED
    assert snap.record.result is None
    types = [e.type for e in snap.events]
    assert EventType.CANCEL_REQUESTED in types
    assert EventType.CANCELLED in types
    assert EventType.SUCCEEDED not in types
    # stays cancelled (handler completion after cancel cannot overwrite)
    await asyncio.sleep(0.1)
    assert (await orch.get_status("t")).record.state is TaskState.CANCELLED
    assert "result-after-sleep" not in side_effects


async def test_cancel_cooperative_sleep_handler(orch):
    # built-in sleep polls its cancel event
    await orch.trigger("sleep", task_id="t", payload={"seconds": 30})
    await asyncio.sleep(0.1)
    assert await orch.cancel("t")
    snap = await orch.get_status("t")
    assert snap.record.state is TaskState.CANCELLED


async def test_cancel_terminal_task_returns_false(orch):
    await orch.trigger("noop", task_id="done")
    await wait_state(orch, "done")
    assert await orch.cancel("done") is False
    snap = await orch.get_status("done")
    assert snap.record.state is TaskState.SUCCEEDED


async def test_cancel_propagates_to_waiting_successor(orch):
    rec = Recorder()
    orch.register_handler("gated", rec.make_handler(wait_for_gate=True))
    await orch.trigger("gated", task_id="p")
    await orch.trigger("gated", task_id="child", deps=["p"])
    await asyncio.sleep(0.1)
    await orch.cancel("p", reason="abort")
    rec.open("p")
    rec = await wait_state(orch, "child")
    # predecessor CANCELLED is a non-success terminal -> child skipped
    assert rec.state is TaskState.SKIPPED
    assert "p" in (rec.skip_reason or "")


async def test_terminal_states_are_all_distinguishable(orch):
    async def slow(ctx):
        await asyncio.sleep(2)

    orch.register_handler("slow", slow)

    await orch.trigger("fail", task_id="f")
    await orch.trigger("slow", task_id="to", timeout=0.05)
    f = await wait_state(orch, "f")
    to = await wait_state(orch, "to")
    assert f.state is TaskState.FAILED
    assert to.state is TaskState.TIMED_OUT
    assert f.error_type != to.error_type or f.state != to.state
    assert to.attempts[-1].error_type in ("TimeoutError",)
