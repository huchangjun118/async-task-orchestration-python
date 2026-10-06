"""Retry counts, exponential backoff and timeout vs. failure distinction."""
from __future__ import annotations

import asyncio

from app.models import EventType, TaskState

from .conftest import wait_state


async def test_flaky_retries_until_success(orch):
    await orch.trigger(
        "flaky",
        task_id="t",
        payload={"fail_times": 2},
        max_attempts=4,
        backoff={"base_delay": 0.01, "multiplier": 2},
    )
    rec = await wait_state(orch, "t")
    assert rec.state is TaskState.SUCCEEDED
    assert len(rec.attempts) == 3
    assert rec.result == {"attempt": 3, "payload": {"fail_times": 2}}
    hist = await orch.get_history("t")
    types = [e.type for e in hist]
    assert types.count(EventType.FAILED) == 2
    assert types.count(EventType.RETRY_SCHEDULED) == 2
    assert types.count(EventType.STARTED) == 3


async def test_exhausted_retries_fails(orch):
    await orch.trigger(
        "fail",
        task_id="t",
        max_attempts=3,
        backoff={"base_delay": 0.005},
    )
    rec = await wait_state(orch, "t")
    assert rec.state is TaskState.FAILED
    assert len(rec.attempts) == 3
    assert rec.error_type == "RuntimeError"
    assert "intentional failure" in (rec.error or "")


async def test_backoff_delays_are_observed(orch):
    # A handler records attempt start times via shared list in payload-like state
    starts: list[float] = []

    async def timed(ctx):
        import time as _t

        starts.append(_t.monotonic())
        if ctx.attempt < 3:
            raise RuntimeError("again")
        return "ok"

    orch.register_handler("timed", timed)
    await orch.trigger(
        "timed",
        task_id="t",
        max_attempts=3,
        backoff={"base_delay": 0.05, "multiplier": 2, "max_delay": 1},
    )
    rec = await wait_state(orch, "t")
    assert rec.state is TaskState.SUCCEEDED
    assert len(starts) == 3
    # delay before attempt2 ~ 0.05, before attempt3 ~ 0.1 (with tolerance)
    assert starts[1] - starts[0] >= 0.045
    assert starts[2] - starts[1] >= 0.09


async def test_timeout_terminates_as_timed_out_not_failed(orch):
    async def slow(ctx):
        await asyncio.sleep(2)
        return "nope"

    orch.register_handler("slow", slow)
    await orch.trigger("slow", task_id="t", timeout=0.05, max_attempts=1)
    rec = await wait_state(orch, "t")
    assert rec.state is TaskState.TIMED_OUT
    assert rec.attempts[-1].state == TaskState.TIMED_OUT.value
    hist = await orch.get_history("t")
    assert hist[-1].type is EventType.TIMED_OUT


async def test_timeout_can_retry_and_then_succeed(orch):
    calls = {"n": 0}

    async def sometimes_slow(ctx):
        calls["n"] += 1
        if calls["n"] == 1:
            await asyncio.sleep(1)
        return {"attempt": ctx.attempt}

    orch.register_handler("sometimes_slow", sometimes_slow)
    await orch.trigger(
        "sometimes_slow",
        task_id="t",
        timeout=0.05,
        max_attempts=2,
        backoff={"base_delay": 0.01},
    )
    rec = await wait_state(orch, "t")
    assert rec.state is TaskState.SUCCEEDED
    assert len(rec.attempts) == 2
    assert rec.attempts[0].state == TaskState.TIMED_OUT.value
    assert rec.attempts[1].state == TaskState.SUCCEEDED.value
    types = [e.type for e in await orch.get_history("t")]
    assert EventType.TIMED_OUT in types and EventType.SUCCEEDED in types


async def test_default_no_retry_single_attempt(orch):
    await orch.trigger("fail", task_id="t")
    rec = await wait_state(orch, "t")
    assert rec.state is TaskState.FAILED
    assert len(rec.attempts) == 1
