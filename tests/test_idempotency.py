"""Duplicate triggers and retry safety: same result, no duplicate side effects."""
from __future__ import annotations

import asyncio

from app.models import TaskState

from .conftest import wait_state


async def test_same_idempotency_key_returns_same_task(orch):
    spec1 = await orch.trigger("noop", idempotency_key="user-123")
    spec2 = await orch.trigger("noop", idempotency_key="user-123")
    assert spec1.id == spec2.id
    tasks = await orch.list_tasks()
    matches = [t for t in tasks if t.record.spec.idempotency_key == "user-123"]
    assert len(matches) == 1


async def test_concurrent_duplicate_triggers_create_one_task(orch):
    results = await asyncio.gather(
        *[
            orch.trigger("noop", idempotency_key="race-key")
            for _ in range(10)
        ]
    )
    ids = {s.id for s in results}
    assert len(ids) == 1
    tasks = await orch.list_tasks()
    assert sum(1 for t in tasks if t.record.spec.idempotency_key == "race-key") == 1


async def test_duplicate_after_completion_returns_original_result(orch):
    await orch.trigger(
        "flaky",
        payload={"fail_times": 2},
        max_attempts=3,
        backoff={"base_delay": 0.01},
        idempotency_key="once",
    )
    # find the generated id
    rec = (await orch.list_tasks())[0]
    task_id = rec.record.id
    await wait_state(orch, task_id)
    snap1 = await orch.get_status(task_id)
    # trigger again with same key: same id, state, result and event history
    spec2 = await orch.trigger("flaky", idempotency_key="once")
    assert spec2.id == task_id
    snap2 = await orch.get_status(task_id)
    assert snap2.record.state is TaskState.SUCCEEDED
    assert snap2.record.result == snap1.record.result
    assert [e.type for e in snap2.events] == [e.type for e in snap1.events]
    assert len(snap2.record.attempts) == 3  # not re-executed


async def test_retry_runs_handler_again_but_final_result_singular(orch):
    invocations: list[int] = []

    async def counting(ctx):
        invocations.append(ctx.attempt)
        if ctx.attempt < 2:
            raise RuntimeError("first fails")
        return "final"

    orch.register_handler("counting", counting)
    await orch.trigger(
        "counting", max_attempts=2, backoff={"base_delay": 0.01}, task_id="t"
    )
    rec = await wait_state(orch, "t")
    assert invocations == [1, 2]
    assert rec.state is TaskState.SUCCEEDED and rec.result == "final"
    # exactly one terminal result event
    hist = await orch.get_history("t")
    assert [e.type.value for e in hist].count("SUCCEEDED") == 1


async def test_attempt_token_is_unique_per_attempt(orch):
    seen: list[str] = []

    async def tok(ctx):
        seen.append(ctx.attempt_token)  # type: ignore[attr-defined]
        if ctx.attempt < 2:
            raise RuntimeError("x")
        return None

    orch.register_handler("tok", tok)
    await orch.trigger(
        "tok", max_attempts=2, backoff={"base_delay": 0.01}, task_id="t"
    )
    await wait_state(orch, "t")
    assert len(seen) == 2 and len(set(seen)) == 2
    assert all(token.startswith("t:") for token in seen)
