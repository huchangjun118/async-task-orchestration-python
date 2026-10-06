"""Built-in task handlers used by the demo, tests and manual runs."""
from __future__ import annotations

import asyncio

from app.models import TaskContext


async def noop(ctx: TaskContext) -> dict:
    """Succeeds immediately, echoing the payload."""
    return {"payload": ctx.payload, "attempt": ctx.attempt}


async def fail(ctx: TaskContext) -> None:
    """Always raises so retry/skip/cancel behaviour can be exercised."""
    raise RuntimeError(str(ctx.payload) if ctx.payload is not None else "intentional failure")


async def flaky(ctx: TaskContext) -> dict:
    """Fails for the first ``fail_times`` attempts, then succeeds.

    Payload: {"fail_times": int} (default 1).
    """
    payload = ctx.payload or {}
    fail_times = int(payload.get("fail_times", 1))
    if ctx.attempt <= fail_times:
        raise RuntimeError(f"flaky failure on attempt {ctx.attempt}")
    return {"attempt": ctx.attempt, "payload": payload}


async def sleep(ctx: TaskContext) -> dict:
    """Sleeps for ``seconds`` from payload and is cooperatively cancel-aware."""
    payload = ctx.payload or {}
    seconds = float(payload.get("seconds", 1.0))
    cancel_event = ctx.cancel_event()  # type: ignore[attr-defined]
    try:
        await asyncio.wait_for(cancel_event.wait(), timeout=seconds)
        raise asyncio.CancelledError("cancelled during sleep")
    except (asyncio.TimeoutError, TimeoutError):
        return {"slept": seconds}
