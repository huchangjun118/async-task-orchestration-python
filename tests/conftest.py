"""Shared pytest fixtures and helpers."""
from __future__ import annotations

import asyncio
import time
from typing import Iterable

import pytest_asyncio

from app.models import TaskState, TERMINAL_STATES
from app.orchestrator import Orchestrator
from app.store import InMemoryTaskStore


@pytest_asyncio.fixture
async def orch():
    """A started in-memory orchestrator; auto-stopped after the test."""
    store = InMemoryTaskStore(clock=time.time)
    orchestrator = Orchestrator(store=store, max_concurrency=8)
    await orchestrator.start()
    yield orchestrator
    await orchestrator.stop()


@pytest_asyncio.fixture
async def concurrency_1():
    store = InMemoryTaskStore(clock=time.time)
    orchestrator = Orchestrator(store=store, max_concurrency=1)
    await orchestrator.start()
    yield orchestrator
    await orchestrator.stop()


async def wait_state(
    orch: Orchestrator,
    task_id: str,
    states: Iterable[TaskState] = TERMINAL_STATES,
    timeout: float = 5.0,
    poll: float = 0.005,
):
    """Poll until a task reaches one of ``states``; return its record."""
    wanted = set(states)
    deadline = time.time() + timeout
    while time.time() < deadline:
        snap = await orch.get_status(task_id)
        if snap.record.state in wanted:
            return snap.record
        await asyncio.sleep(poll)
    snap = await orch.get_status(task_id)
    raise AssertionError(
        f"task {task_id!r} never reached {[s.value for s in wanted]}; "
        f"stuck in {snap.record.state.value}"
    )
