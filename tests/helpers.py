"""Custom handlers for scheduling/cancellation tests."""
from __future__ import annotations

import asyncio
import itertools
from typing import Any


class Recorder:
    """Tracks handler invocations: start order, concurrency, gate events."""

    def __init__(self) -> None:
        self.started: list[str] = []
        self.finished: list[str] = []
        self.active = 0
        self.max_active = 0
        self._lock = asyncio.Lock()
        self.gates: dict[str, asyncio.Event] = {}
        self._counter = itertools.count(1)

    def gate(self, task_id: str) -> asyncio.Event:
        ev = self.gates.get(task_id)
        if ev is None:
            ev = asyncio.Event()
            self.gates[task_id] = ev
        return ev

    def make_handler(self, *, wait_for_gate: bool = False):
        recorder = self

        async def handler(ctx) -> dict[str, Any]:
            async with recorder._lock:
                recorder.started.append(ctx.task_id)
                recorder.active += 1
                recorder.max_active = max(recorder.max_active, recorder.active)
            try:
                if wait_for_gate:
                    gate = recorder.gate(ctx.task_id)
                    try:
                        await asyncio.wait_for(gate.wait(), timeout=10)
                    except (asyncio.TimeoutError, TimeoutError):
                        pass
                return {"attempt": ctx.attempt}
            finally:
                async with recorder._lock:
                    recorder.active -= 1
                    recorder.finished.append(ctx.task_id)

        return handler

    def open(self, task_id: str) -> None:
        self.gate(task_id).set()
