"""In-memory scheduling: priority queue, concurrency limit, mutex groups."""
from __future__ import annotations

import asyncio
import heapq
import itertools
from typing import Optional

from .models import TaskDefinition


class Scheduler:
    """Decides which READY instance runs next.

    Ordering: higher priority first; ties are broken by a monotonic
    sequence number (FIFO within a priority), so the queue order is
    stable and reproducible. Instances whose mutex group is currently
    occupied stay queued until the group is free.
    """

    def __init__(self, max_concurrency: int = 4) -> None:
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be >= 1")
        self._max_concurrency = max_concurrency
        self._counter = itertools.count()
        self._queue: list[tuple[int, int, str]] = []  # (-priority, seq, instance_id)
        self._queued: set[str] = set()
        self._running: set[str] = set()
        self._running_groups: set[str] = set()
        self._groups: dict[str, Optional[str]] = {}
        self._wakeup = asyncio.Event()

    def submit(self, instance_id: str, definition: TaskDefinition) -> None:
        if instance_id in self._queued or instance_id in self._running:
            return
        seq = next(self._counter)
        heapq.heappush(self._queue, (-definition.priority, seq, instance_id))
        self._queued.add(instance_id)
        self._groups[instance_id] = definition.mutex_group
        self._wakeup.set()

    def try_acquire(self) -> Optional[str]:
        """Pop the next runnable instance id, or None if none may run now."""
        if len(self._running) >= self._max_concurrency:
            return None
        blocked: list[tuple[int, int, str]] = []
        chosen: Optional[str] = None
        while self._queue:
            entry = heapq.heappop(self._queue)
            _, _, instance_id = entry
            if instance_id not in self._queued:
                continue  # stale entry (removed while queued)
            group = self._groups.get(instance_id)
            if group is not None and group in self._running_groups:
                blocked.append(entry)
                continue
            chosen = instance_id
            break
        for entry in blocked:
            heapq.heappush(self._queue, entry)
        if chosen is None:
            return None
        self._queued.discard(chosen)
        self._running.add(chosen)
        group = self._groups.get(chosen)
        if group is not None:
            self._running_groups.add(group)
        return chosen

    def release(self, instance_id: str) -> None:
        if instance_id not in self._running:
            return
        self._running.discard(instance_id)
        group = self._groups.pop(instance_id, None)
        if group is not None:
            self._running_groups.discard(group)
        self._wakeup.set()

    def remove(self, instance_id: str) -> None:
        """Drop a queued instance (e.g. cancelled before dispatch)."""
        self._queued.discard(instance_id)
        self._groups.pop(instance_id, None)

    def has_capacity(self) -> bool:
        return len(self._running) < self._max_concurrency

    async def wait_for_work(self) -> None:
        await self._wakeup.wait()
        self._wakeup.clear()

    @property
    def running_count(self) -> int:
        return len(self._running)

    @property
    def queued_count(self) -> int:
        return len(self._queued)
