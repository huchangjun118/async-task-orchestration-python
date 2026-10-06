"""Concurrency controls: priority queue, max concurrency, mutex groups."""
from __future__ import annotations

import asyncio
import heapq


class ConcurrencyLimiter:
    """Bounded async slot limiter."""

    def __init__(self, max_concurrency: int):
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be >= 1")
        self._slots = asyncio.Semaphore(max_concurrency)
        self._max = max_concurrency

    async def acquire(self) -> None:
        await self._slots.acquire()

    def release(self) -> None:
        self._slots.release()

    @property
    def capacity(self) -> int:
        return self._max


class SchedulerQueue:
    """Stable priority queue with tie-breaking by creation sequence.

    Ordering key: ``(-priority, seq)``. ``seq`` is the store-wide monotonic
    creation sequence, so equal-priority tasks always leave the queue in the
    exact order they were created -- including after a process restart, when
    seqs are rebuilt from persistence.
    """

    def __init__(self) -> None:
        self._heap: list[tuple[int, int, str]] = []
        self._entries: dict[str, tuple[int, int, str]] = {}

    def add(self, task_id: str, priority: int, seq: int) -> None:
        if task_id in self._entries:
            self.remove(task_id)
        entry = (-priority, seq, task_id)
        self._entries[task_id] = entry
        heapq.heappush(self._heap, entry)

    def remove(self, task_id: str) -> None:
        # Lazy deletion: drop the index entry; stale heap entries are skipped
        # on pop. This keeps removal O(log n) amortised.
        self._entries.pop(task_id, None)

    def _purge_stale(self) -> None:
        while self._heap and self._entries.get(self._heap[0][2]) is not self._heap[0]:
            heapq.heappop(self._heap)

    def peek_ordered(self) -> list[tuple[str, int, int]]:
        """All queued tasks in scheduling order: (task_id, priority, seq)."""
        live = [
            (task_id, -neg_pri, seq)
            for neg_pri, seq, task_id in self._entries.values()
        ]
        live.sort(key=lambda x: (-x[1], x[2]))
        return live

    def pop_ready(self, occupied_mutex_groups: set[str]) -> tuple[str, int] | None:
        """Pop the highest-priority runnable task.

        A task is runnable when its mutex group (if any) is not occupied by a
        currently running task. Tasks blocked only by mutex stay queued and do
        not block tasks behind them.
        """
        self._purge_stale()
        deferred: list[tuple[int, int, str]] = []
        chosen: tuple[int, int, str] | None = None
        while self._heap:
            entry = heapq.heappop(self._heap)
            neg_pri, _seq, task_id = entry
            if self._entries.get(task_id) is not entry:
                continue  # stale (removed / re-added)
            group: str | None = getattr(self, "_groups", {}).get(task_id)
            if group is not None and group in occupied_mutex_groups:
                deferred.append(entry)
                continue
            chosen = entry
            self._entries.pop(task_id, None)
            break
        for entry in deferred:
            heapq.heappush(self._heap, entry)
        if chosen is None:
            return None
        _neg_pri, seq, task_id = chosen
        return task_id, seq

    def add_grouped(
        self, task_id: str, priority: int, seq: int, group: str | None
    ) -> None:
        if not hasattr(self, "_groups"):
            self._groups = {}
        if group is not None:
            self._groups[task_id] = group
        self.add(task_id, priority, seq)

    def clear_group(self, task_id: str) -> None:
        if hasattr(self, "_groups"):
            self._groups.pop(task_id, None)

    def __len__(self) -> int:
        return len(self._entries)

    def __contains__(self, task_id: object) -> bool:
        return task_id in self._entries
