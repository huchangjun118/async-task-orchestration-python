"""Task persistence interface and in-memory implementation."""
from __future__ import annotations

import asyncio
import copy
import time
from typing import Any, Callable

from app.models import (
    DuplicateTaskError,
    EventType,
    TaskEvent,
    TaskRecord,
    TaskSnapshot,
    TaskState,
)

MutateFn = Callable[[TaskRecord], None]


class TaskStore:
    """Persistence boundary for task records and the event log.

    All state mutations go through compare-and-set style operations so the
    state machine can never transition twice (which is what makes retries,
    duplicate triggers and crash recovery safe).
    """

    async def aclose(self) -> None: ...

    async def create_task(self, record: TaskRecord) -> TaskRecord: ...

    async def get_record(self, task_id: str) -> TaskRecord | None: ...

    async def get_snapshot(self, task_id: str) -> TaskSnapshot | None: ...

    async def list_snapshots(
        self, limit: int = 100, offset: int = 0
    ) -> list[TaskSnapshot]: ...

    async def list_by_idempotency_key(self, key: str) -> TaskRecord | None: ...

    async def all_records(self) -> list[TaskRecord]: ...

    async def compare_and_set_state(
        self,
        task_id: str,
        expected: tuple[TaskState, int] | None,
        target: TaskState,
        event: EventType,
        *,
        mutate: MutateFn | None = None,
        detail: str | None = None,
        payload: Any = None,
        attempt: int | None = None,
    ) -> TaskRecord | None:
        """Atomically transition state if (state, run_token) still match.

        Returns the updated record, or None if the precondition failed.
        Any field changes performed by ``mutate(record)`` are persisted
        together with the appended event in the same atomic operation.
        """
        ...

    async def append_event(
        self,
        task_id: str,
        event: EventType,
        *,
        detail: str | None = None,
        payload: Any = None,
        attempt: int | None = None,
    ) -> TaskEvent: ...


class InMemoryTaskStore:
    """Reference in-process implementation.

    A single asyncio lock guards every read-modify-write; snapshots are deep
    copied under that lock, so every returned snapshot is a consistent view
    even while workers keep mutating records.
    """

    def __init__(self, clock: Callable[[], float] = time.time):
        self._lock = asyncio.Lock()
        self._records: dict[str, TaskRecord] = {}
        self._events: dict[str, list[TaskEvent]] = {}
        self._idempotency: dict[str, str] = {}
        self._seq_counter = 0
        self._event_id_counter = 0
        self._clock = clock

    async def aclose(self) -> None:
        return None

    async def create_task(self, record: TaskRecord) -> TaskRecord:
        async with self._lock:
            if record.id in self._records:
                raise DuplicateTaskError(
                    f"task {record.id!r} already exists", record.id
                )
            key = record.spec.idempotency_key
            if key is not None:
                existing = self._idempotency.get(key)
                if existing is not None:
                    raise DuplicateTaskError(
                        f"idempotency key {key!r} already used by {existing!r}",
                        existing,
                    )
                self._idempotency[key] = record.id
            self._seq_counter += 1
            record.seq = self._seq_counter
            self._records[record.id] = record
            self._events[record.id] = []
            self._append_event_locked(
                record, EventType.CREATED, detail=f"handler={record.spec.handler}"
            )
            return copy.deepcopy(record)

    async def get_record(self, task_id: str) -> TaskRecord | None:
        async with self._lock:
            record = self._records.get(task_id)
            return copy.deepcopy(record) if record else None

    async def get_snapshot(self, task_id: str) -> TaskSnapshot | None:
        async with self._lock:
            record = self._records.get(task_id)
            if record is None:
                return None
            return TaskSnapshot(
                record=copy.deepcopy(record),
                events=tuple(copy.deepcopy(self._events[task_id])),
            )

    async def list_snapshots(
        self, limit: int = 100, offset: int = 0
    ) -> list[TaskSnapshot]:
        async with self._lock:
            ordered = sorted(self._records.values(), key=lambda r: r.seq)
            page = ordered[offset : offset + limit]
            return [
                TaskSnapshot(
                    record=copy.deepcopy(r),
                    events=tuple(copy.deepcopy(self._events[r.id])),
                )
                for r in page
            ]

    async def list_by_idempotency_key(self, key: str) -> TaskRecord | None:
        async with self._lock:
            task_id = self._idempotency.get(key)
            if task_id is None:
                return None
            return copy.deepcopy(self._records[task_id])

    async def all_records(self) -> list[TaskRecord]:
        async with self._lock:
            return [
                copy.deepcopy(r)
                for r in sorted(self._records.values(), key=lambda r: r.seq)
            ]

    async def compare_and_set_state(
        self,
        task_id: str,
        expected: tuple[TaskState, int] | None,
        target: TaskState,
        event: EventType,
        *,
        mutate: MutateFn | None = None,
        detail: str | None = None,
        payload: Any = None,
        attempt: int | None = None,
    ) -> TaskRecord | None:
        async with self._lock:
            record = self._records.get(task_id)
            if record is None:
                return None
            if expected is not None and (record.state, record.run_token) != expected:
                return None
            if mutate is not None:
                mutate(record)
            record.state = target
            record.updated_at = self._clock()
            self._append_event_locked(
                record, event, detail=detail, payload=payload, attempt=attempt
            )
            return copy.deepcopy(record)

    async def append_event(
        self,
        task_id: str,
        event: EventType,
        *,
        detail: str | None = None,
        payload: Any = None,
        attempt: int | None = None,
    ) -> TaskEvent | None:
        async with self._lock:
            record = self._records.get(task_id)
            if record is None:
                return None
            stored = self._append_event_locked(
                record, event, detail=detail, payload=payload, attempt=attempt
            )
            return copy.deepcopy(stored)

    # ------------------------------------------------------------------ #
    # helpers (call with lock held)
    # ------------------------------------------------------------------ #
    def _append_event_locked(
        self,
        record: TaskRecord,
        event: EventType,
        *,
        detail: str | None = None,
        payload: Any = None,
        attempt: int | None = None,
    ) -> TaskEvent:
        per_task_events = self._events[record.id]
        self._event_id_counter += 1
        stored = TaskEvent(
            id=self._event_id_counter,
            task_id=record.id,
            seq=len(per_task_events) + 1,
            type=event,
            timestamp=self._clock(),
            attempt=attempt,
            detail=detail,
            payload=copy.deepcopy(payload) if payload is not None else None,
        )
        per_task_events.append(stored)
        return stored
