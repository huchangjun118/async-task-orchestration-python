"""Public orchestration facade: validation, trigger, query, cancel, history."""
from __future__ import annotations

import time
import uuid
from typing import Any, Callable

from app.engine import TaskExecutor
from app.handlers import fail as _fail_handler
from app.handlers import flaky as _flaky_handler
from app.handlers import noop as _noop_handler
from app.handlers import sleep as _sleep_handler
from app.models import (
    BackoffConfig,
    DepFailPolicy,
    DuplicateTaskError,
    TaskEvent,
    TaskHandler,
    TaskNotFoundError,
    TaskRecord,
    TaskSnapshot,
    TaskSpec,
    TaskState,
)
from app.store import InMemoryTaskStore, TaskStore


def _coerce_backoff(value: BackoffConfig | dict | None) -> BackoffConfig | None:
    if value is None or isinstance(value, BackoffConfig):
        return value
    if isinstance(value, dict):
        return BackoffConfig(
            max_attempts=int(value.get("max_attempts", 1)),
            base_delay=float(value.get("base_delay", 1.0)),
            multiplier=float(value.get("multiplier", 2.0)),
            max_delay=float(value.get("max_delay", 60.0)),
        )
    raise TypeError(f"unsupported backoff value: {value!r}")


def _backoff_from(
    max_attempts: int, backoff: BackoffConfig | dict | None
) -> BackoffConfig:
    backoff = _coerce_backoff(backoff)
    if backoff is not None:
        if max_attempts != backoff.max_attempts:
            return BackoffConfig(
                max_attempts=max_attempts,
                base_delay=backoff.base_delay,
                multiplier=backoff.multiplier,
                max_delay=backoff.max_delay,
            )
        return backoff
    return BackoffConfig(max_attempts=max_attempts)


def topo_sort(specs: dict[str, TaskSpec]) -> list[TaskSpec]:
    """Kahn's algorithm over the given specs; raises on cycles."""
    from app.models import DependencyCycleError

    indeg = {sid: 0 for sid in specs}
    adj: dict[str, list[str]] = {sid: [] for sid in specs}
    for spec in specs.values():
        for dep in spec.deps:
            if dep in specs:
                indeg[spec.id] += 1
                adj[dep].append(spec.id)
    # deterministic order: creation order via sorted ids not needed; use
    # insertion order through a list with stable pops
    ready_ids = [sid for sid, d in indeg.items() if d == 0]
    ordered: list[TaskSpec] = []
    while ready_ids:
        sid = ready_ids.pop(0)
        ordered.append(specs[sid])
        for nxt in adj[sid]:
            indeg[nxt] -= 1
            if indeg[nxt] == 0:
                ready_ids.append(nxt)
    if len(ordered) != len(specs):
        cyclic = [sid for sid, d in indeg.items() if d > 0]
        raise DependencyCycleError(
            f"dependency cycle detected involving: {sorted(cyclic)}"
        )
    return ordered


class Orchestrator:
    """Application-level API over the store and the execution engine."""

    def __init__(
        self,
        store: TaskStore | None = None,
        *,
        max_concurrency: int = 4,
        clock: Callable[[], float] = time.time,
    ):
        self._clock = clock
        self._store: TaskStore = store or InMemoryTaskStore(clock=clock)
        self._handlers: dict[str, TaskHandler] = {}
        self._engine = TaskExecutor(
            self._store,
            self._handlers,
            max_concurrency=max_concurrency,
            clock=clock,
        )
        self._started = False
        # built-in handlers
        self.register_handler("noop", _noop_handler)
        self.register_handler("fail", _fail_handler)
        self.register_handler("flaky", _flaky_handler)
        self.register_handler("sleep", _sleep_handler)

    def register_handler(self, name: str, handler: TaskHandler) -> None:
        self._handlers[name] = handler

    async def start(self) -> None:
        """Start engine and run crash recovery.

        Safe to interleave with triggers: both only use atomic store
        operations, and the scheduler repeatedly re-evaluates everything.
        """
        if self._started:
            return
        await self._engine.start()
        await self._engine.recover()
        self._started = True

    async def stop(self) -> None:
        if not self._started:
            return
        await self._engine.stop()
        self._started = False
        await self._store.aclose()

    async def __aenter__(self) -> "Orchestrator":
        await self.start()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.stop()

    # ------------------------------------------------------------------ #
    # triggering
    # ------------------------------------------------------------------ #
    async def trigger(
        self,
        handler: str,
        *,
        payload: Any = None,
        task_id: str | None = None,
        deps: list[str] | None = None,
        priority: int = 0,
        mutex_group: str | None = None,
        dep_fail_policy: DepFailPolicy | str = DepFailPolicy.SKIP,
        max_attempts: int = 1,
        timeout: float | None = None,
        backoff: BackoffConfig | None = None,
        idempotency_key: str | None = None,
    ) -> TaskSpec:
        from app.models import UnknownDependencyError

        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        policy = (
            dep_fail_policy
            if isinstance(dep_fail_policy, DepFailPolicy)
            else DepFailPolicy(dep_fail_policy)
        )
        tid = task_id or uuid.uuid4().hex
        if idempotency_key is not None:
            existing = await self._store.list_by_idempotency_key(idempotency_key)
            if existing is not None:
                # Duplicate trigger: return the original spec unchanged.
                return existing.spec
        dep_set = frozenset(deps or ())
        for dep_id in dep_set:
            dep = await self._store.get_record(dep_id)
            if dep is None:
                raise UnknownDependencyError(
                    f"dependency {dep_id!r} does not exist"
                )
        spec = TaskSpec(
            id=tid,
            handler=handler,
            deps=dep_set,
            payload=payload,
            priority=priority,
            mutex_group=mutex_group,
            dep_fail_policy=policy,
            max_attempts=max_attempts,
            timeout=timeout,
            backoff=_backoff_from(max_attempts, backoff),
            idempotency_key=idempotency_key,
        )
        now = self._clock()
        record = TaskRecord(
            id=tid,
            spec=spec,
            state=TaskState.PENDING,
            created_at=now,
            updated_at=now,
        )
        try:
            await self._store.create_task(record)
        except DuplicateTaskError as exc:
            # Same explicit task id created concurrently: return the existing.
            existing = await self._store.get_record(exc.existing_task_id or tid)
            if existing is not None:
                return existing.spec
            raise
        self._engine.notify()
        return spec

    async def trigger_workflow(self, specs: list[dict]) -> dict[str, TaskSpec]:
        """Trigger many tasks together; deps may reference ids in the batch.

        The batch plus the existing persisted tasks is validated as one DAG
        (no cycles, no unknown deps). On any validation error nothing is
        created. Returns id -> spec for every newly created task.
        """
        from app.models import UnknownDependencyError

        if not specs:
            return {}

        new_specs: dict[str, TaskSpec] = {}
        for raw in specs:
            tid = raw.get("task_id") or raw.get("id") or uuid.uuid4().hex
            if tid in new_specs:
                raise DuplicateTaskError(f"duplicate task id {tid!r} in workflow", tid)
            policy_raw = raw.get("dep_fail_policy", DepFailPolicy.SKIP)
            policy = (
                policy_raw
                if isinstance(policy_raw, DepFailPolicy)
                else DepFailPolicy(policy_raw)
            )
            max_attempts = int(raw.get("max_attempts", 1))
            if max_attempts < 1:
                raise ValueError("max_attempts must be >= 1")
            new_specs[tid] = TaskSpec(
                id=tid,
                handler=raw["handler"],
                deps=frozenset(raw.get("deps", ())),
                payload=raw.get("payload"),
                priority=int(raw.get("priority", 0)),
                mutex_group=raw.get("mutex_group"),
                dep_fail_policy=policy,
                max_attempts=max_attempts,
                timeout=raw.get("timeout"),
                backoff=_backoff_from(max_attempts, raw.get("backoff")),
                idempotency_key=raw.get("idempotency_key"),
            )

        # validate: unique ids/keys against the store, known deps, no cycles
        existing_records = {r.id: r for r in await self._store.all_records()}
        if any(tid in existing_records for tid in new_specs):
            clash = next(tid for tid in new_specs if tid in existing_records)
            raise DuplicateTaskError(f"task {clash!r} already exists", clash)
        for spec in new_specs.values():
            if spec.idempotency_key is not None:
                existing = await self._store.list_by_idempotency_key(
                    spec.idempotency_key
                )
                if existing is not None:
                    raise DuplicateTaskError(
                        f"idempotency key {spec.idempotency_key!r} already used",
                        existing.id,
                    )
            for dep_id in spec.deps:
                if dep_id not in new_specs and dep_id not in existing_records:
                    raise UnknownDependencyError(
                        f"dependency {dep_id!r} of {spec.id!r} does not exist"
                    )

        ordered = topo_sort(new_specs)
        now = self._clock()
        for spec in ordered:
            await self._store.create_task(
                TaskRecord(
                    id=spec.id,
                    spec=spec,
                    state=TaskState.PENDING,
                    created_at=now,
                    updated_at=now,
                )
            )
        self._engine.notify()
        return {spec.id: spec for spec in ordered}

    # ------------------------------------------------------------------ #
    # queries / cancel
    # ------------------------------------------------------------------ #
    async def get_status(self, task_id: str) -> TaskSnapshot:
        snapshot = await self._store.get_snapshot(task_id)
        if snapshot is None:
            raise TaskNotFoundError(f"task {task_id!r} not found")
        return snapshot

    async def list_tasks(
        self, limit: int = 100, offset: int = 0
    ) -> list[TaskSnapshot]:
        return await self._store.list_snapshots(limit=limit, offset=offset)

    async def get_history(self, task_id: str) -> list[TaskEvent]:
        snapshot = await self._store.get_snapshot(task_id)
        if snapshot is None:
            raise TaskNotFoundError(f"task {task_id!r} not found")
        return list(snapshot.events)

    async def cancel(self, task_id: str, reason: str = "user requested") -> bool:
        snapshot = await self._store.get_snapshot(task_id)
        if snapshot is None:
            raise TaskNotFoundError(f"task {task_id!r} not found")
        return await self._engine.request_cancel(task_id, reason)
