"""Execution engine: state machine, dependency propagation and worker loop."""
from __future__ import annotations

import asyncio
import time
import traceback
from typing import Any, Callable

from app.models import (
    DepFailPolicy,
    EventType,
    TaskAttempt,
    TaskHandler,
    TaskRecord,
    TaskState,
    TERMINAL_STATES,
)
from app.scheduler import ConcurrencyLimiter, SchedulerQueue
from app.store import TaskStore


class _TaskContext:
    """Handler-facing services (see ``TaskContext`` protocol in models)."""

    def __init__(
        self,
        engine: "TaskExecutor",
        record: TaskRecord,
        attempt: int,
        results: dict[str, Any],
    ):
        self._engine = engine
        self._record = record
        self.task_id = record.id
        self.attempt = attempt
        self.payload = record.spec.payload
        self.results = results
        # Idempotency token unique to *this* attempt; handlers talking to
        # external systems can use it as a dedupe key.
        self.attempt_token = f"{record.id}:{attempt}:{record.run_token}"

    async def cancelled(self) -> bool:
        return self._engine.is_cancel_requested(self._record.id)

    def cancel_event(self) -> asyncio.Event:
        return self._engine.cancel_event_for(self._record.id)


class TaskExecutor:
    """Owns the scheduling loop and runs handler attempts.

    State machine (all transitions are compare-and-set in the store)::

        PENDING ──deps ok──────────────► READY
        PENDING ──dep failed (policy)──► SKIPPED | CANCELLED
        READY ─────────────────────────► RUNNING
        RUNNING ──ok───────────────────► SUCCEEDED
        RUNNING ──error, retries left──► RETRY_WAIT ──backoff elapsed──► READY
        RUNNING ──error, exhausted─────► FAILED
        RUNNING ──timeout, retries left► RETRY_WAIT
        RUNNING ──timeout, exhausted───► TIMED_OUT
        any non-terminal ──user cancel► CANCELLED
    """

    TICK = 0.02

    def __init__(
        self,
        store: TaskStore,
        handlers: dict[str, TaskHandler],
        *,
        max_concurrency: int = 4,
        clock: Callable[[], float] = time.time,
    ):
        self._store = store
        self._handlers = handlers
        self._limiter = ConcurrencyLimiter(max_concurrency)
        self._queue = SchedulerQueue()
        self._clock = clock

        self._wake = asyncio.Event()
        self._stopped = asyncio.Event()
        self._loop_task: asyncio.Task | None = None

        self._running: set[str] = set()
        self._running_tasks: dict[str, asyncio.Task] = {}
        self._occupied_groups: set[str] = set()
        self._cancel_events: dict[str, asyncio.Event] = {}

    # ------------------------------------------------------------------ #
    # lifecycle
    # ------------------------------------------------------------------ #
    async def start(self) -> None:
        if self._loop_task is not None:
            return
        self._stopped.clear()
        self._loop_task = asyncio.create_task(self._scheduler_loop())

    async def stop(self) -> None:
        if self._loop_task is None:
            return
        self._stopped.set()
        self._wake.set()
        if self._running_tasks:
            await asyncio.gather(*self._running_tasks.values(), return_exceptions=True)
        await self._loop_task
        self._loop_task = None

    async def recover(self) -> None:
        """Re-evaluate non-terminal tasks after a process restart.

        Idempotent and safe to run concurrently with new triggers: it only
        uses compare-and-set transitions, so an already-terminal task is
        never touched and a task that moved concurrently is skipped.
        """
        records = await self._store.all_records()
        for record in records:
            if record.state in TERMINAL_STATES:
                continue
            await self._recover_one(record)
        self.notify()

    async def _recover_one(self, record: TaskRecord) -> None:
        if record.state is TaskState.RUNNING:
            # The handler died mid-attempt with the process. Record the
            # interrupted attempt; honour the normal retry policy so the
            # task resumes from the next attempt rather than the start.
            attempt_no = len(record.attempts)
            now = self._clock()
            has_retries = attempt_no < record.spec.max_attempts

            if has_retries:
                target = TaskState.RETRY_WAIT
                detail = (
                    f"attempt {attempt_no} interrupted by restart; "
                    f"retry {attempt_no + 1} scheduled"
                )

                def mutate_recover_retry(rec: TaskRecord) -> None:
                    if rec.attempts:
                        att = rec.attempts[-1]
                        att.state = TaskState.FAILED.value
                        att.finished_at = now
                        att.error = "attempt interrupted by process restart"
                        att.error_type = "InterruptedByRestart"
                    rec.not_before = now + rec.spec.backoff.delay_for(attempt_no + 1)

                mutate = mutate_recover_retry
            else:
                target = TaskState.FAILED
                detail = (
                    f"attempt {attempt_no} interrupted by restart; retries exhausted"
                )

                def mutate_recover_final(rec: TaskRecord) -> None:
                    if rec.attempts:
                        att = rec.attempts[-1]
                        att.state = TaskState.FAILED.value
                        att.finished_at = now
                        att.error = "attempt interrupted by process restart"
                        att.error_type = "InterruptedByRestart"
                    rec.error = "interrupted by process restart"
                    rec.error_type = "InterruptedByRestart"

                mutate = mutate_recover_final

            await self._store.compare_and_set_state(
                record.id,
                (TaskState.RUNNING, record.run_token),
                target,
                EventType.RECOVERED,
                mutate=mutate,
                detail=detail,
                attempt=attempt_no,
            )
        else:
            # PENDING / READY / RETRY_WAIT: nothing to undo; the scheduler
            # re-evaluates them. Leave an audit trail in the history.
            await self._store.append_event(
                record.id,
                EventType.RECOVERED,
                detail=f"resumed from state {record.state.value}",
            )

    def notify(self) -> None:
        self._wake.set()

    # ------------------------------------------------------------------ #
    # cancellation hooks used by handlers / facade
    # ------------------------------------------------------------------ #
    def cancel_event_for(self, task_id: str) -> asyncio.Event:
        ev = self._cancel_events.get(task_id)
        if ev is None:
            ev = asyncio.Event()
            self._cancel_events[task_id] = ev
        return ev

    def is_cancel_requested(self, task_id: str) -> bool:
        ev = self._cancel_events.get(task_id)
        return bool(ev and ev.is_set())

    async def request_cancel(self, task_id: str, reason: str) -> bool:
        """Cancel a non-terminal task. Returns True only if it ends CANCELLED.

        A terminal task cannot be cancelled. For a RUNNING task the state is
        flipped to CANCELLED first (so the racing handler can no longer write
        a result), then its asyncio task is cancelled.
        """
        record = await self._store.get_record(task_id)
        if record is None or record.state in TERMINAL_STATES:
            return False

        self.cancel_event_for(task_id).set()
        await self._store.append_event(
            task_id, EventType.CANCEL_REQUESTED, detail=reason
        )

        was_running = record.state is TaskState.RUNNING

        def mutate(rec: TaskRecord) -> None:
            rec.cancelled = True
            rec.cancel_reason = reason
            if was_running and rec.attempts:
                rec.attempts[-1].state = TaskState.CANCELLED.value
                rec.attempts[-1].finished_at = self._clock()

        updated = await self._store.compare_and_set_state(
            task_id,
            (record.state, record.run_token),
            TaskState.CANCELLED,
            EventType.CANCELLED,
            mutate=mutate,
            detail=reason,
            attempt=len(record.attempts) if was_running else None,
        )
        self.notify()
        if updated is None:
            # State moved between read and CAS (e.g. handler just finished).
            fresh = await self._store.get_record(task_id)
            return fresh is not None and fresh.state is TaskState.CANCELLED
        if was_running:
            handle = self._running_tasks.get(task_id)
            if handle is not None:
                handle.cancel()
        return True

    # ------------------------------------------------------------------ #
    # scheduler loop
    # ------------------------------------------------------------------ #
    async def _scheduler_loop(self) -> None:
        while not self._stopped.is_set():
            try:
                await self._dispatch_due()
            except Exception:  # pragma: no cover - defensive
                traceback.print_exc()
            self._wake.clear()
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self.TICK)
            except (asyncio.TimeoutError, TimeoutError):
                pass

    async def _dispatch_due(self) -> int:
        now = self._clock()
        records = await self._store.all_records()

        moved = 0
        for record in records:
            if record.state in TERMINAL_STATES:
                continue
            if record.id in self._running or record.id in self._queue:
                continue

            if record.state is TaskState.PENDING:
                if await self._evaluate(record):
                    moved += 1
                    record = await self._store.get_record(record.id)
                    if record is None or record.state is not TaskState.READY:
                        continue
            elif record.state is TaskState.RETRY_WAIT:
                if record.not_before > now:
                    continue
                updated = await self._store.compare_and_set_state(
                    record.id,
                    (TaskState.RETRY_WAIT, record.run_token),
                    TaskState.READY,
                    EventType.READY,
                    detail=f"backoff elapsed; attempt {len(record.attempts) + 1}",
                )
                if updated is None:
                    continue
                record = updated
                moved += 1

            if record.state is TaskState.READY:
                self._queue.add_grouped(
                    record.id,
                    record.spec.priority,
                    record.seq,
                    record.spec.mutex_group,
                )

        # launch as many READY tasks as slots/mutex allow
        launched = 0
        while len(self._running) < self._limiter.capacity:
            popped = self._queue.pop_ready(self._occupied_groups)
            if popped is None:
                break
            task_id, _seq = popped
            record = await self._store.get_record(task_id)
            if record is None or record.state is not TaskState.READY:
                continue  # vanished/cancelled while queued
            await self._limiter.acquire()
            token = record.run_token + 1
            attempt_no = len(record.attempts) + 1

            def mutate_start(rec: TaskRecord) -> None:
                rec.run_token = token
                rec.attempts.append(
                    TaskAttempt(
                        attempt=attempt_no,
                        state=TaskState.RUNNING.value,
                        started_at=self._clock(),
                    )
                )

            started = await self._store.compare_and_set_state(
                task_id,
                (TaskState.READY, record.run_token),
                TaskState.RUNNING,
                EventType.STARTED,
                mutate=mutate_start,
                attempt=attempt_no,
            )
            if started is None:
                # Cancelled (or otherwise moved) between queue pop and launch.
                self._limiter.release()
                continue

            self._running.add(task_id)
            if record.spec.mutex_group:
                self._occupied_groups.add(record.spec.mutex_group)
            self.cancel_event_for(task_id).clear()
            handle = asyncio.create_task(self._run_task(started))
            self._running_tasks[task_id] = handle
            launched += 1
        return moved + launched

    # ------------------------------------------------------------------ #
    # dependency evaluation
    # ------------------------------------------------------------------ #
    async def _evaluate(self, record: TaskRecord) -> bool:
        """Move a PENDING task forward based on predecessor states.

        Returns True if it became READY.
        """
        dep_results: dict[str, Any] = {}
        blocking: tuple[str, TaskState] | None = None
        for dep_id in sorted(record.spec.deps):
            dep = await self._store.get_record(dep_id)
            if dep is None:
                blocking = (dep_id, TaskState.FAILED)
                break
            if dep.state is TaskState.SUCCEEDED:
                dep_results[dep_id] = dep.result
            elif dep.state in TERMINAL_STATES:
                blocking = (dep_id, dep.state)
                break
            else:
                return False  # a predecessor is still in flight

        if blocking is not None:
            dep_id, dep_state = blocking
            dep_record = await self._store.get_record(dep_id)
            if dep_record is None:
                reason = f"predecessor {dep_id!r} does not exist"
            else:
                reason = f"predecessor {dep_id!r} ended as {dep_record.state.value}"
            policy = record.spec.dep_fail_policy
            target = (
                TaskState.SKIPPED if policy is DepFailPolicy.SKIP else TaskState.CANCELLED
            )

            def mutate_dep(rec: TaskRecord) -> None:
                if target is TaskState.SKIPPED:
                    rec.skip_reason = reason
                else:
                    rec.cancelled = True
                    rec.cancel_reason = reason

            await self._store.compare_and_set_state(
                record.id,
                (TaskState.PENDING, record.run_token),
                target,
                EventType.SKIPPED if target is TaskState.SKIPPED else EventType.CANCELLED,
                mutate=mutate_dep,
                detail=reason,
            )
            self.notify()
            return False

        def mutate_ready(rec: TaskRecord) -> None:
            rec.result = None

        updated = await self._store.compare_and_set_state(
            record.id,
            (TaskState.PENDING, record.run_token),
            TaskState.READY,
            EventType.READY,
            mutate=mutate_ready,
        )
        if updated is not None:
            self.notify()
            return True
        return False

    # ------------------------------------------------------------------ #
    # task execution
    # ------------------------------------------------------------------ #
    async def _run_task(self, record: TaskRecord) -> None:
        task_id = record.id
        group = record.spec.mutex_group
        try:
            token = record.run_token
            attempt_no = len(record.attempts)
            await self._execute_attempt(record, attempt_no, token)
        finally:
            self._running.discard(task_id)
            self._running_tasks.pop(task_id, None)
            if group:
                self._occupied_groups.discard(group)
            self._queue.clear_group(task_id)
            self._limiter.release()
            self.notify()

    async def _execute_attempt(
        self, record: TaskRecord, attempt_no: int, token: int
    ) -> None:
        task_id = record.id
        handler = self._handlers.get(record.spec.handler)

        if handler is None:
            # Misconfiguration: do not burn retry attempts.
            def mutate_no_handler(rec: TaskRecord) -> None:
                rec.error = f"no handler registered for {record.spec.handler!r}"
                rec.error_type = "HandlerNotFound"
                if rec.attempts:
                    rec.attempts[-1].state = TaskState.FAILED.value
                    rec.attempts[-1].finished_at = self._clock()
                    rec.attempts[-1].error = rec.error
                    rec.attempts[-1].error_type = rec.error_type

            await self._store.compare_and_set_state(
                task_id,
                (TaskState.RUNNING, token),
                TaskState.FAILED,
                EventType.FAILED,
                mutate=mutate_no_handler,
                detail=None,
            )
            await self._propagate_failure(task_id)
            return

        dep_results: dict[str, Any] = {}
        for dep_id in record.spec.deps:
            dep = await self._store.get_record(dep_id)
            if dep is not None:
                dep_results[dep_id] = dep.result
        ctx = _TaskContext(self, record, attempt_no, dep_results)

        result: Any = None
        outcome: str  # "ok" | "error" | "timeout" | "cancelled"
        error: BaseException | None = None
        try:
            coro = handler(ctx)
            if record.spec.timeout is not None:
                result = await asyncio.wait_for(coro, timeout=record.spec.timeout)
            else:
                result = await coro
            outcome = "ok"
        except asyncio.CancelledError:
            # User cancel (or shutdown): the state is already CANCELLED and
            # the CAS below would fail anyway. Never write a final result.
            outcome = "cancelled"
        except (asyncio.TimeoutError, TimeoutError) as exc:
            outcome = "timeout"
            error = exc
        except Exception as exc:  # handler failure
            outcome = "error"
            error = exc

        if outcome == "cancelled":
            return

        expected = (TaskState.RUNNING, token)
        now = self._clock()

        if outcome == "ok":
            def mutate_ok(rec: TaskRecord) -> None:
                rec.result = result
                rec.error = None
                rec.error_type = None
                if rec.attempts:
                    att = rec.attempts[-1]
                    att.state = TaskState.SUCCEEDED.value
                    att.finished_at = now
                    att.result = result

            updated = await self._store.compare_and_set_state(
                task_id,
                expected,
                TaskState.SUCCEEDED,
                EventType.SUCCEEDED,
                mutate=mutate_ok,
                attempt=attempt_no,
            )
            if updated is not None:
                self.notify()  # unlock waiting successors
            return

        # ---- failure / timeout with retry policy ------------------------ #
        err_type = type(error).__name__ if error else "TimeoutError"
        err_msg = str(error) if error else f"timed out after {record.spec.timeout}s"
        retries_left = attempt_no < record.spec.max_attempts

        def mutate_fail(rec: TaskRecord) -> None:
            if rec.attempts:
                att = rec.attempts[-1]
                att.state = (
                    TaskState.TIMED_OUT.value
                    if outcome == "timeout"
                    else TaskState.FAILED.value
                )
                att.finished_at = now
                att.error = err_msg
                att.error_type = err_type

        if retries_left:
            next_attempt = attempt_no + 1
            delay = record.spec.backoff.delay_for(next_attempt)

            def mutate_retry(rec: TaskRecord) -> None:
                mutate_fail(rec)
                rec.not_before = now + delay
                rec.error = err_msg
                rec.error_type = err_type

            await self._store.compare_and_set_state(
                task_id,
                expected,
                TaskState.RETRY_WAIT,
                EventType.TIMED_OUT if outcome == "timeout" else EventType.FAILED,
                mutate=mutate_retry,
                detail=f"attempt {attempt_no} {outcome}; retry {next_attempt} in {delay:.3f}s",
                attempt=attempt_no,
            )
            await self._store.append_event(
                task_id,
                EventType.RETRY_SCHEDULED,
                detail=f"attempt {next_attempt} after {delay:.3f}s backoff",
                attempt=next_attempt,
            )
        else:
            terminal = TaskState.TIMED_OUT if outcome == "timeout" else TaskState.FAILED

            def mutate_terminal(rec: TaskRecord) -> None:
                mutate_fail(rec)
                rec.error = err_msg
                rec.error_type = err_type

            await self._store.compare_and_set_state(
                task_id,
                expected,
                terminal,
                EventType.TIMED_OUT if outcome == "timeout" else EventType.FAILED,
                mutate=mutate_terminal,
                detail=f"attempt {attempt_no}: {err_type}: {err_msg}",
                attempt=attempt_no,
            )
            await self._propagate_failure(task_id)
        self.notify()

    async def _propagate_failure(self, failed_id: str) -> None:
        """Wake evaluation so successors get skipped/cancelled by policy."""
        self.notify()
