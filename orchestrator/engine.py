"""OrchestrationEngine: the public in-process API tying everything together.

Concurrency model: everything runs on one asyncio event loop. All state
changes go through TaskStore.compare-and-swap transitions, so races
between triggers, retries, cancels, recovery and runners are resolved
deterministically: the first successful CAS wins, later ones are no-ops.
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Optional

from .executor import AttemptOutcome, Executor
from .models import (
    CancellationToken,
    DependencyFailurePolicy,
    RunSnapshot,
    TaskInstance,
    TaskSnapshot,
    TaskState,
    TransitionRecord,
    TriggerResult,
    new_id,
)
from .registry import TaskRegistry
from .scheduler import Scheduler
from .store import TaskStore

log = logging.getLogger(__name__)


class OrchestrationEngine:
    def __init__(
        self,
        store: TaskStore,
        registry: TaskRegistry,
        max_concurrency: int = 4,
        cancel_grace: float = 0.5,
    ) -> None:
        # grace period (seconds) between signalling a running task's
        # cancellation token and force-cancelling its asyncio task
        self._cancel_grace = cancel_grace
        self._store = store
        self._registry = registry
        self._scheduler = Scheduler(max_concurrency=max_concurrency)
        self._executor = Executor()
        self._tokens: dict[str, CancellationToken] = {}
        self._runner_tasks: dict[str, asyncio.Task] = {}
        self._retry_tasks: set[asyncio.Task] = set()
        self._dispatch_task: Optional[asyncio.Task] = None
        self._started = False

    # ------------------------------------------------------------------ #
    # lifecycle
    # ------------------------------------------------------------------ #
    async def start(self) -> None:
        if self._started:
            return
        self._store.open()
        self._registry.validate()
        self._recover()
        self._dispatch_task = asyncio.create_task(self._dispatch_loop())
        self._started = True

    async def stop(self) -> None:
        if not self._started:
            return
        self._started = False
        if self._dispatch_task is not None:
            self._dispatch_task.cancel()
            await asyncio.gather(self._dispatch_task, return_exceptions=True)
            self._dispatch_task = None
        for t in list(self._retry_tasks):
            t.cancel()
        if self._retry_tasks:
            await asyncio.gather(*self._retry_tasks, return_exceptions=True)
        # do not cancel in-flight runners abruptly: give them a cooperative
        # cancellation signal and a moment to finish their CAS writes
        for token in self._tokens.values():
            token.cancel()
        runners = list(self._runner_tasks.values())
        for t in runners:
            t.cancel()
        if runners:
            await asyncio.gather(*runners, return_exceptions=True)

    # ------------------------------------------------------------------ #
    # trigger
    # ------------------------------------------------------------------ #
    async def trigger(
        self,
        task_name: str,
        payload: Any = None,
        idempotency_key: Optional[str] = None,
    ) -> TriggerResult:
        """Trigger a task (and its dependency graph) exactly once per key.

        If `idempotency_key` is omitted, one is derived deterministically
        from (task_name, payload), so identical repeat triggers collapse
        onto the same run. Re-triggering with an existing key returns the
        original run without creating new instances or side effects.
        """
        definition = self._registry.get(task_name)  # raises for unknown task
        if idempotency_key is None:
            idempotency_key = self._default_key(task_name, payload)
        run_id = new_id()
        if not self._store.claim_idempotency_key(idempotency_key, run_id):
            existing = self._store.run_id_for_key(idempotency_key)
            assert existing is not None
            ids = tuple(i.instance_id for i in self._store.instances_of_run(existing))
            return TriggerResult(run_id=existing, created=False, instance_ids=ids)

        # create instances for the task and all transitive dependencies,
        # dependencies first so creation order matches the DAG
        ordered = self._transitive_dependencies(task_name)
        instance_ids: list[str] = []
        for name in ordered:
            inst = TaskInstance(
                instance_id=f"{run_id}:{name}",
                run_id=run_id,
                task_name=name,
                payload=payload if name == task_name else None,
            )
            self._store.insert_instance(inst)
            instance_ids.append(inst.instance_id)
        # evaluate readiness of the freshly created run
        for name in ordered:
            self._evaluate(f"{run_id}:{name}")
        return TriggerResult(run_id=run_id, created=True, instance_ids=tuple(instance_ids))

    @staticmethod
    def _default_key(task_name: str, payload: Any) -> str:
        try:
            body = json.dumps(payload, sort_keys=True, default=repr)
        except (TypeError, ValueError):
            body = repr(payload)
        return f"auto:{task_name}:{body}"

    def _transitive_dependencies(self, task_name: str) -> list[str]:
        """Return [deps..., task_name] in topological order (deps first)."""
        ordered: list[str] = []
        seen: set[str] = set()

        def visit(name: str) -> None:
            if name in seen:
                return
            seen.add(name)
            for dep in self._registry.get(name).dependencies:
                visit(dep)
            ordered.append(name)

        visit(task_name)
        return ordered

    # ------------------------------------------------------------------ #
    # dependency evaluation
    # ------------------------------------------------------------------ #
    def _evaluate(self, instance_id: str) -> None:
        """Evaluate one PENDING instance against its dependencies' states."""
        inst = self._store.get_instance(instance_id)
        if inst is None or inst.state != TaskState.PENDING:
            return
        definition = self._registry.get(inst.task_name)
        if not definition.dependencies:
            self._make_ready(inst, "no dependencies")
            return
        run_instances = {i.task_name: i for i in self._store.instances_of_run(inst.run_id)}
        blocker: Optional[TaskInstance] = None
        for dep_name in definition.dependencies:
            dep = run_instances.get(dep_name)
            if dep is None:
                blocker = None
                break
            if dep.state == TaskState.SUCCEEDED:
                continue
            if dep.state.is_terminal:
                blocker = dep
                break
            return  # a dependency is still in flight: wait
        if blocker is None:
            self._make_ready(inst, "all dependencies succeeded")
            return
        reason = (
            f"dependency {blocker.task_name!r} ended in state"
            f" {blocker.state.value}"
        )
        if blocker.error:
            reason += f" ({blocker.error})"
        if definition.on_dependency_failure == DependencyFailurePolicy.CANCEL:
            self._store.transition(
                instance_id, TaskState.CANCELLED,
                expected=(TaskState.PENDING,), reason=reason, error=reason,
            )
        else:
            self._store.transition(
                instance_id, TaskState.SKIPPED,
                expected=(TaskState.PENDING,), reason=reason, skip_reason=reason,
            )
        self._evaluate_dependents(inst.run_id, inst.task_name)

    def _make_ready(self, inst: TaskInstance, reason: str) -> None:
        if self._store.transition(
            inst.instance_id, TaskState.READY,
            expected=(TaskState.PENDING,), reason=reason,
        ):
            self._scheduler.submit(
                inst.instance_id, self._registry.get(inst.task_name)
            )

    def _evaluate_dependents(self, run_id: str, task_name: str) -> None:
        """Re-evaluate every instance in the run that depends on task_name."""
        for inst in self._store.instances_of_run(run_id):
            if inst.state != TaskState.PENDING:
                continue
            definition = self._registry.get(inst.task_name)
            if task_name in definition.dependencies:
                self._evaluate(inst.instance_id)

    # ------------------------------------------------------------------ #
    # dispatch & execution
    # ------------------------------------------------------------------ #
    async def _dispatch_loop(self) -> None:
        while True:
            await self._scheduler.wait_for_work()
            while True:
                instance_id = self._scheduler.try_acquire()
                if instance_id is None:
                    break
                task = asyncio.create_task(self._run_instance(instance_id))
                self._runner_tasks[instance_id] = task
                task.add_done_callback(
                    lambda _t, iid=instance_id: self._runner_tasks.pop(iid, None)
                )

    async def _run_instance(self, instance_id: str) -> None:
        try:
            inst = self._store.get_instance(instance_id)
            if inst is None:
                return
            definition = self._registry.get(inst.task_name)
            attempt = inst.attempt + 1
            # CAS READY -> RUNNING; loses the race against cancellation
            if not self._store.transition(
                instance_id, TaskState.RUNNING,
                expected=(TaskState.READY,), attempt=attempt,
                reason=f"attempt {attempt} started",
            ):
                return
            token = CancellationToken()
            self._tokens[instance_id] = token
            try:
                inst = self._store.get_instance(instance_id) or inst
                outcome, result, error = await self._executor.run_attempt(
                    inst, definition, token
                )
            finally:
                self._tokens.pop(instance_id, None)
            await self._settle(instance_id, definition, attempt, outcome, result, error)
        finally:
            self._scheduler.release(instance_id)

    async def _settle(
        self,
        instance_id: str,
        definition,
        attempt: int,
        outcome: str,
        result: Any,
        error: Optional[str],
    ) -> None:
        """Write the outcome of an attempt, via CAS from RUNNING only.

        If the instance was cancelled while running, the CAS fails and the
        result is discarded: a cancelled task never writes a final result.
        """
        inst = self._store.get_instance(instance_id)
        if inst is None:
            return
        if outcome == AttemptOutcome.SUCCEEDED:
            settled = self._store.transition(
                instance_id, TaskState.SUCCEEDED,
                expected=(TaskState.RUNNING,), result=result,
                reason=f"attempt {attempt} succeeded",
            )
        elif outcome in (AttemptOutcome.FAILED, AttemptOutcome.TIMED_OUT):
            settled = await self._maybe_retry(
                instance_id, definition, attempt, outcome, error
            )
        else:  # CANCELLED: the cancel path owns the final transition
            self._store.transition(
                instance_id, TaskState.CANCELLED,
                expected=(TaskState.RUNNING,),
                reason=f"attempt {attempt} cancelled",
            )
            settled = True
        if settled:
            self._evaluate_dependents(inst.run_id, inst.task_name)

    async def _maybe_retry(
        self,
        instance_id: str,
        definition,
        attempt: int,
        outcome: str,
        error: Optional[str],
    ) -> bool:
        """Transition to terminal failure or schedule a retry.

        Returns True if the instance reached a terminal state.
        """
        policy = definition.retry_policy
        final_state = (
            TaskState.TIMED_OUT if outcome == AttemptOutcome.TIMED_OUT
            else TaskState.FAILED
        )
        retryable = attempt <= policy.max_retries and (
            outcome == AttemptOutcome.FAILED or policy.retry_on_timeout
        )
        if not retryable:
            return self._store.transition(
                instance_id, final_state,
                expected=(TaskState.RUNNING,), error=error,
                reason=f"attempt {attempt} {outcome}, retries exhausted",
            )
        delay = policy.delay_for(attempt)
        # RUNNING -> READY: requeue after a backoff delay
        if not self._store.transition(
            instance_id, TaskState.READY,
            expected=(TaskState.RUNNING,), error=error,
            reason=(
                f"attempt {attempt} {outcome}; retry"
                f" {attempt}/{policy.max_retries} in {delay:.3f}s"
            ),
        ):
            return True  # cancelled concurrently: treat as settled
        task = asyncio.create_task(self._retry_later(instance_id, delay))
        self._retry_tasks.add(task)
        task.add_done_callback(self._retry_tasks.discard)
        return False

    async def _retry_later(self, instance_id: str, delay: float) -> None:
        try:
            await asyncio.sleep(delay)
            inst = self._store.get_instance(instance_id)
            # only resubmit if still READY (may have been cancelled meanwhile)
            if inst is not None and inst.state == TaskState.READY:
                self._scheduler.submit(
                    instance_id, self._registry.get(inst.task_name)
                )
        except asyncio.CancelledError:
            pass

    # ------------------------------------------------------------------ #
    # cancel
    # ------------------------------------------------------------------ #
    async def cancel(self, run_id: str, task_name: Optional[str] = None) -> bool:
        """Cancel one task of a run, or the whole run.

        - PENDING/READY: transitioned to CANCELLED immediately.
        - RUNNING: transitioned to CANCELLED (CAS), then the worker is
          signalled and interrupted; its late result is discarded because
          the settle CAS requires state RUNNING.
        Dependents of cancelled tasks are skipped/cancelled per policy.
        Returns True if at least one instance was cancelled.
        """
        cancelled_any = False
        for inst in self._store.instances_of_run(run_id):
            if task_name is not None and inst.task_name != task_name:
                continue
            if inst.state.is_terminal:
                continue
            reason = "cancelled by user"
            if inst.state in (TaskState.PENDING, TaskState.READY):
                if self._store.transition(
                    inst.instance_id, TaskState.CANCELLED,
                    expected=(TaskState.PENDING, TaskState.READY),
                    reason=reason, error=reason,
                ):
                    self._scheduler.remove(inst.instance_id)
                    cancelled_any = True
                    self._evaluate_dependents(inst.run_id, inst.task_name)
            elif inst.state == TaskState.RUNNING:
                if self._store.transition(
                    inst.instance_id, TaskState.CANCELLED,
                    expected=(TaskState.RUNNING,),
                    reason=reason, error=reason,
                ):
                    cancelled_any = True
                    token = self._tokens.get(inst.instance_id)
                    if token is not None:
                        token.cancel()
                    # give the handler a grace period to unwind
                    # cooperatively, then force-cancel the asyncio task
                    task = asyncio.create_task(
                        self._force_cancel_later(inst.instance_id)
                    )
                    self._retry_tasks.add(task)
                    task.add_done_callback(self._retry_tasks.discard)
                    self._evaluate_dependents(inst.run_id, inst.task_name)
        return cancelled_any

    async def _force_cancel_later(self, instance_id: str) -> None:
        try:
            await asyncio.sleep(self._cancel_grace)
            runner = self._runner_tasks.get(instance_id)
            if runner is not None and not runner.done():
                runner.cancel()
        except asyncio.CancelledError:
            pass

    # ------------------------------------------------------------------ #
    # recovery
    # ------------------------------------------------------------------ #
    def _recover(self) -> None:
        """Bring unfinished instances from the store back into motion.

        - RUNNING at crash time: the attempt was interrupted; reset to
          READY so it is re-executed (handlers must be idempotent).
        - READY: re-submitted to the scheduler.
        - PENDING: re-evaluated against dependency states.
        Terminal instances are never touched: completed work is neither
        re-executed nor lost. Recovery uses the same CAS transitions and
        scheduler as live traffic, so triggers arriving during recovery
        interleave deterministically.
        """
        unfinished = self._store.unfinished_instances()
        # reset interrupted RUNNING attempts first
        for inst in unfinished:
            if inst.state == TaskState.RUNNING:
                self._store.transition(
                    inst.instance_id, TaskState.READY,
                    expected=(TaskState.RUNNING,),
                    reason="recovered after restart; interrupted attempt"
                    " will be retried",
                )
        # refresh states, then evaluate PENDING and submit READY
        for inst in unfinished:
            current = self._store.get_instance(inst.instance_id)
            if current is None:
                continue
            if current.state == TaskState.PENDING:
                self._evaluate(current.instance_id)
        for inst in unfinished:
            current = self._store.get_instance(inst.instance_id)
            if current is not None and current.state == TaskState.READY:
                self._scheduler.submit(
                    current.instance_id, self._registry.get(current.task_name)
                )

    # ------------------------------------------------------------------ #
    # queries
    # ------------------------------------------------------------------ #
    def get_run(self, run_id: str) -> Optional[RunSnapshot]:
        return self._store.run_snapshot(run_id)

    def get_task(self, instance_id: str) -> Optional[TaskSnapshot]:
        return self._store.snapshot(instance_id)

    def get_history(self, instance_id: str) -> list[TransitionRecord]:
        return self._store.history(instance_id)

    async def wait_run_done(self, run_id: str, timeout: float = 10.0) -> RunSnapshot:
        """Poll until every task of the run is terminal (test/ops helper)."""
        deadline = asyncio.get_event_loop().time() + timeout
        while True:
            snap = self.get_run(run_id)
            if snap is not None and snap.done:
                return snap
            if asyncio.get_event_loop().time() > deadline:
                raise TimeoutError(f"run {run_id} not done within {timeout}s")
            await asyncio.sleep(0.01)
