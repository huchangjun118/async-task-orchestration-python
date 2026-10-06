"""Core type definitions for the async task orchestration engine.

This module only contains data types and enums: no behaviour.
"""
from __future__ import annotations

import enum
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional


def new_id() -> str:
    return uuid.uuid4().hex


def now() -> float:
    return time.time()


class TaskState(str, enum.Enum):
    """Lifecycle states of a task instance.

    Transitions:
      PENDING   -> READY      (all dependencies succeeded)
      PENDING   -> SKIPPED    (a dependency failed/cancelled/skipped, policy=skip)
      PENDING   -> CANCELLED  (a dependency failed..., policy=cancel, or explicit cancel)
      READY     -> RUNNING    (dispatched by scheduler)
      READY     -> CANCELLED  (explicit cancel before dispatch)
      RUNNING   -> SUCCEEDED  (handler returned)
      RUNNING   -> FAILED     (handler raised, retries exhausted)
      RUNNING   -> TIMED_OUT  (handler exceeded timeout, retries exhausted)
      RUNNING   -> CANCELLED  (explicit cancel while running)
      RUNNING   -> READY      (retry scheduled -> re-queued)
    Terminal states: SUCCEEDED, FAILED, TIMED_OUT, CANCELLED, SKIPPED.
    """

    PENDING = "PENDING"
    READY = "READY"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    TIMED_OUT = "TIMED_OUT"
    CANCELLED = "CANCELLED"
    SKIPPED = "SKIPPED"

    @property
    def is_terminal(self) -> bool:
        return self in TERMINAL_STATES


TERMINAL_STATES = frozenset(
    {
        TaskState.SUCCEEDED,
        TaskState.FAILED,
        TaskState.TIMED_OUT,
        TaskState.CANCELLED,
        TaskState.SKIPPED,
    }
)


class DependencyFailurePolicy(str, enum.Enum):
    """What happens to a task when one of its dependencies does not succeed."""

    SKIP = "skip"        # mark this task SKIPPED (and propagate downstream)
    CANCEL = "cancel"    # mark this task CANCELLED (and propagate downstream)


class BackoffStrategy(str, enum.Enum):
    FIXED = "fixed"
    EXPONENTIAL = "exponential"


@dataclass(frozen=True)
class RetryPolicy:
    max_retries: int = 0                 # number of retries *after* the first attempt
    backoff: BackoffStrategy = BackoffStrategy.FIXED
    base_delay: float = 0.1              # seconds
    max_delay: float = 30.0              # seconds
    retry_on_timeout: bool = True        # whether TIMED_OUT attempts are retried

    def delay_for(self, attempt: int) -> float:
        """Delay before retry number `attempt` (1-based)."""
        if self.backoff == BackoffStrategy.EXPONENTIAL:
            delay = self.base_delay * (2 ** (attempt - 1))
        else:
            delay = self.base_delay
        return min(delay, self.max_delay)


# A task handler: receives the TaskInstance and a CancellationToken.
TaskHandler = Callable[["TaskInstance", "CancellationToken"], Awaitable[Any]]


@dataclass(frozen=True)
class TaskDefinition:
    """Static, registered description of a task kind."""

    name: str
    handler: TaskHandler
    dependencies: tuple[str, ...] = ()          # names of tasks that must succeed first
    priority: int = 0                            # higher runs earlier
    mutex_group: Optional[str] = None            # tasks in the same group never run concurrently
    timeout: Optional[float] = None              # seconds per attempt; None = no timeout
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)
    on_dependency_failure: DependencyFailurePolicy = DependencyFailurePolicy.SKIP


@dataclass
class TaskInstance:
    """A single (idempotent) occurrence of a task inside a run."""

    instance_id: str
    run_id: str
    task_name: str
    state: TaskState = TaskState.PENDING
    attempt: int = 0                 # number of attempts started
    payload: Any = None
    result: Any = None
    error: Optional[str] = None
    skip_reason: Optional[str] = None
    created_at: float = field(default_factory=now)
    updated_at: float = field(default_factory=now)
    # id of the asyncio task currently executing this instance (runtime only)
    worker_id: Optional[str] = None


@dataclass(frozen=True)
class TransitionRecord:
    """One entry in the immutable transition history of a task instance."""

    seq: int                         # monotonically increasing per store
    instance_id: str
    run_id: str
    task_name: str
    from_state: Optional[TaskState]
    to_state: TaskState
    attempt: int
    reason: Optional[str]
    at: float


@dataclass(frozen=True)
class TriggerResult:
    run_id: str
    created: bool        # False when an identical idempotency key already existed
    instance_ids: tuple[str, ...]


@dataclass(frozen=True)
class TaskSnapshot:
    """A consistent point-in-time view of one task instance."""

    instance_id: str
    run_id: str
    task_name: str
    state: TaskState
    attempt: int
    result: Any
    error: Optional[str]
    skip_reason: Optional[str]
    created_at: float
    updated_at: float


@dataclass(frozen=True)
class RunSnapshot:
    run_id: str
    tasks: tuple[TaskSnapshot, ...]

    @property
    def done(self) -> bool:
        return all(t.state.is_terminal for t in self.tasks)


class CancellationToken:
    """Cooperative cancellation handle passed to handlers."""

    def __init__(self) -> None:
        self._cancelled = False

    def cancel(self) -> None:
        self._cancelled = True

    @property
    def cancelled(self) -> bool:
        return self._cancelled

    def throw_if_cancelled(self) -> None:
        if self._cancelled:
            raise TaskCancelledError()


class TaskCancelledError(Exception):
    """Raised by/inside handlers when the task has been cancelled."""
