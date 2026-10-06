"""Core domain model: task state machine, events, specs and exceptions."""
from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Protocol


class TaskState(str, enum.Enum):
    """Lifecycle states of a task.

    PENDING   -> dependencies not all resolved yet (waiting on predecessors)
    READY     -> all dependencies succeeded; waiting for a scheduler slot
    RUNNING   -> a handler attempt is executing
    RETRY_WAIT-> an attempt failed; waiting for backoff before the next attempt
    SUCCEEDED -> terminal; result is final
    FAILED    -> terminal; retries exhausted or non-retryable error
    SKIPPED   -> terminal; a predecessor failed and policy says skip
    CANCELLED -> terminal; cancelled by user (or cascade-cancelled)
    TIMED_OUT -> terminal; final attempt exceeded the configured timeout
    """

    PENDING = "PENDING"
    READY = "READY"
    RETRY_WAIT = "RETRY_WAIT"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    CANCELLED = "CANCELLED"
    TIMED_OUT = "TIMED_OUT"


TERMINAL_STATES = frozenset(
    {
        TaskState.SUCCEEDED,
        TaskState.FAILED,
        TaskState.SKIPPED,
        TaskState.CANCELLED,
        TaskState.TIMED_OUT,
    }
)


class EventType(str, enum.Enum):
    CREATED = "CREATED"
    READY = "READY"
    STARTED = "STARTED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    RETRY_SCHEDULED = "RETRY_SCHEDULED"
    RETRYING = "RETRYING"
    TIMED_OUT = "TIMED_OUT"
    SKIPPED = "SKIPPED"
    CANCEL_REQUESTED = "CANCEL_REQUESTED"
    CANCELLED = "CANCELLED"
    RECOVERED = "RECOVERED"


class DepFailPolicy(str, enum.Enum):
    """What happens to a task when one of its predecessors fails."""

    SKIP = "SKIP"  # task is marked SKIPPED with a recorded reason
    CANCEL = "CANCEL"  # task is marked CANCELLED with a recorded reason


@dataclass(frozen=True)
class BackoffConfig:
    """Retry backoff configuration.

    delay(attempt) = min(max_delay, base_delay * multiplier ** (attempt - 1))
    """

    max_attempts: int = 1
    base_delay: float = 1.0
    multiplier: float = 2.0
    max_delay: float = 60.0

    def delay_for(self, attempt: int) -> float:
        """Delay before attempt number ``attempt`` (1-based)."""
        if attempt <= 1:
            return 0.0
        raw = self.base_delay * (self.multiplier ** (attempt - 2))
        return min(self.max_delay, raw)


@dataclass(frozen=True)
class TaskSpec:
    """Static declaration of a task."""

    id: str
    handler: str
    deps: frozenset[str] = field(default_factory=frozenset)
    payload: Any = None
    priority: int = 0  # larger value = scheduled first
    mutex_group: str | None = None
    dep_fail_policy: DepFailPolicy = DepFailPolicy.SKIP
    max_attempts: int = 1
    timeout: float | None = None
    backoff: BackoffConfig = field(default_factory=BackoffConfig)
    idempotency_key: str | None = None


@dataclass
class TaskAttempt:
    """One execution attempt of a task."""

    attempt: int
    state: str  # RUNNING | SUCCEEDED | FAILED | TIMED_OUT | CANCELLED
    started_at: float
    finished_at: float | None = None
    error: str | None = None
    error_type: str | None = None
    result: Any = None


@dataclass
class TaskRecord:
    """Mutable runtime record for one task."""

    id: str
    spec: TaskSpec
    state: TaskState
    created_at: float
    updated_at: float
    seq: int = 0
    attempts: list[TaskAttempt] = field(default_factory=list)
    result: Any = None
    error: str | None = None
    error_type: str | None = None
    skip_reason: str | None = None
    cancel_reason: str | None = None
    cancelled: bool = False
    not_before: float = 0.0  # earliest time for the next attempt (backoff)
    run_token: int = 0  # fencing token of the currently valid attempt


@dataclass(frozen=True)
class TaskEvent:
    """An append-only state-transition event."""

    id: int
    task_id: str
    seq: int
    type: EventType
    timestamp: float
    attempt: int | None = None
    detail: str | None = None
    payload: Any = None


@dataclass(frozen=True)
class TaskSnapshot:
    """Consistent, immutable view of a task and its full history."""

    record: TaskRecord
    events: tuple[TaskEvent, ...]


class TaskContext(Protocol):
    """Runtime services passed to a task handler."""

    @property
    def task_id(self) -> str: ...
    @property
    def attempt(self) -> int: ...
    @property
    def cancelled(self) -> Awaitable[bool]: ...
    @property
    def results(self) -> dict[str, Any]: ...


TaskHandler = Callable[[TaskContext], Awaitable[Any]]


class OrchestratorError(Exception):
    """Base class for orchestrator errors."""


class TaskNotFoundError(OrchestratorError):
    pass


class DuplicateTaskError(OrchestratorError):
    """Raised when an idempotency key / task id already exists.

    The existing task id is attached.
    """

    def __init__(self, message: str, existing_task_id: str | None = None):
        super().__init__(message)
        self.existing_task_id = existing_task_id


class DependencyCycleError(OrchestratorError):
    pass


class UnknownDependencyError(OrchestratorError):
    pass


class HandlerTimeoutError(OrchestratorError):
    pass
