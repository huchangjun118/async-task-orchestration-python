"""Pydantic request/response models for the HTTP API."""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from app.models import DepFailPolicy


class BackoffIn(BaseModel):
    max_attempts: int = Field(default=1, ge=1)
    base_delay: float = Field(default=1.0, ge=0)
    multiplier: float = Field(default=2.0, ge=1.0)
    max_delay: float = Field(default=60.0, ge=0)


class TriggerIn(BaseModel):
    handler: str
    payload: Any = None
    task_id: str | None = None
    deps: list[str] = Field(default_factory=list)
    priority: int = 0
    mutex_group: str | None = None
    dep_fail_policy: DepFailPolicy = DepFailPolicy.SKIP
    max_attempts: int = Field(default=1, ge=1)
    timeout: float | None = Field(default=None, ge=0)
    backoff: BackoffIn | None = None
    idempotency_key: str | None = None


class WorkflowTaskIn(BaseModel):
    id: str
    handler: str
    payload: Any = None
    deps: list[str] = Field(default_factory=list)
    priority: int = 0
    mutex_group: str | None = None
    dep_fail_policy: DepFailPolicy = DepFailPolicy.SKIP
    max_attempts: int = Field(default=1, ge=1)
    timeout: float | None = Field(default=None, ge=0)
    backoff: BackoffIn | None = None
    idempotency_key: str | None = None


class WorkflowIn(BaseModel):
    tasks: list[WorkflowTaskIn]


class CancelIn(BaseModel):
    reason: str = "user requested"


class TaskOut(BaseModel):
    id: str
    state: str
    handler: str
    deps: list[str]
    priority: int
    attempts: int
    result: Any = None
    error: str | None = None
    error_type: str | None = None
    skip_reason: str | None = None
    cancel_reason: str | None = None
    created_at: float
    updated_at: float


class EventOut(BaseModel):
    seq: int
    type: str
    timestamp: float
    attempt: int | None = None
    detail: str | None = None


class HistoryOut(BaseModel):
    task_id: str
    state: str
    events: list[EventOut]
