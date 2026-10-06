"""HTTP API wiring: trigger / status / cancel / history / list."""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import APIRouter, FastAPI, HTTPException, Query, Request

from app.models import (
    TERMINAL_STATES,
    DepFailPolicy,
    DependencyCycleError,
    DuplicateTaskError,
    TaskNotFoundError,
    UnknownDependencyError,
)
from app.orchestrator import Orchestrator
from app.schemas import (
    CancelIn,
    EventOut,
    HistoryOut,
    TaskOut,
    TriggerIn,
    WorkflowIn,
)
from app.store import InMemoryTaskStore
from app.store_sqlite import SqliteTaskStore

router = APIRouter()

_BAD_REQUEST = (
    DuplicateTaskError,
    DependencyCycleError,
    UnknownDependencyError,
    ValueError,
)


def _orch(request: Request) -> Orchestrator:
    return request.app.state.orchestrator


def _to_task_out(snapshot) -> TaskOut:
    rec = snapshot.record
    spec = rec.spec
    return TaskOut(
        id=rec.id,
        state=rec.state.value,
        handler=spec.handler,
        deps=sorted(spec.deps),
        priority=spec.priority,
        attempts=len(rec.attempts),
        result=rec.result,
        error=rec.error,
        error_type=rec.error_type,
        skip_reason=rec.skip_reason,
        cancel_reason=rec.cancel_reason,
        created_at=rec.created_at,
        updated_at=rec.updated_at,
    )


@router.post("/tasks", response_model=TaskOut, status_code=201)
async def trigger_task(body: TriggerIn, request: Request):
    orch = _orch(request)
    try:
        spec = await orch.trigger(
            body.handler,
            payload=body.payload,
            task_id=body.task_id,
            deps=body.deps,
            priority=body.priority,
            mutex_group=body.mutex_group,
            dep_fail_policy=DepFailPolicy(body.dep_fail_policy),
            max_attempts=body.max_attempts,
            timeout=body.timeout,
            backoff=body.backoff.model_dump() if body.backoff else None,
            idempotency_key=body.idempotency_key,
        )
    except _BAD_REQUEST as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    snapshot = await orch.get_status(spec.id)
    return _to_task_out(snapshot)


@router.post("/workflows", status_code=201)
async def trigger_workflow(body: WorkflowIn, request: Request):
    orch = _orch(request)
    raw_specs = []
    for task in body.tasks:
        raw = task.model_dump(exclude_none=True)
        raw_specs.append(raw)
    try:
        created = await orch.trigger_workflow(raw_specs)
    except _BAD_REQUEST as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    ids = sorted(created)
    return {"task_ids": ids, "count": len(ids)}


@router.get("/tasks", response_model=list[TaskOut])
async def list_tasks(
    request: Request,
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
):
    snapshots = await _orch(request).list_tasks(limit=limit, offset=offset)
    return [_to_task_out(s) for s in snapshots]


@router.get("/tasks/{task_id}", response_model=TaskOut)
async def get_task(task_id: str, request: Request):
    try:
        snapshot = await _orch(request).get_status(task_id)
    except TaskNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return _to_task_out(snapshot)


@router.get("/tasks/{task_id}/history", response_model=HistoryOut)
async def get_history(task_id: str, request: Request):
    try:
        snapshot = await _orch(request).get_status(task_id)
    except TaskNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    events = [
        EventOut(
            seq=e.seq,
            type=e.type.value,
            timestamp=e.timestamp,
            attempt=e.attempt,
            detail=e.detail,
        )
        for e in snapshot.events
    ]
    return HistoryOut(
        task_id=task_id, state=snapshot.record.state.value, events=events
    )


@router.post("/tasks/{task_id}/cancel", response_model=TaskOut)
async def cancel_task(task_id: str, request: Request, body: CancelIn | None = None):
    reason = body.reason if body else "user requested"
    orch = _orch(request)
    try:
        changed = await orch.cancel(task_id, reason)
    except TaskNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    snapshot = await orch.get_status(task_id)
    if not changed and snapshot.record.state not in TERMINAL_STATES:
        raise HTTPException(status_code=409, detail="task could not be cancelled")
    return _to_task_out(snapshot)


def create_app(orchestrator: Orchestrator) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await orchestrator.start()
        try:
            yield
        finally:
            await orchestrator.stop()

    app = FastAPI(
        title="Async Task Orchestration Service",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.state.orchestrator = orchestrator
    app.include_router(router)

    @app.get("/")
    async def root():
        return {
            "service": "async-task-orchestration",
            "endpoints": [
                "POST /tasks",
                "POST /workflows",
                "GET /tasks",
                "GET /tasks/{id}",
                "GET /tasks/{id}/history",
                "POST /tasks/{id}/cancel",
            ],
        }

    return app


def build_default_app(db_path: str, max_concurrency: int) -> FastAPI:
    if db_path == ":memory:":
        store = InMemoryTaskStore()
    else:
        store = SqliteTaskStore(db_path)
    return create_app(
        Orchestrator(store=store, max_concurrency=max_concurrency)
    )
