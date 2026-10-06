"""FastAPI HTTP layer exposing the orchestration engine.

Endpoints:
  POST /tasks/{task_name}/trigger      trigger a task (idempotent)
  GET  /runs/{run_id}                  consistent snapshot of a run
  GET  /instances/{instance_id}        snapshot of one task instance
  GET  /instances/{instance_id}/history  full transition history
  POST /runs/{run_id}/cancel           cancel a run (or one task via ?task_name=)
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from .engine import OrchestrationEngine


class TriggerRequest(BaseModel):
    payload: Any = None
    idempotency_key: Optional[str] = None


class TriggerResponse(BaseModel):
    run_id: str
    created: bool
    instance_ids: list[str]


class CancelResponse(BaseModel):
    cancelled: bool


def create_app(engine: OrchestrationEngine) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await engine.start()
        yield
        await engine.stop()

    app = FastAPI(title="async-task-orchestration", lifespan=lifespan)

    @app.post("/tasks/{task_name}/trigger", response_model=TriggerResponse)
    async def trigger(task_name: str, req: TriggerRequest) -> TriggerResponse:
        try:
            result = await engine.trigger(
                task_name, payload=req.payload, idempotency_key=req.idempotency_key
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return TriggerResponse(
            run_id=result.run_id,
            created=result.created,
            instance_ids=list(result.instance_ids),
        )

    @app.get("/runs/{run_id}")
    async def get_run(run_id: str) -> dict:
        snap = engine.get_run(run_id)
        if snap is None:
            raise HTTPException(status_code=404, detail="run not found")
        return {
            "run_id": snap.run_id,
            "done": snap.done,
            "tasks": [_task_to_dict(t) for t in snap.tasks],
        }

    @app.get("/instances/{instance_id}")
    async def get_instance(instance_id: str) -> dict:
        snap = engine.get_task(instance_id)
        if snap is None:
            raise HTTPException(status_code=404, detail="instance not found")
        return _task_to_dict(snap)

    @app.get("/instances/{instance_id}/history")
    async def get_history(instance_id: str) -> dict:
        if engine.get_task(instance_id) is None:
            raise HTTPException(status_code=404, detail="instance not found")
        return {
            "instance_id": instance_id,
            "transitions": [
                {
                    "seq": t.seq,
                    "from_state": t.from_state.value if t.from_state else None,
                    "to_state": t.to_state.value,
                    "attempt": t.attempt,
                    "reason": t.reason,
                    "at": t.at,
                }
                for t in engine.get_history(instance_id)
            ],
        }

    @app.post("/runs/{run_id}/cancel", response_model=CancelResponse)
    async def cancel(run_id: str, task_name: Optional[str] = None) -> CancelResponse:
        if engine.get_run(run_id) is None:
            raise HTTPException(status_code=404, detail="run not found")
        cancelled = await engine.cancel(run_id, task_name=task_name)
        return CancelResponse(cancelled=cancelled)

    return app


def _task_to_dict(t) -> dict:
    return {
        "instance_id": t.instance_id,
        "run_id": t.run_id,
        "task_name": t.task_name,
        "state": t.state.value,
        "attempt": t.attempt,
        "result": t.result,
        "error": t.error,
        "skip_reason": t.skip_reason,
        "created_at": t.created_at,
        "updated_at": t.updated_at,
    }
