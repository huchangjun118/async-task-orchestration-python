"""Runnable entrypoint: demo registry + file-backed store + HTTP service."""
import asyncio
import os

import uvicorn

from orchestrator import (
    BackoffStrategy,
    OrchestrationEngine,
    RetryPolicy,
    TaskDefinition,
    TaskRegistry,
    TaskStore,
)
from orchestrator.service import create_app

DB_PATH = os.environ.get("ORCHESTRATOR_DB", "orchestrator.db")
MAX_CONCURRENCY = int(os.environ.get("ORCHESTRATOR_MAX_CONCURRENCY", "4"))


def build_registry() -> TaskRegistry:
    registry = TaskRegistry()

    async def extract(instance, token):
        await asyncio.sleep(0.2)
        token.throw_if_cancelled()
        return {"rows": 100}

    async def transform(instance, token):
        await asyncio.sleep(0.2)
        token.throw_if_cancelled()
        return {"rows_transformed": 100}

    async def load(instance, token):
        await asyncio.sleep(0.1)
        token.throw_if_cancelled()
        return {"loaded": True, "payload": instance.payload}

    registry.register(TaskDefinition(
        "extract", extract, priority=1,
        retry_policy=RetryPolicy(max_retries=2, backoff=BackoffStrategy.EXPONENTIAL),
        timeout=10.0,
    ))
    registry.register(TaskDefinition(
        "transform", transform, dependencies=("extract",), priority=1,
        mutex_group="db-writers", timeout=10.0,
    ))
    registry.register(TaskDefinition(
        "load", load, dependencies=("transform",), priority=0,
        mutex_group="db-writers", timeout=10.0,
    ))
    return registry


engine = OrchestrationEngine(
    store=TaskStore(DB_PATH),
    registry=build_registry(),
    max_concurrency=MAX_CONCURRENCY,
)
app = create_app(engine)

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
