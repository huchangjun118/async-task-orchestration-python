"""Service entrypoint.

Run with:  uv run uvicorn main:app
State is persisted to ./data/tasks.db (SQLite) so tasks survive restarts.
Override with TASK_DB_PATH=:memory: for an ephemeral in-memory service.
"""
import os

from app.api import build_default_app

DB_PATH = os.environ.get("TASK_DB_PATH", "data/tasks.db")
MAX_CONCURRENCY = int(os.environ.get("TASK_MAX_CONCURRENCY", "4"))

app = build_default_app(DB_PATH, MAX_CONCURRENCY)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=int(os.environ.get("PORT", "8000")))
