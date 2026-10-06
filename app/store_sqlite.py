"""SQLite-backed task store for cross-process-restart durability.

Uses the stdlib ``sqlite3`` driver in a worker thread. Every state transition
is one ``BEGIN IMMEDIATE`` transaction containing the compare-and-set check,
the record update and the event append, so transitions are atomic even across
threads and recovery cannot double-apply.
"""
from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import time
from typing import Any, Callable

from app.models import (
    BackoffConfig,
    DepFailPolicy,
    DuplicateTaskError,
    EventType,
    TaskAttempt,
    TaskEvent,
    TaskRecord,
    TaskSnapshot,
    TaskSpec,
    TaskState,
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    seq INTEGER NOT NULL UNIQUE,
    state TEXT NOT NULL,
    idempotency_key TEXT,
    data TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    type TEXT NOT NULL,
    ts REAL NOT NULL,
    attempt INTEGER,
    detail TEXT,
    payload TEXT
);
CREATE TABLE IF NOT EXISTS idempotency (
    key TEXT PRIMARY KEY,
    task_id TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);
"""


def _json_default(obj: Any) -> Any:
    if isinstance(obj, frozenset):
        return sorted(obj)
    if isinstance(obj, (TaskState, EventType, DepFailPolicy)):
        return obj.value
    raise TypeError(f"cannot serialize {type(obj)!r}")


def _dumps(obj: Any) -> str:
    return json.dumps(obj, default=_json_default, sort_keys=True)


def _loads(raw: str) -> Any:
    return json.loads(raw)


def _encode_attempt(att: TaskAttempt) -> dict:
    return {
        "attempt": att.attempt,
        "state": att.state,
        "started_at": att.started_at,
        "finished_at": att.finished_at,
        "error": att.error,
        "error_type": att.error_type,
        "result": att.result,
    }


def _decode_attempt(d: dict) -> TaskAttempt:
    return TaskAttempt(
        attempt=d["attempt"],
        state=d["state"],
        started_at=d["started_at"],
        finished_at=d.get("finished_at"),
        error=d.get("error"),
        error_type=d.get("error_type"),
        result=d.get("result"),
    )


def _encode_record(rec: TaskRecord) -> dict:
    spec = rec.spec
    return {
        "id": rec.id,
        "spec": {
            "id": spec.id,
            "handler": spec.handler,
            "deps": sorted(spec.deps),
            "payload": spec.payload,
            "priority": spec.priority,
            "mutex_group": spec.mutex_group,
            "dep_fail_policy": spec.dep_fail_policy.value,
            "max_attempts": spec.max_attempts,
            "timeout": spec.timeout,
            "backoff": {
                "max_attempts": spec.backoff.max_attempts,
                "base_delay": spec.backoff.base_delay,
                "multiplier": spec.backoff.multiplier,
                "max_delay": spec.backoff.max_delay,
            },
            "idempotency_key": spec.idempotency_key,
        },
        "state": rec.state.value,
        "created_at": rec.created_at,
        "updated_at": rec.updated_at,
        "seq": rec.seq,
        "attempts": [_encode_attempt(a) for a in rec.attempts],
        "result": rec.result,
        "error": rec.error,
        "error_type": rec.error_type,
        "skip_reason": rec.skip_reason,
        "cancel_reason": rec.cancel_reason,
        "cancelled": rec.cancelled,
        "not_before": rec.not_before,
        "run_token": rec.run_token,
    }


def _decode_record(d: dict) -> TaskRecord:
    s = d["spec"]
    spec = TaskSpec(
        id=s["id"],
        handler=s["handler"],
        deps=frozenset(s.get("deps", [])),
        payload=s.get("payload"),
        priority=s["priority"],
        mutex_group=s.get("mutex_group"),
        dep_fail_policy=DepFailPolicy(s["dep_fail_policy"]),
        max_attempts=s["max_attempts"],
        timeout=s.get("timeout"),
        backoff=BackoffConfig(**s["backoff"]),
        idempotency_key=s.get("idempotency_key"),
    )
    return TaskRecord(
        id=d["id"],
        spec=spec,
        state=TaskState(d["state"]),
        created_at=d["created_at"],
        updated_at=d["updated_at"],
        seq=d["seq"],
        attempts=[_decode_attempt(a) for a in d.get("attempts", [])],
        result=d.get("result"),
        error=d.get("error"),
        error_type=d.get("error_type"),
        skip_reason=d.get("skip_reason"),
        cancel_reason=d.get("cancel_reason"),
        cancelled=bool(d.get("cancelled")),
        not_before=d.get("not_before", 0.0),
        run_token=d.get("run_token", 0),
    )


def _decode_event(row: tuple) -> TaskEvent:
    _id, task_id, seq, etype, ts, attempt, detail, payload = row
    return TaskEvent(
        id=_id,
        task_id=task_id,
        seq=seq,
        type=EventType(etype),
        timestamp=ts,
        attempt=attempt,
        detail=detail,
        payload=_loads(payload) if payload is not None else None,
    )


class SqliteTaskStore:
    """Durable task store backed by a single SQLite database file."""

    def __init__(self, path: str, clock: Callable[[], float] = time.time):
        self._path = path
        self._clock = clock
        self._lock = asyncio.Lock()
        if path != ":memory:":
            directory = os.path.dirname(os.path.abspath(path))
            os.makedirs(directory, exist_ok=True)
        self._conn = sqlite3.connect(
            path, check_same_thread=False, isolation_level=None
        )
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(SCHEMA)

    async def aclose(self) -> None:
        async with self._lock:
            self._conn.close()

    # ------------------------------------------------------------------ #
    def _next_meta(self, conn: sqlite3.Connection, key: str) -> int:
        row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        value = (row[0] if row else 0) + 1
        conn.execute(
            "INSERT INTO meta(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        return value

    def _insert_event(
        self,
        conn: sqlite3.Connection,
        task_id: str,
        seq: int,
        etype: EventType,
        detail: str | None,
        payload: Any,
        attempt: int | None,
    ) -> TaskEvent:
        eid = self._next_meta(conn, "event_id")
        ts = self._clock()
        conn.execute(
            "INSERT INTO events(id, task_id, seq, type, ts, attempt, detail, payload)"
            " VALUES(?,?,?,?,?,?,?,?)",
            (
                eid,
                task_id,
                seq,
                etype.value,
                ts,
                attempt,
                detail,
                _dumps(payload) if payload is not None else None,
            ),
        )
        return TaskEvent(
            id=eid,
            task_id=task_id,
            seq=seq,
            type=etype,
            timestamp=ts,
            attempt=attempt,
            detail=detail,
            payload=payload,
        )

    # ------------------------------------------------------------------ #
    async def create_task(self, record: TaskRecord) -> TaskRecord:
        async with self._lock:
            return await asyncio.to_thread(self._create_task, record)

    def _create_task(self, record: TaskRecord) -> TaskRecord:
        conn = self._conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            if conn.execute(
                "SELECT 1 FROM tasks WHERE id=?", (record.id,)
            ).fetchone():
                raise DuplicateTaskError(
                    f"task {record.id!r} already exists", record.id
                )
            key = record.spec.idempotency_key
            if key is not None:
                if conn.execute(
                    "SELECT task_id FROM idempotency WHERE key=?", (key,)
                ).fetchone():
                    row = conn.execute(
                        "SELECT task_id FROM idempotency WHERE key=?", (key,)
                    ).fetchone()
                    raise DuplicateTaskError(
                        f"idempotency key {key!r} already used", row[0]
                    )
            seq = self._next_meta(conn, "seq")
            record.seq = seq
            conn.execute(
                "INSERT INTO tasks(id, seq, state, idempotency_key, data,"
                " created_at, updated_at) VALUES(?,?,?,?,?,?,?)",
                (
                    record.id,
                    seq,
                    record.state.value,
                    key,
                    _dumps(_encode_record(record)),
                    record.created_at,
                    record.updated_at,
                ),
            )
            if key is not None:
                conn.execute(
                    "INSERT INTO idempotency(key, task_id) VALUES(?,?)",
                    (key, record.id),
                )
            self._insert_event(
                conn, record.id, 1, EventType.CREATED,
                detail=f"handler={record.spec.handler}", payload=None, attempt=None,
            )
            conn.execute("COMMIT")
            return _decode_record(_loads(_dumps(_encode_record(record))))
        except DuplicateTaskError:
            conn.execute("ROLLBACK")
            raise
        except Exception:
            conn.execute("ROLLBACK")
            raise

    async def get_record(self, task_id: str) -> TaskRecord | None:
        async with self._lock:
            return await asyncio.to_thread(self._get_record, task_id)

    def _get_record(self, task_id: str) -> TaskRecord | None:
        row = self._conn.execute(
            "SELECT data FROM tasks WHERE id=?", (task_id,)
        ).fetchone()
        return _decode_record(_loads(row[0])) if row else None

    def _fetch_events(self, conn: sqlite3.Connection, task_id: str) -> list[TaskEvent]:
        rows = conn.execute(
            "SELECT id, task_id, seq, type, ts, attempt, detail, payload"
            " FROM events WHERE task_id=? ORDER BY id",
            (task_id,),
        ).fetchall()
        return [_decode_event(r) for r in rows]

    async def get_snapshot(self, task_id: str) -> TaskSnapshot | None:
        async with self._lock:
            return await asyncio.to_thread(self._get_snapshot, task_id)

    def _get_snapshot(self, task_id: str) -> TaskSnapshot | None:
        conn = self._conn
        conn.execute("BEGIN")
        try:
            row = conn.execute(
                "SELECT data FROM tasks WHERE id=?", (task_id,)
            ).fetchone()
            if not row:
                conn.execute("COMMIT")
                return None
            record = _decode_record(_loads(row[0]))
            events = tuple(self._fetch_events(conn, task_id))
            conn.execute("COMMIT")
            return TaskSnapshot(record=record, events=events)
        except Exception:
            conn.execute("ROLLBACK")
            raise

    async def list_snapshots(
        self, limit: int = 100, offset: int = 0
    ) -> list[TaskSnapshot]:
        async with self._lock:
            return await asyncio.to_thread(self._list_snapshots, limit, offset)

    def _list_snapshots(self, limit: int, offset: int) -> list[TaskSnapshot]:
        conn = self._conn
        conn.execute("BEGIN")
        try:
            rows = conn.execute(
                "SELECT id FROM tasks ORDER BY seq LIMIT ? OFFSET ?",
                (limit, offset),
            ).fetchall()
            out: list[TaskSnapshot] = []
            for (task_id,) in rows:
                data = conn.execute(
                    "SELECT data FROM tasks WHERE id=?", (task_id,)
                ).fetchone()
                out.append(
                    TaskSnapshot(
                        record=_decode_record(_loads(data[0])),
                        events=tuple(self._fetch_events(conn, task_id)),
                    )
                )
            conn.execute("COMMIT")
            return out
        except Exception:
            conn.execute("ROLLBACK")
            raise

    async def list_by_idempotency_key(self, key: str) -> TaskRecord | None:
        async with self._lock:
            return await asyncio.to_thread(self._list_by_idempotency_key, key)

    def _list_by_idempotency_key(self, key: str) -> TaskRecord | None:
        row = self._conn.execute(
            "SELECT t.data FROM idempotency i JOIN tasks t ON t.id=i.task_id"
            " WHERE i.key=?",
            (key,),
        ).fetchone()
        return _decode_record(_loads(row[0])) if row else None

    async def all_records(self) -> list[TaskRecord]:
        async with self._lock:
            return await asyncio.to_thread(self._all_records)

    def _all_records(self) -> list[TaskRecord]:
        rows = self._conn.execute("SELECT data FROM tasks ORDER BY seq").fetchall()
        return [_decode_record(_loads(r[0])) for r in rows]

    async def compare_and_set_state(
        self,
        task_id: str,
        expected,
        target,
        event,
        *,
        mutate=None,
        detail=None,
        payload=None,
        attempt=None,
    ):
        async with self._lock:
            return await asyncio.to_thread(
                self._compare_and_set_state,
                task_id,
                expected,
                target,
                event,
                mutate,
                detail,
                payload,
                attempt,
            )

    def _compare_and_set_state(
        self, task_id, expected, target, event, mutate, detail, payload, attempt
    ) -> TaskRecord | None:
        conn = self._conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT data FROM tasks WHERE id=?", (task_id,)
            ).fetchone()
            if not row:
                conn.execute("COMMIT")
                return None
            record = _decode_record(_loads(row[0]))
            if expected is not None and (record.state, record.run_token) != tuple(
                expected
            ):
                conn.execute("COMMIT")
                return None
            if mutate is not None:
                mutate(record)
            record.state = target
            record.updated_at = self._clock()
            next_seq = conn.execute(
                "SELECT COALESCE(MAX(seq), 0) + 1 FROM events WHERE task_id=?",
                (task_id,),
            ).fetchone()[0]
            self._insert_event(
                conn, task_id, next_seq, event,
                detail=detail, payload=payload, attempt=attempt,
            )
            conn.execute(
                "UPDATE tasks SET state=?, data=?, updated_at=? WHERE id=?",
                (
                    target.value,
                    _dumps(_encode_record(record)),
                    record.updated_at,
                    task_id,
                ),
            )
            conn.execute("COMMIT")
            return record
        except Exception:
            conn.execute("ROLLBACK")
            raise

    async def append_event(
        self, task_id, event, *, detail=None, payload=None, attempt=None
    ):
        async with self._lock:
            return await asyncio.to_thread(
                self._append_event, task_id, event, detail, payload, attempt
            )

    def _append_event(self, task_id, event, detail, payload, attempt) -> TaskEvent | None:
        conn = self._conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            if not conn.execute(
                "SELECT 1 FROM tasks WHERE id=?", (task_id,)
            ).fetchone():
                conn.execute("COMMIT")
                return None
            next_seq = conn.execute(
                "SELECT COALESCE(MAX(seq), 0) + 1 FROM events WHERE task_id=?",
                (task_id,),
            ).fetchone()[0]
            stored = self._insert_event(
                conn, task_id, next_seq, event,
                detail=detail, payload=payload, attempt=attempt,
            )
            conn.execute("COMMIT")
            return stored
        except Exception:
            conn.execute("ROLLBACK")
            raise
