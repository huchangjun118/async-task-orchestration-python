"""SQLite-backed durable store for task instances and transition history.

Design notes:
- A single connection guarded by an RLock serialises all access, so every
  read is a consistent snapshot and every compare-and-swap transition is
  atomic.
- Every state change appends an immutable row to `transitions`, giving us
  both the audit history and the data needed for crash recovery.
- WAL mode is used for file-backed databases.
"""
from __future__ import annotations

import json
import sqlite3
import threading
from typing import Any, Optional

from .models import (
    RunSnapshot,
    TaskInstance,
    TaskSnapshot,
    TaskState,
    TransitionRecord,
    now,
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS idempotency (
    key        TEXT PRIMARY KEY,
    run_id     TEXT NOT NULL,
    created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS instances (
    instance_id TEXT PRIMARY KEY,
    run_id      TEXT NOT NULL,
    task_name   TEXT NOT NULL,
    state       TEXT NOT NULL,
    attempt     INTEGER NOT NULL DEFAULT 0,
    payload     TEXT,
    result      TEXT,
    error       TEXT,
    skip_reason TEXT,
    created_at  REAL NOT NULL,
    updated_at  REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_instances_run ON instances(run_id);
CREATE INDEX IF NOT EXISTS idx_instances_state ON instances(state);
CREATE TABLE IF NOT EXISTS transitions (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id TEXT NOT NULL,
    run_id      TEXT NOT NULL,
    task_name   TEXT NOT NULL,
    from_state  TEXT,
    to_state    TEXT NOT NULL,
    attempt     INTEGER NOT NULL,
    reason      TEXT,
    at          REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_transitions_inst ON transitions(instance_id, seq);
"""


def _dump(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        return json.dumps(value)
    except (TypeError, ValueError):
        return json.dumps(repr(value))


def _load(raw: Optional[str]) -> Any:
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return raw


class TaskStore:
    def __init__(self, db_path: str = ":memory:") -> None:
        self._db_path = db_path
        self._lock = threading.RLock()
        self._conn: Optional[sqlite3.Connection] = None

    def open(self) -> None:
        if self._conn is not None:
            return
        conn = sqlite3.connect(self._db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.executescript(_SCHEMA)
        conn.commit()
        self._conn = conn

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    def _c(self) -> sqlite3.Connection:
        assert self._conn is not None, "store not opened"
        return self._conn

    # ---- idempotency ----
    def claim_idempotency_key(self, key: str, run_id: str) -> bool:
        """Return True if this call created the key (i.e. first trigger)."""
        with self._lock:
            cur = self._c().execute(
                "INSERT OR IGNORE INTO idempotency(key, run_id, created_at)"
                " VALUES (?, ?, ?)",
                (key, run_id, now()),
            )
            self._c().commit()
            return cur.rowcount == 1

    def run_id_for_key(self, key: str) -> Optional[str]:
        with self._lock:
            row = self._c().execute(
                "SELECT run_id FROM idempotency WHERE key = ?", (key,)
            ).fetchone()
            return row["run_id"] if row else None

    # ---- instances ----
    def insert_instance(self, inst: TaskInstance) -> None:
        with self._lock:
            self._c().execute(
                "INSERT INTO instances(instance_id, run_id, task_name, state,"
                " attempt, payload, result, error, skip_reason, created_at,"
                " updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    inst.instance_id,
                    inst.run_id,
                    inst.task_name,
                    inst.state.value,
                    inst.attempt,
                    _dump(inst.payload),
                    _dump(inst.result),
                    inst.error,
                    inst.skip_reason,
                    inst.created_at,
                    inst.updated_at,
                ),
            )
            self._append_transition(
                inst.instance_id, inst.run_id, inst.task_name,
                None, inst.state, inst.attempt, "created",
            )
            self._c().commit()

    def _row_to_instance(self, row: sqlite3.Row) -> TaskInstance:
        return TaskInstance(
            instance_id=row["instance_id"],
            run_id=row["run_id"],
            task_name=row["task_name"],
            state=TaskState(row["state"]),
            attempt=row["attempt"],
            payload=_load(row["payload"]),
            result=_load(row["result"]),
            error=row["error"],
            skip_reason=row["skip_reason"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def get_instance(self, instance_id: str) -> Optional[TaskInstance]:
        with self._lock:
            row = self._c().execute(
                "SELECT * FROM instances WHERE instance_id = ?", (instance_id,)
            ).fetchone()
            return self._row_to_instance(row) if row else None

    def instances_of_run(self, run_id: str) -> list[TaskInstance]:
        with self._lock:
            rows = self._c().execute(
                "SELECT * FROM instances WHERE run_id = ? ORDER BY created_at, task_name",
                (run_id,),
            ).fetchall()
            return [self._row_to_instance(r) for r in rows]

    def _append_transition(
        self,
        instance_id: str,
        run_id: str,
        task_name: str,
        from_state: Optional[TaskState],
        to_state: TaskState,
        attempt: int,
        reason: Optional[str],
    ) -> None:
        self._c().execute(
            "INSERT INTO transitions(instance_id, run_id, task_name,"
            " from_state, to_state, attempt, reason, at)"
            " VALUES (?,?,?,?,?,?,?,?)",
            (
                instance_id,
                run_id,
                task_name,
                from_state.value if from_state else None,
                to_state.value,
                attempt,
                reason,
                now(),
            ),
        )

    def transition(
        self,
        instance_id: str,
        to_state: TaskState,
        *,
        expected: tuple[TaskState, ...] | None = None,
        attempt: int | None = None,
        reason: str | None = None,
        result: Any = None,
        error: str | None = None,
        skip_reason: str | None = None,
        clear_result: bool = False,
    ) -> bool:
        """Atomically move an instance to `to_state` and append history.

        If `expected` is given, the transition only happens when the current
        state is in `expected` (compare-and-swap). Returns success.
        """
        with self._lock:
            conn = self._c()
            try:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    "SELECT * FROM instances WHERE instance_id = ?",
                    (instance_id,),
                ).fetchone()
                if row is None:
                    conn.execute("ROLLBACK")
                    return False
                current = TaskState(row["state"])
                if expected is not None and current not in expected:
                    conn.execute("ROLLBACK")
                    return False
                new_attempt = row["attempt"] if attempt is None else attempt
                new_result = row["result"] if (result is None and not clear_result) else _dump(result)
                new_error = row["error"] if error is None else error
                new_skip = row["skip_reason"] if skip_reason is None else skip_reason
                conn.execute(
                    "UPDATE instances SET state=?, attempt=?, result=?,"
                    " error=?, skip_reason=?, updated_at=?"
                    " WHERE instance_id=?",
                    (
                        to_state.value,
                        new_attempt,
                        new_result,
                        new_error,
                        new_skip,
                        now(),
                        instance_id,
                    ),
                )
                self._append_transition(
                    instance_id,
                    row["run_id"],
                    row["task_name"],
                    current,
                    to_state,
                    new_attempt,
                    reason,
                )
                conn.execute("COMMIT")
                return True
            except Exception:
                conn.execute("ROLLBACK")
                raise

    # ---- queries ----
    def snapshot(self, instance_id: str) -> Optional[TaskSnapshot]:
        inst = self.get_instance(instance_id)
        if inst is None:
            return None
        return TaskSnapshot(
            instance_id=inst.instance_id,
            run_id=inst.run_id,
            task_name=inst.task_name,
            state=inst.state,
            attempt=inst.attempt,
            result=inst.result,
            error=inst.error,
            skip_reason=inst.skip_reason,
            created_at=inst.created_at,
            updated_at=inst.updated_at,
        )

    def run_snapshot(self, run_id: str) -> Optional[RunSnapshot]:
        with self._lock:
            insts = self.instances_of_run(run_id)
            if not insts:
                return None
            snaps = tuple(self.snapshot(i.instance_id) for i in insts)
            return RunSnapshot(run_id=run_id, tasks=snaps)  # type: ignore[arg-type]

    def history(self, instance_id: str) -> list[TransitionRecord]:
        with self._lock:
            rows = self._c().execute(
                "SELECT * FROM transitions WHERE instance_id = ? ORDER BY seq",
                (instance_id,),
            ).fetchall()
            return [
                TransitionRecord(
                    seq=r["seq"],
                    instance_id=r["instance_id"],
                    run_id=r["run_id"],
                    task_name=r["task_name"],
                    from_state=TaskState(r["from_state"]) if r["from_state"] else None,
                    to_state=TaskState(r["to_state"]),
                    attempt=r["attempt"],
                    reason=r["reason"],
                    at=r["at"],
                )
                for r in rows
            ]

    def unfinished_instances(self) -> list[TaskInstance]:
        """All instances in non-terminal states (for recovery)."""
        with self._lock:
            terminals = tuple(s.value for s in (
                TaskState.SUCCEEDED, TaskState.FAILED, TaskState.TIMED_OUT,
                TaskState.CANCELLED, TaskState.SKIPPED,
            ))
            rows = self._c().execute(
                f"SELECT * FROM instances WHERE state NOT IN"
                f" ({','.join('?' * len(terminals))}) ORDER BY created_at",
                terminals,
            ).fetchall()
            return [self._row_to_instance(r) for r in rows]
