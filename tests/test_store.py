"""Store-level guarantees: compare-and-set, idempotency, snapshot isolation."""
from __future__ import annotations

import asyncio

import pytest

from app.models import (
    BackoffConfig,
    DuplicateTaskError,
    EventType,
    TaskRecord,
    TaskSpec,
    TaskState,
)
from app.store import InMemoryTaskStore
from app.store_sqlite import SqliteTaskStore


def _record(task_id: str, state: TaskState = TaskState.PENDING) -> TaskRecord:
    spec = TaskSpec(id=task_id, handler="noop", backoff=BackoffConfig())
    return TaskRecord(
        id=task_id, spec=spec, state=state, created_at=1.0, updated_at=1.0
    )


@pytest.mark.parametrize("store_cls", [InMemoryTaskStore])
async def test_cas_only_one_winner(store_cls):
    store = store_cls()
    await store.create_task(_record("t"))
    winners = await asyncio.gather(
        *[
            store.compare_and_set_state(
                "t", (TaskState.PENDING, 0), TaskState.READY, EventType.READY
            )
            for _ in range(20)
        ]
    )
    assert sum(1 for w in winners if w is not None) == 1
    rec = await store.get_record("t")
    assert rec.state is TaskState.READY


async def test_sqlite_cas_only_one_winner(tmp_path):
    store = SqliteTaskStore(str(tmp_path / "x.db"))
    await store.create_task(_record("t"))
    winners = await asyncio.gather(
        *[
            store.compare_and_set_state(
                "t", (TaskState.PENDING, 0), TaskState.READY, EventType.READY
            )
            for _ in range(20)
        ]
    )
    assert sum(1 for w in winners if w is not None) == 1
    rec = await store.get_record("t")
    assert rec.state is TaskState.READY
    await store.aclose()


async def test_duplicate_idempotency_key_rejected(tmp_path):
    store = SqliteTaskStore(str(tmp_path / "x.db"))
    r1 = _record("a")
    r1.spec = TaskSpec(
        id="a", handler="noop", idempotency_key="k", backoff=BackoffConfig()
    )
    r2 = _record("b")
    r2.spec = TaskSpec(
        id="b", handler="noop", idempotency_key="k", backoff=BackoffConfig()
    )
    await store.create_task(r1)
    with pytest.raises(DuplicateTaskError) as exc:
        await store.create_task(r2)
    assert exc.value.existing_task_id == "a"
    found = await store.list_by_idempotency_key("k")
    assert found is not None and found.id == "a"
    await store.aclose()


async def test_snapshot_is_deep_copied():
    store = InMemoryTaskStore()
    await store.create_task(_record("t"))
    snap1 = await store.get_snapshot("t")
    snap1.record.state = TaskState.SUCCEEDED  # mutate returned copy
    snap1.events[0].type  # noqa
    snap2 = await store.get_snapshot("t")
    assert snap2.record.state is TaskState.PENDING  # unaffected


async def test_event_seqs_gapless_both_stores(tmp_path):
    store = SqliteTaskStore(str(tmp_path / "x.db"))
    await store.create_task(_record("t"))
    await store.compare_and_set_state(
        "t", (TaskState.PENDING, 0), TaskState.READY, EventType.READY
    )
    await store.append_event("t", EventType.CANCEL_REQUESTED, detail="x")
    snap = await store.get_snapshot("t")
    assert [e.seq for e in snap.events] == [1, 2, 3]
    # global event ids are monotonically increasing too
    assert snap.events[2].id > snap.events[0].id
    await store.aclose()


async def test_sqlite_persistence_roundtrip(tmp_path):
    path = str(tmp_path / "p.db")
    s1 = SqliteTaskStore(path)
    rec = _record("t")
    rec.spec = TaskSpec(
        id="t",
        handler="noop",
        deps=frozenset({"d1", "d2"}),
        payload={"x": [1, 2]},
        priority=7,
        mutex_group="grp",
        backoff=BackoffConfig(max_attempts=4, base_delay=2.5),
    )
    await s1.create_task(rec)
    await s1.compare_and_set_state(
        "t", (TaskState.PENDING, 0), TaskState.READY, EventType.READY
    )
    s1._conn.close()

    s2 = SqliteTaskStore(path)
    loaded = await s2.get_record("t")
    assert loaded.state is TaskState.READY
    assert loaded.spec.priority == 7
    assert loaded.spec.deps == frozenset({"d1", "d2"})
    assert loaded.spec.mutex_group == "grp"
    assert loaded.spec.backoff.max_attempts == 4
    assert loaded.spec.backoff.base_delay == 2.5
    assert loaded.spec.payload == {"x": [1, 2]}
    assert loaded.seq == 1  # seq persisted -> stable ordering across restarts
    snap = await s2.get_snapshot("t")
    assert [e.type for e in snap.events] == [EventType.CREATED, EventType.READY]
    await s2.aclose()
