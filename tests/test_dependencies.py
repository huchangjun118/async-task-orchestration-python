"""Dependency execution, failure propagation and skip/cancel policies."""
from __future__ import annotations

from app.models import DepFailPolicy, TaskState

from .conftest import wait_state


async def test_success_chain_runs_in_order(orch):
    await orch.trigger_workflow(
        [
            {"id": "a", "handler": "noop"},
            {"id": "b", "handler": "noop", "deps": ["a"]},
            {"id": "c", "handler": "noop", "deps": ["a", "b"]},
        ]
    )
    for tid in ("a", "b", "c"):
        rec = await wait_state(orch, tid)
        assert rec.state is TaskState.SUCCEEDED

    c = await orch.get_status("c")
    assert c.events[-1].type.value == "SUCCEEDED"


async def test_failed_predecessor_skips_successor_with_reason(orch):
    await orch.trigger_workflow(
        [
            {"id": "f", "handler": "fail"},
            {
                "id": "s",
                "handler": "noop",
                "deps": ["f"],
                "dep_fail_policy": DepFailPolicy.SKIP,
            },
        ]
    )
    rec = await wait_state(orch, "s")
    assert rec.state is TaskState.SKIPPED
    assert rec.skip_reason is not None
    assert "f" in rec.skip_reason and "FAILED" in rec.skip_reason
    # the successor handler must never have run
    assert rec.attempts == []
    hist = await orch.get_history("s")
    assert [e.type.value for e in hist] == ["CREATED", "SKIPPED"]


async def test_failed_predecessor_cancels_successor_when_policy_cancel(orch):
    await orch.trigger_workflow(
        [
            {"id": "f", "handler": "fail"},
            {
                "id": "z",
                "handler": "noop",
                "deps": ["f"],
                "dep_fail_policy": DepFailPolicy.CANCEL,
            },
        ]
    )
    rec = await wait_state(orch, "z")
    assert rec.state is TaskState.CANCELLED
    assert "f" in rec.cancel_reason


async def test_failure_cascades_through_diamond(orch):
    # root fails; every descendant must terminate (skip by default)
    await orch.trigger_workflow(
        [
            {"id": "root", "handler": "fail"},
            {"id": "m1", "handler": "noop", "deps": ["root"]},
            {"id": "m2", "handler": "noop", "deps": ["root"]},
            {"id": "leaf", "handler": "noop", "deps": ["m1", "m2"]},
        ]
    )
    for tid in ("m1", "m2", "leaf"):
        rec = await wait_state(orch, tid)
        assert rec.state is TaskState.SKIPPED, tid
        assert rec.attempts == []


async def test_success_branch_continues_after_sibling_failure(orch):
    # diamond: root ok; one middle fails, the other succeeds;
    # leaf depends on both -> skipped; the succeeding middle still completes
    await orch.trigger_workflow(
        [
            {"id": "root", "handler": "noop"},
            {"id": "ok_mid", "handler": "noop", "deps": ["root"]},
            {"id": "bad_mid", "handler": "fail", "deps": ["root"]},
            {"id": "leaf", "handler": "noop", "deps": ["ok_mid", "bad_mid"]},
        ]
    )
    rec = await wait_state(orch, "leaf")
    assert rec.state is TaskState.SKIPPED
    assert "bad_mid" in rec.skip_reason
    ok_rec = (await orch.get_status("ok_mid")).record
    assert ok_rec.state is TaskState.SUCCEEDED


async def test_dependency_on_already_succeeded_task(orch):
    await orch.trigger("noop", task_id="done")
    await wait_state(orch, "done")
    await orch.trigger("noop", task_id="after", deps=["done"])
    rec = await wait_state(orch, "after")
    assert rec.state is TaskState.SUCCEEDED


async def test_unknown_dependency_rejected(orch):
    import pytest

    from app.models import UnknownDependencyError

    with pytest.raises(UnknownDependencyError):
        await orch.trigger("noop", task_id="x", deps=["ghost"])


async def test_dependency_cycle_rejected(orch):
    import pytest

    from app.models import DependencyCycleError

    with pytest.raises(DependencyCycleError):
        await orch.trigger_workflow(
            [
                {"id": "c1", "handler": "noop", "deps": ["c3"]},
                {"id": "c2", "handler": "noop", "deps": ["c1"]},
                {"id": "c3", "handler": "noop", "deps": ["c2"]},
            ]
        )
    # nothing created on validation failure
    assert await orch.list_tasks() == []
