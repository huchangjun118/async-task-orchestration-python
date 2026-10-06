"""HTTP surface: trigger, workflow, query, history, cancel, error codes."""
from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient

from app.api import create_app
from app.orchestrator import Orchestrator


@pytest.fixture
def client():
    orch = Orchestrator(max_concurrency=4)
    application = create_app(orch)
    with TestClient(application) as c:
        yield c


def _wait_json_state(client, task_id, states, tries=500):
    for _ in range(tries):
        data = client.get(f"/tasks/{task_id}").json()
        if data["state"] in states:
            return data
        time.sleep(0.01)
    raise AssertionError(f"{task_id} never reached {states}")


def test_root_lists_endpoints(client):
    data = client.get("/").json()
    assert "endpoints" in data
    assert "GET /tasks/{id}/history" in data["endpoints"]


def test_trigger_and_get_status(client):
    r = client.post("/tasks", json={"handler": "noop", "task_id": "t1"})
    assert r.status_code == 201
    body = r.json()
    assert body["id"] == "t1" and body["state"] == "PENDING"
    data = _wait_json_state(client, "t1", {"SUCCEEDED"})
    assert data["result"] == {"payload": None, "attempt": 1}
    assert data["attempts"] == 1


def test_workflow_with_dependency_failure_skip(client):
    r = client.post(
        "/workflows",
        json={
            "tasks": [
                {"id": "f", "handler": "fail"},
                {"id": "s", "handler": "noop", "deps": ["f"]},
            ]
        },
    )
    assert r.status_code == 201
    assert sorted(r.json()["task_ids"]) == ["f", "s"]
    s = _wait_json_state(client, "s", {"SKIPPED"})
    assert s["skip_reason"] and "FAILED" in s["skip_reason"]


def test_history_endpoint_replays_full_chain(client):
    client.post(
        "/tasks",
        json={
            "handler": "flaky",
            "task_id": "hx",
            "payload": {"fail_times": 1},
            "max_attempts": 2,
            "backoff": {"base_delay": 0.01},
        },
    )
    _wait_json_state(client, "hx", {"SUCCEEDED"})
    hist = client.get("/tasks/hx/history").json()
    types = [e["type"] for e in hist["events"]]
    assert types[0] == "CREATED"
    assert types[-1] == "SUCCEEDED"
    assert "FAILED" in types and "RETRY_SCHEDULED" in types
    # seq numbers are gapless and ordered
    seqs = [e["seq"] for e in hist["events"]]
    assert seqs == list(range(1, len(seqs) + 1))
    # the failed event belongs to attempt 1, success to attempt 2
    failed = next(e for e in hist["events"] if e["type"] == "FAILED")
    success = next(e for e in hist["events"] if e["type"] == "SUCCEEDED")
    assert failed["attempt"] == 1 and success["attempt"] == 2


def test_cancel_endpoint(client):
    client.post(
        "/tasks",
        json={"handler": "sleep", "task_id": "long", "payload": {"seconds": 30}},
    )
    _wait_json_state(client, "long", {"RUNNING"})
    r = client.post("/tasks/long/cancel", json={"reason": "via api"})
    assert r.status_code == 200
    assert r.json()["state"] == "CANCELLED"
    assert r.json()["cancel_reason"] == "via api"
    time.sleep(0.05)
    assert client.get("/tasks/long").json()["state"] == "CANCELLED"


def test_error_codes(client):
    assert client.get("/tasks/nope").status_code == 404
    assert client.post("/tasks/nope/cancel").status_code == 404
    # unknown dependency
    r = client.post("/tasks", json={"handler": "noop", "deps": ["ghost"]})
    assert r.status_code == 400
    # cycle
    r = client.post(
        "/workflows",
        json={
            "tasks": [
                {"id": "a", "handler": "noop", "deps": ["b"]},
                {"id": "b", "handler": "noop", "deps": ["a"]},
            ]
        },
    )
    assert r.status_code == 400
    # invalid policy -> schema validation error
    r = client.post(
        "/tasks", json={"handler": "noop", "dep_fail_policy": "WAT"}
    )
    assert r.status_code == 422
    # unknown dependency remains a 400 business error
    r2 = client.post(
        "/tasks", json={"handler": "noop", "task_id": "x9", "deps": ["ghost"]}
    )
    assert r2.status_code == 400


def test_list_pagination_is_stable(client):
    for i in range(5):
        client.post("/tasks", json={"handler": "noop", "task_id": f"p{i}"})
    page1 = client.get("/tasks?limit=2&offset=0").json()
    page2 = client.get("/tasks?limit=2&offset=2").json()
    ids = [t["id"] for t in page1] + [t["id"] for t in page2]
    assert ids == ["p0", "p1", "p2", "p3"]


def test_idempotent_duplicate_post_is_201_same_id(client):
    a = client.post(
        "/tasks", json={"handler": "noop", "idempotency_key": "k"}
    )
    b = client.post(
        "/tasks", json={"handler": "noop", "idempotency_key": "k"}
    )
    assert a.status_code == 201 and b.status_code == 201
    assert a.json()["id"] == b.json()["id"]


def test_snapshot_state_is_always_one_of_valid_states(client):
    client.post(
        "/tasks",
        json={
            "handler": "flaky",
            "task_id": "snap",
            "payload": {"fail_times": 2},
            "max_attempts": 3,
            "backoff": {"base_delay": 0.005},
        },
    )
    valid = {
        "PENDING", "READY", "RETRY_WAIT", "RUNNING", "SUCCEEDED",
        "FAILED", "SKIPPED", "CANCELLED", "TIMED_OUT",
    }
    for _ in range(300):
        data = client.get("/tasks/snap").json()
        assert data["state"] in valid
        # history length is always internally consistent (gapless seq)
        hist = client.get("/tasks/snap/history").json()["events"]
        assert [e["seq"] for e in hist] == list(range(1, len(hist) + 1))
        if data["state"] == "SUCCEEDED":
            break
        time.sleep(0.005)
    else:
        raise AssertionError("never succeeded")
