"""Child process for the restart-recovery test.

Starts an engine on a file-backed store, triggers one task that completes
and one that blocks forever, waits until the blocker is RUNNING, records
the run ids, then dies abruptly via os._exit (simulating power loss).
"""
import asyncio
import json
import os
import sys


async def main(db_path: str, marker_path: str) -> None:
    from orchestrator import OrchestrationEngine, TaskDefinition, TaskRegistry, TaskStore

    reg = TaskRegistry()

    async def done_task(i, t):
        return "finished"

    async def interrupted(i, t):
        await asyncio.sleep(3600)
        return "unreachable"

    reg.register(TaskDefinition("done", done_task))
    reg.register(TaskDefinition("interrupted", interrupted))
    eng = OrchestrationEngine(TaskStore(db_path), reg)
    await eng.start()
    r_done = await eng.trigger("done")
    r_int = await eng.trigger("interrupted")
    await eng.wait_run_done(r_done.run_id)
    for _ in range(500):
        snap = eng.get_task(f"{r_int.run_id}:interrupted")
        if snap and snap.state.value == "RUNNING":
            break
        await asyncio.sleep(0.01)
    with open(marker_path, "w") as f:
        json.dump({"run_done": r_done.run_id, "run_int": r_int.run_id}, f)
    os._exit(1)  # simulated crash: no cleanup, RUNNING row left behind


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1], sys.argv[2]))
