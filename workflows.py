import asyncio
import os
from datetime import timedelta
from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activities import (load_plan, inspect_enrich, start_enrich, save_enrich_checkpoint,
                            start_reindex, check_task, validate_reindex)

A_TIMEOUT = timedelta(minutes=2)
LONG_RETRY_TIMEOUT = timedelta(minutes=10)

async def _wait_task(task_id: str):
    while True:
        state = await workflow.execute_activity(check_task, {"task_id": task_id}, start_to_close_timeout=A_TIMEOUT)
        if state["completed"]:
            return state
        await workflow.sleep(timedelta(seconds=int(os.getenv("POLL_SECONDS", "15"))))

async def _run_reindex(command, dry_run):
    started = await workflow.execute_activity(start_reindex, {"command": command, "dry_run": dry_run},
                                              start_to_close_timeout=A_TIMEOUT)
    if started.get("dry_run"):
        return {"command": command["name"], "status": "DRY_RUN"}
    result = await _wait_task(started["task"])
    return await workflow.execute_activity(validate_reindex, {"command": command, "task_result": result},
                                           start_to_close_timeout=A_TIMEOUT)

async def _run_enrich(policy, dry_run):
    inspected = await workflow.execute_activity(inspect_enrich, policy, start_to_close_timeout=A_TIMEOUT)
    if not inspected["changed"]:
        return {"name": policy["name"], "status": "SKIPPED_UNCHANGED"}
    started = await workflow.execute_activity(start_enrich, {"name": policy["name"], "dry_run": dry_run},
                                              start_to_close_timeout=LONG_RETRY_TIMEOUT)
    if not dry_run:
        task_id = started.get("task")
        if task_id:
            await _wait_task(task_id)
        elif not started.get("completed"):
            raise RuntimeError(f"Unexpected enrich response for {policy['name']}: {started}")
        await workflow.execute_activity(save_enrich_checkpoint, inspected, start_to_close_timeout=A_TIMEOUT)
    return {"name": policy["name"], "status": "DRY_RUN" if dry_run else "SUCCESS"}

async def _bounded(items, fn, limit):
    semaphore = asyncio.Semaphore(limit)
    async def one(item):
        async with semaphore:
            return await fn(item)
    return await asyncio.gather(*(one(i) for i in items))

@workflow.defn(name="VsecaElasticReportingWorkflow")
class VsecaElasticReportingWorkflow:
    @workflow.run
    async def run(self, inp: dict):
        config_path = inp["config_path"]
        dry_run = inp.get("dry_run", True)
        plan = await workflow.execute_activity(load_plan, config_path, start_to_close_timeout=A_TIMEOUT)
        limit = int(inp.get("max_parallel_reindexes", 3))

        # Stage 1 and enrich refresh are independent, so execute them in parallel.
        staging_future = asyncio.create_task(_bounded(plan["backend-to-staging"], lambda c: _run_reindex(c, dry_run), limit))
        enrich_future = asyncio.create_task(_bounded(plan["enrich"], lambda p: _run_enrich(p, dry_run), limit))
        staging, enrich = await asyncio.gather(staging_future, enrich_future)

        # Barrier: every required staging command and enrich policy succeeded or was safely skipped.
        reporting = await _bounded(plan["staging-to-reporting"], lambda c: _run_reindex(c, dry_run), 2)

        # Stage 3 begins only after all Stage 2 commands succeed.
        history = await _bounded(plan["reporting-to-history"], lambda c: _run_reindex(c, dry_run), 2)
        return {"dry_run": dry_run, "staging": staging, "enrich": enrich,
                "reporting": reporting, "history": history, "unmanaged": [c["name"] for c in plan["unmanaged"]]}
