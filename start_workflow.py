import argparse
import asyncio
import os
from datetime import datetime, timedelta, timezone

from temporalio.client import (
    Client,
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleOverlapPolicy,
    SchedulePolicy,
    ScheduleSpec,
    ScheduleUpdate,
)
from temporalio.service import RPCError

from workflows import VsecaElasticReportingWorkflow

DEFAULT_SCHEDULE_ID = "vseca-elastic-6h"
DEFAULT_CRON = "0 */6 * * *"


def build_input(args):
    return {
        "config_path": args.config,
        "dry_run": not args.execute,
        "max_parallel_reindexes": int(os.getenv("MAX_PARALLEL_REINDEXES", "3")),
        "poll_seconds": int(os.getenv("POLL_SECONDS", "15")),
    }


def build_schedule(args):
    action = ScheduleActionStartWorkflow(
        VsecaElasticReportingWorkflow.run,
        args=[build_input(args)],
        id=args.schedule_id,
        task_queue=os.getenv("TEMPORAL_TASK_QUEUE", "vseca-elastic-ops"),
    )
    spec = ScheduleSpec(cron_expressions=[args.cron], time_zone_name=args.timezone)
    policy = SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP, catchup_window=timedelta(hours=1))
    return Schedule(action=action, spec=spec, policy=policy)


async def upsert_schedule(client, args):
    handle = client.get_schedule_handle(args.schedule_id)
    try:
        await handle.describe()
        exists = True
    except RPCError:
        exists = False
    schedule = build_schedule(args)
    if exists:
        await handle.update(lambda _: ScheduleUpdate(schedule=schedule))
        print(f"Updated schedule '{args.schedule_id}' (cron='{args.cron}' tz={args.timezone})")
    else:
        await client.create_schedule(args.schedule_id, schedule)
        print(f"Created schedule '{args.schedule_id}' (cron='{args.cron}' tz={args.timezone})")


async def main():
    p = argparse.ArgumentParser()
    p.add_argument("--config", default=os.getenv("ELASTIC_CONFIG", "./elastic-files.yaml"))
    p.add_argument("--execute", action="store_true", help="Actually call Elasticsearch. Default is dry-run.")
    p.add_argument("--workflow-id")
    p.add_argument("--schedule", action="store_true", help="Create or update the recurring schedule and exit")
    p.add_argument("--schedule-id", default=os.getenv("SCHEDULE_ID", DEFAULT_SCHEDULE_ID))
    p.add_argument("--cron", default=os.getenv("SCHEDULE_CRON", DEFAULT_CRON))
    p.add_argument("--timezone", default=os.getenv("SCHEDULE_TIMEZONE", "UTC"))
    p.add_argument("--pause", action="store_true", help="Pause the schedule and exit")
    p.add_argument("--resume", action="store_true", help="Resume the schedule and exit")
    p.add_argument("--delete", action="store_true", help="Delete the schedule and exit")
    p.add_argument("--describe", action="store_true", help="Show the schedule and exit")
    p.add_argument("--trigger", action="store_true", help="Trigger a scheduled run now and exit")
    args = p.parse_args()

    client = await Client.connect(os.getenv("TEMPORAL_ADDRESS", "localhost:7233"),
                                  namespace=os.getenv("TEMPORAL_NAMESPACE", "default"))

    handle = client.get_schedule_handle(args.schedule_id)
    try:
        if args.delete:
            await handle.delete()
            print(f"Deleted schedule '{args.schedule_id}'")
            return
        if args.pause:
            await handle.pause(note="paused via start_workflow.py")
            print(f"Paused schedule '{args.schedule_id}'")
            return
        if args.resume:
            await handle.unpause(note="resumed via start_workflow.py")
            print(f"Resumed schedule '{args.schedule_id}'")
            return
        if args.trigger:
            await handle.trigger()
            print(f"Triggered schedule '{args.schedule_id}'")
            return
        if args.describe:
            print(await handle.describe())
            return
    except RPCError as exc:
        print(f"Schedule '{args.schedule_id}' error: {exc.message}")
        return
    if args.schedule:
        await upsert_schedule(client, args)
        return

    wid = args.workflow_id or "vseca-elastic-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    result = await client.execute_workflow(VsecaElasticReportingWorkflow.run, build_input(args),
                                           id=wid, task_queue=os.getenv("TEMPORAL_TASK_QUEUE", "vseca-elastic-ops"))
    print(result)


if __name__ == "__main__":
    asyncio.run(main())
