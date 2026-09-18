import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
from temporalio.client import Client
from temporalio.worker import Worker
from workflows import VsecaElasticReportingWorkflow
from activities import (load_plan, inspect_enrich, start_enrich, save_enrich_checkpoint,
                        start_reindex, check_task, validate_reindex)

async def main():
    client = await Client.connect(os.getenv("TEMPORAL_ADDRESS", "localhost:7233"),
                                  namespace=os.getenv("TEMPORAL_NAMESPACE", "default"))
    activities = [load_plan, inspect_enrich, start_enrich, save_enrich_checkpoint,
                  start_reindex, check_task, validate_reindex]
    with ThreadPoolExecutor(max_workers=16) as executor:
        worker = Worker(client, task_queue=os.getenv("TEMPORAL_TASK_QUEUE", "vseca-elastic-ops"),
                        workflows=[VsecaElasticReportingWorkflow], activities=activities,
                        activity_executor=executor)
        await worker.run()

if __name__ == "__main__": asyncio.run(main())
