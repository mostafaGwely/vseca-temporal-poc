import argparse
import asyncio
import os
from datetime import datetime, timezone
from temporalio.client import Client
from workflows import VsecaElasticReportingWorkflow

async def main():
    p = argparse.ArgumentParser()
    p.add_argument("--config", default=os.getenv("ELASTIC_CONFIG", "./elastic-files.yaml"))
    p.add_argument("--execute", action="store_true", help="Actually call Elasticsearch. Default is dry-run.")
    p.add_argument("--workflow-id")
    args = p.parse_args()
    client = await Client.connect(os.getenv("TEMPORAL_ADDRESS", "localhost:7233"),
                                  namespace=os.getenv("TEMPORAL_NAMESPACE", "default"))
    wid = args.workflow_id or "vseca-elastic-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    result = await client.execute_workflow(VsecaElasticReportingWorkflow.run,
        {"config_path": args.config, "dry_run": not args.execute,
         "max_parallel_reindexes": int(os.getenv("MAX_PARALLEL_REINDEXES", "3"))},
        id=wid, task_queue=os.getenv("TEMPORAL_TASK_QUEUE", "vseca-elastic-ops"))
    print(result)

if __name__ == "__main__": asyncio.run(main())
