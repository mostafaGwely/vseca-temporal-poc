# VSECA Temporal Elasticsearch POC

This POC reads your existing `elastic-files.yaml`, ignores its schedules, conditionally refreshes enrich policies, and executes the reporting reindex dependency graph.

## Safety defaults

The starter runs in **dry-run mode by default**. It parses the real plan and performs enrich source inspection, but does not execute enrich policies, launch reindexes, or save checkpoints. Use a non-production Elasticsearch endpoint first.

## Dependency graph

1. Backend-to-staging reindexes and enrich checks run in parallel.
2. An enrich policy executes only when its resolved source-index fingerprint changed.
3. Staging-to-main/cloud starts after Stage 1 and all enrich results complete successfully.
4. Main/cloud-to-history starts after Stage 2 completes successfully.
5. Other reindex commands are reported as `unmanaged` and are not executed.

## Target stack

This POC talks to the local Dockerized Temporal and Elasticsearch stacks by their container
names. The worker joins the `temporal-network` and `elk_elk` external networks
(`docker-compose.yml`) so Docker DNS resolves them:

- Temporal frontend: `temporal:7233` (no security for now)
- Temporal UI: `http://localhost:8080`
- Elasticsearch: `http://elasticsearch:9200` (HTTP, security disabled)

Connection settings live in `.env` (already created from `.env.example`). To point at a stack
outside Docker, replace the service names with a reachable host/IP.

> Source indices, ingest pipelines, enrich policies, and transforms are provisioned by a
> separate application that also loads the data. This worker assumes they already exist and
> does not create or validate them.

## Setup

`.env` and `elastic-files.yaml` are required. They are already present in this repo.

## Run with Docker (recommended)

Start the worker (optionally several replicas sharing the task queue):

```bash
docker compose up --build -d
docker compose up -d --scale worker=3        # 3 workers
```

The external networks `temporal-network` and `elk_elk` must already exist (they belong to your
local Temporal and ELK compose stacks).

Validate the plan with a dry run:

```bash
docker compose run --rm worker python start_workflow.py --config ./elastic-files.yaml
```

After validating the plan in the Temporal UI, execute against the cluster:

```bash
docker compose run --rm worker python start_workflow.py --config ./elastic-files.yaml --execute
```

## Schedule recurring runs (every 6 hours)

The worker executes scheduled runs; the schedule lives in Temporal, so no container needs to run on a timer.

Create (or update) the schedule once:

```bash
docker compose run --rm worker python start_workflow.py --schedule --execute
```

- Default: cron `0 */6 * * *` (00:00, 06:00, 12:00, 18:00), timezone `UTC`, overlap `SKIP` (a tick is skipped if the previous run is still going).
- Without `--execute` the schedule would run in dry-run mode, same as a manual run.
- Override via `--schedule-id`, `--cron`, `--timezone`, or the `SCHEDULE_ID` / `SCHEDULE_CRON` / `SCHEDULE_TIMEZONE` environment variables.

Manage it:

```bash
docker compose run --rm worker python start_workflow.py --describe    # inspect
docker compose run --rm worker python start_workflow.py --pause       # stop firing
docker compose run --rm worker python start_workflow.py --resume      # resume firing
docker compose run --rm worker python start_workflow.py --delete      # remove
```

## Run locally (alternative)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
set -a; source .env; set +a
python worker.py
```

In a second terminal, load the same environment and start a dry run:

```bash
set -a; source .env; set +a
python start_workflow.py --config ./elastic-files.yaml
```

## Authentication

The target Elasticsearch cluster has security disabled, so no credentials are sent. To use a
secured cluster, set `ELASTIC_API_KEY`, or `ELASTIC_USERNAME`/`ELASTIC_PASSWORD`, and configure
`ELASTIC_CA_CERT` for a private CA. Avoid `ELASTIC_VERIFY_TLS=false` outside an isolated test.

## Change detection

For each policy, the worker retrieves the policy definition, resolves its source indices, and hashes each concrete index's UUID, primary document count, deleted count, indexing `index_total`, and store size. A successful fingerprint is stored in `.vseca-temporal-orchestration-state`. Updates can therefore be detected even when document count stays constant. The checkpoint is written only after successful enrich execution.

## Important POC assumptions

- `elastic-files.yaml` is bundled into the worker image (`Dockerfile`) and read from the working directory.
- Source indices, ingest pipelines, enrich policies, and transforms are created and populated by a separate application; this worker does not provision them.
- The Elasticsearch identity can read enrich policies and index stats, execute enrich policies/reindexes, read tasks, and write the orchestration state index.
- The two Stage 2 commands are identified by their current names. The two Stage 3 commands are identified by their current names. Backend-to-staging is identified by the two staging destination indices.
- Enrich executions are fast, so the Elasticsearch task record may already be gone by the time it is polled. A `404` whose top-level reason is exactly `task [<id>] isn't running and hasn't stored its results` is treated as success for enrich, and the checkpoint is still saved. Any other task error, including other `404`s, fails the run. Reindex task polling never treats a missing task as success.
- Stage 2 currently waits for all Stage 1 commands, not merely its corresponding main/cloud branch. This is conservative and safe for the first POC.
- A first execution sees no checkpoint, so every enrich policy is considered changed. Use dry-run first.

## Stop

Stop the worker:

```bash
docker compose down
```

Stop the local Temporal stack with the Compose file you used to start it (in its own project):

```bash
docker compose -f docker-compose-postgres.yml down
```
