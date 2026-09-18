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

This POC talks to the self-hosted Temporal and Elasticsearch stack at `158.220.118.251`:

- Temporal frontend: `158.220.118.251:7233` (no security for now)
- Temporal UI: `http://158.220.118.251:8080`
- Elasticsearch: `http://158.220.118.251:9200` (HTTP, security disabled)

Connection settings live in `.env` (already created from `.env.example`).

> Source indices, ingest pipelines, enrich policies, and transforms are provisioned by a
> separate application that also loads the data. This worker assumes they already exist and
> does not create or validate them.

## Setup

`.env` and `elastic-files.yaml` are required. They are already present in this repo.

## Run with Docker (recommended)

Start the worker:

```bash
docker compose up --build -d
```

Validate the plan with a dry run:

```bash
docker compose run --rm worker python start_workflow.py --config ./elastic-files.yaml
```

After validating the plan in the Temporal UI, execute against the cluster:

```bash
docker compose run --rm worker python start_workflow.py --config ./elastic-files.yaml --execute
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

Stop Temporal on the VPS using the same Compose file you used to start it there:

```bash
docker compose -f docker-compose-postgres.yml down
```
