# OSF Preprints - Modular Pipeline (No Celery)

> **Experiment status:** The experimental phase ended on 17 September 2026. Scheduled production GitHub Actions are paused. Workflows remain available for explicit manual maintenance only; do not resume notification sending without authorization for a new phase.

This repository runs a bounded, stage-based pipeline for OSF preprints using DynamoDB as the single source of truth.

Pipeline stages:
1. `sync`: ingest preprints from OSF
2. `pdf`: download/convert primary files
3. `grobid`: generate TEI from PDFs
4. `extract`: parse TEI and write references
5. `enrich`: fill missing reference DOIs
6. `flora`: FLoRA lookup + screening
7. `author`: author/email candidate extraction

All stages run as normal Python commands and exit. Scheduling is external (cron or GitHub Actions).
The `flora` stage checks whether originals have replications cited in the FLoRA database (the FORRT Library of Replication Attempts).

See the [trial flowchart](docs/protocol_flowchart.md) for a visual overview of how a preprint flows through the pipeline.

## Quick Start (Local)

1. Create a virtual environment and install Python dependencies:
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
2. Install LibreOffice (`soffice`) locally if you need DOCX -> PDF conversion in the `pdf` stage.
3. Configure `.env`:
```bash
cp .env.example .env
```
4. Review committed runtime rules in `config/runtime.toml` (for example `ingest.anchor_date` and FLORA endpoint).
5. Start local infrastructure services (optional if you use AWS DynamoDB and/or a remote GROBID):
```bash
docker compose up -d dynamodb-local grobid
```
6. Initialize DynamoDB tables:
```bash
python -c "from osf_sync.db import init_db; init_db(); print('Dynamo tables ready')"
```
7. Run pipeline stages:
```bash
python -m osf_sync.pipeline run --stage sync --limit 1000
python -m osf_sync.pipeline run --stage pdf --limit 100
python -m osf_sync.pipeline run --stage grobid --limit 50
python -m osf_sync.pipeline run --stage extract --limit 200
python -m osf_sync.pipeline run --stage enrich --limit 300
python -m osf_sync.pipeline run --stage flora --limit-lookup 200 --limit-screen 500
```

## Main Commands

Single stage:
```bash
python -m osf_sync.pipeline run --stage <sync|pdf|grobid|extract|enrich|flora|author> [options]
```

Full bounded run:
```bash
python -m osf_sync.pipeline run-all \
  --sync-limit 1000 --pdf-limit 100 --grobid-limit 50 --extract-limit 200 --enrich-limit 300
```
`run-all` includes the `author` stage by default; use `--skip-author` to disable it for a run.
By default, `run-all` keeps local PDF/TEI files during `author`; use `--cleanup-author-files` to allow cleanup.
By default, `author` updates DynamoDB only (no local CSV output). Use `--write-debug-csv` (and optionally `--out`) for local debug snapshots.

Ad-hoc window sync:
```bash
python -m osf_sync.pipeline sync-from-date --start 2025-07-01
```

One-off preprint:
```bash
python -m osf_sync.pipeline fetch-one --id <OSF_ID>
# or
python -m osf_sync.pipeline fetch-one --doi <DOI_OR_URL>
```

Author-cluster randomisation (standalone, not in `run-all`):
```bash
python -m osf_sync.pipeline author-randomize \
  --network-state-key trial:author_network_state
```
Optionally add `--authors-csv <path>` to use an enriched author CSV if available.
Status: this workflow is not yet validated end-to-end in production and should be treated as experimental.
This command processes only unassigned preprints.
If no prior trial network exists, it initializes one from those preprints; otherwise it loads the latest network from DynamoDB and augments it.
Allocations, graph state, and run metadata are stored in DynamoDB trial tables plus `sync_state`.
Use `--dry-run` to preview candidate processing and allocation counts without writing to DynamoDB:
```bash
python -m osf_sync.pipeline author-randomize --dry-run
```

`python -m osf_sync.cli ...` is now a thin alias to the same pipeline CLI.
## Common Options

- `--limit`: max items for the stage.
- `--max-seconds`: stop the stage after N seconds.
- `--dry-run`: compute/select work without executing mutations.
- `--debug`: enable verbose logging.
- `--owner` and `--lease-seconds` (queue stages): override DynamoDB claim ownership/lease duration.
- `--skip-author` (`run-all`): skip author extraction when needed.
- `--cleanup-author-files` (`run-all`): allow author stage file deletion (off by default).
- `--write-debug-csv` (`author` stage): write a local debug CSV snapshot (`--out` overrides the default path).

## Environment (`.env`)

```dotenv
# local Docker GROBID:
GROBID_URL=http://localhost:8070
# remote GROBID example:
# GROBID_URL=https://grobid.example.org
GROBID_INCLUDE_RAW_CITATIONS=true
PIPELINE_ENV=dev
DDB_BILLING_MODE=PAY_PER_REQUEST
DEV_SYNC_LOOKBACK_DAYS=7
# Optional explicit override for sync start date:
# SYNC_START_DATE_OVERRIDE=2026-01-01
# Optional explicit override for sync end date:
# SYNC_END_DATE_OVERRIDE=2026-03-15
# Safety default: override runs do not rewrite sync cursor.
SYNC_OVERRIDE_WRITES_CURSOR=false
# Optional global cursor-write disable.
SYNC_DISABLE_CURSOR_WRITE=false
DYNAMO_LOCAL_URL=http://localhost:8000
AWS_REGION=eu-north-1
AWS_SECRET_ACCESS_KEY=<AWS_SECRET_ACCESS_KEY>
AWS_ACCESS_KEY_ID=<AWS_ACCESS_KEY_ID>
DDB_TABLE_PREPRINTS=dev_preprints
DDB_TABLE_REFERENCES=dev_preprint_references
DDB_TABLE_TEI=dev_preprint_tei
DDB_TABLE_EXCLUDED_PREPRINTS=dev_excluded_preprints
DDB_TABLE_SYNCSTATE=dev_sync_state
DDB_TABLE_API_CACHE=dev_api_cache
DDB_TABLE_TRIAL_AUTHOR_NODES=dev_trial_author_nodes
DDB_TABLE_TRIAL_AUTHOR_TOKENS=dev_trial_author_tokens
DDB_TABLE_TRIAL_CLUSTERS=dev_trial_clusters
DDB_TABLE_TRIAL_ASSIGNMENTS=dev_trial_preprint_assignments
OPENALEX_EMAIL=<PERSONAL_EMAIL_ID>
PDF_DEST_ROOT=./data/preprints
LOG_LEVEL=INFO
OSF_INGEST_SKIP_EXISTING=false
API_CACHE_TTL_MONTHS=6
PIPELINE_CLAIM_LEASE_SECONDS=1800
```

`sync` window behavior:
- `PIPELINE_ENV=dev`: sync uses a rolling `DEV_SYNC_LOOKBACK_DAYS` window (default 7).
- `PIPELINE_ENV=prod`: sync uses `ingest.anchor_date`/`ingest.window_months` from `config/runtime.toml`.
- In prod, changing `ingest.anchor_date` or `ingest.window_months` triggers an automatic bounded backfill (controlled by `ingest.backfill_on_config_change`).
- `SYNC_START_DATE_OVERRIDE` (optional): forces an explicit start date in either mode.
- `SYNC_END_DATE_OVERRIDE` (optional): explicit end date; in prod override mode, omitted end defaults to `ingest.anchor_date`.
- Recommended naming: keep local `.env` on `dev_*` tables; GH Actions prod workflows are set to `prod_*`.
- `SYNC_OVERRIDE_WRITES_CURSOR=false` (default) keeps continuation cursor unchanged during override/backfill runs.

Backfill without breaking continuation:
1. In GROBID workflow dispatch, set `sync_start_date_override` (and optionally `sync_end_date_override`).
2. Leave `sync_override_writes_cursor` as `false` (default).
3. Run backfill as needed; normal continuation cursor is preserved.
4. Clear override inputs for subsequent normal runs.

## Runtime Rules (`config/runtime.toml`)

These non-secret operational rules are committed in git:

```toml
[ingest]
anchor_date = "2026-02-20" # ISO date/timestamp; empty disables date-window filter
window_months = 6

[flora]
original_lookup_url = "https://rep-api.forrt.org/v1/original-lookup"
cache_ttl_hours = 48
csv_url = "https://github.com/forrtproject/FReD-data/raw/refs/heads/main/output/flora_filtered.csv"
csv_path = "data/flora.csv"
```

## Scheduling

The production GitHub Actions schedules are paused because the experimental phase has ended. The workflows can still be started manually for authorized maintenance.

For a future phase, scheduling can use either:
- Cron/systemd timers on a VM, or
- GitHub Actions `schedule` workflows.

Recommended pattern:
- Run each stage independently on a cadence with bounded limits.
- Allow overlap; claim/lease fields in DynamoDB prevent duplicate processing.

## DynamoDB Queue Flow

1. `sync` sets `queue_pdf=pending` when eligible.
2. `pdf` marks `queue_pdf=done`, `queue_grobid=pending`.
3. `grobid` marks `queue_grobid=done`, `queue_extract=pending`.
4. `extract` marks `queue_extract=done`.

Queue stages use claim/lease metadata (`claim_*_owner`, `claim_*_until`) and error tracking fields (`last_error_*`, `retry_count_*`).

## DOI Experiment Command

Use the module entrypoint directly for DOI matching experiments:

```bash
python -m osf_sync.augmentation.doi_multi_method_lookup --from-db --limit 400 --output doi_multi_method.csv
```
