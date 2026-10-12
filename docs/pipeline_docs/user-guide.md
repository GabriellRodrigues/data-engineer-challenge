# User guide

How to run, operate and extend the pipeline in its current state. For the challenge statement see
the root `README.md`; for the business domain see `docs/domain-glossary.md`.

- [1. What exists today](#1-what-exists-today)
- [2. Repository layout](#2-repository-layout)
- [3. Getting started](#3-getting-started)
- [4. Command reference](#4-command-reference)
- [5. Everyday operations](#5-everyday-operations)
- [6. Extending the pipeline](#6-extending-the-pipeline)
- [7. Troubleshooting](#7-troubleshooting)
- [8. Conventions](#8-conventions)
- [9. Further reading](#9-further-reading)

## 1. What exists today

A medallion lakehouse on local disk: **Delta Lake** tables (written with `deltalake`/delta-rs),
processed and queried with **DuckDB**, run in **Docker Compose**.

| Layer / component | Status | What it does |
|---|---|---|
| **Bronze** | done | Ingests every `.parquet` and `.csv` in `DATA_PATH` into append-only Delta tables, enforcing a data contract per dataset. Invalid rows go to a dead letter table; schema breaks stop the run. Idempotent by file hash, with an explicit reprocess mode. |
| **Data contracts** | done | One ODCS-style YAML per dataset; schema rules plus row rules (`required`, formats, patterns, ranges, valid values). |
| **Table maintenance** | done | Z-order/compaction (only when data changed), vacuum with 30-day retention, checkpoints. |
| **Observability** | done | Run/step registry, metrics, alert rules (warn vs stop) and a Markdown health report. |
| **Silver** | done | Typed, deduplicated, conformed tables built from Bronze in SQL (DuckDB). Idempotent per `reference_date` (partition replace), full rebuild by default. Output contract per table. See [`silver-layer.md`](silver-layer.md). |
| **Data quality checks** | done (Silver, Gold) | Dataset-level checks (unique, not_null, row and sum conservation between layers, relationships, category coherence, recomputed reconciliation, ...) run before publishing; `error` blocks the publish, `warn` only records. Results in `dq_results`. |
| **Gold** | done | Dimensional model (3 dimensions, 5 facts) and daily aggregates per consumer, built from Silver with the same compute-check-publish code. Idempotent per `reference_date`. Business metrics feed the alerts. See [`gold-layer.md`](gold-layer.md). |
| **Example queries** | done | 16 queries for Operations, CFO and Compliance (`make queries`), with the answers on the sample in [`analytics.md`](analytics.md). |

Bronze tables produced from the source files in `data/landing/` (written by `make generate`; same file names as the challenge sample in `docs/sample-data/`):

| Source file(s) | Bronze table | Contract |
|---|---|---|
| `transactions_batch_1.parquet`, `transactions_batch_2.parquet` | `transactions` | `transactions.yml` |
| `settlement_paysettler.csv` | `settlement_paysettler` | `settlement_paysettler.yml` |
| `reconciliation_runs.parquet` | `reconciliation_runs` | `reconciliation_runs.yml` |
| `reconciliation_results.parquet` | `reconciliation_results` | `reconciliation_results.yml` |
| `enterprise_company.parquet` | `enterprise_company` | `enterprise_company.yml` |

Each Bronze table may have a `<table>_refused` sibling holding the rows rejected by its contract.

## 2. Repository layout

```
etl_jobs/                       Entry points (python -m etl_jobs.<...>)
  bronze/ingest_to_bronze.py      Bronze ingestion job
  bronze/contracts/*.yml          Data contracts, one per dataset
  maintenance/optimize_tables.py  Delta maintenance job
  observability/health_report.py  Health report job
  observability/alert_rules.yml   Alert rules (thresholds and deviations)
  observability/lineage.py        Table lineage graph (Mermaid) and impact of a table
  silver/build_silver.py          Silver build job (compute, check, publish)
  silver/tables.py                Silver table registry (inputs, partition, date scope)
  silver/sql/*.sql                One query per Silver table, plus macros.sql
  silver/contracts/*.yml          Output contracts of the Silver tables
  silver/quality_checks.yml       Custom data quality checks of the Silver layer
  gold/build_gold.py              Gold build job (same compute, check, publish as Silver)
  gold/tables.py                  Gold table registry (inputs, partition, date scope)
  gold/sql/*.sql                  One query per Gold table, plus macros.sql
  gold/contracts/*.yml            Output contracts of the Gold tables
  gold/quality_checks.yml         Custom data quality checks of the Gold layer
  gold/queries/<consumer>/*.sql   Example analytical queries (operations, cfo, compliance)
  gold/run_queries.py             Runs the example queries and writes a Markdown report
src/                            Reusable components (no entry points)
  data_contract/                  Contract file, schema and record validation
  observability/                  Run registry, metrics, alerts, report, storage
  common/delta_maintenance.py     Optimize/vacuum logic and table properties
  common/delta_io.py              Pinned-version reads and partition-replace publish
  common/layer_build.py           Shared layer build: compute, contract conform, checks, publish, CLI
  data_quality/checks.py          Dataset-level checks and dq_results persistence
  lineage/graph.py                Lineage graph: SQL references, traversal, criticality, Mermaid
  utils/                          Paths, logger, YAML reader
tests/                          Mirrors the path of the component under test
  src/..., etl_jobs/..., scripts/...
docs/
  domain-glossary.md, sample-data/  Challenge material
  pipeline_docs/                    Pipeline documentation (this folder)
data/                           Generated, not versioned
  landing/                        Pipeline source (DATA_PATH), written by make generate
  bronze/<table>                  Delta tables (+ <table>_refused)
  observability/<table>           Observability Delta tables
  observability/reports/          Health reports (Markdown)
  silver/<table>                  Silver Delta tables
  gold/<table>                    Gold Delta tables
  reports/gold_queries.md         Results of make queries
scripts/generate_sample_data.py Synthetic data generator (from the challenge)
run_duckdb_ui.py                DuckDB UI over all Delta tables (runs on the host)
Makefile                        Shortcuts for every operation (make help)
```

All paths are resolved in `src/utils/paths.py` from the project root (the folder that contains
`pyproject.toml`, or `PROJECT_ROOT` if set). The input folder (`SOURCE_DATA_DIR`) is `DATA_PATH`:
`docker-compose.yml` sets it to `/app/data/landing`, and on the host it defaults to `data/landing`.
The challenge sample (`docs/sample-data/`) is still mounted read-only in the container and can be
used by pointing `DATA_PATH` to it (section 5.6).

## 3. Getting started

### Requirements

- Docker with Docker Compose.
- `make` (on Windows, Git Bash provides it). Optional: every target is a plain `docker compose`
  command you can run yourself.
- Optional, to run on the host and to open the DuckDB UI: [uv](https://docs.astral.sh/uv/) and
  Python 3.12+.

### First run

```bash
make up          # build the image and start the container (docker compose up -d --build)
make generate    # write a synthetic dataset (10k transactions) to data/landing
make bronze      # ingest data/landing into Bronze, then run table maintenance
make silver      # build the Silver layer from Bronze, then run table maintenance
make gold        # build the Gold layer from Silver, then run table maintenance
make queries     # run the example queries, results in data/reports/gold_queries.md
make report      # write data/observability/reports/latest.md
make test        # run the test suite inside the container
make ui          # optional: DuckDB UI at http://localhost:4213 (host, needs uv)
```

The container stays up idle (`tail -f /dev/null`). Jobs run in it with `docker compose exec`, which
is what the `make` targets do. `src/`, `etl_jobs/`, `tests/`, `scripts/`, `data/` and the sample data
are mounted as volumes, so code changes are picked up without rebuilding. **Dependency changes**
in `pyproject.toml` do need a rebuild (`make up`); see section 7.

### Running on the host (without Docker)

```bash
uv sync --extra dev
uv run python -m etl_jobs.bronze.ingest_to_bronze
uv run python -m etl_jobs.silver.build_silver
uv run python -m etl_jobs.gold.build_gold
uv run python -m etl_jobs.gold.run_queries --output data/reports/gold_queries.md
uv run python -m etl_jobs.maintenance.optimize_tables
uv run python -m etl_jobs.observability.health_report
uv run --extra dev pytest
```

On the host, `GIT_SHA` is not set unless you export it, so runs are recorded with
`git_sha = unknown`. `make` passes `git describe --always --dirty` automatically.

## 4. Command reference

| Command | What it runs | Notes |
|---|---|---|
| `make help` | lists all targets | |
| `make up` / `make down` | start (with build) / stop the container | |
| `make build` | rebuild the image only | |
| `make shell` | bash inside the container | |
| `make logs` | follow container logs | |
| `make test` | `pytest tests/ -v` in the container | |
| `make bronze` | Bronze ingestion, then maintenance | stops at the first failing job |
| `make reprocess FILES="a.parquet b.csv" REASON="..."` | Bronze ingestion of only those files, replacing their rows, then maintenance | `REASON` is optional but recommended (audit) |
| `make silver` / `make silver REFERENCE_DATE=2025-03-16` | Silver build (full rebuild, or one business date), then maintenance | a blocking data quality check stops the run before anything is published |
| `make gold` / `make gold REFERENCE_DATE=2025-03-16` | Gold build (full rebuild, or one business date), then maintenance | reads the published Silver; run Silver first for the same dates |
| `make pipeline` / `make pipeline REFERENCE_DATE=2025-03-16` | Bronze, Silver, Gold and maintenance in order | stops at the first failing job |
| `make queries` / `make queries CONSUMER=cfo` | example queries against Gold, results in `data/reports/gold_queries.md` | exit code 1 if a query fails |
| `make lineage` | regenerate `docs/pipeline_docs/lineage.md` | run after changing a Silver/Gold SQL, table registry or `critical` flag; the tests fail when it is stale |
| `make optimize` | maintenance only | |
| `make report` | health report of the latest run of each job | a failed run also lists the critical tables it left stale |
| `make ui` | DuckDB UI on the host | needs uv; downloads the `ui` and `delta` DuckDB extensions on first use |
| `make generate` / `make generate-large` | **replace** `data/landing` with a synthetic dataset (10k / 1M transactions, dirty-data patterns on) | see section 5.6 |
| `make clean` | remove `data/landing` and stop the container | Bronze and observability are kept |

Job options:

```bash
python -m etl_jobs.bronze.ingest_to_bronze [--reprocess FILE [FILE ...]] [--reason TEXT]   # FILE = path relative to DATA_PATH
python -m etl_jobs.silver.build_silver [--reference-date YYYY-MM-DD | --start-date YYYY-MM-DD --end-date YYYY-MM-DD]
python -m etl_jobs.gold.build_gold [--reference-date YYYY-MM-DD | --start-date YYYY-MM-DD --end-date YYYY-MM-DD]
python -m etl_jobs.gold.run_queries [--consumer operations|cfo|compliance] [--output FILE] [--max-rows N]
python -m etl_jobs.observability.health_report [--run-id RUN_ID] [--last N]
python -m etl_jobs.observability.lineage [--output FILE] [--impact LAYER.TABLE]
```

Environment variables: `DATA_PATH` (input folder), `LOG_LEVEL` (default `INFO`), `GIT_SHA` (code
version stored with each run), `PROJECT_ROOT` (override the project root, used in tests and
isolated runs), `DUCKDB_UI_PORT` (default `4213`).

## 5. Everyday operations

### 5.1 Ingest new files

Drop the files in `DATA_PATH` and run `make bronze`.

- Files whose content (SHA-256) is already in Bronze are skipped.
- A new file, or a file with the same name but new content, is appended as a **new load**: previous
  versions stay in Bronze, and Silver will decide which one is current.
- Every row gets `_run_id`, `_source_file`, `_source_file_hash`, `_ingestion_mode` and
  `_ingested_at`.

### 5.2 Read the outcome

- **Exit code:** 0 = success; 1 = a file failed (e.g. schema break) or a blocking alert fired.
- **Logs:** one line per file with rows read / accepted / refused, plus warnings for contract
  violations and alerts.
- **Report:** `make report`, then open `data/observability/reports/latest.md`.
- **SQL:** `make ui`, schemas `bronze`, `silver`, `gold` and `observability`.

```sql
SELECT _source_file, _ingestion_mode, count(*) FROM bronze.transactions GROUP BY ALL;
SELECT * FROM bronze.transactions_refused;  -- refused rows + _refused_reasons (exists only once a row is refused)
SELECT job, mode, status, git_sha, error FROM observability.pipeline_run ORDER BY started_at DESC;
```

### 5.3 Handle refused rows (dead letter table)

`<table>_refused` keeps the full original row plus `_refused_reasons`, the list of violated rules.

- If the **data** is wrong, the producer must fix and resend the file (then see 5.4).
- If the **contract** is wrong, fix the YAML, bump its `version` (6.2) and reprocess the file:

```bash
make reprocess FILES="transactions_batch_2.parquet" REASON="status CANCELLED added to contract 1.1.0"
```

### 5.4 Reprocess files

`make reprocess FILES=... REASON=...` processes **only** the listed files.

1. Each file is validated against the current contract.
2. Only if validation passes, all rows from that `_source_file` (every previous version) are deleted
   from the main and `_refused` tables, and the file is written again.
3. A schema break deletes nothing.

Rows are marked `_ingestion_mode = reprocess`, and the Delta commits carry `run_id`, `reason` and
`contract_version`. Use it after a contract fix or when a producer resends a corrected file.

### 5.5 Rebuild Bronze from scratch

Bronze is fully reproducible from the source files:

```bash
rm -rf data/bronze data/observability    # observability is optional; keep it to preserve run history
make bronze
```

Do this before switching `DATA_PATH` to a different, unrelated dataset (5.6). Otherwise both
datasets mix: for example, `reconciliation_runs` ids restart at 1 in each dataset.

### 5.6 Source data and the synthetic generator

The pipeline reads `data/landing/` (`DATA_PATH`). `make generate` (10k transactions, 30 days, 100
merchants, seed 42) and `make generate-large` (1M, 90 days, 500 merchants) **empty that folder** and
write a new dataset with the same file names as the challenge sample, plus dirty-data patterns:

- null `merchant_id` in internal transactions (refused by the contract);
- duplicated CDC events;
- lowercase merchants, timestamps without timezone and comma/BRL decimals in the CSV.

A dataset from a different seed or size is unrelated to the one already loaded, so rebuild Bronze
(5.5) before ingesting it:

```bash
make generate
rm -rf data/bronze data/observability
make bronze
```

The generator writes one PaySettler file per day to `data/landing/paysettler/` and a copy of the
latest day as `settlement_paysettler.csv` at the top level. The job reads subfolders too, so every
daily file lands in `bronze.settlement_paysettler` with `_source_file =
paysettler/settlement_<date>.csv`. The top-level copy has the same content (hash) as the latest daily
file, which sorts first, so it is skipped instead of duplicated.

To run on the challenge sample instead, override the source:

```bash
docker compose exec -e DATA_PATH=/app/docs/sample-data -e GIT_SHA=$(git describe --always --dirty) \
  pipeline python -m etl_jobs.bronze.ingest_to_bronze
```

### 5.7 Maintenance

`make optimize` (also run by `make bronze` and `make reprocess`):

- Z-orders or compacts Bronze tables only if their data changed since the last optimize.
- Compacts the observability tables.
- Vacuums files older than 30 days and writes a checkpoint.

Each table is a step of the `maintenance.optimize` run.

## 6. Extending the pipeline

### 6.1 Add a new source dataset

1. Put the file in `DATA_PATH` (`.parquet` or `.csv`).
2. Create `etl_jobs/bronze/contracts/<dataset>.yml`. Copy an existing contract.
   - **Name.** The dataset name is the file stem. A `_batch_<N>` suffix is removed, and daily
     `settlement_<date>.csv` files map to `settlement_paysettler` (`DATASET_PATTERNS` in
     `ingest_to_bronze.py`). Add a pattern there if the file names need mapping.
   - **CSV columns.** CSV columns are read as text, so declare them `physicalType: varchar`. Use
     `logicalType` plus `logicalTypeOptions.pattern` for the format.
3. If the dataset has an event-time column other than `_timestamp`, add it to `EVENT_COLUMNS`
   (used for the freshness metric).
4. Add the table to `BRONZE_ZORDER` in `etl_jobs/maintenance/optimize_tables.py`: Z-order columns,
   or `None` for plain compaction. Add the `<dataset>_refused` table too.
5. Run `make test` (`test_contract_file_validator.py` validates every contract) and `make bronze`.

A new file format needs a reader in `READERS` (`ingest_to_bronze.py`).

### 6.2 Change a contract

1. Edit the YAML and bump `version` (semantic: patch for descriptions, minor for new optional
   fields or relaxed rules, major for removed/renamed/required fields).
2. Already ingested files are **not** revalidated. Find the files validated with the old version and
   reprocess them if needed:

```sql
SELECT DISTINCT source_file FROM observability.pipeline_run_step
WHERE dataset = 'transactions' AND contract_version = '1.0.0' AND status = 'success';
```

Supported contract features: `required`; `physicalType` in `bigint`, `integer`, `smallint`,
`double`, `float`, `varchar`, `boolean`, `date`; `logicalType: date` stored as text (must parse as
ISO 8601); `logicalTypeOptions` with `pattern`, `minimum`, `maximum`, `minLength` and `maxLength`;
`quality` with `rule: validValues`.

### 6.3 Add or tune an alert

Edit `etl_jobs/observability/alert_rules.yml`:

- **threshold:** `metric`, `operator` and `value`.
- **deviation:** `metric`, `max_ratio`, `window` and `min_history`.

`severity: error` stops the pipeline (exit 1); `warn` only logs and records. Rules apply to any
metric with that name, optionally restricted to a `layer` and/or a `table_name` (e.g. a cadence
per source with `arrival_lag_hours`). The available metrics are listed in
`observability.md`, section 3.

### 6.4 Add a Gold table or an analytical question

- **Table:** `etl_jobs/gold/sql/<table>.sql`, `etl_jobs/gold/contracts/<table>.yml` and an entry in
  `GOLD_TABLES`; see [`gold-layer.md`](gold-layer.md), section 8.
- **Question:** a new `.sql` in `etl_jobs/gold/queries/<consumer>/`; `test_run_queries.py` runs it
  against the test Gold, and `make queries` includes it. Document the question and the answer in
  [`analytics.md`](analytics.md).

### 6.5 Write a new job

A layer built from another one (like Silver and Gold) only needs a table registry, SQL files,
contracts and checks: `src/common/layer_build.py` does the rest. Any other job follows the Bronze
job:

- Wrap the run in `pipeline_run(...)` and each unit of work in `run.step(...)`.
- Record metrics with `run.metric(...)`, passing `reference_date` for partitioned layers.
- Call `evaluate_alerts(...)` at the end.
- Exit 1 when the run is marked failed.

Put reusable logic in `src/` and the entry point in `etl_jobs/`, add tests under the mirrored path
in `tests/`, and add a `make` target.

## 7. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `ModuleNotFoundError` inside the container (e.g. `yaml`) right after adding a dependency | the image was built before the change; dependencies are installed at build time | `make up` (rebuilds the image) |
| Run fails with `SchemaContractError: ... was removed / was renamed / has type` | the source schema no longer matches the contract (breaking change) | fix the producer, or update the contract (new major version) and reprocess |
| Run fails with `Blocking alerts raised; see pipeline_alerts` | an `error` alert fired (e.g. more than 10% of a file refused) | check the report / `pipeline_alerts`, then the `_refused` table for the reasons |
| A file is ingested on every run | none of its rows were accepted, so its hash never reaches the main table | expected; fix the data or the contract (5.3) |
| A corrected file was not picked up | same file name **and** same content (hash) as before | if only the contract changed, use `make reprocess` |
| Same rows appear twice in Bronze | a file with the same name but different content was ingested without `--reprocess` | `make reprocess FILES=<file>` to keep only the latest version |
| `Files to reprocess not found in ...` | wrong name, or a file in a subfolder given without its folder | use the path relative to `DATA_PATH`, e.g. `transactions_batch_1.parquet` or `paysettler/settlement_2025-03-01.csv` |
| Runs recorded with `git_sha = unknown` | job run without `make` and without `GIT_SHA` | use `make`, or `export GIT_SHA=$(git describe --always --dirty)` |
| `git_sha` ends with `-dirty` | the run used uncommitted code | commit, then rerun if you need a reproducible run |
| Run fails with `settlement_file_overdue` / `reconciliation_run_overdue` | no new file for that source in more than 24 hours (`arrival_lag_hours`) | check whether the producer delivered and whether the file reached `data/landing`; ingest it with `make bronze` |
| Bronze fails with `Source folder ... not found; check DATA_PATH` | `DATA_PATH` points to a folder that does not exist (never generated, or a typo). In **Git Bash**, `docker compose exec -e DATA_PATH=/app/docs/sample-data ...` is rewritten to a Windows path (`C:/Program Files/Git/app/...`) before reaching Docker | prefix the command with `MSYS_NO_PATHCONV=1`, or run it from PowerShell/WSL; check the path with `docker compose exec pipeline ls $DATA_PATH` |
| `make gold` fails with `Silver table '<name>' not found` | Silver was never built (or was deleted) | `make silver`, then `make gold` (or `make pipeline`) |
| Gold numbers look old | Gold reads the published Silver; a failed Silver run leaves the previous version | check the latest `silver.build` run in the report, fix it and rerun Silver, then Gold |
| `make ui` fails to start | runs on the host: needs uv and internet the first time (DuckDB extensions) | `uv sync`, check connectivity, or change `DUCKDB_UI_PORT` |

## 8. Conventions

- No comments or docstrings in Python code; explanations live in `docs/pipeline_docs/`.
- Logging through `src/utils/logger.get_logger`, messages in English; no `print`.
- Tests mirror the component path (`src/x/y.py` → `tests/src/x/test_y.py`), one file per module,
  shared fixtures in a local `conftest.py`. Pytest runs with `--import-mode=importlib`.
- Lint with `uv run --extra dev ruff check` (line length 100). One known finding remains in
  `src/utils/yaml_reader.py` (`TRY004`).
- Delta writes go through `deltalake` with the table properties from
  `src/common/delta_maintenance.py`; reads and transformations use DuckDB.

## 9. Further reading

| Document | Covers |
|---|---|
| [`test-plan.md`](test-plan.md) | Automated tests, end-to-end smoke test with expected numbers, idempotency and failure scenarios, scale test results, verification queries |
| [`gold-layer.md`](gold-layer.md) | Gold model, tables and grain, business rules (reference run, trend, exposure, reconciliation recheck), idempotency, checks, business metrics, limits |
| [`lineage.md`](lineage.md) | Generated graph of table dependencies, critical tables and what feeds them |
| [`analytics.md`](analytics.md) | Questions per consumer, the query that answers each one and its result on the sample, what is deliberately not answered and why |
| [`silver-layer.md`](silver-layer.md) | Silver tables, grain and keys, cleaning and dedup rules, idempotency per reference date, compute-check-publish, data quality checks, limits |
| [`data-product.md`](data-product.md) | Consumers, KPIs per consumer with priorities, sample data findings, scope cuts, what Silver must deliver, where each KPI lives in Gold |
| [`data-contracts.md`](data-contracts.md) | Sources and dataset names, schema and record rules, dead letter table, idempotency, reprocessing, load traceability |
| [`observability.md`](observability.md) | Observability tables, when each record is written, metrics catalog, alerts, health report, SQL recipes |
| [`delta-maintenance.md`](delta-maintenance.md) | Optimize/Z-order rules, why optimize is skipped, retention |
| [`../domain-glossary.md`](../domain-glossary.md) | Business domain: reconciliation categories, tolerance, time window |
