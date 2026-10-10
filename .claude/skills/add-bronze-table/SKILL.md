---
name: add-bronze-table
description: Add a new source dataset (new Bronze table) to the data-engineer-challenge pipeline - profile the source file, write its data contract, register it for naming, freshness and maintenance, ingest it and verify it end to end. Use when the user wants to ingest a new table/file/source or plug a new processor into Bronze.
allowed-tools: Bash(uv *) Bash(make *) Bash(docker compose exec *) Bash(ls *) Read Glob Grep Edit Write
---

Bring a new source dataset into Bronze so that it is validated by a contract, idempotent,
observable and maintained like the existing tables.

## What is required vs what fails silently

| Piece | Where | If missing |
|---|---|---|
| Source file in the landing folder | `data/landing/` (`DATA_PATH`), any subfolder | nothing to ingest |
| Data contract | `etl_jobs/bronze/contracts/<dataset>.yml` | **run fails**: `FileNotFoundError: YAML file not found` for that file |
| File name → dataset mapping | `DATASET_PATTERNS` in `etl_jobs/bronze/ingest_to_bronze.py` | only needed when the file stem is not the dataset name and is not `<name>_batch_<N>`; otherwise the contract is looked up under the wrong name and the run fails |
| Reader for the format | `READERS` in `ingest_to_bronze.py` | only for formats other than `.parquet`/`.csv`; other files are **silently ignored** |
| Event-time column | `EVENT_COLUMNS` in `ingest_to_bronze.py` | **silent**: `freshness_lag_hours` is not recorded when the table has no `_timestamp` column |
| Maintenance entry | `BRONZE_ZORDER` in `etl_jobs/maintenance/optimize_tables.py` (table **and** `<dataset>_refused`) | **silent**: the table is never compacted, Z-ordered or vacuumed; small files pile up until the `too_many_files` alert fires |
| Docs | `docs/pipeline_docs/user-guide.md` (table of Bronze tables) | users do not know the table exists |

Automatic, no change needed: contract validity test (parametrized over every contract), alert rules
(apply to any table by metric name), observability metrics, DuckDB UI views, dead letter table.

## 1. Understand the source

Ask (or confirm from the user's message) what the dataset is, who produces it, how it is
delivered (one file, daily files, CDC batches), its key and its event-time column. Then profile the
real file:

```bash
uv run python -c "
import duckdb, sys
path = sys.argv[1]
src = f\"read_csv('{path}', all_varchar=true, header=true)\" if path.endswith('.csv') else f\"'{path}'\"
rel = duckdb.sql(f'SELECT * FROM {src}')
print(rel.count('*').fetchone()[0], 'rows')
for name, dtype in zip(rel.columns, rel.types):
    col = '\"' + name + '\"'
    nulls, distinct = duckdb.sql(f'SELECT count(*) FILTER (WHERE {col} IS NULL), count(DISTINCT {col}) FROM {src}').fetchone()
    sample = duckdb.sql(f'SELECT DISTINCT {col}::VARCHAR FROM {src} WHERE {col} IS NOT NULL LIMIT 5').fetchall()
    print(f'{name:25} {str(dtype):10} nulls={nulls:<6} distinct={distinct:<6} sample={[s[0] for s in sample]}')
" data/landing/<file>
```

For CSV, read every column as text (`all_varchar=true`), exactly like the job does.

## 2. Choose the dataset name

- Lowercase `snake_case`, named after the business entity, not the delivery
  (`settlement_<processor>`, not `daily_export`).
- If the file stem already is the dataset name, or is `<dataset>_batch_<N>`, nothing to register.
- Otherwise add a pattern to `DATASET_PATTERNS` (first match wins; `fullmatch` on the stem), e.g.
  `(re.compile(r"settlement_acme_\d{8}"), "settlement_acme")`, and extend
  `test_dataset_name` in `tests/etl_jobs/bronze/test_ingest_to_bronze.py` with the new file names.

## 3. Write the contract

Create `etl_jobs/bronze/contracts/<dataset>.yml`, copying the closest existing contract
(`settlement_paysettler.yml` for CSV, `transactions.yml` for CDC parquet). Rules:

- `apiVersion: v3.0.2`, `kind: DataContract`, `id: <domain>.<dataset>`, `name: <dataset>`,
  **`version: 1.0.0`**, `status: active`, `description.purpose/usage/limitations` (limitations =
  the known dirty patterns and what Silver must handle).
- One schema object named exactly `<dataset>`, one property per source column, **same names and
  case** as the file.
- `physicalType` from the profile: parquet `BIGINT`→`bigint`, `INTEGER`→`integer`,
  `DOUBLE`→`double`, `VARCHAR`→`varchar`, `BOOLEAN`→`boolean`, `DATE`→`date`. CSV: always
  `varchar`. Supported values only: `bigint integer smallint double float varchar boolean date`.
- `logicalType`: `string`, `integer`, `number`, `boolean`, `date` (timestamps stored as text use
  `date`, which makes the job check they parse as ISO 8601), `object`, `array`.
- `required: true` only for columns that must never be null **by definition** (keys, event time,
  CDC `Op`). A column that is null in the sample for a legitimate reason is `required: false` with
  the reason in `description`.
- Value rules only when documented or unambiguous in the profile: `logicalTypeOptions.pattern`
  (UUIDs, ISO codes, formats), `minimum`/`maximum`, `minLength`/`maxLength`,
  `quality: [{type: library, rule: validValues, validValues: [...]}]` for closed domains.
  Over-strict rules send valid rows to the dead letter table; when in doubt, leave the rule out and
  record it as a Silver data quality check.
- Mark `primaryKey: true` on the business key (documentation only; Bronze does not enforce
  uniqueness).

Dry run the contract against the real file before ingesting anything:

```bash
uv run python -c "
import duckdb, sys
from pathlib import Path
from etl_jobs.bronze.ingest_to_bronze import READERS, dataset_name
from src.data_contract.contract_file_validator import load_contract
from src.data_contract.record_validator import split_records
from src.data_contract.schema_validator import enforce_schema
from src.utils.paths import BRONZE_CONTRACTS_DIR
path = Path(sys.argv[1]); con = duckdb.connect()
contract = load_contract(dataset_name(path), BRONZE_CONTRACTS_DIR)
valid, refused = split_records(con, enforce_schema(READERS[path.suffix](con, path), contract), contract)
print('accepted', valid.num_rows, 'refused', refused.num_rows)
for reasons in refused['_refused_reasons'].to_pylist()[:10]: print(reasons)
" data/landing/<file>
```

Every refused row must be explainable as genuinely bad data. If valid rows are refused, fix the
contract, not the data.

## 4. Register the table

- `EVENT_COLUMNS` in `ingest_to_bronze.py`: add `"<dataset>": "<event_column>"` unless the
  event-time column is `_timestamp` (CDC). Use the column that says when the business event
  happened (e.g. `settled_at`), not the extraction time.
- `BRONZE_ZORDER` in `etl_jobs/maintenance/optimize_tables.py`: add `"<dataset>"` with the columns
  most used in filters/joins (usually the event date and the business key), or `None` for small
  tables (plain compaction), **and** `"<dataset>_refused": None`.
- New file format only: add a reader to `READERS` that returns a `pa.Table` (text sources: read
  every column as `varchar`).

## 5. Ingest and verify

```bash
make bronze
make report
```

Check, and report the numbers:

- Logs: `Ingestion of <file> completed (N rows read, N accepted, M refused)` for every new file, no
  `ERROR`, run `finished with status success`.
- `data/observability/reports/latest.md`: steps `success`, alerts (a `refused_rate` warn/fail
  means the contract or the data needs attention), metrics including `freshness_lag_hours` and
  `table_file_count` for the new table.
- `make ui` (or DuckDB): `SELECT _source_file, count(*) FROM bronze.<dataset> GROUP BY 1`; if rows
  were refused, `SELECT _refused_reasons, count(*) FROM bronze.<dataset>_refused GROUP BY 1`.
- Run `make bronze` again: every new file must be `skipped` (idempotency).
- The maintenance run lists `optimize:<dataset>` (and `optimize:<dataset>_refused` when it exists).

## 6. Tests and docs

```bash
uv run --extra dev pytest -q tests/src/data_contract/test_contract_file_validator.py   # new contract is valid
uv run --extra dev pytest -q && uv run --extra dev ruff check src etl_jobs tests
make test
```

Add tests only for code you changed (`DATASET_PATTERNS` → `test_dataset_name`; a new reader →
a test in `tests/etl_jobs/bronze/test_ingest_to_bronze.py`). Use the `unit-tests` skill for them.

Docs: add the row to "Bronze tables produced from the source files" in
`docs/pipeline_docs/user-guide.md`; if the dataset introduced a new naming pattern or format, update
"Sources and dataset names" in `docs/pipeline_docs/data-contracts.md`.

## 7. Report

Dataset name and source files, contract highlights (key, required fields, value rules, known dirty
patterns left for Silver), registrations done (pattern, event column, maintenance), ingestion
numbers (read/accepted/refused), idempotent rerun result, tests run, docs changed, and open points
(e.g. rules deferred to Silver data quality).
