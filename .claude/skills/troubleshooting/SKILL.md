---
name: troubleshooting
description: Diagnose a failure or a data problem in the data-engineer-challenge pipeline (Bronze, Silver, Gold, maintenance, health report) starting from the error a job returned or from a symptom in the tables (missing data, wrong counts, duplicates, alerts, quarantined or refused rows) - locate where it happened with the observability tables and the Delta state, match it to a known cause, propose the fix and verify it. Use when the user pastes a traceback/log line, says a job failed, or says a table or dashboard looks wrong.
allowed-tools: Bash(uv *) Bash(make *) Bash(docker compose exec *) Bash(ls *) Bash(find *) Bash(du *) Bash(cat *) Read Glob Grep Edit
---

Go from the user's input (an error returned by a job, or a sign that something is wrong in the
data) to a confirmed cause, a fix and a verification. Diagnose with evidence; never guess from the
message alone when the observability tables or the Delta logs can confirm it.

## 1. Classify the input

| Input | Starting point |
|---|---|
| Traceback or `ERROR` log line from `bronze.ingest`, `silver.build`, `gold.build`, `maintenance.optimize`, `observability.report` | §2 environment, then §4 known errors |
| `WARNING` lines / alerts in the log or in the health report | §5 alerts and checks |
| "The table/dashboard is empty, stale, has too many/too few rows, duplicated rows, nulls" | §6 data symptoms |
| Exit code 1 with no traceback | the run failed by a blocking alert or data quality check: §3, then §5 |

Write down from the input: job, step (`ingest:<file>`, `compute:<table>`, `quality:silver`, `quality:gold`,
`publish:<table>`, `optimize:<table>`), table, file, run_id and time, when present.

## 2. Where did it run

Some failures depend only on the environment. Find out before reading code:

- **Log timezone.** In the container and in WSL the logs are in UTC; in a Windows terminal they
  are in local time. Compare the log time with `started_at` in `pipeline_run` (stored in UTC).
- **`make bronze|silver|gold|pipeline|queries|optimize|reprocess|report|test`** run inside the container
  (`docker compose exec pipeline ...`); `uv run python -m ...` runs on the host.
- **Windows + container (or WSL on `/mnt/c`)**: `./data` is a 9p/drvfs mount. Large Parquet writes
  fail there (see the first row of §4).
- **Stale image.** `pyproject.toml`, `uv.lock` and root files are baked into the image; `src/`,
  `etl_jobs/`, `tests/` are mounted. A dependency error only in the container means `make up`
  (rebuild) was not run.

## 3. Gather evidence

Query helper (host; registers every Delta table as `<layer>.<table>`, like `make ui`):

```bash
PYTHONIOENCODING=utf-8 uv run python -c "
import duckdb, sys
from run_duckdb_ui import register_views
con = duckdb.connect(); con.execute('LOAD delta'); register_views(con); con.execute(\"SET TimeZone = 'UTC'\")
print(con.sql(sys.argv[1]))
" "<SQL>" 2>&1 | grep -v "Registered view"
```

Column names that are easy to get wrong: `pipeline_run.job` (not `job_name`),
`pipeline_run_step.step` (not `step_name`), `dq_results.check_name`, `pipeline_alerts.rule`.

Run, in this order, and keep the output for the report:

```sql
-- 1. Last runs
SELECT run_id, job, mode, status, started_at, duration_s, error
FROM observability.pipeline_run ORDER BY started_at DESC LIMIT 10;

-- 2. Steps of the run in question (failed and skipped first)
SELECT step, status, source_file, output_table, rows_in, rows_out, rows_rejected,
       version_before, version_after, left(error, 300) AS error
FROM observability.pipeline_run_step WHERE run_id = '<run_id>'
ORDER BY status = 'success', started_at;

-- 3. Alerts and data quality checks that did not pass
SELECT rule, severity, status, table_name, source_file, metric, value, expected, message
FROM observability.pipeline_alerts WHERE run_id = '<run_id>' AND status <> 'pass';
SELECT check_name, severity, status, table_name, failing_rows, threshold, message
FROM observability.dq_results WHERE run_id = '<run_id>' AND status <> 'pass';

-- 4. When did it last work?
SELECT job, max(started_at) FILTER (WHERE status = 'success') AS last_success,
       max(started_at) AS last_run
FROM observability.pipeline_run GROUP BY job;
```

`make report` (or `python -m etl_jobs.observability.health_report --run-id <run_id>`) renders the
same information as Markdown in `data/observability/reports/`.

Delta state of a suspect table (a folder with Parquet files but no `_delta_log`, or a version that
did not move, says more than the error):

```bash
for d in data/bronze/* data/silver/*; do echo "$d: logs=$(ls $d/_delta_log 2>/dev/null | grep -c json) parquet=$(find $d -name '*.parquet' | wc -l) size=$(du -sh $d | cut -f1)"; done
uv run python -c "
import sys; from deltalake import DeltaTable
t = DeltaTable(sys.argv[1]); print('version', t.version())
for h in t.history(5): print(h['version'], h['operation'], h.get('run_id'), h.get('scope'), h.get('operationMetrics', {}).get('num_added_files'))
" data/<layer>/<table>
```

Each commit written by a job carries `run_id` (and, in Silver, `mode`, `scope`,
`contract_version`, `bronze_versions`), which links the Delta version to `pipeline_run`.

## 4. Known errors

| Error (message fragment) | Where | Cause | Fix |
|---|---|---|---|
| `DeltaError: Failed to parse parquet: External: Generic LocalFileSystem error: Upload aborted` (often with a panic `object_store ... buffered.rs ... Already shut down`) | any write: `ingest:<file>`, `publish:<table>`, `optimize:<table>` (`z_order_optimize`) | Writes over a few MB use multipart upload, which fails on the Windows 9p bind mount (container or WSL on `/mnt/c`). Not a data or code bug | Run the jobs on the Windows host with `uv run python -m ...`, or clone the repo inside the WSL filesystem. A failed **ingest/publish of a new table** leaves Parquet without `_delta_log`: delete that table folder (ask first) and rerun. A failed optimize commits nothing; just rerun it on the host |
| `FileNotFoundError: YAML file not found: .../bronze/contracts/<name>.yml` | `ingest:<file>` | A file in `data/landing` maps to a dataset without a contract (new source, or a name that does not match `DATASET_PATTERNS`) | Use the `add-bronze-table` skill, or fix the file name |
| `SchemaContractError: Schema does not match data contract '<name>'` (`column ... has type ...`, missing required column) | `ingest:<file>` | The producer changed the file (type, renamed or removed column) | Confirm with the profile of the file; if the change is legitimate, bump the contract `version` and update it (docs in `data-contracts.md`); if not, it is a producer issue. The other files of the run continue |
| `SchemaContractError: ... column '<c>' is not declared in the contract` | `publish:<table>` (Silver) | A Silver SQL in `etl_jobs/silver/sql/` outputs a column missing from `etl_jobs/silver/contracts/<table>.yml` | Add the column to the contract or remove it from the SQL |
| `FileNotFoundError: Bronze table '<name>' not found at ...` | `silver.build` | Bronze not ingested, or the Bronze table folder has no `_delta_log` (see the first row) | Run Bronze; check the Delta state (§3) |
| `FileNotFoundError: Silver table '<name>' not found at ...` | `gold.build` | Silver never built, or its folder has no `_delta_log` | Run Silver (`make pipeline` runs the layers in order) |
| `SchemaContractError: ... column '<c>' is not declared in the contract` | `publish:<table>` (Gold) | A Gold SQL in `etl_jobs/gold/sql/` outputs a column missing from `etl_jobs/gold/contracts/<table>.yml` | Add the column to the contract or remove it from the SQL |
| `Query <consumer>/<name> failed` | `run_queries` | An example query references a Gold column or table that changed | Fix the query; `test_run_queries.py` should have caught it |
| `Blocking data quality checks failed: <checks>; nothing was published` | `quality:silver` / `quality:gold` | An `error` check in the layer's `quality_checks.yml` or a contract `unique`/`not_null` failed. The layer kept its previous version | Query 3 in §3, then the check's SQL against the temp logic (§6 has the usual causes). Fix the data path, not the check |
| `Blocking alerts raised; see pipeline_alerts` | any job | An `error` alert rule fired (`refused_rate_high`, `no_rows_accepted`, `settlement_file_overdue`, `reconciliation_run_overdue`) | §5 |
| `Ingestion failed for: <files>` / `Maintenance failed for: <tables>` | run summary | Only the summary; one or more steps failed | Read the failed steps (query 2) and the first traceback for each |
| `Files to reprocess not found in ...` | `make reprocess` | `FILES` does not match a path relative to `data/landing` | `ls data/landing` and use the relative path |
| `No pipeline run found to report on` | `health_report` | `data/observability` is empty or the `--run-id` does not exist | Run a job first; check the run_id with query 1 |
| `Required module 'pytz' failed to import` / `ModuleNotFoundError` | container only | Image built before a dependency was added | `make up` |
| `UnicodeEncodeError: 'charmap' codec` | ad hoc scripts on Windows | Console encoding cp1252 | `PYTHONIOENCODING=utf-8` |
| `BinderException: Referenced column ... not found` | ad hoc SQL | Wrong column name | See the names in §3 or `observability.md` §4 |
| path too long / `os error 206` | Windows, deep folders | Windows `MAX_PATH` with long Delta file names | Use a shorter base folder |

If the error is not in the table, read the traceback from the bottom up to the first frame in
`src/` or `etl_jobs/`, read that code, and reproduce it in isolation (one file, one table, a probe
in the scratchpad) before proposing a cause.

## 5. Alerts and checks

Rules are in `etl_jobs/observability/alert_rules.yml`; checks in `etl_jobs/silver/quality_checks.yml`
and `etl_jobs/gold/quality_checks.yml`, plus `unique`/`not_null` derived from the contracts.
`observability.md` §5, `silver-layer.md` and `gold-layer.md` explain each one.

| Signal | What to look at | Usual cause |
|---|---|---|
| `refused_rate_high` / `refused_rate_elevated` | `SELECT _refused_reasons, count(*) FROM bronze.<dataset>_refused WHERE _source_file = '<f>' GROUP BY 1` | Dirty source data, or a contract rule that is too strict (valid rows refused: fix the contract) |
| `no_rows_accepted` | the file and the refused table | Empty file or every row breaks the contract |
| `settlement_file_overdue` / `reconciliation_run_overdue` / `transactions_stale` | `arrival_lag_hours` in `pipeline_metrics`; files in `data/landing` | No new file delivered for more than 24h |
| `rows_read_deviation` | `rows_read` history per table | Truncated or duplicated file; **expected** on the first Bronze run after changing the dataset volume while `data/observability` is kept (`test-plan.md` §4) |
| `too_many_files` | `table_file_count`; was maintenance run? | Maintenance not run; expected on partitioned Silver tables (one file per partition) |
| `undeclared_columns` | the WARNING line naming the column | Producer added a column; update the contract |
| `results_quarantined`, `relationships:*` | `silver.reconciliation_results_quarantine` | Results whose `run_id` has no run |
| `run_total_coherence`, `category_coherence`, `merchant_unknown` | the check SQL in `quality_checks.yml` | Data produced by the reconciliation service; reported, not corrected |
| `row_conservation:*` (error) | Bronze latest-load count vs Silver + duplicates + quarantine | A join or filter in the Silver SQL that drops or multiplies rows |
| `row_conservation:fact_*`, `sum_conservation:agg_*` (Gold, error) | Silver count/sum vs the Gold fact or aggregate | A Gold join that fans out (e.g. a dimension joined without the SCD2 `valid_at` filter) or a filter that drops rows |
| `discrepancy_rate_spike`, `reconciliation_runs_missing`, `missing_run:*`, `reconciliation_recheck:*` (Gold, warn) | `gold.agg_reconciliation_daily`, `gold.audit_reconciliation_recheck` | Business signals from the reconciliation service, not pipeline defects: a worse run, a failed or absent run, verdicts that do not follow the rules |
| `one_current_version:merchants` (error) | `SELECT merchant_id, count(*) FROM silver.merchants WHERE is_current GROUP BY 1 HAVING count(*) > 1` | SCD2 logic or CDC ties in `enterprise_company` |

## 6. Data symptoms

Walk the chain backwards until the numbers stop matching: **landing → Bronze → Silver → Gold**, for the
same date or key.

```sql
-- Bronze loads per file (latest load is what Silver reads)
SELECT _source_file, _ingested_at, _run_id, count(*) FROM bronze.<table> GROUP BY ALL ORDER BY 1, 2;
-- Silver per date and the run that published it
SELECT reference_date, count(*), any_value(_silver_run_id), max(_silver_built_at)
FROM silver.<table> GROUP BY 1 ORDER BY 1 DESC LIMIT 10;
```

| Symptom | Check, in order |
|---|---|
| **No data since date X** (README Part 3.2) | 1) query 4: did the jobs run after X, and did they succeed? 2) did files for dates after X arrive in `data/landing` (`arrival_lag_hours`, overdue alerts)? 3) `reconciliation_runs` after X: `FAILED`, stuck `IN_PROGRESS`, or missing (`run_stuck`, `no_reconciliation_results`)? 4) Silver partitions after X exist (query above) and the last `publish:` steps succeeded? 5) Gold: `gold.build` ran after the last Silver run, `agg_reconciliation_daily` has the dates (`is_missing_run`)? Stop at the first layer where data is missing; that layer owns the problem |
| Fewer rows than expected | refused rows in `<dataset>_refused`; Silver dedup (`duplicates_removed`); quarantine; `is_deleted` rows filtered by the consumer; a file skipped because its hash was already ingested |
| More rows / duplicates | a file loaded twice under **different names** (dedup is by content hash + latest load per `_source_file`); a query summing across runs (a transaction appears in several runs by design, `data-product.md` §3) |
| Old values in Silver after a fix in the source | Bronze still skipping the file (same hash): use `make reprocess`; or Silver not rebuilt for that date |
| Null `merchant_id` in results | `merchant_id_origin`; `merchant_unknown` check |
| Settlement amounts null | `parse_amount` in `etl_jobs/silver/sql/macros.sql` did not recognise the format; look at `amount_raw` |
| Two datasets mixed (e.g. 10k and 1M) | Bronze is append-only: after replacing `data/landing` with a different dataset, `data/bronze` and `data/silver` must be rebuilt from scratch |

## 7. Fix

- Prefer a rerun: Bronze skips ingested files by hash and Silver replaces its scope, so rerunning
  the same command is safe and converges.
- Ask before anything destructive: deleting a table folder, `rm -rf data/...`, `make reprocess`,
  and **never** delete `data/observability` without asking (it holds the evidence and the
  history used by the deviation rules).
- Fix contracts and SQL, never edit Bronze data by hand. A contract change bumps its `version`.
- Do not weaken a check or an alert to make a run pass; if a rule is wrong, say so and propose the
  change separately.
- Code changes follow the repo rules: no comments, logs in English, tests mirrored (use the
  `unit-tests` skill for a regression test of a real bug).

## 8. Verify

Rerun the job that failed (in the environment of §2) and confirm: run `success`, the step that
failed is now `success` with the expected rows, the alert/check passes or is explained, and the
Delta version of the affected table moved (or did not, when nothing should change). For data
symptoms, rerun the query that showed the problem.

## 9. Report

- **Symptom:** the input, job/step/table/run_id.
- **Cause:** confirmed cause and the evidence (query output, Delta state, reproduction).
- **Impact:** what was written or not, which layers/dates are affected, whether consumers saw bad
  data.
- **Fix:** what was done (or proposed and waiting for approval), and the verification result.
- **Prevention:** a check, alert, test or doc change that would catch it earlier, if any.
