# Test plan

How to validate that the pipeline (Bronze, Silver and Gold) behaves as documented: automated tests, an
end-to-end smoke test with known expected numbers, idempotency and failure scenarios, and a scale
test with recorded results. Run the automated tests on every change; run the other sections
before a delivery or after changing a job, a contract or a check.

- [1. Automated tests](#1-automated-tests)
- [2. End-to-end smoke test on the challenge sample](#2-end-to-end-smoke-test-on-the-challenge-sample)
- [3. Idempotency and failure scenarios](#3-idempotency-and-failure-scenarios)
- [4. Scale test](#4-scale-test)
- [5. Verification queries](#5-verification-queries)

## 1. Automated tests

```bash
make test                      # in the container
uv run --extra dev pytest      # on the host
```

279 tests, mirrored on the path of the module they cover (`tests/src/...`, `tests/etl_jobs/...`).
Every test writes to `tmp_path` and never touches `data/`.

| Area | Test files | Tests | What is covered |
|---|---|---|---|
| Bronze job | `tests/etl_jobs/bronze/test_ingest_to_bronze.py` | 32 | idempotency by hash, reprocess, dead letter table, schema breaks, dataset naming, metrics and alerts |
| Gold job | `tests/etl_jobs/gold/test_build_gold.py` | 24 | dimensional model on a hand-checked Silver fixture: CNPJ masked and unknown member, merchant version valid at the transaction (SCD2), one reference run per date, rates, cadence and missing runs, trend baseline, amount at risk per currency, current reconciliation status, exposure, aging, resolution, changes after reconciliation, recheck check (differences counted, passes on a coherent case), **same date twice = same content as a full rebuild**, a date run touches only its partitions, blocking check stops the publish, business metrics and alerts, empty Silver, missing Silver table, CLI validation |
| Example queries | `tests/etl_jobs/gold/test_run_queries.py` | 22 | every shipped query runs against a Gold built from the fixture; key answers (trend dates, transaction trail, TPV); discovery per consumer; Markdown output; a failing query exits 1 |
| Shared layer build | `tests/src/common/test_layer_build.py` | 16 | date scope with lookback, table registry, contract conform (order, optional columns, undeclared columns, wrong types), macros, run stamping, CLI dates, job wrapper (failed and date-mode runs) |
| Silver job | `tests/etl_jobs/silver/test_build_silver.py` | 23 | parsing (3 amount formats, timestamps without timezone), CDC tie-break, deletes, SCD2, quarantine, latest load per file, **same date twice = same content**, a date run touches only its partitions, 7-day window, blocking check stops the publish, pinned Bronze versions, CLI validation |
| Data contracts | `tests/src/data_contract/*` | 51 | contract files (all Bronze, Silver and Gold contracts are valid), schema enforcement and new types, record rules |
| Data quality | `tests/src/data_quality/test_checks.py` | 15 | unique / not_null / contract-driven checks, severity and threshold, broken queries, persistence in `dq_results` |
| Delta I/O and maintenance | `tests/src/common/*`, `tests/etl_jobs/maintenance/*` | 15 | partition replace, empty scope, writes outside the predicate refused, pinned reads, optimize/vacuum |
| Observability | `tests/src/observability/*`, `tests/etl_jobs/observability/test_health_report.py` | 35 | run/step registry, metrics, alert rules, report, critical tables left stale by a failed run |
| Lineage | `tests/src/lineage/test_graph.py`, `tests/etl_jobs/observability/test_lineage.py` | 21 | SQL references, transitive walks, criticality from the contract, Mermaid output, SQL agrees with the declared inputs, no cycles, **committed `lineage.md` is up to date**, impact CLI |
| Utilities, generator, UI | `tests/src/utils/*`, `tests/scripts/*`, `tests/test_run_duckdb_ui.py` | 26 | paths, logger, YAML reader, synthetic generator, DuckDB UI views |

**Pass criterion:** all tests green. Lint with `uv run ruff check etl_jobs src tests` (a few
findings in `scripts/generate_sample_data.py` and `src/utils/yaml_reader.py` predate the Silver
work).

## 2. End-to-end smoke test on the challenge sample

Runs the three layers on `docs/sample-data` and compares with numbers verified by hand. It uses a
clean Bronze, Silver, Gold and observability; everything under `data/` is generated and
reproducible.

```bash
make up
rm -rf data/bronze data/silver data/gold data/observability
docker compose exec -e DATA_PATH=/app/docs/sample-data pipeline python -m etl_jobs.bronze.ingest_to_bronze
make silver
make gold
make queries
```

**Expected results**

| Table | Bronze rows | Silver rows | Why they differ |
|---|---|---|---|
| `transactions` → `transactions_history` / `transactions` | 1060 | 1060 / 1000 | 1060 CDC events of 1000 transactions; 5 are deleted (`is_deleted`), all BRL |
| `settlement_paysettler` → `settlements` | 510 | 500 | 10 repeated lines collapsed; 27 `REVERSED`; one `reference_date` (2025-03-16, from `max(settled_at)` because the file name has no date) |
| `reconciliation_runs` | 19 | 10 | I + U versions of 10 runs (run 7 failed and has only the I version) |
| `reconciliation_results` | 837 | 788 + 5 in quarantine | 44 duplicates removed (run 1: 119 → 106); `run_id = 99` quarantined; 16 merchants filled from the transaction (`merchant_id_origin = 'transaction'`) |
| `enterprise_company` → `merchants` | 70 | 62 | 50 merchants, 50 current versions; 8 updates without attribute changes collapsed |

Silver categories: 685 `MATCHED`, 59 `MISMATCHED`, 25 `UNRECONCILED_PROCESSOR`, 19
`UNRECONCILED_INTERNAL`.

| Check | Expected status |
|---|---|
| every `error` check | `pass` (the run ends `success`) |
| `relationships:reconciliation_results.run_id` | `warn`, 5 rows (run 99) |
| `run_total_coherence:reconciliation_runs` | `warn`, 9 runs (the service totals count the duplicates) |
| any other check | `pass` |

Alerts: `results_quarantined` and `settlement_duplicates` warn. Verify with the queries of
section 5.

**Expected Gold results**

| Table | Rows | Why |
|---|---|---|
| `dim_date` | 31 | 2025-03-01 to 2025-03-31 |
| `dim_category` | 4 | static |
| `dim_merchant` | 63 | 62 Silver versions + `UNKNOWN` |
| `fact_reconciliation_run` | 10 | one per run; 9 reference runs (run 7 failed) |
| `fact_reconciliation_result` | 788 | one per Silver result |
| `fact_transaction_event` / `fact_transaction` | 1060 / 1000 | one per Silver event / transaction |
| `fact_settlement` | 500 | one per Silver settlement |
| `agg_reconciliation_daily` | 28 | 2025-03-04 to 2025-03-31; cadence 3 days, 10 expected dates, 1 missing (2025-03-22) |
| `agg_amount_at_risk_daily` | 9 | 9 reference runs, all BRL |
| `agg_volume_daily` | 83 | 30 days × 3 statuses, minus empty combinations |
| `agg_settlement_daily` | 1 | 2025-03-16, BRL: 473 settled, 27 reversed |

KPIs: discrepancy rate between 11.36% and 14.58% per run; COMPLETED volume R$ 21,124,903.04 in
March. The answers of every example query are in [`analytics.md`](analytics.md).

| Gold check | Expected status |
|---|---|
| every `error` check | `pass` (the run ends `success`) |
| `missing_run:agg_reconciliation_daily` | `warn`, 1 row (2025-03-22) |
| `reconciliation_recheck:fact_reconciliation_result` | `warn`, 544 differences for run 5 (2025-03-16, the only date with a settlement file): its 93 verdicts plus the 500 lines of the CSV ([`gold-layer.md`](gold-layer.md), section 3) |
| any other check | `pass` |

Gold alerts: `reconciliation_runs_missing` warns; `discrepancy_rate_spike` passes (the latest
deviation is −2.08 p.p.).

## 3. Idempotency and failure scenarios

Start from the state of section 2 (or of `make generate` + `make bronze` + `make silver`). Each
scenario lists how to reproduce it, what must happen and where to look. Automated coverage is the
test that asserts the same behavior.

| # | Scenario | Steps | Expected | Where to check | Automated by |
|---|---|---|---|---|---|
| 1 | Bronze rerun | `make bronze` twice | second run: every file `skipped`, no new rows, run `success` | `pipeline_run_step.status`; row counts | `test_rerun_skips_already_ingested_file` |
| 2 | Silver, same date twice | `make silver REFERENCE_DATE=2025-03-16` twice | the same content after each run: same count and hash per table (query 5.3); one new Delta version per table and run | query 5.3; `version_after` per publish step | `test_rerun_for_the_same_reference_date_is_idempotent` |
| 3 | Date run does not touch other dates | change a file of another date in Bronze, then run the date D | partitions of D replaced; other dates unchanged until their own date runs | query 5.4 (counts per partition) | `test_reference_date_run_only_replaces_its_partitions` |
| 4 | File resent with new content, without `--reprocess` | drop a changed copy with the same name in `data/landing` and `make bronze`, then `make silver` | Bronze keeps both loads; Silver uses only the latest load of that `_source_file` | Bronze `_ingested_at` per `_source_file`; Silver values | `test_only_the_latest_load_of_a_source_file_is_used` |
| 5 | Reprocess | `make reprocess FILES="..." REASON="..."`, then `make silver` | Bronze rows of the file replaced; Silver identical to a fresh build | `pipeline_run.mode = reprocess`; query 5.3 | Bronze reprocess tests |
| 6 | Blocking data quality check | temporarily add `{name: always_fails, table: transactions, severity: error, query: SELECT 1}` to `etl_jobs/silver/quality_checks.yml` and `make silver`; remove it afterwards | exit code 1, run `failed`, **no Silver table changes** (no new Delta version) | `dq_results` (`fail`); `pipeline_run.error`; Delta history | `test_blocking_check_prevents_publishing` |
| 7 | Failure in the middle of the publish | interrupt `make silver` (Ctrl+C) while the `publish:` steps are logged, then rerun the same command | first run `failed` (or unfinished) with some tables already replaced; the rerun ends `success` with the same content as an uninterrupted run | query 5.3 compared with scenario 2 | not automated (manual) |
| 8 | Silver before Bronze | `rm -rf data/bronze` and `make silver` | run `failed` with `Bronze table '<name>' not found`; nothing published | `pipeline_run.error` | `test_missing_bronze_table_fails_the_run` |
| 9 | Invalid CLI dates | `python -m etl_jobs.silver.build_silver --start-date 2025-03-10 --end-date 2025-03-01` | usage error, exit code 2, no run recorded | console | `test_invalid_date_arguments_are_rejected` |
| 10 | Contract break in a source | rename a column in a source file and `make bronze` | the file fails with `SchemaContractError`; other files continue; run `failed` | `pipeline_run_step.error` | Bronze schema tests |
| 11 | Gold, same date twice / full vs dates | `make gold`, then `make gold REFERENCE_DATE=2025-03-16` twice, then a range run | every Gold table has the same content as the full rebuild (query 5.3 on `gold.*`, excluding `_gold_run_id` and `_gold_built_at`) | query 5.3 | `test_rerun_for_the_same_reference_date_is_idempotent` |
| 12 | Gold date run does not touch other dates | remove the results of one run in Silver, run Gold for another date, then for the run's date | the run's rows stay until its own date runs | query 5.4 on `gold.fact_reconciliation_result` | `test_reference_date_run_only_replaces_its_partitions` |
| 13 | Gold before Silver | `rm -rf data/silver` and `make gold` | run `failed` with `Silver table '<name>' not found`; nothing published | `pipeline_run.error` | `test_missing_silver_table_fails_the_run` |
| 14 | Broken example query | change a column used by a query in `etl_jobs/gold/queries/` | `make queries` logs the failing query and exits 1; `make test` fails | console | `test_shipped_query_runs_against_gold`, `test_failing_query_exits_with_error` |

## 4. Scale test

The README asks to design for ~5M transactions/month and to grow toward 5M/day. This section
measures where the current design stands.

**Procedure** (it replaces `data/landing`, Bronze and Silver):

```bash
make generate-large                        # 1M transactions, 90 days, 500 merchants
rm -rf data/bronze data/silver              # a different dataset: rebuild the layers
time make bronze
time make silver
time make silver REFERENCE_DATE=2025-03-16
docker stats --no-stream                    # in another terminal while the jobs run: memory
```

**On Windows, run the jobs on the host.** Docker Desktop mounts `./data` into the container over
9p (drvfs), and delta-rs fails to write Parquet files larger than a few MB there
(`DeltaError: ... Generic LocalFileSystem error: Upload aborted`), leaving Parquet files without a
`_delta_log`. The 1M dataset hits it on `transactions` and `reconciliation_results`. Generation
still works in the container; the jobs run on the host with uv (this is how the results below were
measured):

```bash
make generate-large
rm -rf data/bronze data/silver
time uv run python -m etl_jobs.bronze.ingest_to_bronze
uv run python -m etl_jobs.maintenance.optimize_tables
time uv run python -m etl_jobs.silver.build_silver
time uv run python -m etl_jobs.silver.build_silver --reference-date 2025-03-16
time uv run python -m etl_jobs.gold.build_gold
time uv run python -m etl_jobs.gold.build_gold --reference-date 2025-03-16
```

Cloning the repository inside the WSL2 filesystem (`\\wsl$\<distro>\home\...`) and running Docker
from there also avoids it, because the bind mount becomes native ext4.

For the README volume of ~5M/month, generate with the script directly:

```bash
docker compose exec pipeline rm -rf /app/data/landing
docker compose exec pipeline python scripts/generate_sample_data.py \
  --rows 5000000 --days 30 --merchants 500 --seed 42 --out /app/data/landing
```

Durations per step are in `pipeline_run_step.duration_s` (query 5.5).

**Expected warnings, not defects**

- `rows_read_deviation` (warn) on **every file of the first Bronze run after changing the volume**,
  when `data/observability` is kept. The rule compares each file with the median of the last 7
  values of the same table, and that history belongs to the previous, smaller dataset. The alert is
  doing its job (the volume jumped by an order of magnitude); the pipeline simply does not know the
  change is intentional. Deleting `data/observability` too avoids it, at the cost of losing the run
  history. Later runs do not repeat it, because ingested files are skipped by hash.
- `too_many_files` (warn) on the partitioned Silver and Gold tables: with 90 days they have ~90
  files (one per partition) even after optimize. The rule counts files of the whole table, which fits the
  unpartitioned Bronze but not partitioned tables.

**Recorded results**

Host run (not the container), Windows 11, 12 logical CPUs, 15.7 GiB RAM, measured on 2026-10-10
with a process monitor (peak resident memory of the job process). Fresh observability, so no
deviation warnings.

| Dataset | Job | Wall time | Peak memory | Notes |
|---|---|---|---|---|
| sample (1k) | Bronze | 1.2 s | 113 MiB | |
| sample (1k) | Silver full | 2.9 s | 162 MiB | |
| 1M / 90 days (105 settlement files, 951k CDC events, 1M results) | Bronze | 20.8 s | 1.1 GiB | ingest steps 14.5 s |
| 1M / 90 days | maintenance | 10.0 s | 0.9 GiB | first optimize after ingestion |
| 1M / 90 days | Silver full | 25.2 s | **3.2 GiB** | compute 8.3 s, checks 1.8 s, publish 3.8 s; the rest is reading Bronze and table metrics |
| 1M / 90 days | Silver one date | 18.4 s | 2.4 GiB | compute 6.6 s, publish 0.5 s |
| 1M / 90 days | Gold full | 74.4 s | **3.9 GiB** | compute 20.3 s, checks 2.1 s, publish 5.6 s; the rest is reading Silver and table metrics |
| 1M / 90 days | Gold one date | 59.0 s | 2.8 GiB | compute 18.7 s, publish 1.3 s |
| 1M / 90 days | example queries | 23.0 s | | all example queries |
| 5M / 30 days | all jobs | see the end-to-end table below | | |

Storage at 1M: Bronze 310 MB, Silver 490 MB, Gold 372 MB (current version; the four large facts
take ~60–80 MB each, aggregates a few MB). Data quality at 1M: all `error` checks pass;
`run_total_coherence` warns on 90 runs and `relationships:reconciliation_results.transaction_id`
on 95 results (transactions refused by the Bronze contract). Gold at 1M: all `error` checks pass,
including row and sum conservation; `currency_unknown` warns on 3 verdicts and
`reconciliation_recheck` on 7,555 differences (99.5% of the service verdicts agree with the
recomputation; [`gold-layer.md`](gold-layer.md), section 3).

The recheck only counts the transactions the service judged or the processor sent. Counting every
internal transaction of each 7-day window gave 6.66M "differences" for 1M transactions, because the
service judges an unmatched transaction in one run only; the scale test is what showed it.

**End-to-end runs per source scale** (2026-10-11, commit `df74e30-dirty`, fresh Bronze, Silver,
Gold and observability for each level). Sample and 10k run in the container through `make`; 1M and
5M run on the host (9p limitation above). Memory is the peak working set of the job process; the
value in parentheses is the peak committed memory (RAM + page file).

| Level | Source | Bronze | Silver | Gold full | Gold one date | Queries | Result |
|---|---|---|---|---|---|---|---|
| sample | `docs/sample-data` (1k) | 2.0 s | 8.9 s* | 14.2 s* | | 2.0 s | identical to section 2 |
| small | `make generate` (10k, 30 days) | 14.2 s | 11.3 s* | 17.1 s* | | 2.8 s | all `error` checks pass; 4 dates without a run detected (the generator has no run for them); recheck agrees on 99.5% |
| large | `make generate-large` (1M, 90 days) | 22.5 s, 1.0 GiB | 20.6 s, 3.1 GiB | 34.5 s, 4.1 GiB (6.3) | 25.5 s, 2.7 GiB | 10.3 s | all `error` checks pass; recheck agrees on 99.5% (7,555 differences) |
| README volume | generator, 5M, 30 days | 36.2 s, 3.3 GiB (6.3) | 77.1 s, 8.0 GiB (**16.8**) | 155.8 s, 7.9 GiB (**20.6**) | 118.6 s, 8.3 GiB (13.4) | 9.0 s | all `error` checks pass; recheck agrees on 99.5% (38,162 differences); `discrepancy_rate_spike` fired (see below) |

\* includes the maintenance run by `make silver` / `make gold`.

Storage at 5M (current Delta versions): Bronze 954 MB, Silver 1.24 GB, Gold 1.51 GB; source files
1.2 GB. Maintenance: 33–35 s per run.

Findings of these runs:

- **5M/month completes on a 15.7 GiB host, at its limit.** Silver and Gold commit 16.8 and 20.6 GiB:
  more than the RAM, so Windows pages to disk. Working set grows less than linearly (3.1 → 8.0 GiB
  for 5× the data), but the committed memory is what a container limit would kill (Docker Desktop
  has 8 GiB here). The README growth target (~5M per **day**) is 30× this volume: the in-memory
  "whole layer per run" design does not reach it; partition pruning on read and `MERGE` are required
  (`silver-layer.md` and `gold-layer.md`, section 9).
- **A one-date Gold run costs ~75% of a full one** (118.6 s vs 155.8 s), because it computes the
  whole layer and only publishes less.
- **`discrepancy_rate_spike` fired at 5M on 2025-04-14**, with +7.5 p.p. That date has 89 results
  against thousands on normal days: the generator stops creating transactions after day 30, and the
  last runs only see late settlements. **Fixed:** a date is only compared when it has at least half
  of the median result count of the previous 28 dates (`is_comparable_volume`), and non-comparable
  dates no longer enter the rate baseline. A first version used the median of only the previous 5
  dates and drifted down with the tail (at 10k, 2-result dates became comparable again and showed
  +46 p.p.); the 28-date volume baseline fixed it. At 10k the whole April tail is now "low volume",
  while a real spike at normal volume (2025-03-25, +7.3 p.p.) is still detected.
- **Git Bash rewrites container paths.** The first sample run used `-e DATA_PATH=/app/docs/sample-data`
  from Git Bash, which became a Windows path: Bronze read nothing and ended `success`, and only
  Silver failed. Use `MSYS_NO_PATHCONV=1` (user guide, troubleshooting). **Fixed in Bronze:** a
  source folder that does not exist now stops the job with exit code 1 before anything is written
  (an existing empty folder is still a valid run, e.g. a rerun with nothing new).

**Reading the numbers**

- Silver memory grows with the whole Bronze, not with the date: ~3.2 GiB for 1M results and
  transactions. By linear extrapolation, 5M/month would need ~15 GiB, more than this host and
  the default Docker Desktop limit. That is the first point where the design breaks (Part 3.3 of
  the README).
- A one-date run saves on writing (0.5 s vs 3.8 s) but not on compute or memory, because it
  computes the whole layer. The fix is the one listed in `silver-layer.md` (section 9): partitioned
  Bronze with `[D − 7, D]` pruning on read, and `MERGE` for the current-state tables.
- Bronze loads each file whole into memory; at 1M it peaks at ~1.1 GiB.
- Gold has the same shape as Silver and is the heaviest job (3.9 GiB at 1M): it reads all of Silver
  and keeps every fact in memory before publishing. Partition pruning on the Silver read and
  `MERGE` into `fact_transaction` are the same fix (`gold-layer.md`, section 9).

## 5. Verification queries

Run in the DuckDB UI (`make ui`), where every Delta table is a view `<layer>.<table>`.

**5.1 Last run of each job**

```sql
SELECT job, mode, status, round(duration_s, 1) AS duration_s, error
FROM observability.pipeline_run
QUALIFY row_number() OVER (PARTITION BY job ORDER BY started_at DESC) = 1;
```

**5.2 Checks and alerts that did not pass in the last Silver run**

```sql
WITH last_run AS (
    SELECT run_id FROM observability.pipeline_run
    WHERE job = 'silver.build' ORDER BY started_at DESC LIMIT 1
)
SELECT 'check' AS kind, check_name AS name, status, failing_rows AS value, message
FROM observability.dq_results WHERE run_id IN (SELECT run_id FROM last_run) AND status <> 'pass'
UNION ALL
SELECT 'alert', rule, status, value, message
FROM observability.pipeline_alerts WHERE run_id IN (SELECT run_id FROM last_run) AND status <> 'pass';
```

**5.3 Content fingerprint of a Silver table (idempotency)**

Same count and hash before and after a rerun mean the same content. The Silver run columns are
excluded because they change on every run by design.

```sql
SELECT count(*) AS n,
       sum(hash(*COLUMNS(* EXCLUDE (_silver_run_id, _silver_built_at))))::VARCHAR AS fingerprint
FROM silver.settlements;
```

**5.4 Rows per partition**

```sql
SELECT reference_date, count(*) AS n FROM silver.reconciliation_results GROUP BY 1 ORDER BY 1;
```

**5.5 Duration per step of the last Silver run**

```sql
SELECT step, status, rows_in, rows_out, version_after, round(duration_s, 2) AS duration_s, inputs
FROM observability.pipeline_run_step
WHERE run_id = (SELECT run_id FROM observability.pipeline_run
                WHERE job = 'silver.build' ORDER BY started_at DESC LIMIT 1)
ORDER BY started_at;
```
