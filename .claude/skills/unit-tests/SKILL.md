---
name: unit-tests
description: Write or extend pytest unit tests for a module of the data-engineer-challenge repo, following its conventions (mirrored paths, isolated Delta tables, no comments) and a standard catalog of cases (types/schema, extreme and boundary values, nulls/empty input, special strings, timestamps, errors, idempotency). Use when the user asks to create, add or improve tests for a component, or to check what a module is missing.
allowed-tools: Bash(uv *) Bash(make *) Bash(docker compose exec *) Read Glob Grep Edit Write
---

Create unit tests for one module (or the modules the user names) that pass on the host **and** in
the container, and report which standard cases were covered and which do not apply.

## 1. Locate the module and its test file

- Test path mirrors the source path: `src/x/y.py` → `tests/src/x/test_y.py`,
  `etl_jobs/a/b.py` → `tests/etl_jobs/a/test_b.py`, root `run_duckdb_ui.py` →
  `tests/test_run_duckdb_ui.py`. One test file per module.
- If the test file exists, read it first and **extend** it; do not duplicate cases already there.
- Read the module and list its public behaviors: functions/classes used by other modules, CLI
  arguments, files or tables it writes, exceptions it raises, log messages other code relies on.
- Shared fixtures go in a `conftest.py` in the same test folder (see
  `tests/src/data_contract/conftest.py`: `contract`, `orders`). Reuse them before creating new ones.

## 2. Standard case catalog

Go through every category. Write the tests that apply; for the ones that do not, say why in the
final report (not in the code). Prefer `@pytest.mark.parametrize` for tables of inputs.

### 2.1 Happy path

The main behavior with small, realistic input. Assert on the **output values**, not only on "no
exception". For jobs, assert the rows written, the run/step status and the exit code.

### 2.2 Types and schema

- Arrow types produced or required, per contract `physicalType`: `bigint`→`int64`,
  `integer`→`int32`, `double`→`float64`, `varchar`→`string` (and `large_string` accepted),
  `boolean`, `date`→`date32`.
- Wrong type in the input (e.g. `amount` as string) → the documented error
  (`SchemaContractError`, `ValueError`...) with `pytest.raises(..., match=...)`.
- Return types the callers depend on (`set`, `list[str]`, `pa.Table`, `float` metrics,
  `int | None` versions).
- Schema drift: missing optional column, missing required column, renamed column (case/underscore
  only), new undeclared column.
- CSV sources are read as text: numbers and dates arrive as `str`.

### 2.3 Extreme and boundary values

| Kind | Cases |
|---|---|
| Size | empty table/file (0 rows), one row, file with only a header, empty list/dict arguments |
| Boundaries | exactly **at** the limit, just below, just above (`minimum`/`maximum`, `minLength`/`maxLength`, alert `value` with `>` vs `>=`, `min_history`, tolerance 0.01) |
| Numbers | 0, negative, very large (`2**63 - 1`, `1e18`), very small (`1e-9`), `float("nan")`, `float("inf")` where floats are accepted, division by zero paths (e.g. rate with 0 rows) |
| Nulls | null in required vs optional fields, all-null column, null in a key column |
| Strings | empty string, whitespace only, single quote `'` and double quote `"` (SQL/identifier escaping), `|` and newline (Markdown report), accents and emoji, very long values |
| Timestamps | ISO with `Z`, with `+00:00`, without timezone, date only, invalid text, future dates |
| Collections | duplicated keys/values, unsorted input, many items (enough to cross a threshold such as files per table) |

### 2.4 Errors and edge paths

- Every `raise` in the module has a test with `pytest.raises(ExactType, match="stable part of the message")`.
- Missing file/table/folder, path that is a **file** where a folder is expected.
- Jobs: `SystemExit` code (`1` for pipeline failure, `2` for argparse errors).
- A failure in one item must not stop the others when the code promises that (e.g. maintenance per
  table, ingestion per file).

### 2.5 Idempotency and state

- Running twice gives the same result (no duplicated rows, same versions when nothing changed).
- Reprocess/replace paths remove the previous rows they claim to remove, and only those.
- Caches are invalidated when the underlying state changes (assert the reload count with a
  `monkeypatch`-wrapped function, as in `test_skipped_steps_do_not_reload_the_table`).

### 2.6 Observability side effects (jobs and `src/observability`)

When the code runs inside `pipeline_run`/`run.step`: assert the `pipeline_run` status and error,
step status (`success`/`skipped`/`failed`), row counts, the metrics recorded, alert statuses, and
log lines with `caplog` when they are part of the behavior.

## 3. Repository rules for tests

- **No comments or docstrings** in test code; the test name says what it checks:
  `test_<behavior>_<condition>` (e.g. `test_deviation_waits_for_minimum_history`).
- **Never touch real data.** Use `tmp_path` for Delta tables and files; monkeypatch module globals
  of the code under test (`BRONZE_DIR`, `OBSERVABILITY_DIR`, `SOURCE_DATA_DIR`,
  `BRONZE_CONTRACTS_DIR`, `ALERT_RULES_FILE`, `OBSERVABILITY_REPORTS_DIR`) and env vars
  (`GIT_SHA`, `PROJECT_ROOT`, `DATA_PATH`) with `monkeypatch`. Patch the name **in the module that
  uses it** (`monkeypatch.setattr(ingest_to_bronze, "BRONZE_DIR", ...)`), not in `src.utils.paths`.
- Job tests that are not about alerts use an empty rules file (`rules: []`); alert behavior is
  tested with the real `ALERT_RULES_FILE` explicitly.
- **Determinism:** reading a Delta table written by several commits has no guaranteed row order:
  sort (`ORDER BY` in DuckDB, `table.sort_by(...)`, `sorted(...)`) before comparing. No `sleep`;
  compare durations with regexes or `pytest.approx`.
- **Timestamps with timezone:** do not call `to_pylist()`/`fetchall()` on tz-aware columns (fails on
  Windows without `tzdata`). Select the other columns, sort by the timestamp in Arrow/DuckDB, or
  format it in DuckDB after `SET TimeZone = 'UTC'`.
- **Linux vs Windows:** some library behavior differs (e.g. `DeltaTable.is_deltatable` raises on a
  file path on Linux). Tests touching the filesystem must also pass in the container.
- Import from the real module path (`from src.observability.alerts import ...`,
  `from etl_jobs.bronze import ingest_to_bronze`). Pytest runs with `--import-mode=importlib`; do
  not add `__init__.py` under `tests/`.
- Builders over fixtures for test data: small functions such as `orders(ids, statuses)` that return
  a `pa.Table`, with explicit types when they matter (`pa.array([...], pa.string())`).
- Use `with (a, b as x):` instead of nested `with` (ruff `SIM117`).

## 4. Run and verify

```bash
uv run --extra dev pytest -q tests/<path>/test_<module>.py     # the new/changed file
uv run --extra dev pytest -q                                  # whole suite
uv run --extra dev ruff check tests                           # lint the tests
make test                                                     # same suite in the container (Linux)
```

If `make test` runs an old copy of a root-level file or of `pyproject.toml`, rebuild the image first
(`make up`); `src/`, `etl_jobs/` and `tests/` are mounted and need no rebuild.

A failing new test is either a wrong test or a **real bug**. Do not weaken the assertion to make it
pass: confirm the expected behavior in the module/docs; if it is a bug, report it (and fix it only
if the user asked for fixes).

## 5. Report

- Test file(s) created or extended and the number of tests added.
- Catalog coverage: which categories of §2 were covered, and which were skipped and why.
- Commands run and results (host and container).
- Bugs or ambiguous behaviors found.
