---
name: review-changes
description: Review the changes of the current branch (commits plus uncommitted work) of the data-engineer-challenge repo - infer the objective, check that the change achieves it, that errors are handled the way the pipeline expects, and that everything depending on the changed code, contracts, tables, paths, rules or docs was adapted (or does not need to be). Outputs a short verdict with what still has to change. Use when the user asks to review code/changes/a branch, before committing, and always as the first step of the open-pr skill.
allowed-tools: Bash(git *) Bash(uv *) Bash(make *) Bash(ls *) Read Glob Grep
---

Review what the branch changes, judged against **what it is trying to do**. This is a read-only
review: it reports, it does not fix. The output is short (§6); the work behind it is not.

## 1. Scope of the change

```bash
git branch --show-current
git fetch upstream main 2>/dev/null || git fetch origin main
base=$(git merge-base HEAD upstream/main 2>/dev/null || git merge-base HEAD origin/main)
git log --oneline --no-merges "$base"..HEAD
git diff --stat "$base"                        # committed + uncommitted, against the base
git status --short                             # untracked files are part of the change too
```

- The change = `git diff "$base"` plus untracked files that belong to the work. Read **every**
  changed file in full, not only the hunks: a hunk does not show what the rest of the function
  assumes.
- Ignore and flag if staged/committed: `data/`, `.venv/`, `.idea/`, `.claude/settings.local.json`,
  local context files (`claude_*_context.txt`, `Claude context*`). They must never reach a PR.
- When the user names a narrower scope (a file, a commit, "only Silver"), review that scope but
  still run §4 on it.

## 2. Objective

Infer it, in this order, and stop at the first that is clear: what the user said in the
conversation; the branch name (`feat/silver-layer`); commit messages; the docs changed in
`docs/pipeline_docs/`; the challenge `README.md` part it answers. Write it in one or two sentences
with the expected observable result (e.g. "`make silver` builds the 7 Silver tables, idempotent per
`reference_date`, blocked by `error` checks").

Do not ask the user to confirm it. If it is ambiguous, state the assumption in the output and
review against it.

## 3. Does it achieve the objective

- Trace the main path end to end: entry point (`make` target / CLI `main`) → functions → what is
  written (Delta table, observability rows, report) → what is logged.
- Check the edge cases the objective implies: empty input, rerun (idempotency), partial failure,
  first run with no previous state, schema drift, Windows host and Linux container.
- Run the checks; never report one you did not run:

```bash
uv run --extra dev ruff check src etl_jobs tests run_duckdb_ui.py
uv run --extra dev pytest -q
```

  For pipeline behavior, when data is available, run the job on the host (`uv run python -m ...`,
  see the `troubleshooting` skill for why not `make` on Windows) and look at the run status and row
  counts. A failure caused by the environment is reported as such, not as a defect of the change.
- Tests: the changed behavior has tests in the mirrored path (`tests/src/...`, `tests/etl_jobs/...`)
  covering the main path and the error paths; follow the `unit-tests` skill conventions.

## 4. Error handling

What the pipeline expects; flag each deviation with file and line:

| Rule | Look for |
|---|---|
| Failures are isolated where the job promises it | Bronze: one file failing does not stop the others (`ingest_file` in a `try`, collected in `failed`); maintenance: per table. Silver and Gold: any exception fails the whole run |
| A failure fails the run | `run.mark_failed(...)` with a useful message, `sys.exit(1)`, step status `failed` with the error stored in `pipeline_run_step.error` |
| Nothing swallowed | no bare `except:`/`except Exception: pass`; caught exceptions are logged with `logger.exception` (traceback) or re-raised; no `print` |
| Writes are atomic and safe to rerun | one Delta commit per table write; Silver and Gold compute → check → publish (no publish after a blocking check); reruns converge (hash skip in Bronze, `replaceWhere` in Silver and Gold) |
| Invalid input is rejected with a clear error | contract violations raise `SchemaContractError`/`ContractValidationError`; bad CLI arguments use `parser.error` (exit 2); config files (`alert_rules.yml`, `quality_checks.yml`) are validated on load |
| Bad rows are not silently dropped | refused rows go to `<dataset>_refused`, orphans to quarantine, dedups are counted in metrics |
| Messages are actionable | the error names the file/table/check and what was expected; logs in English |

## 5. Dependencies of the change

For each changed item, find what depends on it (`Grep` for the name across `src`, `etl_jobs`,
`tests`, `docs`, `Makefile`, `docker-compose.yml`, `pyproject.toml`) and decide **adapted / not
needed / missing**. Known couplings in this repo:

| If the change touches | Then check |
|---|---|
| A Bronze contract (`etl_jobs/bronze/contracts/*.yml`) | `version` bumped; Silver SQL reading the column; `register_bronze` NULL-fill for removed optional columns; `data-contracts.md` |
| A Silver contract or SQL (`etl_jobs/silver/contracts`, `sql/`) | both sides agree (`conform` rejects undeclared columns); `primaryKey`/`required` imply `unique`/`not_null` checks; `quality_checks.yml` queries using the column; **Gold SQL and Gold checks reading the column** (`silver_<table>` views); `silver-layer.md` |
| A Gold contract or SQL (`etl_jobs/gold/contracts`, `sql/`) | both sides agree; Gold `quality_checks.yml`; the example queries in `etl_jobs/gold/queries/` (`test_run_queries.py` runs them); `gold-layer.md` and `analytics.md` (rules and answers on the sample) |
| `PHYSICAL_TYPES` / contract validator | contract file validator accepts the type; tests parametrized over every contract |
| A new or renamed table | Bronze: `DATASET_PATTERNS`, `EVENT_COLUMNS`, `BRONZE_ZORDER` (+ `_refused`); Silver/Gold: `SILVER_TABLES`/`GOLD_TABLES` order and `inputs`, `SILVER_ZORDER`/`GOLD_ZORDER`, scope/partition column; DuckDB UI picks it up automatically; docs table lists |
| `src/observability/store.py` schemas | **existing Delta tables in `data/observability` keep the old schema** (a renamed/removed column breaks appends: document the migration or the reset); `run_registry.py`, `report.py`, `alerts.py`, `observability.md` §4 |
| `alert_rules.yml` / `quality_checks.yml` | metric/table names exist and are emitted by a job; severity choice (an `error` stops the run and downstream); `observability.md` §5 / `silver-layer.md` / `gold-layer.md` catalog |
| `src/utils/paths.py` | every test that monkeypatches the old name; `docker-compose.yml` mounts and `DATA_PATH` |
| `src/common`, `src/data_contract`, `src/data_quality` (shared code) | every caller in Bronze, Silver, Gold, maintenance and tests, not only the one the change was made for (`layer_build.py` is shared by Silver and Gold) |
| `pyproject.toml` | `uv.lock` updated; the container image needs `make up`; mention it in the PR |
| `Makefile` | `.PHONY`, `make help` text, `user-guide.md` commands |
| A Silver/Gold SQL, `SILVER_TABLES`/`GOLD_TABLES`, or a `critical` custom property in a contract | `make lineage` was run (`docs/pipeline_docs/lineage.md` regenerated; `test_committed_lineage_doc_is_up_to_date` fails otherwise) |
| Behavior described in docs | `docs/pipeline_docs/*.md` and `README.md` sections still true (commands, table lists, numbers, limits) |

Repository rules that also apply to the diff: no comments or docstrings in code; logs in English;
tests mirror the module path, one file per module.

## 6. Output

Keep it short, in the user's language, in this structure:

```
**Objective (understood):** one or two sentences. Assumption, if any.

**Meets the objective:** Yes / Partially / No — one sentence why, with the checks run and their result.

**Error handling:** what is good, and each gap with `file:line`.

**Other relevant points:** dependencies adapted or not needed (one line each, only the relevant
ones), tests, docs, risks (scale, environment, data migration).

**Needs change before it works / before the PR:**
1. `file:line` — what and why (blocking)
2. ...
Optional improvements (non-blocking) listed separately, or "Nothing blocking."
```

Only list what you verified. Separate **blocking** (the objective is not met, a dependent piece is
broken, a check fails, an error is swallowed, forbidden files staged) from **non-blocking**
(style, docs polish, follow-ups).
